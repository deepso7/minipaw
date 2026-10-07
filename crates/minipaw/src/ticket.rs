//! The `mp…` address a server prints and a client pastes.
//!
//! Layout before base64url: `[version][flags][token; 16][peer-id len][peer-id]`
//! and, when flag `EMBEDDED_RELAY` is set, `[relay len][relay multiaddr]`.
//! Servers on the default relay leave it out to keep tickets short.

use std::fmt;
use std::str::FromStr;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use minip2p::{Multiaddr, PeerAddr, PeerId};

use crate::wire::Token;

const PREFIX: &str = "mp";
const VERSION: u8 = 1;
const EMBEDDED_RELAY: u8 = 1;

/// What a dialer needs to reach a listener: its peer id, a secret token,
/// and the relay it is reachable through. Its text form starts with `mp`;
/// parse one with [`str::parse`].
///
/// Anyone holding a ticket can connect, until the listener has a peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ticket {
    pub(crate) peer: PeerId,
    pub(crate) token: Token,
    /// `None` means the built-in default relay.
    pub(crate) relay: Option<PeerAddr>,
}

impl Ticket {
    /// The listener's peer id.
    pub fn peer(&self) -> &PeerId {
        &self.peer
    }

    /// The relay the listener is reachable through, or `None` for
    /// [`DEFAULT_RELAY`](crate::DEFAULT_RELAY).
    pub fn relay(&self) -> Option<&PeerAddr> {
        self.relay.as_ref()
    }
}

/// Why a string is not a valid [`Ticket`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TicketError(String);

impl fmt::Display for TicketError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TicketError {}

impl fmt::Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let peer = self.peer.to_bytes();
        let relay = self.relay.as_ref().map(|r| r.to_multiaddr().to_bytes());
        let mut raw = vec![VERSION, if relay.is_some() { EMBEDDED_RELAY } else { 0 }];
        raw.extend_from_slice(&self.token);
        push_field(&mut raw, &peer);
        if let Some(relay) = &relay {
            push_field(&mut raw, relay);
        }
        write!(f, "{PREFIX}{}", B64.encode(raw))
    }
}

/// Ticket fields carry a one-byte length.
const MAX_FIELD: usize = u8::MAX as usize;

/// Whether `relay` fits in a ticket; check before printing one.
pub(crate) fn check_relay(relay: &PeerAddr) -> Result<(), String> {
    let len = relay.to_multiaddr().to_bytes().len();
    if len > MAX_FIELD {
        return Err(format!(
            "relay address is {len} bytes encoded; tickets fit at most {MAX_FIELD}"
        ));
    }
    Ok(())
}

fn push_field(raw: &mut Vec<u8>, field: &[u8]) {
    raw.push(u8::try_from(field.len()).unwrap_or(u8::MAX));
    raw.extend_from_slice(field);
}

impl FromStr for Ticket {
    type Err = TicketError;

    fn from_str(s: &str) -> Result<Self, TicketError> {
        parse(s).map_err(TicketError)
    }
}

fn parse(s: &str) -> Result<Ticket, String> {
    let body = s
        .trim()
        .strip_prefix(PREFIX)
        .ok_or_else(|| format!("not a minipaw address (expected a '{PREFIX}' prefix)"))?;
    let raw = B64
        .decode(body)
        .map_err(|e| format!("invalid minipaw address: {e}"))?;
    let mut r = raw.as_slice();

    let [version, flags, rest @ ..] = r else {
        return Err("minipaw address is truncated".into());
    };
    if *version != VERSION {
        return Err(format!("unsupported minipaw address version {version}"));
    }
    r = rest;
    let (token, rest) = r
        .split_first_chunk::<16>()
        .ok_or("minipaw address is truncated")?;
    r = rest;
    let peer = PeerId::from_bytes(take_field(&mut r)?)
        .map_err(|e| format!("invalid peer id in minipaw address: {e}"))?;
    let relay = if flags & EMBEDDED_RELAY != 0 {
        let addr = Multiaddr::from_bytes(take_field(&mut r)?)
            .map_err(|e| format!("invalid relay in minipaw address: {e}"))?;
        Some(
            PeerAddr::from_multiaddr(&addr)
                .map_err(|e| format!("invalid relay in minipaw address: {e}"))?,
        )
    } else {
        None
    };
    if !r.is_empty() {
        return Err("minipaw address has trailing bytes".into());
    }
    Ok(Ticket {
        peer,
        token: *token,
        relay,
    })
}

fn take_field<'a>(r: &mut &'a [u8]) -> Result<&'a [u8], String> {
    let (len, rest) = r.split_first().ok_or("minipaw address is truncated")?;
    let (field, rest) = rest
        .split_at_checked(*len as usize)
        .ok_or("minipaw address is truncated")?;
    *r = rest;
    Ok(field)
}

#[cfg(test)]
mod tests {
    use super::*;
    use minip2p::Ed25519Keypair;

    #[test]
    fn tickets_round_trip_with_and_without_a_relay() {
        let peer = Ed25519Keypair::generate().peer_id();
        let relay: PeerAddr = format!(
            "/ip4/203.0.113.7/udp/19876/quic-v1/p2p/{}",
            Ed25519Keypair::generate().peer_id()
        )
        .parse()
        .unwrap();

        for relay in [None, Some(relay)] {
            let ticket = Ticket {
                peer: peer.clone(),
                token: [3; 16],
                relay,
            };
            let text = ticket.to_string();
            assert_eq!(text.parse::<Ticket>().unwrap(), ticket);
        }
    }

    #[test]
    fn default_relay_ticket_stays_short() {
        let ticket = Ticket {
            peer: Ed25519Keypair::generate().peer_id(),
            token: [0; 16],
            relay: None,
        };
        assert!(ticket.to_string().len() <= 80, "{ticket}");
    }

    #[test]
    fn oversized_relays_are_rejected() {
        let peer = Ed25519Keypair::generate().peer_id();
        let addr = |host: &str| -> PeerAddr {
            format!("/dns/{host}/udp/19876/quic-v1/p2p/{peer}")
                .parse()
                .unwrap()
        };
        assert!(check_relay(&addr("relay.example.com")).is_ok());
        let long = format!("{}.com", "a".repeat(240));
        assert!(check_relay(&addr(&long)).is_err());
    }

    #[test]
    fn garbage_is_rejected() {
        assert!("".parse::<Ticket>().is_err());
        assert!("mp".parse::<Ticket>().is_err());
        assert!("mp!!!".parse::<Ticket>().is_err());
        assert!("tcAAAA".parse::<Ticket>().is_err());
    }
}
