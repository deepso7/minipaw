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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ticket {
    pub peer: PeerId,
    pub token: Token,
    /// `None` means the built-in default relay.
    pub relay: Option<PeerAddr>,
}

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

fn push_field(raw: &mut Vec<u8>, field: &[u8]) {
    raw.push(u8::try_from(field.len()).unwrap_or(u8::MAX));
    raw.extend_from_slice(field);
}

impl FromStr for Ticket {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
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
    fn garbage_is_rejected() {
        assert!("".parse::<Ticket>().is_err());
        assert!("mp".parse::<Ticket>().is_err());
        assert!("mp!!!".parse::<Ticket>().is_err());
        assert!("tcAAAA".parse::<Ticket>().is_err());
    }
}
