//! Session settings.

use minip2p::{Multiaddr, PeerAddr};

use crate::Error;

/// The relay used when neither [`Config::relay`] nor the ticket names one.
pub const DEFAULT_RELAY: &str = "/dns/relay.minip2p.com/udp/19876/quic-v1/p2p/12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf";

/// How a session connects. [`Config::default`] uses the built-in relay with
/// hole punching on.
///
/// ```
/// let mut config = minipaw::Config::default();
/// config.force_relay = true;
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct Config {
    /// The relay to go through; `None` means [`DEFAULT_RELAY`].
    ///
    /// A listener reserves a slot there and puts it in its ticket unless it
    /// is the default. A dialer uses it only for tickets that carry no
    /// relay of their own.
    pub relay: Option<PeerAddr>,
    /// Keep to the relay: no direct dials and no hole punching.
    pub force_relay: bool,
    /// For a dialer, a listener address to dial alongside the relay, for
    /// benchmarks and paths hole punching cannot find. The ticket's peer id
    /// is added to it.
    pub direct: Option<Multiaddr>,
    /// Test hook: once this many session bytes have arrived, a listener
    /// forgets its stream without closing it, as a relay that drops a
    /// circuit and tells only one side would.
    #[doc(hidden)]
    pub test_drop_link_after: Option<u64>,
}

/// Parses a relay address, checking that it is a QUIC address that fits in
/// a ticket.
///
/// ```
/// assert!(minipaw::parse_relay(minipaw::DEFAULT_RELAY).is_ok());
/// assert!(minipaw::parse_relay("garbage").is_err());
/// ```
///
/// # Errors
///
/// [`Error::Config`] when `raw` is not such an address.
pub fn parse_relay(raw: &str) -> Result<PeerAddr, Error> {
    let relay: PeerAddr = raw
        .parse()
        .map_err(|e| Error::Config(format!("invalid relay address '{raw}': {e}")))?;
    check(&relay, raw)?;
    Ok(relay)
}

/// Checks a relay that did not come through [`parse_relay`].
pub(crate) fn check_relay(relay: &PeerAddr) -> Result<(), Error> {
    check(relay, &relay.to_string())
}

fn check(relay: &PeerAddr, raw: &str) -> Result<(), Error> {
    crate::ticket::check_relay(relay).map_err(Error::Config)?;
    if !relay.transport().is_quic_transport() {
        return Err(Error::Config(format!(
            "relay must be a QUIC address (…/udp/<port>/quic-v1/p2p/<id>), got '{raw}'"
        )));
    }
    Ok(())
}

pub(crate) fn default_relay() -> Option<PeerAddr> {
    DEFAULT_RELAY.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relays_must_be_quic() {
        assert!(parse_relay(DEFAULT_RELAY).is_ok());
        let tcp = DEFAULT_RELAY.replace("/udp/19876/quic-v1", "/tcp/19876");
        let err = parse_relay(&tcp).unwrap_err().to_string();
        assert!(err.starts_with("relay must be a QUIC address"), "{err}");
        let err = parse_relay("nope").unwrap_err().to_string();
        assert!(err.starts_with("invalid relay address 'nope': "), "{err}");
    }
}
