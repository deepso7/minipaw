//! Endpoint setup and teardown shared by both modes.

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use minip2p::{
    Endpoint, EndpointEvent, EndpointWaitOutcome, NatConfig, NatEvent, Path, PeerAddr,
    ReservationPolicy,
};

use crate::pipe::{Link, Pipe};
use crate::wire::{Frame, PROTOCOL};
use minip2p::PeerId;

const AGENT: &str = concat!("minipaw/", env!("CARGO_PKG_VERSION"));

/// The relay a ticket without an embedded relay goes through, unless
/// `--relay` or `MINIPAW_RELAY` names another.
pub const DEFAULT_RELAY: &str = "/dns/relay.minip2p.com/udp/19876/quic-v1/p2p/12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf";

/// How long either side keeps trying to get a lost stream back.
pub const RESUME_TIMEOUT: Duration = Duration::from_secs(60);
/// After finishing, how long we wait for the peer to finish too.
pub const LINGER: Duration = Duration::from_secs(2);
/// With all data confirmed but the stream lost, how long we keep the
/// session open so the peer can resume and collect our last ack.
pub const DELIVERED_GRACE: Duration = Duration::from_secs(10);
/// On Ctrl-C or an error, how long we wait for a blocked stdout to drain.
const STDOUT_GRACE: Duration = Duration::from_secs(1);
/// After Ctrl-C mid-migration, how long we wait for the session's stream to
/// come back so the peer can be told, rather than left waiting to resume.
pub const ABORT_GRACE: Duration = Duration::from_secs(3);

pub fn default_relay() -> Option<PeerAddr> {
    DEFAULT_RELAY.parse().ok()
}

/// Resolves `--relay`, then `MINIPAW_RELAY`, then the built-in default.
pub fn resolve_relay(flag: Option<&str>) -> Result<PeerAddr, Box<dyn Error>> {
    let env = std::env::var("MINIPAW_RELAY").ok();
    let raw = flag
        .or(env.as_deref())
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_RELAY);
    if raw.is_empty() {
        return Err("no relay configured: pass --relay <multiaddr> or set MINIPAW_RELAY".into());
    }
    let relay: PeerAddr = raw
        .parse()
        .map_err(|e| format!("invalid relay address '{raw}': {e}"))?;
    crate::ticket::check_relay(&relay)?;
    if !relay.transport().is_quic_transport() {
        return Err(format!(
            "relay must be a QUIC address (…/udp/<port>/quic-v1/p2p/<id>), got '{raw}'"
        )
        .into());
    }
    Ok(relay)
}

/// QUIC on every interface, the pipe protocol, and NAT traversal through
/// `relay`. Servers hold a reservation there to be reachable; clients only
/// open circuits through it, then hole-punch with DCUtR.
pub fn bind(relay: &PeerAddr, reserve: bool) -> Result<Endpoint, Box<dyn Error>> {
    let nat = NatConfig {
        reservation_policy: if reserve {
            ReservationPolicy::Always
        } else {
            ReservationPolicy::Never
        },
        // Test hook: relay only, no direct dials or hole punching.
        force_relay: std::env::var_os("MINIPAW_FORCE_RELAY").is_some(),
        ..NatConfig::default()
    };
    let mut endpoint = Endpoint::builder()
        .agent_version(AGENT)
        .protocol(PROTOCOL)
        .nat_config(nat)
        .relay(relay.clone())
        .listen_default()?
        .bind()?;
    // Bound addresses are the local half of the hole-punch candidates.
    for addr in endpoint.listen_all()? {
        crate::debug!("bound {addr}");
    }
    Ok(endpoint)
}

pub fn random16() -> Result<[u8; 16], Box<dyn Error>> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| format!("system randomness: {e}"))?;
    Ok(bytes)
}

/// Whether the endpoint's current connection to `peer` skips the relay.
pub fn is_direct(endpoint: &Endpoint, peer: &minip2p::PeerId) -> bool {
    !matches!(endpoint.path(peer), Some(Path::Relayed { .. }))
}

pub fn path_label(direct: bool) -> &'static str {
    if direct { "direct" } else { "via relay" }
}

pub fn path_name(path: &Path) -> &'static str {
    match path {
        Path::DirectDialed => "direct",
        Path::DirectPunched => "direct (hole-punched)",
        Path::Relayed { .. } => "relayed",
    }
}

pub fn log_event(event: &EndpointEvent) {
    if !crate::verbose() {
        return;
    }
    match event {
        EndpointEvent::ConnectionEstablished { peer_id, conn_id } => {
            crate::debug!("connected to {peer_id} ({conn_id:?})");
        }
        EndpointEvent::ConnectionClosed { peer_id, conn_id } => {
            crate::debug!("disconnected from {peer_id} ({conn_id:?})");
        }
        EndpointEvent::ConnectionReplaced { peer_id, old, new } => {
            crate::debug!("connection to {peer_id} replaced ({old:?} -> {new:?})");
        }
        EndpointEvent::ConnectSettled { outcome, .. } => {
            crate::debug!("connect attempt settled: {outcome:?}");
        }
        EndpointEvent::Nat(nat) => match nat {
            NatEvent::RelayReserved { relay, .. } => crate::debug!("reserved on relay {relay}"),
            NatEvent::RelayReservationLost { relay } => {
                crate::debug!("reservation on relay {relay} lost");
            }
            NatEvent::PathEstablished { path, .. }
            | NatEvent::InboundPathEstablished { path, .. } => {
                crate::debug!("path established: {}", path_name(path));
            }
            NatEvent::PathUpgraded { to, .. } => crate::debug!("path upgraded: {}", path_name(to)),
            NatEvent::InboundDirectUpgrade { .. } => crate::debug!("path upgraded: direct"),
            NatEvent::HolePunchFailed {
                attempt, reason, ..
            } => crate::debug!("hole punch attempt {attempt} failed: {reason}"),
            NatEvent::FellBackToRelay { .. } => crate::debug!("staying on the relay"),
            other => crate::debug!("{other:?}"),
        },
        EndpointEvent::Error(error) => crate::debug!("endpoint error: {error:?}"),
        _ => {}
    }
}

/// Half-closes the finished session's stream and gives the peer a moment
/// to finish its side, so our last ack is not lost to an early exit.
pub fn linger_and_close(mut endpoint: Endpoint, link: Option<Link>) {
    if let Some(link) = link {
        if let Err(e) = endpoint.close_stream_write(&link.peer, link.conn, link.stream) {
            crate::debug!("close stream: {e}");
        }
        let deadline = Instant::now() + LINGER;
        loop {
            match endpoint.wait(deadline) {
                Ok(EndpointWaitOutcome::Event(
                    EndpointEvent::StreamRemoteWriteClosed {
                        peer_id,
                        conn_id,
                        stream_id,
                    }
                    | EndpointEvent::StreamClosed {
                        peer_id,
                        conn_id,
                        stream_id,
                    },
                )) if link.is(&peer_id, conn_id, stream_id) => break,
                Ok(EndpointWaitOutcome::Event(EndpointEvent::ConnectionClosed {
                    conn_id, ..
                })) if conn_id == link.conn => break,
                Ok(EndpointWaitOutcome::Event(_) | EndpointWaitOutcome::Interrupted) => {}
                Ok(EndpointWaitOutcome::Deadline) | Err(_) => break,
            }
        }
    }
    if let Err(e) = endpoint.close() {
        crate::debug!("close endpoint: {e}");
    }
}

/// How a session loop ended without an error.
pub enum Exit {
    /// Both directions finished and were acknowledged.
    Done,
    /// All data in both directions is confirmed, but the stream is gone and
    /// the peer did not come back for our last ack.
    Delivered,
    /// Ctrl-C: tell the peer, then stop.
    Interrupted,
    /// A local I/O error: tell the peer, then fail.
    Failed(String),
}

/// Ends a session however it ended: tells the peer when it should know,
/// flushes stdout (bounded when the session did not complete), and maps the
/// outcome to `run`'s result.
///
/// `peer` is the other end of the session, once there is one.
pub fn finish(
    endpoint: Endpoint,
    pipe: &mut Pipe,
    link: Option<Link>,
    peer: Option<PeerId>,
    exit: Result<Exit, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    match exit {
        Ok(Exit::Done) => {
            pipe.finish(None);
            linger_and_close(endpoint, link);
            Ok(())
        }
        Ok(Exit::Delivered) => {
            pipe.finish(None);
            Ok(())
        }
        Ok(Exit::Interrupted) => {
            abort(endpoint, link, peer, "interrupted");
            pipe.finish(Some(STDOUT_GRACE));
            Err(Interrupted.into())
        }
        Ok(Exit::Failed(message)) => {
            abort(endpoint, link, peer, "the other side hit a local I/O error");
            pipe.finish(Some(STDOUT_GRACE));
            Err(message.into())
        }
        Err(e) if e.is::<PeerEnded>() => {
            pipe.finish(Some(STDOUT_GRACE));
            if let Err(e) = endpoint.close() {
                crate::debug!("close endpoint: {e}");
            }
            Err(e)
        }
        Err(e) => {
            abort(endpoint, link, peer, "the other side failed");
            pipe.finish(Some(STDOUT_GRACE));
            Err(e)
        }
    }
}

/// The error `main` turns into exit status 130, silently.
#[derive(Debug)]
pub struct Interrupted;

impl fmt::Display for Interrupted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("interrupted")
    }
}

impl Error for Interrupted {}

static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Routes Ctrl-C to the event loop: the first sets [`interrupted`] and wakes
/// `wait`; a second exits on the spot.
pub fn handle_interrupt(wake: minip2p::WaitHandle) -> Result<(), Box<dyn Error>> {
    ctrlc::set_handler(move || {
        if INTERRUPTED.swap(true, Ordering::SeqCst) {
            std::process::exit(130);
        }
        wake.interrupt();
    })
    .map_err(|e| format!("installing the Ctrl-C handler: {e}"))?;
    Ok(())
}

pub fn interrupted() -> bool {
    INTERRUPTED.load(Ordering::SeqCst)
}

/// The peer ended the session (an `Error` frame). Nothing is owed back, so
/// we just leave.
#[derive(Debug)]
pub struct PeerEnded(pub String);

impl fmt::Display for PeerEnded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for PeerEnded {}

/// Ends the session after a local action rather than a quiet disconnect,
/// so the peer exits now instead of waiting out [`RESUME_TIMEOUT`]. The
/// `Error` on the current stream can be lost if the peer is just moving to
/// a new path, so for a short while we also answer the peer's resume
/// attempts with it, leaving once the peer disconnects.
fn abort(mut endpoint: Endpoint, link: Option<Link>, peer: Option<PeerId>, reason: &str) {
    let error = Frame::Error(reason.into());
    if let Some(link) = &link {
        if let Err(e) = link.send(&mut endpoint, &error) {
            crate::debug!("abort not sent: {e}");
        }
        if let Err(e) = endpoint.close_stream_write(&link.peer, link.conn, link.stream) {
            crate::debug!("close stream: {e}");
        }
    }
    if let Some(peer) = peer {
        let deadline = Instant::now() + ABORT_GRACE;
        loop {
            match endpoint.wait(deadline) {
                Ok(EndpointWaitOutcome::Event(EndpointEvent::ConnectionClosed {
                    peer_id, ..
                })) if peer_id == peer => break,
                Ok(EndpointWaitOutcome::Event(EndpointEvent::StreamReady {
                    peer_id,
                    conn_id,
                    stream_id,
                    initiated_locally: false,
                    ..
                })) if peer_id == peer => {
                    let late = Link::new(peer_id, conn_id, stream_id);
                    if let Err(e) = late.send(&mut endpoint, &error) {
                        crate::debug!("abort not sent on the resumed stream: {e}");
                    }
                    if let Err(e) = endpoint.close_stream_write(&late.peer, late.conn, late.stream)
                    {
                        crate::debug!("close stream: {e}");
                    }
                }
                Ok(EndpointWaitOutcome::Event(_) | EndpointWaitOutcome::Interrupted) => {}
                Ok(EndpointWaitOutcome::Deadline) | Err(_) => break,
            }
        }
    }
    if let Err(e) = endpoint.close() {
        crate::debug!("close endpoint: {e}");
    }
}
