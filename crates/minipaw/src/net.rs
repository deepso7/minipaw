//! Endpoint setup and teardown shared by both modes.

use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

use minip2p::{
    Endpoint, EndpointEvent, EndpointWaitOutcome, NatConfig, NatEvent, Path, PeerAddr,
    ReservationPolicy,
};

use crate::pipe::{BACKPRESSURE_RETRY, Link, LocalFailure, Pipe, SendError};
use crate::session::{Outcome, Shared};
use crate::wire::{Frame, PROTOCOL};
use minip2p::{ConnectionId, PeerId, StreamId};

const AGENT: &str = concat!("minipaw/", env!("CARGO_PKG_VERSION"));

/// How long either side keeps trying to get a lost stream back.
pub const RESUME_TIMEOUT: Duration = Duration::from_secs(60);
/// After finishing, how long we wait for the peer to finish too.
pub const LINGER: Duration = Duration::from_secs(2);
/// With all data confirmed but the stream lost, how long we keep the
/// session open so the peer can resume and collect our last ack.
pub const DELIVERED_GRACE: Duration = Duration::from_secs(10);
/// On a stop or an error, how long we wait for a blocked output to drain.
const STDOUT_GRACE: Duration = Duration::from_secs(1);
/// After a stop request or a local failure, how long we try to tell the peer: first
/// waiting for a stream if the session is between streams, then for the
/// peer to confirm it got the news.
pub const ABORT_GRACE: Duration = Duration::from_secs(3);

/// QUIC on every interface, the pipe protocol, and NAT traversal through
/// `relay`. Servers hold a reservation there to be reachable; clients only
/// open circuits through it, then hole-punch with DCUtR. `force_relay`
/// keeps to the relay: no direct dials or hole punching.
pub fn bind(
    relay: &PeerAddr,
    reserve: bool,
    force_relay: bool,
) -> Result<Endpoint, Box<dyn Error>> {
    let nat = NatConfig {
        reservation_policy: if reserve {
            ReservationPolicy::Always
        } else {
            ReservationPolicy::Never
        },
        force_relay,
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
        log::debug!("bound {addr}");
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

pub fn path_name(path: &Path) -> &'static str {
    match path {
        Path::DirectDialed => "direct",
        Path::DirectPunched => "direct (hole-punched)",
        Path::Relayed { .. } => "relayed",
    }
}

pub fn log_event(event: &EndpointEvent) {
    if !log::log_enabled!(log::Level::Debug) {
        return;
    }
    match event {
        EndpointEvent::ConnectionEstablished { peer_id, conn_id } => {
            log::debug!("connected to {peer_id} ({conn_id:?})");
        }
        EndpointEvent::ConnectionClosed { peer_id, conn_id } => {
            log::debug!("disconnected from {peer_id} ({conn_id:?})");
        }
        EndpointEvent::ConnectionReplaced { peer_id, old, new } => {
            log::debug!("connection to {peer_id} replaced ({old:?} -> {new:?})");
        }
        EndpointEvent::ConnectSettled { outcome, .. } => {
            log::debug!("connect attempt settled: {outcome:?}");
        }
        EndpointEvent::Nat(nat) => match nat {
            NatEvent::RelayReserved { relay, .. } => log::debug!("reserved on relay {relay}"),
            NatEvent::RelayReservationLost { relay } => {
                log::debug!("reservation on relay {relay} lost");
            }
            NatEvent::PathEstablished { path, .. }
            | NatEvent::InboundPathEstablished { path, .. } => {
                log::debug!("path established: {}", path_name(path));
            }
            NatEvent::PathUpgraded { to, .. } => log::debug!("path upgraded: {}", path_name(to)),
            NatEvent::InboundDirectUpgrade { .. } => log::debug!("path upgraded: direct"),
            NatEvent::HolePunchFailed {
                attempt, reason, ..
            } => log::debug!("hole punch attempt {attempt} failed: {reason}"),
            NatEvent::FellBackToRelay { .. } => log::debug!("staying on the relay"),
            other => log::debug!("{other:?}"),
        },
        EndpointEvent::Error(error) => log::debug!("endpoint error: {error:?}"),
        _ => {}
    }
}

/// Half-closes the finished session's stream and gives the peer a moment
/// to finish its side, so our last ack is not lost to an early exit.
/// Puts whatever is queued (a last frame, a stream half-close) on the wire:
/// a wait can return an already-buffered event without polling the
/// transport, and closing drops what was never sent. A zero wait always
/// polls once.
fn flush(endpoint: &mut Endpoint) {
    for _ in 0..16 {
        match endpoint.wait(Duration::ZERO) {
            Ok(EndpointWaitOutcome::Deadline) | Err(_) => break,
            Ok(EndpointWaitOutcome::Event(_) | EndpointWaitOutcome::Interrupted) => {}
        }
    }
}

/// Flushes, then closes the endpoint.
fn close(mut endpoint: Endpoint) {
    flush(&mut endpoint);
    if let Err(e) = endpoint.close() {
        log::debug!("close endpoint: {e}");
    }
}

pub fn linger_and_close(mut endpoint: Endpoint, link: Option<Link>) {
    if let Some(link) = link {
        if let Err(e) = endpoint.close_stream_write(&link.peer, link.conn, link.stream) {
            log::debug!("close stream: {e}");
        }
        let deadline = Instant::now() + LINGER;
        loop {
            // A past deadline makes `wait` return without polling anything.
            if Instant::now() >= deadline {
                break;
            }
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
    close(endpoint);
}

/// How a session loop ended without an error.
pub enum Exit {
    /// Both directions finished and were acknowledged.
    Done,
    /// All data in both directions is confirmed, but the stream is gone and
    /// the peer did not come back for our last ack.
    Delivered,
    /// A stop request or a local failure.
    Stopped(Stop),
}

/// A session ending on our side's initiative. Once seen it is the outcome,
/// even if the transfer completes meanwhile.
///
/// The loop keeps running while stopping, so the session's usual resume
/// machinery carries the news: the `Error` goes out on whatever stream is
/// up, again on a fresh one if that stream dies first, until the peer
/// confirms by half-closing a stream we sent it on, or `by` passes.
pub struct Stop {
    reason: StopReason,
    pub by: Instant,
    /// Streams the `Error` went out on.
    told: Vec<(PeerId, ConnectionId, StreamId)>,
    /// The peer half-closed one of them: it read the `Error`.
    confirmed: bool,
    /// The last send hit backpressure; try again then.
    retry_at: Option<Instant>,
}

enum StopReason {
    Interrupted,
    Failed(LocalFailure),
}

impl Stop {
    /// A stop request or the pipe's first local I/O error, if either
    /// happened.
    pub fn check(shared: &Shared, pipe: &Pipe) -> Option<Stop> {
        let reason = if shared.stopped() {
            StopReason::Interrupted
        } else {
            StopReason::Failed(pipe.take_failure()?)
        };
        log::debug!("stopping; telling the peer");
        Some(Stop {
            reason,
            by: Instant::now() + ABORT_GRACE,
            told: Vec::new(),
            confirmed: false,
            retry_at: None,
        })
    }

    fn message(&self) -> &'static str {
        match self.reason {
            StopReason::Interrupted => "interrupted",
            StopReason::Failed(_) => "the other side hit a local I/O error",
        }
    }

    /// Notes the peer gracefully half-closing a stream we told it on, which
    /// it does once it has read the `Error`. A reset is not confirmation: it
    /// discards what we queued.
    pub fn observe(&mut self, event: &EndpointEvent) {
        if let EndpointEvent::StreamRemoteWriteClosed {
            peer_id,
            conn_id,
            stream_id,
        } = event
            && self
                .told
                .iter()
                .any(|(p, c, s)| p == peer_id && c == conn_id && s == stream_id)
        {
            self.confirmed = true;
        }
    }

    /// One loop iteration while stopping: sends the `Error` on `link` (the
    /// stream that can carry it, if one is up) unless it already went out
    /// there. `peer_has_session` is whether the peer may hold our session
    /// and so must be told. `Ok(true)` ends the loop; `Err` means `link` is
    /// dead and should be dropped, so a fresh stream can carry the news.
    pub fn step(
        &mut self,
        endpoint: &mut Endpoint,
        link: Option<&Link>,
        peer_has_session: bool,
    ) -> Result<bool, String> {
        let now = Instant::now();
        if self.confirmed || !peer_has_session || now >= self.by {
            return Ok(true);
        }
        let Some(link) = link else {
            return Ok(false);
        };
        let key = (link.peer.clone(), link.conn, link.stream);
        if self.told.contains(&key) || self.retry_at.is_some_and(|at| now < at) {
            return Ok(false);
        }
        self.retry_at = None;
        match link.try_send(endpoint, &Frame::Error(self.message().into())) {
            Ok(()) => {
                if let Err(e) = endpoint.close_stream_write(&link.peer, link.conn, link.stream) {
                    log::debug!("close stream: {e}");
                }
                self.told.push(key);
                Ok(false)
            }
            Err(SendError::Full) => {
                self.retry_at = Some(now + BACKPRESSURE_RETRY);
                Ok(false)
            }
            Err(SendError::Dead(e)) => Err(e),
        }
    }

    /// When the loop must wake for the stop: a send retry or the deadline.
    pub fn wake_at(&self) -> Instant {
        self.retry_at.map_or(self.by, |at| at.min(self.by))
    }
}

/// Ends a session however it ended: tells the peer when it should know,
/// flushes the output (bounded when the session did not complete), and
/// maps the outcome to `run`'s result.
///
/// `peer` is the other end of the session, once there is one.
pub fn finish(
    endpoint: Endpoint,
    pipe: &mut Pipe,
    link: Option<Link>,
    peer: Option<PeerId>,
    exit: Result<Exit, Box<dyn Error>>,
) -> Result<Outcome, crate::Error> {
    match exit {
        Ok(Exit::Done) => {
            pipe.finish(None);
            linger_and_close(endpoint, link);
            Ok(Outcome::Done)
        }
        Ok(Exit::Delivered) => {
            pipe.finish(None);
            Ok(Outcome::Delivered)
        }
        Ok(Exit::Stopped(stop)) => {
            // The loop already told the peer, or gave up trying.
            drop(link);
            close(endpoint);
            pipe.finish(Some(STDOUT_GRACE));
            Err(match stop.reason {
                StopReason::Interrupted => crate::Error::Stopped,
                StopReason::Failed(LocalFailure::Read(e)) => crate::Error::Input(e),
                StopReason::Failed(LocalFailure::Write(e)) => crate::Error::Output(e),
            })
        }
        Err(e) if e.is::<PeerEnded>() => {
            pipe.finish(Some(STDOUT_GRACE));
            // Half-closing back confirms to the peer that its Error landed.
            linger_and_close(endpoint, link);
            Err(crate::Error::from_internal(e))
        }
        Err(e) => {
            abort(
                endpoint,
                link,
                peer,
                "the other side failed",
                Instant::now() + ABORT_GRACE,
            );
            pipe.finish(Some(STDOUT_GRACE));
            Err(crate::Error::from_internal(e))
        }
    }
}

/// The peer went away and did not come back in time.
#[derive(Debug)]
pub struct Disconnected(pub &'static str);

impl fmt::Display for Disconnected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl Error for Disconnected {}

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
/// so the peer exits now instead of waiting out [`RESUME_TIMEOUT`].
///
/// The `Error` can be lost with its stream, for instance when the peer is
/// moving to a new path or a relay cuts the circuit, so until `by` we answer
/// every stream the peer opens with it too. A lost connection proves
/// nothing; the peer half-closing a stream we sent the `Error` on does.
/// `peer` is `None` when the peer holds no session of ours to resume.
fn abort(
    mut endpoint: Endpoint,
    link: Option<Link>,
    peer: Option<PeerId>,
    reason: &str,
    by: Instant,
) {
    let error = Frame::Error(reason.into());
    let mut told: Vec<Link> = Vec::new();
    let tell = |endpoint: &mut Endpoint, told: &mut Vec<Link>, link: Link| {
        if let Err(e) = link.send(endpoint, &error) {
            log::debug!("abort not sent: {e}");
        }
        if let Err(e) = endpoint.close_stream_write(&link.peer, link.conn, link.stream) {
            log::debug!("close stream: {e}");
        }
        told.push(link);
    };
    if let Some(link) = link {
        tell(&mut endpoint, &mut told, link);
    }
    if let Some(peer) = peer {
        loop {
            // A past deadline makes `wait` return without polling anything.
            if Instant::now() >= by {
                break;
            }
            match endpoint.wait(by) {
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
                )) if told.iter().any(|l| l.is(&peer_id, conn_id, stream_id)) => break,
                Ok(EndpointWaitOutcome::Event(EndpointEvent::StreamReady {
                    peer_id,
                    conn_id,
                    stream_id,
                    initiated_locally: false,
                    ..
                })) if peer_id == peer => {
                    tell(
                        &mut endpoint,
                        &mut told,
                        Link::new(peer_id, conn_id, stream_id),
                    );
                }
                Ok(EndpointWaitOutcome::Event(_) | EndpointWaitOutcome::Interrupted) => {}
                Ok(EndpointWaitOutcome::Deadline) | Err(_) => break,
            }
        }
    }
    close(endpoint);
}
