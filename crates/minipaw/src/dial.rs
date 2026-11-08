//! Client mode: connect to a ticket's server through its relay (DCUtR
//! upgrades the path to direct when it can) and pipe stdio over a session
//! that resumes on a fresh stream whenever the current one dies.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use minip2p::{
    ConnectId, ConnectOutcome, ConnectionId, Endpoint, EndpointEvent, EndpointWaitOutcome,
    NatEvent, PeerAddr, PeerId, StreamId,
};

use crate::config::Config;
use crate::event::{Event, Events, PathKind};
use crate::io::Io;
use crate::net::{
    self, DELIVERED_GRACE, Disconnected, Exit, LinkState, Lost, RESUME_TIMEOUT, Stop,
};
use crate::pipe::{Link, Pipe};
use crate::session::{Outcome, Shared};
use crate::ticket::Ticket;
use crate::wire::{Frame, PROTOCOL, SessionId};

/// Ceiling on stream negotiation plus the Hello/Welcome exchange.
const SETUP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

/// Up on a welcomed stream, or down while an attempt gets one.
type Phase = LinkState<Link, Attempt>;

/// Getting a fresh stream. There is only ever one, so a `Link` held inline
/// costs nothing a box would save.
#[allow(clippy::large_enum_variant)]
enum Attempt {
    /// No stream; try again at `at`.
    Idle {
        at: Instant,
    },
    Connecting {
        id: ConnectId,
    },
    Opening {
        conn: ConnectionId,
        stream: StreamId,
        since: Instant,
    },
    /// Hello sent, waiting for Welcome.
    Handshaking {
        link: Link,
        since: Instant,
    },
}

struct Client {
    endpoint: Endpoint,
    pipe: Pipe,
    ticket: Ticket,
    session: SessionId,
    phase: Phase,
    backoff: Duration,
    /// Whether a stream was ever up: for messages, and every later `Hello`
    /// asks to resume.
    was_up: bool,
    /// Whether a Hello ever went out: from then on the server may hold our
    /// session, and must be told if we stop.
    hello_sent: bool,
    /// The user has been told the session runs on a direct path.
    told_direct: bool,
    /// A stop request or a local failure, once seen.
    stop: Option<Stop>,
    shared: Arc<Shared>,
    events: Events,
    /// Test hook ([`Config::direct`]): a server address to dial alongside the
    /// relay, for benchmarks and paths hole punching cannot find.
    direct: Option<PeerAddr>,
    /// Test hook ([`Config::test_dead_after`]): how long a stream may stay
    /// silent before we drop it.
    dead_after: Option<Duration>,
}

/// Runs a dialer's session to `ticket`'s listener through `relay`.
pub fn run(
    ticket: Ticket,
    relay: PeerAddr,
    config: &Config,
    io: Io,
    shared: Arc<Shared>,
    events: Events,
) -> Result<Outcome, crate::Error> {
    let direct = match &config.direct {
        Some(addr) => Some(
            PeerAddr::new(addr.clone(), ticket.peer.clone()).map_err(|e| {
                crate::Error::Config(format!("invalid direct address '{addr}': {e}"))
            })?,
        ),
        None => None,
    };
    let endpoint = net::bind(&relay, false, config).map_err(crate::Error::from_internal)?;
    let session = net::random16().map_err(crate::Error::from_internal)?;
    shared.set_wake(endpoint.wait_handle());
    let pipe = Pipe::new(&endpoint.wait_handle(), io, shared.stats.clone());
    let now = Instant::now();
    let mut client = Client {
        endpoint,
        pipe,
        ticket,
        session,
        phase: LinkState::down(now, Attempt::Idle { at: now }),
        backoff: Duration::ZERO,
        was_up: false,
        hello_sent: false,
        told_direct: false,
        stop: None,
        shared,
        events,
        direct,
        dead_after: config.test_dead_after,
    };

    let exit = client.drive();
    let link = client.take_link();
    let server = client.hello_sent.then(|| client.ticket.peer.clone());
    net::finish(client.endpoint, &mut client.pipe, link, server, exit)
}

impl Client {
    /// Runs the session. Once stopping, the stop is the outcome whatever
    /// else goes wrong: the peer ending too, or a timeout.
    fn drive(&mut self) -> Result<Exit, Box<dyn Error>> {
        match self.drive_until_exit() {
            Err(e) if self.stop.is_some() => {
                log::debug!("while stopping: {e}");
                Ok(Exit::Stopped(self.stop.take().ok_or("no stop")?))
            }
            result => result,
        }
    }

    fn drive_until_exit(&mut self) -> Result<Exit, Box<dyn Error>> {
        loop {
            self.on_timers()?;
            let delivered_by = self
                .phase
                .since()
                .filter(|_| self.pipe.delivered())
                .map(|t| t + DELIVERED_GRACE);
            let deadline = [
                // Pipe timers are for sending, which stops when stopping.
                self.stop
                    .is_none()
                    .then(|| self.pipe.deadline(self.phase.up()))
                    .flatten(),
                self.phase_deadline(),
                self.stop.as_ref().map(Stop::wake_at),
                delivered_by,
                self.phase.handover_by(),
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1))
            // A past deadline polls once but never sleeps: a timer due
            // but not yet acted on would spin the loop.
            .max(Instant::now() + Duration::from_millis(1));
            if let EndpointWaitOutcome::Event(event) = self.endpoint.wait(deadline)? {
                if let Some(stop) = &mut self.stop {
                    stop.observe(&event);
                }
                self.on_event(event)?;
            }

            if self.stop.is_none() {
                if let Err(e) = self.pipe.pump(&mut self.endpoint, self.phase.up()) {
                    self.lose(&format!("send failed: {e}"));
                }
                self.stop = Stop::check(&self.shared, &self.pipe);
                if self.stop.is_some() {
                    self.events.emit(Event::Stopping);
                }
            }
            if let Some(stop) = &mut self.stop {
                // From Hello on the server may hold our session, so the news
                // goes out on a stream still waiting for Welcome too.
                let link = match &self.phase {
                    LinkState::Up(link) | LinkState::Down(_, Attempt::Handshaking { link, .. }) => {
                        Some(link)
                    }
                    LinkState::Down(..) => None,
                };
                match stop.step(&mut self.endpoint, link, self.hello_sent) {
                    Ok(true) => return Ok(Exit::Stopped(self.stop.take().ok_or("no stop")?)),
                    Ok(false) => {}
                    Err(e) => self.lose(&format!("stop not sent: {e}")),
                }
                continue;
            }
            if self.pipe.done() {
                return Ok(Exit::Done);
            }
            if delivered_by.is_some_and(|by| Instant::now() >= by) {
                return Ok(Exit::Delivered);
            }
        }
    }

    /// The current stream, whether or not its handshake finished.
    fn take_link(&mut self) -> Option<Link> {
        match self
            .phase
            .lose(Instant::now(), false, Attempt::Idle { at: Instant::now() })
        {
            Lost::Up(link, _) | Lost::Attempt(Attempt::Handshaking { link, .. }) => Some(link),
            Lost::Attempt(_) => None,
        }
    }

    fn peer(&self) -> &PeerId {
        &self.ticket.peer
    }

    /// The current stream, whether or not its handshake finished.
    fn link(&self) -> Option<&Link> {
        match &self.phase {
            LinkState::Up(link) | LinkState::Down(_, Attempt::Handshaking { link, .. }) => {
                Some(link)
            }
            LinkState::Down(..) => None,
        }
    }

    fn link_mut(&mut self) -> Option<&mut Link> {
        match &mut self.phase {
            LinkState::Up(link) | LinkState::Down(_, Attempt::Handshaking { link, .. }) => {
                Some(link)
            }
            LinkState::Down(..) => None,
        }
    }

    /// Where the next attempt goes; ignored while up.
    fn set_attempt(&mut self, next: Attempt) {
        if let LinkState::Down(_, attempt) = &mut self.phase {
            *attempt = next;
        }
    }

    fn phase_deadline(&self) -> Option<Instant> {
        match &self.phase {
            LinkState::Down(_, Attempt::Idle { at }) => Some(*at),
            LinkState::Down(
                _,
                Attempt::Opening { since, .. } | Attempt::Handshaking { since, .. },
            ) => Some(*since + SETUP_TIMEOUT),
            LinkState::Up(link) => Some(link.dead_at()),
            LinkState::Down(_, Attempt::Connecting { .. }) => None,
        }
    }

    fn on_timers(&mut self) -> Result<(), Box<dyn Error>> {
        let now = Instant::now();
        if let Some(since) = self.phase.since()
            && now.duration_since(since) >= RESUME_TIMEOUT
        {
            return Err(Disconnected(if self.was_up {
                "lost the connection to the server"
            } else {
                "could not reach the server"
            })
            .into());
        }
        if self.phase.handover_overran(now) {
            self.events.emit(Event::LinkLost {
                reason: "connection closed".to_owned(),
            });
        }
        match &self.phase {
            LinkState::Down(_, Attempt::Idle { at }) if now >= *at => self.start(),
            LinkState::Down(
                _,
                Attempt::Opening { since, .. } | Attempt::Handshaking { since, .. },
            ) if now.duration_since(*since) >= SETUP_TIMEOUT => {
                self.lose("stream setup timed out");
            }
            LinkState::Up(link) if now >= link.dead_at() => {
                // The connection under a silent stream may be a relayed
                // circuit the relay dropped without telling us; a stream on
                // it would be just as dead, so start over with a fresh one.
                self.lose("no word from the server");
                if let Err(e) = self.endpoint.disconnect(&self.ticket.peer) {
                    log::debug!("disconnect: {e}");
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Opens a stream on the existing connection, or connects first.
    fn start(&mut self) {
        if self.endpoint.connection_id(self.peer()).is_some() {
            return self.open();
        }
        let attempt = match &self.direct {
            Some(addr) => self.endpoint.connect(addr.clone()),
            None => self.endpoint.connect(self.ticket.peer.clone()),
        };
        match attempt {
            Ok(id) => {
                log::debug!("connecting to {}", self.peer());
                self.events.emit(Event::Connecting {
                    peer: self.peer().clone(),
                });
                self.set_attempt(Attempt::Connecting { id });
            }
            Err(e) => self.retry(&format!("connect: {e}")),
        }
    }

    fn open(&mut self) {
        let peer = self.peer().clone();
        match self.endpoint.open_stream(&peer, PROTOCOL) {
            Ok((conn, stream)) => {
                self.set_attempt(Attempt::Opening {
                    conn,
                    stream,
                    since: Instant::now(),
                });
            }
            Err(e) => self.retry(&format!("open stream: {e}")),
        }
    }

    fn on_event(&mut self, event: EndpointEvent) -> Result<(), Box<dyn Error>> {
        net::log_event(&event);
        match event {
            // Start on the provisional relayed path rather than waiting out
            // the hole punch; the session moves over if the punch lands.
            EndpointEvent::Nat(NatEvent::PathEstablished { connect_id, .. }) if matches!(self.phase, LinkState::Down(_, Attempt::Connecting { id }) if id == connect_id) =>
            {
                self.open();
            }
            EndpointEvent::ConnectSettled {
                connect_id,
                outcome,
                ..
            } if matches!(self.phase, LinkState::Down(_, Attempt::Connecting { id }) if id == connect_id) => {
                match outcome {
                    ConnectOutcome::Connected { .. } => self.open(),
                    ConnectOutcome::Failed(failure) => self.retry(&format!("connect: {failure}")),
                    ConnectOutcome::Cancelled => self.retry("connect cancelled"),
                    _ => self.retry("connect: unknown outcome"),
                }
            }
            EndpointEvent::StreamReady {
                peer_id,
                conn_id,
                stream_id,
                initiated_locally: true,
                ..
            } if matches!(self.phase, LinkState::Down(_, Attempt::Opening { conn, stream, .. })
                    if conn == conn_id && stream == stream_id)
                && peer_id == *self.peer() =>
            {
                let mut link = Link::new(peer_id, conn_id, stream_id);
                if let Some(after) = self.dead_after {
                    link = link.with_dead_after(after);
                }
                // Once welcomed, the server holds our session: one that
                // does not (it restarted, or ended the session) must refuse
                // rather than start a fresh one.
                let hello = Frame::Hello {
                    token: self.ticket.token,
                    session: self.session,
                    recv: self.pipe.recv_offset(),
                    resume: self.was_up,
                };
                self.set_attempt(Attempt::Handshaking {
                    link,
                    since: Instant::now(),
                });
                if let LinkState::Down(_, Attempt::Handshaking { link, .. }) = &self.phase {
                    match link.send(&mut self.endpoint, &hello) {
                        Ok(()) => self.hello_sent = true,
                        Err(e) => self.lose(&format!("hello: {e}")),
                    }
                }
            }
            EndpointEvent::StreamData {
                peer_id,
                conn_id,
                stream_id,
                data,
            } => self.on_data(&peer_id, conn_id, stream_id, &data)?,
            EndpointEvent::StreamWritable {
                peer_id,
                conn_id,
                stream_id,
            } => {
                if let Some(link) = self.link()
                    && link.is(&peer_id, conn_id, stream_id)
                {
                    link.writable();
                }
            }
            EndpointEvent::StreamClosed {
                peer_id,
                conn_id,
                stream_id,
            }
            | EndpointEvent::StreamWriteStopped {
                peer_id,
                conn_id,
                stream_id,
                ..
            } if self.carries(&peer_id, conn_id, Some(stream_id)) => self.lose("stream closed"),
            EndpointEvent::ConnectionClosed {
                peer_id, conn_id, ..
            } if self.carries(&peer_id, conn_id, None) => {
                self.lose("connection closed");
            }
            EndpointEvent::ConnectionReplaced { peer_id, old, new }
                if self.carries(&peer_id, old, None) =>
            {
                if net::is_upgrade(old, new) {
                    self.hand_over();
                } else {
                    self.lose("connection closed");
                }
            }
            EndpointEvent::Nat(NatEvent::PathUpgraded { peer, .. }) if peer == *self.peer() => {
                if self.was_up && !self.told_direct {
                    self.told_direct = true;
                    self.events.emit(Event::Upgraded);
                }
                // The relayed circuit is closed under the upgrade; move the
                // session onto the direct connection.
                let current = self.endpoint.connection_id(&peer);
                if let Some(conn) = self.phase_conn()
                    && Some(conn) != current
                {
                    self.hand_over();
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn on_data(
        &mut self,
        peer: &PeerId,
        conn: ConnectionId,
        stream: StreamId,
        data: &[u8],
    ) -> Result<(), Box<dyn Error>> {
        let Some(link) = self.link_mut() else {
            return Ok(());
        };
        if !link.is(peer, conn, stream) {
            return Ok(());
        }
        link.push(data);
        loop {
            let Some(link) = self.link_mut() else {
                return Ok(());
            };
            let Some(frame) = link.reader.next()? else {
                return Ok(());
            };
            match (frame, &mut self.phase) {
                (
                    Frame::Welcome { recv },
                    LinkState::Down(_, attempt @ Attempt::Handshaking { .. }),
                ) => {
                    self.pipe.attach(recv)?;
                    let Attempt::Handshaking { link, .. } =
                        std::mem::replace(attempt, Attempt::Idle { at: Instant::now() })
                    else {
                        return Ok(());
                    };
                    let (_, resumed) = self.phase.resume(link);
                    if !self.was_up {
                        let direct = net::is_direct(&self.endpoint, &self.ticket.peer);
                        self.told_direct = direct;
                        self.events.emit(Event::Connected {
                            peer: self.ticket.peer.clone(),
                            path: PathKind::from_direct(direct),
                        });
                    } else if resumed {
                        log::debug!("session resumed");
                        self.events.emit(Event::Resumed);
                    } else {
                        log::debug!("session moved to the direct connection");
                    }
                    self.backoff = Duration::ZERO;
                    self.was_up = true;
                }
                (Frame::Error(message), LinkState::Down(_, Attempt::Handshaking { .. })) => {
                    let what = if self.was_up {
                        "server ended the session"
                    } else {
                        "server refused"
                    };
                    return Err(
                        net::PeerEnded(format!("{what}: {}", message.escape_debug())).into(),
                    );
                }
                (frame, LinkState::Up(_)) => self.pipe.on_frame(frame)?,
                (frame, _) => return Err(format!("unexpected {frame:?} before Welcome").into()),
            }
        }
    }

    fn phase_conn(&self) -> Option<ConnectionId> {
        match &self.phase {
            LinkState::Down(_, Attempt::Opening { conn, .. }) => Some(*conn),
            LinkState::Down(_, Attempt::Idle { .. } | Attempt::Connecting { .. }) => None,
            _ => self.link().map(|link| link.conn),
        }
    }

    /// Whether the current stream (or, with no `stream`, its connection)
    /// is the one named.
    fn carries(&self, peer: &PeerId, conn: ConnectionId, stream: Option<StreamId>) -> bool {
        if peer != self.peer() {
            return false;
        }
        match (&self.phase, stream) {
            (
                LinkState::Down(
                    _,
                    Attempt::Opening {
                        conn: c, stream: s, ..
                    },
                ),
                Some(stream),
            ) => *c == conn && *s == stream,
            (_, Some(stream)) => self.link().is_some_and(|link| link.is(peer, conn, stream)),
            (_, None) => self.phase_conn() == Some(conn),
        }
    }

    /// Drops the current stream, if any, and schedules a fresh one.
    fn lose(&mut self, reason: &str) {
        self.drop_stream(reason, false);
    }

    /// Moves the session off a relayed circuit onto the direct connection
    /// that replaced it; a planned move, so not reported as a lost link.
    fn hand_over(&mut self) {
        self.drop_stream("moving to the direct connection", true);
    }

    fn drop_stream(&mut self, reason: &str, handover: bool) {
        let now = Instant::now();
        match self.phase.lose(now, handover, Attempt::Idle { at: now }) {
            Lost::Attempt(Attempt::Opening { conn, stream, .. }) => {
                if let Err(e) = self.endpoint.abandon_stream(conn, stream) {
                    log::debug!("abandon stream: {e}");
                }
            }
            Lost::Attempt(Attempt::Handshaking { link, .. }) => link.abandon(&mut self.endpoint),
            Lost::Up(link, report) => {
                link.abandon(&mut self.endpoint);
                if report {
                    self.events.emit(Event::LinkLost {
                        reason: reason.to_owned(),
                    });
                }
            }
            Lost::Attempt(Attempt::Idle { .. } | Attempt::Connecting { .. }) => {}
        }
        self.retry(reason);
    }

    fn retry(&mut self, reason: &str) {
        log::debug!("{reason}; retrying in {:?}", self.backoff);
        self.set_attempt(Attempt::Idle {
            at: Instant::now() + self.backoff,
        });
        self.backoff = (self.backoff * 2)
            .max(Duration::from_millis(250))
            .min(MAX_BACKOFF);
    }
}
