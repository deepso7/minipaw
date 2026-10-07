//! Client mode: connect to a ticket's server through its relay (DCUtR
//! upgrades the path to direct when it can) and pipe stdio over a session
//! that resumes on a fresh stream whenever the current one dies.

use std::error::Error;
use std::time::{Duration, Instant};

use minip2p::{
    ConnectId, ConnectOutcome, ConnectionId, Endpoint, EndpointEvent, EndpointWaitOutcome,
    Multiaddr, NatEvent, PeerAddr, PeerId, StreamId,
};

use crate::net::{self, DELIVERED_GRACE, Exit, RESUME_TIMEOUT, Stop};
use crate::pipe::{Link, Pipe};
use crate::ticket::Ticket;
use crate::wire::{Frame, PROTOCOL, SessionId};

/// Ceiling on stream negotiation plus the Hello/Welcome exchange.
const SETUP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

enum Phase {
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
    Up {
        link: Link,
    },
}

struct Client {
    endpoint: Endpoint,
    pipe: Pipe,
    ticket: Ticket,
    session: SessionId,
    phase: Phase,
    backoff: Duration,
    /// Since when we have had no working stream (`None` while up).
    lost_since: Option<Instant>,
    /// Whether a stream was ever up, for messages.
    was_up: bool,
    /// Whether a Hello ever went out: from then on the server may hold our
    /// session, and must be told if we stop.
    hello_sent: bool,
    /// The user has been told the session runs on a direct path.
    told_direct: bool,
    /// Ctrl-C or a local failure, once seen.
    stop: Option<Stop>,
    /// Test hook (`MINIPAW_DIRECT`): a server address to dial alongside the
    /// relay, for benchmarks and paths hole punching cannot find.
    direct: Option<PeerAddr>,
}

pub fn run(ticket: Ticket, relay: PeerAddr) -> Result<(), Box<dyn Error>> {
    let endpoint = net::bind(&relay, false)?;
    net::handle_interrupt(endpoint.wait_handle())?;
    let pipe = Pipe::new(&endpoint.wait_handle());
    let direct = match std::env::var("MINIPAW_DIRECT") {
        Ok(raw) => {
            let addr: Multiaddr = raw
                .parse()
                .map_err(|e| format!("invalid MINIPAW_DIRECT '{raw}': {e}"))?;
            Some(
                PeerAddr::new(addr, ticket.peer.clone())
                    .map_err(|e| format!("invalid MINIPAW_DIRECT '{raw}': {e}"))?,
            )
        }
        Err(_) => None,
    };
    let now = Instant::now();
    let mut client = Client {
        endpoint,
        pipe,
        ticket,
        session: net::random16()?,
        phase: Phase::Idle { at: now },
        backoff: Duration::ZERO,
        lost_since: Some(now),
        was_up: false,
        hello_sent: false,
        told_direct: false,
        stop: None,
        direct,
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
                crate::debug!("while stopping: {e}");
                Ok(Exit::Stopped(self.stop.take().ok_or("no stop")?))
            }
            result => result,
        }
    }

    fn drive_until_exit(&mut self) -> Result<Exit, Box<dyn Error>> {
        loop {
            self.on_timers()?;
            let delivered_by = self
                .lost_since
                .filter(|_| self.pipe.delivered())
                .map(|t| t + DELIVERED_GRACE);
            let deadline = [
                // Pipe timers are for sending, which stops when stopping.
                self.stop
                    .is_none()
                    .then(|| self.pipe.deadline(self.up_link()))
                    .flatten(),
                self.phase_deadline(),
                self.stop.as_ref().map(Stop::wake_at),
                delivered_by,
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1))
            // A past deadline makes `wait` return without polling anything.
            .max(Instant::now() + Duration::from_millis(1));
            if let EndpointWaitOutcome::Event(event) = self.endpoint.wait(deadline)? {
                if let Some(stop) = &mut self.stop {
                    stop.observe(&event);
                }
                self.on_event(event)?;
            }

            if self.stop.is_none() {
                let link = match &self.phase {
                    Phase::Up { link } => Some(link),
                    _ => None,
                };
                if let Err(e) = self.pipe.pump(&mut self.endpoint, link) {
                    self.lose(&format!("send failed: {e}"));
                }
                self.stop = Stop::check(&self.pipe);
            }
            if let Some(stop) = &mut self.stop {
                // From Hello on the server may hold our session, so the news
                // goes out on a stream still waiting for Welcome too.
                let link = match &self.phase {
                    Phase::Up { link } | Phase::Handshaking { link, .. } => Some(link),
                    _ => None,
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
        match std::mem::replace(&mut self.phase, Phase::Idle { at: Instant::now() }) {
            Phase::Handshaking { link, .. } | Phase::Up { link } => Some(link),
            _ => None,
        }
    }

    fn peer(&self) -> &PeerId {
        &self.ticket.peer
    }

    fn up_link(&self) -> Option<&Link> {
        match &self.phase {
            Phase::Up { link } => Some(link),
            _ => None,
        }
    }

    fn phase_deadline(&self) -> Option<Instant> {
        match &self.phase {
            Phase::Idle { at } => Some(*at),
            Phase::Opening { since, .. } | Phase::Handshaking { since, .. } => {
                Some(*since + SETUP_TIMEOUT)
            }
            Phase::Up { link } => Some(link.dead_at()),
            Phase::Connecting { .. } => None,
        }
    }

    fn on_timers(&mut self) -> Result<(), Box<dyn Error>> {
        let now = Instant::now();
        if let Some(since) = self.lost_since
            && now.duration_since(since) >= RESUME_TIMEOUT
        {
            return Err(if self.was_up {
                "lost the connection to the server".into()
            } else {
                "could not reach the server".into()
            });
        }
        match &self.phase {
            Phase::Idle { at } if now >= *at => self.start(),
            Phase::Opening { since, .. } | Phase::Handshaking { since, .. }
                if now.duration_since(*since) >= SETUP_TIMEOUT =>
            {
                self.lose("stream setup timed out");
            }
            Phase::Up { link } if now >= link.dead_at() => {
                // The connection under a silent stream may be a relayed
                // circuit the relay dropped without telling us; a stream on
                // it would be just as dead, so start over with a fresh one.
                self.lose("no word from the server");
                if let Err(e) = self.endpoint.disconnect(&self.ticket.peer) {
                    crate::debug!("disconnect: {e}");
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
                crate::debug!("connecting to {}", self.peer());
                self.phase = Phase::Connecting { id };
            }
            Err(e) => self.retry(&format!("connect: {e}")),
        }
    }

    fn open(&mut self) {
        let peer = self.peer().clone();
        match self.endpoint.open_stream(&peer, PROTOCOL) {
            Ok((conn, stream)) => {
                self.phase = Phase::Opening {
                    conn,
                    stream,
                    since: Instant::now(),
                };
            }
            Err(e) => self.retry(&format!("open stream: {e}")),
        }
    }

    fn on_event(&mut self, event: EndpointEvent) -> Result<(), Box<dyn Error>> {
        net::log_event(&event);
        match event {
            // Start on the provisional relayed path rather than waiting out
            // the hole punch; the session moves over if the punch lands.
            EndpointEvent::Nat(NatEvent::PathEstablished { connect_id, .. }) if matches!(self.phase, Phase::Connecting { id } if id == connect_id) =>
            {
                self.open();
            }
            EndpointEvent::ConnectSettled {
                connect_id,
                outcome,
                ..
            } if matches!(self.phase, Phase::Connecting { id } if id == connect_id) => {
                match outcome {
                    ConnectOutcome::Connected { .. } => self.open(),
                    ConnectOutcome::Failed(failure) => self.retry(&format!("connect: {failure}")),
                    ConnectOutcome::Cancelled => self.retry("connect cancelled"),
                }
            }
            EndpointEvent::StreamReady {
                peer_id,
                conn_id,
                stream_id,
                initiated_locally: true,
                ..
            } if matches!(self.phase, Phase::Opening { conn, stream, .. }
                    if conn == conn_id && stream == stream_id)
                && peer_id == *self.peer() =>
            {
                let link = Link::new(peer_id, conn_id, stream_id);
                let hello = Frame::Hello {
                    token: self.ticket.token,
                    session: self.session,
                    recv: self.pipe.recv_offset(),
                };
                self.phase = Phase::Handshaking {
                    link,
                    since: Instant::now(),
                };
                if let Phase::Handshaking { link, .. } = &self.phase {
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
            EndpointEvent::ConnectionClosed { peer_id, conn_id }
            | EndpointEvent::ConnectionReplaced {
                peer_id,
                old: conn_id,
                ..
            } if self.carries(&peer_id, conn_id, None) => self.lose("connection closed"),
            EndpointEvent::Nat(NatEvent::PathUpgraded { peer, .. }) if peer == *self.peer() => {
                if self.was_up && !self.told_direct {
                    self.told_direct = true;
                    eprintln!("# upgraded to a direct connection");
                }
                // The relayed circuit is closed under the upgrade; move the
                // session onto the direct connection.
                let current = self.endpoint.connection_id(&peer);
                if let Some(conn) = self.phase_conn()
                    && Some(conn) != current
                {
                    self.lose("path upgraded");
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
        let (Phase::Handshaking { link, .. } | Phase::Up { link }) = &mut self.phase else {
            return Ok(());
        };
        if !link.is(peer, conn, stream) {
            return Ok(());
        }
        link.push(data);
        loop {
            let (Phase::Handshaking { link, .. } | Phase::Up { link }) = &mut self.phase else {
                return Ok(());
            };
            let Some(frame) = link.reader.next()? else {
                return Ok(());
            };
            match (frame, &self.phase) {
                (Frame::Welcome { recv }, Phase::Handshaking { .. }) => {
                    self.pipe.attach(recv)?;
                    let Phase::Handshaking { link, .. } =
                        std::mem::replace(&mut self.phase, Phase::Idle { at: Instant::now() })
                    else {
                        return Ok(());
                    };
                    if self.was_up {
                        crate::debug!("session resumed");
                    } else {
                        let direct = net::is_direct(&self.endpoint, &self.ticket.peer);
                        self.told_direct = direct;
                        eprintln!("# connected ({})", net::path_label(direct));
                    }
                    self.phase = Phase::Up { link };
                    self.lost_since = None;
                    self.backoff = Duration::ZERO;
                    self.was_up = true;
                }
                (Frame::Error(message), Phase::Handshaking { .. }) => {
                    let what = if self.was_up {
                        "server ended the session"
                    } else {
                        "server refused"
                    };
                    return Err(
                        net::PeerEnded(format!("{what}: {}", message.escape_debug())).into(),
                    );
                }
                (frame, Phase::Up { .. }) => self.pipe.on_frame(frame)?,
                (frame, _) => return Err(format!("unexpected {frame:?} before Welcome").into()),
            }
        }
    }

    fn phase_conn(&self) -> Option<ConnectionId> {
        match &self.phase {
            Phase::Opening { conn, .. } => Some(*conn),
            Phase::Handshaking { link, .. } | Phase::Up { link } => Some(link.conn),
            Phase::Idle { .. } | Phase::Connecting { .. } => None,
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
                Phase::Opening {
                    conn: c, stream: s, ..
                },
                Some(stream),
            ) => *c == conn && *s == stream,
            (Phase::Handshaking { link, .. } | Phase::Up { link }, Some(stream)) => {
                link.is(peer, conn, stream)
            }
            (_, None) => self.phase_conn() == Some(conn),
            _ => false,
        }
    }

    /// Drops the current stream, if any, and schedules a fresh one.
    fn lose(&mut self, reason: &str) {
        let peer = self.peer().clone();
        match std::mem::replace(&mut self.phase, Phase::Idle { at: Instant::now() }) {
            Phase::Opening { conn, stream, .. } => {
                if let Err(e) = self.endpoint.abandon_stream(&peer, conn, stream) {
                    crate::debug!("abandon stream: {e}");
                }
            }
            Phase::Handshaking { link, .. } | Phase::Up { link } => {
                link.abandon(&mut self.endpoint)
            }
            Phase::Idle { .. } | Phase::Connecting { .. } => {}
        }
        self.retry(reason);
    }

    fn retry(&mut self, reason: &str) {
        crate::debug!("{reason}; retrying in {:?}", self.backoff);
        self.lost_since.get_or_insert_with(Instant::now);
        self.phase = Phase::Idle {
            at: Instant::now() + self.backoff,
        };
        self.backoff = (self.backoff * 2)
            .max(Duration::from_millis(250))
            .min(MAX_BACKOFF);
    }
}
