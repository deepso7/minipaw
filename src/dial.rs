//! Client mode: connect to a ticket's server through its relay (DCUtR
//! upgrades the path to direct when it can) and pipe stdio over a session
//! that resumes on a fresh stream whenever the current one dies.

use std::error::Error;
use std::time::{Duration, Instant};

use minip2p::{
    ConnectId, ConnectOutcome, ConnectionId, Endpoint, EndpointEvent, EndpointWaitOutcome,
    NatEvent, PeerId, StreamId,
};

use crate::net::{self, RESUME_TIMEOUT};
use crate::pipe::{Link, Pipe};
use crate::ticket::Ticket;
use crate::wire::{Frame, PROTOCOL, SessionId};

/// Ceiling on stream negotiation plus the Hello/Welcome exchange.
const SETUP_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BACKOFF: Duration = Duration::from_secs(2);

enum Phase {
    /// No stream; try again at `at`.
    Idle { at: Instant },
    Connecting { id: ConnectId },
    Opening {
        conn: ConnectionId,
        stream: StreamId,
        since: Instant,
    },
    /// Hello sent, waiting for Welcome.
    Handshaking { link: Link, since: Instant },
    Up { link: Link },
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
    /// Whether a stream was ever up, for error wording.
    was_up: bool,
}

pub fn run(ticket: Ticket, relay: minip2p::PeerAddr) -> Result<(), Box<dyn Error>> {
    let endpoint = net::bind(&relay, false)?;
    let pipe = Pipe::new(&endpoint.wait_handle());
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
    };

    loop {
        client.on_timers()?;
        let deadline = [client.pipe.deadline(), client.phase_deadline()]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1))
            // A past deadline makes `wait` return without polling anything.
            .max(Instant::now() + Duration::from_millis(1));
        if let EndpointWaitOutcome::Event(event) = client.endpoint.wait(deadline)? {
            client.on_event(event)?;
        }

        let link = match &client.phase {
            Phase::Up { link } => Some(link),
            _ => None,
        };
        if let Err(e) = client.pipe.pump(&mut client.endpoint, link) {
            client.lose(&format!("send failed: {e}"));
        }
        if client.pipe.done() {
            let Client {
                endpoint,
                pipe,
                phase,
                ..
            } = client;
            pipe.finish();
            let link = match phase {
                Phase::Up { link } => Some(link),
                _ => None,
            };
            net::linger_and_close(endpoint, link);
            return Ok(());
        }
        if client.lost_since.is_some() && client.pipe.nearly_done() {
            // The server saw our Fin and sent all of its data; it is gone.
            client.pipe.finish();
            return Ok(());
        }
    }
}

impl Client {
    fn peer(&self) -> &PeerId {
        &self.ticket.peer
    }

    fn phase_deadline(&self) -> Option<Instant> {
        match &self.phase {
            Phase::Idle { at } => Some(*at),
            Phase::Opening { since, .. } | Phase::Handshaking { since, .. } => {
                Some(*since + SETUP_TIMEOUT)
            }
            Phase::Connecting { .. } | Phase::Up { .. } => None,
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
            _ => {}
        }
        Ok(())
    }

    /// Opens a stream on the existing connection, or connects first.
    fn start(&mut self) {
        if self.endpoint.connection_id(self.peer()).is_some() {
            return self.open();
        }
        let peer = self.peer().clone();
        match self.endpoint.connect(peer) {
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
            EndpointEvent::Nat(NatEvent::PathEstablished { connect_id, .. })
                if matches!(self.phase, Phase::Connecting { id } if id == connect_id) =>
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
                if let Phase::Handshaking { link, .. } = &self.phase
                    && let Err(e) = link.send(&mut self.endpoint, &hello)
                {
                    self.lose(&format!("hello: {e}"));
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
        link.reader.push(data);
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
                    }
                    self.phase = Phase::Up { link };
                    self.lost_since = None;
                    self.backoff = Duration::ZERO;
                    self.was_up = true;
                }
                (Frame::Error(message), _) => {
                    return Err(format!("server refused: {message}").into());
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
            (Phase::Opening { conn: c, stream: s, .. }, Some(stream)) => *c == conn && *s == stream,
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
            Phase::Handshaking { link, .. } | Phase::Up { link } => link.abandon(&mut self.endpoint),
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
