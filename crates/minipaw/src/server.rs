//! The server loop: one endpoint and ticket, and any number of sessions,
//! each in a [`Slot`] that goes from connecting through running to ending.
//!
//! Nothing that serves one session may block the loop. Each wait is a
//! deadline the loop's one `Endpoint::wait` honours, every endpoint event
//! goes to the slot whose stream it names, and a session that fails ends
//! alone; only the endpoint failing ends the server.
//!
//! What differs between servers is their [`Host`]: where a new session's
//! local end comes from, where milestones go, and whether the server goes
//! on once a session ends.

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use minip2p::{
    ConnectionId, Endpoint, EndpointEvent, EndpointWaitOutcome, NatEvent, PeerId, StreamId,
};

use crate::config::Config;
use crate::event::{Event, PathKind};
use crate::net::{
    self, ABORT_GRACE, DELIVERED_GRACE, Disconnected, HANDOVER_GRACE, LINGER, PeerEnded,
    RESUME_TIMEOUT, STDOUT_GRACE, Stop,
};
use crate::pipe::{Link, LocalFailure, Pipe};
use crate::session::{Outcome, Shared};
use crate::ticket::Ticket;
use crate::wire::{Frame, FrameReader, PROTOCOL, SessionId, Token};

/// Inbound streams still waiting for their `Hello`.
const MAX_PENDING: usize = 16;
/// How long a new stream has to present its `Hello`.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a peer has from its first connection to get a session. Fresh
/// streams and replaced connections do not extend it, so a peer that never
/// authenticates cannot keep its connection by churning them.
const ADMISSION_TIMEOUT: Duration = Duration::from_secs(HELLO_TIMEOUT.as_secs() + 5);
/// How long a refusal's `Error` has to go out before its peer, holding no
/// session, is disconnected.
const REFUSAL_GRACE: Duration = Duration::from_secs(1);
/// How long a reservation may take before we say so.
const RESERVE_WARNING: Duration = Duration::from_secs(15);

type StreamKey = (PeerId, ConnectionId, StreamId);
/// A session: its dialer and the id it picked. Ids are random, so the pair
/// names one dialer's run.
type SlotKey = (PeerId, SessionId);

/// What differs between servers.
pub trait Host {
    /// The local end for new session `id` from `peer`: `Ok(Some)` now,
    /// `Ok(None)` later through [`ready`](Self::ready), or `Err` to refuse
    /// the session with the reason. Must not block.
    fn open(&mut self, id: u64, peer: &PeerId) -> Result<Option<Pipe>, String>;

    /// A local end [`open`](Self::open) promised, or why there is none.
    /// One for a session that is gone is handed back to
    /// [`unused`](Self::unused).
    fn ready(&mut self) -> Option<(u64, Result<Pipe, String>)>;

    /// Takes back session `id`'s local end, which never started: its
    /// `Welcome` could not be sent, or the session went away first.
    fn unused(&mut self, id: u64, pipe: Pipe);

    /// A local failure that ends the server while it has no session, such
    /// as a pre-created local end failing.
    fn failure(&mut self) -> Option<LocalFailure>;

    /// Why a new session is turned away when `sessions` are already open.
    fn busy(&self, sessions: usize) -> String;

    /// A milestone of the server rather than of a session.
    fn server_event(&mut self, event: Event);

    /// A stream from `peer` was turned away with `reason`, which the peer
    /// is told.
    fn refused(&mut self, peer: &PeerId, reason: &str);

    /// A milestone of session `id`.
    fn session_event(&mut self, id: u64, event: Event);

    /// Session `id` with `peer` is over and torn down. Returns whether the
    /// server ends with it.
    fn ended(&mut self, id: u64, peer: &PeerId, result: Result<Outcome, crate::Error>) -> bool;
}

/// How [`ServerCore::run`] ended, the endpoint aside.
pub enum Exit {
    /// The host ended the server after a session ended.
    Host,
    /// A stop or a local failure while no session was open.
    Stopped(crate::Error),
}

/// The server loop and everything it owns; see the module docs.
pub struct ServerCore<H> {
    endpoint: Endpoint,
    host: H,
    token: Token,
    ticket: Ticket,
    /// The relay's peer id: its connection is never ours to drop.
    relay: PeerId,
    max_sessions: usize,
    /// Not-yet-admitted streams and when they must have said `Hello`.
    pending: HashMap<StreamKey, (FrameReader, Instant)>,
    /// Peers seen connecting, and when they are disconnected unless they
    /// have a session by then.
    admission: HashMap<PeerId, Admission>,
    sessions: HashMap<SlotKey, Slot>,
    next_id: u64,
    /// A stop was requested: no new sessions, and every slot is ending.
    stopping: bool,
    /// Once stopping, when whatever is still ending is cut short: one
    /// deadline for the whole server, however many sessions it had.
    shutdown_by: Option<Instant>,
    shared: Arc<Shared>,
    started: Instant,
    /// The ticket has been announced.
    announced: bool,
    /// The reservation was lost since then, so getting it back is news.
    lost: bool,
    warned: bool,
    /// Test hook ([`Config::test_drop_link_after`]): once a session has
    /// received this many bytes, forget its link without closing it, as a
    /// relay that drops a circuit and tells only us would. The client sees
    /// its stream go silent.
    drop_link_after: Option<u64>,
    /// Test hook ([`Config::test_drop_welcome`]).
    drop_welcome: bool,
    /// How long a session that lost its stream waits for the client:
    /// [`RESUME_TIMEOUT`] unless [`Config::test_resume_timeout`] says.
    resume_timeout: Duration,
}

/// A peer's time to authenticate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Admission {
    /// Fixed at the peer's first connection.
    deadline: Instant,
    /// When it is disconnected: the deadline, or sooner once refused.
    by: Instant,
}

impl Admission {
    fn new(now: Instant) -> Self {
        let deadline = now + ADMISSION_TIMEOUT;
        Admission {
            deadline,
            by: deadline,
        }
    }

    /// After a refusal: long enough for the `Error` to go out, but never
    /// much past the deadline, however often the peer is refused.
    fn refused(&mut self, now: Instant) {
        self.by = now.min(self.deadline) + REFUSAL_GRACE;
    }
}

struct Slot {
    /// Names the session in logs and events. Never reused, so a local end
    /// that arrives for a slot since replaced is not mistaken for its.
    id: u64,
    state: State,
}

enum State {
    /// Waiting for the host's local end. Holds the newest stream and its
    /// `Hello`'s `recv`; they get the `Welcome` once the end is ready.
    Connecting(Option<(Link, u64)>),
    Running(Live),
    /// Tearing down: telling the peer, lingering for its last ack, and
    /// draining the output, each against a deadline.
    Ending(Live, Ending),
    /// Torn down; the slot is removed.
    Done,
}

/// A session that has started.
struct Live {
    pipe: Pipe,
    link: Option<Link>,
    /// Since when the session has had no stream.
    lost_since: Option<Instant>,
    /// The user has been told the client is on a direct path.
    told_direct: bool,
    /// The stream was dropped as the client moved onto a direct
    /// connection: its loss and resume go unreported unless it takes
    /// [`HANDOVER_GRACE`].
    handover: bool,
}

/// Why a running session ends.
enum End {
    /// Both directions finished and were acknowledged.
    Done,
    /// All data in both directions is confirmed, but the stream is gone and
    /// the client did not come back for our last ack.
    Delivered,
    /// A stop request or a local failure.
    Stopped(Stop),
    /// The peer ended it, or it broke.
    Failed(Box<dyn Error>),
}

struct Ending {
    /// Reported once the teardown is over.
    result: Result<Outcome, crate::Error>,
    /// The session completed: a client that lost our last ack with its
    /// stream gets it again.
    clean: bool,
    /// The peer is being told the session is over, until it confirms.
    tell: Option<Stop>,
    /// Waiting for the peer to close its side of the link, until then.
    linger: Option<Instant>,
    /// The deadline for resending our last ack; fixed when ending begins.
    by: Instant,
    /// How long a blocked output may take to drain.
    drain_by: Instant,
    /// The output has drained, or had its time.
    drained: bool,
}

impl<H: Host> ServerCore<H> {
    /// A server on `endpoint` admitting at most `max_sessions` at once.
    /// `relay` is the relay's peer id.
    pub fn new(
        endpoint: Endpoint,
        ticket: Ticket,
        relay: PeerId,
        config: &Config,
        shared: Arc<Shared>,
        host: H,
        max_sessions: usize,
    ) -> Self {
        ServerCore {
            endpoint,
            host,
            token: ticket.token,
            ticket,
            relay,
            max_sessions,
            pending: HashMap::new(),
            admission: HashMap::new(),
            sessions: HashMap::new(),
            next_id: 1,
            stopping: false,
            shutdown_by: None,
            shared,
            started: Instant::now(),
            announced: false,
            lost: false,
            warned: false,
            drop_link_after: config.test_drop_link_after,
            drop_welcome: config.test_drop_welcome,
            resume_timeout: config.test_resume_timeout.unwrap_or(RESUME_TIMEOUT),
        }
    }

    /// Closes the endpoint, handing back the host.
    pub fn close(self) -> H {
        net::close(self.endpoint);
        self.host
    }

    /// Runs the server until the host ends it, or a stop with no session
    /// left. An `Err` is the endpoint failing; every session has been
    /// reported ended by then.
    pub fn run(&mut self) -> Result<Exit, Box<dyn Error>> {
        self.started = Instant::now();
        self.host.server_event(Event::Reserving {
            relay: self.relay.clone(),
        });
        loop {
            let outcome = match self.endpoint.wait(self.deadline()) {
                Ok(outcome) => outcome,
                Err(e) => {
                    self.fail(&e.to_string());
                    return Err(e.into());
                }
            };
            if let EndpointWaitOutcome::Event(event) = outcome {
                self.on_event(event);
            }
            while let Some((id, result)) = self.host.ready() {
                self.on_ready(id, result);
            }
            self.expire_pending();
            self.expire_admission();
            if !self.announced && !self.warned && self.started.elapsed() >= RESERVE_WARNING {
                self.warned = true;
                self.host.server_event(Event::ReservationSlow);
            }
            if let Some(exit) = self.check_stop() {
                return Ok(exit);
            }
            if self.step_sessions() {
                return Ok(Exit::Host);
            }
            if self.stopping && self.sessions.is_empty() {
                return Ok(Exit::Stopped(crate::Error::Stopped));
            }
        }
    }

    fn deadline(&self) -> Instant {
        let now = Instant::now();
        let core = [
            (!self.announced && !self.warned).then(|| self.started + RESERVE_WARNING),
            self.pending.values().map(|(_, deadline)| *deadline).min(),
            self.admission.values().map(|a| a.by).min(),
            self.shutdown_by,
        ];
        core.into_iter()
            .chain(
                self.sessions
                    .values()
                    .map(|s| s.deadline(self.resume_timeout)),
            )
            .flatten()
            .min()
            .unwrap_or_else(|| now + Duration::from_secs(1))
            // A past deadline makes `wait` return without polling anything.
            .max(now + Duration::from_millis(1))
    }

    fn on_event(&mut self, event: EndpointEvent) {
        net::log_event(&event);
        for slot in self.sessions.values_mut() {
            if let State::Ending(
                _,
                Ending {
                    tell: Some(stop), ..
                },
            ) = &mut slot.state
            {
                stop.observe(&event);
            }
        }
        match event {
            EndpointEvent::Nat(NatEvent::RelayReserved { .. }) => {
                if !self.announced {
                    self.announced = true;
                    self.host.server_event(Event::Listening {
                        ticket: self.ticket.clone(),
                    });
                } else if self.lost {
                    self.host.server_event(Event::ReservationRestored);
                }
                self.lost = false;
            }
            EndpointEvent::Nat(NatEvent::RelayReservationLost { .. }) => {
                self.lost = self.announced;
                self.host.server_event(Event::ReservationLost);
            }
            EndpointEvent::Nat(NatEvent::InboundDirectUpgrade { peer }) => {
                for ((p, _), slot) in &mut self.sessions {
                    if let State::Running(live) = &mut slot.state
                        && *p == peer
                        && !live.told_direct
                    {
                        live.told_direct = true;
                        self.host.session_event(slot.id, Event::Upgraded);
                    }
                }
            }
            EndpointEvent::ConnectionEstablished { peer_id, .. } if peer_id != self.relay => {
                self.admission
                    .entry(peer_id)
                    .or_insert_with(|| Admission::new(Instant::now()));
            }
            EndpointEvent::StreamReady {
                peer_id,
                conn_id,
                stream_id,
                protocol_id,
                initiated_locally: false,
            } if protocol_id == PROTOCOL => {
                // The pool is small and expiring. When full, the oldest
                // stream makes way: an honest dialer says Hello within a
                // round trip, so squatters cannot keep it out.
                if self.pending.len() >= MAX_PENDING {
                    self.evict_oldest_pending();
                }
                let deadline = Instant::now() + HELLO_TIMEOUT;
                self.pending.insert(
                    (peer_id, conn_id, stream_id),
                    (FrameReader::default(), deadline),
                );
            }
            EndpointEvent::StreamData {
                peer_id,
                conn_id,
                stream_id,
                data,
            } => match self.slot_on(&peer_id, conn_id, stream_id) {
                Some(key) => self.on_slot_data(&key, &data),
                None => self.on_pending_data((peer_id, conn_id, stream_id), &data),
            },
            EndpointEvent::StreamRemoteWriteClosed {
                peer_id,
                conn_id,
                stream_id,
            } => {
                // A lingering session's peer has finished too.
                if let Some(key) = self.slot_on(&peer_id, conn_id, stream_id)
                    && let Some(Slot {
                        state: State::Ending(_, ending),
                        ..
                    }) = self.sessions.get_mut(&key)
                {
                    ending.linger = None;
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
            } => {
                self.pending.remove(&(peer_id.clone(), conn_id, stream_id));
                if let Some(key) = self.slot_on(&peer_id, conn_id, stream_id) {
                    self.drop_link(&key, false, "stream closed");
                }
            }
            EndpointEvent::ConnectionClosed { peer_id, conn_id } => {
                // Gone altogether: a new connection starts a new deadline.
                self.admission.remove(&peer_id);
                self.drop_conn(&peer_id, conn_id, false);
            }
            EndpointEvent::ConnectionReplaced { peer_id, old, new } => {
                self.drop_conn(&peer_id, old, net::is_upgrade(old, new));
            }
            _ => {}
        }
    }

    /// The session whose current stream is the one named.
    fn slot_on(&self, peer: &PeerId, conn: ConnectionId, stream: StreamId) -> Option<SlotKey> {
        self.sessions
            .iter()
            .find(|((p, _), slot)| {
                p == peer && slot.link().is_some_and(|l| l.is(peer, conn, stream))
            })
            .map(|(key, _)| key.clone())
    }

    fn on_slot_data(&mut self, key: &SlotKey, data: &[u8]) {
        let Some(slot) = self.sessions.get_mut(key) else {
            return;
        };
        match &mut slot.state {
            State::Connecting(Some((link, _))) => {
                link.reader.push(data);
                self.check_early(key);
            }
            State::Running(live) => {
                if let Some(link) = &mut live.link {
                    link.push(data);
                }
                if let Err(e) = self.read_frames(key) {
                    self.end(key, End::Failed(e));
                }
            }
            // Owed nothing more but the teardown.
            _ => {}
        }
    }

    /// Hands a running session's buffered frames to its pipe.
    fn read_frames(&mut self, key: &SlotKey) -> Result<(), Box<dyn Error>> {
        let Some(Slot {
            state: State::Running(live),
            ..
        }) = self.sessions.get_mut(key)
        else {
            return Ok(());
        };
        let Some(link) = &mut live.link else {
            return Ok(());
        };
        while let Some(frame) = link.reader.next()? {
            live.pipe.on_frame(frame)?;
        }
        if self
            .drop_link_after
            .is_some_and(|after| live.pipe.recv_offset() >= after)
        {
            self.drop_link_after = None;
            log::debug!("test hook: dropping the link without closing it");
            live.link = None;
            live.lost_since = Some(Instant::now());
        }
        Ok(())
    }

    /// Buffers data on a not-yet-admitted stream until its `Hello` is
    /// complete, then admits or refuses it. Nothing about a session changes
    /// until the stream is fully set up, so a refused or failed admission
    /// leaves every session and link as they were.
    fn on_pending_data(&mut self, key: StreamKey, data: &[u8]) {
        let Some((reader, _)) = self.pending.get_mut(&key) else {
            return;
        };
        reader.push(data);
        let hello = reader.next();
        if matches!(hello, Ok(None)) {
            return;
        }
        let reader = self
            .pending
            .remove(&key)
            .map(|(reader, _)| reader)
            .unwrap_or_default();
        let (peer, conn, stream) = key;
        let mut link = Link::new(peer, conn, stream);
        link.reader = reader;
        let (token, session, recv, resume) = match hello {
            Ok(Some(Frame::Hello {
                token,
                session,
                recv,
                resume,
            })) => (token, session, recv, resume),
            Ok(_) => return self.refuse(link, "expected Hello"),
            Err(e) => return self.refuse(link, &e),
        };
        if !constant_time_eq(&token, &self.token) {
            return self.refuse(link, "wrong token");
        }
        self.admit(link, session, recv, resume);
    }

    /// Where an authenticated stream goes. A stream for a session we hold
    /// rejoins it, whether or not it asks to resume: without the flag, the
    /// client never got the `Welcome` we sent. Otherwise only a client
    /// starting out gets a new session.
    fn admit(&mut self, link: Link, session: SessionId, recv: u64, resume: bool) {
        let key = (link.peer.clone(), session);
        if let Some(slot) = self.sessions.get_mut(&key) {
            match &mut slot.state {
                State::Connecting(held) => {
                    log::debug!("session {}: client is back while connecting", slot.id);
                    if let Some((old, _)) = held.replace((link, recv)) {
                        old.abandon(&mut self.endpoint);
                    }
                    self.check_early(&key);
                }
                State::Running(_) => self.resume(&key, link, recv),
                State::Ending(..) => self.rejoin_ending(&key, link),
                State::Done => self.refuse(link, "session ended"),
            }
            return;
        }
        if resume {
            return self.refuse(link, "session ended");
        }
        if self.stopping {
            return self.refuse(link, "interrupted");
        }
        if self.sessions.len() >= self.max_sessions {
            let reason = self.host.busy(self.sessions.len());
            return self.refuse(link, &reason);
        }
        let id = self.next_id;
        self.next_id += 1;
        match self.host.open(id, &link.peer) {
            Err(reason) => self.refuse(link, &reason),
            Ok(None) => {
                let state = State::Connecting(Some((link, recv)));
                self.sessions.insert(key.clone(), Slot { id, state });
                self.check_early(&key);
            }
            Ok(Some(pipe)) => self.start(key, id, pipe, link, recv),
        }
    }

    /// A connecting session's local end is ready, or failed.
    fn on_ready(&mut self, id: u64, result: Result<Pipe, String>) {
        let key = self
            .sessions
            .iter()
            .find(|(_, slot)| slot.id == id && matches!(slot.state, State::Connecting(_)))
            .map(|(key, _)| key.clone());
        let Some(key) = key else {
            if let Ok(pipe) = result {
                self.host.unused(id, pipe);
            }
            return;
        };
        let Some(Slot {
            state: State::Connecting(held),
            ..
        }) = self.sessions.remove(&key)
        else {
            return;
        };
        match (result, held) {
            (Ok(pipe), Some((link, recv))) => self.start(key, id, pipe, link, recv),
            // The client lost its stream meanwhile. Never welcomed, it
            // comes back as a new session.
            (Ok(pipe), None) => {
                self.host.unused(id, pipe);
                self.readmit(&key.0);
            }
            (Err(reason), Some((link, _))) => self.refuse(link, &reason),
            (Err(reason), None) => {
                log::debug!("session {id}: {reason}");
                self.readmit(&key.0);
            }
        }
    }

    /// Reads what a connecting session's client sent after its `Hello`. An
    /// honest dialer sends nothing more until welcomed but its own stop, so
    /// nothing is kept for later: the session ends on the first frame, and
    /// what is buffered stays under one frame.
    fn check_early(&mut self, key: &SlotKey) {
        let Some(Slot {
            id,
            state: State::Connecting(Some((link, _))),
        }) = self.sessions.get_mut(key)
        else {
            return;
        };
        let refusal = match link.reader.next() {
            Ok(None) => return,
            Ok(Some(Frame::Error(_))) => None,
            Ok(Some(_)) => Some("unexpected frame before Welcome".to_owned()),
            Err(e) => Some(e),
        };
        let id = *id;
        let Some(Slot {
            state: State::Connecting(Some((link, _))),
            ..
        }) = self.sessions.remove(key)
        else {
            return;
        };
        // Its local end is handed back when it comes.
        if let Some(reason) = refusal {
            return self.refuse(link, &reason);
        }
        // Closing our side confirms the stop, as for a running session.
        log::debug!("session {id}: the client stopped before its Welcome");
        if let Err(e) = self
            .endpoint
            .close_stream_write(&link.peer, link.conn, link.stream)
        {
            log::debug!("close stream: {e}");
        }
        self.disconnect_soon(link.peer);
    }

    /// Starts a new session on `link` with its local end: once the
    /// `Welcome` is out, the session counts as admitted.
    fn start(&mut self, key: SlotKey, id: u64, pipe: Pipe, link: Link, recv: u64) {
        if let Err(e) = pipe.check_attach(recv) {
            self.host.unused(id, pipe);
            return self.refuse(link, &e);
        }
        let link = if std::mem::take(&mut self.drop_welcome) {
            log::debug!("test hook: dropping the Welcome");
            link.abandon(&mut self.endpoint);
            None
        } else if let Err(e) = link.send(
            &mut self.endpoint,
            &Frame::Welcome {
                recv: pipe.recv_offset(),
            },
        ) {
            log::debug!("welcome failed: {e}");
            link.abandon(&mut self.endpoint);
            self.host.unused(id, pipe);
            // Its admission deadline may have lapsed while it was
            // connecting; without one, an idle peer would stay forever.
            self.readmit(&key.0);
            return;
        } else {
            Some(link)
        };
        let direct = net::is_direct(&self.endpoint, &key.0);
        self.host.session_event(
            id,
            Event::Accepted {
                peer: key.0.clone(),
                path: PathKind::from_direct(direct),
            },
        );
        let mut live = Live {
            pipe,
            lost_since: link.is_none().then(Instant::now),
            link,
            told_direct: direct,
            handover: false,
        };
        let attached = live.pipe.attach(recv);
        self.sessions.insert(
            key.clone(),
            Slot {
                id,
                state: State::Running(live),
            },
        );
        // Frames that arrived together with the Hello.
        if let Err(e) = attached
            .map_err(Into::into)
            .and_then(|()| self.read_frames(&key))
        {
            self.end(&key, End::Failed(e));
        }
    }

    /// A running session's client is back on a fresh stream.
    fn resume(&mut self, key: &SlotKey, link: Link, recv: u64) {
        let Some(Slot {
            id,
            state: State::Running(live),
        }) = self.sessions.get_mut(key)
        else {
            return;
        };
        if let Err(e) = live.pipe.check_attach(recv) {
            return self.refuse(link, &e);
        }
        let welcome = Frame::Welcome {
            recv: live.pipe.recv_offset(),
        };
        if let Err(e) = link.send(&mut self.endpoint, &welcome) {
            log::debug!("welcome failed: {e}");
            link.abandon(&mut self.endpoint);
            return;
        }
        log::debug!("client resumed at offset {recv}");
        if !std::mem::take(&mut live.handover) {
            self.host.session_event(*id, Event::Resumed);
        }
        if let Some(old) = live.link.replace(link) {
            old.abandon(&mut self.endpoint);
        }
        live.lost_since = None;
        let attached = live.pipe.attach(recv);
        if let Err(e) = attached
            .map_err(Into::into)
            .and_then(|()| self.read_frames(key))
        {
            self.end(key, End::Failed(e));
        }
    }

    /// A stream for a session that is ending can only see the teardown
    /// through, within its original deadline: never back to running.
    fn rejoin_ending(&mut self, key: &SlotKey, link: Link) {
        let Some(Slot {
            id,
            state: State::Ending(live, ending),
        }) = self.sessions.get_mut(key)
        else {
            return;
        };
        if ending.tell.is_some() {
            // The Error goes out on it next, before any Welcome; the dialer
            // takes it then too. The old stream is kept for confirmation.
            log::debug!("session {id}: client is back; telling it the session is over");
            live.link = Some(link);
        } else if ending.clean && Instant::now() < ending.by {
            // The dialer takes an Ack only once welcomed.
            log::debug!("session {id}: client is back for our last ack");
            let welcome = Frame::Welcome {
                recv: live.pipe.recv_offset(),
            };
            let sent = link
                .send(&mut self.endpoint, &welcome)
                .and_then(|()| link.send(&mut self.endpoint, &live.pipe.final_ack()))
                .and_then(|()| {
                    self.endpoint
                        .close_stream_write(&link.peer, link.conn, link.stream)
                        .map_err(|e| e.to_string())
                });
            match sent {
                Ok(()) => {
                    live.link = Some(link);
                    ending.linger = Some(ending.by);
                }
                Err(e) => {
                    log::debug!("last ack not resent: {e}");
                    link.abandon(&mut self.endpoint);
                }
            }
        } else {
            self.refuse(link, "session ended");
        }
    }

    /// Turns a stream away with an `Error`; an admission outcome. A peer
    /// with no session is disconnected once the `Error` had time to go out.
    fn refuse(&mut self, link: Link, reason: &str) {
        log::debug!("refusing stream from {}: {reason}", link.peer);
        self.host.refused(&link.peer, reason);
        if let Err(e) = link.send(&mut self.endpoint, &Frame::Error(reason.into())) {
            log::debug!("refusal not sent: {e}");
        }
        if let Err(e) = self
            .endpoint
            .close_stream_write(&link.peer, link.conn, link.stream)
        {
            log::debug!("close refused stream: {e}");
        }
        self.disconnect_soon(link.peer);
    }

    /// A peer with no session is disconnected once what we last sent it
    /// had time to go out.
    fn disconnect_soon(&mut self, peer: PeerId) {
        if peer != self.relay && !self.has_session(&peer) {
            let now = Instant::now();
            self.admission
                .entry(peer)
                .or_insert_with(|| Admission::new(now))
                .refused(now);
        }
    }

    /// A peer left with no session must get a new one in time, as a
    /// newcomer must: its admission deadline is gone once it had one.
    fn readmit(&mut self, peer: &PeerId) {
        if *peer != self.relay && !self.has_session(peer) {
            self.admission
                .entry(peer.clone())
                .or_insert_with(|| Admission::new(Instant::now()));
        }
    }

    fn has_session(&self, peer: &PeerId) -> bool {
        self.sessions.keys().any(|(p, _)| p == peer)
    }

    fn evict_oldest_pending(&mut self) {
        let oldest = self
            .pending
            .iter()
            .min_by_key(|(_, (_, deadline))| *deadline)
            .map(|(key, _)| key.clone());
        if let Some((peer, conn, stream)) = oldest {
            log::debug!("dropping a stream from {peer}: too many pending");
            self.pending.remove(&(peer.clone(), conn, stream));
            Link::new(peer, conn, stream).abandon(&mut self.endpoint);
        }
    }

    /// Drops pending streams that never presented a `Hello`.
    fn expire_pending(&mut self) {
        let now = Instant::now();
        let expired: Vec<StreamKey> = self
            .pending
            .iter()
            .filter(|(_, (_, deadline))| now >= *deadline)
            .map(|(key, _)| key.clone())
            .collect();
        for (peer, conn, stream) in expired {
            log::debug!("dropping a stream from {peer}: no Hello in time");
            self.pending.remove(&(peer.clone(), conn, stream));
            Link::new(peer, conn, stream).abandon(&mut self.endpoint);
        }
    }

    /// Disconnects peers that got no session in time. One with a session,
    /// in any state, has authenticated and is left alone.
    fn expire_admission(&mut self) {
        let now = Instant::now();
        let expired: Vec<PeerId> = self
            .admission
            .iter()
            .filter(|(_, admission)| now >= admission.by)
            .map(|(peer, _)| peer.clone())
            .collect();
        for peer in expired {
            self.admission.remove(&peer);
            if self.has_session(&peer) {
                continue;
            }
            log::debug!("disconnecting {peer}: no session in time");
            let streams: Vec<StreamKey> = self
                .pending
                .keys()
                .filter(|(p, _, _)| *p == peer)
                .cloned()
                .collect();
            for (peer, conn, stream) in streams {
                self.pending.remove(&(peer.clone(), conn, stream));
                Link::new(peer, conn, stream).abandon(&mut self.endpoint);
            }
            if let Err(e) = self.endpoint.disconnect(&peer) {
                log::debug!("disconnect {peer}: {e}");
            }
        }
    }

    /// Forgets streams on a connection that is gone. A running session on
    /// a relayed circuit replaced by a direct connection is moving over,
    /// so its loss is not reported.
    fn drop_conn(&mut self, peer: &PeerId, conn: ConnectionId, upgrade: bool) {
        self.pending.retain(|(p, c, _), _| p != peer || *c != conn);
        let keys: Vec<SlotKey> = self
            .sessions
            .iter()
            .filter(|((p, _), slot)| p == peer && slot.link().is_some_and(|l| l.conn == conn))
            .map(|(key, _)| key.clone())
            .collect();
        let reason = if upgrade {
            "moving to the direct connection"
        } else {
            "connection closed"
        };
        for key in keys {
            self.drop_link(&key, upgrade, reason);
        }
    }

    /// A session's stream is gone.
    fn drop_link(&mut self, key: &SlotKey, upgrade: bool, reason: &str) {
        let Some(slot) = self.sessions.get_mut(key) else {
            return;
        };
        match &mut slot.state {
            State::Connecting(held) => {
                if let Some((link, _)) = held.take() {
                    link.abandon(&mut self.endpoint);
                }
            }
            State::Running(live) => {
                live.handover = upgrade;
                live.lose(slot.id, &mut self.endpoint, &mut self.host, reason);
            }
            State::Ending(live, _) => {
                if let Some(link) = live.link.take() {
                    link.abandon(&mut self.endpoint);
                }
            }
            State::Done => {}
        }
    }

    /// A stop request, or the host failing while no session holds its end.
    /// Returns the exit when there is no session to see out.
    fn check_stop(&mut self) -> Option<Exit> {
        if self.stopping {
            return None;
        }
        let mut stop = if self.shared.stopped() {
            Stop::interrupted()
        } else {
            Stop::failed(self.host.failure()?)
        };
        self.stopping = true;
        // Telling a peer takes the longest part of a stop; every slot gets
        // that long, together.
        self.shutdown_by = Some(Instant::now() + ABORT_GRACE);
        // A session already ending ends as it would have.
        if self.sessions.is_empty()
            || self
                .sessions
                .values()
                .any(|slot| !matches!(slot.state, State::Ending(..)))
        {
            self.host.server_event(Event::Stopping);
        }
        if self.sessions.is_empty() {
            return Some(Exit::Stopped(stop.take_error()));
        }
        let keys: Vec<SlotKey> = self.sessions.keys().cloned().collect();
        for key in keys {
            // One still connecting never started: its stream is told, and
            // its local end handed back if it comes.
            if let Some(Slot {
                state: State::Connecting(held),
                ..
            }) = self.sessions.get_mut(&key)
            {
                let held = held.take();
                self.sessions.remove(&key);
                if let Some((link, _)) = held {
                    self.refuse(link, "interrupted");
                }
            } else {
                self.end(&key, End::Stopped(Stop::interrupted()));
            }
        }
        None
    }

    /// One loop iteration's work for every session. Returns whether the
    /// host ends the server.
    fn step_sessions(&mut self) -> bool {
        let keys: Vec<SlotKey> = self.sessions.keys().cloned().collect();
        for key in keys {
            if let Some(end) = self.step_running(&key) {
                self.end(&key, end);
            }
            if self.step_ending(&key) {
                return true;
            }
        }
        false
    }

    /// Sends what a running session has to send, and sees whether it ends.
    fn step_running(&mut self, key: &SlotKey) -> Option<End> {
        let Some(Slot {
            id,
            state: State::Running(live),
        }) = self.sessions.get_mut(key)
        else {
            return None;
        };
        let (endpoint, host) = (&mut self.endpoint, &mut self.host);
        if live
            .link
            .as_ref()
            .is_some_and(|l| Instant::now() >= l.dead_at())
        {
            live.lose(*id, endpoint, host, "no word from the client");
        }
        if let Err(e) = live.pipe.pump(endpoint, live.link.as_ref()) {
            live.lose(*id, endpoint, host, &format!("send failed: {e}"));
        }
        if let Some(failure) = live.pipe.take_failure() {
            host.session_event(*id, Event::Stopping);
            return Some(End::Stopped(Stop::failed(failure)));
        }
        if live.pipe.done() {
            return Some(End::Done);
        }
        let since = live.lost_since?;
        if live.handover && since.elapsed() >= HANDOVER_GRACE {
            live.handover = false;
            host.session_event(
                *id,
                Event::LinkLost {
                    reason: "connection closed".to_owned(),
                },
            );
        }
        if live.pipe.delivered() && since.elapsed() >= DELIVERED_GRACE {
            return Some(End::Delivered);
        }
        if since.elapsed() >= self.resume_timeout {
            return Some(End::Failed(
                Disconnected("client disconnected and did not come back").into(),
            ));
        }
        None
    }

    /// Moves a running session to ending.
    fn end(&mut self, key: &SlotKey, end: End) {
        let Some(slot) = self.sessions.get_mut(key) else {
            return;
        };
        match std::mem::replace(&mut slot.state, State::Done) {
            State::Running(mut live) => {
                log::debug!("session {} ending", slot.id);
                let ending = Ending::begin(end, &mut live, &mut self.endpoint);
                slot.state = State::Ending(live, ending);
            }
            other => slot.state = other,
        }
    }

    /// Advances an ending session's teardown. Once all of it is over, the
    /// session is reported ended and removed. Returns whether the host ends
    /// the server with it.
    fn step_ending(&mut self, key: &SlotKey) -> bool {
        let Some(Slot {
            id,
            state: State::Ending(live, ending),
        }) = self.sessions.get_mut(key)
        else {
            return false;
        };
        let now = Instant::now();
        if let Some(stop) = &mut ending.tell {
            match stop.step(&mut self.endpoint, live.link.as_ref(), true) {
                Ok(true) => ending.tell = None,
                Ok(false) => {}
                // A fresh stream from the client can carry it instead.
                Err(e) => {
                    log::debug!("session {id}: stop not sent: {e}");
                    if let Some(link) = live.link.take() {
                        link.abandon(&mut self.endpoint);
                    }
                }
            }
        }
        if !ending.lingering(live.link.is_some(), now) {
            ending.linger = None;
        }
        if !ending.drained {
            ending.drained = live.pipe.output_closed() || now >= ending.drain_by;
        }
        let waiting = ending.tell.is_some() || ending.linger.is_some() || !ending.drained;
        // Past the server's shutdown deadline, nothing waits any more.
        let cut_short = waiting && self.shutdown_by.is_some_and(|by| now >= by);
        if waiting && !cut_short {
            return false;
        }
        if cut_short {
            log::debug!("session {id}: shutting down; cutting its teardown short");
        }

        let Some(Slot {
            id,
            state: State::Ending(mut live, ending),
        }) = self.sessions.remove(key)
        else {
            return false;
        };
        // A writer still blocked on the output is left behind, unless
        // cancelling ends its write.
        if ending.clean && !cut_short {
            live.pipe.cancel_clean();
        } else {
            live.pipe.cancel_abort();
        }
        log::debug!("session {id} ended");
        let (peer, _) = key;
        if self.host.ended(id, peer, ending.result) {
            return true;
        }
        // The endpoint stays up for others; the peer goes unless it has
        // more in flight. Streams it opened meanwhile must lead to a
        // session in time.
        if self.pending.keys().any(|(p, _, _)| p == peer) {
            self.readmit(peer);
        } else if !self.has_session(peer)
            && *peer != self.relay
            && let Err(e) = self.endpoint.disconnect(peer)
        {
            log::debug!("disconnect {peer}: {e}");
        }
        false
    }

    /// The endpoint failed: every session ends now. Their outputs get one
    /// grace between them, as nothing else is left to serve.
    fn fail(&mut self, error: &str) {
        let mut ended = Vec::new();
        for ((peer, _), slot) in self.sessions.drain() {
            let (mut live, result) = match slot.state {
                State::Running(live) => (live, Err(crate::Error::Other(error.to_owned()))),
                State::Ending(live, ending) => (live, ending.result),
                State::Connecting(_) | State::Done => continue,
            };
            live.pipe.close_output();
            ended.push((slot.id, peer, live, result));
        }
        let by = Instant::now() + STDOUT_GRACE;
        for (id, peer, mut live, result) in ended {
            live.pipe
                .finish(Some(by.saturating_duration_since(Instant::now())));
            self.host.ended(id, &peer, result);
        }
    }
}

impl Slot {
    /// The stream the session is on.
    fn link(&self) -> Option<&Link> {
        match &self.state {
            State::Connecting(held) => held.as_ref().map(|(link, _)| link),
            State::Running(live) | State::Ending(live, _) => live.link.as_ref(),
            State::Done => None,
        }
    }

    /// When the loop must wake for this slot; `resume_timeout` is how long
    /// a running session without a stream waits for the client.
    fn deadline(&self, resume_timeout: Duration) -> Option<Instant> {
        match &self.state {
            State::Running(live) => {
                let lost = |grace| live.lost_since.map(|t| t + grace);
                [
                    live.pipe.deadline(live.link.as_ref()),
                    live.link.as_ref().map(Link::dead_at),
                    lost(DELIVERED_GRACE).filter(|_| live.pipe.delivered()),
                    lost(HANDOVER_GRACE).filter(|_| live.handover),
                    lost(resume_timeout),
                ]
                .into_iter()
                .flatten()
                .min()
            }
            State::Ending(_, ending) => [
                ending.tell.as_ref().map(Stop::wake_at),
                ending.linger,
                (!ending.drained).then_some(ending.drain_by),
            ]
            .into_iter()
            .flatten()
            .min(),
            State::Connecting(_) | State::Done => None,
        }
    }
}

impl Live {
    fn lose<H: Host>(&mut self, id: u64, endpoint: &mut Endpoint, host: &mut H, reason: &str) {
        if let Some(link) = self.link.take() {
            log::debug!("lost the client stream ({reason}); waiting for it to resume");
            link.abandon(endpoint);
            self.lost_since = Some(Instant::now());
            if !self.handover {
                host.session_event(
                    id,
                    Event::LinkLost {
                        reason: reason.to_owned(),
                    },
                );
            }
        }
    }
}

impl Ending {
    /// Whether the teardown still waits for the peer to finish, given
    /// whether it has a stream. A completed session waits out its deadline
    /// even without one: a client that lost the stream before our last ack
    /// comes back for it on a fresh one.
    fn lingering(&self, has_link: bool, now: Instant) -> bool {
        self.linger
            .is_some_and(|by| now < by && (has_link || self.clean))
    }

    /// Starts tearing a session down as a dialer's [`net::finish`] does,
    /// but as deadlines: a completed session lingers for the peer to
    /// finish too, so our last ack is not lost; a stopped or broken one
    /// tells the peer; a peer that ended it is answered by closing our
    /// side. The output drains meanwhile, bounded unless the session
    /// completed.
    fn begin(end: End, live: &mut Live, endpoint: &mut Endpoint) -> Ending {
        let now = Instant::now();
        let mut ending = Ending {
            result: Ok(Outcome::Done),
            clean: false,
            tell: None,
            linger: None,
            by: now + LINGER,
            drain_by: now + STDOUT_GRACE,
            drained: false,
        };
        let mut linger = |live: &mut Live| {
            let link = live.link.as_ref()?;
            if let Err(e) = endpoint.close_stream_write(&link.peer, link.conn, link.stream) {
                log::debug!("close stream: {e}");
            }
            Some(now + LINGER)
        };
        match end {
            End::Done => {
                ending.clean = true;
                // Without a stream, still wait: the client may come back
                // on a fresh one for our last ack.
                ending.linger = linger(live).or(Some(now + LINGER));
            }
            End::Delivered => {
                ending.clean = true;
                ending.result = Ok(Outcome::Delivered);
            }
            End::Stopped(mut stop) => {
                ending.result = Err(stop.take_error());
                ending.tell = Some(stop);
            }
            End::Failed(e) if e.is::<PeerEnded>() => {
                // Half-closing back confirms to the peer that its Error
                // landed.
                ending.linger = linger(live);
                ending.result = Err(crate::Error::from_internal(e));
            }
            End::Failed(e) => {
                log::debug!("session failed: {e}");
                ending.tell = Some(Stop::broken());
                ending.result = Err(crate::Error::from_internal(e));
            }
        }
        live.pipe.close_output();
        ending
    }
}

fn constant_time_eq(a: &Token, b: &Token) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusals_hasten_a_disconnect_but_never_extend_it_far() {
        let t0 = Instant::now();
        let mut admission = Admission::new(t0);
        assert_eq!(admission.by, t0 + ADMISSION_TIMEOUT);

        // Refused early: gone shortly after, not at the deadline.
        admission.refused(t0 + Duration::from_secs(2));
        assert_eq!(admission.by, t0 + Duration::from_secs(2) + REFUSAL_GRACE);

        // Refused again and again: the deadline still holds, give or take
        // the last Error's grace.
        for secs in [5, 14, 20, 60] {
            admission.refused(t0 + Duration::from_secs(secs));
        }
        assert_eq!(admission.by, t0 + ADMISSION_TIMEOUT + REFUSAL_GRACE);
        assert_eq!(admission.deadline, t0 + ADMISSION_TIMEOUT);
    }

    #[test]
    fn a_completed_session_waits_out_its_linger_without_a_stream() {
        let t0 = Instant::now();
        let ending = |clean| Ending {
            result: Ok(Outcome::Done),
            clean,
            tell: None,
            linger: Some(t0 + LINGER),
            by: t0 + LINGER,
            drain_by: t0 + STDOUT_GRACE,
            drained: true,
        };
        // Its client may come back for our last ack, until the deadline.
        assert!(ending(true).lingering(false, t0));
        assert!(ending(true).lingering(true, t0));
        assert!(!ending(true).lingering(false, t0 + LINGER));
        // Otherwise only a stream the peer can still finish is waited on.
        assert!(ending(false).lingering(true, t0));
        assert!(!ending(false).lingering(false, t0));
        assert!(!ending(false).lingering(true, t0 + LINGER));
        // The peer finishing ends the wait.
        let mut finished = ending(true);
        finished.linger = None;
        assert!(!finished.lingering(true, t0));
    }

    #[test]
    fn tokens_compare_whole() {
        assert!(constant_time_eq(&[7; 16], &[7; 16]));
        let mut other = [7; 16];
        other[15] = 8;
        assert!(!constant_time_eq(&[7; 16], &other));
    }
}
