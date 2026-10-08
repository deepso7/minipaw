//! Serving many dialers at once, each session with a local end of its own,
//! such as a fresh connection to a port being forwarded.
//!
//! This is the server loop with room for [`Config::max_sessions`]. Getting
//! a session's local end may block (connecting to a target), so it never
//! happens on the loop's thread: each new session's `accept` call runs on
//! a short-lived worker, whose result comes back over a channel.

use std::fmt;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

use minip2p::{PeerAddr, PeerId, WaitHandle};

use crate::Error;
use crate::config::{self, Config};
use crate::event::{Event, PathKind};
use crate::io::Io;
use crate::net;
use crate::pipe::{LocalFailure, Pipe, Stats};
use crate::server::{Exit, Host, ServerCore};
use crate::session::{Handle, Outcome, Progress, Shared};
use crate::ticket::Ticket;

/// Makes the local end of each new session.
type Accept = dyn Fn(&PeerId) -> std::io::Result<Io> + Send + Sync;

/// A milestone of a [`Server`], delivered synchronously on its thread.
///
/// Sessions are numbered from 1 in the order they arrive; the numbers are
/// never reused within a run.
#[derive(Debug)]
#[non_exhaustive]
pub enum ServeEvent {
    /// A milestone of the server itself: [`Event::Reserving`],
    /// [`Event::Listening`] with the ticket, the `Reservation*` events, and
    /// [`Event::Stopping`].
    Server(Event),
    /// A dialer was admitted and its session is up: its `accept` call
    /// returned a local end.
    Opened {
        /// The session's number.
        session: u64,
        /// The dialer's peer id.
        peer: PeerId,
        /// How the connection is routed.
        path: PathKind,
    },
    /// A milestone of an open session: [`Event::Upgraded`],
    /// [`Event::LinkLost`], [`Event::Resumed`], or [`Event::Stopping`]
    /// after a local I/O error.
    Session {
        /// The session's number.
        session: u64,
        /// What happened.
        event: Event,
    },
    /// A dialer was turned away and told `reason`: a wrong token, the
    /// server is busy or stopping, its `accept` call failed, or it tried to
    /// resume a session that is over.
    Refused {
        /// The dialer's peer id.
        peer: PeerId,
        /// Why, as the dialer is told.
        reason: String,
    },
    /// An [`Opened`](ServeEvent::Opened) session is over and torn down;
    /// its local end is closed.
    Ended {
        /// The session's number.
        session: u64,
        /// The dialer's peer id.
        peer: PeerId,
        /// How it ended, as [`Session::run`](crate::Session::run) would
        /// report it. [`Error::Stopped`] after [`Handle::stop`].
        result: Result<Outcome, Error>,
        /// The bytes it moved.
        progress: Progress,
    },
}

/// A server, ready to [`run`](Self::run). Create one with [`serve`].
pub struct Server {
    config: Config,
    accept: Arc<Accept>,
    events: Box<dyn FnMut(ServeEvent) + Send>,
    shared: Arc<Shared>,
}

/// A server: it reserves a slot on the relay, reports a ticket through
/// [`Event::Listening`] (in [`ServeEvent::Server`]), and serves every dialer
/// that presents it, up to [`Config::max_sessions`] at once, each in a
/// session of its own. It runs until stopped.
///
/// For each new session, `accept` is called with the dialer's peer id to
/// make the session's local end, typically [`Io::tcp`] on a fresh
/// connection to the service being forwarded. An `Err` refuses the dialer
/// with the error's text.
///
/// `accept` runs on a worker thread of its own, so a slow one holds up no
/// other session. It must still return promptly (connect with a timeout,
/// for instance), because it cannot be cancelled: a call still running
/// when the server stops is left to finish, and what it returns is closed.
/// Each call in flight counts toward `max_sessions` until it returns.
///
/// A [`Config::identity`] keeps the ticket the same from run to run, but
/// sessions do not survive a restart: a dialer that was connected when the
/// server stopped gets [`Error::PeerEnded`] and must start over.
///
/// A relay may limit the circuits it carries to one server (the minip2p
/// relay allows 4 by default, each for a limited time and size). Sessions
/// that cannot hole-punch a direct path share those circuits, and resume
/// as they turn over.
pub fn serve<F>(config: Config, accept: F) -> Server
where
    F: Fn(&PeerId) -> std::io::Result<Io> + Send + Sync + 'static,
{
    Server {
        config,
        accept: Arc::new(accept),
        events: Box::new(|_| {}),
        shared: Arc::default(),
    }
}

impl Server {
    /// Calls `sink` with each [`ServeEvent`], synchronously on the thread
    /// running the server, so it must return quickly and must not block.
    /// Replaces any earlier sink.
    #[must_use]
    pub fn on_event(mut self, sink: impl FnMut(ServeEvent) + Send + 'static) -> Self {
        self.events = Box::new(sink);
        self
    }

    /// A handle to stop the server or read its cumulative progress from
    /// other threads.
    pub fn handle(&self) -> Handle {
        Handle(self.shared.clone())
    }

    /// Binds the network endpoint and serves on the calling thread until
    /// [`Handle::stop`]. Each session's input and output run on helper
    /// threads.
    ///
    /// On a stop, the server admits no one new, stops every session (each
    /// dialer is told), gives them a few seconds between them to finish
    /// tearing down, then cuts short whatever is left and returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for an invalid [`Config`]; otherwise the network
    /// failing, such as the endpoint not binding. Sessions failing do not
    /// end the server: they are reported in [`ServeEvent::Ended`].
    pub fn run(self) -> Result<(), Error> {
        let Server {
            config,
            accept,
            events,
            shared,
        } = self;
        // A stop during setup is seen once the endpoint is bound.
        if shared.stopped() {
            return Ok(());
        }
        if config.force_relay && config.direct.is_some() {
            return Err(Error::Config(
                "a direct address and force_relay cannot both be set".into(),
            ));
        }
        if config.max_sessions == 0 {
            return Err(Error::Config("max_sessions must be at least 1".into()));
        }
        let relay = config::relay_or_default(config.relay.as_ref())?;
        let result = run(relay, &config, accept, shared.clone(), events);
        shared.clear_wake();
        result
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn run(
    relay: PeerAddr,
    config: &Config,
    accept: Arc<Accept>,
    shared: Arc<Shared>,
    events: Box<dyn FnMut(ServeEvent) + Send>,
) -> Result<(), Error> {
    let endpoint = net::bind(&relay, true, config).map_err(Error::from_internal)?;
    // A saved identity keeps the ticket the same from run to run.
    let token = match &config.identity {
        Some(identity) => identity.token(),
        None => net::random16().map_err(Error::from_internal)?,
    };
    let ticket = Ticket::listener(endpoint.peer_id().clone(), token, Some(&relay));
    shared.set_wake(endpoint.wait_handle());
    let host = ServeHost::new(
        accept,
        endpoint.wait_handle(),
        config.max_sessions,
        shared.clone(),
        events,
    );
    let relay = relay.peer_id().clone();
    let max = config.max_sessions;
    let mut server = ServerCore::new(endpoint, ticket, relay, config, shared, host, max);
    let exit = server.run();
    // Calls still in flight find nobody to take their local ends, which
    // close as they drop.
    server.close();
    match exit {
        Ok(Exit::Stopped(Error::Stopped)) => Ok(()),
        Ok(Exit::Stopped(e)) => Err(e),
        // The host never ends the server.
        Ok(Exit::Host) => Ok(()),
        Err(e) => Err(Error::from_internal(e)),
    }
}

/// An `accept` call's result for session `id`.
type Accepted = (u64, std::io::Result<Io>);

/// The server's side of many sessions: local ends from worker threads,
/// and events.
struct ServeHost {
    accept: Arc<Accept>,
    /// Wakes the loop: workers use it once their result is sent, and each
    /// session's pipe as for any session.
    wake: WaitHandle,
    results: Receiver<Accepted>,
    sender: Sender<Accepted>,
    /// `accept` calls running, from spawning a worker until the loop takes
    /// its result, even when its session went away meanwhile. Never more
    /// than `max`, so churning dialers cannot pile up workers or target
    /// connections.
    in_flight: usize,
    max: usize,
    shared: Arc<Shared>,
    events: Box<dyn FnMut(ServeEvent) + Send>,
}

impl ServeHost {
    fn new(
        accept: Arc<Accept>,
        wake: WaitHandle,
        max: usize,
        shared: Arc<Shared>,
        events: Box<dyn FnMut(ServeEvent) + Send>,
    ) -> Self {
        let (sender, results) = mpsc::channel();
        ServeHost {
            accept,
            wake,
            results,
            sender,
            in_flight: 0,
            max,
            shared,
            events,
        }
    }
}

impl Host for ServeHost {
    fn open(&mut self, id: u64, peer: &PeerId) -> Result<Option<Pipe>, String> {
        if self.in_flight >= self.max {
            return Err(self.busy(self.max));
        }
        let accept = self.accept.clone();
        let sender = self.sender.clone();
        let wake = self.wake.clone();
        let dialer = peer.clone();
        let spawned = thread::Builder::new()
            .name(format!("minipaw-accept-{id}"))
            .spawn(move || {
                let io = panic::catch_unwind(AssertUnwindSafe(|| accept(&dialer)))
                    .unwrap_or_else(|_| Err(std::io::Error::other("accept panicked")));
                // The server may be gone; the local end then closes as it
                // drops.
                if sender.send((id, io)).is_ok() {
                    wake.interrupt();
                }
            });
        match spawned {
            Ok(_) => {
                self.in_flight += 1;
                Ok(None)
            }
            Err(e) => Err(format!("cannot start a worker: {e}")),
        }
    }

    fn ready(&mut self) -> Option<(u64, Result<Pipe, String>)> {
        let (id, io) = self.results.try_recv().ok()?;
        self.in_flight -= 1;
        let result = match io {
            Ok(io) => {
                let stats = Arc::new(Stats::default());
                self.shared.track(id, stats.clone());
                Ok(Pipe::new(&self.wake, io, stats))
            }
            Err(e) => Err(e.to_string()),
        };
        Some((id, result))
    }

    fn unused(&mut self, id: u64, mut pipe: Pipe) {
        log::debug!("session {id}: local end unused; closing it");
        self.shared.forget(id);
        pipe.cancel_abort();
    }

    fn failure(&mut self) -> Option<LocalFailure> {
        None
    }

    fn busy(&self, sessions: usize) -> String {
        format!("busy ({sessions} sessions)")
    }

    fn server_event(&mut self, event: Event) {
        (self.events)(ServeEvent::Server(event));
    }

    fn refused(&mut self, peer: &PeerId, reason: &str) {
        (self.events)(ServeEvent::Refused {
            peer: peer.clone(),
            reason: reason.to_owned(),
        });
    }

    fn session_event(&mut self, id: u64, event: Event) {
        let event = match event {
            Event::Accepted { peer, path } => ServeEvent::Opened {
                session: id,
                peer,
                path,
            },
            event => ServeEvent::Session { session: id, event },
        };
        (self.events)(event);
    }

    fn ended(&mut self, id: u64, peer: &PeerId, result: Result<Outcome, Error>) -> bool {
        let progress = self.shared.retire(id);
        (self.events)(ServeEvent::Ended {
            session: id,
            peer: peer.clone(),
            result,
            progress,
        });
        false
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::sync::Mutex;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::io::tests::tcp_pair;

    fn peer() -> PeerId {
        crate::Identity::generate().unwrap().peer_id()
    }

    fn host(accept: impl Fn(&PeerId) -> std::io::Result<Io> + Send + Sync + 'static) -> ServeHost {
        let events = Box::new(|_| {});
        let shared = Arc::default();
        ServeHost::new(Arc::new(accept), WaitHandle::noop(), 2, shared, events)
    }

    /// The next result, waiting for a worker to send it.
    fn next(host: &mut ServeHost) -> (u64, Result<Pipe, String>) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(result) = host.ready() {
                return result;
            }
            assert!(Instant::now() < deadline, "no result");
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn calls_in_flight_hold_permits_until_their_results_are_taken() {
        // Each call waits for its own go-ahead.
        let (go, gate) = mpsc::channel::<()>();
        let gate = Mutex::new(gate);
        let mut host = host(move |_| {
            gate.lock().unwrap().recv().unwrap();
            Err(std::io::Error::other("target down"))
        });
        let peer = peer();
        assert!(matches!(host.open(1, &peer), Ok(None)));
        assert!(matches!(host.open(2, &peer), Ok(None)));
        assert_eq!(host.open(3, &peer).err().unwrap(), "busy (2 sessions)");

        // A call that returned still holds its permit until the loop takes
        // its result, whether or not its session is still around.
        go.send(()).unwrap();
        thread::sleep(Duration::from_millis(50));
        assert!(host.open(4, &peer).is_err());
        let (id, result) = next(&mut host);
        assert!(id == 1 || id == 2);
        assert_eq!(result.err().unwrap(), "target down");
        assert!(matches!(host.open(5, &peer), Ok(None)));
        assert!(host.open(6, &peer).is_err());

        go.send(()).unwrap();
        go.send(()).unwrap();
        assert!(next(&mut host).1.is_err());
        assert!(next(&mut host).1.is_err());
        assert_eq!(host.in_flight, 0);
    }

    #[test]
    fn a_panicking_accept_still_gives_its_permit_back() {
        let mut host = host(|_| panic!("boom"));
        let peer = peer();
        for id in 1..=4 {
            assert!(matches!(host.open(id, &peer), Ok(None)));
            let (got, result) = next(&mut host);
            assert_eq!(got, id);
            assert_eq!(result.err().unwrap(), "accept panicked");
        }
        assert_eq!(host.in_flight, 0);
    }

    #[test]
    fn an_unused_local_end_is_closed_and_not_counted() {
        let (target, ours) = mpsc::channel();
        let mut host = host(move |_| {
            let (ours_end, theirs) = tcp_pair();
            target.send(theirs).unwrap();
            Io::tcp(ours_end)
        });
        // Session 7 went away (or was replaced) before its target was
        // connected: the core hands the local end straight back.
        assert!(matches!(host.open(7, &peer()), Ok(None)));
        let (id, pipe) = next(&mut host);
        assert_eq!(id, 7);
        host.unused(id, pipe.unwrap());
        assert!(host.shared.tally_is_empty());

        // The target sees its connection close rather than linger.
        let mut theirs = ours.recv().unwrap();
        theirs
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        assert!(matches!(theirs.read(&mut [0; 16]), Ok(0) | Err(_)));
    }

    #[test]
    fn progress_adds_up_every_session_and_never_goes_down() {
        let shared = Arc::new(Shared::default());
        let handle = Handle(shared.clone());
        let (a, b) = (Arc::new(Stats::default()), Arc::new(Stats::default()));
        shared.track(1, a.clone());
        shared.track(2, b.clone());
        a.read.store(100, Ordering::Relaxed);
        a.written.0.store(5, Ordering::Relaxed);
        b.received.store(40, Ordering::Relaxed);
        let before = handle.progress();
        assert_eq!((before.read, before.received, before.written), (100, 40, 5));

        let ended = shared.retire(1);
        assert_eq!((ended.read, ended.written), (100, 5));
        // A detached writer counting on after the end is not seen.
        a.written.0.store(9, Ordering::Relaxed);
        assert_eq!(handle.progress(), before);

        // One that never started counts for nothing.
        shared.track(3, Arc::new(Stats::default()));
        shared.forget(3);
        b.received.store(50, Ordering::Relaxed);
        shared.retire(2);
        let after = handle.progress();
        assert_eq!((after.read, after.received, after.written), (100, 50, 5));
        assert!(shared.tally_is_empty());
    }

    #[test]
    fn a_stop_before_run_returns_at_once() {
        let server = serve(Config::default(), |_| Err(std::io::Error::other("unused")));
        server.handle().stop();
        assert!(server.run().is_ok());
    }

    #[test]
    fn zero_sessions_is_a_config_error() {
        let config = Config {
            max_sessions: 0,
            ..Config::default()
        };
        let server = serve(config, |_| Err(std::io::Error::other("unused")));
        assert!(matches!(server.run(), Err(Error::Config(_))));
    }

    #[test]
    fn servers_move_between_threads() {
        fn assert_send<T: Send>() {}
        assert_send::<Server>();
    }
}
