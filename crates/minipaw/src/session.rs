//! Starting, driving and stopping a session.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use minip2p::WaitHandle;

use crate::config::{self, Config};
use crate::event::{Event, Events};
use crate::io::Io;
use crate::pipe::Stats;
use crate::{Error, Ticket, dial as dialer, listen as listener};

/// Which end of a session this is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Waits for a dialer, reachable through a ticket.
    Listener,
    /// Connects to a listener's ticket.
    Dialer,
}

/// How a session ended successfully.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Both directions finished and each side confirmed the other's data.
    Done,
    /// All data in both directions is confirmed, but the connection went
    /// away before the peer collected our last acknowledgement.
    Delivered,
}

/// Bytes moved so far, from [`Handle::progress`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    /// Bytes taken from the session's input.
    pub read: u64,
    /// Of those, bytes the peer has confirmed writing to its output.
    pub acked: u64,
    /// Bytes received from the peer.
    pub received: u64,
    /// Of those, bytes written to the session's output.
    pub written: u64,
}

/// What a session shares with other threads: stop requests and progress.
#[derive(Default)]
pub(crate) struct Shared {
    stop: AtomicBool,
    /// Wakes the session's `Endpoint::wait`, once it has an endpoint.
    wake: Mutex<Option<WaitHandle>>,
    pub(crate) stats: Arc<Stats>,
}

impl Shared {
    /// Asks the session to stop and wakes it.
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Ok(wake) = self.wake.lock()
            && let Some(wake) = wake.as_ref()
        {
            wake.interrupt();
        }
    }

    pub(crate) fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Lets [`stop`](Self::stop) wake the session's endpoint. A stop that
    /// came first wakes it now, so the loop sees it at once.
    pub(crate) fn set_wake(&self, wake: WaitHandle) {
        if let Ok(mut slot) = self.wake.lock() {
            *slot = Some(wake.clone());
        }
        if self.stopped() {
            wake.interrupt();
        }
    }

    fn clear_wake(&self) {
        if let Ok(mut slot) = self.wake.lock() {
            *slot = None;
        }
    }
}

/// Controls a session from other threads: stop it, or read its progress.
/// Cheap to clone.
#[derive(Clone)]
pub struct Handle(Arc<Shared>);

impl Handle {
    /// Ends the session as Ctrl-C does in the `minipaw` CLI: the peer is
    /// told (for a few seconds at most), output is flushed (for a second at
    /// most), and [`Session::run`] returns [`Error::Stopped`].
    ///
    /// Idempotent, never blocks for long, and safe to call from a signal
    /// handler thread, before [`Session::run`] or after it returned.
    pub fn stop(&self) {
        self.0.stop();
    }

    /// A snapshot of the session's byte counters. Cheap enough to poll
    /// several times a second.
    pub fn progress(&self) -> Progress {
        let stats = &self.0.stats;
        Progress {
            read: stats.read.load(Ordering::Relaxed),
            acked: stats.acked.load(Ordering::Relaxed),
            received: stats.received.load(Ordering::Relaxed),
            written: stats.written.0.load(Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("stopped", &self.0.stopped())
            .field("progress", &self.progress())
            .finish()
    }
}

enum Kind {
    Listen,
    Dial(Ticket),
}

/// A session, ready to [`run`](Self::run). Create one with [`listen`] or
/// [`dial`].
pub struct Session {
    kind: Kind,
    config: Config,
    io: Io,
    events: Events,
    shared: Arc<Shared>,
}

/// A listening session: it reserves a slot on the relay, reports a ticket
/// through [`Event::Listening`], and pipes `io` with the first dialer that
/// presents the ticket.
pub fn listen(config: Config, io: Io) -> Session {
    Session::new(Kind::Listen, config, io)
}

/// A session that connects to the listener behind `ticket` and pipes `io`
/// with it.
pub fn dial(ticket: Ticket, config: Config, io: Io) -> Session {
    Session::new(Kind::Dial(ticket), config, io)
}

impl Session {
    fn new(kind: Kind, config: Config, io: Io) -> Self {
        Session {
            kind,
            config,
            io,
            events: Events::default(),
            shared: Arc::default(),
        }
    }

    /// Calls `sink` with each [`Event`], synchronously on the thread running
    /// the session, so it must return quickly and must not block. Replaces
    /// any earlier sink.
    #[must_use]
    pub fn on_event(mut self, sink: impl FnMut(Event) + Send + 'static) -> Self {
        self.events = Events::new(sink);
        self
    }

    /// A handle to stop the session or read its progress from other
    /// threads.
    pub fn handle(&self) -> Handle {
        Handle(self.shared.clone())
    }

    /// Whether this session listens or dials.
    pub fn role(&self) -> Role {
        match self.kind {
            Kind::Listen => Role::Listener,
            Kind::Dial(_) => Role::Dialer,
        }
    }

    /// Binds the network endpoint and drives the session on the calling
    /// thread until it ends. Input and output run on helper threads.
    ///
    /// # Errors
    ///
    /// When the session did not complete; see [`Error`].
    pub fn run(self) -> Result<Outcome, Error> {
        let Session {
            kind,
            config,
            io,
            events,
            shared,
        } = self;
        if let Some(relay) = &config.relay {
            config::check_relay(relay)?;
        }
        let default = || {
            config::default_relay()
                .ok_or_else(|| Error::Config("the built-in relay address is invalid".into()))
        };
        let result = match kind {
            Kind::Listen => {
                let relay = config.relay.clone().map_or_else(default, Ok)?;
                listener::run(relay, &config, io, shared.clone(), events)
            }
            Kind::Dial(ticket) => {
                let relay = match ticket.relay.clone().or_else(|| config.relay.clone()) {
                    Some(relay) => relay,
                    None => default()?,
                };
                dialer::run(ticket, relay, &config, io, shared.clone(), events)
            }
        };
        shared.clear_wake();
        result
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn sessions_move_between_threads() {
        assert_send::<Session>();
        assert_send_sync::<Handle>();
    }

    #[test]
    fn a_stop_before_run_wins() {
        // A relay nobody answers on: without the stop, the listener would
        // wait for a reservation.
        let mut config = Config::default();
        config.relay = Some(config::parse_relay(UNREACHABLE_RELAY).expect("relay address"));
        let session = listen(config, Io::new(std::io::empty(), std::io::sink()));
        let handle = session.handle();
        handle.stop();
        let started = Instant::now();
        assert!(matches!(session.run(), Err(Error::Stopped)));
        assert!(started.elapsed() < Duration::from_secs(5));
        handle.stop();
    }

    /// Discard port on loopback, with the default relay's peer id.
    const UNREACHABLE_RELAY: &str =
        "/ip4/127.0.0.1/udp/9/quic-v1/p2p/12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf";
}
