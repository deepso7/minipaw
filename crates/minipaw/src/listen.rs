//! Server mode: reserve a relay slot, print a ticket, and pipe stdio with
//! the first client that presents the ticket's token. That client may come
//! back on new streams (path upgrades, relay cutoffs) to resume its
//! session; anyone else is turned away.
//!
//! This is the server loop with room for one session, whose local end
//! exists before any client does, and which ends with that session.

use std::sync::Arc;

use minip2p::{PeerAddr, PeerId};

use crate::config::Config;
use crate::event::{Event, Events};
use crate::io::Io;
use crate::net::{self, STDOUT_GRACE};
use crate::pipe::{LocalFailure, Pipe};
use crate::server::{Exit, Host, ServerCore};
use crate::session::{Outcome, Shared};
use crate::ticket::Ticket;

/// The one session's side of the server.
struct Listener {
    /// The local end, until the session takes it. Created up front, so a
    /// local input error ends the listener even before a client comes.
    pipe: Option<Pipe>,
    events: Events,
    /// How the session ended, once it has.
    result: Option<Result<Outcome, crate::Error>>,
}

/// Runs a listener's session through `relay`.
pub fn run(
    relay: PeerAddr,
    config: &Config,
    io: Io,
    shared: Arc<Shared>,
    events: Events,
) -> Result<Outcome, crate::Error> {
    let endpoint = net::bind(&relay, true, config).map_err(crate::Error::from_internal)?;
    // A saved identity keeps the ticket the same from run to run.
    let token = match &config.identity {
        Some(identity) => identity.token(),
        None => net::random16().map_err(crate::Error::from_internal)?,
    };
    let ticket = Ticket::listener(endpoint.peer_id().clone(), token, Some(&relay));
    shared.set_wake(endpoint.wait_handle());
    let pipe = Pipe::new(&endpoint.wait_handle(), io, shared.stats.clone());
    let listener = Listener {
        pipe: Some(pipe),
        events,
        result: None,
    };
    let relay = relay.peer_id().clone();
    let mut server = ServerCore::new(endpoint, ticket, relay, config, shared.clone(), listener, 1);
    let exit = server.run();
    let mut listener = server.close();
    if let Some(mut pipe) = listener.pipe.take() {
        pipe.finish(Some(STDOUT_GRACE));
    }
    match (listener.result, exit) {
        // Once asked to stop, the stop is the outcome whatever else went
        // wrong meanwhile, e.g. the peer stopping at the same moment.
        (Some(Err(_)), _) if shared.stopped() => Err(crate::Error::Stopped),
        (Some(result), _) => result,
        (None, Ok(Exit::Stopped(e))) => Err(e),
        (None, Err(e)) => Err(crate::Error::from_internal(e)),
        (None, Ok(Exit::Host)) => Err(crate::Error::Other("the session never ended".into())),
    }
}

impl Host for Listener {
    fn open(&mut self, _id: u64, _peer: &PeerId) -> Result<Option<Pipe>, String> {
        self.pipe.take().map(Some).ok_or_else(|| self.busy(1))
    }

    fn ready(&mut self) -> Option<(u64, Result<Pipe, String>)> {
        None
    }

    fn unused(&mut self, _id: u64, pipe: Pipe) {
        self.pipe = Some(pipe);
    }

    fn failure(&mut self) -> Option<LocalFailure> {
        self.pipe.as_ref()?.take_failure()
    }

    fn busy(&self, _sessions: usize) -> String {
        "busy with another client".to_owned()
    }

    fn server_event(&mut self, event: Event) {
        self.events.emit(event);
    }

    fn refused(&mut self, _peer: &PeerId, _reason: &str) {}

    fn session_event(&mut self, _id: u64, event: Event) {
        self.events.emit(event);
    }

    fn ended(&mut self, _id: u64, _peer: &PeerId, result: Result<Outcome, crate::Error>) -> bool {
        self.result = Some(result);
        true
    }
}
