//! What a session reports as it goes.

use std::fmt;

use minip2p::PeerId;
use minipaw::Ticket;

/// How the connection to the peer is routed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathKind {
    /// Through the relay's circuit.
    Relayed,
    /// Straight to the peer, dialed or hole-punched.
    Direct,
}

/// `direct` or `via relay`.
impl fmt::Display for PathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            PathKind::Direct => "direct",
            PathKind::Relayed => "via relay",
        })
    }
}

impl PathKind {
    pub(crate) fn from_direct(direct: bool) -> Self {
        if direct {
            PathKind::Direct
        } else {
            PathKind::Relayed
        }
    }
}

/// A milestone in a session, delivered synchronously on the session's
/// thread.
#[derive(Clone, Debug)]
#[non_exhaustive]
#[allow(dead_code)] // Fields only UIs read; the plain printer does not.
pub enum Event {
    /// The listener is asking `relay` for a slot.
    Reserving {
        /// The relay's peer id.
        relay: PeerId,
    },
    /// The listener holds a relay slot; dialers can connect with `ticket`.
    Listening {
        /// What a dialer needs to connect.
        ticket: Ticket,
    },
    /// The relay reservation is taking a long time.
    ReservationSlow,
    /// The relay dropped the reservation; the listener is getting another.
    ReservationLost,
    /// The dialer is connecting to `peer`.
    Connecting {
        /// The listener's peer id.
        peer: PeerId,
    },
    /// The listener admitted a dialer.
    Accepted {
        /// The dialer's peer id.
        peer: PeerId,
        /// How the connection is routed.
        path: PathKind,
    },
    /// The dialer's session is up.
    Connected {
        /// The listener's peer id.
        peer: PeerId,
        /// How the connection is routed.
        path: PathKind,
    },
    /// The connection moved from the relay to a direct path.
    Upgraded,
    /// The stream carrying the session died; it is being resumed.
    LinkLost {
        /// What happened to it.
        reason: String,
    },
    /// The session resumed on a fresh stream.
    Resumed,
    /// The session is stopping on our side and telling the peer.
    Stopping,
}

/// Where a session's events go.
#[derive(Default)]
pub struct Events(Option<Box<dyn FnMut(Event) + Send>>);

impl Events {
    pub fn new(sink: impl FnMut(Event) + Send + 'static) -> Self {
        Events(Some(Box::new(sink)))
    }

    pub fn emit(&mut self, event: Event) {
        if let Some(sink) = &mut self.0 {
            sink(event);
        }
    }
}
