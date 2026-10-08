#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod config;
mod dial;
mod error;
mod event;
mod identity;
mod io;
mod listen;
mod net;
mod pipe;
mod server;
mod session;
mod ticket;
mod window;
mod wire;

pub use config::{Config, DEFAULT_RELAY, parse_relay};
pub use error::Error;
pub use event::{Event, PathKind};
pub use identity::Identity;
pub use io::Io;
pub use minip2p::{Multiaddr, PeerAddr, PeerId};
pub use session::{Handle, Outcome, Progress, Role, Session, dial, listen};
pub use ticket::{Ticket, TicketError};
pub use wire::{PROTOCOL, VERSION as PROTOCOL_VERSION};
