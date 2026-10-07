//! minipaw: netcat between two machines over minip2p — QUIC, relayed
//! bootstrap, and hole-punched direct paths — with no accounts and no
//! control plane. Connection details travel out of band as a ticket.

pub mod ticket;
#[doc(hidden)]
pub mod window;
#[doc(hidden)]
pub mod wire;

pub use ticket::Ticket;
pub use wire::{PROTOCOL, VERSION as PROTOCOL_VERSION};
