//! Chat mode: a full-screen chat when stdin and stdout are the terminal.
//!
//! A stub for now: it runs [`plain`](super::plain) mode.

use minipaw::{Error, Outcome};

use super::{Launch, plain};

/// Runs the session as a chat.
pub fn run(launch: Launch) -> Result<Outcome, Error> {
    plain::run(launch)
}
