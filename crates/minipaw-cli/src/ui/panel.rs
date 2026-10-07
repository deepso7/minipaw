//! Panel mode: an inline status panel on stderr while stdout carries data.
//!
//! A stub for now: it runs [`plain`](super::plain) mode.

use minipaw::{Error, Outcome};

use super::{Launch, plain};

/// Runs the session with the status panel.
pub fn run(launch: Launch) -> Result<Outcome, Error> {
    plain::run(launch)
}
