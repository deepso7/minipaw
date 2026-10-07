//! The ways `minipaw` talks to the person running it, picked by
//! [`choose_mode`]:
//!
//! - [`plain`]: `#` status lines on stderr, as scripts expect;
//! - [`panel`]: an inline status panel on stderr while stdout carries data;
//! - [`chat`]: a full-screen chat when stdin and stdout are a terminal.
//!
//! The interfaces here and in [`state`], [`fmt`], [`term`] and [`log`] are
//! what the panel and chat build on.

pub mod chat;
pub mod chat_io;
pub mod fmt;
pub mod log;
pub mod panel;
pub mod plain;
pub mod state;
pub mod term;

use std::io::IsTerminal as _;

use minipaw::{Config, Error, Event, Io, Outcome, Role, Session, Ticket};

/// How to show a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// `#` lines on stderr.
    Plain,
    /// An inline status panel on stderr; stdout carries data.
    Panel,
    /// A full-screen chat; stdin and stdout are the terminal.
    Chat,
}

/// The smallest terminal, as `(columns, rows)`, the panel and chat draw
/// in; anything smaller gets [`Mode::Plain`].
pub const MIN_SIZE: (u16, u16) = (40, 8);

/// Picks the mode from the facts that decide it: `--plain`, which of
/// stdin, stdout and stderr are terminals, and whether `TERM` is dumb or
/// unset.
pub fn select_mode(
    plain: bool,
    stdin_tty: bool,
    stdout_tty: bool,
    stderr_tty: bool,
    dumb: bool,
) -> Mode {
    if plain || !stderr_tty || dumb {
        Mode::Plain
    } else if stdin_tty && stdout_tty {
        Mode::Chat
    } else if !stdout_tty {
        Mode::Panel
    } else {
        Mode::Plain
    }
}

/// [`select_mode`] for this process, falling back to [`Mode::Plain`] when
/// the terminal is smaller than [`MIN_SIZE`] or its size is unknown.
pub fn choose_mode(plain: bool) -> Mode {
    let dumb = std::env::var_os("TERM").is_none_or(|term| term.is_empty() || term == "dumb");
    let mode = select_mode(
        plain,
        std::io::stdin().is_terminal(),
        std::io::stdout().is_terminal(),
        std::io::stderr().is_terminal(),
        dumb,
    );
    if mode == Mode::Plain {
        return mode;
    }
    match term::size() {
        Some((cols, rows)) if cols >= MIN_SIZE.0 && rows >= MIN_SIZE.1 => {}
        _ => return Mode::Plain,
    }
    if mode == Mode::Chat && !term::raw_mode_works() {
        return Mode::Plain;
    }
    mode
}

/// Everything needed to start the session, whatever the mode.
#[derive(Clone, Debug)]
pub struct Launch {
    /// Listening, or dialing [`ticket`](Self::ticket).
    pub role: Role,
    /// The ticket to dial; `None` when listening.
    pub ticket: Option<Ticket>,
    /// Session settings.
    pub config: Config,
    /// `-v`: show the SDK's debug log.
    pub verbose: bool,
    /// The size of stdin when it is a regular file, for progress and ETA.
    pub input_len: Option<u64>,
}

impl Launch {
    /// A launch that dials `ticket`, or listens without one.
    pub fn new(ticket: Option<Ticket>, config: Config, verbose: bool) -> Self {
        Launch {
            role: if ticket.is_some() {
                Role::Dialer
            } else {
                Role::Listener
            },
            ticket,
            config,
            verbose,
            input_len: stdin_len(),
        }
    }

    /// The session, over `io`.
    pub fn session(&self, io: Io) -> Session {
        match &self.ticket {
            Some(ticket) => minipaw::dial(ticket.clone(), self.config.clone(), io),
            None => minipaw::listen(self.config.clone(), io),
        }
    }
}

/// The size of stdin, when it is a regular file.
fn stdin_len() -> Option<u64> {
    #[cfg(unix)]
    {
        std::fs::metadata("/dev/stdin")
            .ok()
            .filter(std::fs::Metadata::is_file)
            .map(|meta| meta.len())
    }
    #[cfg(not(unix))]
    {
        None
    }
}

/// What a terminal UI's loop receives from the session's thread, in order,
/// over one channel: events from [`Session::on_event`] and log records
/// from [`log::route_logs_to`].
#[derive(Debug)]
pub enum UiMsg {
    /// A session event.
    Event(Event),
    /// A log record that passed the level filter: its level and message.
    /// [`log::plain_line`] formats it as plain mode would.
    Log(::log::Level, String),
}

/// Runs the session in `mode`. The logger is installed (`-v` decides the
/// level) and the terminal is restored by the caller.
pub fn run(mode: Mode, launch: Launch) -> Result<Outcome, Error> {
    log::install(launch.verbose);
    match mode {
        Mode::Plain => plain::run(launch),
        Mode::Panel => panel::run(launch),
        Mode::Chat => chat::run(launch),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_table() {
        use Mode::{Chat, Panel, Plain};
        // (plain, stdin, stdout, stderr, dumb) -> mode
        let table = [
            ((false, true, true, true, false), Chat),
            ((false, false, true, true, false), Plain),
            ((false, true, false, true, false), Panel),
            ((false, false, false, true, false), Panel),
            // --plain, a redirected stderr or a dumb terminal win.
            ((true, true, true, true, false), Plain),
            ((true, false, false, true, false), Plain),
            ((false, true, true, false, false), Plain),
            ((false, false, false, false, false), Plain),
            ((false, true, true, true, true), Plain),
            ((false, false, false, true, true), Plain),
        ];
        for ((plain, stdin, stdout, stderr, dumb), want) in table {
            assert_eq!(
                select_mode(plain, stdin, stdout, stderr, dumb),
                want,
                "plain={plain} stdin={stdin} stdout={stdout} stderr={stderr} dumb={dumb}"
            );
        }
    }
}
