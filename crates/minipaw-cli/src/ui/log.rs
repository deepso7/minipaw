//! The process's one logger, whose output can move between stderr and a
//! UI's channel.
//!
//! [`install`] runs once, from `main`. Records start out going to stderr as
//! plain lines ([`plain_line`]); a terminal UI calls [`route_logs_to`] to
//! receive them as [`UiMsg::Log`] on its channel instead, and
//! [`route_logs_to_stderr`] to give them back (dropping the receiver does
//! too: a failed send falls back to stderr).

use std::sync::mpsc::Sender;
use std::sync::{Mutex, PoisonError};

use super::UiMsg;

/// Where records go.
enum Sink {
    Stderr,
    Channel(Sender<UiMsg>),
}

/// Keeps only the SDK's records (`minipaw` and `minipaw::*` targets) and
/// hands them to the current sink.
struct Logger {
    sink: Mutex<Sink>,
}

static LOGGER: Logger = Logger {
    sink: Mutex::new(Sink::Stderr),
};

/// Installs the logger at [`level`]. Later calls do nothing.
pub fn install(verbose: bool, quiet: bool) {
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(level(verbose, quiet));
    }
}

/// Debug records with `verbose`, warnings and errors otherwise, and none
/// at all with `quiet`, which wins: under ssh's ProxyCommand stderr is the
/// user's terminal, shared with the ssh session.
pub fn level(verbose: bool, quiet: bool) -> log::LevelFilter {
    if quiet {
        log::LevelFilter::Off
    } else if verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
    }
}

/// Sends records to `tx` as [`UiMsg::Log`] from now on.
pub fn route_logs_to(tx: Sender<UiMsg>) {
    *LOGGER.sink.lock().unwrap_or_else(PoisonError::into_inner) = Sink::Channel(tx);
}

/// Prints records to stderr again, as [`plain_line`]s.
pub fn route_logs_to_stderr() {
    *LOGGER.sink.lock().unwrap_or_else(PoisonError::into_inner) = Sink::Stderr;
}

/// How plain mode prints a record: diagnostics as `# …`, warnings and
/// errors as `minipaw: …`. No trailing newline.
pub fn plain_line(level: log::Level, message: &str) -> String {
    match level {
        log::Level::Error | log::Level::Warn => format!("minipaw: {message}"),
        _ => format!("# {message}"),
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        let target = metadata.target();
        metadata.level() <= log::max_level()
            && (target == "minipaw" || target.starts_with("minipaw::"))
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let level = record.level();
        let mut sink = self.sink.lock().unwrap_or_else(PoisonError::into_inner);
        if let Sink::Channel(tx) = &*sink {
            if tx
                .send(UiMsg::Log(level, record.args().to_string()))
                .is_ok()
            {
                return;
            }
            // The UI is gone; nobody else will show it.
            *sink = Sink::Stderr;
        }
        drop(sink);
        match level {
            log::Level::Error | log::Level::Warn => eprintln!("minipaw: {}", record.args()),
            _ => eprintln!("# {}", record.args()),
        }
    }

    fn flush(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_lines_match_the_old_format() {
        assert_eq!(plain_line(log::Level::Debug, "bound x"), "# bound x");
        assert_eq!(plain_line(log::Level::Info, "hi"), "# hi");
        assert_eq!(plain_line(log::Level::Warn, "uh oh"), "minipaw: uh oh");
        assert_eq!(plain_line(log::Level::Error, "bad"), "minipaw: bad");
    }

    #[test]
    fn quiet_overrides_verbose() {
        assert_eq!(level(false, false), log::LevelFilter::Warn);
        assert_eq!(level(true, false), log::LevelFilter::Debug);
        assert_eq!(level(false, true), log::LevelFilter::Off);
        assert_eq!(level(true, true), log::LevelFilter::Off);
    }
}
