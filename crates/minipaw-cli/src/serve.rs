//! `minipaw serve`: a server with a stable ticket that forwards every
//! session to a local TCP port, such as sshd's.
//!
//! It is a daemon, so it never draws a UI: it prints plain `#` lines on
//! stderr, the same whether that is a terminal, a pipe or a journal. Its
//! sessions are numbered, `# [3] …`, as the SDK numbers them.

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use minipaw::{Error, Event, Io, PeerId, ServeEvent};

use crate::args::{self, Forward};
use crate::ui::{fmt, plain, term};

/// How long to wait for the target to accept a connection. The SDK runs
/// each attempt on a worker of its own, which cannot be cancelled, so this
/// bounds how long a stop can wait on one.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// `serve`'s flags.
pub struct Options<'a> {
    pub forward: &'a Forward,
    pub identity: Option<&'a Path>,
    pub new: bool,
    pub max_sessions: usize,
    pub relay: Option<&'a str>,
}

/// Serves until Ctrl-C (exit 0); a second Ctrl-C exits 130 at once.
/// Problems at startup, such as a bad identity file or a target that does
/// not resolve, exit 1.
pub fn run(options: &Options<'_>) -> ExitCode {
    match serve(options) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("minipaw: {e}");
            ExitCode::FAILURE
        }
    }
}

fn serve(options: &Options<'_>) -> Result<(), String> {
    let settings = args::serve_config(options.relay, options.max_sessions)?;
    let mut config = settings.config;
    let targets = options.forward.resolve()?;
    let (identity, path, created) = args::load_identity(options.identity, options.new)?;
    config.identity = Some(identity);
    eprintln!("{}", args::identity_line(&path, created));
    eprintln!("# forwarding to {}", target_text(options.forward, &targets));

    let delay = settings.connect_delay;
    let server = minipaw::serve(config, move |peer| connect(peer, &targets, delay));
    let mut printer = Printer::default();
    let server = server.on_event(move |event| printer.print(event));
    term::install_interrupts(server.handle())?;
    server.run().map_err(|e| e.to_string())?;
    eprintln!("# stopped");
    Ok(())
}

/// `127.0.0.1:22`, or `localhost:22 (127.0.0.1:22)` for a name.
fn target_text(forward: &Forward, targets: &[SocketAddr]) -> String {
    let given = forward.to_string();
    let resolved: Vec<String> = targets.iter().map(ToString::to_string).collect();
    if resolved == [given.clone()] {
        given
    } else {
        format!("{given} ({})", resolved.join(", "))
    }
}

/// The local end of a new session: a fresh connection to the target,
/// trying each of its addresses in turn. Runs on a worker thread.
fn connect(peer: &PeerId, targets: &[SocketAddr], delay: Option<Duration>) -> io::Result<Io> {
    let peer = fmt::short_peer(peer);
    if let Some(delay) = delay {
        log::debug!("test hook: waiting {delay:?} before connecting for {peer}");
        std::thread::sleep(delay);
    }
    let mut last = None;
    for addr in targets {
        log::debug!("target: connecting to {addr} for {peer}");
        match TcpStream::connect_timeout(addr, CONNECT_TIMEOUT) {
            Ok(stream) => {
                // Interactive traffic like ssh's keystrokes must not wait
                // for Nagle.
                stream.set_nodelay(true)?;
                log::debug!("target: connected to {addr} for {peer}");
                return Io::tcp(stream);
            }
            Err(e) => {
                log::debug!("target: connecting to {addr} for {peer}: {e}");
                last = Some(e);
            }
        }
    }
    let e = last.unwrap_or_else(|| io::Error::other("no target address"));
    // The text is what the dialer is told: "server refused: <it>".
    Err(io::Error::new(e.kind(), connect_reason(&e)))
}

/// A short reason for a failed connection to the target, without the
/// `(os error 61)` noise.
fn connect_reason(e: &io::Error) -> String {
    match e.kind() {
        io::ErrorKind::ConnectionRefused => "connection refused".into(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => "connect timed out".into(),
        _ => io_text(e),
    }
}

/// An I/O error's text, lower-cased, without its `(os error N)` suffix.
fn io_text(e: &io::Error) -> String {
    let text = e.to_string();
    let text = match text.rfind(" (os error ") {
        Some(at) if text.ends_with(')') => &text[..at],
        _ => &text,
    };
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// Prints the server's events as `#` lines, remembering when each session
/// opened for its summary.
#[derive(Default)]
struct Printer {
    opened: HashMap<u64, Instant>,
}

impl Printer {
    fn print(&mut self, event: ServeEvent) {
        if let Some(text) = self.text(event, Instant::now()) {
            eprintln!("{text}");
        }
    }

    /// The line for `event`, if it has one, without a trailing newline.
    fn text(&mut self, event: ServeEvent, now: Instant) -> Option<String> {
        Some(match event {
            // The ticket as plain mode prints it (scripts read the
            // `minipaw mp…` line), then how to use it with ssh.
            ServeEvent::Server(Event::Listening { ticket }) => {
                let line = plain::event_text(&Event::Listening {
                    ticket: ticket.clone(),
                })?;
                format!("{line}\n# ssh: ssh -o ProxyCommand='minipaw -q {ticket}' user@host")
            }
            ServeEvent::Server(Event::Stopping) => match self.opened.len() {
                0 => "# stopping".to_owned(),
                1 => "# stopping; ending 1 session".to_owned(),
                n => format!("# stopping; ending {n} sessions"),
            },
            ServeEvent::Server(event) => plain::event_text(&event)?,
            ServeEvent::Opened {
                session,
                peer,
                path,
            } => {
                self.opened.insert(session, now);
                format!(
                    "# [{session}] {} connected ({path})",
                    fmt::short_peer(&peer)
                )
            }
            // Upgraded, as plain mode puts it; -v covers the rest.
            ServeEvent::Session { session, event } => {
                let text = plain::event_text(&event)?;
                let text = text.strip_prefix("# ").unwrap_or(&text);
                format!("# [{session}] {text}")
            }
            ServeEvent::Refused { peer, reason } => {
                format!("# refused {}: {reason}", fmt::short_peer(&peer))
            }
            ServeEvent::Ended {
                session,
                result,
                progress,
                ..
            } => {
                let took = self
                    .opened
                    .remove(&session)
                    .map_or(Duration::ZERO, |t| now.duration_since(t));
                match result {
                    // As the panel's summary: sent is what the dialer
                    // confirmed of the target's bytes, received what
                    // reached the target.
                    Ok(_) => format!(
                        "# [{session}] ended: {} sent, {} received in {}",
                        fmt::bytes(progress.acked),
                        fmt::bytes(progress.written),
                        fmt::duration(took),
                    ),
                    Err(e) => format!("# [{session}] ended: {}", error_text(&e)),
                }
            }
            _ => return None,
        })
    }
}

/// Why a session failed, from the server's side: its input and output are
/// the target.
fn error_text(e: &Error) -> String {
    match e {
        Error::Input(e) => format!("reading from the target: {}", io_text(e)),
        Error::Output(e) => format!("writing to the target: {}", io_text(e)),
        e => e.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use minipaw::{Outcome, PathKind, Progress};

    use super::*;

    fn peer() -> PeerId {
        "12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf"
            .parse()
            .expect("peer id")
    }

    #[test]
    fn connect_errors_read_short() {
        let reason = |kind, text: &str| connect_reason(&io::Error::new(kind, text.to_owned()));
        assert_eq!(
            reason(io::ErrorKind::ConnectionRefused, "x"),
            "connection refused"
        );
        assert_eq!(reason(io::ErrorKind::TimedOut, "x"), "connect timed out");
        assert_eq!(
            reason(io::ErrorKind::Other, "Network is unreachable (os error 51)"),
            "network is unreachable"
        );
        assert!(!io_text(&io::Error::from_raw_os_error(61)).contains("os error"));
        assert_eq!(io_text(&io::Error::other("")), "");
    }

    #[test]
    fn a_closed_port_is_refused() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);
        let e = connect(&peer(), &[addr], None).err().expect("refused");
        assert_eq!(e.to_string(), "connection refused");
    }

    #[test]
    fn targets_show_what_they_resolved_to() {
        let forward: Forward = "127.0.0.1:22".parse().expect("forward");
        let addrs = forward.resolve().expect("resolve");
        assert_eq!(target_text(&forward, &addrs), "127.0.0.1:22");
        let named = Forward {
            host: "localhost".into(),
            port: 22,
        };
        assert_eq!(target_text(&named, &addrs), "localhost:22 (127.0.0.1:22)");
    }

    #[test]
    fn session_lines() {
        let mut printer = Printer::default();
        let t0 = Instant::now();
        let opened = ServeEvent::Opened {
            session: 3,
            peer: peer(),
            path: PathKind::Direct,
        };
        assert_eq!(
            printer.text(opened, t0).as_deref(),
            Some("# [3] 12D3KooW…tLdf connected (direct)")
        );
        let upgraded = ServeEvent::Session {
            session: 3,
            event: Event::Upgraded,
        };
        assert_eq!(
            printer.text(upgraded, t0).as_deref(),
            Some("# [3] upgraded to a direct connection")
        );
        let lost = ServeEvent::Session {
            session: 3,
            event: Event::LinkLost { reason: "x".into() },
        };
        assert_eq!(printer.text(lost, t0), None);
        assert_eq!(
            printer
                .text(ServeEvent::Server(Event::Stopping), t0)
                .as_deref(),
            Some("# stopping; ending 1 session")
        );
        let ended = ServeEvent::Ended {
            session: 3,
            peer: peer(),
            result: Ok(Outcome::Done),
            progress: Progress {
                acked: 1_300_000,
                written: 40_000,
                ..Progress::default()
            },
        };
        assert_eq!(
            printer
                .text(ended, t0 + Duration::from_secs(182))
                .as_deref(),
            Some("# [3] ended: 1.2 MiB sent, 39.1 KiB received in 3:02")
        );
        let failed = ServeEvent::Ended {
            session: 4,
            peer: peer(),
            result: Err(Error::Output(io::Error::from_raw_os_error(32))),
            progress: Progress::default(),
        };
        let text = printer.text(failed, t0).expect("line");
        assert!(
            text.starts_with("# [4] ended: writing to the target: "),
            "{text}"
        );
        assert!(!text.contains("os error"), "{text}");
        let refused = ServeEvent::Refused {
            peer: peer(),
            reason: "busy (16 sessions)".into(),
        };
        assert_eq!(
            printer.text(refused, t0).as_deref(),
            Some("# refused 12D3KooW…tLdf: busy (16 sessions)")
        );
    }
}
