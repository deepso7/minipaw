//! minipaw: netcat between two machines over minip2p — QUIC, relayed
//! bootstrap, and hole-punched direct paths — with no accounts and no
//! control plane. Connection details travel out of band as a ticket.

use std::process::ExitCode;
use std::sync::Arc;

mod dial;
mod event;
mod io;
mod listen;
mod net;
mod pipe;

use minipaw::Ticket;

use event::{Event, Events};
use io::Io;
use net::Shared;

/// Prints the session's log records to stderr: diagnostics as `# …` lines,
/// warnings and errors as `minipaw: …`.
struct PlainLogger;

impl PlainLogger {
    /// Installs the logger: debug records with `-v`, warnings and errors
    /// otherwise.
    fn install(verbose: bool) {
        static LOGGER: PlainLogger = PlainLogger;
        if log::set_logger(&LOGGER).is_ok() {
            log::set_max_level(if verbose {
                log::LevelFilter::Debug
            } else {
                log::LevelFilter::Warn
            });
        }
    }
}

impl log::Log for PlainLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        let target = metadata.target();
        metadata.level() <= log::max_level()
            && (target == "minipaw" || target.starts_with("minipaw::"))
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        match record.level() {
            log::Level::Error | log::Level::Warn => eprintln!("minipaw: {}", record.args()),
            _ => eprintln!("# {}", record.args()),
        }
    }

    fn flush(&self) {}
}

/// Prints a session's events as today's `#` status lines.
fn print_event(event: Event) {
    match event {
        Event::Reserving { relay } => eprintln!("# reserving a slot on relay {relay}…"),
        Event::Listening { ticket } => {
            eprintln!("# 🐾 listening; connect with:\nminipaw {ticket}");
        }
        Event::ReservationSlow => {
            eprintln!("# still no relay reservation; is the relay reachable over UDP?");
        }
        Event::ReservationLost => eprintln!("# lost the relay reservation; reacquiring"),
        Event::Accepted { peer, path } => eprintln!("# connection from {peer} ({path})"),
        Event::Connected { path, .. } => eprintln!("# connected ({path})"),
        Event::Upgraded => eprintln!("# upgraded to a direct connection"),
        Event::Connecting { .. } | Event::LinkLost { .. } | Event::Resumed | Event::Stopping => {}
    }
}

const USAGE: &str = "\
minipaw: pipe stdin/stdout between two machines, peer to peer

USAGE:
    minipaw [--relay <multiaddr>] [-v]           listen and print a ticket
    minipaw [-v] <ticket>                        connect to a listening server
    minipaw parse <ticket>                       show what a ticket contains

OPTIONS:
    --relay <multiaddr>   Circuit Relay v2 server (QUIC), e.g.
                          /ip4/203.0.113.7/udp/19876/quic-v1/p2p/12D3KooW…
                          Defaults to $MINIPAW_RELAY, then the built-in relay.
    -v, --verbose         Log connection progress to stderr
    -h, --help            Show this help
";

struct Args {
    command: Command,
    verbose: bool,
}

enum Command {
    Listen { relay: Option<String> },
    Dial { ticket: Ticket },
    Parse { ticket: Ticket },
    Help,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut verbose = false;
    let command = parse_command(args, &mut verbose)?;
    Ok(Args { command, verbose })
}

fn parse_command(
    args: impl IntoIterator<Item = String>,
    verbose: &mut bool,
) -> Result<Command, String> {
    let mut relay = None;
    let mut positional = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-v" | "--verbose" => *verbose = true,
            "--relay" => relay = Some(args.next().ok_or("--relay requires a value")?),
            flag if flag.starts_with("--relay=") => {
                relay = flag.strip_prefix("--relay=").map(str::to_owned);
            }
            flag if flag.starts_with('-') => return Err(format!("unknown flag '{flag}'")),
            _ => positional.push(arg),
        }
    }
    match positional.as_slice() {
        [] => Ok(Command::Listen { relay }),
        [cmd, ticket] if cmd == "parse" => Ok(Command::Parse {
            ticket: ticket.parse()?,
        }),
        [ticket] => {
            let ticket: Ticket = ticket.parse()?;
            if relay.is_some() {
                return Err(
                    "--relay only applies when listening; tickets carry their relay".into(),
                );
            }
            Ok(Command::Dial { ticket })
        }
        _ => Err("too many arguments".into()),
    }
}

fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Help => print!("{USAGE}"),
        Command::Listen { relay } => {
            let relay = net::resolve_relay(relay.as_deref())?;
            let shared = Arc::new(Shared::default());
            net::handle_interrupt(shared.clone())?;
            listen::run(relay, Io::stdio(), shared, Events::new(print_event))?;
        }
        Command::Dial { ticket } => {
            let relay = match &ticket.relay {
                Some(relay) => relay.clone(),
                None => net::resolve_relay(None)?,
            };
            let shared = Arc::new(Shared::default());
            net::handle_interrupt(shared.clone())?;
            dial::run(ticket, relay, Io::stdio(), shared, Events::new(print_event))?;
        }
        Command::Parse { ticket } => {
            println!("peer:  {}", ticket.peer);
            match &ticket.relay {
                Some(relay) => println!("relay: {relay}"),
                None => println!("relay: (default)"),
            }
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let Args { command, verbose } = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("minipaw: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    PlainLogger::install(verbose);
    match run(command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) if e.is::<net::Interrupted>() => ExitCode::from(130),
        Err(e) => {
            eprintln!("minipaw: {e}");
            ExitCode::FAILURE
        }
    }
}
