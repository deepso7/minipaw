//! minipaw: netcat between two machines over minip2p — QUIC, relayed
//! bootstrap, and hole-punched direct paths — with no accounts and no
//! control plane. Connection details travel out of band as a ticket.

use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};

mod dial;
mod listen;
mod net;
mod pipe;
mod session;
mod ticket;
mod wire;

use ticket::Ticket;

static VERBOSE: AtomicBool = AtomicBool::new(false);

pub fn verbose() -> bool {
    VERBOSE.load(Ordering::Relaxed)
}

/// Prints a `#`-prefixed diagnostic to stderr under `-v`.
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        if $crate::verbose() {
            eprintln!("# {}", format_args!($($arg)*));
        }
    };
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

enum Command {
    Listen { relay: Option<String> },
    Dial { ticket: Ticket },
    Parse { ticket: Ticket },
    Help,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut relay = None;
    let mut positional = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-v" | "--verbose" => VERBOSE.store(true, Ordering::Relaxed),
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
                return Err("--relay only applies when listening; tickets carry their relay".into());
            }
            Ok(Command::Dial { ticket })
        }
        _ => Err("too many arguments".into()),
    }
}

fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Help => print!("{USAGE}"),
        Command::Listen { relay } => listen::run(net::resolve_relay(relay.as_deref())?)?,
        Command::Dial { ticket } => {
            let relay = match &ticket.relay {
                Some(relay) => relay.clone(),
                None => net::resolve_relay(None)?,
            };
            dial::run(ticket, relay)?;
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
    let command = match parse_args(std::env::args().skip(1)) {
        Ok(command) => command,
        Err(e) => {
            eprintln!("minipaw: {e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match run(command) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("minipaw: {e}");
            ExitCode::FAILURE
        }
    }
}
