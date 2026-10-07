//! minipaw: netcat between two machines over minip2p — QUIC, relayed
//! bootstrap, and hole-punched direct paths — with no accounts and no
//! control plane. Connection details travel out of band as a ticket.

use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};

use minipaw::{Config, Event, Handle, Io, Multiaddr, PeerAddr, Session, Ticket};

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

/// Prints a session's events as `#` status lines.
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
        // Connecting, LinkLost, Resumed and Stopping have no line; -v covers them.
        _ => {}
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
            ticket: ticket.parse().map_err(|e| format!("{e}"))?,
        }),
        [ticket] => {
            let ticket: Ticket = ticket.parse().map_err(|e| format!("{e}"))?;
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

/// `--relay`, else `MINIPAW_RELAY`; `None` means the built-in default.
fn relay_setting(flag: Option<&str>) -> Result<Option<PeerAddr>, minipaw::Error> {
    let env = std::env::var("MINIPAW_RELAY").ok();
    flag.or(env.as_deref())
        .filter(|s| !s.is_empty())
        .map(minipaw::parse_relay)
        .transpose()
}

/// The settings both modes share, from the environment.
fn base_config() -> Config {
    let mut config = Config::default();
    // Test hook: relay only, no direct dials or hole punching.
    config.force_relay = std::env::var_os("MINIPAW_FORCE_RELAY").is_some();
    config
}

/// Test hook: a server address to dial alongside the relay, for benchmarks
/// and paths hole punching cannot find.
fn direct_setting(ticket: &Ticket) -> Result<Option<Multiaddr>, String> {
    let Ok(raw) = std::env::var("MINIPAW_DIRECT") else {
        return Ok(None);
    };
    let addr: Multiaddr = raw
        .parse()
        .map_err(|e| format!("invalid MINIPAW_DIRECT '{raw}': {e}"))?;
    PeerAddr::new(addr.clone(), ticket.peer().clone())
        .map_err(|e| format!("invalid MINIPAW_DIRECT '{raw}': {e}"))?;
    Ok(Some(addr))
}

/// Test hook: the server forgets its stream after this many bytes.
fn drop_link_setting() -> Result<Option<u64>, String> {
    match std::env::var("MINIPAW_TEST_DROP_LINK_AFTER") {
        Ok(raw) => raw
            .parse()
            .map(Some)
            .map_err(|e| format!("invalid MINIPAW_TEST_DROP_LINK_AFTER '{raw}': {e}")),
        Err(_) => Ok(None),
    }
}

/// Routes Ctrl-C to the session: the first stops it, telling the peer; a
/// second exits on the spot.
fn handle_interrupt(handle: Handle) -> Result<(), String> {
    static PRESSED: AtomicBool = AtomicBool::new(false);
    ctrlc::set_handler(move || {
        if PRESSED.swap(true, Ordering::SeqCst) {
            std::process::exit(130);
        }
        handle.stop();
    })
    .map_err(|e| format!("installing the Ctrl-C handler: {e}"))
}

/// Runs a session with stdio and the plain status printer.
fn run_session(session: Session) -> Result<(), Box<dyn std::error::Error>> {
    let session = session.on_event(print_event);
    handle_interrupt(session.handle())?;
    session.run()?;
    Ok(())
}

fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Help => print!("{USAGE}"),
        Command::Listen { relay } => {
            let mut config = base_config();
            config.relay = relay_setting(relay.as_deref())?;
            config.test_drop_link_after = drop_link_setting()?;
            run_session(minipaw::listen(config, Io::stdio()))?;
        }
        Command::Dial { ticket } => {
            let mut config = base_config();
            // Tickets carry their relay; only one without needs ours.
            if ticket.relay().is_none() {
                config.relay = relay_setting(None)?;
            }
            config.direct = direct_setting(&ticket)?;
            run_session(minipaw::dial(ticket, config, Io::stdio()))?;
        }
        Command::Parse { ticket } => {
            println!("peer:  {}", ticket.peer());
            match ticket.relay() {
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
    let Err(e) = run(command) else {
        return ExitCode::SUCCESS;
    };
    match e.downcast_ref::<minipaw::Error>() {
        Some(minipaw::Error::Stopped) => return ExitCode::from(130),
        Some(minipaw::Error::Input(e)) => eprintln!("minipaw: reading stdin: {e}"),
        Some(minipaw::Error::Output(e)) => eprintln!("minipaw: writing stdout: {e}"),
        _ => eprintln!("minipaw: {e}"),
    }
    ExitCode::FAILURE
}
