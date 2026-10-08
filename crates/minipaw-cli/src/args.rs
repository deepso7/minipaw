//! The command line, and the settings it and the environment make.

use std::ffi::OsString;

use clap::error::ErrorKind;
use clap::{CommandFactory as _, Parser, Subcommand};
use minipaw::{Config, Multiaddr, PeerAddr, Ticket};

/// minipaw: pipe stdin/stdout between two machines, peer to peer.
///
/// Without a ticket, minipaw listens and prints a ticket; on the other
/// machine, `minipaw <ticket>` connects to it. Whatever each side reads
/// from stdin comes out of the other's stdout.
#[derive(Debug, Parser)]
#[command(
    name = "minipaw",
    version,
    override_usage = "minipaw [OPTIONS] [TICKET]\n       minipaw [-v] parse <TICKET>",
    after_help = "\
Examples:
  minipaw <big.iso                 send a file; prints a ticket
  minipaw <ticket> >big.iso        receive it on another machine
  minipaw / minipaw <ticket>       chat, when run in a terminal on both ends

Environment:
  MINIPAW_RELAY   relay to use when --relay is not given (empty: the built-in relay)"
)]
pub struct Args {
    /// A listener's ticket to connect to. Without one, listen and print a
    /// ticket.
    #[arg(value_name = "TICKET")]
    pub ticket: Option<Ticket>,

    /// Circuit Relay v2 server (QUIC) to listen through, e.g.
    /// /ip4/203.0.113.7/udp/19876/quic-v1/p2p/12D3KooW…
    /// Defaults to $MINIPAW_RELAY, then the built-in relay. Tickets carry
    /// their relay, so this only applies when listening.
    #[arg(long, value_name = "MULTIADDR", conflicts_with = "ticket")]
    pub relay: Option<String>,

    /// Log connection progress to stderr.
    #[arg(short, long, global = true)]
    pub verbose: bool,

    /// Print plain status lines instead of the terminal UI.
    #[arg(long)]
    pub plain: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Args {
    /// Parses `argv` (program name first). Session options cannot go with
    /// a subcommand; `-v` goes with anything.
    ///
    /// # Errors
    ///
    /// A usage error, or `--help`/`--version`; `exit` prints it.
    pub fn try_from_argv(
        argv: impl IntoIterator<Item = impl Into<OsString> + Clone>,
    ) -> Result<Self, clap::Error> {
        let args = Self::try_parse_from(argv)?;
        if args.command.is_some() {
            let session = [
                (args.ticket.is_some(), "[TICKET]"),
                (args.relay.is_some(), "--relay"),
                (args.plain, "--plain"),
            ];
            if let Some((_, name)) = session.iter().find(|(used, _)| *used) {
                return Err(Self::command().error(
                    ErrorKind::ArgumentConflict,
                    format!("{name} cannot be used with a subcommand"),
                ));
            }
        }
        Ok(args)
    }
}

/// Subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Show what a ticket contains.
    Parse {
        /// The ticket to show.
        #[arg(value_name = "TICKET")]
        ticket: Ticket,
    },
}

/// The session settings from `args` and the `MINIPAW_*` environment.
///
/// # Errors
///
/// When a setting is invalid, with the message to print.
pub fn config(args: &Args) -> Result<Config, String> {
    config_with(args, |name| std::env::var_os(name))
}

/// [`config`] with `env` standing in for the environment.
fn config_with(args: &Args, env: impl Fn(&str) -> Option<OsString>) -> Result<Config, String> {
    // Unicode-only variables count as unset when they are not.
    let var = |name: &str| env(name).and_then(|v| v.into_string().ok());
    let mut config = Config::default();
    // Test hook: relay only, no direct dials or hole punching.
    config.force_relay = env("MINIPAW_FORCE_RELAY").is_some();
    // `--relay`, else `MINIPAW_RELAY`; empty or neither means the default.
    let relay = |flag: Option<&str>| -> Result<Option<PeerAddr>, String> {
        let env = var("MINIPAW_RELAY");
        flag.or(env.as_deref())
            .filter(|s| !s.is_empty())
            .map(minipaw::parse_relay)
            .transpose()
            .map_err(|e| e.to_string())
    };
    match &args.ticket {
        None => {
            config.relay = relay(args.relay.as_deref())?;
            // Test hook: the listener forgets its stream after this many
            // bytes.
            if let Some(raw) = var("MINIPAW_TEST_DROP_LINK_AFTER") {
                config.test_drop_link_after =
                    Some(raw.parse().map_err(|e| {
                        format!("invalid MINIPAW_TEST_DROP_LINK_AFTER '{raw}': {e}")
                    })?);
            }
        }
        Some(ticket) => {
            // Tickets carry their relay; only one without needs ours.
            if ticket.relay().is_none() {
                config.relay = relay(None)?;
            }
            // Test hook: a listener address to dial alongside the relay,
            // for benchmarks and paths hole punching cannot find.
            if let Some(raw) = var("MINIPAW_DIRECT") {
                let addr: Multiaddr = raw
                    .parse()
                    .map_err(|e| format!("invalid MINIPAW_DIRECT '{raw}': {e}"))?;
                PeerAddr::new(addr.clone(), ticket.peer().clone())
                    .map_err(|e| format!("invalid MINIPAW_DIRECT '{raw}': {e}"))?;
                config.direct = Some(addr);
            }
        }
    }
    Ok(config)
}

/// Prints what `ticket` contains, for `minipaw parse`.
pub fn print_ticket(ticket: &Ticket) {
    println!("peer:  {}", ticket.peer());
    match ticket.relay() {
        Some(relay) => println!("relay: {relay}"),
        None => println!("relay: (default)"),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const RELAY: &str =
        "/ip4/127.0.0.1/udp/19876/quic-v1/p2p/12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf";

    fn parse(argv: &[&str]) -> Result<Args, clap::Error> {
        Args::try_from_argv(std::iter::once("minipaw").chain(argv.iter().copied()))
    }

    fn env<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<OsString> + 'a {
        move |name| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| OsString::from(v))
        }
    }

    /// A ticket a listener on the default relay printed, so it carries no
    /// relay.
    const TICKET: &str =
        "mpAQDrKEuiH1EeUzlyYzX5zShnJgAkCAESIJXT0qGmgiYC5QMCzvVNIDiMmFduWCFl_Yuh1DfJ1h8B";

    #[test]
    fn the_command_is_well_formed() {
        Args::command().debug_assert();
    }

    #[test]
    fn listen_dial_and_parse() {
        let args = parse(&["-v", "--relay", RELAY]).expect("listen");
        assert!(args.verbose && args.ticket.is_none() && args.command.is_none());
        assert_eq!(args.relay.as_deref(), Some(RELAY));

        let args = parse(&["--plain", TICKET]).expect("dial");
        assert!(args.plain && args.ticket.is_some());

        let args = parse(&["parse", TICKET]).expect("parse");
        assert!(matches!(args.command, Some(Command::Parse { .. })));

        // `-v` goes with anything, so wrappers can always pass it.
        for argv in [["-v", "parse", TICKET], ["parse", "-v", TICKET]] {
            let args = parse(&argv).expect("verbose parse");
            assert!(args.verbose && matches!(args.command, Some(Command::Parse { .. })));
        }
    }

    #[test]
    fn usage_errors() {
        let kind = |argv: &[&str]| parse(argv).map(|_| ()).unwrap_err().kind();
        assert_eq!(
            kind(&["--relay", RELAY, TICKET]),
            ErrorKind::ArgumentConflict
        );
        for argv in [["--plain", "parse", TICKET], [TICKET, "parse", TICKET]] {
            assert_eq!(kind(&argv), ErrorKind::ArgumentConflict, "{argv:?}");
        }
        assert_eq!(
            kind(&["--relay", RELAY, "parse", TICKET]),
            ErrorKind::ArgumentConflict
        );
        assert_eq!(kind(&["garbage"]), ErrorKind::ValueValidation);
        assert_eq!(kind(&["--bogus"]), ErrorKind::UnknownArgument);
        assert_eq!(
            kind(&["parse", "--plain", TICKET]),
            ErrorKind::UnknownArgument
        );
        assert_eq!(kind(&["parse"]), ErrorKind::MissingRequiredArgument);
        assert_eq!(parse(&["parse", "x"]).unwrap_err().exit_code(), 2);
    }

    #[test]
    fn config_from_the_environment() {
        let listen = parse(&[]).expect("listen");
        let config = config_with(&listen, env(&[])).expect("config");
        assert!(!config.force_relay && config.relay.is_none());

        let vars = [
            ("MINIPAW_FORCE_RELAY", ""),
            ("MINIPAW_RELAY", RELAY),
            ("MINIPAW_TEST_DROP_LINK_AFTER", "1000"),
        ];
        let config = config_with(&listen, env(&vars)).expect("config");
        assert!(config.force_relay);
        assert!(config.relay.is_some());
        assert_eq!(config.test_drop_link_after, Some(1000));

        // An empty MINIPAW_RELAY is unset.
        let config = config_with(&listen, env(&[("MINIPAW_RELAY", "")])).expect("config");
        assert!(config.relay.is_none());

        let dial = parse(&[TICKET]).expect("dial");
        let config = config_with(
            &dial,
            env(&[("MINIPAW_DIRECT", "/ip4/127.0.0.1/udp/9/quic-v1")]),
        )
        .expect("config");
        assert!(config.direct.is_some());
        // The dialer uses MINIPAW_RELAY for a ticket without a relay.
        let config = config_with(&dial, env(&[("MINIPAW_RELAY", RELAY)])).expect("config");
        assert!(config.relay.is_some());
    }

    #[test]
    fn config_errors_keep_their_wording() {
        let listen = parse(&[]).expect("listen");
        let err = |args: &Args, vars: &[(&str, &str)]| config_with(args, env(vars)).unwrap_err();
        assert_eq!(
            err(&listen, &[("MINIPAW_TEST_DROP_LINK_AFTER", "lots")]),
            "invalid MINIPAW_TEST_DROP_LINK_AFTER 'lots': invalid digit found in string"
        );
        assert!(
            err(&listen, &[("MINIPAW_RELAY", "nope")])
                .starts_with("invalid relay address 'nope': ")
        );
        let flag = parse(&["--relay", "nope"]).expect("listen");
        assert!(err(&flag, &[]).starts_with("invalid relay address 'nope': "));

        let dial = parse(&[TICKET]).expect("dial");
        assert!(
            err(&dial, &[("MINIPAW_DIRECT", "nope")])
                .starts_with("invalid MINIPAW_DIRECT 'nope': ")
        );
        let with_peer = format!(
            "/ip4/127.0.0.1/udp/9/quic-v1/p2p/{}",
            "12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf"
        );
        let e = err(&dial, &[("MINIPAW_DIRECT", &with_peer)]);
        assert!(e.starts_with("invalid MINIPAW_DIRECT '/ip4/"), "{e}");
    }
}
