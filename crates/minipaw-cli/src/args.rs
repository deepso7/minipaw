//! The command line, and the settings it and the environment make.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use clap::error::ErrorKind;
use clap::{CommandFactory as _, Parser, Subcommand};
use minipaw::{Config, Identity, Multiaddr, PeerAddr, Ticket};

/// minipaw: pipe stdin/stdout between two machines, peer to peer.
///
/// Without a ticket, minipaw listens and prints a ticket; on the other
/// machine, `minipaw <ticket>` connects to it. Whatever each side reads
/// from stdin comes out of the other's stdout.
#[derive(Debug, Parser)]
#[command(
    name = "minipaw",
    version,
    override_usage = "minipaw [OPTIONS] [TICKET]\n       minipaw [-v] parse <TICKET>\n       minipaw [-v] ticket [--identity PATH] [--relay MULTIADDR]",
    after_help = "\
Examples:
  minipaw <big.iso                 send a file; prints a ticket
  minipaw <ticket> >big.iso        receive it on another machine
  minipaw / minipaw <ticket>       chat, when run in a terminal on both ends
  minipaw --identity my.key        listen with the same ticket every run

Environment:
  MINIPAW_RELAY   relay to use when --relay is not given (empty: the built-in relay)
  MINIPAW_HOME    where `minipaw ticket` keeps serve.key (default:
                  $XDG_CONFIG_HOME/minipaw, else ~/.config/minipaw)"
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

    /// Listen as the identity saved at PATH, created if missing, so the
    /// ticket stays the same from run to run (for the same relay). Anyone
    /// holding that ticket can connect whenever this listens.
    #[arg(long, value_name = "PATH", conflicts_with = "ticket")]
    pub identity: Option<PathBuf>,

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
                (args.identity.is_some(), "--identity"),
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
    /// Print the stable ticket of a saved identity, creating the identity
    /// if missing. The ticket goes to stdout, a `# identity: PATH
    /// (new|existing)` line to stderr.
    Ticket {
        /// The identity file. Defaults to serve.key in $MINIPAW_HOME, else
        /// $XDG_CONFIG_HOME/minipaw, else ~/.config/minipaw.
        #[arg(long, value_name = "PATH")]
        identity: Option<PathBuf>,
        /// The relay the listener will use; it is part of the ticket.
        /// Defaults to $MINIPAW_RELAY, then the built-in relay.
        #[arg(long, value_name = "MULTIADDR")]
        relay: Option<String>,
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
    let relay = |flag: Option<&str>| relay_with(flag, &env);
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
            // Test hook: the listener never sends its first Welcome.
            config.test_drop_welcome = env("MINIPAW_TEST_DROP_WELCOME").is_some();
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

/// `--relay`, else `MINIPAW_RELAY`; empty or neither means the default.
fn relay_with(
    flag: Option<&str>,
    env: impl Fn(&str) -> Option<OsString>,
) -> Result<Option<PeerAddr>, String> {
    let env = env("MINIPAW_RELAY").and_then(|v| v.into_string().ok());
    flag.or(env.as_deref())
        .filter(|s| !s.is_empty())
        .map(minipaw::parse_relay)
        .transpose()
        .map_err(|e| e.to_string())
}

/// Where `minipaw ticket` keeps its identity without `--identity`:
/// `serve.key` in `$MINIPAW_HOME`, else `$XDG_CONFIG_HOME/minipaw`, else
/// `~/.config/minipaw`. Empty variables count as unset, and so does a
/// relative `XDG_CONFIG_HOME`, as the XDG spec says.
fn default_identity_with(env: impl Fn(&str) -> Option<OsString>) -> Result<PathBuf, String> {
    let var = |name: &str| env(name).filter(|v| !v.is_empty()).map(PathBuf::from);
    let dir = var("MINIPAW_HOME")
        .or_else(|| {
            var("XDG_CONFIG_HOME")
                .filter(|p| p.is_absolute())
                .map(|p| p.join("minipaw"))
        })
        .or_else(|| var("HOME").map(|p| p.join(".config").join("minipaw")))
        .ok_or("cannot find a home directory; set MINIPAW_HOME or pass --identity")?;
    Ok(dir.join("serve.key"))
}

/// Loads the identity at `path`, creating it if missing, and says whether
/// it was created. Without a `path` it is the default one, whose directory
/// is created (mode 0700) if missing; an existing directory is never
/// changed.
///
/// # Errors
///
/// When the identity cannot be loaded or created, with the message to print.
pub fn load_identity(path: Option<&Path>) -> Result<(Identity, PathBuf, bool), String> {
    let path = match path {
        Some(path) => path.to_owned(),
        None => {
            let path = default_identity_with(|name| std::env::var_os(name))?;
            if let Some(dir) = path.parent() {
                create_private_dir(dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
            }
            path
        }
    };
    let (identity, created) = Identity::load_or_create(&path).map_err(|e| e.to_string())?;
    Ok((identity, path, created))
}

/// Creates `dir` and any missing parents with mode 0700. An existing `dir`
/// is left alone; the SDK checks it.
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    if dir.symlink_metadata().is_ok() {
        return Ok(());
    }
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)
}

/// The stable ticket for `minipaw ticket`, with the identity's path and
/// whether it was just created.
///
/// # Errors
///
/// When the relay or the identity is invalid, with the message to print.
pub fn stable_ticket(
    identity: Option<&Path>,
    relay: Option<&str>,
) -> Result<(Ticket, PathBuf, bool), String> {
    let relay = relay_with(relay, |name| std::env::var_os(name))?;
    let (identity, path, created) = load_identity(identity)?;
    Ok((identity.ticket(relay.as_ref()), path, created))
}

/// The `# identity: PATH (new|existing)` status line.
pub fn identity_line(path: &Path, created: bool) -> String {
    let state = if created { "new" } else { "existing" };
    format!("# identity: {} ({state})", path.display())
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
    fn identity_and_ticket() {
        let args = parse(&["--identity", "my.key"]).expect("listen");
        assert_eq!(args.identity.as_deref(), Some(Path::new("my.key")));

        let args = parse(&["-v", "ticket"]).expect("ticket");
        assert!(matches!(
            args.command,
            Some(Command::Ticket {
                identity: None,
                relay: None
            })
        ));
        let args = parse(&["ticket", "--identity", "my.key", "--relay", RELAY]).expect("ticket");
        let Some(Command::Ticket { identity, relay }) = args.command else {
            panic!("not ticket");
        };
        assert_eq!(identity.as_deref(), Some(Path::new("my.key")));
        assert_eq!(relay.as_deref(), Some(RELAY));
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
        // An identity is for listening, and `ticket` takes its own flags.
        assert_eq!(
            kind(&["--identity", "my.key", TICKET]),
            ErrorKind::ArgumentConflict
        );
        for argv in [
            ["--identity", "my.key", "ticket"],
            ["--relay", RELAY, "ticket"],
            ["--plain", "ticket", "-v"],
        ] {
            assert_eq!(kind(&argv), ErrorKind::ArgumentConflict, "{argv:?}");
        }
        assert_eq!(kind(&["ticket", TICKET]), ErrorKind::UnknownArgument);
        assert_eq!(kind(&["ticket", "--plain"]), ErrorKind::UnknownArgument);
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
            ("MINIPAW_TEST_DROP_WELCOME", "1"),
        ];
        let config = config_with(&listen, env(&vars)).expect("config");
        assert!(config.force_relay);
        assert!(config.relay.is_some());
        assert_eq!(config.test_drop_link_after, Some(1000));
        assert!(config.test_drop_welcome);

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
    fn the_default_identity_path() {
        let path = |vars: &[(&str, &str)]| default_identity_with(env(vars));
        let ok = |vars: &[(&str, &str)]| path(vars).expect("path");
        let home = ("HOME", "/home/me");
        assert_eq!(ok(&[home]), Path::new("/home/me/.config/minipaw/serve.key"));
        assert_eq!(
            ok(&[home, ("XDG_CONFIG_HOME", "/xdg")]),
            Path::new("/xdg/minipaw/serve.key")
        );
        // A relative or empty XDG_CONFIG_HOME is ignored.
        for xdg in ["rel", ""] {
            assert_eq!(
                ok(&[home, ("XDG_CONFIG_HOME", xdg)]),
                Path::new("/home/me/.config/minipaw/serve.key")
            );
        }
        assert_eq!(
            ok(&[home, ("XDG_CONFIG_HOME", "/xdg"), ("MINIPAW_HOME", "/mp")]),
            Path::new("/mp/serve.key")
        );
        assert!(path(&[]).unwrap_err().contains("MINIPAW_HOME"));
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
