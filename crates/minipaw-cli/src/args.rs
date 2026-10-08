//! The command line, and the settings it and the environment make.

use std::ffi::OsString;
use std::fmt;
use std::net::{SocketAddr, ToSocketAddrs as _};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use clap::builder::RangedU64ValueParser;
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
    override_usage = "minipaw [OPTIONS] [TICKET]\n       minipaw [-v] parse <TICKET>\n       minipaw [-v] ticket [--identity PATH] [--relay MULTIADDR]\n       minipaw [-v] serve [--forward HOST:PORT|PORT] [--identity PATH] [--new] [--max-sessions N] [--relay MULTIADDR]\n       minipaw ssh [SSH OPTIONS] [USER@]TICKET [COMMAND]\n       minipaw cp [SCP OPTIONS] SOURCE... TARGET",
    after_help = "\
Examples:
  minipaw <big.iso                 send a file; prints a ticket
  minipaw <ticket> >big.iso        receive it on another machine
  minipaw / minipaw <ticket>       chat, when run in a terminal on both ends
  minipaw --identity my.key        listen with the same ticket every run
  minipaw serve                    forward every session to local port 22
  minipaw ssh <ticket>             ssh to that machine through it
  minipaw cp f <ticket>:           copy a file to it with scp

Environment:
  MINIPAW_RELAY   relay to use when --relay is not given (empty: the built-in relay)
  MINIPAW_HOME    where `minipaw ticket` and `serve` keep serve.key (default:
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

    /// Print nothing on stderr but a fatal error: no UI, no status lines,
    /// no log, even with -v. For ssh's ProxyCommand, which shares stderr
    /// with the ssh session.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Print plain status lines instead of the terminal UI.
    #[arg(long)]
    pub plain: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

impl Args {
    /// Parses `argv` (program name first). Session options cannot go with
    /// a subcommand; `-v` goes with anything, and `-q` with anything but
    /// `ticket` and `serve`, whose status lines are their output.
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
        if args.quiet
            && matches!(
                args.command,
                Some(Command::Ticket { .. } | Command::Serve { .. })
            )
        {
            return Err(Self::command().error(
                ErrorKind::ArgumentConflict,
                "--quiet cannot be used with ticket or serve",
            ));
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
    /// Listen with a stable ticket and forward every session to a local
    /// TCP port, several at once, until Ctrl-C. Status goes to stderr as
    /// `#` lines.
    Serve {
        /// Where to connect each session: HOST:PORT, or a PORT on
        /// 127.0.0.1. Resolved once, at startup.
        #[arg(long, value_name = "HOST:PORT", default_value = "127.0.0.1:22")]
        forward: Forward,
        /// The identity file, created if missing. Defaults to serve.key in
        /// $MINIPAW_HOME, else $XDG_CONFIG_HOME/minipaw, else
        /// ~/.config/minipaw.
        #[arg(long, value_name = "PATH")]
        identity: Option<PathBuf>,
        /// Replace the identity with a new one first, so the old ticket
        /// stops working.
        #[arg(long)]
        new: bool,
        /// How many sessions to serve at once; more are refused as busy.
        #[arg(
            long,
            value_name = "N",
            default_value_t = minipaw::DEFAULT_MAX_SESSIONS,
            value_parser = RangedU64ValueParser::<usize>::new().range(1..),
        )]
        max_sessions: usize,
        /// Circuit Relay v2 server (QUIC) to listen through; it is part of
        /// the ticket. Defaults to $MINIPAW_RELAY, then the built-in relay.
        #[arg(long, value_name = "MULTIADDR")]
        relay: Option<String>,
    },
    /// ssh to a machine running `minipaw serve`, its ticket in place of the
    /// host.
    ///
    /// `minipaw ssh [SSH OPTIONS] [USER@]TICKET [COMMAND]` runs the system's
    /// ssh with every other argument as is, reaching the host through
    /// `minipaw -q TICKET`. Its host key is filed in known_hosts as
    /// minipaw-<peer id>. $MINIPAW_SSH names another ssh to run.
    Ssh {
        /// ssh's arguments, with the ticket as the destination.
        #[arg(
            value_name = "SSH ARGS",
            trailing_var_arg = true,
            allow_hyphen_values = true,
            required = true
        )]
        args: Vec<OsString>,
    },
    /// Copy files to or from a machine running `minipaw serve` with scp.
    ///
    /// `minipaw cp [SCP OPTIONS] SOURCE... TARGET` runs the system's scp
    /// with remote paths written `[USER@]TICKET:PATH`, all with the same
    /// ticket, and every other argument as is. $MINIPAW_SCP names another
    /// scp to run.
    Cp {
        /// scp's arguments, with remote paths as TICKET:PATH.
        #[arg(
            value_name = "SCP ARGS",
            trailing_var_arg = true,
            allow_hyphen_values = true,
            required = true
        )]
        args: Vec<OsString>,
    },
}

/// Where `serve` forwards to, as given: a host (name or IP) and a port.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Forward {
    /// A name or an IP address, without brackets.
    pub host: String,
    /// The port.
    pub port: u16,
}

impl Forward {
    /// The addresses to connect to, in order: resolved now, once, so a
    /// name that stops resolving later cannot fail a session.
    ///
    /// # Errors
    ///
    /// When it does not resolve, with the message to print.
    pub fn resolve(&self) -> Result<Vec<SocketAddr>, String> {
        let addrs: Vec<SocketAddr> = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|e| format!("cannot resolve {self}: {e}"))?
            .collect();
        if addrs.is_empty() {
            return Err(format!("cannot resolve {self}: no addresses"));
        }
        Ok(addrs)
    }
}

/// A bare port means 127.0.0.1; an IPv6 host goes in brackets.
impl FromStr for Forward {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let port = |p: &str| match p.parse::<u16>() {
            Ok(0) | Err(_) => Err(format!("invalid port '{p}': expected 1-65535")),
            Ok(port) => Ok(port),
        };
        if !s.contains(':') {
            if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
                return Ok(Forward {
                    host: "127.0.0.1".into(),
                    port: port(s)?,
                });
            }
            return Err(format!("expected HOST:PORT or PORT, got '{s}'"));
        }
        let (host, p) = s
            .rsplit_once(':')
            .ok_or_else(|| format!("expected HOST:PORT or PORT, got '{s}'"))?;
        let host = match host.strip_prefix('[') {
            Some(inner) => inner
                .strip_suffix(']')
                .ok_or_else(|| format!("unclosed '[' in '{s}'"))?,
            // A bare IPv6 address would be ambiguous: `::1:22`.
            None if host.contains(':') => {
                return Err(format!("put an IPv6 host in brackets: '[{host}]:PORT'"));
            }
            None => host,
        };
        if host.is_empty() {
            return Err(format!("expected HOST:PORT or PORT, got '{s}'"));
        }
        Ok(Forward {
            host: host.to_owned(),
            port: port(p)?,
        })
    }
}

impl fmt::Display for Forward {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
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
            listener_hooks(&mut config, &env)?;
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

/// The test hooks a listener and a server share.
fn listener_hooks(
    config: &mut Config,
    env: impl Fn(&str) -> Option<OsString>,
) -> Result<(), String> {
    // Test hook: the listener forgets its stream after this many bytes.
    if let Some(raw) = env("MINIPAW_TEST_DROP_LINK_AFTER").and_then(|v| v.into_string().ok()) {
        config.test_drop_link_after = Some(
            raw.parse()
                .map_err(|e| format!("invalid MINIPAW_TEST_DROP_LINK_AFTER '{raw}': {e}"))?,
        );
    }
    // Test hook: the listener never sends its first Welcome.
    config.test_drop_welcome = env("MINIPAW_TEST_DROP_WELCOME").is_some();
    // Test hook: how long the listener waits for a client to resume.
    config.test_resume_timeout = seconds(&env, "MINIPAW_TEST_RESUME_TIMEOUT")?;
    Ok(())
}

/// A test hook's duration in (fractional) seconds, if it is set.
fn seconds(env: impl Fn(&str) -> Option<OsString>, name: &str) -> Result<Option<Duration>, String> {
    let Some(raw) = env(name).and_then(|v| v.into_string().ok()) else {
        return Ok(None);
    };
    raw.parse::<f64>()
        .ok()
        .and_then(|secs| Duration::try_from_secs_f64(secs).ok())
        .map(Some)
        .ok_or_else(|| format!("invalid {name} '{raw}': expected seconds"))
}

/// What `serve` runs with besides its target and identity.
#[derive(Debug)]
pub struct ServeConfig {
    /// The server's settings, without the identity.
    pub config: Config,
    /// Test hook `MINIPAW_TEST_CONNECT_DELAY` (seconds): how long each
    /// session waits before connecting to the target, to hold it in its
    /// connecting state.
    pub connect_delay: Option<Duration>,
}

/// The settings for `serve` from its flags and the `MINIPAW_*`
/// environment.
///
/// # Errors
///
/// When a setting is invalid, with the message to print.
pub fn serve_config(relay: Option<&str>, max_sessions: usize) -> Result<ServeConfig, String> {
    serve_config_with(relay, max_sessions, |name| std::env::var_os(name))
}

/// [`serve_config`] with `env` standing in for the environment.
fn serve_config_with(
    relay: Option<&str>,
    max_sessions: usize,
    env: impl Fn(&str) -> Option<OsString>,
) -> Result<ServeConfig, String> {
    let mut config = Config::default();
    config.force_relay = env("MINIPAW_FORCE_RELAY").is_some();
    config.relay = relay_with(relay, &env)?;
    config.max_sessions = max_sessions;
    listener_hooks(&mut config, &env)?;
    let connect_delay = seconds(&env, "MINIPAW_TEST_CONNECT_DELAY")?;
    Ok(ServeConfig {
        config,
        connect_delay,
    })
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
/// it was created. With `new`, a fresh one replaces whatever is there.
/// Without a `path` it is the default one, whose directory is created
/// (mode 0700) if missing; an existing directory is never changed.
///
/// # Errors
///
/// When the identity cannot be loaded or created, with the message to print.
pub fn load_identity(path: Option<&Path>, new: bool) -> Result<(Identity, PathBuf, bool), String> {
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
    if new {
        let identity = Identity::generate().map_err(|e| e.to_string())?;
        identity.replace(&path).map_err(|e| e.to_string())?;
        return Ok((identity, path, true));
    }
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
    let (identity, path, created) = load_identity(identity, false)?;
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
    fn serve_and_its_defaults() {
        let args = parse(&["serve"]).expect("serve");
        let Some(Command::Serve {
            forward,
            identity,
            new,
            max_sessions,
            relay,
        }) = args.command
        else {
            panic!("not serve");
        };
        assert_eq!(forward.to_string(), "127.0.0.1:22");
        assert!(identity.is_none() && !new && relay.is_none());
        assert_eq!(max_sessions, minipaw::DEFAULT_MAX_SESSIONS);

        let argv = [
            "-v",
            "serve",
            "--forward",
            "2222",
            "--identity",
            "s.key",
            "--new",
            "--max-sessions",
            "3",
            "--relay",
            RELAY,
        ];
        let args = parse(&argv).expect("serve");
        assert!(args.verbose);
        let Some(Command::Serve {
            forward,
            identity,
            new,
            max_sessions,
            relay,
        }) = args.command
        else {
            panic!("not serve");
        };
        assert_eq!(forward.to_string(), "127.0.0.1:2222");
        assert_eq!(identity.as_deref(), Some(Path::new("s.key")));
        assert!(new);
        assert_eq!(max_sessions, 3);
        assert_eq!(relay.as_deref(), Some(RELAY));
    }

    #[test]
    fn forward_targets() {
        let ok = |s: &str| s.parse::<Forward>().expect(s);
        let fwd = |host: &str, port| Forward {
            host: host.into(),
            port,
        };
        assert_eq!(ok("22"), fwd("127.0.0.1", 22));
        assert_eq!(ok("example.com:8022"), fwd("example.com", 8022));
        assert_eq!(ok("10.0.0.5:22"), fwd("10.0.0.5", 22));
        assert_eq!(ok("[::1]:2222"), fwd("::1", 2222));
        assert_eq!(ok("[::1]:2222").to_string(), "[::1]:2222");
        for bad in [
            "",
            "host",
            "host:",
            ":22",
            "host:0",
            "host:65536",
            "0",
            "99999",
            "::1:22",
            "[::1:22",
            "[]:22",
            "host:x",
        ] {
            assert!(bad.parse::<Forward>().is_err(), "{bad:?}");
        }
        assert_eq!(
            ok("127.0.0.1:22").resolve().expect("resolve"),
            ["127.0.0.1:22".parse::<SocketAddr>().expect("addr")]
        );
        assert_eq!(
            parse(&["serve", "--forward", "nope"]).unwrap_err().kind(),
            ErrorKind::ValueValidation
        );
    }

    #[test]
    fn quiet_goes_with_sessions_and_overrides_nothing_else() {
        let args = parse(&["-q", "-v", TICKET]).expect("quiet dial");
        assert!(args.quiet && args.verbose);
        let args = parse(&[TICKET, "--quiet"]).expect("quiet dial");
        assert!(args.quiet);
        let args = parse(&["-q", "--identity", "my.key"]).expect("quiet listen");
        assert!(args.quiet);
        let args = parse(&["-q", "parse", TICKET]).expect("quiet parse");
        assert!(args.quiet);
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
        // `serve` takes no ticket or session flags, and its status lines
        // are its output, so neither it nor `ticket` goes quiet.
        for argv in [
            &[TICKET, "serve"][..],
            &["--plain", "serve"],
            &["--relay", RELAY, "serve"],
            &["--identity", "my.key", "serve"],
            &["-q", "serve"],
            &["serve", "--quiet"],
            &["-q", "ticket"],
        ] {
            assert_eq!(kind(argv), ErrorKind::ArgumentConflict, "{argv:?}");
        }
        assert_eq!(kind(&["serve", TICKET]), ErrorKind::UnknownArgument);
        assert_eq!(kind(&["serve", "--plain"]), ErrorKind::UnknownArgument);
        for n in ["0", "x", "1.5"] {
            assert_eq!(
                kind(&["serve", "--max-sessions", n]),
                ErrorKind::ValueValidation,
                "{n}"
            );
        }
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
            ("MINIPAW_TEST_RESUME_TIMEOUT", "0.5"),
        ];
        let config = config_with(&listen, env(&vars)).expect("config");
        assert!(config.force_relay);
        assert!(config.relay.is_some());
        assert_eq!(config.test_drop_link_after, Some(1000));
        assert!(config.test_drop_welcome);
        assert_eq!(config.test_resume_timeout, Some(Duration::from_millis(500)));
        assert_eq!(
            config_with(&listen, env(&[("MINIPAW_TEST_RESUME_TIMEOUT", "x")])).unwrap_err(),
            "invalid MINIPAW_TEST_RESUME_TIMEOUT 'x': expected seconds"
        );

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
    fn serve_config_from_the_environment() {
        let config = serve_config_with(None, 4, env(&[])).expect("config");
        assert_eq!(config.config.max_sessions, 4);
        assert!(config.config.relay.is_none() && !config.config.force_relay);
        assert!(config.connect_delay.is_none());

        let vars = [
            ("MINIPAW_FORCE_RELAY", ""),
            ("MINIPAW_RELAY", RELAY),
            ("MINIPAW_TEST_DROP_LINK_AFTER", "1000"),
            ("MINIPAW_TEST_DROP_WELCOME", "1"),
            ("MINIPAW_TEST_CONNECT_DELAY", "2.5"),
        ];
        let config = serve_config_with(None, 16, env(&vars)).expect("config");
        assert!(config.config.force_relay && config.config.relay.is_some());
        assert_eq!(config.config.test_drop_link_after, Some(1000));
        assert!(config.config.test_drop_welcome);
        assert_eq!(config.connect_delay, Some(Duration::from_millis(2500)));
        assert!(config.config.test_resume_timeout.is_none());
        let config = serve_config_with(None, 1, env(&[("MINIPAW_TEST_RESUME_TIMEOUT", "2")]))
            .expect("config");
        assert_eq!(
            config.config.test_resume_timeout,
            Some(Duration::from_secs(2))
        );

        let err = |vars: &[(&str, &str)]| serve_config_with(None, 1, env(vars)).unwrap_err();
        for raw in ["soon", "-1", "inf"] {
            assert_eq!(
                err(&[("MINIPAW_TEST_CONNECT_DELAY", raw)]),
                format!("invalid MINIPAW_TEST_CONNECT_DELAY '{raw}': expected seconds")
            );
        }
        assert!(
            serve_config_with(Some("nope"), 1, env(&[]))
                .unwrap_err()
                .starts_with("invalid relay address 'nope': ")
        );
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
