//! `minipaw ssh` and `minipaw cp`: the system's ssh and scp, reaching a
//! `minipaw serve` through a ticket.
//!
//! The ticket in the arguments is swapped for a placeholder host, and ssh
//! is told to reach that host through `minipaw -q <ticket>` as its
//! ProxyCommand. Everything else goes to ssh or scp untouched, so their
//! options, prompts and exit codes are their own.
//!
//! Host keys are filed in known_hosts under `minipaw-<peer id>` (ssh's
//! HostKeyAlias), so each served machine is remembered by its identity,
//! whatever name it is reached by.

use std::ffi::{OsStr, OsString};
use std::process::{Command, ExitCode};

use minipaw::Ticket;

/// The host name ssh sees in place of the ticket.
const HOST: &str = "minipaw";

/// Which client to run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tool {
    /// `ssh [OPTIONS] [USER@]TICKET [COMMAND...]`
    Ssh,
    /// `scp [OPTIONS] SOURCE... TARGET`, remote paths as
    /// `[USER@]TICKET:PATH`.
    Cp,
}

impl Tool {
    fn name(self) -> &'static str {
        match self {
            Tool::Ssh => "ssh",
            Tool::Cp => "cp",
        }
    }

    /// The program to run: ssh or scp, or $MINIPAW_SSH / $MINIPAW_SCP.
    fn program(self) -> OsString {
        let (var, default) = match self {
            Tool::Ssh => ("MINIPAW_SSH", "ssh"),
            Tool::Cp => ("MINIPAW_SCP", "scp"),
        };
        std::env::var_os(var)
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| default.into())
    }
}

/// Runs ssh or scp with `args`, as `minipaw ssh|cp` was given them. On
/// unix the client replaces this process, so its exit code is the exit
/// code; failing to start it exits 1.
pub fn run(tool: Tool, args: &[OsString]) -> ExitCode {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => return fail(tool, &format!("finding minipaw's own path: {e}")),
    };
    let argv = match client_args(tool, &exe, args) {
        Ok(argv) => argv,
        Err(e) => return fail(tool, &e),
    };
    let program = tool.program();
    let mut command = Command::new(&program);
    command.args(&argv);
    match exec(command) {
        Ok(code) => code,
        Err(e) => fail(tool, &format!("running {}: {e}", program.to_string_lossy())),
    }
}

fn fail(tool: Tool, message: &str) -> ExitCode {
    eprintln!("minipaw {}: {message}", tool.name());
    ExitCode::FAILURE
}

#[cfg(unix)]
fn exec(mut command: Command) -> std::io::Result<ExitCode> {
    use std::os::unix::process::CommandExt as _;
    // Only returns on failure.
    Err(command.exec())
}

#[cfg(not(unix))]
fn exec(mut command: Command) -> std::io::Result<ExitCode> {
    let status = command.status()?;
    let code = status.code().unwrap_or(1);
    Ok(ExitCode::from(u8::try_from(code).unwrap_or(1)))
}

/// The client's arguments: ours, then `args` with the ticket swapped for
/// [`HOST`]. `exe` is minipaw's own path, for the ProxyCommand.
fn client_args(
    tool: Tool,
    exe: &std::path::Path,
    args: &[OsString],
) -> Result<Vec<OsString>, String> {
    let (ticket, rest) = match tool {
        Tool::Ssh => swap_destination(args)?,
        Tool::Cp => swap_paths(args)?,
    };
    let proxy = format!("ProxyCommand={} -q {ticket}", proxy_path(exe)?);
    let alias = format!("HostKeyAlias={HOST}-{}", ticket.peer());
    let mut argv: Vec<OsString> = vec!["-o".into(), proxy.into(), "-o".into(), alias.into()];
    argv.extend(rest);
    Ok(argv)
}

/// ssh's destination: the first argument that is `[USER@]TICKET`. Later
/// ones are left alone, as part of the remote command.
fn swap_destination(args: &[OsString]) -> Result<(Ticket, Vec<OsString>), String> {
    let mut found = None;
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        if found.is_none()
            && let Some((user, ticket)) = arg.to_str().and_then(user_ticket)
        {
            out.push(format!("{user}{HOST}").into());
            found = Some(ticket);
            continue;
        }
        out.push(arg.clone());
    }
    let ticket = found.ok_or("no ticket among the arguments; expected [USER@]TICKET")?;
    Ok((ticket, out))
}

/// scp's remote paths: every `[USER@]TICKET:PATH`. They must all name the
/// same ticket, as one ProxyCommand serves them all.
fn swap_paths(args: &[OsString]) -> Result<(Ticket, Vec<OsString>), String> {
    let mut found: Option<(Ticket, String)> = None;
    let mut out = Vec::with_capacity(args.len());
    for arg in args {
        let remote = arg.to_str().and_then(|s| {
            let (head, path) = s.split_once(':')?;
            let (user, ticket) = user_ticket(head)?;
            Some((user, ticket, path))
        });
        let Some((user, ticket, path)) = remote else {
            out.push(arg.clone());
            continue;
        };
        let text = ticket.to_string();
        match &found {
            Some((_, first)) if *first != text => {
                return Err("every remote path must use the same ticket".into());
            }
            Some(_) => {}
            None => found = Some((ticket, text)),
        }
        out.push(format!("{user}{HOST}:{path}").into());
    }
    let (ticket, _) =
        found.ok_or("no remote path among the arguments; expected [USER@]TICKET:PATH")?;
    Ok((ticket, out))
}

/// Splits `[USER@]TICKET` into `"USER@"` (or `""`) and the ticket.
fn user_ticket(s: &str) -> Option<(&str, Ticket)> {
    let at = s.rfind('@').map_or(0, |i| i + 1);
    let ticket = s[at..].parse().ok()?;
    Some((&s[..at], ticket))
}

/// `exe` as ssh's ProxyCommand needs it. ssh expands `%` tokens and then
/// hands the line to the user's shell, which may be sh or fish, so the
/// path is single-quoted, and paths those shells would read differently
/// are refused.
fn proxy_path(exe: &std::path::Path) -> Result<String, String> {
    let path = exe
        .to_str()
        .ok_or_else(|| format!("minipaw's path is not UTF-8: {}", exe.display()))?;
    if path.contains(['\'', '\\', '\n']) {
        return Err(format!(
            "minipaw's path has a quote, backslash or newline: {path}"
        ));
    }
    let plain = |c: char| c.is_ascii_alphanumeric() || "/._-+,=".contains(c);
    let path = path.replace('%', "%%");
    if path.chars().all(plain) {
        Ok(path)
    } else {
        Ok(format!("'{path}'"))
    }
}

/// `minipaw ssh|cp ARGS...` as a tool and its arguments, taken from argv
/// before clap sees it: clap would claim flags such as `-v` and `-q` as
/// minipaw's own, where they are ssh's. No arguments, or a help flag on
/// its own, stay with clap.
pub fn split_argv(argv: &[OsString]) -> Option<(Tool, &[OsString])> {
    let tool = match argv.get(1)?.to_str()? {
        "ssh" => Tool::Ssh,
        "cp" => Tool::Cp,
        _ => return None,
    };
    let args = &argv[2..];
    let help = |a: &OsString| a == OsStr::new("-h") || a == OsStr::new("--help");
    match args {
        [] => None,
        [one] if help(one) => None,
        _ => Some((tool, args)),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    const TICKET: &str =
        "mpAQDrKEuiH1EeUzlyYzX5zShnJgAkCAESIJXT0qGmgiYC5QMCzvVNIDiMmFduWCFl_Yuh1DfJ1h8B";

    fn args(tool: Tool, exe: &str, argv: &[&str]) -> Result<Vec<String>, String> {
        let argv: Vec<OsString> = argv.iter().map(OsString::from).collect();
        client_args(tool, Path::new(exe), &argv).map(|v| {
            v.into_iter()
                .map(|a| a.into_string().expect("utf-8"))
                .collect()
        })
    }

    fn ours(exe: &str) -> Vec<String> {
        let ticket: Ticket = TICKET.parse().expect("ticket");
        vec![
            "-o".into(),
            format!("ProxyCommand={exe} -q {TICKET}"),
            "-o".into(),
            format!("HostKeyAlias=minipaw-{}", ticket.peer()),
        ]
    }

    #[test]
    fn ssh_swaps_the_destination_only() {
        let got = args(
            Tool::Ssh,
            "/usr/bin/minipaw",
            &["-p", "2222", &format!("me@{TICKET}"), "echo", TICKET],
        )
        .expect("args");
        let mut want = ours("/usr/bin/minipaw");
        want.extend(["-p", "2222", "me@minipaw", "echo", TICKET].map(String::from));
        assert_eq!(got, want);

        let got = args(Tool::Ssh, "/m", &["-t", TICKET]).expect("args");
        assert_eq!(got[4..], ["-t", "minipaw"]);
    }

    #[test]
    fn cp_swaps_every_remote_path() {
        let got = args(
            Tool::Cp,
            "/m",
            &[
                "-r",
                "a",
                &format!("{TICKET}:b"),
                &format!("u@{TICKET}:/tmp/c d"),
            ],
        )
        .expect("args");
        assert_eq!(got[4..], ["-r", "a", "minipaw:b", "u@minipaw:/tmp/c d"]);

        let got = args(Tool::Cp, "/m", &[&format!("{TICKET}:"), "."]).expect("args");
        assert_eq!(got[4..], ["minipaw:", "."]);
    }

    #[test]
    fn missing_or_mixed_tickets() {
        let err = args(Tool::Ssh, "/m", &["-v", "host"]).expect_err("no ticket");
        assert!(err.contains("no ticket"), "{err}");
        let err = args(Tool::Cp, "/m", &["a", TICKET]).expect_err("no remote path");
        assert!(err.contains("no remote path"), "{err}");

        let other = minipaw::Identity::generate()
            .expect("identity")
            .ticket(None)
            .to_string();
        let err = args(
            Tool::Cp,
            "/m",
            &[&format!("{TICKET}:a"), &format!("{other}:b")],
        )
        .expect_err("two tickets");
        assert!(err.contains("same ticket"), "{err}");
    }

    #[test]
    fn ssh_and_cp_take_their_arguments_raw() {
        let argv = |a: &[&str]| -> Vec<OsString> { a.iter().map(OsString::from).collect() };
        let v = argv(&["minipaw", "ssh", "-v", "-q", TICKET]);
        let (tool, args) = split_argv(&v).expect("ssh");
        assert_eq!((tool, args), (Tool::Ssh, &v[2..]));
        let v = argv(&["minipaw", "cp", "-r", "a", "b"]);
        assert_eq!(split_argv(&v).map(|(t, _)| t), Some(Tool::Cp));
        // Left to clap: help, no arguments, other commands, flags first.
        for a in [
            &["minipaw", "ssh"][..],
            &["minipaw", "ssh", "--help"],
            &["minipaw", "cp", "-h"],
            &["minipaw", "-v", "ssh", TICKET],
            &["minipaw", "serve"],
            &["minipaw"],
        ] {
            assert!(split_argv(&argv(a)).is_none(), "{a:?}");
        }
    }

    #[test]
    fn the_proxy_command_quotes_minipaw_s_path() {
        let proxy = |exe: &str| proxy_path(Path::new(exe));
        assert_eq!(proxy("/opt/bin/minipaw").as_deref(), Ok("/opt/bin/minipaw"));
        assert_eq!(
            proxy("/Users/a b/minipaw").as_deref(),
            Ok("'/Users/a b/minipaw'")
        );
        assert_eq!(proxy("/x/100%/m").as_deref(), Ok("'/x/100%%/m'"));
        assert!(proxy("/x/it's/m").is_err());
        assert!(proxy("/x/a\\b/m").is_err());
    }
}
