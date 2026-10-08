//! `minipaw ssh` and `minipaw cp`: the system's ssh and scp, reaching a
//! `minipaw serve` through a ticket.
//!
//! The ticket in the arguments is swapped for a host named after the
//! server's identity, `minipaw-<peer id>`, and ssh is told to reach that
//! host through `minipaw -q <ticket>` as its ProxyCommand. Everything else
//! goes to ssh or scp untouched, so their options, prompts and exit codes
//! are their own.
//!
//! The host's key is filed in known_hosts under that name too (ssh's
//! HostKeyAlias), and connection sharing (ControlPath `%h`, `%C`) keeps
//! each server apart, so one server is never taken for another.

use std::ffi::{OsStr, OsString};
use std::process::{Command, ExitCode};

use minipaw::Ticket;

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
/// its [`host`]. `exe` is minipaw's own path, for the ProxyCommand.
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
    let alias = format!("HostKeyAlias={}", host(&ticket));
    let mut argv: Vec<OsString> = vec!["-o".into(), proxy.into(), "-o".into(), alias.into()];
    argv.extend(rest);
    Ok(argv)
}

/// ssh's destination, its first operand, which must be `[USER@]TICKET`.
/// Option values and the remote command after it are left alone.
fn swap_destination(args: &[OsString]) -> Result<(Ticket, Vec<OsString>), String> {
    let mut out = args.to_vec();
    let at = *operands(Tool::Ssh, args, false)
        .operands
        .first()
        .ok_or("no destination; expected [USER@]TICKET")?;
    let (user, ticket) = args[at].to_str().and_then(user_ticket).ok_or_else(|| {
        format!(
            "the destination is not a ticket: {}",
            args[at].to_string_lossy()
        )
    })?;
    out[at] = format!("{user}{}", host(&ticket)).into();
    Ok((ticket, out))
}

/// scp's remote paths: every operand that is `[USER@]TICKET:PATH`. They
/// must all name the same ticket, as one ProxyCommand serves them all, and
/// no other remote path may go with them, as it would go through that
/// ProxyCommand too.
fn swap_paths(args: &[OsString]) -> Result<(Ticket, Vec<OsString>), String> {
    let mut found: Option<(Ticket, String)> = None;
    let mut out = args.to_vec();
    let parsed = operands(Tool::Cp, args, scp_permutes());
    let mut remotes = 0;
    for &at in &parsed.operands {
        let Some((user, ticket, path)) = remote_path(&args[at])? else {
            continue;
        };
        let text = ticket.to_string();
        let name = host(&ticket);
        match &found {
            Some((_, first)) if *first != text => {
                return Err("every remote path must use the same ticket".into());
            }
            Some(_) => {}
            None => found = Some((ticket, text)),
        }
        let mut arg = OsString::from(format!("{user}{name}:"));
        arg.push(path);
        out[at] = arg;
        remotes += 1;
    }
    // With -R, a copy between remote paths runs on the source machine,
    // which has no ProxyCommand and no such host; by default it goes
    // through this one.
    if remotes > 1 && parsed.flags.contains(&b'R') {
        return Err(
            "-R copies between remote paths from the source machine, which cannot reach a \
             ticket; leave out -R"
                .into(),
        );
    }
    let (ticket, _) =
        found.ok_or("no remote path among the arguments; expected [USER@]TICKET:PATH")?;
    Ok((ticket, out))
}

/// Whether scp takes options among its operands: glibc's getopt does,
/// unless $POSIXLY_CORRECT is set, even empty; BSD's and musl's do not.
fn scp_permutes() -> bool {
    cfg!(all(target_os = "linux", target_env = "gnu"))
        && std::env::var_os("POSIXLY_CORRECT").is_none()
}

/// The host ssh sees in place of `ticket`: one per server identity.
fn host(ticket: &Ticket) -> String {
    format!("minipaw-{}", ticket.peer())
}

/// A command line, as [`operands`] reads it.
#[derive(Debug, Default, PartialEq, Eq)]
struct Parsed {
    /// The positions of the operands.
    operands: Vec<usize>,
    /// The option letters given, without their values.
    flags: Vec<u8>,
}

/// Reads `args` as ssh and scp read their command lines: options may be
/// grouped (`-vp22`, `-vp 22`), and `--` ends them. They end at the first
/// operand too, unless `permute` (see [`scp_permutes`]). For ssh, only the
/// first operand, its destination, is read: what follows is the remote
/// command.
fn operands(tool: Tool, args: &[OsString], permute: bool) -> Parsed {
    // The single-letter options that take a value, from ssh(1) and scp(1).
    let with_value = match tool {
        Tool::Ssh => "BbcDEeFIiJLlmOoPpQRSWw",
        Tool::Cp => "cDFiJloPSX",
    };
    let mut found = Parsed::default();
    let mut options = true;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_encoded_bytes();
        i += 1;
        if options && arg == b"--" {
            options = false;
        } else if options && arg.len() > 1 && arg[0] == b'-' {
            // The first option that takes a value takes the rest of the
            // group, or else the next argument.
            let group = &arg[1..];
            let value = group
                .iter()
                .position(|&b| with_value.contains(char::from(b)));
            let end = value.map_or(group.len(), |k| k + 1);
            found.flags.extend_from_slice(&group[..end]);
            if value == Some(group.len() - 1) {
                i += 1;
            }
        } else {
            found.operands.push(i - 1);
            if tool == Tool::Ssh {
                break;
            }
            options &= permute;
        }
    }
    found
}

/// A remote path: its `"USER@"` (or `""`), ticket and path.
type Remote<'a> = (&'a str, Ticket, OsString);

/// Splits a remote path, `[USER@]TICKET:PATH`, into its parts, the path
/// kept as it is; `None` for a local path. As with scp, a path is local if
/// it starts with `:` or has a `/` before its first `:` (or none), and on
/// Windows if it starts with a drive (`C:`).
///
/// # Errors
///
/// A remote path that is not a ticket's, such as `host:path` or an
/// `scp://` URI.
fn remote_path(arg: &OsStr) -> Result<Option<Remote<'_>>, String> {
    let bytes = arg.as_encoded_bytes();
    let Some(colon) = bytes.iter().position(|&b| b == b':' || b == b'/') else {
        return Ok(None);
    };
    let drive = cfg!(windows) && colon == 1 && bytes[0].is_ascii_alphabetic();
    if colon == 0 || bytes[colon] == b'/' || drive {
        return Ok(None);
    }
    std::str::from_utf8(&bytes[..colon])
        .ok()
        .and_then(user_ticket)
        .and_then(|(user, ticket)| Some((user, ticket, os_string(&bytes[colon + 1..])?)))
        .map(Some)
        .ok_or_else(|| {
            format!(
                "not a ticket's remote path: {}; write [USER@]TICKET:PATH",
                arg.to_string_lossy()
            )
        })
}

/// The bytes after an ASCII prefix of an `OsStr`, back as an `OsString`.
#[cfg(unix)]
fn os_string(bytes: &[u8]) -> Option<OsString> {
    use std::os::unix::ffi::OsStrExt as _;
    Some(OsStr::from_bytes(bytes).to_owned())
}

/// The bytes after an ASCII prefix of an `OsStr`, back as an `OsString`:
/// only Unicode ones here.
#[cfg(not(unix))]
fn os_string(bytes: &[u8]) -> Option<OsString> {
    std::str::from_utf8(bytes).ok().map(OsString::from)
}

/// Splits `[USER@]TICKET` into `"USER@"` (or `""`) and the ticket.
fn user_ticket(s: &str) -> Option<(&str, Ticket)> {
    let at = s.rfind('@').map_or(0, |i| i + 1);
    let ticket = s[at..].parse().ok()?;
    Some((&s[..at], ticket))
}

/// `exe` as ssh's ProxyCommand needs it. ssh expands `%` tokens first.
/// On unix it then hands the line to the user's shell, which may be sh or
/// fish, so the path is single-quoted, and paths those shells would read
/// differently are refused.
#[cfg(unix)]
fn proxy_path(exe: &std::path::Path) -> Result<String, String> {
    let path = utf8_path(exe)?;
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

/// `exe` as ssh's ProxyCommand needs it. ssh expands `%` tokens first,
/// then on Windows starts the command line itself, so the path is
/// double-quoted.
#[cfg(not(unix))]
fn proxy_path(exe: &std::path::Path) -> Result<String, String> {
    let path = utf8_path(exe)?;
    if path.contains(['"', '\n']) {
        return Err(format!("minipaw's path has a quote or newline: {path}"));
    }
    Ok(format!("\"{}\"", path.replace('%', "%%")))
}

fn utf8_path(exe: &std::path::Path) -> Result<&str, String> {
    exe.to_str()
        .ok_or_else(|| format!("minipaw's path is not UTF-8: {}", exe.display()))
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

    /// The host that stands in for TICKET.
    fn h() -> String {
        host(&TICKET.parse().expect("ticket"))
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
        want.extend(["-p", "2222", &format!("me@{}", h()), "echo", TICKET].map(String::from));
        assert_eq!(got, want);

        let got = args(Tool::Ssh, "/m", &["-t", TICKET]).expect("args");
        assert_eq!(got[4..], ["-t".to_owned(), h()]);
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
        let want = ["-r", "a", "H:b", "u@H:/tmp/c d"].map(|a| a.replace('H', &h()));
        assert_eq!(got[4..], want);

        let got = args(Tool::Cp, "/m", &[&format!("{TICKET}:"), "."]).expect("args");
        assert_eq!(got[4..], [format!("{}:", h()), ".".into()]);
    }

    #[test]
    fn missing_or_mixed_tickets() {
        let err = args(Tool::Ssh, "/m", &["-v", "host", TICKET]).expect_err("host");
        assert!(err.contains("not a ticket: host"), "{err}");
        let err = args(Tool::Ssh, "/m", &["-v", "-l", TICKET]).expect_err("no operand");
        assert!(err.contains("no destination"), "{err}");
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
    fn option_values_are_not_operands() {
        let t = |s: &str| s.replace("{T}", TICKET).replace("{H}", &h());
        // ssh: -l, -i and -o take a value, apart or attached; -v does not.
        let argv = [
            "-l",
            "{T}",
            "-vi",
            "{T}",
            "-oUser={T}",
            "-v",
            "u@{T}",
            "{T}",
        ]
        .map(t);
        let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
        let got = args(Tool::Ssh, "/m", &argv).expect("args");
        let want = [
            "-l",
            "{T}",
            "-vi",
            "{T}",
            "-oUser={T}",
            "-v",
            "u@{H}",
            "{T}",
        ]
        .map(t);
        assert_eq!(got[4..], want);

        // scp: -i and -P take a value. Options after an operand are
        // operands too, except with glibc; -rP22 is a local path either
        // way. After `--` everything is an operand.
        let argv = ["-i", "{T}:k", "a", "-rP22", "{T}:b", "--", "-x"].map(t);
        let argv: Vec<&str> = argv.iter().map(String::as_str).collect();
        let got = args(Tool::Cp, "/m", &argv).expect("args");
        let want = ["-i", "{T}:k", "a", "-rP22", "{H}:b", "--", "-x"].map(t);
        assert_eq!(got[4..], want);

        // Whether options may follow operands depends on scp's getopt.
        let argv: Vec<OsString> = ["a", "-i", "b", "--", "-c", "d"]
            .iter()
            .map(OsString::from)
            .collect();
        assert_eq!(
            operands(Tool::Cp, &argv, false).operands,
            [0, 1, 2, 3, 4, 5]
        );
        assert_eq!(operands(Tool::Cp, &argv, true).operands, [0, 4, 5]);
        // After `--`, an operand does not bring options back.
        let argv: Vec<OsString> = ["--", "a", "-i", "b"].iter().map(OsString::from).collect();
        assert_eq!(operands(Tool::Cp, &argv, true).operands, [1, 2, 3]);
        // Option letters, but not their values.
        let argv: Vec<OsString> = ["-rRP22", "-iR", "a", "-v"]
            .iter()
            .map(OsString::from)
            .collect();
        assert_eq!(operands(Tool::Cp, &argv, true).flags, b"rRPiv");
    }

    #[test]
    fn remote_to_remote_with_r_is_refused() {
        let (a, b) = (format!("{TICKET}:a"), format!("{TICKET}:b"));
        let err = args(Tool::Cp, "/m", &["-pR", &a, &b]).expect_err("-R");
        assert!(err.contains("leave out -R"), "{err}");
        // Without -R scp goes through here; with one remote path -R is moot.
        assert!(args(Tool::Cp, "/m", &[&a, &b]).is_ok());
        assert!(args(Tool::Cp, "/m", &["-R", &a, "."]).is_ok());
    }

    #[test]
    fn a_slash_before_the_colon_is_a_local_path() {
        let local = format!("./u@{TICKET}:f");
        let got = args(Tool::Cp, "/m", &[&local, &format!("{TICKET}:g")]).expect("args");
        assert_eq!(got[4..], [local, format!("{}:g", h())]);
    }

    #[test]
    fn other_remote_paths_are_refused() {
        for other in ["host:f", "u@host:f", "scp://host/f", "[::1]:f"] {
            let err = args(Tool::Cp, "/m", &[&format!("{TICKET}:a"), other]).expect_err(other);
            assert!(err.contains("not a ticket's remote path"), "{err}");
        }
        // Local: a leading `:`, a `/` first, no `:` at all.
        let got = args(Tool::Cp, "/m", &[":a", "b/c:d", "e", &format!("{TICKET}:")]);
        assert!(got.is_ok(), "{got:?}");
    }

    #[test]
    fn each_server_has_its_own_host() {
        let other = minipaw::Identity::generate()
            .expect("identity")
            .ticket(None);
        assert_ne!(host(&other), h());
        assert!(h().starts_with("minipaw-12D3KooW"), "{}", h());
    }

    #[cfg(unix)]
    #[test]
    fn remote_paths_keep_their_bytes() {
        use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
        let mut raw = format!("{TICKET}:caf").into_bytes();
        raw.push(0xe9);
        let argv = [OsString::from_vec(raw), OsString::from(".")];
        let got = client_args(Tool::Cp, Path::new("/m"), &argv).expect("args");
        let mut want = format!("{}:caf", h()).into_bytes();
        want.push(0xe9);
        assert_eq!(got[4].as_bytes(), want);
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

    #[cfg(unix)]
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
