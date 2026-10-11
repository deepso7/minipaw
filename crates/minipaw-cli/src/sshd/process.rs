//! A login's program: spawning it, seeing it exit without reaping it, and
//! tearing it down.
//!
//! Every program leads a process group of its own (a new session with a
//! PTY, `process_group(0)` without). Its leader is not reaped until
//! teardown is over, so its pid, and with it the group id, cannot be reused
//! while the group is being signalled.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use pty_process::{OwnedReadPty, OwnedWritePty};
use russh::Sig;
use rustix::io::Errno;
use rustix::process::{Pid, Signal, WaitId, WaitIdOptions};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};

use super::config::{EXIT_POLL, GRACE};

/// The `PATH` a login gets when the server has none.
const DEFAULT_PATH: &str = "/usr/local/bin:/usr/bin:/bin";

/// Serialises PTY allocation against every child spawn in the CLI.
///
/// pty-process opens a PTY master without `CLOEXEC` and sets it a moment
/// later (atomic `CLOEXEC` is Linux-only). A child forked in between by
/// another runtime worker would inherit the master, keeping the terminal
/// from hanging up when its session ends. Only `serve --ssh` spawns
/// anything while it runs, and it spawns through here.
static SPAWN_LOCK: Mutex<()> = Mutex::new(());

fn spawn_lock() -> MutexGuard<'static, ()> {
    SPAWN_LOCK.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The account every login runs as: the serving user's, from the passwd
/// entry of the effective uid, read once at startup. Never from `$SHELL`
/// or `$HOME`.
#[derive(Clone, Debug)]
pub struct Account {
    pub name: String,
    pub uid: u32,
    pub home: PathBuf,
    pub shell: PathBuf,
}

impl Account {
    pub fn current() -> Account {
        use uzers::os::unix::UserExt as _;

        let uid = rustix::process::geteuid().as_raw();
        let Some(user) = uzers::get_user_by_uid(uid) else {
            return Account {
                name: uid.to_string(),
                uid,
                home: "/".into(),
                shell: "/bin/sh".into(),
            };
        };
        let or = |path: &Path, default: &str| {
            if path.as_os_str().is_empty() {
                PathBuf::from(default)
            } else {
                path.to_owned()
            }
        };
        Account {
            name: user.name().to_string_lossy().into_owned(),
            uid,
            home: or(user.home_dir(), "/"),
            shell: or(user.shell(), "/bin/sh"),
        }
    }

    /// The shell's name as a login shell's `argv[0]`: `-bash`.
    fn login_name(&self) -> OsString {
        let mut name = OsString::from("-");
        name.push(self.shell.file_name().unwrap_or(self.shell.as_os_str()));
        name
    }

    /// Where a login starts: home, else `/`.
    fn cwd(&self) -> &Path {
        if self.home.is_dir() {
            &self.home
        } else {
            Path::new("/")
        }
    }
}

/// A terminal size, clamped to what a PTY takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WinSize {
    pub cols: u16,
    pub rows: u16,
    pub px_width: u16,
    pub px_height: u16,
}

impl WinSize {
    /// Cells clamp to `1..=u16::MAX`, pixels to `0..=u16::MAX`.
    pub fn clamped(cols: u32, rows: u32, px_width: u32, px_height: u32) -> WinSize {
        let cells = |n: u32| n.clamp(1, u32::from(u16::MAX)) as u16;
        let pixels = |n: u32| n.min(u32::from(u16::MAX)) as u16;
        WinSize {
            cols: cells(cols),
            rows: cells(rows),
            px_width: pixels(px_width),
            px_height: pixels(px_height),
        }
    }
}

impl Default for WinSize {
    fn default() -> WinSize {
        WinSize::clamped(80, 24, 0, 0)
    }
}

impl From<WinSize> for pty_process::Size {
    fn from(size: WinSize) -> pty_process::Size {
        pty_process::Size::new_with_pixel(size.rows, size.cols, size.px_width, size.px_height)
    }
}

/// What a login asked to run.
pub struct Login<'a> {
    /// `exec`'s command, or `None` for a shell.
    pub command: Option<&'a OsStr>,
    /// With a PTY: its `TERM` and size.
    pub pty: Option<(&'a str, WinSize)>,
    /// The dialer's peer id, for `SSH_PEER_ID`.
    pub peer: &'a str,
}

/// A program's end of its I/O.
pub enum Io {
    Pty(OwnedReadPty, OwnedWritePty),
    Pipes(ChildStdin, ChildStdout, ChildStderr),
}

/// The program's environment: a fixed set, nothing from the server's but
/// `PATH`, and nothing from the client.
fn environment(account: &Account, login: &Login<'_>) -> Vec<(&'static str, OsString)> {
    let path = std::env::var_os("PATH")
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| DEFAULT_PATH.into());
    let mut env = vec![
        ("HOME", account.home.clone().into_os_string()),
        ("USER", account.name.clone().into()),
        ("LOGNAME", account.name.clone().into()),
        ("SHELL", account.shell.clone().into_os_string()),
        ("PATH", path),
        ("SSH_PEER_ID", login.peer.into()),
        ("SSH_CONNECTION", "127.0.0.1 0 127.0.0.1 0".into()),
    ];
    if let Some((term, _)) = login.pty {
        env.push(("TERM", term.into()));
    }
    env
}

/// Starts `login` as `account`, registering its leader in `registry`.
///
/// # Errors
///
/// No PTY could be had, or the shell did not start.
pub fn spawn(
    account: &Account,
    login: &Login<'_>,
    registry: &Arc<Registry>,
) -> io::Result<(Process, Io)> {
    let env = environment(account, login);
    let (child, io) = match login.pty {
        Some((_, size)) => {
            let _lock = spawn_lock();
            let (pty, pts) = pty_process::open().map_err(pty_error)?;
            pty.resize(size.into()).map_err(pty_error)?;
            let mut command = pty_process::Command::new(&account.shell)
                .env_clear()
                .envs(env)
                .current_dir(account.cwd());
            command = match login.command {
                Some(cmd) => command.arg("-c").arg(cmd),
                None => command.arg0(account.login_name()),
            };
            // The pts goes with the command, dropped as it returns.
            let child = command.spawn(pts).map_err(pty_error)?;
            let (read, write) = pty.into_split();
            (child, Io::Pty(read, write))
        }
        None => {
            let mut command = tokio::process::Command::new(&account.shell);
            command
                .env_clear()
                .envs(env)
                .current_dir(account.cwd())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .process_group(0);
            match login.command {
                Some(cmd) => command.arg("-c").arg(cmd),
                None => command.arg0(account.login_name()),
            };
            let mut child = {
                let _lock = spawn_lock();
                command.spawn()?
            };
            let (Some(stdin), Some(stdout), Some(stderr)) =
                (child.stdin.take(), child.stdout.take(), child.stderr.take())
            else {
                return Err(io::Error::other("the program has no pipes"));
            };
            (child, Io::Pipes(stdin, stdout, stderr))
        }
    };
    let pid = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(Pid::from_raw)
        .filter(|pid| pid.as_raw_nonzero().get() > 1)
        .ok_or_else(|| io::Error::other("the program has no pid"))?;
    let entry = registry.add(pid);
    let process = Process {
        child: Some(child),
        pid,
        pty: login.pty.is_some(),
        entry,
        registry: registry.clone(),
    };
    Ok((process, io))
}

fn pty_error(e: pty_process::Error) -> io::Error {
    match e {
        pty_process::Error::Io(e) => e,
        e => io::Error::other(e.to_string()),
    }
}

/// A running program. Its leader is reaped only by [`reap`](Self::reap);
/// dropped before that, it is killed and left to tokio to reap.
pub struct Process {
    child: Option<Child>,
    pid: Pid,
    pty: bool,
    entry: u64,
    registry: Arc<Registry>,
}

impl Process {
    pub fn pid(&self) -> i32 {
        self.pid.as_raw_nonzero().get()
    }

    /// Resolves once the leader has exited, without reaping it.
    pub async fn exited(&self) {
        while !self.has_exited() {
            tokio::time::sleep(EXIT_POLL).await;
        }
    }

    fn has_exited(&self) -> bool {
        let options = WaitIdOptions::EXITED | WaitIdOptions::NOWAIT | WaitIdOptions::NOHANG;
        match rustix::process::waitid(WaitId::Pid(self.pid), options) {
            Ok(status) => status.is_some(),
            Err(Errno::INTR) => false,
            // Not ours to wait for any more (ECHILD): nothing to wait on.
            Err(_) => true,
        }
    }

    /// Signals the leader's group. Safe while the leader is unreaped.
    fn signal(&self, signal: Signal) {
        #[cfg(test)]
        self.registry
            .signals
            .lock()
            .unwrap()
            .push((self.pid(), signal.as_raw()));
        let _ = rustix::process::kill_process_group(self.pid, signal);
    }

    /// Ends a torn-down program: by its group without a PTY; with one,
    /// whose master the caller has closed, after the hangup's grace.
    pub async fn end(&self) {
        if self.pty {
            self.end_session().await;
        } else {
            self.end_group().await;
        }
    }

    /// Ends a program without a PTY: SIGHUP and SIGTERM to its group,
    /// [`GRACE`] for the group to handle them, then SIGKILL to whatever is
    /// left of it. The full grace even if the leader exits early: the rest
    /// of its group may still be cleaning up.
    async fn end_group(&self) {
        self.signal(Signal::HUP);
        self.signal(Signal::TERM);
        tokio::time::sleep(GRACE).await;
        self.signal(Signal::KILL);
    }

    /// Ends a program on a PTY whose master is already closed, which hung
    /// the terminal up: [`GRACE`] for the shell's group to handle it, then
    /// SIGKILL to that group. The full grace even if the shell leaves
    /// early, since members of its group may still be handling the hangup,
    /// or ignoring it. Other job-control groups get only the hangup, as
    /// under sshd: their ids may be reused once the shell reaps them.
    async fn end_session(&self) {
        tokio::time::sleep(GRACE).await;
        self.signal(Signal::KILL);
    }

    /// Reaps the leader, its registry entry gone first, for its exit.
    pub async fn reap(mut self) -> io::Result<ExitStatus> {
        self.registry.remove(self.entry);
        let mut child = self.child.take().expect("reaped once");
        child.wait().await
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.child.is_some() {
            // Under the registry's lock, so the last resort cannot signal
            // this group at the same time.
            let mut leaders = self.registry.lock();
            leaders.remove(&self.entry);
            self.signal(Signal::KILL);
        }
    }
}

/// Every unreaped leader, for the last resort of a stop that ran out of
/// time. Signalling from it and removing an entry are serialised by its
/// lock, and an entry is removed before its leader is reaped or dropped, so
/// it never signals a stale pid.
#[derive(Default)]
pub struct Registry {
    leaders: Mutex<HashMap<u64, Pid>>,
    next: AtomicU64,
    /// Every group signalled, and with what.
    #[cfg(test)]
    pub signals: Mutex<Vec<(i32, i32)>>,
}

impl Registry {
    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Pid>> {
        self.leaders.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn add(&self, pid: Pid) -> u64 {
        let entry = self.next.fetch_add(1, Ordering::Relaxed);
        self.lock().insert(entry, pid);
        entry
    }

    fn remove(&self, entry: u64) {
        self.lock().remove(&entry);
    }

    /// SIGKILLs the group of every leader still registered, and says how
    /// many there were.
    pub fn kill_all(&self) -> usize {
        let leaders = self.lock();
        for pid in leaders.values() {
            let _ = rustix::process::kill_process_group(*pid, Signal::KILL);
        }
        leaders.len()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lock().len()
    }
}

/// How a program ended, as SSH reports it.
#[derive(Debug, PartialEq, Eq)]
pub enum Exit {
    Status(u32),
    /// The signal's SSH name, and whether it dumped core.
    Signal(&'static str, bool),
}

impl Exit {
    /// russh's name for an [`Exit::Signal`]'s signal.
    pub fn sig(name: &str) -> Sig {
        match name {
            "HUP" => Sig::HUP,
            "INT" => Sig::INT,
            "QUIT" => Sig::QUIT,
            "KILL" => Sig::KILL,
            "TERM" => Sig::TERM,
            "PIPE" => Sig::PIPE,
            "ALRM" => Sig::ALRM,
            "USR1" => Sig::USR1,
            "ABRT" => Sig::ABRT,
            "SEGV" => Sig::SEGV,
            name => Sig::Custom(name.to_owned()),
        }
    }
}

/// The SSH report for `status`. A signal without an SSH name is reported as
/// exit status 128 + its number, as shells do.
pub fn exit_of(status: ExitStatus) -> Exit {
    if let Some(code) = status.code() {
        return Exit::Status(code as u32);
    }
    match status.signal() {
        Some(n) => match signal_name(n) {
            Some(sig) => Exit::Signal(sig, status.core_dumped()),
            None => Exit::Status(128 + n as u32),
        },
        None => Exit::Status(255),
    }
}

/// The SSH name of signal `n`, for the signals RFC 4254 names.
pub fn signal_name(n: i32) -> Option<&'static str> {
    let table = [
        (Signal::HUP, "HUP"),
        (Signal::INT, "INT"),
        (Signal::QUIT, "QUIT"),
        (Signal::KILL, "KILL"),
        (Signal::TERM, "TERM"),
        (Signal::PIPE, "PIPE"),
        (Signal::ALARM, "ALRM"),
        (Signal::USR1, "USR1"),
        (Signal::USR2, "USR2"),
        (Signal::ABORT, "ABRT"),
        (Signal::SEGV, "SEGV"),
    ];
    table
        .into_iter()
        .find(|(signal, _)| signal.as_raw() == n)
        .map(|(_, sig)| sig)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_signal_table() {
        assert_eq!(signal_name(Signal::HUP.as_raw()), Some("HUP"));
        assert_eq!(signal_name(Signal::KILL.as_raw()), Some("KILL"));
        assert_eq!(signal_name(Signal::ALARM.as_raw()), Some("ALRM"));
        assert_eq!(signal_name(Signal::ABORT.as_raw()), Some("ABRT"));
        assert_eq!(signal_name(Signal::USR2.as_raw()), Some("USR2"));
        assert!(matches!(Exit::sig("USR2"), Sig::Custom(name) if name == "USR2"));
        assert!(matches!(Exit::sig("TERM"), Sig::TERM));
        assert_eq!(signal_name(Signal::WINCH.as_raw()), None);
        assert_eq!(signal_name(Signal::CHILD.as_raw()), None);

        let raw = |status: i32| ExitStatus::from_raw(status);
        assert_eq!(exit_of(raw(3 << 8)), Exit::Status(3));
        assert_eq!(
            exit_of(raw(Signal::TERM.as_raw())),
            Exit::Signal("TERM", false)
        );
        assert_eq!(
            exit_of(raw(Signal::SEGV.as_raw() | 0x80)),
            Exit::Signal("SEGV", true)
        );
        let winch = Signal::WINCH.as_raw();
        assert_eq!(exit_of(raw(winch)), Exit::Status(128 + winch as u32));
    }

    #[test]
    fn sizes_are_clamped() {
        let size = WinSize::clamped(0, 0, 0, 0);
        assert_eq!((size.cols, size.rows), (1, 1));
        let size = WinSize::clamped(u32::MAX, 70_000, u32::MAX, 65_535);
        assert_eq!(
            (size.cols, size.rows, size.px_width, size.px_height),
            (u16::MAX, u16::MAX, u16::MAX, u16::MAX)
        );
        let size = WinSize::clamped(132, 43, 10, 20);
        assert_eq!(
            (size.cols, size.rows, size.px_width, size.px_height),
            (132, 43, 10, 20)
        );
        assert_eq!(WinSize::default(), WinSize::clamped(80, 24, 0, 0));
    }

    #[test]
    fn the_account_comes_from_passwd() {
        let account = Account::current();
        assert_eq!(account.uid, rustix::process::geteuid().as_raw());
        assert!(account.shell.is_absolute() && account.home.is_absolute());
        assert!(account.login_name().to_string_lossy().starts_with('-'));
    }
}
