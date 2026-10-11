//! One connection's russh handler: auth, and its session channel's state
//! machine and input buffer, then the channel's supervisor, which runs the
//! program (a process, or the SFTP loop) and sends its output and exit.
//!
//! A connection has at most one session channel, so russh's per-connection
//! stalls (docs/plans/builtin-ssh.md, "Flow control") have no sibling
//! channel to hold up. Input arrives only through the `data` callback,
//! which buffers it, up to [`INPUT_CAP`], and never waits on the program.
//! Output goes through the channel's window-aware writers. Every send the
//! supervisor makes can be cut short by teardown, because a russh session
//! with data still pending for a closed channel stops taking any.

use std::collections::VecDeque;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use pty_process::{OwnedReadPty, OwnedWritePty};
use russh::keys::{HashAlg, PublicKey};
use russh::server::{self, Auth, ChannelOpenHandle, Msg, Session};
use russh::{Channel, ChannelId, ChannelOpenFailure, ChannelWriteHalf, Pty, Sig};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::process::ChildStdin;
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::Shared;
use super::config::{CAP_MESSAGE, CHUNK, INPUT_CAP};
use super::process::{self, Exit, Io, Login, Process, WinSize};
use super::sftp;

/// How long a PTY whose shell has exited may stay quiet before its output
/// counts as drained, when something else still holds the terminal open.
const PTY_IDLE: Duration = Duration::from_millis(250);

/// How long the cap's message has to reach the client.
const CAP_MESSAGE_TIMEOUT: Duration = Duration::from_secs(2);

/// The `TERM` used when the client's is missing or odd.
const DEFAULT_TERM: &str = "xterm-256color";

/// The client's `TERM` if it is a plausible name (`[A-Za-z0-9._+-]`, at
/// most 64 bytes), else [`DEFAULT_TERM`].
pub fn term(requested: &str) -> String {
    let ok = !requested.is_empty()
        && requested.len() <= 64
        && requested
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b));
    if ok { requested } else { DEFAULT_TERM }.to_owned()
}

/// A channel's input on its way to the program, filled only by the `data`
/// callback and drained by the supervisor's writer.
#[derive(Default)]
pub struct Input {
    state: Mutex<InputState>,
    notify: Notify,
}

#[derive(Default)]
struct InputState {
    queue: VecDeque<Vec<u8>>,
    /// Bytes queued.
    len: usize,
    /// The client sent EOF: the writer ends once the queue is empty.
    eof: bool,
    /// Nothing more goes to the program: it stopped reading, or the
    /// channel is going away. Later data is dropped.
    closed: bool,
}

impl Input {
    fn lock(&self) -> MutexGuard<'_, InputState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `data`; `false` if that would pass [`INPUT_CAP`], which
    /// closes the input.
    pub fn push(&self, data: &[u8]) -> bool {
        let mut state = self.lock();
        if state.closed {
            return true;
        }
        if data.is_empty() {
            return true;
        }
        if state.len + data.len() > INPUT_CAP {
            drop(state);
            self.close();
            return false;
        }
        // Small packets join the last piece, so the cap bounds memory and
        // not just the bytes counted.
        match state.queue.back_mut() {
            Some(last) if last.len() + data.len() <= CHUNK => last.extend_from_slice(data),
            _ => state.queue.push_back(data.to_vec()),
        }
        state.len += data.len();
        drop(state);
        self.notify.notify_one();
        true
    }

    pub fn eof(&self) {
        self.lock().eof = true;
        self.notify.notify_one();
    }

    /// Whether the input was closed, rather than ended by the client.
    pub fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Drops what is queued and anything that comes later.
    pub fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        state.queue.clear();
        state.len = 0;
        drop(state);
        self.notify.notify_one();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.lock().len
    }

    /// The next piece of input; `None` at EOF or once closed.
    pub async fn pop(&self) -> Option<Vec<u8>> {
        loop {
            {
                let mut state = self.lock();
                if state.closed {
                    return None;
                }
                if let Some(data) = state.queue.pop_front() {
                    state.len -= data.len();
                    return Some(data);
                }
                if state.eof {
                    return None;
                }
            }
            self.notify.notified().await;
        }
    }
}

/// Why a running channel is being torn down.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    /// The client closed it.
    Closed,
    /// Its input passed the cap.
    Cap,
    /// The client broke the protocol (data after EOF).
    Violation,
}

/// The connection's handler. russh drops it when the connection ends,
/// which tears down its channel.
pub struct Conn {
    shared: Arc<Shared>,
    /// The dialer's peer id.
    peer: String,
    /// How log lines name this connection.
    label: String,
    /// Set once auth succeeds, which disarms the pre-auth deadline.
    authed: watch::Sender<bool>,
    /// Cancelled when the connection ends or the server stops.
    token: CancellationToken,
    /// The channel's supervisor.
    tasks: TaskTracker,
    /// Whether a session channel was ever opened: there is one at most.
    opened: bool,
    chan: Option<Chan>,
}

/// The session channel's state.
struct Chan {
    id: ChannelId,
    state: State,
    input: Arc<Input>,
}

enum State {
    /// Before `shell` or `exec`.
    Configuring {
        /// The PTY's `TERM`, once requested.
        term: Option<String>,
        size: WinSize,
        eof: bool,
        /// Held until the supervisor takes it.
        write: ChannelWriteHalf<Msg>,
        handle: server::Handle,
    },
    /// The program runs under its supervisor.
    Running {
        pty: bool,
        eof: bool,
        resize: watch::Sender<WinSize>,
        /// Set by the supervisor once the exit sequence sent `close`.
        finished: Arc<AtomicBool>,
        reason: Arc<OnceLock<Reason>>,
        teardown: CancellationToken,
    },
    Finished,
}

impl Chan {
    /// Settles `Running` into `Finished` once the supervisor says so.
    fn state(&mut self) -> &mut State {
        if let State::Running { finished, .. } = &self.state
            && finished.load(Ordering::Acquire)
        {
            self.state = State::Finished;
        }
        &mut self.state
    }

    /// Tears the running program down for `why`.
    fn tear_down(&self, why: Reason) {
        if let State::Running {
            reason, teardown, ..
        } = &self.state
        {
            let _ = reason.set(why);
            teardown.cancel();
        }
    }
}

impl Conn {
    pub fn new(
        shared: Arc<Shared>,
        peer: String,
        label: String,
        authed: watch::Sender<bool>,
        token: CancellationToken,
        tasks: TaskTracker,
    ) -> Conn {
        Conn {
            shared,
            peer,
            label,
            authed,
            token,
            tasks,
            opened: false,
            chan: None,
        }
    }

    fn chan(&mut self, id: ChannelId) -> Option<&mut Chan> {
        self.chan.as_mut().filter(|c| c.id == id)
    }

    fn logged_in(&self, user: &str, how: &str) {
        let _ = self.authed.send(true);
        log::info!(
            "ssh {}: logged in as {} with {how} (asked for user {user:?})",
            self.label,
            self.shared.account.name
        );
    }

    /// Starts the channel's program: a shell, a command, or SFTP.
    fn start(
        &mut self,
        id: ChannelId,
        run: Run<'_>,
        session: &mut Session,
    ) -> Result<(), russh::Error> {
        let shared = self.shared.clone();
        let label = self.label.clone();
        let peer = self.peer.clone();
        let stopping = self.token.is_cancelled();
        let token = self.token.clone();
        let tasks = self.tasks.clone();
        let Some(chan) = self.chan(id) else {
            return session.channel_failure(id);
        };
        let input = chan.input.clone();
        let state = std::mem::replace(&mut chan.state, State::Finished);
        let State::Configuring {
            term,
            size,
            eof,
            write,
            handle,
        } = state
        else {
            chan.state = state;
            return session.channel_failure(id);
        };
        let what = describe(run, term.is_some());
        let started = if stopping {
            Err(std::io::Error::other("the server is stopping"))
        } else {
            match run {
                // Home as configured, not checked here: a stat could hang
                // on a stuck filesystem, and absolute paths work anyway.
                Run::Sftp => Ok(Program::Sftp(sftp::Fs::new(
                    shared.account.home.clone(),
                    sftp::Jobs::new(shared.sftp_jobs.clone()),
                ))),
                Run::Exec(cmd) if cmd.as_bytes().contains(&0) => {
                    Err(std::io::Error::other("the command has a NUL byte"))
                }
                Run::Shell | Run::Exec(_) => {
                    let login = Login {
                        command: match run {
                            Run::Exec(cmd) => Some(cmd),
                            _ => None,
                        },
                        pty: term.as_deref().map(|t| (t, size)),
                        peer: &peer,
                    };
                    process::spawn(&shared.account, &login, &shared.registry)
                        .map(|(process, io)| Program::Process(process, io))
                }
            }
        };
        let program = match started {
            Ok(program) => program,
            Err(e) => {
                log::info!("ssh {label}: could not run {what}: {e}");
                input.close();
                session.channel_failure(id)?;
                return session.close(id);
            }
        };
        match &program {
            Program::Process(process, _) => {
                log::info!("ssh {label}: running {what} (pid {})", process.pid());
            }
            Program::Sftp(_) => log::info!("ssh {label}: running {what}"),
        }
        session.channel_success(id)?;
        // SFTP ignores a PTY it was given: its bytes go to it as they are,
        // and its input can reach EOF.
        let pty = term.is_some() && !matches!(run, Run::Sftp);
        if eof && !pty {
            input.eof();
        }
        let (resize, resized) = watch::channel(size);
        let finished = Arc::new(AtomicBool::new(false));
        let reason = Arc::new(OnceLock::new());
        let teardown = token.child_token();
        chan.state = State::Running {
            pty,
            eof,
            resize,
            finished: finished.clone(),
            reason: reason.clone(),
            teardown: teardown.clone(),
        };
        let supervisor = Supervisor {
            id,
            write,
            handle,
            input,
            resized,
            finished,
            reason,
            teardown,
            connection: token,
            label,
            what,
            #[cfg(test)]
            _live: Live::new(&shared.supervisors),
        };
        tasks.spawn(supervisor.run(program));
        Ok(())
    }
}

/// What a channel was asked to run.
#[derive(Clone, Copy)]
enum Run<'a> {
    Shell,
    Exec(&'a OsStr),
    Sftp,
}

/// How log lines describe what ran.
fn describe(run: Run<'_>, pty: bool) -> String {
    let with = if pty { " with a PTY" } else { "" };
    match run {
        Run::Shell => format!("a shell{with}"),
        Run::Exec(cmd) => {
            let cmd = cmd.to_string_lossy();
            let cmd: String = cmd.chars().take(200).collect();
            format!("{cmd:?}{with}")
        }
        Run::Sftp => "sftp".to_owned(),
    }
}

impl server::Handler for Conn {
    type Error = russh::Error;

    async fn auth_none(&mut self, user: &str) -> Result<Auth, Self::Error> {
        if self.shared.policy.accepts_none() {
            self.logged_in(user, "no auth (--no-auth)");
            return Ok(Auth::Accept);
        }
        Ok(Auth::reject())
    }

    async fn auth_publickey_offered(
        &mut self,
        user: &str,
        key: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        if self.shared.policy.accepts(key) {
            return Ok(Auth::Accept);
        }
        log::info!(
            "ssh {}: refused key {} (asked for user {user:?})",
            self.label,
            key.fingerprint(HashAlg::Sha256)
        );
        Ok(Auth::reject())
    }

    async fn auth_publickey(&mut self, user: &str, key: &PublicKey) -> Result<Auth, Self::Error> {
        // Checked again: a client may sign without offering first.
        if !self.shared.policy.accepts(key) {
            log::info!(
                "ssh {}: refused key {} (asked for user {user:?})",
                self.label,
                key.fingerprint(HashAlg::Sha256)
            );
            return Ok(Auth::reject());
        }
        let how = format!("key {}", key.fingerprint(HashAlg::Sha256));
        self.logged_in(user, &how);
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if self.opened || self.token.is_cancelled() {
            log::debug!("ssh {}: refused a second session channel", self.label);
            reply
                .reject(ChannelOpenFailure::AdministrativelyProhibited)
                .await;
            return Ok(());
        }
        self.opened = true;
        reply.accept().await;
        let id = channel.id();
        // Input comes only through `data`; russh's sends to the dropped
        // read half fail at once.
        let (_read, write) = channel.split();
        self.chan = Some(Chan {
            id,
            state: State::Configuring {
                term: None,
                size: WinSize::default(),
                eof: false,
                write,
                handle: session.handle(),
            },
            input: Arc::default(),
        });
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        requested: &str,
        cols: u32,
        rows: u32,
        px_width: u32,
        px_height: u32,
        _modes: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(chan) = self.chan(channel)
            && let State::Configuring {
                term: t @ None,
                size,
                ..
            } = chan.state()
        {
            *t = Some(term(requested));
            *size = WinSize::clamped(cols, rows, px_width, px_height);
            return session.channel_success(channel);
        }
        session.channel_failure(channel)
    }

    async fn x11_request(
        &mut self,
        channel: ChannelId,
        _single_connection: bool,
        _protocol: &str,
        _cookie: &str,
        _screen: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)
    }

    async fn env_request(
        &mut self,
        channel: ChannelId,
        _name: &str,
        _value: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.start(channel, Run::Shell, session)
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.start(channel, Run::Exec(OsStr::from_bytes(data)), session)
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name == "sftp" {
            return self.start(channel, Run::Sftp, session);
        }
        log::info!("ssh {}: refused subsystem {name:?}", self.label);
        session.channel_failure(channel)
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        cols: u32,
        rows: u32,
        px_width: u32,
        px_height: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let new = WinSize::clamped(cols, rows, px_width, px_height);
        if let Some(chan) = self.chan(channel) {
            match chan.state() {
                State::Configuring { size, .. } => *size = new,
                State::Running { resize, .. } => {
                    resize.send_replace(new);
                }
                State::Finished => {}
            }
        }
        session.channel_success(channel)
    }

    async fn signal(
        &mut self,
        channel: ChannelId,
        _signal: Sig,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let label = self.label.clone();
        let Some(chan) = self.chan(channel) else {
            return Ok(());
        };
        let eof = match chan.state() {
            State::Configuring { eof, .. } | State::Running { eof, .. } => *eof,
            State::Finished => return Ok(()),
        };
        if eof {
            log::info!("ssh {label}: data after EOF; closing the channel");
            return close(chan, Reason::Violation, session);
        }
        if !chan.input.push(data) {
            log::info!(
                "ssh {label}: input passed the {} MiB buffer; closing the channel",
                INPUT_CAP >> 20
            );
            return close(chan, Reason::Cap, session);
        }
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(chan) = self.chan(channel) {
            let input = chan.input.clone();
            match chan.state() {
                State::Configuring { eof, .. } => *eof = true,
                State::Running { eof, pty, .. } => {
                    *eof = true;
                    // A PTY cannot be half-closed.
                    if !*pty {
                        input.eof();
                    }
                }
                State::Finished => {}
            }
        }
        Ok(())
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        // russh has already replied. Our state goes when the supervisor
        // ends; a later close of ours never gets here (russh issue C).
        if let Some(chan) = self.chan(channel) {
            if matches!(chan.state(), State::Running { .. }) {
                chan.tear_down(Reason::Closed);
            } else {
                chan.input.close();
                self.chan = None;
            }
        }
        Ok(())
    }
}

/// Closes the channel for `why` from a callback: before the program runs,
/// at once; after, through its supervisor.
fn close(chan: &mut Chan, why: Reason, session: &mut Session) -> Result<(), russh::Error> {
    chan.input.close();
    match chan.state() {
        State::Configuring { .. } => {
            chan.state = State::Finished;
            if why == Reason::Cap {
                session.extended_data(chan.id, 1, CAP_MESSAGE.as_bytes().to_vec())?;
            }
            session.close(chan.id)
        }
        State::Running { .. } => {
            chan.tear_down(why);
            Ok(())
        }
        State::Finished => Ok(()),
    }
}

/// What a channel's supervisor runs.
enum Program {
    Process(Process, Io),
    Sftp(sftp::Fs),
}

/// Runs a channel's program and reports its end to the client.
struct Supervisor {
    id: ChannelId,
    write: ChannelWriteHalf<Msg>,
    handle: server::Handle,
    input: Arc<Input>,
    resized: watch::Receiver<WinSize>,
    finished: Arc<AtomicBool>,
    reason: Arc<OnceLock<Reason>>,
    /// Cancelled to tear the program down: the channel closing, its cap,
    /// the connection ending, or the server stopping.
    teardown: CancellationToken,
    /// Cancelled when the connection ends or the server stops.
    connection: CancellationToken,
    label: String,
    what: String,
    #[cfg(test)]
    _live: Live,
}

/// Counts a running supervisor, for tests, until it is dropped.
#[cfg(test)]
struct Live(Arc<std::sync::atomic::AtomicUsize>);

#[cfg(test)]
impl Live {
    fn new(count: &Arc<std::sync::atomic::AtomicUsize>) -> Live {
        count.fetch_add(1, Ordering::SeqCst);
        Live(count.clone())
    }
}

#[cfg(test)]
impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Supervisor {
    async fn run(self, program: Program) {
        match program {
            Program::Process(process, io) => self.process(process, io).await,
            Program::Sftp(fs) => self.sftp(fs).await,
        }
        self.finished.store(true, Ordering::Release);
    }

    /// Runs a process until it exits, then reaps it and reports its end;
    /// or, torn down, ends it.
    async fn process(&self, process: Process, io: Io) {
        let exited = match io {
            Io::Pipes(stdin, stdout, stderr) => self.pipes(&process, stdin, stdout, stderr).await,
            Io::Pty(read, write) => self.pty(&process, read, write).await,
        };
        self.input.close();
        if exited {
            let exit = match process.reap().await {
                Ok(status) => process::exit_of(status),
                Err(_) => Exit::Status(255),
            };
            log::info!("ssh {}: {} ended: {exit:?}", self.label, self.what);
            if !self.report(exit).await {
                let why = self.ending().await;
                self.close_after(why).await;
            }
        } else {
            let why = self.ending().await;
            process.end().await;
            let _ = process.reap().await;
            self.close_after(why).await;
        }
    }

    /// Serves SFTP until the client's EOF or the first framing or transport
    /// error, then reports the end as sftp-server's exit would (status 0,
    /// or 1 after an error); or, torn down, stops at once.
    async fn sftp(&self, mut fs: sftp::Fs) {
        let served = async {
            let input = sftp::InputReader::new(self.input.clone());
            sftp::serve(&mut fs, input, &mut self.write.make_writer()).await
        };
        let served = tokio::select! {
            served = served => Some(served),
            () = self.teardown.cancelled() => None,
        };
        // Its input closed under it (the cap, or data after EOF), which
        // reads as EOF: the teardown that follows every such close decides.
        let served = match served {
            Some(_) if self.input.is_closed() => {
                self.teardown.cancelled().await;
                None
            }
            served => served,
        };
        // Its files are closed in the background: close(2) may hang.
        fs.close_all();
        self.input.close();
        match served {
            Some(served) => {
                let exit = match served {
                    Ok(()) => Exit::Status(0),
                    Err(e) => {
                        log::info!("ssh {}: sftp: {e}", self.label);
                        Exit::Status(1)
                    }
                };
                log::info!("ssh {}: {} ended: {exit:?}", self.label, self.what);
                if !self.report(exit).await {
                    let why = self.ending().await;
                    self.close_after(why).await;
                }
            }
            None => {
                let why = self.ending().await;
                self.close_after(why).await;
            }
        }
    }

    /// The normal end: exit status, EOF and close, each cut short by a
    /// teardown; `false` if one was, so the caller finishes as a teardown
    /// does, closing the channel if the server tore it down.
    async fn report(&self, exit: Exit) -> bool {
        let report = async {
            let sent = match exit {
                Exit::Status(code) => self.handle.exit_status_request(self.id, code).await,
                Exit::Signal(name, core) => {
                    let sig = Exit::sig(name);
                    self.handle
                        .exit_signal_request(self.id, sig, core, String::new(), String::new())
                        .await
                }
            };
            if sent.is_ok() && self.write.eof().await.is_ok() {
                let _ = self.write.close().await;
            }
        };
        tokio::select! {
            () = report => true,
            () = self.teardown.cancelled() => false,
        }
    }

    /// Logs why the program is being torn down and, if it is for the cap,
    /// sends the cap's message first, while the client still reads the
    /// channel.
    async fn ending(&self) -> Option<Reason> {
        let why = self.reason.get().copied();
        log::info!(
            "ssh {}: ending {} ({})",
            self.label,
            self.what,
            match why {
                Some(Reason::Closed) => "the client closed the channel",
                Some(Reason::Cap) => "too much input",
                Some(Reason::Violation) => "data after EOF",
                None => "the connection ended",
            }
        );
        let say = async {
            if why == Some(Reason::Cap) {
                let mut stderr = self.write.make_writer_ext(Some(1));
                let _ = stderr.write_all(CAP_MESSAGE.as_bytes()).await;
            }
        };
        bounded(&self.connection, CAP_MESSAGE_TIMEOUT, say).await;
        why
    }

    /// Closes the channel after a teardown the server started; the client's
    /// own close was answered already.
    async fn close_after(&self, why: Option<Reason>) {
        if matches!(why, Some(Reason::Cap | Reason::Violation)) {
            bounded(&self.connection, CAP_MESSAGE_TIMEOUT, async {
                let _ = self.write.close().await;
            })
            .await;
        }
    }

    /// Runs a program without a PTY until it exits and its stdout and
    /// stderr reach EOF (`true`), or until teardown (`false`).
    async fn pipes(
        &self,
        process: &Process,
        stdin: ChildStdin,
        stdout: tokio::process::ChildStdout,
        stderr: tokio::process::ChildStderr,
    ) -> bool {
        let done = async {
            tokio::join!(
                pump(stdout, self.write.make_writer()),
                pump(stderr, self.write.make_writer_ext(Some(1))),
                process.exited(),
            );
        };
        let feeding = async {
            feed(stdin, &self.input).await;
            std::future::pending::<()>().await;
        };
        tokio::select! {
            () = done => true,
            () = feeding => unreachable!("feeding never ends"),
            () = self.teardown.cancelled() => false,
        }
    }

    /// Runs a program on a PTY until it exits and the terminal is drained
    /// (`true`), or until teardown (`false`). The PTY's halves are dropped
    /// on return, which closes its only master descriptor.
    async fn pty(&self, process: &Process, read: OwnedReadPty, write: OwnedWritePty) -> bool {
        let done = async {
            pump_pty(read, self.write.make_writer(), process).await;
            process.exited().await;
        };
        let feeding = async {
            feed_pty(write, &self.input, self.resized.clone()).await;
            std::future::pending::<()>().await;
        };
        tokio::select! {
            () = done => true,
            () = feeding => unreachable!("feeding never ends"),
            () = self.teardown.cancelled() => false,
        }
    }
}

/// Runs `fut` for up to `limit`, or until `token` is cancelled.
async fn bounded(token: &CancellationToken, limit: Duration, fut: impl Future<Output = ()>) {
    tokio::select! {
        _ = tokio::time::timeout(limit, fut) => {}
        () = token.cancelled() => {}
    }
}

/// Copies a program's output into the channel, at most [`CHUNK`] at a
/// time, until EOF or a failed write.
async fn pump(mut from: impl AsyncRead + Unpin, mut to: impl AsyncWrite + Unpin) {
    let mut buf = vec![0; CHUNK];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// Copies a PTY's output into the channel until the terminal hangs up
/// (EIO, or EOF on some systems), or until the shell has exited and the
/// terminal has been quiet for [`PTY_IDLE`]: a background job may still
/// hold it open, and sshd does not wait for those either. Waiting on the
/// client's window is not quiet: no output is lost to it.
async fn pump_pty(mut from: OwnedReadPty, mut to: impl AsyncWrite + Unpin, process: &Process) {
    let mut buf = vec![0; CHUNK];
    let mut exited = false;
    loop {
        let n = tokio::select! {
            read = from.read(&mut buf) => match read {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            },
            () = process.exited(), if !exited => {
                exited = true;
                continue;
            }
            () = tokio::time::sleep(PTY_IDLE), if exited => return,
        };
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// Feeds the input to the program's stdin, closing it at EOF.
async fn feed(mut stdin: ChildStdin, input: &Input) {
    while let Some(data) = input.pop().await {
        if stdin.write_all(&data).await.is_err() {
            // It stopped reading: drop the rest.
            input.close();
            return;
        }
    }
}

/// Feeds the input to the PTY and applies the client's resizes, even while
/// a write waits on a program that is not reading.
async fn feed_pty(mut pty: OwnedWritePty, input: &Input, mut resized: watch::Receiver<WinSize>) {
    loop {
        tokio::select! {
            data = input.pop() => {
                let Some(data) = data else {
                    break;
                };
                let mut sent = 0;
                while sent < data.len() {
                    tokio::select! {
                        wrote = pty.write(&data[sent..]) => match wrote {
                            Ok(0) | Err(_) => {
                                input.close();
                                break;
                            }
                            Ok(n) => sent += n,
                        },
                        Ok(()) = resized.changed() => {
                            let _ = pty.resize((*resized.borrow_and_update()).into());
                        }
                    }
                }
            }
            Ok(()) = resized.changed() => {
                let _ = pty.resize((*resized.borrow_and_update()).into());
            }
        }
    }
    // No more input; resizes still apply.
    while resized.changed().await.is_ok() {
        let _ = pty.resize((*resized.borrow_and_update()).into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_names() {
        for ok in [
            "xterm-256color",
            "screen.xterm",
            "vt100",
            "rxvt-unicode+x",
            "A_b-9",
        ] {
            assert_eq!(term(ok), ok);
        }
        let long = "x".repeat(65);
        for bad in [
            "",
            "xterm 256",
            "a/b",
            "x\u{e9}",
            "$(id)",
            "a\nb",
            long.as_str(),
        ] {
            assert_eq!(term(bad), DEFAULT_TERM, "{bad:?}");
        }
        assert_eq!(term(&"x".repeat(64)), "x".repeat(64));
    }

    #[test]
    fn the_input_cap() {
        let input = Input::default();
        let chunk = vec![0; 1024 * 1024];
        for _ in 0..8 {
            assert!(input.push(&chunk));
        }
        assert_eq!(input.len(), INPUT_CAP);
        assert!(!input.push(&[0]));
        // Closed: later data is dropped, quietly.
        assert_eq!(input.len(), 0);
        assert!(input.push(&chunk));
        assert_eq!(input.len(), 0);
    }

    /// Empty and tiny packets cost no more than the bytes they carry: they
    /// add no pieces of their own, so the cap bounds memory.
    #[test]
    fn small_input_is_coalesced() {
        let input = Input::default();
        for _ in 0..1000 {
            assert!(input.push(&[]));
        }
        assert_eq!(input.lock().queue.len(), 0);
        for _ in 0..CHUNK + 1 {
            assert!(input.push(&[1]));
        }
        assert_eq!(input.len(), CHUNK + 1);
        let queue = &input.lock().queue;
        assert_eq!(queue.iter().map(Vec::len).collect::<Vec<_>>(), [CHUNK, 1]);
    }
}
