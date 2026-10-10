//! In-process tests: russh's client against the server over socket pairs,
//! with no network. Every wait is bounded, so a hang fails the test rather
//! than the CI job.
//!
//! Two russh endpoints on one socket pair deadlock on full-duplex bulk data
//! (neither reads while its write is blocked), so bulk tests put a relay
//! that always keeps reading between them, as OpenSSH and minipaw's `Io`
//! do. A client close is checked on the server's side: russh's client
//! never tells its own read half.

use std::net::Shutdown;
use std::os::unix::net::UnixStream as StdStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use russh::client::{self, Handle};
use russh::keys::agent::AgentIdentity;
use russh::keys::{HashAlg, PrivateKey, PrivateKeyWithHashAlg, PublicKeyOrCertificate};
use russh::{ChannelMsg, ChannelReadHalf, ChannelWriteHalf};
use rustix::process::{Pid, Signal};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::UnixStream;
use tokio::time::{Instant, sleep, timeout};

use super::*;

/// The most any test may take.
const LIMIT: Duration = Duration::from_secs(60);

const MIB: usize = 1024 * 1024;

async fn bounded<T>(test: impl Future<Output = T>) -> T {
    timeout(LIMIT, test).await.expect("the test timed out")
}

/// Test keys: 1 is authorized, others are not. 7 is the host's.
fn key(n: u8) -> PrivateKey {
    config::host_key(&[n; 32])
}

/// The serving account, with `/bin/sh` for the scripts below.
fn account() -> Account {
    Account {
        shell: "/bin/sh".into(),
        ..Account::current()
    }
}

fn keys() -> Policy {
    Policy::Keys(vec![key(1).public_key().key_data().clone()])
}

struct Client;

impl client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(matches!(key, PublicKeyOrCertificate::PublicKey { key: k, .. } if k == key7()))
    }
}

fn key7() -> &'static russh::keys::PublicKey {
    static KEY: std::sync::OnceLock<russh::keys::PublicKey> = std::sync::OnceLock::new();
    KEY.get_or_init(|| key(7).public_key().clone())
}

fn client_config() -> Arc<client::Config> {
    Arc::new(client::Config {
        inactivity_timeout: None,
        ..client::Config::default()
    })
}

/// A server on the test's runtime, driven as [`Sshd`] drives its own.
struct Server {
    shared: Arc<Shared>,
    stop: CancellationToken,
    conns: TaskTracker,
}

impl Server {
    fn new(policy: Policy) -> Server {
        Server::with(policy, account())
    }

    fn with(policy: Policy, account: Account) -> Server {
        let no_auth = matches!(policy, Policy::NoAuth);
        Server {
            shared: Arc::new(Shared {
                config: Arc::new(config::server(key(7), no_auth)),
                deadline: config::PRE_AUTH_DEADLINE,
                policy,
                account,
                registry: Arc::default(),
            }),
            stop: CancellationToken::new(),
            conns: TaskTracker::new(),
        }
    }

    fn registry(&self) -> &Registry {
        &self.shared.registry
    }

    /// Serves one end of a new socket pair, through a relay with `relay`;
    /// returns the other end and a handle to cut it with, as an exiting
    /// client would.
    fn socket(&self, relay: bool) -> (UnixStream, StdStream) {
        let (client, theirs) = StdStream::pair().expect("pair");
        let cut = client.try_clone().expect("clone");
        let served = if relay {
            let (near, far) = StdStream::pair().expect("pair");
            spawn_relay(stream(theirs), stream(near));
            far
        } else {
            theirs
        };
        let kill = served.try_clone().expect("clone");
        served.set_nonblocking(true).expect("nonblocking");
        let conn = connection(
            self.shared.clone(),
            served,
            kill,
            "12D3KooWTestPeer".into(),
            self.stop.clone(),
        );
        self.conns.spawn(conn);
        (stream(client), cut)
    }

    async fn connect(&self, relay: bool) -> (Handle<Client>, StdStream) {
        let (stream, cut) = self.socket(relay);
        let handle = client::connect_stream(client_config(), stream, Client)
            .await
            .expect("handshake");
        (handle, cut)
    }

    async fn login(&self, relay: bool) -> (Handle<Client>, StdStream) {
        let (mut handle, cut) = self.connect(relay).await;
        let auth = handle
            .authenticate_publickey("me", PrivateKeyWithHashAlg::new(Arc::new(key(1)), None))
            .await
            .expect("auth");
        assert!(auth.success(), "{auth:?}");
        (handle, cut)
    }

    /// Stops as `Sshd` does, and checks nothing was left running.
    async fn stop(self) {
        self.stop.cancel();
        self.conns.close();
        timeout(config::SHUTDOWN_BUDGET, self.conns.wait())
            .await
            .expect("connections ended in time");
        assert_eq!(self.registry().len(), 0, "programs left running");
    }
}

fn stream(std: StdStream) -> UnixStream {
    std.set_nonblocking(true).expect("nonblocking");
    UnixStream::from_std(std).expect("tokio stream")
}

/// A full-duplex relay with an unbounded queue each way, so it always keeps
/// reading both sides.
fn spawn_relay(a: UnixStream, b: UnixStream) {
    let (ar, aw) = a.into_split();
    let (br, bw) = b.into_split();
    one_way(ar, bw);
    one_way(br, aw);
}

fn one_way(mut from: tokio::net::unix::OwnedReadHalf, mut to: tokio::net::unix::OwnedWriteHalf) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    tokio::spawn(async move {
        let mut buf = vec![0; 64 * 1024];
        while let Ok(n @ 1..) = from.read(&mut buf).await {
            if tx.send(buf[..n].to_vec()).is_err() {
                return;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if to.write_all(&data).await.is_err() {
                return;
            }
        }
        let _ = to.shutdown().await;
    });
}

/// What a channel's client saw, output merged.
#[derive(Debug, PartialEq, Eq)]
enum Ev {
    Out(Vec<u8>),
    Err(Vec<u8>),
    Status(u32),
    Signal(String),
    Eof,
    Close,
    Success,
    Failure,
}

fn out(text: &str) -> Ev {
    Ev::Out(text.as_bytes().to_vec())
}

/// Everything until the server closes the channel.
async fn collect(read: &mut ChannelReadHalf) -> Vec<Ev> {
    let mut events: Vec<Ev> = Vec::new();
    while let Some(msg) = read.wait().await {
        let ev = match msg {
            ChannelMsg::Data { data } => Ev::Out(data.to_vec()),
            ChannelMsg::ExtendedData { data, ext: 1 } => Ev::Err(data.to_vec()),
            ChannelMsg::ExitStatus { exit_status } => Ev::Status(exit_status),
            ChannelMsg::ExitSignal { signal_name, .. } => Ev::Signal(format!("{signal_name:?}")),
            ChannelMsg::Eof => Ev::Eof,
            ChannelMsg::Close => Ev::Close,
            ChannelMsg::Success => Ev::Success,
            ChannelMsg::Failure => Ev::Failure,
            _ => continue,
        };
        let close = ev == Ev::Close;
        match (events.last_mut(), ev) {
            (Some(Ev::Out(a)), Ev::Out(b)) | (Some(Ev::Err(a)), Ev::Err(b)) => a.extend(b),
            (_, ev) => events.push(ev),
        }
        if close {
            break;
        }
    }
    events
}

/// The next reply to a request.
async fn reply(read: &mut ChannelReadHalf) -> Ev {
    loop {
        match read.wait().await.expect("a reply") {
            ChannelMsg::Success => return Ev::Success,
            ChannelMsg::Failure => return Ev::Failure,
            ChannelMsg::Close => return Ev::Close,
            _ => {}
        }
    }
}

/// Stdout until it holds `needle`.
async fn output_until(read: &mut ChannelReadHalf, needle: &str) -> String {
    let mut seen = Vec::new();
    loop {
        match read.wait().await {
            Some(ChannelMsg::Data { data }) => seen.extend_from_slice(&data),
            Some(ChannelMsg::Close) | None => {
                panic!(
                    "closed before {needle:?}: {:?}",
                    String::from_utf8_lossy(&seen)
                )
            }
            _ => {}
        }
        let text = String::from_utf8_lossy(&seen).into_owned();
        if text.contains(needle) {
            return text;
        }
    }
}

/// Opens a session channel and runs `command`, with a PTY if `pty`.
async fn run(
    handle: &Handle<Client>,
    command: &str,
    pty: bool,
) -> (ChannelReadHalf, ChannelWriteHalf<client::Msg>) {
    let channel = handle.channel_open_session().await.expect("session");
    if pty {
        channel
            .request_pty(false, "xterm", 100, 30, 0, 0, &[])
            .await
            .expect("pty");
    }
    channel.exec(true, command).await.expect("exec");
    let (mut read, write) = channel.split();
    assert_eq!(reply(&mut read).await, Ev::Success);
    (read, write)
}

/// The first `n` lines of output, without their line ends.
async fn lines(read: &mut ChannelReadHalf, n: usize) -> Vec<String> {
    let mut seen = Vec::new();
    loop {
        match read.wait().await {
            Some(ChannelMsg::Data { data }) => seen.extend_from_slice(&data),
            Some(ChannelMsg::Close) | None => {
                panic!("closed after {:?}", String::from_utf8_lossy(&seen))
            }
            _ => {}
        }
        let text = String::from_utf8_lossy(&seen).into_owned();
        if text.matches('\n').count() >= n {
            return text.lines().take(n).map(|l| l.trim().to_owned()).collect();
        }
    }
}

fn pid(line: &str) -> Pid {
    Pid::from_raw(line.parse().expect("a pid")).expect("pid")
}

/// The first line of output as a pid.
async fn read_pid(read: &mut ChannelReadHalf) -> Pid {
    pid(&lines(read, 1).await[0])
}

fn alive(pid: Pid) -> bool {
    rustix::process::test_kill_process(pid).is_ok()
}

/// Waits up to `limit` for `pid` to be gone, and says how long it took.
async fn gone(pid: Pid, limit: Duration) -> Duration {
    let start = Instant::now();
    timeout(limit, async {
        while alive(pid) {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{pid:?} still alive after {limit:?}"));
    start.elapsed()
}

/// Waits for every program of `server` to be reaped.
async fn none_running(server: &Server) {
    timeout(Duration::from_secs(10), async {
        while server.registry().len() > 0 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("programs still running");
}

/// A deterministic byte stream and a hash of it, for bulk tests.
struct Pattern(u64);

impl Pattern {
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            chunk.copy_from_slice(&self.0.to_le_bytes()[..chunk.len()]);
        }
    }
}

fn fnv(hash: &mut u64, data: &[u8]) {
    for &b in data {
        *hash = (*hash ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
    }
}

/// Writes `total` bytes of the pattern to the channel as fast as it takes
/// them, then EOF if `eof`; returns the hash of what it wrote.
fn spawn_writer(
    write: Arc<ChannelWriteHalf<client::Msg>>,
    total: usize,
    eof: bool,
) -> tokio::task::JoinHandle<u64> {
    tokio::spawn(async move {
        let mut to = write.make_writer();
        let mut pattern = Pattern(0x9e37_79b9_7f4a_7c15);
        let mut hash = 0xcbf2_9ce4_8422_2325;
        let mut buf = vec![0; 32 * 1024];
        let mut sent = 0;
        while sent < total {
            let n = buf.len().min(total - sent);
            pattern.fill(&mut buf[..n]);
            if to.write_all(&buf[..n]).await.is_err() {
                break;
            }
            fnv(&mut hash, &buf[..n]);
            sent += n;
        }
        if eof && sent == total {
            let _ = write.eof().await;
        }
        hash
    })
}

// --- auth ------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_an_authorized_key_logs_in() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let (mut read, _write) = run(&handle, "echo \"$USER $HOME $TERM\"", false).await;
        let account = &server.shared.account;
        let want = format!("{} {} \n", account.name, account.home.display());
        assert_eq!(
            collect(&mut read).await,
            [out(&want), Ev::Status(0), Ev::Eof, Ev::Close]
        );

        let (mut handle, _cut) = server.connect(false).await;
        let wrong = PrivateKeyWithHashAlg::new(Arc::new(key(2)), None);
        let auth = handle
            .authenticate_publickey("me", wrong)
            .await
            .expect("auth");
        assert!(!auth.success());
        // `none` is refused without --no-auth.
        let auth = handle.authenticate_none("me").await.expect("auth");
        assert!(!auth.success());
        assert!(handle.channel_open_session().await.is_err());
        server.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_auth_takes_anyone_with_the_ticket() {
    bounded(async {
        let server = Server::new(Policy::NoAuth);
        let (mut handle, _cut) = server.connect(false).await;
        let auth = handle.authenticate_none("anyone").await.expect("auth");
        assert!(auth.success());
        let (mut read, _write) = run(&handle, "echo $SSH_PEER_ID", false).await;
        assert_eq!(collect(&mut read).await[0], out("12D3KooWTestPeer\n"));
        server.stop().await;
    })
    .await;
}

/// A connection that never says a word is cut at the deadline.
#[tokio::test(start_paused = true)]
async fn a_silent_client_is_cut_at_20_s() {
    bounded(async {
        let server = Server::new(keys());
        let start = Instant::now();
        let (mut stream, _cut) = server.socket(false);
        let mut buf = vec![0; 1024];
        let mut got = 0;
        while let Ok(n @ 1..) = stream.read(&mut buf).await {
            got += n;
        }
        let took = start.elapsed();
        assert!(got > 0, "no identification from the server");
        assert!(
            took >= config::PRE_AUTH_DEADLINE
                && took < config::PRE_AUTH_DEADLINE + Duration::from_secs(3),
            "{took:?}"
        );
        server.stop().await;
    })
    .await;
}

/// A signer that never signs: the client offers a key, the server says it
/// would take it, and the client never proves it holds it.
struct Unsigned;

#[derive(Debug)]
struct NotSigned;

impl From<russh::SendError> for NotSigned {
    fn from(_: russh::SendError) -> NotSigned {
        NotSigned
    }
}

impl russh::Signer for Unsigned {
    type Error = NotSigned;

    async fn auth_sign(
        &mut self,
        _key: &AgentIdentity,
        _hash: Option<HashAlg>,
        _data: Vec<u8>,
    ) -> Result<Vec<u8>, NotSigned> {
        Err(NotSigned)
    }
}

/// Paused time is no use here: tokio advances a paused clock while socket
/// I/O is still in flight, so the handshake itself would hit the deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsigned_offer_is_not_a_login() {
    bounded(async {
        let mut server = Server::new(keys());
        let deadline = Duration::from_secs(3);
        Arc::get_mut(&mut server.shared).expect("unshared").deadline = deadline;
        let start = Instant::now();
        let (mut handle, _cut) = server.connect(false).await;
        let public = key(1).public_key().clone();
        let signed = handle
            .authenticate_publickey_with("me", public, None, &mut Unsigned)
            .await;
        assert!(
            signed.is_err(),
            "the server should have asked for a signature"
        );
        // The server cuts the connection at the deadline. (russh's client,
        // waiting on the signature, does not notice; ssh would.)
        server.conns.close();
        timeout(
            deadline + config::CONNECTION_END + Duration::from_secs(1),
            server.conns.wait(),
        )
        .await
        .expect("cut at the deadline");
        let took = start.elapsed();
        assert!(took >= deadline, "{took:?}");
        drop(handle);
        server.stop().await;
    })
    .await;
}

// --- exec and channels -----------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exec_sends_output_then_status_eof_and_close() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let (mut read, _write) = run(&handle, "printf out; printf err >&2; exit 3", false).await;
        let events = collect(&mut read).await;
        let (output, end) = events.split_at(2);
        assert!(output.contains(&out("out")), "{events:?}");
        assert!(output.contains(&Ev::Err(b"err".to_vec())), "{events:?}");
        assert_eq!(end, [Ev::Status(3), Ev::Eof, Ev::Close]);

        // A death by signal is reported as one; a signal SSH has no name
        // for, as 128 + its number.
        let (handle, _cut) = server.login(false).await;
        let (mut read, _write) = run(&handle, "kill -USR2 $$", false).await;
        let events = collect(&mut read).await;
        assert_eq!(
            events[0],
            Ev::Signal("Custom(\"USR2\")".into()),
            "{events:?}"
        );
        let (handle, _cut) = server.login(false).await;
        let (mut read, _write) = run(&handle, "kill -PROF $$", false).await;
        let prof = Signal::PROF.as_raw() as u32;
        assert_eq!(collect(&mut read).await[0], Ev::Status(128 + prof));
        server.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_eof_before_exec_still_reaches_cat() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let channel = handle.channel_open_session().await.expect("session");
        channel.data(&b"hello\n"[..]).await.expect("data");
        channel.eof().await.expect("eof");
        channel.exec(true, "cat").await.expect("exec");
        let (mut read, _write) = channel.split();
        assert_eq!(
            collect(&mut read).await,
            [
                Ev::Success,
                out("hello\n"),
                Ev::Status(0),
                Ev::Eof,
                Ev::Close
            ]
        );
        server.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn requests_out_of_turn_are_refused() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let channel = handle.channel_open_session().await.expect("session");
        channel.set_env(true, "FOO", "bar").await.expect("env");
        channel
            .request_subsystem(true, "sftp")
            .await
            .expect("subsystem");
        channel
            .exec(true, "echo $FOO; exec sleep 1000")
            .await
            .expect("exec");
        channel.exec(true, "true").await.expect("exec");
        channel.request_shell(true).await.expect("shell");
        channel
            .request_pty(true, "xterm", 80, 24, 0, 0, &[])
            .await
            .expect("pty");
        let (mut read, write) = channel.split();
        let mut replies = Vec::new();
        let mut output = Vec::new();
        while replies.len() < 6 || output.is_empty() {
            match read.wait().await.expect("open") {
                ChannelMsg::Success => replies.push(Ev::Success),
                ChannelMsg::Failure => replies.push(Ev::Failure),
                ChannelMsg::Data { data } => output.extend_from_slice(&data),
                _ => {}
            }
        }
        use Ev::{Failure, Success};
        assert_eq!(
            replies,
            [Failure, Failure, Success, Failure, Failure, Failure]
        );
        assert_eq!(output, b"\n", "env must not pass");

        // One session channel per connection, and no other kind at all.
        assert!(handle.channel_open_session().await.is_err());
        let tcp = handle
            .channel_open_direct_tcpip("127.0.0.1", 22, "127.0.0.1", 1)
            .await;
        assert!(tcp.is_err());
        assert!(handle.channel_open_x11("127.0.0.1", 1).await.is_err());
        // The first channel still works.
        write.close().await.expect("close");
        none_running(&server).await;
        server.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shell_that_cannot_start_fails_and_closes() {
    bounded(async {
        let account = Account {
            shell: "/nonexistent/shell".into(),
            ..account()
        };
        let server = Server::with(keys(), account);
        let (handle, _cut) = server.login(false).await;
        let channel = handle.channel_open_session().await.expect("session");
        channel.exec(true, "true").await.expect("exec");
        let (mut read, _write) = channel.split();
        assert_eq!(collect(&mut read).await, [Ev::Failure, Ev::Close]);
        // A NUL in the command is refused the same way.
        let server2 = Server::new(keys());
        let (handle, _cut) = server2.login(false).await;
        let channel = handle.channel_open_session().await.expect("session");
        channel.exec(true, &b"echo a\0b"[..]).await.expect("exec");
        let (mut read, _write) = channel.split();
        assert_eq!(collect(&mut read).await, [Ev::Failure, Ev::Close]);
        server.stop().await;
        server2.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_after_eof_closes_the_channel() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let (mut read, write) = run(&handle, "exec sleep 1000", false).await;
        write.eof().await.expect("eof");
        write.data(&b"late"[..]).await.expect("data");
        assert_eq!(collect(&mut read).await, [Ev::Close]);
        none_running(&server).await;
        server.stop().await;
    })
    .await;
}

// --- PTY -------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pty_has_the_size_asked_for_and_follows_resizes() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let (mut read, write) =
            run(&handle, "stty size; echo $TERM; read x; stty size", true).await;
        let text = output_until(&mut read, "xterm").await;
        assert!(text.starts_with("30 100\r\n"), "{text:?}");
        write.window_change(120, 40, 0, 0).await.expect("resize");
        write.data(&b"\n"[..]).await.expect("data");
        output_until(&mut read, "40 120").await;
        let events = collect(&mut read).await;
        assert_eq!(
            events[events.len() - 3..],
            [Ev::Status(0), Ev::Eof, Ev::Close]
        );
        server.stop().await;
    })
    .await;
}

// --- teardown --------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_the_channel_kills_its_program() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let (mut read, write) = run(&handle, "echo $$; exec sleep 1000", false).await;
        let pid = read_pid(&mut read).await;
        write.close().await.expect("close");
        // SIGHUP does it at once, well inside the grace.
        assert!(gone(pid, Duration::from_secs(5)).await < config::GRACE);
        none_running(&server).await;
        server.stop().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_connection_kills_a_group_that_ignores_hup() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, cut) = server.login(false).await;
        let (mut read, _write) = run(
            &handle,
            "trap '' HUP TERM; sleep 1000 & echo $!; wait",
            false,
        )
        .await;
        let pid = read_pid(&mut read).await;
        cut.shutdown(Shutdown::Both).expect("cut");
        // SIGKILL, once the grace is over.
        let took = gone(pid, Duration::from_secs(10)).await;
        assert!(
            took >= config::GRACE - Duration::from_millis(300),
            "{took:?}"
        );
        none_running(&server).await;
        let signals = server.registry().signals.lock().unwrap().clone();
        let sent: Vec<i32> = signals.iter().map(|(_, signal)| *signal).collect();
        assert_eq!(
            sent,
            [Signal::HUP, Signal::TERM, Signal::KILL].map(Signal::as_raw)
        );
        server.stop().await;
    })
    .await;
}

/// Closing a PTY hangs its terminal up. The shell gets SIGHUP and, as it
/// exits, so does its foreground job (on BSD the job gets it at once).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pty_hangup_reaches_the_foreground_job() {
    bounded(async {
        let marker = std::env::temp_dir().join(format!("minipaw-hup-{}", std::process::id()));
        let _ = std::fs::remove_file(&marker);
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let script = format!(
            "echo $$; sh -c 'trap \"echo job-hup >> {m}; exit 0\" HUP; echo $$; \
             while :; do sleep 0.1; done'",
            m = marker.display()
        );
        let (mut read, write) = run(&handle, &script, true).await;
        let pids = lines(&mut read, 2).await;
        let (shell, job) = (pid(&pids[0]), pid(&pids[1]));
        write.close().await.expect("close");
        assert!(gone(job, Duration::from_secs(5)).await < config::GRACE);
        gone(shell, Duration::from_secs(5)).await;
        none_running(&server).await;
        let marked = std::fs::read_to_string(&marker).unwrap_or_default();
        let _ = std::fs::remove_file(&marker);
        assert!(marked.starts_with("job-hup\n"), "{marked:?}");
        // The hangup did it all: nothing needed a signal of ours.
        assert_eq!(*server.registry().signals.lock().unwrap(), []);
        server.stop().await;
    })
    .await;
}

/// A shell that ignores the hangup is killed after the grace, by its own
/// group only: a job it reaped meanwhile draws no stray signal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_job_reaped_during_the_grace_draws_no_stray_signal() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let script = "trap '' HUP; echo $$; sh -c 'echo $$; exec sleep 1'; exec sleep 1000";
        let (mut read, write) = run(&handle, script, true).await;
        let pids = lines(&mut read, 2).await;
        let (shell, job) = (pid(&pids[0]), pid(&pids[1]));
        write.close().await.expect("close");
        // The job ends by itself, inside the grace; the shell lasts until
        // the grace is over.
        assert!(gone(job, Duration::from_secs(5)).await < config::GRACE);
        let took = gone(shell, Duration::from_secs(10)).await;
        assert!(took > Duration::from_millis(500), "{took:?}");
        none_running(&server).await;
        let signals = server.registry().signals.lock().unwrap().clone();
        let shell = shell.as_raw_nonzero().get();
        assert_eq!(signals, [(shell, Signal::KILL.as_raw())]);
        server.stop().await;
    })
    .await;
}

// --- flow control ----------------------------------------------------------

/// `cat` with 20 MB both ways at once, through a relay that keeps reading
/// as OpenSSH does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cat_moves_20_mb_both_ways() {
    bounded(async {
        const TOTAL: usize = 20_000_000;
        let server = Server::new(keys());
        let (handle, _cut) = server.login(true).await;
        let (mut read, write) = run(&handle, "cat", false).await;
        let writer = spawn_writer(Arc::new(write), TOTAL, true);
        let mut got = 0;
        let mut hash = 0xcbf2_9ce4_8422_2325;
        let mut end = Vec::new();
        while let Some(msg) = read.wait().await {
            match msg {
                ChannelMsg::Data { data } => {
                    got += data.len();
                    fnv(&mut hash, &data);
                }
                ChannelMsg::ExitStatus { exit_status } => end.push(Ev::Status(exit_status)),
                ChannelMsg::Close => break,
                _ => {}
            }
        }
        assert_eq!(got, TOTAL);
        assert_eq!(hash, writer.await.expect("writer"));
        assert_eq!(end, [Ev::Status(0)]);
        server.stop().await;
    })
    .await;
}

/// A slow consumer fed less than the cap gets all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_consumer_under_the_cap_completes() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let (mut read, write) = run(&handle, "sleep 2; wc -c", false).await;
        let _writer = spawn_writer(Arc::new(write), 4 * MIB, true);
        let events = collect(&mut read).await;
        let Ev::Out(count) = &events[0] else {
            panic!("{events:?}");
        };
        assert_eq!(String::from_utf8_lossy(count).trim(), (4 * MIB).to_string());
        assert_eq!(events[1..], [Ev::Status(0), Ev::Eof, Ev::Close]);
        server.stop().await;
    })
    .await;
}

/// A slow consumer fed past the cap: its channel gets the message and is
/// closed, and the server goes on serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn input_past_the_cap_closes_only_that_channel() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, cut) = server.login(false).await;
        let start = Instant::now();
        let (mut read, write) = run(&handle, "sleep 5; cat >/dev/null", false).await;
        let writer = spawn_writer(Arc::new(write), 12 * MIB, true);
        let events = collect(&mut read).await;
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "cut only after the sleep"
        );
        assert_eq!(
            events,
            [Ev::Err(config::CAP_MESSAGE.as_bytes().to_vec()), Ev::Close]
        );
        writer.abort();
        none_running(&server).await;
        // The client may still have data queued for the closed channel,
        // which wedges russh's client; an exiting ssh cuts its socket.
        cut.shutdown(Shutdown::Both).expect("cut");

        let (handle, _cut) = server.login(false).await;
        let (mut read, _write) = run(&handle, "echo ok", false).await;
        assert_eq!(
            collect(&mut read).await,
            [out("ok\n"), Ev::Status(0), Ev::Eof, Ev::Close]
        );
        server.stop().await;
    })
    .await;
}

/// With 6 MiB waiting on a consumer that sleeps, the connection still
/// answers, and a close still ends the program.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_consumer_leaves_the_connection_responsive() {
    bounded(async {
        let server = Server::new(keys());
        let (handle, _cut) = server.login(false).await;
        let (_read, write) = run(&handle, "sleep 30; cat >/dev/null", false).await;
        let write = Arc::new(write);
        spawn_writer(write.clone(), 6 * MIB, false)
            .await
            .expect("writer");
        for _ in 0..3 {
            timeout(Duration::from_secs(2), handle.send_ping())
                .await
                .expect("ping in time")
                .expect("ping");
        }
        write.close().await.expect("close");
        none_running(&server).await;
        server.stop().await;
    })
    .await;
}

/// Many connections one after another leave nothing behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequential_connections_leave_nothing_behind() {
    bounded(async {
        let server = Server::new(keys());
        for _ in 0..25 {
            let (handle, cut) = server.login(false).await;
            let (mut read, _write) = run(&handle, "echo hi", false).await;
            assert_eq!(collect(&mut read).await[0], out("hi\n"));
            cut.shutdown(Shutdown::Both).expect("cut");
        }
        none_running(&server).await;
        server.stop().await;
    })
    .await;
}

// --- admission and shutdown ------------------------------------------------

#[test]
fn the_gate_admits_no_one_after_it_closes() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    // A worker inside the gate holds up the stop's wait.
    let tracker = TaskTracker::new();
    let gate = Gate::new(tracker.clone());
    let token = gate.enter().expect("open");
    gate.close();
    tracker.close();
    assert!(gate.enter().is_none());
    runtime.block_on(async {
        let waiting = timeout(Duration::from_millis(100), tracker.wait()).await;
        assert!(waiting.is_err(), "the wait ended with a worker inside");
        drop(token);
        timeout(Duration::from_secs(1), tracker.wait())
            .await
            .expect("drained");
    });

    // Workers racing a stop: any that got in is waited for.
    for _ in 0..20 {
        let tracker = TaskTracker::new();
        let gate = Arc::new(Gate::new(tracker.clone()));
        let barrier = Arc::new(Barrier::new(9));
        let waited = Arc::new(AtomicBool::new(false));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let (gate, barrier, waited) = (gate.clone(), barrier.clone(), waited.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    let mut admitted = 0;
                    while let Some(token) = gate.enter() {
                        assert!(!waited.load(Ordering::SeqCst), "admitted after the wait");
                        admitted += 1;
                        drop(token);
                    }
                    admitted
                })
            })
            .collect();
        barrier.wait();
        std::thread::sleep(Duration::from_millis(1));
        gate.close();
        tracker.close();
        runtime
            .block_on(async { timeout(Duration::from_secs(5), tracker.wait()).await })
            .expect("drained");
        waited.store(true, Ordering::SeqCst);
        for worker in workers {
            worker.join().expect("worker");
        }
    }
}

/// Dropping [`Sshd`] stops it within its budget, even with a PTY read
/// blocked and a program that ignores SIGHUP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_ends_every_login_in_time() {
    bounded(async {
        let setup = Setup {
            policy: keys(),
            account: account(),
            lines: Vec::new(),
        };
        let sshd = Sshd::start(setup, &[9; 32]).expect("start");
        let registry = sshd.registry.clone();
        let acceptor = sshd.acceptor();
        let mut pids = Vec::new();
        let mut handles = Vec::new();
        for (command, pty) in [
            ("echo $$; exec sleep 1000", true),
            ("trap '' HUP TERM; echo $$; exec sleep 1000", false),
        ] {
            let socket = acceptor.open("12D3KooWTestPeer").expect("open");
            let mut handle = client::connect_stream(client_config(), stream(socket), AnyHost)
                .await
                .expect("handshake");
            let key = PrivateKeyWithHashAlg::new(Arc::new(key(1)), None);
            assert!(
                handle
                    .authenticate_publickey("me", key)
                    .await
                    .expect("auth")
                    .success()
            );
            let channel = handle.channel_open_session().await.expect("session");
            if pty {
                channel
                    .request_pty(false, "xterm", 80, 24, 0, 0, &[])
                    .await
                    .expect("pty");
            }
            channel.exec(true, command).await.expect("exec");
            let (mut read, write) = channel.split();
            assert_eq!(reply(&mut read).await, Ev::Success);
            pids.push(read_pid(&mut read).await);
            handles.push((handle, read, write));
        }
        let start = Instant::now();
        tokio::task::spawn_blocking(move || drop(sshd))
            .await
            .expect("stopped");
        let took = start.elapsed();
        assert!(took < config::SHUTDOWN_BUDGET, "{took:?}");
        assert_eq!(registry.len(), 0);
        for pid in pids {
            assert!(!alive(pid), "{pid:?} outlived the stop");
        }
        // Closed for good.
        assert!(acceptor.open("12D3KooWTestPeer").is_err());
    })
    .await;
}

/// A client for [`Sshd`]'s own host key, from another seed.
struct AnyHost;

impl client::Handler for AnyHost {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// The last resort kills what teardown could not, and forgets what was
/// reaped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_last_resort_kills_unreaped_groups() {
    bounded(async {
        let registry = Arc::new(Registry::default());
        let login = process::Login {
            command: Some(std::ffi::OsStr::new("exec sleep 1000")),
            pty: None,
            peer: "p",
        };
        let (process, _io) = process::spawn(&account(), &login, &registry).expect("spawn");
        let pid = Pid::from_raw(process.pid()).expect("pid");
        assert_eq!(registry.kill_all(), 1);
        gone_or_zombie(&process).await;
        let status = process.reap().await.expect("reap");
        assert_eq!(
            process::exit_of(status),
            process::Exit::Signal("KILL", false)
        );
        assert_eq!(registry.kill_all(), 0);
        assert!(!alive(pid));
    })
    .await;
}

async fn gone_or_zombie(process: &process::Process) {
    timeout(Duration::from_secs(5), process.exited())
        .await
        .expect("killed");
}
