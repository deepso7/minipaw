//! The spike server: the plan's channel and flow-control design, minus
//! auth, PTY and the full teardown ladder (a SIGKILL to the group stands in).

use std::collections::VecDeque;
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use russh::keys::PrivateKey;
use russh::keys::ssh_key::private::Ed25519Keypair;
use russh::server::{self, Auth, ChannelOpenHandle, Msg, Session};
use russh::{
    Channel, ChannelId, ChannelMsg, ChannelOpenFailure, ChannelReadHalf, ChannelWriteHalf,
    MethodKind, MethodSet,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::{CAP_MESSAGE, CHUNK, INPUT_CAP, max_i64, max_usize};

/// Counters the tests read. One `Stats` per spike server.
#[derive(Default, Debug)]
pub struct Stats {
    pub conns_started: AtomicU64,
    pub conns_ended: AtomicU64,
    pub conn_errors: AtomicU64,
    pub channels_accepted: AtomicU64,
    pub channels_rejected: AtomicU64,
    pub cap_trips: AtomicU64,
    /// Largest number of bytes buffered for a child at once.
    pub max_buffered: AtomicUsize,
    /// Bytes received through `data` callbacks.
    pub input_bytes: AtomicU64,
    /// Largest single `data` payload the handler saw.
    pub max_data_packet: AtomicUsize,
    pub supervisors_started: AtomicU64,
    pub supervisors_done: AtomicU64,
    /// Input buffers still alive (dropped when the supervisor and handler
    /// both let go).
    pub live_inputs: AtomicI64,
    pub channel_close_callbacks: AtomicU64,
    pub window_adjusted_callbacks: AtomicU64,
    /// `window_adjusted` saw data still pending after russh flushed (the
    /// visible trace of russh issue B).
    pub pending_after_adjust: AtomicU64,
    /// Output bytes our writers had accepted by `ChannelTx` (credit taken).
    pub reserved_out: AtomicU64,
    /// Largest `reserved_out - credit granted by the client` seen (needs a
    /// proxy to count the credit). Positive means russh issue B.
    pub overcommit_max: AtomicI64,
}

#[derive(Clone, Default)]
pub struct Options {
    /// Server keepalive interval.
    pub keepalive: Option<Duration>,
    /// Negotiate cipher and MAC `none` so the proxy can read and rewrite
    /// packets. Only for the abuse tests.
    pub cleartext: bool,
    /// Credit the client has granted, counted by the proxy.
    pub client_credit: Option<Arc<AtomicU64>>,
    /// Override the server's receive window (russh default 2 MiB).
    pub window: Option<u32>,
    /// The design the plan rejects, for the #730 repro: keep the
    /// `ChannelReadHalf` and feed the child from it with back-pressure
    /// (`write_all` to stdin before taking the next message), instead of
    /// the capped callback buffer.
    pub naive_read_half: bool,
}

pub struct Server {
    pub config: Arc<server::Config>,
    pub stats: Arc<Stats>,
    opts: Options,
}

pub fn cleartext_preferred() -> russh::Preferred {
    russh::Preferred {
        cipher: std::borrow::Cow::Borrowed(&[russh::cipher::NONE]),
        mac: std::borrow::Cow::Borrowed(&[russh::mac::NONE]),
        ..Default::default()
    }
}

impl Server {
    pub fn new(opts: Options) -> Self {
        let key = PrivateKey::from(Ed25519Keypair::from_seed(&[7; 32]));
        let mut config = server::Config {
            methods: MethodSet::from(&[MethodKind::None][..]),
            auth_rejection_time: Duration::from_millis(10),
            auth_rejection_time_initial: Some(Duration::ZERO),
            keys: vec![key],
            inactivity_timeout: None,
            keepalive_interval: opts.keepalive,
            ..Default::default()
        };
        if let Some(w) = opts.window {
            config.window_size = w;
        }
        if opts.cleartext {
            config.preferred = cleartext_preferred();
        }
        Self {
            config: Arc::new(config),
            stats: Arc::new(Stats::default()),
            opts,
        }
    }

    /// Serve one connection; resolves when russh's session task ends.
    pub fn serve(&self, stream: UnixStream) -> JoinHandle<()> {
        let config = self.config.clone();
        let stats = self.stats.clone();
        let handler = Conn {
            stats: stats.clone(),
            client_credit: self.opts.client_credit.clone(),
            naive: self.opts.naive_read_half,
            chan: None,
            opened: false,
        };
        stats.conns_started.fetch_add(1, Relaxed);
        tokio::spawn(async move {
            let result = match server::run_stream(config, stream, handler).await {
                Ok(running) => running.await,
                Err(e) => Err(e),
            };
            if result.is_err() {
                stats.conn_errors.fetch_add(1, Relaxed);
            }
            stats.conns_ended.fetch_add(1, Relaxed);
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    Cap,
    ClientClose,
    ConnectionEnd,
}

/// The per-channel input buffer, filled only from `data` callbacks.
struct Input {
    st: Mutex<InputState>,
    notify: Notify,
    stats: Arc<Stats>,
}

#[derive(Default)]
struct InputState {
    queue: VecDeque<Bytes>,
    len: usize,
    eof: bool,
    dead: bool,
}

impl Input {
    fn new(stats: Arc<Stats>) -> Arc<Self> {
        stats.live_inputs.fetch_add(1, Relaxed);
        Arc::new(Self {
            st: Mutex::new(InputState::default()),
            notify: Notify::new(),
            stats,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, InputState> {
        self.st
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Append; `false` when this would pass the cap (the buffer is then dead).
    fn push(&self, data: &[u8]) -> bool {
        let mut st = self.lock();
        if st.dead || st.eof {
            return true;
        }
        if st.len + data.len() > INPUT_CAP {
            st.dead = true;
            st.queue.clear();
            st.len = 0;
            drop(st);
            self.notify.notify_one();
            return false;
        }
        st.queue.push_back(Bytes::copy_from_slice(data));
        st.len += data.len();
        max_usize(&self.stats.max_buffered, st.len);
        drop(st);
        self.notify.notify_one();
        true
    }

    fn eof(&self) {
        self.lock().eof = true;
        self.notify.notify_one();
    }

    fn kill(&self) {
        let mut st = self.lock();
        st.dead = true;
        st.queue.clear();
        st.len = 0;
        drop(st);
        self.notify.notify_one();
    }

    async fn pop(&self) -> Option<Bytes> {
        loop {
            {
                let mut st = self.lock();
                if st.dead {
                    return None;
                }
                if let Some(b) = st.queue.pop_front() {
                    st.len -= b.len();
                    return Some(b);
                }
                if st.eof {
                    return None;
                }
            }
            self.notify.notified().await;
        }
    }
}

impl Drop for Input {
    fn drop(&mut self) {
        self.stats.live_inputs.fetch_sub(1, Relaxed);
    }
}

struct Chan {
    id: ChannelId,
    input: Arc<Input>,
    /// Held until exec moves it to the supervisor.
    write: Option<ChannelWriteHalf<Msg>>,
    /// Only with `naive_read_half`.
    read: Option<ChannelReadHalf>,
    teardown: watch::Sender<Option<Reason>>,
}

impl Chan {
    fn trigger(&self, r: Reason) {
        self.teardown.send_if_modified(|cur| {
            if cur.is_none() {
                *cur = Some(r);
                true
            } else {
                false
            }
        });
    }
}

impl Drop for Chan {
    fn drop(&mut self) {
        // The handler is dropped when russh's session task ends.
        self.trigger(Reason::ConnectionEnd);
    }
}

struct Conn {
    stats: Arc<Stats>,
    client_credit: Option<Arc<AtomicU64>>,
    naive: bool,
    chan: Option<Chan>,
    opened: bool,
}

impl Conn {
    fn chan(&mut self, id: ChannelId) -> Option<&mut Chan> {
        self.chan.as_mut().filter(|c| c.id == id)
    }
}

impl server::Handler for Conn {
    type Error = russh::Error;

    async fn auth_none(&mut self, _user: &str) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if self.opened {
            self.stats.channels_rejected.fetch_add(1, Relaxed);
            reply
                .reject(ChannelOpenFailure::AdministrativelyProhibited)
                .await;
            return Ok(());
        }
        self.opened = true;
        reply.accept().await;
        self.stats.channels_accepted.fetch_add(1, Relaxed);
        let id = channel.id();
        let (read, write) = channel.split();
        // The plan: drop the read half; russh's sends to it fail at once.
        let read = if self.naive { Some(read) } else { None };
        let (teardown, _) = watch::channel(None);
        self.chan = Some(Chan {
            id,
            input: Input::new(self.stats.clone()),
            write: Some(write),
            read,
            teardown,
        });
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let stats = self.stats.clone();
        let credit = self.client_credit.clone();
        let Some(chan) = self.chan(channel) else {
            session.channel_failure(channel)?;
            return Ok(());
        };
        let Some(write) = chan.write.take() else {
            session.channel_failure(channel)?;
            return Ok(());
        };
        session.channel_success(channel)?;
        let cmd = String::from_utf8_lossy(data).into_owned();
        let source = match chan.read.take() {
            Some(read) => Source::ReadHalf(read),
            None => Source::Buffer(chan.input.clone()),
        };
        let input = chan.input.clone();
        let rx = chan.teardown.subscribe();
        tokio::spawn(supervise(cmd, write, source, input, rx, stats, credit));
        Ok(())
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.stats.input_bytes.fetch_add(data.len() as u64, Relaxed);
        max_usize(&self.stats.max_data_packet, data.len());
        let stats = self.stats.clone();
        let naive = self.naive;
        if let Some(chan) = self.chan(channel)
            && !naive
            && !chan.input.push(data)
        {
            stats.cap_trips.fetch_add(1, Relaxed);
            if chan.write.is_some() {
                // No supervisor yet: report and close from here.
                session.extended_data(channel, 1, Bytes::from_static(CAP_MESSAGE.as_bytes()))?;
                session.close(channel)?;
            }
            chan.trigger(Reason::Cap);
        }
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(chan) = self.chan(channel) {
            chan.input.eof();
        }
        Ok(())
    }

    async fn channel_close(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.stats.channel_close_callbacks.fetch_add(1, Relaxed);
        if let Some(chan) = self.chan(channel) {
            chan.trigger(Reason::ClientClose);
        }
        Ok(())
    }

    async fn window_adjusted(
        &mut self,
        channel: ChannelId,
        _new_size: u32,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.stats.window_adjusted_callbacks.fetch_add(1, Relaxed);
        if session.has_pending_data(channel) {
            self.stats.pending_after_adjust.fetch_add(1, Relaxed);
        }
        Ok(())
    }
}

/// Copy child output into the channel in ≤32 KiB chunks through the
/// window-aware writer, counting the credit `ChannelTx` takes.
async fn pump<R, W>(mut from: R, mut to: W, stats: Arc<Stats>, credit: Option<Arc<AtomicU64>>)
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; CHUNK];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let mut off = 0;
        while off < n {
            let Ok(k) = to.write(&buf[off..n]).await else {
                return;
            };
            off += k;
            let reserved = stats.reserved_out.fetch_add(k as u64, Relaxed) + k as u64;
            if let Some(c) = &credit {
                let granted = c.load(Relaxed);
                max_i64(&stats.overcommit_max, reserved as i64 - granted as i64);
            }
        }
    }
}

/// Drain the input buffer into the child's stdin; close stdin at EOF.
enum Source {
    Buffer(Arc<Input>),
    ReadHalf(ChannelReadHalf),
}

/// Drain the input into the child's stdin; close stdin at EOF.
async fn feed(mut stdin: tokio::process::ChildStdin, source: Source) {
    match source {
        Source::Buffer(input) => {
            while let Some(b) = input.pop().await {
                if stdin.write_all(&b).await.is_err() {
                    input.kill();
                    return;
                }
            }
        }
        Source::ReadHalf(mut read) => {
            while let Some(msg) = read.wait().await {
                match msg {
                    ChannelMsg::Data { data } => {
                        if stdin.write_all(&data).await.is_err() {
                            return;
                        }
                    }
                    ChannelMsg::Eof | ChannelMsg::Close => return,
                    _ => {}
                }
            }
        }
    }
}

enum End {
    Exited(std::process::ExitStatus),
    Teardown(Reason),
}

async fn supervise(
    cmd: String,
    write: ChannelWriteHalf<Msg>,
    source: Source,
    input: Arc<Input>,
    mut teardown: watch::Receiver<Option<Reason>>,
    stats: Arc<Stats>,
    credit: Option<Arc<AtomicU64>>,
) {
    stats.supervisors_started.fetch_add(1, Relaxed);
    let spawned = tokio::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(&cmd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .kill_on_drop(true)
        .spawn();
    let mut child = match spawned {
        Ok(c) => c,
        Err(_) => {
            let _ = write.exit_status(127).await;
            let _ = write.eof().await;
            let _ = write.close().await;
            stats.supervisors_done.fetch_add(1, Relaxed);
            return;
        }
    };
    let pgid = child
        .id()
        .and_then(|p| rustix::process::Pid::from_raw(p as i32));
    let (Some(stdin), Some(stdout), Some(stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        stats.supervisors_done.fetch_add(1, Relaxed);
        return;
    };
    let feeder = tokio::spawn(feed(stdin, source));
    let out = tokio::spawn(pump(
        stdout,
        write.make_writer(),
        stats.clone(),
        credit.clone(),
    ));
    let err = tokio::spawn(pump(
        stderr,
        write.make_writer_ext(Some(1)),
        stats.clone(),
        credit,
    ));
    let (out_abort, err_abort) = (out.abort_handle(), err.abort_handle());

    let end = tokio::select! {
        status = async {
            let _ = out.await;
            let _ = err.await;
            child.wait().await
        } => match status {
            Ok(s) => End::Exited(s),
            Err(_) => End::Teardown(Reason::ConnectionEnd),
        },
        r = teardown.wait_for(Option::is_some) => {
            End::Teardown(r.ok().and_then(|r| *r).unwrap_or(Reason::ConnectionEnd))
        }
    };

    match end {
        End::Exited(status) => {
            let code = status
                .code()
                .map(|c| c as u32)
                .or_else(|| status.signal().map(|s| 128 + s as u32))
                .unwrap_or(255);
            let _ = write.exit_status(code).await;
            let _ = write.eof().await;
            let _ = write.close().await;
        }
        End::Teardown(reason) => {
            if reason == Reason::Cap {
                let _ = tokio::time::timeout(Duration::from_secs(2), async {
                    let mut w = write.make_writer_ext(Some(1));
                    w.write_all(CAP_MESSAGE.as_bytes()).await
                })
                .await;
            }
            if let Some(pgid) = pgid {
                let _ = rustix::process::kill_process_group(pgid, rustix::process::Signal::KILL);
            }
            out_abort.abort();
            err_abort.abort();
            let _ = child.wait().await;
            if reason == Reason::Cap {
                let _ = write.close().await;
            }
        }
    }
    feeder.abort();
    input.kill();
    stats.supervisors_done.fetch_add(1, Relaxed);
}
