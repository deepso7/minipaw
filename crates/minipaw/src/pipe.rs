//! The stdin/stdout session both modes run over whatever stream is attached.
//!
//! Two helper threads do the blocking I/O and wake the network thread
//! through the endpoint's [`WaitHandle`]: one reads stdin into a bounded
//! channel, one writes received bytes to stdout. Acks report bytes the
//! writer has actually written, so a slow stdout throttles the remote
//! sender instead of growing a buffer here. A local I/O error is fatal:
//! [`Pipe::failure`] reports it so the session ends nonzero rather than
//! claiming a complete transfer.

use std::cell::Cell;
use std::fmt;
use std::io::{ErrorKind, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use minip2p::{ConnectionId, Endpoint, Error, PeerId, StreamId, TransportError, WaitHandle};

use crate::window::{Inbound, Outbound};
use crate::wire::{Frame, FrameReader, MAX_DATA};

use crate::io::Io;

/// A non-urgent ack waits this long after the first unacked write, so it
/// covers more of them.
const ACK_DELAY: Duration = Duration::from_millis(10);
/// minip2p has no writable event, so a full send buffer is retried after
/// this pause.
pub const BACKPRESSURE_RETRY: Duration = Duration::from_millis(5);
/// A link that has sent nothing for this long sends a `Ping`.
const PING_INTERVAL: Duration = Duration::from_secs(5);
/// A link that has heard nothing for this long is dead. minip2p does not
/// always report a relayed circuit closing on one side (deepso7/minip2p#306),
/// so without this a session could wait on a dead stream for ever. A
/// relayed path delivers whole Yamux frames, up to `MAX_DATA` each, so a
/// working path slower than `MAX_DATA` per `DEAD_AFTER` (about 1.6 KiB/s)
/// looks dead too; it then keeps resuming from its last acked byte.
const DEAD_AFTER: Duration = Duration::from_secs(20);

pub enum SendError {
    /// The stream's send buffer is full; the frame was not queued.
    Full,
    Dead(String),
}

/// A refused write that is backpressure rather than a dead stream: QUIC's
/// full write queue, or a full Yamux buffer on a relayed circuit.
fn is_backpressure(error: &Error) -> bool {
    match error {
        Error::Transport(TransportError::ResourceExhausted { .. }) => true,
        Error::Transport(TransportError::StreamSendFailed { reason, .. }) => {
            reason.contains("send buffer is full")
        }
        _ => false,
    }
}

/// The stream currently carrying the session.
pub struct Link {
    pub peer: PeerId,
    pub conn: ConnectionId,
    pub stream: StreamId,
    pub reader: FrameReader,
    /// When the peer last sent anything on this stream.
    heard: Instant,
    /// When we last queued a frame on it.
    sent: Cell<Instant>,
}

impl Link {
    pub fn new(peer: PeerId, conn: ConnectionId, stream: StreamId) -> Self {
        let now = Instant::now();
        Link {
            peer,
            conn,
            stream,
            reader: FrameReader::default(),
            heard: now,
            sent: Cell::new(now),
        }
    }

    /// Takes data the peer sent on this stream.
    pub fn push(&mut self, data: &[u8]) {
        self.heard = Instant::now();
        self.reader.push(data);
    }

    /// When the link counts as dead unless the peer is heard from.
    pub fn dead_at(&self) -> Instant {
        self.heard + DEAD_AFTER
    }

    fn ping_at(&self) -> Instant {
        self.sent.get() + PING_INTERVAL
    }

    pub fn is(&self, peer: &PeerId, conn: ConnectionId, stream: StreamId) -> bool {
        self.peer == *peer && self.conn == conn && self.stream == stream
    }

    pub fn try_send(&self, endpoint: &mut Endpoint, frame: &Frame) -> Result<(), SendError> {
        endpoint
            .send_stream(&self.peer, self.conn, self.stream, frame.encode())
            .map_err(|e| {
                if is_backpressure(&e) {
                    SendError::Full
                } else {
                    SendError::Dead(e.to_string())
                }
            })?;
        self.sent.set(Instant::now());
        Ok(())
    }

    /// Sends a frame that must go out now, such as a handshake.
    pub fn send(&self, endpoint: &mut Endpoint, frame: &Frame) -> Result<(), String> {
        self.try_send(endpoint, frame).map_err(|e| match e {
            SendError::Full => "send buffer full".into(),
            SendError::Dead(e) => e,
        })
    }

    /// Drops the stream and any events still buffered for it.
    pub fn abandon(self, endpoint: &mut Endpoint) {
        if let Err(e) = endpoint.abandon_stream(&self.peer, self.conn, self.stream) {
            log::debug!("abandon stream {}: {e}", self.stream);
        }
    }
}

enum Input {
    Data(Vec<u8>),
    Eof,
}

/// Keeps a value on its own cache line, so threads updating neighbours do
/// not contend for it.
#[derive(Default)]
#[repr(align(128))]
pub struct CachePadded<T>(pub T);

/// Session byte counters, for progress reports. The session thread keeps
/// all but `written` with relaxed stores; the output thread counts
/// `written`, and acks report it, so it is read with `Acquire`.
#[derive(Default)]
pub struct Stats {
    /// Bytes taken from local input.
    pub read: AtomicU64,
    /// Of those, bytes the peer has confirmed writing.
    pub acked: AtomicU64,
    /// Bytes received from the peer.
    pub received: AtomicU64,
    /// Of those, bytes written to local output.
    pub written: CachePadded<AtomicU64>,
}

/// A local I/O error that ends the session.
#[derive(Debug)]
pub enum LocalFailure {
    /// Reading local input failed.
    Read(std::io::Error),
    /// Writing local output failed (a closed reader is not a failure).
    Write(std::io::Error),
}

impl fmt::Display for LocalFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocalFailure::Read(e) => write!(f, "reading input: {e}"),
            LocalFailure::Write(e) => write!(f, "writing output: {e}"),
        }
    }
}

impl std::error::Error for LocalFailure {}

#[derive(Default)]
struct FailureSlot {
    /// Whether any failure happened, even once taken.
    hit: bool,
    first: Option<LocalFailure>,
}

/// The first local I/O error either helper thread hit.
#[derive(Clone, Default)]
struct Failure(Arc<Mutex<FailureSlot>>);

impl Failure {
    fn set(&self, failure: LocalFailure) {
        if let Ok(mut slot) = self.0.lock()
            && !slot.hit
        {
            slot.hit = true;
            slot.first = Some(failure);
        }
    }

    fn is_set(&self) -> bool {
        self.0.lock().is_ok_and(|slot| slot.hit)
    }

    fn take(&self) -> Option<LocalFailure> {
        self.0.lock().ok().and_then(|mut slot| slot.first.take())
    }
}

pub struct Pipe {
    out: Outbound,
    inb: Inbound,
    stdin: Receiver<Input>,
    stdin_open: bool,
    /// Interactive stdin never ends on its own, so the peer finishing ends
    /// our side too; piped input is always sent through to its EOF.
    close_on_peer_fin: bool,
    stdout: Option<Sender<Vec<u8>>>,
    writer: Option<JoinHandle<()>>,
    /// Signals once the writer has flushed and exited.
    writer_done: Receiver<()>,
    stats: Arc<Stats>,
    failure: Failure,
    ack_due: Option<Instant>,
    /// Sends paused by backpressure until then.
    blocked_until: Option<Instant>,
}

impl Pipe {
    pub fn new(wake: &WaitHandle, io: Io, stats: Arc<Stats>) -> Self {
        let Io {
            input,
            output,
            close_on_peer_fin,
        } = io;
        let failure = Failure::default();
        let (stdin_tx, stdin) = mpsc::sync_channel(8);
        spawn_input(input, stdin_tx, failure.clone(), wake.clone());
        let (stdout, stdout_rx) = mpsc::channel();
        let (done_tx, writer_done) = mpsc::channel();
        let writer = spawn_output(
            output,
            stdout_rx,
            stats.clone(),
            failure.clone(),
            done_tx,
            wake.clone(),
        );
        Pipe {
            out: Outbound::default(),
            inb: Inbound::default(),
            stdin,
            stdin_open: true,
            close_on_peer_fin,
            stdout: Some(stdout),
            writer: Some(writer),
            writer_done,
            stats,
            failure,
            ack_due: None,
            blocked_until: None,
        }
    }

    fn written(&self) -> u64 {
        self.stats.written.0.load(Ordering::Acquire)
    }

    /// Session bytes received so far: the resume point we announce.
    pub fn recv_offset(&self) -> u64 {
        self.inb.recv
    }

    /// Whether [`attach`](Self::attach) would accept `peer_recv`.
    pub fn check_attach(&self, peer_recv: u64) -> Result<(), String> {
        self.out.check_rewind(peer_recv)
    }

    /// A new stream is attached and the peer has received `peer_recv`
    /// bytes: resend from there, and re-ack on the new stream.
    pub fn attach(&mut self, peer_recv: u64) -> Result<(), String> {
        log::debug!(
            "resuming: peer has {peer_recv} bytes of ours, we have {} of theirs ({} written)",
            self.inb.recv,
            self.written()
        );
        self.out.rewind(peer_recv)?;
        self.inb.resume();
        self.ack_due = None;
        self.blocked_until = None;
        Ok(())
    }

    pub fn on_frame(&mut self, frame: Frame) -> Result<(), Box<dyn std::error::Error>> {
        match frame {
            Frame::Data(data) => {
                self.inb.on_data(data.len(), self.written())?;
                self.stats.received.store(self.inb.recv, Ordering::Relaxed);
                // A writer that stopped on a local error has already
                // reported it; what still arrives has nowhere to go.
                if let Some(stdout) = &self.stdout
                    && stdout.send(data).is_err()
                    && !self.failure.is_set()
                {
                    return Err("stdout writer exited".into());
                }
            }
            Frame::Ack { offset, fin } => {
                self.out.ack(offset, fin)?;
                self.stats.acked.store(self.out.acked(), Ordering::Relaxed);
            }
            Frame::Fin { offset } => {
                self.inb.on_fin(offset)?;
                if self.close_on_peer_fin {
                    self.stdin_open = false;
                    self.out.close();
                }
            }
            Frame::Error(message) => {
                return Err(crate::net::PeerEnded(format!(
                    "peer ended the session: {}",
                    message.escape_debug()
                ))
                .into());
            }
            // Hearing it is all that matters, and `Link::push` saw to that.
            Frame::Ping => {}
            Frame::Hello { .. } | Frame::Welcome { .. } => {
                return Err("unexpected handshake frame mid-session".into());
            }
        }
        Ok(())
    }

    /// Moves stdin into the send buffer and, with a link attached, puts
    /// pending data, `Fin`, and acks on it. An error means the link is dead.
    pub fn pump(&mut self, endpoint: &mut Endpoint, link: Option<&Link>) -> Result<(), String> {
        self.pull_stdin();
        let Some(link) = link else {
            // Send timers only matter with a link; a stale one would hand
            // `Endpoint::wait` a past deadline, which returns at once
            // without driving the endpoint.
            self.ack_due = None;
            self.blocked_until = None;
            return Ok(());
        };
        let now = Instant::now();
        if self.blocked_until.is_some_and(|until| now < until) {
            return Ok(());
        }
        self.blocked_until = None;
        match self.send_pending(endpoint, link, now) {
            Ok(()) => Ok(()),
            Err(SendError::Full) => {
                self.blocked_until = Some(now + BACKPRESSURE_RETRY);
                Ok(())
            }
            Err(SendError::Dead(e)) => Err(e),
        }
    }

    fn send_pending(
        &mut self,
        endpoint: &mut Endpoint,
        link: &Link,
        now: Instant,
    ) -> Result<(), SendError> {
        let written = self.written();
        if self.inb.ack_pending(written) {
            if self.inb.ack_urgent(written) || self.ack_due.is_some_and(|due| now >= due) {
                let (offset, fin) = self.inb.ack(written);
                link.try_send(endpoint, &Frame::Ack { offset, fin })?;
                self.inb.mark_acked(offset, fin);
                self.ack_due = None;
            } else if self.ack_due.is_none() {
                self.ack_due = Some(now + ACK_DELAY);
            }
        } else {
            self.ack_due = None;
        }

        while let Some(chunk) = self.out.next_chunk() {
            let len = chunk.len();
            if let Err(e) = link.try_send(endpoint, &Frame::Data(chunk)) {
                self.out.unsend(len);
                return Err(e);
            }
        }
        if let Some(offset) = self.out.fin_due() {
            link.try_send(endpoint, &Frame::Fin { offset })?;
            self.out.mark_fin_sent();
        }
        if now >= link.ping_at() {
            link.try_send(endpoint, &Frame::Ping)?;
        }
        Ok(())
    }

    fn pull_stdin(&mut self) {
        while self.stdin_open && self.out.has_room() {
            match self.stdin.try_recv() {
                Ok(Input::Data(data)) => {
                    self.out.push(&data);
                    self.stats.read.store(self.out.end(), Ordering::Relaxed);
                }
                Ok(Input::Eof) | Err(TryRecvError::Disconnected) => {
                    self.stdin_open = false;
                    self.out.close();
                }
                Err(TryRecvError::Empty) => break,
            }
        }
    }

    /// When `pump` next has timed work on `link`: a backpressure retry, or
    /// else a delayed ack or a ping (which cannot go out while sends are
    /// blocked anyway).
    pub fn deadline(&self, link: Option<&Link>) -> Option<Instant> {
        let link = link?;
        self.blocked_until.or_else(|| {
            let ping = link.ping_at();
            Some(self.ack_due.map_or(ping, |due| due.min(ping)))
        })
    }

    /// The local I/O error that ends the session, if one happened and was
    /// not already taken.
    pub fn take_failure(&self) -> Option<LocalFailure> {
        self.failure.take()
    }

    fn peer_finished(&self) -> bool {
        self.inb.fin.is_some_and(|fin| fin == self.written())
    }

    /// Both directions are complete and confirmed.
    pub fn done(&self) -> bool {
        self.inb.finished(self.written()) && self.out.fin_acked
    }

    /// Both directions' data is confirmed: everything the peer sent is
    /// written, and the peer acked writing everything we sent. Only our ack
    /// of its `Fin` may be unconfirmed, so with the link gone nothing can be
    /// lost by stopping.
    pub fn delivered(&self) -> bool {
        self.peer_finished() && self.out.fin_acked
    }

    /// Flushes stdout and stops the writer, waiting at most `limit` (or for
    /// ever) for a blocked stdout. Idempotent.
    pub fn finish(&mut self, limit: Option<Duration>) {
        self.stdout = None;
        let Some(writer) = self.writer.take() else {
            return;
        };
        if let Some(limit) = limit
            && self.writer_done.recv_timeout(limit).is_err()
        {
            // Still blocked on stdout; leave it behind.
            return;
        }
        if writer.join().is_err() {
            log::error!("stdout writer panicked");
        }
    }
}

fn spawn_input(
    mut reader: Box<dyn Read + Send>,
    tx: SyncSender<Input>,
    failure: Failure,
    wake: WaitHandle,
) {
    thread::spawn(move || {
        let mut buf = vec![0u8; MAX_DATA];
        loop {
            let input = match reader.read(&mut buf) {
                Ok(0) => Input::Eof,
                Ok(n) => Input::Data(buf.get(..n).unwrap_or_default().to_vec()),
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => {
                    failure.set(LocalFailure::Read(e));
                    wake.interrupt();
                    return;
                }
            };
            let eof = matches!(input, Input::Eof);
            if tx.send(input).is_err() {
                return;
            }
            wake.interrupt();
            if eof {
                return;
            }
        }
    });
}

fn spawn_output(
    mut output: Box<dyn Write + Send>,
    rx: Receiver<Vec<u8>>,
    stats: Arc<Stats>,
    failure: Failure,
    done: Sender<()>,
    wake: WaitHandle,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut closed = false;
        for data in rx {
            // A reader that went away (`| head`) wants no more output, like
            // netcat: keep counting so the session still finishes.
            if !closed && let Err(e) = output.write_all(&data).and_then(|()| output.flush()) {
                if e.kind() != ErrorKind::BrokenPipe {
                    failure.set(LocalFailure::Write(e));
                    wake.interrupt();
                    break;
                }
                closed = true;
            }
            stats
                .written
                .0
                .fetch_add(data.len() as u64, Ordering::Release);
            wake.interrupt();
        }
        // The receiver is gone once the session has ended.
        if done.send(()).is_err() {}
    })
}
