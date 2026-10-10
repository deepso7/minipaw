//! The stdin/stdout session both modes run over whatever stream is attached.
//!
//! Two helper threads do the blocking I/O and wake the network thread
//! through the endpoint's [`WaitHandle`]: one reads stdin into a bounded
//! channel, one writes received bytes to stdout. Acks report bytes the
//! writer has actually written, so a slow stdout throttles the remote
//! sender instead of growing a buffer here. A local I/O error is fatal:
//! [`Pipe::failure`] reports it so the session ends nonzero rather than
//! claiming a complete transfer.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::io::{ErrorKind, Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use minip2p::{Bytes, ConnectionId, Endpoint, Error, PeerId, StreamId, TransportError, WaitHandle};

use crate::window::{Inbound, Outbound};
use crate::wire::{Frame, FrameReader, MAX_DATA};

use crate::io::{Cancel, Io};

/// Chunks read ahead of the send buffer, waiting in the input channel.
const INPUT_QUEUE: usize = 8;
/// A non-urgent ack waits this long after the first unacked write, so it
/// covers more of them.
const ACK_DELAY: Duration = Duration::from_millis(10);
/// A stream that refused a write at a resource limit, which arms no
/// `StreamWritable`, is retried after this pause.
const BACKPRESSURE_RETRY: Duration = Duration::from_millis(5);
/// A stream that answered `Full` waits for its `StreamWritable`; this bounds
/// the wait, should the event never come.
const WRITABLE_FALLBACK: Duration = Duration::from_millis(250);
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
    /// The stream's send buffer is full; the frame was not queued, or a
    /// held tail is still unsent.
    Full,
    Dead(String),
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
    /// The end of a frame the stream took only part of. It goes out before
    /// anything else, or the peer would read a torn frame.
    unsent: RefCell<Bytes>,
    /// The stream refused a write: sends wait until then, or until its
    /// `StreamWritable`.
    blocked_until: Cell<Option<Instant>>,
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
            unsent: RefCell::new(Bytes::new()),
            blocked_until: Cell::new(None),
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

    /// Whether sends wait for the stream to take writes again.
    pub fn blocked(&self, now: Instant) -> bool {
        if self.blocked_until.get().is_some_and(|until| now < until) {
            return true;
        }
        self.blocked_until.set(None);
        false
    }

    /// When a blocked stream is tried again without its `StreamWritable`.
    pub fn retry_at(&self) -> Option<Instant> {
        self.blocked_until.get()
    }

    /// The stream's `StreamWritable`: it takes writes again.
    pub fn writable(&self) {
        self.blocked_until.set(None);
    }

    /// Holds sends on the stream for `wait`, or until its `StreamWritable`.
    fn block(&self, wait: Duration) -> SendError {
        self.blocked_until.set(Some(Instant::now() + wait));
        SendError::Full
    }

    /// A refused write is backpressure if the stream hit a resource limit
    /// (a partly taken write is `Error::Full`, handled by the caller);
    /// anything else means the stream is dead.
    fn refused(&self, error: &Error) -> SendError {
        if let Error::Transport(TransportError::ResourceExhausted { .. }) = error {
            self.block(BACKPRESSURE_RETRY)
        } else {
            SendError::Dead(error.to_string())
        }
    }

    /// Queues `frame` after any held tail. `Full` means none of it was
    /// queued; a frame the stream took part of counts as queued, and its
    /// end is held for [`flush`](Self::flush).
    pub fn try_send(&self, endpoint: &mut Endpoint, frame: &Frame) -> Result<(), SendError> {
        self.flush(endpoint)?;
        let data = Bytes::from(frame.encode());
        let len = data.len();
        match endpoint.send_stream(&self.peer, self.conn, self.stream, data) {
            Ok(()) => {}
            Err(Error::Full { unsent, .. }) => {
                let full = self.block(WRITABLE_FALLBACK);
                if unsent.len() == len {
                    return Err(full);
                }
                *self.unsent.borrow_mut() = unsent;
            }
            Err(e) => return Err(self.refused(&e)),
        }
        self.sent.set(Instant::now());
        Ok(())
    }

    /// Sends the held end of a partly queued frame, if any.
    pub fn flush(&self, endpoint: &mut Endpoint) -> Result<(), SendError> {
        let data = self.unsent.take();
        if data.is_empty() {
            return Ok(());
        }
        match endpoint.send_stream(&self.peer, self.conn, self.stream, data) {
            Ok(()) => Ok(()),
            Err(Error::Full { unsent, .. }) => {
                *self.unsent.borrow_mut() = unsent;
                Err(self.block(WRITABLE_FALLBACK))
            }
            Err(e) => Err(self.refused(&e)),
        }
    }

    /// Half-closes our side. A tail the stream still cannot take is lost,
    /// leaving the peer a torn last frame; only a teardown closes, so the
    /// peer treats it as the stream ending.
    pub fn close_write(&self, endpoint: &mut Endpoint) {
        if let Err(SendError::Full) = self.flush(endpoint) {
            log::debug!(
                "closing stream {} with {} bytes unsent",
                self.stream,
                self.unsent.borrow().len()
            );
        }
        if let Err(e) = endpoint.close_stream_write(&self.peer, self.conn, self.stream) {
            log::debug!("close stream: {e}");
        }
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
    /// Writing local output failed (for [`Io::stdio`], a closed reader is
    /// not a failure).
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
    /// Input read before the peer's `Fin` closed ours, still to be sent.
    held: VecDeque<Vec<u8>>,
    /// Interactive stdin never ends on its own, so the peer finishing ends
    /// our side too; piped input is always sent through to its EOF.
    close_on_peer_fin: bool,
    /// Dropped once the peer's `Fin` arrives, so the writer drains what it
    /// has and then drops the output.
    stdout: Option<Sender<Vec<u8>>>,
    writer: Option<JoinHandle<()>>,
    /// Signals once the writer has flushed and exited.
    writer_done: Receiver<()>,
    /// Ends the helper threads' blocking calls at teardown, if `Io` can.
    cancel: Option<Cancel>,
    stats: Arc<Stats>,
    failure: Failure,
    ack_due: Option<Instant>,
}

impl Pipe {
    pub fn new(wake: &WaitHandle, io: Io, stats: Arc<Stats>) -> Self {
        let Io {
            input,
            output,
            close_on_peer_fin,
            quiet_broken_pipe,
            cancel,
        } = io;
        let failure = Failure::default();
        let (stdin_tx, stdin) = mpsc::sync_channel(INPUT_QUEUE);
        spawn_input(input, stdin_tx, failure.clone(), wake.clone());
        let (stdout, stdout_rx) = mpsc::channel();
        let (done_tx, writer_done) = mpsc::channel();
        let writer = spawn_output(
            output,
            quiet_broken_pipe,
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
            held: VecDeque::new(),
            close_on_peer_fin,
            stdout: Some(stdout),
            writer: Some(writer),
            writer_done,
            cancel,
            stats,
            failure,
            ack_due: None,
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
                // Nothing more will come: the writer drains what it has,
                // then drops the output (half-closing a socket).
                self.stdout = None;
                if self.close_on_peer_fin && self.stdin_open {
                    // Input already read still goes out, even past a full
                    // window; only what was not read yet is cut off. Take
                    // just the queue's backlog: a busy reader refills it.
                    self.stdin_open = false;
                    for _ in 0..INPUT_QUEUE {
                        let Ok(Input::Data(data)) = self.stdin.try_recv() else {
                            break;
                        };
                        self.held.push_back(data);
                    }
                    self.pull_stdin();
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
            // Send timers only matter with a link; a stale one would keep
            // the loop from sleeping.
            self.ack_due = None;
            return Ok(());
        };
        let now = Instant::now();
        if link.blocked(now) {
            return Ok(());
        }
        match self.send_pending(endpoint, link, now) {
            // The link holds when to try again.
            Ok(()) | Err(SendError::Full) => Ok(()),
            Err(SendError::Dead(e)) => Err(e),
        }
    }

    fn send_pending(
        &mut self,
        endpoint: &mut Endpoint,
        link: &Link,
        now: Instant,
    ) -> Result<(), SendError> {
        link.flush(endpoint)?;
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
        if !self.stdin_open {
            while self.out.has_room()
                && let Some(data) = self.held.pop_front()
            {
                self.push_input(&data);
            }
            if self.held.is_empty() {
                self.out.close();
            }
            return;
        }
        while self.out.has_room() {
            match self.stdin.try_recv() {
                Ok(Input::Data(data)) => self.push_input(&data),
                Ok(Input::Eof) | Err(TryRecvError::Disconnected) => {
                    self.stdin_open = false;
                    self.out.close();
                }
                Err(TryRecvError::Empty) => break,
            }
        }
    }

    fn push_input(&mut self, data: &[u8]) {
        self.out.push(data);
        self.stats.read.store(self.out.end(), Ordering::Relaxed);
    }

    /// When `pump` next has timed work on `link`: a backpressure retry, or
    /// else a delayed ack or a ping (which cannot go out while sends are
    /// blocked anyway). A `StreamWritable` wakes it sooner.
    pub fn deadline(&self, link: Option<&Link>) -> Option<Instant> {
        let link = link?;
        link.retry_at().or_else(|| {
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

    /// Our ack of everything the peer sent, `Fin` included, for a peer that
    /// lost it with its stream once the session was complete.
    pub fn final_ack(&self) -> Frame {
        let (offset, fin) = self.inb.ack(self.written());
        Frame::Ack { offset, fin }
    }

    /// Lets the writer finish: it writes what it has, then drops the
    /// output. See [`output_closed`](Self::output_closed).
    pub fn close_output(&mut self) {
        self.stdout = None;
    }

    /// Whether the writer has exited, after
    /// [`close_output`](Self::close_output); never blocks. The endpoint's
    /// wait wakes when it does.
    pub fn output_closed(&mut self) -> bool {
        if self.writer.is_none() {
            return true;
        }
        match self.writer_done.try_recv() {
            Err(TryRecvError::Empty) => false,
            // Sent just before it exits, its writer already dropped, or
            // it is gone: joining cannot block for long.
            Ok(()) | Err(TryRecvError::Disconnected) => {
                if let Some(writer) = self.writer.take()
                    && writer.join().is_err()
                {
                    log::error!("stdout writer panicked");
                }
                true
            }
        }
    }

    /// Flushes stdout and stops the writer, waiting at most `limit` (or for
    /// ever) for a blocked stdout, then cancels the helper threads: without
    /// a limit the session ended cleanly, with one it did not. Idempotent.
    pub fn finish(&mut self, limit: Option<Duration>) {
        self.stdout = None;
        if let Some(writer) = self.writer.take() {
            let drained = limit.is_none_or(|limit| self.writer_done.recv_timeout(limit).is_ok());
            // A writer still blocked on stdout is left behind, unless
            // cancelling below ends its write.
            if drained && writer.join().is_err() {
                log::error!("stdout writer panicked");
            }
        }
        if limit.is_none() {
            self.cancel_clean();
        } else {
            self.cancel_abort();
        }
    }

    /// After a clean end, once the output has finished: ends a read still
    /// blocked on an [`Io::tcp`] or `Io::unix` socket. Idempotent; a no-op
    /// for other `Io`s.
    pub fn cancel_clean(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.clean();
        }
    }

    /// After a stop or a failure, once the output had its grace: shuts an
    /// [`Io::tcp`] or `Io::unix` socket down both ways, ending a blocked read and a
    /// write to a target that stopped reading. Idempotent; a no-op for
    /// other `Io`s, and what dropping the pipe does if neither ran.
    pub fn cancel_abort(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.abort();
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
    quiet_broken_pipe: bool,
    rx: Receiver<Vec<u8>>,
    stats: Arc<Stats>,
    failure: Failure,
    done: Sender<()>,
    wake: WaitHandle,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut closed = false;
        for data in rx {
            // A stdout reader that went away (`| head`) wants no more
            // output, like netcat: keep counting so the session still
            // finishes. Any other writer failing is an error.
            if !closed && let Err(e) = output.write_all(&data).and_then(|()| output.flush()) {
                if !(quiet_broken_pipe && e.kind() == ErrorKind::BrokenPipe) {
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
        // Closed before saying so: a writer whose drop blocks (a final
        // flush to a stuck socket) must not hold up whoever joins us.
        drop(output);
        // The receiver is gone once the session has ended.
        if done.send(()).is_ok() {
            wake.interrupt();
        }
    })
}

#[cfg(test)]
mod tests {
    /// A writer whose drop blocks until told to go on.
    struct SlowDrop(std::sync::mpsc::Receiver<()>);

    impl Write for SlowDrop {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Drop for SlowDrop {
        fn drop(&mut self) {
            let _ = self.0.recv_timeout(Duration::from_secs(10));
        }
    }

    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc::RecvTimeoutError;

    use super::*;
    use crate::io::tests::{Socket, on_every_socket};
    use crate::window::WINDOW;

    /// Yields the chunks sent to it, then blocks until the sender is gone,
    /// counting its `read` calls.
    struct ChunkReader(Receiver<Vec<u8>>, Arc<AtomicUsize>);

    impl Read for ChunkReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.1.fetch_add(1, Ordering::SeqCst);
            let Ok(chunk) = self.0.recv() else {
                return Ok(0);
            };
            buf[..chunk.len()].copy_from_slice(&chunk);
            Ok(chunk.len())
        }
    }

    struct BrokenWriter;

    impl Write for BrokenWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn pipe(io: Io) -> Pipe {
        Pipe::new(&WaitHandle::new(|| {}), io, Arc::default())
    }

    #[test]
    fn a_writer_slow_to_drop_does_not_block_checking_the_output() {
        let (go, wait) = mpsc::channel();
        let mut pipe = pipe(Io::new(io::empty(), SlowDrop(wait)));
        pipe.close_output();
        // The writer thread is stuck dropping its writer: not closed yet,
        // and asking must not block.
        let asked = Instant::now();
        thread::sleep(Duration::from_millis(100));
        assert!(!pipe.output_closed());
        assert!(asked.elapsed() < Duration::from_secs(1));
        go.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !pipe.output_closed() {
            assert!(Instant::now() < deadline, "output never closed");
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn peer_fin_sends_input_already_read_past_a_full_window() {
        let (tx, rx) = mpsc::channel();
        let reads = Arc::new(AtomicUsize::new(0));
        let io = Io::new(ChunkReader(rx, reads.clone()), io::sink()).close_on_peer_fin(true);
        let mut pipe = pipe(io);
        let chunks = WINDOW / MAX_DATA + INPUT_QUEUE;
        for _ in 0..chunks {
            tx.send(vec![1; MAX_DATA]).unwrap();
        }
        // The window fills, and once the reader is asked for more than we
        // sent, every chunk it returned is queued for the pipe.
        let deadline = Instant::now() + Duration::from_secs(10);
        while pipe.out.has_room() || reads.load(Ordering::SeqCst) <= chunks {
            assert!(Instant::now() < deadline, "input never filled the window");
            pipe.pull_stdin();
            thread::yield_now();
        }

        pipe.on_frame(Frame::Fin { offset: 0 }).unwrap();
        for _ in 0..=chunks {
            while pipe.out.next_chunk().is_some() {}
            pipe.out.ack(pipe.out.end(), false).unwrap();
            pipe.pull_stdin();
        }
        assert_eq!(pipe.out.end(), (chunks * MAX_DATA) as u64);
        assert_eq!(pipe.out.fin_due(), Some(pipe.out.end()));
    }

    #[test]
    fn peer_fin_takes_only_the_backlog_of_a_busy_reader() {
        let io = Io::new(io::repeat(1), io::sink()).close_on_peer_fin(true);
        let mut pipe = pipe(io);
        let deadline = Instant::now() + Duration::from_secs(10);
        while pipe.out.has_room() {
            assert!(Instant::now() < deadline, "input never filled the window");
            pipe.pull_stdin();
        }
        pipe.on_frame(Frame::Fin { offset: 0 }).unwrap();
        assert!(pipe.held.len() <= INPUT_QUEUE);
    }

    #[test]
    fn peer_fin_half_closes_a_socket_output_while_replies_keep_flowing() {
        fn test<S: Socket>() {
            let (ours, mut theirs) = S::pair();
            let mut pipe = pipe(ours.io());
            pipe.on_frame(Frame::Data(b"request".to_vec())).unwrap();
            pipe.on_frame(Frame::Fin { offset: 7 }).unwrap();
            // EOF arrives without the pipe finishing.
            let mut got = Vec::new();
            theirs.read_to_end(&mut got).unwrap();
            assert_eq!(got, b"request");

            theirs.write_all(b"reply").unwrap();
            theirs.shut(std::net::Shutdown::Write);
            let deadline = Instant::now() + Duration::from_secs(10);
            while pipe.stdin_open {
                assert!(Instant::now() < deadline, "reply never ended");
                pipe.pull_stdin();
                thread::yield_now();
            }
            assert_eq!(pipe.out.next_chunk().as_deref(), Some(&b"reply"[..]));
            pipe.finish(None);
            assert!(pipe.take_failure().is_none());
        }
        on_every_socket!(test);
    }

    #[test]
    fn a_clean_finish_ends_the_input_thread_of_a_target_still_open() {
        fn test<S: Socket>() {
            // The target neither sends nor closes.
            let (ours, _theirs) = S::pair();
            let mut pipe = pipe(ours.io());
            pipe.on_frame(Frame::Fin { offset: 0 }).unwrap();
            pipe.finish(None);
            match pipe.stdin.recv_timeout(Duration::from_secs(10)) {
                Ok(Input::Eof) | Err(RecvTimeoutError::Disconnected) => {}
                Ok(Input::Data(_)) => panic!("the target sent nothing"),
                Err(RecvTimeoutError::Timeout) => panic!("input still blocked"),
            }
        }
        on_every_socket!(test);
    }

    /// A target that won't read until it has written, like a russh server,
    /// still gets its writes through while ours to it are blocked.
    #[cfg(unix)]
    #[test]
    fn input_keeps_flowing_while_output_to_a_unix_target_is_blocked() {
        use std::os::unix::net::UnixStream;

        let (ours, mut theirs) = <UnixStream as Socket>::pair();
        let probe = ours.try_clone().unwrap();
        // Whether the socket's buffer is full, so a write to it blocks. A
        // byte that fits is harmless: the target never reads.
        let full = || match rustix::net::send(&probe, &[0], rustix::net::SendFlags::DONTWAIT) {
            Ok(_) => false,
            Err(e) if e == rustix::io::Errno::AGAIN => true,
            Err(e) => panic!("probe: {e}"),
        };
        let mut pipe = pipe(ours.io());
        // A full window is far more than a Unix socket buffers.
        for _ in 0..WINDOW / MAX_DATA {
            pipe.on_frame(Frame::Data(vec![1; MAX_DATA])).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !full() {
            assert!(Instant::now() < deadline, "the socket never filled");
            thread::sleep(Duration::from_millis(10));
        }

        theirs.write_all(b"reply").unwrap();
        let mut got = Vec::new();
        while got.len() < 5 {
            assert!(Instant::now() < deadline, "reply never read");
            pipe.pull_stdin();
            while let Some(chunk) = pipe.out.next_chunk() {
                got.extend_from_slice(&chunk);
            }
            thread::yield_now();
        }
        assert_eq!(got, b"reply");
        // Nothing drained the socket, so the writer is still blocked.
        assert!(full());
        assert!(pipe.written() < WINDOW as u64);
        pipe.finish(Some(Duration::from_millis(50)));
    }

    #[test]
    fn unclean_finish_ends_threads_blocked_on_a_socket_target() {
        on_every_socket!(unclean_finish_ends_threads_blocked_on);
    }

    fn unclean_finish_ends_threads_blocked_on<S: Socket>() {
        for _ in 0..10 {
            // The target neither reads nor sends.
            let (ours, _theirs) = S::pair();
            let mut pipe = pipe(ours.io());
            for _ in 0..WINDOW / MAX_DATA {
                pipe.on_frame(Frame::Data(vec![1; MAX_DATA])).unwrap();
            }
            pipe.finish(Some(Duration::from_millis(50)));

            // The writer signals done, or is gone if it already had.
            let wait = Duration::from_secs(10);
            assert!(matches!(
                pipe.writer_done.recv_timeout(wait),
                Ok(()) | Err(RecvTimeoutError::Disconnected)
            ));
            loop {
                match pipe.stdin.recv_timeout(wait) {
                    Ok(Input::Eof) | Err(RecvTimeoutError::Disconnected) => break,
                    Ok(Input::Data(_)) => {}
                    Err(RecvTimeoutError::Timeout) => panic!("input still blocked"),
                }
            }
        }
    }

    #[test]
    fn a_closed_output_drains_without_blocking_the_caller() {
        let (tx, rx) = mpsc::channel();
        let mut pipe = pipe(Io::new(io::empty(), ChannelWriter(tx)));
        pipe.on_frame(Frame::Data(b"last words".to_vec())).unwrap();
        pipe.on_frame(Frame::Fin { offset: 10 }).unwrap();
        pipe.close_output();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !pipe.output_closed() {
            assert!(Instant::now() < deadline, "writer never exited");
            thread::yield_now();
        }
        assert!(pipe.writer.is_none());
        assert_eq!(rx.try_iter().flatten().collect::<Vec<u8>>(), b"last words");
        // Everything is written, so the ack covers the Fin.
        assert_eq!(
            pipe.final_ack(),
            Frame::Ack {
                offset: 10,
                fin: true
            }
        );
        assert!(pipe.output_closed());
    }

    /// Passes what is written on to a channel.
    struct ChannelWriter(Sender<Vec<u8>>);

    impl Write for ChannelWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .send(buf.to_vec())
                .map_err(|_| ErrorKind::BrokenPipe)?;
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn broken_pipe_from_a_custom_writer_is_an_error() {
        let mut pipe = pipe(Io::new(io::empty(), BrokenWriter));
        pipe.on_frame(Frame::Data(vec![1; 16])).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match pipe.take_failure() {
                Some(LocalFailure::Write(e)) => {
                    assert_eq!(e.kind(), ErrorKind::BrokenPipe);
                    break;
                }
                Some(other) => panic!("unexpected failure: {other}"),
                None => assert!(Instant::now() < deadline, "no write failure"),
            }
            thread::yield_now();
        }
    }
}
