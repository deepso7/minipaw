//! The stdin/stdout session both modes run over whatever stream is attached.
//!
//! Two helper threads do the blocking I/O and wake the network thread
//! through the endpoint's [`WaitHandle`]: one reads stdin into a bounded
//! channel, one writes received bytes to stdout. Acks report bytes the
//! writer has actually written, so a slow stdout throttles the remote
//! sender instead of growing a buffer here. A local I/O error is fatal:
//! [`Pipe::failure`] reports it so the session ends nonzero rather than
//! claiming a complete transfer.

use std::io::{ErrorKind, IsTerminal as _, Read as _, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use minip2p::{ConnectionId, Endpoint, Error, PeerId, StreamId, TransportError, WaitHandle};

use crate::session::{Inbound, Outbound};
use crate::wire::{Frame, FrameReader, MAX_DATA};

/// Ack at least this often, in written bytes...
const ACK_EVERY: u64 = 256 * 1024;
/// ...or this long after the first unacked write.
const ACK_DELAY: Duration = Duration::from_millis(10);
/// minip2p has no writable event, so a full send buffer is retried after
/// this pause.
const BACKPRESSURE_RETRY: Duration = Duration::from_millis(5);

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
}

impl Link {
    pub fn new(peer: PeerId, conn: ConnectionId, stream: StreamId) -> Self {
        Link {
            peer,
            conn,
            stream,
            reader: FrameReader::default(),
        }
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
            })
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
            crate::debug!("abandon stream {}: {e}", self.stream);
        }
    }
}

enum Input {
    Data(Vec<u8>),
    Eof,
}

/// The first local I/O error either helper thread hit.
#[derive(Clone, Default)]
struct Failure(Arc<Mutex<Option<String>>>);

impl Failure {
    fn set(&self, message: String) {
        if let Ok(mut slot) = self.0.lock() {
            slot.get_or_insert(message);
        }
    }

    fn get(&self) -> Option<String> {
        self.0.lock().ok().and_then(|slot| slot.clone())
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
    written: Arc<AtomicU64>,
    failure: Failure,
    /// Written offset last acked to the peer, and whether that ack covered
    /// its `Fin`.
    acked: u64,
    fin_acked: bool,
    ack_due: Option<Instant>,
    /// Sends paused by backpressure until then.
    blocked_until: Option<Instant>,
}

impl Pipe {
    pub fn new(wake: &WaitHandle) -> Self {
        let failure = Failure::default();
        let (stdin_tx, stdin) = mpsc::sync_channel(8);
        spawn_stdin(stdin_tx, failure.clone(), wake.clone());
        let (stdout, stdout_rx) = mpsc::channel();
        let (done_tx, writer_done) = mpsc::channel();
        let written = Arc::new(AtomicU64::new(0));
        let writer = spawn_stdout(
            stdout_rx,
            written.clone(),
            failure.clone(),
            done_tx,
            wake.clone(),
        );
        Pipe {
            out: Outbound::default(),
            inb: Inbound::default(),
            stdin,
            stdin_open: true,
            close_on_peer_fin: std::io::stdin().is_terminal(),
            stdout: Some(stdout),
            writer: Some(writer),
            writer_done,
            written,
            failure,
            acked: 0,
            fin_acked: false,
            ack_due: None,
            blocked_until: None,
        }
    }

    /// Session bytes received so far: the resume point we announce.
    pub fn recv_offset(&self) -> u64 {
        self.inb.recv
    }

    /// A new stream is attached and the peer has received `peer_recv`
    /// bytes: resend from there, and re-ack on the new stream.
    pub fn attach(&mut self, peer_recv: u64) -> Result<(), String> {
        crate::debug!(
            "resuming: peer has {peer_recv} bytes of ours, we have {} of theirs ({} written)",
            self.inb.recv,
            self.written.load(Ordering::Acquire)
        );
        self.out.rewind(peer_recv)?;
        self.fin_acked = false;
        self.ack_due = Some(Instant::now());
        self.blocked_until = None;
        Ok(())
    }

    pub fn on_frame(&mut self, frame: Frame) -> Result<(), Box<dyn std::error::Error>> {
        match frame {
            Frame::Data(data) => {
                self.inb
                    .on_data(data.len(), self.written.load(Ordering::Acquire))?;
                if let Some(stdout) = &self.stdout
                    && stdout.send(data).is_err()
                {
                    return Err("stdout writer exited".into());
                }
            }
            Frame::Ack { offset, fin } => self.out.ack(offset, fin)?,
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
        let written = self.written.load(Ordering::Acquire);
        let fin_consumed = self.inb.fin == Some(written);
        if written > self.acked || (fin_consumed && !self.fin_acked) {
            let due = fin_consumed
                || written - self.acked >= ACK_EVERY
                || self.ack_due.is_some_and(|due| now >= due);
            if due {
                let ack = Frame::Ack {
                    offset: written,
                    fin: fin_consumed,
                };
                link.try_send(endpoint, &ack)?;
                self.acked = written;
                self.fin_acked = fin_consumed;
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
        Ok(())
    }

    fn pull_stdin(&mut self) {
        while self.stdin_open && self.out.has_room() {
            match self.stdin.try_recv() {
                Ok(Input::Data(data)) => self.out.push(&data),
                Ok(Input::Eof) | Err(TryRecvError::Disconnected) => {
                    self.stdin_open = false;
                    self.out.close();
                }
                Err(TryRecvError::Empty) => break,
            }
        }
    }

    /// When `pump` next has timed work: a backpressure retry, or else a
    /// delayed ack (which cannot go out while sends are blocked anyway).
    pub fn deadline(&self) -> Option<Instant> {
        self.blocked_until.or(self.ack_due)
    }

    /// A local I/O error that ends the session.
    pub fn failure(&self) -> Option<String> {
        self.failure.get()
    }

    fn peer_finished(&self) -> bool {
        self.inb
            .fin
            .is_some_and(|fin| fin == self.written.load(Ordering::Acquire))
    }

    /// Both directions are complete and confirmed.
    pub fn done(&self) -> bool {
        self.peer_finished() && self.fin_acked && self.out.fin_acked
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
            eprintln!("minipaw: stdout writer panicked");
        }
    }
}

fn spawn_stdin(tx: SyncSender<Input>, failure: Failure, wake: WaitHandle) {
    thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buf = vec![0u8; MAX_DATA];
        loop {
            let input = match stdin.read(&mut buf) {
                Ok(0) => Input::Eof,
                Ok(n) => Input::Data(buf.get(..n).unwrap_or_default().to_vec()),
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => {
                    failure.set(format!("reading stdin: {e}"));
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

fn spawn_stdout(
    rx: Receiver<Vec<u8>>,
    written: Arc<AtomicU64>,
    failure: Failure,
    done: Sender<()>,
    wake: WaitHandle,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut stdout = std::io::stdout().lock();
        let mut closed = false;
        for data in rx {
            // A reader that went away (`| head`) wants no more output, like
            // netcat: keep counting so the session still finishes.
            if !closed && let Err(e) = stdout.write_all(&data).and_then(|()| stdout.flush()) {
                if e.kind() != ErrorKind::BrokenPipe {
                    failure.set(format!("writing stdout: {e}"));
                    wake.interrupt();
                    break;
                }
                closed = true;
            }
            written.fetch_add(data.len() as u64, Ordering::Release);
            wake.interrupt();
        }
        // The receiver is gone once the session has ended.
        if done.send(()).is_err() {}
    })
}
