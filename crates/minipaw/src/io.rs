//! Where a session's bytes come from and go to.

use std::io::{IsTerminal as _, Read, Write};
use std::net::{Shutdown, TcpStream};

/// The local ends of a session: bytes read from `input` go to the peer, and
/// bytes from the peer are written to `output`.
///
/// Each runs on its own helper thread, so both may block. Once the peer
/// has finished sending and all of it is written, the output thread drops
/// `output`.
///
/// A blocking read cannot be cancelled, so the input thread can outlive
/// [`Session::run`](crate::Session::run): it stays parked in `read` until
/// that returns, then sees the session is gone and exits without using
/// what it read. With a reader you own, end it once `run` returns (for a
/// socket, `shutdown`; for a channel, drop the sender). With
/// [`Io::stdio`], the thread may take one more chunk of stdin, so run one
/// stdio session per process.
///
/// The output thread can outlive it too: when the session stops or fails,
/// `run` waits up to a second for a blocked `write`, then leaves the thread
/// behind. It still owns `output`, finishes writing the data it had already
/// received, and drops `output` after that. With a writer you own, unblock
/// it to end it early (for a socket, `shutdown`). [`Io::tcp`] does both of
/// these itself.
pub struct Io {
    pub(crate) input: Box<dyn Read + Send>,
    pub(crate) output: Box<dyn Write + Send>,
    pub(crate) close_on_peer_fin: bool,
    /// A closed stdout reader (`| head`) is not an error.
    pub(crate) quiet_broken_pipe: bool,
    /// Ends the helper threads' blocking calls at teardown, for an `Io`
    /// that can.
    pub(crate) cancel: Option<Cancel>,
}

impl Io {
    /// A session over `input` and `output`. Input is always sent through to
    /// its end; see [`close_on_peer_fin`](Self::close_on_peer_fin).
    pub fn new(input: impl Read + Send + 'static, output: impl Write + Send + 'static) -> Self {
        Io {
            input: Box::new(input),
            output: Box::new(output),
            close_on_peer_fin: false,
            quiet_broken_pipe: false,
            cancel: None,
        }
    }

    /// The process's stdin and stdout. Interactive stdin never ends on its
    /// own, so when it is a terminal the peer finishing ends our side too.
    /// As with netcat, stdout's reader going away (`| head`) is not an
    /// error: the rest of the peer's data is discarded.
    ///
    /// Dropping a stdout handle does not close it, so stdout stays open
    /// until the process exits, even after the peer finished.
    pub fn stdio() -> Self {
        let mut io = Io::new(std::io::stdin(), std::io::stdout())
            .close_on_peer_fin(std::io::stdin().is_terminal());
        io.quiet_broken_pipe = true;
        io
    }

    /// Both directions of a TCP connection, such as one to a local service
    /// being forwarded.
    ///
    /// Half-closes are passed on: once the peer has finished sending and all
    /// of it is written, the socket's write side is shut down, so the target
    /// reads EOF right then while its replies keep flowing back until it
    /// closes its own side.
    ///
    /// No helper thread outlives the session. When it ends cleanly, the
    /// socket's read side is shut down, ending the input thread. Otherwise,
    /// once a blocked write has had its grace (see [`Io`]), the whole socket
    /// is shut down, which also ends a write to a target that stopped
    /// reading.
    ///
    /// # Errors
    ///
    /// Cloning the socket's handle failed.
    pub fn tcp(stream: TcpStream) -> std::io::Result<Self> {
        let input = stream.try_clone()?;
        let cancel = Cancel(Some(stream.try_clone()?));
        let mut io = Io::new(input, TcpOutput(stream));
        io.cancel = Some(cancel);
        Ok(io)
    }

    /// Whether the peer finishing its side also ends ours, as if `input`
    /// had ended. Off by default.
    #[must_use]
    pub fn close_on_peer_fin(mut self, close: bool) -> Self {
        self.close_on_peer_fin = close;
        self
    }
}

/// The output of [`Io::tcp`]: dropping it half-closes the socket.
struct TcpOutput(TcpStream);

impl Write for TcpOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

impl Drop for TcpOutput {
    fn drop(&mut self) {
        // The target may have closed already.
        if let Err(e) = self.0.shutdown(Shutdown::Write) {
            log::debug!("half-close output: {e}");
        }
    }
}

/// Ends the blocking calls of an [`Io::tcp`]'s helper threads at teardown.
/// Dropped unused, it aborts, so every way out of a session cleans up.
pub(crate) struct Cancel(Option<TcpStream>);

impl Cancel {
    /// After a clean end: the input thread reads EOF and exits. The output
    /// half-closed the socket when it finished.
    pub(crate) fn clean(mut self) {
        self.shutdown(Shutdown::Read);
    }

    /// After a stop or a failure, once the output had its grace: every
    /// blocked call returns, even a write to a target that stopped reading.
    pub(crate) fn abort(mut self) {
        self.shutdown(Shutdown::Both);
    }

    fn shutdown(&mut self, how: Shutdown) {
        // The target may have closed already.
        if let Some(stream) = self.0.take()
            && let Err(e) = stream.shutdown(how)
        {
            log::debug!("shutdown {how:?}: {e}");
        }
    }
}

impl Drop for Cancel {
    fn drop(&mut self) {
        self.shutdown(Shutdown::Both);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io;
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    /// A connected loopback pair: ours, and the target's.
    pub fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let ours = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (theirs, _) = listener.accept().unwrap();
        (ours, theirs)
    }

    /// Runs `f` on a thread; the returned closure waits at most ten seconds
    /// for its result and for the thread to exit.
    fn spawn<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> impl FnOnce() -> T {
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            // The test has failed if nobody is waiting.
            if tx.send(f()).is_err() {}
        });
        move || {
            let value = rx
                .recv_timeout(Duration::from_secs(10))
                .expect("thread still blocked");
            thread.join().unwrap();
            value
        }
    }

    #[test]
    fn dropping_the_output_half_closes_while_replies_still_flow() {
        let (ours, mut theirs) = tcp_pair();
        let Io {
            mut input,
            mut output,
            // Dropping it would shut the socket down.
            cancel: _cancel,
            ..
        } = Io::tcp(ours).unwrap();
        output.write_all(b"request").unwrap();
        drop(output);
        let mut got = Vec::new();
        theirs.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"request");

        theirs.write_all(b"reply").unwrap();
        theirs.shutdown(Shutdown::Write).unwrap();
        let mut got = Vec::new();
        input.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"reply");
    }

    #[test]
    fn clean_cancel_ends_a_blocked_read() {
        for _ in 0..20 {
            let (ours, _theirs) = tcp_pair();
            let Io {
                mut input, cancel, ..
            } = Io::tcp(ours).unwrap();
            let read = spawn(move || input.read(&mut [0; 64]).map_err(|e| e.kind()));
            thread::sleep(Duration::from_millis(10));
            cancel.unwrap().clean();
            assert_eq!(read(), Ok(0));
        }
    }

    #[test]
    fn abort_ends_a_write_to_a_target_that_stopped_reading() {
        for _ in 0..20 {
            let (ours, _theirs) = tcp_pair();
            let Io {
                mut input,
                mut output,
                cancel,
                ..
            } = Io::tcp(ours).unwrap();
            let (progress, wrote) = mpsc::channel();
            let write = spawn(move || {
                let chunk = vec![0; 64 * 1024];
                loop {
                    if output.write_all(&chunk).is_err() {
                        return;
                    }
                    if progress.send(()).is_err() {}
                }
            });
            let read = spawn(move || input.read(&mut [0; 64]).map_err(|e| e.kind()));
            // Until the socket buffers fill and the writes block.
            while wrote.recv_timeout(Duration::from_millis(100)).is_ok() {}
            cancel.unwrap().abort();
            write();
            assert!(matches!(read(), Ok(0) | Err(_)));
        }
    }

    #[test]
    fn dropping_an_unused_cancel_aborts() {
        let (ours, _theirs) = tcp_pair();
        let Io {
            mut input, cancel, ..
        } = Io::tcp(ours).unwrap();
        let read = spawn(move || input.read(&mut [0; 64]).map_err(|e| e.kind()));
        thread::sleep(Duration::from_millis(10));
        drop(cancel);
        assert!(matches!(read(), Ok(0) | Err(_)));
    }

    #[test]
    fn only_tcp_has_something_to_cancel() {
        assert!(Io::stdio().cancel.is_none());
        assert!(Io::new(io::empty(), io::sink()).cancel.is_none());
    }
}
