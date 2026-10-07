//! Where a session's bytes come from and go to.

use std::io::{IsTerminal as _, Read, Write};

/// The local ends of a session: bytes read from `input` go to the peer, and
/// bytes from the peer are written to `output`.
///
/// Each runs on its own helper thread, so both may block.
///
/// A blocking read cannot be cancelled, so the input thread can outlive
/// [`Session::run`](crate::Session::run): it stays parked in `read` until
/// that returns, then sees the session is gone and exits without using
/// what it read. With a reader you own, end it once `run` returns (for a
/// socket, `shutdown`; for a channel, drop the sender). With
/// [`Io::stdio`], the thread may take one more chunk of stdin, so run one
/// stdio session per process.
pub struct Io {
    pub(crate) input: Box<dyn Read + Send>,
    pub(crate) output: Box<dyn Write + Send>,
    pub(crate) close_on_peer_fin: bool,
}

impl Io {
    /// A session over `input` and `output`. Input is always sent through to
    /// its end; see [`close_on_peer_fin`](Self::close_on_peer_fin).
    pub fn new(input: impl Read + Send + 'static, output: impl Write + Send + 'static) -> Self {
        Io {
            input: Box::new(input),
            output: Box::new(output),
            close_on_peer_fin: false,
        }
    }

    /// The process's stdin and stdout. Interactive stdin never ends on its
    /// own, so when it is a terminal the peer finishing ends our side too.
    pub fn stdio() -> Self {
        Io::new(std::io::stdin(), std::io::stdout())
            .close_on_peer_fin(std::io::stdin().is_terminal())
    }

    /// Whether the peer finishing its side also ends ours, as if `input`
    /// had ended. Off by default.
    #[must_use]
    pub fn close_on_peer_fin(mut self, close: bool) -> Self {
        self.close_on_peer_fin = close;
        self
    }
}
