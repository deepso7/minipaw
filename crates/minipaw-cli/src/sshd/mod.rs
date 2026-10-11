//! `serve --ssh`: an SSH server inside minipaw, answering each session
//! itself instead of forwarding it to sshd.
//!
//! Every login runs as the user running `serve`. The SDK stays synchronous:
//! for each session, [`Acceptor::accept`] makes a Unix socket pair, serves
//! one end with russh on this module's tokio runtime, and hands the other to
//! the SDK as the session's [`Io`]. See docs/plans/builtin-ssh.md.
//!
//! - `config.rs`: russh's settings, the host key and the limits
//! - `auth.rs`: the authorized keys
//! - `channel.rs`: the handler, the channel state machine and its input
//!   buffer, and the channel's supervisor
//! - `process.rs`: spawning, exit observation and teardown

mod auth;
mod channel;
mod config;
mod process;
#[cfg(test)]
mod tests;

use std::io;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use minipaw::{Io, PeerId};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::ui::fmt;

use auth::Policy;
use process::{Account, Registry};

/// `serve --ssh`'s flags.
pub struct Options<'a> {
    /// `--authorized-keys`; empty means `~/.ssh/authorized_keys`.
    pub authorized_keys: &'a [PathBuf],
    pub no_auth: bool,
}

/// What every connection shares.
struct Shared {
    config: Arc<russh::server::Config>,
    /// How long a connection has to log in: [`config::PRE_AUTH_DEADLINE`].
    deadline: Duration,
    policy: Policy,
    account: Account,
    registry: Arc<Registry>,
}

/// Who may log in and as whom, settled before the server starts.
pub struct Setup {
    policy: Policy,
    account: Account,
    /// `#` lines for stderr: warnings, then the status line.
    pub lines: Vec<String>,
}

impl Setup {
    /// Reads the serving account and the authorized keys.
    ///
    /// # Errors
    ///
    /// The keys cannot be used, with the message to print.
    pub fn load(options: &Options<'_>) -> Result<Setup, String> {
        let account = Account::current();
        if options.no_auth {
            let lines = vec![
                "# WARNING: --no-auth: whoever has the ticket gets a shell, with no key".to_owned(),
                format!(
                    "# ssh: built-in server as {}, NO AUTH: anyone with the ticket gets a shell",
                    account.name
                ),
            ];
            return Ok(Setup {
                policy: Policy::NoAuth,
                account,
                lines,
            });
        }
        let paths = if options.authorized_keys.is_empty() {
            vec![account.home.join(".ssh").join("authorized_keys")]
        } else {
            options.authorized_keys.to_vec()
        };
        let (policy, mut lines) = auth::load(&paths, account.uid)?;
        let n = match &policy {
            Policy::Keys(keys) => keys.len(),
            Policy::NoAuth => 0,
        };
        let keys = if n == 1 { "key" } else { "keys" };
        lines.push(format!(
            "# ssh: built-in server as {}, {n} {keys}",
            account.name
        ));
        Ok(Setup {
            policy,
            account,
            lines,
        })
    }
}

/// Lets accept workers in until the server stops. A worker takes a
/// [`TaskTracker`] token under the same lock that closing takes, so none
/// can pass the gate and register after the stop has waited for the
/// tracker: the SDK's accept workers can outlive `Server::run`.
struct Gate {
    open: Mutex<bool>,
    tracker: TaskTracker,
}

impl Gate {
    fn new(tracker: TaskTracker) -> Gate {
        Gate {
            open: Mutex::new(true),
            tracker,
        }
    }

    fn enter(&self) -> Option<TaskTrackerToken> {
        let open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        open.then(|| self.tracker.token())
    }

    fn close(&self) {
        *self.open.lock().unwrap_or_else(PoisonError::into_inner) = false;
    }
}

/// The running server. Dropping it stops it: on every path out of `serve`,
/// including a failed `Server::run`.
pub struct Sshd {
    runtime: Option<Runtime>,
    acceptor: Acceptor,
    tracker: TaskTracker,
    registry: Arc<Registry>,
}

/// Makes each session's local end. Runs on the SDK's accept workers.
#[derive(Clone)]
pub struct Acceptor {
    shared: Arc<Shared>,
    gate: Arc<Gate>,
    runtime: tokio::runtime::Handle,
    stop: CancellationToken,
}

impl Sshd {
    /// Starts the runtime the server's connections run on, with the host
    /// key from `host_seed`.
    ///
    /// # Errors
    ///
    /// The runtime does not start.
    pub fn start(setup: Setup, host_seed: &[u8; 32]) -> Result<Sshd, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("minipaw-sshd")
            .enable_all()
            .build()
            .map_err(|e| format!("starting the SSH server: {e}"))?;
        let no_auth = matches!(setup.policy, Policy::NoAuth);
        let registry = Arc::new(Registry::default());
        let shared = Arc::new(Shared {
            config: Arc::new(config::server(config::host_key(host_seed), no_auth)),
            deadline: config::PRE_AUTH_DEADLINE,
            policy: setup.policy,
            account: setup.account,
            registry: registry.clone(),
        });
        let tracker = TaskTracker::new();
        let acceptor = Acceptor {
            shared,
            gate: Arc::new(Gate::new(tracker.clone())),
            runtime: runtime.handle().clone(),
            stop: CancellationToken::new(),
        };
        Ok(Sshd {
            runtime: Some(runtime),
            acceptor,
            tracker,
            registry,
        })
    }

    pub fn acceptor(&self) -> Acceptor {
        self.acceptor.clone()
    }
}

impl Drop for Sshd {
    /// Admits no one new, tears every connection down at once, and waits
    /// for them within [`config::SHUTDOWN_BUDGET`]. Past it, SIGKILLs
    /// whatever programs are still unreaped.
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        self.acceptor.gate.close();
        self.acceptor.stop.cancel();
        self.tracker.close();
        let tracker = self.tracker.clone();
        let done = runtime.block_on(async {
            tokio::time::timeout(config::SHUTDOWN_BUDGET, tracker.wait())
                .await
                .is_ok()
        });
        if !done {
            let n = self.registry.kill_all();
            log::warn!("ssh: stopping took too long; killed {n} program group(s)");
        }
        runtime.shutdown_timeout(Duration::from_secs(1));
    }
}

impl Acceptor {
    /// The local end of a new session for `peer`: one end of a socket pair
    /// whose other end an SSH connection serves.
    ///
    /// # Errors
    ///
    /// The server is stopping, or no socket pair could be had.
    pub fn accept(&self, peer: &PeerId) -> io::Result<Io> {
        Io::unix(self.open(&peer.to_string())?)
    }

    /// [`accept`](Self::accept)'s socket.
    fn open(&self, peer: &str) -> io::Result<UnixStream> {
        let Some(token) = self.gate.enter() else {
            return Err(io::Error::other("stopping"));
        };
        let (ours, theirs) = UnixStream::pair()?;
        // A second handle to cut the connection with: dropping russh's
        // session future leaves its task running.
        let kill = ours.try_clone()?;
        ours.set_nonblocking(true)?;
        let shared = self.shared.clone();
        let stop = self.stop.clone();
        let peer = peer.to_owned();
        self.runtime.spawn(async move {
            let _token = token;
            connection(shared, ours, kill, peer, stop).await;
        });
        Ok(theirs)
    }
}

/// Serves one SSH connection on `stream` until it ends, it fails to log in
/// in time, or the server stops; then tears its channel down and waits for
/// that.
async fn connection(
    shared: Arc<Shared>,
    stream: UnixStream,
    kill: UnixStream,
    peer: String,
    stop: CancellationToken,
) {
    let label = fmt::short_id(&peer);
    let stream = match tokio::net::UnixStream::from_std(stream) {
        Ok(stream) => stream,
        Err(e) => {
            log::info!("ssh {label}: {e}");
            return;
        }
    };
    let token = stop.child_token();
    let tasks = TaskTracker::new();
    let (authed, mut authed_rx) = tokio::sync::watch::channel(false);
    let handler = channel::Conn::new(
        shared.clone(),
        peer,
        label.clone(),
        authed,
        token.clone(),
        tasks.clone(),
    );
    // One deadline from the first byte: `run_stream` itself waits for the
    // client's identification.
    let deadline = tokio::time::Instant::now() + shared.deadline;
    let config = shared.config.clone();
    let session = async move {
        match russh::server::run_stream(config, stream, handler).await {
            Ok(running) => running.await,
            Err(e) => Err(e),
        }
    };
    tokio::pin!(session);
    let unauthenticated = async {
        // Disarmed only by a login; the handler going away means the
        // session ended, which the select below sees.
        let authed = async {
            let _ = authed_rx.wait_for(|authed| *authed).await;
        };
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => {}
            () = authed => std::future::pending().await,
        }
    };
    let cut = tokio::select! {
        result = &mut session => {
            match result {
                Ok(()) => log::debug!("ssh {label}: connection closed"),
                Err(e) => log::debug!("ssh {label}: connection ended: {e}"),
            }
            None
        }
        () = unauthenticated => Some("no login in time"),
        () = stop.cancelled() => Some("the server is stopping"),
    };
    if let Some(why) = cut {
        log::info!("ssh {label}: cutting the connection: {why}");
        let _ = kill.shutdown(Shutdown::Both);
        if tokio::time::timeout(config::CONNECTION_END, &mut session)
            .await
            .is_err()
        {
            log::debug!("ssh {label}: the connection did not end in time");
        }
    }
    token.cancel();
    tasks.close();
    tasks.wait().await;
}
