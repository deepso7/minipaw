# Plan: a built-in SSH server for `minipaw serve` (russh)

Status: rollout steps 1 to 4 are done: the spike, the library (PR 1), the
server with shell, exec and PTYs (PR 2), and SFTP (PR 3). Phase 3 is next.
Uses stock russh from crates.io, with no fork. Where the code differs from
this plan, the implementation notes below say how and why.

## Goal

`minipaw serve --ssh` answers each session with an SSH server inside the
minipaw process, instead of forwarding to sshd on `127.0.0.1:22`. It works
on machines with no sshd, as a normal user, with no setup. It follows
tailcat's `ssh` and `no-auth-ssh` modes.

Forwarding to sshd stays the default and does not change.

## Non-goals (v1)

- Switching users, or root acting on behalf of others: every login runs as
  the user running `minipaw serve`.
- Port forwarding (`direct-tcpip`, `tcpip-forward`), agent forwarding, X11,
  compression, and password, keyboard-interactive or certificate auth.
- **RSA user keys.** russh 0.64.1 accepts `ssh-rsa` (SHA-1) signatures
  whatever `Preferred::key` says (reproduced). RSA lines are
  skipped with a warning.
- Reloading authorized keys while running. Keys are read once at startup,
  so a restart applies edits and revocations.
- **Forced commands (`-- CMD`) and client env forwarding.** Deferred to
  phase 3 to keep v1 small.
- **More than one channel per connection.** A second session channel is
  rejected, so OpenSSH `ControlMaster` multiplexing doesn't work; each
  `ssh`, `scp` or `sftp` run opens its own connection, as it does by
  default.
- **Per-channel back-pressure on input.** A sender that outruns its
  program by more than the input cap has its channel closed rather than
  slowed down (see flow control).
- Windows. v1 is `cfg(unix)` only; CI covers ubuntu and macos.
- Any change to the minipaw wire protocol or the ticket format.
- **Processes that leave the session's process groups.** Anything that
  calls `setsid`, or a job-control group that ignores SIGHUP, can outlive
  its session, as under sshd. This is documented.

## CLI

```
minipaw serve --ssh [--authorized-keys PATH]... [--no-auth]
```

- `--ssh` conflicts with `--forward`, and the conflict still fires with
  forward's default. `--authorized-keys` and `--no-auth` each `requires`
  `--ssh`. Verified with clap.
- `--authorized-keys PATH` takes files only and can be repeated. It
  defaults to `~/.ssh/authorized_keys` of the serving user.
  - Entries with options (`command=`, `from=`, `restrict`, …) or
    `cert-authority` are a startup error that names the file and line.
  - Unsupported key types (RSA, DSA, `sk-*`) are skipped with a `#`
    warning naming the file and line.
  - If no usable key is left, startup fails.
- `--no-auth` accepts `none` auth, so the ticket alone is the credential.
  It conflicts with `--authorized-keys` and prints a loud `#` warning.
- The stderr line replacing `# forwarding to …`:
  - `# ssh: built-in server as <user>, N keys`
  - `# ssh: built-in server as <user>, NO AUTH: anyone with the ticket gets a shell`
- `ServeEvent`s are unchanged. Per-login lines (auth ok or failed,
  requested username, what ran) go to `log::info!`, shown with `-v`.

## Architecture

### Where the code lives

- The `minipaw` library stays synchronous and tokio-free. It gains:
  - `Io::unix(UnixStream)` (cfg unix), mirroring `Io::tcp`. `Cancel` and
    `TcpOutput` become generic over a private `Shutdown` trait.
  - `Identity::ssh_host_seed(&self) -> [u8; 32]`, a derived seed and never
    the raw secret (see Host key). It uses the `hkdf` crate; `hmac` and
    `sha2` are already in the lockfile.
- The CLI gets `crates/minipaw-cli/src/sshd/` with these modules:
  - `mod.rs`: runtime, admission, supervision
  - `config.rs`
  - `auth.rs`
  - `channel.rs`: the state machine and the input buffer
  - `process.rs`: spawning, exit observation, teardown
  - `sftp.rs`: phase 2, the `sftp` subsystem
- All of it sits behind a cargo feature `ssh-server` of `minipaw-cli`,
  default on and `cfg(unix)`.

### Flow control on stock russh

**A. Input credit.** russh 0.64.1 grants receive window by itself. In
`Encrypted::adjust_window_size` it queues `CHANNEL_WINDOW_ADJUST` once
credit falls below half the target, *before* data reaches the handler.
Nothing ties credit to consumption, and data over the window or the
maximum packet size is still delivered. This is upstream's intended
design: the maintainer closed #730 (consumption-gated `WINDOW_ADJUST`)
saying an application must always drain its channels. So russh offers two
choices for a slow consumer:

- stop reading: russh's bounded internal queue fills and the **whole
  connection** stalls (output, window adjusts, close, keepalives), and a
  program that reads and writes at once (`ssh h cat < big`) deadlocks;
- keep reading: the application buffers whatever the peer sends.

**Plan:** keep reading, and bound the buffer ourselves.

- Callbacks never await child I/O. `data` appends to the channel's input
  buffer and returns.
- **Input cap: 8 MiB per channel**, counted in bytes actually received,
  so data over the window is counted too. A `data` that would pass the
  cap tears the channel down: write `minipaw: input exceeded 8 MiB
  buffer, closing` to the client's stderr, then the normal teardown. Only
  that channel is affected.
- A writer task drains the buffer into the child (PTY master or stdin).
- Worst-case input memory is `8 MiB × max_sessions`.

The trade-off: a sender that outruns its program by more than 8 MiB is
cut off instead of slowed down (`cat big.iso | ssh h 'slow-thing'`).
Interactive shells, git, and ordinary pipes stay far below it. It is
documented, and a `--ssh-input-buffer` flag can come later if it bites.

**The other two russh issues** don't need a fork either:

- **B. Output reservations.** `ChannelTx` takes credit before enqueueing,
  and an incoming `WINDOW_ADJUST` *overwrites* the counter, erasing
  reservations still queued. The excess becomes pending data, and while it
  is pending, handle dispatch waits for *every* channel on the connection
  (deliberate since #725). With **one session channel per connection**
  there is no sibling to stall: the pending bytes go out when the client
  next grants window, which it does as it reads.
- **C. Local close lifecycle.** After a local `Encrypted::close`, the
  peer's reciprocal close never reaches `channel_close`, and russh keeps
  that channel's references. Our state is released when the channel's
  supervisor completes, not in `channel_close`. With one channel per
  connection, russh's leftover references are freed when the connection
  ends.

**Upstream reports**, not blocking: a comment on #730 with the `cat`
deadlock case, and issues for B (with the 16-bytes-on-9-credit probe), C,
and unchecked data over the window or packet size.

### Channels and output

- `channel_open_session(channel, reply, session)`:
  - if no session channel has been opened on this connection yet,
    `reply.accept().await`, else reject (one per connection, see flow
    control);
  - `channel.split()`: **drop the `ChannelReadHalf`** (sends to a dropped
    receiver fail at once and are ignored, verified) and keep the
    `ChannelWriteHalf`.
  - The channel's app state is released when **its supervisor
    completes**, not in `channel_close`, which does not fire after a
    local close (russh issue C).
- Every other channel type is rejected by the default handlers, and tests
  check this.
- **Input:** only through handler callbacks into the per-channel buffer,
  for every channel kind (shell, exec, and SFTP in phase 2, which reads the
  buffer through an `AsyncRead` adapter).
- **Output:** written through the `ChannelWriteHalf`'s window-aware writer
  (`make_writer` / `make_writer_ext(Some(1))` for stderr) in chunks of at
  most 32 KiB. A stall therefore waits in our task, *before* the shared
  handle queue.

### Channel state machine (`channel.rs`)

States: `Configuring { pty, eof_seen } → Running → Finished`. Every
request gets an explicit `channel_success` or `channel_failure` when
`want_reply` is set.

| Request | Configuring | Running | Finished |
|---|---|---|---|
| `pty_request` | First one ok, then failure. TERM: `[A-Za-z0-9._+-]`, ≤64 B, else `xterm-256color`. cols/rows clamped to `1..=u16::MAX`, px to `0..=u16::MAX`. Modes ignored. | failure | failure |
| `env_request` | failure (v1) | failure | failure |
| `shell_request` / `exec_request` | Spawn → success, then `Running`. A spawn error or a command containing NUL → failure, then close. | failure | failure |
| `subsystem_request("sftp")` | Success, then `Running` with the SFTP loop as the program (phase 2; v1 refused it). A PTY asked for before is ignored. Other names: failure. | failure | failure |
| `window_change_request` | Store the size (clamped). | Resize the PTY (clamped). | ignore |
| `data` | Buffer (capped at 8 MiB; over the cap → teardown). | Same. Data after EOF is a protocol violation: close the channel. | discard |
| `extended_data` | discard | same | same |
| `channel_eof` | `eof_seen = true`, applied after spawn | No PTY: close the child's stdin once the buffer drains. PTY: ignore. | ignore |
| `channel_close` | drop the state | Send a teardown request to the supervisor and return at once. | drop the state |
| `signal` | ignore | ignore | ignore |

`Finished` starts when the exit sequence below has sent `close`.

### Processes (`process.rs`)

Account data (name, home, shell) comes once at startup from the passwd
entry of the *effective* uid, through `uzers`. It is never taken from the
server's `$SHELL` or `$HOME`. If there is no entry, the fallbacks are
`/bin/sh` and `/`.

**Spawning:**

- **PTY: `pty-process` 0.5 with feature `async`**, replacing portable-pty.
  - `Pty::open` opens the master *without* `CLOEXEC` and sets it after
    grant and unlock. A child spawned in that window by another runtime
    worker inherits the master and delays the hangup. A process-wide
    `spawn_lock` serialises PTY allocation (up to the point `FD_CLOEXEC`
    is set) against **every** child spawn in the CLI, on every platform,
    so no pty-process fork is needed. Atomic `OpenptFlags::CLOEXEC` on
    Linux goes upstream; once released, the lock is needed on macOS only.
  - Its `Pty` is a non-blocking master registered with tokio
    (`AsyncRead`/`AsyncWrite`, `into_split`, `resize`). Reads and writes
    are cancellable, and dropping every half closes the *only* master
    descriptor. There are no blocking duplicate fds and no destructor that
    writes.
  - Its `Command` wraps `tokio::process::Command`, does setsid and
    TIOCSCTTY itself (so our crate's `unsafe_code = "forbid"` holds), and
    supports `arg0`.
  - The pts handle is dropped right after spawn.
  - Shell: `<shell>` with `arg0 = "-<basename>"`. Exec: `<shell> -c <cmd>`.
- **No PTY:** `tokio::process::Command` with piped stdio and
  `process_group(0)`. Shell is `<shell>` with arg0 `-<basename>`; exec is
  `<shell> -c <cmd>`.
- **Environment:** start from `env_clear()`, then set:
  - `HOME`, `USER`, `LOGNAME`, `SHELL`;
  - `PATH` (the server's, else `/usr/local/bin:/usr/bin:/bin`);
  - `TERM` (PTY only);
  - `SSH_PEER_ID` (under `--no-auth` it's a key the dialer made up, so
    it's for logs only, and documented that way);
  - `SSH_CONNECTION="127.0.0.1 0 127.0.0.1 0"`.
- cwd: home, else `/`.

**Exit observation without reaping.** One supervisor per channel owns the
`Child`. It never calls `wait`/`try_wait` until teardown is over. It
observes exit with `rustix::process::waitid(WaitId::Pid(pid), EXITED |
NOWAIT | NOHANG)`, polled every 50 ms. The unreaped leader keeps its
pid/pgid from being reused, so the group signals below always hit our
group. The final `child.wait()` reaps and gives the real status (exit code
or signal number) for `exit-status` / `exit-signal`.

**Normal exit, in order:**

1. No PTY: wait for EOF on stdout and on stderr (pumped concurrently), as
   sshd does (`ssh h 'sleep 100 &'` waits too). This wait can be cancelled
   by teardown.
2. PTY: read the master until EIO, still honouring SSH back-pressure. A
   client that holds back credit delays this but loses no output. Only
   teardown cancels it; there is no wall-clock cutoff on a normal exit.
3. Reap.
4. `exit_status_request(code)`, or `exit_signal_request(name, …)`, using a
   fixed table mapping signal numbers to SSH names (`HUP`, `INT`, `QUIT`,
   `KILL`, `TERM`, `PIPE`, `ALRM`, `USR1`, `USR2`, `ABRT`, `SEGV`; others
   are sent as an exit status of `128 + n`).
5. `eof`, then `close`.

**Teardown** (channel close, connection end, shutdown). All of a
connection's channels tear down **concurrently**.

- **No PTY:**
  1. `kill(-pgid, SIGHUP)`, then `SIGTERM`.
  2. Wait up to 2 s (NOWAIT polling).
  3. `kill(-pgid, SIGKILL)`.
  4. Cancel the pumps and reap.
- **PTY:**
  1. Cancel the pumps and drop all `Pty` halves. That closes the only
     master fd, the kernel hangs up the terminal, and SIGHUP goes to the
     session leader and the foreground group.
  2. Wait up to 2 s.
  3. `kill(-shell_pgid, SIGKILL)` while the shell is still unreaped.
  4. Reap.
  - Job-control groups other than the shell's are **not** signalled by
    pgid number, because their pgid may be reused after the shell reaps
    them. They get only the hangup, as under sshd, and this is
    documented.
- Tests:
  - channel close kills `sleep 1000`;
  - connection drop kills an `exec` group that ignores HUP (SIGKILL);
  - a PTY foreground job gets SIGHUP;
  - a foreground job that exits and is reaped during the grace period does
    not cause a stray signal.

### Bridging, admission and the pre-auth deadline

`minipaw::serve(config, accept)` calls `accept(&PeerId)` on a worker thread
after the token check and the `max_sessions` permit. With `--ssh`, `accept`:

1. Takes the **admission mutex**. It checks the `open` flag (returning
   `Err("stopping")` if it is closed) and gets a `TaskTracker` token while
   *still holding it*. Shutdown closes the flag under the same mutex, so no
   worker can pass the gate and then register after `tracker.wait()`.
   Library accept workers can outlive `server.run()`, so this matters. A
   barrier-controlled race test covers it.
2. Calls `UnixStream::pair()`. On our end it uses `try_clone()` to keep a
   **kill switch** (a std `UnixStream` we can `shutdown(Both)`), then sets
   it non-blocking.
3. Registers a supervisor with the `TaskTracker` and spawns it on the
   runtime handle. The supervisor:
   - starts **one absolute 20 s deadline before calling `run_stream`**,
     because `run_stream` already waits for the client's identification
     before it returns;
   - `select!`s the whole thing (`run_stream(...).await`, then awaiting the
     `RunningSession`) against the deadline (disarmed by a `watch` that is
     set **only** in `auth_publickey` / `auth_none` returning `Accept`) and
     the server `CancellationToken`;
   - on deadline or cancel: `kill_switch.shutdown(Both)`. Dropping
     `RunningSession` alone leaves russh's spawned task running. It then
     awaits the connection's end (bounded at 2 s; the closed socket makes
     russh exit);
   - in every case, tears down all channels concurrently and awaits them.
4. Returns `Io::unix(other_end)`.

### Runtime and shutdown

- `serve` owns the `Runtime` (`new_multi_thread`, `worker_threads(2)`,
  `enable_all`). The accept closure captures a `Handle`, the gate, the
  `TaskTracker` and the `CancellationToken`.
- Cleanup runs on **every** path out of `serve`, including when
  `server.run()` fails. It is structured as a guard and not as `?`:
  1. Set the gate. Accept workers still in flight see it, or, if they
     already registered, are tracked.
  2. `token.cancel()`.
  3. `tracker.close()`.
  4. `runtime.block_on(async { timeout(5s, tracker.wait()).await })`.
     Teardown is concurrent, so 2 s grace + 2 s connection end fits.
  5. If the budget runs out: a synchronous last resort that `SIGKILL`s
     every still-unreaped group leader (recorded in a shared registry),
     logs it, and then `runtime.shutdown_timeout(1s)`. Signalling from the
     registry and removing an entry are serialised by the registry's lock.
     An entry is removed *before* its `Child` is reaped or dropped,
     including on error and panic paths, so the last resort never signals
     a stale pid.

### russh configuration (`config.rs`)

- `russh` 0.64.1 from crates.io, with `default-features = false,
  features = ["ring"]`. That means no aws-lc-rs, flate2 or rsa.
- `methods`: `MethodSet` with `MethodKind::PublicKey`, or only
  `MethodKind::None` under `--no-auth`.
- `preferred`:
  - kex: `mlkem768x25519-sha256`, `curve25519-sha256`, `ext-info-s`,
    `kex-strict-s-v00@openssh.com`;
  - cipher: `chacha20-poly1305@openssh.com`, `aes256-gcm@openssh.com`,
    `aes128-gcm@openssh.com`;
  - compression: `none`;
  - host key algorithm: `ssh-ed25519`.
- `max_auth_attempts = 6`, `auth_rejection_time = 1s`,
  `auth_rejection_time_initial = Some(Duration::ZERO)`.
- `inactivity_timeout = None` and `keepalive_interval = Some(30s)`. Before
  auth, the supervisor's deadline applies.

### Host key

- `HKDF-SHA256(ikm = identity secret, salt = none,
  info = "minipaw/ssh-host-key/v1", L = 32)` gives an ed25519 seed.
- It is stable whenever the ticket is, and rotates with `serve --new`.
  There is no signing reuse with Noise or libp2p-tls.
- A test pins a known identity to its known host public key.
- `ssh_host_seed` is documented as secret.
- `minipaw ssh` is unchanged: `HostKeyAlias` plus TOFU. Pinning is in
  phase 3.

### Auth (`auth.rs`)

Keys are loaded once at startup.

**Path policy**, like sshd's StrictModes:

- Resolve the path one component at a time. Each symlink followed must sit
  in a directory that passes the checks below, with every directory above
  it, since whoever can write that directory could point the link anywhere.
  Then walk the resolved path from `/` one component at a time with
  `openat(dirfd, name, O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)`. Because the
  path was resolved first, a symlink swapped in after resolution fails
  rather than being followed.
- Each directory, checked with `fstat`, must be owned by root or the
  serving user and not group- or world-writable (so a sticky `/tmp` is
  refused).
- The file: `openat(…, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)`.
  `fstat` must show a regular file, owned by root or the user, not group-
  or world-writable (`0644` is fine). Read at most `1 MiB + 1` bytes; more
  than that is an error.

**Parsing:**

- Each original line is parsed on its own, keeping file and line for
  diagnostics, with `ssh_key::PublicKey::from_openssh`, after detecting
  options. A line whose first token is not a key type has options and is
  rejected.
- Keys are compared on `key_data()`.

**Methods:**

- `auth_publickey_offered`: `Accept` only for a key in the set.
- `auth_publickey`: check again; on `Accept`, set the authenticated
  `watch`.
- `auth_none`: `Accept` only with `--no-auth`; it sets the `watch` too.

**Usernames:** any requested name logs in as the serving account. It is
logged as untrusted metadata next to the real account and not used for
anything else.

### Phase 2: SFTP (`sftp.rs`)

- Input comes from the same capped callback buffer (an `AsyncRead`
  adapter); output goes through the `ChannelWriteHalf`.
- Use russh-sftp's protocol types and `server::Handler` on `tokio::fs`,
  but **our own supervised loop**, not the detached `server::run`. It is:
  - tracked by the connection supervisor and cancelled with the channel;
  - ended by any framing or transport error.
- **Bounded framing:** read the 4-byte length and reject anything over
  256 KiB + header *before* allocating.
- Requests:
  - `open`/`close`/`read`/`write` and `lstat`/`fstat`/`stat`;
  - `setstat`/`fsetstat` (mode and times; anything else gives
    `OpUnsupported`);
  - `opendir`/`readdir`, paginated at 128 entries;
  - `remove`, `mkdir`, `rmdir`, `realpath`, `rename`, `readlink`,
    `symlink`.
- Caps: 256 handles, reads of at most 255 KiB (so the reply fits OpenSSH's
  256 KiB message limit, as sftp-server does), writes of at most 256 KiB.
- Paths are relative to home; absolute paths are allowed. This is not a
  sandbox.
- Adds `tokio/fs`.

### Phase 3 (later)

- Forced commands (`-- CMD`, `SSH_ORIGINAL_COMMAND`) and client env
  forwarding (an allow-list with validation).
- Host key pinning in `minipaw ssh`, through an identity-signed binding.
  This needs a wire or ticket change.
- RSA user keys, once russh can enforce the signature algorithm (upstream
  change).
- `direct-tcpip`, Windows, `user@github` keys, and reloading keys that
  fails closed.

## Dependencies (minipaw-cli, optional under `ssh-server`)

```toml
russh = { version = "=0.64.1", default-features = false, features = ["ring"] }
tokio = { version = "1.53", default-features = false, features = ["rt-multi-thread", "net", "io-util", "process", "sync", "time", "macros"] }
tokio-util = { version = "0.7", default-features = false, features = ["rt"] }
pty-process = { version = "0.5", features = ["async"] }
rustix = { workspace = true, features = ["fs", "process"] }
uzers = { version = "0.12", default-features = false }
# phase 2: the version pinned exactly, as russh is
russh-sftp = "=3.0.1"
bytes = { version = "1", default-features = false }  # its Bytes; already in the tree
# no tokio "fs": the file operations run on our own bounded jobs
```

The library also gets `hkdf = "0.13"`.

CI changes:

- `cargo deny check advisories`;
- `cargo build -p minipaw-cli --no-default-features`;
- the binary size before and after, reported in the PR.

## Tests

- **Library:**
  - `Io::unix`: half-close, cancel unblocking a read or write, and no
    helper thread outliving a clean end;
  - `ssh_host_seed` against a known vector.
- **CLI unit tests:**
  - authorized_keys: options rejected, comments, bad base64, RSA skipped,
    `key_data` equality, the path policy (symlinks, a writable ancestor,
    a FIFO, an oversize file);
  - TERM handling and size clamping;
  - the signal table;
  - argument conflicts and requires.
- **In-process integration** (`UnixStream::pair` + russh client, no
  network):
  - auth: the right key, a wrong key, `none` without `--no-auth`, a silent
    client cut at 20 s (with time paused), and an unsigned offer not
    counting as auth;
  - exec ordering: stdout, stderr, status 3, eof, close;
  - an EOF sent before exec still reaches `cat`;
  - flow control: the spike cases;
  - a second session channel on one connection is rejected;
  - PTY size and resize;
  - the teardown cases above;
  - shutdown completing within budget while a PTY read is blocked;
  - non-session opens refused, a second exec refused.
- **End-to-end** (CI): `serve --ssh` with system `ssh` through
  `minipaw -q`, running `ssh … true`, `ssh -tt … 'stty size'`, and
  `ssh … cat` with 20 MB both ways. Phase 2 adds `scp` of 5 MB both
  ways and an `sftp -b` batch.

## CLI wiring notes

- With `--ssh`, choose the SSH backend *before* `forward.resolve()`.
- Pass the new flags through `main.rs` and into `serve::Options`.
- Gate the flags and modules on `cfg(all(unix, feature = "ssh-server"))`,
  with a clear error from clap when they're unavailable.

## Rollout

1. **Flow-control spike on stock russh** (go/no-go). An in-process russh
   server with the input cap and one channel per connection, checked
   with:
   - `ssh h cat` with 100 MB in both directions at once;
   - a slow consumer (`sleep 5; cat >/dev/null`) fed 4 MiB: completes;
   - the same fed 100 MB at line rate: that channel gets the stderr
     message and closes, and the server stays healthy;
   - keepalive and close keep working while the consumer is slow;
   - a peer that sends over its window or packet size is caught by the
     cap;
   - a `WINDOW_ADJUST` arriving while stdout and stderr are queued: all
     output and the exit status still arrive (russh issue B);
   - 1,000 sequential connections, each running a command: memory
     stays flat (russh issue C).

   Also file the upstream reports. If the spike shows stalls that the
   cap and one-channel rule don't cover, stop here.
2. PR 1, library: `Io::unix`, `Identity::ssh_host_seed`.
3. PR 2: the `ssh-server` feature (shell and exec, PTY, auth), README and
   CI.
4. PR 3: SFTP.
5. PR 4 onward: phase 3.

## Implementation notes (PR 2)

Where PR 2 differs from the plan above, and why:

- **PTY drain on a normal exit.** The master is read until EIO, *or* until
  the shell has exited and the terminal has been quiet for 250 ms. A
  background job that still holds the pts would otherwise keep the session
  open for good; sshd does not wait for those either. Waiting on the
  client's window is not quiet, so no output is lost to back-pressure.
- **No-PTY teardown order.** The pumps are cancelled when teardown starts,
  before SIGHUP and SIGTERM, rather than after SIGKILL: output written
  during the grace has no reader anyway, as the channel is going away.
- **A spawn while stopping** is refused like a failed spawn (failure, then
  close).
- **authorized_keys parsing.** A line whose first token starts with
  `ssh-`, `ecdsa-` or `sk-` is a key line; anything else counts as options
  and is an error. A key that does not parse (bad base64, a blob of another
  type) is an error too, naming the file and line. Unsupported types,
  certificates (`*-cert-v01@openssh.com`) included, are skipped with a
  warning. The default file is under the home directory of the passwd
  entry, not `$HOME`.
- **Without the feature**, `--ssh` and its flags still parse, and clap
  rejects `--ssh` with a message naming the `ssh-server` feature.
- **tokio's `fs` feature** comes in with pty-process (its dependency), not
  in phase 2.
- **Tests.**
  - The silent-client deadline test uses paused time. The unsigned-offer
    test cannot: tokio advances a paused clock while socket I/O is still in
    flight, so the handshake itself hit the deadline. It uses a real 3 s
    deadline instead (the deadline is a field of the server's shared state).
  - On Linux a PTY hangup signals only the session leader; the foreground
    job gets SIGHUP when the leader exits. The test checks that chain end to
    end (BSD signals the foreground group at once).
  - Spike cases 1, 2, 3, 4 and 7 are ported at CI sizes (20 MB, not
    100 MB; 25 connections, not 1,000). Cases 5 and 6 need the cleartext
    abuse proxy and exercise russh internals; the spike covers them.
  - The end-to-end check is `serve_ssh` in `scripts/check.sh`, which CI
    already runs on ubuntu and macos. Its key file lives under `$HOME`,
    since the path policy refuses a world-writable `/tmp`.
- **No `deny.toml`**: `cargo deny check advisories` passes on its defaults.
- **Teardown always waits the full 2 s grace**, with or without a PTY,
  then SIGKILLs the leader's group, even when the leader exits at once.
  Members of its group that ignore the signals would otherwise survive, and
  killing as soon as the leader leaves would cut short a member still
  cleaning up. The leader stays unreaped, so its pgid stays ours, until
  then.
- **Input is coalesced**: empty `data` is ignored and small packets join
  the last queued piece (up to 32 KiB), so the 8 MiB cap bounds memory and
  not only the bytes counted.

## Implementation notes (PR 3, SFTP)

Where PR 3 differs from the plan above, and why:

- **Our own dispatch.** russh-sftp keeps its request dispatch private to
  its detached `server::run`. The loop in `sftp.rs` decodes requests with
  russh-sftp's `Packet` and calls its `Handler` methods itself, mirroring
  that dispatch. The loop runs inside the channel's supervisor in a
  `select!` against the channel's teardown token, so its reads, file work
  and replies are all dropped at teardown. Its replies go through the
  channel's writer in pieces of at most 32 KiB.
- **Framing.** A request's length may be at most 256 KiB + 1 KiB (room for
  a 256 KiB write's header); a longer one, a zero length, or a request cut
  off by EOF ends the session. An unknown type, or a reply type sent by
  the client, gets `OpUnsupported` with its id, and the session goes on.
- **Strict layouts.** russh-sftp's decoder takes a request cut short after
  a field as having empty attributes (so an `OPEN` cut off after its
  flags would create or truncate a file), ignores trailing bytes, and
  replaces bytes of names that are not UTF-8. So each request's layout is
  read from the wire first: every field its type has, the attribute
  fields its flags promise (extended pairs included), and nothing after.
  A known request that does not have its layout ends the session, as
  other framing errors do. A path, handle or link target that is not
  UTF-8 gets `Failure` and nothing is done.
- **The end.** The client's EOF between requests is the clean end: exit
  status 0, EOF, close. Any error ends it with status 1 (OpenSSH's
  sftp-server uses other codes, 11 for an oversize request). The SFTP
  reader cannot tell the input being closed (the cap, data after EOF)
  from EOF, so when it ends with the input closed, the supervisor waits
  for the teardown that always follows such a close, and the client gets
  the cap's message and a close rather than status 0. A teardown that
  cuts the exit report short (of a process or of SFTP) goes on as any
  teardown does, so the server's close is still sent for the cap or data
  after EOF.
- **Bounded file operations, not `tokio::fs`.** Every file operation runs
  on the blocking pool through one runner, holding one of 64 server-wide
  permits, moved into the job, until the syscall really returns. A
  teardown drops its wait at once but cannot abort a syscall: a job stuck
  in the filesystem (NFS, FUSE) keeps its permit, so stuck jobs cannot
  use up tokio's blocking pool, and once all 64 are stuck, new file
  operations wait (cancellably) for one. A job still queued when its
  session goes does nothing. The jobs use rustix and std directly;
  `tokio::fs` would put its own unbounded blocking jobs underneath. File
  data is read and written with `pread`/`pwrite`, so a handle needs no
  seek state, and a write's error comes with its own reply. A handle
  opened with APPEND writes with `write(2)` instead, at the end whatever
  the offset: `pwrite(2)` honours `O_APPEND` on Linux but not on macOS.
- **Closing never blocks the runtime.** close(2) can hang on a stuck
  filesystem too, so descriptors are dropped on the blocking pool, as a
  job under a permit (so they are bounded with everything else) but even
  once the session is gone, and nothing waits for them: `CLOSE` is
  answered at once, and a session's open handles are closed in the
  background when it ends. Its home is used as configured, without the
  stat a shell's working directory gets, for the same reason.
- **Handles are descriptors.** A file or directory handle holds its open
  descriptor: `fstat`, `fsetstat` (`fchmod`, `futimens`) and `readdir`
  (`fdopendir`, then `fstatat` per entry) act on what was opened, even
  after a rename or a new directory in its place. A directory handle has
  one descriptor, its stream's. The handle cap is a field of the session
  (256), so its test can use 8 and stay far below macOS's default limit
  of 256 descriptors.
- **No extensions are advertised**, in particular not
  `limits@openssh.com`: with it, OpenSSH's client raises its request size
  to the server's limit and keeps 64 requests in flight, 16 MiB, past the
  8 MiB input cap. Without it, it sends 32 KiB requests, 2 MiB in flight.
  `posix-rename`, `statvfs`, `fsync`, `hardlink` and `expand-path` are
  missing too; `SSH_FXP_EXTENDED` gets `OpUnsupported`.
- **setstat** applies nothing when anything but the mode and the times is
  asked for. russh-sftp's attributes drop extended pairs, so the raw
  attribute flags are checked on the wire as well.
- **symlink** takes OpenSSH's argument order (target, then link path),
  which every client follows, not the draft's (or russh-sftp's field
  names). The target is stored as given.
- **rename** fails when the new name exists, as SFTP v3 says, atomically:
  `renameat2(RENAME_NOREPLACE)` on Linux, `renameatx_np(RENAME_EXCL)` on
  macOS, through rustix's `renameat_with`. Where the filesystem has no such
  rename (EINVAL, ENOSYS, ENOTSUP), a file is moved as OpenSSH's
  sftp-server moves it, by a hard link and an unlink, which cannot replace
  either; if the unlink fails, the new link is removed again and the
  unlink's error is the reply, so a retry does not find the new name
  taken. A directory gets the original error.
- **open** maps SFTP's flags straight to `open(2)`'s (Rust's `OpenOptions`
  refuses a create opened only for reading), creates files with the
  client's mode less the umask (`0666` without one), and opens
  non-blocking, so a FIFO cannot hold a job forever; reading one with no
  data fails instead of waiting.
- **realpath** canonicalizes the path as given first, so `file/` and
  `file/.` fail as they should; only if part of it is missing does it
  resolve the longest prefix of the path that exists,
  symlinks and all, then works the rest, which does not exist, out by
  name (`link/../new`, with `link` pointing at `/srv/data`, is
  `/srv/new`). Only a missing component is passed over; any other error
  is the reply.
- **readdir** leaves out `.` and `..`, entries that went away while being
  listed, and names that are not UTF-8 (logged at debug). Long names are
  `ls -l` style with numeric owners.
- **Names are UTF-8**: russh-sftp's types are `String`s. No name is ever
  converted lossily: requests with other names are refused, listings
  leave them out, and `readlink` or `realpath` of a result that is not
  UTF-8 fails.
- **Dependencies.** russh-sftp's own russh dependency is dev-only: no
  second russh, tokio or crypto stack. New in the lockfile: russh-sftp,
  dashmap (with crossbeam-utils and a second, older hashbrown),
  serde_bytes, and gloo-timers (wasm only, never built). chrono and
  iana-time-zone, already in the lockfile, are now built into the binary.
- **Tests.** No in-process test drives an SFTP session past the input cap:
  the loop drains its input faster than a client can fill it, and a
  client that stops reading stalls its own russh session first. The cap's
  path is the one shell and exec use. The no-replace rename's link
  fallback is a function of its own, tested directly (success, an
  existing name, a directory, and an unlink that fails), since the test
  filesystems all have `renameat2`/`renameatx_np`. A teardown landing
  inside the exit report has no test: no hook makes it land there without
  test-only waits in the supervisor. The close test checks that closes
  happen and give their permits back, not that they are off the runtime.
  The append test only
  fails on macOS if append writes regress to `pwrite`; Linux appends
  either way. A test-only count of running channel
  supervisors lets the channel-close test see the supervisor end while
  the connection stays up.

## Risks

Top risks:

1. **russh input with no per-channel back-pressure.** Fast senders into
   slow programs are cut off at 8 MiB rather than slowed. If real use hits
   this, the options are a bigger or configurable cap, or a russh fork
   with manual window grants (upstream has declined that once, in #730).
2. **PTY fd and pid ownership through cancellation and reaping.**
3. **Shutdown admission and per-channel resource cleanup across many
   channels.**

Also:

- **russh churn and advisories.** The exact version is pinned, and
  `cargo deny` runs in CI. Only ticket holders reach the SSH layer, and the
  pre-auth deadline bounds stalls.
- **Process edge cases.** Children are not reaped until escalation is done,
  teardown is concurrent and bounded, and there is a last-resort registry.
  Detached and job-control survivors are documented, matching sshd.
- **SFTP clients that pipeline deeply.** The input cap applies to SFTP
  requests too. OpenSSH's scp and sftp keep about 2 MiB in flight; a
  client that queues more than 8 MiB of writes ahead of their replies has
  its session closed.
