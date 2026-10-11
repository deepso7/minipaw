# Spike: flow control on stock russh 0.64.1

Rollout step 1 of [builtin-ssh.md](builtin-ssh.md). Code:
`spike/russh-flow` (`publish = false`, throwaway). It is its own cargo
workspace, kept out of minipaw's build, CI and lockfile.

**Verdict: GO.** Every case in the plan passes on stock russh `=0.64.1`
(`default-features = false, features = ["ring"]`) with the plan's
mitigations. The spike found two more russh stalls, and neither reaches
production through the plan's design. Both change how PR 2 has to write its
tests (see "What later PRs must account for").

## What was built

- **Server** (`src/server.rs`), as the plan describes:
  - one session channel per connection (a second open is rejected);
  - `channel.split()`, and the `ChannelReadHalf` is dropped;
  - input only through `data` callbacks into a per-channel buffer capped at
    8 MiB, counted in bytes received. Passing the cap writes `minipaw: input
    exceeded 8 MiB buffer, closing` to stderr and closes that channel;
  - a writer task drains the buffer into the child's stdin;
  - output through `make_writer()` / `make_writer_ext(Some(1))` in chunks
    of at most 32 KiB;
  - a real child (`/bin/sh -c`, `tokio::process`, piped stdio,
    `process_group(0)`). Exit sends exit-status, eof, close. Teardown sends
    SIGKILL to the group, standing in for the plan's full ladder.
  - Auth is `none` and there is no PTY.
- **Client**: stock russh 0.64.1 client over `UnixStream::pair()`.
- **Relay** (`proxy::spawn_relay`): a full-duplex byte relay with an
  unbounded queue in each direction. It always keeps reading both sides,
  as OpenSSH does and as minipaw's `Io` helper threads do. Case 1b adds a
  rate limit on the download direction.
- **Abuse proxy** (`proxy::spawn`): both ends prefer cipher `none` and MAC
  `none` (russh offers both), so after kex every packet is cleartext
  `len | pad | payload | padding`. The proxy parses each packet and can:
  - rewrite the server's `CHANNEL_OPEN_CONFIRMATION`;
  - hold back the client's `CHANNEL_WINDOW_ADJUST`s and release them in a
    burst;
  - split each adjust into N back-to-back adjusts (most of them for 0 bytes);
  - count the credit each side grants.

  russh checks no sequence numbers without a MAC, so re-timed packets are
  accepted.

Run with:

```
cd spike/russh-flow && cargo test --release -- --test-threads=1 --nocapture
```

Every test prints a `RESULT` line. All of them are `#[ignore]`d in debug
builds, so the workspace's debug `cargo test` in CI skips them. The test
file is `cfg(target_os = "linux")` because it reads `/proc/self`.

## Results

Release build, one process per case (`--exact`). Machine: Linux 7.0
(aws), the same box as development. The whole suite takes 28.0 s
(`--test-threads=1`, 12 passed, 1 ignored) and was stable 5/5. Peak RSS
for the whole suite in one process was 27 MiB. "Peak RSS" below is the
process's VmHWM, which includes the client, the relay, and the test
harness.

| # | Case | Result | Wall | Peak RSS | Key numbers |
|---|---|---|---|---|---|
| 1 | `cat`, 100 MB both ways at once (through the relay) | **PASS** | 0.36 s | 16 MiB | 100,000,000 B each way, SHA-256 match, exit 0. Input buffer peaked at 1.3–4.5 MiB across runs, no cap trip. |
| 1b | Same, with downloads limited to 5 MB/s (asymmetric path) | PASS | 4.0 s | 11 MiB | Completes, exit 0. Input buffer peaked at 0.9 MiB. |
| 1c | #730 repro, rejected design: read half kept, child fed with back-pressure; client withholds window 2 s while uploading 8 MiB | **deadlocks, as expected** | stuck for good | 13 MiB | Output frozen at 2 MiB, input at 6.25 MiB; still stuck 10 s after the window came back. |
| 1d | Same scenario, plan's design | **PASS** | 2.01 s | 13 MiB | Completes 13 ms after the window returns. Buffer peaked at 4.9 MiB, no cap trip. |
| 2 | Slow consumer (`sleep 5; cat >/dev/null`), 4 MiB | **PASS** | 5.01 s | 12 MiB | Exit 0. 3.9 MiB buffered (the rest sat in the pipe). |
| 3 | Same consumer, 100 MB at line rate | **PASS** | 0.03 s | 16 MiB | Cut off 27–39 ms in. stderr is exactly the cap message, channel closed, no exit status. 8.6–9.5 MB received before the close landed. A second connection to the same server then runs `echo ok` with exit 0. 20/20 runs green. |
| 4 | Keepalive, ping and close while the consumer is slow (6 MiB buffered) | **PASS** | 2.03 s | 14 MiB | Server keepalive every 200 ms answered for 1.5 s, worst ping 0.17 ms, a second channel open refused. Client close reached `channel_close` in 2.1 ms and the child was reaped by then. |
| 5 | Peer over its window and packet size | **PASS** | 0.02 s | 17 MiB | Client sent 128 KiB packets (server max 32 KiB) and ran up to 2.3 MiB over the real 256 KiB window. russh passed every packet to `data`. The cap tripped at 8 MiB with the stderr message and a close. |
| 6 | `WINDOW_ADJUST` burst while stdout and stderr are queued (issue B) | **PASS** | 0.41–0.48 s | 20 MiB | 6,888,896 B stdout plus 3,388,895 B stderr, both byte-exact, exit 7, eof, close. B triggered: 32 KiB of credit taken beyond what was granted, and pending data after 161–209 adjusts. |
| 6b | Probe: 16-byte lines on a 9-byte window | PASS | 0.41 s | 6.5 MiB | B triggered: 18 bytes over credit, pending data after about 31,000 adjusts. All 32,000 + 32,000 bytes and exit 0 arrived. |
| 7 | 1,000 sequential connections, each `echo hi` (issue C) | **PASS** | 1.7 s | 6.8 MiB | RSS in KiB every 100 connections: 5252 (before the first), 6836, 6880, 6912, 6916, 6924, 6924, 6932, 6936, 6936, 6944. That is **+108 KiB over 900 connections** after warm-up. fds steady at 10, 0 input buffers live. A 10,000-connection run (`SPIKE_CONNS=10000`, 16.7 s): 6996 → 7092 KiB, **+96 KiB over 9,000**. |
| – | Second session channel refused | PASS | 0.00 s | – | Refused, and the first channel still works. |
| – | Direct russh client to russh server, `cat` 100 MB (no relay) | **deadlocks** (`#[ignore]`d) | – | – | Stalls after about 655 KB each way. See finding 1. |

### Issue B in detail

`ChannelTx` takes credit from a shared counter before it queues a message
(`channels/io/tx.rs:82`). An incoming `WINDOW_ADJUST` *sets* that counter
to `recipient_window_size + amount − flushed`
(`server/encrypted.rs:1373–1386`, `channels/mod.rs:131`), which wipes
reservations still in the handle queue. The writers then reserve the same
credit again, and the excess becomes pending data.

- With russh's own client, which sends adjusts normally, and even with
  held-then-released bursts, the spike saw **no** over-commit (overcommit 0,
  pending 0).
- It shows up when adjusts arrive in quick succession, simulated by
  splitting each into N adjusts, most of them for 0 bytes.

Either way the pending bytes go out on the next grant, and every byte plus
the exit status arrives. With one channel per connection there is no
sibling for the gated handle queue to stall. This matches the plan.

### Issue C in detail

In case 7 the server always closes first (exit, then eof and close). The
client's reciprocal `CHANNEL_CLOSE` is dropped by `is_established_channel`
(`server/encrypted.rs:1285`), because `Encrypted::close` had already
removed the channel (`session.rs:287–292`).

- `channel_close` fired **0 times in 1,000 connections**, which reproduces
  C.
- Memory stays flat because the leftover `ChannelRef` is freed with the
  connection. One channel per connection makes C harmless, as the plan
  says.
- The spike frees its own state when the supervisor finishes. The plan's
  rule holds: never release state in `channel_close`.

## Findings that change or sharpen the plan

1. **New: two russh endpoints deadlock each other on a full-duplex stream.**
   russh's session loop does not read while it flushes. After each
   `select!` it awaits `flush_or_timeout` (`server/session.rs:811`,
   client side `client/mod.rs:1490`).
   - A russh client wired straight to the russh server over
     `UnixStream::pair()` deadlocks on `cat` with 100 MB after about
     655 KB: both loops sit in `write`, both sockets' buffers are full,
     and neither reads.
   - The same happens through any relay that forwards with a blocking
     `write_all`. The first version of the abuse proxy did this. It now
     queues each direction.
   - This is **not on the production path**. The server's socket peer is
     minipaw's `Io` (separate input and output threads, an unbounded queue
     toward the socket, in-flight data bounded by peer credit), and the
     far end is OpenSSH, which uses non-blocking I/O. Case 1 through the
     relay passes.
   - It does contradict the plan's test design: "In-process integration
     (`UnixStream::pair` + russh client)" for flow-control cases will hang
     unless a full-duplex relay sits between them.
   - **Invariant to keep:** whatever sits on our end of the socket
     (`Io::unix`) must keep reading it whatever its writes are doing.
     Today's helper threads do.

2. **New: the russh client wedges when the server closes a channel while
   the client still has queued data.**
   - The client's `Encrypted::close` defers the close while data is
     pending (`session.rs:287–289`). No window will come for a closed
     channel, so the client's `has_any_pending_data()` stays true.
   - The client then stops taking `Handle` messages at all
     (`client/mod.rs:1376`): `disconnect`, `send_ping` and new channels
     hang.
   - Case 3 hit this in 7 of 15 runs until the test cut the client's
     socket instead (what an exiting `ssh` does).
   - OpenSSH does not have this bug, so production is unaffected, but PR 2's
     tests of the cap must do the same as the spike.
   - The **server has the mirror image.** If a client closes a channel
     while the server has pending data (reachable only through B), the
     server's handle queue is gated for the rest of the connection. Our
     supervisor's later `ChannelWriteHalf` sends (exit-status, eof, close)
     would then block until the connection ends.
   - **PR 2 must bound or cancel every `ChannelWriteHalf` send in the
     supervisor** (select against teardown or connection end). The plan's
     "awaits them" teardown needs this to stay bounded.

3. **A client close is not visible to the client's own read half.**
   - In case 4, after `write.close()` from the client, the server's
     `channel_close` fired, and the child was killed and reaped within
     2 ms.
   - The client's `ChannelReadHalf` never got `Close`. It ended only with
     the connection. This is C on the client side.
   - Tests must watch the server for this, not
     `ChannelMsg::Close` on the client.

4. **An asymmetric path doesn't fill the buffer as quickly as the plan
   implies.**
   - russh grants receive window on arrival, but its `WINDOW_ADJUST`s
     travel server-to-client behind up to a full client window of queued
     output. A client uploading into `cat` over a slower download path is
     therefore paced by that path.
   - Case 1b's buffer stayed under 1 MiB at 5 MB/s against an unlimited
     upload.
   - The cap only bites when the program stops reading (case 3), or when
     the client stops granting window while it keeps uploading (1c/1d at
     8 MiB). At 9 MiB or more, 1d's scenario would trip the cap. That is
     the trade-off the plan already documents.

5. **Data over the window or packet size is unchecked, as the plan says,
   but bounded per packet.**
   - `adjust_window_size` ignores overrun (`session.rs:316`, "Ignore extra
     data") and still delivers the data. Nothing compares the payload to
     the advertised maximum packet size.
   - The only limit is the transport's maximum packet length (256 KiB
     plus headroom, `cipher/mod.rs:313,366–375`). So one `data` callback
     is at most about 256 KiB, and the cap's overshoot is bounded by that.
   - The cap counts received bytes, so it catches this (case 5).

6. **The #730 deadlock is real and the plan's design avoids it.**
   - With the rejected design (1c), once the client withholds window:
     `cat`'s stdout blocks, so the read half fills, so the session loop
     blocks in `chan.send` (`server/encrypted.rs:1352`). It never processes
     the client's later `WINDOW_ADJUST`s, so the connection is stuck for
     good.
   - With the same traffic, the plan's design (1d) completes.
   - Note that a russh *client* that simply stops reading does not trigger
     it. Its own loop stalls first, so it stops uploading. The repro needs
     OpenSSH-like behaviour (keeps uploading, withholds window), which the
     abuse proxy provides.

No case showed a stall that the 8 MiB cap and the one-channel rule don't
cover. Hence **GO**.

## What later PRs must account for

- **Tests (PR 2):**
  - Put a full-duplex relay (`proxy::spawn_relay`, or the real
    `Io::unix` path) between the russh client and server for any
    bidirectional bulk test, or it deadlocks (finding 1).
  - End connections in cap and close tests by cutting the client's socket,
    not `Handle::disconnect` (finding 2).
  - Check close on the server side (finding 3).
  - Mark timing and bulk tests release-only, or keep them small for CI.
- **Supervisor (PR 2):** every `ChannelWriteHalf` send (output, exit-status,
  eof, close, the cap message) must be cancellable by teardown or
  connection end. Never await one unbounded (finding 2). The spike bounds
  the cap message with a 2 s timeout.
- **`Io::unix` (PR 1):** document and test that both directions pump
  independently. A version that stops reading the socket while a write to
  it is blocked would deadlock the russh server (finding 1).
- **Cap teardown:** discard data that arrives after the cap trips, as the
  spike does. russh keeps granting window until our close lands, and in
  case 3 about 0.2–1.1 MB more arrived in that gap.
- **Unchanged:** 8 MiB cap, one channel per connection, read half dropped,
  ≤32 KiB window-aware writes, state released when the supervisor
  finishes, not in `channel_close`.

## Upstream reports

Drafted, not filed: [russh-upstream-reports.md](russh-upstream-reports.md).
They cover the #730 comment, B, C, unchecked data over the window or packet
size, and the client wedge from finding 2. The plan didn't list the last
one, but it came out of the spike and is cheap to file with the others.
