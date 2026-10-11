# Draft upstream reports for russh (not filed)

Drafts for <https://github.com/warp-tech/russh>, from the flow-control spike
([builtin-ssh-spike.md](builtin-ssh-spike.md)). Nothing here has been
posted. Every repro is taken from `spike/russh-flow` and was run against
russh `=0.64.1` (`default-features = false, features = ["ring"]`), tokio
1.53, Linux.

Shared harness for the repros below (see `spike/russh-flow/src/proxy.rs`):

- **relay**: a full-duplex byte relay between client and server, with an
  unbounded queue per direction (it always keeps reading both sides, like
  OpenSSH);
- **cleartext proxy**: both ends set `Preferred { cipher: &[cipher::NONE],
  mac: &[mac::NONE], .. }`. After kex each packet is then
  `u32 len | u8 padlen | payload | padding`, so the proxy can read, count,
  hold back or rewrite single messages (russh checks no sequence numbers
  without a MAC).

---

## 1. Comment on #730 (consumption-gated `WINDOW_ADJUST`)

> Following up with a concrete case where "always drain your channels" has
> no bounded answer, in case it changes the calculus.
>
> **Setup.** A server runs `exec cat` and feeds the child from the
> channel the natural way:
>
> ```rust
> // server: after channel_open_session accepts and exec spawns `cat`
> let (mut read, write) = channel.split();
> tokio::spawn(async move {            // stdout -> channel
>     let mut w = write.make_writer();
>     tokio::io::copy(&mut child_stdout, &mut w).await
> });
> while let Some(msg) = read.wait().await {   // channel -> stdin
>     if let ChannelMsg::Data { data } = msg {
>         child_stdin.write_all(&data).await?; // back-pressure from the child
>     }
> }
> ```
>
> The client is OpenSSH-like: `ssh h cat < big | slow`. It keeps uploading
> but grants window for stdout only as `slow` consumes it.
>
> **What happens (0.64.1).**
>
> 1. `cat`'s stdout blocks once the client's window is used up.
> 2. `cat` stops reading stdin, so the pipe fills, and so does the
>    `ChannelReadHalf` (`channel_buffer_size` = 100 messages).
> 3. The session loop then blocks in `chan.send(ChannelMsg::Data { .. }).await`
>    (`server/encrypted.rs:1352`) while handling the next incoming data
>    packet.
> 4. From then on it reads nothing else. When the client grants window,
>    its `CHANNEL_WINDOW_ADJUST` sits unread, so `cat`'s stdout never
>    unblocks.
>
> The connection is dead for good: keepalives and close are stuck too.
>
> **Repro numbers.** 8 MiB through `cat`; for the first 2 s the client's
> `WINDOW_ADJUST`s are held back by a proxy, then released. The server
> sends 2 MiB and receives 6.25 MiB, then nothing more, still 10 s after
> the release. Replace the read half with an unbounded buffer filled in
> `Handler::data` and the same run completes 13 ms after the release
> (4.9 MiB buffered).
>
> So the application's only choices are a connection-wide deadlock or an
> unbounded (or capped-and-kill) buffer per channel. We've gone with an
> 8 MiB cap per channel that closes the channel when it's exceeded, which
> works, but it means a fast sender into a slow program is cut off rather
> than slowed. A way to tie `WINDOW_ADJUST` to consumption would let a
> server apply per-channel back-pressure. That could be opt-in, for
> example a `Handler::adjust_window` that can return 0, plus a
> `Session::grant_window(channel, n)`.
>
> Repro (tokio test, about 80 lines plus the proxy):
> `case1c_naive_read_half_deadlocks` versus
> `case1d_plan_design_survives_withheld_window` in
> <https://github.com/deepso7/minipaw/blob/main/spike/russh-flow/tests/spike.rs>.

---

## 2. Issue: `WINDOW_ADJUST` overwrites `ChannelTx`'s credit counter, losing in-flight reservations

**Title:** Incoming WINDOW_ADJUST overwrites the writer's window counter;
reserved-but-queued bytes are sent twice against the same credit

**russh version:** 0.64.1 (server side; the client has the same code path).

**Summary.**

- `ChannelTx::poll_writable` (`channels/io/tx.rs:82`) and
  `ChannelWriteHalf::reserve_writable_chunk` subtract credit from the
  shared `WindowSizeRef` *before* the message is queued on the session's
  bounded handle channel.
- When a `CHANNEL_WINDOW_ADJUST` arrives, the session computes
  `new_size = recipient_window_size + amount − flushed`
  (`server/encrypted.rs:1373–1384`) and **sets** the shared counter to
  it (`chan.window_size().update(new_size)`, `channels/mod.rs:131`).
- `recipient_window_size` knows nothing about messages already reserved
  but still in the handle queue. Those reservations are wiped, so writers
  reserve the same credit again.
- When the queued messages reach `Encrypted::data`, they go past the real
  window and become `pending_data`.
- While any channel has pending data, the session stops taking messages
  from the handle queue at all (`server/session.rs:681,791`), so **every
  channel on the connection** waits for that one channel's next grant
  (the #725 behaviour).

No data is lost; the effect is extra pending data and cross-channel
stalls.

**Probe (16 bytes on 9 credit).**

- The client opens a session with `window_size = 9`. The server runs
  `yes 0123456789abcde | head -n 2000 >&2 & yes 0123456789abcde | head -n 2000`,
  so there are two writers (stdout through `make_writer()`, stderr through
  `make_writer_ext(Some(1))`), each writing 16-byte lines.
- A cleartext proxy forwards each client `WINDOW_ADJUST(n)` as `n`
  followed by 15 `WINDOW_ADJUST(0)`. These are legal and model adjusts
  arriving back to back.
- We count credit taken by `ChannelTx` (bytes returned from `poll_write`)
  against credit the client granted (initial window plus forwarded
  adjusts).

Result:

- Credit taken ran **18 bytes ahead** of credit granted (at most two
  writers × 9 bytes).
- `Session::has_pending_data(channel)` was true in `window_adjusted` for
  about 31,000 of the adjusts.
- Everything arrived (32,000 + 32,000 bytes, exit-status 0).

With bulk output (`seq 1 1000000 & seq 1 500000 >&2`, 64 KiB client
window, adjusts held back and released in bursts, each split in 8), credit
ran **32 KiB ahead**, and pending data was seen after about 160–210
adjusts. Without the split, we saw no over-commit with russh's own client.

**Expected.**

- `WINDOW_ADJUST` should *add* `amount` to the writer-side counter (minus
  what the flush consumed), not set it to the transport's view.
- Alternatively, the transport's `recipient_window_size` could count
  reservations.

**Repro:** `case6b_probe_16_bytes_on_9_credit` and
`case6_window_adjust_while_output_queued` in the spike. Both use the
cleartext proxy with `split_adjusts`.

---

## 3. Issue: after a local `close`, the peer's `CHANNEL_CLOSE` is dropped; `Handler::channel_close` never runs and the `ChannelRef` leaks until disconnect

**russh version:** 0.64.1, server (and client, symmetrically).

**Summary.**

- `Encrypted::close` (`session.rs:287–292`) sends `CHANNEL_CLOSE` and
  removes the channel from `enc.channels` at once.
- When the peer's reciprocal `CHANNEL_CLOSE` arrives,
  `is_established_channel` is false (`server/encrypted.rs:1282–1287`), so
  the message is ignored, and three things never happen:
  - `Handler::channel_close` is never called;
  - `Session::channels` keeps the `ChannelRef` (a `Sender<ChannelMsg>` and
    a `WindowSizeRef`) until the connection ends;
  - a live `ChannelReadHalf` never receives `ChannelMsg::Close`.

**Repro.**

- A server that, on exec, runs the command and then calls
  `exit_status`, `eof`, and `close` on its `ChannelWriteHalf`.
- 1,000 sequential connections, each `exec "echo hi"`. The client reads
  until `Close`, then disconnects.

```text
channel_close callbacks:            0 of 1000
RSS (KiB) every 100 connections:    5252 6836 6880 6912 6916 6924 6924 6932 6936 6936 6944
```

- Memory stays flat only because each connection ends.
- On a long-lived connection that opens and closes many channels
  (multiplexing, port forwarding), each locally closed channel leaks its
  `ChannelRef` until disconnect.
- Client side, the same holds: after `ChannelWriteHalf::close()` from
  the client, the server's `channel_close` fires, but the client's
  `ChannelReadHalf` never sees `Close` (spike `case4`).

**Expected.** Keep the channel in a "close sent" state until the peer's
close arrives (RFC 4254 §5.3), then remove it and call
`Handler::channel_close`. Or remove it from `Session::channels` too on a
local close.

**Repro:** `case7_thousand_connections_flat_memory` (counts
`channel_close` callbacks) and `case4_keepalive_and_close_while_slow`.

---

## 4. Issue: data beyond the advertised window or maximum packet size is accepted and delivered

**russh version:** 0.64.1 (server; the client's code is the same).

**Summary.**

- `Encrypted::adjust_window_size` (`session.rs:305–333`) skips the
  decrement when a `CHANNEL_DATA` exceeds the remaining window ("Ignore
  extra data", citing RFC 4254 §5.2). It still sends `WINDOW_ADJUST`,
  and the caller delivers the data to the channel and to `Handler::data`
  anyway (`server/encrypted.rs:1314–1358`).
- Nothing compares the payload length with the maximum packet size the
  receiver advertised (`Config::maximum_packet_size`).
- The only limit is the transport's maximum packet length (256 KiB plus
  headroom, `cipher/mod.rs:313`).

RFC 4254 §5.2 says the recipient MAY ignore such data. Today russh neither
ignores it nor disconnects. It passes it through unaccounted, so a peer can
push unbounded data into an application that relies on the window to bound
its buffering.

**Repro.**

- Server config: `window_size = 256 KiB`, `maximum_packet_size = 32 KiB`.
- A cleartext proxy rewrites the server's `CHANNEL_OPEN_CONFIRMATION` to
  advertise window `0x7fffffff` and maximum packet 128 KiB to the client.
  It is a stock russh client, which then follows the advertised values.
- The client writes 64 MiB into `exec "sleep 30; cat >/dev/null"`.

```text
largest CHANNEL_DATA payload delivered to Handler::data: 131072 (limit 32768)
client bytes beyond the server's real window:            up to 2.3 MiB
```

Our application cap (8 MiB per channel, counted in bytes received) caught
it. Without one, buffering is unbounded.

**Expected.** Reject (disconnect with `PROTOCOL_ERROR`), or at least drop,
any `CHANNEL_DATA` / `CHANNEL_EXTENDED_DATA` whose payload exceeds the
remaining window or the advertised maximum packet size. Optionally make
the choice configurable.

**Repro:** `case5_peer_over_window_and_packet_size`.

---

## 5. Issue (found in the spike, optional): closing a channel with pending data blocks the session's handle queue for good

**Summary.**

- If a channel has `pending_data` when it is closed (by the peer's
  `CHANNEL_CLOSE` or by a local close), `Encrypted::close` only sets
  `pending_close` (`session.rs:287–289`).
- The channel is closed, so no more `WINDOW_ADJUST` will come for it. The
  pending data never flushes, and `has_any_pending_data()` stays true for
  the rest of the connection.
- Both loops then stop taking messages from the handle queue
  (`server/session.rs:681,791`; `client/mod.rs:1376,1454,1479`). On the
  client this means `Handle::disconnect`, `send_ping` and
  `channel_open_*` hang. On the server every `Handle` /
  `ChannelWriteHalf` send for any channel blocks.

**Repro (client side).**

- The client uploads 100 MB into `exec "sleep 5; cat >/dev/null"`.
- A server-side cap closes the channel after 8 MiB.
- When the server's `CHANNEL_CLOSE` lands while the client still has
  queued data, the client's `disconnect()` never goes out and the
  connection stays open. This happened in 7 of 15 runs.

**Expected.** On close, drop that channel's pending data, since the peer
won't accept it. Then send `CHANNEL_CLOSE` (local close) or finish the
close (remote close), and stop counting the channel in
`has_any_pending_data()`.

**Repro:** `case3_slow_consumer_100mb_trips_cap`, before its switch to
cutting the socket: replace `kill.shutdown(..)` with
`h.disconnect(..).await` and loop the test.

---

## 6. Note, not an issue: two russh endpoints on one socket deadlock under full-duplex bulk

This may be worth a sentence in the docs rather than an issue.

- Both session loops await `flush_or_timeout` after each `select!`
  (`server/session.rs:811`, `client/mod.rs:1490`) and read nothing while
  a write is blocked.
- A russh client connected straight to a russh server (socket pair or
  loopback TCP) running `cat` with 100 MB each way deadlocks after about
  655 KB. Each side's kernel buffer is full and each loop is stuck in
  `write`.
- It works with any peer that keeps reading (OpenSSH, or a full-duplex
  relay in between).

Repro: `case1_direct_russh_client_deadlocks` (`#[ignore]`d).
