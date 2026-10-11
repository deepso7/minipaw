//! The spike cases of docs/plans/builtin-ssh.md, rollout step 1.
//!
//! Every case is ignored in debug builds (so the workspace's debug
//! `cargo test` in CI skips them: they sleep, push 100 MB, and assert
//! timings); run them with
//!
//! ```text
//! cd spike/russh-flow && cargo test --release -- --nocapture --test-threads=1
//! ```
//!
//! Each test prints one `RESULT` line with its wall time and the process's
//! peak RSS (VmHWM). Run one test per process (`--exact`) for a clean peak.
//!
//! Linux only: the measurements read `/proc/self`, and the spike is not
//! meant to gate CI on macOS.
#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use russh::Disconnect;
use russh::client::Handle;
use russh_flow::client::{self as sc, Client, Outcome, collect, sha256};
use russh_flow::proxy::{self, ProxyState};
use russh_flow::server::{Options, Server, cleartext_preferred};
use russh_flow::{CAP_MESSAGE, CHUNK, Stream, fd_count, get, hwm_kib, rss_kib};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const MIB: usize = 1024 * 1024;

fn report(case: &str, start: Instant, extra: &str) {
    println!(
        "RESULT case={case} wall_ms={} hwm_kib={} rss_kib={} {extra}",
        start.elapsed().as_millis(),
        hwm_kib(),
        rss_kib()
    );
}

async fn conn(server: &Server, cfg: russh::client::Config) -> (Handle<Client>, JoinHandle<()>) {
    let (a, b) = UnixStream::pair().unwrap();
    let served = server.serve(b);
    (sc::connect(a, cfg).await, served)
}

/// [`conn`], also returning a clone of the client's socket so a test can
/// cut it as a client process exiting would.
async fn conn_killable(
    server: &Server,
    cfg: russh::client::Config,
) -> (
    Handle<Client>,
    JoinHandle<()>,
    std::os::unix::net::UnixStream,
) {
    let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
    let kill = a.try_clone().unwrap();
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    let served = server.serve(UnixStream::from_std(b).unwrap());
    let h = sc::connect(UnixStream::from_std(a).unwrap(), cfg).await;
    (h, served, kill)
}

/// Connect through `proxy::spawn_relay`, a peer that keeps reading while
/// its writes are pending (as OpenSSH does).
async fn relayed(server: &Server, cfg: russh::client::Config) -> (Handle<Client>, JoinHandle<()>) {
    let (c1, c2) = UnixStream::pair().unwrap();
    let (s1, s2) = UnixStream::pair().unwrap();
    proxy::spawn_relay(c2, s1);
    let served = server.serve(s2);
    (sc::connect(c1, cfg).await, served)
}

async fn proxied(
    server: &Server,
    mut cfg: russh::client::Config,
    st: Arc<ProxyState>,
) -> (Handle<Client>, JoinHandle<()>) {
    let (c1, c2) = UnixStream::pair().unwrap();
    let (s1, s2) = UnixStream::pair().unwrap();
    proxy::spawn(c2, s1, st);
    let served = server.serve(s2);
    cfg.preferred = cleartext_preferred();
    (sc::connect(c1, cfg).await, served)
}

/// Write `total` bytes of the deterministic stream (seed 1) as fast as the
/// channel takes them, then EOF. Returns the SHA-256 of what was written.
fn spawn_writer(
    write: Arc<russh::ChannelWriteHalf<russh::client::Msg>>,
    total: usize,
    chunk: usize,
    eof: bool,
) -> JoinHandle<(usize, Vec<u8>)> {
    tokio::spawn(async move {
        let mut w = write.make_writer();
        let mut s = Stream::new(1);
        let mut sha = Sha256::new();
        let mut buf = vec![0u8; chunk];
        let mut sent = 0;
        while sent < total {
            let n = chunk.min(total - sent);
            s.fill(&mut buf[..n]);
            if w.write_all(&buf[..n]).await.is_err() {
                break;
            }
            sha.update(&buf[..n]);
            sent += n;
        }
        if eof && sent == total {
            let _ = write.eof().await;
        }
        (sent, sha.finalize().to_vec())
    })
}

/// Waits up to 5 s for `n` channel supervisors, helpers and all, to have
/// finished, so what is measured next is not cleanup still under way.
async fn settled(server: &Server, n: u64) {
    let done = &server.stats.supervisors_done;
    timeout(Duration::from_secs(5), async {
        while get(done) < n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{} of {n} supervisors finished", get(done)));
}

async fn run_simple(server: &Server, cmd: &str) -> Outcome {
    let (h, served) = conn(server, sc::config()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, cmd).await.unwrap();
    let (read, write) = ch.split();
    write.eof().await.unwrap();
    let out = timeout(Duration::from_secs(10), collect(read))
        .await
        .expect("simple exec stalled");
    h.disconnect(Disconnect::ByApplication, "", "")
        .await
        .unwrap();
    timeout(Duration::from_secs(5), served)
        .await
        .expect("server connection did not end")
        .unwrap();
    out
}

/// Case 1: `cat` with 100 MB in both directions at once, against a peer
/// that keeps reading while it writes (OpenSSH, or minipaw's tunnel in
/// front of it).
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case1_cat_100mb_both_ways() {
    cat_both_ways(true, "1-cat-100MB-relay").await;
}

/// Case 1 on an asymmetric path: the server's output reaches the client at
/// 5 MB/s while the client uploads as fast as it can. russh grants window
/// on receipt, but the grants travel server-to-client behind up to a
/// client window of queued `cat` output, so the client's upload slows to
/// about the download rate and the input buffer stays near one window.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case1b_cat_with_slower_download() {
    let start = Instant::now();
    let server = Server::new(Options::default());
    let (c1, c2) = UnixStream::pair().unwrap();
    let (s1, s2) = UnixStream::pair().unwrap();
    proxy::spawn_relay_limited(c2, s1, Some(5_000_000));
    let _served = server.serve(s2);
    let h = sc::connect(c1, sc::config()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, "cat").await.unwrap();
    let (read, write) = ch.split();
    let writer = spawn_writer(Arc::new(write), 20_000_000, CHUNK, true);
    let out = timeout(Duration::from_secs(60), collect(read))
        .await
        .expect("stalled");
    writer.abort();
    let st = &server.stats;
    report(
        "1b-cat-slow-download",
        start,
        &format!(
            "received={} exit={:?} max_buffered={} cap_trips={} stderr={:?}",
            out.stdout_len,
            out.exit_status,
            st.max_buffered.load(Relaxed),
            get(&st.cap_trips),
            String::from_utf8_lossy(&out.stderr),
        ),
    );
    assert_eq!(get(&st.cap_trips), 0);
    assert_eq!(out.stdout_len, 20_000_000);
    assert_eq!(out.exit_status, Some(0));
    assert!(st.max_buffered.load(Relaxed) < 4 * MIB);
}

/// How a withheld-window run ended.
#[derive(Debug, PartialEq)]
enum Withheld {
    /// All of `cat`'s output and exit status 0 arrived.
    Completed,
    /// Nothing ended within 10 s of the window coming back: the deadlock.
    Stuck,
    /// Anything else (an early close, a wrong status, a failed task),
    /// which proves neither.
    Failed(String),
}

/// The #730 repro, A/B. `cat` with 8 MiB, and for the first 2 s the client
/// grants no window for output while it keeps uploading, as OpenSSH does
/// when its stdout is slow (`ssh h cat <big | slow`). A proxy holds back the
/// client's WINDOW_ADJUSTs, then releases them.
///
/// - `naive` (the design the plan rejects): the server keeps the
///   `ChannelReadHalf` and feeds the child from it with back-pressure.
///   `cat`'s stdout blocks on window, `cat` stops reading stdin, the read
///   half fills, and the server's session loop blocks in `chan.send` for
///   the next incoming data. It then never processes the released
///   WINDOW_ADJUSTs: the connection stays stuck for good.
/// - the plan's design (read half dropped, capped callback buffer) buffers
///   the input and completes once window comes back.
async fn cat_with_withheld_window(naive: bool, name: &str) -> Withheld {
    let start = Instant::now();
    let st = ProxyState::new(None);
    st.hold_adjusts.store(true, Relaxed);
    let server = Server::new(Options {
        naive_read_half: naive,
        cleartext: true,
        ..Default::default()
    });
    let (h, _served) = proxied(&server, sc::config(), st.clone()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, "cat").await.unwrap();
    let (read, write) = ch.split();
    let total = 8 * MIB;
    let writer = spawn_writer(Arc::new(write), total, CHUNK, true);
    let collector = tokio::spawn(collect(read));
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        eprintln!(
            "progress: held={} server_in={} server_out={} buffered={}",
            get(&st.adjusts_held),
            get(&server.stats.input_bytes),
            get(&server.stats.reserved_out),
            server.stats.max_buffered.load(Relaxed)
        );
    }
    let held = get(&st.adjusts_held);
    eprintln!("releasing");
    let released = timeout(Duration::from_secs(3), st.release()).await.is_ok();
    eprintln!("released={released}");
    let out = timeout(Duration::from_secs(10), collector).await;
    let ss = &server.stats;
    let outcome = match &out {
        Ok(Ok(o)) if o.exit_status == Some(0) && o.stdout_len == total as u64 => {
            Withheld::Completed
        }
        Err(_) => Withheld::Stuck,
        Ok(Ok(o)) => Withheld::Failed(format!(
            "ended early: exit={:?} stdout={}",
            o.exit_status, o.stdout_len
        )),
        Ok(Err(e)) => Withheld::Failed(format!("collector failed: {e}")),
    };
    report(
        name,
        start,
        &format!(
            "outcome={outcome:?} adjusts_held={held} server_in={} server_out={} max_buffered={} cap_trips={}",
            get(&ss.input_bytes),
            get(&ss.reserved_out),
            ss.max_buffered.load(Relaxed),
            get(&ss.cap_trips),
        ),
    );
    writer.abort();
    outcome
}

#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case1c_naive_read_half_deadlocks() {
    assert_eq!(
        cat_with_withheld_window(true, "1c-withheld-window-naive").await,
        Withheld::Stuck,
        "naive design did not deadlock"
    );
}

#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case1d_plan_design_survives_withheld_window() {
    assert_eq!(
        cat_with_withheld_window(false, "1d-withheld-window-plan").await,
        Withheld::Completed
    );
}

/// Case 1 with the russh client wired straight to the russh server: both
/// session loops block in their socket write without reading, and the
/// connection deadlocks after a few hundred KiB. Kept as the upstream
/// repro; ignored because it hangs until its 120 s timeout.
#[ignore = "deadlocks: russh client to russh server over a socket pair, see builtin-ssh-spike.md"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case1_direct_russh_client_deadlocks() {
    cat_both_ways(false, "1-cat-100MB-direct").await;
}

async fn cat_both_ways(relay: bool, name: &str) {
    let start = Instant::now();
    let server = Server::new(Options::default());
    let (h, _served) = if relay {
        relayed(&server, sc::config()).await
    } else {
        conn(&server, sc::config()).await
    };
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, "cat").await.unwrap();
    let (read, write) = ch.split();
    let total = 100_000_000;
    let writer = spawn_writer(Arc::new(write), total, CHUNK, true);
    let watch = {
        let st = server.stats.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                eprintln!(
                    "progress: server_in={} buffered_max={} reserved_out={} cap_trips={} pending_after_adjust={}",
                    get(&st.input_bytes),
                    st.max_buffered.load(Relaxed),
                    get(&st.reserved_out),
                    get(&st.cap_trips),
                    get(&st.pending_after_adjust),
                );
            }
        })
    };
    let out = timeout(Duration::from_secs(120), collect(read))
        .await
        .expect("cat 100 MB stalled");
    watch.abort();
    let (sent, in_sha) = writer.await.unwrap();
    let st = &server.stats;
    report(
        name,
        start,
        &format!(
            "sent={sent} received={} exit={:?} max_buffered={} cap_trips={} pending_after_adjust={}",
            out.stdout_len,
            out.exit_status,
            st.max_buffered.load(Relaxed),
            get(&st.cap_trips),
            get(&st.pending_after_adjust),
        ),
    );
    assert_eq!(sent, total);
    assert_eq!(out.stdout_len, total as u64);
    assert_eq!(out.stdout_sha, in_sha, "bytes differ");
    assert_eq!(out.exit_status, Some(0));
    assert!(out.eof && out.closed);
    assert_eq!(get(&st.cap_trips), 0);
}

/// Case 2: a slow consumer fed 4 MiB completes.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case2_slow_consumer_4mib() {
    let start = Instant::now();
    let server = Server::new(Options::default());
    let (h, _served) = conn(&server, sc::config()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, "sleep 5; cat >/dev/null").await.unwrap();
    let (read, write) = ch.split();
    let writer = spawn_writer(Arc::new(write), 4 * MIB, CHUNK, true);
    let out = timeout(Duration::from_secs(30), collect(read))
        .await
        .expect("slow consumer stalled");
    let (sent, _) = writer.await.unwrap();
    let st = &server.stats;
    report(
        "2-slow-4MiB",
        start,
        &format!(
            "sent={sent} exit={:?} max_buffered={} cap_trips={}",
            out.exit_status,
            st.max_buffered.load(Relaxed),
            get(&st.cap_trips)
        ),
    );
    assert_eq!(sent, 4 * MIB);
    assert_eq!(out.exit_status, Some(0));
    assert!(out.closed);
    assert_eq!(get(&st.cap_trips), 0);
    assert!(start.elapsed() >= Duration::from_secs(5));
}

/// Case 3: the same consumer fed 100 MB at line rate is cut off at the cap,
/// and the server keeps serving.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case3_slow_consumer_100mb_trips_cap() {
    let start = Instant::now();
    let server = Server::new(Options::default());
    let (h, served, kill) = conn_killable(&server, sc::config()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, "sleep 5; cat >/dev/null").await.unwrap();
    let (read, write) = ch.split();
    let writer = spawn_writer(Arc::new(write), 100_000_000, CHUNK, true);
    let out = timeout(Duration::from_secs(30), collect(read))
        .await
        .expect("cap teardown stalled");
    let tripped_after = start.elapsed();
    writer.abort();
    let st = &server.stats;
    let input_at_close = get(&st.input_bytes);
    drop(h);
    // `h.disconnect` (and `send_ping`) are not used: russh's client holds Handle messages while
    // any channel has pending data, and a remote close of a channel with
    // pending data is deferred until window that never comes, so the
    // client's `Disconnect` sometimes never leaves (see the spike notes).
    // Cut the socket instead, as an exiting `ssh` would.
    kill.shutdown(std::net::Shutdown::Both).unwrap();
    timeout(Duration::from_secs(5), served)
        .await
        .expect("first connection did not end")
        .unwrap();

    let second = run_simple(&server, "echo ok").await;
    settled(&server, 2).await;
    report(
        "3-slow-100MB-cap",
        start,
        &format!(
            "closed_after_ms={} input_bytes={input_at_close} max_buffered={} cap_trips={} stderr={:?} exit={:?} second_exit={:?} supervisors_done={}",
            tripped_after.as_millis(),
            st.max_buffered.load(Relaxed),
            get(&st.cap_trips),
            String::from_utf8_lossy(&out.stderr),
            out.exit_status,
            second.exit_status,
            get(&st.supervisors_done),
        ),
    );
    assert_eq!(String::from_utf8_lossy(&out.stderr), CAP_MESSAGE);
    assert!(out.closed);
    assert_eq!(out.exit_status, None);
    assert_eq!(get(&st.cap_trips), 1);
    assert!(
        tripped_after < Duration::from_secs(5),
        "cut off only after the sleep"
    );
    assert_eq!(second.exit_status, Some(0));
    assert_eq!(second.stdout_len, 3);
    assert_eq!(get(&st.supervisors_done), 2);
    assert_eq!(st.live_inputs.load(Relaxed), 0);
}

/// Case 4: keepalives, pings and close keep working while the consumer is
/// slow and 6 MiB sit in its input buffer.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case4_keepalive_and_close_while_slow() {
    let start = Instant::now();
    let server = Server::new(Options {
        keepalive: Some(Duration::from_millis(200)),
        ..Default::default()
    });
    let (h, served) = conn(&server, sc::config()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, "sleep 30; cat >/dev/null").await.unwrap();
    let (read, write) = ch.split();
    let write = Arc::new(write);
    let writer = spawn_writer(write.clone(), 6 * MIB, CHUNK, false);
    let st = server.stats.clone();
    // Wait until the buffer holds (nearly) everything.
    timeout(Duration::from_secs(10), async {
        while st.max_buffered.load(Relaxed) < 6 * MIB - 128 * 1024 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("input never buffered");
    let (sent, _) = writer.await.unwrap();

    // 1.5 s with nothing from the client but keepalive replies; the server
    // drops the connection after 3 unanswered keepalives (600-800 ms).
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        get(&st.conns_ended),
        0,
        "server dropped the client while slow"
    );

    let mut ping_max = Duration::ZERO;
    for _ in 0..5 {
        let t = Instant::now();
        timeout(Duration::from_secs(2), h.send_ping())
            .await
            .expect("ping stalled")
            .unwrap();
        ping_max = ping_max.max(t.elapsed());
    }
    let buffered_during_pings = st.max_buffered.load(Relaxed);

    // Close the channel from the client while the child still sleeps.
    let t_close = Instant::now();
    // A second session channel on this connection is refused.
    assert!(h.channel_open_session().await.is_err());
    write.close().await.unwrap();
    let collector = tokio::spawn(collect(read));
    timeout(Duration::from_secs(5), async {
        while get(&st.channel_close_callbacks) < 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("server never saw the close");
    let close_rtt = t_close.elapsed();
    timeout(Duration::from_secs(5), async {
        while get(&st.supervisors_done) < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("child not torn down after close");
    let teardown = t_close.elapsed();
    // russh's client drops the server's reciprocal CHANNEL_CLOSE after a
    // local close (issue C, client side), so the read half never sees
    // `Close`; it only ends with the connection. Record what it saw.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let client_saw_close = collector.is_finished();
    timeout(Duration::from_secs(2), h.send_ping())
        .await
        .expect("ping after close stalled")
        .unwrap();

    report(
        "4-keepalive-close-slow",
        start,
        &format!(
            "sent={sent} buffered={buffered_during_pings} ping_max_ms={:.2} server_close_cb_ms={:.2} child_reaped_ms={:.2} channel_close_cb={} client_read_half_saw_close={}",
            ping_max.as_secs_f64() * 1e3,
            close_rtt.as_secs_f64() * 1e3,
            teardown.as_secs_f64() * 1e3,
            get(&st.channel_close_callbacks),
            client_saw_close,
        ),
    );
    assert!(ping_max < Duration::from_millis(500));
    assert!(teardown < Duration::from_secs(2));
    assert_eq!(get(&st.channel_close_callbacks), 1);
    assert_eq!(get(&st.conns_ended), 0);
    h.disconnect(Disconnect::ByApplication, "", "")
        .await
        .unwrap();
    timeout(Duration::from_secs(5), served)
        .await
        .unwrap()
        .unwrap();
    // With the connection gone the client's read half finally ends.
    let out = timeout(Duration::from_secs(5), collector)
        .await
        .unwrap()
        .unwrap();
    assert!(!out.closed || client_saw_close);
    assert_eq!(get(&st.conn_errors), 0);
}

/// Case 5: a client that ignores the server's window and maximum packet
/// size is caught by the cap.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case5_peer_over_window_and_packet_size() {
    let start = Instant::now();
    let st = ProxyState::new(Some((0x7fff_ffff, 128 * 1024)));
    let server = Server::new(Options {
        cleartext: true,
        window: Some(256 * 1024),
        ..Default::default()
    });
    let (h, _served) = proxied(&server, sc::config(), st.clone()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(true, "sleep 30; cat >/dev/null").await.unwrap();
    let (read, write) = ch.split();
    let writer = spawn_writer(Arc::new(write), 64 * MIB, 128 * 1024, true);
    let out = timeout(Duration::from_secs(20), collect(read))
        .await
        .expect("abusive peer not cut off");
    writer.abort();
    let ss = &server.stats;
    report(
        "5-over-window-and-packet",
        start,
        &format!(
            "client_max_packet={} server_saw_max_packet={} max_over_window={} input_bytes={} max_buffered={} cap_trips={} stderr={:?} closed={}",
            st.max_client_packet.load(Relaxed),
            ss.max_data_packet.load(Relaxed),
            st.max_over_window.load(Relaxed),
            get(&ss.input_bytes),
            ss.max_buffered.load(Relaxed),
            get(&ss.cap_trips),
            String::from_utf8_lossy(&out.stderr),
            out.closed,
        ),
    );
    assert!(
        ss.max_data_packet.load(Relaxed) > 32 * 1024,
        "oversize packet not seen"
    );
    assert!(
        st.max_over_window.load(Relaxed) > 0,
        "never sent over the window"
    );
    assert_eq!(get(&ss.cap_trips), 1);
    assert_eq!(String::from_utf8_lossy(&out.stderr), CAP_MESSAGE);
    assert!(out.closed);
}

fn seq(n: u32) -> Vec<u8> {
    (1..=n)
        .flat_map(|i| format!("{i}\n").into_bytes())
        .collect()
}

/// Case 6 (russh issue B): stdout and stderr are queued while the client
/// holds back its window, then the held WINDOW_ADJUSTs arrive in a burst.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case6_window_adjust_while_output_queued() {
    let start = Instant::now();
    let st = ProxyState::new(None);
    let server = Server::new(Options {
        cleartext: true,
        client_credit: Some(st.client_credit.clone()),
        ..Default::default()
    });
    let mut cfg = sc::config();
    cfg.window_size = 64 * 1024;
    // Each released adjust arrives as 8 back-to-back adjusts.
    st.split_adjusts.store(8, Relaxed);
    let (h, _served) = proxied(&server, cfg, st.clone()).await;
    let ch = h.channel_open_session().await.unwrap();
    st.hold_adjusts.store(true, Relaxed);
    ch.exec(true, "seq 1 1000000 & seq 1 500000 >&2; wait; exit 7")
        .await
        .unwrap();
    let (read, write) = ch.split();
    write.eof().await.unwrap();
    let reader = tokio::spawn(collect(read));
    let toggler = {
        let st = st.clone();
        tokio::spawn(async move {
            loop {
                st.hold_adjusts.store(true, Relaxed);
                tokio::time::sleep(Duration::from_millis(60)).await;
                st.release().await;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };
    let out = timeout(Duration::from_secs(60), reader)
        .await
        .expect("output stalled after window adjusts")
        .unwrap();
    toggler.abort();
    let ss = &server.stats;
    let (want_out, want_err) = (seq(1_000_000), seq(500_000));
    report(
        "6-window-adjust-queued",
        start,
        &format!(
            "stdout={} stderr={} exit={:?} adjusts_held={} overcommit_max={} pending_after_adjust={} window_adjusted_cb={}",
            out.stdout_len,
            out.stderr.len(),
            out.exit_status,
            get(&st.adjusts_held),
            ss.overcommit_max.load(Relaxed),
            get(&ss.pending_after_adjust),
            get(&ss.window_adjusted_callbacks),
        ),
    );
    assert_eq!(out.stdout_sha, sha256(&want_out));
    assert!(out.stderr == want_err, "stderr differs");
    assert_eq!(out.exit_status, Some(7));
    assert!(out.eof && out.closed);
    assert!(get(&st.adjusts_held) > 0);
    // The run only counts as a repro if B happened: credit taken past the
    // window the client granted.
    assert!(
        ss.overcommit_max.load(Relaxed) > 0,
        "issue B not reproduced"
    );
}

/// The 16-bytes-on-9-credit probe for the upstream B report: the client
/// grants a 9-byte window and the server writes 16-byte lines on stdout and
/// stderr. Reports how far credit taken by `ChannelTx` ran ahead of credit
/// the client had granted.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case6b_probe_16_bytes_on_9_credit() {
    let start = Instant::now();
    let st = ProxyState::new(None);
    let server = Server::new(Options {
        cleartext: true,
        client_credit: Some(st.client_credit.clone()),
        ..Default::default()
    });
    let mut cfg = sc::config();
    cfg.window_size = 9;
    // Each client adjust (5..9 bytes) arrives as 16 adjusts, most of them
    // for 0 bytes: every one resets ChannelTx's counter to russh's view.
    st.split_adjusts.store(16, Relaxed);
    let (h, _served) = proxied(&server, cfg, st.clone()).await;
    let ch = h.channel_open_session().await.unwrap();
    ch.exec(
        true,
        "yes 0123456789abcde | head -n 2000 >&2 & yes 0123456789abcde | head -n 2000; wait",
    )
    .await
    .unwrap();
    let (read, write) = ch.split();
    write.eof().await.unwrap();
    let out = timeout(Duration::from_secs(60), collect(read))
        .await
        .expect("probe stalled");
    let ss = &server.stats;
    report(
        "6b-probe-16-on-9",
        start,
        &format!(
            "stdout={} stderr={} exit={:?} reserved={} client_credit={} overcommit_max={} pending_after_adjust={}",
            out.stdout_len,
            out.stderr.len(),
            out.exit_status,
            get(&ss.reserved_out),
            get(&st.client_credit),
            ss.overcommit_max.load(Relaxed),
            get(&ss.pending_after_adjust),
        ),
    );
    assert_eq!(out.stdout_len, 32_000);
    assert_eq!(out.stderr.len(), 32_000);
    assert_eq!(out.exit_status, Some(0));
    assert!(
        ss.overcommit_max.load(Relaxed) > 0,
        "issue B not reproduced"
    );
}

/// Case 7 (russh issue C): 1,000 sequential connections, each running a
/// command; memory and descriptors stay flat.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case7_thousand_connections_flat_memory() {
    let start = Instant::now();
    let server = Server::new(Options::default());
    let mut samples = Vec::new();
    let mut fds = Vec::new();
    // SPIKE_CONNS overrides the count, to check the trend over longer runs.
    let n: u32 = std::env::var("SPIKE_CONNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    for i in 0..=n {
        if i % (n / 10).max(1) == 0 {
            settled(&server, u64::from(i)).await;
            samples.push((i, rss_kib()));
            fds.push((i, fd_count()));
        }
        if i == n {
            break;
        }
        let out = run_simple(&server, "echo hi").await;
        assert_eq!(out.exit_status, Some(0), "connection {i}");
        assert_eq!(out.stdout_len, 3);
    }
    let st = &server.stats;
    let base = samples[1].1; // after warm-up (the first tenth)
    let last = samples.last().unwrap().1;
    report(
        "7-1000-connections",
        start,
        &format!(
            "rss_kib={samples:?} fds={fds:?} growth_after_100_kib={} conns_ended={} live_inputs={} channel_close_cb={} conn_errors={}",
            last as i64 - base as i64,
            get(&st.conns_ended),
            st.live_inputs.load(Relaxed),
            get(&st.channel_close_callbacks),
            get(&st.conn_errors),
        ),
    );
    assert_eq!(get(&st.conns_ended), u64::from(n));
    assert_eq!(st.live_inputs.load(Relaxed), 0);
    assert!(last < base + 8 * 1024, "RSS grew by more than 8 MiB");
    assert_eq!(fds.first().unwrap().1, fds.last().unwrap().1);
}

/// Plan detail: a second session channel on one connection is refused, and
/// the first keeps working.
#[cfg_attr(
    debug_assertions,
    ignore = "spike: run with cargo test --release in spike/russh-flow"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extra_second_channel_rejected() {
    let server = Server::new(Options::default());
    let (h, _served) = conn(&server, sc::config()).await;
    let ch = h.channel_open_session().await.unwrap();
    assert!(h.channel_open_session().await.is_err());
    ch.exec(true, "echo one").await.unwrap();
    let (read, write) = ch.split();
    write.eof().await.unwrap();
    let out = timeout(Duration::from_secs(5), collect(read))
        .await
        .unwrap();
    assert_eq!(out.exit_status, Some(0));
    assert_eq!(get(&server.stats.channels_rejected), 1);
}
