//! Throwaway spike for rollout step 1 of `docs/plans/builtin-ssh.md`.
//!
//! An in-process russh 0.64.1 server built the way the plan says
//! (one session channel per connection, the `ChannelReadHalf` dropped after
//! `split()`, input only through handler callbacks into an 8 MiB capped
//! buffer, output through `make_writer` / `make_writer_ext(Some(1))` in
//! chunks of at most 32 KiB), plus a russh client and a cleartext
//! man-in-the-middle used to abuse flow control. Findings are in
//! `docs/plans/builtin-ssh-spike.md`.

pub mod client;
pub mod proxy;
pub mod server;

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};

/// Per-channel input cap from the plan.
pub const INPUT_CAP: usize = 8 * 1024 * 1024;
/// Output chunk size from the plan.
pub const CHUNK: usize = 32 * 1024;
/// The stderr line a channel gets when its input passes the cap.
pub const CAP_MESSAGE: &str = "minipaw: input exceeded 8 MiB buffer, closing\n";

/// Raise `cell` to at least `v`.
pub fn max_usize(cell: &AtomicUsize, v: usize) {
    cell.fetch_max(v, Ordering::Relaxed);
}

/// Raise `cell` to at least `v`.
pub fn max_i64(cell: &AtomicI64, v: i64) {
    cell.fetch_max(v, Ordering::Relaxed);
}

/// Read a counter.
pub fn get(cell: &AtomicU64) -> u64 {
    cell.load(Ordering::Relaxed)
}

/// A `kB` field of `/proc/self/status`, such as `VmRSS:` or `VmHWM:`.
pub fn proc_status_kib(key: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    status
        .lines()
        .find_map(|l| l.strip_prefix(key))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// Current resident set size in KiB.
pub fn rss_kib() -> u64 {
    proc_status_kib("VmRSS:")
}

/// Peak resident set size of this process in KiB.
pub fn hwm_kib() -> u64 {
    proc_status_kib("VmHWM:")
}

/// Open descriptors of this process.
pub fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").map_or(0, Iterator::count)
}

/// A deterministic byte stream (xorshift64*), so both ends can hash it.
pub struct Stream(u64);

impl Stream {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            let v = self.0.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}
