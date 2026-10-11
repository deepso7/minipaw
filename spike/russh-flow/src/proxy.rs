//! A man-in-the-middle for connections that negotiated cipher and MAC
//! `none`, used to make a stock russh client misbehave on the wire.
//!
//! With no cipher and no MAC, every binary packet after the identification
//! line is `u32 length | u8 padding | payload | padding`, so the proxy can
//! read, count, rewrite in place, or hold back individual messages. russh
//! does not check sequence numbers without a MAC, so holding a message back
//! and sending it later is accepted.
//!
//! Like [`spawn_relay`], each direction reads into an unbounded queue and a
//! separate task writes it out, so the proxy always keeps reading. A proxy
//! that forwarded with a blocking `write_all` would pass back-pressure
//! straight through and deadlock two russh endpoints, as a direct socket
//! pair does (see `builtin-ssh-spike.md`).

use std::sync::atomic::{
    AtomicBool, AtomicI64, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed,
};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use crate::{max_i64, max_usize};

const CHANNEL_OPEN: u8 = 90;
const CHANNEL_OPEN_CONFIRMATION: u8 = 91;
const CHANNEL_WINDOW_ADJUST: u8 = 93;
const CHANNEL_DATA: u8 = 94;

#[derive(Default)]
pub struct ProxyState {
    /// Rewrite the server's CHANNEL_OPEN_CONFIRMATION to advertise this
    /// (window, maximum packet size) to the client.
    pub fake_open_confirmation: Option<(u32, u32)>,
    /// Hold client WINDOW_ADJUST messages instead of forwarding them.
    pub hold_adjusts: AtomicBool,
    /// Forward each client WINDOW_ADJUST as this many back-to-back adjusts
    /// with the same total (0 or 1: unchanged). Pieces may be 0 bytes.
    pub split_adjusts: AtomicU32,
    held: Mutex<Vec<Vec<u8>>>,
    to_server: Mutex<Option<Queue>>,
    /// Credit the client granted the server and that reached the server
    /// (initial window + forwarded adjusts).
    pub client_credit: Arc<AtomicU64>,
    /// Credit the server really granted the client.
    pub server_credit: AtomicU64,
    /// Client CHANNEL_DATA payload bytes sent to the server.
    pub client_data: AtomicU64,
    /// Largest `client_data - server_credit`: bytes over the real window.
    pub max_over_window: AtomicI64,
    /// Largest CHANNEL_DATA payload from the client.
    pub max_client_packet: AtomicUsize,
    pub adjusts_held: AtomicU64,
    pub adjusts_forwarded: AtomicU64,
}

impl ProxyState {
    pub fn new(fake_open_confirmation: Option<(u32, u32)>) -> Arc<Self> {
        Arc::new(Self {
            fake_open_confirmation,
            ..Default::default()
        })
    }

    /// Stop holding and send every held WINDOW_ADJUST back to back.
    pub async fn release(&self) {
        // Under the `held` lock, so the reader cannot hold one more adjust
        // after this drains the list.
        let mut held = self.held.lock().unwrap_or_else(|e| e.into_inner());
        self.hold_adjusts.store(false, Relaxed);
        for pkt in std::mem::take(&mut *held) {
            self.send_to_server(self.forward_adjust(&pkt));
        }
    }

    fn send_to_server(&self, bytes: Vec<u8>) {
        if let Some(q) = self
            .to_server
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            let _ = q.send(bytes);
        }
    }
}

impl ProxyState {
    /// Count and maybe split one client WINDOW_ADJUST packet.
    fn forward_adjust(&self, pkt: &[u8]) -> Vec<u8> {
        let body = &pkt[5..];
        let (Some(recipient), Some(amount)) = (be32(body, 1), be32(body, 5)) else {
            return pkt.to_vec();
        };
        self.client_credit.fetch_add(u64::from(amount), Relaxed);
        let n = self.split_adjusts.load(Relaxed).max(1);
        if n == 1 {
            self.adjusts_forwarded.fetch_add(1, Relaxed);
            return pkt.to_vec();
        }
        let mut out = Vec::new();
        for i in 0..n {
            // The remainder goes first, so a small grant is one real adjust
            // followed by zero-byte ones.
            let piece = amount / n + if i == 0 { amount % n } else { 0 };
            out.extend_from_slice(&adjust_packet(recipient, piece));
        }
        self.adjusts_forwarded.fetch_add(u64::from(n), Relaxed);
        out
    }
}

/// A cleartext CHANNEL_WINDOW_ADJUST packet.
fn adjust_packet(recipient: u32, amount: u32) -> Vec<u8> {
    // 4 length + 1 padding length + 9 payload + 10 padding = 24 (8-aligned).
    let mut p = Vec::with_capacity(24);
    p.extend_from_slice(&20u32.to_be_bytes());
    p.push(10);
    p.push(CHANNEL_WINDOW_ADJUST);
    p.extend_from_slice(&recipient.to_be_bytes());
    p.extend_from_slice(&amount.to_be_bytes());
    p.extend_from_slice(&[0; 10]);
    p
}

fn be32(b: &[u8], at: usize) -> Option<u32> {
    b.get(at..at + 4)
        .map(|s| u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

type Queue = tokio::sync::mpsc::UnboundedSender<Vec<u8>>;

/// A task writing everything queued to `w`, then shutting it down.
fn writer(mut w: OwnedWriteHalf) -> Queue {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    tokio::spawn(async move {
        while let Some(chunk) = rx.recv().await {
            if w.write_all(&chunk).await.is_err() {
                return;
            }
        }
        let _ = w.shutdown().await;
    });
    tx
}

/// Splice `client` and `server` through the proxy.
pub fn spawn(client: UnixStream, server: UnixStream, st: Arc<ProxyState>) {
    let (cr, cw) = client.into_split();
    let (sr, sw) = server.into_split();
    *st.to_server.lock().unwrap_or_else(|e| e.into_inner()) = Some(writer(sw));
    let to_client = writer(cw);
    {
        let st = st.clone();
        tokio::spawn(async move {
            let _ = client_to_server(cr, &st).await;
            // Dropping the queue ends the writer, which shuts the socket.
            st.to_server
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
        });
    }
    tokio::spawn(async move {
        let _ = server_to_client(sr, &to_client, &st).await;
    });
}

async fn copy_id_line(r: &mut OwnedReadHalf) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let mut line = Vec::new();
        loop {
            let b = r.read_u8().await?;
            line.push(b);
            if b == b'\n' {
                break;
            }
        }
        let is_id = line.starts_with(b"SSH-");
        out.extend_from_slice(&line);
        if is_id {
            return Ok(out);
        }
    }
}

/// One whole packet: the 4 length bytes plus the rest.
async fn read_packet(r: &mut OwnedReadHalf) -> std::io::Result<Vec<u8>> {
    let len = r.read_u32().await? as usize;
    let mut pkt = vec![0u8; 4 + len];
    pkt[..4].copy_from_slice(&(len as u32).to_be_bytes());
    r.read_exact(&mut pkt[4..]).await?;
    Ok(pkt)
}

async fn client_to_server(mut r: OwnedReadHalf, st: &ProxyState) -> std::io::Result<()> {
    let id = copy_id_line(&mut r).await?;
    st.send_to_server(id);
    loop {
        let pkt = read_packet(&mut r).await?;
        // pkt[4] is the padding length, pkt[5..] the payload and padding.
        let body = &pkt[5..];
        match body.first().copied() {
            Some(CHANNEL_OPEN) => {
                // type, string kind, sender, window, max packet
                if let Some(kind_len) = be32(body, 1) {
                    let at = 1 + 4 + kind_len as usize + 4;
                    if let Some(window) = be32(body, at) {
                        st.client_credit.fetch_add(u64::from(window), Relaxed);
                    }
                }
            }
            Some(CHANNEL_WINDOW_ADJUST) => {
                {
                    let mut held = st.held.lock().unwrap_or_else(|e| e.into_inner());
                    if st.hold_adjusts.load(Relaxed) {
                        st.adjusts_held.fetch_add(1, Relaxed);
                        held.push(pkt);
                        continue;
                    }
                }
                st.send_to_server(st.forward_adjust(&pkt));
                continue;
            }
            Some(CHANNEL_DATA) => {
                if let Some(n) = be32(body, 5) {
                    max_usize(&st.max_client_packet, n as usize);
                    let sent = st.client_data.fetch_add(u64::from(n), Relaxed) + u64::from(n);
                    let granted = st.server_credit.load(Relaxed);
                    max_i64(&st.max_over_window, sent as i64 - granted as i64);
                }
            }
            _ => {}
        }
        st.send_to_server(pkt);
    }
}

async fn server_to_client(mut r: OwnedReadHalf, w: &Queue, st: &ProxyState) -> std::io::Result<()> {
    let id = copy_id_line(&mut r).await?;
    let _ = w.send(id);
    loop {
        let mut pkt = read_packet(&mut r).await?;
        let body = &mut pkt[5..];
        match body.first().copied() {
            Some(CHANNEL_OPEN_CONFIRMATION) => {
                // type, recipient, sender, window, max packet
                if let Some(window) = be32(body, 9) {
                    st.server_credit.fetch_add(u64::from(window), Relaxed);
                }
                if let Some((window, max_packet)) = st.fake_open_confirmation {
                    body[9..13].copy_from_slice(&window.to_be_bytes());
                    body[13..17].copy_from_slice(&max_packet.to_be_bytes());
                }
            }
            Some(CHANNEL_WINDOW_ADJUST) => {
                if let Some(amount) = be32(body, 5) {
                    st.server_credit.fetch_add(u64::from(amount), Relaxed);
                }
            }
            _ => {}
        }
        if w.send(pkt).is_err() {
            return Ok(());
        }
    }
}

/// A full-duplex relay with unbounded buffers in each direction: it always
/// reads both sides, whatever the state of its writes. It stands in for a
/// peer like OpenSSH (non-blocking I/O, keeps reading while its writes are
/// pending) or for minipaw's tunnel in front of such a peer. In-flight data
/// is still bounded by the SSH windows.
pub fn spawn_relay(client: UnixStream, server: UnixStream) {
    spawn_relay_limited(client, server, None);
}

/// [`spawn_relay`], with the server-to-client direction limited to
/// `s2c_bytes_per_sec` (an asymmetric path: the server uploads slower than
/// the client).
pub fn spawn_relay_limited(client: UnixStream, server: UnixStream, s2c_bytes_per_sec: Option<u64>) {
    let (cr, cw) = client.into_split();
    let (sr, sw) = server.into_split();
    relay_one_way(cr, sw, None);
    relay_one_way(sr, cw, s2c_bytes_per_sec);
}

fn relay_one_way(mut r: OwnedReadHalf, mut w: OwnedWriteHalf, rate: Option<u64>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match r.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if tx.send(buf[..n].to_vec()).is_err() {
                        return;
                    }
                }
            }
        }
    });
    tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        let mut written = 0u64;
        while let Some(chunk) = rx.recv().await {
            if w.write_all(&chunk).await.is_err() {
                return;
            }
            written += chunk.len() as u64;
            if let Some(rate) = rate {
                let due = start + std::time::Duration::from_secs_f64(written as f64 / rate as f64);
                tokio::time::sleep_until(due).await;
            }
        }
        let _ = w.shutdown().await;
    });
}
