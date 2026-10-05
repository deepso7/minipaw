//! Resumable byte-stream bookkeeping, independent of any I/O.
//!
//! A session outlives the libp2p stream carrying it: a relayed path
//! upgrading to a direct one, a connection replacement, or a relay cutting
//! a circuit all kill the stream. Each side keeps every byte it sent until
//! the peer acknowledges it, so a fresh stream can pick up exactly where the
//! peer's receive offset says the old one stopped.

use std::collections::VecDeque;

use crate::wire::MAX_DATA;

/// Unacknowledged bytes we buffer before we stop reading local input.
pub const WINDOW: usize = 4 * 1024 * 1024;

/// Our half of the session: bytes on their way to the peer.
#[derive(Default)]
pub struct Outbound {
    /// Unacknowledged bytes, starting at session offset `acked`.
    buf: VecDeque<u8>,
    acked: u64,
    /// Next offset to put on the current stream.
    sent: u64,
    /// Local input ended; no bytes are added past `end()`.
    eof: bool,
    /// `Fin` went out on the current stream.
    fin_sent: bool,
    /// `Fin` went out on some stream.
    pub fin_sent_ever: bool,
    pub fin_acked: bool,
}

impl Outbound {
    fn end(&self) -> u64 {
        self.acked + self.buf.len() as u64
    }

    pub fn has_room(&self) -> bool {
        !self.eof && self.buf.len() < WINDOW
    }

    pub fn push(&mut self, data: &[u8]) {
        if !self.eof {
            self.buf.extend(data);
        }
    }

    pub fn close(&mut self) {
        self.eof = true;
    }

    /// The peer consumed everything before `offset` (and our `Fin`, when
    /// `fin`). Acks arriving out of order across streams may be stale.
    pub fn ack(&mut self, offset: u64, fin: bool) -> Result<(), String> {
        if offset > self.end() {
            return Err(format!(
                "peer acked offset {offset} past our end {}",
                self.end()
            ));
        }
        if offset > self.acked {
            let n = (offset - self.acked) as usize;
            self.buf.drain(..n);
            self.acked = offset;
            self.sent = self.sent.max(offset);
        }
        if fin {
            if !self.eof || offset != self.end() {
                return Err("peer acked a Fin we never sent".into());
            }
            self.fin_acked = true;
        }
        Ok(())
    }

    /// A new stream is attached and the peer has received everything before
    /// `peer_recv`: resend from there.
    pub fn rewind(&mut self, peer_recv: u64) -> Result<(), String> {
        if peer_recv < self.acked {
            return Err(format!(
                "peer resumed at {peer_recv}, before already-acked offset {}",
                self.acked
            ));
        }
        self.ack(peer_recv, false)?;
        self.sent = peer_recv;
        self.fin_sent = false;
        Ok(())
    }

    /// The next `Data` payload for the current stream.
    pub fn next_chunk(&mut self) -> Option<Vec<u8>> {
        let start = (self.sent - self.acked) as usize;
        let len = self.buf.len().checked_sub(start)?.min(MAX_DATA);
        if len == 0 {
            return None;
        }
        let chunk: Vec<u8> = self.buf.range(start..start + len).copied().collect();
        self.sent += len as u64;
        Some(chunk)
    }

    /// Takes back the last `len` bytes from `next_chunk`, which the stream
    /// refused.
    pub fn unsend(&mut self, len: usize) {
        self.sent -= len as u64;
    }

    /// The offset to announce in a `Fin`, once everything before it is on
    /// the current stream and the `Fin` itself is not.
    pub fn fin_due(&self) -> Option<u64> {
        (self.eof && !self.fin_sent && !self.fin_acked && self.sent == self.end())
            .then(|| self.end())
    }

    pub fn mark_fin_sent(&mut self) {
        self.fin_sent = true;
        self.fin_sent_ever = true;
    }
}

/// The peer's half: bytes arriving from it.
#[derive(Default)]
pub struct Inbound {
    /// Session bytes received so far (handed to the output, maybe not yet
    /// written).
    pub recv: u64,
    /// Where the peer's `Fin` put the end of its data.
    pub fin: Option<u64>,
}

impl Inbound {
    pub fn on_data(&mut self, len: usize) -> Result<(), String> {
        if self.fin.is_some() {
            return Err("peer sent data after its Fin".into());
        }
        self.recv += len as u64;
        Ok(())
    }

    pub fn on_fin(&mut self, offset: u64) -> Result<(), String> {
        match self.fin {
            // A resend after a stream switch.
            Some(fin) if fin == offset => Ok(()),
            Some(fin) => Err(format!("peer moved its Fin from {fin} to {offset}")),
            None if offset != self.recv => Err(format!(
                "peer's Fin at {offset} does not match the {} bytes received",
                self.recv
            )),
            None => {
                self.fin = Some(offset);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(out: &mut Outbound) -> Vec<u8> {
        let mut all = Vec::new();
        while let Some(chunk) = out.next_chunk() {
            all.extend(chunk);
        }
        all
    }

    #[test]
    fn rewind_resends_exactly_what_the_peer_missed() {
        let mut out = Outbound::default();
        out.push(b"hello world");
        assert_eq!(drain(&mut out), b"hello world");

        out.ack(3, false).unwrap();
        // The stream died; the peer had received 6 bytes.
        out.rewind(6).unwrap();
        assert_eq!(drain(&mut out), b"world");
        assert!(out.next_chunk().is_none());
    }

    #[test]
    fn fin_is_resent_after_rewind_until_acked() {
        let mut out = Outbound::default();
        out.push(b"abc");
        out.close();
        assert_eq!(out.fin_due(), None, "data must go first");
        drain(&mut out);
        assert_eq!(out.fin_due(), Some(3));
        out.mark_fin_sent();
        assert_eq!(out.fin_due(), None);

        out.rewind(3).unwrap();
        assert_eq!(out.fin_due(), Some(3));
        out.mark_fin_sent();
        out.ack(3, true).unwrap();
        assert!(out.fin_acked);
        out.rewind(3).unwrap();
        assert_eq!(out.fin_due(), None);
    }

    #[test]
    fn invalid_acks_and_rewinds_are_rejected() {
        let mut out = Outbound::default();
        out.push(b"abc");
        drain(&mut out);
        assert!(out.ack(4, false).is_err());
        assert!(out.ack(3, true).is_err(), "no Fin was sent");
        out.ack(2, false).unwrap();
        assert!(out.rewind(1).is_err());
        out.ack(1, false).unwrap(); // stale, ignored
    }

    #[test]
    fn inbound_fin_must_match_received_bytes() {
        let mut inb = Inbound::default();
        inb.on_data(4).unwrap();
        assert!(inb.on_fin(3).is_err());
        inb.on_fin(4).unwrap();
        inb.on_fin(4).unwrap();
        assert!(inb.on_data(1).is_err());
    }
}
