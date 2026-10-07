//! Frames on a `/minipaw/pipe/1` stream: `[kind u8][len u32 BE][payload]`.
//!
//! The dialer opens every stream with `Hello`; the server answers `Welcome`.
//! Both then exchange `Data`, `Ack`, `Fin`, and `Ping`. Offsets count
//! payload bytes of the whole session, not of one stream, so a session
//! survives its stream being replaced. `VERSION` in `Hello` is bumped
//! whenever a peer would misread the other's frames.

pub const PROTOCOL: &str = "/minipaw/pipe/1";
pub const VERSION: u8 = 2;

/// Largest `Data` payload we send.
pub const MAX_DATA: usize = 32 * 1024;
/// Largest payload we accept for any frame.
const MAX_PAYLOAD: usize = 64 * 1024;
const HEADER_LEN: usize = 5;

const HELLO: u8 = 1;
const WELCOME: u8 = 2;
const DATA: u8 = 3;
const ACK: u8 = 4;
const FIN: u8 = 5;
const ERROR: u8 = 6;
const PING: u8 = 7;

pub type Token = [u8; 16];
pub type SessionId = [u8; 16];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// Dialer → server: authenticate, and name the session and how many
    /// bytes of it the dialer has received so far.
    Hello {
        token: Token,
        session: SessionId,
        recv: u64,
    },
    /// Server → dialer: how many session bytes the server has received.
    Welcome {
        recv: u64,
    },
    Data(Vec<u8>),
    /// Cumulative count of bytes the receiver has consumed; `fin` once it
    /// has consumed everything up to the sender's `Fin`.
    Ack {
        offset: u64,
        fin: bool,
    },
    /// The sender will write nothing past `offset`.
    Fin {
        offset: u64,
    },
    /// Fatal refusal; the stream is closed after it.
    Error(String),
    /// Keeps a quiet stream alive: each side sends one when it has sent
    /// nothing else for a while, and any frame shows the sender is there.
    Ping,
}

impl Frame {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Frame::Hello {
                token,
                session,
                recv,
            } => {
                let mut payload = Vec::with_capacity(1 + 16 + 16 + 8);
                payload.push(VERSION);
                payload.extend_from_slice(token);
                payload.extend_from_slice(session);
                payload.extend_from_slice(&recv.to_be_bytes());
                frame(HELLO, &payload)
            }
            Frame::Welcome { recv } => frame(WELCOME, &recv.to_be_bytes()),
            Frame::Data(data) => frame(DATA, data),
            Frame::Ack { offset, fin } => {
                let mut payload = offset.to_be_bytes().to_vec();
                payload.push(u8::from(*fin));
                frame(ACK, &payload)
            }
            Frame::Fin { offset } => frame(FIN, &offset.to_be_bytes()),
            Frame::Error(message) => frame(ERROR, message.as_bytes()),
            Frame::Ping => frame(PING, &[]),
        }
    }

    fn decode(kind: u8, payload: &[u8]) -> Result<Frame, String> {
        let mut r = Reader(payload);
        let frame = match kind {
            HELLO => {
                let version = r.u8()?;
                if version != VERSION {
                    return Err(format!("unsupported protocol version {version}"));
                }
                Frame::Hello {
                    token: r.array()?,
                    session: r.array()?,
                    recv: r.u64()?,
                }
            }
            WELCOME => Frame::Welcome { recv: r.u64()? },
            DATA => return Ok(Frame::Data(payload.to_vec())),
            ACK => Frame::Ack {
                offset: r.u64()?,
                fin: r.u8()? != 0,
            },
            FIN => Frame::Fin { offset: r.u64()? },
            ERROR => return Ok(Frame::Error(String::from_utf8_lossy(payload).into_owned())),
            PING => Frame::Ping,
            other => return Err(format!("unknown frame kind {other}")),
        };
        if !r.0.is_empty() {
            return Err(format!("{} trailing bytes in frame kind {kind}", r.0.len()));
        }
        Ok(frame)
    }
}

fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(kind);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or("truncated frame payload")?;
        self.0 = rest;
        Ok(*head)
    }

    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.array::<1>()?[0])
    }

    fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_be_bytes(self.array()?))
    }
}

/// Reassembles frames from stream chunks, which may split or coalesce them.
#[derive(Default)]
pub struct FrameReader {
    buf: Vec<u8>,
    head: usize,
}

impl FrameReader {
    pub fn push(&mut self, data: &[u8]) {
        if self.head != 0 {
            self.buf.drain(..self.head);
            self.head = 0;
        }
        self.buf.extend_from_slice(data);
    }

    #[allow(clippy::should_implement_trait)] // pub only until the session moves in
    pub fn next(&mut self) -> Result<Option<Frame>, String> {
        let rest = self.buf.get(self.head..).unwrap_or_default();
        let Some((header, body)) = rest.split_first_chunk::<HEADER_LEN>() else {
            return Ok(None);
        };
        let [kind, len @ ..] = *header;
        let len = u32::from_be_bytes(len) as usize;
        if len > MAX_PAYLOAD {
            return Err(format!(
                "frame of {len} bytes exceeds the {MAX_PAYLOAD} byte limit"
            ));
        }
        let Some(payload) = body.get(..len) else {
            return Ok(None);
        };
        let frame = Frame::decode(kind, payload)?;
        self.head += HEADER_LEN + len;
        Ok(Some(frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_through_split_and_coalesced_chunks() {
        let frames = vec![
            Frame::Hello {
                token: [7; 16],
                session: [9; 16],
                recv: 42,
            },
            Frame::Welcome { recv: 1 << 40 },
            Frame::Data(b"hello".to_vec()),
            Frame::Data(vec![0xab; MAX_DATA]),
            Frame::Ack {
                offset: 5,
                fin: true,
            },
            Frame::Fin { offset: 5 },
            Frame::Error("busy".into()),
            Frame::Ping,
        ];
        let bytes: Vec<u8> = frames.iter().flat_map(Frame::encode).collect();

        for chunk_len in [1, 3, 7, 4096, bytes.len()] {
            let mut reader = FrameReader::default();
            let mut decoded = Vec::new();
            for chunk in bytes.chunks(chunk_len) {
                reader.push(chunk);
                while let Some(frame) = reader.next().unwrap() {
                    decoded.push(frame);
                }
            }
            assert_eq!(decoded, frames, "chunk_len={chunk_len}");
        }
    }

    #[test]
    fn oversized_and_malformed_frames_are_rejected() {
        let mut reader = FrameReader::default();
        reader.push(&[DATA, 0xff, 0xff, 0xff, 0xff]);
        assert!(reader.next().is_err());

        let mut reader = FrameReader::default();
        reader.push(&frame(ACK, &[0; 3]));
        assert!(reader.next().is_err());

        let mut reader = FrameReader::default();
        reader.push(&frame(PING, &[0]));
        assert!(reader.next().is_err());

        let mut reader = FrameReader::default();
        reader.push(&frame(99, &[]));
        assert!(reader.next().is_err());
    }
}
