//! Test helper for `scripts/check.sh`: misbehaving minipaw clients, to check
//! a server copes with them while serving real clients.
//!
//!   squat <server-quic-peer-addr> <streams> <seconds>
//!   squat --churn <server-quic-peer-addr> <seconds>
//!   squat --flood <server-quic-peer-addr> <ticket> <seconds>
//!   squat --leave <server-quic-peer-addr> <ticket> <hold> <seconds>
//!
//! (Run as `cargo run --release -p minipaw --example squat -- …`.)
//!
//! The first form opens pipe streams and never sends `Hello`, holding
//! `<streams>` of them for `<seconds>`. With `--churn` it keeps opening a
//! fresh stream on its one connection, four a second, so it never runs out
//! of pending ones, until the server disconnects it: then it prints
//! `squat: disconnected after <secs>s, <n> streams opened` and exits 0.
//! Still connected after `<seconds>`, it exits 1.
//!
//! The ticket's forms authenticate. `--flood` sends a `Hello` and then
//! `Data` as fast as the stream takes it, without waiting for a `Welcome`.
//! Refused before one, it prints `squat: refused after <n> bytes: <reason>`
//! and exits 0; welcomed, or neither within `<seconds>`, it exits 1.
//! `--leave` gets a session and keeps it for `<hold>` seconds, then opens a
//! second stream that never says `Hello` and ends the session, leaving its
//! connection idle. Disconnected by the server within `<seconds>`, it
//! prints `squat: disconnected <secs>s after its session` and exits 0.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use minip2p::{ConnectionId, Endpoint, EndpointEvent, EndpointWaitOutcome, PeerAddr, StreamId};

use minipaw::PROTOCOL;

#[allow(dead_code)]
#[path = "../src/wire.rs"]
mod wire;

use wire::{Frame, FrameReader};

const CHURN_EVERY: Duration = Duration::from_millis(250);

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> Result<ExitCode> {
    let mut args = std::env::args().skip(1).peekable();
    let usage = "usage: squat <server-peer-addr> <streams> <seconds>\n       squat --churn <server-peer-addr> <seconds>\n       squat --flood <server-peer-addr> <ticket> <seconds>\n       squat --leave <server-peer-addr> <ticket> <hold> <seconds>";
    if let Some(mode) = args.next_if(|a| a == "--flood" || a == "--leave") {
        let target: PeerAddr = args.next().ok_or(usage)?.parse()?;
        let token = token(&args.next().ok_or(usage)?)?;
        let hold = if mode == "--leave" {
            Some(Duration::from_secs(args.next().ok_or(usage)?.parse()?))
        } else {
            None
        };
        let seconds: u64 = args.next().ok_or(usage)?.parse()?;
        return talk(&target, token, hold, Duration::from_secs(seconds));
    }
    let churn = args.next_if(|a| a == "--churn").is_some();
    let target: PeerAddr = args.next().ok_or(usage)?.parse()?;
    let streams: usize = if churn {
        usize::MAX
    } else {
        args.next().ok_or(usage)?.parse()?
    };
    let seconds: u64 = args.next().ok_or(usage)?.parse()?;

    let mut endpoint = Endpoint::builder()
        .protocol(PROTOCOL)
        .listen_on("/ip4/127.0.0.1/udp/0/quic-v1")?
        .bind()?;
    endpoint.listen_all()?;
    let peer = target.peer_id().clone();
    endpoint.connect(target)?;

    let end = Instant::now() + Duration::from_secs(seconds);
    let (mut opened, mut ready) = (0, 0);
    let mut connected: Option<Instant> = None;
    let mut next_open = Instant::now();
    while Instant::now() < end {
        let up = endpoint.connection_id(&peer).is_some();
        if up && opened < streams && Instant::now() >= next_open {
            endpoint.open_stream(&peer, PROTOCOL)?;
            opened += 1;
            if churn {
                next_open = Instant::now() + CHURN_EVERY;
            }
        }
        match endpoint.wait(Duration::from_millis(50))? {
            EndpointWaitOutcome::Event(EndpointEvent::StreamReady { .. }) => {
                ready += 1;
                if ready == streams {
                    eprintln!("squat: {ready} streams held");
                }
            }
            EndpointWaitOutcome::Event(EndpointEvent::ConnectionEstablished {
                peer_id, ..
            }) if peer_id == peer && connected.is_none() => {
                connected = Some(Instant::now());
                if churn {
                    eprintln!("squat: connected; churning streams");
                }
            }
            EndpointWaitOutcome::Event(EndpointEvent::ConnectionClosed { peer_id, .. })
                if churn && peer_id == peer =>
            {
                let after = connected.map_or(0.0, |t| t.elapsed().as_secs_f64());
                eprintln!("squat: disconnected after {after:.1}s, {opened} streams opened");
                return Ok(ExitCode::SUCCESS);
            }
            _ => {}
        }
    }
    if churn {
        eprintln!("squat: still connected after {seconds}s, {opened} streams opened");
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

/// The secret token in a ticket: `mp` + base64url of
/// `[version][flags][token; 16]…`.
fn token(ticket: &str) -> Result<[u8; 16]> {
    let raw = B64.decode(ticket.trim().strip_prefix("mp").ok_or("not a ticket")?)?;
    Ok(*raw
        .get(2..)
        .and_then(|r| r.first_chunk::<16>())
        .ok_or("ticket is truncated")?)
}

fn bind() -> Result<Endpoint> {
    let mut endpoint = Endpoint::builder()
        .protocol(PROTOCOL)
        .listen_on("/ip4/127.0.0.1/udp/0/quic-v1")?
        .bind()?;
    endpoint.listen_all()?;
    Ok(endpoint)
}

/// `--flood`, or with `hold`, `--leave`; see the module docs.
fn talk(
    target: &PeerAddr,
    token: [u8; 16],
    hold: Option<Duration>,
    limit: Duration,
) -> Result<ExitCode> {
    let mut endpoint = bind()?;
    let peer = target.peer_id().clone();
    endpoint.connect(target.clone())?;
    let end = Instant::now() + limit;
    let mut session: Option<(ConnectionId, StreamId)> = None;
    let mut reader = FrameReader::default();
    let mut welcomed: Option<Instant> = None;
    let mut leaving = false;
    let mut ended: Option<Instant> = None;
    let mut sent = 0;
    let mut ping_at = Instant::now();
    let chunk = Frame::Data(vec![0; wire::MAX_DATA]).encode();
    while Instant::now() < end {
        let wait = if hold.is_none() && session.is_some() {
            // Flooding: back for more as soon as the stream takes it.
            Duration::from_millis(1)
        } else {
            Duration::from_millis(50)
        };
        let event = match endpoint.wait(wait)? {
            EndpointWaitOutcome::Event(event) => Some(event),
            _ => None,
        };
        match event {
            Some(EndpointEvent::ConnectionEstablished { peer_id, .. })
                if peer_id == peer && session.is_none() =>
            {
                endpoint.open_stream(&peer, PROTOCOL)?;
            }
            Some(EndpointEvent::StreamReady {
                peer_id,
                conn_id,
                stream_id,
                initiated_locally: true,
                ..
            }) if peer_id == peer => {
                if session.is_none() {
                    let hello = Frame::Hello {
                        token,
                        session: session_id(),
                        recv: 0,
                        resume: false,
                    };
                    endpoint.send_stream(&peer, conn_id, stream_id, hello.encode())?;
                    session = Some((conn_id, stream_id));
                } else if let Some((conn, stream)) = session
                    && leaving
                    && ended.is_none()
                {
                    // The second stream is up and says nothing; now end the
                    // session, as a client does on a stop.
                    let bye = Frame::Error("leaving".into()).encode();
                    endpoint.send_stream(&peer, conn, stream, bye)?;
                    endpoint.close_stream_write(&peer, conn, stream)?;
                    ended = Some(Instant::now());
                }
            }
            Some(EndpointEvent::StreamData {
                peer_id,
                conn_id,
                stream_id,
                data,
            }) if peer_id == peer && session == Some((conn_id, stream_id)) => {
                reader.push(&data);
                while let Some(frame) = reader.next()? {
                    match frame {
                        Frame::Welcome { .. } if hold.is_none() => {
                            eprintln!("squat: welcomed after {sent} bytes");
                            return Ok(ExitCode::FAILURE);
                        }
                        Frame::Welcome { .. } => {
                            eprintln!("squat: in a session");
                            welcomed = Some(Instant::now());
                        }
                        Frame::Error(reason) if welcomed.is_none() => {
                            eprintln!("squat: refused after {sent} bytes: {reason}");
                            return Ok(if hold.is_none() {
                                ExitCode::SUCCESS
                            } else {
                                ExitCode::FAILURE
                            });
                        }
                        _ => {}
                    }
                }
            }
            Some(EndpointEvent::ConnectionClosed { peer_id, .. }) if peer_id == peer => {
                return Ok(match ended {
                    Some(at) => {
                        let after = at.elapsed().as_secs_f64();
                        eprintln!("squat: disconnected {after:.1}s after its session");
                        ExitCode::SUCCESS
                    }
                    None => {
                        eprintln!("squat: disconnected");
                        ExitCode::FAILURE
                    }
                });
            }
            _ => {}
        }
        let Some((conn, stream)) = session else {
            continue;
        };
        match (hold, welcomed) {
            (None, _) => {
                // A full send buffer is fine: more goes out next time.
                if endpoint
                    .send_stream(&peer, conn, stream, chunk.clone())
                    .is_ok()
                {
                    sent += wire::MAX_DATA;
                }
            }
            (Some(hold), Some(at)) if !leaving && at.elapsed() >= hold => {
                // The session ends once this stream is ready.
                endpoint.open_stream(&peer, PROTOCOL)?;
                leaving = true;
            }
            (Some(_), Some(_)) if !leaving && Instant::now() >= ping_at => {
                // Keeps the session's stream alive meanwhile.
                let _ = endpoint.send_stream(&peer, conn, stream, Frame::Ping.encode());
                ping_at = Instant::now() + Duration::from_secs(2);
            }
            _ => {}
        }
    }
    eprintln!("squat: nothing decisive within {}s", limit.as_secs());
    Ok(ExitCode::FAILURE)
}

fn session_id() -> [u8; 16] {
    let mut id = [0; 16];
    getrandom::fill(&mut id).expect("system randomness");
    id
}
