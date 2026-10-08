//! Test helper for `scripts/check.sh`: opens pipe streams to a minipaw server
//! and never sends `Hello`, to check a squatter cannot lock out real clients.
//!
//!   cargo run --release -p minipaw --example squat -- <server-quic-peer-addr> <streams> <seconds>
//!   cargo run --release -p minipaw --example squat -- --churn <server-quic-peer-addr> <seconds>
//!
//! The first form holds `<streams>` streams for `<seconds>`. With `--churn`
//! it keeps opening a fresh stream on its one connection, four a second, so
//! it never runs out of pending ones, until the server disconnects it: then
//! it prints `squat: disconnected after <secs>s, <n> streams opened` and
//! exits 0. Still connected after `<seconds>`, it exits 1.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use minip2p::{Endpoint, EndpointEvent, EndpointWaitOutcome, PeerAddr};

use minipaw::PROTOCOL;

const CHURN_EVERY: Duration = Duration::from_millis(250);

fn main() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1).peekable();
    let usage = "usage: squat <server-peer-addr> <streams> <seconds>\n       squat --churn <server-peer-addr> <seconds>";
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
