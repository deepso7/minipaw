//! Test helper for `scripts/check.sh`: opens pipe streams to a minipaw server
//! and never sends `Hello`, to check a squatter cannot lock out real clients.
//!
//!   cargo run --release -p minipaw --example squat -- <server-quic-peer-addr> <streams> <seconds>

use std::time::{Duration, Instant};

use minip2p::{Endpoint, EndpointEvent, EndpointWaitOutcome, PeerAddr};

use minipaw::PROTOCOL;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: squat <server-peer-addr> <streams> <seconds>";
    let target: PeerAddr = args.next().ok_or(usage)?.parse()?;
    let streams: usize = args.next().ok_or(usage)?.parse()?;
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
    while Instant::now() < end {
        if opened < streams && endpoint.connection_id(&peer).is_some() {
            endpoint.open_stream(&peer, PROTOCOL)?;
            opened += 1;
        }
        if let EndpointWaitOutcome::Event(EndpointEvent::StreamReady { .. }) =
            endpoint.wait(Duration::from_millis(50))?
        {
            ready += 1;
            if ready == streams {
                eprintln!("squat: {ready} streams held");
            }
        }
    }
    Ok(())
}
