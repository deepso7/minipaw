//! Server mode: reserve a relay slot, print a ticket, and pipe stdio with
//! the first client that presents the ticket's token. That client may come
//! back on new streams (path upgrades, relay cutoffs) to resume its
//! session; anyone else is turned away.

use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};

use minip2p::{
    ConnectionId, Endpoint, EndpointEvent, EndpointWaitOutcome, NatEvent, PeerAddr, PeerId,
    StreamId,
};

use crate::net::{self, LINGER, RESUME_TIMEOUT};
use crate::pipe::{Link, Pipe};
use crate::ticket::Ticket;
use crate::wire::{Frame, FrameReader, PROTOCOL, SessionId, Token};

/// Inbound streams still waiting for their `Hello`.
const MAX_PENDING: usize = 16;
/// How long a reservation may take before we say so.
const RESERVE_WARNING: Duration = Duration::from_secs(15);

type StreamKey = (PeerId, ConnectionId, StreamId);

struct Server {
    endpoint: Endpoint,
    pipe: Pipe,
    token: Token,
    /// The admitted client and its session, fixed by the first valid Hello.
    client: Option<(PeerId, SessionId)>,
    link: Option<Link>,
    pending: HashMap<StreamKey, FrameReader>,
    /// Since when an admitted client has had no stream.
    lost_since: Option<Instant>,
}

pub fn run(relay: PeerAddr) -> Result<(), Box<dyn Error>> {
    let endpoint = net::bind(&relay, true)?;
    let token = net::random16()?;
    let embed = net::default_relay().is_none_or(|default| default != relay);
    let ticket = Ticket {
        peer: endpoint.peer_id().clone(),
        token,
        relay: embed.then(|| relay.clone()),
    };
    let pipe = Pipe::new(&endpoint.wait_handle());
    let mut server = Server {
        endpoint,
        pipe,
        token,
        client: None,
        link: None,
        pending: HashMap::new(),
        lost_since: None,
    };

    eprintln!("# reserving a slot on relay {}…", relay.peer_id());
    let started = Instant::now();
    let mut announced = false;
    let mut warned = false;
    loop {
        let deadline = [
            server.pipe.deadline(),
            (!announced && !warned).then(|| started + RESERVE_WARNING),
            server.lost_since.map(|t| t + LINGER),
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or_else(|| Instant::now() + Duration::from_secs(1));

        if let EndpointWaitOutcome::Event(event) = server.endpoint.wait(deadline)? {
            if let EndpointEvent::Nat(NatEvent::RelayReserved { .. }) = &event
                && !announced
            {
                announced = true;
                eprintln!("# 🐾 listening; connect with:\nminipaw {ticket}");
            }
            if let EndpointEvent::Nat(NatEvent::RelayReservationLost { .. }) = &event {
                eprintln!("# lost the relay reservation; reacquiring");
            }
            server.on_event(event)?;
        }
        if !announced && !warned && started.elapsed() >= RESERVE_WARNING {
            warned = true;
            eprintln!("# still no relay reservation; is the relay reachable over UDP?");
        }

        if let Err(e) = server.pipe.pump(&mut server.endpoint, server.link.as_ref()) {
            server.lose(&format!("send failed: {e}"));
        }
        if server.pipe.done() {
            let Server {
                endpoint,
                pipe,
                link,
                ..
            } = server;
            pipe.finish();
            net::linger_and_close(endpoint, link);
            return Ok(());
        }
        if let Some(since) = server.lost_since {
            if server.pipe.nearly_done() && since.elapsed() >= LINGER {
                server.pipe.finish();
                return Ok(());
            }
            if since.elapsed() >= RESUME_TIMEOUT {
                return Err("client disconnected and did not come back".into());
            }
        }
    }
}

impl Server {
    fn on_event(&mut self, event: EndpointEvent) -> Result<(), Box<dyn Error>> {
        net::log_event(&event);
        match event {
            EndpointEvent::StreamReady {
                peer_id,
                conn_id,
                stream_id,
                protocol_id,
                initiated_locally: false,
            } if protocol_id == PROTOCOL => {
                if self.pending.len() >= MAX_PENDING {
                    self.refuse((peer_id, conn_id, stream_id), "too many pending streams");
                } else {
                    self.pending
                        .insert((peer_id, conn_id, stream_id), FrameReader::default());
                }
            }
            EndpointEvent::StreamData {
                peer_id,
                conn_id,
                stream_id,
                data,
            } => {
                if let Some(link) = &mut self.link
                    && link.is(&peer_id, conn_id, stream_id)
                {
                    link.reader.push(&data);
                    while let Some(frame) = link.reader.next()? {
                        self.pipe.on_frame(frame)?;
                    }
                } else {
                    self.on_pending_data((peer_id, conn_id, stream_id), &data);
                }
            }
            EndpointEvent::StreamClosed {
                peer_id,
                conn_id,
                stream_id,
            }
            | EndpointEvent::StreamWriteStopped {
                peer_id,
                conn_id,
                stream_id,
                ..
            } => {
                self.pending.remove(&(peer_id.clone(), conn_id, stream_id));
                if self
                    .link
                    .as_ref()
                    .is_some_and(|l| l.is(&peer_id, conn_id, stream_id))
                {
                    self.lose("stream closed");
                }
            }
            EndpointEvent::ConnectionClosed { peer_id, conn_id }
            | EndpointEvent::ConnectionReplaced {
                peer_id,
                old: conn_id,
                ..
            } => {
                self.pending
                    .retain(|(peer, conn, _), _| *peer != peer_id || *conn != conn_id);
                if self
                    .link
                    .as_ref()
                    .is_some_and(|l| l.peer == peer_id && l.conn == conn_id)
                {
                    self.lose("connection closed");
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Buffers data on a not-yet-admitted stream until its `Hello` is
    /// complete, then admits or refuses it.
    fn on_pending_data(&mut self, key: StreamKey, data: &[u8]) {
        let Some(reader) = self.pending.get_mut(&key) else {
            return;
        };
        reader.push(data);
        let hello = match reader.next() {
            Ok(None) => return,
            Ok(Some(Frame::Hello {
                token,
                session,
                recv,
            })) => (token, session, recv),
            Ok(Some(_)) => return self.refuse(key, "expected Hello"),
            Err(e) => return self.refuse(key, &e),
        };
        let (token, session, recv) = hello;
        if !constant_time_eq(&token, &self.token) {
            return self.refuse(key, "wrong token");
        }
        let peer = &key.0;
        match &self.client {
            Some((client, admitted)) if client != peer || *admitted != session => {
                return self.refuse(key, "busy with another client");
            }
            Some(_) => crate::debug!("client resumed at offset {recv}"),
            None => {
                eprintln!("# connection from {peer}");
                self.client = Some((peer.clone(), session));
            }
        }

        let reader = self.pending.remove(&key).unwrap_or_default();
        let (peer, conn, stream) = key;
        let mut link = Link::new(peer, conn, stream);
        link.reader = reader;
        if let Some(old) = self.link.take() {
            old.abandon(&mut self.endpoint);
        }
        let welcome = Frame::Welcome {
            recv: self.pipe.recv_offset(),
        };
        if let Err(e) = self.pipe.attach(recv) {
            let key = (link.peer.clone(), link.conn, link.stream);
            return self.refuse(key, &e);
        }
        if let Err(e) = link.send(&mut self.endpoint, &welcome) {
            crate::debug!("welcome failed: {e}");
            return link.abandon(&mut self.endpoint);
        }
        self.link = Some(link);
        self.lost_since = None;
    }

    fn refuse(&mut self, key: StreamKey, reason: &str) {
        crate::debug!("refusing stream from {}: {reason}", key.0);
        self.pending.remove(&key);
        let (peer, conn, stream) = key;
        let link = Link::new(peer, conn, stream);
        if let Err(e) = link.send(&mut self.endpoint, &Frame::Error(reason.into())) {
            crate::debug!("refusal not sent: {e}");
        }
        if let Err(e) = self
            .endpoint
            .close_stream_write(&link.peer, link.conn, link.stream)
        {
            crate::debug!("close refused stream: {e}");
        }
    }

    fn lose(&mut self, reason: &str) {
        if let Some(link) = self.link.take() {
            crate::debug!("lost the client stream ({reason}); waiting for it to resume");
            link.abandon(&mut self.endpoint);
            self.lost_since = Some(Instant::now());
        }
    }
}

fn constant_time_eq(a: &Token, b: &Token) -> bool {
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
