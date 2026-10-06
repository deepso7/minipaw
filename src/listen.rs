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

use crate::net::{self, ABORT_GRACE, Exit, LINGER, RESUME_TIMEOUT};
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
    /// The user has been told the client is on a direct path.
    told_direct: bool,
    /// Set by Ctrl-C while the client is between streams: give up waiting
    /// for it to resume by then.
    abort_by: Option<Instant>,
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
    net::handle_interrupt(endpoint.wait_handle())?;
    let pipe = Pipe::new(&endpoint.wait_handle());
    let mut server = Server {
        endpoint,
        pipe,
        token,
        client: None,
        link: None,
        pending: HashMap::new(),
        lost_since: None,
        told_direct: false,
        abort_by: None,
    };

    eprintln!("# reserving a slot on relay {}…", relay.peer_id());
    let exit = server.drive(&ticket);
    // Whatever arrived reaches stdout, however the session ended.
    server.pipe.finish();
    let link = server.link.take();
    match exit? {
        Exit::Done => net::linger_and_close(server.endpoint, link),
        Exit::PeerGone => {}
        Exit::Interrupted => {
            net::abort(server.endpoint, link, "interrupted");
            return Err(net::Interrupted.into());
        }
    }
    Ok(())
}

impl Server {
    fn drive(&mut self, ticket: &Ticket) -> Result<Exit, Box<dyn Error>> {
        let started = Instant::now();
        let mut announced = false;
        let mut warned = false;
        loop {
            let deadline = [
                self.pipe.deadline(),
                (!announced && !warned).then(|| started + RESERVE_WARNING),
                self.lost_since.map(|t| t + LINGER),
                self.abort_by,
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(1))
            // A past deadline makes `wait` return without polling anything.
            .max(Instant::now() + Duration::from_millis(1));

            if let EndpointWaitOutcome::Event(event) = self.endpoint.wait(deadline)? {
                if let EndpointEvent::Nat(NatEvent::RelayReserved { .. }) = &event
                    && !announced
                {
                    announced = true;
                    eprintln!("# 🐾 listening; connect with:\nminipaw {ticket}");
                }
                if let EndpointEvent::Nat(NatEvent::RelayReservationLost { .. }) = &event {
                    eprintln!("# lost the relay reservation; reacquiring");
                }
                self.on_event(event)?;
            }
            if net::interrupted() && self.ready_to_abort() {
                return Ok(Exit::Interrupted);
            }
            if !announced && !warned && started.elapsed() >= RESERVE_WARNING {
                warned = true;
                eprintln!("# still no relay reservation; is the relay reachable over UDP?");
            }

            if let Err(e) = self.pipe.pump(&mut self.endpoint, self.link.as_ref()) {
                self.lose(&format!("send failed: {e}"));
            }
            if self.pipe.done() {
                return Ok(Exit::Done);
            }
            if let Some(since) = self.lost_since {
                if self.pipe.nearly_done() && since.elapsed() >= LINGER {
                    return Ok(Exit::PeerGone);
                }
                if since.elapsed() >= RESUME_TIMEOUT {
                    return Err("client disconnected and did not come back".into());
                }
            }
        }
    }

    fn on_event(&mut self, event: EndpointEvent) -> Result<(), Box<dyn Error>> {
        net::log_event(&event);
        match event {
            EndpointEvent::Nat(NatEvent::InboundDirectUpgrade { peer })
                if self.client.as_ref().is_some_and(|(client, _)| *client == peer)
                    && !self.told_direct =>
            {
                self.told_direct = true;
                eprintln!("# upgraded to a direct connection");
            }
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
                let direct = net::is_direct(&self.endpoint, peer);
                self.told_direct = direct;
                eprintln!("# connection from {peer} ({})", net::path_label(direct));
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

    /// Whether Ctrl-C can end the session now: the client's stream is up to
    /// carry the news, or there is no client, or the grace ran out.
    fn ready_to_abort(&mut self) -> bool {
        if self.link.is_some() || self.client.is_none() {
            return true;
        }
        let by = *self.abort_by.get_or_insert_with(|| {
            crate::debug!("interrupted between streams; waiting briefly to tell the client");
            Instant::now() + ABORT_GRACE
        });
        Instant::now() >= by
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
