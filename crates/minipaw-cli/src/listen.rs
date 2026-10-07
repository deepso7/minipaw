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

use crate::net::{self, DELIVERED_GRACE, Exit, RESUME_TIMEOUT, Stop};
use crate::pipe::{Link, Pipe};
use minipaw::ticket::Ticket;
use minipaw::wire::{Frame, FrameReader, PROTOCOL, SessionId, Token};

/// Inbound streams still waiting for their `Hello`.
const MAX_PENDING: usize = 16;
/// How long a new stream has to present its `Hello`.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
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
    /// Not-yet-admitted streams and when they must have said `Hello`.
    pending: HashMap<StreamKey, (FrameReader, Instant)>,
    /// Since when an admitted client has had no stream.
    lost_since: Option<Instant>,
    /// The user has been told the client is on a direct path.
    told_direct: bool,
    /// Ctrl-C or a local failure, once seen.
    stop: Option<Stop>,
    /// Test hook (`MINIPAW_TEST_DROP_LINK_AFTER=<bytes>`): once this many
    /// session bytes have arrived, forget the link without closing it, as
    /// a relay that drops a circuit and tells only us would. The client
    /// sees its stream go silent.
    drop_link_after: Option<u64>,
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
    let drop_link_after = match std::env::var("MINIPAW_TEST_DROP_LINK_AFTER") {
        Ok(raw) => Some(
            raw.parse()
                .map_err(|e| format!("invalid MINIPAW_TEST_DROP_LINK_AFTER '{raw}': {e}"))?,
        ),
        Err(_) => None,
    };
    let mut server = Server {
        endpoint,
        pipe,
        token,
        client: None,
        link: None,
        pending: HashMap::new(),
        lost_since: None,
        told_direct: false,
        stop: None,
        drop_link_after,
    };

    eprintln!("# reserving a slot on relay {}…", relay.peer_id());
    let exit = server.drive(&ticket);
    let link = server.link.take();
    let client = server.client.take().map(|(peer, _)| peer);
    net::finish(server.endpoint, &mut server.pipe, link, client, exit)
}

impl Server {
    /// Runs the session. Once stopping, the stop is the outcome whatever
    /// else goes wrong: the client ending too, or a timeout.
    fn drive(&mut self, ticket: &Ticket) -> Result<Exit, Box<dyn Error>> {
        match self.drive_until_exit(ticket) {
            Err(e) if self.stop.is_some() => {
                crate::debug!("while stopping: {e}");
                Ok(Exit::Stopped(self.stop.take().ok_or("no stop")?))
            }
            result => result,
        }
    }

    fn drive_until_exit(&mut self, ticket: &Ticket) -> Result<Exit, Box<dyn Error>> {
        let started = Instant::now();
        let mut announced = false;
        let mut warned = false;
        loop {
            let deadline = [
                // Pipe timers are for sending, which stops when stopping.
                self.stop
                    .is_none()
                    .then(|| self.pipe.deadline(self.link.as_ref()))
                    .flatten(),
                self.link.as_ref().map(Link::dead_at),
                (!announced && !warned).then(|| started + RESERVE_WARNING),
                self.lost_since
                    .filter(|_| self.pipe.delivered())
                    .map(|t| t + DELIVERED_GRACE),
                self.stop.as_ref().map(Stop::wake_at),
                self.pending.values().map(|(_, deadline)| *deadline).min(),
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
                if let Some(stop) = &mut self.stop {
                    stop.observe(&event);
                }
                self.on_event(event)?;
            }
            self.expire_pending();
            if self
                .link
                .as_ref()
                .is_some_and(|l| Instant::now() >= l.dead_at())
            {
                self.lose("no word from the client");
            }
            if !announced && !warned && started.elapsed() >= RESERVE_WARNING {
                warned = true;
                eprintln!("# still no relay reservation; is the relay reachable over UDP?");
            }

            if self.stop.is_none() {
                if let Err(e) = self.pipe.pump(&mut self.endpoint, self.link.as_ref()) {
                    self.lose(&format!("send failed: {e}"));
                }
                self.stop = Stop::check(&self.pipe);
            }
            if let Some(stop) = &mut self.stop {
                // A server with no client has nobody to tell.
                match stop.step(
                    &mut self.endpoint,
                    self.link.as_ref(),
                    self.client.is_some(),
                ) {
                    Ok(true) => return Ok(Exit::Stopped(self.stop.take().ok_or("no stop")?)),
                    Ok(false) => {}
                    Err(e) => self.lose(&format!("stop not sent: {e}")),
                }
                continue;
            }
            if self.pipe.done() {
                return Ok(Exit::Done);
            }
            if let Some(since) = self.lost_since {
                if self.pipe.delivered() && since.elapsed() >= DELIVERED_GRACE {
                    return Ok(Exit::Delivered);
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
                if self
                    .client
                    .as_ref()
                    .is_some_and(|(client, _)| *client == peer)
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
                // The pool is small and expiring. When full, the oldest
                // stream makes way: an honest dialer says Hello within a
                // round trip, so squatters cannot keep it out.
                if self.pending.len() >= MAX_PENDING {
                    self.evict_oldest_pending();
                }
                let deadline = Instant::now() + HELLO_TIMEOUT;
                self.pending.insert(
                    (peer_id, conn_id, stream_id),
                    (FrameReader::default(), deadline),
                );
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
                    link.push(&data);
                    while let Some(frame) = link.reader.next()? {
                        self.pipe.on_frame(frame)?;
                    }
                    if self
                        .drop_link_after
                        .is_some_and(|after| self.pipe.recv_offset() >= after)
                    {
                        self.drop_link_after = None;
                        crate::debug!("test hook: dropping the link without closing it");
                        self.link = None;
                        self.lost_since = Some(Instant::now());
                    }
                } else {
                    self.on_pending_data((peer_id, conn_id, stream_id), &data)?;
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
    /// complete, then admits or refuses it. Nothing about the session
    /// changes until the stream is fully set up, so a refused or failed
    /// admission leaves the current client and link as they were.
    fn on_pending_data(&mut self, key: StreamKey, data: &[u8]) -> Result<(), Box<dyn Error>> {
        let Some((reader, _)) = self.pending.get_mut(&key) else {
            return Ok(());
        };
        reader.push(data);
        let (token, session, recv) = match reader.next() {
            Ok(None) => return Ok(()),
            Ok(Some(Frame::Hello {
                token,
                session,
                recv,
            })) => (token, session, recv),
            Ok(Some(_)) => return self.refused(key, "expected Hello"),
            Err(e) => return self.refused(key, &e),
        };
        if !constant_time_eq(&token, &self.token) {
            return self.refused(key, "wrong token");
        }
        let resuming = match &self.client {
            Some((client, admitted)) if *client != key.0 || *admitted != session => {
                return self.refused(key, "busy with another client");
            }
            Some(_) => true,
            None => false,
        };
        if let Err(e) = self.pipe.check_attach(recv) {
            return self.refused(key, &e);
        }

        let reader = self
            .pending
            .remove(&key)
            .map(|(reader, _)| reader)
            .unwrap_or_default();
        let (peer, conn, stream) = key;
        let mut link = Link::new(peer, conn, stream);
        link.reader = reader;
        let welcome = Frame::Welcome {
            recv: self.pipe.recv_offset(),
        };
        if let Err(e) = link.send(&mut self.endpoint, &welcome) {
            crate::debug!("welcome failed: {e}");
            link.abandon(&mut self.endpoint);
            return Ok(());
        }

        if resuming {
            crate::debug!("client resumed at offset {recv}");
        } else {
            let direct = net::is_direct(&self.endpoint, &link.peer);
            self.told_direct = direct;
            eprintln!(
                "# connection from {} ({})",
                link.peer,
                net::path_label(direct)
            );
            self.client = Some((link.peer.clone(), session));
        }
        if let Some(old) = self.link.take() {
            old.abandon(&mut self.endpoint);
        }
        self.pipe.attach(recv)?;
        self.lost_since = None;
        // Frames that arrived together with the Hello.
        let link = self.link.insert(link);
        while let Some(frame) = link.reader.next()? {
            self.pipe.on_frame(frame)?;
        }
        Ok(())
    }

    fn evict_oldest_pending(&mut self) {
        let oldest = self
            .pending
            .iter()
            .min_by_key(|(_, (_, deadline))| *deadline)
            .map(|(key, _)| key.clone());
        if let Some((peer, conn, stream)) = oldest {
            crate::debug!("dropping a stream from {peer}: too many pending");
            self.pending.remove(&(peer.clone(), conn, stream));
            Link::new(peer, conn, stream).abandon(&mut self.endpoint);
        }
    }

    /// Drops pending streams that never presented a `Hello`.
    fn expire_pending(&mut self) {
        let now = Instant::now();
        let expired: Vec<StreamKey> = self
            .pending
            .iter()
            .filter(|(_, (_, deadline))| now >= *deadline)
            .map(|(key, _)| key.clone())
            .collect();
        for (peer, conn, stream) in expired {
            crate::debug!("dropping a stream from {peer}: no Hello in time");
            self.pending.remove(&(peer.clone(), conn, stream));
            Link::new(peer, conn, stream).abandon(&mut self.endpoint);
        }
    }

    /// Turns a pending stream away with an `Error`; an admission outcome.
    fn refused(&mut self, key: StreamKey, reason: &str) -> Result<(), Box<dyn Error>> {
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
        Ok(())
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
