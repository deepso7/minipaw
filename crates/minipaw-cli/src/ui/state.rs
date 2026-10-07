//! What the terminal UIs show, kept up to date from a session's events and
//! progress snapshots. Pure: no terminal, no clock of its own.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use minipaw::{Error, Event, Outcome, PathKind, PeerId, Progress, Role, Ticket};

use super::Launch;

/// How many warnings [`State`] keeps; older ones are dropped.
pub const MAX_WARNINGS: usize = 8;

/// Where a session is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Nothing reported yet.
    Starting,
    /// A listener asking the relay for a slot (again, after losing it).
    Reserving,
    /// A listener holding a slot, its ticket known, waiting for a dialer.
    Waiting,
    /// A dialer connecting to the listener.
    Connecting,
    /// Piping data with the peer.
    Connected,
    /// The link died; the session is resuming on a fresh stream.
    Resuming,
    /// Stopping on our side and telling the peer.
    Stopping,
    /// Ended successfully.
    Done,
    /// Ended with an error, including being stopped.
    Failed,
}

impl Phase {
    /// A short lowercase label, such as `waiting for a peer`.
    pub fn label(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Reserving => "reserving a relay slot",
            Phase::Waiting => "waiting for a peer",
            Phase::Connecting => "connecting",
            Phase::Connected => "connected",
            Phase::Resuming => "resuming",
            Phase::Stopping => "stopping",
            Phase::Done => "done",
            Phase::Failed => "failed",
        }
    }

    /// Whether the session is over: [`Done`](Phase::Done) or
    /// [`Failed`](Phase::Failed).
    pub fn is_ended(self) -> bool {
        matches!(self, Phase::Done | Phase::Failed)
    }
}

/// A throughput estimate over a sliding window of `(time, total bytes)`
/// samples.
#[derive(Clone, Debug)]
pub struct RateMeter {
    window: Duration,
    samples: VecDeque<(Instant, u64)>,
}

impl Default for RateMeter {
    fn default() -> Self {
        RateMeter::new(RateMeter::WINDOW)
    }
}

impl RateMeter {
    /// The default window.
    pub const WINDOW: Duration = Duration::from_secs(2);

    /// Spans shorter than this give no rate yet: too noisy.
    const MIN_SPAN: Duration = Duration::from_millis(200);

    /// A meter averaging over `window`.
    pub fn new(window: Duration) -> Self {
        RateMeter {
            window,
            samples: VecDeque::new(),
        }
    }

    /// Records that `total` bytes had moved by `now`. Samples must come in
    /// time order; a total lower than the last one restarts the meter.
    pub fn record(&mut self, now: Instant, total: u64) {
        if self.samples.back().is_some_and(|&(_, last)| total < last) {
            self.samples.clear();
        }
        self.samples.push_back((now, total));
        // Keep one sample at or before the window's start, so the span
        // covers the whole window once there is that much history.
        let start = now.checked_sub(self.window);
        while self.samples.len() > 2 && start.is_some_and(|start| self.samples[1].0 <= start) {
            self.samples.pop_front();
        }
    }

    /// Bytes per second over the window, or `None` until the samples span
    /// at least 200 ms.
    pub fn rate(&self) -> Option<f64> {
        let (&(t0, b0), &(t1, b1)) = (self.samples.front()?, self.samples.back()?);
        let span = t1.duration_since(t0);
        if span < Self::MIN_SPAN {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        Some((b1 - b0) as f64 / span.as_secs_f64())
    }
}

/// A session as the UIs see it. Feed it every [`Event`] with
/// [`apply`](Self::apply), a [`Progress`] snapshot about ten times a second
/// with [`tick`](Self::tick), and the result with [`finish`](Self::finish).
#[derive(Clone, Debug)]
pub struct State {
    /// Which end we are.
    pub role: Role,
    /// Where the session is.
    pub phase: Phase,
    /// The peer: the listener from the ticket for a dialer, the dialer once
    /// accepted for a listener.
    pub peer: Option<PeerId>,
    /// The relay a listener reserves on, once reported.
    pub relay: Option<PeerId>,
    /// How the connection is routed, once connected.
    pub path: Option<PathKind>,
    /// Whether the connection moved from the relay to a direct path.
    pub upgraded: bool,
    /// The listener's ticket: ours once listening, or the one we dial.
    pub ticket: Option<Ticket>,
    /// Recent trouble worth showing (slow reservation, lost links), oldest
    /// first, at most [`MAX_WARNINGS`].
    pub warnings: Vec<String>,
    /// When the session started.
    pub started: Instant,
    /// When we first connected to the peer.
    pub connected_at: Option<Instant>,
    /// When the session ended, from [`finish`](Self::finish).
    pub ended_at: Option<Instant>,
    /// Why the session failed, from [`finish`](Self::finish).
    pub error: Option<String>,
    /// How the session succeeded, from [`finish`](Self::finish).
    pub outcome: Option<Outcome>,
    /// The size of our input, when it is a regular file.
    pub input_len: Option<u64>,
    /// The latest progress snapshot.
    pub progress: Progress,
    /// Outgoing throughput: bytes the peer confirmed (`acked`).
    pub up: RateMeter,
    /// Incoming throughput: bytes written to our output (`written`).
    pub down: RateMeter,
}

impl State {
    /// A session about to start at `now`.
    pub fn new(launch: &Launch, now: Instant) -> Self {
        State {
            role: launch.role,
            phase: Phase::Starting,
            peer: launch.ticket.as_ref().map(|t| t.peer().clone()),
            relay: None,
            path: None,
            upgraded: false,
            ticket: launch.ticket.clone(),
            warnings: Vec::new(),
            started: now,
            connected_at: None,
            ended_at: None,
            error: None,
            outcome: None,
            input_len: launch.input_len,
            progress: Progress::default(),
            up: RateMeter::default(),
            down: RateMeter::default(),
        }
    }

    /// Updates the state for `event`, as of now.
    pub fn apply(&mut self, event: &Event) {
        self.apply_at(event, Instant::now());
    }

    /// Updates the state for `event`, which happened at `now`.
    pub fn apply_at(&mut self, event: &Event, now: Instant) {
        let before_peer = matches!(
            self.phase,
            Phase::Starting | Phase::Reserving | Phase::Waiting
        );
        match event {
            Event::Reserving { relay } => {
                self.relay = Some(relay.clone());
                if before_peer {
                    self.phase = Phase::Reserving;
                }
            }
            Event::Listening { ticket } => {
                self.ticket = Some(ticket.clone());
                if before_peer {
                    self.phase = Phase::Waiting;
                }
            }
            Event::ReservationSlow => {
                self.warn("still no relay reservation; is the relay reachable over UDP?");
            }
            Event::ReservationLost => {
                self.warn("lost the relay reservation; reacquiring");
                if before_peer {
                    self.phase = Phase::Reserving;
                }
            }
            Event::Connecting { peer } => {
                self.peer = Some(peer.clone());
                self.phase = Phase::Connecting;
            }
            Event::Accepted { peer, path } | Event::Connected { peer, path } => {
                self.peer = Some(peer.clone());
                self.path = Some(*path);
                self.phase = Phase::Connected;
                self.connected_at.get_or_insert(now);
            }
            Event::Upgraded => {
                self.path = Some(PathKind::Direct);
                self.upgraded = true;
            }
            Event::LinkLost { reason } => {
                self.warn(&format!("link lost: {reason}"));
                if self.phase != Phase::Stopping {
                    self.phase = Phase::Resuming;
                }
            }
            Event::Resumed => {
                if self.phase != Phase::Stopping {
                    self.phase = Phase::Connected;
                }
                self.connected_at.get_or_insert(now);
            }
            Event::Stopping => self.phase = Phase::Stopping,
            // Events added to the SDK later change nothing here.
            _ => {}
        }
    }

    /// Records a progress snapshot taken at `now`.
    pub fn tick(&mut self, now: Instant, progress: Progress) {
        self.progress = progress;
        self.up.record(now, progress.acked);
        self.down.record(now, progress.written);
    }

    /// Records how the session ended, at `now`.
    pub fn finish(&mut self, result: &Result<Outcome, Error>, now: Instant) {
        self.ended_at = Some(now);
        match result {
            Ok(outcome) => {
                self.phase = Phase::Done;
                self.outcome = Some(*outcome);
            }
            Err(e) => {
                self.phase = Phase::Failed;
                self.error = Some(e.to_string());
            }
        }
    }

    /// Outgoing bytes per second the peer is confirming.
    pub fn up_rate(&self) -> Option<f64> {
        self.up.rate()
    }

    /// Incoming bytes per second we are writing out.
    pub fn down_rate(&self) -> Option<f64> {
        self.down.rate()
    }

    /// Of our input, the fraction the peer has confirmed (0 to 1), when its
    /// size is known.
    pub fn up_fraction(&self) -> Option<f64> {
        let len = self.input_len?;
        if len == 0 {
            return Some(1.0);
        }
        #[allow(clippy::cast_precision_loss)]
        Some((self.progress.acked.min(len) as f64 / len as f64).clamp(0.0, 1.0))
    }

    /// Time until the peer confirms all our input at the current rate,
    /// when the input's size is known and data is moving.
    pub fn eta(&self) -> Option<Duration> {
        let remaining = self.input_len?.saturating_sub(self.progress.acked);
        if remaining == 0 {
            return Some(Duration::ZERO);
        }
        let rate = self.up_rate().filter(|&r| r > 0.0)?;
        #[allow(clippy::cast_precision_loss)]
        Duration::try_from_secs_f64(remaining as f64 / rate).ok()
    }

    /// Time since the session started, frozen once it ended.
    pub fn elapsed(&self, now: Instant) -> Duration {
        self.ended_at
            .unwrap_or(now)
            .saturating_duration_since(self.started)
    }

    /// Time since we first connected, frozen once the session ended.
    pub fn connected_for(&self, now: Instant) -> Option<Duration> {
        let at = self.connected_at?;
        Some(self.ended_at.unwrap_or(now).saturating_duration_since(at))
    }

    fn warn(&mut self, warning: &str) {
        if self.warnings.len() == MAX_WARNINGS {
            self.warnings.remove(0);
        }
        self.warnings.push(warning.to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minipaw::Config;

    const PEER: &str = "12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf";

    fn peer() -> PeerId {
        PEER.parse().expect("peer id")
    }

    fn launch(input_len: Option<u64>) -> Launch {
        Launch {
            role: Role::Listener,
            ticket: None,
            config: Config::default(),
            verbose: false,
            input_len,
        }
    }

    fn secs(t0: Instant, s: f64) -> Instant {
        t0 + Duration::from_secs_f64(s)
    }

    #[test]
    fn a_listener_reserves_waits_connects_and_upgrades() {
        let t0 = Instant::now();
        let mut state = State::new(&launch(None), t0);
        assert_eq!(state.phase, Phase::Starting);
        state.apply_at(&Event::Reserving { relay: peer() }, t0);
        assert_eq!(state.phase, Phase::Reserving);
        assert_eq!(state.relay, Some(peer()));
        state.apply_at(&Event::ReservationSlow, t0);
        assert_eq!(state.phase, Phase::Reserving);
        assert_eq!(state.warnings.len(), 1);
        let path = PathKind::Relayed;
        state.apply_at(&Event::Accepted { peer: peer(), path }, secs(t0, 3.0));
        assert_eq!(state.phase, Phase::Connected);
        assert_eq!(state.path, Some(PathKind::Relayed));
        assert_eq!(
            state.connected_for(secs(t0, 5.0)),
            Some(Duration::from_secs(2))
        );
        state.apply_at(&Event::Upgraded, t0);
        assert!(state.upgraded);
        assert_eq!(state.path, Some(PathKind::Direct));
        // Losing the reservation while connected leaves the phase alone.
        state.apply_at(&Event::ReservationLost, t0);
        assert_eq!(state.phase, Phase::Connected);
        state.apply_at(
            &Event::LinkLost {
                reason: "gone".into(),
            },
            t0,
        );
        assert_eq!(state.phase, Phase::Resuming);
        assert_eq!(
            state.warnings.last().map(String::as_str),
            Some("link lost: gone")
        );
        state.apply_at(&Event::Resumed, secs(t0, 9.0));
        assert_eq!(state.phase, Phase::Connected);
        assert_eq!(state.connected_at, Some(secs(t0, 3.0)));
        state.apply_at(&Event::Stopping, t0);
        assert_eq!(state.phase, Phase::Stopping);
        state.apply_at(&Event::Resumed, t0);
        assert_eq!(state.phase, Phase::Stopping);
        state.finish(&Err(Error::Stopped), secs(t0, 10.0));
        assert_eq!(state.phase, Phase::Failed);
        assert_eq!(state.error.as_deref(), Some("stopped"));
        assert_eq!(state.elapsed(secs(t0, 60.0)), Duration::from_secs(10));
    }

    #[test]
    fn a_dialer_connects() {
        let t0 = Instant::now();
        let mut l = launch(None);
        l.role = Role::Dialer;
        let mut state = State::new(&l, t0);
        assert_eq!(state.peer, None);
        state.apply_at(&Event::Connecting { peer: peer() }, t0);
        assert_eq!(state.phase, Phase::Connecting);
        assert_eq!(state.peer, Some(peer()));
        let path = PathKind::Direct;
        state.apply_at(&Event::Connected { peer: peer(), path }, t0);
        assert_eq!(state.phase, Phase::Connected);
        assert!(!state.upgraded);
        state.finish(&Ok(Outcome::Done), t0);
        assert_eq!(state.phase, Phase::Done);
        assert_eq!(state.outcome, Some(Outcome::Done));
        assert!(state.phase.is_ended());
    }

    #[test]
    fn warnings_are_capped() {
        let mut state = State::new(&launch(None), Instant::now());
        for i in 0..20 {
            state.apply(&Event::LinkLost {
                reason: i.to_string(),
            });
        }
        assert_eq!(state.warnings.len(), MAX_WARNINGS);
        assert_eq!(state.warnings[MAX_WARNINGS - 1], "link lost: 19");
    }

    #[test]
    fn rate_meter_averages_over_its_window() {
        let t0 = Instant::now();
        let mut meter = RateMeter::default();
        assert_eq!(meter.rate(), None);
        meter.record(t0, 0);
        meter.record(secs(t0, 0.1), 100);
        assert_eq!(meter.rate(), None, "too short a span");
        // 1000 B/s for 5 s, sampled at 10 Hz.
        for i in 2u32..=50 {
            meter.record(secs(t0, f64::from(i) / 10.0), u64::from(i) * 100);
        }
        let rate = meter.rate().expect("rate");
        assert!((rate - 1000.0).abs() < 1.0, "{rate}");
        // Then it stalls: the rate decays to zero within the window.
        for i in 51u32..=80 {
            meter.record(secs(t0, f64::from(i) / 10.0), 5000);
        }
        assert_eq!(meter.rate(), Some(0.0));
        // Samples older than the window are dropped.
        assert!(meter.samples.len() <= 22, "{}", meter.samples.len());
        // A counter that went backwards restarts the meter.
        meter.record(secs(t0, 8.1), 10);
        assert_eq!(meter.rate(), None);
    }

    #[test]
    fn eta_from_input_length_and_rate() {
        let t0 = Instant::now();
        let mut state = State::new(&launch(Some(10_000)), t0);
        assert_eq!(state.eta(), None);
        assert_eq!(state.up_fraction(), Some(0.0));
        let at = |acked| Progress {
            acked,
            ..Progress::default()
        };
        state.tick(t0, at(0));
        state.tick(secs(t0, 1.0), at(2000));
        // 8000 bytes left at 2000 B/s.
        let eta = state.eta().expect("eta");
        assert!((eta.as_secs_f64() - 4.0).abs() < 0.01, "{eta:?}");
        assert_eq!(state.up_fraction(), Some(0.2));
        state.tick(secs(t0, 2.0), at(10_000));
        assert_eq!(state.eta(), Some(Duration::ZERO));
        assert_eq!(state.up_fraction(), Some(1.0));

        let mut unknown = State::new(&launch(None), t0);
        unknown.tick(t0, at(0));
        unknown.tick(secs(t0, 1.0), at(2000));
        assert_eq!(unknown.eta(), None);
        assert_eq!(unknown.up_fraction(), None);
        assert!(unknown.up_rate().is_some());

        let mut stalled = State::new(&launch(Some(10)), t0);
        stalled.tick(t0, at(0));
        stalled.tick(secs(t0, 1.0), at(0));
        assert_eq!(stalled.eta(), None);
    }
}
