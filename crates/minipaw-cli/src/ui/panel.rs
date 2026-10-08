//! Panel mode: an inline status panel on stderr while stdout carries data.
//!
//! The session runs on its own thread; its events and log records come
//! over one channel to a loop here that redraws the panel ten times a
//! second, polling the session's progress counters.
//!
//! Status lines (and `-v` lines) are printed above the panel as plain
//! text, worded exactly as in [`plain`] mode, so they stay in the
//! scrollback and a long ticket soft-wraps and copies cleanly. To print
//! them, the panel is cleared, the lines are written where it was, and a
//! fresh inline viewport is anchored below them.
//!
//! Everything goes to stderr: stdout is the session's data, and stdin may
//! be too, so the terminal is never put in raw mode, never read from and
//! never asked where its cursor is.

use std::io::{self, IsTerminal as _, Write as _};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use minipaw::{Error, Io, Outcome, Role};
use ratatui::backend::{Backend as _, ClearType};
use ratatui::layout::{Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::{Terminal, TerminalOptions, Viewport};

use super::state::{Phase, State};
use super::term::{self, StderrBackend, TerminalGuard};
use super::{Launch, UiMsg, fmt, log, plain};

/// The panel's height in rows.
const HEIGHT: u16 = 5;
/// How often the panel redraws.
const TICK: Duration = Duration::from_millis(100);
/// How often the panel is repainted from scratch when stdin is a terminal,
/// whose echo of typed input can scribble over it.
const REPAINT: Duration = Duration::from_secs(1);
/// The widest the progress bar gets.
const MAX_BAR: usize = 40;
/// Narrower than this, the progress bar is left out.
const MIN_BAR: usize = 6;

/// Runs the session with the status panel, falling back to plain mode when
/// the panel cannot be set up.
pub fn run(launch: Launch) -> Result<Outcome, Error> {
    let Ok(mut screen) = Screen::new() else {
        return plain::run(launch);
    };
    let paint = Paint::from_env();
    let mut state = State::new(&launch, Instant::now());
    let mut text = Vec::new();

    let (tx, rx) = mpsc::channel();
    let events = tx.clone();
    let session = launch.session(Io::stdio()).on_event(move |event| {
        // The UI only goes away once the session has ended.
        let _ = events.send(UiMsg::Event(event));
    });
    let handle = session.handle();
    term::install_interrupts(handle.clone()).map_err(Error::Other)?;
    let _guard = TerminalGuard::new();
    log::route_logs_to(tx);
    let session = match thread::Builder::new()
        .name("session".into())
        .spawn(move || session.run())
    {
        Ok(thread) => thread,
        Err(e) => {
            log::route_logs_to_stderr();
            let _ = screen.finish(None);
            return Err(Error::Other(format!("starting the session thread: {e}")));
        }
    };

    let mut next_tick = Instant::now();
    loop {
        // Take messages as they come until the next tick is due.
        while let Some(wait) = next_tick
            .checked_duration_since(Instant::now())
            .filter(|wait| !wait.is_zero())
        {
            match rx.recv_timeout(wait) {
                Ok(msg) => receive(&mut state, &mut text, msg, Instant::now()),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => thread::sleep(wait),
            }
        }
        let now = Instant::now();
        let ended = session.is_finished();
        state.tick(now, handle.progress());
        // A panel that fails to draw must not stop the session.
        let _ = screen.update(&mut text, &state, now, paint);
        if ended {
            break;
        }
        next_tick = (next_tick + TICK).max(now);
    }

    let result = match session.join() {
        Ok(result) => result,
        Err(panic) => {
            // The panic hook has already restored the terminal and printed
            // the message; leave the panel's last frame as it is.
            log::route_logs_to_stderr();
            term::restore();
            std::panic::resume_unwind(panic);
        }
    };
    log::route_logs_to_stderr();
    let now = Instant::now();
    while let Ok(msg) = rx.try_recv() {
        receive(&mut state, &mut text, msg, now);
    }
    state.tick(now, handle.progress());
    state.finish(&result, now);
    let _ = screen.print(&mut text);
    let summary = result.is_ok().then(|| summary(&state, now));
    let _ = screen.finish(summary.as_deref());
    result
}

/// Applies a message from the session's thread, queueing the text plain
/// mode would print for it.
fn receive(state: &mut State, text: &mut Vec<String>, msg: UiMsg, now: Instant) {
    match msg {
        UiMsg::Event(event) => {
            state.apply_at(&event, now);
            text.extend(plain::event_text(&event));
        }
        UiMsg::Log(level, message) => text.push(log::plain_line(level, &message)),
    }
}

/// The line printed in place of the panel when the session succeeds.
fn summary(state: &State, now: Instant) -> String {
    let took = state
        .connected_for(now)
        .unwrap_or_else(|| state.elapsed(now));
    let mut line = format!(
        "# done: {} sent, {} received in {}",
        fmt::bytes(state.progress.acked),
        fmt::bytes(state.progress.written),
        fmt::duration(took),
    );
    if let Some(path) = state.path {
        line.push_str(&format!(" ({path})"));
    }
    line
}

/// A ratatui terminal with an inline viewport at the bottom of the screen.
fn inline_terminal() -> io::Result<Terminal<StderrBackend>> {
    Terminal::with_options(
        StderrBackend::new(),
        TerminalOptions {
            viewport: Viewport::Inline(HEIGHT),
        },
    )
}

/// The panel on the terminal, and what it takes to keep it tidy.
struct Screen {
    terminal: Terminal<StderrBackend>,
    /// The terminal's size when the viewport was last anchored.
    size: Option<(u16, u16)>,
    /// Whether stdin is the terminal, which then echoes typed input.
    stdin_tty: bool,
    /// When the panel was last repainted from scratch.
    repainted: Instant,
}

impl Screen {
    fn new() -> io::Result<Self> {
        Ok(Screen {
            terminal: inline_terminal()?,
            size: term::size(),
            stdin_tty: io::stdin().is_terminal(),
            repainted: Instant::now(),
        })
    }

    /// Prints queued text above the panel, re-anchors after a resize,
    /// and draws the panel.
    fn update(
        &mut self,
        text: &mut Vec<String>,
        state: &State,
        now: Instant,
        paint: Paint,
    ) -> io::Result<()> {
        let size = term::size();
        if size != self.size {
            self.clear()?;
            self.terminal = inline_terminal()?;
            self.size = size;
        }
        self.print(text)?;
        if self.stdin_tty && now.saturating_duration_since(self.repainted) >= REPAINT {
            self.terminal.clear()?;
            self.repainted = now;
        }
        self.terminal.draw(|frame| {
            let area = frame.area();
            let lines = panel_lines(state, now, area.width, paint);
            frame.render_widget(Paragraph::new(lines), area);
        })?;
        // Park the (hidden) cursor on the panel's top row, so a terminal
        // echoing typed input never scrolls the screen from the last row.
        let top = self.area().as_position();
        self.terminal.set_cursor_position(top)
    }

    fn area(&mut self) -> Rect {
        self.terminal.get_frame().area()
    }

    /// Clears the panel and everything below it, leaving the cursor at the
    /// start of its top row.
    fn clear(&mut self) -> io::Result<()> {
        let area = self.area();
        // After a shrink, the panel's old top may be off the screen.
        let rows = term::size().map_or(area.bottom(), |(_, rows)| rows);
        let top = area.y.min(rows.saturating_sub(area.height.max(1)));
        let backend = self.terminal.backend_mut();
        backend.set_cursor_position(Position::new(0, top))?;
        backend.clear_region(ClearType::AfterCursor)
    }

    /// Writes `text` where the panel was and anchors a fresh panel below
    /// it. The lines are plain text, so the terminal soft-wraps long ones.
    fn print(&mut self, text: &mut Vec<String>) -> io::Result<()> {
        if text.is_empty() {
            return Ok(());
        }
        self.clear()?;
        let backend = self.terminal.backend_mut();
        for line in text.drain(..) {
            writeln!(backend, "{line}")?;
        }
        io::Write::flush(backend)?;
        // The new viewport takes the bottom rows, scrolling the text up
        // to sit right above it.
        self.terminal = inline_terminal()?;
        Ok(())
    }

    /// Clears the panel for good, printing `summary` in its place.
    fn finish(&mut self, summary: Option<&str>) -> io::Result<()> {
        self.clear()?;
        let backend = self.terminal.backend_mut();
        if let Some(summary) = summary {
            writeln!(backend, "{summary}")?;
        }
        io::Write::flush(backend)
    }
}

/// Styles, or none at all when `NO_COLOR` is set.
#[derive(Clone, Copy, Debug)]
struct Paint {
    color: bool,
}

impl Paint {
    fn from_env() -> Self {
        Paint {
            color: std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty()),
        }
    }

    fn span(self, text: impl Into<String>, style: Style) -> Span<'static> {
        let style = if self.color { style } else { Style::new() };
        Span::styled(text.into(), style)
    }

    fn dim(self, text: impl Into<String>) -> Span<'static> {
        self.span(text, Style::new().add_modifier(Modifier::DIM))
    }
}

fn plain_span(text: impl Into<String>) -> Span<'static> {
    Span::raw(text.into())
}

/// The colour of the dot by the state's label.
fn phase_color(phase: Phase) -> Color {
    match phase {
        Phase::Connected | Phase::Done => Color::Green,
        Phase::Failed => Color::Red,
        Phase::Starting
        | Phase::Reserving
        | Phase::Waiting
        | Phase::Connecting
        | Phase::Resuming
        | Phase::Stopping => Color::Yellow,
    }
}

/// The panel's lines, at most [`HEIGHT`], for a viewport `width` wide.
/// Lines too long for it are cut off at the edge.
fn panel_lines(state: &State, now: Instant, width: u16, paint: Paint) -> Vec<Line<'static>> {
    let mut lines = vec![header(state, now, paint)];
    let before_peer = state.connected_at.is_none() && !state.phase.is_ended();
    if before_peer {
        if state.role == Role::Listener && state.phase == Phase::Waiting {
            lines.push(Line::from(paint.dim(
                "  run the `minipaw …` line printed above on the other machine",
            )));
        }
    } else {
        lines.push(up_line(state, usize::from(width), paint));
        lines.push(down_line(state, paint));
    }
    if let Some(warning) = state.warnings.last() {
        let mut text = format!("⚠ {warning}");
        if state.warnings.len() > 1 {
            text.push_str(&format!(" (+{} earlier)", state.warnings.len() - 1));
        }
        lines.push(Line::from(paint.span(
            text,
            Style::new().fg(Color::Yellow).add_modifier(Modifier::DIM),
        )));
    }
    lines.truncate(usize::from(HEIGHT));
    lines
}

/// `🐾 minipaw  ● connected · direct (upgraded) · peer 12D3KooW…tLdf · 0:12`
fn header(state: &State, now: Instant, paint: Paint) -> Line<'static> {
    let color = Style::new().fg(phase_color(state.phase));
    let mut spans = vec![
        paint.span("🐾 minipaw", Style::new().add_modifier(Modifier::BOLD)),
        plain_span("  "),
        paint.span("●", color),
        plain_span(" "),
        paint.span(state.phase.label(), color.add_modifier(Modifier::BOLD)),
    ];
    let mut parts = Vec::new();
    if let Some(path) = state.path {
        let mut part = path.to_string();
        if state.upgraded {
            part.push_str(" (upgraded)");
        }
        parts.push(part);
    }
    if let Some(peer) = &state.peer {
        parts.push(format!("peer {}", fmt::short_peer(peer)));
    }
    parts.push(fmt::duration(state.elapsed(now)));
    for part in parts {
        spans.push(paint.dim(" · "));
        spans.push(plain_span(part));
    }
    Line::from(spans)
}

/// `↑ sent   4.1 MiB / 10.0 MiB ━━━━━━━──────── 41%  2.0 MiB/s  ETA 0:03`,
/// or without the total, bar and ETA when the input's size is unknown.
fn up_line(state: &State, width: usize, paint: Paint) -> Line<'static> {
    let accent = Style::new().fg(Color::Cyan);
    let live = !state.phase.is_ended();
    let mut spans = vec![
        paint.span("↑", accent.add_modifier(Modifier::BOLD)),
        paint.dim(" sent  "),
    ];
    let acked = state.progress.acked;
    let Some((len, fraction)) = state.input_len.zip(state.up_fraction()) else {
        spans.push(plain_span(fmt::bytes(acked)));
        if let Some(rate) = state.up_rate().filter(|_| live) {
            spans.push(paint.dim("  ·  "));
            spans.push(plain_span(fmt::rate(rate)));
        }
        return Line::from(spans);
    };
    spans.push(plain_span(format!(
        "{} / {}",
        fmt::bytes(acked),
        fmt::bytes(len)
    )));
    // Whole percents, rounded down: 100% only once everything is acked.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let percent = (fraction * 100.0).floor() as u8;
    let mut tail = vec![plain_span(format!(" {percent:>3}%"))];
    if live {
        if let Some(rate) = state.up_rate() {
            tail.push(paint.dim("  ·  "));
            tail.push(plain_span(fmt::rate(rate)));
        }
        if let Some(eta) = state.eta().filter(|eta| !eta.is_zero()) {
            tail.push(paint.dim("  ETA "));
            tail.push(plain_span(fmt::duration(eta)));
        }
    }
    let used = Line::from(spans.clone()).width() + Line::from(tail.clone()).width() + 1;
    let bar = width.saturating_sub(used).min(MAX_BAR);
    if bar >= MIN_BAR {
        spans.push(plain_span(" "));
        spans.extend(bar_spans(fraction, bar, paint));
    }
    spans.extend(tail);
    Line::from(spans)
}

/// A progress bar `width` cells wide: heavy for done, light for to do, so
/// it reads without colour too.
fn bar_spans(fraction: f64, width: usize, paint: Paint) -> [Span<'static>; 2] {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    let done = ((fraction.clamp(0.0, 1.0) * width as f64).round() as usize).min(width);
    [
        paint.span("━".repeat(done), Style::new().fg(Color::Cyan)),
        paint.dim("─".repeat(width - done)),
    ]
}

/// `↓ recv  1.2 MiB  ·  1.0 MiB/s`
fn down_line(state: &State, paint: Paint) -> Line<'static> {
    let mut spans = vec![
        paint.span(
            "↓",
            Style::new().fg(Color::Magenta).add_modifier(Modifier::BOLD),
        ),
        paint.dim(" recv  "),
        plain_span(fmt::bytes(state.progress.written)),
    ];
    if let Some(rate) = state.down_rate().filter(|_| !state.phase.is_ended()) {
        spans.push(paint.dim("  ·  "));
        spans.push(plain_span(fmt::rate(rate)));
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use minipaw::{Config, Event, PathKind, PeerId, Progress};
    use ratatui::backend::TestBackend;

    const PEER: &str = "12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf";

    fn launch(role: Role, input_len: Option<u64>) -> Launch {
        Launch {
            role,
            ticket: None,
            config: Config::default(),
            verbose: false,
            input_len,
        }
    }

    /// The panel as text, one string per row, trailing spaces trimmed.
    fn render(state: &State, now: Instant, width: u16) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, HEIGHT)).expect("test terminal");
        terminal
            .draw(|frame| {
                let area = frame.area();
                let lines = panel_lines(state, now, area.width, Paint { color: true });
                frame.render_widget(Paragraph::new(lines), area);
            })
            .expect("draw");
        let buffer = terminal.backend().buffer();
        (0..HEIGHT)
            .map(|y| {
                let mut row = String::new();
                let mut x = 0;
                while x < width {
                    let symbol = buffer[(x, y)].symbol();
                    row.push_str(symbol);
                    // A wide character's second cell is padding.
                    x += u16::try_from(Span::raw(symbol).width().max(1)).unwrap_or(1);
                }
                row.trim_end().to_owned()
            })
            .collect()
    }

    fn transferring(t0: Instant) -> State {
        let peer: PeerId = PEER.parse().expect("peer id");
        let mut state = State::new(&launch(Role::Dialer, Some(10 << 20)), t0);
        state.apply_at(&Event::Connecting { peer: peer.clone() }, t0);
        let path = PathKind::Relayed;
        state.apply_at(&Event::Connected { peer, path }, t0);
        state.apply_at(&Event::Upgraded, t0);
        let at = |s: u64| Progress {
            acked: s << 20,
            written: s << 19,
            ..Progress::default()
        };
        state.tick(t0, at(0));
        state.tick(t0 + Duration::from_secs(4), at(4));
        state
    }

    #[test]
    fn a_transfer_shows_progress_rates_and_eta() {
        let t0 = Instant::now();
        let now = t0 + Duration::from_secs(4);
        let rows = render(&transferring(t0), now, 100);
        assert_eq!(
            rows[0],
            "🐾 minipaw  ● connected · direct (upgraded) · peer 12D3KooW…tLdf · 0:04"
        );
        assert!(
            rows[1].starts_with("↑ sent  4.0 MiB / 10.0 MiB ━"),
            "{rows:?}"
        );
        assert!(rows[1].ends_with("40%  ·  1.0 MiB/s  ETA 0:06"), "{rows:?}");
        assert_eq!(rows[2], "↓ recv  2.0 MiB  ·  512.0 KiB/s");
        assert_eq!(rows[3], "");
    }

    #[test]
    fn a_waiting_listener_points_at_the_ticket() {
        let t0 = Instant::now();
        let mut state = State::new(&launch(Role::Listener, None), t0);
        // As after Event::Listening, which needs a real ticket.
        state.phase = Phase::Waiting;
        state.apply_at(&Event::ReservationSlow, t0);
        let rows = render(&state, t0 + Duration::from_secs(75), 80);
        assert_eq!(rows[0], "🐾 minipaw  ● waiting for a peer · 1:15");
        assert!(rows[1].contains("printed above"), "{rows:?}");
        assert!(
            rows[2].starts_with("⚠ still no relay reservation"),
            "{rows:?}"
        );
    }

    #[test]
    fn tiny_widths_do_not_panic() {
        let t0 = Instant::now();
        let state = transferring(t0);
        for width in 1..=50 {
            let rows = render(&state, t0, width);
            assert_eq!(rows.len(), usize::from(HEIGHT));
        }
    }

    #[test]
    fn the_summary_names_the_path() {
        let t0 = Instant::now();
        let mut state = transferring(t0);
        let end = t0 + Duration::from_secs(12);
        state.finish(&Ok(Outcome::Done), end);
        assert_eq!(
            summary(&state, end),
            "# done: 4.0 MiB sent, 2.0 MiB received in 0:12 (direct)"
        );
    }
}
