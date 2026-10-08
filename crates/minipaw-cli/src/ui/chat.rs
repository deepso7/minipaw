//! Chat mode: a chat when stdin and stdout are the terminal.
//!
//! Until a peer connects, status lines (and the ticket) print as in plain
//! mode, so they stay in the scrollback where they can be copied, and
//! Ctrl-C is an ordinary SIGINT. Once connected, the terminal goes into raw
//! mode and a small inline box (a prompt and a status line) sits below the
//! conversation, which is printed into the ordinary scrollback line by
//! line. The session's input and output are channels to this UI (see
//! [`chat_io`](super::chat_io)); what the peer sends is split into lines
//! and sanitised before it is printed. When the session ends, the box is
//! cleared and the terminal restored.

use std::io;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use minipaw::{Error, Event, Handle, Io, Outcome};
use ratatui::backend::{Backend as _, ClearType, IntoCrossterm as _};
use ratatui::crossterm::event::{
    self as term_event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use ratatui::crossterm::queue;
use ratatui::crossterm::style::{Print, PrintStyledContent, StyledContent};
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use ratatui::{Frame, Terminal, TerminalOptions, Viewport};

use super::chat_io::{
    ChannelReader, ChannelWriter, LineEdit, LineSplitter, OUTPUT_CAPACITY, sanitize_str,
};
use super::state::{Phase, State};
use super::{Launch, UiMsg, fmt, log, plain, term};

/// How often the screen refreshes when nothing happens.
const TICK: Duration = Duration::from_millis(100);

/// Terminal events handled per refresh at most, so a big paste cannot
/// starve the rest of the loop.
const MAX_KEYS_PER_TICK: usize = 1024;
/// Received bytes taken in per tick while running; see [`Chat::drain`].
const MAX_OUTPUT_PER_TICK: usize = 256 * 1024;
/// Events and log records taken in per tick while running.
const MAX_MSGS_PER_TICK: usize = 1000;

/// The box's height: a blank line, the input line and a status line.
const INLINE_HEIGHT: u16 = 3;
/// The box's key hints, left out when the box is narrow.
const INLINE_HINTS: &str = "Ctrl-D end · Ctrl-C quit";
/// Narrower than this, the box leaves out the peer.
const INLINE_WIDE: u16 = 80;

/// Runs the session as a chat.
pub fn run(launch: Launch) -> Result<Outcome, Error> {
    let (ui_tx, ui_rx) = std::sync::mpsc::channel();
    let (input_tx, reader) = ChannelReader::channel();
    let (writer, output_rx) = ChannelWriter::channel(OUTPUT_CAPACITY);
    let io = Io::new(reader, writer).close_on_peer_fin(true);
    let events = ui_tx.clone();
    let session = launch.session(io).on_event(move |e| {
        let _ = events.send(UiMsg::Event(e));
    });
    let handle = session.handle();
    term::install_panic_hook();
    let _guard = term::TerminalGuard::new();
    term::install_interrupts(handle.clone()).map_err(Error::Other)?;
    log::route_logs_to(ui_tx);
    let session = thread::Builder::new()
        .name("minipaw-session".into())
        .spawn(move || session.run())
        .map_err(|e| Error::Other(format!("starting the session: {e}")))?;

    let mut state = State::new(&launch, Instant::now());
    if !wait_for_peer(&ui_rx, &session, &mut state) {
        // Ended before a peer connected: print what it reported up to its
        // very end, then whatever logs come after.
        let result = join(session);
        print_pending(&ui_rx);
        log::route_logs_to_stderr();
        return result;
    }

    let terminal = enter_inline();
    let mut terminal = match terminal {
        Ok(terminal) => terminal,
        Err(e) => {
            term::restore();
            handle.stop();
            let _ = join(session);
            print_pending(&ui_rx);
            log::route_logs_to_stderr();
            return Err(Error::Other(format!("setting up the terminal: {e}")));
        }
    };

    let mut chat = Chat::new(state, input_tx, color_enabled());
    let result = chat.run(&mut terminal, &ui_rx, &output_rx, &handle, session);
    let _ = chat.close(&mut terminal, matches!(result, Ok(Ok(_))));

    drop(terminal);
    term::restore();
    log::route_logs_to_stderr();
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// Prints status lines as plain mode would until a peer connects; false
/// when the session ended first.
fn wait_for_peer(
    ui_rx: &Receiver<UiMsg>,
    session: &JoinHandle<Result<Outcome, Error>>,
    state: &mut State,
) -> bool {
    loop {
        match ui_rx.recv_timeout(TICK) {
            Ok(UiMsg::Event(event)) => {
                plain::print_event(&event);
                state.apply(&event);
                if matches!(event, Event::Accepted { .. } | Event::Connected { .. }) {
                    return true;
                }
            }
            Ok(UiMsg::Log(level, message)) => eprintln!("{}", log::plain_line(level, &message)),
            Err(RecvTimeoutError::Timeout) if session.is_finished() => return false,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return false,
        }
    }
}

/// Prints whatever the session reported before ending, as plain mode
/// would.
fn print_pending(ui_rx: &Receiver<UiMsg>) {
    while let Ok(msg) = ui_rx.try_recv() {
        match msg {
            UiMsg::Event(event) => plain::print_event(&event),
            UiMsg::Log(level, message) => eprintln!("{}", log::plain_line(level, &message)),
        }
    }
}

/// The session's result, or its panic carried on to this thread.
fn join(session: JoinHandle<Result<Outcome, Error>>) -> Result<Outcome, Error> {
    match session.join() {
        Ok(result) => result,
        Err(panic) => {
            term::restore();
            std::panic::resume_unwind(panic)
        }
    }
}

/// Turns on raw mode and sets up ratatui in a small box right below what
/// has been printed.
fn enter_inline() -> io::Result<Terminal<term::StderrBackend>> {
    term::enable_raw_mode()?;
    inline_terminal()
}

/// A ratatui terminal with the inline box at the cursor, and the cursor
/// parked on the box's top-left cell.
///
/// The box draws its own input cursor, so the terminal's (hidden) one can
/// stay parked there: that cell starts a line, which stays where it is
/// however the terminal rewraps lines on a resize. So clearing from the
/// cursor down always clears the whole box.
fn inline_terminal() -> io::Result<Terminal<term::StderrBackend>> {
    let mut terminal = Terminal::with_options(
        term::StderrBackend::querying_cursor(),
        TerminalOptions {
            viewport: Viewport::Inline(INLINE_HEIGHT),
        },
    )?;
    park(&mut terminal)?;
    Ok(terminal)
}

/// Moves the cursor to the inline box's top-left cell.
fn park(terminal: &mut Terminal<term::StderrBackend>) -> io::Result<()> {
    let top = terminal.get_frame().area().as_position();
    terminal.set_cursor_position(top)
}

/// Whether to use colour: `NO_COLOR` set to anything non-empty turns it
/// off.
fn color_enabled() -> bool {
    std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty())
}

/// Who a conversation line is from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Who {
    /// Us: a line we sent.
    You,
    /// The peer: a line it sent.
    Peer,
    /// A note about the session: an event or a log record.
    Note,
    /// A note about trouble.
    Warn,
}

impl Who {
    /// The label: left-aligned, so the text starts in one column after
    /// `you›` and `peer›`, and notes read like the `#` lines above.
    fn label(self) -> &'static str {
        match self {
            Who::You => "you›  ",
            Who::Peer => "peer› ",
            Who::Note | Who::Warn => "· ",
        }
    }
}

/// One line of the conversation; `text` is already sanitised.
#[derive(Clone, Debug)]
struct Entry {
    who: Who,
    text: String,
}

/// The chat's styles: coloured, or only bold/dim/reverse under `NO_COLOR`.
struct Theme {
    you: Style,
    peer: Style,
    note: Style,
    warn: Style,
    text: Style,
    hint: Style,
    color: bool,
}

impl Theme {
    fn new(color: bool) -> Self {
        let plain = Style::new();
        let dim = plain.add_modifier(Modifier::DIM);
        let bold = plain.add_modifier(Modifier::BOLD);
        if color {
            Theme {
                you: bold.fg(Color::Cyan),
                peer: bold.fg(Color::Magenta),
                note: plain.fg(Color::Indexed(244)),
                warn: plain.fg(Color::Yellow),
                text: plain,
                hint: plain.fg(Color::Indexed(244)),
                color,
            }
        } else {
            Theme {
                you: bold,
                peer: bold,
                note: dim,
                warn: bold,
                text: plain,
                hint: dim,
                color,
            }
        }
    }

    /// The status dot's style for `phase`.
    fn dot(&self, phase: Phase) -> Style {
        if !self.color {
            return Style::new();
        }
        let color = match phase {
            Phase::Connected | Phase::Done => Color::Green,
            Phase::Failed => Color::Red,
            Phase::Resuming | Phase::Connecting | Phase::Stopping => Color::Yellow,
            _ => Color::Indexed(244),
        };
        Style::new().fg(color)
    }

    fn label(&self, who: Who) -> Style {
        match who {
            Who::You => self.you,
            Who::Peer => self.peer,
            Who::Note => self.note,
            Who::Warn => self.warn,
        }
    }

    fn body(&self, who: Who) -> Style {
        match who {
            Who::You | Who::Peer => self.text,
            Who::Note => self.note,
            Who::Warn => self.warn,
        }
    }
}

/// The chat's state.
struct Chat {
    state: State,
    /// Lines not yet printed above the box.
    outbox: Vec<Entry>,
    /// The terminal's size when the box was last anchored.
    size: Option<(u16, u16)>,
    input: LineEdit,
    /// Our side of the session; `None` once we ended it with Ctrl-D.
    sender: Option<Sender<Vec<u8>>>,
    splitter: LineSplitter,
    /// Set once reading terminal events failed for good and the session
    /// was stopped.
    keys_failed: bool,
    theme: Theme,
}

impl Chat {
    fn new(state: State, sender: Sender<Vec<u8>>, color: bool) -> Self {
        Chat {
            state,
            outbox: Vec::new(),
            size: term::size(),
            input: LineEdit::new(),
            sender: Some(sender),
            splitter: LineSplitter::new(),
            keys_failed: false,
            theme: Theme::new(color),
        }
    }

    /// Runs the chat until the session thread ends; returns its result, or
    /// its panic.
    fn run(
        &mut self,
        terminal: &mut Terminal<term::StderrBackend>,
        ui_rx: &Receiver<UiMsg>,
        output_rx: &Receiver<Vec<u8>>,
        handle: &Handle,
        session: JoinHandle<Result<Outcome, Error>>,
    ) -> thread::Result<Result<Outcome, Error>> {
        loop {
            self.read_keys(handle);
            self.drain(ui_rx, output_rx, MAX_MSGS_PER_TICK, MAX_OUTPUT_PER_TICK);
            if session.is_finished() {
                break;
            }
            let now = Instant::now();
            self.state.tick(now, handle.progress());
            // A failed draw (the terminal went away) is not worth ending
            // the session for; the next one may work.
            let _ = self.show(terminal, now);
        }
        let result = session.join()?;
        // Everything the peer sent was acked once written here; keep it all.
        self.drain(ui_rx, output_rx, usize::MAX, usize::MAX);
        for line in self.splitter.finish() {
            self.push(Who::Peer, line);
        }
        // The last tick may predate the session's last bytes.
        let now = Instant::now();
        self.state.tick(now, handle.progress());
        self.state.finish(&result, now);
        Ok(result)
    }

    /// Waits up to a tick for terminal events and handles them. Interrupted
    /// reads are retried; any other error stops the session, since without
    /// keys the chat could not be ended.
    fn read_keys(&mut self, handle: &Handle) {
        if self.keys_failed {
            thread::sleep(TICK);
            return;
        }
        let mut wait = TICK;
        for _ in 0..MAX_KEYS_PER_TICK {
            let event =
                term_event::poll(wait).and_then(|ready| ready.then(term_event::read).transpose());
            match event {
                Ok(Some(term_event::Event::Key(key))) => self.on_key(key),
                // Resizes are picked up by the next draw.
                Ok(Some(_)) => {}
                Ok(None) => return,
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
                    ) =>
                {
                    // Retried next tick; the pause keeps a terminal that
                    // keeps saying so from spinning the loop.
                    thread::sleep(Duration::from_millis(10));
                    return;
                }
                Err(e) => {
                    self.keys_failed = true;
                    self.push(
                        Who::Warn,
                        sanitize_str(&format!("reading the keyboard failed: {e}; stopping")),
                    );
                    handle.stop();
                    return;
                }
            }
            wait = Duration::ZERO;
        }
    }

    /// Takes in up to `msgs` of the session's events and logs, and up to
    /// `limit` bytes of its output.
    fn drain(
        &mut self,
        ui_rx: &Receiver<UiMsg>,
        output_rx: &Receiver<Vec<u8>>,
        msgs: usize,
        limit: usize,
    ) {
        // Bounded too, so a flood of log records cannot starve keys.
        for _ in 0..msgs {
            let Ok(msg) = ui_rx.try_recv() else {
                break;
            };
            match msg {
                UiMsg::Event(event) => {
                    self.state.apply(&event);
                    self.on_event(&event);
                }
                UiMsg::Log(level, message) => {
                    let who = match level {
                        ::log::Level::Error | ::log::Level::Warn => Who::Warn,
                        _ => Who::Note,
                    };
                    self.push(who, sanitize_str(&message));
                }
            }
        }
        // A bounded share per tick, so a peer streaming faster than we can
        // sanitise cannot starve keys and redraws; the rest waits in the
        // bounded channel, which slows the peer through acks.
        let mut taken = 0;
        while taken < limit
            && let Ok(chunk) = output_rx.try_recv()
        {
            taken += chunk.len();
            for line in self.splitter.push(&chunk) {
                self.push(Who::Peer, line);
            }
        }
    }

    /// Notes a session event in the conversation.
    fn on_event(&mut self, event: &Event) {
        let (who, text) = match event {
            Event::Accepted { peer, path } | Event::Connected { peer, path } => (
                Who::Note,
                format!("connected to {} ({path})", fmt::short_peer(peer)),
            ),
            Event::Upgraded => (Who::Note, "upgraded to a direct connection".to_owned()),
            Event::LinkLost { reason } => (
                Who::Warn,
                format!("link lost: {}; resuming…", sanitize_str(reason)),
            ),
            Event::Resumed => (Who::Note, "resumed".to_owned()),
            Event::ReservationLost => (
                Who::Note,
                "lost the relay reservation; reacquiring".to_owned(),
            ),
            Event::ReservationRestored => (Who::Note, "relay reservation restored".to_owned()),
            Event::ReservationSlow => (
                Who::Warn,
                "still no relay reservation; is the relay reachable over UDP?".to_owned(),
            ),
            Event::Stopping => (
                Who::Note,
                "stopping; telling the peer… (Ctrl-C again to quit now)".to_owned(),
            ),
            _ => return,
        };
        self.push(who, text);
    }

    /// Queues a line; it is printed, and let go of, on the next tick.
    fn push(&mut self, who: Who, text: String) {
        self.outbox.push(Entry { who, text });
    }

    fn on_key(&mut self, key: KeyEvent) {
        if key.kind == KeyEventKind::Release {
            return;
        }
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char('c') if ctrl => term::interrupt(),
            KeyCode::Char('d') if ctrl => {
                if self.input.is_empty() {
                    if self.sender.take().is_some() {
                        self.push(
                            Who::Note,
                            "you ended your side; waiting for the peer…".to_owned(),
                        );
                    }
                } else {
                    self.input.delete();
                }
            }
            KeyCode::Char('u') if ctrl => self.input.clear(),
            KeyCode::Char('a') if ctrl => self.input.home(),
            KeyCode::Char('e') if ctrl => self.input.end(),
            KeyCode::Char(c)
                if !ctrl
                    && !key.modifiers.contains(KeyModifiers::ALT)
                    && self.sender.is_some()
                    && !c.is_control() =>
            {
                self.input.insert(c);
            }
            KeyCode::Enter => self.send(),
            KeyCode::Backspace => self.input.backspace(),
            KeyCode::Delete => self.input.delete(),
            KeyCode::Left => self.input.left(),
            KeyCode::Right => self.input.right(),
            KeyCode::Home => self.input.home(),
            KeyCode::End => self.input.end(),
            KeyCode::Esc => self.input.clear(),
            _ => {}
        }
    }

    /// Sends the input line to the peer and shows it.
    fn send(&mut self) {
        let Some(sender) = &self.sender else {
            return;
        };
        if self.input.is_empty() {
            return;
        }
        let line = self.input.take();
        let mut bytes = line.clone().into_bytes();
        bytes.push(b'\n');
        if sender.send(bytes).is_err() {
            // The session stopped reading: it is ending.
            self.sender = None;
            return;
        }
        self.push(Who::You, sanitize_str(&line));
    }

    /// The input line in `inner`, with the cursor; or, once we ended our
    /// side, what we are waiting for.
    fn draw_input_line(&self, frame: &mut Frame<'_>, inner: Rect) {
        let t = &self.theme;
        if self.sender.is_none() {
            let text = Span::styled(
                "waiting for the peer to finish · Ctrl-C to quit now",
                t.hint,
            );
            frame.render_widget(Paragraph::new(text), inner);
            return;
        }
        if self.input.is_empty() {
            let hint = Span::styled("type a message", t.hint);
            frame.render_widget(Paragraph::new(hint), inner);
        }
        // Scroll the line sideways so the cursor stays in view.
        let width = usize::from(inner.width).saturating_sub(1).max(1);
        let chars = self.input.chars();
        let cursor = self.input.cursor();
        let (mut start, mut before) = (cursor, 0);
        while let Some(&c) = start.checked_sub(1).and_then(|i| chars.get(i))
            && before + char_width(c) <= width
        {
            before += char_width(c);
            start -= 1;
        }
        if !self.input.is_empty() {
            let mut shown = String::new();
            let mut used = 0;
            for &c in &chars[start..] {
                let w = char_width(c);
                if used + w > usize::from(inner.width) {
                    break;
                }
                used += w;
                shown.push(c);
            }
            frame.render_widget(Paragraph::new(Span::styled(shown, t.text)), inner);
        }
        let x = u16::try_from(before).unwrap_or(u16::MAX);
        let cursor = Position {
            x: inner
                .x
                .saturating_add(x)
                .min(inner.right().saturating_sub(1)),
            y: inner.y,
        };
        // The terminal's cursor stays parked; see `inline_terminal`.
        if let Some(cell) = frame.buffer_mut().cell_mut(cursor) {
            cell.set_style(Style::new().add_modifier(Modifier::REVERSED));
        }
    }

    /// Re-anchors the box after a resize, prints new lines above it, and
    /// draws it.
    fn show(
        &mut self,
        terminal: &mut Terminal<term::StderrBackend>,
        now: Instant,
    ) -> io::Result<()> {
        let size = term::size();
        if size != self.size || !self.outbox.is_empty() {
            // Clear the box (the cursor is parked on it), print the new
            // lines where it was, and set up a fresh box below them.
            self.print_outbox(terminal.backend_mut())?;
            *terminal = inline_terminal()?;
            self.size = size;
        }
        terminal.draw(|frame| self.draw_box(frame, now))?;
        park(terminal)
    }

    /// Clears the box and prints the lines not yet shown in its
    /// place, as plain styled text: the terminal wraps long lines itself,
    /// rewraps them on a resize, and copies them back as typed.
    fn print_outbox(&mut self, backend: &mut term::StderrBackend) -> io::Result<()> {
        backend.clear_region(ClearType::AfterCursor)?;
        let theme = &self.theme;
        for entry in self.outbox.drain(..) {
            let label =
                StyledContent::new(theme.label(entry.who).into_crossterm(), entry.who.label());
            let text = StyledContent::new(theme.body(entry.who).into_crossterm(), entry.text);
            queue!(
                backend,
                PrintStyledContent(label),
                PrintStyledContent(text),
                Print("\r\n")
            )?;
        }
        io::Write::flush(backend)
    }

    /// The box: a blank line setting it off from the conversation, a
    /// prompt with the input line, and a dim status line under it.
    fn draw_box(&self, frame: &mut Frame<'_>, now: Instant) {
        let t = &self.theme;
        let state = &self.state;
        let [_, input, status_area] =
            Layout::vertical([Constraint::Length(1); 3]).areas(frame.area());
        // Under the input text, past the prompt.
        let [_, status_area] =
            Layout::horizontal([Constraint::Length(2), Constraint::Min(1)]).areas(status_area);
        let wide = status_area.width >= INLINE_WIDE;
        let sep = || Span::styled(" · ", t.hint);

        let open = self.sender.is_some();
        let prompt = Span::styled("› ", if open { t.you } else { t.hint });
        frame.render_widget(Paragraph::new(prompt), input);
        let [_, text] =
            Layout::horizontal([Constraint::Length(2), Constraint::Min(1)]).areas(input);
        self.draw_input_line(frame, text);

        let mut status = vec![
            Span::styled("● ", t.dot(state.phase)),
            Span::styled(state.phase.label(), t.hint),
        ];
        if let Some(path) = state.path {
            status.extend([sep(), Span::styled(path.to_string(), t.hint)]);
        }
        if wide && let Some(peer) = &state.peer {
            status.extend([sep(), Span::styled(fmt::short_peer(peer), t.hint)]);
        }
        status.extend([
            sep(),
            Span::styled(
                format!(
                    "↑ {}  ↓ {}",
                    fmt::bytes(state.progress.acked),
                    fmt::bytes(state.progress.written)
                ),
                t.hint,
            ),
            sep(),
            Span::styled(fmt::duration(state.elapsed(now)), t.hint),
        ]);
        let status = Line::from(status);
        // The hints only where they fit beside the status, with a gap.
        let room = usize::from(status_area.width).saturating_sub(status.width());
        if room >= INLINE_HINTS.chars().count() + 2 {
            let hints = Line::styled(INLINE_HINTS, t.hint).right_aligned();
            frame.render_widget(Paragraph::new(hints), status_area);
        }
        frame.render_widget(Paragraph::new(status), status_area);
    }

    /// Prints what is left in place of the box, leaving the cursor after
    /// it, for whatever is printed next.
    fn close(&mut self, terminal: &mut Terminal<term::StderrBackend>, ok: bool) -> io::Result<()> {
        if ok {
            let state = &self.state;
            let mut line = format!(
                "chat ended: {} sent, {} received",
                fmt::bytes(state.progress.acked),
                fmt::bytes(state.progress.written)
            );
            if let Some(path) = state.path {
                line.push_str(&format!(" ({path})"));
            }
            self.push(Who::Note, line);
        }
        self.print_outbox(terminal.backend_mut())
    }
}

/// How many cells `c` takes on screen.
fn char_width(c: char) -> usize {
    if c.is_ascii() {
        1
    } else {
        let mut buf = [0u8; 4];
        Span::raw(&*c.encode_utf8(&mut buf)).width()
    }
}

#[cfg(test)]
mod tests {
    use minipaw::PathKind;

    use super::*;

    #[test]
    fn ticks_take_a_bounded_share_but_the_end_takes_everything() {
        let launch = Launch::new(None, minipaw::Config::default(), false);
        let (input_tx, _input_rx) = std::sync::mpsc::channel();
        let mut chat = Chat::new(State::new(&launch, Instant::now()), input_tx, false);
        let (_ui_tx, ui_rx) = std::sync::mpsc::channel::<UiMsg>();
        let (out_tx, out_rx) = std::sync::mpsc::channel();
        // 1 MiB of 1 KiB lines, four times what a tick may take.
        let line = format!("{}\n", "x".repeat(1023));
        for _ in 0..1024 {
            out_tx.send(line.clone().into_bytes()).unwrap();
        }
        chat.drain(&ui_rx, &out_rx, MAX_MSGS_PER_TICK, MAX_OUTPUT_PER_TICK);
        assert_eq!(chat.outbox.len(), MAX_OUTPUT_PER_TICK / 1024);
        chat.drain(&ui_rx, &out_rx, usize::MAX, usize::MAX);
        assert_eq!(chat.outbox.len(), 1024);
    }

    #[test]
    fn ticks_take_a_bounded_number_of_messages() {
        let launch = Launch::new(None, minipaw::Config::default(), false);
        let (input_tx, _input_rx) = std::sync::mpsc::channel();
        let mut chat = Chat::new(State::new(&launch, Instant::now()), input_tx, false);
        let (ui_tx, ui_rx) = std::sync::mpsc::channel::<UiMsg>();
        let (_out_tx, out_rx) = std::sync::mpsc::channel();
        for i in 0..MAX_MSGS_PER_TICK + 10 {
            ui_tx
                .send(UiMsg::Log(::log::Level::Info, i.to_string()))
                .unwrap();
        }
        chat.drain(&ui_rx, &out_rx, MAX_MSGS_PER_TICK, MAX_OUTPUT_PER_TICK);
        assert_eq!(chat.outbox.len(), MAX_MSGS_PER_TICK);
        chat.drain(&ui_rx, &out_rx, usize::MAX, usize::MAX);
        assert_eq!(chat.outbox.len(), MAX_MSGS_PER_TICK + 10);
    }

    /// The box drawn `width` cells wide, as text rows.
    fn box_rows(chat: &Chat, width: u16, now: Instant) -> Vec<String> {
        let backend = ratatui::backend::TestBackend::new(width, INLINE_HEIGHT);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| chat.draw_box(frame, now)).unwrap();
        let buffer = terminal.backend().buffer();
        (0..INLINE_HEIGHT)
            .map(|y| {
                (0..width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_owned()
            })
            .collect()
    }

    #[test]
    fn the_box_is_a_prompt_and_a_status_line() {
        let launch = Launch::new(None, minipaw::Config::default(), false);
        let (input_tx, _input_rx) = std::sync::mpsc::channel();
        let t0 = Instant::now();
        let mut state = State::new(&launch, t0);
        let peer: minipaw::PeerId = "12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf"
            .parse()
            .expect("peer id");
        state.apply_at(
            &Event::Accepted {
                peer,
                path: PathKind::Relayed,
            },
            t0,
        );
        let chat = Chat::new(state, input_tx, false);

        let rows = box_rows(&chat, 90, t0);
        assert_eq!(rows[0], "", "{rows:?}");
        assert_eq!(rows[1], "› type a message", "{rows:?}");
        assert!(
            rows[2].starts_with("  ● connected · via relay · 12D3KooW…tLdf · ↑ 0 B  ↓ 0 B · 0:00"),
            "{rows:?}"
        );
        assert!(rows[2].ends_with("Ctrl-D end · Ctrl-C quit"), "{rows:?}");

        // Where the hints would run into the status, they are left out.
        let rows = box_rows(&chat, 82, t0);
        assert!(rows[2].contains("12D3KooW…tLdf"), "{rows:?}");
        assert!(!rows[2].contains("Ctrl-"), "{rows:?}");

        // Narrow: no hints, no peer.
        let rows = box_rows(&chat, 50, t0);
        assert_eq!(
            rows[2], "  ● connected · via relay · ↑ 0 B  ↓ 0 B · 0:00",
            "{rows:?}"
        );
    }

    #[test]
    fn wide_characters_take_two_cells() {
        assert_eq!(char_width('a'), 1);
        assert_eq!(char_width('é'), 1);
        assert_eq!(char_width('界'), 2);
        assert_eq!(char_width('🐾'), 2);
    }
}
