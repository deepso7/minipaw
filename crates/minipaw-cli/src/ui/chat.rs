//! Chat mode: a full-screen chat when stdin and stdout are the terminal.
//!
//! Until a peer connects, status lines (and the ticket) print as in plain
//! mode, so they stay in the scrollback where they can be copied, and
//! Ctrl-C is an ordinary SIGINT. Once connected, the chat takes over the
//! alternate screen in raw mode: a status bar, the conversation, and an
//! input line. The session's input and output are channels to this UI (see
//! [`chat_io`](super::chat_io)); what the peer sends is split into lines and
//! sanitised before it is drawn. When the session ends, the terminal is
//! restored and the end of the conversation printed, so it is not lost.

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use minipaw::{Error, Event, Handle, Io, Outcome, PathKind};
use ratatui::crossterm::event::{
    self as term_event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Padding, Paragraph};
use ratatui::{Frame, Terminal};

use super::chat_io::{
    ChannelReader, ChannelWriter, LineEdit, LineSplitter, OUTPUT_CAPACITY, sanitize_str,
};
use super::state::{Phase, State};
use super::{Launch, UiMsg, fmt, log, plain, term};

/// How many conversation lines the chat keeps; older ones are dropped.
const MAX_ENTRIES: usize = 5000;

/// How many conversation lines are printed once the chat closes.
const TRANSCRIPT_LINES: usize = 200;

/// How often the screen refreshes when nothing happens.
const TICK: Duration = Duration::from_millis(100);

/// Terminal events handled per refresh at most, so a big paste cannot
/// starve the rest of the loop.
const MAX_KEYS_PER_TICK: usize = 1024;

/// The width of the `peer› ` label column.
const LABEL_WIDTH: usize = 6;

const FOOTER: &str = "Enter send · Ctrl-D end · Ctrl-C quit · PgUp/PgDn scroll";

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
    let Some(first) = wait_for_peer(&ui_rx, &session, &mut state) else {
        // Ended before a peer connected: everything already printed.
        print_pending(&ui_rx);
        log::route_logs_to_stderr();
        return join(session);
    };

    let mut terminal = match enter_full_screen() {
        Ok(terminal) => terminal,
        Err(e) => {
            term::restore();
            handle.stop();
            let _ = join(session);
            log::route_logs_to_stderr();
            return Err(Error::Other(format!("setting up the terminal: {e}")));
        }
    };

    let mut chat = Chat::new(state, input_tx, color_enabled());
    chat.on_event(&first);
    let result = chat.run(&mut terminal, &ui_rx, &output_rx, &handle, session);

    drop(terminal);
    term::restore();
    log::route_logs_to_stderr();
    let result = match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    chat.print_transcript();
    result
}

/// Prints status lines as plain mode would until a peer connects,
/// returning the connecting event, or `None` when the session ended first.
fn wait_for_peer(
    ui_rx: &Receiver<UiMsg>,
    session: &JoinHandle<Result<Outcome, Error>>,
    state: &mut State,
) -> Option<Event> {
    loop {
        match ui_rx.recv_timeout(TICK) {
            Ok(UiMsg::Event(event)) => {
                plain::print_event(&event);
                state.apply(&event);
                if matches!(event, Event::Accepted { .. } | Event::Connected { .. }) {
                    return Some(event);
                }
            }
            Ok(UiMsg::Log(level, message)) => eprintln!("{}", log::plain_line(level, &message)),
            Err(RecvTimeoutError::Timeout) if session.is_finished() => return None,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return None,
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

/// Switches to the alternate screen in raw mode and sets up ratatui there.
fn enter_full_screen() -> std::io::Result<Terminal<term::StderrBackend>> {
    term::enter_alternate_screen()?;
    term::enable_raw_mode()?;
    let mut terminal = Terminal::new(term::StderrBackend::new())?;
    terminal.clear()?;
    Ok(terminal)
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
    /// The label before the text, right-aligned in [`LABEL_WIDTH`].
    fn label(self) -> &'static str {
        match self {
            Who::You => "  you› ",
            Who::Peer => " peer› ",
            Who::Note | Who::Warn => "     · ",
        }
    }

    /// The label for the printed transcript.
    fn transcript_label(self) -> &'static str {
        match self {
            Who::You => "you› ",
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
    bar: Style,
    brand: Style,
    sep: Style,
    you: Style,
    peer: Style,
    note: Style,
    warn: Style,
    text: Style,
    border: Style,
    border_closed: Style,
    hint: Style,
    color: bool,
}

impl Theme {
    fn new(color: bool) -> Self {
        let plain = Style::new();
        let dim = plain.add_modifier(Modifier::DIM);
        let bold = plain.add_modifier(Modifier::BOLD);
        if color {
            let bar = plain.bg(Color::Indexed(236)).fg(Color::Indexed(252));
            Theme {
                bar,
                brand: bar.fg(Color::Indexed(215)).add_modifier(Modifier::BOLD),
                sep: bar.fg(Color::Indexed(242)),
                you: bold.fg(Color::Cyan),
                peer: bold.fg(Color::Magenta),
                note: plain.fg(Color::Indexed(244)),
                warn: plain.fg(Color::Yellow),
                text: plain,
                border: plain.fg(Color::Cyan),
                border_closed: plain.fg(Color::Indexed(240)),
                hint: plain.fg(Color::Indexed(244)),
                color,
            }
        } else {
            let bar = plain.add_modifier(Modifier::REVERSED);
            Theme {
                bar,
                brand: bar.add_modifier(Modifier::BOLD),
                sep: bar,
                you: bold,
                peer: bold,
                note: dim,
                warn: bold,
                text: plain,
                border: plain,
                border_closed: dim,
                hint: dim,
                color,
            }
        }
    }

    /// The status dot's style for `phase`.
    fn dot(&self, phase: Phase) -> Style {
        if !self.color {
            return self.bar;
        }
        let color = match phase {
            Phase::Connected | Phase::Done => Color::Green,
            Phase::Failed => Color::Red,
            Phase::Resuming | Phase::Connecting | Phase::Stopping => Color::Yellow,
            _ => Color::Indexed(244),
        };
        self.bar.fg(color)
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

/// The full-screen chat's state.
struct Chat {
    state: State,
    entries: VecDeque<Entry>,
    /// Lines that fell off [`entries`](Self::entries).
    dropped: usize,
    input: LineEdit,
    /// Our side of the session; `None` once we ended it with Ctrl-D.
    sender: Option<Sender<Vec<u8>>>,
    splitter: LineSplitter,
    /// How many rows the log is scrolled up from the bottom.
    scroll: usize,
    /// The log's height at the last draw, for paging.
    page: usize,
    /// Whether reading terminal events still works.
    keys_ok: bool,
    theme: Theme,
}

impl Chat {
    fn new(state: State, sender: Sender<Vec<u8>>, color: bool) -> Self {
        Chat {
            state,
            entries: VecDeque::new(),
            dropped: 0,
            input: LineEdit::new(),
            sender: Some(sender),
            splitter: LineSplitter::new(),
            scroll: 0,
            page: 10,
            keys_ok: true,
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
            self.read_keys();
            self.drain(ui_rx, output_rx);
            if session.is_finished() {
                break;
            }
            let now = Instant::now();
            self.state.tick(now, handle.progress());
            // A failed draw (the terminal went away) is not worth ending
            // the session for; the next one may work.
            let _ = terminal.draw(|frame| self.draw(frame, now));
        }
        let result = session.join()?;
        self.drain(ui_rx, output_rx);
        if let Some(line) = self.splitter.finish() {
            self.push(Who::Peer, line);
        }
        self.state.finish(&result, Instant::now());
        Ok(result)
    }

    /// Waits up to a tick for terminal events and handles them.
    fn read_keys(&mut self) {
        if !self.keys_ok {
            thread::sleep(TICK);
            return;
        }
        let mut wait = TICK;
        for _ in 0..MAX_KEYS_PER_TICK {
            match term_event::poll(wait) {
                Ok(true) => {}
                Ok(false) => return,
                Err(_) => {
                    self.keys_ok = false;
                    return;
                }
            }
            match term_event::read() {
                Ok(term_event::Event::Key(key)) => self.on_key(key),
                // Resizes are picked up by the next draw.
                Ok(_) => {}
                Err(_) => {
                    self.keys_ok = false;
                    return;
                }
            }
            wait = Duration::ZERO;
        }
    }

    /// Takes in everything the session sent since the last tick.
    fn drain(&mut self, ui_rx: &Receiver<UiMsg>, output_rx: &Receiver<Vec<u8>>) {
        while let Ok(msg) = ui_rx.try_recv() {
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
        while let Ok(chunk) = output_rx.try_recv() {
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

    fn push(&mut self, who: Who, text: String) {
        if self.entries.len() == MAX_ENTRIES {
            self.entries.pop_front();
            self.dropped += 1;
        }
        self.entries.push_back(Entry { who, text });
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
            KeyCode::Up => self.scroll_by(1),
            KeyCode::Down => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::PageUp => self.scroll_by(self.page.saturating_sub(1).max(1)),
            KeyCode::PageDown => {
                self.scroll = self
                    .scroll
                    .saturating_sub(self.page.saturating_sub(1).max(1));
            }
            _ => {}
        }
    }

    fn scroll_by(&mut self, rows: usize) {
        // Clamped to the log's length when drawn.
        self.scroll = self.scroll.saturating_add(rows);
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
        self.scroll = 0;
        self.push(Who::You, sanitize_str(&line));
    }

    fn draw(&mut self, frame: &mut Frame<'_>, now: Instant) {
        let [bar, log, input, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .areas(frame.area());
        self.draw_bar(frame, bar, now);
        self.draw_log(frame, log);
        self.draw_input(frame, input);
        self.draw_footer(frame, footer);
    }

    fn draw_bar(&self, frame: &mut Frame<'_>, area: Rect, now: Instant) {
        let t = &self.theme;
        let state = &self.state;
        let sep = || Span::styled(" │ ", t.sep);
        let mut spans = vec![
            Span::styled(" 🐾 minipaw", t.brand),
            sep(),
            Span::styled("● ", t.dot(state.phase)),
            Span::styled(state.phase.label(), t.bar),
        ];
        if let Some(path) = state.path {
            let path = match (path, state.upgraded) {
                (PathKind::Direct, true) => "direct (upgraded)".to_owned(),
                (path, _) => path.to_string(),
            };
            spans.extend([sep(), Span::styled(path, t.bar)]);
        }
        if let Some(peer) = &state.peer {
            spans.extend([
                sep(),
                Span::styled(format!("peer {}", fmt::short_peer(peer)), t.bar),
            ]);
        }
        spans.extend([
            sep(),
            Span::styled(
                format!(
                    "↑ {}  ↓ {}",
                    fmt::bytes(state.progress.acked),
                    fmt::bytes(state.progress.written)
                ),
                t.bar,
            ),
            sep(),
            Span::styled(fmt::duration(state.elapsed(now)), t.bar),
        ]);
        frame.render_widget(Paragraph::new(Line::from(spans)).style(t.bar), area);
    }

    fn draw_log(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let height = usize::from(area.height);
        let width = usize::from(area.width.saturating_sub(1)).max(LABEL_WIDTH + 4);
        self.page = height;
        // Wrap from the newest line back, only as far as the view needs.
        let want = self.scroll.saturating_add(height);
        let mut rows: VecDeque<Line<'static>> = VecDeque::new();
        for entry in self.entries.iter().rev() {
            for row in wrap(entry, width, &self.theme).into_iter().rev() {
                rows.push_front(row);
            }
            if rows.len() >= want {
                break;
            }
        }
        self.scroll = self.scroll.min(rows.len().saturating_sub(height));
        let end = rows.len() - self.scroll;
        let start = end.saturating_sub(height);
        let visible: Vec<Line<'static>> = rows.drain(start..end).collect();
        // Anchor the conversation to the bottom, as chats do.
        let pad = u16::try_from(height - visible.len()).unwrap_or(0);
        let area = Rect {
            y: area.y + pad,
            height: area.height - pad,
            ..area
        };
        frame.render_widget(Paragraph::new(visible), area);
    }

    fn draw_input(&self, frame: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let open = self.sender.is_some();
        let block = Block::bordered()
            .border_type(BorderType::Rounded)
            .padding(Padding::horizontal(1))
            .border_style(if open { t.border } else { t.border_closed })
            .title(Span::styled(
                if open { " message " } else { " ended " },
                if open { t.you } else { t.hint },
            ));
        let inner = block.inner(area);
        frame.render_widget(block, area);
        if !open {
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
        let mut start = 0;
        while start < cursor && span_width(&chars[start..cursor]) > width {
            start += 1;
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
        let x = u16::try_from(span_width(&chars[start..cursor])).unwrap_or(u16::MAX);
        frame.set_cursor_position(Position {
            x: inner
                .x
                .saturating_add(x)
                .min(inner.right().saturating_sub(1)),
            y: inner.y,
        });
    }

    fn draw_footer(&self, frame: &mut Frame<'_>, area: Rect) {
        let t = &self.theme;
        let mut spans = vec![Span::styled(format!(" {FOOTER}"), t.hint)];
        if self.scroll > 0 {
            spans.push(Span::styled(
                format!("  ↑ scrolled {} lines", self.scroll),
                t.warn,
            ));
        }
        frame.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    /// Prints the end of the conversation to stderr, on the restored
    /// terminal.
    fn print_transcript(&self) {
        let skip = self.entries.len().saturating_sub(TRANSCRIPT_LINES);
        let hidden = self.dropped + skip;
        if hidden > 0 {
            eprintln!("# … {hidden} earlier chat lines not shown");
        }
        for entry in self.entries.iter().skip(skip) {
            eprintln!("{}{}", entry.who.transcript_label(), entry.text);
        }
    }
}

/// `entry` as screen rows `width` cells wide: the label, then the text
/// wrapped (at spaces where it can) and indented under it.
fn wrap(entry: &Entry, width: usize, theme: &Theme) -> Vec<Line<'static>> {
    let avail = width.saturating_sub(LABEL_WIDTH + 1).max(1);
    let style = theme.body(entry.who);
    let mut rows = Vec::new();
    for (i, chunk) in wrap_text(&entry.text, avail).into_iter().enumerate() {
        let label = if i == 0 {
            Span::styled(entry.who.label(), theme.label(entry.who))
        } else {
            Span::raw(" ".repeat(LABEL_WIDTH + 1))
        };
        rows.push(Line::from(vec![label, Span::styled(chunk, style)]));
    }
    rows
}

/// Splits `text` into pieces at most `width` cells wide, breaking after a
/// space when there is one in the piece, and anywhere otherwise.
fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let mut rows = Vec::new();
    let mut row = String::new();
    let mut used = 0;
    // Where `row` may break: its length in bytes and width after a space.
    let mut space: Option<(usize, usize)> = None;
    for c in text.chars() {
        let w = char_width(c);
        if used + w > width && used > 0 {
            match space {
                Some((at, at_width)) if at < row.len() => {
                    let rest = row.split_off(at);
                    rows.push(std::mem::replace(&mut row, rest));
                    used -= at_width;
                }
                _ => {
                    rows.push(std::mem::take(&mut row));
                    used = 0;
                }
            }
            space = None;
        }
        row.push(c);
        used += w;
        if c == ' ' {
            space = Some((row.len(), used));
        }
    }
    rows.push(row);
    rows
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

fn span_width(chars: &[char]) -> usize {
    chars.iter().map(|&c| char_width(c)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_wraps_at_spaces_and_inside_long_words() {
        assert_eq!(wrap_text("", 10), [""]);
        assert_eq!(wrap_text("hello world", 20), ["hello world"]);
        assert_eq!(wrap_text("hello big world", 10), ["hello big ", "world"]);
        assert_eq!(wrap_text("abcdefghij", 4), ["abcd", "efgh", "ij"]);
        // Wide characters count double.
        assert_eq!(wrap_text("🐾🐾🐾", 4), ["🐾🐾", "🐾"]);
        assert_eq!(char_width('a'), 1);
        assert_eq!(char_width('é'), 1);
        assert_eq!(char_width('界'), 2);
    }

    #[test]
    fn wrapped_rows_are_indented_under_the_label() {
        let theme = Theme::new(false);
        let entry = Entry {
            who: Who::Peer,
            text: "one two three".into(),
        };
        let rows: Vec<String> = wrap(&entry, 14, &theme)
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(rows, [" peer› one ", "       two ", "       three"]);
    }
}
