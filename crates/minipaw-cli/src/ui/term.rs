//! Terminal plumbing shared by the terminal UIs: a ratatui backend on
//! stderr that never queries the cursor, putting the terminal back however
//! the process ends, and Ctrl-C.
//!
//! Everything here draws on and restores **stderr**. Stdout may carry the
//! session's data, so nothing may write escape sequences to it; that is why
//! `ratatui::init`/`restore` and `crossterm::cursor::position` (which writes
//! `ESC[6n` to stdout) are banned in `clippy.toml`.
//!
//! Terminal modes are switched on only through [`enable_raw_mode`],
//! [`enter_alternate_screen`] and [`StderrBackend`] (which tracks the
//! cursor's visibility), so [`restore`] knows what to undo.

use std::io::{self, Stderr, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use minipaw::Handle;
use ratatui::backend::{Backend, ClearType, CrosstermBackend, WindowSize};
use ratatui::buffer::Cell;
use ratatui::crossterm::{cursor, execute, terminal};
use ratatui::layout::{Position, Size};

static RAW_MODE: AtomicBool = AtomicBool::new(false);
static ALTERNATE_SCREEN: AtomicBool = AtomicBool::new(false);
static CURSOR_HIDDEN: AtomicBool = AtomicBool::new(false);

/// The terminal's size as `(columns, rows)`, from an ioctl: nothing is
/// written to the terminal.
pub fn size() -> Option<(u16, u16)> {
    terminal::size().ok()
}

/// Puts the terminal in raw mode, for [`restore`] to undo.
pub fn enable_raw_mode() -> io::Result<()> {
    terminal::enable_raw_mode()?;
    RAW_MODE.store(true, Ordering::SeqCst);
    Ok(())
}

/// Whether raw mode can be turned on, leaving it off. Chat mode checks this
/// before the session starts, so a terminal that refuses falls back to
/// plain mode instead of ending a session already under way.
pub fn raw_mode_works() -> bool {
    terminal::enable_raw_mode().is_ok() && terminal::disable_raw_mode().is_ok()
}

/// Switches stderr's terminal to the alternate screen, for [`restore`] to
/// undo.
pub fn enter_alternate_screen() -> io::Result<()> {
    execute!(io::stderr(), terminal::EnterAlternateScreen)?;
    ALTERNATE_SCREEN.store(true, Ordering::SeqCst);
    Ok(())
}

/// Undoes whatever terminal state this module set up: raw mode, the
/// alternate screen, a hidden cursor. Idempotent and safe from any thread,
/// including the panic hook and the Ctrl-C handler; errors are ignored.
pub fn restore() {
    if RAW_MODE.swap(false, Ordering::SeqCst) {
        let _ = terminal::disable_raw_mode();
    }
    let mut stderr = io::stderr();
    if ALTERNATE_SCREEN.swap(false, Ordering::SeqCst) {
        let _ = execute!(stderr, terminal::LeaveAlternateScreen);
    }
    if CURSOR_HIDDEN.swap(false, Ordering::SeqCst) {
        let _ = execute!(stderr, cursor::Show);
    }
    let _ = stderr.flush();
}

/// Makes panics [`restore`] the terminal before the panic message prints.
pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        restore();
        previous(info);
    }));
}

/// Calls [`restore`] when dropped, so early returns and unwinding put the
/// terminal back.
#[derive(Debug, Default)]
#[must_use = "the terminal is restored when the guard is dropped"]
pub struct TerminalGuard;

impl TerminalGuard {
    /// A guard for the terminal state set up from now on.
    pub fn new() -> Self {
        TerminalGuard
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore();
    }
}

static SESSION: OnceLock<Handle> = OnceLock::new();
static INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Routes Ctrl-C (SIGINT) to [`interrupt`] for the session behind `handle`.
/// Call once, before running the session.
///
/// # Errors
///
/// When a handler is already installed, or installing it failed.
pub fn install_interrupts(handle: Handle) -> Result<(), String> {
    SESSION
        .set(handle)
        .map_err(|_| "installing the Ctrl-C handler: already installed".to_owned())?;
    ctrlc::set_handler(interrupt).map_err(|e| format!("installing the Ctrl-C handler: {e}"))
}

/// What Ctrl-C does: the first stops the session, telling the peer; a
/// second [`restore`]s the terminal and exits with status 130 on the spot.
/// In raw mode Ctrl-C arrives as a key, and the UI calls this itself.
pub fn interrupt() {
    if INTERRUPTED.swap(true, Ordering::SeqCst) {
        restore();
        std::process::exit(130);
    }
    if let Some(handle) = SESSION.get() {
        handle.stop();
    }
}

/// A ratatui backend drawing on stderr.
///
/// It delegates to [`CrosstermBackend`] except for
/// [`get_cursor_position`](Backend::get_cursor_position), which reports
/// the bottom-left cell instead of asking the terminal: crossterm's query
/// writes `ESC[6n` to stdout, which may be the session's data. An inline
/// viewport therefore starts at the bottom of the screen. It also records
/// hiding the cursor, so [`restore`] shows it again.
pub struct StderrBackend {
    inner: CrosstermBackend<Stderr>,
}

impl StderrBackend {
    /// A backend on the process's stderr.
    pub fn new() -> Self {
        StderrBackend {
            inner: CrosstermBackend::new(io::stderr()),
        }
    }
}

impl Default for StderrBackend {
    fn default() -> Self {
        StderrBackend::new()
    }
}

impl Write for StderrBackend {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Write::flush(&mut self.inner)
    }
}

impl Backend for StderrBackend {
    type Error = io::Error;

    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        self.inner.draw(content)
    }

    fn append_lines(&mut self, n: u16) -> io::Result<()> {
        self.inner.append_lines(n)
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        self.inner.hide_cursor()?;
        CURSOR_HIDDEN.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        self.inner.show_cursor()?;
        CURSOR_HIDDEN.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        let rows = self.inner.size().map_or(1, |size| size.height);
        Ok(Position {
            x: 0,
            y: rows.saturating_sub(1),
        })
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        self.inner.set_cursor_position(position)
    }

    fn clear(&mut self) -> io::Result<()> {
        self.inner.clear()
    }

    fn clear_region(&mut self, clear_type: ClearType) -> io::Result<()> {
        self.inner.clear_region(clear_type)
    }

    fn size(&self) -> io::Result<Size> {
        self.inner.size()
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        self.inner.window_size()
    }

    fn flush(&mut self) -> io::Result<()> {
        Backend::flush(&mut self.inner)
    }
}
