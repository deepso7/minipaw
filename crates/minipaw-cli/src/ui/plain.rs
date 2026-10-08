//! Plain mode: `#` status lines on stderr, printed as things happen. The
//! output is what scripts (and `scripts/check.sh`) read, so it must not
//! change.

use std::io::{ErrorKind, IsTerminal as _, Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::time::{Duration, Instant};

#[cfg(unix)]
use minipaw::Handle;
use minipaw::{Error, Event, Io, Outcome};

use super::{Launch, log, term};

/// Runs the session over stdin and stdout, printing its events and log
/// records to stderr, with Ctrl-C stopping it.
pub fn run(launch: Launch) -> Result<Outcome, Error> {
    log::route_logs_to_stderr();
    let session = launch.session(Io::stdio()).on_event(|e| print_event(&e));
    term::install_interrupts(session.handle()).map_err(Error::Other)?;
    session.run()
}

/// Runs the session over stdin and stdout printing nothing, for `-q`;
/// `main` prints a fatal error. Ctrl-C stops it, as in plain mode.
///
/// As a ProxyCommand, ssh closes our stdin and stdout and hangs up on us
/// (SIGHUP) as it exits. A session that is up and whose input has ended is
/// finishing on its own then, and the hangup leaves it to: killed, the
/// server would hold it until it gave up on the link (over a minute),
/// counting against its session limit. Any other hangup stops the session
/// as Ctrl-C does, telling the server: before it is up, the dial would
/// otherwise go on for ssh that is gone.
pub fn run_quiet(launch: Launch) -> Result<Outcome, Error> {
    let up = Arc::new(AtomicBool::new(false));
    let ended = Arc::new(AtomicBool::new(false));
    let io =
        Io::new(Stdin(ended.clone()), Stdout).close_on_peer_fin(std::io::stdin().is_terminal());
    let session = launch.session(io).on_event({
        let up = up.clone();
        move |e| {
            if matches!(e, Event::Connected { .. } | Event::Accepted { .. }) {
                up.store(true, Ordering::SeqCst);
            }
        }
    });
    #[cfg(unix)]
    on_hangup(session.handle(), up, ended)?;
    term::install_interrupts(session.handle()).map_err(Error::Other)?;
    session.run()
}

/// Routes SIGHUP to [`hangup`] on a thread of its own.
#[cfg(unix)]
fn on_hangup(handle: Handle, up: Arc<AtomicBool>, ended: Arc<AtomicBool>) -> Result<(), Error> {
    let mut signals = signal_hook::iterator::Signals::new([signal_hook::consts::SIGHUP])
        .map_err(|e| Error::Other(format!("handling SIGHUP: {e}")))?;
    std::thread::spawn(move || {
        for _ in signals.forever() {
            // ssh closes our stdin just before it hangs up; give the input
            // thread a moment to see that.
            let by = Instant::now() + EOF_GRACE;
            while up.load(Ordering::SeqCst) && !ended.load(Ordering::SeqCst) && Instant::now() < by
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            if hangup_stops(up.load(Ordering::SeqCst), ended.load(Ordering::SeqCst)) {
                handle.stop();
            } else {
                ::log::debug!("hung up; the session is finishing on its own");
            }
        }
    });
    Ok(())
}

/// How long a hangup waits for stdin to end before it stops the session.
#[cfg(unix)]
const EOF_GRACE: Duration = Duration::from_millis(200);

/// Whether a hangup stops a session that is `up` (welcomed) and whose
/// input has `ended`.
#[cfg_attr(not(unix), allow(dead_code))]
fn hangup_stops(up: bool, ended: bool) -> bool {
    !(up && ended)
}

/// Stdin, noting when it ends.
struct Stdin(Arc<AtomicBool>);

impl Read for Stdin {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = std::io::stdin().read(buf)?;
        if n == 0 && !buf.is_empty() {
            self.0.store(true, Ordering::SeqCst);
        }
        Ok(n)
    }
}

/// Stdout. As with [`Io::stdio`], its reader going away (ssh exiting) is
/// not an error: the rest of the peer's data is discarded.
struct Stdout;

impl Write for Stdout {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match std::io::stdout().write(buf) {
            Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(buf.len()),
            result => result,
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match std::io::stdout().flush() {
            Err(e) if e.kind() == ErrorKind::BrokenPipe => Ok(()),
            result => result,
        }
    }
}

/// Prints a session event as a `#` status line on stderr, if it has one.
pub fn print_event(event: &Event) {
    if let Some(text) = event_text(event) {
        eprintln!("{text}");
    }
}

/// The `#` status line plain mode prints for a session event, if it has
/// one, without a trailing newline. The listening line carries the ticket on
/// a line of its own.
pub fn event_text(event: &Event) -> Option<String> {
    Some(match event {
        Event::Reserving { relay } => format!("# reserving a slot on relay {relay}…"),
        Event::Listening { ticket } => format!("# 🐾 listening; connect with:\nminipaw {ticket}"),
        Event::ReservationSlow => {
            "# still no relay reservation; is the relay reachable over UDP?".to_owned()
        }
        Event::ReservationLost => "# lost the relay reservation; reacquiring".to_owned(),
        Event::Accepted { peer, path } => format!("# connection from {peer} ({path})"),
        Event::Connected { path, .. } => format!("# connected ({path})"),
        Event::Upgraded => "# upgraded to a direct connection".to_owned(),
        // Connecting, LinkLost, Resumed, ReservationRestored and Stopping
        // have no line; -v covers them.
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hangup_stops_all_but_a_finishing_session() {
        // Still dialing, or waiting for a client: ssh is gone, so stop.
        assert!(hangup_stops(false, false));
        assert!(hangup_stops(false, true));
        // Up with input still open: a hangup that is not ssh finishing.
        assert!(hangup_stops(true, false));
        // ssh sent everything and left: the session finishes on its own.
        assert!(!hangup_stops(true, true));
    }
}
