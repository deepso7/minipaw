//! Plain mode: `#` status lines on stderr, printed as things happen. The
//! output is what scripts (and `scripts/check.sh`) read, so it must not
//! change.

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
        // Connecting, LinkLost, Resumed and Stopping have no line; -v covers them.
        _ => return None,
    })
}
