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
    match event {
        Event::Reserving { relay } => eprintln!("# reserving a slot on relay {relay}…"),
        Event::Listening { ticket } => {
            eprintln!("# 🐾 listening; connect with:\nminipaw {ticket}");
        }
        Event::ReservationSlow => {
            eprintln!("# still no relay reservation; is the relay reachable over UDP?");
        }
        Event::ReservationLost => eprintln!("# lost the relay reservation; reacquiring"),
        Event::Accepted { peer, path } => eprintln!("# connection from {peer} ({path})"),
        Event::Connected { path, .. } => eprintln!("# connected ({path})"),
        Event::Upgraded => eprintln!("# upgraded to a direct connection"),
        // Connecting, LinkLost, Resumed and Stopping have no line; -v covers them.
        _ => {}
    }
}
