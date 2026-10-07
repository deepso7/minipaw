# minipaw

Pipe bytes between two machines, peer to peer, over
[minip2p](https://github.com/deepso7/minip2p): QUIC, a relay to meet
through, and a hole-punched direct path when one can be found. There are no
accounts and no control plane: a listener hands out a ticket, and a dialer
connects with it.

This is the library behind the `minipaw` command (the `minipaw-cli`
package). It is blocking, with no async runtime: [`Session::run`] drives a
session on the calling thread, with helper threads for input and output.

```rust,no_run
use std::io::{stdin, stdout};

use minipaw::{Config, Event, Io};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Listen, and pipe stdin and stdout with the first dialer.
    let session = minipaw::listen(Config::default(), Io::new(stdin(), stdout()))
        .on_event(|event| {
            if let Event::Listening { ticket } = event {
                eprintln!("connect with: {ticket}");
            }
        });

    // Another thread can stop the session or poll its progress.
    let handle = session.handle();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(60));
        eprintln!("sent {} bytes", handle.progress().acked);
        handle.stop();
    });

    let outcome = session.run()?;
    eprintln!("finished: {outcome:?}");

    // The other side:
    let ticket: minipaw::Ticket = "mp…".parse()?;
    minipaw::dial(ticket, Config::default(), Io::stdio()).run()?;
    Ok(())
}
```

Sessions survive their stream: when a relay cuts a circuit or the path
upgrades to a direct one, the dialer resumes on a fresh stream from the last
byte the other side confirmed. Status changes arrive as [`Event`]s;
diagnostics go through the [`log`](https://docs.rs/log) facade under the
`minipaw` target, and the library never installs a logger itself.
