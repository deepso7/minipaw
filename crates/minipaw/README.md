# minipaw

Pipe bytes between two machines, peer to peer, over
[minip2p](https://github.com/deepso7/minip2p): QUIC, a relay to meet
through, and a hole-punched direct path when one can be found. There are no
accounts and no control plane: a listener hands out a ticket, and a dialer
connects with it. Every byte is end-to-end encrypted, and both sides are
authenticated by their peer ids.

This is the library behind the `minipaw` command (the
[`minipaw-cli`](https://github.com/deepso7/minipaw) package).

## Blocking API

There is no async runtime. You build a `Session` with `minipaw::listen` or
`minipaw::dial`, and `Session::run` drives it on the calling thread until
it ends, with helper threads reading its input and writing its output.

- `on_event` registers a callback for status changes (`Event`). It runs
  synchronously on the session's thread, so it must not block.
- `handle()` returns a `Handle` you can clone and send to other threads:
  `stop()` ends the session (the peer is told), and `progress()` returns a
  cheap snapshot of the byte counters, meant for polling.
- `run()` returns an `Outcome` when both directions finished, or an `Error`
  saying why not.

## Dial

```rust,no_run
use std::{thread, time::Duration};

use minipaw::{Config, Error, Event, Io, Outcome, Ticket};

fn main() {
    let ticket: Ticket = std::env::args()
        .nth(1)
        .expect("usage: dial <ticket>")
        .parse()
        .expect("not a minipaw ticket");

    // Send stdin to the listener and write what it sends to stdout.
    let session = minipaw::dial(ticket, Config::default(), Io::stdio()).on_event(|event| {
        match event {
            Event::Connected { peer, path } => eprintln!("connected to {peer} ({path})"),
            Event::Upgraded => eprintln!("now direct"),
            _ => {}
        }
    });

    // Give up after ten minutes, and report progress meanwhile.
    let handle = session.handle();
    thread::spawn(move || {
        for _ in 0..600 {
            thread::sleep(Duration::from_secs(1));
            let p = handle.progress();
            eprintln!("sent {} of {} bytes, received {}", p.acked, p.read, p.received);
        }
        handle.stop();
    });

    let code = match session.run() {
        Ok(Outcome::Done | Outcome::Delivered) => 0,
        Err(Error::Stopped) => 130,
        Err(Error::Input(e)) => {
            eprintln!("reading stdin: {e}");
            1
        }
        Err(e) => {
            eprintln!("{e}");
            1
        }
    };
    std::process::exit(code);
}
```

## Listen

```rust,no_run
use minipaw::{Config, Event, Io};

fn main() -> Result<(), minipaw::Error> {
    let outcome = minipaw::listen(Config::default(), Io::stdio())
        .on_event(|event| {
            if let Event::Listening { ticket } = event {
                eprintln!("connect with: {ticket}");
            }
        })
        .run()?;
    eprintln!("finished: {outcome:?}");
    Ok(())
}
```

A listener serves one dialer per session. `Event::Listening` arrives once
it holds a relay slot; the ticket carries its peer id, a random token and
the relay (if not the default). Anyone with the ticket can connect, so treat
it like a password. `Ticket` parses from and displays as the `mp…` string.

## Input and output

`Io::stdio()` uses the process's stdin and stdout, and `Io::tcp` a
`TcpStream`. For anything else, pass any `Read + Send` and `Write + Send` to
`Io::new`, such as a pair of channels. Each runs on its own helper thread,
so both may block.

The session ends once both sides have finished sending and each confirmed
the other's data. An interactive input never ends on its own, so
`Io::close_on_peer_fin(true)` ends our side when the peer finishes;
`Io::stdio()` turns this on when stdin is a terminal.

Once the peer has finished sending and all of it is written, the output is
dropped. For `Io::tcp` that half-closes the socket, so the target reads EOF
right then while its replies keep flowing back. Dropping stdout does not
close it, so with `Io::stdio()` it closes only when the process exits.

Bytes are acknowledged only once written to the output, so a slow writer
slows the sender down instead of filling memory.

A blocking read cannot be cancelled, so the input thread can outlive
`Session::run`, parked in `read` until it returns. End a reader you own once
`run` returns: `shutdown` a socket, drop a channel's sender. `Io::stdio()`
may take one more chunk of stdin that way, so run one stdio session per
process.

The output thread can outlive `run` as well: when a session stops or fails,
`run` waits up to a second for a blocked write, then returns. The thread
finishes writing the data it already received and only then drops the
writer; unblock a writer you own (`shutdown` a socket) to end it early.

`Io::tcp` does this cleanup itself, so none of its threads outlive the
session: after a clean end it shuts down the socket's read side; otherwise,
once a blocked write has had its second of grace, it shuts down both
sides, which also ends a write to a target that stopped reading.

`Io::stdio()` treats stdout's reader going away (`| head`) as netcat does:
the rest of the peer's data is discarded and the session still succeeds.
For writers passed to `Io::new`, any write error, `BrokenPipe` included,
ends the session with `Error::Output`.

## Events and logging

`Event` reports milestones: reserving, holding, losing and regaining a relay
slot, connecting, being connected relayed or direct (`PathKind`), the
upgrade to a direct path, a lost stream and its resumption, and stopping.
Byte counts are not events; poll `Handle::progress` instead.

Sessions survive their stream: when a relay cuts a circuit or the
connection moves to a direct path, the dialer resumes on a fresh stream
from the last byte the other side confirmed, for up to 60 seconds.

Diagnostics go through the [`log`](https://docs.rs/log) facade under
`minipaw` targets. The library never installs a logger; install one (such
as `env_logger`) to see them.

## Configuration and constraints

`Config::default()` uses the hosted relay `DEFAULT_RELAY`. Set
`Config::relay` (see `parse_relay`) to use your own
[minip2p relay](https://github.com/deepso7/minip2p/tree/main/examples/relay-server),
or `Config::force_relay` to skip hole punching.

- Blocking only: no async API.
- QUIC only: direct paths are QUIC (TLS 1.3), and relays must be QUIC
  addresses. Relayed paths run an end-to-end Noise session, so the relay
  sees only ciphertext.
- One session per `Session`, one dialer per listener.

## License

Licensed under either of [MIT](https://github.com/deepso7/minipaw/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/deepso7/minipaw/blob/main/LICENSE-APACHE),
at your option.
