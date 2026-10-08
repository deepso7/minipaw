# minipaw

[![CI](https://github.com/deepso7/minipaw/actions/workflows/ci.yml/badge.svg)](https://github.com/deepso7/minipaw/actions/workflows/ci.yml)

Pipe stdin/stdout between two machines, peer to peer — like netcat, but it
gets through NATs, needs no accounts, and every byte is end-to-end encrypted.

```console
$ minipaw                                  # machine A
# 🐾 listening; connect with:
minipaw mpAQDEiiVb-s5zPPFvR0SjX0lVJgAkCAESINgMwzfDtX-D_vKZ0slyfVb4K-lw-ijkyHNLfPt1UkM2

$ echo hello | minipaw mpAQDEiiVb-s5zPPF…  # machine B
```

minipaw is an alternative to [tailcat](https://github.com/tailscale/tailcat)
built on [minip2p](https://github.com/deepso7/minip2p), a small libp2p
implementation in Rust.

## Install

```sh
cargo install --git https://github.com/deepso7/minipaw minipaw-cli
```

This installs the `minipaw` binary.

## Use

One side listens and prints a ticket; the other pastes it. The session ends
when both sides have sent everything, or when either side hits Ctrl-C.

```sh
# chat
minipaw                       # A: prints a ticket
minipaw <ticket>              # B: type on either side

# send a file
minipaw > report.pdf          # A: receive
minipaw <ticket> < report.pdf # B: send

# anything that reads stdin or writes stdout
tar cz dir/ | minipaw         # A
minipaw <ticket> | tar xz     # B
```

Status goes to stderr (see [Terminal UI](#terminal-ui)), so redirecting
stdout captures only the data. Add `-v` to see connection details.
`minipaw parse <ticket>` shows what a ticket contains.

A ticket is new every run unless the listener keeps its key:
`minipaw --identity my.key` creates `my.key` (mode 0600) on the first run
and prints the same ticket on every later one. `minipaw ticket` prints
that stable ticket without listening, for the identity in
`~/.config/minipaw/serve.key` by default (`$MINIPAW_HOME`,
`--identity PATH`); pass the listener's `--relay`, which is part of the
ticket. The file and its directory must be private to you. Anyone with
the ticket can connect whenever you listen, so delete the file to rotate it.

Exit status: `0` when both directions finished and were confirmed, `1` on
any error (including the other side hitting one), `130` on Ctrl-C.

## Serve a port (SSH over minipaw)

`minipaw serve` keeps listening with a stable ticket and forwards every
session to a local TCP port, several at once (16 by default,
`--max-sessions N`). It defaults to sshd on `127.0.0.1:22`; `--forward
HOST:PORT` or `--forward PORT` picks another.

```console
$ minipaw serve                             # on the machine to reach
# identity: /home/me/.config/minipaw/serve.key (new)
# forwarding to 127.0.0.1:22
# 🐾 listening; connect with:
minipaw mpAQ…
# ssh: minipaw ssh user@mpAQ…
# [1] 12D3KooW…ab12 connected (direct)
# [1] ended: 1.2 MiB sent, 40.0 KiB received in 3:02
```

On the other machine, the ticket stands in for the host:

```sh
minipaw ssh me@mpAQ…                   # a shell
minipaw ssh -p 2222 me@mpAQ… uptime    # any ssh options, a command
minipaw cp -r photos me@mpAQ…:backup/  # scp; remote paths are TICKET:PATH
```

These run the system's `ssh` and `scp` with every other argument as is,
adding a ProxyCommand of `minipaw -q <ticket>` (`-q` keeps it silent,
since ssh shares its stderr). The host key is remembered in known_hosts
under `minipaw-<peer id>` (which ssh lowercases), so the first connection asks to trust it, as
ssh does for any new host.

For other tools that run ssh, such as git or rsync, put the ProxyCommand
in `~/.ssh/config`:

```
Host home
  User me
  HostKeyAlias minipaw-12D3KooW…
  ProxyCommand minipaw -q mpAQ…
```

Then `ssh home`, `scp file home:`, `git clone home:repo` all go through
minipaw.

- serve prints `#` lines on stderr, never a terminal UI, so it runs as
  is under systemd or nohup. Ctrl-C stops it (exit 0).
- The identity lives in `~/.config/minipaw/serve.key` (`--identity PATH`),
  so the ticket survives restarts. `minipaw serve --new` rotates it, and
  the old ticket stops working.
- Sessions don't survive a serve restart: a connected ssh is cut off and
  just reconnects.
- Without hole punching, sessions go through the relay: slower, and a
  relay with limits cuts its circuits every so often; sessions resume
  across each cut, but the relay also rate limits new circuits, so a long,
  large relayed transfer can fail.
- The ticket is a secret: anyone holding it can reach the forwarded port.

## Terminal UI

minipaw picks how to show status from where its streams point:

- **Status panel** when stdout is redirected or piped and stderr is a
  terminal, as when sending or receiving a file. A small panel on stderr
  shows the ticket, the connection state, relay or direct, bytes sent and
  received, rates, and an ETA when sending a regular file. Nothing but data
  ever goes to stdout.
- **Chat** when stdin and stdout are both terminals: the conversation
  prints into your terminal's normal scrollback, above a small input box
  that shows the connection state and traffic. Enter sends, Ctrl-D ends
  your side, Ctrl-C stops.
- **Plain `#` lines** when stderr isn't a terminal, or with `--plain`. This
  is what scripts and logs see.
- **Nothing** with `-q`, not even `-v`'s log: only a fatal error prints.

The panel and chat need stderr to be a terminal, `TERM` set to something
other than `dumb`, and a terminal of at least 40×8; the chat also needs raw
mode to work. Otherwise minipaw falls back to plain lines.

## How it works

- **Ticket.** The `mp…` ticket holds the server's peer ID (its public key),
  a random 16-byte token, and the relay if it isn't the default. Anyone with
  the ticket can connect, so share it like a password. The server serves one
  client per run; `minipaw serve` serves many.
- **Relay first, then direct.** The server reserves a slot on a
  [Circuit Relay v2](https://github.com/libp2p/specs/blob/master/relay/circuit-v2.md)
  server (by default `relay.minip2p.com`). The client connects through it,
  then DCUtR hole punching upgrades the connection to a direct QUIC path when
  the networks allow. The session moves to the direct path mid-transfer
  without losing or repeating a byte.
- **Resumable sessions.** If the connection drops — a relay cutting a
  circuit, a path upgrade, a network blip — the client reconnects and both
  sides resume from the last byte the other received, for up to 60 seconds.
  A quiet session pings every few seconds, so a connection that dies
  without a word is noticed within 20 seconds too.
- **Backpressure.** The receiver acknowledges bytes only once they are
  written to its stdout, so a slow reader slows the sender instead of
  filling memory.

Direct paths are QUIC (TLS 1.3); relayed paths run an end-to-end Noise
session through the relay. Either way the relay sees only encrypted
traffic, and both sides are authenticated by their peer IDs.

### Your own relay

Run [`minip2p-relay`](https://github.com/deepso7/minip2p/tree/main/examples/relay-server)
somewhere public, then point the server at it. The ticket embeds a
non-default relay, so clients need no flag.

```sh
minipaw --relay /dns/relay.example.com/udp/19876/quic-v1/p2p/12D3KooW…
# or: export MINIPAW_RELAY=…
```

With the relay's default limits a circuit carries only 128 KiB each way,
which is fine when hole punching succeeds. For peers that can't punch
through, run it with
`--max-circuit-bytes 0 --max-circuit-duration 0 --circuit-peer-rate off`.

## Library

The networking lives in the [`minipaw`](crates/minipaw) crate, a blocking
library you can embed: listen or dial, give it any reader and writer, and
get events and progress back.

```rust
use minipaw::{Config, Event, Io};

let outcome = minipaw::listen(Config::default(), Io::stdio())
    .on_event(|event| {
        if let Event::Listening { ticket } = event {
            eprintln!("connect with: {ticket}");
        }
    })
    .run()?;
```

See [its README](crates/minipaw/README.md) for dialing, stopping and
custom streams.

## Development

```sh
cargo test --workspace              # unit tests
scripts/check.sh                    # end-to-end behaviour checks (~1.5 min)
scripts/bench.sh                    # loopback throughput, direct + relayed
scripts/bench-remote.sh <ssh-host>  # real-network throughput
```

The workspace has two crates: `crates/minipaw` is the library and
`crates/minipaw-cli` is the `minipaw` command built on it.

The scripts start a throwaway local relay from a sibling `../minip2p`
checkout; build it once with
`cargo build --release -p minip2p-relay-server-example` there, or set
`MINIP2P_RELAY` to a relay binary (see `scripts/lib.sh`). To skip the local
relay, set `RELAY` to any relay address, such as the hosted default (CI does
this). `resume` still needs a relay binary, since it starts its own relay
with circuit limits; `check.sh` skips it when there is none. Benchmark results
accumulate in `bench/results.tsv`, one row per run with the commit.

A few environment variables exist for testing only:
`MINIPAW_FORCE_RELAY=1` disables direct connections,
`MINIPAW_DIRECT=<multiaddr>` makes the client also dial the server directly,
`MINIPAW_TEST_DROP_LINK_AFTER=<bytes>` makes the server go silent on its
stream once, after that many bytes, `MINIPAW_TEST_DROP_WELCOME=1` makes it
drop its first `Welcome`, `MINIPAW_TEST_RESUME_TIMEOUT=<secs>` shortens how
long it waits for a client to resume, and `MINIPAW_TEST_CONNECT_DELAY=<secs>`
makes `serve` wait that long before each connection to its target.

## License

Licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.
