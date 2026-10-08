#!/usr/bin/env bash
# End-to-end behaviour checks against a relay.
#
#   scripts/check.sh            # every check
#   scripts/check.sh <name>...  # just these (see `scripts/check.sh list`)
#
# Starts its own relays unless RELAY is set (see scripts/lib.sh). Each check
# prints PASS or FAIL with a reason; the script exits nonzero if any failed.
# Set SHOW_LOGS=1 to print both sides' stderr for failures.
set -uo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

BIN=${BIN:-target/release/minipaw}
cargo build -q --release -p minipaw-cli --bin minipaw || exit 2
cargo build -q --release -p minipaw --example squat || exit 2
SQUAT=target/release/examples/squat

T=$(mktemp -d)
cleanup() {
  reap_all
  stop_relay
  if [ -n "${LIMITED_PID:-}" ]; then
    kill "$LIMITED_PID" 2>/dev/null
    wait "$LIMITED_PID" 2>/dev/null
    rm -f "$LIMITED_LOG"
  fi
  rm -rf "$T"
}
trap cleanup EXIT
ensure_relay

# Each check reports exactly one result; one that ends without any is a
# failure too.
FAILED=()
RESULT=
pass() {
  [ -n "$RESULT" ] && return
  RESULT=pass
  printf 'PASS  %-24s %s\n' "$CHECK" "$*"
}
fail() {
  [ -n "$RESULT" ] && return
  RESULT=fail
  printf 'FAIL  %-24s %s\n' "$CHECK" "$*"
  FAILED+=("$CHECK")
  if [ -n "${SHOW_LOGS:-}" ]; then
    for f in "$T"/*.err; do echo "--- $f"; tail -20 "$f"; done
  fi
}
# For a check that cannot run here; it neither passes nor fails the run.
skip() {
  [ -n "$RESULT" ] && return
  RESULT=skip
  printf 'SKIP  %-24s %s\n' "$CHECK" "$*"
}
elapsed() { echo $(($(date +%s) - $1)); }

# A transfer both ways; data must arrive byte for byte and both exit 0.
# Fails the check and returns 1 otherwise.
transfer() { # transfer <up-bytes> <down-bytes>
  head -c "$1" /dev/urandom >"$T/up"
  head -c "$2" /dev/urandom >"$T/down"
  start_server "$T/down" "$T/server.out" "$T/server.err" || { fail "no ticket"; return 1; }
  local client=0 server=0
  "$BIN" -v "$TICKET" <"$T/up" >"$T/client.out" 2>"$T/client.err" &
  track $!
  wait_upto $! 120 || client=$?
  wait_upto "$SERVER_PID" 30 || server=$?
  cmp -s "$T/up" "$T/server.out" || { fail "upload differs"; return 1; }
  cmp -s "$T/down" "$T/client.out" || { fail "download differs"; return 1; }
  [ "$client$server" = 00 ] || { fail "exit codes client=$client server=$server"; return 1; }
}

# Sized to finish well inside the timeout on a relayed path too: CI's macOS
# runners cannot hole-punch and get about 300 KB/s through the hosted relay.
check_transfer() {
  transfer 10000000 5000000 && pass "10 MB up, 5 MB down"
}

check_forced_relay() {
  MINIPAW_FORCE_RELAY=1 transfer 5000000 5000000 && pass "5 MB each way, relay only"
}

# A relay with default circuit limits (128 KiB per direction) cuts the
# circuit every few hundred KB; the session must resume each time. It needs
# a local relay binary, since it runs its own relay with those limits.
check_resume() {
  if [ -z "${LIMITED:-}" ]; then
    relay_bin >/dev/null || { skip "no local minip2p-relay binary"; return; }
    local saved=$RELAY saved_pid=${RELAY_PID:-} saved_log=${RELAY_LOG:-}
    RELAY= RELAY_PID=
    ensure_relay --circuit-peer-rate off --circuit-ip-rate off
    LIMITED=$RELAY LIMITED_PID=$RELAY_PID LIMITED_LOG=$RELAY_LOG
    RELAY=$saved RELAY_PID=$saved_pid RELAY_LOG=$saved_log
  fi
  local before
  before=$(grep -c 'circuit opened' "$LIMITED_LOG")
  RELAY=$LIMITED MINIPAW_FORCE_RELAY=1 transfer 2000000 500000 || return
  local circuits=$(($(grep -c 'circuit opened' "$LIMITED_LOG") - before))
  [ "$circuits" -gt 5 ] || { fail "only $circuits circuits: the relay never cut the session"; return; }
  pass "2 MB up, 0.5 MB down across $circuits relay circuits"
}

# A relay that drops a circuit may tell only one side (minip2p#306). The
# server forgets its link after 1 MB without closing it; the client must
# notice the silence, reconnect, and finish the session where it left off.
# Relay only, as in the real case; a path upgrade would also recover it.
check_heartbeat() {
  MINIPAW_FORCE_RELAY=1 MINIPAW_TEST_DROP_LINK_AFTER=1000000 transfer 3000000 1000000 || return
  grep -q "test hook: dropping the link" "$T/server.err" ||
    { fail "the server never dropped its link"; return; }
  grep -q "no word from the server" "$T/client.err" ||
    { fail "the client never noticed the silence"; return; }
  pass "client noticed a silent link and resumed: 3 MB up, 1 MB down"
}

# Ctrl-C on one side ends both: it exits 130, the peer exits 1, promptly.
# With `both`, both sides are interrupted at once and both exit 130.
interrupt() { # interrupt <client|server|blocked|both>
  local who=$1 server_in=$T/server.in
  if [ "$who" = blocked ]; then
    # The server floods a client whose stdout reader never reads.
    mkfifo "$server_in"
    (echo from-server; head -c 50000000 /dev/zero; exec sleep 60) >"$server_in" &
  else
    mkfifo "$server_in"
    (echo from-server; exec sleep 60) >"$server_in" &
  fi
  track $!
  start_server "$server_in" "$T/server.out" "$T/server.err" || { fail "no ticket"; return; }
  local client_out=$T/client.out
  if [ "$who" = blocked ]; then
    mkfifo "$T/stuck"
    sleep 120 <"$T/stuck" &
    track $!
    client_out=$T/stuck
  fi
  mkfifo "$T/client.in"
  (echo from-client; exec sleep 60) >"$T/client.in" &
  track $!
  "$BIN" -v "$TICKET" <"$T/client.in" >"$client_out" 2>"$T/client.err" &
  local client=$!
  track $client
  for _ in $(seq 1 100); do
    grep -q from-client "$T/server.out" 2>/dev/null && break
    sleep 0.1
  done
  [ "$who" = blocked ] && sleep 2 # let the client's stdout pipe fill
  local victim=$client peer=$SERVER_PID
  [ "$who" = server ] && { victim=$SERVER_PID; peer=$client; }
  local start
  start=$(date +%s)
  kill -INT "$victim"
  [ "$who" = both ] && kill -INT "$peer"
  local victim_code=0 peer_code=0
  wait_upto "$victim" 10 || victim_code=$?
  wait_upto "$peer" 10 || peer_code=$?
  local took
  took=$(elapsed "$start")
  local want_peer=1
  [ "$who" = both ] && want_peer=130
  [ "$victim_code" = 130 ] && [ "$peer_code" = "$want_peer" ] ||
    { fail "exit codes interrupted=$victim_code peer=$peer_code"; return; }
  [ "$took" -le 5 ] || { fail "took ${took}s to end both sides"; return; }
  grep -q from-client "$T/server.out" || { fail "server lost the client's data"; return; }
  pass "both ended in ${took}s"
}

check_interrupt_client() { interrupt client; }
check_interrupt_server() { interrupt server; }
check_interrupt_blocked() { interrupt blocked; }
check_interrupt_both() { interrupt both; }

# A wrong token is refused at once.
check_wrong_token() {
  mkfifo "$T/server.in"
  sleep 30 >"$T/server.in" &
  track $!
  start_server "$T/server.in" /dev/null "$T/server.err" || { fail "no ticket"; return; }
  local bad
  bad=$(python3 -c '
import base64, sys
t = sys.argv[1][2:]
raw = bytearray(base64.urlsafe_b64decode(t + "=" * (-len(t) % 4)))
raw[2] ^= 1
print("mp" + base64.urlsafe_b64encode(bytes(raw)).decode().rstrip("="))' "$TICKET")
  local code=0
  echo hi | "$BIN" "$bad" >/dev/null 2>"$T/client.err" &
  track $!
  wait_upto $! 30 || code=$?
  kill "$SERVER_PID" 2>/dev/null
  [ "$code" = 1 ] && grep -q "wrong token" "$T/client.err" ||
    { fail "exit=$code: $(cat "$T/client.err")"; return; }
  pass "refused"
}

# A second client is turned away while the first holds the session.
check_busy() {
  mkfifo "$T/server.in"
  sleep 30 >"$T/server.in" &
  track $!
  start_server "$T/server.in" /dev/null "$T/server.err" || { fail "no ticket"; return; }
  mkfifo "$T/first.in"
  sleep 30 >"$T/first.in" &
  track $!
  "$BIN" "$TICKET" <"$T/first.in" >/dev/null 2>"$T/first.err" &
  track $!
  for _ in $(seq 1 100); do
    grep -q "connection from" "$T/server.err" && break
    sleep 0.1
  done
  local code=0
  echo hi | "$BIN" "$TICKET" >/dev/null 2>"$T/client.err" &
  track $!
  wait_upto $! 30 || code=$?
  kill "$SERVER_PID" 2>/dev/null
  [ "$code" = 1 ] && grep -q "busy" "$T/client.err" ||
    { fail "exit=$code: $(cat "$T/client.err")"; return; }
  pass "second client refused"
}

# Streams that never say Hello must not lock out a real client.
check_squatters() {
  mkfifo "$T/server.in"
  (echo from-server; exec sleep 40) >"$T/server.in" &
  track $!
  start_server "$T/server.in" "$T/server.out" "$T/server.err" || { fail "no ticket"; return; }
  local port peer
  port=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/\([0-9]*\)/.*#\1#p' "$T/server.err" | head -1)
  peer=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/[0-9]*/quic-v1/p2p/\(.*\)#\1#p' "$T/server.err" | head -1)
  "$SQUAT" "/ip4/127.0.0.1/udp/$port/quic-v1/p2p/$peer" 16 30 2>"$T/squat.err" &
  track $!
  for _ in $(seq 1 100); do
    grep -q "streams held" "$T/squat.err" && break
    sleep 0.1
  done
  grep -q "streams held" "$T/squat.err" || { fail "squatter could not open its streams"; return; }
  # Well inside the squatters' 10s Hello deadline, so the pool is still full
  # and admitting the client means evicting one of them. The client dials
  # the server directly too, so a slow relay cannot push it past that.
  MINIPAW_DIRECT="/ip4/127.0.0.1/udp/$port/quic-v1" \
    "$BIN" "$TICKET" < <(echo from-client) >"$T/client.out" 2>"$T/client.err" &
  local client=$!
  track $client
  for _ in $(seq 1 80); do
    grep -q from-server "$T/client.out" && break
    sleep 0.1
  done
  grep -q "too many pending" "$T/server.err" ||
    { fail "the client got in without evicting a squatter"; return; }
  kill "$client" "$SERVER_PID" 2>/dev/null
  wait "$client" 2>/dev/null
  grep -q from-server "$T/client.out" || { fail "client locked out: $(cat "$T/client.err")"; return; }
  pass "client admitted past 16 squatting streams"
}

# A local read error is a failure, not a clean end of input. It happens
# before the client connects, so the server never had a session and keeps
# listening for one.
check_stdin_error() {
  mkfifo "$T/server.in"
  sleep 30 >"$T/server.in" &
  track $!
  start_server "$T/server.in" /dev/null "$T/server.err" || { fail "no ticket"; return; }
  local code=0
  "$BIN" "$TICKET" </ >/dev/null 2>"$T/client.err" &
  track $!
  wait_upto $! 30 || code=$?
  local listening=no
  kill -0 "$SERVER_PID" 2>/dev/null && listening=yes
  kill "$SERVER_PID" 2>/dev/null
  [ "$code" = 1 ] && grep -q "reading stdin" "$T/client.err" ||
    { fail "client exit=$code: $(cat "$T/client.err")"; return; }
  [ "$listening" = yes ] || { fail "server exited without ever having a client"; return; }
  pass "client exits 1, server keeps listening"
}

# A local write error (not a closed reader) fails both sides.
check_stdout_error() {
  head -c 100000 /dev/urandom >"$T/down"
  start_server "$T/down" /dev/null "$T/server.err" || { fail "no ticket"; return; }
  local code=0 server=0
  # Past a 1-block file size limit (with SIGXFSZ ignored), writes fail with
  # EFBIG. A closed stdout would not do: Rust treats EBADF there as success.
  (trap '' XFSZ; ulimit -f 1; exec "$BIN" "$TICKET" </dev/null >"$T/client.out" 2>"$T/client.err") &
  track $!
  wait_upto $! 30 || code=$?
  wait_upto "$SERVER_PID" 30 || server=$?
  [ "$code" = 1 ] && [ "$server" = 1 ] && grep -q "writing stdout" "$T/client.err" ||
    { fail "client=$code server=$server: $(cat "$T/client.err")"; return; }
  pass "both exit 1"
}

# A reader that goes away (`| head`) is a normal, quiet end.
check_closed_reader() {
  head -c 5000000 /dev/urandom >"$T/down"
  start_server "$T/down" /dev/null "$T/server.err" || { fail "no ticket"; return; }
  mkfifo "$T/client.pipe"
  local client=0
  "$BIN" "$TICKET" </dev/null 2>"$T/client.err" >"$T/client.pipe" &
  local pid=$!
  track $pid
  local got
  # The reader takes 1000 bytes and leaves; the client then hits a closed pipe.
  got=$(head -c 1000 <"$T/client.pipe" | wc -c | tr -d ' ')
  wait_upto $pid 30 || client=$?
  local server=0
  wait_upto "$SERVER_PID" 30 || server=$?
  [ "$got" = 1000 ] && [ "$client" = 0 ] && [ "$server" = 0 ] ||
    { fail "got=$got client=$client server=$server: $(cat "$T/client.err")"; return; }
  pass "both exit 0"
}

# Runs a command with its stderr (only) on a fresh pseudo-terminal of the
# given size, its controlling terminal, copying what it draws there to a
# file; exits with the command's status. stdin and stdout pass through.
#   python3 -c "$PTY_RUN" <capture> <cols> <rows> <cmd> [args...]
PTY_RUN='
import fcntl, os, struct, sys, termios
out, cols, rows, cmd = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4:]
master, slave = os.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
pid = os.fork()
if pid == 0:
    os.close(master)
    os.setsid()
    fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
    os.dup2(slave, 2)
    os.close(slave)
    os.execvp(cmd[0], cmd)
os.close(slave)
with open(out, "wb") as f:
    while True:
        try:
            data = os.read(master, 65536)
        except OSError:
            break
        if not data:
            break
        f.write(data)
_, status = os.waitpid(pid, 0)
sys.exit(os.waitstatus_to_exitcode(status) % 256)
'

# With stderr on a terminal and stdout redirected, the client shows the
# status panel; it draws on stderr only, so the data on stdout is intact.
check_panel_stdout() {
  head -c 5000000 /dev/urandom >"$T/up"
  # No ESC bytes in the data, so any on the client's stdout are the panel's.
  head -c 2000000 /dev/urandom | LC_ALL=C tr -d '\033' >"$T/down"
  start_server "$T/down" "$T/server.out" "$T/server.err" || { fail "no ticket"; return; }
  local client=0 server=0
  TERM=xterm-256color python3 -c "$PTY_RUN" "$T/client.pty" 100 30 "$BIN" -v "$TICKET" \
    <"$T/up" >"$T/client.out" &
  track $!
  wait_upto $! 120 || client=$?
  wait_upto "$SERVER_PID" 30 || server=$?
  cmp -s "$T/up" "$T/server.out" || { fail "upload differs"; return; }
  cmp -s "$T/down" "$T/client.out" || { fail "download differs"; return; }
  [ "$client$server" = 00 ] || { fail "exit codes client=$client server=$server"; return; }
  LC_ALL=C grep -q $'\033' "$T/client.out" && { fail "escape sequences on stdout"; return; }
  grep -q 'recv' "$T/client.pty" && grep -q '# done: ' "$T/client.pty" ||
    { fail "no panel on the terminal: $(LC_ALL=C tr -cd '[:print:]\n' <"$T/client.pty" | tail -5)"; return; }
  pass "5 MB up, 2 MB down with the panel on stderr"
}

# --- minipaw serve ---------------------------------------------------------

# A local TCP target for serve, one thread per connection. It notes
# `accepted <n>` for each connection in its log, and what it saw. Modes:
#   echo    sends back everything, closing once the client's side ends
#   count   reads to EOF, then replies with the byte count and closes
#   banner  sends a banner and shuts down its write side at once, then
#           reads to EOF and notes `received <bytes>`
#   stall   never reads its first connection (notes `stalled`); echoes
#           the others
#   late    reads to EOF, then replies `late reply` two seconds later and
#           closes
#   python3 -c "$TARGET_PY" <mode> <log>   (prints its port on stdout)
TARGET_PY='
import socket, sys, threading
mode, log = sys.argv[1], sys.argv[2]
lock = threading.Lock()
def note(line):
    with lock, open(log, "a") as f:
        f.write(line + "\n")
def drain(c):
    n = 0
    while True:
        data = c.recv(65536)
        if not data:
            return n
        n += len(data)
def serve(c, i):
    note("accepted %d" % i)
    try:
        if mode == "echo" or (mode == "stall" and i > 1):
            while True:
                data = c.recv(65536)
                if not data:
                    break
                c.sendall(data)
        elif mode == "count":
            c.sendall(b"%d\n" % drain(c))
        elif mode == "banner":
            c.sendall(b"hello from the target\n")
            c.shutdown(socket.SHUT_WR)
            note("received %d" % drain(c))
        elif mode == "stall":
            note("stalled")
            threading.Event().wait()
        elif mode == "late":
            drain(c)
            threading.Event().wait(2)
            c.sendall(b"late reply\n")
    except OSError as e:
        note("error %d: %s" % (i, e))
    c.close()
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", 0))
srv.listen(64)
print(srv.getsockname()[1], flush=True)
n = 0
while True:
    c, _ = srv.accept()
    n += 1
    threading.Thread(target=serve, args=(c, n), daemon=True).start()
'

# Polls a command every 0.1 s until it succeeds, for at most <secs>.
wait_until() { # wait_until <secs> <cmd> [args...]
  local deadline=$(($(date +%s) + $1))
  shift
  until "$@"; do
    [ "$(date +%s)" -lt "$deadline" ] || return 1
    sleep 0.1
  done
}

# Whether <file> holds at least <bytes>.
has_bytes() { [ -f "$1" ] && [ $(($(wc -c <"$1"))) -ge "$2" ]; }

# Seconds since <start> (from `now`), to a tenth.
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }

# Whether an awk condition holds, for comparing fractional numbers.
holds() { awk "BEGIN { exit !($1) }"; }

# How many lines of <file> match <regex>.
count() { grep -c -- "$2" "$1" 2>/dev/null; }

# Whether <file> has at least <n> lines matching <regex>.
has_lines() { [ "$(count "$1" "$2")" -ge "$3" ]; }

# Starts a target (see TARGET_PY) and sets TARGET_PORT.
start_target() { # start_target <mode>
  python3 -c "$TARGET_PY" "$1" "$T/target.log" >"$T/target.port" &
  track $!
  wait_until 10 test -s "$T/target.port" || return 1
  TARGET_PORT=$(cat "$T/target.port")
}

# Starts `minipaw -v serve` with its identity in $T/id, a private directory,
# and waits for its ticket. Sets SERVER_PID and TICKET, and DIRECT to the
# address its clients can dial it at directly, alongside the relay, so big
# transfers stay fast through a slow hosted relay too.
start_serve() { # start_serve <stderr> [serve args...]
  local err=$1
  shift
  [ -d "$T/id" ] || mkdir -m 700 "$T/id"
  "$BIN" -v serve --relay "$RELAY" --identity "$T/id/serve.key" "$@" </dev/null >/dev/null 2>"$err" &
  SERVER_PID=$!
  track "$SERVER_PID"
  wait_ticket "$err" || return 1
  local port
  port=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/\([0-9]*\)/.*#\1#p' "$err" | head -1)
  DIRECT=/ip4/127.0.0.1/udp/$port/quic-v1
}

# Starts a client of TICKET in the background reading <stdin>, writing
# $T/<name>.out and $T/<name>.err. With `direct`, it also dials DIRECT.
# Sets CLIENT_PID.
client() { # client <name> <stdin> [direct]
  local name=$1 in=$2
  if [ "${3:-}" = direct ]; then
    MINIPAW_DIRECT=$DIRECT "$BIN" -v "$TICKET" <"$in" >"$T/$name.out" 2>"$T/$name.err" &
  else
    "$BIN" -v "$TICKET" <"$in" >"$T/$name.out" 2>"$T/$name.err" &
  fi
  CLIENT_PID=$!
  track $CLIENT_PID
}

# Makes <fifo> an input that sends <first>, then, once <fifo>.go exists,
# <rest> and its end; so a transfer is known to be under way meanwhile.
feed() { # feed <fifo> <first> <rest>
  mkfifo "$1"
  feeder "$@" >"$1" &
  track $!
}
feeder() {
  cat "$2"
  for _ in $(seq 1 600); do
    [ -e "$1.go" ] && break
    sleep 0.1
  done
  cat "$3"
}

# Makes <fifo> an input that sends <line> and then stays open.
held() { # held <fifo> <line>
  mkfifo "$1"
  (echo "$2"; exec sleep 60) >"$1" &
  track $!
}

# Waits for a client and checks it exited <code>, else fails the check.
expect_exit() { # expect_exit <name> <pid> <code> <secs>
  local code=0
  wait_upto "$2" "$4" || code=$?
  [ "$code" = "$3" ] && return 0
  fail "$1 exited $code, not $3: $(tail -2 "$T/$1.err" | tr '\n' ' ')"
  return 1
}

# A session line in serve's log: `# [N] … connected (…)`, `# [N] ended: …`.
CONNECTED='^# \[[0-9]*\] .* connected ('
ENDED_OK='^# \[[0-9]*\] ended: .* sent, .* received in '

check_serve_forward() {
  start_target echo || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  local i pids=()
  for i in 1 2 3; do
    head -c 1000000 /dev/urandom >"$T/up$i.a"
    head -c 1000000 /dev/urandom >"$T/up$i.b"
    feed "$T/c$i.in" "$T/up$i.a" "$T/up$i.b"
    client "c$i" "$T/c$i.in" direct
    pids+=("$CLIENT_PID")
  done
  # All three sessions are open before any of them finishes.
  wait_until 30 has_lines "$T/serve.err" "$CONNECTED" 3 ||
    { fail "not all 3 sessions opened: $(count "$T/serve.err" "$CONNECTED")"; return; }
  for i in 1 2 3; do touch "$T/c$i.in.go"; done
  for i in 1 2 3; do
    expect_exit "c$i" "${pids[i - 1]}" 0 60 || return
    cat "$T/up$i.a" "$T/up$i.b" | cmp -s - "$T/c$i.out" ||
      { fail "client $i got different data back"; return; }
  done
  wait_until 10 has_lines "$T/serve.err" "$ENDED_OK" 3 ||
    { fail "$(count "$T/serve.err" "$ENDED_OK") sessions ended cleanly, not 3"; return; }
  kill -0 "$SERVER_PID" 2>/dev/null || { fail "serve exited"; return; }
  pass "3 concurrent sessions echoed 2 MB each; serve still up"
}

check_serve_halfclose() {
  start_target count || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  head -c 3000000 /dev/urandom >"$T/up"
  client c "$T/up" direct
  expect_exit c "$CLIENT_PID" 0 60 || return
  [ "$(cat "$T/c.out")" = 3000000 ] || { fail "the target counted '$(head -c 100 "$T/c.out")'"; return; }
  pass "the target saw the client's EOF and answered 3000000"
}

check_serve_reverse_halfclose() {
  start_target banner || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  head -c 1500000 /dev/urandom >"$T/up.a"
  head -c 1500000 /dev/urandom >"$T/up.b"
  feed "$T/c.in" "$T/up.a" "$T/up.b"
  client c "$T/c.in" direct
  # The banner and the target's EOF arrive while the client still sends.
  wait_until 30 grep -q 'hello from the target' "$T/c.out" || { fail "no banner"; return; }
  touch "$T/c.in.go"
  expect_exit c "$CLIENT_PID" 0 60 || return
  [ "$(cat "$T/c.out")" = "hello from the target" ] || { fail "client got more than the banner"; return; }
  wait_until 10 grep -qs '^received 3000000$' "$T/target.log" ||
    { fail "target: $(grep received "$T/target.log")"; return; }
  wait_until 10 grep -q "$ENDED_OK" "$T/serve.err" || { fail "session did not end cleanly"; return; }
  pass "banner out, then 3 MB in after the target's EOF"
}

check_serve_restart() {
  start_target echo || { fail "no target"; return; }
  start_serve "$T/serve1.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  local first=$TICKET
  kill "$SERVER_PID"
  wait_upto "$SERVER_PID" 10
  start_serve "$T/serve2.err" --forward "$TARGET_PORT" || { fail "no ticket after a restart"; return; }
  [ "$TICKET" = "$first" ] || { fail "the ticket changed across a restart"; return; }
  grep -q '^# identity: .* (existing)$' "$T/serve2.err" || { fail "identity not reused"; return; }
  head -c 300000 /dev/urandom >"$T/up1"
  client c1 "$T/up1"
  expect_exit c1 "$CLIENT_PID" 0 60 || return
  cmp -s "$T/up1" "$T/c1.out" || { fail "the old ticket's session got different data"; return; }

  # Killed and restarted mid-transfer, the new server does not know the
  # session: the client is refused when it resumes, and never reaches the
  # target again.
  head -c 500000 /dev/urandom >"$T/up2"
  feed "$T/c2.in" "$T/up2" "$T/up2"
  client c2 "$T/c2.in"
  local c2=$CLIENT_PID
  wait_until 30 has_bytes "$T/c2.out" 500000 || { fail "transfer did not start"; return; }
  local accepted
  accepted=$(count "$T/target.log" '^accepted')
  kill "$SERVER_PID"
  wait_upto "$SERVER_PID" 10
  start_serve "$T/serve3.err" --forward "$TARGET_PORT" || { fail "no ticket after a restart"; return; }
  expect_exit c2 "$c2" 1 60 || return
  grep -q 'session ended' "$T/c2.err" || { fail "client: $(tail -1 "$T/c2.err")"; return; }
  grep -q '^# refused .*: session ended' "$T/serve3.err" || { fail "the new server did not refuse it"; return; }
  [ "$(count "$T/target.log" '^accepted')" = "$accepted" ] || { fail "the target got a new connection"; return; }
  pass "same ticket after a restart; a session cut by one is refused"
}

# The server gives up on a session whose client went silent (the link
# dropped, the resume timeout shortened by test hooks), so the client
# resuming later is refused.
check_serve_late_resume() {
  start_target echo || { fail "no target"; return; }
  MINIPAW_TEST_DROP_LINK_AFTER=300000 MINIPAW_TEST_RESUME_TIMEOUT=1 \
    start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  head -c 1000000 /dev/urandom >"$T/up"
  feed "$T/c.in" "$T/up" "$T/up"
  client c "$T/c.in"
  expect_exit c "$CLIENT_PID" 1 60 || return
  grep -q 'test hook: dropping the link' "$T/serve.err" || { fail "the link was never dropped"; return; }
  grep -q '^# \[1\] ended: client disconnected and did not come back' "$T/serve.err" ||
    { fail "the server did not give up on the session"; return; }
  grep -q '^# refused .*: session ended' "$T/serve.err" || { fail "the resume was not refused"; return; }
  grep -q 'session ended' "$T/c.err" || { fail "client: $(tail -1 "$T/c.err")"; return; }
  pass "a resume after the server ended the session is refused"
}

check_serve_limit() {
  start_target echo || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" --max-sessions 1 || { fail "no ticket"; return; }
  head -c 1000000 /dev/urandom >"$T/up.a"
  head -c 1000000 /dev/urandom >"$T/up.b"
  feed "$T/first.in" "$T/up.a" "$T/up.b"
  client first "$T/first.in" direct
  local first=$CLIENT_PID
  wait_until 30 has_bytes "$T/first.out" 1000000 || { fail "first transfer did not start"; return; }
  echo hi >"$T/hi"
  client second "$T/hi"
  expect_exit second "$CLIENT_PID" 1 30 || return
  grep -q busy "$T/second.err" || { fail "second: $(tail -1 "$T/second.err")"; return; }
  grep -q '^# refused .*: busy (1 sessions)$' "$T/serve.err" || { fail "no busy refusal logged"; return; }
  touch "$T/first.in.go"
  expect_exit first "$first" 0 60 || return
  cat "$T/up.a" "$T/up.b" | cmp -s - "$T/first.out" || { fail "first session's data differs"; return; }
  pass "second client busy; first echoed 2 MB"
}

check_serve_target_down() {
  local port
  port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
  start_serve "$T/serve.err" --forward "$port" || { fail "no ticket"; return; }
  echo hi >"$T/hi"
  local i
  for i in 1 2; do
    client "c$i" "$T/hi"
    expect_exit "c$i" "$CLIENT_PID" 1 30 || return
    grep -q 'server refused: connection refused' "$T/c$i.err" || { fail "client: $(tail -1 "$T/c$i.err")"; return; }
  done
  [ "$(count "$T/serve.err" '^# refused .*: connection refused$')" = 2 ] || { fail "refusals not logged"; return; }
  kill -0 "$SERVER_PID" 2>/dev/null || { fail "serve exited"; return; }
  pass "refused twice; serve still up"
}

# One session's target never reads; another session is unaffected, and a
# stop still ends promptly.
check_serve_isolation() {
  start_target stall || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  mkfifo "$T/stuck.in"
  (head -c 30000000 /dev/zero; exec sleep 60) >"$T/stuck.in" &
  track $!
  client stuck "$T/stuck.in" direct
  local stuck=$CLIENT_PID
  wait_until 30 grep -qs '^stalled' "$T/target.log" || { fail "the stalled session never connected"; return; }
  head -c 5000000 /dev/urandom >"$T/up"
  client fine "$T/up" direct
  expect_exit fine "$CLIENT_PID" 0 60 || return
  cmp -s "$T/up" "$T/fine.out" || { fail "the other session's data differs"; return; }
  kill -0 "$stuck" 2>/dev/null && [ "$(count "$T/serve.err" '^# \[[0-9]*\] ended')" = 1 ] ||
    { fail "the stalled session did not stay open"; return; }
  local start code=0
  start=$(now)
  kill -INT "$SERVER_PID"
  wait_upto "$SERVER_PID" 15 || code=$?
  local took
  took=$(since "$start")
  [ "$code" = 0 ] || { fail "serve exited $code"; return; }
  holds "$took <= 5" || { fail "stopping took ${took}s"; return; }
  pass "5 MB echoed past a stalled session; stopped in ${took}s"
}

check_serve_mixed() {
  start_target echo || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  head -c 2000000 /dev/urandom >"$T/up.a"
  head -c 2000000 /dev/urandom >"$T/up.b"
  feed "$T/long.in" "$T/up.a" "$T/up.b"
  client long "$T/long.in" direct
  local long=$CLIENT_PID
  wait_until 30 has_bytes "$T/long.out" 1000000 || { fail "the long transfer did not start"; return; }
  head -c 100000 /dev/urandom >"$T/short.up"
  client short "$T/short.up" direct
  expect_exit short "$CLIENT_PID" 0 30 || return
  cmp -s "$T/short.up" "$T/short.out" || { fail "the short session's data differs"; return; }
  wait_until 10 has_lines "$T/serve.err" "$ENDED_OK" 1 || { fail "the short session did not end cleanly"; return; }
  kill -0 "$long" 2>/dev/null && [ "$(count "$T/serve.err" '^# \[[0-9]*\] ended')" = 1 ] ||
    { fail "the long session ended with the short one"; return; }
  touch "$T/long.in.go"
  expect_exit long "$long" 0 60 || return
  cat "$T/up.a" "$T/up.b" | cmp -s - "$T/long.out" || { fail "the long session's data differs"; return; }
  pass "a short session came and went during a 4 MB one"
}

# A stop while a session waits for its target (held there by a test hook).
check_serve_stop_connecting() {
  start_target echo || { fail "no target"; return; }
  MINIPAW_TEST_CONNECT_DELAY=5 start_serve "$T/serve.err" --forward "$TARGET_PORT" ||
    { fail "no ticket"; return; }
  held "$T/c.in" hi
  client c "$T/c.in"
  wait_until 30 grep -q 'test hook: waiting' "$T/serve.err" || { fail "never connecting"; return; }
  local start code=0
  start=$(now)
  kill -INT "$SERVER_PID"
  wait_upto "$SERVER_PID" 15 || code=$?
  local took
  took=$(since "$start")
  [ "$code" = 0 ] || { fail "serve exited $code"; return; }
  holds "$took <= 4" || { fail "stopping took ${took}s"; return; }
  expect_exit c "$CLIENT_PID" 1 10 || return
  grep -q interrupted "$T/c.err" || { fail "client: $(tail -1 "$T/c.err")"; return; }
  grep -q "$CONNECTED" "$T/serve.err" && { fail "the session was reported connected"; return; }
  pass "stopped in ${took}s while connecting; client told"
}

# One peer that keeps opening streams without a Hello is disconnected at
# its admission deadline (15 s), however many fresh streams it opens, while
# a real client gets in meanwhile.
check_serve_auth_deadline() {
  start_target echo || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  local peer
  peer=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/[0-9]*/quic-v1/p2p/\(.*\)#\1#p' "$T/serve.err" | head -1)
  "$SQUAT" --churn "$DIRECT/p2p/$peer" 40 2>"$T/squat.err" &
  local squat=$!
  track $squat
  wait_until 10 grep -q 'churning' "$T/squat.err" || { fail "the squatter never connected"; return; }
  head -c 200000 /dev/urandom >"$T/up"
  client c "$T/up" direct
  expect_exit c "$CLIENT_PID" 0 30 || return
  cmp -s "$T/up" "$T/c.out" || { fail "the client's data differs"; return; }
  local code=0
  wait_upto "$squat" 40 || code=$?
  [ "$code" = 0 ] || { fail "squatter: $(tail -1 "$T/squat.err")"; return; }
  local after opened
  after=$(sed -n 's/^squat: disconnected after \([0-9.]*\)s, .*/\1/p' "$T/squat.err")
  opened=$(sed -n 's/^squat: disconnected after .*, \([0-9]*\) streams opened/\1/p' "$T/squat.err")
  holds "$after >= 13 && $after <= 20" || { fail "disconnected after ${after}s"; return; }
  [ "$opened" -ge 40 ] || { fail "only $opened streams opened"; return; }
  pass "squatter out after ${after}s and $opened streams; client served"
}

# serve's own peer address, for squat to dial directly.
serve_addr() { # serve_addr <serve.err>
  echo "$DIRECT/p2p/$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/[0-9]*/quic-v1/p2p/\(.*\)#\1#p' "$1" | head -1)"
}

# A client that floods its stream before its Welcome (while its session
# waits for the target, held there by a test hook) is refused at once,
# with nothing kept, while another session transfers normally.
check_serve_flood() {
  start_target echo || { fail "no target"; return; }
  MINIPAW_TEST_CONNECT_DELAY=3 start_serve "$T/serve.err" --forward "$TARGET_PORT" ||
    { fail "no ticket"; return; }
  head -c 1000000 /dev/urandom >"$T/up.a"
  head -c 1000000 /dev/urandom >"$T/up.b"
  feed "$T/c.in" "$T/up.a" "$T/up.b"
  client c "$T/c.in" direct
  local c=$CLIENT_PID
  wait_until 30 has_bytes "$T/c.out" 1000000 || { fail "the transfer did not start"; return; }
  local code=0
  "$SQUAT" --flood "$(serve_addr "$T/serve.err")" "$TICKET" 20 2>"$T/squat.err" || code=$?
  [ "$code" = 0 ] || { fail "flood: $(tail -1 "$T/squat.err")"; return; }
  grep -q '^# refused .*: unexpected frame before Welcome$' "$T/serve.err" ||
    { fail "the flood was not refused"; return; }
  touch "$T/c.in.go"
  expect_exit c "$c" 0 60 || return
  cat "$T/up.a" "$T/up.b" | cmp -s - "$T/c.out" || { fail "the other session's data differs"; return; }
  pass "$(sed -n 's/^squat: //p' "$T/squat.err"); 2 MB echoed meanwhile"
}

# A peer whose session outlived its admission deadline, and that opens a
# stream without a Hello before the session ends, is still disconnected
# once it is left with no session: it is admitted afresh.
check_serve_leave() {
  start_target echo || { fail "no target"; return; }
  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  local code=0
  "$SQUAT" --leave "$(serve_addr "$T/serve.err")" "$TICKET" 17 40 2>"$T/squat.err" || code=$?
  [ "$code" = 0 ] || { fail "squat: $(tail -1 "$T/squat.err")"; return; }
  grep -q '^# \[1\] ended' "$T/serve.err" || { fail "the session was not reported ended"; return; }
  local after
  after=$(sed -n 's/^squat: disconnected \([0-9.]*\)s after its session/\1/p' "$T/squat.err")
  holds "$after >= 10 && $after <= 20" || { fail "disconnected ${after}s after its session"; return; }
  pass "disconnected ${after}s after its session ended"
}

# Inflight `accept` calls (from the -v log): the most at once, and now.
inflight() { # inflight <serve.err> -> "<max> <now>"
  awk '/test hook: waiting/ { n++; if (n > max) max = n }
    /# target: connected to / || /# target: connecting to .* for [^ ]*: / { n-- }
    END { print max + 0, n + 0 }' "$1"
}
none_inflight() { [ "$(inflight "$1" | cut -d' ' -f2)" = 0 ]; }

# Clients that come and go while their sessions connect (held there by a
# test hook) never take serve past its limit, and leave no slot behind.
check_serve_churn() {
  start_target echo || { fail "no target"; return; }
  MINIPAW_TEST_CONNECT_DELAY=3 start_serve "$T/serve.err" --forward "$TARGET_PORT" --max-sessions 4 ||
    { fail "no ticket"; return; }
  # Direct dials, since a relay allows only so many circuits to one peer.
  local wave i pids
  for wave in 1 2 3; do
    pids=()
    for i in 1 2 3 4 5 6; do
      held "$T/w$wave-$i.in" hi
      client "w$wave-$i" "$T/w$wave-$i.in" direct
      pids+=("$CLIENT_PID")
    done
    # Four take a slot; the other two are refused as busy.
    wait_until 20 has_lines "$T/serve.err" 'test hook: waiting' $((4 * wave)) ||
      { fail "wave $wave: $(count "$T/serve.err" 'test hook: waiting') connecting in all"; return; }
    wait_until 20 has_lines "$T/serve.err" '^# refused .*: busy (4 sessions)$' $((2 * wave)) ||
      { fail "wave $wave: $(count "$T/serve.err" 'busy (4 sessions)') busy in all"; return; }
    kill -INT "${pids[@]}" 2>/dev/null
    for i in "${pids[@]}"; do wait_upto "$i" 10; done
    wait_until 20 none_inflight "$T/serve.err" || { fail "wave $wave: connects never finished"; return; }
  done
  local max
  max=$(inflight "$T/serve.err" | cut -d' ' -f1)
  [ "$max" = 4 ] || { fail "$max connecting at once"; return; }
  head -c 200000 /dev/urandom >"$T/up"
  client real "$T/up" direct
  expect_exit real "$CLIENT_PID" 0 30 || return
  cmp -s "$T/up" "$T/real.out" || { fail "the real client's data differs"; return; }
  pass "18 clients came and went, at most $max connecting; then one served"
}

# -q keeps stderr silent, even with -v and stderr on a terminal.
check_quiet_stderr() {
  local flags
  for flags in -q "-q -v"; do
    rm -f "$T"/*.out "$T/client.pty"
    head -c 1000000 /dev/urandom >"$T/up"
    head -c 1000000 /dev/urandom >"$T/down"
    start_server "$T/down" "$T/server.out" "$T/server.err" || { fail "no ticket"; return; }
    local client=0 server=0
    # shellcheck disable=SC2086 # flags is a word list
    TERM=xterm-256color python3 -c "$PTY_RUN" "$T/client.pty" 100 30 "$BIN" $flags "$TICKET" \
      <"$T/up" >"$T/client.out" &
    track $!
    wait_upto $! 60 || client=$?
    wait_upto "$SERVER_PID" 30 || server=$?
    [ "$client$server" = 00 ] || { fail "$flags: exit codes client=$client server=$server"; return; }
    cmp -s "$T/up" "$T/server.out" || { fail "$flags: upload differs"; return; }
    cmp -s "$T/down" "$T/client.out" || { fail "$flags: download differs"; return; }
    [ -s "$T/client.pty" ] &&
      { fail "$flags wrote to stderr: $(LC_ALL=C tr -cd '[:print:]\n' <"$T/client.pty" | head -3)"; return; }
  done
  pass "-q and -q -v: nothing on the terminal, data intact"
}

# SIGHUP to `-q`, as ssh sends its ProxyCommand when it exits: a client
# still connecting stops, telling the server, and one whose session is up
# with stdin open stops too; one whose stdin ended finishes its session.
check_quiet_hangup() {
  start_target late || { fail "no target"; return; }
  MINIPAW_TEST_CONNECT_DELAY=4 start_serve "$T/serve1.err" --forward "$TARGET_PORT" ||
    { fail "no ticket"; return; }
  "$BIN" -q "$TICKET" </dev/null >"$T/early.out" 2>"$T/early.err" &
  local pid=$! start
  track $pid
  wait_until 30 grep -q 'test hook: waiting' "$T/serve1.err" || { fail "never connecting"; return; }
  start=$(now)
  kill -HUP $pid
  expect_exit early $pid 130 10 || return
  local took
  took=$(since "$start")
  holds "$took <= 3" || { fail "stopping while connecting took ${took}s"; return; }
  wait_until 10 grep -q 'the client stopped before its Welcome' "$T/serve1.err" ||
    { fail "the server was not told"; return; }
  wait_until 10 grep -q '# target: connected to ' "$T/serve1.err"
  grep -q "$CONNECTED" "$T/serve1.err" && { fail "the session was reported connected"; return; }
  kill "$SERVER_PID"
  wait_upto "$SERVER_PID" 10

  start_serve "$T/serve.err" --forward "$TARGET_PORT" || { fail "no ticket"; return; }
  held "$T/open.in" hi
  "$BIN" -q "$TICKET" <"$T/open.in" >"$T/open.out" 2>"$T/open.err" &
  pid=$!
  track $pid
  wait_until 30 has_lines "$T/serve.err" "$CONNECTED" 1 || { fail "the session never opened"; return; }
  sleep 0.5
  kill -HUP $pid
  expect_exit open $pid 130 10 || return
  wait_until 5 has_lines "$T/serve.err" '^# \[[0-9]*\] ended' 1 || { fail "serve still holds the session"; return; }

  echo bye >"$T/bye"
  "$BIN" -q "$TICKET" <"$T/bye" >"$T/done.out" 2>"$T/done.err" &
  pid=$!
  track $pid
  wait_until 30 has_lines "$T/serve.err" "$CONNECTED" 2 || { fail "the last session never opened"; return; }
  sleep 0.5
  kill -HUP $pid
  expect_exit done $pid 0 30 || return
  [ "$(cat "$T/done.out")" = "late reply" ] || { fail "got '$(head -c 100 "$T/done.out")'"; return; }
  wait_until 5 has_lines "$T/serve.err" "$ENDED_OK" 1 || { fail "the last session did not end cleanly"; return; }
  pass "stopped in ${took}s while connecting; stopped when up; finished after stdin's end"
}

ALL="transfer forced_relay resume heartbeat interrupt_client interrupt_server interrupt_blocked
interrupt_both wrong_token busy squatters stdin_error stdout_error closed_reader panel_stdout
serve_forward serve_halfclose serve_reverse_halfclose serve_restart serve_late_resume serve_limit
serve_target_down serve_isolation serve_mixed serve_stop_connecting serve_auth_deadline serve_churn
serve_flood serve_leave quiet_stderr quiet_hangup"
if [ "${1:-}" = list ]; then
  echo $ALL
  exit 0
fi
for CHECK in ${@:-$ALL}; do
  if ! declare -F "check_$CHECK" >/dev/null; then
    echo "unknown check '$CHECK' (try: scripts/check.sh list)" >&2
    exit 2
  fi
  rm -rf "${T:?}"/*
  RESULT=
  "check_$CHECK"
  [ -n "$RESULT" ] || fail "ended without a result"
  reap_all
done
[ ${#FAILED[@]} -eq 0 ] || { echo "failed: ${FAILED[*]}"; exit 1; }
