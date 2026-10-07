#!/usr/bin/env bash
# End-to-end behaviour checks against a local relay.
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
cargo build -q --release --bin minipaw --example squat || exit 2
SQUAT=target/release/examples/squat

T=$(mktemp -d)
CHILDREN=()
cleanup() {
  for pid in "${CHILDREN[@]:-}"; do [ -n "$pid" ] && kill "$pid" 2>/dev/null; done
  stop_relay
  [ -n "${LIMITED_PID:-}" ] && kill "$LIMITED_PID" 2>/dev/null
  rm -rf "$T"
}
trap cleanup EXIT
ensure_relay

FAILED=()
pass() { printf 'PASS  %-18s %s\n' "$CHECK" "$*"; }
fail() {
  printf 'FAIL  %-18s %s\n' "$CHECK" "$*"
  FAILED+=("$CHECK")
  if [ -n "${SHOW_LOGS:-}" ]; then
    for f in "$T"/*.err; do echo "--- $f"; tail -20 "$f"; done
  fi
}
elapsed() { echo $(($(date +%s) - $1)); }

# A transfer both ways; data must arrive byte for byte and both exit 0.
transfer() { # transfer <up-bytes> <down-bytes>
  head -c "$1" /dev/urandom >"$T/up"
  head -c "$2" /dev/urandom >"$T/down"
  start_server "$T/down" "$T/server.out" "$T/server.err" || return 1
  local client=0 server=0
  "$BIN" -v "$TICKET" <"$T/up" >"$T/client.out" 2>"$T/client.err" &
  wait_upto $! 120 || client=$?
  wait_upto "$SERVER_PID" 30 || server=$?
  cmp -s "$T/up" "$T/server.out" || { fail "upload differs"; return; }
  cmp -s "$T/down" "$T/client.out" || { fail "download differs"; return; }
  [ "$client$server" = 00 ] || { fail "exit codes client=$client server=$server"; return; }
  pass "$(($1 / 1000000)) MB up, $(($2 / 1000000)) MB down"
}

check_transfer() { transfer 30000000 10000000; }

check_forced_relay() {
  MINIPAW_FORCE_RELAY=1 transfer 5000000 5000000
}

# A relay with default circuit limits (128 KiB per direction) cuts the
# circuit every few hundred KB; the session must resume each time.
check_resume() {
  if [ -z "${LIMITED:-}" ]; then
    local saved=$RELAY saved_pid=${RELAY_PID:-} saved_log=${RELAY_LOG:-}
    RELAY= RELAY_PID=
    ensure_relay --circuit-peer-rate off --circuit-ip-rate off
    LIMITED=$RELAY LIMITED_PID=$RELAY_PID LIMITED_LOG=$RELAY_LOG
    RELAY=$saved RELAY_PID=$saved_pid RELAY_LOG=$saved_log
  fi
  RELAY=$LIMITED MINIPAW_FORCE_RELAY=1 transfer 2000000 500000
  local circuits
  circuits=$(grep -c 'circuit opened' "$LIMITED_LOG")
  [ "$circuits" -gt 5 ] || fail "only $circuits circuits: the relay never cut the session"
}

# Ctrl-C on one side ends both: it exits 130, the peer exits 1, promptly.
interrupt() { # interrupt <client|server|blocked>
  local who=$1 server_in=$T/server.in
  if [ "$who" = blocked ]; then
    # The server floods a client whose stdout reader never reads.
    mkfifo "$server_in"
    (echo from-server; head -c 50000000 /dev/zero; sleep 60) >"$server_in" &
  else
    mkfifo "$server_in"
    (echo from-server; sleep 60) >"$server_in" &
  fi
  CHILDREN+=($!)
  start_server "$server_in" "$T/server.out" "$T/server.err" || { fail "no ticket"; return; }
  local client_out=$T/client.out
  if [ "$who" = blocked ]; then
    mkfifo "$T/stuck"
    sleep 120 <"$T/stuck" &
    CHILDREN+=($!)
    client_out=$T/stuck
  fi
  mkfifo "$T/client.in"
  (echo from-client; sleep 60) >"$T/client.in" &
  CHILDREN+=($!)
  "$BIN" -v "$TICKET" <"$T/client.in" >"$client_out" 2>"$T/client.err" &
  local client=$!
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
  local victim_code=0 peer_code=0
  wait_upto "$victim" 10 || victim_code=$?
  wait_upto "$peer" 10 || peer_code=$?
  local took
  took=$(elapsed "$start")
  [ "$victim_code" = 130 ] && [ "$peer_code" = 1 ] ||
    { fail "exit codes interrupted=$victim_code peer=$peer_code"; return; }
  [ "$took" -le 5 ] || { fail "took ${took}s to end both sides"; return; }
  grep -q from-client "$T/server.out" || { fail "server lost the client's data"; return; }
  pass "both ended in ${took}s"
}

check_interrupt_client() { interrupt client; }
check_interrupt_server() { interrupt server; }
check_interrupt_blocked() { interrupt blocked; }

# A wrong token is refused at once.
check_wrong_token() {
  mkfifo "$T/server.in"
  sleep 30 >"$T/server.in" &
  CHILDREN+=($!)
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
  CHILDREN+=($!)
  start_server "$T/server.in" /dev/null "$T/server.err" || { fail "no ticket"; return; }
  mkfifo "$T/first.in"
  sleep 30 >"$T/first.in" &
  CHILDREN+=($!)
  "$BIN" "$TICKET" <"$T/first.in" >/dev/null 2>"$T/first.err" &
  CHILDREN+=($!)
  for _ in $(seq 1 100); do
    grep -q "connection from" "$T/server.err" && break
    sleep 0.1
  done
  local code=0
  echo hi | "$BIN" "$TICKET" >/dev/null 2>"$T/client.err" &
  wait_upto $! 30 || code=$?
  kill "$SERVER_PID" 2>/dev/null
  [ "$code" = 1 ] && grep -q "busy" "$T/client.err" ||
    { fail "exit=$code: $(cat "$T/client.err")"; return; }
  pass "second client refused"
}

# Streams that never say Hello must not lock out a real client.
check_squatters() {
  mkfifo "$T/server.in"
  (echo from-server; sleep 40) >"$T/server.in" &
  CHILDREN+=($!)
  start_server "$T/server.in" "$T/server.out" "$T/server.err" || { fail "no ticket"; return; }
  local port peer
  port=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/\([0-9]*\)/.*#\1#p' "$T/server.err" | head -1)
  peer=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/[0-9]*/quic-v1/p2p/\(.*\)#\1#p' "$T/server.err" | head -1)
  "$SQUAT" "/ip4/127.0.0.1/udp/$port/quic-v1/p2p/$peer" 16 30 2>"$T/squat.err" &
  CHILDREN+=($!)
  for _ in $(seq 1 100); do
    grep -q "streams held" "$T/squat.err" && break
    sleep 0.1
  done
  grep -q "streams held" "$T/squat.err" || { fail "squatter could not open its streams"; return; }
  local code=0
  "$BIN" "$TICKET" < <(echo from-client) >"$T/client.out" 2>"$T/client.err" &
  local client=$!
  for _ in $(seq 1 100); do
    grep -q from-server "$T/client.out" && break
    sleep 0.1
  done
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
  CHILDREN+=($!)
  start_server "$T/server.in" /dev/null "$T/server.err" || { fail "no ticket"; return; }
  local code=0
  "$BIN" "$TICKET" </ >/dev/null 2>"$T/client.err" &
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
  local got
  got=$("$BIN" "$TICKET" </dev/null 2>"$T/client.err" | head -c 1000 | wc -c | tr -d ' ')
  local codes=("${PIPESTATUS[@]}")
  local server=0
  wait_upto "$SERVER_PID" 30 || server=$?
  [ "$got" = 1000 ] && [ "${codes[0]}" = 0 ] && [ "$server" = 0 ] ||
    { fail "got=$got client=${codes[0]} server=$server: $(cat "$T/client.err")"; return; }
  pass "both exit 0"
}

ALL="transfer forced_relay resume interrupt_client interrupt_server interrupt_blocked
wrong_token busy squatters stdin_error stdout_error closed_reader"
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
  "check_$CHECK"
done
[ ${#FAILED[@]} -eq 0 ] || { echo "failed: ${FAILED[*]}"; exit 1; }
