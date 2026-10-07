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
cargo build -q --release --bin minipaw --example squat || exit 2
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
  printf 'PASS  %-18s %s\n' "$CHECK" "$*"
}
fail() {
  [ -n "$RESULT" ] && return
  RESULT=fail
  printf 'FAIL  %-18s %s\n' "$CHECK" "$*"
  FAILED+=("$CHECK")
  if [ -n "${SHOW_LOGS:-}" ]; then
    for f in "$T"/*.err; do echo "--- $f"; tail -20 "$f"; done
  fi
}
# For a check that cannot run here; it neither passes nor fails the run.
skip() {
  [ -n "$RESULT" ] && return
  RESULT=skip
  printf 'SKIP  %-18s %s\n' "$CHECK" "$*"
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

ALL="transfer forced_relay resume heartbeat interrupt_client interrupt_server interrupt_blocked
interrupt_both wrong_token busy squatters stdin_error stdout_error closed_reader"
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
