# Shared helpers for the test and benchmark scripts. Source it; don't run it.
#
# Every script needs a relay. Set RELAY to use one you run yourself;
# otherwise `ensure_relay` starts a throwaway local one with no circuit
# limits, so the run measures minipaw rather than relay policy. It looks for
# the relay binary in $MINIP2P_RELAY, then `minip2p-relay` on PATH, then a
# release build in a sibling minip2p checkout. Build one with:
#
#   cd ../minip2p && cargo build --release -p minip2p-relay-server-example

# Starts the local relay unless RELAY is already set, and exports RELAY.
# Pass extra relay flags to replace the no-limits defaults.
ensure_relay() {
  [ -n "${RELAY:-}" ] && return 0
  local bin=${MINIP2P_RELAY:-}
  if [ -z "$bin" ]; then
    bin=$(command -v minip2p-relay || true)
  fi
  if [ -z "$bin" ]; then
    local here
    here=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
    bin=$here/minip2p/target/release/minip2p-relay
  fi
  if [ ! -x "$bin" ]; then
    echo "no relay: set RELAY, or MINIP2P_RELAY to a minip2p-relay binary" >&2
    echo "  (build one: cd ../minip2p && cargo build --release -p minip2p-relay-server-example)" >&2
    exit 2
  fi
  local flags=("$@")
  [ ${#flags[@]} -gt 0 ] || flags=(--max-circuit-bytes 0 --max-circuit-duration 0
    --circuit-peer-rate off --circuit-ip-rate off)
  RELAY_LOG=$(mktemp)
  "$bin" --quic 127.0.0.1:0 --tcp 127.0.0.1:0 "${flags[@]}" >"$RELAY_LOG" 2>&1 &
  RELAY_PID=$!
  for _ in $(seq 1 50); do
    RELAY=$(grep -m1 -o '/ip4/127\.0\.0\.1/udp/[0-9]*/quic-v1/p2p/[A-Za-z0-9]*' "$RELAY_LOG" || true)
    [ -n "$RELAY" ] && break
    sleep 0.1
  done
  [ -n "$RELAY" ] || { echo "local relay did not start:" >&2; cat "$RELAY_LOG" >&2; exit 2; }
  export RELAY
}

# Stops a relay `ensure_relay` started. Safe to call from an EXIT trap.
stop_relay() {
  if [ -n "${RELAY_PID:-}" ]; then
    kill "$RELAY_PID" 2>/dev/null || true
    wait "$RELAY_PID" 2>/dev/null || true
    rm -f "$RELAY_LOG"
  fi
}

# Starts a server with the given stdin file and waits for its ticket.
#   start_server <stdin> <stdout> <stderr> [extra minipaw args...]
# SERVER_WRAP, if set, prefixes the command (e.g. "/usr/bin/time -p").
# Sets SERVER_PID and TICKET.
start_server() {
  local in=$1 out=$2 err=$3
  shift 3
  # shellcheck disable=SC2086 # SERVER_WRAP is a word list
  ${SERVER_WRAP:-} "$BIN" -v --relay "$RELAY" "$@" <"$in" >"$out" 2>"$err" &
  SERVER_PID=$!
  TICKET=
  for _ in $(seq 1 150); do
    TICKET=$(awk '/^minipaw mp/{print $2}' "$err")
    [ -n "$TICKET" ] && return 0
    kill -0 "$SERVER_PID" 2>/dev/null || break
    sleep 0.1
  done
  echo "server never printed a ticket:" >&2
  cat "$err" >&2
  return 1
}

# Waits for a background process for at most <secs>, killing it after that.
# Returns its exit code, or 124 on timeout.
wait_upto() { # wait_upto <pid> <secs>
  local pid=$1 deadline=$(($(date +%s) + $2))
  while kill -0 "$pid" 2>/dev/null; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
      kill -9 "$pid" 2>/dev/null
      wait "$pid" 2>/dev/null
      return 124
    fi
    sleep 0.1
  done
  wait "$pid"
}

now() { perl -MTime::HiRes=time -e 'printf "%.3f", time'; }
