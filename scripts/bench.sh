#!/usr/bin/env bash
# One-way bulk throughput, client -> server, over loopback.
#
#   scripts/bench.sh [direct|relayed|both] [MiB] [runs]     # defaults: both 512 5
#
# direct:  the client dials the server's QUIC address (MINIPAW_DIRECT); the
#          relay only carries the reservation.
# relayed: everything goes through the relay (MINIPAW_FORCE_RELAY).
#
# Starts a local relay unless RELAY is set (see scripts/lib.sh). The relay
# must be a release build: a debug one halves relayed throughput.
#
# A single run can land in a slow mode (deepso7/minip2p#303), so each mode
# runs several times and the median is what counts. Every invocation appends
# one row per mode to bench/results.tsv. Wall time includes connection setup
# (a few hundred ms on loopback), so keep transfers large.
#
# Compare against minip2p's own ceiling on the same machine:
#   cargo bench -p minip2p-rs --bench endpoint_throughput --features quic,tcp,nat,relay-server
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

MODES=${1:-both}
MIB=${2:-512}
RUNS=${3:-5}
BIN=${BIN:-target/release/minipaw}
[ "$MODES" = both ] && MODES="direct relayed"
for mode in $MODES; do
  case "$mode" in direct | relayed) ;; *) echo "mode must be direct, relayed or both" >&2; exit 2 ;; esac
done
[ -x "$BIN" ] || cargo build -q --release

T=$(mktemp -d)
trap 'kill "${SERVER_PID:-}" 2>/dev/null || true; stop_relay; rm -rf "$T"' EXIT
ensure_relay

cpu() { awk '/^user/{u=$2} /^sys/{s=$2} END{printf "%.2f", u+s}' "$1"; }

# One transfer; prints "<MiB/s> <client cpu> <server cpu>".
run_once() {
  local mode=$1 bytes=$((MIB * 1024 * 1024))
  if [ "$mode" = relayed ]; then export MINIPAW_FORCE_RELAY=1; else unset MINIPAW_FORCE_RELAY; fi
  unset MINIPAW_DIRECT
  rm -f "$T/sink" && mkfifo "$T/sink"
  wc -c <"$T/sink" | tr -d ' ' >"$T/count" &
  local counter=$!
  SERVER_WRAP="/usr/bin/time -p" start_server /dev/null "$T/sink" "$T/server.err" || exit 1
  if [ "$mode" = direct ]; then
    local port
    port=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/\([0-9]*\)/.*#\1#p' "$T/server.err" | head -1)
    export MINIPAW_DIRECT=/ip4/127.0.0.1/udp/$port/quic-v1
  fi
  local start end
  start=$(now)
  head -c "$bytes" /dev/zero | /usr/bin/time -p "$BIN" "$TICKET" >/dev/null 2>"$T/client.err"
  end=$(now)
  wait "$SERVER_PID"
  wait "$counter"
  local got
  got=$(cat "$T/count")
  if [ "$got" != "$bytes" ]; then
    echo "server received $got bytes, expected $bytes" >&2
    exit 1
  fi
  echo "$(echo "$MIB / ($end - $start)" | bc -l) $(cpu "$T/client.err") $(cpu "$T/server.err")"
}

# Median of numbers on stdin.
median() { sort -n | awk '{v[NR]=$1} END{print (NR%2 ? v[(NR+1)/2] : (v[NR/2]+v[NR/2+1])/2)}'; }

mkdir -p bench
[ -f bench/results.tsv ] ||
  printf 'date\tcommit\tmode\tmib\truns\tmedian_mib_s\tmin_mib_s\tmax_mib_s\tclient_cpu_s\tserver_cpu_s\thost\n' >bench/results.tsv
commit=$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- src Cargo.toml Cargo.lock || echo "+dirty")

for mode in $MODES; do
  : >"$T/runs"
  for i in $(seq 1 "$RUNS"); do
    result=$(run_once "$mode")
    echo "$result" >>"$T/runs"
    printf '%-7s run %d/%d: %6.1f MiB/s  cpu client %ss server %ss\n' "$mode" "$i" "$RUNS" $result
  done
  med=$(cut -d' ' -f1 "$T/runs" | median)
  lo=$(cut -d' ' -f1 "$T/runs" | sort -n | head -1)
  hi=$(cut -d' ' -f1 "$T/runs" | sort -n | tail -1)
  ccpu=$(cut -d' ' -f2 "$T/runs" | median)
  scpu=$(cut -d' ' -f3 "$T/runs" | median)
  printf '%-7s median %.1f MiB/s (min %.1f, max %.1f) over %d x %d MiB\n' "$mode" "$med" "$lo" "$hi" "$RUNS" "$MIB"
  printf '%s\t%s\t%s\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.2f\t%.2f\t%s\n' "$(date -u +%FT%TZ)" "$commit" "$mode" \
    "$MIB" "$RUNS" "$med" "$lo" "$hi" "$ccpu" "$scpu" "$(uname -sm | tr ' ' '-')" >>bench/results.tsv
done
