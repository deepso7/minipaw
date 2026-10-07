#!/usr/bin/env bash
# Real-network throughput between this machine and an ssh host, through the
# default public relay and whatever path hole punching finds.
#
#   scripts/bench-remote.sh <ssh-host> [MiB] [runs]     # defaults: 64 3
#
# The server runs on the host, the client here. Each run times an upload
# (here -> host) and a download (host -> here) and reports the path taken.
# REMOTE_BIN names minipaw on the host (default: minipaw on its PATH); build
# it from the same commit as this checkout. Set MINIPAW_FORCE_RELAY=1 to
# measure the relayed path instead. Rows go to bench/results.tsv as
# up@<host> / down@<host>.
set -euo pipefail
cd "$(dirname "$0")/.."
. scripts/lib.sh

HOST=${1:?usage: scripts/bench-remote.sh <ssh-host> [MiB] [runs]}
MIB=${2:-64}
RUNS=${3:-3}
BIN=${BIN:-target/release/minipaw}
REMOTE_BIN=${REMOTE_BIN:-minipaw}
FORCE=${MINIPAW_FORCE_RELAY:-}
[ -x "$BIN" ] || cargo build -q --release
T=$(mktemp -d)
trap 'ssh "$HOST" "pkill -f -- \"$REMOTE_BIN\" >/dev/null 2>&1" 2>/dev/null || true; rm -rf "$T"' EXIT

# Starts a server on the host. Its stdin is `head -c <bytes>`, its stdout
# is counted. Sets TICKET.
remote_server() {
  ssh "$HOST" bash -s -- "$REMOTE_BIN" "$1" "$FORCE" >"$T/remote" <<'EOF'
cd /tmp && rm -f mpbench.err mpbench.count
[ -n "$3" ] && export MINIPAW_FORCE_RELAY=1
nohup bash -c "head -c $2 /dev/zero | $1 -v 2>mpbench.err | wc -c >mpbench.count" >/dev/null 2>&1 &
for _ in $(seq 1 150); do grep -q '^minipaw mp' mpbench.err 2>/dev/null && break; sleep 0.1; done
awk '/^minipaw mp/{print $2}' mpbench.err
EOF
  TICKET=$(tail -1 "$T/remote")
  [ -n "$TICKET" ] || { echo "server on $HOST never printed a ticket" >&2; exit 1; }
}

remote_count() {
  for _ in $(seq 1 50); do
    local n
    n=$(ssh "$HOST" 'cat /tmp/mpbench.count 2>/dev/null' | tr -d ' ')
    [ -n "$n" ] && { echo "$n"; return; }
    sleep 0.2
  done
  echo missing
}

path_of() { grep -q 'upgraded to a direct\|connected (direct)' "$1" && echo direct || echo relayed; }

bytes=$((MIB * 1024 * 1024))
median() { sort -n | awk '{v[NR]=$1} END{print (NR%2 ? v[(NR+1)/2] : (v[NR/2]+v[NR/2+1])/2)}'; }
: >"$T/up"
: >"$T/down"
for i in $(seq 1 "$RUNS"); do
  # Upload: we send, the host only listens.
  remote_server 0
  start=$(now)
  head -c "$bytes" /dev/zero | "$BIN" "$TICKET" >/dev/null 2>"$T/c.err"
  end=$(now)
  got=$(remote_count)
  [ "$got" = "$bytes" ] || { echo "host received $got of $bytes bytes" >&2; cat "$T/c.err" >&2; exit 1; }
  up=$(echo "$MIB / ($end - $start)" | bc -l)
  echo "$up" >>"$T/up"
  printf 'run %d/%d up:   %6.1f MiB/s (%s)\n' "$i" "$RUNS" "$up" "$(path_of "$T/c.err")"

  # Download: the host sends, we only listen.
  remote_server "$bytes"
  start=$(now)
  got=$("$BIN" "$TICKET" </dev/null 2>"$T/c.err" | wc -c | tr -d ' ')
  end=$(now)
  [ "$got" = "$bytes" ] || { echo "received $got of $bytes bytes" >&2; cat "$T/c.err" >&2; exit 1; }
  down=$(echo "$MIB / ($end - $start)" | bc -l)
  echo "$down" >>"$T/down"
  printf 'run %d/%d down: %6.1f MiB/s (%s)\n' "$i" "$RUNS" "$down" "$(path_of "$T/c.err")"
done

mkdir -p bench
[ -f bench/results.tsv ] ||
  printf 'date\tcommit\tmode\tmib\truns\tmedian_mib_s\tmin_mib_s\tmax_mib_s\tclient_cpu_s\tserver_cpu_s\thost\n' >bench/results.tsv
commit=$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- src Cargo.toml Cargo.lock || echo "+dirty")
suffix=${FORCE:+-relayed}
for dir in up down; do
  med=$(median <"$T/$dir")
  lo=$(sort -n "$T/$dir" | head -1)
  hi=$(sort -n "$T/$dir" | tail -1)
  printf '%-4s median %.1f MiB/s (min %.1f, max %.1f) over %d x %d MiB\n' "$dir" "$med" "$lo" "$hi" "$RUNS" "$MIB"
  printf '%s\t%s\t%s\t%d\t%d\t%.1f\t%.1f\t%.1f\t\t\t%s\n' "$(date -u +%FT%TZ)" "$commit" "$dir$suffix@$HOST" \
    "$MIB" "$RUNS" "$med" "$lo" "$hi" "$(uname -sm | tr ' ' '-')" >>bench/results.tsv
done
