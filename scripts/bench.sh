#!/usr/bin/env bash
# One-way bulk throughput, client -> server, over loopback.
#
#   RELAY=/ip4/127.0.0.1/udp/29877/quic-v1/p2p/12D3KooW… scripts/bench.sh [direct|relayed] [MiB]
#
# direct:  the client dials the server's QUIC address (MINIPAW_DIRECT); the
#          relay only carries the reservation.
# relayed: everything goes through the relay (MINIPAW_FORCE_RELAY). Run a
#          release-build relay (a debug one halves relayed throughput)
#          without circuit limits:
#            minip2p-relay --quic 127.0.0.1:29877 --tcp 127.0.0.1:29879 \
#              --max-circuit-bytes 0 --max-circuit-duration 0 --circuit-peer-rate off
#
# Compare against minip2p's own ceiling on the same machine:
#   cargo bench -p minip2p-rs --bench endpoint_throughput --features quic,tcp,nat,relay-server
#
# Wall time includes connection setup (a few hundred ms on loopback), so use
# a large transfer. Each run appends a row to bench/results.tsv.
set -euo pipefail

: "${RELAY:?set RELAY to the QUIC peer address of a local relay}"
MODE=${1:-direct}
MIB=${2:-1024}
BIN=${BIN:-target/release/minipaw}
T=$(mktemp -d)
trap 'kill "${SP:-}" 2>/dev/null || true; rm -rf "$T"' EXIT

case "$MODE" in
  direct) unset MINIPAW_FORCE_RELAY ;;
  relayed) export MINIPAW_FORCE_RELAY=1 ;;
  *) echo "mode must be direct or relayed"; exit 2 ;;
esac

# Server: nothing to send (stdin EOF), counts what it receives.
(/usr/bin/time -p "$BIN" -v --relay "$RELAY" </dev/null 2>"$T/server.err" | wc -c >"$T/count") &
SP=$!
for _ in $(seq 1 100); do
  grep -q '^minipaw mp' "$T/server.err" && break
  sleep 0.1
done
TICKET=$(awk '/^minipaw mp/{print $2}' "$T/server.err")
[ -n "$TICKET" ] || { echo "server never printed a ticket"; cat "$T/server.err"; exit 1; }
if [ "$MODE" = direct ]; then
  PORT=$(sed -n 's#^\# bound /ip4/0\.0\.0\.0/udp/\([0-9]*\)/.*#\1#p' "$T/server.err" | head -1)
  export MINIPAW_DIRECT=/ip4/127.0.0.1/udp/$PORT/quic-v1
fi

start=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
head -c $((MIB * 1024 * 1024)) /dev/zero | /usr/bin/time -p "$BIN" "$TICKET" >/dev/null 2>"$T/client.err"
end=$(perl -MTime::HiRes=time -e 'printf "%.3f", time')
wait "$SP"

got=$(tr -d ' ' <"$T/count")
[ "$got" = $((MIB * 1024 * 1024)) ] || { echo "server received $got bytes, expected $((MIB * 1024 * 1024))"; exit 1; }

cpu() { awk '/^user/{u=$2} /^sys/{s=$2} END{printf "%.2f", u+s}' "$1"; }
wall=$(echo "$end - $start" | bc -l)
rate=$(echo "$MIB / $wall" | bc -l)
client_cpu=$(cpu "$T/client.err")
server_cpu=$(cpu "$T/server.err")
printf 'mode=%s size=%dMiB wall=%.2fs throughput=%.1fMiB/s client_cpu=%ss server_cpu=%ss\n' \
  "$MODE" "$MIB" "$wall" "$rate" "$client_cpu" "$server_cpu"

mkdir -p bench
[ -f bench/results.tsv ] || printf 'date\tcommit\tmode\tmib\tmib_per_s\tclient_cpu_s\tserver_cpu_s\thost\n' >bench/results.tsv
commit=$(git rev-parse --short HEAD)$(git diff --quiet HEAD -- src Cargo.toml Cargo.lock || echo "+dirty")
printf '%s\t%s\t%s\t%d\t%.1f\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$commit" "$MODE" "$MIB" "$rate" \
  "$client_cpu" "$server_cpu" "$(uname -sm | tr ' ' '-')" >>bench/results.tsv
