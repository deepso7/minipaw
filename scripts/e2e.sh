#!/usr/bin/env bash
# Bidirectional transfer between a minipaw server and client through a relay.
#
#   RELAY=/ip4/127.0.0.1/udp/29876/quic-v1/p2p/12D3KooW… scripts/e2e.sh [up-bytes] [down-bytes]
#
# Start a local relay first, e.g. from a minip2p checkout:
#   cargo run -p minip2p-relay-server-example -- --quic 127.0.0.1:29876
set -euo pipefail

: "${RELAY:?set RELAY to the QUIC peer address of a relay}"
UP=${1:-5000000}
DOWN=${2:-3000000}
BIN=${BIN:-target/debug/minipaw}
T=$(mktemp -d)
trap 'kill "${SP:-}" 2>/dev/null || true; rm -rf "$T"' EXIT

head -c "$UP" /dev/urandom >"$T/up.bin"
head -c "$DOWN" /dev/urandom >"$T/down.bin"

"$BIN" -v --relay "$RELAY" <"$T/down.bin" >"$T/server.out" 2>"$T/server.err" &
SP=$!
for _ in $(seq 1 100); do
  grep -q '^minipaw mp' "$T/server.err" && break
  sleep 0.2
done
TICKET=$(grep '^minipaw mp' "$T/server.err" | awk '{print $2}')
[ -n "$TICKET" ] || { echo "server never printed a ticket"; cat "$T/server.err"; exit 1; }

start=$(date +%s)
client=0
"$BIN" -v "$TICKET" <"$T/up.bin" >"$T/client.out" 2>"$T/client.err" || client=$?
server=0
wait "$SP" || server=$?
end=$(date +%s)
echo "client exit=$client server exit=$server elapsed=$((end - start))s"

ok=1
cmp -s "$T/up.bin" "$T/server.out" && echo "upload ok ($UP bytes)" || { echo "UPLOAD MISMATCH"; ok=0; }
cmp -s "$T/down.bin" "$T/client.out" && echo "download ok ($DOWN bytes)" || { echo "DOWNLOAD MISMATCH"; ok=0; }
if [ "$ok" = 0 ] || [ "$client" != 0 ] || [ "$server" != 0 ] || [ -n "${SHOW_LOGS:-}" ]; then
  echo "--- client"; grep -v '^# Stream' "$T/client.err" | tail -40
  echo "--- server"; grep -v '^# Stream' "$T/server.err" | tail -40
fi
[ "$ok" = 1 ] && [ "$client" = 0 ] && [ "$server" = 0 ]
