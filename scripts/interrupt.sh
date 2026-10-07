#!/usr/bin/env bash
# Ctrl-C on one side must end the session on both, promptly.
#
#   RELAY=/ip4/…/quic-v1/p2p/12D3KooW… scripts/interrupt.sh [client|server|blocked]
#
# Both sides keep stdin open, so only the interrupt can end the session.
# Expects: the interrupted side exits 130, the peer exits 1 within a few
# seconds, and the line each side sent before the interrupt was written.
# `blocked` interrupts a client whose stdout reader has stopped reading
# while the server floods it: teardown must still be prompt.
set -uo pipefail

: "${RELAY:?set RELAY to the QUIC peer address of a relay}"
WHO=${1:-client}
BIN=${BIN:-target/debug/minipaw}
T=$(mktemp -d)
trap 'kill ${SP:-} ${CP:-} ${RP:-} 2>/dev/null; rm -rf "$T"' EXIT

if [ "$WHO" = blocked ]; then
  "$BIN" -v --relay "$RELAY" < <(echo from-server; head -c 50000000 /dev/zero; sleep 60) >"$T/server.out" 2>"$T/server.err" &
else
  "$BIN" -v --relay "$RELAY" < <(echo from-server; sleep 60) >"$T/server.out" 2>"$T/server.err" &
fi
SP=$!
for _ in $(seq 1 100); do
  grep -q '^minipaw mp' "$T/server.err" && break
  sleep 0.2
done
TICKET=$(grep '^minipaw mp' "$T/server.err" | awk '{print $2}')
[ -n "$TICKET" ] || { echo "server never printed a ticket"; exit 1; }

if [ "$WHO" = blocked ]; then
  # A reader that never reads: the client's stdout pipe fills and blocks.
  mkfifo "$T/fifo"
  sleep 120 <"$T/fifo" &
  RP=$!
  "$BIN" -v "$TICKET" < <(echo from-client; sleep 60) >"$T/fifo" 2>"$T/client.err" &
  CP=$!
  for _ in $(seq 1 100); do
    grep -q from-client "$T/server.out" && break
    sleep 0.1
  done
  sleep 2 # let the pipe fill
else
  "$BIN" -v "$TICKET" < <(echo from-client; sleep 60) >"$T/client.out" 2>"$T/client.err" &
  CP=$!
  for _ in $(seq 1 100); do
    grep -q from-client "$T/server.out" && grep -q from-server "$T/client.out" && break
    sleep 0.1
  done
fi

if [ "$WHO" = server ]; then VICTIM=$SP PEER=$CP; else VICTIM=$CP PEER=$SP; fi
start=$(date +%s)
kill -INT "$VICTIM"
wait "$VICTIM"; victim_code=$?
wait "$PEER"; peer_code=$?
end=$(date +%s)
echo "interrupted $WHO: exit=$victim_code; peer exit=$peer_code; both gone in $((end - start))s"
grep -q from-client "$T/server.out" && echo "server got client data" || echo "SERVER MISSING DATA"
if [ "$WHO" != blocked ]; then
  grep -q from-server "$T/client.out" && echo "client got server data" || echo "CLIENT MISSING DATA"
fi
grep -h "minipaw:" "$T/server.err" "$T/client.err"

[ "$victim_code" = 130 ] && [ "$peer_code" = 1 ] && [ $((end - start)) -le 5 ]
