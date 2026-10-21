#!/bin/bash
# Runs the bot and `minipaw serve` in front of it. Either one exiting stops
# the container, so its restart policy brings both back together.
#   (no args)  serve the bot
#   ticket     print the bot's ticket (creating its identity on first use)
set -euo pipefail

identity=/data/serve.key
port="${PAWBOT_PORT:-7777}"

if [[ "${1:-}" == ticket ]]; then
  exec minipaw ticket --identity "$identity"
fi

node /app/pawbot.ts &
# Each session is forwarded to the bot over loopback; serve itself only
# dials out, to its relay reservation, so no port needs opening.
minipaw serve --forward "127.0.0.1:$port" --identity "$identity" &

trap 'kill $(jobs -p) 2>/dev/null' INT TERM
wait -n
status=$?
kill $(jobs -p) 2>/dev/null || true
exit "$status"
