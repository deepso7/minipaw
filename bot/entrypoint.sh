#!/bin/bash
# Runs the bot and `minipaw serve` in front of it. Either one exiting stops
# the container, so its restart policy brings both back together.
#   (no args)  serve the bot
#   ticket     print the bot's ticket (creating its identity on first use)
set -euo pipefail

identity=/data/serve.key
# The same rule as pawbot.ts: a blank or malformed port means the default.
port="${PAWBOT_PORT:-}"
port="${port//[[:space:]]/}"
if [[ ! "$port" =~ ^[0-9]+$ ]] || ((10#$port < 1)); then
  port=7777
fi
export PAWBOT_PORT="$port"

if [[ "${1:-}" == ticket ]]; then
  exec minipaw ticket --identity "$identity"
fi

node /app/pawbot.ts &
# Each session is forwarded to the bot over loopback; serve itself only
# dials out, to its relay reservation, so no port needs opening.
# The landing page connects whoever clicks into its chat box, so allow more
# chats than serve's default 16; the bot closes idle ones after 5 minutes.
minipaw serve --forward "127.0.0.1:$port" --identity "$identity" \
  --max-sessions "${PAWBOT_SESSIONS:-64}" &

# Stops whichever of the two is still running.
stop() {
  local pids
  pids="$(jobs -p)"
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086 # one PID per word
    kill $pids 2>/dev/null || true
  fi
}
trap stop INT TERM
status=0
wait -n || status=$?
stop
exit "$status"
