#!/usr/bin/env bash
# Runs the paw bot and `minipaw serve` in front of it. Either one exiting
# stops both, so systemd (pawbot.service) restarts them together.
#   run.sh          serve the bot
#   run.sh ticket   print the bot's ticket (creating its identity on first use)
#
# Looks for `pawbot` beside this script and `minipaw` on PATH or beside it;
# PAWBOT_BIN and MINIPAW_BIN override. State (the identity and the day's
# reply count) lives in PAWBOT_DATA, else systemd's STATE_DIRECTORY, else
# ~/.local/state/pawbot.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
pawbot="${PAWBOT_BIN:-$here/pawbot}"
minipaw="${MINIPAW_BIN:-$(command -v minipaw || echo "$here/minipaw")}"

data="${PAWBOT_DATA:-${STATE_DIRECTORY:-$HOME/.local/state/pawbot}}"
# minipaw refuses an identity in a directory others can read or write.
mkdir -p "$data"
chmod 700 "$data"
identity="$data/serve.key"
export PAWBOT_STATE="${PAWBOT_STATE:-$data/usage.json}"

if [[ "${1:-}" == ticket ]]; then
  exec "$minipaw" ticket --identity "$identity"
fi

# The same rule as pawbot.ts: a blank or malformed port means the default.
port="${PAWBOT_PORT:-}"
port="${port//[[:space:]]/}"
if [[ ! "$port" =~ ^[0-9]+$ ]] || ((10#$port < 1)); then
  port=7777
fi
export PAWBOT_PORT="$port"

"$pawbot" &
bot=$!
# Each session is forwarded to the bot over loopback; serve itself only
# dials out, to its relay reservation, so no port needs opening. The landing
# page connects whoever clicks into its chat box, so allow more chats than
# serve's default 16; the bot closes idle ones after 5 minutes.
"$minipaw" serve --forward "127.0.0.1:$port" --identity "$identity" \
  --max-sessions "${PAWBOT_SESSIONS:-64}" &
serve=$!

stop() {
  kill "$bot" "$serve" 2>/dev/null || true
}
trap stop INT TERM
# Until one of them exits (polled, as macOS's bash 3.2 has no `wait -n`).
while kill -0 "$bot" 2>/dev/null && kill -0 "$serve" 2>/dev/null; do
  sleep 1
done
status=0
if ! kill -0 "$bot" 2>/dev/null; then
  wait "$bot" || status=$?
else
  wait "$serve" || status=$?
fi
stop
exit "$status"
