# paw bot

The chat on paw.minip2p.com's landing page: `minipaw serve` forwards each visitor to `pawbot.ts`, which answers each line with Claude (`claude-haiku-5-5`). Visitors arrive through the relay, so the host needs no open port.

## Build

On your machine, not the server: Bun compiles the bot into one executable per Linux architecture.

```sh
bun install
bun run build    # dist/pawbot-linux-x64, dist/pawbot-linux-arm64
```

## Deploy

Copy the build for the server's architecture (`uname -m`) and the files beside it, then install the prebuilt `minipaw` there (a download, nothing to compile):

```sh
scp dist/pawbot-linux-arm64 server:/tmp/pawbot
scp run.sh pawbot.service server:/tmp/
ssh server
sudo install -D -m 0755 /tmp/pawbot /opt/pawbot/pawbot
sudo install -m 0755 /tmp/run.sh /opt/pawbot/run.sh
curl -fsSL https://paw.minip2p.com/install.sh | sudo MINIPAW_INSTALL=/opt/pawbot sh
echo 'ANTHROPIC_API_KEY=…' | sudo install -m 0600 /dev/stdin /etc/pawbot.env
sudo install -m 0644 /tmp/pawbot.service /etc/systemd/system/pawbot.service
sudo systemctl daemon-reload && sudo systemctl enable --now pawbot
journalctl -u pawbot | grep -m1 'minipaw mp'    # the ticket for web/pages/index.astro
```

The identity, and so the ticket, lives in `/var/lib/pawbot` and survives restarts and redeploys. To redeploy, copy the new `pawbot` over and `sudo systemctl restart pawbot`.

## Settings

In `/etc/pawbot.env`, besides `ANTHROPIC_API_KEY`:

| Variable               | Default            |                                     |
| ---------------------- | ------------------ | ----------------------------------- |
| `PAWBOT_MODEL`         | `claude-haiku-5-5` |                                     |
| `PAWBOT_DAILY_LIMIT`   | 300                | replies a day, across all chats     |
| `PAWBOT_SESSION_LIMIT` | 20                 | replies per chat                    |
| `PAWBOT_SESSIONS`      | 64                 | chats at once (`serve --max-sessions`) |
| `PAWBOT_PORT`          | 7777               | loopback port between serve and bot |

Chats close after 5 quiet minutes. Locally, `bun pawbot.ts` runs the bot alone, and `./run.sh` runs both (with `pawbot` built for your machine via `bun build pawbot.ts --compile --outfile pawbot`).
