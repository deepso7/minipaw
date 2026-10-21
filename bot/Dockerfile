# The paw bot: `minipaw serve` forwarding each chat to pawbot.ts.
#   docker build -t pawbot bot/
#   docker run --rm -v pawbot:/data pawbot ticket
#   docker run -d --name pawbot --restart unless-stopped \
#     -v pawbot:/data --env-file /etc/pawbot.env pawbot
FROM node:24-bookworm-slim

ARG MINIPAW_VERSION=0.1.1

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
# The site's installer checks the tarball against the release's SHA256SUMS.
RUN curl -fsSL https://paw.minip2p.com/install.sh \
  | MINIPAW_VERSION="$MINIPAW_VERSION" MINIPAW_INSTALL=/usr/local/bin sh

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY pawbot.ts entrypoint.sh ./

# minipaw refuses an identity in a directory others can write.
RUN mkdir /data && chown node:node /data && chmod 700 /data
USER node
VOLUME /data

ENTRYPOINT ["/app/entrypoint.sh"]
