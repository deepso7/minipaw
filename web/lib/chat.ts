// A minipaw dialer for the browser: the client side of crates/minipaw's
// session (src/dial.rs, src/pipe.rs) over @minip2p/web, for typing to a
// `minipaw` running in a terminal. The chat page imports this module only
// when someone connects, so the wasm (3.6 MB, about 0.5 MB compressed) and its
// glue load then, not with the page.
//
// Browsers can't listen or hole punch, so the session always runs through
// the default relay's WebTransport address. Like the CLI, it survives the
// stream under it dying: each side keeps what it sent until the other acks
// writing it, and a fresh stream resumes from the peer's receive offset.

import { Minip2p, generateSecretKey, initMinip2p } from "@minip2p/web/manual";
import type { Stream } from "@minip2p/web/manual";

// The package exports only its entry points, so the `.wasm` is imported by
// path; Vite emits it as a hashed asset and this is its URL.
import wasm from "../node_modules/@minip2p/web/wasm/minip2p_web_bg.wasm?url";
import type { Ticket } from "./ticket";
import {
  ACK,
  DATA,
  ERROR,
  FIN,
  FatalError,
  MAX_DATA,
  PING,
  PROTOCOL,
  WELCOME,
  ack,
  frame,
  frameReader,
  hello,
  u64,
} from "./wire";
import type { Frame } from "./wire";

const RELAY_PEER = "12D3KooWNAHhp6rp11SvCDA84zua3hhEYTLNjgKmEDmt1BddtLdf";
/** relay.minip2p.com, the CLI's default relay, at its WebTransport address.
 * No certhash: the relay serves a CA-signed certificate for its domain,
 * which the browser checks as for any HTTPS page, so renewals need no
 * change here. */
const RELAY = `/dns/relay.minip2p.com/udp/19876/quic-v1/webtransport/p2p/${RELAY_PEER}`;

/** How long to keep trying to get a stream back, as the CLI does. */
const RESUME_TIMEOUT = 60_000;
/** Ceiling on each of connecting, opening the stream, and the handshake. */
const SETUP_TIMEOUT = 15_000;
const PING_INTERVAL = 5000;
/** A stream silent this long is dead, even if nothing said so. */
const DEAD_AFTER = 20_000;
const MAX_BACKOFF = 2000;
/** Unacknowledged bytes we hold before refusing more. A chat never gets
 * near it unless the terminal stalls; it keeps every write under the
 * stream's 256 KiB buffer and far under the terminal's 4 MiB window. */
const MAX_UNACKED = 128 * 1024;
/** How long the last ack or Error gets to leave before the endpoint closes. */
const LINGER = 2000;

export type ChatState =
  | { kind: "loading" }
  | { kind: "connecting" }
  | { kind: "up"; resumed: boolean }
  | { kind: "reconnecting"; reason: string }
  | { kind: "ended"; ok: boolean; reason: string };

export interface ChatHandlers {
  state: (state: ChatState) => void;
  /** Text from the terminal, as it arrives. */
  text: (text: string) => void;
  /** The terminal has written our bytes out up to this session offset. */
  acked: (offset: number) => void;
}

export interface Chat {
  /** Sends `text` and returns the session offset just past it, or
   * undefined when it isn't taken: after leaving, or while the terminal is
   * too far behind. */
  send: (text: string) => number | undefined;
  /** Finishes our side; the session ends once the terminal's side has too. */
  leave: () => void;
  /** Ends at once, telling the terminal why. */
  close: (reason: string) => void;
}

const sleep = (ms: number) =>
  // oxlint-disable-next-line promise/avoid-new -- setTimeout has no promise form in browsers.
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

class Session implements Chat {
  readonly #ticket: Ticket;
  readonly #on: ChatHandlers;
  readonly #id = crypto.getRandomValues(new Uint8Array(16));
  readonly #decoder = new TextDecoder();
  #endpoint: Minip2p | undefined;
  /** The current stream, and whether its Welcome came. */
  #stream: Stream | undefined;
  #up = false;
  #wasUp = false;
  /** A Hello went out: from then on the terminal may hold our session, so
   * stopping must tell it, even on a stream still waiting for Welcome. */
  #helloSent = false;
  #ended = false;
  /** Since when we've had no working stream; 0 while up. */
  #lostSince = Date.now();
  #heardAt = 0;
  #sentAt = 0;
  #timer: ReturnType<typeof setInterval> | undefined;

  // Our half: unacked bytes start at session offset `#acked`.
  #unacked = new Uint8Array();
  #acked = 0;
  #sent = 0;
  #eof = false;
  #finSent = false;
  #finAcked = false;

  // The terminal's half.
  #recv = 0;
  #peerFin: number | undefined;
  #peerFinAcked = false;

  constructor(ticket: Ticket, handlers: ChatHandlers) {
    this.#ticket = ticket;
    this.#on = handlers;
  }

  get #end() {
    return this.#acked + this.#unacked.length;
  }

  send(text: string) {
    if (this.#eof || this.#ended) {
      return;
    }
    const bytes = new TextEncoder().encode(text);
    if (this.#unacked.length + bytes.length > MAX_UNACKED) {
      return;
    }
    const unacked = new Uint8Array(this.#unacked.length + bytes.length);
    unacked.set(this.#unacked);
    unacked.set(bytes, this.#unacked.length);
    this.#unacked = unacked;
    this.#flush();
    return this.#end;
  }

  leave() {
    if (this.#ended) {
      return;
    }
    if (!this.#wasUp) {
      this.close("left before connecting");
      return;
    }
    this.#eof = true;
    this.#flush();
  }

  close(reason: string) {
    if (this.#ended) {
      return;
    }
    if (this.#helloSent) {
      this.#write(frame(ERROR, new TextEncoder().encode(reason)));
    }
    this.#finish(true, reason);
  }

  async run() {
    this.#on.state({ kind: "loading" });
    try {
      await initMinip2p({ wasm });
      // Closed while the wasm loaded: no endpoint to start, or to leak.
      if (this.#ended) {
        return;
      }
      this.#endpoint = Minip2p.create({
        // Streams open only on registered protocols, outbound ones too.
        protocols: [PROTOCOL],
        relays: [RELAY],
        secretKey: generateSecretKey(),
      });
    } catch (error) {
      this.#finish(false, `couldn't start minip2p: ${String(error)}`);
      return;
    }
    this.#on.state({ kind: "connecting" });
    // The resume window covers reaching the terminal, not loading the wasm.
    this.#lostSince = Date.now();
    this.#timer = setInterval(() => this.#tick(), 1000);
    let backoff = 0;
    while (!this.#ended) {
      try {
        // oxlint-disable-next-line no-await-in-loop -- one stream at a time, by design.
        await this.#link();
        backoff = 0;
      } catch (error) {
        if (error instanceof FatalError) {
          this.#finish(false, error.message);
        } else {
          this.#lose(String(error));
        }
      }
      if (this.#ended) {
        return;
      }
      if (Date.now() - this.#lostSince >= RESUME_TIMEOUT) {
        this.#finish(
          false,
          this.#wasUp
            ? "lost the connection to the terminal"
            : "couldn't reach the terminal"
        );
        return;
      }
      // oxlint-disable-next-line no-await-in-loop -- backing off between attempts.
      await sleep(backoff);
      backoff = Math.min(Math.max(backoff * 2, 250), MAX_BACKOFF);
    }
  }

  /** Runs one stream, from connecting to its end. */
  async #link() {
    const endpoint = this.#endpoint;
    if (!endpoint) {
      return;
    }
    const peer = this.#ticket.peerId;
    const options = { cancelOnTimeout: true, timeoutMs: SETUP_TIMEOUT };
    const connected = endpoint.connectedPeers();
    if (!connected.includes(peer)) {
      // Dialing a peer ID goes through a connected relay, so reach the
      // relay first; `create` only starts on it.
      if (!connected.includes(RELAY_PEER)) {
        await endpoint.connect(RELAY, options);
      }
      await endpoint.connect(peer, options);
    }
    const stream = await endpoint.openStream(peer, PROTOCOL, {
      timeoutMs: SETUP_TIMEOUT,
    });
    if (this.#ended) {
      stream.reset();
      return;
    }
    this.#stream = stream;
    this.#heardAt = Date.now();
    // Once welcomed, the terminal holds our session: from then on a Hello
    // asks to resume it, and a terminal that doesn't have it refuses.
    this.#write(hello(this.#ticket.token, this.#id, this.#recv, this.#wasUp));
    this.#helloSent = true;
    const reader = frameReader();
    try {
      for (;;) {
        // oxlint-disable-next-line no-await-in-loop -- a stream reads in order.
        const chunk = await stream.read();
        // Closed while waiting: what arrives now is for nobody.
        if (this.#ended) {
          return;
        }
        if (chunk === undefined) {
          throw new Error("stream closed");
        }
        this.#heardAt = Date.now();
        reader.push(chunk);
        for (let next = reader.next(); next; next = reader.next()) {
          if (this.#up) {
            this.#onFrame(next);
          } else {
            this.#onHandshake(next);
          }
        }
        this.#flush();
        if (this.#finAcked && this.#peerFinAcked) {
          this.#finish(true, "the chat is over");
          return;
        }
      }
    } finally {
      if (this.#stream === stream) {
        this.#stream = undefined;
      }
      this.#up = false;
    }
  }

  #onHandshake(next: Frame) {
    if (next.kind === ERROR) {
      throw new FatalError(
        `${this.#wasUp ? "the terminal ended the session" : "the terminal refused"}: ${next.message}`
      );
    }
    if (next.kind !== WELCOME) {
      throw new FatalError(`unexpected frame kind ${next.kind} before Welcome`);
    }
    if (next.recv < this.#acked || next.recv > this.#end) {
      throw new FatalError(
        `the terminal resumed at ${next.recv}, outside ${this.#acked}..=${this.#end}`
      );
    }
    // Resend from what it received; acked bytes stay until it writes them.
    this.#sent = next.recv;
    this.#finSent = false;
    this.#up = true;
    this.#on.state({ kind: "up", resumed: this.#wasUp });
    this.#wasUp = true;
    this.#lostSince = 0;
    // The last ack may have died with the old stream.
    if (this.#recv > 0 || this.#peerFin !== undefined) {
      this.#sendAck();
    }
  }

  #onFrame(next: Frame) {
    switch (next.kind) {
      case DATA: {
        if (this.#peerFin !== undefined) {
          throw new FatalError("the terminal sent data after its Fin");
        }
        if (next.data.length === 0) {
          throw new FatalError("the terminal sent an empty Data frame");
        }
        this.#recv += next.data.length;
        this.#on.text(this.#decoder.decode(next.data, { stream: true }));
        // Shown is written: ack it now, so the terminal's window moves.
        this.#sendAck();
        break;
      }
      case ACK: {
        this.#onAck(next.offset, next.fin);
        break;
      }
      case FIN: {
        this.#onFin(next.offset);
        break;
      }
      case ERROR: {
        throw new FatalError(`the terminal ended the session: ${next.message}`);
      }
      case PING: {
        break;
      }
      default: {
        throw new FatalError("unexpected Welcome mid-session");
      }
    }
  }

  #onAck(offset: number, fin: boolean) {
    if (offset > this.#end) {
      throw new FatalError(
        `the terminal acked ${offset} past our end ${this.#end}`
      );
    }
    if (offset > this.#acked) {
      this.#unacked = this.#unacked.slice(offset - this.#acked);
      this.#acked = offset;
      this.#sent = Math.max(this.#sent, offset);
      this.#on.acked(offset);
    }
    if (fin) {
      if (!this.#eof || offset !== this.#end) {
        throw new FatalError("the terminal acked a Fin we never sent");
      }
      this.#finAcked = true;
    }
  }

  #onFin(offset: number) {
    if (this.#peerFin !== undefined && this.#peerFin !== offset) {
      throw new FatalError(
        `the terminal moved its Fin from ${this.#peerFin} to ${offset}`
      );
    }
    if (offset !== this.#recv) {
      throw new FatalError("the terminal's Fin doesn't match what it sent");
    }
    this.#peerFin = offset;
    this.#on.text(this.#decoder.decode());
    this.#sendAck();
    // As an interactive `minipaw` does: when the other side is done typing,
    // the chat is over, so finish ours too.
    this.#eof = true;
  }

  #sendAck() {
    const fin = this.#peerFin === this.#recv;
    this.#write(ack(this.#recv, fin));
    if (fin) {
      this.#peerFinAcked = true;
    }
  }

  /** Puts pending data, then our Fin, on the stream once it's up. */
  #flush() {
    if (!this.#up) {
      return;
    }
    while (this.#sent < this.#end) {
      const start = this.#sent - this.#acked;
      const chunk = this.#unacked.slice(start, start + MAX_DATA);
      this.#write(frame(DATA, chunk));
      this.#sent += chunk.length;
    }
    if (this.#eof && !this.#finSent && !this.#finAcked) {
      this.#write(frame(FIN, u64(this.#end)));
      this.#finSent = true;
    }
  }

  #write(bytes: Uint8Array) {
    const stream = this.#stream;
    if (!stream) {
      return;
    }
    this.#sentAt = Date.now();
    void this.#writeTo(stream, bytes);
  }

  // Writes queue in order; a failed one means the stream is dead, and
  // resetting it ends the read loop so a fresh one takes over.
  async #writeTo(stream: Stream, bytes: Uint8Array) {
    try {
      await stream.write(bytes);
    } catch {
      if (this.#stream === stream) {
        stream.reset();
      }
    }
  }

  #tick() {
    const stream = this.#stream;
    if (!stream) {
      return;
    }
    const now = Date.now();
    if (now - this.#heardAt >= (this.#up ? DEAD_AFTER : SETUP_TIMEOUT)) {
      // A relayed circuit can die without word; a stream on the same
      // connection would be just as dead, so drop the connection too.
      stream.reset();
      try {
        this.#endpoint?.disconnect(this.#ticket.peerId);
      } catch {
        // Already gone.
      }
      return;
    }
    if (this.#up && now - this.#sentAt >= PING_INTERVAL) {
      this.#write(frame(PING));
    }
  }

  #lose(reason: string) {
    console.debug(`minipaw: ${reason}; retrying`);
    // Report the first failure only, not every retry after it.
    if (this.#lostSince !== 0) {
      return;
    }
    this.#lostSince = Date.now();
    this.#on.state({ kind: "reconnecting", reason });
  }

  #finish(ok: boolean, reason: string) {
    if (this.#ended) {
      return;
    }
    this.#ended = true;
    clearInterval(this.#timer);
    this.#on.state({ kind: "ended", ok, reason });
    const stream = this.#stream;
    stream?.closeWrite();
    setTimeout(
      () => {
        void this.#closeEndpoint();
      },
      stream ? LINGER : 0
    );
  }

  async #closeEndpoint() {
    try {
      await this.#endpoint?.close();
    } catch {
      // Closing is best effort; the page is done with it either way.
    }
  }
}

/** Starts a chat with the terminal behind `ticket`. */
export const startChat = (ticket: Ticket, handlers: ChatHandlers): Chat => {
  const session = new Session(ticket, handlers);
  void session.run();
  return session;
};
