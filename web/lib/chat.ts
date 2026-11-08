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

/** The session offset just past a sent text, or why it wasn't taken:
 * "closed" once either side has finished or the session has ended, "full"
 * while the terminal is too far behind. */
export type Sent = number | "closed" | "full";

export interface Chat {
  /** Sends `text`; see {@link Sent}. */
  send: (text: string) => Sent;
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

/** Where the session's connection is. `dialing` and `handshaking` count
 * the resume window from `lostSince`; `heardAt` is when the stream last
 * delivered anything. */
type Link =
  | { kind: "loading" }
  /** No stream; `reason` is why the last working one was lost. */
  | { kind: "dialing"; lostSince: number; reason?: string }
  /** A Hello is out on `stream`, waiting for Welcome. */
  | { kind: "handshaking"; stream: Stream; lostSince: number; heardAt: number }
  | { kind: "up"; stream: Stream; heardAt: number }
  | { kind: "ended"; ok: boolean; reason: string };

class Session implements Chat {
  readonly #ticket: Ticket;
  readonly #on: ChatHandlers;
  readonly #id = crypto.getRandomValues(new Uint8Array(16));
  readonly #decoder = new TextDecoder();
  #endpoint: Minip2p | undefined;
  #link: Link = { kind: "loading" };
  /** Some stream has been welcomed. */
  #wasUp = false;
  /** A Hello went out: from then on the terminal may hold our session, so
   * stopping must tell it, even on a stream still waiting for Welcome. */
  #helloSent = false;
  #sentAt = 0;
  /** Runs from dialing until the end; only `#go` touches it. */
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

  get #ended() {
    return this.#link.kind === "ended";
  }

  /** The current stream, while there is one. */
  get #stream() {
    const link = this.#link;

    return link.kind === "handshaking" || link.kind === "up"
      ? link.stream
      : undefined;
  }

  send(text: string): Sent {
    if (this.#eof || this.#ended) {
      return "closed";
    }

    const bytes = new TextEncoder().encode(text);

    if (this.#unacked.length + bytes.length > MAX_UNACKED) {
      return "full";
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
    this.#go({ kind: "loading" });

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

    // The resume window covers reaching the terminal, not loading the wasm.
    this.#go({ kind: "dialing", lostSince: Date.now() });
    let backoff = 0;

    while (!this.#ended) {
      try {
        // oxlint-disable-next-line no-await-in-loop -- one stream at a time, by design.
        await this.#connect();
      } catch (error) {
        this.#lose(error instanceof FatalError ? error : String(error));
      }

      // Short of the end, a stream that stopped leaves us dialing.
      const link = this.#link;

      if (link.kind !== "dialing") {
        return;
      }

      if (Date.now() - link.lostSince >= RESUME_TIMEOUT) {
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
  async #connect() {
    const endpoint = this.#endpoint;
    const link = this.#link;

    if (!endpoint || link.kind !== "dialing") {
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

    this.#go({
      heardAt: Date.now(),
      kind: "handshaking",
      lostSince: link.lostSince,
      stream,
    });
    // Once welcomed, the terminal holds our session: from then on a Hello
    // asks to resume it, and a terminal that doesn't have it refuses.
    this.#write(hello(this.#ticket.token, this.#id, this.#recv, this.#wasUp));
    this.#helloSent = true;
    const reader = frameReader();

    try {
      for (;;) {
        // oxlint-disable-next-line no-await-in-loop -- a stream reads in order.
        const chunk = await stream.read();

        const current = this.#link;

        // Closed while waiting: what arrives now is for nobody.
        if (current.kind !== "handshaking" && current.kind !== "up") {
          return;
        }

        if (chunk === undefined) {
          throw new Error("stream closed");
        }

        current.heardAt = Date.now();
        reader.push(chunk);

        for (let next = reader.next(); next; next = reader.next()) {
          const now = this.#link;

          if (now.kind === "up") {
            this.#onFrame(next);
          } else if (now.kind === "handshaking") {
            this.#onHandshake(next, now);
          }
        }

        this.#flush();

        if (this.#finAcked && this.#peerFinAcked) {
          this.#finish(true, "the chat is over");

          return;
        }
      }
    } catch (error) {
      // Off this stream before anything else can write to it.
      this.#lose(error instanceof FatalError ? error : String(error));
    }
  }

  #onHandshake(next: Frame, link: Link & { kind: "handshaking" }) {
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
    this.#go({ heardAt: link.heardAt, kind: "up", stream: link.stream });

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
    if (this.#link.kind !== "up") {
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
    const link = this.#link;

    if (link.kind !== "handshaking" && link.kind !== "up") {
      return;
    }

    const now = Date.now();
    const up = link.kind === "up";

    if (now - link.heardAt >= (up ? DEAD_AFTER : SETUP_TIMEOUT)) {
      // A relayed circuit can die without word; a stream on the same
      // connection would be just as dead, so drop the connection too.
      link.stream.reset();

      try {
        this.#endpoint?.disconnect(this.#ticket.peerId);
      } catch {
        // Already gone.
      }

      return;
    }

    if (up && now - this.#sentAt >= PING_INTERVAL) {
      this.#write(frame(PING));
    }
  }

  /** A stream, or the attempt at one, failed: end on a FatalError, else go
   * back to dialing. */
  #lose(error: FatalError | string) {
    // A stream failing after the end is just the endpoint closing.
    if (this.#ended) {
      return;
    }

    if (error instanceof FatalError) {
      // The stream broke the protocol: drop it rather than close it.
      this.#finish(false, error.message, false);

      return;
    }

    const reason = error;
    console.debug(`minipaw: ${reason}; retrying`);
    const link = this.#link;

    // Report the first failure only, not every retry after it.
    if (link.kind === "up") {
      this.#go({ kind: "dialing", lostSince: Date.now(), reason });
    } else if (link.kind === "handshaking") {
      this.#go({ kind: "dialing", lostSince: link.lostSince });
    }
  }

  /** Moves the link to `next` and tells the page; nothing moves it once
   * ended. Returns whether it moved. */
  #go(next: Link) {
    const prev = this.#link;

    if (prev.kind === "ended") {
      return false;
    }

    this.#link = next;

    // The timer is set before the page hears, so a handler that closes the
    // session at once clears it.
    if (next.kind === "ended") {
      clearInterval(this.#timer);
    } else if (next.kind !== "loading") {
      this.#timer ??= setInterval(() => this.#tick(), 1000);
    }

    switch (next.kind) {
      case "loading": {
        this.#on.state({ kind: "loading" });
        break;
      }

      case "dialing": {
        if (prev.kind === "loading") {
          this.#on.state({ kind: "connecting" });
        } else if (prev.kind === "up") {
          this.#on.state({ kind: "reconnecting", reason: next.reason ?? "" });
        }

        break;
      }

      case "up": {
        const resumed = this.#wasUp;
        this.#wasUp = true;
        this.#on.state({ kind: "up", resumed });
        break;
      }

      case "ended": {
        this.#on.state({ kind: "ended", ok: next.ok, reason: next.reason });
        break;
      }

      default: {
        break;
      }
    }

    return true;
  }

  /** Ends the session; `graceful` closes the stream's write side and gives
   * what's on it time to leave. */
  #finish(ok: boolean, reason: string, graceful = true) {
    const stream = graceful ? this.#stream : undefined;

    if (!this.#go({ kind: "ended", ok, reason })) {
      return;
    }

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
