// The paw bot: a TCP server that `minipaw serve --forward` hands each visitor
// to, one connection per chat. It reads lines and answers each with Claude.
// It listens on loopback only; visitors reach it through minipaw and the
// relay, so nothing here is exposed to the network.
//
// Environment: ANTHROPIC_API_KEY, and optionally PAWBOT_PORT (7777),
// PAWBOT_MODEL (claude-opus-5-5), PAWBOT_DAILY_LIMIT (300 replies per UTC
// day across all visitors), PAWBOT_SESSION_LIMIT (20 replies per chat).

import net from "node:net";

import Anthropic from "@anthropic-ai/sdk";

const PORT = Number(process.env.PAWBOT_PORT ?? 7777);
const MODEL = process.env.PAWBOT_MODEL ?? "claude-opus-5-5";
const DAILY_LIMIT = Number(process.env.PAWBOT_DAILY_LIMIT ?? 300);
const SESSION_LIMIT = Number(process.env.PAWBOT_SESSION_LIMIT ?? 20);
/** Longest line we answer; the page caps its input at 500 characters too. */
const MAX_LINE = 500;
/** Lines waiting while a reply is being written; more are dropped. */
const MAX_QUEUE = 3;

const SYSTEM = `You are the paw bot, a small friendly bot on the minipaw website (paw.minip2p.com). Visitors reach you by pressing "say hi" on the landing page.

minipaw pipes stdin/stdout between two machines peer to peer, like netcat but through NATs, with no accounts and end-to-end encryption. One side runs \`minipaw\` and prints a ticket (mp…); the other runs \`minipaw <ticket>\`. It starts on a relay, hole-punches a direct QUIC path when it can, and resumes across drops for 60 s. \`minipaw serve\` keeps a stable ticket and forwards sessions to a local port (default sshd); \`minipaw ssh\` and \`minipaw cp\` use it. Install: curl -fsSL https://paw.minip2p.com/install.sh | sh (Linux and macOS), or cargo install minipaw-cli. It is built on minip2p, a small libp2p implementation in Rust.

How this very chat reaches you: the visitor's browser runs minip2p compiled to WebAssembly and dials relay.minip2p.com over WebTransport. The relay bridges a circuit to \`minipaw serve\` on your server, which forwards the session to you over local TCP. Browsers can't hole-punch, so it stays on the relay, which only sees ciphertext.

Your replies show up as plain text lines in a small terminal-style chat pane. Write one to three short sentences, no Markdown, no headings or bullet lists, no code fences (inline commands are fine as plain text). Be warm and a little playful, but useful first. If you don't know something about minipaw, say so rather than guessing, and point to https://paw.minip2p.com/docs. Latency-sensitive; begin your visible answer immediately.`;

const GREETING =
  "🐾 hi! I'm the paw bot, answering from a server through minipaw. Ask me how this chat reaches me, or anything else.";

const client = new Anthropic();

// Replies across all chats today, against DAILY_LIMIT.
let day = "";
let repliesToday = 0;

const takeDailyBudget = () => {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== day) {
    day = today;
    repliesToday = 0;
  }
  if (repliesToday >= DAILY_LIMIT) {return false;}
  repliesToday += 1;
  return true;
};

const textOf = (message: Anthropic.Beta.BetaMessage) =>
  message.content
    .flatMap((block) => (block.type === "text" ? [block.text] : []))
    .join("")
    .trim();

/** Claude's answer to the chat so far, or a line saying why there's none. */
const reply = async (history: Anthropic.Beta.BetaMessageParam[], signal: AbortSignal) => {
  try {
    const message = await client.beta.messages.create(
      {
      betas: ["server-side-fallback-2026-07-01"],
      // On a safety decline, rerun on Anthropic's recommended fallback model.
      fallbacks: "default",
      // Short chat replies; low effort keeps them quick and cheap.
      max_tokens: 2048,
      messages: history,
      model: MODEL,
      output_config: { effort: "low" },
      system: SYSTEM,
      },
      { signal }
    );
    if (message.stop_reason === "refusal") {
      return { ok: false, text: "I'd rather not answer that one. Ask me something else?" };
    }
    const text = textOf(message);
    return text ? { ok: true, text } : { ok: false, text: "…I lost my train of thought. Try again?" };
  } catch (error) {
    // The visitor left mid-request; nobody is waiting for an answer.
    if (signal.aborted) {
      return { ok: false, text: "" };
    }
    if (error instanceof Anthropic.RateLimitError) {
      return { ok: false, text: "Lots of paws at once right now. Try again in a minute." };
    }
    if (error instanceof Anthropic.APIError) {
      console.error(`claude: ${error.status ?? "?"} ${error.name}`);
    } else {
      console.error(`claude: ${String(error)}`);
    }
    return { ok: false, text: "My paws slipped (an API error). Try again in a bit." };
  }
};

let sessions = 0;

const serve = (socket: net.Socket) => {
  sessions += 1;
  const id = sessions;
  const started = Date.now();
  const history: Anthropic.Beta.BetaMessageParam[] = [];
  const queue: string[] = [];
  let replies = 0;
  let busy = false;
  let partial = "";
  // Dropping a line too long to answer, up to its next newline.
  let skipping = false;
  // Aborted when the visitor leaves, along with any request in flight.
  const gone = new AbortController();

  const write = (text: string) => {
    if (!socket.writable) {return;}
    // One line per line; the pane shows each as its own message.
    socket.write(`${text.replaceAll("\r", "").replaceAll(/\n+/gu, "\n")}\n`);
  };

  const finish = (text: string) => {
    write(text);
    socket.end();
  };

  const answer = async (line: string) => {
    if (replies >= SESSION_LIMIT) {
      finish("That's all the paw time this chat gets. Thanks for stopping by! 🐾");
      return;
    }
    if (!takeDailyBudget()) {
      finish("I've used up today's treats (the daily reply budget). Come back tomorrow! 🐾");
      return;
    }
    replies += 1;
    history.push({ content: line, role: "user" });
    const result = await reply(history, gone.signal);
    if (gone.signal.aborted) {
      return;
    }
    if (result.ok) {
      history.push({ content: result.text, role: "assistant" });
    } else {
      // Keep turns alternating: a turn with no answer leaves no trace.
      history.pop();
    }
    write(result.text);
  };

  const drain = async () => {
    if (busy) {return;}
    busy = true;
    for (let line = queue.shift(); line !== undefined && !gone.signal.aborted; line = queue.shift()) {
      // oxlint-disable-next-line no-await-in-loop -- a chat answers in order.
      await answer(line);
    }
    busy = false;
  };

  const tooLong = () => write(`That's a long one; I read up to ${MAX_LINE} characters per message.`);

  const onLine = (raw: string) => {
    const line = raw.trim();
    if (line === "") {return;}
    if (line.length > MAX_LINE) {
      tooLong();
      return;
    }
    if (queue.length >= MAX_QUEUE) {return;}
    queue.push(line);
    void drain();
  };

  console.log(`[${id}] connected`);
  socket.setEncoding("utf8");
  write(GREETING);
  socket.on("data", (chunk: string) => {
    const lines = `${partial}${chunk}`.split("\n");
    partial = lines.pop() ?? "";
    // The first piece ends the line we were dropping.
    if (skipping && lines.length > 0) {
      lines.shift();
      skipping = false;
    }
    for (const line of lines) {onLine(line);}
    // A line already past the limit can't come back under it: drop it all,
    // rather than answer whatever follows its cut.
    if (partial.length > MAX_LINE) {
      if (!skipping) {tooLong();}
      skipping = true;
      partial = "";
    }
  });
  const leave = () => {
    queue.length = 0;
    gone.abort();
  };
  // The visitor left (their Fin, through minipaw): stop answering, and
  // finish our side too.
  socket.on("end", () => {
    leave();
    socket.end();
  });
  socket.on("error", (error) => console.error(`[${id}] ${error.message}`));
  socket.on("close", () => {
    leave();
    const secs = Math.round((Date.now() - started) / 1000);
    console.log(`[${id}] closed after ${secs}s, ${replies} replies (${repliesToday} today)`);
  });
};

net.createServer(serve).listen(PORT, "127.0.0.1", () => {
  console.log(`pawbot on 127.0.0.1:${PORT}, ${MODEL}, ${DAILY_LIMIT} replies a day`);
});
