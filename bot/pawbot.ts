// The paw bot: a TCP server that `minipaw serve --forward` hands each visitor
// to, one connection per chat. It reads lines and answers each with Claude.
// It listens on loopback only; visitors reach it through minipaw and the
// relay, so nothing here is exposed to the network.
//
// Runs on Bun, compiled to one executable (`bun run build`); run.sh starts it
// beside `minipaw serve`.
//
// Environment: ANTHROPIC_API_KEY, and optionally PAWBOT_PORT (7777),
// PAWBOT_MODEL (claude-haiku-5-5), PAWBOT_DAILY_LIMIT (300 replies per UTC
// day across all visitors), PAWBOT_SESSION_LIMIT (20 replies per chat), and
// PAWBOT_STATE, a file that keeps the day's count across restarts (run.sh
// sets one beside the identity; unset, the count lives in memory only).

import { readFileSync, writeFileSync } from "node:fs";
import net from "node:net";

import Anthropic from "@anthropic-ai/sdk";

/** A whole-number setting of at least `min`, or `fallback` when it's unset,
 * blank or malformed: a NaN limit would compare false and switch the cap off,
 * and a blank port would read as 0. run.sh reads PAWBOT_PORT by the
 * same rule, so both processes agree on it. */
const setting = (name: string, fallback: number, min = 0) => {
  const raw = process.env[name]?.trim() ?? "";
  const value = Number(raw);
  return /^\d+$/u.test(raw) && value >= min ? value : fallback;
};

const PORT = setting("PAWBOT_PORT", 7777, 1);
const MODEL = process.env.PAWBOT_MODEL ?? "claude-haiku-5-5";
const DAILY_LIMIT = setting("PAWBOT_DAILY_LIMIT", 300);
const SESSION_LIMIT = setting("PAWBOT_SESSION_LIMIT", 20);
/** Longest line we answer; the page caps its input at 500 characters too. */
const MAX_LINE = 500;
/** Lines waiting while a reply is being written; more are dropped. */
const MAX_QUEUE = 3;
/** A chat with no traffic either way this long is closed, so a tab left
 * open doesn't hold one of serve's session slots. */
const IDLE = 5 * 60_000;
/** Output a visitor hasn't read yet before we hang up on them. Replies are
 * short, so only a peer that stopped reading gets near it. */
const MAX_UNREAD = 64 * 1024;

// Short on purpose: it rides along with every reply. The docs answer the
// questions, so the bot points to them instead of carrying the facts.
const SYSTEM = `You are the paw bot on paw.minip2p.com, minipaw's website. Reply in one or two short, friendly sentences; your replies show as plain text in a small chat pane.

When asked about minipaw or minip2p, don't explain it or say what you don't know: point them warmly to the docs, where they can read it directly. minipaw: https://paw.minip2p.com/docs, minip2p: https://minip2p.com/intro.`;

const GREETING =
  "🐾 hi! I'm the paw bot, answering from a server through minipaw. Ask me how this chat reaches me, or anything else.";

const client = new Anthropic();

const STATE = process.env.PAWBOT_STATE;

// Replies across all chats today, against DAILY_LIMIT. Saved to STATE on
// every reply, so a restart or redeploy doesn't hand out a fresh budget.
let day = "";
let repliesToday = 0;

if (STATE) {
  try {
    const saved: unknown = JSON.parse(readFileSync(STATE, "utf8"));
    if (saved && typeof saved === "object" && "day" in saved && "replies" in saved) {
      day = String(saved.day);
      repliesToday = Number(saved.replies) || 0;
    }
  } catch {
    // No count saved yet (or unreadable): start the day from zero.
  }
}

const saveBudget = () => {
  if (!STATE) {return;}
  try {
    writeFileSync(STATE, JSON.stringify({ day, replies: repliesToday }));
  } catch (error) {
    console.error(`state: ${String(error)}`);
  }
};

const takeDailyBudget = () => {
  const today = new Date().toISOString().slice(0, 10);
  if (today !== day) {
    day = today;
    repliesToday = 0;
  }
  if (repliesToday >= DAILY_LIMIT) {return false;}
  repliesToday += 1;
  saveBudget();
  return true;
};

const textOf = (message: Anthropic.Message) =>
  message.content
    .flatMap((block) => (block.type === "text" ? [block.text] : []))
    .join("")
    .trim();

/** Claude's answer to the chat so far, or a line saying why there's none. */
const reply = async (history: Anthropic.MessageParam[], signal: AbortSignal) => {
  try {
    const message = await client.messages.create(
      {
        // Short chat replies; low effort keeps them quick and cheap.
        max_tokens: 1024,
        messages: history,
        model: MODEL,
        output_config: { effort: "low" },
        system: SYSTEM,
      },
      { signal }
    );
    // Haiku has no server-side refusal fallback, so a decline ends here.
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
  const history: Anthropic.MessageParam[] = [];
  const queue: string[] = [];
  let replies = 0;
  let busy = false;
  let partial = "";
  // Dropping a line too long to answer, up to its next newline.
  let skipping = false;
  // Aborted when the visitor leaves, along with any request in flight.
  const gone = new AbortController();
  const leave = () => {
    queue.length = 0;
    gone.abort();
  };

  const write = (text: string) => {
    if (!socket.writable) {return;}
    if (socket.writableLength > MAX_UNREAD) {
      console.log(`[${id}] not reading; hanging up`);
      socket.destroy();
      return;
    }
    // One line per line; the pane shows each as its own message.
    socket.write(`${text.replaceAll("\r", "").replaceAll(/\n+/gu, "\n")}\n`);
  };

  /** How long a closing chat waits for its last line to be read. */
  const LINGER = 5000;

  // Ends the chat from our side: says why, stops all work, and closes. A
  // half-close alone would leave the session up for as long as the visitor
  // keeps its side open, holding one of serve's slots.
  const finish = (text: string) => {
    write(text);
    leave();
    socket.end();
    // A reset, not a plain close: the FIN already went out, so closing would
    // tell serve nothing more, and it would keep the session.
    setTimeout(() => socket.resetAndDestroy(), LINGER).unref();
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
    try {
      for (let line = queue.shift(); line !== undefined && !gone.signal.aborted; line = queue.shift()) {
        // oxlint-disable-next-line no-await-in-loop -- a chat answers in order.
        await answer(line);
      }
    } finally {
      busy = false;
    }
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
  socket.setTimeout(IDLE, () => finish("I'm dozing off after 5 quiet minutes. Type again to wake me. 🐾"));
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
  console.log(
    `pawbot on 127.0.0.1:${PORT}, ${MODEL}, ${DAILY_LIMIT} replies a day (${repliesToday} used ${day || "today"})`
  );
});
