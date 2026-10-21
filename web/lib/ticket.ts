// minipaw tickets, as crates/minipaw/src/ticket.rs reads them: `mp`, then
// base64url of [version 1][flags][token; 16][peer id len][peer id], then
// [relay len][relay multiaddr] when flags is odd, and nothing after.

export interface Ticket {
  /** The listener's peer ID, in its base58 text form. */
  peerId: string;
  token: Uint8Array;
  /** Whether the listener uses a relay other than the default one. */
  ownRelay: boolean;
}

/** Finds tickets inside a pasted line such as `minipaw mpAQ…`. */
export const CANDIDATE = /\bmp[\w-]+/gu;

// Whether `field` is a peer ID, as libp2p reads one: a multihash that is
// either the public key inline (identity, code 0x00, at most 42 bytes) or
// its sha2-256 (code 0x12, 32 bytes).
const isPeerId = (field: Uint8Array) =>
  (field[0] === 0x00 && field[1] === field.length - 2 && field.length <= 44) ||
  (field[0] === 0x12 && field[1] === 32 && field.length === 34);

const decode = (body: string): Uint8Array | undefined => {
  try {
    const binary = atob(body.replaceAll("-", "+").replaceAll("_", "/"));
    // atob ignores stray bits in the last character, which the CLI's
    // decoder refuses; re-encoding catches a typo there.
    const canonical = btoa(binary)
      .replaceAll("+", "-")
      .replaceAll("/", "_")
      .replace(/=+$/u, "");
    return canonical === body
      ? Uint8Array.from(binary, (char) => char.codePointAt(0) ?? 0)
      : undefined;
  } catch {
    return undefined;
  }
};

const BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

const base58 = (bytes: Uint8Array) => {
  const digits: number[] = [];
  for (const byte of bytes) {
    let carry = byte;
    for (let i = 0; i < digits.length; i += 1) {
      carry += (digits[i] ?? 0) * 256;
      digits[i] = carry % 58;
      carry = Math.floor(carry / 58);
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = Math.floor(carry / 58);
    }
  }
  let out = "";
  for (const byte of bytes) {
    if (byte !== 0) {
      break;
    }
    out += "1";
  }
  for (let i = digits.length - 1; i >= 0; i -= 1) {
    out += BASE58[digits[i] ?? 0];
  }
  return out;
};

/** Parses `text` as a whole ticket, or returns undefined. */
export const parseTicket = (text: string): Ticket | undefined => {
  if (!text.startsWith("mp")) {
    return undefined;
  }
  const bytes = decode(text.slice(2));
  if (!bytes || bytes.length < 19 || bytes[0] !== 1) {
    return undefined;
  }
  let at = 18;
  // Takes one length-prefixed field; undefined when it runs off the end.
  const field = () => {
    const len = bytes[at] ?? 0;
    const start = at + 1;
    at = start + len;
    return len > 0 && at <= bytes.length
      ? bytes.subarray(start, at)
      : undefined;
  };
  const peer = field();
  if (!peer || !isPeerId(peer)) {
    return undefined;
  }
  // The relay, which only a listener on a non-default relay embeds, is
  // checked for length only.
  const ownRelay = (bytes[1] ?? 0) % 2 === 1;
  if (ownRelay && !field()) {
    return undefined;
  }
  if (at !== bytes.length) {
    return undefined;
  }
  return { ownRelay, peerId: base58(peer), token: bytes.slice(2, 18) };
};

/** The first whole ticket in a pasted line, if any. */
export const findTicket = (line: string) => {
  for (const [candidate] of line.trim().matchAll(CANDIDATE)) {
    const ticket = parseTicket(candidate);
    if (ticket) {
      return { text: candidate, ticket };
    }
  }
};
