// Frames on a `/minipaw/pipe/1` stream, as crates/minipaw/src/wire.rs
// writes them: `[kind u8][len u32 BE][payload]`. The dialer opens every
// stream with Hello and the listener answers Welcome; both then exchange
// Data, Ack, Fin and Ping. Offsets count payload bytes of the whole
// session, not of one stream, so a session survives its stream being
// replaced.

export const PROTOCOL = "/minipaw/pipe/1";
/** The session protocol version a Hello carries; the CLI must match. */
const VERSION = 3;

export const HELLO = 1;
export const WELCOME = 2;
export const DATA = 3;
export const ACK = 4;
export const FIN = 5;
export const ERROR = 6;
export const PING = 7;

/** Largest Data payload we send. */
export const MAX_DATA = 32 * 1024;
/** Largest payload we accept for any frame. */
const MAX_PAYLOAD = 64 * 1024;
const HEADER_LEN = 5;

/** A refusal or protocol violation: retrying on a new stream won't help. */
export class FatalError extends Error {
  override name = "FatalError";
}

export type Frame =
  | { kind: typeof WELCOME; recv: number }
  | { kind: typeof DATA; data: Uint8Array }
  | { kind: typeof ACK; offset: number; fin: boolean }
  | { kind: typeof FIN; offset: number }
  | { kind: typeof ERROR; message: string }
  | { kind: typeof PING };

export const frame = (kind: number, payload: Uint8Array = new Uint8Array()) => {
  const out = new Uint8Array(HEADER_LEN + payload.length);
  const view = new DataView(out.buffer);
  view.setUint8(0, kind);
  view.setUint32(1, payload.length);
  out.set(payload, HEADER_LEN);
  return out;
};

/** `value` as a big-endian u64, with `extra` zero bytes after it. */
export const u64 = (value: number, extra = 0) => {
  const out = new Uint8Array(8 + extra);
  new DataView(out.buffer).setBigUint64(0, BigInt(value));
  return out;
};

export const hello = (
  token: Uint8Array,
  session: Uint8Array,
  recv: number,
  resume: boolean
) => {
  const payload = new Uint8Array(1 + 16 + 16 + 8 + 1);
  payload[0] = VERSION;
  payload.set(token, 1);
  payload.set(session, 17);
  payload.set(u64(recv), 33);
  payload[41] = resume ? 1 : 0;
  return frame(HELLO, payload);
};

export const ack = (offset: number, fin: boolean) => {
  const payload = u64(offset, 1);
  payload[8] = fin ? 1 : 0;
  return frame(ACK, payload);
};

const decode = (kind: number, payload: Uint8Array): Frame => {
  const view = new DataView(
    payload.buffer,
    payload.byteOffset,
    payload.byteLength
  );
  const sized = (len: number) => {
    if (payload.length !== len) {
      throw new FatalError(`malformed frame kind ${kind}`);
    }
    return len > 0 ? Number(view.getBigUint64(0)) : 0;
  };
  switch (kind) {
    case WELCOME: {
      return { kind, recv: sized(8) };
    }
    case DATA: {
      return { data: payload, kind };
    }
    case ACK: {
      return { fin: payload[8] !== 0, kind, offset: sized(9) };
    }
    case FIN: {
      return { kind, offset: sized(8) };
    }
    case ERROR: {
      return { kind, message: new TextDecoder().decode(payload) };
    }
    case PING: {
      sized(0);
      return { kind };
    }
    default: {
      throw new FatalError(`unexpected frame kind ${kind}`);
    }
  }
};

/** Reassembles frames from stream chunks, which may split or coalesce them. */
export const frameReader = () => {
  let buf = new Uint8Array();
  return {
    next: (): Frame | undefined => {
      if (buf.length < HEADER_LEN) {
        return;
      }
      const view = new DataView(buf.buffer, buf.byteOffset);
      const len = view.getUint32(1);
      if (len > MAX_PAYLOAD) {
        throw new FatalError(
          `frame of ${len} bytes exceeds the ${MAX_PAYLOAD} byte limit`
        );
      }
      if (buf.length < HEADER_LEN + len) {
        return;
      }
      const kind = view.getUint8(0);
      const payload = buf.slice(HEADER_LEN, HEADER_LEN + len);
      buf = buf.subarray(HEADER_LEN + len);
      return decode(kind, payload);
    },
    push: (chunk: Uint8Array) => {
      const next = new Uint8Array(buf.length + chunk.length);
      next.set(buf);
      next.set(chunk, buf.length);
      buf = next;
    },
  };
};
