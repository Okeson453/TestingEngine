/**
 * Engine.IO v3 custom binary frames used by BC.Game crash (`/g/cm`).
 * Hot path is allocation-light: no full-buffer UTF-8, no BigInt varints.
 */

export type ParsedPacket =
  | { kind: "connect"; nsp: string }
  | { kind: "event"; nsp: string; event: string; payload: Uint8Array }
  | { kind: "other" };

const te = new TextEncoder();
const td = new TextDecoder();

export const NSP = "/g/cm";

export function encodeConnect(nsp: string): Uint8Array {
  const nspBuf = te.encode(nsp);
  const out = new Uint8Array(3 + nspBuf.length + 1);
  out[0] = 0x04;
  out[1] = 0x00;
  out[2] = nspBuf.length;
  out.set(nspBuf, 3);
  out[3 + nspBuf.length] = 0;
  return out;
}

export function encodeJoin(nsp: string, room = "join"): Uint8Array {
  const nspBuf = te.encode(nsp);
  const ev = te.encode(room);
  const out = new Uint8Array(7 + nspBuf.length + 1 + ev.length);
  out[0] = 0x04;
  out[1] = 0x82;
  out[6] = nspBuf.length;
  out.set(nspBuf, 7);
  const evOff = 7 + nspBuf.length;
  out[evOff] = ev.length;
  out.set(ev, evOff + 1);
  return out;
}

export function encodeEvent(nsp: string, event: string, payload: Uint8Array): Uint8Array {
  const nspBuf = te.encode(nsp);
  const ev = te.encode(event);
  const out = new Uint8Array(3 + nspBuf.length + 1 + ev.length + payload.length);
  out[0] = 0x04;
  out[1] = 0x02;
  out[2] = nspBuf.length;
  out.set(nspBuf, 3);
  let o = 3 + nspBuf.length;
  out[o++] = ev.length;
  out.set(ev, o);
  o += ev.length;
  out.set(payload, o);
  return out;
}

export function parsePacket(buf: Uint8Array): ParsedPacket {
  if (buf.length < 2 || buf[0] !== 0x04) return { kind: "other" };
  const type = buf[1]!;
  const base = type & 0x0f;
  let offset = 2;
  if (type & 0x80) offset += 4;
  if (offset >= buf.length) return { kind: "other" };
  const nspLen = buf[offset]!;
  offset += 1;
  if (offset + nspLen > buf.length) return { kind: "other" };
  const nsp = td.decode(buf.subarray(offset, offset + nspLen));
  offset += nspLen;
  if (base === 0) return { kind: "connect", nsp };
  if (offset >= buf.length) return { kind: "connect", nsp };
  const evLen = buf[offset]!;
  offset += 1;
  if (offset + evLen > buf.length) return { kind: "other" };
  const event = td.decode(buf.subarray(offset, offset + evLen));
  offset += evLen;
  return { kind: "event", nsp, event, payload: buf.subarray(offset) };
}

/** Fast unsigned varint. Avoids BigInt — crash fields fit in 53 bits. */
export function readVarint(buf: Uint8Array, offset: number): { value: number; offset: number } {
  const b0 = buf[offset];
  if (b0 === undefined) return { value: 0, offset };
  if (b0 < 0x80) return { value: b0, offset: offset + 1 };
  const b1 = buf[offset + 1];
  if (b1 !== undefined && b1 < 0x80) {
    return { value: (b0 & 0x7f) | (b1 << 7), offset: offset + 2 };
  }
  let x = b0 & 0x7f;
  let shift = 7;
  offset += 1;
  while (offset < buf.length) {
    const b = buf[offset]!;
    offset += 1;
    x += (b & 0x7f) * 2 ** shift;
    if ((b & 0x80) === 0) break;
    shift += 7;
    if (shift > 53) break;
  }
  return { value: x, offset };
}

export function writeVarint(n: number, out: number[]): void {
  let x = Math.max(0, Math.floor(n));
  while (x > 0x7f) {
    out.push((x & 0x7f) | 0x80);
    x = Math.floor(x / 128);
  }
  out.push(x);
}

export function decodeProtobuf(buf: Uint8Array): Record<number, number | string> {
  const fields: Record<number, number | string> = {};
  let offset = 0;
  const len = buf.length;
  while (offset < len) {
    const tag = readVarint(buf, offset);
    offset = tag.offset;
    const field = tag.value >> 3;
    const wire = tag.value & 7;
    if (wire === 0) {
      const value = readVarint(buf, offset);
      offset = value.offset;
      fields[field] = value.value;
    } else if (wire === 2) {
      const size = readVarint(buf, offset);
      offset = size.offset;
      const end = offset + size.value;
      if (end > len) break;
      fields[field] = td.decode(buf.subarray(offset, end));
      offset = end;
    } else {
      break;
    }
  }
  return fields;
}

/** Hottest path: `pg` is almost always field 1 (elapsedMs) as a 1–2 byte varint. */
export function decodeProgressElapsed(buf: Uint8Array): number | null {
  if (buf.length === 0) return null;
  if (buf[0] === 8) {
    const v = readVarint(buf, 1);
    return v.value;
  }
  const fields = decodeProtobuf(buf);
  return typeof fields[1] === "number" ? fields[1] : null;
}

export function encodeProtobuf(fields: Array<[number, number | string]>): Uint8Array {
  const out: number[] = [];
  for (const [field, value] of fields) {
    if (typeof value === "number") {
      writeVarint((field << 3) | 0, out);
      writeVarint(value, out);
    } else {
      writeVarint((field << 3) | 2, out);
      const bytes = te.encode(value);
      writeVarint(bytes.length, out);
      for (let i = 0; i < bytes.length; i++) out.push(bytes[i]!);
    }
  }
  return new Uint8Array(out);
}

export function multiplierFromElapsed(elapsedMs: number): number {
  const x = Math.floor(100 * Math.exp(6e-5 * Math.max(0, elapsedMs))) / 100;
  return x < 1 ? 1 : x;
}

export function elapsedFromMultiplier(multiplier: number): number {
  if (multiplier <= 1) return 0;
  return Math.log(multiplier) / 6e-5;
}

export function fieldsToPayload(event: string, fields: Record<number, number | string>) {
  if (event === "pg") {
    const elapsedMs = typeof fields[1] === "number" ? fields[1] : null;
    return {
      gameId: null as string | null,
      multiplier: elapsedMs === null ? null : multiplierFromElapsed(elapsedMs),
      hash: null as string | null,
      beginTime: null as number | null,
      endTime: null as number | null,
      elapsedMs,
    };
  }
  const gameId = fields[1];
  const multiplierHundredths = typeof fields[6] === "number" ? fields[6] : null;
  const hash = typeof fields[7] === "string" ? fields[7] : null;
  const beganAt =
    typeof fields[3] === "number" ? fields[3] : typeof fields[4] === "number" ? fields[4] : null;
  return {
    gameId: typeof gameId === "number" ? String(gameId) : typeof gameId === "string" ? gameId : null,
    multiplier: multiplierHundredths,
    hash,
    beginTime: beganAt,
    endTime: event === "ed" || event === "st" ? Date.now() : null,
    elapsedMs: null as number | null,
  };
}

export function isEngineOpenFrame(buf: Uint8Array): boolean {
  return buf.length > 1 && buf[0] === 0x30 && buf[1] === 0x7b; // 0{
}

export function isEnginePing(buf: Uint8Array): boolean {
  return buf.length === 1 && buf[0] === 0x32; // 2
}

export function isEnginePong(buf: Uint8Array): boolean {
  return buf.length === 1 && buf[0] === 0x33; // 3
}
