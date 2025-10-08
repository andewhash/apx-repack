import * as fs from "fs";
import * as path from "path";
import * as zlib from "zlib";

export enum DSpec { ext=0,u8=1,u16=2,u24=3,u32=4,u64=5,f16=6,f32=7,f64=8,Null=9,a16=10,a32=11 }
export enum ExtId { stop=0, ts=1, dir=2, field=3, evtid=4, evt=8, jso=9, raw=10, zip=11 }

// ----- helpers -----
function cstr(s: string): Buffer {
  return Buffer.concat([Buffer.from(s, "utf8"), Buffer.from([0x00])]);
}

/** APX "qCompress" payload: [len_be(4)] + deflate(data) */
function qCompress(payload: Buffer): Buffer {
  const comp = zlib.deflateSync(payload);
  const out = Buffer.allocUnsafe(4 + comp.length);
  out.writeUInt32BE(payload.length >>> 0, 0);
  comp.copy(out, 4);
  return out;
}

// half-float encode/decode helpers
function f32ToF16Bits(val: number): number {
  const f32 = new Float32Array(1);
  const u32 = new Uint32Array(f32.buffer);
  f32[0] = val;
  const x = u32[0];

  const sign = (x >>> 31) & 0x1;
  const exp  = (x >>> 23) & 0xff;
  const mant = x & 0x7fffff;

  if (exp === 0xff) { // Inf/NaN
    const isNaN = mant !== 0;
    return (sign << 15) | (0x1f << 10) | (isNaN ? 0x200 : 0);
  }

  let exp16 = exp - 127 + 15;

  if (exp <= 112) { // subnormal or zero
    if (exp < 103) return (sign << 15);
    const shift = 113 - exp;
    let mant16 = (0x800000 | mant) >>> shift;
    if ((mant16 & 0x1) && ((mant & ((1 << (shift - 1)) - 1)) !== 0)) mant16++;
    return (sign << 15) | mant16;
  } else if (exp16 >= 0x1f) {
    return (sign << 15) | (0x1f << 10);
  } else {
    let mant16 = mant >>> 13;
    const roundBit = (mant >>> 12) & 1;
    if (roundBit && ((mant16 & 1) || (mant & 0xFFF))) mant16++;
    if (mant16 === 0x400) {
      mant16 = 0;
      exp16++;
      if (exp16 >= 0x1f) return (sign << 15) | (0x1f << 10);
    }
    return (sign << 15) | ((exp16 & 0x1f) << 10) | (mant16 & 0x3ff);
  }
}

function f16ToF32Approx(bits: number): number {
  const s = (bits & 0x8000) ? -1 : 1;
  const e = (bits >>> 10) & 0x1f;
  const f = bits & 0x3ff;
  if (e === 0) return s * Math.pow(2, -14) * (f / 1024);
  if (e === 31) return f ? NaN : s * Infinity;
  return s * Math.pow(2, e - 15) * (1 + f / 1024);
}

/** choose f16 when exactly reversible (within strict equality), else f32 */
function chooseFloatSpec(val: number): { dspec: DSpec; writer: (buf: Buffer, off: number)=>void; size: number } {
  if (Number.isFinite(val)) {
    const h = f32ToF16Bits(val);
    const back = f16ToF32Approx(h);
    if (Object.is(back, val)) {
      return { dspec: DSpec.f16, writer: (b, off) => b.writeUInt16LE(h, off), size: 2 };
    }
  }
  return { dspec: DSpec.f32, writer: (b, off) => b.writeFloatLE(val, off), size: 4 };
}

// ----- writer -----
export class ApxTlmWriter {
  private declaredFields = 0;
  private headerWritten = false;
  private ws: fs.WriteStream;

  private lastWidx: number = -1;
  private lastDown = new Map<number, number>();
  private lastUp = new Map<number, number>();

  constructor(
    private version: number = 1,                 // version 1 for repack
    private utcOffsetSeconds: number = 0,         // seconds
    private startTimestampMs64: number = 0,       // ms epoch
    outFilePath?: string,
    outStream?: fs.WriteStream
  ) {
    if (outStream) {
      this.ws = outStream;
    } else if (outFilePath) {
      const dir = path.dirname(outFilePath);
      fs.mkdirSync(dir, { recursive: true });
      this.ws = fs.createWriteStream(outFilePath, { highWaterMark: 100 * 1024 });
    } else {
      throw new Error("ApxTlmWriter: provide outFilePath or outStream");
    }
  }

  private write(buf: Buffer) { this.ws.write(buf); }
  private pushExt(id: ExtId) { this.write(Buffer.from([(id << 4) | 0x00])); }

  /** Fixed-size header placeholder. */
  writeHeaderPlaceholder() {
    if (this.headerWritten) return;
    const b = Buffer.alloc(44, 0);
    b.write("APXTLM", 0, "ascii");
    b.writeUInt16LE(this.version & 0xFFFF, 16);
    b.writeUInt16LE(44, 18);
    b.writeBigUInt64LE(BigInt(this.startTimestampMs64), 32);
    b.writeInt32LE(this.utcOffsetSeconds | 0, 40);
    this.write(b);
    this.headerWritten = true;
  }

  // ----- info -----
  emitInfo(info: any) {
    if (typeof info === "object" && info) {
      (info as any).utc_offset = this.utcOffsetSeconds | 0;
      if ((info as any).timestamp == null) (info as any).timestamp = this.startTimestampMs64 >>> 0;
    }
    this.pushExt(ExtId.jso);
    const nameLit = this.cachedLit("info");
    const payload = Buffer.from(JSON.stringify(info), "utf8");
    const q = qCompress(payload);
    const sz = Buffer.allocUnsafe(4);
    sz.writeUInt32LE(q.length, 0);
    this.write(Buffer.concat([nameLit, sz, q]));
  }

  // ----- registry -----
  emitField(name: string, info: string[] = []) {
    this.pushExt(ExtId.field);
    const parts: Buffer[] = [ cstr(name), Buffer.from([info.length & 0xFF]) ];
    for (const s of info) parts.push(cstr(s));
    this.write(Buffer.concat(parts));
    this.declaredFields++;
  }

  emitEvtId(name: string, keys: string[]) {
    this.pushExt(ExtId.evtid);
    const parts: Buffer[] = [ cstr(name), Buffer.from([keys.length & 0xFF]) ];
    for (const k of keys) parts.push(cstr(k));
    this.write(Buffer.concat(parts));
  }

  // ----- timestamp -----
  emitTs(ms: number) {
    this.pushExt(ExtId.ts);
    const b = Buffer.allocUnsafe(4);
    b.writeUInt32LE(ms >>> 0, 0);
    this.write(b);
    this.lastWidx = -1;
  }

  // ----- values (optimized) -----
  private writeIndexAndSpec(dspec: DSpec, fieldIndex: number) {
    if (this.lastWidx >= 0) {
      const delta = fieldIndex - this.lastWidx - 1;
      if (delta >= 0 && delta <= 7) {
        const b0 = 0x10 | ((delta & 0x07) << 5) | (dspec & 0x0F); // opt8
        this.write(Buffer.from([b0]));
        this.lastWidx = fieldIndex;
        return;
      }
    }
    const low3 = fieldIndex & 0x07;
    const hi   = (fieldIndex >> 3) & 0xFF;
    const b0   = (low3 << 5) | (dspec & 0x0F);
    const b1   = hi;
    this.write(Buffer.from([b0, b1]));
    this.lastWidx = fieldIndex;
  }

  /** write numeric value with change filtering and best packing */
  emitNumber(fieldIndex: number, v: number, uplink = false) {
    if (!(fieldIndex >= 0 && fieldIndex < this.declaredFields)) return;

    const cache = uplink ? this.lastUp : this.lastDown;
    const prev = cache.get(fieldIndex);
    if (prev !== undefined && Object.is(prev, v)) return;
    cache.set(fieldIndex, v);

    if (uplink) this.pushExt(ExtId.dir);

    const { dspec, writer, size } = chooseFloatSpec(v);
    this.writeIndexAndSpec(dspec, fieldIndex);
    const b = Buffer.allocUnsafe(size);
    writer(b, 0);
    this.write(b);
  }

  // legacy API
  emitValueF32(fieldIndex: number, v: number) { this.emitNumber(fieldIndex, v, false); }
  emitUplinkValueF32(fieldIndex: number, v: number) { this.emitNumber(fieldIndex, v, true); }

  // ----- strings/events/objects -----
  private cachedLit(s: string): Buffer {
    return Buffer.concat([ Buffer.from([0xFF]), cstr(s) ]);
  }

  emitEvt(evIndex: number, values: string[]) {
    this.pushExt(ExtId.evt);
    const parts: Buffer[] = [ Buffer.from([evIndex & 0xFF]) ];
    for (const v of values) parts.push(this.cachedLit(v ?? ""));
    this.write(Buffer.concat(parts));
  }

  emitJso(name: string, obj: any, ts?: number) {
    if (typeof ts === "number") this.emitTs(ts >>> 0);
    this.pushExt(ExtId.jso);
    const nameLit = this.cachedLit(name);
    const payload = Buffer.from(JSON.stringify(obj), "utf8");
    const q = qCompress(payload);
    const sz = Buffer.allocUnsafe(4);
    sz.writeUInt32LE(q.length, 0);
    this.write(Buffer.concat([nameLit, sz, q]));
  }

  /** choose RAW or ZIP automatically (ZIP if smaller) */
  emitRaw(name: string, data: Buffer, ts?: number) {
    if (typeof ts === "number") this.emitTs(ts >>> 0);
    const nameLit = this.cachedLit(name);

    const comp = zlib.deflateSync(data);
    const zipped = Buffer.concat([Buffer.alloc(4), comp]);
    zipped.writeUInt32BE(data.length >>> 0, 0); // qCompress header

    const useZip = zipped.length < data.length + 2;

    if (useZip) {
      this.pushExt(ExtId.zip);
      const sz = Buffer.allocUnsafe(4);
      sz.writeUInt32LE(zipped.length, 0);
      this.write(Buffer.concat([nameLit, sz, zipped]));
    } else {
      this.pushExt(ExtId.raw);
      if (data.length > 0xFFFF) {
        let off = 0;
        while (off < data.length) {
          const chunk = data.subarray(off, Math.min(off + 0xFFFF, data.length));
          const sz = Buffer.allocUnsafe(2);
          sz.writeUInt16LE(chunk.length, 0);
          this.write(Buffer.concat([nameLit, sz, chunk]));
          off += chunk.length;
        }
      } else {
        const sz = Buffer.allocUnsafe(2);
        sz.writeUInt16LE(data.length, 0);
        this.write(Buffer.concat([nameLit, sz, data]));
      }
    }
  }

  finalizeToFile(): Promise<void> {
    this.write(Buffer.from([0x00])); // ExtId.stop
    return new Promise<void>((resolve, reject) => {
      this.ws.once("error", reject);
      this.ws.once("finish", resolve);
      this.ws.end();
    });
  }

  getDeclaredFieldCount() { return this.declaredFields; }
}
