import * as fs from "fs";
import * as path from "path";
import * as zlib from "zlib";

export enum DSpec { ext=0,u8=1,u16=2,u24=3,u32=4,u64=5,f16=6,f32=7,f64=8,Null=9,a16=10,a32=11 }
export enum ExtId { stop=0, ts=1, dir=2, field=3, evtid=4, evt=8, jso=9, raw=10, zip=11 }

function cstr(s: string): Buffer {
  return Buffer.concat([Buffer.from(s,'utf8'), Buffer.from([0x00])]);
}

function dspecByte(dspec: DSpec, vidx: number): [number, number] {
  const low3 = vidx & 0x07;
  const hi   = (vidx >> 3) & 0xFF;
  const b0   = (low3 << 5) | (dspec & 0x0F);
  const b1   = hi;
  return [b0, b1];
}

function qCompress(payload: Buffer): Buffer {
  const comp = zlib.deflateSync(payload);
  const out = Buffer.allocUnsafe(4 + comp.length);
  out.writeUInt32BE(payload.length, 0);
  comp.copy(out, 4);
  return out;
}

export class ApxTlmWriter {
  private declaredFields = 0;
  private headerWritten = false;
  private ws: fs.WriteStream;

  constructor(
    private version: number = 0x0100,
    private utcOffset: number = 0,
    private startTimestampMs64: number = 0,
    outFilePath?: string,
    outStream?: fs.WriteStream
  ) {
    if (outStream) {
      this.ws = outStream;
    } else if (outFilePath) {
      const dir = path.dirname(outFilePath);
      fs.mkdirSync(dir, { recursive: true });
      this.ws = fs.createWriteStream(outFilePath, { highWaterMark: 100*1024 });
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
    b.write("APXTLM", 0, "ascii"); // magic
    b.writeUInt16LE(1, 16); // версия файла 1
    b.writeUInt16LE(44, 18); // payload_offset
    b.writeBigUInt64LE(BigInt(this.startTimestampMs64 >>> 0), 32); // timestamp
    b.writeInt32LE(this.utcOffset | 0, 40); // utc_offset
    this.write(b);
    this.headerWritten = true;
  }

  // Now, let's handle the `info` section.
  emitInfo(info: any) {
    this.pushExt(ExtId.jso);
    const nameLit = this.cachedLit("info");
    const payload = Buffer.from(JSON.stringify(info), "utf8");
    const q = qCompress(payload);
    const sz = Buffer.allocUnsafe(4);
    sz.writeUInt32LE(q.length, 0);
    this.write(Buffer.concat([nameLit, sz, q]));
  }

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

  emitTs(ms: number) {
    this.pushExt(ExtId.ts);
    const b = Buffer.allocUnsafe(4);
    b.writeUInt32LE(ms >>> 0, 0);
    this.write(b);
  }

  emitValueF32(fieldIndex: number, v: number) {
    if (!(fieldIndex >= 0 && fieldIndex < this.declaredFields)) return;
    const [b0, b1] = dspecByte(DSpec.f32, fieldIndex);
    const b = Buffer.allocUnsafe(6);
    b[0] = b0; b[1] = b1;
    b.writeFloatLE(v, 2);
    this.write(b);
  }

  emitUplinkValueF32(fieldIndex: number, v: number) {
    this.pushExt(ExtId.dir);
    this.emitValueF32(fieldIndex, v);
  }

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

  emitRaw(name: string, data: Buffer, ts?: number) {
    if (typeof ts === "number") this.emitTs(ts >>> 0);
    this.pushExt(ExtId.raw);
    const nameLit = this.cachedLit(name);
    const sz = Buffer.allocUnsafe(2);
    sz.writeUInt16LE(data.length, 0);
    this.write(Buffer.concat([nameLit, sz, data]));
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
