import * as fs from "fs";
import * as path from "path";
import sax from "sax";
import { XMLParser } from "fast-xml-parser";
import { ApxTlmWriter } from "./apxtlm-writer.service";
import { buildInfoForInput } from "./info.util";

const MAX_FIELDS = 2048;
const EPOCH_2000_MS = Date.UTC(2000, 0, 1);

function fileMtimeMs(p: string): number {
  try { return Math.floor(fs.statSync(p).mtimeMs); } catch { return Date.now(); }
}

type Ctx = {
  inTelemetry: boolean;
  inData: boolean;
  inFields: boolean;
  inD: boolean;
  inE: boolean;
  currentD_ts: number;
  currentD_text: string;
  currentE_attrs: Record<string, string>;
  currentE_text: string;
  fields: string[];
  fieldsDeclared: boolean;
  evtIndex: Map<string, number>;
  writer?: ApxTlmWriter;
  wroteHeader: boolean;
  baseTs: number;         // ms epoch
  utcOffsetSec: number;   // seconds
  inDataDepth: number;
  capJsoActive: boolean;
  capJsoDepth: number;
  capJsoName: string;
  capXml: string[];
};

function ensureWriter(ctx: Ctx) {
  if (!ctx.writer) throw new Error("Writer not initialized yet");
  return ctx.writer;
}

function declareFieldsIfNeeded(ctx: Ctx, tokenCountHint?: number) {
  if (ctx.fieldsDeclared) return;
  const wr = ensureWriter(ctx);

  if (!ctx.fields.length) {
    const n = Math.min(tokenCountHint ?? 0, MAX_FIELDS);
    ctx.fields = Array.from({ length: n }, (_, i) => `#${i}`);
  }

  if (ctx.fields.length > MAX_FIELDS) ctx.fields.length = MAX_FIELDS;

  for (const f of ctx.fields) wr.emitField(f, []);
  ctx.fieldsDeclared = true;
}

function declareEventIfNeeded(ctx: Ctx, name: string, keys: string[]) {
  if (!ctx.writer) return;
  if (!ctx.evtIndex.has(name)) {
    const idx = ctx.evtIndex.size;
    ctx.evtIndex.set(name, idx);
    ctx.writer.emitEvtId(name, keys);
  }
}

function parseCsvKeepEmpty(s: string): string[] {
  return String(s).split(",");
}

function openTagToString(name: string, attrs: Record<string, any>): string {
  const a = Object.entries(attrs ?? {}).map(([k, v]) => `${k}="${String(v)}"`).join(" ");
  return a.length ? `<${name} ${a}>` : `<${name}>`;
}
function closeTagToString(name: string): string { return `</${name}>`; }

export async function repackTelemetryToApx_stream(
  inputFile: string,
  outFile: string,
  opts: { utcOffset?: number; includeJso?: boolean } = {}
): Promise<void> {
  const { utcOffset = 0, includeJso = true } = opts;
  const utcOffsetSec = utcOffset | 0;

  const ctx: Ctx = {
    inTelemetry: false,
    inData: false,
    inFields: false,
    inD: false,
    inE: false,
    currentD_ts: 0,
    currentD_text: "",
    currentE_attrs: {},
    currentE_text: "",
    fields: [],
    fieldsDeclared: false,
    evtIndex: new Map(),
    writer: undefined,
    wroteHeader: false,
    baseTs: 0,
    utcOffsetSec,
    inDataDepth: 0,
    capJsoActive: false,
    capJsoDepth: 0,
    capJsoName: "",
    capXml: [],
  };

  const parser = sax.createStream(true, { trim: false, normalize: false });
  const jsoSkip = new Set(["D", "E", "U"]);

  // ===== open tag =====
  parser.on("opentag", (tag) => {
    const name = tag.name;
    const attrs = tag.attributes as Record<string, any>;

    if (!ctx.inTelemetry && name.toLowerCase() === "telemetry") {
      ctx.inTelemetry = true;
      return;
    }
    if (!ctx.inTelemetry) return;

    if (name === "fields") {
      ctx.inFields = true;
      return;
    }

    if (name === "info") {
      if (attrs && attrs["time"] != null) {
        const t = Number(attrs["time"]);
        if (Number.isFinite(t)) ctx.baseTs = Math.floor(t);
      }
    }

    if (name === "timestamp" && attrs && attrs["value"]) {
      const t = Date.parse(String(attrs["value"]));
      if (Number.isFinite(t)) ctx.baseTs = Math.floor(t);
    }

    if (name === "data") {
      ctx.inData = true;
      ctx.inDataDepth = 1;

      // нормализуем baseTs или берём mtime файла
      if (!(ctx.baseTs >= EPOCH_2000_MS)) ctx.baseTs = fileMtimeMs(inputFile);

      if (!ctx.wroteHeader) {
        ctx.writer = new ApxTlmWriter(1, ctx.utcOffsetSec, ctx.baseTs, outFile);
        ctx.writer.writeHeaderPlaceholder();
        // info самым первым
        ctx.writer.emitInfo(buildInfoForInput(inputFile, "telemetry", ctx.baseTs, {}, ctx.utcOffsetSec));
        ctx.wroteHeader = true;
      }
      return;
    }

    if (!ctx.inData) return;

    if (name === "D") {
      ctx.inD = true;
      ctx.currentD_ts = attrs?.["t"] != null ? Number(attrs["t"]) >>> 0 : 0;
      ctx.currentD_text = "";
      return;
    }

    if (name === "E") {
      ctx.inE = true;
      ctx.currentE_attrs = Object.fromEntries(Object.entries(attrs || {}).map(([k, v]) => [k, String(v)]));
      ctx.currentE_text = "";
      return;
    }

    if (includeJso && !jsoSkip.has(name)) {
      if (!ctx.capJsoActive) {
        ctx.capJsoActive = true;
        ctx.capJsoDepth = ctx.inDataDepth + 1;
        ctx.capJsoName = name;
        ctx.capXml = [];
      }
      ctx.capXml.push(openTagToString(name, attrs));
    }

    if (ctx.inData) ctx.inDataDepth++;
  });

  // ===== text =====
  parser.on("text", (txt) => {
    if (ctx.inFields) {
      const raw = (txt ?? "").trim();
      if (raw) ctx.fields = raw.split(",").map(s => s.trim()).filter(Boolean);
    }
    if (ctx.inD) ctx.currentD_text += txt;
    if (ctx.inE) ctx.currentE_text += txt;
    if (ctx.capJsoActive && txt) ctx.capXml.push(txt);
  });

  // ===== close tag =====
  parser.on("closetag", (name) => {
    if (name === "fields") {
      ctx.inFields = false;
      return;
    }

    if (name === "D" && ctx.inD) {
      ctx.inD = false;

      declareFieldsIfNeeded(ctx, parseCsvKeepEmpty(ctx.currentD_text).length);

      const wr = ctx.writer!;
      wr.emitTs(ctx.currentD_ts >>> 0);

      const parts = parseCsvKeepEmpty(ctx.currentD_text);
      const lim = Math.min(parts.length, ctx.fields.length || MAX_FIELDS);
      for (let i = 0; i < lim; i++) {
        const s = parts[i];
        if (s === "") continue;
        const n = Number(s);
        if (Number.isFinite(n)) wr.emitNumber(i, n, false);
      }
      return;
    }

    if (name === "E" && ctx.inE) {
      ctx.inE = false;

      const wr = ctx.writer!;
      const a = ctx.currentE_attrs;
      const evName = a.name ?? "event";
      const ts = a.t != null ? Number(a.t) : 0;

      const keys = Object.keys(a).filter(k => k !== "name" && k !== "t");
      if (ctx.currentE_text && ctx.currentE_text.trim().length) keys.push("text");
      declareEventIfNeeded(ctx, evName, keys);

      wr.emitTs(ts >>> 0);
      const idx = ctx.evtIndex.get(evName)!;
      const vals: string[] = [];
      for (const k of keys) vals.push(k === "text" ? ctx.currentE_text.trim() : String(a[k] ?? ""));
      wr.emitEvt(idx, vals);
      return;
    }

    if (ctx.capJsoActive) {
      ctx.capXml.push(closeTagToString(name));

      if (name === ctx.capJsoName && ctx.inDataDepth === ctx.capJsoDepth) {
        try {
          const xmlStr = ctx.capXml.join("");
          const parser = new XMLParser({
            ignoreAttributes: false,
            ignoreDeclaration: true,
            attributeNamePrefix: "@_",
            textNodeName: "#text",
            trimValues: true,
            allowBooleanAttributes: true,
            parseTagValue: false
          });
          const obj = parser.parse(xmlStr);
          const val = (obj as any)[ctx.capJsoName] ?? obj;
          ctx.writer!.emitJso(ctx.capJsoName, val, ctx.baseTs >>> 0);
        } catch (e: any) {
          console.warn(`[repack][warn] JSO parse failed <${ctx.capJsoName}>: ${e?.message ?? e}`);
        }
        ctx.capJsoActive = false;
        ctx.capJsoDepth = 0;
        ctx.capJsoName = "";
        ctx.capXml = [];
      }
    }

    if (ctx.inData) ctx.inDataDepth--;

    if (name.toLowerCase() === "telemetry") ctx.inTelemetry = false;
    if (name === "data") ctx.inData = false;
  });

  // ===== errors / finish =====
  const stream = fs.createReadStream(inputFile, { encoding: "utf8", highWaterMark: 100 * 1024 });

  await new Promise<void>((resolve, reject) => {
    stream.on("error", reject);
    parser.on("error", reject);
    parser.on("end", resolve);
    stream.pipe(parser);
  });

  if (ctx.writer) {
    await ctx.writer.finalizeToFile();
  } else {
    const baseTs = fileMtimeMs(inputFile);
    const wr = new ApxTlmWriter(1, utcOffsetSec, baseTs, outFile);
    wr.writeHeaderPlaceholder();
    wr.emitInfo(buildInfoForInput(inputFile, "telemetry", baseTs, {}, utcOffsetSec));
    await wr.finalizeToFile();
  }

  try {
    const sz = fs.statSync(outFile).size;
    console.log(`[repack] DONE → ${path.resolve(outFile)} size=${sz} bytes`);
  } catch {}
}
