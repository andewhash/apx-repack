import * as fs from "fs";
import * as path from "path";
import sax from "sax";
import { XMLParser } from "fast-xml-parser";
import { ApxTlmWriter } from "./apxtlm-writer.service";
import { buildInfoForInput } from "./info.util";

const MAX_FIELDS = 2048;
const TELEMETRY_TAGS = new Set(["S", "D"]);
const EVENT_TAGS = new Set(["event", "evt"]);
const SKIP_TOP_LEVEL = new Set(["S", "D", "event", "evt", "#text", "@_"]);

const EPOCH_2000_MS = Date.UTC(2000, 0, 1);

function fileMtimeMs(p: string): number {
  try { return Math.floor(fs.statSync(p).mtimeMs); } catch { return Date.now(); }
}

/** normalize potential seconds→ms; reject pre-2000 values (fallback to file mtime) */
function normalizeEpochMs(n: number, inputFile: string): number {
  if (!Number.isFinite(n)) return fileMtimeMs(inputFile);
  let v = n;
  if (v < 1e12 && v >= 1e9) v = v * 1000; // seconds → ms
  v = Math.floor(v);
  if (v < EPOCH_2000_MS) return fileMtimeMs(inputFile);
  return v;
}

type Ctx = {
  writer?: ApxTlmWriter;
  wroteHeader: boolean;
  utcOffsetSec: number;
  baseTs: number;           // ms
  fields: string[];
  fieldsDeclared: boolean;

  stack: string[];
  inCsv: boolean;
  csvTag: string;
  csvAttrs: Record<string, string>;
  csvText: string;

  inEvt: boolean;
  evtName: string;
  evtAttrs: Record<string, string>;
  evtText: string;
  evtIndex: Map<string, number>;

  capJsoActive: boolean;
  capJsoDepth: number;
  capJsoName: string;
  capXml: string[];

  logFile?: fs.WriteStream;
};

function declareFieldsIfNeeded(ctx: Ctx, tokenCountHint?: number) {
  if (ctx.fieldsDeclared) return;
  if (!ctx.writer) return;

  if (!ctx.fields.length) {
    const n = Math.min(tokenCountHint ?? 0, MAX_FIELDS);
    ctx.fields = Array.from({ length: n }, (_, i) => `#${i}`);
  }

  if (ctx.fields.length > MAX_FIELDS) ctx.fields.length = MAX_FIELDS;

  for (const f of ctx.fields) ctx.writer.emitField(f, []);
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

function splitTokensLoose(s: string): string[] {
  return String(s).split(/[,\s;]+/).map(x => x.trim()).filter(x => x.length || x === "");
}

function openTagToString(name: string, attrs: Record<string, any>): string {
  const a = Object.entries(attrs ?? {}).map(([k, v]) => `${k}="${String(v)}"`).join(" ");
  return a.length ? `<${name} ${a}>` : `<${name}>`;
}
function closeTagToString(name: string): string { return `</${name}>`; }

function logToFile(ctx: Ctx, message: string) {
  if (!ctx.logFile) {
    ctx.logFile = fs.createWriteStream(path.resolve(__dirname, "log.txt"), { flags: "a" });
  }
  ctx.logFile.write(`[${new Date().toISOString()}] ${message}\n`);
}

export async function repackDatalinkToApx_stream(
  inputFile: string,
  outFile: string,
  opts: { utcOffset?: number; includeJso?: boolean } = {}
): Promise<void> {
  const { utcOffset = 0, includeJso = true } = opts;
  const utcOffsetSec = utcOffset | 0;

  const ctx: Ctx = {
    writer: undefined,
    wroteHeader: false,
    utcOffsetSec,
    baseTs: 0,
    fields: [],
    fieldsDeclared: false,

    stack: [],
    inCsv: false,
    csvTag: "",
    csvAttrs: {},
    csvText: "",

    inEvt: false,
    evtName: "",
    evtAttrs: {},
    evtText: "",
    evtIndex: new Map(),

    capJsoActive: false,
    capJsoDepth: 0,
    capJsoName: "",
    capXml: [],

    logFile: undefined,
  };

  const parser = sax.createStream(true, { trim: false, normalize: false });

  parser.on("opentag", (tag) => {
    const name = tag.name;
    const attrs = tag.attributes as Record<string, any>;

    ctx.stack.push(name);

    if (ctx.stack.length === 1) {
      const tRaw = (attrs?.["time_ms"] ?? attrs?.["UTC"]);
      if (tRaw != null) {
        const n = Number(tRaw);
        ctx.baseTs = normalizeEpochMs(n, inputFile);
      } else {
        ctx.baseTs = fileMtimeMs(inputFile);
      }

      if (!ctx.writer) {
        ctx.writer = new ApxTlmWriter(1, ctx.utcOffsetSec, ctx.baseTs, outFile);
        ctx.writer.writeHeaderPlaceholder();
        ctx.wroteHeader = true;

        // info первым блоком
        const info = buildInfoForInput(inputFile, "datalink", ctx.baseTs, {}, ctx.utcOffsetSec);
        ctx.writer.emitInfo(info);
      }
    }

    if (TELEMETRY_TAGS.has(name)) {
      ctx.inCsv = true;
      ctx.csvTag = name;
      ctx.csvAttrs = Object.fromEntries(Object.entries(attrs || {}).map(([k, v]) => [k, String(v)]));
      ctx.csvText = "";
      return;
    }

    if (EVENT_TAGS.has(name)) {
      ctx.inEvt = true;
      ctx.evtName = (attrs?.["name"] as string) ?? name;
      ctx.evtAttrs = Object.fromEntries(Object.entries(attrs || {}).map(([k, v]) => [k, String(v)]));
      ctx.evtText = "";
      return;
    }

    if (includeJso && ctx.stack.length === 2 && !SKIP_TOP_LEVEL.has(name)) {
      ctx.capJsoActive = true;
      ctx.capJsoDepth = ctx.stack.length;
      ctx.capJsoName = name;
      ctx.capXml = [];
      ctx.capXml.push(openTagToString(name, attrs));
      return;
    }

    if (ctx.capJsoActive) ctx.capXml.push(openTagToString(name, attrs));
  });

  parser.on("text", (txt) => {
    if (ctx.stack.length >= 2 && ctx.stack[ctx.stack.length - 1] === "fields" && ctx.stack.includes("mandala")) {
      const raw = (txt ?? "").trim();
      if (raw) ctx.fields = raw.split(/[,\s;]+/).map(s => s.trim()).filter(Boolean);
    }

    if (ctx.inCsv) ctx.csvText += txt;
    if (ctx.inEvt) ctx.evtText += txt;
    if (ctx.capJsoActive && txt) ctx.capXml.push(txt);
  });

  parser.on("closetag", (name) => {
    if (ctx.inCsv && name === ctx.csvTag) {
      ctx.inCsv = false;
      const tsAttr = ctx.csvAttrs["t"] ?? ctx.csvAttrs["ts"] ?? ctx.csvAttrs["time_ms"] ?? ctx.csvAttrs["UTC"];
      const ts = tsAttr != null ? Number(tsAttr) : 0;

      const parts = splitTokensLoose(ctx.csvText);
      declareFieldsIfNeeded(ctx, parts.length);

      const wr = ctx.writer!;
      wr.emitTs((Number.isFinite(ts) ? ts : 0) >>> 0);

      const lim = Math.min(parts.length, ctx.fields.length || MAX_FIELDS);
      for (let i = 0; i < lim; i++) {
        const s = parts[i];
        if (s === "") continue;
        const n = Number(s);
        if (Number.isFinite(n)) wr.emitNumber(i, n, false);
      }
    }

    if (ctx.inEvt && EVENT_TAGS.has(name)) {
      ctx.inEvt = false;
      const wr = ctx.writer!;
      const a = ctx.evtAttrs;
      const evName = ctx.evtName;
      const ts = a.t != null ? Number(a.t) : 0;

      const keys = Object.keys(a).filter(k => k !== "name" && k !== "t");
      if (ctx.evtText && ctx.evtText.trim().length) keys.push("text");
      declareEventIfNeeded(ctx, evName, keys);

      wr.emitTs(ts >>> 0);
      const idx = ctx.evtIndex.get(evName)!;
      const vals: string[] = [];
      for (const k of keys) vals.push(k === "text" ? ctx.evtText.trim() : String(a[k] ?? ""));
      wr.emitEvt(idx, vals);
    }

    if (ctx.capJsoActive) {
      ctx.capXml.push(closeTagToString(name));
      if (ctx.stack.length === ctx.capJsoDepth && name === ctx.capJsoName) {
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
          logToFile(ctx, `[repack][warn] JSO parse failed <${ctx.capJsoName}>: ${e?.message ?? e}`);
        }
        ctx.capJsoActive = false;
        ctx.capJsoDepth = 0;
        ctx.capJsoName = "";
        ctx.capXml = [];
      }
    }

    ctx.stack.pop();
  });

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
    wr.emitInfo(buildInfoForInput(inputFile, "datalink", baseTs, {}, utcOffsetSec));
    await wr.finalizeToFile();
  }

  try {
    const sz = fs.statSync(outFile).size;
    logToFile(ctx, `[repack] DONE → ${path.resolve(outFile)} size=${sz} bytes`);
  } catch {}
}
