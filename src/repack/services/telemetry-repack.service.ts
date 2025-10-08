import * as fs from "fs";
import * as path from "path";
import sax from "sax";
import { XMLParser } from "fast-xml-parser";
import { ApxTlmWriter } from "./apxtlm-writer.service";
import { buildInfoForInput } from "./info.util";
import { createHash } from "crypto";

const MAX_FIELDS = 2048;
const EPOCH_2000_MS = Date.UTC(2000, 0, 1);

function fileMtimeMs(p: string): number {
  try { return Math.floor(fs.statSync(p).mtimeMs); } catch { return Date.now(); }
}

/* ---------- НОРМАЛИЗАЦИЯ NODES (оба формата) ---------- */

type DictField = { name: string; title: string; type: string };
type DictBuild = { fields: DictField[]; values: Record<string, any> };

function pushField(out: DictBuild, name: string, title: any, type: any, value: any) {
  const fname = String(name || "").trim();
  if (!fname) return;
  const ftitle = String(title ?? fname);
  const ftype  = normalizeType(type);

  out.fields.push({ name: fname, title: ftitle, type: ftype });

  if (value !== undefined && value !== null) {
    const s = String(value).trim();
    let v: any = s;
    if (/^(i|u\d*|int)/i.test(ftype) || /^(f|double|float)/i.test(ftype)) {
      const n = Number(s);
      v = Number.isFinite(n) ? n : 0;
    } else if (/^bool/i.test(ftype)) {
      v = /^(1|true|yes|on)$/i.test(s);
    }
    out.values[fname] = v;
  }
}

/** вариант А: datalink-style: node.fields.field[] */
function buildDictFromFlatFields(nodeObj: any): DictBuild | null {
  let fields = nodeObj?.fields?.field;
  if (!fields) return null;
  if (!Array.isArray(fields)) fields = [fields];

  const out: DictBuild = { fields: [], values: {} };
  let autoIdx = 0;

  for (const f of fields) {
    const name = f?.name ?? f?.["@_name"] ?? f?.id ?? `f${autoIdx++}`;
    const title = f?.title ?? name;
    const type  = normalizeType(f?.struct?.type ?? f?.type ?? "string");
    const value = f?.value;

    pushField(out, name, title, type, value);
  }
  if (!out.fields.length) return null;
  return out;
}
function normalizeType(t: any): string {
  const s = String(
    (t?.struct?.type) ?? (t?.type) ?? t ?? "string"
  ).toLowerCase();
  // привести «Option»/«enum» к строковому типу, чтобы Ground не спотыкался
  if (s === "option" || s === "enum") return "string";
  return s;
}

/** вариант C: telemetry-style: node.field[] со свойствами {type,title,value,"@_name",...} */
function buildDictFromNodeFieldArray(nodeObj: any): DictBuild | null {
  let fields = nodeObj?.field;
  if (!fields) return null;
  if (!Array.isArray(fields)) fields = [fields];

  const out: DictBuild = { fields: [], values: {} };

  for (const f of fields) {
    const name = f?.["@_name"] ?? f?.name ?? f?.id;
    const title = f?.title ?? name;
    const type  = normalizeType(f?.type ?? f?.struct?.type);
    const value = f?.value ?? f?.["#text"];

    if (!name) continue;

    // записываем поле
    out.fields.push({ name: String(name), title: String(title ?? name), type });

    // и начальное значение, если оно есть
    if (value !== undefined && value !== null) {
      const s = String(value).trim();
      let v: any = s;
      if (/^(i|u\d*|int)/i.test(type) || /^(f|double|float)/i.test(type)) {
        const n = Number(s);
        v = Number.isFinite(n) ? n : 0;
      } else if (/^bool/i.test(type)) {
        v = /^(1|true|yes|on)$/i.test(s);
      }
      out.values[String(name)] = v;
    }
  }

  if (!out.fields.length) return null;
  return out;
}


/** рекурсивный обход «dictionary» (telemetry-style) */
function buildDictFromDictionary(dict: any): DictBuild | null {
  if (!dict || typeof dict !== "object") return null;

  const out: DictBuild = { fields: [], values: {} };

  function walk(node: any) {
    if (!node || typeof node !== "object") return;

    // частые контейнеры: fields/field, group, item, values, dictionary, …
    const keys = Object.keys(node);
    // если это конкретное поле
    const hasName = node.name ?? node["@_name"];
    const hasType = (node.struct?.type ?? node.type);
    if (hasName && hasType) {
      pushField(
        out,
        node.name ?? node["@_name"],
        node.title ?? node["@_title"] ?? node.name ?? node["@_name"],
        node.struct?.type ?? node.type,
        // значение может быть и в node.value, и в #text
        (node.value ?? node["#text"])
      );
      // продолжаем обход — на случай вложенных подсекций
    }

    for (const k of keys) {
      const v = (node as any)[k];
      if (v == null) continue;
      if (Array.isArray(v)) {
        for (const it of v) walk(it);
      } else if (typeof v === "object") {
        // пропустим явные сервисные/метаданные
        if (k === "info" || k === "hardware" || k === "version") continue;
        walk(v);
      }
    }
  }

  walk(dict);

  if (!out.fields.length) return null;
  return out;
}

/** универсальная нормализация raw → {nodes:[{info,dict,values,time}]} */
function tryNormalizeNodes(raw: any, baseTs: number) {
  if (!raw || typeof raw !== "object") return null;
  console.log('qqqq');
  const nodeObj = raw?.node ?? raw; // иногда узел прямо в корне
  const ident   = raw.ident || raw.identity || raw.vehicle || raw.identify || {};
  const uid     = String(ident.uid ?? ident.UID ?? "").trim();
  const name    = String(ident.callsign ?? ident.name ?? "LOCAL").trim();
  const type    = String(ident.class ?? ident.type ?? "UAV").trim();

  // version/hardware могут лежать в разных местах
  const ninfo     = nodeObj?.info || {};
  const version   = (ninfo.version ?? raw.version) as string | undefined;
  const hardware1 = ninfo.hardware as string | undefined;
  const hardware2 = nodeObj?.dictionary?.hardware as string | undefined;
  const hardware  = hardware1 ?? hardware2;

  // A: datalink-style flat fields
  let built: DictBuild | null = buildDictFromFlatFields(nodeObj);

  // C: telemetry-style node.field[] 
  if (!built) built = buildDictFromNodeFieldArray(nodeObj);

  // B: telemetry-style dictionary
  if (!built) built = buildDictFromDictionary(nodeObj?.dictionary);

  if (!built || built.fields.length === 0) return null;

  // ограничим поля
  if (built.fields.length > MAX_FIELDS) built.fields.length = MAX_FIELDS;

  // cache по схемe полей
  const h = createHash("sha1");
  h.update(JSON.stringify(built.fields));
  const cache = h.digest("hex").slice(0, 8).toUpperCase();

  const node = {
    info: {
      uid, name, type,
      time: baseTs >>> 0,
      ...(version ? { version } : {}),
      ...(hardware ? { hardware } : {}),
    },
    dict: { cache, fields: built.fields },
    values: built.values,
    time: baseTs >>> 0,
  };

  return { nodes: [node] };
}

/* ---------- ОСНОВНОЙ ПАРСЕР (без изменений в логике D/E/U/TS) ---------- */

type Ctx = {
  inTelemetry: boolean;
  inData: boolean;
  inFields: boolean;
  inD: boolean;
  inE: boolean;
  inU: boolean;

  currentD_ts: number;
  currentD_text: string;

  currentE_attrs: Record<string, string>;
  currentE_text: string;

  uStackDepth: number;
  uCurName: string;
  uCurTs: number;
  uCurText: string;

  fields: string[];
  fieldsDeclared: boolean;
  nameToIndex: Map<string, number>;

  evtIndex: Map<string, number>;

  writer?: ApxTlmWriter;
  wroteHeader: boolean;
  baseTs: number;
  utcOffsetSec: number;

  stack: string[];
  inDataDepth: number;
  capJsoActive: boolean;
  capJsoDepth: number;
  capJsoName: string;
  capXml: string[];

  lastTs: number;
};

function ensureWriter(ctx: Ctx) {
  if (!ctx.writer) throw new Error("Writer not initialized yet");
  return ctx.writer;
}

function splitFieldsSmart(s: string): string[] {
  return String(s).split(/[,\s;]+/).map(x => x.trim()).filter(Boolean);
}

function declareFieldsIfNeeded(ctx: Ctx, tokenCountHint?: number) {
  if (ctx.fieldsDeclared) return;
  const wr = ensureWriter(ctx);

  if (!ctx.fields.length) {
    const n = Math.min(tokenCountHint ?? 0, MAX_FIELDS);
    ctx.fields = Array.from({ length: n }, (_, i) => `#${i}`);
  }
  if (ctx.fields.length > MAX_FIELDS) ctx.fields.length = MAX_FIELDS;

  for (let i = 0; i < ctx.fields.length; i++) {
    const f = ctx.fields[i];
    wr.emitField(f, []);
    ctx.nameToIndex.set(f, i);
  }
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

function openTagToString(name: string, attrs: Record<string, any>) {
  const a = Object.entries(attrs ?? {}).map(([k, v]) => `${k}="${String(v)}"`).join(" ");
  return a.length ? `<${name} ${a}>` : `<${name}>`;
}
function closeTagToString(name: string) { return `</${name}>`; }

function maybeEmitTs(ctx: Ctx, t: number) {
  const ts = (t >>> 0);
  if (ctx.lastTs !== ts) {
    ctx.writer!.emitTs(ts);
    ctx.lastTs = ts;
  }
}

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
    inU: false,

    currentD_ts: 0,
    currentD_text: "",

    currentE_attrs: {},
    currentE_text: "",

    uStackDepth: 0,
    uCurName: "",
    uCurTs: 0,
    uCurText: "",

    fields: [],
    fieldsDeclared: false,
    nameToIndex: new Map<string, number>(),

    evtIndex: new Map(),

    writer: undefined,
    wroteHeader: false,
    baseTs: 0,
    utcOffsetSec,
    stack: [],
    inDataDepth: 0,
    capJsoActive: false,
    capJsoDepth: 0,
    capJsoName: "",
    capXml: [],

    lastTs: -1,
  };

  const parser = sax.createStream(true, { trim: false, normalize: false });
  const jsoSkip = new Set(["D", "E", "U"]);

  parser.on("opentag", (tag) => {
    const name = tag.name;
    const attrs = tag.attributes as Record<string, any>;
    ctx.stack.push(name);

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

      if (!(ctx.baseTs >= EPOCH_2000_MS)) ctx.baseTs = fileMtimeMs(inputFile);

      if (!ctx.wroteHeader) {
        ctx.writer = new ApxTlmWriter(1, ctx.utcOffsetSec, ctx.baseTs, outFile);
        ctx.writer.writeHeaderPlaceholder();
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

    if (name === "U") {
      ctx.inU = true;
      ctx.uStackDepth = ctx.stack.length;
      return;
    }

    if (ctx.inU) {
      const nm = (attrs?.["name"] ?? attrs?.["@_name"]);
      const t  = (attrs?.["t"] != null ? Number(attrs["t"]) : undefined);
      ctx.uCurName = nm ? String(nm) : "";
      ctx.uCurTs   = Number.isFinite(t) ? (t as number) >>> 0 : 0;
      ctx.uCurText = "";
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

  parser.on("text", (txt) => {
    if (ctx.inFields) {
      const raw = (txt ?? "").trim();
      if (raw) {
        const candidate = splitFieldsSmart(raw);
        if (candidate.length >= 5) ctx.fields = candidate;
      }
    }

    if (ctx.inD) ctx.currentD_text += txt;
    if (ctx.inE) ctx.currentE_text += txt;
    if (ctx.inU) ctx.uCurText += txt;
    if (ctx.capJsoActive && txt) ctx.capXml.push(txt);
  });

  parser.on("closetag", (name) => {
    if (name === "fields") {
      ctx.inFields = false;
      return;
    }

    if (name === "D" && ctx.inD) {
      ctx.inD = false;
      declareFieldsIfNeeded(ctx, parseCsvKeepEmpty(ctx.currentD_text).length);

      const wr = ctx.writer!;
      const parts = parseCsvKeepEmpty(ctx.currentD_text);
      const lim = Math.min(parts.length, ctx.fields.length || MAX_FIELDS);

      maybeEmitTs(ctx, ctx.currentD_ts);
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

      maybeEmitTs(ctx, ts);
      const idx = ctx.evtIndex.get(evName)!;
      const vals: string[] = [];
      for (const k of keys) vals.push(k === "text" ? ctx.currentE_text.trim() : String(a[k] ?? ""));
      wr.emitEvt(idx, vals);
      return;
    }

    if (name === "U" && ctx.inU && ctx.stack.length === ctx.uStackDepth) {
      ctx.inU = false;
      ctx.uStackDepth = 0;
      return;
    }

    if (ctx.inU && name !== "U") {
      const nm = ctx.uCurName?.trim();
      const txt = ctx.uCurText?.trim();
      if (nm && txt != null) {
        if (!ctx.fieldsDeclared && ctx.fields.length >= 1) {
          declareFieldsIfNeeded(ctx, ctx.fields.length);
        }
        let idx = ctx.nameToIndex.get(nm);
        if (idx === undefined) {
          const newIdx = ctx.fields.length;
          if (newIdx < MAX_FIELDS) {
            ctx.fields.push(nm);
            ensureWriter(ctx).emitField(nm, []);
            ctx.nameToIndex.set(nm, newIdx);
            idx = newIdx;
          }
        }
        if (idx !== undefined) {
          const v = Number(txt);
          if (Number.isFinite(v)) {
            maybeEmitTs(ctx, ctx.uCurTs >>> 0);
            ensureWriter(ctx).emitNumber(idx, v, true);
          }
        }
      }
      ctx.uCurName = "";
      ctx.uCurTs = 0;
      ctx.uCurText = "";
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

          const normalized = tryNormalizeNodes(val, ctx.baseTs);
          if (normalized) {
            ctx.writer!.emitJso("nodes", normalized);
          } else {
            ctx.writer!.emitJso(ctx.capJsoName, val);
          }
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
    wr.emitInfo(buildInfoForInput(inputFile, "telemetry", baseTs, {}, utcOffsetSec));
    await wr.finalizeToFile();
  }

  try {
    const sz = fs.statSync(outFile).size;
    console.log(`[repack] DONE → ${path.resolve(outFile)} size=${sz} bytes`);
  } catch {}
}
