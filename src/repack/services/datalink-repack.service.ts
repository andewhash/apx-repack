import * as fs from "fs";
import * as path from "path";
import sax from "sax";
import { XMLParser } from "fast-xml-parser";
import { ApxTlmWriter } from "./apxtlm-writer.service";

const MAX_FIELDS = 2048;
const TELEMETRY_TAGS = new Set(["S", "D"]);
const EVENT_TAGS = new Set(["event", "evt"]);
const SKIP_TOP_LEVEL = new Set(["S", "D", "event", "evt", "#text", "@_"]);

type Ctx = {
  writer?: ApxTlmWriter;
  wroteHeader: boolean;
  utcOffset: number;
  baseTs: number;
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
    ctx.fields = Array.from({ length: n }, (_, i) => `.#${i}`.slice(1)); 
  }

  if (ctx.fields.length > MAX_FIELDS) ctx.fields.length = MAX_FIELDS;

  for (const f of ctx.fields) {
    ctx.writer.emitField(f, []);
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

function splitTokensLoose(s: string): string[] {
  return String(s).split(/[,\s;]+/).map(x => x.trim()).filter(x => x.length || x === "");
}

function openTagToString(name: string, attrs: Record<string, any>): string {
  const a = Object.entries(attrs ?? {}).map(([k, v]) => `${k}="${String(v)}"`).join(" ");
  return a.length ? `<${name} ${a}>` : `<${name}>`;
}

function closeTagToString(name: string): string {
  return `</${name}>`;
}

function logToFile(ctx: Ctx, message: string) {
  if (!ctx.logFile) {
    ctx.logFile = fs.createWriteStream(path.resolve(__dirname, 'log.txt'), { flags: 'a' });
  }
  ctx.logFile.write(`[${new Date().toISOString()}] ${message}\n`);
}

export async function repackDatalinkToApx_stream(
  inputFile: string,
  outFile: string,
  opts: { utcOffset?: number; includeJso?: boolean } = {}
): Promise<void> {
  const { utcOffset = 0, includeJso = true } = opts;

  const ctx: Ctx = {
    writer: undefined,
    wroteHeader: false,
    utcOffset,
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
      const t = attrs?.["time_ms"] ?? attrs?.["UTC"];
      if (t != null) {
        const n = Number(t);
        if (Number.isFinite(n)) {
          ctx.baseTs = n >>> 0;
        } else {
          logToFile(ctx, `Invalid timestamp detected, setting default: ${t}`);
          ctx.baseTs = 0;
        }
      }

      if (!ctx.writer) {
        ctx.writer = new ApxTlmWriter(0x0100, ctx.utcOffset, ctx.baseTs >>> 0, outFile);
        ctx.writer.writeHeaderPlaceholder();
        ctx.wroteHeader = true;

        const timestamp = new Date(ctx.baseTs);
        const utcOffsetInMinutes = ctx.utcOffset / 60;
        
        const info = {
          conf: "Borey-801",
          host: {
            hostname: "Ovsannikovs-MacBook-Pro",
            uid: "86C97FFC9CD3544D0D7B61F984B621B48430910F",
            username: "nikolay"
          },
          sw: {
            hash: "fe7bd27c",
            version: "11.2.12"
          },
          title: "250910_1730_32396E3-BOREY",
          unit: {
            name: "BOREY",
            time: timestamp.getTime(),
            type: "UAV",
            uid: "230047000F51323032343731"
          },
          timestamp: timestamp.getTime(), // Unix timestamp в миллисекундах
          utc_offset: utcOffsetInMinutes,  // Смещение в минутах
        };

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

    if (ctx.capJsoActive) {
      ctx.capXml.push(openTagToString(name, attrs));
    }
  });

  parser.on("text", (txt) => {
    if (ctx.stack.length >= 2 && ctx.stack[ctx.stack.length - 1] === "fields" && ctx.stack.includes("mandala")) {
      const raw = (txt ?? "").trim();
      if (raw) {
        ctx.fields = raw.split(/[,\s;]+/).map(s => s.trim()).filter(Boolean);
      }
    }

    if (ctx.inCsv) ctx.csvText += txt;
    if (ctx.inEvt) ctx.evtText += txt;

    if (ctx.capJsoActive && txt) {
      ctx.capXml.push(txt);
    }
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
      let any = false;
      for (let i = 0; i < lim; i++) {
        const s = parts[i];
        if (s !== "") {
          const n = Number(s);
          if (Number.isFinite(n)) { wr.emitValueF32(i, n); any = true; }
        }
      }
      if (!any && (ctx.fields.length || MAX_FIELDS) > 0) {
        wr.emitValueF32(0, Number.NaN);
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
      for (const k of keys) {
        if (k === "text") vals.push(ctx.evtText.trim());
        else vals.push(String(a[k] ?? ""));
      }
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

  const stream = fs.createReadStream(inputFile, { encoding: "utf8", highWaterMark: 100*1024 });
  await new Promise<void>((resolve, reject) => {
    stream.on("error", reject);
    parser.on("error", reject);
    parser.on("end", resolve);
    stream.pipe(parser);
  });

  if (ctx.writer) {
    await ctx.writer.finalizeToFile();
  } else {
    const wr = new ApxTlmWriter(0x0100, utcOffset, ctx.baseTs >>> 0, outFile);
    wr.writeHeaderPlaceholder();
    await wr.finalizeToFile();
  }

  try {
    const sz = fs.statSync(outFile).size;
    logToFile(ctx, `[repack] DONE → ${path.resolve(outFile)} size=${sz} bytes`);
  } catch {}
}
