import * as fs from "fs";
import * as path from "path";
import { XMLParser } from "fast-xml-parser";

export type FileKind = "telemetry" | "datalink";

/** Cheap, defensive sniff of file kind by extension, content head and XML tags. */
export function sniffXmlKind(filePath: string): FileKind | null {
  const fd = fs.openSync(filePath, "r");
  try {
    const buf = Buffer.allocUnsafe(64 * 1024);
    const n = fs.readSync(fd, buf, 0, buf.length, 0);
    const head = buf.slice(0, Math.max(0, n)).toString("utf8");
    const lower = head.toLowerCase();

    // Extension hints
    const base = path.basename(filePath).toLowerCase();
    if (base.endsWith(".telemetry")) return "telemetry";
    if (base.endsWith(".datalink.xml") || base.includes(".datalink")) return "datalink";

    // Text heuristics
    if (lower.includes("<telemetry")) return "telemetry";
    if (lower.includes("<mandala") || lower.includes("<s>") || lower.includes("<d>")) return "datalink";

    // Fallback: try parsing the head
    try {
      const parser = new XMLParser({
        ignoreAttributes: false,
        ignoreDeclaration: true,
        attributeNamePrefix: "@_",
        textNodeName: "#text",
        trimValues: true,
        allowBooleanAttributes: true,
        parseTagValue: false
      });
      const doc = parser.parse(head);
      const keys = Object.keys(doc).filter(k => !k.startsWith("?"));
      if (keys.some(k => k.toLowerCase().includes("telemetry"))) return "telemetry";
      if (keys.some(k => k.toLowerCase().includes("datalink") || k.toLowerCase().includes("mandala")))
        return "datalink";
    } catch { /* ignore */ }

    return null;
  } finally {
    fs.closeSync(fd);
  }
}
