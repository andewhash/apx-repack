import * as fs from "fs";
import * as path from "path";
import { createHash } from "crypto";

export type InfoObject = {
  conf?: string;
  host?: { hostname?: string; uid?: string; username?: string };
  sw?: { hash?: string; version?: string };
  title: string;
  unit?: { name?: string; time?: number; type?: string; uid?: string };
  import?: {
    name: string;
    title: string;
    format: "telemetry" | "datalink";
    timestamp?: string | number;
    exported?: string | number;
  };
  timestamp?: number;      // ms (epoch)
  utc_offset?: number;     // seconds (matches APXTLM header)
  hash?: string;
};

/** async md5 of a file */
export function md5File(filePath: string): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const h = createHash("md5");
    const s = fs.createReadStream(filePath);
    s.on("data", (c) => h.update(c));
    s.on("error", reject);
    s.on("end", () => resolve(h.digest("hex")));
  });
}

export function md5FileSync(filePath: string): string {
  const h = createHash("md5");
  const b = fs.readFileSync(filePath);
  h.update(b);
  return h.digest("hex");
}

/**
 * Build minimal 'info' block used by ground for naming and meta.
 * utcOffsetSeconds must be in seconds and matches APXTLM header.
 */
export function buildInfoForInput(
  inputFile: string,
  kind: "telemetry" | "datalink",
  baseTsMs: number,
  {
    conf = undefined,
    unitName = undefined,
    unitType = "UAV",
    unitUid = undefined,
  }: {
    conf?: string;
    unitName?: string;
    unitType?: string;
    unitUid?: string;
  } = {},
  utcOffsetSeconds: number = 0
): InfoObject {
  const name = path.basename(inputFile);
  const title = path.parse(inputFile).name;

  const info: InfoObject = {
    conf,
    sw: undefined,
    host: undefined,
    title,
    unit: (unitName || unitUid)
      ? { name: unitName, time: baseTsMs >>> 0, type: unitType, uid: unitUid }
      : undefined,
    import: {
      name,
      title,
      format: kind,
      timestamp: baseTsMs,
    },
    timestamp: baseTsMs >>> 0,
    utc_offset: utcOffsetSeconds | 0,
  };

  return info;
}
