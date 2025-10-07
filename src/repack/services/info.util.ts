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
  hash?: string; 
};

export function md5File(filePath: string): string {
  const h = createHash("md5");
  const s = fs.createReadStream(filePath);
  return new Promise<string>((resolve, reject) => {
    s.on("data", (c) => h.update(c));
    s.on("error", reject);
    s.on("end", () => resolve(h.digest("hex")));
  }) as unknown as string; 
}
export function md5FileSync(filePath: string): string {
  const h = createHash("md5");
  const b = fs.readFileSync(filePath);
  h.update(b);
  return h.digest("hex");
}

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
  } = {}
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
  };

  return info;
}
