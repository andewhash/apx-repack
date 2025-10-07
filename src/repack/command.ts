import * as fs from "fs";
import * as path from "path";
import { sniffXmlKind, FileKind } from "./sniff.service";
import { createRepackRunner } from "./factory";

export async function runRepack(params: {
  inFile: string;
  outFile: string;
  utcOffset?: number;
  includeJso?: boolean;
}) {
  const { inFile, outFile, utcOffset = 0, includeJso = false } = params;

  if (!fs.existsSync(inFile)) {
    throw new Error(`No such file: ${inFile}`);
  }

  const absIn = path.resolve(inFile);
  const absOut = path.resolve(outFile);

  const kind = sniffXmlKind(absIn);
  if (!kind) throw new Error(`Cannot detect file type (telemetry/datalink) from: ${absIn}`);

  const run = createRepackRunner(kind as FileKind);
  await run(absIn, absOut, { utcOffset, includeJso });

  console.log(`✅ Repacked [${kind}] ${absIn} → ${absOut}`);
}
