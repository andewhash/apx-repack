// CLI entry for APX repacker.
// Usage:
//   apx-repack --in <input> --out <result.apxtlm> [--utc <offsetSec>] [--with-jso]

import { runRepack } from "./repack/command";

function usage() {
  console.log(`Usage:
  apx-repack --in <path/to/input> --out <result.apxtlm> [--utc <offsetSec>] [--with-jso]

Examples:
  apx-repack --in ./sample.telemetry --out ./result.apxtlm
  apx-repack --in ./sample.datalink.xml --out ./result.apxtlm --utc 10800 --with-jso
`);
}

function parseArgv(argv: string[]) {
  const a = argv.slice(2);
  const out: { in?: string; out?: string; utc?: number; includeJso: boolean; help?: boolean } = { includeJso: true };
  for (let i = 0; i < a.length; i++) {
    const x = a[i];
    if (x === "-h" || x === "--help") out.help = true;
    else if (x === "--in") out.in = a[++i];
    else if (x === "--out") out.out = a[++i];
    else if (x === "--utc") out.utc = Number(a[++i] ?? 0);
  }
  return out;
}

(async () => {
  const args = parseArgv(process.argv);
  if (args.help || !args.in || !args.out) {
    usage();
    process.exit(args.help ? 0 : 1);
  }
  try {
    await runRepack({
      inFile: args.in,
      outFile: args.out,
      utcOffset: Number.isFinite(args.utc) ? (args.utc as number) : 0,
      includeJso: !!args.includeJso
    });
  } catch (e: any) {
    console.error("❌ repack error:", e?.message ?? e);
    process.exit(1);
  }
})();
