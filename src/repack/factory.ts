import { repackDatalinkToApx_stream } from "./services/datalink-repack.service";
import { repackTelemetryToApx_stream } from "./services/telemetry-repack.service";
import { FileKind } from "./sniff.service";

export function createRepackRunner(kind: FileKind) {
  switch (kind) {
    case "telemetry":
      return repackTelemetryToApx_stream;
    case "datalink":
      return repackDatalinkToApx_stream;
    default:
      throw new Error(`Unsupported kind: ${kind as never}`);
  }
}
