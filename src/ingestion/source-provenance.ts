import { createHash } from "node:crypto";

export function sourceContentMetadata(bytes: Uint8Array): {
  sourceSizeBytes: number;
  sourceSha256: string;
} {
  return {
    sourceSizeBytes: bytes.byteLength,
    sourceSha256: createHash("sha256").update(bytes).digest("hex"),
  };
}
