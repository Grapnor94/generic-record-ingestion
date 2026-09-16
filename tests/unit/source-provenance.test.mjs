import test from "node:test";
import assert from "node:assert/strict";
import { sourceContentMetadata } from "../../dist/ingestion/source-provenance.js";

test("byte provenance uses deterministic lowercase SHA-256, including empty content", () => {
  assert.deepEqual(sourceContentMetadata(Buffer.from("abc")), {
    sourceSizeBytes: 3,
    sourceSha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  });
  assert.deepEqual(sourceContentMetadata(new Uint8Array()), {
    sourceSizeBytes: 0,
    sourceSha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  });
});

test("byte provenance measures multibyte UTF-8 and only hashes the supplied view", () => {
  assert.equal(sourceContentMetadata(Buffer.from("é😀", "utf8")).sourceSizeBytes, 6);
  const view = Uint8Array.from([0, 97, 98, 99, 255]).subarray(1, 4);
  assert.deepEqual(sourceContentMetadata(view), {
    sourceSizeBytes: 3,
    sourceSha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  });
});
