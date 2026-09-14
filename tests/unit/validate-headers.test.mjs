import test from "node:test";
import assert from "node:assert/strict";
import { assertSupportedRecordHeaders } from "../../dist/ingestion/validate-headers.js";

const contract = {
  schemaVersion: "GENERIC_V1",
  requiredHeaders: ["record_id", "first_name", "status"],
  optionalHeaders: ["region"],
};

test("accepts required and optional headers", () => {
  assert.doesNotThrow(() => assertSupportedRecordHeaders(contract, ["record_id", "first_name", "status", "region"]));
});

test("header order is not significant", () => {
  assert.doesNotThrow(() => assertSupportedRecordHeaders(contract, ["status", "record_id", "first_name"]));
});

test("rejects missing required headers", () => {
  assert.throws(() => assertSupportedRecordHeaders(contract, ["record_id", "first_name"]), /missing required columns: status/i);
});

test("rejects unknown headers", () => {
  assert.throws(() => assertSupportedRecordHeaders(contract, ["record_id", "first_name", "status", "unexpected"]), /unknown columns: unexpected/i);
});
