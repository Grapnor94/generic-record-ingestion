import test from "node:test";
import assert from "node:assert/strict";
import { FrameworkError } from "../../dist/errors.js";
import {
  DEFAULT_INGESTION_LIMITS,
  MAX_INGESTION_LIMITS,
  resolveIngestionLimits,
} from "../../dist/ingestion/limits.js";

const fields = ["maxSourceBytes", "maxDataRows"];

test("omitted limits resolve to finite positive safe defaults inside the envelope", () => {
  for (const input of [undefined, {}, { maxDataRows: undefined }]) {
    const result = resolveIngestionLimits(input);
    assert.deepEqual(result, DEFAULT_INGESTION_LIMITS);
    for (const field of fields) {
      assert.ok(Number.isSafeInteger(result[field]) && result[field] > 0);
      assert.ok(Number.isSafeInteger(MAX_INGESTION_LIMITS[field]));
      assert.ok(result[field] <= MAX_INGESTION_LIMITS[field]);
    }
  }
});

for (const field of fields) {
  for (const value of [0, -1, 1.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, null, "10", true]) {
    test(`${field} rejects ${String(value)} with a framework error`, () => {
      assert.throws(
        () => resolveIngestionLimits({ [field]: value }),
        error => error instanceof FrameworkError && error.code === "INVALID_INGESTION_LIMIT",
      );
    });
  }

  test(`${field} rejects values above the characterized maximum`, () => {
    assert.throws(
      () => resolveIngestionLimits({ [field]: MAX_INGESTION_LIMITS[field] + 1 }),
      error => error instanceof FrameworkError && error.code === "INVALID_INGESTION_LIMIT",
    );
  });

  test(`${field} accepts its boundaries and defaults the omitted field`, () => {
    const other = fields.find(candidate => candidate !== field);
    for (const value of [1, 17, MAX_INGESTION_LIMITS[field]]) {
      const input = { [field]: value };
      const result = resolveIngestionLimits(input);
      assert.equal(result[field], value);
      assert.equal(result[other], DEFAULT_INGESTION_LIMITS[other]);
      assert.deepEqual(input, { [field]: value });
    }
  });
}

test("resolving both fields preserves both supplied values", () => {
  assert.deepEqual(resolveIngestionLimits({ maxSourceBytes: 123, maxDataRows: 7 }), {
    maxSourceBytes: 123, maxDataRows: 7,
  });
});

test("callers cannot mutate the shared defaults or enlarge the envelope", () => {
  for (const limits of [DEFAULT_INGESTION_LIMITS, MAX_INGESTION_LIMITS]) {
    assert.throws(() => { limits.maxDataRows = Infinity; }, TypeError);
  }
  const result = resolveIngestionLimits();
  result.maxDataRows = 1;
  assert.deepEqual(resolveIngestionLimits(), DEFAULT_INGESTION_LIMITS);
});
