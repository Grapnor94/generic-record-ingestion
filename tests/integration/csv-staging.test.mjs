import test from "node:test";
import assert from "node:assert/strict";
import { parseCsvRecords } from "../../dist/csv/parse-csv-records.js";
import { prepareRecordStaging } from "../../dist/ingestion/prepare-record-staging.js";

test("CSV output composes with staging while preserving raw source values", () => {
  const parsed = parseCsvRecords(
    'id,name,note\nA-1,"  Alice  ",\nA-2,Bob,review\n',
  );

  const contract = {
    schemaVersion: "csv-test-v1",
    requiredHeaders: ["id", "name", "note"],
  };

  const prepared = prepareRecordStaging({
    contract,
    headers: parsed.headers,
    rows: parsed.rows,
    transform: (row) => ({
      id: row.id,
      name: row.name.trim(),
      note: row.note,
    }),
    getRecordId: (_raw, canonical) => canonical.id,
    diagnose: (raw) =>
      raw.note === "review"
        ? [
            {
              code: "REVIEW",
              severity: "WARNING",
              fieldKey: "note",
              detail: "Needs review",
            },
          ]
        : [],
  });

  assert.equal(prepared.rows[0].rawSourceRow.name, "  Alice  ");
  assert.equal(prepared.rows[0].sourceRow.name, "Alice");
  assert.equal(prepared.rows[0].rawSourceRow.note, "");
  assert.equal(prepared.rows[1].diagnostics[0].code, "REVIEW");
  assert.equal(prepared.report.warningCount, 1);
  assert.equal(prepared.report.canProceedToPersistence, true);
});

test("schema contract validation remains owned by staging", () => {
  const parsed = parseCsvRecords("id,name,unexpected\nA-1,Alice,value\n");

  assert.deepEqual(parsed.headers, ["id", "name", "unexpected"]);

  assert.throws(
    () =>
      prepareRecordStaging({
        contract: {
          schemaVersion: "csv-test-v1",
          requiredHeaders: ["id", "name"],
        },
        headers: parsed.headers,
        rows: parsed.rows,
        transform: (row) => row,
        getRecordId: (row) => row.id,
      }),
    /unknown|unsupported/i,
  );
});
