import test from "node:test";
import assert from "node:assert/strict";
import { prepareRecordStaging } from "../../dist/ingestion/prepare-record-staging.js";

function prepare(ids, diagnose) {
  return prepareRecordStaging({
    contract: { schemaVersion: "SYNTHETIC_SCALE_V1", requiredHeaders: ["record_id"] },
    headers: ["record_id"], rows: ids.map(record_id => ({ record_id })),
    transform: () => ({}), getRecordId: row => row.record_id, diagnose,
  });
}

for (const kind of ["duplicate", "missing"]) {
  test(`${kind}-heavy batches do not repeatedly scan prepared rows`, () => {
    const count = 2000;
    const ids = Array(count).fill(kind === "duplicate" ? "SKU-1" : " ");
    const originalFind = Array.prototype.find;
    let predicateVisits = 0;
    let result;
    // Count the identified bottleneck without a machine-dependent timing gate.
    // Execute the real search and restore immediately; no result is mocked.
    Array.prototype.find = function(predicate, thisArg) {
      return originalFind.call(this, function(value, index, array) {
        predicateVisits++;
        return predicate.call(thisArg, value, index, array);
      });
    };
    try {
      result = prepare(ids);
    } finally {
      Array.prototype.find = originalFind;
    }
    assert.equal(result.report.errorCount, kind === "duplicate" ? count - 1 : count);
    assert.equal(result.report.canProceedToPersistence, false);
    for (let index = 0; index < count; index++) {
      const codes = result.rows[index].diagnostics.map(diagnostic => diagnostic.code);
      assert.deepEqual(codes, kind === "duplicate" && index === 0 ? [] :
        [kind === "duplicate" ? "DUPLICATE_RECORD_ID" : "MISSING_RECORD_ID"]);
    }
    assert.ok(predicateVisits <= count * 4,
      `Expected a linear search-work budget; observed ${predicateVisits} predicate visits for ${count} rows`);
  });
}

test("mixed identity errors retain row attachment, normalization and diagnostic order", () => {
  const result = prepare([" SKU-1 ", " ", "SKU-2", "SKU-1", "", "SKU-2"],
    () => [{ code: "REVIEW", severity: "WARNING", detail: "Synthetic review." }]);
  assert.deepEqual(result.rows.map(row => ({ row: row.rowNumber, codes: row.diagnostics.map(d => d.code) })), [
    { row: 1, codes: ["REVIEW"] },
    { row: 2, codes: ["REVIEW", "MISSING_RECORD_ID"] },
    { row: 3, codes: ["REVIEW"] },
    { row: 4, codes: ["REVIEW", "DUPLICATE_RECORD_ID"] },
    { row: 5, codes: ["REVIEW", "MISSING_RECORD_ID"] },
    { row: 6, codes: ["REVIEW", "DUPLICATE_RECORD_ID"] },
  ]);
  assert.equal(result.report.errorCount, 4);
  assert.equal(result.report.warningCount, 6);
  assert.equal(result.report.rowsWithDiagnostics, 6);
  assert.equal(result.rows[0].rawSourceRow.record_id, " SKU-1 ");
});
