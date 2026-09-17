import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

test("runnable packed inventory example demonstrates raw data and validation outcomes", () => {
  const result = spawnSync(process.execPath, ["scripts/run-inventory-example.mjs"], {
    cwd: fileURLToPath(new URL("../../", import.meta.url)),
    encoding: "utf8", timeout: 90_000,
  });
  assert.ifError(result.error);
  assert.equal(result.status, 0, result.stderr);
  const scenarios = JSON.parse(result.stdout);
  assert.equal(scenarios.clean.report.canProceedToPersistence, true);
  assert.equal(scenarios.clean.report.errorCount, 0);
  assert.equal(scenarios.clean.report.warningCount, 0);
  assert.deepEqual(scenarios.clean.rows[0].sourceRow, {
    itemName: "Widget", quantity: 12, warehouse: "NORTH",
  });
  assert.equal(scenarios.clean.rows[0].rawSourceRow.item_name, "  Widget  ");
  assert.equal(scenarios.clean.rows[0].rawSourceRow.quantity, " 12 ");
  assert.equal(scenarios.warning.rows[0].rawSourceRow.legacy_code, " OLD-7 ");
  assert.equal("legacy_code" in scenarios.warning.rows[0].sourceRow, false);
  assert.equal(scenarios.warning.report.warningCount, 1);
  assert.equal(scenarios.warning.report.errorCount, 0);
  assert.equal(scenarios.warning.report.canProceedToPersistence, true);
  assert.equal(scenarios.invalidQuantity.rows[0].sourceRow.quantity, null);
  assert.equal(scenarios.invalidQuantity.report.errorCount, 1);
  assert.equal(scenarios.invalidQuantity.report.canProceedToPersistence, false);
  assert.equal(scenarios.duplicate.report.errorCount, 1);
  assert.equal(scenarios.duplicate.report.canProceedToPersistence, false);
  assert.equal(scenarios.duplicate.rows[1].diagnostics[0].code, "DUPLICATE_RECORD_ID");
});
