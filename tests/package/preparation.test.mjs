import test from "node:test";
import { writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { createRequire } from "node:module";
import { run, withPackedConsumer } from "./packed-consumer.mjs";

const require = createRequire(import.meta.url);

test("packed preparation API works without runtime dependencies in JavaScript and TypeScript", async () => withPackedConsumer(async dir => {
    await writeFile(join(dir, "consumer.mjs"), `
import assert from "node:assert/strict";
import { prepareRecordStaging, RecordStagingCallbackError, UnsupportedRecordSchemaError } from "generic-record-ingestion";
const contract = { schemaVersion: "SYNTHETIC_V1", requiredHeaders: ["record_id", "label"] };
const input = { contract, headers: ["label", "record_id"], rows: [{ record_id: "R-1", label: "  original  " }],
  transform: row => { row.label = row.label.trim(); return { label: row.label }; },
  getRecordId: row => row.record_id,
  diagnose: () => [{ code: "REVIEW", severity: "WARNING", detail: "Review requested." }] };
const prepared = prepareRecordStaging(input);
assert.equal(prepared.rows[0].rawSourceRow.label, "  original  ");
assert.equal(prepared.rows[0].sourceRow.label, "original");
assert.equal(prepared.report.warningCount, 1);
assert.equal(prepared.report.canProceedToPersistence, true);
assert.throws(() => prepareRecordStaging({ ...input, headers: ["record_id"] }), UnsupportedRecordSchemaError);
const cause = new Error("synthetic callback failure");
assert.throws(() => prepareRecordStaging({ ...input, transform: () => { throw cause; } }),
  error => error instanceof RecordStagingCallbackError && error.cause === cause && error.rowNumber === 1);
`);
    run(process.execPath, [join(dir, "consumer.mjs")], dir);
    await writeFile(join(dir, "consumer.ts"), `
import { prepareRecordStaging, RecordStagingCallbackError, UnsupportedRecordSchemaError,
  type RecordSchemaContract, type PreparedRecord, type StagingDiagnostic, type StagingReport } from "generic-record-ingestion";
const contract: RecordSchemaContract = { schemaVersion: "SYNTHETIC_V1", requiredHeaders: ["record_id"] };
const warning: StagingDiagnostic = { code: "REVIEW", severity: "WARNING", detail: "Review requested." };
const result = prepareRecordStaging({ contract, headers: ["record_id"], rows: [{ record_id: "R-1" }],
  transform: row => ({ id: row.record_id }), getRecordId: row => row.record_id, diagnose: () => [warning] });
const rows: PreparedRecord[] = result.rows;
const report: StagingReport = result.report;
const callbackError: Error = new RecordStagingCallbackError(1, new Error("failure"));
const schemaError: Error = new UnsupportedRecordSchemaError("invalid headers");
// @ts-expect-error severity must remain a checked union, not any
const invalid: StagingDiagnostic = { code: "X", severity: "INFO", detail: "invalid" };
void [rows, report, callbackError, schemaError, invalid];
`);
    await writeFile(join(dir, "tsconfig.json"), JSON.stringify({ compilerOptions: {
      target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true,
      noEmit: true, types: [], skipLibCheck: false,
    }, files: ["consumer.ts"] }));
    run(process.execPath, [resolve(require.resolve("typescript"), "../../bin/tsc"), "-p", join(dir, "tsconfig.json")], dir);
}));
