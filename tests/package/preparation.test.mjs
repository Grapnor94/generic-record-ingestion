import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";

const require = createRequire(import.meta.url);
const root = fileURLToPath(new URL("../../", import.meta.url));

function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", timeout: 60_000 });
  assert.ifError(result.error);
  assert.equal(result.status, 0, `${command} ${args.join(" ")}\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

test("packed preparation API works in an isolated JavaScript and TypeScript consumer", async () => {
  const dir = await mkdtemp(join(tmpdir(), "generic-package-"));
  try {
    // Pack the actual artifact; do not link to the source checkout or install runtime dependencies.
    assert.ok(process.env.npm_execpath, "Run this test through npm run test:package");
    const output = run(process.execPath, [process.env.npm_execpath, "pack", "--json", "--cache", join(dir, "cache"), "--pack-destination", dir], root);
    const packed = JSON.parse(output);
    const archive = Array.isArray(packed) ? packed[0] : packed["generic-record-ingestion"];
    const packageDir = join(dir, "node_modules", "generic-record-ingestion");
    await mkdir(packageDir, { recursive: true });
    run("tar", ["-xzf", join(dir, archive.filename), "-C", packageDir, "--strip-components=1"], dir);
    await writeFile(join(dir, "package.json"), JSON.stringify({ type: "module" }));
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
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
