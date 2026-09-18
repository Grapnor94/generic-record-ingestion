import test from "node:test";
import { writeFile } from "node:fs/promises";
import { join } from "node:path";
import { run, withPackedConsumer } from "../package/packed-consumer.mjs";

test("packed root supports live client and pool workflows, bounded reads, and RECEIVED recovery", async () => withPackedConsumer(async dir => {
  await writeFile(join(dir, "consumer.mjs"), `
import assert from "node:assert/strict";
import pg from "pg";
import { prepareRecordStaging, startDurableImport, resumeDurableImport,
  getImportAttempt, getImportSummary, getImportRowsPage, getImportIssuesPage,
  runMigrations, FrameworkError } from "generic-record-ingestion";
const config = { host: process.env.PGHOST ?? "127.0.0.1", port: Number(process.env.PGPORT ?? "5432"),
  user: process.env.PGUSER ?? "postgres", password: process.env.PGPASSWORD ?? "postgres", database: process.env.PGDATABASE ?? "postgres" };
const admin = new pg.Client(config);
await admin.connect();
const schema = "packed_root_" + process.pid + "_" + Date.now();
const pool = new pg.Pool({ ...config, max: 1, options: "-c search_path=" + schema });
const database = { kind: "POOL", pool };
const clientDatabase = { kind: "CLIENT", client: admin };
const returned = () => { assert.equal(pool.idleCount, 1); assert.equal(pool.waitingCount, 0); };
try {
  await admin.query('create schema "' + schema + '"');
  await admin.query('set search_path to "' + schema + '"');
  const firstMigration = await runMigrations(database);
  assert.equal(firstMigration.applied.length, 4);
  assert.deepEqual(firstMigration.legacyUnverified, []);
  returned();
  const secondMigration = await runMigrations(clientDatabase);
  assert.deepEqual(secondMigration.verified, firstMigration.applied);
  assert.deepEqual(secondMigration.applied, []);
  const callbacks = { contract: { schemaVersion: "EXAMPLE_V1", requiredHeaders: ["record_id", "label"] },
    transform: row => ({ label: row.label.trim() }), getRecordId: row => row.record_id,
    diagnose: () => [{ code: "REVIEW", severity: "WARNING", detail: "Review requested." }] };
  assert.equal(prepareRecordStaging({ ...callbacks, headers: ["record_id", "label"], rows: [{ record_id: "R-1", label: " One " }] }).report.warningCount, 1);
  const source = { kind: "CSV_TEXT", text: "record_id,label\\nR-1, One \\nR-2,Two\\n" };
  const input = { ...callbacks, database, importId: "packed-one", source };
  const result = await startDurableImport(input);
  assert.equal(result.status, "VALIDATED");
  assert.equal(result.summary.rowCount, 2);
  assert.equal(result.summary.warningCount, 2);
  returned();
  const attempt = await getImportAttempt(database, input.importId);
  assert.equal(attempt.status, "VALIDATED");
  assert.equal(attempt.sourceKind, "CSV_TEXT");
  assert.match(attempt.sourceSha256, /^[a-f0-9]{64}$/);
  assert.equal(attempt.sourceSizeBytes, Buffer.byteLength(source.text));
  returned();
  assert.deepEqual(await getImportSummary(clientDatabase, input.importId), result.summary);
  const first = await getImportRowsPage(database, input.importId, { pageSize: 1, status: "VALID" });
  returned();
  assert.deepEqual(first.items.map(row => row.recordId), ["R-1"]);
  assert.equal(first.items[0].rawSourceRow.label, " One ");
  assert.equal(first.items[0].sourceRow.label, "One");
  assert.equal(typeof first.nextCursor, "string");
  const second = await getImportRowsPage(clientDatabase, input.importId, { pageSize: 1, status: "VALID", cursor: first.nextCursor });
  assert.deepEqual(second.items.map(row => row.recordId), ["R-2"]);
  assert.equal(second.nextCursor, null);
  const issue1 = await getImportIssuesPage(database, input.importId, { pageSize: 1, severity: "WARNING" });
  returned();
  const issue2 = await getImportIssuesPage(clientDatabase, input.importId, { pageSize: 1, severity: "WARNING", cursor: issue1.nextCursor });
  assert.deepEqual([...issue1.items, ...issue2.items].map(issue => [issue.rowNumber, issue.issueCode]), [[1, "REVIEW"], [2, "REVIEW"]]);
  assert.equal(issue2.nextCursor, null);
  await assert.rejects(getImportRowsPage(database, input.importId, { pageSize: 0 }), error => error instanceof FrameworkError && error.code === "INVALID_PAGE_SIZE");
  returned();
  await assert.rejects(getImportIssuesPage(database, input.importId, { cursor: first.nextCursor }), error => error instanceof FrameworkError && error.code === "INVALID_CURSOR");
  returned();
  assert.equal(await getImportAttempt(clientDatabase, "missing"), null);
  assert.equal(await getImportSummary(database, "missing"), null);
  returned();
  await assert.rejects(startDurableImport(input), error => error instanceof FrameworkError && error.code === "IMPORT_ALREADY_EXISTS");
  returned();
  // Seed a RECEIVED attempt with partial provenance, as after interruption before atomic staging.
  await admin.query("insert into import_batch (import_id, schema_version, status) values ('packed-resume', 'EXAMPLE_V1', 'RECEIVED')");
  const resumed = await resumeDurableImport({ ...input, importId: "packed-resume" });
  assert.equal(resumed.status, "VALIDATED");
  assert.equal(resumed.summary.rowCount, 2);
  assert.equal(resumed.summary.errorCount, 0);
  assert.deepEqual((await getImportRowsPage(database, "packed-resume")).items.map(row => row.recordId), ["R-1", "R-2"]);
  assert.deepEqual((await getImportIssuesPage(database, "packed-resume")).items.map(issue => issue.issueCode), ["REVIEW", "REVIEW"]);
  returned();
  await assert.rejects(resumeDurableImport({ ...input, importId: "packed-resume" }), error => error instanceof FrameworkError && error.code === "IMPORT_NOT_RESUMABLE");
  returned();
  const clientResult = await startDurableImport({ ...input, database: clientDatabase, importId: "packed-client" });
  assert.equal(clientResult.summary.rowCount, 2);
  assert.equal((await admin.query("select 1 as usable")).rows[0].usable, 1);
} finally {
  await pool.end();
  try { await admin.query('drop schema if exists "' + schema + '" cascade'); }
  finally { await admin.end(); }
}
`);
  run(process.execPath, [join(dir, "consumer.mjs")], dir);
}, { runtimeDependencies: true }));
