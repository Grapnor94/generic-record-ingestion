import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryImportDb } from "../support/memory-import-db.mjs";
import { createImportBatch, getImportBatch } from "../../dist/db/imports.js";
import { FrameworkError } from "../../dist/errors.js";
const workflow = await import("../../dist/ingestion/durable-import.js").catch(error => {
  if (error.code === "ERR_MODULE_NOT_FOUND") return {};
  throw error;
});
const csvText = "id,name\n1,Ada\n";
const contract = { schemaVersion: "v1", requiredHeaders: ["id", "name"] };
const metadata = { sourceKind: "CSV_TEXT", sourceName: null, sourceSizeBytes: Buffer.byteLength(csvText), sourceSha256: createHash("sha256").update(csvText).digest("hex"), sourcePath: null };
function input(db, source = { kind: "CSV_TEXT", text: csvText }) {
  return { database: { kind: "CLIENT", client: db }, importId: "resume", contract, source, transform: row => ({ name: row.name }), getRecordId: row => row.id };
}
async function resume(options) {
  assert.equal(typeof workflow.resumeDurableImport, "function", "explicit resume workflow exists");
  return workflow.resumeDurableImport(options);
}
const code = expected => error => error instanceof FrameworkError && error.code === expected;
async function unchanged(db, operation, errorCode, field) {
  const before = db.clone();
  await assert.rejects(operation, error => code(errorCode)(error) && (field === undefined || error.details?.field === field));
  assert.deepEqual(db.clone(), before);
}

test("resume missing attempt fails with IMPORT_NOT_FOUND without creating it", async () => {
  const db = new MemoryImportDb();
  await unchanged(db, () => resume(input(db)), "IMPORT_NOT_FOUND");
});
for (const status of ["VALIDATING", "VALIDATED", "FAILED"]) test(`resume refuses ${status} and preserves the attempt`, async () => {
  const db = new MemoryImportDb();
  await createImportBatch(db, { importId: "resume", schemaVersion: "v1", ...metadata });
  db.batches.get("resume").status = status;
  await unchanged(db, () => resume(input(db)), "IMPORT_NOT_RESUMABLE");
});
test("duplicate explicit start remains an error instead of recovering RECEIVED", async () => {
  const db = new MemoryImportDb();
  await createImportBatch(db, { importId: "resume", schemaVersion: "v1", ...metadata });
  assert.equal(typeof workflow.startDurableImport, "function");
  await unchanged(db, () => workflow.startDurableImport(input(db)), "IMPORT_ALREADY_EXISTS");
});
for (const kind of ["CSV_TEXT", "LOCAL_FILE"]) {
  for (const partial of [false, true]) test(`${kind} resume accepts ${partial ? "partial" : "complete"} provenance`, async () => {
    const dir = await mkdtemp(join(tmpdir(), "resume-"));
    try {
      const filePath = join(dir, "data.csv"); await writeFile(filePath, csvText);
      const source = kind === "CSV_TEXT" ? { kind, text: csvText } : { kind, filePath };
      const expected = { ...metadata, sourceKind: kind, sourceName: kind === "LOCAL_FILE" ? "data.csv" : null };
      const db = new MemoryImportDb();
      await createImportBatch(db, { importId: "resume", schemaVersion: "v1", ...(partial ? { sourceKind: kind } : expected) });
      const created = (await getImportBatch(db, "resume")).createdAt;
      const result = await resume(input(db, source));
      assert.equal(result.status, "VALIDATED"); assert.equal(result.summary.rowCount, 1);
      for (const [field, value] of Object.entries(expected)) assert.equal(result.summary[field], value);
      assert.deepEqual((await getImportBatch(db, "resume")).createdAt, created);
    } finally { await rm(dir, { recursive: true, force: true }); }
  });
  for (const [field, conflicting] of [["schemaVersion", "v2"], ["sourceKind", kind === "CSV_TEXT" ? "LOCAL_FILE" : "CSV_TEXT"], ["sourceName", "other.csv"], ["sourceSizeBytes", 99], ["sourceSha256", "0".repeat(64)], ["sourcePath", "established-path"]]) {
    test(`${kind} resume rejects conflicting ${field} without mutation`, async () => {
      const dir = await mkdtemp(join(tmpdir(), "resume-mismatch-"));
      try {
        const filePath = join(dir, "data.csv"); await writeFile(filePath, csvText);
        const source = kind === "CSV_TEXT" ? { kind, text: csvText } : { kind, filePath };
        const db = new MemoryImportDb();
        await createImportBatch(db, { importId: "resume", schemaVersion: "v1", ...metadata, sourceKind: kind, sourceName: kind === "LOCAL_FILE" ? "data.csv" : null, [field]: conflicting });
        await unchanged(db, () => resume(input(db, source)), "SOURCE_PROVENANCE_MISMATCH", field);
      } finally { await rm(dir, { recursive: true, force: true }); }
    });
  }
}


test("legacy all-null provenance can be completed from resupplied text", async () => {
  const db = new MemoryImportDb();
  await createImportBatch(db, { importId: "resume", schemaVersion: "v1" });
  const result = await resume(input(db));
  assert.equal(result.status, "VALIDATED");
  for (const [field, value] of Object.entries(metadata)) assert.equal(result.summary[field], value);
});

test("matching established local source path is preserved", async () => {
  const dir = await mkdtemp(join(tmpdir(), "resume-path-"));
  try {
    const filePath = join(dir, "data.csv"); await writeFile(filePath, csvText);
    const db = new MemoryImportDb();
    await createImportBatch(db, { importId: "resume", schemaVersion: "v1", ...metadata, sourceKind: "LOCAL_FILE", sourceName: "data.csv", sourcePath: filePath });
    const result = await resume(input(db, { kind: "LOCAL_FILE", filePath }));
    assert.equal(result.status, "VALIDATED");
    assert.equal(result.summary.sourcePath, filePath);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test("resume preserves callback failure identity and terminalizes best-effort", async () => {
  const db = new MemoryImportDb();
  await createImportBatch(db, { importId: "resume", schemaVersion: "v1", ...metadata });
  const failure = new Error("callback unavailable");
  await assert.rejects(() => resume({ ...input(db), transform() { throw failure; } }), error => error === failure);
  assert.equal(db.batches.get("resume").status, "FAILED");
  assert.equal(db.issues[0].issue_code, "STAGING_CALLBACK_ERROR");
  assert.equal(db.stage.length, 0);
});
for (const kind of ["CSV_TEXT", "LOCAL_FILE"]) test(`${kind} resume obeys source bounds without staging`, async () => {
  const dir = await mkdtemp(join(tmpdir(), "resume-limit-"));
  try {
    const filePath = join(dir, "data.csv"); await writeFile(filePath, csvText);
    const db = new MemoryImportDb();
    await createImportBatch(db, { importId: "resume", schemaVersion: "v1", ...metadata, sourceKind: kind, sourceName: kind === "LOCAL_FILE" ? "data.csv" : null });
    const source = kind === "CSV_TEXT" ? { kind, text: csvText } : { kind, filePath };
    const result = await resume({ ...input(db, source), limits: { maxSourceBytes: 1 } });
    assert.equal(result.status, "FAILED");
    assert.equal(result.summary.rowCount, 0);
    assert.equal(db.issues[0].issue_code, "SOURCE_SIZE_LIMIT_EXCEEDED");
  } finally { await rm(dir, { recursive: true, force: true }); }
});
