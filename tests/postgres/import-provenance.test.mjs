import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { tmpdir } from "node:os";
import pg from "pg";
import { runMigrations } from "../../dist/db/migrations.js";
import * as imports from "../../dist/db/imports.js";
import { runRecordImport } from "../../dist/ingestion/run-record-import.js";
import { runRecordFileImport } from "../../dist/ingestion/run-record-file-import.js";

const nullSource = { sourceKind: null, sourceName: null, sourceSizeBytes: null, sourceSha256: null, sourcePath: null };
const hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const contract = { schemaVersion: "v7", requiredHeaders: ["id", "name"] };
const callbacks = { transform: row => ({ name: row.name }), getRecordId: row => row.id || null };
function source(value) { return Object.fromEntries(Object.keys(nullSource).map(key => [key, value[key]])); }
function database(client) { return { kind: "CLIENT", client }; }

async function withDatabase(fn) {
  const client = new pg.Client({ host: process.env.PGHOST ?? "127.0.0.1", port: Number(process.env.PGPORT ?? "5432"), user: process.env.PGUSER ?? "postgres", password: process.env.PGPASSWORD ?? "postgres", database: process.env.PGDATABASE ?? "postgres" });
  await client.connect();
  const schema = `provenance_${process.pid}_${Date.now()}`;
  try {
    await client.query(`create schema "${schema}"`);
    await client.query(`set search_path to "${schema}", public`);
    await fn(client);
  } finally {
    await client.query("reset search_path");
    await client.query(`drop schema "${schema}" cascade`);
    await client.end();
  }
}

test("provenance migration preserves legacy rows in every lifecycle state", async () => withDatabase(async db => {
  const legacy = ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql"];
  await db.query("create table schema_migration (filename text primary key, applied_at timestamptz not null default current_timestamp)");
  for (const filename of legacy) {
    await db.query(await readFile(new URL(`../../db/migrations/${filename}`, import.meta.url), "utf8"));
    await db.query("insert into schema_migration (filename) values ($1)", [filename]);
  }
  for (const status of ["RECEIVED", "VALIDATING", "VALIDATED", "FAILED"]) {
    await db.query("insert into import_batch (import_id, schema_version, status) values ($1, 'v6', $1)", [status]);
  }
  const before = (await db.query("select * from import_batch order by import_id")).rows;
  const migrated = await runMigrations(database(db));
  assert.deepEqual(migrated.applied, ["0002_add_import_provenance.sql", "0003_add_import_query_indexes.sql"]);
  assert.deepEqual(migrated.legacyUnverified, legacy);
  assert.deepEqual(migrated.verified, []);
  for (const row of before) {
    const batch = await imports.getImportBatch(db, row.import_id);
    assert.deepEqual(source(batch), nullSource);
    assert.deepEqual(source(await imports.getImportSummary(db, row.import_id)), nullSource);
    assert.equal(batch.status, row.status);
    assert.deepEqual(batch.createdAt, row.created_at);
    assert.deepEqual(batch.updatedAt, row.updated_at);
  }
  assert.deepEqual(await runMigrations(database(db)), { applied: [], verified: migrated.applied, legacyUnverified: legacy });
}));

test("PostgreSQL constrains kind, safe nonnegative byte size and lowercase SHA-256", async () => withDatabase(async db => {
  await runMigrations(database(db));
  const invalid = [
    ["HTTP", 3, hash], ["csv_text", 3, hash],
    ["CSV_TEXT", -1, hash], ["CSV_TEXT", "9007199254740992", hash],
    ["CSV_TEXT", 3, hash.toUpperCase()], ["CSV_TEXT", 3, "g".repeat(64)],
    ["CSV_TEXT", 3, "a".repeat(63)], ["CSV_TEXT", 3, "a".repeat(65)],
  ];
  for (const values of invalid) {
    await assert.rejects(() => db.query("insert into import_batch (import_id, schema_version, status, source_kind, source_size_bytes, source_sha256) values ('bad', 'v7', 'RECEIVED', $1, $2, $3)", values), e => e.code === "23514");
  }
  for (const size of [0, 9007199254740991]) {
    const batch = await imports.createImportBatch(db, { importId: String(size), schemaVersion: "v7", sourceKind: "CSV_TEXT", sourceSizeBytes: size, sourceSha256: hash });
    assert.equal(batch.sourceSizeBytes, size);
    assert.equal((await imports.getImportSummary(db, String(size))).sourceSizeBytes, size);
  }
}));

test("batch creation and queries expose provenance and permit duplicate content", async () => withDatabase(async db => {
  await runMigrations(database(db));
  const metadata = { sourceKind: "LOCAL_FILE", sourceName: "input.csv", sourceSizeBytes: 3, sourceSha256: hash, sourcePath: null };
  for (const importId of ["first", "second"]) {
    assert.deepEqual(source(await imports.createImportBatch(db, { importId, schemaVersion: "v7", ...metadata })), metadata);
    assert.deepEqual(source(await imports.getImportBatch(db, importId)), metadata);
    assert.deepEqual(source(await imports.getImportSummary(db, importId)), metadata);
  }
  await assert.rejects(() => imports.createImportBatch(db, { importId: "first", schemaVersion: "v7", ...metadata }), /already exists/);
}));

test("content metadata update is limited to RECEIVED and preserves source identity", async () => withDatabase(async db => {
  await runMigrations(database(db));
  assert.equal(typeof imports.updateImportSourceContentMetadata, "function");
  await imports.createImportBatch(db, { importId: "file", schemaVersion: "v7", sourceKind: "LOCAL_FILE", sourceName: "file.csv" });
  await imports.updateImportSourceContentMetadata(db, { importId: "file", sourceSizeBytes: 3, sourceSha256: hash });
  const batch = await imports.getImportBatch(db, "file");
  assert.equal(batch.status, "RECEIVED");
  assert.deepEqual(source(batch), { sourceKind: "LOCAL_FILE", sourceName: "file.csv", sourceSizeBytes: 3, sourceSha256: hash, sourcePath: null });
  for (const status of ["VALIDATING", "VALIDATED", "FAILED"]) {
    await db.query("update import_batch set status = $1 where import_id = 'file'", [status]);
    await assert.rejects(() => imports.updateImportSourceContentMetadata(db, { importId: "file", sourceSizeBytes: 0, sourceSha256: "0".repeat(64) }), /RECEIVED/);
    assert.deepEqual(source(await imports.getImportBatch(db, "file")), source(batch));
  }
  await assert.rejects(() => imports.updateImportSourceContentMetadata(db, { importId: "missing", sourceSizeBytes: 0, sourceSha256: hash }), /RECEIVED/);
}));

test("live CSV imports expose exact text provenance including duplicate content attempts", async () => withDatabase(async db => {
  await runMigrations(database(db));
  for (const importId of ["text-a", "text-b"]) {
    const result = await runRecordImport({ db: database(db), importId, contract, csvText: "abc", ...callbacks });
    assert.equal(result.status, "FAILED");
    const expected = { sourceKind: "CSV_TEXT", sourceName: null, sourceSizeBytes: 3, sourceSha256: hash, sourcePath: null };
    assert.deepEqual(source(result.summary), expected);
    assert.deepEqual(source(await imports.getImportBatch(db, importId)), expected);
    assert.equal((await imports.listImportIssues(db, importId))[0].issueCode, "SCHEMA_HEADER_ERROR");
  }
}));

test("live files retain original-byte provenance for success, invalid UTF-8 and unreadable paths", async () => withDatabase(async db => {
  await runMigrations(database(db));
  const dir = await mkdtemp(join(tmpdir(), "provenance-live-file-"));
  try {
    for (const [name, bytes, status] of [
      ["bom.csv", Buffer.from("\uFEFFid,name\r\n1,é😀\r\n"), "VALIDATED"],
      ["invalid.csv", Buffer.from([0xc3, 0x28]), "FAILED"],
      ["absent.csv", null, "FAILED"],
    ]) {
      const filePath = join(dir, name);
      if (bytes !== null) await writeFile(filePath, bytes);
      const result = await runRecordFileImport({ db: database(db), importId: name, contract, filePath, ...callbacks });
      const expected = { sourceKind: "LOCAL_FILE", sourceName: name, sourceSizeBytes: bytes === null ? null : bytes.length, sourceSha256: bytes === null ? null : createHash("sha256").update(bytes).digest("hex"), sourcePath: null };
      assert.equal(result.status, status);
      assert.deepEqual(source(result.summary), expected);
      assert.deepEqual(source(await imports.getImportBatch(db, name)), expected);
      if (status === "FAILED") assert.equal((await imports.listImportIssues(db, name))[0].issueCode, "FILE_READ_ERROR");
    }
  } finally { await rm(dir, { recursive: true, force: true }); }
}));

test("live provenance write failure terminalizes separately from FILE_READ_ERROR", async () => withDatabase(async db => {
  await runMigrations(database(db));
  await db.query(`create function reject_content_metadata() returns trigger language plpgsql as $$ begin raise exception 'metadata unavailable'; end $$`);
  await db.query("create trigger reject_metadata before update of source_size_bytes on import_batch for each row execute function reject_content_metadata()");
  const dir = await mkdtemp(join(tmpdir(), "provenance-live-error-"));
  try {
    const filePath = join(dir, "invalid.csv");
    await writeFile(filePath, Buffer.from([0xff]));
    await assert.rejects(() => runRecordFileImport({ db: database(db), importId: "db-error", contract, filePath, ...callbacks }), e => e.code === "P0001" && e.message === "metadata unavailable");
    const batch = await imports.getImportBatch(db, "db-error");
    assert.equal(batch.status, "FAILED");
    assert.deepEqual(source(batch), { ...nullSource, sourceKind: "LOCAL_FILE", sourceName: "invalid.csv" });
    assert.equal((await imports.listImportIssues(db, "db-error"))[0].issueCode, "IMPORT_PROVENANCE_ERROR");
    assert.deepEqual(await imports.listImportRows(db, "db-error"), []);
  } finally { await rm(dir, { recursive: true, force: true }); }
}));

test("every batch mutation advances updatedAt while preserving createdAt", async () => withDatabase(async db => {
  await runMigrations(database(db));
  const initial = await imports.createImportBatch(db, { importId: "clock", schemaVersion: "v7" });
  const before = (await db.query("select updated_at::text as timestamp from import_batch where import_id = 'clock'")).rows[0].timestamp;
  await imports.updateImportSourceContentMetadata(db, { importId: "clock", sourceSizeBytes: 3, sourceSha256: hash });
  const provenance = await imports.getImportBatch(db, "clock");
  assert.equal((await db.query("select updated_at > $1::timestamptz as advanced from import_batch where import_id = 'clock'", [before])).rows[0].advanced, true);
  assert.deepEqual(provenance.createdAt, initial.createdAt);
  const afterProvenance = (await db.query("select updated_at::text as timestamp from import_batch where import_id = 'clock'")).rows[0].timestamp;
  await imports.failImportBatch(db, { importId: "clock", issueCode: "TEST", detail: "failure" });
  const failed = await imports.getImportBatch(db, "clock");
  assert.equal((await db.query("select updated_at > $1::timestamptz as advanced from import_batch where import_id = 'clock'", [afterProvenance])).rows[0].advanced, true);
  assert.deepEqual(failed.createdAt, initial.createdAt);
}));
