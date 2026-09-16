import test from "node:test";
import assert from "node:assert/strict";
import { copyFile, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import pg from "pg";
import { runMigrations } from "../../dist/db/migrations.js";
import * as imports from "../../dist/db/imports.js";

const nullSource = { sourceKind: null, sourceName: null, sourceSizeBytes: null, sourceSha256: null, sourcePath: null };
const hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
function source(value) { return Object.fromEntries(Object.keys(nullSource).map(key => [key, value[key]])); }

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
  const dir = await mkdtemp(join(tmpdir(), "provenance-migration-"));
  try {
    for (const filename of ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql"]) {
      await copyFile(new URL(`../../db/migrations/${filename}`, import.meta.url), join(dir, filename));
    }
    await runMigrations(db, { migrationsDir: dir });
    for (const status of ["RECEIVED", "VALIDATING", "VALIDATED", "FAILED"]) {
      await db.query("insert into import_batch (import_id, schema_version, status) values ($1, 'v6', $1)", [status]);
    }
    const before = (await db.query("select * from import_batch order by import_id")).rows;
    const migrated = await runMigrations(db);
    assert.deepEqual(migrated.applied, ["0002_add_import_provenance.sql"]);
    for (const row of before) {
      const batch = await imports.getImportBatch(db, row.import_id);
      assert.deepEqual(source(batch), nullSource);
      assert.deepEqual(source(await imports.getImportSummary(db, row.import_id)), nullSource);
      assert.equal(batch.status, row.status);
      assert.deepEqual(batch.createdAt, row.created_at);
      assert.deepEqual(batch.updatedAt, row.updated_at);
    }
    assert.deepEqual((await runMigrations(db)).applied, []);
  } finally { await rm(dir, { recursive: true, force: true }); }
}));

test("PostgreSQL constrains kind, safe nonnegative byte size and lowercase SHA-256", async () => withDatabase(async db => {
  await runMigrations(db);
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
  await runMigrations(db);
  const metadata = { sourceKind: "LOCAL_FILE", sourceName: "input.csv", sourceSizeBytes: 3, sourceSha256: hash, sourcePath: null };
  for (const importId of ["first", "second"]) {
    assert.deepEqual(source(await imports.createImportBatch(db, { importId, schemaVersion: "v7", ...metadata })), metadata);
    assert.deepEqual(source(await imports.getImportBatch(db, importId)), metadata);
    assert.deepEqual(source(await imports.getImportSummary(db, importId)), metadata);
  }
  await assert.rejects(() => imports.createImportBatch(db, { importId: "first", schemaVersion: "v7", ...metadata }), /already exists/);
}));

test("content metadata update is limited to RECEIVED and preserves source identity", async () => withDatabase(async db => {
  await runMigrations(db);
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
