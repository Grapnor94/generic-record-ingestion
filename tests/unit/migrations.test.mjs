import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { discoverMigrations } from "../../dist/db/migrations.js";

async function withTempDir(fn) {
  const dir = await mkdtemp(join(tmpdir(), "generic-record-migrations-"));
  try {
    await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("discovers SQL migrations in lexical order and ignores non-SQL files", async () => {
  await withTempDir(async (dir) => {
    await writeFile(join(dir, "0002_third.sql"), "select 3;");
    await writeFile(join(dir, "README.md"), "ignore me");
    await writeFile(join(dir, "0000_first.sql"), "select 1;");
    await writeFile(join(dir, "0001_second.sql"), "select 2;");

    const result = await discoverMigrations(dir);
    assert.deepEqual(
      result.map((migration) => migration.filename),
      ["0000_first.sql", "0001_second.sql", "0002_third.sql"],
    );
  });
});

test("rejects malformed SQL migration filenames", async () => {
  await withTempDir(async (dir) => {
    await writeFile(join(dir, "create_tables.sql"), "select 1;");
    await assert.rejects(
      () => discoverMigrations(dir),
      /Invalid migration filename: create_tables\.sql/,
    );
  });
});

test("empty migration directory returns an empty list", async () => {
  await withTempDir(async (dir) => {
    assert.deepEqual(await discoverMigrations(dir), []);
  });
});
