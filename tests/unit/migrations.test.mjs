import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { discoverMigrations, runMigrations } from "../../dist/db/migrations.js";

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

for (const failurePoint of ["select 'migration';", "commit"]) {
  test(`preserves ${failurePoint} error with rollback, unlock and release failures secondary`, async () => {
    await withTempDir(async dir => {
      await writeFile(join(dir, "0000_first.sql"), "select 'migration';");
      const primary = new Error("primary failure");
      const rollback = new Error("rollback failure");
      const unlock = new Error("unlock failure");
      const release = new Error("release failure");
      const calls = [];
      const client = {
        async query(sql, values) {
          calls.push({ sql: sql.trim(), values });
          if (sql === failurePoint) throw primary;
          if (sql === "rollback") throw rollback;
          if (/pg_advisory_unlock/.test(sql)) throw unlock;
          return { rows: [], rowCount: 0 };
        },
        release() { calls.push({ sql: "release" }); throw release; },
      };
      await assert.rejects(() => runMigrations({ kind: "POOL", pool: { async connect() { return client; } } }, { migrationsDir: dir }), error => {
        assert.equal(error, primary);
        assert.deepEqual(error.cleanupErrors, [rollback, unlock, release]);
        return true;
      });
      assert.match(calls[0].sql, /pg_advisory_lock/);
      const unlockCall = calls.find(call => /pg_advisory_unlock/.test(call.sql));
      assert.deepEqual(unlockCall.values, calls[0].values);
      assert.equal(calls.at(-1).sql, "release");
    });
  });
}

test("a failed lock acquisition releases the connection without attempting unlock or ledger work", async () => {
  await withTempDir(async dir => {
    const failure = new Error("cannot acquire lock");
    const calls = [];
    const client = {
      async query(sql) { calls.push(sql); throw failure; },
      release() { calls.push("release"); },
    };
    await assert.rejects(() => runMigrations({ kind: "POOL", pool: { async connect() { return client; } } }, { migrationsDir: dir }), error => error === failure);
    assert.match(calls[0], /pg_advisory_lock/);
    assert.equal(calls.length, 2);
    assert.equal(calls[1], "release");
  });
});
