import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runMigrations } from "../../dist/db/migrations.js";

async function withMigrations(files, fn) {
  const dir = await mkdtemp(join(tmpdir(), "generic-record-runner-"));
  try {
    for (const [name, sql] of Object.entries(files)) {
      await writeFile(join(dir, name), sql);
    }
    await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

class FakeDb {
  constructor({ applied = [], failSql } = {}) {
    this.applied = new Set(applied);
    this.failSql = failSql;
    this.calls = [];
    this.inTransaction = false;
    this.pendingLedger = null;
  }

  async query(text, values = []) {
    const normalized = text.trim();
    this.calls.push({ text: normalized, values });

    if (/^create table if not exists schema_migration/i.test(normalized)) {
      return { rowCount: null, rows: [] };
    }

    if (/^select filename from schema_migration/i.test(normalized)) {
      return {
        rowCount: this.applied.size,
        rows: [...this.applied].map((filename) => ({ filename })),
      };
    }

    if (normalized.toLowerCase() === "begin") {
      this.inTransaction = true;
      this.pendingLedger = null;
      return { rowCount: null, rows: [] };
    }

    if (normalized.toLowerCase() === "commit") {
      if (this.pendingLedger) this.applied.add(this.pendingLedger);
      this.pendingLedger = null;
      this.inTransaction = false;
      return { rowCount: null, rows: [] };
    }

    if (normalized.toLowerCase() === "rollback") {
      this.pendingLedger = null;
      this.inTransaction = false;
      return { rowCount: null, rows: [] };
    }

    if (/^insert into schema_migration/i.test(normalized)) {
      this.pendingLedger = values[0];
      return { rowCount: 1, rows: [] };
    }

    if (this.failSql && normalized.includes(this.failSql)) {
      throw new Error("synthetic migration failure");
    }

    return { rowCount: null, rows: [] };
  }
}

test("creates the migration ledger and applies pending migrations in order", async () => {
  await withMigrations(
    {
      "0001_second.sql": "select 'second';",
      "0000_first.sql": "select 'first';",
    },
    async (dir) => {
      const db = new FakeDb();
      const result = await runMigrations(
        { kind: "CLIENT", client: db },
        { migrationsDir: dir },
      );
      assert.deepEqual(result, {
        applied: ["0000_first.sql", "0001_second.sql"],
        skipped: [],
      });
      assert.deepEqual([...db.applied], ["0000_first.sql", "0001_second.sql"]);
    },
  );
});

test("skips migrations already recorded in the ledger", async () => {
  await withMigrations(
    {
      "0000_first.sql": "select 'first';",
      "0001_second.sql": "select 'second';",
    },
    async (dir) => {
      const db = new FakeDb({ applied: ["0000_first.sql"] });
      const result = await runMigrations(
        { kind: "CLIENT", client: db },
        { migrationsDir: dir },
      );
      assert.deepEqual(result, {
        applied: ["0001_second.sql"],
        skipped: ["0000_first.sql"],
      });
    },
  );
});

test("rolls back a failed migration and does not record it", async () => {
  await withMigrations(
    {
      "0000_first.sql": "select 'first';",
      "0001_broken.sql": "select 'BROKEN';",
    },
    async (dir) => {
      const db = new FakeDb({ failSql: "BROKEN" });
      await assert.rejects(
        () => runMigrations(
          { kind: "CLIENT", client: db },
          { migrationsDir: dir },
        ),
        /Migration 0001_broken\.sql failed: synthetic migration failure/,
      );
      assert.deepEqual([...db.applied], ["0000_first.sql"]);
      assert.equal(db.inTransaction, false);
      assert.equal(db.calls.at(-1).text.toLowerCase(), "rollback");
    },
  );
});
