import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
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
    this.applied = new Map(applied.map(filename => [filename, null]));
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

    if (/^select filename(?:, checksum_sha256)? from schema_migration/i.test(normalized)) {
      return {
        rowCount: this.applied.size,
        rows: [...this.applied].map(([filename, checksum_sha256]) => ({ filename, checksum_sha256 })),
      };
    }

    if (normalized.toLowerCase() === "begin") {
      this.inTransaction = true;
      this.pendingLedger = null;
      return { rowCount: null, rows: [] };
    }

    if (normalized.toLowerCase() === "commit") {
      if (this.pendingLedger) this.applied.set(...this.pendingLedger);
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
      this.pendingLedger = [values[0], values[1]];
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
        verified: [],
        legacyUnverified: [],
      });
      assert.deepEqual([...db.applied.keys()], ["0000_first.sql", "0001_second.sql"]);
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
        verified: [],
        legacyUnverified: ["0000_first.sql"],
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
        /synthetic migration failure/,
      );
      assert.deepEqual([...db.applied.keys()], ["0000_first.sql"]);
      assert.equal(db.inTransaction, false);
      assert.ok(db.calls.some(call => call.text.toLowerCase() === "rollback"));
    },
  );
});

test("stores exact byte checksums and verifies unchanged files on rerun", async () => {
  const bytes = Buffer.from("-- é\r\nselect 1;\r\n");
  await withMigrations({ "0000_first.sql": bytes }, async dir => {
    const db = new FakeDb();
    await runMigrations({ kind: "CLIENT", client: db }, { migrationsDir: dir });
    assert.equal(db.applied.get("0000_first.sql"), createHash("sha256").update(bytes).digest("hex"));
    assert.deepEqual(await runMigrations({ kind: "CLIENT", client: db }, { migrationsDir: dir }), {
      applied: [], verified: ["0000_first.sql"], legacyUnverified: [],
    });
  });
});

test("checks all applied checksums before executing any pending migration", async () => {
  await withMigrations({ "0001_applied.sql": "select 1;" }, async dir => {
    const db = new FakeDb();
    const database = { kind: "CLIENT", client: db };
    await runMigrations(database, { migrationsDir: dir });
    await writeFile(join(dir, "0001_applied.sql"), "select 2;");
    await writeFile(join(dir, "0000_pending.sql"), "select 'pending';");
    await writeFile(join(dir, "0002_later.sql"), "select 'later';");
    db.calls.length = 0;
    await assert.rejects(() => runMigrations(database, { migrationsDir: dir }), error => error.code === "MIGRATION_CHECKSUM_MISMATCH");
    assert.equal(db.calls.some(call => /pending|later/.test(call.text)), false);
    assert.deepEqual([...db.applied.keys()], ["0001_applied.sql"]);
  });
});

test("a missing checksummed migration fails before pending migrations", async () => {
  await withMigrations({ "0000_applied.sql": "select 1;" }, async dir => {
    const db = new FakeDb();
    const database = { kind: "CLIENT", client: db };
    await runMigrations(database, { migrationsDir: dir });
    await rm(join(dir, "0000_applied.sql"));
    await writeFile(join(dir, "0001_pending.sql"), "select 'pending';");
    await assert.rejects(() => runMigrations(database, { migrationsDir: dir }), error => error.code === "MIGRATION_CHECKSUM_MISMATCH");
    assert.deepEqual([...db.applied.keys()], ["0000_applied.sql"]);
  });
});

test("default migrations include the import issue pagination index in lexical order", async () => {
  const db = new FakeDb();
  const result = await runMigrations({ kind: "CLIENT", client: db });
  assert.deepEqual(result.applied, [
    "0000_create_core_tables.sql",
    "0001_add_raw_source_row.sql",
    "0002_add_import_provenance.sql",
    "0003_add_import_query_indexes.sql",
  ]);
});
