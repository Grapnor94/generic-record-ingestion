import test from "node:test";
import assert from "node:assert/strict";
import pg from "pg";
import { runMigrations } from "../../dist/db/migrations.js";
import { persistRecordStaging } from "../../dist/ingestion/persist-record-staging.js";

const { Client } = pg;

const connectionConfig = {
  host: process.env.PGHOST ?? "127.0.0.1",
  port: Number(process.env.PGPORT ?? "5432"),
  user: process.env.PGUSER ?? "postgres",
  password: process.env.PGPASSWORD ?? "postgres",
  database: process.env.PGDATABASE ?? "postgres",
};

async function withDatabase(fn) {
  const client = new Client(connectionConfig);
  await client.connect();
  const schema = `generic_ingestion_live_${process.pid}_${Date.now()}`;
  try {
    await client.query(`create schema "${schema}"`);
    await client.query(`set search_path to "${schema}", public`);
    await fn(client, schema);
  } finally {
    await client.query("reset search_path").catch(() => {});
    await client.query(`drop schema if exists "${schema}" cascade`).catch(() => {});
    await client.end();
  }
}

const warningRow = {
  rowNumber: 1,
  recordId: "R-1",
  rawSourceRow: {
    record_id: " R-1 ",
    first_name: " Ada ",
    legacy_history_code: " RAW-7 ",
  },
  sourceRow: { first_name: "Ada" },
  diagnostics: [
    {
      code: "LEGACY_VALUE",
      severity: "WARNING",
      fieldKey: "legacy_history_code",
      detail: "Legacy value retained in raw source.",
    },
  ],
};

test("migrations bootstrap an empty PostgreSQL schema and are idempotent", async () => {
  await withDatabase(async (client) => {
    const first = await runMigrations(client);
    assert.deepEqual(first, {
      applied: ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql"],
      skipped: [],
    });

    const tables = await client.query(`
      select table_name
      from information_schema.tables
      where table_schema = current_schema()
        and table_name in ('import_batch', 'import_stage_row', 'import_issue', 'schema_migration')
      order by table_name
    `);
    assert.deepEqual(
      tables.rows.map((row) => row.table_name),
      ["import_batch", "import_issue", "import_stage_row", "schema_migration"],
    );

    const rawColumn = await client.query(`
      select data_type, is_nullable
      from information_schema.columns
      where table_schema = current_schema()
        and table_name = 'import_stage_row'
        and column_name = 'raw_source_row'
    `);
    assert.equal(rawColumn.rowCount, 1);
    assert.equal(rawColumn.rows[0].data_type, "jsonb");
    assert.equal(rawColumn.rows[0].is_nullable, "YES");

    const ledger = await client.query(
      "select filename from schema_migration order by filename",
    );
    assert.deepEqual(
      ledger.rows.map((row) => row.filename),
      ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql"],
    );

    const second = await runMigrations(client);
    assert.deepEqual(second, {
      applied: [],
      skipped: ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql"],
    });
  });
});

test("real persistence keeps raw/canonical JSON separate and warning nonblocking", async () => {
  await withDatabase(async (client) => {
    await runMigrations(client);
    await client.query(
      "insert into import_batch (import_id, schema_version, status) values ($1, $2, 'RECEIVED')",
      ["LIVE-1", "test-v1"],
    );

    const result = await persistRecordStaging(client, {
      importId: "LIVE-1",
      rows: [warningRow],
    });

    assert.deepEqual(result, { status: "VALIDATED", issueCount: 1 });

    const staged = await client.query(`
      select record_id, source_row, raw_source_row, validation_status
      from import_stage_row
      where import_id = 'LIVE-1' and row_number = 1
    `);
    assert.equal(staged.rows[0].record_id, "R-1");
    assert.deepEqual(staged.rows[0].source_row, { first_name: "Ada" });
    assert.deepEqual(staged.rows[0].raw_source_row, warningRow.rawSourceRow);
    assert.equal(staged.rows[0].validation_status, "VALID");

    const batch = await client.query("select status from import_batch where import_id = 'LIVE-1'");
    assert.equal(batch.rows[0].status, "VALIDATED");

    const issue = await client.query("select severity, issue_code from import_issue where import_id = 'LIVE-1'");
    assert.equal(issue.rows[0].severity, "WARNING");
    assert.equal(issue.rows[0].issue_code, "LEGACY_VALUE");
  });
});

test("real persistence marks blocking errors invalid and fails batch", async () => {
  await withDatabase(async (client) => {
    await runMigrations(client);
    await client.query(
      "insert into import_batch (import_id, schema_version, status) values ($1, $2, 'RECEIVED')",
      ["LIVE-2", "test-v1"],
    );

    const errorRow = {
      ...warningRow,
      diagnostics: [
        {
          code: "INVALID_VALUE",
          severity: "ERROR",
          fieldKey: "status",
          detail: "Synthetic blocking error.",
        },
      ],
    };

    const result = await persistRecordStaging(client, {
      importId: "LIVE-2",
      rows: [errorRow],
    });

    assert.equal(result.status, "FAILED");
    const staged = await client.query("select validation_status from import_stage_row where import_id = 'LIVE-2'");
    assert.equal(staged.rows[0].validation_status, "INVALID");
    const batch = await client.query("select status from import_batch where import_id = 'LIVE-2'");
    assert.equal(batch.rows[0].status, "FAILED");
  });
});

test("real PostgreSQL transaction rolls back partial writes on injected failure", async () => {
  await withDatabase(async (client) => {
    await runMigrations(client);
    await client.query(
      "insert into import_batch (import_id, schema_version, status) values ($1, $2, 'RECEIVED')",
      ["LIVE-3", "test-v1"],
    );

    const failingDb = {
      async query(text, values) {
        if (/insert\s+into\s+import_issue/i.test(text)) {
          throw new Error("injected live PostgreSQL issue failure");
        }
        return client.query(text, values);
      },
    };

    await assert.rejects(
      () => persistRecordStaging(failingDb, { importId: "LIVE-3", rows: [warningRow] }),
      /injected live PostgreSQL issue failure/,
    );

    const batch = await client.query("select status from import_batch where import_id = 'LIVE-3'");
    assert.equal(batch.rows[0].status, "RECEIVED");
    const staged = await client.query("select count(*)::int as count from import_stage_row where import_id = 'LIVE-3'");
    assert.equal(staged.rows[0].count, 0);
    const issues = await client.query("select count(*)::int as count from import_issue where import_id = 'LIVE-3'");
    assert.equal(issues.rows[0].count, 0);
  });
});
