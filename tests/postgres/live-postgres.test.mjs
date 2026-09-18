import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import pg from "pg";
import { runMigrations } from "../../dist/db/migrations.js";
import { persistRecordStaging } from "../../dist/ingestion/persist-record-staging.js";
import { runRecordImport } from "../../dist/ingestion/run-record-import.js";
import {
  runRecordFileImport,
  runRecordFileImportWithFileOperationsForTest,
} from "../../dist/ingestion/run-record-file-import.js";
import {
  createImportBatch,
  getImportBatch,
  listImportRows,
  listImportIssues,
  listImportRowsPage,
  listImportIssuesPage,
  getImportSummary,
} from "../../dist/db/imports.js";
import { FrameworkError } from "../../dist/errors.js";

const { Client, Pool } = pg;
const connectionConfig = { host: process.env.PGHOST ?? "127.0.0.1", port: Number(process.env.PGPORT ?? "5432"), user: process.env.PGUSER ?? "postgres", password: process.env.PGPASSWORD ?? "postgres", database: process.env.PGDATABASE ?? "postgres" };

async function withDatabase(fn) {
  const client = new Client(connectionConfig); await client.connect();
  const schema = `generic_ingestion_live_${process.pid}_${Date.now()}`;
  try { await client.query(`create schema "${schema}"`); await client.query(`set search_path to "${schema}", public`); await fn(client, schema); }
  finally { await client.query("reset search_path").catch(() => {}); await client.query(`drop schema if exists "${schema}" cascade`).catch(() => {}); await client.end(); }
}
async function withTempDir(fn) { const dir = await mkdtemp(join(tmpdir(), "record-file-live-")); try { return await fn(dir); } finally { await rm(dir, { recursive: true, force: true }); } }
function clientDatabase(client) { return { kind: "CLIENT", client }; }
function guardedPool(pool, schema, wrapClient = (client) => client) {
  return {
    async connect() {
      const client = await pool.connect();
      try {
        await client.query(`set search_path to "${schema}", public`);
        return wrapClient(client);
      } catch (error) {
        client.release();
        throw error;
      }
    },
    async query() {
      throw new Error("pool.query must not be used for dedicated work");
    },
  };
}
function assertPoolReturned(pool) {
  assert.equal(pool.totalCount, 1);
  assert.equal(pool.idleCount, 1);
  assert.equal(pool.waitingCount, 0);
}

const warningRow = { rowNumber: 1, recordId: "R-1", rawSourceRow: { record_id: " R-1 ", first_name: " Ada ", legacy_history_code: " RAW-7 " }, sourceRow: { first_name: "Ada" }, diagnostics: [{ code: "LEGACY_VALUE", severity: "WARNING", fieldKey: "legacy_history_code", detail: "Legacy value retained in raw source." }] };

test("migrations bootstrap an empty PostgreSQL schema and are idempotent", async () => { await withDatabase(async (client) => {
  const filenames = ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql", "0002_add_import_provenance.sql", "0003_add_import_query_indexes.sql"];
  const first = await runMigrations(clientDatabase(client)); assert.deepEqual(first, { applied: filenames, verified: [], legacyUnverified: [] });
  const tables = await client.query(`select table_name from information_schema.tables where table_schema = current_schema() and table_name in ('import_batch','import_stage_row','import_issue','schema_migration') order by table_name`);
  assert.deepEqual(tables.rows.map((r) => r.table_name), ["import_batch","import_issue","import_stage_row","schema_migration"]);
  const rawColumn = await client.query(`select data_type,is_nullable from information_schema.columns where table_schema=current_schema() and table_name='import_stage_row' and column_name='raw_source_row'`);
  assert.equal(rawColumn.rowCount,1); assert.equal(rawColumn.rows[0].data_type,"jsonb"); assert.equal(rawColumn.rows[0].is_nullable,"YES");
  const issueIndexes = await client.query("select indexname from pg_indexes where schemaname = current_schema() and tablename = 'import_issue' order by indexname");
  assert.deepEqual(issueIndexes.rows.map(row => row.indexname), ["import_issue_import_row_issue_idx", "import_issue_pkey"]);
  const stageIndexes = await client.query("select indexname from pg_indexes where schemaname = current_schema() and tablename = 'import_stage_row' order by indexname");
  assert.deepEqual(stageIndexes.rows.map(row => row.indexname), ["import_stage_row_pkey"]);
  const ledger = await client.query("select filename from schema_migration order by filename"); assert.deepEqual(ledger.rows.map((r)=>r.filename), filenames);
  const second = await runMigrations(clientDatabase(client)); assert.deepEqual(second,{applied:[],verified:filenames,legacyUnverified:[]});
}); });

test("concurrent migrators serialize ledger bootstrap and execution on dedicated sessions", async () => withDatabase(async (client, schema) => {
  await withTempDir(async dir => {
    const filename = "0000_slow.sql";
    const bytes = Buffer.from("select pg_sleep(0.2);\r\ncreate table migration_once (value text); -- é\r\n");
    await writeFile(join(dir, filename), bytes);
    const pool = new Pool({ ...connectionConfig, max: 2, application_name: schema });
    const database = { kind: "POOL", pool: guardedPool(pool, schema) };
    try {
      const results = await Promise.all([
        runMigrations(database, { migrationsDir: dir }),
        runMigrations(database, { migrationsDir: dir }),
      ]);
      assert.equal(pool.totalCount, 2);
      assert.equal(pool.idleCount, 2);
      assert.deepEqual(results.sort((a, b) => b.applied.length - a.applied.length), [
        { applied: [filename], verified: [], legacyUnverified: [] },
        { applied: [], verified: [filename], legacyUnverified: [] },
      ]);
      assert.deepEqual((await client.query("select filename, checksum_sha256 from schema_migration")).rows, [
        { filename, checksum_sha256: createHash("sha256").update(bytes).digest("hex") },
      ]);
      assert.equal((await client.query("select count(*)::int as count from pg_locks where locktype = 'advisory' and pid in (select pid from pg_stat_activity where application_name = $1)", [schema])).rows[0].count, 0);
    } finally { await pool.end(); }
  });
}));

test("live checksum drift prevents earlier pending and later migrations from running", async () => withDatabase(async client => {
  await withTempDir(async dir => {
    await writeFile(join(dir, "0001_applied.sql"), "create table original (id int);");
    await runMigrations(clientDatabase(client), { migrationsDir: dir });
    await writeFile(join(dir, "0001_applied.sql"), "create table original (id bigint);");
    await writeFile(join(dir, "0000_pending.sql"), "create table pending (id int);");
    await writeFile(join(dir, "0002_later.sql"), "create table later (id int);");
    await assert.rejects(() => runMigrations(clientDatabase(client), { migrationsDir: dir }), error => error instanceof FrameworkError && error.code === "MIGRATION_CHECKSUM_MISMATCH");
    assert.deepEqual((await client.query("select filename from schema_migration")).rows, [{ filename: "0001_applied.sql" }]);
    assert.deepEqual((await client.query("select to_regclass('pending') as pending, to_regclass('later') as later")).rows, [{ pending: null, later: null }]);
  });
}));

test("live failed migration rolls back SQL and does not advance the checksum ledger", async () => withDatabase(async client => {
  await withTempDir(async dir => {
    await writeFile(join(dir, "0000_broken.sql"), "create table rolled_back (id int); select * from missing_migration_table;");
    await assert.rejects(() => runMigrations(clientDatabase(client), { migrationsDir: dir }), error => error.code === "42P01");
    assert.deepEqual((await client.query("select * from schema_migration")).rows, []);
    assert.equal((await client.query("select to_regclass('rolled_back') as relation")).rows[0].relation, null);
    await writeFile(join(dir, "0000_broken.sql"), "create table rolled_back (id int);");
    assert.deepEqual((await runMigrations(clientDatabase(client), { migrationsDir: dir })).applied, ["0000_broken.sql"]);
    assert.match((await client.query("select checksum_sha256 from schema_migration")).rows[0].checksum_sha256, /^[0-9a-f]{64}$/);
  });
}));

test("legacy ledger upgrade keeps historical checksums NULL across repeated runs", async () => withDatabase(async client => {
  const legacy = ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql", "0002_add_import_provenance.sql"];
  await client.query("create table schema_migration (filename text primary key, applied_at timestamptz not null default current_timestamp)");
  for (const filename of legacy) {
    await client.query(await readFile(new URL(`../../db/migrations/${filename}`, import.meta.url), "utf8"));
    await client.query("insert into schema_migration (filename) values ($1)", [filename]);
  }
  const filename = "0003_add_import_query_indexes.sql";
  assert.deepEqual(await runMigrations(clientDatabase(client)), { applied: [filename], verified: [], legacyUnverified: legacy });
  assert.deepEqual(await runMigrations(clientDatabase(client)), { applied: [], verified: [filename], legacyUnverified: legacy });
  const ledger = (await client.query("select filename, checksum_sha256 from schema_migration order by filename")).rows;
  assert.deepEqual(ledger.slice(0, 3), legacy.map(filename => ({ filename, checksum_sha256: null })));
  assert.equal(ledger[3].checksum_sha256, createHash("sha256").update(await readFile(new URL(`../../db/migrations/${filename}`, import.meta.url))).digest("hex"));
  for (const checksum of ["A".repeat(64), "g".repeat(64), "a".repeat(63), "a".repeat(65)]) {
    await assert.rejects(() => client.query("update schema_migration set checksum_sha256 = $1 where filename = $2", [checksum, filename]), error => error.code === "23514");
  }
}));

test("real persistence keeps raw/canonical JSON separate and warning nonblocking", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); await client.query("insert into import_batch (import_id,schema_version,status) values ($1,$2,'RECEIVED')",["LIVE-1","test-v1"]);
  assert.deepEqual(await persistRecordStaging(client,{importId:"LIVE-1",rows:[warningRow]}),{status:"VALIDATED",issueCount:1});
  const staged=await client.query("select record_id,source_row,raw_source_row,validation_status from import_stage_row where import_id='LIVE-1' and row_number=1");
  assert.equal(staged.rows[0].record_id,"R-1"); assert.deepEqual(staged.rows[0].source_row,{first_name:"Ada"}); assert.deepEqual(staged.rows[0].raw_source_row,warningRow.rawSourceRow); assert.equal(staged.rows[0].validation_status,"VALID");
  assert.equal((await client.query("select status from import_batch where import_id='LIVE-1'")).rows[0].status,"VALIDATED");
}); });

test("real persistence marks blocking errors invalid and fails batch", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); await client.query("insert into import_batch (import_id,schema_version,status) values ($1,$2,'RECEIVED')",["LIVE-2","test-v1"]);
  const errorRow={...warningRow,diagnostics:[{code:"INVALID_VALUE",severity:"ERROR",fieldKey:"status",detail:"Synthetic blocking error."}]};
  assert.equal((await persistRecordStaging(client,{importId:"LIVE-2",rows:[errorRow]})).status,"FAILED");
  assert.equal((await client.query("select validation_status from import_stage_row where import_id='LIVE-2'")).rows[0].validation_status,"INVALID");
}); });

test("real PostgreSQL transaction rolls back partial writes on injected failure", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); await client.query("insert into import_batch (import_id,schema_version,status) values ($1,$2,'RECEIVED')",["LIVE-3","test-v1"]);
  const failingDb={async query(text,values){if(/insert\s+into\s+import_issue/i.test(text)) throw new Error("injected live PostgreSQL issue failure"); return client.query(text,values);}};
  await assert.rejects(()=>persistRecordStaging(failingDb,{importId:"LIVE-3",rows:[warningRow]}),/injected live PostgreSQL issue failure/);
  assert.equal((await client.query("select status from import_batch where import_id='LIVE-3'")).rows[0].status,"RECEIVED");
  assert.equal((await client.query("select count(*)::int as count from import_stage_row where import_id='LIVE-3'")).rows[0].count,0);
}); });

test("live import lifecycle and query API works after clean bootstrap", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); const created=await createImportBatch(client,{importId:"LIVE-Q-1",schemaVersion:"test-v1"}); assert.equal(created.status,"RECEIVED");
  assert.equal((await getImportBatch(client,"LIVE-Q-1"))?.schemaVersion,"test-v1"); await assert.rejects(()=>createImportBatch(client,{importId:"LIVE-Q-1",schemaVersion:"test-v1"}),{message:"Import batch already exists: LIVE-Q-1"});
  const rows=[{rowNumber:1,recordId:"R-1",rawSourceRow:{record_id:"R-1",value:"ok"},sourceRow:{value:"ok"},diagnostics:[]},{rowNumber:2,recordId:"R-2",rawSourceRow:{record_id:"R-2",value:"warn"},sourceRow:{value:"warn"},diagnostics:[{code:"WARN",severity:"WARNING",fieldKey:"value",detail:"Synthetic warning."}]},{rowNumber:3,recordId:"R-3",rawSourceRow:{record_id:"R-3",value:"bad"},sourceRow:{value:"bad"},diagnostics:[{code:"BAD",severity:"ERROR",fieldKey:"value",detail:"Synthetic error."}]}];
  assert.deepEqual(await persistRecordStaging(client,{importId:"LIVE-Q-1",rows}),{status:"FAILED",issueCount:2});
  assert.deepEqual((await listImportRows(client,"LIVE-Q-1")).map((r)=>[r.rowNumber,r.validationStatus]),[[1,"VALID"],[2,"VALID"],[3,"INVALID"]]);
  assert.deepEqual(await getImportSummary(client,"LIVE-Q-1"),{sourceKind:null,sourceName:null,sourceSizeBytes:null,sourceSha256:null,sourcePath:null,importId:"LIVE-Q-1",schemaVersion:"test-v1",status:"FAILED",rowCount:3,validRowCount:2,invalidRowCount:1,pendingRowCount:0,errorCount:1,warningCount:1});
  assert.equal(await getImportBatch(client,"MISSING"),null); assert.equal(await getImportSummary(client,"MISSING"),null); assert.deepEqual(await listImportRows(client,"MISSING"),[]); assert.deepEqual(await listImportIssues(client,"MISSING"),[]);
}); });

const orchestrationContract={schemaVersion:"live-orchestration-v1",requiredHeaders:["id","name"]};
const orchestrationTransform=(row)=>({name:row.name.trim()}); const orchestrationRecordId=(row)=>row.id||null;

test("live orchestration persists a successful two-row CSV import", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); const result=await runRecordImport({db:clientDatabase(client),importId:"LIVE-O-1",contract:orchestrationContract,csvText:"id,name\n1,Alice\n2,Bob\n",transform:orchestrationTransform,getRecordId:orchestrationRecordId});
  assert.equal(result.status,"VALIDATED"); assert.equal(result.summary.rowCount,2); const rows=await listImportRows(client,"LIVE-O-1"); assert.deepEqual(rows.map((r)=>r.validationStatus),["VALID","VALID"]); assert.deepEqual(rows[0].rawSourceRow,{id:"1",name:"Alice"});
}); });

test("live orchestration durably records a pre-staging CSV failure", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); const result=await runRecordImport({db:clientDatabase(client),importId:"LIVE-O-2",contract:orchestrationContract,csvText:'id,name\n1,"Alice\n',transform:orchestrationTransform,getRecordId:orchestrationRecordId});
  assert.equal(result.status,"FAILED"); assert.equal(result.summary.rowCount,0); const issues=await listImportIssues(client,"LIVE-O-2"); assert.equal(issues[0].issueCode,"CSV_PARSE_ERROR");
}); });

test("live orchestration persists row-level validation errors and fails the batch", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); const result=await runRecordImport({db:clientDatabase(client),importId:"LIVE-O-3",contract:orchestrationContract,csvText:"id,name\n1,Alice\n",transform:orchestrationTransform,getRecordId:orchestrationRecordId,diagnose:()=>[{code:"LIVE_INVALID_NAME",severity:"ERROR",fieldKey:"name",detail:"Synthetic live blocking diagnostic."}]});
  assert.equal(result.status,"FAILED"); assert.equal(result.summary.invalidRowCount,1); assert.equal((await listImportRows(client,"LIVE-O-3"))[0].validationStatus,"INVALID");
}); });

test("live paginated queries are bounded, stable, filtered, and gap-free", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client));
  await client.query("insert into import_batch (import_id, schema_version, status) values ('PAGE-1', 'test-v1', 'RECEIVED'), ('PAGE-EMPTY', 'test-v1', 'RECEIVED')");
  await client.query(`
    insert into import_stage_row (import_id, row_number, record_id, source_row, validation_status)
    select 'PAGE-1', row_number, 'R-' || row_number, jsonb_build_object('rowNumber', row_number),
      case when row_number % 2 = 0 then 'INVALID' else 'VALID' end
    from generate_series(1, 6) as row_number
  `);
  await client.query(`
    insert into import_issue (import_id, row_number, record_id, issue_code, severity, detail)
    values
      ('PAGE-1', null, null, 'NULL-1', 'ERROR', 'null one'),
      ('PAGE-1', null, null, 'NULL-2', 'WARNING', 'null two'),
      ('PAGE-1', null, null, 'NULL-3', 'ERROR', 'null three'),
      ('PAGE-1', 1, 'R-1', 'ROW-1', 'ERROR', 'row one'),
      ('PAGE-1', 2, 'R-2', 'ROW-2A', 'WARNING', 'row two a'),
      ('PAGE-1', 2, 'R-2', 'ROW-2B', 'ERROR', 'row two b'),
      ('PAGE-1', 2, 'R-2', 'ROW-2C', 'ERROR', 'row two c')
  `);

  const collect = async (fetchPage, options) => {
    const items = [];
    let cursor;
    let pageCount = 0;
    do {
      const page = await fetchPage({ ...options, ...(cursor === undefined ? {} : { cursor }) });
      items.push(...page.items);
      cursor = page.nextCursor ?? undefined;
      pageCount += 1;
    } while (cursor !== undefined);
    return { items, pageCount };
  };

  const firstRows = await listImportRowsPage(client, "PAGE-1", { pageSize: 2 });
  assert.deepEqual(await listImportRowsPage(client, "PAGE-1", { pageSize: 2 }), firstRows);
  const rows = await collect(options => listImportRowsPage(client, "PAGE-1", options), { pageSize: 2 });
  assert.equal(rows.pageCount, 3);
  assert.deepEqual(rows.items.map(row => row.rowNumber), [1, 2, 3, 4, 5, 6]);

  const invalidRows = await collect(options => listImportRowsPage(client, "PAGE-1", options), { pageSize: 1, status: "INVALID" });
  assert.equal(invalidRows.pageCount, 3);
  assert.deepEqual(invalidRows.items.map(row => row.rowNumber), [2, 4, 6]);

  const firstIssues = await listImportIssuesPage(client, "PAGE-1", { pageSize: 2 });
  assert.deepEqual(await listImportIssuesPage(client, "PAGE-1", { pageSize: 2 }), firstIssues);
  const issues = await collect(options => listImportIssuesPage(client, "PAGE-1", options), { pageSize: 2 });
  assert.equal(issues.pageCount, 4);
  assert.deepEqual(issues.items.map(issue => [issue.rowNumber, issue.issueCode]), [
    [null, "NULL-1"], [null, "NULL-2"], [null, "NULL-3"],
    [1, "ROW-1"], [2, "ROW-2A"], [2, "ROW-2B"], [2, "ROW-2C"],
  ]);

  const errorIssues = await collect(options => listImportIssuesPage(client, "PAGE-1", options), { pageSize: 2, severity: "ERROR" });
  assert.equal(errorIssues.pageCount, 3);
  assert.deepEqual(errorIssues.items.map(issue => issue.issueCode), ["NULL-1", "NULL-3", "ROW-1", "ROW-2B", "ROW-2C"]);
  const rowTwoIssues = await collect(options => listImportIssuesPage(client, "PAGE-1", options), { pageSize: 1, rowNumber: 2 });
  assert.equal(rowTwoIssues.pageCount, 3);
  assert.deepEqual(rowTwoIssues.items.map(issue => issue.issueCode), ["ROW-2A", "ROW-2B", "ROW-2C"]);

  assert.deepEqual(await listImportRowsPage(client, "PAGE-EMPTY"), { items: [], nextCursor: null });
  assert.deepEqual(await listImportIssuesPage(client, "PAGE-EMPTY"), { items: [], nextCursor: null });
  assert.deepEqual(await listImportRowsPage(client, "MISSING"), { items: [], nextCursor: null });
  assert.deepEqual(await listImportIssuesPage(client, "MISSING"), { items: [], nextCursor: null });
  assert.equal((await listImportRowsPage(client, "PAGE-1", { pageSize: 1000 })).items.length, 6);
  assert.equal((await listImportIssuesPage(client, "PAGE-1", { pageSize: 1000 })).items.length, 7);

  for (const request of [
    () => listImportRowsPage(client, "PAGE-1", { pageSize: Number.POSITIVE_INFINITY }),
    () => listImportIssuesPage(client, "PAGE-1", { pageSize: 0 }),
  ]) {
    await assert.rejects(request, error => error instanceof FrameworkError && error.code === "INVALID_PAGE_SIZE");
  }
  for (const request of [
    () => listImportRowsPage(client, "PAGE-1", { cursor: "not+base64url" }),
    () => listImportIssuesPage(client, "PAGE-1", { cursor: "not+base64url" }),
  ]) {
    await assert.rejects(request, error => error instanceof FrameworkError && error.code === "INVALID_CURSOR");
  }
}); });

test("live oversized text import fails atomically with complete provenance", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client));
  const csvText = "id,name\n1,é😀\n";
  const result = await runRecordImport({
    db: clientDatabase(client), importId: "LIVE-LIMIT-TEXT", contract: orchestrationContract,
    csvText, transform: orchestrationTransform, getRecordId: orchestrationRecordId,
    limits: { maxSourceBytes: Buffer.byteLength(csvText, "utf8") - 1 },
  });
  const batch = await getImportBatch(client, "LIVE-LIMIT-TEXT");
  assert.equal(result.status, "FAILED"); assert.equal(batch?.status, "FAILED");
  assert.equal(batch?.sourceSizeBytes, Buffer.byteLength(csvText, "utf8"));
  assert.equal(batch?.sourceSha256, createHash("sha256").update(csvText, "utf8").digest("hex"));
  assert.deepEqual(await listImportRows(client, "LIVE-LIMIT-TEXT"), []);
  assert.equal((await listImportIssues(client, "LIVE-LIMIT-TEXT"))[0].issueCode, "SOURCE_SIZE_LIMIT_EXCEEDED");
}); });

test("live file metadata overflow fails atomically with partial provenance", async () => withTempDir(async (dir) => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client));
  const bytes = Buffer.from([0xff, 0xff]);
  const filePath = join(dir, "metadata-overflow.csv"); await writeFile(filePath, bytes);
  const result = await runRecordFileImport({
    db: clientDatabase(client), importId: "LIVE-LIMIT-FILE-METADATA", contract: orchestrationContract,
    filePath, transform: orchestrationTransform, getRecordId: orchestrationRecordId,
    limits: { maxSourceBytes: 1 },
  });
  const batch = await getImportBatch(client, "LIVE-LIMIT-FILE-METADATA");
  assert.equal(result.status, "FAILED"); assert.equal(batch?.status, "FAILED");
  assert.equal(batch?.sourceName, "metadata-overflow.csv");
  assert.equal(batch?.sourceSizeBytes, null); assert.equal(batch?.sourceSha256, null);
  assert.deepEqual(await listImportRows(client, "LIVE-LIMIT-FILE-METADATA"), []);
  assert.equal((await listImportIssues(client, "LIVE-LIMIT-FILE-METADATA"))[0].issueCode, "SOURCE_SIZE_LIMIT_EXCEEDED");
}); }));

test("live post-stat file growth fails atomically with exact provenance", async () => withTempDir(async (dir) => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client));
  const bytes = Buffer.from("id,name\n1,Alice\n", "utf8");
  const filePath = join(dir, "grew.csv");
  const result = await runRecordFileImportWithFileOperationsForTest({
    db: clientDatabase(client), importId: "LIVE-LIMIT-FILE-GROWTH", contract: orchestrationContract,
    filePath, transform: orchestrationTransform, getRecordId: orchestrationRecordId,
    limits: { maxSourceBytes: bytes.length - 1 },
  }, {
    stat: async () => ({ size: 1 }),
    readFile: async () => bytes,
  });
  const batch = await getImportBatch(client, "LIVE-LIMIT-FILE-GROWTH");
  assert.equal(result.status, "FAILED"); assert.equal(batch?.status, "FAILED");
  assert.equal(batch?.sourceSizeBytes, bytes.length);
  assert.equal(batch?.sourceSha256, createHash("sha256").update(bytes).digest("hex"));
  assert.deepEqual(await listImportRows(client, "LIVE-LIMIT-FILE-GROWTH"), []);
  assert.equal((await listImportIssues(client, "LIVE-LIMIT-FILE-GROWTH"))[0].issueCode, "SOURCE_SIZE_LIMIT_EXCEEDED");
}); }));

test("live row overflow fails atomically with complete file provenance", async () => withTempDir(async (dir) => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client));
  const bytes = Buffer.from("id,name\n1,Alice\n2,Bob\n", "utf8");
  const filePath = join(dir, "row-overflow.csv"); await writeFile(filePath, bytes);
  const result = await runRecordFileImport({
    db: clientDatabase(client), importId: "LIVE-LIMIT-ROWS", contract: orchestrationContract,
    filePath, transform: orchestrationTransform, getRecordId: orchestrationRecordId,
    limits: { maxDataRows: 1 },
  });
  const batch = await getImportBatch(client, "LIVE-LIMIT-ROWS");
  assert.equal(result.status, "FAILED"); assert.equal(batch?.status, "FAILED");
  assert.equal(batch?.sourceSizeBytes, bytes.length);
  assert.equal(batch?.sourceSha256, createHash("sha256").update(bytes).digest("hex"));
  assert.deepEqual(await listImportRows(client, "LIVE-LIMIT-ROWS"), []);
  assert.equal((await listImportIssues(client, "LIVE-LIMIT-ROWS"))[0].issueCode, "ROW_LIMIT_EXCEEDED");
}); }));

test("live bounded imports still accept identical content under distinct IDs", async () => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client));
  const csvText = "id,name\n1,Alice\n";
  for (const importId of ["LIVE-LIMIT-SAME-A", "LIVE-LIMIT-SAME-B"]) {
    const result = await runRecordImport({
      db: clientDatabase(client), importId, contract: orchestrationContract,
      csvText, transform: orchestrationTransform, getRecordId: orchestrationRecordId,
      limits: { maxSourceBytes: Buffer.byteLength(csvText, "utf8"), maxDataRows: 1 },
    });
    assert.equal(result.status, "VALIDATED");
  }
  assert.equal((await listImportRows(client, "LIVE-LIMIT-SAME-A")).length, 1);
  assert.equal((await listImportRows(client, "LIVE-LIMIT-SAME-B")).length, 1);
  assert.equal((await getImportBatch(client, "LIVE-LIMIT-SAME-A"))?.sourceSha256, (await getImportBatch(client, "LIVE-LIMIT-SAME-B"))?.sourceSha256);
}); });

test("pg.Pool workflow uses one acquired connection and returns it after success", async () => { await withDatabase(async (client, schema) => {
  const pool = new Pool({ ...connectionConfig, max: 1 });
  const database = { kind: "POOL", pool: guardedPool(pool, schema) };
  try {
    await runMigrations(database);
    assertPoolReturned(pool);
    const result = await runRecordImport({
      db: database,
      importId: "LIVE-POOL-1",
      contract: orchestrationContract,
      csvText: "id,name\n1,Alice\n",
      transform: orchestrationTransform,
      getRecordId: orchestrationRecordId,
    });
    assert.equal(result.status, "VALIDATED");
    assert.equal((await listImportRows(client, "LIVE-POOL-1")).length, 1);
    assertPoolReturned(pool);
  } finally {
    await pool.end();
  }
}); });

test("pg.Pool workflow rolls back partial staging and returns the connection", async () => { await withDatabase(async (client, schema) => {
  const pool = new Pool({ ...connectionConfig, max: 1 });
  const primary = new Error("injected pool issue failure");
  let injectFailure = true;
  const database = {
    kind: "POOL",
    pool: guardedPool(pool, schema, (acquired) => ({
      async query(text, values) {
        if (injectFailure && /insert\s+into\s+import_issue/i.test(text)) {
          injectFailure = false;
          throw primary;
        }
        return acquired.query(text, values);
      },
      release() {
        acquired.release();
      },
    })),
  };
  try {
    await runMigrations(database);
    assertPoolReturned(pool);
    await assert.rejects(
      () => runRecordImport({
        db: database,
        importId: "LIVE-POOL-ROLLBACK",
        contract: orchestrationContract,
        csvText: "id,name\n1,Alice\n",
        transform: orchestrationTransform,
        getRecordId: orchestrationRecordId,
        diagnose: () => [{
          code: "POOL_WARNING",
          severity: "WARNING",
          fieldKey: "name",
          detail: "Trigger the injected issue failure.",
        }],
      }),
      (error) => error === primary,
    );
    assert.equal((await client.query("select status from import_batch where import_id = 'LIVE-POOL-ROLLBACK'")).rows[0].status, "FAILED");
    assert.equal((await client.query("select count(*)::int as count from import_stage_row where import_id = 'LIVE-POOL-ROLLBACK'")).rows[0].count, 0);
    assert.equal((await listImportIssues(client, "LIVE-POOL-ROLLBACK"))[0].issueCode, "IMPORT_PERSISTENCE_ERROR");
    assertPoolReturned(pool);
  } finally {
    await pool.end();
  }
}); });

test("live filesystem import persists a successful CSV file", async () => withTempDir(async (dir) => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); const filePath=join(dir,"records.csv"); await writeFile(filePath,"id,name\n1,Alice\n2,Bob\n","utf8");
  const result=await runRecordFileImport({db:clientDatabase(client),importId:"LIVE-F-1",contract:orchestrationContract,filePath,transform:orchestrationTransform,getRecordId:orchestrationRecordId});
  assert.equal(result.status,"VALIDATED"); assert.equal(result.summary.rowCount,2); const rows=await listImportRows(client,"LIVE-F-1"); assert.deepEqual(rows.map((r)=>r.validationStatus),["VALID","VALID"]); assert.deepEqual(rows[0].rawSourceRow,{id:"1",name:"Alice"});
}); }));

test("live filesystem import durably records a missing-file failure", async () => withTempDir(async (dir) => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); const result=await runRecordFileImport({db:clientDatabase(client),importId:"LIVE-F-2",contract:orchestrationContract,filePath:join(dir,"missing.csv"),transform:orchestrationTransform,getRecordId:orchestrationRecordId});
  assert.equal(result.status,"FAILED"); assert.equal(result.summary.rowCount,0); assert.equal((await getImportBatch(client,"LIVE-F-2"))?.status,"FAILED"); assert.deepEqual(await listImportRows(client,"LIVE-F-2"),[]);
  const issues=await listImportIssues(client,"LIVE-F-2"); assert.equal(issues.length,1); assert.equal(issues[0].issueCode,"FILE_READ_ERROR"); assert.equal(issues[0].rowNumber,null); assert.equal(issues[0].recordId,null);
}); }));

test("live filesystem import preserves downstream row-error semantics", async () => withTempDir(async (dir) => { await withDatabase(async (client) => {
  await runMigrations(clientDatabase(client)); const filePath=join(dir,"invalid-row.csv"); await writeFile(filePath,"id,name\n1,Alice\n","utf8");
  const result=await runRecordFileImport({db:clientDatabase(client),importId:"LIVE-F-3",contract:orchestrationContract,filePath,transform:orchestrationTransform,getRecordId:orchestrationRecordId,diagnose:()=>[{code:"LIVE_FILE_INVALID_NAME",severity:"ERROR",fieldKey:"name",detail:"Synthetic filesystem blocking diagnostic."}]});
  assert.equal(result.status,"FAILED"); assert.equal(result.summary.invalidRowCount,1); const rows=await listImportRows(client,"LIVE-F-3"); assert.equal(rows[0].validationStatus,"INVALID"); const issues=await listImportIssues(client,"LIVE-F-3"); assert.equal(issues[0].issueCode,"LIVE_FILE_INVALID_NAME"); assert.equal(issues[0].rowNumber,1);
}); }));

test("live staging claim and terminalization each advance the lifecycle clock", async () => withDatabase(async client => {
  await runMigrations(clientDatabase(client));
  const initial = await createImportBatch(client, { importId: "clock-stage", schemaVersion: "v1" });
  const before = (await client.query("select updated_at::text as timestamp from import_batch where import_id = 'clock-stage'")).rows[0].timestamp;
  let claimed;
  let claimedTimestamp;
  const observed = { async query(sql, values) {
    const result = await client.query(sql, values);
    if (/set status = 'VALIDATING'/i.test(sql)) {
      claimed = await getImportBatch(client, "clock-stage");
      claimedTimestamp = (await client.query("select updated_at::text as timestamp from import_batch where import_id = 'clock-stage'")).rows[0].timestamp;
    }
    return result;
  } };
  await persistRecordStaging(observed, { importId: "clock-stage", rows: [warningRow] });
  const final = await getImportBatch(client, "clock-stage");
  assert.equal((await client.query("select $2::timestamptz > $1::timestamptz as advanced", [before, claimedTimestamp])).rows[0].advanced, true);
  assert.equal((await client.query("select updated_at > $1::timestamptz as advanced from import_batch where import_id = 'clock-stage'", [claimedTimestamp])).rows[0].advanced, true);
  assert.deepEqual(claimed.createdAt, initial.createdAt);
  assert.deepEqual(final.createdAt, initial.createdAt);
}));

for (const partial of [false, true]) test(`two simultaneous matching resumes claim one row set with ${partial ? "partial" : "complete"} provenance`, async () => withDatabase(async (client, schema) => {
  const workflow = await import("../../dist/ingestion/durable-import.js").catch(() => ({}));
  assert.equal(typeof workflow.resumeDurableImport, "function");
  await runMigrations(clientDatabase(client));
  const text = "id,name\n1,Ada\n";
  const provenance = { sourceKind: "CSV_TEXT", sourceSizeBytes: Buffer.byteLength(text), sourceSha256: createHash("sha256").update(text).digest("hex") };
  const initial = await createImportBatch(client, { importId: "race-resume", schemaVersion: "v1", ...(partial ? {} : provenance) });
  const pool = new Pool({ ...connectionConfig, max: 2 });
  let arrivals = 0; let release;
  const gate = new Promise(resolve => { release = resolve; });
  const database = { kind: "POOL", pool: guardedPool(pool, schema, connection => ({
    release: () => connection.release(),
    async query(sql, values) {
      const result = await connection.query(sql, values);
      if (/from import_batch\s+where import_id/i.test(sql) && ++arrivals <= 2) {
        if (arrivals === 2) release();
        await gate;
      }
      return result;
    },
  })) };
  try {
    const input = { database, importId: "race-resume", contract: { schemaVersion: "v1", requiredHeaders: ["id", "name"] }, source: { kind: "CSV_TEXT", text }, transform: row => ({ name: row.name }), getRecordId: row => row.id, diagnose: () => [{ code: "WARN", severity: "WARNING", detail: "one warning" }] };
    const outcomes = await Promise.allSettled([workflow.resumeDurableImport(input), workflow.resumeDurableImport(input)]);
    assert.equal(outcomes.filter(outcome => outcome.status === "fulfilled" && outcome.value.status === "VALIDATED").length, 1);
    const rejected = outcomes.find(outcome => outcome.status === "rejected");
    assert.ok(rejected.reason instanceof FrameworkError);
    assert.equal(rejected.reason.code, "IMPORT_NOT_RESUMABLE");
    assert.equal((await listImportRows(client, input.importId)).length, 1);
    assert.equal((await listImportIssues(client, input.importId)).length, 1);
    const final = await getImportBatch(client, input.importId);
    for (const key of ["sourceKind", "sourceName", "sourceSizeBytes", "sourceSha256", "sourcePath", "createdAt"]) assert.deepEqual(final[key], key in provenance ? provenance[key] : initial[key]);
  } finally { await pool.end(); }
}));


test("resume cannot overwrite provenance established after its initial read", async () => withDatabase(async client => {
  const { resumeDurableImport } = await import("../../dist/ingestion/durable-import.js");
  await runMigrations(clientDatabase(client));
  await createImportBatch(client, { importId: "provenance-race", schemaVersion: "v1" });
  let concurrentBatch;
  let firstRead = true;
  const observed = { async query(sql, values) {
    const result = await client.query(sql, values);
    if (firstRead && /from import_batch\s+where import_id/i.test(sql)) {
      firstRead = false;
      await client.query("update import_batch set source_sha256 = $1 where import_id = 'provenance-race'", ["0".repeat(64)]);
      concurrentBatch = await getImportBatch(client, "provenance-race");
    }
    return result;
  } };
  await assert.rejects(() => resumeDurableImport({
    database: clientDatabase(observed), importId: "provenance-race",
    contract: { schemaVersion: "v1", requiredHeaders: ["id", "name"] },
    source: { kind: "CSV_TEXT", text: "id,name\n1,Ada\n" },
    transform: row => ({ name: row.name }), getRecordId: row => row.id,
  }), error => error instanceof FrameworkError && error.code === "SOURCE_PROVENANCE_MISMATCH" && error.details.field === "sourceSha256");
  assert.deepEqual(await getImportBatch(client, "provenance-race"), concurrentBatch);
  assert.deepEqual(await listImportRows(client, "provenance-race"), []);
  assert.deepEqual(await listImportIssues(client, "provenance-race"), []);
}));
