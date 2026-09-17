import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import pg from "pg";
import { runMigrations } from "../../dist/db/migrations.js";
import { persistRecordStaging } from "../../dist/ingestion/persist-record-staging.js";
import { runRecordImport } from "../../dist/ingestion/run-record-import.js";
import { runRecordFileImport } from "../../dist/ingestion/run-record-file-import.js";
import {
  createImportBatch,
  getImportBatch,
  listImportRows,
  listImportIssues,
  getImportSummary,
} from "../../dist/db/imports.js";

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
  const first = await runMigrations(clientDatabase(client)); assert.deepEqual(first, { applied: ["0000_create_core_tables.sql", "0001_add_raw_source_row.sql", "0002_add_import_provenance.sql"], skipped: [] });
  const tables = await client.query(`select table_name from information_schema.tables where table_schema = current_schema() and table_name in ('import_batch','import_stage_row','import_issue','schema_migration') order by table_name`);
  assert.deepEqual(tables.rows.map((r) => r.table_name), ["import_batch","import_issue","import_stage_row","schema_migration"]);
  const rawColumn = await client.query(`select data_type,is_nullable from information_schema.columns where table_schema=current_schema() and table_name='import_stage_row' and column_name='raw_source_row'`);
  assert.equal(rawColumn.rowCount,1); assert.equal(rawColumn.rows[0].data_type,"jsonb"); assert.equal(rawColumn.rows[0].is_nullable,"YES");
  const ledger = await client.query("select filename from schema_migration order by filename"); assert.deepEqual(ledger.rows.map((r)=>r.filename),["0000_create_core_tables.sql","0001_add_raw_source_row.sql", "0002_add_import_provenance.sql"]);
  const second = await runMigrations(clientDatabase(client)); assert.deepEqual(second,{applied:[],skipped:["0000_create_core_tables.sql","0001_add_raw_source_row.sql", "0002_add_import_provenance.sql"]});
}); });

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
