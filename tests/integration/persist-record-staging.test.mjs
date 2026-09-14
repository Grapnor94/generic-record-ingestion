import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { persistRecordStaging } from "../../dist/ingestion/persist-record-staging.js";

class MemoryDb {
  constructor(){this.batches=new Map();this.stage=[];this.issues=[];this.snapshot=null;this.failOnIssue=false;}
  clone(){return {batches:new Map(this.batches),stage:structuredClone(this.stage),issues:structuredClone(this.issues)};}
  async query(sql,values=[]){
    const q=sql.replace(/\s+/g," ").trim().toLowerCase();
    if(q==="begin"){this.snapshot=this.clone(); return {rowCount:null,rows:[]};}
    if(q==="commit"){this.snapshot=null; return {rowCount:null,rows:[]};}
    if(q==="rollback"){if(this.snapshot){this.batches=this.snapshot.batches;this.stage=this.snapshot.stage;this.issues=this.snapshot.issues;}this.snapshot=null;return {rowCount:null,rows:[]};}
    if(q.startsWith("update import_batch") && q.includes("validating")){
      const [id]=values; if(this.batches.get(id)!=="RECEIVED") return {rowCount:0,rows:[]}; this.batches.set(id,"VALIDATING"); return {rowCount:1,rows:[{import_id:id}]};
    }
    if(q.startsWith("insert into import_stage_row")){
      const [importId,rowNumber,recordId,sourceJson,rawJson]=values; this.stage.push({importId,rowNumber,recordId,source_row:JSON.parse(sourceJson),raw_source_row:rawJson?JSON.parse(rawJson):null,validation_status:"PENDING"}); return {rowCount:1,rows:[]};
    }
    if(q.startsWith("insert into import_issue")){
      if(this.failOnIssue) throw new Error("injected issue failure");
      const [importId,rowNumber,recordId,issue_code,severity,field_key,detail]=values; this.issues.push({importId,rowNumber,recordId,issue_code,severity,field_key,detail}); return {rowCount:1,rows:[]};
    }
    if(q.startsWith("update import_stage_row")){
      const [importId,invalidRows]=values; for(const row of this.stage){if(row.importId===importId)row.validation_status=invalidRows.includes(row.rowNumber)?"INVALID":"VALID";} return {rowCount:this.stage.length,rows:[]};
    }
    if(q.startsWith("update import_batch set status")){
      const [id,status]=values; this.batches.set(id,status); return {rowCount:1,rows:[]};
    }
    throw new Error(`Unsupported SQL: ${q}`);
  }
}

const warningRow={rowNumber:1,recordId:"R-1",rawSourceRow:{record_id:" R-1 ",first_name:" Ada ",legacy_history_code:" RAW-7 "},sourceRow:{first_name:"Ada"},diagnostics:[{code:"LEGACY_VALUE",severity:"WARNING",fieldKey:"legacy_history_code",detail:"Legacy value retained in raw source."}]};

test("migration adds nullable raw_source_row",async()=>{
  const sql=await readFile(new URL("../../db/migrations/0001_add_raw_source_row.sql",import.meta.url),"utf8");
  assert.match(sql,/add column if not exists raw_source_row jsonb/i);
  assert.doesNotMatch(sql,/raw_source_row jsonb not null/i);
});

test("persists raw/canonical rows separately and warning stays valid",async()=>{
  const db=new MemoryDb(); db.batches.set("I-1","RECEIVED");
  const result=await persistRecordStaging(db,{importId:"I-1",rows:[warningRow]});
  assert.deepEqual(result,{status:"VALIDATED",issueCount:1});
  assert.deepEqual(db.stage[0].source_row,{first_name:"Ada"});
  assert.deepEqual(db.stage[0].raw_source_row,warningRow.rawSourceRow);
  assert.equal(db.stage[0].validation_status,"VALID");
  assert.equal(db.issues[0].severity,"WARNING");
});

test("error invalidates row and fails batch",async()=>{
  const db=new MemoryDb(); db.batches.set("I-2","RECEIVED");
  const row={...warningRow,diagnostics:[{code:"BAD_STATUS",severity:"ERROR",fieldKey:"status",detail:"Bad status."}]};
  const result=await persistRecordStaging(db,{importId:"I-2",rows:[row]});
  assert.equal(result.status,"FAILED"); assert.equal(db.stage[0].validation_status,"INVALID"); assert.equal(db.batches.get("I-2"),"FAILED");
});

test("rolls back all writes on database failure",async()=>{
  const db=new MemoryDb(); db.batches.set("I-3","RECEIVED"); db.failOnIssue=true;
  await assert.rejects(()=>persistRecordStaging(db,{importId:"I-3",rows:[warningRow]}),/injected issue failure/);
  assert.equal(db.stage.length,0); assert.equal(db.issues.length,0); assert.equal(db.batches.get("I-3"),"RECEIVED");
});
