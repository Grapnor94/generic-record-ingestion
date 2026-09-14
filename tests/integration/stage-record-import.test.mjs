import test from "node:test";
import assert from "node:assert/strict";
import { stageRecordImport } from "../../dist/ingestion/stage-record-import.js";

class MemoryDb {
  constructor(){this.batches=new Map();this.stage=[];this.issues=[];this.snapshot=null;}
  clone(){return {batches:new Map(this.batches),stage:structuredClone(this.stage),issues:structuredClone(this.issues)};}
  async query(sql,values=[]){const q=sql.replace(/\s+/g," ").trim().toLowerCase();
    if(q==="begin"){this.snapshot=this.clone();return {rowCount:null,rows:[]};}
    if(q==="commit"){this.snapshot=null;return {rowCount:null,rows:[]};}
    if(q==="rollback"){if(this.snapshot){this.batches=this.snapshot.batches;this.stage=this.snapshot.stage;this.issues=this.snapshot.issues;}this.snapshot=null;return {rowCount:null,rows:[]};}
    if(q.startsWith("update import_batch")&&q.includes("validating")){const[id]=values;if(this.batches.get(id)!=="RECEIVED")return{rowCount:0,rows:[]};this.batches.set(id,"VALIDATING");return{rowCount:1,rows:[{import_id:id}]};}
    if(q.startsWith("insert into import_stage_row")){const[importId,rowNumber,recordId,sourceJson,rawJson]=values;this.stage.push({importId,rowNumber,recordId,source_row:JSON.parse(sourceJson),raw_source_row:JSON.parse(rawJson),validation_status:"PENDING"});return{rowCount:1,rows:[]};}
    if(q.startsWith("insert into import_issue")){const[importId,rowNumber,recordId,issue_code,severity,field_key,detail]=values;this.issues.push({importId,rowNumber,recordId,issue_code,severity,field_key,detail});return{rowCount:1,rows:[]};}
    if(q.startsWith("update import_stage_row")){const[importId,invalidRows]=values;for(const row of this.stage){if(row.importId===importId)row.validation_status=invalidRows.includes(row.rowNumber)?"INVALID":"VALID";}return{rowCount:this.stage.length,rows:[]};}
    if(q.startsWith("update import_batch set status")){const[id,status]=values;this.batches.set(id,status);return{rowCount:1,rows:[]};}
    throw new Error(`Unsupported SQL: ${q}`);
  }
}
const contract={schemaVersion:"GENERIC_V1",requiredHeaders:["record_id","first_name","last_name","status"],optionalHeaders:["region","legacy_history_code"]};
const headers=["record_id","first_name","last_name","status","region","legacy_history_code"];
const record=(o={})=>({record_id:" R-001 ",first_name:" Ada ",last_name:" Example ",status:" active ",region:" north ",legacy_history_code:" RAW-7 ",...o});
const transform=(r)=>({first_name:r.first_name.trim(),last_name:r.last_name.trim(),status:r.status.trim().toUpperCase(),region:r.region.trim()});
const getRecordId=(r)=>r.record_id.trim()||null;

test("end-to-end staging validates, transforms, warns, and persists",async()=>{
  const db=new MemoryDb();db.batches.set("I-10","RECEIVED");
  const result=await stageRecordImport(db,{importId:"I-10",contract,headers,rows:[record({record_id:"R-1"}),record({record_id:"R-2",status:" legacy "})],transform,getRecordId,diagnose:(r)=>r.status.trim()==="legacy"?[{code:"LEGACY_STATUS",severity:"WARNING",fieldKey:"status",detail:"Legacy status encountered."}]:[]});
  assert.deepEqual(result.report,{schemaVersion:"GENERIC_V1",headerStatus:"VALID",rowCount:2,warningCount:1,errorCount:0,rowsWithDiagnostics:1,canProceedToPersistence:true});
  assert.deepEqual(result.persistence,{status:"VALIDATED",issueCount:1});
  assert.equal(db.stage[1].source_row.status,"LEGACY");
  assert.equal(db.stage[1].raw_source_row.status," legacy ");
  assert.equal(db.stage[1].validation_status,"VALID");
});

test("invalid headers fail before persistence state transition",async()=>{
  const db=new MemoryDb();db.batches.set("I-11","RECEIVED");
  await assert.rejects(()=>stageRecordImport(db,{importId:"I-11",contract,headers:headers.filter(h=>h!=="status"),rows:[record()],transform,getRecordId}),/missing required columns: status/i);
  assert.equal(db.batches.get("I-11"),"RECEIVED");
  assert.equal(db.stage.length,0);
});
