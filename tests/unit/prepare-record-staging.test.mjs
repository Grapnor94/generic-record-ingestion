import test from "node:test";
import assert from "node:assert/strict";
import { prepareRecordStaging } from "../../dist/ingestion/prepare-record-staging.js";

const headers=["record_id","first_name","last_name","status","region","legacy_history_code"];
const contract={schemaVersion:"GENERIC_V1",requiredHeaders:["record_id","first_name","last_name","status"],optionalHeaders:["region","legacy_history_code"]};
const record=(o={})=>({record_id:" R-001 ",first_name:" Ada ",last_name:" Example ",status:" active ",region:" north ",legacy_history_code:" RAW-7 ",...o});
const transform=(r)=>({first_name:r.first_name.trim(),last_name:r.last_name.trim(),status:r.status.trim().toUpperCase(),region:r.region.trim()});
const getRecordId=(r)=>r.record_id.trim()||null;

test("preserves raw row and isolates canonical output",()=>{
  const raw=record();
  const result=prepareRecordStaging({contract,headers,rows:[raw],transform,getRecordId});
  assert.deepEqual(result.rows[0].rawSourceRow,raw);
  assert.notEqual(result.rows[0].rawSourceRow,raw);
  assert.deepEqual(result.rows[0].sourceRow,{first_name:"Ada",last_name:"Example",status:"ACTIVE",region:"north"});
  assert.equal("legacy_history_code" in result.rows[0].sourceRow,false);
  assert.equal(raw.first_name," Ada ");
});

test("warning is counted but nonblocking",()=>{
  const result=prepareRecordStaging({contract,headers,rows:[record({status:" legacy "})],transform,getRecordId,diagnose:(r)=>r.status.trim()==="legacy"?[{code:"LEGACY_STATUS",severity:"WARNING",fieldKey:"status",detail:"Legacy status encountered."}]:[]});
  assert.equal(result.report.warningCount,1); assert.equal(result.report.errorCount,0); assert.equal(result.report.canProceedToPersistence,true);
});

test("caller error blocks persistence",()=>{
  const result=prepareRecordStaging({contract,headers,rows:[record()],transform,getRecordId,diagnose:()=>[{code:"INVALID_STATUS",severity:"ERROR",fieldKey:"status",detail:"Status is invalid."}]});
  assert.equal(result.report.errorCount,1); assert.equal(result.report.canProceedToPersistence,false);
});

test("duplicate record IDs block and attach to later row",()=>{
  const result=prepareRecordStaging({contract,headers,rows:[record({record_id:"R-1"}),record({record_id:"R-1"})],transform,getRecordId});
  assert.equal(result.report.canProceedToPersistence,false);
  assert.equal(result.rows[1].diagnostics.some(d=>d.code==="DUPLICATE_RECORD_ID"),true);
});

test("transform exception includes row number",()=>{
  assert.throws(()=>prepareRecordStaging({contract,headers,rows:[record()],transform:()=>{throw new Error("cannot normalize")},getRecordId}),/row 1: cannot normalize/i);
});
