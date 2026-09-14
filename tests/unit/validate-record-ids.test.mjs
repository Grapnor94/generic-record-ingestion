import test from "node:test";
import assert from "node:assert/strict";
import { validateRecordIds } from "../../dist/ingestion/validate-record-ids.js";

test("reports blank record ID", () => {
  assert.deepEqual(validateRecordIds([{rowNumber:1,recordId:"   "}]), [{rowNumber:1,diagnostic:{code:"MISSING_RECORD_ID",severity:"ERROR",fieldKey:"record_id",detail:"Record identifier is required."}}]);
});

test("reports only later duplicate", () => {
  assert.deepEqual(validateRecordIds([{rowNumber:1,recordId:"A-1"},{rowNumber:2,recordId:"A-1"}]), [{rowNumber:2,diagnostic:{code:"DUPLICATE_RECORD_ID",severity:"ERROR",fieldKey:"record_id",detail:"Duplicate record identifier A-1 within the incoming batch."}}]);
});

test("unique ids produce no diagnostics", () => {
  assert.deepEqual(validateRecordIds([{rowNumber:1,recordId:"A-1"},{rowNumber:2,recordId:"A-2"}]), []);
});
