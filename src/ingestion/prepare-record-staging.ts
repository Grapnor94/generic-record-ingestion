import type { PreparedRecord, RecordSchemaContract, StagingDiagnostic, StagingReport } from "./types.js";
import { assertSupportedRecordHeaders } from "./validate-headers.js";
import { validateRecordIds } from "./validate-record-ids.js";

export function prepareRecordStaging(input: {
  contract: RecordSchemaContract;
  headers: readonly string[];
  rows: readonly Record<string,string>[];
  transform: (row: Record<string,string>) => Record<string,unknown>;
  getRecordId: (row: Record<string,string>, canonical: Record<string,unknown>) => string | null;
  diagnose?: (row: Record<string,string>, canonical: Record<string,unknown>) => StagingDiagnostic[];
}): { rows: PreparedRecord[]; report: StagingReport } {
  assertSupportedRecordHeaders(input.contract,input.headers);
  const rows=input.rows.map((incoming,index):PreparedRecord=>{
    const rawSourceRow={...incoming};
    try {
      const sourceRow=input.transform(rawSourceRow);
      const recordId=input.getRecordId(rawSourceRow,sourceRow);
      const diagnostics=[...(input.diagnose?.(rawSourceRow,sourceRow)??[])];
      return {rowNumber:index+1,recordId,rawSourceRow,sourceRow,diagnostics};
    } catch(error) {
      const message=error instanceof Error?error.message:String(error);
      throw new Error(`Record staging failed at row ${index+1}: ${message}`);
    }
  });
  for (const item of validateRecordIds(rows.map(({rowNumber,recordId})=>({rowNumber,recordId})))) {
    const row=rows.find(r=>r.rowNumber===item.rowNumber);
    if (row) row.diagnostics.push(item.diagnostic);
  }
  const all=rows.flatMap(r=>r.diagnostics);
  const warningCount=all.filter(d=>d.severity==="WARNING").length;
  const errorCount=all.filter(d=>d.severity==="ERROR").length;
  return {rows,report:{schemaVersion:input.contract.schemaVersion,headerStatus:"VALID",rowCount:rows.length,warningCount,errorCount,rowsWithDiagnostics:rows.filter(r=>r.diagnostics.length>0).length,canProceedToPersistence:errorCount===0}};
}
