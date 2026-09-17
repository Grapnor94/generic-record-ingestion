import type { PreparedRecord, RecordSchemaContract, StagingDiagnostic, StagingReport } from "./types.js";
import { assertSupportedRecordHeaders } from "./validate-headers.js";
import { validateRecordIds } from "./validate-record-ids.js";

export class RecordStagingCallbackError extends Error {
  readonly rowNumber: number;
  readonly cause: unknown;

  constructor(rowNumber: number, cause: unknown) {
    const message = cause instanceof Error ? cause.message : String(cause);
    super(`Record staging failed at row ${rowNumber}: ${message}`);
    this.name = "RecordStagingCallbackError";
    this.rowNumber = rowNumber;
    this.cause = cause;
  }
}

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
    // Callbacks share a working row, never the retained raw snapshot.
    const workingRow={...rawSourceRow};
    try {
      const sourceRow=input.transform(workingRow);
      const recordId=input.getRecordId(workingRow,sourceRow);
      const diagnostics=[...(input.diagnose?.(workingRow,sourceRow)??[])];
      return {rowNumber:index+1,recordId,rawSourceRow,sourceRow,diagnostics};
    } catch(error) {
      throw new RecordStagingCallbackError(index+1,error);
    }
  });
  for (const item of validateRecordIds(rows.map(({rowNumber,recordId})=>({rowNumber,recordId})))) {
    // Row numbers are assigned above as contiguous, one-based array positions.
    const row=rows[item.rowNumber-1];
    if (row) row.diagnostics.push(item.diagnostic);
  }
  const all=rows.flatMap(r=>r.diagnostics);
  const warningCount=all.filter(d=>d.severity==="WARNING").length;
  const errorCount=all.filter(d=>d.severity==="ERROR").length;
  return {rows,report:{schemaVersion:input.contract.schemaVersion,headerStatus:"VALID",rowCount:rows.length,warningCount,errorCount,rowsWithDiagnostics:rows.filter(r=>r.diagnostics.length>0).length,canProceedToPersistence:errorCount===0}};
}
