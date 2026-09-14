import type { PreparedRecord, StagingDiagnostic } from "./types.js";

export type RecordIdDiagnostic = {
  rowNumber: number;
  diagnostic: StagingDiagnostic;
};

export function validateRecordIds(
  rows: Array<Pick<PreparedRecord, "rowNumber" | "recordId">>,
): RecordIdDiagnostic[] {
  const diagnostics: RecordIdDiagnostic[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    const recordId = row.recordId?.trim() || null;
    if (!recordId) {
      diagnostics.push({rowNumber: row.rowNumber, diagnostic:{code:"MISSING_RECORD_ID",severity:"ERROR",fieldKey:"record_id",detail:"Record identifier is required."}});
      continue;
    }
    if (seen.has(recordId)) {
      diagnostics.push({rowNumber: row.rowNumber, diagnostic:{code:"DUPLICATE_RECORD_ID",severity:"ERROR",fieldKey:"record_id",detail:`Duplicate record identifier ${recordId} within the incoming batch.`}});
      continue;
    }
    seen.add(recordId);
  }
  return diagnostics;
}
