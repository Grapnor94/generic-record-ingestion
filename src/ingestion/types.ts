export type RecordSchemaContract = {
  schemaVersion: string;
  requiredHeaders: readonly string[];
  optionalHeaders?: readonly string[];
};

export type StagingDiagnostic = {
  code: string;
  severity: "ERROR" | "WARNING";
  fieldKey?: string;
  detail: string;
};

export type PreparedRecord = {
  rowNumber: number;
  recordId: string | null;
  rawSourceRow: Record<string, string>;
  sourceRow: Record<string, unknown>;
  diagnostics: StagingDiagnostic[];
};

export type StagingReport = {
  schemaVersion: string;
  headerStatus: "VALID";
  rowCount: number;
  warningCount: number;
  errorCount: number;
  rowsWithDiagnostics: number;
  canProceedToPersistence: boolean;
};
