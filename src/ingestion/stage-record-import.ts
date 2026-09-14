import type { Queryable } from "./persist-record-staging.js";
import type { RecordSchemaContract, StagingDiagnostic, StagingReport } from "./types.js";
import { prepareRecordStaging } from "./prepare-record-staging.js";
import { persistRecordStaging } from "./persist-record-staging.js";

export async function stageRecordImport(
  db: Queryable,
  input: {
    importId:string;
    contract:RecordSchemaContract;
    headers:readonly string[];
    rows:readonly Record<string,string>[];
    transform:(row:Record<string,string>)=>Record<string,unknown>;
    getRecordId:(row:Record<string,string>,canonical:Record<string,unknown>)=>string|null;
    diagnose?:(row:Record<string,string>,canonical:Record<string,unknown>)=>StagingDiagnostic[];
  },
): Promise<{report:StagingReport;persistence:{status:"VALIDATED"|"FAILED";issueCount:number}}> {
  const prepared=prepareRecordStaging(input);
  const persistence=await persistRecordStaging(db,{importId:input.importId,rows:prepared.rows});
  return {report:prepared.report,persistence};
}
