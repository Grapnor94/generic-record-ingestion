import type { PreparedRecord } from "./types.js";

type QueryResult<T extends Record<string,unknown> = Record<string,unknown>> = { rowCount: number | null; rows: T[] };
export type Queryable = { query<T extends Record<string,unknown> = Record<string,unknown>>(text:string, values?:unknown[]):Promise<QueryResult<T>> };

export async function persistRecordStaging(
  db: Queryable,
  input: { importId:string; rows:PreparedRecord[] },
): Promise<{status:"VALIDATED"|"FAILED"; issueCount:number}> {
  await db.query("begin");
  try {
    const transition=await db.query(`update import_batch set status = 'VALIDATING' where import_id = $1 and status = 'RECEIVED' returning import_id`,[input.importId]);
    if(transition.rowCount!==1) throw new Error("Import must be in RECEIVED status before validation.");
    for(const row of input.rows){
      await db.query(`insert into import_stage_row (import_id, row_number, record_id, source_row, raw_source_row, validation_status) values ($1,$2,$3,$4::jsonb,$5::jsonb,'PENDING')`,[input.importId,row.rowNumber,row.recordId?.trim()||null,JSON.stringify(row.sourceRow),JSON.stringify(row.rawSourceRow)]);
      for(const diagnostic of row.diagnostics){
        await db.query(`insert into import_issue (import_id,row_number,record_id,issue_code,severity,field_key,detail) values ($1,$2,$3,$4,$5,$6,$7)`,[input.importId,row.rowNumber,row.recordId?.trim()||null,diagnostic.code,diagnostic.severity,diagnostic.fieldKey??null,diagnostic.detail]);
      }
    }
    const invalidRows=input.rows.filter(r=>r.diagnostics.some(d=>d.severity==="ERROR")).map(r=>r.rowNumber);
    await db.query(`update import_stage_row set validation_status = case when row_number = any($2::bigint[]) then 'INVALID' else 'VALID' end where import_id = $1`,[input.importId,invalidRows]);
    const status:"FAILED"|"VALIDATED"=invalidRows.length>0?"FAILED":"VALIDATED";
    await db.query(`update import_batch set status = $2 where import_id = $1`,[input.importId,status]);
    await db.query("commit");
    return {status,issueCount:input.rows.reduce((sum,row)=>sum+row.diagnostics.length,0)};
  } catch(error){
    await db.query("rollback");
    throw error;
  }
}
