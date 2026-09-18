create index import_issue_import_row_issue_idx
  on import_issue (import_id, row_number asc nulls first, issue_id asc);
