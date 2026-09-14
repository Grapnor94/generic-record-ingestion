#!/usr/bin/env bash
set -euo pipefail

: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=5432}"
: "${PGUSER:=postgres}"
: "${PGPASSWORD:=postgres}"
: "${PGDATABASE:=postgres}"
export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

if ! command -v psql >/dev/null 2>&1; then
  echo "ERROR: psql is required for the live PostgreSQL gate." >&2
  exit 2
fi

if ! psql -v ON_ERROR_STOP=1 -Atqc 'select 1' >/dev/null; then
  echo "ERROR: PostgreSQL is not reachable with the configured PG* environment variables." >&2
  exit 3
fi

if ! node -e 'import("pg").catch(() => process.exit(1))'; then
  echo "ERROR: Node package 'pg' is required. Install it with: npm install --no-save pg@8" >&2
  exit 4
fi

npm run build
node --test tests/postgres/*.test.mjs
