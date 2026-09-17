import { readFile, readdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  withDedicatedConnection,
  withTransaction,
  type PostgresDatabase,
  type PostgresQueryable,
} from "./postgres.js";

const MIGRATION_FILENAME = /^[0-9]{4}_.+\.sql$/;
const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const DEFAULT_MIGRATIONS_DIR = resolve(MODULE_DIR, "../../db/migrations");

export type MigrationFile = {
  filename: string;
  absolutePath: string;
};

export async function discoverMigrations(
  migrationsDir: string,
): Promise<MigrationFile[]> {
  const entries = await readdir(migrationsDir, { withFileTypes: true });
  const sqlFiles = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sql"))
    .map((entry) => entry.name);

  for (const filename of sqlFiles) {
    if (!MIGRATION_FILENAME.test(filename)) {
      throw new Error(`Invalid migration filename: ${filename}`);
    }
  }

  return sqlFiles
    .sort((a, b) => a.localeCompare(b))
    .map((filename) => ({
      filename,
      absolutePath: resolve(migrationsDir, filename),
    }));
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export async function runMigrations(
  database: PostgresDatabase,
  options: { migrationsDir?: string } = {},
): Promise<{ applied: string[]; skipped: string[] }> {
  return withDedicatedConnection(
    database,
    (client) => runMigrationsOnConnection(client, options),
  );
}

async function runMigrationsOnConnection(
  db: PostgresQueryable,
  options: { migrationsDir?: string },
): Promise<{ applied: string[]; skipped: string[] }> {
  const migrationsDir = options.migrationsDir ?? DEFAULT_MIGRATIONS_DIR;

  await db.query(`
    create table if not exists schema_migration (
      filename text primary key,
      applied_at timestamptz not null default current_timestamp
    )
  `);

  const migrationFiles = await discoverMigrations(migrationsDir);
  const appliedRows = await db.query<{ filename: string }>(
    "select filename from schema_migration order by filename",
  );
  const appliedSet = new Set(appliedRows.rows.map((row) => row.filename));

  const applied: string[] = [];
  const skipped: string[] = [];

  for (const migration of migrationFiles) {
    if (appliedSet.has(migration.filename)) {
      skipped.push(migration.filename);
      continue;
    }

    const sql = await readFile(migration.absolutePath, "utf8");
    try {
      await withTransaction(db, async (transaction) => {
        await transaction.query(sql);
        await transaction.query(
          "insert into schema_migration (filename) values ($1)",
          [migration.filename],
        );
      });
      appliedSet.add(migration.filename);
      applied.push(migration.filename);
    } catch (error) {
      throw new Error(
        `Migration ${migration.filename} failed: ${errorMessage(error)}`,
        { cause: error },
      );
    }
  }

  return { applied, skipped };
}
