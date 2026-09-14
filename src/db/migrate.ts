import pg from "pg";
import { runMigrations } from "./migrations.js";

const { Client } = pg;

const client = new Client({
  host: process.env.PGHOST ?? "127.0.0.1",
  port: Number(process.env.PGPORT ?? "5432"),
  user: process.env.PGUSER ?? "postgres",
  password: process.env.PGPASSWORD ?? "postgres",
  database: process.env.PGDATABASE ?? "postgres",
});

async function main(): Promise<void> {
  await client.connect();
  try {
    const result = await runMigrations(client);
    for (const filename of result.applied) {
      console.log(`applied ${filename}`);
    }
    for (const filename of result.skipped) {
      console.log(`skipped ${filename}`);
    }
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
