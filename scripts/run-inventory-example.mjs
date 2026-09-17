import { mkdtemp, mkdir, copyFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = fileURLToPath(new URL("../", import.meta.url));
function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", timeout: 60_000 });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`Example command failed (${result.status}): ${result.stdout}\n${result.stderr}`);
  }
  return result.stdout;
}

if (!process.env.npm_execpath) {
  throw new Error("Run through npm run example:inventory.");
}
const dir = await mkdtemp(join(tmpdir(), "inventory-consumer-"));
try {
  // Exercise the actual private package artifact without registry access or installation.
  const output = run(process.execPath, [process.env.npm_execpath, "pack", "--json",
    "--cache", join(dir, "cache"), "--pack-destination", dir], root);
  const packed = JSON.parse(output);
  const archive = Array.isArray(packed) ? packed[0] : packed["generic-record-ingestion"];
  const destination = join(dir, "node_modules", "generic-record-ingestion");
  await mkdir(destination, { recursive: true });
  run("tar", ["-xzf", join(dir, archive.filename), "-C", destination, "--strip-components=1"], dir);
  await copyFile(join(root, "examples", "inventory.mjs"), join(dir, "inventory.mjs"));
  process.stdout.write(run(process.execPath, [join(dir, "inventory.mjs")], dir));
} finally {
  await rm(dir, { recursive: true, force: true });
}
