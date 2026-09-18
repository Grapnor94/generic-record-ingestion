import assert from "node:assert/strict";
import { cp, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";

const root = fileURLToPath(new URL("../../", import.meta.url));
const require = createRequire(import.meta.url);

export function run(command, args, cwd) {
  const result = spawnSync(command, args, { cwd, encoding: "utf8", timeout: 60_000 });
  assert.ifError(result.error);
  assert.equal(result.status, 0, `${command} ${args.join(" ")}\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

export async function withPackedConsumer(work, { runtimeDependencies = false } = {}) {
  const dir = await mkdtemp(join(tmpdir(), "generic-package-"));
  try {
    const npm = process.env.npm_execpath ?? require.resolve("npm/bin/npm-cli.js");
    const packed = JSON.parse(run(process.execPath, [npm, "pack", "--json", "--cache", join(dir, "cache"), "--pack-destination", dir], root));
    const archive = Array.isArray(packed) ? packed[0] : packed["generic-record-ingestion"];
    const destination = join(dir, "node_modules", "generic-record-ingestion");
    await mkdir(destination, { recursive: true });
    // Relative arguments also work with GNU tar, which treats a Windows drive
    // letter in an archive filename as a remote-host separator.
    run("tar", ["-xzf", archive.filename, "-C", "node_modules/generic-record-ingestion", "--strip-components=1"], dir);
    await writeFile(join(dir, "package.json"), JSON.stringify({ type: "module" }));
    if (runtimeDependencies) {
      const copied = new Set();
      async function copyDependency(name, resolver) {
        if (copied.has(name)) return;
        let location = dirname(resolver.resolve(name));
        let manifest;
        while (true) {
          try { manifest = JSON.parse(await readFile(join(location, "package.json"), "utf8")); } catch {}
          if (manifest?.name === name) break;
          const parent = dirname(location);
          assert.notEqual(parent, location, `Cannot locate dependency ${name}`);
          location = parent;
        }
        copied.add(name);
        await cp(location, join(dir, "node_modules", name), { recursive: true });
        const childRequire = createRequire(join(location, "package.json"));
        for (const child of Object.keys(manifest.dependencies ?? {})) await copyDependency(child, childRequire);
      }
      const manifest = JSON.parse(await readFile(join(destination, "package.json"), "utf8"));
      for (const name of Object.keys(manifest.dependencies ?? {})) await copyDependency(name, require);
    }
    return await work(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
