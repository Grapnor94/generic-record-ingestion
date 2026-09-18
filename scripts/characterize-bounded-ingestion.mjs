import assert from "node:assert/strict";
import { spawn, execFileSync } from "node:child_process";
import { once } from "node:events";
import { mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";

const scriptPath = fileURLToPath(import.meta.url);
const root = resolve(dirname(scriptPath), "..");
const outputPath = resolve(root, "outputs/characterization/v0.8-bounded-ingestion.json");
const rowCounts = [10_000, 25_000, 50_000, 100_000];
const shapes = ["narrow", "moderately-wide"];
const sampleIntervalMs = 5;

// A separate thread samples process-wide RSS while synchronous production code
// blocks the main thread. The sampler's own overhead is included in the results.
if (!isMainThread) {
  let peakRssBytes = 0;
  let rssSampleCount = 0;
  function sample() {
    peakRssBytes = Math.max(peakRssBytes, process.memoryUsage.rss());
    rssSampleCount++;
  }
  sample();
  const timer = setInterval(sample, workerData.sampleIntervalMs);
  parentPort.postMessage({ ready: true });
  parentPort.once("message", () => {
    clearInterval(timer);
    sample();
    parentPort.postMessage({ peakRssBytes, rssSampleCount });
    parentPort.close();
  });
} else if (process.argv[2] === "--scenario") {
  await runScenario(process.argv[3], Number(process.argv[4]));
} else {
  await characterize();
}

async function runScenario(shape, rowCount) {
  assert.ok(shapes.includes(shape));
  assert.ok(rowCounts.includes(rowCount));
  const { parseCsvRecords } = await import("../dist/csv/parse-csv-records.js");
  const { prepareRecordStaging } = await import("../dist/ingestion/prepare-record-staging.js");
  const sampler = new Worker(new URL(import.meta.url), { workerData: { sampleIntervalMs } });
  await once(sampler, "message");
  const startingRssBytes = process.memoryUsage.rss();
  const started = performance.now();
  const headers = shape === "narrow"
    ? ["id", "name"]
    : ["id", ...Array.from({ length: 24 }, (_, index) => `text_${index + 1}`)];
  const lines = [headers.join(",")];
  for (let index = 0; index < rowCount; index++) {
    const cells = headers.slice(1).map((_, column) =>
      `field-${String(column + 1).padStart(2, "0")}-${"x".repeat(23)}`,
    );
    // One multibyte cell per group of 1,000 data rows; all other payloads are
    // fixed 32-byte ASCII strings, with deterministic fixed-width unique IDs.
    if (index % 1000 === 0) cells[0] = "café-東京-🙂";
    lines.push([`R${String(index + 1).padStart(9, "0")}`, ...cells].join(","));
  }
  const source = `${lines.join("\n")}\n`;
  const sourceBytes = Buffer.byteLength(source, "utf8");
  // Do not retain the generator's row strings during the measured parse/prepare.
  lines.length = 0;
  const sourceReadyRssBytes = process.memoryUsage.rss();
  const pipelineStarted = performance.now();
  const parsed = parseCsvRecords(source);
  const parsedRssBytes = process.memoryUsage.rss();
  const prepared = prepareRecordStaging({
    contract: { schemaVersion: "CHARACTERIZATION_V1", requiredHeaders: headers },
    headers: parsed.headers,
    rows: parsed.rows,
    transform: row => ({ ...row }),
    getRecordId: row => row.id,
  });
  const pipelineElapsedMs = performance.now() - pipelineStarted;
  const endingRssBytes = process.memoryUsage.rss();
  const elapsedMs = performance.now() - started;
  assert.equal(parsed.rows.length, rowCount);
  assert.equal(prepared.rows.length, rowCount);
  assert.equal(prepared.report.canProceedToPersistence, true);
  assert.equal(prepared.report.errorCount, 0);
  assert.equal(prepared.rows[0].rawSourceRow[headers[1]], "café-東京-🙂");
  assert.equal(prepared.rows.at(-1).recordId, `R${String(rowCount).padStart(9, "0")}`);
  const samplesPromise = once(sampler, "message");
  const samplerExit = once(sampler, "exit");
  sampler.postMessage("stop");
  const [samples] = await samplesPromise;
  await samplerExit;
  process.stdout.write(`${JSON.stringify({
    shape, requestedRows: rowCount, sourceBytes, elapsedMs, pipelineElapsedMs,
    startingRssBytes, endingRssBytes,
    peakRssBytes: Math.max(samples.peakRssBytes, startingRssBytes, sourceReadyRssBytes, parsedRssBytes, endingRssBytes),
    rssSampleCount: samples.rssSampleCount,
    osHighWaterRssBytes: process.resourceUsage().maxRSS * 1024,
    parsedRowCount: parsed.rows.length, preparedRowCount: prepared.rows.length,
    childPid: process.pid,
  })}\n`);
}

async function characterize() {
  const report = {
    generatedAt: new Date().toISOString(),
    nodeVersion: process.version,
    os: { platform: os.platform(), release: os.release(), arch: os.arch(), totalMemoryBytes: os.totalmem() },
    commit: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
    sampleIntervalMs,
    timeoutMs: 120_000,
    scenarios: [],
  };
  await mkdir(dirname(outputPath), { recursive: true });
  for (const rowCount of rowCounts) {
    for (const shape of shapes) {
      const started = performance.now();
      const child = spawn(process.execPath, [scriptPath, "--scenario", shape, String(rowCount)], {
        cwd: root, stdio: ["ignore", "pipe", "pipe"], windowsHide: true,
      });
      let stdout = "";
      let stderr = "";
      let timedOut = false;
      child.stdout.setEncoding("utf8").on("data", chunk => { stdout += chunk; });
      child.stderr.setEncoding("utf8").on("data", chunk => { stderr += chunk; });
      const timer = setTimeout(() => { timedOut = true; child.kill(); }, report.timeoutMs);
      const [exitCode, signal] = await once(child, "close");
      clearTimeout(timer);
      let measurement = {};
      try { measurement = JSON.parse(stdout); }
      catch { /* Failed children still produce an explicit failed scenario. */ }
      const success = exitCode === 0 && !timedOut
        && measurement.parsedRowCount === rowCount && measurement.preparedRowCount === rowCount;
      const scenario = {
        shape, requestedRows: rowCount, ...measurement,
        processElapsedMs: performance.now() - started,
        exitCode, signal, timedOut, success, ...(stderr ? { stderr } : {}),
      };
      report.scenarios.push(scenario);
      await writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`);
      console.log(`${shape} ${rowCount}: ${success ? "PASS" : "FAIL"}; bytes=${measurement.sourceBytes ?? "n/a"}; elapsedMs=${measurement.elapsedMs?.toFixed(1) ?? "n/a"}; peakRSS=${measurement.peakRssBytes ?? "n/a"}`);
      if (!success && rowCount === 10_000) {
        console.error("10,000-row characterization floor failed; stop and escalate scope.");
        process.exitCode = 1;
        return;
      }
    }
  }
  if (report.scenarios.some(scenario => !scenario.success)) process.exitCode = 1;
  console.log(`Characterization written to ${outputPath}`);
}
