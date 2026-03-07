import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import fs from 'node:fs/promises';
import net from 'node:net';
import path from 'node:path';
import process from 'node:process';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const frontendDir = path.resolve(scriptDir, '..');
const repoRoot = path.resolve(frontendDir, '..');
const backendDir = path.join(repoRoot, 'backend', 'services');
const backendPort = Number(process.env.KAIRYX_E2E_BACKEND_PORT || 8001);
const backendUrl = `http://127.0.0.1:${backendPort}`;
const predictionCacheDir = path.join(backendDir, '.cache', 'predictions');
const predictionJobsFile = path.join(backendDir, '.prediction_jobs.json');
const sqliteDbPath = path.join(backendDir, '.kairyx_local.db');
const pythonBin = existsSync(path.join(repoRoot, '.venv', 'bin', 'python'))
  ? path.join(repoRoot, '.venv', 'bin', 'python')
  : 'python3';

function log(message) {
  console.log(`[e2e] ${message}`);
}

function npxCommand() {
  return process.platform === 'win32' ? 'npx.cmd' : 'npx';
}

async function assertPortAvailable(port) {
  await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on('error', (error) => {
      reject(new Error(`Port ${port} is already in use. Stop the existing service before running e2e cleanup tests.`));
    });
    server.listen(port, '127.0.0.1', () => {
      server.close((closeError) => {
        if (closeError) {
          reject(closeError);
          return;
        }
        resolve();
      });
    });
  });
}

function startService(name, command, args, options) {
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: { ...process.env, ...options.env },
    stdio: ['ignore', 'pipe', 'pipe'],
    detached: process.platform !== 'win32',
  });

  let output = '';
  const appendOutput = (chunk) => {
    output = `${output}${chunk.toString()}`.slice(-8000);
  };

  child.stdout.on('data', appendOutput);
  child.stderr.on('data', appendOutput);

  return {
    name,
    child,
    output: () => output,
  };
}

async function waitForHttpOk(name, url, service, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (service.child.exitCode !== null) {
      throw new Error(`${name} exited early with code ${service.child.exitCode}.\n${service.output()}`);
    }
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // keep polling until timeout
    }
    await delay(500);
  }
  throw new Error(`${name} did not become ready at ${url}.\n${service.output()}`);
}

async function requestJson(pathname, init) {
  const response = await fetch(`${backendUrl}${pathname}`, init);
  const text = await response.text();
  const body = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(`${init?.method || 'GET'} ${pathname} failed (${response.status}): ${JSON.stringify(body)}`);
  }
  return body;
}

async function readJsonFile(filePath, fallback) {
  try {
    return JSON.parse(await fs.readFile(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

async function listPredictionCacheFiles() {
  try {
    return await fs.readdir(predictionCacheDir);
  } catch {
    return [];
  }
}

async function snapshotBaseline() {
  const connectors = await requestJson('/connectors/list');
  const imports = await requestJson('/list-imports');
  return {
    connectorNames: new Set((connectors.connectors || []).map((connector) => connector.name)),
    importNames: new Set((imports.imports || []).map((job) => job.name)),
    predictionCacheFiles: new Set(await listPredictionCacheFiles()),
    predictionJobs: await readJsonFile(predictionJobsFile, []),
  };
}

async function cleanupBackendArtifacts(baseline) {
  log('Cleaning test-created connectors and import data');

  const imports = await requestJson('/list-imports');
  const currentImportNames = (imports.imports || []).map((job) => job.name);
  for (const jobName of currentImportNames) {
    if (!baseline.importNames.has(jobName)) {
      await requestJson(`/job/${encodeURIComponent(jobName)}`, { method: 'DELETE' });
    }
  }

  const connectors = await requestJson('/connectors/list');
  const currentConnectorNames = (connectors.connectors || []).map((connector) => connector.name);
  for (const connectorName of currentConnectorNames) {
    if (!baseline.connectorNames.has(connectorName)) {
      await requestJson(`/connector/${encodeURIComponent(connectorName)}`, { method: 'DELETE' });
    }
  }
}

async function syncPredictionJobsSqlite(jobs) {
  if (!existsSync(sqliteDbPath)) {
    return;
  }

  await new Promise((resolve, reject) => {
    const child = spawn(
      pythonBin,
      [
        '-c',
        [
          'import json, sqlite3, sys',
          'db_path = sys.argv[1]',
          'jobs = json.loads(sys.argv[2])',
          'conn = sqlite3.connect(db_path)',
          'cur = conn.cursor()',
          'cur.execute("DELETE FROM prediction_jobs")',
          'for job in jobs:',
          '    cur.execute("INSERT OR REPLACE INTO prediction_jobs(id, payload) VALUES (?, ?)", (job.get("id"), json.dumps(job)))',
          'conn.commit()',
          'conn.close()',
        ].join('\n'),
        sqliteDbPath,
        JSON.stringify(jobs),
      ],
      { cwd: repoRoot, stdio: 'ignore' },
    );
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`Failed to restore prediction_jobs sqlite state (exit ${code}).`));
    });
    child.on('error', reject);
  });
}

async function cleanupPredictionArtifacts(baseline) {
  log('Cleaning test-created prediction artifacts');

  const currentFiles = await listPredictionCacheFiles();
  for (const filename of currentFiles) {
    if (!baseline.predictionCacheFiles.has(filename)) {
      await fs.rm(path.join(predictionCacheDir, filename), { force: true });
    }
  }

  await fs.writeFile(predictionJobsFile, `${JSON.stringify(baseline.predictionJobs, null, 2)}\n`, 'utf8');
  await syncPredictionJobsSqlite(baseline.predictionJobs);
}

async function stopService(service) {
  if (!service || service.child.exitCode !== null) {
    return;
  }

  const sendSignal = (signal) => {
    try {
      if (process.platform === 'win32') {
        service.child.kill(signal);
      } else {
        process.kill(-service.child.pid, signal);
      }
    } catch {
      // process already gone
    }
  };

  sendSignal('SIGTERM');
  const terminated = await Promise.race([
    new Promise((resolve) => service.child.once('exit', () => resolve(true))),
    delay(5000, false),
  ]);

  if (!terminated) {
    sendSignal('SIGKILL');
    await Promise.race([
      new Promise((resolve) => service.child.once('exit', () => resolve(true))),
      delay(2000, false),
    ]);
  }
}

async function verifyServicesStopped() {
  await assertPortAvailable(backendPort);
}

async function main() {
  await assertPortAvailable(backendPort);

  const backend = startService('backend', pythonBin, ['-m', 'uvicorn', 'main_service:app', '--host', '127.0.0.1', '--port', String(backendPort)], {
    cwd: backendDir,
    env: {
      DATA_BACKEND_MODE: 'mock',
      PYTHONUNBUFFERED: '1',
    },
  });

  let baseline = null;
  let exitCode = 1;
  const handleSignal = async (signal) => {
    log(`Received ${signal}, shutting down test services`);
    await stopService(backend);
    process.exit(1);
  };

  process.once('SIGINT', handleSignal);
  process.once('SIGTERM', handleSignal);

  try {
    log('Waiting for backend service');
    await waitForHttpOk('backend', `${backendUrl}/health`, backend);

    baseline = await snapshotBaseline();

    const args = ['playwright', 'test', ...process.argv.slice(2)];
    log('Running Playwright e2e suite');
    exitCode = await new Promise((resolve, reject) => {
      const child = spawn(npxCommand(), args, {
        cwd: frontendDir,
        env: {
          ...process.env,
          KAIRYX_E2E_MANAGED_SERVICES: '1',
          KAIRYX_E2E_BACKEND_PORT: String(backendPort),
          KAIRYX_E2E_BASE_URL: backendUrl,
          KAIRYX_E2E_BACKEND_URL: backendUrl,
        },
        stdio: 'inherit',
      });
      child.on('exit', (code) => resolve(code ?? 1));
      child.on('error', reject);
    });
  } finally {
    try {
      if (baseline) {
        await cleanupBackendArtifacts(baseline);
      }
    } catch (error) {
      console.error(`[e2e] Backend cleanup failed: ${error.message}`);
      exitCode = exitCode || 1;
    }

    await stopService(backend);

    try {
      await verifyServicesStopped();
    } catch (error) {
      console.error(`[e2e] Service shutdown verification failed: ${error.message}`);
      exitCode = exitCode || 1;
    }

    try {
      if (baseline) {
        await cleanupPredictionArtifacts(baseline);
      }
    } catch (error) {
      console.error(`[e2e] Prediction artifact cleanup failed: ${error.message}`);
      exitCode = exitCode || 1;
    }
  }

  process.exit(exitCode);
}

main().catch((error) => {
  console.error(`[e2e] ${error.message}`);
  process.exit(1);
});
