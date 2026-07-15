// k6 load test for the durable workflow execution runtime.
//
// Two workloads, run together or alone (rates/VUs via env):
//
//   churn   - submitters push short workflows at a target arrival rate;
//             worker VUs lease and complete them. Every lifecycle is 3+
//             durable log events (submitted, leased, completed), measured
//             server-side via goqueue_wf_events_total.
//
//   hold    - holder VUs submit long-running workflows, lease them, and
//             heartbeat until the test ends, sustaining a large number of
//             concurrent running executions (goqueue_wf_executions gauge).
//
// Usage (from repo root, broker already running):
//   k6 run load/k6/workflow_load.js
//   k6 run -e SUBMIT_RATE=8000 -e WORKER_VUS=256 -e HOLD_COUNT=2500 load/k6/workflow_load.js
//
// Server-side rates: go run ./load/metricsampler while this runs.

import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

const ADDR = __ENV.GRPC_ADDR || '127.0.0.1:9095';
const DURATION_S = parseInt(__ENV.DURATION_S || '90');
const SUBMIT_RATE = parseInt(__ENV.SUBMIT_RATE || '2000'); // churn submits/sec
const SUBMIT_VUS = parseInt(__ENV.SUBMIT_VUS || '128');
const WORKER_VUS = parseInt(__ENV.WORKER_VUS || '128');
const HOLD_COUNT = parseInt(__ENV.HOLD_COUNT || '0'); // long-running executions to sustain
const HOLDER_VUS = parseInt(__ENV.HOLDER_VUS || '64');
const HOLD_TTL_MS = parseInt(__ENV.HOLD_TTL_MS || '45000');
// BATCH > 1 switches churn to the batched RPCs (SubmitWorkflows,
// LeaseTask max_tasks, CompleteTasks), amortizing RPC overhead the way
// production task queues do. Every submit/lease/complete remains an
// individual durable log event either way.
const BATCH = parseInt(__ENV.BATCH || '1');
const RUN_ID = __ENV.RUN_ID || `r${Date.now() % 100000}`;

const scenarios = {};
if (SUBMIT_RATE > 0) {
  scenarios.churn_submit = {
    executor: 'constant-arrival-rate',
    exec: 'submitChurn',
    rate: SUBMIT_RATE,
    timeUnit: '1s',
    duration: `${DURATION_S}s`,
    preAllocatedVUs: SUBMIT_VUS,
    maxVUs: SUBMIT_VUS * 4,
  };
  scenarios.churn_work = {
    executor: 'constant-vus',
    exec: 'workChurn',
    vus: WORKER_VUS,
    duration: `${DURATION_S + 10}s`, // drain the tail after submitters stop
    gracefulStop: '10s',
  };
}
if (HOLD_COUNT > 0) {
  scenarios.holders = {
    executor: 'per-vu-iterations',
    exec: 'holdLongRunning',
    vus: HOLDER_VUS,
    iterations: 1,
    maxDuration: `${DURATION_S + 30}s`,
    gracefulStop: '15s',
  };
}

export const options = {
  scenarios,
  // The server-side Prometheus counters are the source of truth; these
  // thresholds just fail the run loudly if the broker starts erroring.
  thresholds: {
    checks: ['rate>0.99'],
  },
};

const client = new grpc.Client();
client.load(['../../proto'], 'goqueue.proto');

let connected = false;
function ensureConnected() {
  if (!connected) {
    client.connect(ADDR, { plaintext: true });
    connected = true;
  }
}

const SVC = 'goqueue.v1.WorkflowService';

export function submitChurn() {
  ensureConnected();
  const base = `${RUN_ID}-c-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  if (BATCH <= 1) {
    const res = client.invoke(`${SVC}/SubmitWorkflow`, {
      tenant: 'load',
      project: 'churn',
      workflow_id: base,
      task_type: 'churn',
      max_attempts: 3,
      lease_ttl_ms: 30000,
    });
    check(res, { 'submit ok': (r) => r && r.status === grpc.StatusOK });
    return;
  }
  const requests = [];
  for (let i = 0; i < BATCH; i++) {
    requests.push({
      tenant: 'load',
      project: 'churn',
      workflow_id: `${base}-${i}`,
      task_type: 'churn',
      max_attempts: 3,
      lease_ttl_ms: 30000,
    });
  }
  const res = client.invoke(`${SVC}/SubmitWorkflows`, { requests });
  check(res, { 'submit ok': (r) => r && r.status === grpc.StatusOK });
}

export function workChurn() {
  ensureConnected();
  const workerId = `k6-w${exec.vu.idInTest}`;
  const res = client.invoke(`${SVC}/LeaseTask`, {
    task_type: 'churn',
    worker_id: workerId,
    wait_ms: 1000,
    max_tasks: BATCH,
  });
  const ok = check(res, { 'lease ok': (r) => r && r.status === grpc.StatusOK });
  if (!ok || !res.message.found) {
    return;
  }
  const m = res.message;
  if (BATCH <= 1) {
    const done = client.invoke(`${SVC}/CompleteTask`, {
      tenant: m.tenant,
      project: m.project,
      workflow_id: m.workflowId,
      worker_id: workerId,
      attempt: m.attempt,
    });
    check(done, { 'complete ok': (r) => r && r.status === grpc.StatusOK });
    return;
  }
  const requests = m.tasks.map((t) => ({
    tenant: t.tenant,
    project: t.project,
    workflow_id: t.workflowId,
    worker_id: workerId,
    attempt: t.attempt,
  }));
  const done = client.invoke(`${SVC}/CompleteTasks`, { requests });
  check(done, { 'complete ok': (r) => r && r.status === grpc.StatusOK });
}

// holdLongRunning submits and leases a slice of long-running executions,
// then heartbeats them until the test window closes. HOLDER_VUS * slice
// covers HOLD_COUNT executions concurrently running for the whole test.
export function holdLongRunning() {
  ensureConnected();
  const workerId = `k6-h${exec.vu.idInTest}`;
  const per = Math.ceil(HOLD_COUNT / HOLDER_VUS);

  for (let i = 0; i < per; i++) {
    const res = client.invoke(`${SVC}/SubmitWorkflow`, {
      tenant: 'load',
      project: 'hold',
      workflow_id: `${RUN_ID}-h-${exec.vu.idInTest}-${i}`,
      task_type: 'hold',
      max_attempts: 3,
      lease_ttl_ms: HOLD_TTL_MS,
    });
    check(res, { 'hold submit ok': (r) => r && r.status === grpc.StatusOK });
  }

  const held = [];
  for (let i = 0; i < per; i++) {
    const res = client.invoke(`${SVC}/LeaseTask`, {
      task_type: 'hold',
      worker_id: workerId,
      wait_ms: 5000,
    });
    if (res && res.status === grpc.StatusOK && res.message.found) {
      held.push(res.message);
    }
  }
  check(held, { 'held a full slice': (h) => h.length > 0 });

  // Heartbeat every ~TTL/4 so leases never expire while held.
  const hbEvery = Math.max(2, HOLD_TTL_MS / 1000 / 4);
  const endAt = Date.now() + DURATION_S * 1000;
  while (Date.now() < endAt) {
    for (const t of held) {
      const hb = client.invoke(`${SVC}/HeartbeatTask`, {
        tenant: t.tenant,
        project: t.project,
        workflow_id: t.workflowId,
        worker_id: workerId,
        attempt: t.attempt,
      });
      check(hb, { 'heartbeat valid': (r) => r && r.status === grpc.StatusOK && r.message.valid });
    }
    sleep(hbEvery);
  }

  // Complete everything held so the run ends with a clean ledger.
  for (const t of held) {
    client.invoke(`${SVC}/CompleteTask`, {
      tenant: t.tenant,
      project: t.project,
      workflow_id: t.workflowId,
      worker_id: workerId,
      attempt: t.attempt,
    });
  }
}
