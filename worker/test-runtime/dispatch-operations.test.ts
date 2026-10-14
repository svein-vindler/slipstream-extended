import { env, evictDurableObject, runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";
import { syntheticStartupReceipt } from "../test-support/dispatch-receipt";
import type { DispatchOperation, DispatchClaim } from "../src/dispatch-operations";

async function withStartup(stub: ReturnType<Env["REFRESH_COORDINATOR"]["getByName"]>,
  reading: { operation: DispatchOperation | null; claim?: DispatchClaim }, runId: number) {
  let current = reading.operation!;
  current = (await stub.takeDispatchRead(current, reading.claim!, current.revision, "receipt", now))!;
  const saved = await stub.recordDispatchReceipt(current, reading.claim!, current.revision,
    syntheticStartupReceipt(current, runId), now);
  expect(saved.receipt_verified).toBe(true);
  return { ...reading, operation: saved.operation! };
}


const now = Date.parse("2026-10-14T12:00:00Z");
const scope = "night/2026-10-14";
const descriptor = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main",
  mode: "night" as const, scope, date: "2026-10-14", activity_id: null,
  new_expected: false, repair_only: false, include_granular: false, components: ["sleep", "hrv"] };

it("preserves a same-operation ACK after a status claim advances and closes its revision", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  const reading = await stub.claimDispatchRead(identity, "status", "synthetic-actor", now);
  expect(reading.acquired).toBe(true);
  const closed = (await stub.finishDispatchRead(identity, reading.claim!, reading.operation!.revision, null, now)).operation;
  expect(closed!.revision).toBeGreaterThan(reading.operation!.revision);
  const saved = await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, now);
  expect(saved).toMatchObject({ candidate_run_id: 55, dispatch_state: "candidate", owner_released_at: null });
  expect(await stub.dispatchForRun(55)).toMatchObject({ operation_id: identity.operation_id });
  expect((await stub.freshJob(identity.operation_id))!.polls).toBe(1);
});

it("discards a stale GET after ACK but closes only its own claim, then protects a newer owner", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  const old = await stub.claimDispatchRead(identity, "status", "synthetic-actor", now);
  const ack = await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, now);
  expect(ack!.read_claim!.claim_id).toBe(old.claim!.claim_id);
  expect((await stub.claimDispatchRead(identity, "status", "another-actor", now)).acquired).toBe(false);
  const evidence = { run_id: 55, status: "completed", conclusion: "success", binding: {
    run_id: 55, repository: descriptor.repository, workflow: descriptor.workflow, ref: "main",
    source_sha: "a".repeat(40), correlation_source: "request_receipt", coverage: descriptor } };
  const staleResult = await stub.finishDispatchRead(identity, old.claim!, old.operation!.revision, evidence, now);
  expect(staleResult.observation_committed).toBe(false);
  const stale = staleResult.operation;
  expect(stale).toMatchObject({ dispatch_state: "candidate", owner_released_at: null, read_claim: null, last_observation: null });
  const newer = await stub.claimDispatchRead(identity, "status", "synthetic-actor", now);
  await stub.finishDispatchRead(identity, old.claim!, newer.operation!.revision, evidence, now);
  expect((await stub.dispatchOperation(identity.operation_id))!.read_claim!.claim_id).toBe(newer.claim!.claim_id);
  const ready = await withStartup(stub, newer, 55);
  await stub.finishDispatchRead(identity, ready.claim!, ready.operation!.revision, evidence, now);
  const next = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now + 300_001, false, descriptor);
  expect(next.acquired).toBe(true);
  const before = await stub.dispatchOperation(next.job!.request_id);
  await stub.mergeDispatchPost(identity, { kind: "post_rejected", method: "POST", phase: "dispatch", status: 422 }, now + 300_001);
  await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 66 }, now + 300_001);
  await stub.finishDispatchRead(identity, old.claim!, old.operation!.revision, evidence, now + 300_001);
  expect(await stub.dispatchOperation(next.job!.request_id)).toEqual(before);
});

it("keeps candidate and spent tickets across eviction, expiry, aliases and a new UTC day", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, now);
  for (let i = 0; i < 3; i++) {
    const alias = await stub.dispatchForRun(55);
    const reading = await stub.claimDispatchRead(alias!, "status", "synthetic-actor", now + i);
    expect(reading.acquired).toBe(true);
    await stub.finishDispatchRead(identity, reading.claim!, reading.operation!.revision, null, now + i);
  }
  await evictDurableObject(stub);
  const recovered = await stub.dispatchOperation(identity.operation_id);
  expect(recovered).toMatchObject({ candidate_run_id: 55, dispatch_state: "candidate", owner_released_at: null });
  expect((await stub.claimDispatchRead(recovered!, "status", "other-actor", now + 86_400_000)).acquired).toBe(false);
  expect((await stub.freshJob(identity.operation_id))!.polls).toBe(3);
  const budget = await runInDurableObject(stub, (_instance, state) => state.storage.sql.exec<{ used: number }>(
    "SELECT used FROM daily_budgets WHERE budget_key = 'sync-latest-night'").toArray());
  expect(budget).toEqual([{ used: 1 }]);
});

it("persists a general owner before POST and retains its five initial reads without a targeted budget", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const lease = await stub.reserve("refresh-today", now, 120_000);
  const general = { ...descriptor, mode: "general" as const, scope: null, date: null, include_granular: true,
    components: ["summary", "health", "body", "activity_files", "coach_input"] };
  const operation = await stub.beginGeneralDispatch(lease.token!, general, now);
  expect(operation).toMatchObject({ operation_id: lease.token, owner_key: "general-refresh", dispatch_state: "unconfirmed" });
  await stub.mergeDispatchPost(operation!, { kind: "ack_candidate", run_id: 77 }, now);
  const reading = await stub.claimDispatchRead(operation!, "initial", "synthetic-actor", now);
  expect(reading.acquired).toBe(true);
  let current = (await withStartup(stub, reading, 77)).operation;
  for (let i = 0; i < 5; i++) {
    current = (await stub.takeDispatchRead(operation!, reading.claim!, current.revision, "run", now))!;
    expect(current).not.toBeNull();
  }
  expect(await stub.takeDispatchRead(operation!, reading.claim!, current.revision, "run", now)).toBeNull();
  expect(current.general_initial_reads_used).toBe(5);
  expect(current.read_claim!.run_gets_used).toBe(5);
  await stub.finishDispatchRead(operation!, reading.claim!, current.revision, null, now);
  expect((await stub.claimDispatchRead(operation!, "initial", "synthetic-actor", now)).acquired).toBe(false);
  const budgets = await runInDurableObject(stub, (_instance, state) => state.storage.sql.exec("SELECT * FROM daily_budgets").toArray());
  expect(budgets).toEqual([]);
});

it.each(["contract_version", "dispatch_state"])("keeps an unknown persisted %s fail-closed without adopting or rewriting it", async field => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const id = begun.job!.request_id;
  const saved = await runInDurableObject(stub, (_instance, state) => {
    const row = state.storage.sql.exec<{ payload: string }>("SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", id).toArray()[0];
    const payload = JSON.parse(row.payload); payload[field] = field === "contract_version" ? 2 : "unsupported_future_state";
    const body = JSON.stringify(payload);
    state.storage.sql.exec("UPDATE dispatch_operations_v1 SET payload = ? WHERE operation_id = ?", body, id);
    return body;
  });
  const refusal = await runInDurableObject(stub, instance => {
    try { instance.dispatchOperation(id); return "unexpectedly accepted"; }
    catch (error) { return error instanceof Error ? error.message : "unknown"; }
  });
  expect(refusal).toBe("Unverified dispatch operation contract.");
  const after = await runInDurableObject(stub, (_instance, state) => state.storage.sql.exec<{ payload: string }>(
    "SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", id).toArray()[0].payload);
  expect(after).toBe(saved);
});

it.each([false, true])("enforces two run reads or one after expiry=%s plus fixed metadata reads per ticket", async expired => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, now);
  const reading = await stub.claimDispatchRead(identity, "status", "synthetic-actor", now + (expired ? 3 * 60 * 60_000 : 1));
  let current = (await withStartup(stub, reading, 55)).operation;
  for (const kind of [...Array(expired ? 1 : 2).fill("run"), "report", "diagnostics"] as const) {
    current = (await stub.takeDispatchRead(identity, reading.claim!, current.revision, kind, now))!;
    expect(current).not.toBeNull();
  }
  for (const kind of ["receipt", "run", "report", "diagnostics"] as const)
    expect(await stub.takeDispatchRead(identity, reading.claim!, current.revision, kind, now)).toBeNull();
  await stub.finishDispatchRead(identity, reading.claim!, current.revision, null, now);
  expect((await stub.freshJob(identity.operation_id))!.polls).toBe(1);
});

it("keeps the general status window at five reads and 25 seconds for every alias", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const lease = await stub.reserve("refresh-today", now, 120_000);
  const op = (await stub.beginGeneralDispatch(lease.token!, { ...descriptor, mode: "general", scope: null, date: null, components: ["summary", "health", "body"] }, now))!;
  await stub.mergeDispatchPost(op, { kind: "ack_candidate", run_id: 77 }, now);
  const initial = await stub.claimDispatchRead(op, "status", "synthetic-actor", now);
  let current = (await withStartup(stub, initial, 77)).operation;
  for (let i = 0; i < 5; i++) current = (await stub.takeDispatchRead(op, initial.claim!, current.revision, "run", now))!;
  expect(await stub.takeDispatchRead(op, initial.claim!, current.revision, "run", now)).toBeNull();
  await stub.finishDispatchRead(op, initial.claim!, current.revision, null, now);
  const alias = (await stub.dispatchForRun(77))!;
  expect((await stub.claimDispatchRead(alias, "status", "synthetic-actor", now + 24_999)).acquired).toBe(false);
  expect((await stub.claimDispatchRead(alias, "status", "synthetic-actor", now + 25_000)).acquired).toBe(true);
});

it("keeps an interrupted claim held across eviction and expiry without a ticket refund", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  const first = await stub.claimDispatchRead(identity, "status", "synthetic-actor", now);
  await evictDurableObject(stub);
  const late = await stub.claimDispatchRead(identity, "status", "another-actor", now + 86_400_000);
  expect(late.acquired).toBe(false);
  expect(late.operation!.read_claim!.claim_id).toBe(first.claim!.claim_id);
  expect((await stub.freshJob(identity.operation_id))!.polls).toBe(1);
});

it.each(["post422", "otherACK", "changedSource"])("keeps verified binding immutable under contradictory %s", async conflict => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, now);
  const first = await withStartup(stub, await stub.claimDispatchRead(identity, "status", "synthetic-actor", now), 55);
  const binding = { run_id: 55, repository: descriptor.repository, workflow: descriptor.workflow, ref: "main",
    source_sha: "a".repeat(40), correlation_source: "request_receipt", coverage: descriptor };
  const active = (await stub.finishDispatchRead(identity, first.claim!, first.operation!.revision,
    { run_id: 55, status: "in_progress", conclusion: null, binding }, now)).operation!;
  expect(active.dispatch_state).toBe("verified_active");
  expect(await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, now)).toEqual(active);
  if (conflict === "changedSource") {
    const next = await stub.claimDispatchRead(identity, "status", "synthetic-actor", now);
    await stub.finishDispatchRead(identity, next.claim!, next.operation!.revision, { run_id: 55,
      status: "completed", conclusion: "success", binding: { ...binding, source_sha: "b".repeat(40) } }, now);
  } else await stub.mergeDispatchPost(identity, conflict === "post422"
    ? { kind: "post_rejected", method: "POST", phase: "dispatch", status: 422 }
    : { kind: "ack_candidate", run_id: 66 }, now);
  expect(await stub.dispatchOperation(identity.operation_id)).toMatchObject({ evidence_conflict: true,
    dispatch_state: "verified_active", owner_released_at: null, verified_binding: binding, candidate_run_id: 55 });
  expect((await stub.claimDispatchRead(identity, "status", "synthetic-actor", now)).acquired).toBe(false);
});

it("releases only a fully bound terminal operation and preserves its candidate alias", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  await stub.mergeDispatchPost(identity, { kind: "ack_candidate", run_id: 55 }, now);
  const reading = await withStartup(stub, await stub.claimDispatchRead(identity, "status", "synthetic-actor", now), 55);
  const terminalResult = await stub.finishDispatchRead(identity, reading.claim!, reading.operation!.revision,
    { run_id: 55, status: "completed", conclusion: "success", binding: {
      run_id: 55, repository: descriptor.repository, workflow: descriptor.workflow, ref: "main",
      source_sha: "a".repeat(40), correlation_source: "request_receipt", coverage: descriptor } }, now);
  expect(terminalResult.observation_committed).toBe(true);
  const terminal = terminalResult.operation;
  expect(terminal).toMatchObject({ dispatch_state: "verified_terminal", owner_released_at: now,
    candidate_run_id: 55, verified_binding: { run_id: 55 }, read_claim: null });
  expect(await stub.freshJob(identity.operation_id)).toMatchObject({ state: "completed", polls: 1 });
  expect(await stub.dispatchForRun(55)).toEqual(terminal);
});

it("preserves a typed POST rejection across a completed read claim without refunding admission or its ticket", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const identity = { operation_id: begun.job!.request_id, owner_key: "targeted:" + scope, owner_token: begun.job!.request_id };
  const reading = await stub.claimDispatchRead(identity, "status", "synthetic-actor", now);
  await stub.finishDispatchRead(identity, reading.claim!, reading.operation!.revision, null, now);
  const outcome = { kind: "post_rejected" as const, method: "POST" as const, phase: "dispatch" as const, status: 422 };
  const rejected = await stub.mergeDispatchPost(identity, outcome, now);
  expect(rejected).toMatchObject({ dispatch_state: "post_rejected", owner_released_at: now, read_claim: null });
  expect(await stub.mergeDispatchPost(identity, outcome, now)).toEqual(rejected);
  expect(await stub.freshJob(identity.operation_id)).toMatchObject({ state: "failed", polls: 1 });
  const next = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now + 1, false, descriptor);
  expect(next).toMatchObject({ acquired: false, reason: "cooldown" });
});

it.each(["missing_binding", "candidate_mismatch", "nonterminal_observation"])(
  "rejects internally inconsistent persisted terminal %s without releasing an owner", async defect => {
    const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
    const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
    const id = begun.job!.request_id;
    await runInDurableObject(stub, (_instance, state) => {
      const row = state.storage.sql.exec<{ payload: string }>("SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", id).toArray()[0];
      const op = JSON.parse(row.payload);
      op.dispatch_state = "verified_terminal"; op.owner_released_at = now; op.candidate_run_id = 55;
      op.last_observation = { status: defect === "nonterminal_observation" ? "in_progress" : "completed",
        conclusion: "success", observed_at: now };
      op.verified_binding = defect === "missing_binding" ? null : { run_id: defect === "candidate_mismatch" ? 66 : 55,
        repository: descriptor.repository, workflow: descriptor.workflow, ref: descriptor.ref,
        source_sha: "a".repeat(40), correlation_source: "request_receipt", coverage: descriptor };
      state.storage.sql.exec("UPDATE dispatch_operations_v1 SET payload = ? WHERE operation_id = ?", JSON.stringify(op), id);
    });
    const failure = await runInDurableObject(stub, instance => {
      try { instance.dispatchOperation(id); return "accepted"; } catch (error) { return (error as Error).message; }
    });
    expect(failure).toBe("Unverified dispatch operation contract.");
    expect(await stub.freshJob(id)).toMatchObject({ state: "active", polls: 0 });
  });

it("rejects another operation's candidate and an incorrect owner triple without replacing identity", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const first = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false, descriptor);
  const op = (await stub.dispatchOperation(first.job!.request_id))!;
  await stub.mergeDispatchPost(op, { kind: "ack_candidate", run_id: 55 }, now);
  const nextScope = "night/2026-10-15", nextDescriptor = { ...descriptor, scope: nextScope, date: "2026-10-15" };
  // Create a separate native owner to exercise the collision boundary, not admission bypass.
  const otherId = crypto.randomUUID();
  await runInDurableObject(stub, (_instance, state) => {
    const clone = { ...op, operation_id: otherId, owner_token: otherId, owner_key: "targeted:" + nextScope,
      descriptor: nextDescriptor };
    state.storage.sql.exec("INSERT INTO dispatch_operations_v1 (operation_id,owner_key,owner_token,revision,payload) VALUES (?,?,?,0,?)",
      otherId, clone.owner_key, otherId, JSON.stringify(clone));
  });
  const other = (await stub.dispatchOperation(otherId))!;
  expect(await stub.mergeDispatchPost({ ...other, owner_token: crypto.randomUUID() }, { kind: "ack_candidate", run_id: 66 }, now)).toBeNull();
  const collision = await stub.mergeDispatchPost(other, { kind: "ack_candidate", run_id: 55 }, now);
  expect(collision).toMatchObject({ candidate_run_id: null, evidence_conflict: true, owner_released_at: null });
  expect((await stub.dispatchForRun(55))!.operation_id).toBe(op.operation_id);
});
