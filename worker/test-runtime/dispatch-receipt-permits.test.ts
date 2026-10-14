import { env, runInDurableObject } from "cloudflare:test";
import { expect, it } from "vitest";
import { syntheticStartupReceipt } from "../test-support/dispatch-receipt";

const now = Date.parse("2026-10-14T12:00:00Z");
const descriptor = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main",
  mode: "general" as const, scope: null, date: null, activity_id: null, new_expected: false,
  repair_only: false, include_granular: true, components: ["summary", "health", "body", "activity_files", "coach_input"] };
async function general() {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const lease = await stub.reserve("refresh-today", now, 120_000);
  return { stub, operation: (await stub.beginGeneralDispatch(lease.token!, descriptor, now))! };
}

it("does not bypass an existing actor/run window when a status receipt first identifies its run", async () => {
  const { stub, operation } = await general();
  const legacy = await stub.reserve("refresh-status:actor:55", now, 25_000);
  const reading = await stub.claimDispatchRead(operation, "status", "actor", now);
  expect(reading.acquired).toBe(true);
  let current = (await stub.takeDispatchRead(operation, reading.claim!, reading.operation!.revision, "receipt", now))!;
  const saved = await stub.recordDispatchReceipt(operation, reading.claim!, current.revision,
    syntheticStartupReceipt(operation, 55), now);
  expect(saved.receipt_verified).toBe(true); current = saved.operation!;
  expect(current.read_claim!.status_run_reserved).toBe(false);
  expect(await stub.takeDispatchRead(operation, reading.claim!, current.revision, "run", now)).toBeNull();
  await stub.finishDispatchRead(operation, reading.claim!, current.revision, null, now);
  expect((await stub.claimDispatchRead(operation, "status", "actor", now + 24_999)).acquired).toBe(false);
  const rows = await runInDurableObject(stub, (_instance, state) =>
    state.storage.sql.exec<{ token: string }>("SELECT token FROM refresh_leases WHERE lease_key = ?", "refresh-status:actor:55").toArray());
  expect(rows).toEqual([{ token: legacy.token }]);
  expect((await stub.claimDispatchRead(operation, "status", "actor", now + 25_000)).acquired).toBe(true);
});

it("does not consume a new operation window when a known run's window rejects admission", async () => {
  const { stub, operation } = await general();
  await stub.mergeDispatchPost(operation, { kind: "ack_candidate", run_id: 55 }, now);
  await stub.reserve("refresh-status:actor:55", now, 25_000);
  const before = await stub.dispatchOperation(operation.operation_id);
  expect((await stub.claimDispatchRead(operation, "status", "actor", now)).acquired).toBe(false);
  expect(await stub.dispatchOperation(operation.operation_id)).toEqual(before);
  const rows = await runInDurableObject(stub, (_instance, state) =>
    state.storage.sql.exec("SELECT token FROM refresh_leases WHERE lease_key = ?", "refresh-status-op:actor:" + operation.operation_id).toArray());
  expect(rows).toEqual([]);
});

it("does not renew the original initial lease to read a late general receipt", async () => {
  const { stub, operation } = await general();
  expect((await stub.claimDispatchRead(operation, "initial", "actor", now + 120_000)).acquired).toBe(false);
  expect(await stub.dispatchOperation(operation.operation_id)).toEqual(operation);
});

it("holds an unmarked persisted C8 owner without rewriting it or spending a targeted ticket", async () => {
  const stub = env.REFRESH_COORDINATOR.getByName(crypto.randomUUID());
  const scope = "night/2026-10-14";
  const begun = await stub.beginFresh(scope, "night", JSON.stringify({ kind: "night", date: "2026-10-14" }), now, false,
    { ...descriptor, mode: "night", scope, date: "2026-10-14", include_granular: false, components: ["sleep", "hrv"] });
  const id = begun.job!.request_id;
  const before = await runInDurableObject(stub, (_instance, state) => {
    const op = JSON.parse(state.storage.sql.exec<{ payload: string }>("SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", id).toArray()[0].payload);
    delete op.startup_receipt;
    const raw = JSON.stringify(op);
    state.storage.sql.exec("UPDATE dispatch_operations_v1 SET payload = ? WHERE operation_id = ?", raw, id);
    return raw;
  });
  const operation = (await stub.dispatchOperation(id))!;
  expect((await stub.claimDispatchRead(operation, "status", "actor", now)).acquired).toBe(false);
  expect(await stub.freshJob(id)).toMatchObject({ polls: 0, state: "active" });
  const after = await runInDurableObject(stub, (_instance, state) =>
    state.storage.sql.exec<{ payload: string }>("SELECT payload FROM dispatch_operations_v1 WHERE operation_id = ?", id).toArray()[0].payload);
  expect(after).toBe(before);
});

it("keeps the first startup source immutable after a failed observation and a contradictory receipt", async () => {
  const { stub, operation } = await general();
  const first = await stub.claimDispatchRead(operation, "initial", "actor", now);
  let current = (await stub.takeDispatchRead(operation, first.claim!, first.operation!.revision, "receipt", now))!;
  const evidence = syntheticStartupReceipt(operation, 55);
  current = (await stub.recordDispatchReceipt(operation, first.claim!, current.revision, evidence, now)).operation!;
  await stub.finishDispatchRead(operation, first.claim!, current.revision, null, now);
  const second = await stub.claimDispatchRead(operation, "status", "actor", now);
  current = (await stub.takeDispatchRead(operation, second.claim!, second.operation!.revision, "receipt", now))!;
  const conflict = await stub.recordDispatchReceipt(operation, second.claim!, current.revision,
    { ...evidence, source: { ...evidence.source, source_sha: "b".repeat(40) } }, now);
  expect(conflict.receipt_verified).toBe(false);
  expect(conflict.operation).toMatchObject({ candidate_run_id: 55, owner_released_at: null,
    evidence_conflict: true, startup_receipt: evidence });
  expect(await stub.takeDispatchRead(operation, second.claim!, conflict.operation!.revision, "run", now)).toBeNull();
});
