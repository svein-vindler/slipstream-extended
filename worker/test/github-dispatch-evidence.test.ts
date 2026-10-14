import { expect, it } from "vitest";
import { dispatchLatestNight, GitHubRequestError } from "../src/github";

const config = { repository: "synthetic/slipstream", workflow: "refresh.yml", ref: "main",
  token: "synthetic-offline-token", cooldownMinutes: 30 };

it("returns the direct ACK before any run lookup can lose its evidence", async () => {
  const methods: string[] = [];
  const fetcher: typeof fetch = async (_url, init) => {
    methods.push(init?.method ?? "GET");
    if (init?.method === "POST") return Response.json({ workflow_run_id: 55 });
    return new Response("SYNTHETIC_PROVIDER_TEXT_MUST_NOT_SELECT_POST_REJECTION", { status: 422 });
  };
  await expect(dispatchLatestNight(config, "2026-10-14", "12345678-1234-4234-9234-123456789abc", fetcher))
    .resolves.toEqual({ kind: "ack_candidate", run_id: 55 });
  expect(methods).toEqual(["POST"]);
});

it.each([400, 401, 403, 404, 422, 429])("returns typed direct POST rejection %s without reading provider text", async status => {
  const fetcher: typeof fetch = async () => new Response("SYNTHETIC_PROVIDER_TEXT", { status });
  const result = await dispatchLatestNight(config, "2026-10-14", "12345678-1234-4234-9234-123456789abc", fetcher);
  expect(result).toEqual({ kind: "post_rejected", method: "POST", phase: "dispatch", status });
  expect(JSON.stringify(result)).not.toContain("SYNTHETIC_PROVIDER_TEXT");
});

it("does not mistake a thrown provider exception for an observed POST response", async () => {
  const fetcher: typeof fetch = async () => { throw new GitHubRequestError(422, "POST", "SYNTHETIC_PROVIDER_TEXT"); };
  await expect(dispatchLatestNight(config, "2026-10-14", "12345678-1234-4234-9234-123456789abc", fetcher))
    .resolves.toEqual({ kind: "unconfirmed", reason: "transport" });
});

it("keeps malformed successful acknowledgement JSON unconfirmed", async () => {
  const fetcher: typeof fetch = async () => new Response("SYNTHETIC_PROVIDER_TEXT_NOT_JSON", { status: 200 });
  await expect(dispatchLatestNight(config, "2026-10-14", "12345678-1234-4234-9234-123456789abc", fetcher))
    .resolves.toEqual({ kind: "unconfirmed", reason: "invalid_ack" });
});
