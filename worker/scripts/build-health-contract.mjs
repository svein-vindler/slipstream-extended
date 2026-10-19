import { execFileSync } from "node:child_process";
import { copyFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_training_day_contract.py", "worker/test-runtime/generated/training-day.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_training_context_contract.py", "worker/test-runtime/generated/training-context.json",
], { cwd: root, stdio: "inherit" });
// Keep genuine historical 1.1.0 bytes for the legacy-reader tests. The current
// producer is exercised separately below, never relabelled as an old engine.
copyFileSync(new URL("../../tests/fixtures/coach_engine_110_transition.json", import.meta.url),
  new URL("../test-runtime/generated/recorded-running-metrics.json", import.meta.url));
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_running_segments_contract.py", "worker/test-runtime/generated/recorded-running-segments.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_coach_stored_context_contract.py", "worker/test-runtime/generated/coach-stored-context.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_health_contract.py", "--output",
  "worker/test-runtime/generated/health-contract.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_weight_contract.py", "--output",
  "worker/test-runtime/generated/weight-contract.json",
], { cwd: root, stdio: "inherit" });

execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_coach_versions_contract.py", "worker/test-runtime/generated/coach-versions.json",
], { cwd: root, stdio: "inherit" });

execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_dispatch_contract.py", "worker/test-runtime/generated/dispatch-contract.json",
], { cwd: root, stdio: "inherit" });

execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_source_status_contract.py", "worker/test-runtime/generated/source-status-contract.json",
], { cwd: root, stdio: "inherit" });

execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_scalar_leaf_contract.py", "worker/test-runtime/generated/scalar-leaf-contract.json",
], { cwd: root, stdio: "inherit" });
