import { execFileSync } from "node:child_process";
import { copyFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_functional_fix_contract.py", "worker/test-runtime/generated/functional-fix-contract.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_gct_sides_contract.py", "worker/test-runtime/generated/gct-sides.json",
  "worker/test-runtime/generated/gct-sides-work",
], { cwd: root, stdio: "inherit" });
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
  "scripts/build_running_segments_contract.py", "worker/test-runtime/generated/coach-segment-metrics.json",
  "worker/test-runtime/generated/coach-segment-metrics-work",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_native_lap_binding_contract.py", "worker/test-runtime/generated/native-lap-binding.json",
], { cwd: root, stdio: "inherit" });
// Frozen genuine generations remain distinct from the current producer.
copyFileSync(new URL("../../tests/fixtures/coach_engine_190_logical_schema1.json", import.meta.url),
  new URL("../test-runtime/generated/logical-bouts.json", import.meta.url));
copyFileSync(new URL("../../tests/fixtures/coach_engine_190_source_order.json", import.meta.url),
  new URL("../test-runtime/generated/logical-bouts-source-order.json", import.meta.url));
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_logical_bouts_contract.py", "worker/test-runtime/generated/logical-bouts-current.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_logical_bouts_contract.py", "worker/test-runtime/generated/recorded-cadence-current.json", "cadence",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_coach_stored_context_contract.py", "worker/test-runtime/generated/coach-stored-context.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_coach_training_effect_contract.py", "worker/test-runtime/generated/coach-training-effect.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_coach_training_effect_matrix.py", "worker/test-runtime/generated/coach-training-effect-matrix.json",
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

// Night signals use the actual ordinary collector and canonical writers.
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_night_signals_contract.py", "worker/test-runtime/generated/night-signals-contract.json",
], { cwd: root, stdio: "inherit" });

execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_night_products_contract.py", "worker/test-runtime/generated/night-products-contract.json",
], { cwd: root, stdio: "inherit" });

execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_night_training_contract.py", "worker/test-runtime/generated/night-training-contract.json",
], { cwd: root, stdio: "inherit" });

// B retains the additional channels through the actual bounded synthetic facade.
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_night_signals_contract.py", "--additional-signals",
  "worker/test-runtime/generated/night-additional-signals-contract.json",
], { cwd: root, stdio: "inherit" });

// Partial-source retention uses the same public collection/storage path.
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_signal_partial_contract.py", "--output",
  "worker/test-runtime/generated/night-signals-partial-null.json",
], { cwd: root, stdio: "inherit" });

// Unknown legacy point provenance survives two distinct partial collections.
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_signal_partial_contract.py", "--unknown-revision", "--output",
  "worker/test-runtime/generated/night-signals-unknown-revision.json",
], { cwd: root, stdio: "inherit" });
