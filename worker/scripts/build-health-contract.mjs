import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_health_contract.py", "--output",
  "worker/test-runtime/generated/health-contract.json",
], { cwd: root, stdio: "inherit" });
execFileSync(process.env.SLIPSTREAM_TEST_PYTHON || "python", [
  "scripts/build_weight_contract.py", "--output",
  "worker/test-runtime/generated/weight-contract.json",
], { cwd: root, stdio: "inherit" });
