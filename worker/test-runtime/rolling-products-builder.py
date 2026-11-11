"""Generate small real offline-CLI products for stored rolling MCP checks."""
import json
import os
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    end = date(2026, 11, 7)
    nights = []
    for offset in range(365):
        day = (end - timedelta(days=364-offset)).isoformat()
        channels = {}
        for channel, value, unit in (("heart_rate", 60, "bpm"), ("hrv", 40, "ms")):
            channels[channel] = {"status": "available", "unit": unit,
                "source_revisions": [{"date": day, "revision": "synthetic-v1"}],
                "points": [{"timestamp_utc": day + "T00:00:00Z", "value": value, "time_basis": "utc"}]}
        nights.append({"date": day, "wake_date": day, "method_version": "night-signals-v1",
            "resolution": "native", "status": "available", "sleep_start_utc": day + "T00:00:00Z",
            "sleep_end_utc": day + "T01:00:00Z", "actual_night_timezone": "Europe/Oslo",
            "user_context": None, "channels": channels})
    products = []
    with tempfile.TemporaryDirectory(dir=os.environ.get("TEMP")) as folder:
        source = Path(folder) / "source.json"
        source.write_text(json.dumps({"nights": nights}), encoding="utf-8")
        for window in (7, 28, 30, 90, 365):
            for channel in ("heart_rate", "hrv"):
                extra = ["--start-date", (end-timedelta(days=29)).isoformat()] if window == 30 else []
                command = [sys.executable, "-B", "-X", "utf8", "-m", "pipeline.night_products", "baseline",
                    "--input", str(source), "--end-date", end.isoformat(), "--period", "custom" if window == 30 else str(window),
                    "--alignment", "sleep_start", "--bin-minutes", "30", "--channel", channel, *extra]
                result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=True,
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
                products.append({"window": window, "channel": channel, **json.loads(result.stdout)})
    output = ROOT / "worker/test-runtime/generated/rolling-products-contract.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"products": products}, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"products": len(products), "bytes": output.stat().st_size}))


if __name__ == "__main__":
    main()
