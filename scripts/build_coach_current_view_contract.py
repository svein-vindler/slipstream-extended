"""Capture a genuine current-generation synthetic stored-source Coach publication."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_logical_bouts_contract import build  # noqa: E402

if __name__ == "__main__":
    Path(sys.argv[1]).write_text(
        json.dumps(build(cadence=True), sort_keys=True) + "\n", encoding="utf-8")
