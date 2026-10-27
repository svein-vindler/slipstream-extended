"""Real Python CSV output through the Worker's public parser and filter."""
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline.schema import Activity
from pipeline.writer import write_dataset


@pytest.mark.parametrize("name", [
    "Synthetic\nrun", "Synthetic\r\nrun", 'Hill, "repeats"\nsecond line',
    "Trailing\n", "Plain æ run", "", '"Quoted"',
])
def test_worker_reads_one_csv_record_per_written_activity(tmp_path, name):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js with TypeScript stripping is required for the Worker seam")
    write_dataset([Activity(
        source="garmin", source_id="123", start=datetime(2026, 10, 23, 12, tzinfo=timezone.utc),
        sport="run", name=name, distance_km=10, moving_s=3000,
        local_start_date="2026-10-23",
    )], str(tmp_path))
    module = (Path(__file__).resolve().parents[1] / "worker/src/lib.ts").as_uri()
    code = f"""
import {{ readFileSync }} from 'node:fs';
import {{ parseCsv, filterActs, summarize }} from {json.dumps(module)};
const rows = parseCsv(readFileSync(process.argv[1], 'utf8'));
console.log(JSON.stringify({{count: rows.length, name: rows[0].name,
  date: rows[0].date.toISOString(), localDate: rows[0].localDate,
  runs: summarize(filterActs(rows, {{sport_type: 'run'}}))}}));
"""
    process = subprocess.run(
        [node, "--experimental-strip-types", "--input-type=module", "-e", code,
         str(tmp_path / "activities.csv")], capture_output=True, text=True, encoding="utf-8", check=True,
    )
    result = json.loads(process.stdout)
    assert result["count"] == 1
    assert result["name"] == name
    assert result["date"] == "2026-10-23T12:00:00.000Z"
    assert result["localDate"] == "2026-10-23"
    assert result["runs"]["activities"] == 1
    assert result["runs"]["total_distance_km"] == 10
