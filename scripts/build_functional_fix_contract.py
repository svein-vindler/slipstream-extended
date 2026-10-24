"""Synthetic producer inputs for the functional-fix Worker response regressions."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.granular import normalize_strength_session  # noqa: E402
from pipeline.schema import Activity  # noqa: E402
from pipeline.writer import write_dataset  # noqa: E402


def build():
    names = ["Synthetic\nrun", "Synthetic\r\nrun", 'Hill, "repeats"\nsecond line', "Plain æ", ""]
    activities = [Activity(source="garmin", source_id=str(100 + index),
                           start=datetime(2026, 10, 23, 12, index, tzinfo=timezone.utc),
                           sport="run", name=name, distance_km=10, moving_s=3000,
                           local_start_date="2026-10-23") for index, name in enumerate(names)]
    cases = {}
    for index, (name, sets) in enumerate([
        ("mixed", [{"setType": "ACTIVE", "repetitionCount": 10, "weight": 10000, "duration": 30},
                   {"setType": "ACTIVE", "repetitionCount": 10, "weight": None, "duration": None}]),
        ("zero", [{"setType": "ACTIVE", "repetitionCount": 0, "weight": 0, "duration": 0}]),
        ("empty", []),
    ]):
        identifier = str(200 + index)
        cases[name] = normalize_strength_session(identifier, {"exerciseSets": sets})
        activities.append(Activity(source="garmin", source_id=identifier,
                                   start=datetime(2026, 10, 23, 13, index, tzinfo=timezone.utc),
                                   sport="strength", name="Synthetic strength",
                                   local_start_date="2026-10-23"))
    with TemporaryDirectory() as temporary:
        write_dataset(activities, temporary)
        csv = (Path(temporary) / "activities.csv").read_bytes().decode("utf-8")
    return {"csv": csv, "names": names, "strength": cases}


if __name__ == "__main__":
    output = Path(sys.argv[1])
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(build(), ensure_ascii=False), encoding="utf-8")
