"""Generate synthetic FIT-to-R2 bytes; never initializes Garmin or an R2 client."""
from __future__ import annotations

import base64
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from garmin_fit_sdk import Encoder  # noqa: E402

from pipeline.granular_export import export_activity  # noqa: E402
from pipeline.sources.garmin import fetch  # noqa: E402
from pipeline.writer import write_dataset  # noqa: E402

START = datetime(2026, 10, 7, 23, 30, tzinfo=timezone.utc)
DTO = {
    "activityId": 42, "activityName": "Synthetic run",
    "activityType": {"typeKey": "running"},
    "startTimeGMT": "2026-10-07 23:30:00", "startTimeLocal": "2026-10-08 01:30:00",
    "trainingEffect": 4.8, "anaerobicTrainingEffect": 4.9, "activityTrainingLoad": 123,
    "summaryDTO": {"trainingEffect": 1.1, "anaerobicTrainingEffect": 1.2},
}
TCX = b'''<TrainingCenterDatabase><Activities><Activity Sport="Running">
<Id>2026-10-07T23:30:00Z</Id><Lap StartTime="2026-10-07T23:30:00Z">
<TotalTimeSeconds>1800</TotalTimeSeconds><DistanceMeters>5000</DistanceMeters>
<Track><Trackpoint><Time>2026-10-07T23:30:00Z</Time><DistanceMeters>0</DistanceMeters>
<HeartRateBpm><Value>130</Value></HeartRateBpm></Trackpoint>
<Trackpoint><Time>2026-10-08T00:00:00Z</Time><DistanceMeters>5000</DistanceMeters>
<HeartRateBpm><Value>140</Value></HeartRateBpm></Trackpoint></Track>
</Lap></Activity></Activities></TrainingCenterDatabase>'''


def synthetic_fit(aerobic=3.4, anaerobic=0, *, records=0):
    encoder = Encoder()
    for second in range(records):
        encoder.write_mesg({"mesg_num": 20, "timestamp": START + timedelta(seconds=second),
                            "heart_rate": 130 + second % 15, "distance": second * 5000 / 1800})
    encoder.write_mesg({
        "mesg_num": 18, "message_index": 0, "start_time": START,
        "timestamp": START + timedelta(minutes=30), "sport": "running",
        "total_training_effect": aerobic, "total_anaerobic_training_effect": anaerobic,
        "training_load_peak": 120,
    })
    return bytes(encoder.close())


class SyntheticGarmin:
    def __init__(self):
        self.calls = []

    def get_activities_by_date(self, *args):
        self.calls.append("get_activities_by_date")
        return [DTO]

    def download_activity(self, activity_id, *, dl_fmt):
        from garminconnect import Garmin
        self.calls.append("download_activity")
        assert activity_id == 42
        return synthetic_fit(records=1800) if dl_fmt == Garmin.ActivityDownloadFormat.ORIGINAL else TCX


class SyntheticStore:
    def __init__(self):
        self.objects = {}

    def put(self, key, data, content_type, *, encoding=None):
        self.objects[key] = {"body": base64.b64encode(data).decode(),
                             "content_type": content_type, "encoding": encoding}


def build():
    garmin, store = SyntheticGarmin(), SyntheticStore()
    with TemporaryDirectory() as temporary:
        output = Path(temporary)
        activities = fetch(client=garmin, end_date=START.date())
        write_dataset(activities, str(output))
        store.put("summary/activities.csv", (output / "activities.csv").read_bytes(), "text/csv")
        export_activity(DTO, garmin, store, output)
    # Raw FIT/TCX remain local synthetic test inputs and are not needed by MCP.
    return {key: value for key, value in store.objects.items()
            if key.endswith(".json") or key.endswith(".csv")}


if __name__ == "__main__":
    target = Path(sys.argv[1])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(build(), sort_keys=True), encoding="utf-8")
