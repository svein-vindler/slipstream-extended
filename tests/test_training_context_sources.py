"""Verify source representation using only invented data and the pinned SDK."""
import base64
import copy
import gzip
import json
from importlib.metadata import version

import pytest
from garmin_fit_sdk import Decoder, Profile, Stream
from garmin_fit_sdk import fit as FIT

from pipeline.granular import decode_fit
from pipeline.manual_activity_refresh import _complete_activity_details
from scripts.build_training_context_contract import DTO, build, synthetic_fit


def test_pinned_session_fields_and_decoder_scale():
    assert version("garmin-fit-sdk") == "21.217.0"
    session = Profile["messages"][18]
    assert session["messages_key"] == "session_mesgs"
    for number, name in [(24, "total_training_effect"), (137, "total_anaerobic_training_effect")]:
        field = session["fields"][number]
        assert (field["name"], field["base_type"], field["scale"], field["offset"], field["units"]) == (
            name, "uint8", [10], [0], "")
    assert FIT.BASE_TYPE_DEFINITIONS[2]["invalid"] == 255
    raw, errors = Decoder(Stream.from_byte_array(synthetic_fit())).read(
        apply_scale_and_offset=False, merge_heart_rates=False)
    assert errors == []
    assert raw["session_mesgs"][0]["total_training_effect"] == 34
    decoded = decode_fit(synthetic_fit(), activity=DTO)
    row = decoded["messages"]["session_mesgs"][0]
    assert row["total_training_effect"] == 3.4
    assert row["total_anaerobic_training_effect"] == 0
    assert decoded["decode_errors"] == []


@pytest.mark.parametrize("value,expected", [(0, 0), (5, 5), (None, None), (25.5, None)])
def test_zero_and_raw_invalid_sentinel_are_not_confused(value, expected):
    decoded = decode_fit(synthetic_fit(value, value), activity=DTO)
    row = decoded["messages"]["session_mesgs"][0]
    assert row.get("total_training_effect") == expected
    assert row.get("total_anaerobic_training_effect") == expected
    if expected is None:
        assert "total_training_effect" not in row


@pytest.mark.parametrize("nested", [False, True])
def test_dto_variants_only_preserve_existing_identity_clocks(nested):
    dto = copy.deepcopy(DTO)
    if nested:
        dto["summaryDTO"].update({key: dto.pop(key) for key in ("startTimeLocal", "startTimeGMT")})
    row = {"Activity ID": "garmin-42", "Activity Date": DTO["startTimeGMT"], "Activity Type": "running"}
    complete = _complete_activity_details(dto, row)
    decoded = decode_fit(synthetic_fit(), activity=complete)
    assert decoded["activity"]["id"] == "42"
    assert decoded["activity"]["start_time_local"] == "2026-10-08 01:30:00"
    assert "trainingEffect" not in decoded["activity"]
    assert "summaryDTO" not in decoded


def test_export_retains_session_fields_without_adding_storage_objects():
    objects = build()
    assert set(objects) == {"summary/activities.csv", "activities/2026/42/activity.v1.json",
                            "activities/2026/42/activity.endurance.v1.json"}
    decoded = json.loads(gzip.decompress(base64.b64decode(objects["activities/2026/42/activity.v1.json"]["body"])))
    assert decoded["messages"]["session_mesgs"][0]["total_training_effect"] == 3.4
    assert "training_context" not in decoded  # Pure read projection: storage remains unchanged.
