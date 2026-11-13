"""Verify source representation using only invented data and the pinned SDK."""
import base64
import copy
import gzip
import hashlib
import json
from importlib.metadata import version

import pytest
from garmin_fit_sdk import Decoder, Profile, Stream
from garmin_fit_sdk import fit as FIT

from pipeline.granular import decode_fit, json_bytes
from pipeline.manual_activity_refresh import _complete_activity_details
from scripts.build_training_context_contract import DTO, build, synthetic_fit


def test_pinned_session_fields_and_decoder_scale():
    assert version("garmin-fit-sdk") == "21.221.0"
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


@pytest.mark.parametrize("wire,raw_sha256,message_key,expected", [
    ("0e20e2520e0000002e4649545df6400000140001720488000000c07fc1b5",
     "f13f214323f0a882e3cc7630c2383465ffb13f00451419a81181331db40a68e7",
     "record_mesgs", {}),
    ("0e20e252120000002e4649545caf400000a40001050888000000c07f000020400fba",
     "166463776a4f6c272f63c8a77b840285a560db84da702c2c93278f14ddcb3eb8",
     "gyroscope_data_mesgs", {"calibrated_gyro_x": [None, 2.5]}),
    ("0e20e252120000002e4649545caf400000a40001050888000000c07f0000c07f066a",
     "7510661917ff5cc797e8d30440c9ab3f68d4a12f28d70ed6d1fe6add1c18cf40",
     "gyroscope_data_mesgs", {}),
], ids=["scalar-nan", "mixed-array-nan", "all-array-nan"])
def test_raw_float_nan_projection_keeps_missing_values_and_real_source_hash(
        wire, raw_sha256, message_key, expected):
    # Invented single-message FIT: quiet-NaN float32 payloads with valid CRCs.
    # Retain these wire bytes across SDK updates; never regenerate old history.
    raw = bytes.fromhex(wire)
    assert hashlib.sha256(raw).hexdigest() == raw_sha256
    for scale in (False, True):
        decoder = Decoder(Stream.from_byte_array(bytearray(raw)))
        assert decoder.is_fit() and decoder.check_integrity()
        messages, errors = Decoder(Stream.from_byte_array(bytearray(raw))).read(
            apply_scale_and_offset=scale, merge_heart_rates=False)
        assert errors == []
        assert messages == {message_key: [expected]}
    decoded = decode_fit(raw, activity=DTO)
    assert decoded["source_fit_sha256"] == raw_sha256
    assert decoded["decode_errors"] == []
    assert decoded["message_counts"] == {message_key: 1}
    assert decoded["messages"] == {message_key: [expected]}
    projected = json.loads(json_bytes(decoded))
    assert projected["messages"] == {message_key: [expected]}
    assert projected["source_fit_sha256"] == raw_sha256
    assert projected["activity"] == {
        "id": "42", "name": DTO["activityName"], "type": "running",
        "start_time_gmt": DTO["startTimeGMT"], "start_time_local": DTO["startTimeLocal"],
    }


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
