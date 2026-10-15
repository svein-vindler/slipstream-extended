"""Scalar leaf contracts at the actual producer and storage boundaries."""

import pytest

from pipeline.granular import normalize_strength_session
from pipeline.granular_export import export_activity
from pipeline.health_detail import normalize_sleep_detail
from pipeline.health_sync import HealthNotReady, store_day
from scripts.build_scalar_leaf_contract import DAY, build, sleep_source, strength_row
from scripts.build_training_context_contract import synthetic_fit


def test_strength_rejects_compound_metric_instead_of_publishing_it():
    with pytest.raises(ValueError, match="^Unsupported strength metric shape\\.$"):
        normalize_strength_session("123", {"exerciseSets": [{
            "setType": "ACTIVE", "future_metric": {"private": "D4_SYNTHETIC_MARKER"},
        }]})


def test_sleep_rejects_compound_qualifier_before_string_conversion():
    with pytest.raises(ValueError, match="^Unsupported sleep qualifier shape\\.$"):
        normalize_sleep_detail("2026-09-30", {"dailySleepDTO": {"sleepTimeSeconds": 25200, "sleepScores": {
            "future_score": {"value": 0, "qualifierKey": {"private": "D4_SYNTHETIC_MARKER"}},
        }}})


@pytest.mark.parametrize("value", [{}, [], [0, False, None, "D4_SYNTHETIC_MARKER"],
                                 {"nested": ["D4_SYNTHETIC_MARKER"]}])
@pytest.mark.parametrize("name", ["__proto__", "constructor", "future_metric"])
def test_all_metric_names_require_scalar_leaves(name, value):
    row = strength_row()
    row[name] = value
    with pytest.raises(ValueError, match="^Unsupported strength metric shape\\.$"):
        normalize_strength_session("123", {"exerciseSets": [row]})


def test_legal_producer_scalars_units_literal_dimensions_and_history_are_preserved():
    result = build()
    strength = result["strength"]
    assert strength["sets"][0]["performance_metrics"] == {
        "avgConcentricMeanVelocity": 0.42, "tal_grit": None, "__proto__": 0,
        "constructor": False, "future_numeric_text": "0", "future_text": "scalar",
        "future_true": True,
    }
    assert strength["sets"][0]["weight_kg"] == 60
    assert strength["sets"][0]["repetitions"] == 0
    assert strength["sets"][0]["active_duration_seconds"] == 12.5
    assert strength["summary"]["by_exercise"] == {
        "__proto__": {"sets": 1, "repetitions": 0, "external_volume_kg": 0.0},
    }
    assert strength["summary"]["by_category"] == strength["summary"]["by_exercise"]
    scores = result["sleep"]["score_breakdown"]
    assert scores["future_zero"] == {"value": 0, "qualifier": None}
    assert scores["future_null"] == {"value": None, "qualifier": "UNKNOWN"}
    assert scores["future_numeric_qualifier"] == {"value": 0, "qualifier": "0"}
    assert scores["future_boolean"] == {"value": 0, "qualifier": "False"}
    assert scores["future_fallback"] == {"value": 1, "qualifier": "GOOD"}
    assert scores["future_history"]["qualifier"] == "{'historical': ['synthetic text']}"


@pytest.mark.parametrize("field", ["qualifierKey", "qualifier"])
@pytest.mark.parametrize("value", [{}, [], {"note": "D4_SYNTHETIC_MARKER"}, [0, None]])
def test_sleep_rejects_selected_compound_qualifier_alias(field, value):
    raw = sleep_source()
    raw["dailySleepDTO"]["sleepScores"]["future_score"] = {"value": 0, field: value}
    with pytest.raises(ValueError, match="^Unsupported sleep qualifier shape\\.$"):
        normalize_sleep_detail(DAY, raw)


def test_qualifier_alias_order_does_not_examine_unused_fallback_or_forgive_bad_first():
    raw = sleep_source()
    score = {"value": 0, "qualifierKey": "GOOD", "qualifier": {"unused": "D4_SYNTHETIC_MARKER"}}
    raw["dailySleepDTO"]["sleepScores"]["future_score"] = score
    assert normalize_sleep_detail(DAY, raw)["score_breakdown"]["future_score"]["qualifier"] == "GOOD"
    score.update(qualifierKey={}, qualifier="GOOD")
    with pytest.raises(ValueError, match="^Unsupported sleep qualifier shape\\.$"):
        normalize_sleep_detail(DAY, raw)


@pytest.mark.parametrize("value,expected", [(0, "0"), (-3, "-3"), (1.25, "1.25"),
                                          (True, "True"), (False, "False"),
                                          ("0", "0"), ("GOOD", "GOOD"), (None, None), ("", None)])
def test_qualifier_scalar_conversion_is_unchanged(value, expected):
    raw = sleep_source()
    raw["dailySleepDTO"]["sleepScores"]["future_score"] = {"value": 0, "qualifierKey": value}
    assert normalize_sleep_detail(DAY, raw)["score_breakdown"]["future_score"] == {
        "value": 0, "qualifier": expected,
    }


class MemoryStore:
    def __init__(self):
        self.objects = {}
        self.calls = []

    def get(self, key):
        self.calls.append("GET")
        return self.objects[key]

    def put(self, key, data, content_type, *, encoding=None):
        self.calls.append("PUT")
        self.objects[key] = data


@pytest.mark.parametrize("value", [{"private": "D4_SYNTHETIC_MARKER"}, []])
def test_sleep_bad_qualifier_preserves_good_canonical_without_store_io(value, record_property):
    store = MemoryStore()
    raw = sleep_source()
    store_day(store, "sleep", DAY, raw)
    before = dict(store.objects)
    store.calls.clear()
    raw["dailySleepDTO"]["sleepScores"]["future_score"] = {"value": 0, "qualifierKey": value}
    with pytest.raises(HealthNotReady) as caught:
        store_day(store, "sleep", DAY, raw)
    assert caught.value.status == "invalid_response"
    assert store.objects == before
    assert store.calls == []
    record_property("resources", "invalid sleep: GET=0 PUT=0; previous canonical bytes unchanged")


def test_strength_bad_metric_preserves_good_canonical_using_stored_fit(tmp_path, record_property):
    store = MemoryStore()
    activity = {"activityId": 123, "activityType": {"typeKey": "strength_training"},
                "startTimeGMT": "2026-09-30 10:00:00", "startTimeLocal": "2026-09-30 12:00:00"}
    fit_key = "activities/2026/123/activity.fit"
    json_key = "activities/2026/123/activity.v1.json"
    store.objects.update({fit_key: synthetic_fit(), json_key: b"synthetic previous good canonical"})
    before = dict(store.objects)
    row = strength_row()
    row["future_metric"] = {"private": "D4_SYNTHETIC_MARKER"}
    # Missing inventory entry requires re-decoding; existing canonical bytes stay intact.
    with pytest.raises(ValueError, match="^Unsupported strength metric shape\\.$"):
        export_activity(activity, object(), store, tmp_path, existing_keys={fit_key},
                        exercise_sets={"exerciseSets": [row]})
    assert store.objects == before
    assert store.calls == ["GET"]
    record_property("resources", "invalid strength stored FIT: GET=1 PUT=0 download=0; canonical unchanged")
