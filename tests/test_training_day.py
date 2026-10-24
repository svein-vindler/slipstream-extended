import hashlib
import io
import json
import socket
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError

from pipeline import refresh, training_sync
from pipeline.r2_store import R2BudgetError, R2ReadBoundaryError, R2Store
from pipeline.sources.garmin_readonly import GarminReadOnlyViolation
from pipeline.sources.training_readiness import ReadinessRead, bounded_read
from pipeline.training_day import (
    MAX_DAY_BYTES,
    configuration,
    day_key,
    decode_day,
    encode_day,
    merge_day,
    normalize,
)

DAY = "2026-10-08"
NOW = datetime(2026, 10, 8, 6, tzinfo=timezone.utc)
CHECK = NOW.isoformat()
CONFIG = {"status": "enabled", "date": DAY, "timezone": "Europe/Oslo", "device_id": None}
DTO = {"deviceId": 42, "calendarDate": DAY, "timestampLocal": "2026-10-08T07:30:00",
       "timestamp": "2026-10-08T05:30:00Z", "score": 80, "recoveryTime": 0,
       "inputContext": "AFTER_WAKEUP_RESET", "privateUnknownField": "never-export-this"}


class MemoryClient:
    """Synthetic S3, same application counters as real R2Store, no SDK retries."""
    def __init__(self):
        self.objects = {}
        self.metadata = {}
        self.bodies = []
        self.upload_bytes = 0
        self.download_bytes = 0
        self.consumed_bytes = 0
        self.counts = dict.fromkeys(("get", "head", "put", "list"), 0)

    def get_object(self, *, Key, **kwargs):
        self.counts["get"] += 1
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        body = io.BytesIO(self.objects[Key])
        client = self
        class CountedBody:
            def read(self, n):
                data = body.read(n)
                client.consumed_bytes += len(data)
                return data
            def close(self):
                body.close()
            @property
            def closed(self):
                return body.closed
        reader = CountedBody()
        self.bodies.append(reader)
        self.download_bytes += len(self.objects[Key])
        return {"Body": reader, "ContentLength": len(self.objects[Key]), **self.metadata.get(Key, {})}

    def head_object(self, *, Key, **kwargs):
        self.counts["head"] += 1
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        body = self.objects[Key]
        return {"ContentLength": len(body), "ContentType": "application/json",
                "ETag": hashlib.md5(body, usedforsecurity=False).hexdigest()}

    def put_object(self, *, Key, Body, **kwargs):
        self.counts["put"] += 1
        self.upload_bytes += len(Body)
        self.objects[Key] = Body

    def get_paginator(self, _):
        client = self
        class Pages:
            def paginate(self, **kwargs):
                client.counts["list"] += 1
                return [{"Contents": [{"Key": key, "Size": len(body)} for key, body in client.objects.items()]}]
        return Pages()


def make_day(rows=None, checked=CHECK, previous=None, config=None):
    config = config or CONFIG
    devices, status = normalize([DTO] if rows is None else rows, config, checked)
    return merge_day(previous, devices, status, config, checked)


def test_new_collection_records_the_reviewed_sdk_without_changing_measurements():
    day = decode_day(encode_day(make_day()), DAY)
    assert day["sdk_version"] == "0.3.20"
    assert day["devices"][0]["latest"]["readiness"]["value"] == 80
    assert day["devices"][0]["latest"]["recovery"]["value"] == 0


@pytest.mark.parametrize("sdk_version", ["0.3.19", "0.4.0", None, {}, []])
def test_unreviewed_or_malformed_stored_sdk_version_is_rejected(sdk_version):
    day = {**make_day(), "sdk_version": sdk_version}
    with pytest.raises(ValueError, match="Unverified training day"):
        decode_day(encode_day(day), DAY)


def test_legacy_retention_and_new_collection_do_not_relabel_each_other():
    # Captured from the real f4346f18 producer before this upgrade, not a
    # current-generation object relabelled as historical data.
    raw = (Path(__file__).parent / "fixtures/training_day_sdk0317.json").read_bytes()
    previous = decode_day(raw, DAY)
    original = encode_day(previous)
    retained = merge_day(previous, [], "source_error", CONFIG, CHECK)
    assert retained["sdk_version"] == "0.3.17"
    row = {**DTO, "score": 90, "recoveryTime": None,
           "timestamp": "2026-10-08T06:30:00Z", "timestampLocal": "2026-10-08T08:30:00"}
    mixed = decode_day(encode_day(make_day([row], previous=previous)), DAY)
    assert mixed["sdk_version"] == "mixed"
    observation = mixed["devices"][0]["latest"]
    assert observation["readiness"]["value"] == 90
    assert observation["recovery"]["value"] == 0
    assert observation["recovery"]["retained_previous"] is True
    assert observation["recovery"]["source_checked_at"] == CHECK
    replaced = make_day([{**row, "recoveryTime": 30}], previous=previous)
    assert replaced["sdk_version"] == "0.3.20"
    assert encode_day(previous) == original
    assert (Path(__file__).parent / "fixtures/training_day_sdk0317.json").read_bytes() == raw


def test_failed_check_does_not_relabel_an_empty_legacy_day():
    previous = json.loads((Path(__file__).parent / "fixtures/training_day_sdk0317.json").read_bytes())
    previous.update(devices=[], selection={"device_id": None, "status": "no_observation"},
                    last_attempt={"status": "no_observation", "attempted_at": CHECK},
                    source_revision=hashlib.sha256(b"[]").hexdigest())
    previous = decode_day(encode_day(previous), DAY)
    after = merge_day(previous, [], "source_error", CONFIG, "2026-10-08T08:00:00+00:00")
    assert after["sdk_version"] == "0.3.17"
    assert after["source_checked_at"] == CHECK
    assert after["source_revision"] == previous["source_revision"]


def provider(payload=None, status="available"):
    calls = []
    def read(day):
        calls.append(day)
        result = ReadinessRead(status, [DTO] if payload is None else payload)
        result.metrics["fitness_http_attempts"] = 1
        result.metrics["body_bytes"] = len(json.dumps(result.payload).encode()) if status == "available" else 0
        return result
    return SimpleNamespace(get_training_readiness_bounded=read), calls


@pytest.fixture(autouse=True)
def no_external_io(monkeypatch):
    monkeypatch.setattr(socket.socket, "connect", lambda *a, **kw: pytest.fail("External network forbidden"))
    monkeypatch.delenv("TRAINING_READINESS_ENABLED", raising=False)


@pytest.mark.parametrize("flag", [None, "false", "TRUE", "yes", "", "0"])
def test_default_off_invalid_flags_before_all_operations(flag):
    env = {} if flag is None else {"TRAINING_READINESS_ENABLED": flag}
    result = training_sync.run(env=env, store=object(), garmin=object())
    assert result["status"] == ("disabled" if flag in {None, "false"} else "configuration_required")
    assert result["fitness_http_attempts"] == result["component_auth_http_attempts"] == 0


@pytest.mark.parametrize("zone,device", [(None, None), ("UTC+2", None), ("Europe/Oslo", "01"), ("Europe/Oslo", "42?x=1")])
def test_invalid_private_config_has_no_date_fallback(zone, device):
    env = {"TRAINING_READINESS_ENABLED": "true", "HEALTH_TIMEZONE": zone, "TRAINING_READINESS_DEVICE_ID": device}
    assert configuration(env, now=NOW)["status"] == "configuration_required"


def test_local_date_and_minimal_fields_units_zero_and_privacy():
    env = {"TRAINING_READINESS_ENABLED": "true", "HEALTH_TIMEZONE": "Europe/Oslo"}
    assert configuration(env, now=datetime(2026, 10, 7, 23, tzinfo=timezone.utc))["date"] == DAY
    day = decode_day(encode_day(make_day()), DAY)
    obs = day["devices"][0]["latest"]
    assert obs["readiness"]["value"] == 80 and obs["readiness"]["unit"] == "score_1_100"
    assert obs["recovery"]["value"] == 0 and obs["recovery"]["unit"] == "minutes"
    assert obs["observation_at"] == "2026-10-08T05:30:00+00:00"
    assert "never-export-this" not in encode_day(day).decode()


@pytest.mark.parametrize("score", [0, 101, -1, True, "80", float("nan"), float("inf")])
def test_invalid_score_does_not_replace_good_or_check_time(score):
    old = make_day()
    new = make_day([{**DTO, "score": score, "recoveryTime": 30}], "2026-10-08T08:00:00+00:00", old)
    value = new["devices"][0]["latest"]
    assert value["readiness"]["value"] == 80 and value["readiness"]["source_checked_at"] == CHECK
    assert value["readiness"]["retained_previous"] is True
    assert value["recovery"]["source_checked_at"] == "2026-10-08T08:00:00+00:00"
    assert new["source_checked_at"] == CHECK


def test_recovery_conflict_is_retained_without_countdown():
    day = make_day([{**DTO, "recoveryTime": 60, "recoveryTimeChangePhrase": "REACHED_ZERO"}])
    field = day["devices"][0]["latest"]["recovery"]
    assert field["value"] == 60 and field["status"] == "conflicting" and field["reached_zero"]
    unknown = make_day([{**DTO, "recoveryTimeChangePhrase": "unverified-code"}])
    assert unknown["devices"][0]["latest"]["recovery"]["reached_zero"] is None


def test_devices_morning_ambiguity_missing_selection_no_first_fallback():
    other = {**DTO, "deviceId": 43}
    assert make_day([DTO, other])["selection"] == {"status": "ambiguous", "device_id": None}
    day = make_day([DTO, {**DTO, "score": 85}])
    assert day["devices"][0]["morning"] is None and day["devices"][0]["morning_status"] == "ambiguous"
    assert day["devices"][0]["latest"] is None
    assert make_day([DTO, DTO])["devices"][0]["morning"] is not None
    assert make_day([DTO], config={**CONFIG, "device_id": "99"})["selection"]["status"] == "selected_device_unavailable"
    with pytest.raises(ValueError):
        make_day([{**DTO, "deviceId": n} for n in range(1, 6)])


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("status", ["auth_required", "timeout", "source_error", "schema_unverified", "oversize", "wrong_date", "partial"])
def test_configured_device_failure_is_not_absence_and_preserves_provenance(status, retained):
    config = {**CONFIG, "device_id": "42"}
    previous = make_day(config=config) if retained else None
    day = merge_day(previous, [], status, config, "2026-10-08T08:00:00+00:00")
    assert day["selection"] == {"status": "available" if retained else "unknown", "device_id": "42"}
    assert day["last_attempt"]["status"] == status
    assert decode_day(encode_day(day), DAY) == day
    if retained:
        assert day["source_checked_at"] == previous["source_checked_at"]
        assert day["source_revision"] == previous["source_revision"]
        for slot in ("morning", "latest"):
            for field in ("readiness", "recovery"):
                assert day["devices"][0][slot][field] == {**previous["devices"][0][slot][field], "retained_previous": True}
    else:
        assert not day["devices"] and day["source_checked_at"] is None


@pytest.mark.parametrize("retained", [False, True])
def test_partial_other_device_does_not_prove_selected_absence_or_fall_back(retained):
    config = {**CONFIG, "device_id": "42"}
    previous = make_day(config=config) if retained else None
    # Incomplete response, not just a hand-set status; omitted/bad rows matter.
    day = make_day([{**DTO, "deviceId": 43}, {"deviceId": "unverified"}], previous=previous, config=config)
    assert day["last_attempt"]["status"] == "partial"
    assert day["selection"] == {"status": "available" if retained else "unknown", "device_id": "42"}
    if retained:
        selected = next(device for device in day["devices"] if device["device_id"] == "42")
        assert selected["latest"]["readiness"] == {**previous["devices"][0]["latest"]["readiness"], "retained_previous": True}


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("rows", [[], [{**DTO, "deviceId": 43}]])
def test_valid_complete_response_establishes_configured_device_absence(rows, retained):
    config = {**CONFIG, "device_id": "42"}
    previous = make_day(config=config) if retained else None
    day = make_day(rows, previous=previous, config=config)
    assert day["selection"] == {"status": "selected_device_unavailable", "device_id": "42"}
    assert day["last_attempt"]["status"] in {"available", "no_observation"}


@pytest.mark.parametrize("day,local", [("2026-03-29", "2026-03-29T02:30:00"), ("2026-10-25", "2026-10-25T02:30:00")])
def test_dst_hole_and_repeat_unknown_instead_of_fixed_offset(day, local):
    value = make_day([{**DTO, "calendarDate": day, "timestampLocal": local, "timestamp": local}], config={**CONFIG, "date": day})
    assert value["devices"][0]["latest"]["observation_at"] is None


def test_travel_conflict_and_wrong_date():
    value = make_day([{**DTO, "timestampLocal": "2026-10-08T12:30:00"}])
    assert value["devices"][0]["latest"]["time_source"] == "timezone_conflict"
    assert value["devices"][0]["latest"]["observation_at"] is None
    with pytest.raises(ValueError):
        make_day([{**DTO, "calendarDate": "2026-10-07"}])


def test_utc_only_on_another_configured_day_does_not_invent_a_local_clock():
    row = {**DTO, "timestampLocal": None, "timestamp": "2026-10-07T05:30:00Z"}
    day = make_day([row])
    observation = day["devices"][0]["latest"]
    assert day["last_attempt"]["status"] == "partial"
    assert observation["observation_at"] is None and observation["observation_local"] is None
    assert observation["readiness"]["time_source"] == "timezone_conflict"
    assert observation["recovery"]["observation_at"] is None


@pytest.mark.parametrize("field", [None, "readiness", "recovery"])
def test_canonical_wrong_local_date_is_not_preserved(field):
    day = make_day()
    observation = day["devices"][0]["latest"]
    target = observation if field is None else observation[field]
    target["observation_local"] = "2026-10-07T07:30:00"
    with pytest.raises(ValueError, match="local training"):
        decode_day(encode_day(day), DAY)


@pytest.mark.parametrize("status,payload", [("available", []), ("auth_required", []), ("source_error", []), ("oversize", [])])
def test_sync_preserves_values_checks_and_budget_operations(status, payload):
    client = MemoryClient()
    original = make_day()
    client.objects[day_key(DAY)] = encode_day(original)
    store = R2Store(client=client, bucket="synthetic")
    store._bucket_budget.inventory = {"objects": 1, "bytes": len(client.objects[day_key(DAY)])}
    source, calls = provider(payload, status)
    result = training_sync.run(config=CONFIG, store=store, garmin=source, now=datetime(2026, 10, 8, 8, tzinfo=timezone.utc))
    current = decode_day(client.objects[day_key(DAY)], DAY)
    assert calls == [DAY] and result["fitness_http_attempts"] == 1 and result["component_auth_http_attempts"] == 0
    assert current["devices"][0]["latest"]["readiness"]["value"] == 80
    assert current["devices"][0]["latest"]["readiness"]["source_checked_at"] == CHECK
    assert current["devices"][0]["latest"]["readiness"]["retained_previous"] is True
    assert client.counts == {"get": 1, "head": 1, "put": 1, "list": 0}
    assert all(body.closed for body in client.bodies)


def test_oversize_r2_metadata_and_stream_close_before_parse():
    client = MemoryClient()
    client.objects["synthetic"] = b"x" * (MAX_DAY_BYTES + 10)
    store = R2Store(client=client, bucket="synthetic")
    with pytest.raises(R2ReadBoundaryError):
        store.get_bounded("synthetic", MAX_DAY_BYTES)
    assert client.consumed_bytes == 0 and client.bodies[-1].closed
    client.metadata["synthetic"] = {"ContentLength": 1}
    with pytest.raises(R2ReadBoundaryError):
        store.get_bounded("synthetic", MAX_DAY_BYTES)
    assert client.consumed_bytes == MAX_DAY_BYTES + 1 and client.bodies[-1].closed


def test_unknown_day_schema_no_provider_or_overwrite():
    client = MemoryClient()
    client.objects[day_key(DAY)] = b'{"schema_version":999}'
    store = R2Store(client=client, bucket="synthetic")
    source, calls = provider()
    result = training_sync.run(config=CONFIG, store=store, garmin=source, now=NOW)
    assert result["status"] == "schema_unverified" and not calls
    assert client.counts == {"get": 1, "head": 0, "put": 0, "list": 0}


def test_changed_day_timezone_does_not_relabel_retained_data_or_fetch():
    client = MemoryClient()
    original = encode_day(make_day())
    client.objects[day_key(DAY)] = original
    store = R2Store(client=client, bucket="synthetic")
    source, calls = provider()
    result = training_sync.run(config={**CONFIG, "timezone": "America/New_York"}, store=store, garmin=source, now=NOW)
    assert result["status"] == "configuration_required" and not calls
    assert client.objects[day_key(DAY)] == original
    assert client.counts == {"get": 1, "head": 0, "put": 0, "list": 0}


def test_policy_and_budget_propagate_optional_source_errors_do_not_retry():
    store = R2Store(client=MemoryClient(), bucket="synthetic", max_writes_per_run=1)
    source, _ = provider()
    store._writes = 1
    with pytest.raises(R2BudgetError):
        training_sync.run(config=CONFIG, store=store, garmin=source, now=NOW)
    source.get_training_readiness_bounded = lambda _: (_ for _ in ()).throw(GarminReadOnlyViolation("fixed policy error"))
    with pytest.raises(GarminReadOnlyViolation):
        training_sync.run(config=CONFIG, store=store, garmin=source, now=NOW)


def test_bounded_policy_failure_never_publishes_a_source_error_day():
    client = MemoryClient()
    original = encode_day(make_day())
    client.objects[day_key(DAY)] = original
    source, _ = provider()
    policy = GarminReadOnlyViolation("fixed policy rejection")
    attempts = []
    def rejected(*a):
        attempts.append(1)
        raise policy
    source.get_training_readiness_bounded = lambda _: bounded_read(rejected, {})
    with pytest.raises(GarminReadOnlyViolation) as raised:
        training_sync.run(config=CONFIG, store=R2Store(client=client, bucket="synthetic"), garmin=source, now=NOW)
    assert raised.value is policy and attempts == [1]
    assert client.objects[day_key(DAY)] == original
    assert client.counts == {"get": 1, "head": 0, "put": 0, "list": 0}


def test_uncertain_put_is_not_retried_and_missing_shared_auth_is_not_repaired(monkeypatch):
    client = MemoryClient()
    store = R2Store(client=client, bucket="synthetic")
    def put(**kwargs):
        client.counts["put"] += 1
        raise TimeoutError("private-object-and-token")
    monkeypatch.setattr(client, "put_object", put)
    result = training_sync.run(config=CONFIG, store=store, garmin=None, now=NOW)
    assert result["status"] == "publication_unknown" and result["fitness_http_attempts"] == 0
    assert client.counts["put"] == 1


def test_refresh_gates_and_reuses_last_budget_after_critical_work(monkeypatch, tmp_path):
    monkeypatch.setenv("TRAINING_READINESS_ENABLED", "true")
    monkeypatch.setenv("HEALTH_TIMEZONE", "Europe/Oslo")
    client = MemoryClient()
    root = R2Store(client=client, bucket="synthetic")
    stores, order = [], []
    def ordinary(name):
        def run(*a, store=None, **kw):
            if store is None and name == "restore":
                store = a[1]
            order.append(name)
            stores.append(store)
            if name == "summary":
                tmp_path.mkdir(exist_ok=True)
                (tmp_path / "health_daily.csv").write_bytes(b"Date\n")
            if name == "body":
                store.timings_ms["get"] = 5
            return SimpleNamespace(base=object()) if name == "restore" else {}
        return run
    for module, method, name in ((refresh.summary_restore, "restore_with_base", "restore"),
        (refresh.refresh_summaries, "run", "summary"), (refresh.recent_health, "run", "health"),
        (refresh.recent_body, "run", "body"), (refresh.latest_activity, "run", "activity"),
        (refresh.latest_night, "run", "night")):
        monkeypatch.setattr(module, method, ordinary(name))
    def collect(*, store, garmin, config):
        order.append("training")
        assert store is stores[-1]  # No reset of stage allowance.
        assert garmin is source
        store.timings_ms["get"] += 2
        return {}
    monkeypatch.setattr(refresh.training_sync, "run", collect)
    source = SimpleNamespace(connectapi=lambda *a, **kw: None)
    options = {"store_factory": lambda: root, "login": lambda: source, "data_dir": str(tmp_path)}
    report = refresh.run(refresh.RefreshRequest(), **options)
    assert order == ["restore", "summary", "health", "body", "training"]
    assert report["stages"][-1]["r2_get_ms"] == report["stages"][-1]["r2_read_ms"] == 2
    for mode in ("activity", "night"):
        order.clear()
        refresh.run(refresh.RefreshRequest(mode=mode, wake_date=datetime.now().date().isoformat()), **options)
        assert "training" not in order
    monkeypatch.setattr(refresh.fetch, "run", ordinary("history"))
    monkeypatch.setattr(refresh.summary_export, "publish_derived", ordinary("export"))
    order.clear()
    refresh.run(refresh.RefreshRequest(health_start="2026-10-01", health_end="2026-10-02"), **options)
    assert "training" not in order
    order.clear()
    refresh.run(refresh.RefreshRequest(mode="activity", repair_only=True), **options)
    assert "training" not in order
    order.clear()
    monkeypatch.setenv("TRAINING_READINESS_ENABLED", "false")
    refresh.run(refresh.RefreshRequest(), **options)
    assert "training" not in order
