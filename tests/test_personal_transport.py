"""Synthetic transport failures stay bounded, private and terminal for the job."""

import io
import zipfile

import pytest
from test_garmin_personal import HTTP, Response, authenticated


def client_for(response, path="/hrv-service/hrv/2026-10-25"):
    from pipeline.sources.garmin_personal import PersonalGarmin

    http = authenticated(("GET", path, response))
    return PersonalGarmin("synthetic-key", expected_account="synthetic-account", session=http), http


@pytest.mark.parametrize("kind", ["encoding", "length", "close", "oversize", "mime"])
def test_transport_rejects_ambiguous_or_unbounded_response_once_with_fixed_errors(kind):
    from pipeline.sources.garmin_personal import MAX_JSON_BYTES, PersonalSourceError

    response = Response({"hrvSummary": {"calendarDate": "2026-10-25"}})
    if kind == "encoding":
        response.headers["Content-Encoding"] = "gzip"
    if kind == "length":
        response.headers["Content-Length"] = "1"
    if kind == "close":

        def fail():
            raise RuntimeError("sensitive-close-detail")

        response.close = fail
    if kind == "oversize":
        response.headers["Content-Length"] = str(MAX_JSON_BYTES + 1)
    if kind == "mime":
        response.headers["Content-Type"] = "text/html"
    client, http = client_for(response)
    for _ in range(2):
        with pytest.raises(PersonalSourceError) as failure:
            client.get_hrv_data("2026-10-25")
        assert "sensitive" not in str(failure.value)
    assert len(http.sent) == 3


@pytest.mark.parametrize("key", ["synthetic-é", "synthetic\nkey"])
def test_invalid_header_key_fails_without_http(key):
    from pipeline.sources.garmin_personal import PersonalGarmin

    with pytest.raises(ValueError):
        PersonalGarmin(key, expected_account="synthetic-account", session=HTTP([]))


def test_binary_response_requires_documented_archive_media_type():
    from pipeline.sources.garmin_personal import PersonalSourceError

    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("track.gpx", b"<gpx/>")
    client, _ = client_for(
        Response(stream.getvalue(), content_type="text/html"),
        "/download-service/files/activity/123",
    )
    with pytest.raises(PersonalSourceError):
        client.download_activity(123)


def test_fake_fit_inside_valid_zip_is_rejected():
    from pipeline.sources.garmin_personal import PersonalSourceError

    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("track.fit", b"not-a-fit-file")
    client, _ = client_for(
        Response(stream.getvalue(), content_type="application/zip"),
        "/download-service/files/activity/123",
    )
    with pytest.raises(PersonalSourceError):
        client.download_activity(123)


def test_no_content_daily_summary_range_is_empty():
    client, http = client_for(
        Response(b"", status=204), "/sleep-service/sleep/dailySleepSummariesByDate"
    )
    assert client.get_sleep_daily("2026-10-25", "2026-10-25") == []
    assert not http.rows
