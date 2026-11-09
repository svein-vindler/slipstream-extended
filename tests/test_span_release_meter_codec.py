"""A new identity requires a semantically qualified release frame."""
import json

import pytest
from test_rolling_span_meter_proof import codec_fixture

from pipeline.history_state import Stop, digest, encoded
from pipeline.rolling_reference_span import _meter_link
from pipeline.rolling_span_meter_proof import _reader_result


@pytest.mark.parametrize("fault", ["bare_identity", "opaque_transition"])
def test_span_release_codec_rejects_unqualified_identity_changes(tmp_path, fault):
    meter, path, append, counters = codec_fixture(tmp_path / "proofs")
    if fault == "bare_identity":
        rows = path.read_bytes().splitlines(keepends=True)
        frame = json.loads(rows[-1])
        frame["identity"] = "f" * 64
        rows[-1] = encoded(frame)
        path.write_bytes(b"".join(rows))
    else:
        evidence = tmp_path / "opaque.json"
        evidence.write_bytes(encoded({"syntheticUnqualifiedAuthority": True}))
        reference = {"path": str(evidence.resolve()), "bytes": evidence.stat().st_size,
                     "sha256": digest(evidence.read_bytes())}
        append(path, "span_release_transition", 264, transition_ref=reference,
               admission_ref=reference, round=None, baseline=173, initial=0, **counters)
    before = path.read_bytes()
    with pytest.raises(Stop):
        _meter_link(meter())
    assert path.read_bytes() == before


@pytest.mark.parametrize("format", ["json", "sse"])
def test_matched_reader_accepts_equivalent_json_numbers(format):
    envelope = {"jsonrpc": "2.0", "id": 1, "result": {"structuredContent": {"quantile": 3},
        "content": [{"type": "text", "text": '{"quantile":3.0}'}]}}
    body = json.dumps(envelope).encode()
    if format == "sse":
        body = b"event: message\ndata: " + body + b"\n\n"
    assert _reader_result(body, {"response_format": format})["quantile"] == 3


@pytest.mark.parametrize("number,text", [(3, '{"quantile":3.1}'), (1, '{"quantile":true}')])
def test_reader_rejects_different_number_or_boolean(number, text):
    envelope = {"structuredContent": {"quantile": number}, "content": [{"type": "text", "text": text}]}
    with pytest.raises(Stop):
        _reader_result(json.dumps(envelope).encode(), {"response_format": "json"})
