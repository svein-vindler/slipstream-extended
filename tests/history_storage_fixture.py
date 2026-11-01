import gzip
import hashlib
import io
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape

from botocore.awsrequest import AWSResponse

BASE = Path(__file__).resolve().parents[1]
ACCOUNT = "a" * 32
BUCKET = "synthetic-data"
HOST = ACCOUNT + ".r2.cloudflarestorage.com"


class Raw:
    def __init__(self, data, fail_close=False):
        self.data = io.BytesIO(data)
        self.observed = 0
        self.fail_close = fail_close

    def read(self, amt=None, **kwargs):
        value = self.data.read(amt)
        self.observed += len(value)
        return value

    def stream(self, amt=1024, **kwargs):
        while value := self.read(amt, **kwargs):
            yield value

    def close(self):
        if self.fail_close:
            raise OSError("SYNTHETIC_BODY_CLOSE")
        self.data.close()


def wire_payload(request):
    raw = request.body if isinstance(request.body, bytes) else request.body.read()
    headers = {
        str(k).lower(): v.decode() if isinstance(v, bytes) else v
        for k, v in request.headers.items()
    }
    if "aws-chunked" not in headers.get("content-encoding", ""):
        return raw
    output = bytearray()
    pos = 0
    while True:
        end = raw.index(b"\r\n", pos)
        count = int(raw[pos:end].split(b";")[0], 16)
        pos = end + 2
        if not count:
            return bytes(output)
        output.extend(raw[pos : pos + count])
        pos += count + 2


class Server:
    def __init__(self, mode="normal"):
        self.mode = mode
        self.objects = {}
        self.calls = []
        self.puts = []
        self.responses = []
        self.flags = []
        self.metadata = {}

    def send(self, request):
        assert request.stream_output is True
        self.flags.append((request, request.stream_output))
        u = urlsplit(request.url)
        query = parse_qs(u.query)
        key = unquote(u.path).removeprefix("/" + BUCKET + "/")
        self.calls.append((request.method, key, query))
        headers = {}
        status = 200
        body = b""
        if request.method == "GET" and query.get("list-type") == ["2"]:
            prefix = query.get("prefix", [""])[0]
            keys = [k for k in self.objects if k.startswith(prefix)]
            rows = "".join(
                "<Contents><Key>"
                + escape(k)
                + "</Key><Size>"
                + str(len(self.objects[k][0]))
                + "</Size><ETag>"
                + escape(self.objects[k][1])
                + "</ETag></Contents>"
                for k in keys
            )
            body = (
                "<ListBucketResult><Name>"
                + BUCKET
                + "</Name><IsTruncated>false</IsTruncated><KeyCount>"
                + str(len(keys))
                + "</KeyCount>"
                + rows
                + "</ListBucketResult>"
            ).encode()
            if self.mode == "truncated_list":
                body = b"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"
            if self.mode == "large_list":
                body = b"<ListBucketResult>" + b"x" * 2000000 + b"</ListBucketResult>"
        elif request.method == "HEAD":
            if key not in self.objects:
                status = 404
            else:
                headers = {
                    **self.metadata.get(
                        key, {"content-type": "application/json", "content-encoding": "gzip"}
                    ),
                    "etag": self.objects[key][1],
                    "content-length": str(len(self.objects[key][0])),
                }
        elif request.method == "GET":
            if key not in self.objects:
                status = 404
                body = b"<Error><Code>NoSuchKey</Code></Error>"
            else:
                body, etag = self.objects[key]
                headers = {
                    "etag": etag,
                    **self.metadata.get(
                        key, {"content-type": "application/json", "content-encoding": "gzip"}
                    ),
                }
                if self.mode == "large_get":
                    body = b"x" * 800000
                if self.mode == "decoded_get":
                    body = gzip.compress(b"x" * 2000000, mtime=0)
                if self.mode == "bad_gzip_get":
                    body = gzip.compress(b"synthetic", mtime=0)[:-8] + b"bad-crc!"
        elif request.method == "PUT":
            payload = wire_payload(request)
            self.puts.append(key)
            h = {
                str(k).lower(): v.decode() if isinstance(v, bytes) else v
                for k, v in request.headers.items()
            }
            conflict = ("if-none-match" in h and key in self.objects) or (
                "if-match" in h
                and (key not in self.objects or self.objects[key][1] != h["if-match"])
            )
            if self.mode == "conflict" or conflict:
                status = 412
                body = b"<Error><Code>PreconditionFailed</Code></Error>"
            else:
                etag = '"' + hashlib.md5(payload, usedforsecurity=False).hexdigest() + '"'
                self.objects[key] = (payload, etag)
                self.metadata[key] = {
                    k: v for k, v in h.items() if k in {"content-type", "content-encoding"}
                }
                headers["etag"] = etag
                if self.mode in {"500", "503", "large_error"}:
                    status = 500 if self.mode == "large_error" else int(self.mode)
                    body = (
                        b"<Error><Code>PreconditionFailed</Code><Message>"
                        + (b"x" * 200000 if self.mode == "large_error" else b"synthetic")
                        + b"</Message></Error>"
                    )
                if self.mode == "lost_ack":
                    raise TimeoutError("SYNTHETIC_LOST_ACK")
                if self.mode == "missing_ack":
                    headers = {}
        else:
            raise AssertionError("Unexpected synthetic request")
        headers.setdefault("content-length", str(len(body)))
        raw = Raw(
            body,
            self.mode == "body_close" and request.method == "GET" and key in self.objects,
        )
        self.responses.append((status, raw))
        return AWSResponse(request.url, status, headers, raw)
