"""The queue message: one raw venue response plus the metadata needed to replay it.

Recorders never parse venue data (design decision A1). They wrap the response bytes
in an Envelope and publish it. The writer parses later, and the raw bytes are archived
so a parsing bug can be fixed by replaying them.
"""

from __future__ import annotations

import gzip
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

ENVELOPE_VERSION = 1
# Every non-empty body is gzip-compressed on the wire and on disk. Until 2026-11-11 only
# bodies over 64 KB were, which left ~15 KB Polymarket books uncompressed (~460 MB/day);
# measured ratios: Polymarket books 4.7x, Deribit chains 10.6x, Kalshi 43x, and even
# 200-byte tickers 1.2x. Older files say `identity` in their header and still decode.
STATUS_ERROR = "error"
_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9_.]+")


@dataclass(frozen=True, slots=True)
class Envelope:
    """One fetch from one venue endpoint at one scheduled tick.

    Timestamps are integer milliseconds since the Unix epoch, UTC.
    """

    venue: str
    endpoint: str
    snapshot_ts: int  # scheduled tick: the cross-venue alignment key
    request_ts: int  # local clock when the request was sent
    recv_ts: int  # local clock when the response (or final error) arrived
    status: str  # HTTP status code as text ("200", "429", ...) or "error"
    body: bytes  # raw response body, or the error message when status is not 200
    params: Mapping[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == "200"

    @property
    def key(self) -> str:
        """Deterministic, filename-safe identity of *what* was fetched.

        The same endpoint and params give the same key, so (snapshot_ts, venue, key)
        names a fetch uniquely. The writer derives file names from it, which makes
        reprocessing a redelivered message overwrite rather than duplicate.
        """
        parts = [self.endpoint, *(self.params[k] for k in sorted(self.params))]
        return "-".join(_UNSAFE_CHARS.sub("_", p) for p in parts)

    # Redis stream fields ---------------------------------------------------------

    def to_fields(self) -> dict[str, bytes]:
        """Encode as Redis stream fields (Redis stores flat byte-string maps)."""
        body, enc = _compress(self.body)
        header = {k: str(v).encode() for k, v in self._header().items()}
        return {**header, "enc": enc.encode(), "body": body}

    @classmethod
    def from_fields(cls, fields: Mapping[bytes | str, bytes | str]) -> Envelope:
        f = {_text(k): v for k, v in fields.items()}
        version = int(_text(f["v"]))
        if version != ENVELOPE_VERSION:
            raise ValueError(f"unsupported envelope version {version}")
        body = f["body"] if isinstance(f["body"], bytes) else f["body"].encode()
        return cls(
            venue=_text(f["venue"]),
            endpoint=_text(f["endpoint"]),
            params=json.loads(_text(f["params"])),
            snapshot_ts=int(_text(f["snapshot_ts"])),
            request_ts=int(_text(f["request_ts"])),
            recv_ts=int(_text(f["recv_ts"])),
            status=_text(f["status"]),
            body=_decompress(body, _text(f["enc"])),
        )

    # Single-blob form, used for the disk spool and the raw archive ---------------

    def encode(self) -> bytes:
        """One JSON header line, a newline, then the (possibly gzipped) body."""
        body, enc = _compress(self.body)
        header = {**self._header(), "enc": enc}
        return json.dumps(header, separators=(",", ":")).encode() + b"\n" + body

    @classmethod
    def decode(cls, blob: bytes) -> Envelope:
        header_line, _, body = blob.partition(b"\n")
        h = json.loads(header_line)
        if int(h["v"]) != ENVELOPE_VERSION:
            raise ValueError(f"unsupported envelope version {h['v']}")
        return cls(
            venue=h["venue"],
            endpoint=h["endpoint"],
            params=json.loads(h["params"]),
            snapshot_ts=int(h["snapshot_ts"]),
            request_ts=int(h["request_ts"]),
            recv_ts=int(h["recv_ts"]),
            status=h["status"],
            body=_decompress(body, h["enc"]),
        )

    def _header(self) -> dict[str, str | int]:
        return {
            "v": ENVELOPE_VERSION,
            "venue": self.venue,
            "endpoint": self.endpoint,
            "params": json.dumps(dict(self.params), sort_keys=True),
            "snapshot_ts": self.snapshot_ts,
            "request_ts": self.request_ts,
            "recv_ts": self.recv_ts,
            "status": self.status,
        }


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _compress(body: bytes) -> tuple[bytes, str]:
    if body:
        return gzip.compress(body, compresslevel=6), "gzip"
    return body, "identity"


def _decompress(body: bytes, enc: str) -> bytes:
    if enc == "gzip":
        return gzip.decompress(body)
    if enc == "identity":
        return body
    raise ValueError(f"unknown body encoding {enc!r}")
