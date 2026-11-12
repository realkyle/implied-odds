from __future__ import annotations

import gzip
import json

import pytest
from ppm.pipeline.clock import floor_tick, next_tick
from ppm.pipeline.envelope import Envelope

BIG = 200_000


@pytest.mark.parametrize("body", [b'{"x":1}', b"x" * BIG])
def test_redis_fields_round_trip(make, body):
    env = make(body)
    fields = {k.encode(): v for k, v in env.to_fields().items()}  # Redis returns bytes keys
    assert Envelope.from_fields(fields) == env


@pytest.mark.parametrize("size", [20, 15_000, BIG])
def test_every_non_empty_body_is_compressed_on_the_wire(make, size):
    fields = make(b'{"price":"0.42"}' * (size // 16)).to_fields()
    assert fields["enc"] == b"gzip"


def test_empty_body_is_stored_as_is(make):
    assert make(b"").to_fields()["enc"] == b"identity"


def test_files_written_before_the_change_still_decode(make):
    """Raw files from before 2026-11-11 hold small bodies uncompressed (`identity`)."""
    env = make(b'{"x":1}')
    header = {**json.loads(env.encode().split(b"\n", 1)[0]), "enc": "identity"}
    old = json.dumps(header).encode() + b"\n" + env.body
    assert Envelope.decode(old) == env
    assert gzip.decompress(env.encode().split(b"\n", 1)[1]) == env.body


def test_compression_does_not_depend_on_the_wall_clock(make, monkeypatch):
    """gzip writes a timestamp into its header unless mtime is fixed; the same message
    encoded in two different seconds must still give identical bytes (merge determinism)."""
    import time

    env = make(b'{"price":"0.42"}' * 100)
    monkeypatch.setattr(time, "time", lambda: 1_000_000_000.0)
    first = (env.encode(), env.to_fields())
    monkeypatch.setattr(time, "time", lambda: 2_000_000_000.0)
    assert (env.encode(), env.to_fields()) == first


@pytest.mark.parametrize("body", [b"", b"line1\nline2", b"y" * BIG])
def test_blob_round_trip(make, body):
    env = make(body)
    assert Envelope.decode(env.encode()) == env


def test_key_is_deterministic_and_filename_safe(make):
    a = make(params={"kind": "option", "currency": "BTC"})
    b = make(params={"currency": "BTC", "kind": "option"})
    assert a.key == b.key == "book_summary-BTC-option"  # param order doesn't matter
    assert make(params={"x": "a/b c"}).key == "book_summary-a_b_c"


def test_rejects_unknown_version(make):
    fields = make().to_fields()
    fields["v"] = b"99"
    with pytest.raises(ValueError, match="version"):
        Envelope.from_fields(fields)


def test_ticks_align_to_the_minute():
    t = 1_794_330_012_345  # 12.345 s past a minute
    assert floor_tick(t) == 1_794_330_000_000
    assert next_tick(t) == 1_794_330_060_000
    assert next_tick(floor_tick(t)) == 1_794_330_060_000  # exactly on a tick → the next one
