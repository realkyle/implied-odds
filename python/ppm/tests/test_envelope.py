from __future__ import annotations

import pytest
from ppm.pipeline.clock import floor_tick, next_tick
from ppm.pipeline.envelope import GZIP_THRESHOLD_BYTES, Envelope


@pytest.mark.parametrize("body", [b'{"x":1}', b"x" * (GZIP_THRESHOLD_BYTES + 1)])
def test_redis_fields_round_trip(make, body):
    env = make(body)
    fields = {k.encode(): v for k, v in env.to_fields().items()}  # Redis returns bytes keys
    assert Envelope.from_fields(fields) == env


def test_large_bodies_are_compressed_on_the_wire(make):
    fields = make(b"a" * (GZIP_THRESHOLD_BYTES + 1)).to_fields()
    assert fields["enc"] == b"gzip"
    assert len(fields["body"]) < GZIP_THRESHOLD_BYTES


@pytest.mark.parametrize("body", [b"", b"line1\nline2", b"y" * (GZIP_THRESHOLD_BYTES * 2)])
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
