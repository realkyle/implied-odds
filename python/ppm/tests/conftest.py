from __future__ import annotations

from pathlib import Path

import fakeredis
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def anyio_backend() -> str:
    # Run async tests on asyncio (anyio's pytest plugin ships with httpx).
    return "asyncio"


@pytest.fixture
def redis():
    """In-memory Redis: same commands as the real server, no Docker needed."""
    return fakeredis.FakeAsyncRedis()


def fixture_bytes(*parts: str) -> bytes:
    return FIXTURES.joinpath(*parts).read_bytes()


def make_envelope(body: bytes = b'{"x":1}', **overrides):
    from ppm.pipeline.envelope import Envelope

    base = dict(
        venue="deribit",
        endpoint="book_summary",
        params={"currency": "BTC", "kind": "option"},
        snapshot_ts=1_794_330_000_000,
        request_ts=1_794_330_000_010,
        recv_ts=1_794_330_000_250,
        status="200",
        body=body,
    )
    return Envelope(**{**base, **overrides})


@pytest.fixture
def make():
    """Factory fixture: `make(body, **field_overrides)` builds a test Envelope."""
    return make_envelope
