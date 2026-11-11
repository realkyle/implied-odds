from __future__ import annotations

import httpx
import pytest
from ppm.pipeline.bus import DiskSpool, PublishResult, RedisBus, stream_name
from ppm.pipeline.envelope import Envelope
from ppm.recorders.deribit import DeribitRecorder
from ppm.tests.conftest import fixture_bytes

pytestmark = pytest.mark.anyio
TICK = 1_794_330_000_000


def recorder(redis, tmp_path, handler, **kw) -> DeribitRecorder:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**8), 1000)
    times = iter(range(TICK + 5, TICK + 10**6, 7))  # deterministic, increasing clock
    return DeribitRecorder(client, bus, clock=lambda: next(times), retry_base_s=0, **kw)


async def published(redis) -> list[Envelope]:
    return [Envelope.from_fields(f) for _, f in await redis.xrange(stream_name("deribit"))]


async def test_one_tick_publishes_every_endpoint_with_the_same_snapshot_ts(redis, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=fixture_bytes("deribit", "index_price_btc_usd.json"))

    results = await recorder(redis, tmp_path, handler).run_tick(TICK)
    assert results == [PublishResult.PUBLISHED] * 6  # 2 currencies x (option, future, index)
    envs = await published(redis)
    assert {e.snapshot_ts for e in envs} == {TICK}
    assert {e.key for e in envs} == {
        "book_summary-BTC-option",
        "book_summary-BTC-future",
        "index_price-btc_usd",
        "book_summary-ETH-option",
        "book_summary-ETH-future",
        "index_price-eth_usd",
    }
    assert all(e.recv_ts > e.request_ts for e in envs)


async def test_retries_then_publishes_the_failure(redis, tmp_path):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectTimeout("timed out", request=request)

    await recorder(redis, tmp_path, handler, retries=2).run_tick(TICK)
    assert calls == 6 * 3  # every endpoint tried 1 + 2 retries
    envs = await published(redis)
    assert all(e.status == "error" and b"ConnectTimeout" in e.body for e in envs)


async def test_http_error_status_is_kept(redis, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, text="rate limited")

    await recorder(redis, tmp_path, handler, retries=0).run_tick(TICK)
    assert {e.status for e in await published(redis)} == {"429"}


async def test_transient_failure_recovers_on_retry(redis, tmp_path):
    seen: set[str] = set()

    def handler(request: httpx.Request) -> httpx.Response:
        key = str(request.url)
        if key not in seen:
            seen.add(key)
            return httpx.Response(502, text="bad gateway")
        return httpx.Response(200, content=b'{"result": {}}')

    await recorder(redis, tmp_path, handler, retries=1).run_tick(TICK)
    assert {e.status for e in await published(redis)} == {"200"}


async def test_late_wakeup_skips_the_tick_instead_of_mislabelling_data(redis, tmp_path):
    """Laptop slept through a tick: we must not stamp later prices with the old time."""
    import asyncio

    stop = asyncio.Event()
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"{}")

    # The clock jumps 11 minutes past the tick (like the 2026-11-10 sleep), then the test stops.
    times = iter([TICK - 1_000, TICK, TICK + 11 * 60_000])

    def clock() -> int:
        t = next(times, None)
        if t is None:
            stop.set()
            return TICK + 11 * 60_000
        return t

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    rec = DeribitRecorder(client, RedisBus(redis, DiskSpool(tmp_path, 10**8), 1000), clock=clock)
    await asyncio.wait_for(rec.run_forever(stop), timeout=5)
    assert calls == 0 and await published(redis) == []


async def test_kalshi_and_reference_fetch_specs():
    from ppm.recorders.kalshi import KalshiRecorder
    from ppm.recorders.reference import ReferenceRecorder

    keys = {}
    for cls in (KalshiRecorder, ReferenceRecorder):
        specs = await cls(client=None, bus=None).fetch_specs(TICK)  # type: ignore[arg-type]
        keys[cls.venue] = sorted(
            make_key(s.endpoint, s.params)
            for s in specs  # same rule the envelope uses
        )
    assert keys["kalshi"] == [
        "markets-KXBTC",
        "markets-KXBTCD",
        "markets-KXETH",
        "markets-KXETHD",
    ]
    assert keys["reference"] == [
        "binance_klines-BTCUSDT",
        "binance_klines-ETHUSDT",
        "coinbase_ticker-BTC_USD",
        "coinbase_ticker-ETH_USD",
    ]


def make_key(endpoint: str, params: dict[str, str]) -> str:
    env = Envelope("v", endpoint, 0, 0, 0, "200", b"", dict(params))
    return env.key
