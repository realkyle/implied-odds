"""Settlement collector tests against real settled/resolved responses (2026-11-11)."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, date, datetime

import httpx
import pytest
from ppm.pipeline.bus import DiskSpool, RedisBus, stream_name
from ppm.pipeline.envelope import Envelope
from ppm.recorders.settlements import HOUR_MS, SettlementRecorder, noon_et_ms
from ppm.tests.conftest import fixture_bytes, make_envelope
from ppm.writer.parsers import parse
from ppm.writer.schemas import validate_rows

KALSHI = fixture_bytes("settlements", "kalshi_settled_KXBTCD.json")
PM = fixture_bytes("settlements", "polymarket_resolved.json")
CANDLE = fixture_bytes("settlements", "binance_noon_BTCUSDT_2026-11-10.json")


def ms(*args) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


def env(endpoint: str, body: bytes, **params: str) -> Envelope:
    return make_envelope(body, venue="settlements", endpoint=endpoint, params=params)


def test_noon_et_follows_daylight_saving():
    assert noon_et_ms(date(2026, 11, 10)) == ms(2026, 11, 10, 17)  # EST, UTC-5
    assert noon_et_ms(date(2026, 7, 10)) == ms(2026, 7, 10, 16)  # EDT, UTC-4


# --- parsers ---------------------------------------------------------------------


def test_kalshi_rows_carry_result_and_the_brti_settlement_value():
    raw = json.loads(KALSHI)["markets"]
    rows = parse(env("kalshi_settled", KALSHI, series="KXBTCD", page="00")).tables["settlements"]
    assert len(rows) == len(raw)
    table, rejected = validate_rows("settlements", rows)
    assert rejected == [] and table.num_rows == len(raw)
    for row, m in zip(rows, raw, strict=True):
        assert row["contract_id"] == m["ticker"] and row["venue"] == "kalshi"
        assert row["settlement_value"] == float(m["expiration_value"])
        assert row["settle_ts"] < row["exchange_ts"]  # settled a few minutes after close


def test_kalshi_results_agree_with_strike_and_settlement_value():
    """Invariant: an "above" contract is YES exactly when the BRTI average > strike."""
    markets = {m["ticker"]: m for m in json.loads(KALSHI)["markets"]}
    rows = parse(env("kalshi_settled", KALSHI, series="KXBTCD", page="00")).tables["settlements"]
    assert {r["result"] for r in rows} == {"yes", "no"}  # fixture has both
    for row in rows:
        strike = markets[row["contract_id"]]["floor_strike"]
        assert (row["result"] == "yes") == (row["settlement_value"] > strike)


def test_unsettled_kalshi_markets_are_skipped_not_guessed():
    doc = json.loads(KALSHI)
    doc["markets"][0].update(status="closed", result="")
    rows = parse(env("kalshi_settled", json.dumps(doc).encode(), series="KXBTCD", page="00"))
    assert len(rows.tables["settlements"]) == len(doc["markets"]) - 1


def test_binance_noon_candle_row():
    rows = parse(env("binance_settle_candle", CANDLE, symbol="BTCUSDT", date="2026-11-10")).tables[
        "settlements"
    ]
    (row,) = rows
    assert row["settle_ts"] == noon_et_ms(date(2026, 11, 10))
    assert row["exchange_ts"] == row["settle_ts"] - 1  # candle closes 1 ms before noon
    assert row["settlement_value"] == 72880.64 and row["contract_id"] == "BTCUSDT@2026-11-10"


def test_binance_returning_a_different_minute_is_rejected():
    # Binance answers with the next candle that exists if the requested one is missing.
    result = parse(env("binance_settle_candle", CANDLE, symbol="BTCUSDT", date="2026-11-09"))
    assert "settlements" not in result.tables
    assert "no candle opening at" in result.rejected["settlements"][0].reason


def pays_yes(kind: str, low: float | None, high: float | None, close: float) -> bool:
    """The bracket rules as documented in the polymarket_contracts schema."""
    if kind == "above":
        return close > low
    if kind == "below":
        return close < high
    return low <= close < high


def test_polymarket_outcomes_match_the_binance_noon_close():
    """Cross-venue check of our bracket rules: every resolved contract's YES/NO must
    follow from the Binance noon close (72,880.64 on 2026-11-10) and its strikes."""
    close = 72880.64
    resolved = parse(env("polymarket_resolved", PM)).tables["settlements"]
    contracts = {
        c["yes_token_id"]: c
        for c in parse(make_envelope(PM, venue="polymarket", endpoint="events", params={})).tables[
            "polymarket_contracts"
        ]
    }
    assert len(resolved) == 22
    for row in resolved:
        c = contracts[row["contract_id"]]
        expected = pays_yes(c["contract_type"], c["strike_low"], c["strike_high"], close)
        assert (row["result"] == "yes") == expected, (c["question"], row["result"])
    assert sum(r["result"] == "yes" for r in resolved if "price-on" in r["event_id"]) == 1
    table, rejected = validate_rows("settlements", resolved)
    assert rejected == [] and table.num_rows == 22


def test_unresolved_polymarket_contracts_are_skipped():
    events = json.loads(PM)
    events[0]["markets"][0]["outcomePrices"] = '["0.97", "0.03"]'
    rows = parse(env("polymarket_resolved", json.dumps(events).encode())).tables["settlements"]
    assert len(rows) == 21


# --- recorder --------------------------------------------------------------------

TICK = ms(2026, 11, 11, 18)  # 13:00 ET: the Nov 11 noon candle has closed


def make_recorder(redis, tmp_path, handler, **kw) -> SettlementRecorder:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**8), 10_000)
    return SettlementRecorder(client, bus, retries=0, page_interval_s=0, **kw)


async def published(redis) -> list[Envelope]:
    return [Envelope.from_fields(f) for _, f in await redis.xrange(stream_name("settlements"))]


@pytest.mark.anyio
async def test_kalshi_pages_follow_the_cursor_in_order(redis, tmp_path):
    seen: list[tuple[str, str | None, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        q = request.url.params
        if request.url.host == "api.elections.kalshi.com":
            cursor = q.get("cursor")
            seen.append((q["series_ticker"], cursor, q["min_close_ts"]))
            nxt = {None: "c1", "c1": "c2"}.get(cursor, "")
            return httpx.Response(200, json={"cursor": nxt, "markets": []})
        return httpx.Response(200, content=b"[]")

    await make_recorder(redis, tmp_path, handler).run_tick(TICK)
    btcd = [s for s in seen if s[0] == "KXBTCD"]
    assert [c for _, c, _ in btcd] == [None, "c1", "c2"]
    assert {m for _, _, m in seen} == {str((TICK - 48 * HOUR_MS) // 1000)}  # 48 h window
    keys = [e.key for e in await published(redis) if e.endpoint == "kalshi_settled"]
    # params sort by name (page, series); each page is its own message, in order
    assert [k for k in keys if k.endswith("-KXBTCD")] == [
        "kalshi_settled-00-KXBTCD",
        "kalshi_settled-01-KXBTCD",
        "kalshi_settled-02-KXBTCD",
    ]
    assert len(keys) == 4 * 3


@pytest.mark.anyio
async def test_a_failed_page_stops_that_series_and_is_published(redis, tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("cursor") == "c1":
            return httpx.Response(500, text="boom")
        if request.url.host == "api.elections.kalshi.com":
            return httpx.Response(200, json={"cursor": "c1", "markets": []})
        return httpx.Response(200, content=b"[]")

    await make_recorder(redis, tmp_path, handler).run_tick(TICK)
    kalshi = [e for e in await published(redis) if e.endpoint == "kalshi_settled"]
    assert sorted(e.status for e in kalshi) == ["200"] * 4 + ["500"] * 4


@pytest.mark.anyio
async def test_binance_candles_only_for_noons_that_have_passed(redis, tmp_path):
    rec = make_recorder(redis, tmp_path, lambda r: httpx.Response(200))
    specs = await rec.fetch_specs(ms(2026, 11, 11, 16))  # 11:00 ET, before noon
    dates = sorted({s.params["date"] for s in specs if s.endpoint == "binance_settle_candle"})
    assert dates == ["2026-11-09", "2026-11-10"]
    (gamma,) = [s for s in specs if s.endpoint == "polymarket_resolved"]
    assert gamma.query["slug"][0] == "bitcoin-above-on-november-9-2026"


@pytest.mark.anyio
async def test_collects_at_start_up_then_hourly(redis, tmp_path):
    stop = asyncio.Event()
    ticks: list[int] = []

    class Probe(SettlementRecorder):
        async def run_tick(self, snapshot_ts: int):
            ticks.append(snapshot_ts)
            stop.set()
            return []

    now = TICK + 25 * 60_000
    client = httpx.AsyncClient()
    rec = Probe(client, RedisBus(redis, DiskSpool(tmp_path, 10**8), 10), clock=lambda: now)
    await asyncio.wait_for(rec.run_forever(stop), timeout=5)
    assert ticks == [TICK]  # ran immediately, labelled with the current hour


@pytest.mark.anyio
async def test_kalshi_series_are_paged_one_at_a_time(redis, tmp_path):
    """Parallel paging tripped Kalshi's rate limit (HTTP 429) on 2026-11-11."""
    in_flight = peak = 0
    lock = asyncio.Lock()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        if request.url.host != "api.elections.kalshi.com":
            return httpx.Response(200, content=b"[]")
        async with lock:
            in_flight += 1
            peak = max(peak, in_flight)
        await asyncio.sleep(0.01)
        async with lock:
            in_flight -= 1
        nxt = "" if request.url.params.get("cursor") else "c1"
        return httpx.Response(200, json={"cursor": nxt, "markets": []})

    await make_recorder(redis, tmp_path, handler).run_tick(TICK)
    assert peak == 1
