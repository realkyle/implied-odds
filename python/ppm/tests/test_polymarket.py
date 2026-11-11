"""Polymarket recorder and parser tests against real Gamma and CLOB responses (2026-11-11)."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

import httpx
import pytest
from ppm.pipeline.bus import DiskSpool, RedisBus, stream_name
from ppm.pipeline.envelope import Envelope
from ppm.recorders.polymarket import (
    DISCOVERY_INTERVAL_MS,
    PolymarketRecorder,
    event_slugs,
    ladders_from_events,
)
from ppm.tests.conftest import fixture_bytes, make_envelope
from ppm.writer.parsers import parse
from ppm.writer.schemas import validate_rows

EVENTS = fixture_bytes("polymarket", "events.json")
BOOKS = fixture_bytes("polymarket", "books_bitcoin-price.json")
PRICE_SLUG = "bitcoin-price-on-november-12-2026"


def ms(*args) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


# --- discovery -------------------------------------------------------------------


def test_event_slugs_cover_both_assets_and_ladder_kinds_for_each_day():
    slugs = event_slugs(date(2026, 11, 30), days=2)
    assert slugs == [
        "bitcoin-above-on-november-30-2026",
        "bitcoin-price-on-november-30-2026",
        "ethereum-above-on-november-30-2026",
        "ethereum-price-on-november-30-2026",
        "bitcoin-above-on-december-1-2026",  # no leading zero, month rolls over
        "bitcoin-price-on-december-1-2026",
        "ethereum-above-on-december-1-2026",
        "ethereum-price-on-december-1-2026",
    ]


def test_discovery_uses_the_new_york_date_not_utc():
    rec = PolymarketRecorder(client=None, bus=None)  # type: ignore[arg-type]
    spec = rec.discovery_spec(ms(2026, 11, 12, 3))  # 03:00 UTC = 22:00 ET on Nov 11
    assert spec.query["slug"][0] == "bitcoin-above-on-november-11-2026"


def test_ladders_hold_the_yes_token_of_every_open_contract():
    ladders = {lad.slug: lad for lad in ladders_from_events(EVENTS)}
    assert set(ladders) == {
        "bitcoin-above-on-november-12-2026",
        PRICE_SLUG,
        "ethereum-price-on-november-12-2026",
    }
    raw = {e["slug"]: e for e in json.loads(EVENTS)}
    first = raw[PRICE_SLUG]["markets"][0]
    assert ladders[PRICE_SLUG].token_ids[0] == json.loads(first["clobTokenIds"])[0]
    assert all(len(lad.token_ids) == 11 for lad in ladders.values())
    assert ladders[PRICE_SLUG].end_ts == ms(2026, 11, 12, 17)  # noon ET


def test_closed_contracts_are_not_fetched():
    events = json.loads(EVENTS)
    for m in events[0]["markets"]:
        m["closed"] = True
    ladders = ladders_from_events(json.dumps(events).encode())
    assert events[0]["slug"] not in {lad.slug for lad in ladders}


# --- parsers ---------------------------------------------------------------------


def contracts():
    env = make_envelope(EVENTS, venue="polymarket", endpoint="events", params={})
    return parse(env)


def test_every_contract_becomes_a_valid_row():
    result = contracts()
    rows = result.tables["polymarket_contracts"]
    assert len(rows) == 33 and not result.rejected
    table, rejected = validate_rows("polymarket_contracts", rows)
    assert rejected == [] and table.num_rows == 33
    assert {r["asset"] for r in rows} == {"BTC", "ETH"}
    assert all(r["settle_ts"] == ms(2026, 11, 12, 17) for r in rows)
    assert all(r["fees_enabled"] and "rate" in r["fee_schedule"] for r in rows)


def test_range_ladder_brackets_tile_the_price_line():
    rows = [r for r in contracts().tables["polymarket_contracts"] if r["event_slug"] == PRICE_SLUG]
    rows.sort(key=lambda r: r["strike_low"] or 0)
    assert rows[0]["contract_type"] == "below" and rows[0]["strike_low"] is None
    assert rows[-1]["contract_type"] == "above" and rows[-1]["strike_high"] is None
    middle = rows[1:-1]
    assert all(r["contract_type"] == "range" for r in middle)
    # no gaps or overlaps: each bracket starts where the previous one ended
    edges = [rows[0]["strike_high"]] + [r["strike_high"] for r in middle]
    assert [r["strike_low"] for r in middle + [rows[-1]]] == edges


def test_unrecognised_title_is_quarantined():
    events = json.loads(EVENTS)
    events[0]["markets"][0]["groupItemTitle"] = "Above 70k?"
    env = make_envelope(
        json.dumps(events).encode(), venue="polymarket", endpoint="events", params={}
    )
    result = parse(env)
    assert len(result.rejected["polymarket_contracts"]) == 1
    assert len(result.tables["polymarket_contracts"]) == 32


def books():
    env = make_envelope(BOOKS, venue="polymarket", endpoint="books", params={"slug": PRICE_SLUG})
    return parse(env).tables["polymarket_books"]


def test_best_prices_are_found_whatever_order_the_clob_sends():
    raw = json.loads(BOOKS)
    for row, book in zip(books(), raw, strict=True):
        bids = [float(x["price"]) for x in book["bids"]]
        asks = [float(x["price"]) for x in book["asks"]]
        assert row["best_bid"] == (max(bids) if bids else None)
        assert row["best_ask"] == (min(asks) if asks else None)
        # the CLOB puts the best price LAST; taking element 0 would pick the worst
        if len(bids) > 1:
            assert row["best_bid"] != float(book["bids"][0]["price"])
        tops = [lv["price"] for lv in row["bids_top5"]]
        assert tops == sorted(tops, reverse=True) and len(tops) <= 5


def test_empty_side_gives_null_not_zero_and_rows_validate():
    rows = books()
    assert any(r["best_bid"] is None and r["bids_top5"] == [] for r in rows)
    assert all(r["best_ask"] is not None for r in rows)
    assert all(r["best_bid"] is None or r["best_bid"] < r["best_ask"] for r in rows)
    table, rejected = validate_rows("polymarket_books", rows)
    assert rejected == [] and table.num_rows == 11


# --- recorder --------------------------------------------------------------------

TICK = ms(2026, 11, 12, 15)


class FakeVenue:
    def __init__(self, gamma_status: int = 200) -> None:
        self.gamma_calls = 0
        self.book_bodies: list[list[dict]] = []
        self.gamma_status = gamma_status

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "gamma-api.polymarket.com":
            self.gamma_calls += 1
            return httpx.Response(self.gamma_status, content=EVENTS)
        self.book_bodies.append(json.loads(request.content))
        return httpx.Response(200, content=BOOKS)


def make_recorder(redis, tmp_path, venue: FakeVenue) -> PolymarketRecorder:
    client = httpx.AsyncClient(transport=httpx.MockTransport(venue))
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**8), 1000)
    return PolymarketRecorder(client, bus, retries=0)


@pytest.mark.anyio
async def test_discovery_is_cached_for_ten_minutes(redis, tmp_path):
    venue = FakeVenue()
    rec = make_recorder(redis, tmp_path, venue)
    for i in range(11):  # 11 one-minute ticks
        await rec.run_tick(TICK + i * 60_000)
    assert venue.gamma_calls == 2  # at tick 0 and tick 10
    assert len(venue.book_bodies) == 11 * 3  # 3 ladders every minute
    assert DISCOVERY_INTERVAL_MS == 10 * 60_000
    envs = [Envelope.from_fields(f) for _, f in await redis.xrange(stream_name("polymarket"))]
    assert sorted({e.key for e in envs})[:2] == [
        "books-bitcoin_above_on_november_12_2026",
        "books-bitcoin_price_on_november_12_2026",
    ]
    assert sum(e.endpoint == "events" for e in envs) == 2


@pytest.mark.anyio
async def test_failed_discovery_is_retried_next_tick_and_published(redis, tmp_path):
    venue = FakeVenue(gamma_status=503)
    rec = make_recorder(redis, tmp_path, venue)
    await rec.run_tick(TICK)
    await rec.run_tick(TICK + 60_000)
    assert venue.gamma_calls == 2 and venue.book_bodies == []
    envs = [Envelope.from_fields(f) for _, f in await redis.xrange(stream_name("polymarket"))]
    assert [e.status for e in envs] == ["503", "503"]  # the gap is logged with its cause


@pytest.mark.anyio
async def test_settled_ladders_are_not_fetched(redis, tmp_path):
    venue = FakeVenue()
    rec = make_recorder(redis, tmp_path, venue)
    await rec.run_tick(ms(2026, 11, 12, 17))  # exactly at settlement (noon ET)
    assert venue.book_bodies == []
    body_tokens = {t["token_id"] for t in (await rec.fetch_specs(TICK))[0].json}
    assert len(body_tokens) == 11
