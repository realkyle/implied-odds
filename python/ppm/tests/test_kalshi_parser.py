"""Kalshi parser tests against real /markets responses saved on 2026-11-11 (trimmed)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from ppm.tests.conftest import fixture_bytes, make_envelope
from ppm.writer.parsers import parse
from ppm.writer.schemas import validate_rows


def kalshi_env(series: str, body: bytes | None = None):
    body = body if body is not None else fixture_bytes("kalshi", f"markets_{series}.json")
    return make_envelope(body, venue="kalshi", endpoint="markets", params={"series": series})


@pytest.mark.parametrize("series", ["KXBTCD", "KXBTC"])
def test_every_market_becomes_a_valid_row(series):
    raw = json.loads(fixture_bytes("kalshi", f"markets_{series}.json"))["markets"]
    result = parse(kalshi_env(series))
    rows = result.tables["kalshi_markets"]
    assert len(rows) == len(raw) and not result.rejected
    table, rejected = validate_rows("kalshi_markets", rows)
    assert rejected == [] and table.num_rows == len(raw)
    for row, m in zip(rows, raw, strict=True):
        assert row["ticker"] == m["ticker"] and row["asset"] == "BTC"
        # dollar strings become floats exactly
        assert row["yes_bid"] == float(m["yes_bid_dollars"])
        assert row["yes_ask"] == float(m["yes_ask_dollars"])
        assert row["exchange_ts"] is None  # updated_time is not a quote time


def test_strike_types_keep_the_right_bounds():
    rows = {r["strike_type"]: r for r in parse(kalshi_env("KXBTC")).tables["kalshi_markets"]}
    assert set(rows) == {"greater", "less", "between"}
    assert rows["greater"]["floor_strike"] is not None and rows["greater"]["cap_strike"] is None
    assert rows["less"]["floor_strike"] is None and rows["less"]["cap_strike"] is not None
    between = rows["between"]
    assert between["floor_strike"] < between["cap_strike"]


def test_settle_ts_is_close_time_not_the_later_expiration_time():
    market = {
        "ticker": "KXBTCD-26NOV1117-T83749.99",
        "event_ticker": "KXBTCD-26NOV1117",
        "strike_type": "greater",
        "floor_strike": 83749.99,
        "close_time": "2026-11-11T22:00:00Z",  # 5 pm ET: when the BRTI average is taken
        "expiration_time": "2026-11-18T22:00:00Z",  # latest possible, a week later
        "yes_bid_dollars": "0.4200",
        "yes_ask_dollars": "0.4400",
    }
    body = json.dumps({"cursor": "", "markets": [market]}).encode()
    (row,) = parse(kalshi_env("KXBTCD", body)).tables["kalshi_markets"]
    assert row["settle_ts"] == int(datetime(2026, 11, 11, 22, tzinfo=UTC).timestamp() * 1000)
    assert (row["yes_bid"], row["yes_ask"]) == (0.42, 0.44)
    assert row["yes_bid_size"] is None  # missing fields stay null, not 0


def test_unknown_series_is_quarantined_not_guessed():
    body = fixture_bytes("kalshi", "markets_KXBTCD.json")
    result = parse(kalshi_env("KXSOLD", body))
    assert "kalshi_markets" not in result.tables
    assert all("unknown series" in r.reason for r in result.rejected["kalshi_markets"])


def test_truncated_page_is_logged(caplog):
    body = json.dumps({"cursor": "abc", "markets": []}).encode()
    parse(kalshi_env("KXBTCD", body))
    assert "another page" in caplog.text
