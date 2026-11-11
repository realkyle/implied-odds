"""Reference-price parser tests against real Binance and Coinbase responses (2026-11-11)."""

from __future__ import annotations

import json

from ppm.tests.conftest import fixture_bytes, make_envelope
from ppm.writer.parsers import parse
from ppm.writer.schemas import validate_rows

MINUTE = 60_000


def klines_env(body: bytes, snapshot_ts: int):
    return make_envelope(
        body,
        venue="reference",
        endpoint="binance_klines",
        params={"symbol": "BTCUSDT"},
        snapshot_ts=snapshot_ts,
    )


def test_kline_keeps_the_candle_that_closed_before_the_tick():
    candles = json.loads(fixture_bytes("reference", "binance_klines_BTCUSDT.json"))
    closed, forming = candles
    tick = forming[0]  # fetched at the tick when the second candle had just opened
    result = parse(klines_env(fixture_bytes("reference", "binance_klines_BTCUSDT.json"), tick))
    (row,) = result.tables["reference_prices"]
    assert row["price"] == float(closed[4])  # close of [tick-60s, tick-1ms]
    assert row["exchange_ts"] == closed[6] == tick - 1
    assert (row["asset"], row["source"], row["symbol"]) == ("BTC", "binance_vision", "BTCUSDT")
    table, rejected = validate_rows("reference_prices", [row])
    assert rejected == [] and table.num_rows == 1


def test_forming_candle_is_never_used_even_if_it_comes_first():
    candles = json.loads(fixture_bytes("reference", "binance_klines_BTCUSDT.json"))
    tick = candles[1][0]
    body = json.dumps(list(reversed(candles))).encode()
    (row,) = parse(klines_env(body, tick)).tables["reference_prices"]
    assert row["price"] == float(candles[0][4])


def test_no_closed_candle_is_quarantined():
    candles = json.loads(fixture_bytes("reference", "binance_klines_BTCUSDT.json"))
    result = parse(klines_env(json.dumps(candles).encode(), snapshot_ts=candles[0][0]))
    assert "reference_prices" not in result.tables
    assert result.rejected["reference_prices"][0].reason == "no candle closed before tick"


def test_coinbase_ticker():
    raw = json.loads(fixture_bytes("reference", "coinbase_ticker_BTC-USD.json"))
    env = make_envelope(
        fixture_bytes("reference", "coinbase_ticker_BTC-USD.json"),
        venue="reference",
        endpoint="coinbase_ticker",
        params={"product": "BTC-USD"},
    )
    (row,) = parse(env).tables["reference_prices"]
    assert row["price"] == float(raw["price"])
    assert row["bid"] == float(raw["bid"]) and row["ask"] == float(raw["ask"])
    assert row["bid"] <= row["ask"]
    assert row["exchange_ts"] is not None  # nanosecond ISO time parsed
    assert (row["asset"], row["source"], row["symbol"]) == ("BTC", "coinbase", "BTC-USD")
    table, rejected = validate_rows("reference_prices", [row])
    assert rejected == [] and table.num_rows == 1
