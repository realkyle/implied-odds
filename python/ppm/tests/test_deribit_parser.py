"""Parsers are tested against real Deribit responses saved on 2026-11-10."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from ppm.tests.conftest import fixture_bytes, make_envelope
from ppm.writer.parsers import parse
from ppm.writer.schemas import validate_rows


def ms(*args) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


def option_env():
    return make_envelope(fixture_bytes("deribit", "book_summary_BTC_option.json"))


def test_option_chain_rows_match_the_raw_response():
    raw = json.loads(fixture_bytes("deribit", "book_summary_BTC_option.json"))["result"]
    result = parse(option_env())
    rows = result.tables["deribit_options"]
    assert len(rows) == len(raw) and not result.rejected
    for row, item in zip(rows, raw, strict=True):
        assert row["instrument"] == item["instrument_name"]
        assert row["bid"] == item.get("bid_price") and row["mark_iv"] == item["mark_iv"]
        assert row["exchange_ts"] == item["creation_timestamp"]
    assert result.server_ts is not None


def test_instrument_name_is_decoded():
    item = {"instrument_name": "BTC-25DEC26-140000-C", "mark_iv": 63.79, "bid_price": 0.0001}
    body = json.dumps({"result": [item], "usOut": 1_794_329_910_319_353}).encode()
    result = parse(make_envelope(body))
    (row,) = result.tables["deribit_options"]
    assert row["asset"] == "BTC"
    assert row["strike"] == 140_000.0
    assert row["option_type"] == "C"
    assert row["expiry_ts"] == ms(2026, 12, 25, 8)  # Deribit expiries are 08:00 UTC
    assert result.server_ts == 1_794_329_910_319  # usOut is microseconds


def test_option_rows_pass_schema_validation_including_missing_bids():
    rows = parse(option_env()).tables["deribit_options"]
    assert any(r["bid"] is None for r in rows)  # fixture includes options with no bid
    table, rejected = validate_rows("deribit_options", rows)
    assert rejected == [] and table.num_rows == len(rows)


def test_futures_parse_perpetual_and_dated():
    env = make_envelope(
        fixture_bytes("deribit", "book_summary_ETH_future.json"),
        params={"currency": "ETH", "kind": "future"},
    )
    rows = {r["instrument"]: r for r in parse(env).tables["deribit_futures"]}
    perp = rows["ETH-PERPETUAL"]
    assert perp["is_perpetual"] and perp["expiry_ts"] is None and perp["funding_8h"] is not None
    assert rows["ETH-26MAR27"]["expiry_ts"] == ms(2027, 3, 26, 8)
    _, rejected = validate_rows("deribit_futures", list(rows.values()))
    assert rejected == []


def test_index_price_becomes_a_reference_price():
    env = make_envelope(
        fixture_bytes("deribit", "index_price_btc_usd.json"),
        endpoint="index_price",
        params={"index_name": "btc_usd"},
    )
    (row,) = parse(env).tables["reference_prices"]
    assert row["source"] == "deribit" and row["symbol"] == "btc_usd" and row["asset"] == "BTC"
    assert row["price"] > 0


def test_unrecognised_instrument_is_rejected_not_guessed():
    body = json.dumps({"result": [{"instrument_name": "BTC-WEIRD"}], "usOut": 1}).encode()
    result = parse(make_envelope(body))
    assert "deribit_options" not in result.tables
    assert "unrecognised" in result.rejected["deribit_options"][0].reason


def test_fractional_strikes():
    body = json.dumps({"result": [{"instrument_name": "ETH-11NOV26-2d5-P"}]}).encode()
    (row,) = parse(make_envelope(body)).tables["deribit_options"]
    assert row["strike"] == 2.5 and row["option_type"] == "P"
