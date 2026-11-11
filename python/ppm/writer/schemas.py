"""Parquet table schemas: the data contract from docs/design/recorder-pipeline.md §4.

`nullable=False` marks required columns. pyarrow does not enforce nullability when
building a table, so `validate_rows` checks it explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pyarrow as pa

TS = pa.timestamp("ms", tz="UTC")


def _f(name: str, typ: pa.DataType, required: bool = False) -> pa.Field:
    return pa.field(name, typ, nullable=not required)


COMMON = [
    _f("snapshot_ts", TS, required=True),  # scheduled tick: cross-venue join key
    _f("recv_ts", TS, required=True),  # local receive time
    _f("exchange_ts", TS),  # venue's own timestamp for the data, if it gives one
    _f("asset", pa.string(), required=True),  # BTC / ETH
]

# One order-book level.
LEVEL = pa.struct([("price", pa.float64()), ("size", pa.float64())])

SCHEMAS: dict[str, pa.Schema] = {
    "deribit_options": pa.schema(
        [
            *COMMON,
            _f("instrument", pa.string(), required=True),
            _f("expiry_ts", TS, required=True),
            _f("strike", pa.float64(), required=True),
            _f("option_type", pa.string(), required=True),  # "C" or "P"
            # Option prices are in the underlying coin (inverse contracts), not USD.
            _f("bid", pa.float64()),
            _f("ask", pa.float64()),
            _f("mark", pa.float64()),
            _f("mark_iv", pa.float64()),  # percent, e.g. 63.79
            _f("underlying_price", pa.float64()),
            _f("underlying_index", pa.string()),
            _f("index_price", pa.float64()),
            _f("open_interest", pa.float64()),
            _f("volume", pa.float64()),
            _f("interest_rate", pa.float64()),
        ]
    ),
    "deribit_futures": pa.schema(
        [
            *COMMON,
            _f("instrument", pa.string(), required=True),
            _f("expiry_ts", TS),  # null for perpetuals
            _f("is_perpetual", pa.bool_(), required=True),
            _f("bid", pa.float64()),  # USD
            _f("ask", pa.float64()),
            _f("mark", pa.float64()),
            _f("last", pa.float64()),
            _f("funding_8h", pa.float64()),
            _f("current_funding", pa.float64()),
            _f("open_interest", pa.float64()),
            _f("index_price", pa.float64()),
        ]
    ),
    "kalshi_markets": pa.schema(
        [
            *COMMON,  # exchange_ts is null: the REST response has no quote timestamp
            _f("ticker", pa.string(), required=True),  # e.g. KXBTCD-26NOV1117-T83749.99
            _f("series", pa.string(), required=True),
            _f("event_ticker", pa.string(), required=True),
            _f("strike_type", pa.string(), required=True),  # greater / less / between
            _f("floor_strike", pa.float64()),  # set for greater and between
            _f("cap_strike", pa.float64()),  # set for less and between
            _f("settle_ts", TS, required=True),  # close_time: end of the 60 s averaging window
            # Prices are in dollars per $1 contract (0.42 = 42 cents = 42% implied).
            _f("yes_bid", pa.float64()),
            _f("yes_ask", pa.float64()),
            _f("yes_bid_size", pa.float64()),  # contracts
            _f("yes_ask_size", pa.float64()),
            _f("last_price", pa.float64()),
            _f("volume", pa.float64()),
            _f("open_interest", pa.float64()),
            _f("status", pa.string()),
        ]
    ),
    # Dimension table: one row per contract per discovery (merge keeps the latest by
    # yes_token_id). Join to polymarket_books on yes_token_id.
    "polymarket_contracts": pa.schema(
        [
            *COMMON,  # snapshot_ts = discovery tick; exchange_ts null
            _f("yes_token_id", pa.string(), required=True),
            _f("no_token_id", pa.string()),
            _f("condition_id", pa.string(), required=True),
            _f("event_slug", pa.string(), required=True),
            _f("question", pa.string()),
            # above: pays if price > strike_low; below: price < strike_high;
            # range: strike_low <= price < strike_high (a tie goes to the higher bracket).
            _f("contract_type", pa.string(), required=True),
            _f("strike_low", pa.float64()),
            _f("strike_high", pa.float64()),
            _f("settle_ts", TS, required=True),  # noon ET: Binance 1m candle closing then
            _f("closed", pa.bool_()),
            _f("fees_enabled", pa.bool_()),
            _f("fee_type", pa.string()),  # e.g. crypto_fees_v2
            _f("fee_schedule", pa.string()),  # raw JSON, e.g. {"rate":0.07,"takerOnly":true}
            _f("tick_size", pa.float64()),
            _f("min_order_size", pa.float64()),
            _f("neg_risk", pa.bool_()),
        ]
    ),
    # Fact table: one row per contract per minute.
    "polymarket_books": pa.schema(
        [
            *COMMON,  # exchange_ts = book timestamp from the CLOB
            _f("yes_token_id", pa.string(), required=True),
            _f("condition_id", pa.string()),
            _f("event_slug", pa.string(), required=True),
            _f("best_bid", pa.float64()),  # null when that side of the book is empty
            _f("best_ask", pa.float64()),
            _f("bid_size", pa.float64()),  # shares at the best price
            _f("ask_size", pa.float64()),
            _f("bids_top5", pa.list_(LEVEL)),  # best first
            _f("asks_top5", pa.list_(LEVEL)),
            _f("last_trade", pa.float64()),
            _f("book_hash", pa.string()),
            _f("tick_size", pa.float64()),
        ]
    ),
    "reference_prices": pa.schema(
        [
            *COMMON,
            _f("source", pa.string(), required=True),  # deribit / binance_vision / coinbase
            _f("symbol", pa.string(), required=True),
            _f("price", pa.float64(), required=True),
            _f("bid", pa.float64()),
            _f("ask", pa.float64()),
        ]
    ),
    # What each contract settled on. Collected hourly over a 48 h window, so the same
    # contract appears many times in staging; the daily merge keeps one row per
    # (venue, contract_id).
    "settlements": pa.schema(
        [
            *COMMON,  # snapshot_ts = collection run; exchange_ts = when the venue settled
            _f("venue", pa.string(), required=True),  # kalshi / polymarket / binance
            _f("contract_id", pa.string(), required=True),  # ticker / yes_token_id / SYMBOL@date
            _f("event_id", pa.string()),  # event_ticker / event slug / symbol
            _f("settle_ts", TS, required=True),  # the moment the outcome refers to
            _f("result", pa.string()),  # yes / no; null for binance price rows
            # kalshi: the 60 s BRTI average; binance: the candle close; polymarket: null
            # (join the binance row for the same asset and settle_ts)
            _f("settlement_value", pa.float64()),
        ]
    ),
    # One row per fetch, success or failure: the gap log and latency/skew record.
    "fetch_log": pa.schema(
        [
            _f("snapshot_ts", TS, required=True),
            _f("venue", pa.string(), required=True),
            _f("endpoint_key", pa.string(), required=True),
            _f("request_ts", TS, required=True),
            _f("recv_ts", TS, required=True),
            _f("latency_ms", pa.int64(), required=True),
            _f("status", pa.string(), required=True),
            _f("error", pa.string()),
            _f("server_ts", TS),  # venue server clock, where the response includes it
            _f("rows", pa.int64()),  # rows parsed (all tables), null on failure
            _f("rejected", pa.int64()),  # rows quarantined
        ]
    ),
}


@dataclass(frozen=True, slots=True)
class Rejected:
    row: dict[str, Any]
    reason: str


def validate_rows(table: str, rows: list[dict[str, Any]]) -> tuple[pa.Table | None, list[Rejected]]:
    """Split rows into a valid Arrow table and rejected rows with reasons.

    Checks: no unknown columns, no nulls in required columns, values convertible to
    the declared types. A venue changing a field's type or name shows up here as
    quarantined rows instead of silently corrupting data/.
    """
    schema = SCHEMAS[table]
    names = set(schema.names)
    required = [f.name for f in schema if not f.nullable]
    good: list[dict[str, Any]] = []
    rejected: list[Rejected] = []
    for row in rows:
        unknown = set(row) - names
        missing = [c for c in required if row.get(c) is None]
        if unknown:
            rejected.append(Rejected(row, f"unknown columns {sorted(unknown)}"))
        elif missing:
            rejected.append(Rejected(row, f"null in required columns {missing}"))
        else:
            good.append(row)
    try:
        arrow = pa.Table.from_pylist(good, schema=schema) if good else None
    except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError):
        # Slow path, only on failure: convert row by row to find the bad ones.
        kept: list[dict[str, Any]] = []
        for row in good:
            try:
                pa.Table.from_pylist([row], schema=schema)
                kept.append(row)
            except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError) as exc:
                rejected.append(Rejected(row, f"type error: {exc}"))
        arrow = pa.Table.from_pylist(kept, schema=schema) if kept else None
    return arrow, rejected
