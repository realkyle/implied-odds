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
