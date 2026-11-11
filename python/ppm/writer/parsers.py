"""Parsers: raw venue response → table rows. Pure functions (bytes in, rows out).

Each parser handles one (venue, endpoint) and is tested against saved real responses in
python/ppm/tests/fixtures/. A row the parser cannot interpret becomes a `Rejected` with a
reason, so it is quarantined rather than dropped or guessed at.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ppm.pipeline.envelope import Envelope
from ppm.writer.schemas import Rejected

log = logging.getLogger(__name__)

Row = dict[str, Any]


@dataclass(slots=True)
class ParseResult:
    tables: dict[str, list[Row]] = field(default_factory=dict)
    rejected: dict[str, list[Rejected]] = field(default_factory=dict)
    server_ts: int | None = None  # venue server clock (ms), for skew measurement

    def add(self, table: str, row: Row) -> None:
        self.tables.setdefault(table, []).append(row)

    def reject(self, table: str, row: Row, reason: str) -> None:
        self.rejected.setdefault(table, []).append(Rejected(row, reason))


Parser = Callable[[Envelope], ParseResult]
_REGISTRY: dict[tuple[str, str], Parser] = {}


def parser(venue: str, endpoint: str) -> Callable[[Parser], Parser]:
    """Register a parser for (venue, endpoint)."""

    def register(fn: Parser) -> Parser:
        _REGISTRY[(venue, endpoint)] = fn
        return fn

    return register


def parse(env: Envelope) -> ParseResult:
    try:
        fn = _REGISTRY[(env.venue, env.endpoint)]
    except KeyError:
        raise LookupError(f"no parser for {env.venue}/{env.endpoint}") from None
    return fn(env)


# --- Deribit -------------------------------------------------------------------

_MONTHS = {m: i for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1)}
# e.g. BTC-25DEC26-140000-C. Deribit writes fractional strikes with "d" (e.g. 2d5 = 2.5).
_OPTION_RE = re.compile(
    r"^(?P<asset>[A-Z]+)-(?P<day>\d{1,2})(?P<mon>[A-Z]{3})(?P<yy>\d{2})-(?P<strike>\d+(?:d\d+)?)-(?P<cp>[CP])$"
)
# e.g. BTC-PERPETUAL or BTC-27NOV26
_FUTURE_RE = re.compile(
    r"^(?P<asset>[A-Z]+)-(?:(?P<perp>PERPETUAL)|(?P<day>\d{1,2})(?P<mon>[A-Z]{3})(?P<yy>\d{2}))$"
)
DERIBIT_EXPIRY_HOUR_UTC = 8  # all Deribit options and dated futures expire at 08:00 UTC


def _deribit_expiry_ms(day: str, mon: str, yy: str) -> int:
    dt = datetime(2000 + int(yy), _MONTHS[mon], int(day), DERIBIT_EXPIRY_HOUR_UTC, tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def _deribit_payload(env: Envelope) -> tuple[Any, int | None]:
    """Unwrap Deribit's JSON-RPC envelope. `usOut` is the server's send time (µs)."""
    doc = json.loads(env.body)
    if "result" not in doc:
        raise ValueError(f"deribit error response: {doc.get('error')}")
    us_out = doc.get("usOut")
    return doc["result"], (us_out // 1000 if us_out else None)


@parser("deribit", "book_summary")
def parse_deribit_book_summary(env: Envelope) -> ParseResult:
    items, server_ts = _deribit_payload(env)
    out = ParseResult(server_ts=server_ts)
    kind = env.params["kind"]
    for item in items:
        name = item.get("instrument_name", "")
        common = {
            "snapshot_ts": env.snapshot_ts,
            "recv_ts": env.recv_ts,
            "exchange_ts": item.get("creation_timestamp"),
            "instrument": name,
        }
        if kind == "option":
            m = _OPTION_RE.match(name)
            if not m:
                out.reject("deribit_options", item, f"unrecognised option name {name!r}")
                continue
            out.add(
                "deribit_options",
                {
                    **common,
                    "asset": m["asset"],
                    "expiry_ts": _deribit_expiry_ms(m["day"], m["mon"], m["yy"]),
                    "strike": float(m["strike"].replace("d", ".")),
                    "option_type": m["cp"],
                    "bid": item.get("bid_price"),
                    "ask": item.get("ask_price"),
                    "mark": item.get("mark_price"),
                    "mark_iv": item.get("mark_iv"),
                    "underlying_price": item.get("underlying_price"),
                    "underlying_index": item.get("underlying_index"),
                    "index_price": item.get("estimated_delivery_price"),
                    "open_interest": item.get("open_interest"),
                    "volume": item.get("volume"),
                    "interest_rate": item.get("interest_rate"),
                },
            )
        elif kind == "future":
            m = _FUTURE_RE.match(name)
            if not m:
                out.reject("deribit_futures", item, f"unrecognised future name {name!r}")
                continue
            perp = m["perp"] is not None
            out.add(
                "deribit_futures",
                {
                    **common,
                    "asset": m["asset"],
                    "expiry_ts": None if perp else _deribit_expiry_ms(m["day"], m["mon"], m["yy"]),
                    "is_perpetual": perp,
                    "bid": item.get("bid_price"),
                    "ask": item.get("ask_price"),
                    "mark": item.get("mark_price"),
                    "last": item.get("last"),
                    "funding_8h": item.get("funding_8h"),
                    "current_funding": item.get("current_funding"),
                    "open_interest": item.get("open_interest"),
                    "index_price": item.get("estimated_delivery_price"),
                },
            )
        else:
            raise ValueError(f"unexpected book_summary kind {kind!r}")
    return out


@parser("deribit", "index_price")
def parse_deribit_index_price(env: Envelope) -> ParseResult:
    result, server_ts = _deribit_payload(env)
    index_name = env.params["index_name"]  # e.g. btc_usd
    out = ParseResult(server_ts=server_ts)
    out.add(
        "reference_prices",
        {
            "snapshot_ts": env.snapshot_ts,
            "recv_ts": env.recv_ts,
            "exchange_ts": server_ts,
            "asset": index_name.split("_")[0].upper(),
            "source": "deribit",
            "symbol": index_name,
            "price": result.get("index_price"),
            "bid": None,
            "ask": None,
        },
    )
    return out


# --- Shared helpers ----------------------------------------------------------------


def _iso_ms(value: str | None) -> int | None:
    """ISO-8601 UTC string (e.g. 2026-11-11T22:00:00Z, any fractional digits) → ms."""
    if not value:
        return None
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def _num(value: Any) -> float | None:
    """Venue number that may arrive as a string ("0.0100") or be missing."""
    return None if value is None or value == "" else float(value)


# --- Kalshi --------------------------------------------------------------------

# Series → underlying asset. Unknown series are rejected, not guessed.
KALSHI_SERIES_ASSET = {"KXBTCD": "BTC", "KXBTC": "BTC", "KXETHD": "ETH", "KXETH": "ETH"}


@parser("kalshi", "markets")
def parse_kalshi_markets(env: Envelope) -> ParseResult:
    doc = json.loads(env.body)
    series = env.params["series"]
    out = ParseResult()
    if doc.get("cursor"):
        # A further page exists, so this snapshot is incomplete. Keep what we have and
        # say so loudly; raise PAGE_LIMIT in recorders/kalshi.py if this ever appears.
        log.warning("kalshi %s response has another page; snapshot truncated", series)
    asset = KALSHI_SERIES_ASSET.get(series)
    for m in doc["markets"]:
        if asset is None:
            out.reject("kalshi_markets", m, f"unknown series {series!r}")
            continue
        out.add(
            "kalshi_markets",
            {
                "snapshot_ts": env.snapshot_ts,
                "recv_ts": env.recv_ts,
                # `updated_time` is when the market's metadata last changed, not the
                # quote time, so it would give a wrong clock-skew figure. Leave null.
                "exchange_ts": None,
                "asset": asset,
                "ticker": m.get("ticker"),
                "series": series,
                "event_ticker": m.get("event_ticker"),
                "strike_type": m.get("strike_type"),
                "floor_strike": _num(m.get("floor_strike")),
                "cap_strike": _num(m.get("cap_strike")),
                "settle_ts": _iso_ms(m.get("close_time")),
                "yes_bid": _num(m.get("yes_bid_dollars")),
                "yes_ask": _num(m.get("yes_ask_dollars")),
                "yes_bid_size": _num(m.get("yes_bid_size_fp")),
                "yes_ask_size": _num(m.get("yes_ask_size_fp")),
                "last_price": _num(m.get("last_price_dollars")),
                "volume": _num(m.get("volume_fp")),
                "open_interest": _num(m.get("open_interest_fp")),
                "status": m.get("status"),
            },
        )
    return out


# --- Reference prices ------------------------------------------------------------

# Binance kline array positions (https://developers.binance.com/docs/binance-spot-api-docs).
_K_OPEN_TIME, _K_CLOSE, _K_CLOSE_TIME = 0, 4, 6


@parser("reference", "binance_klines")
def parse_binance_klines(env: Envelope) -> ParseResult:
    """Keep the latest candle that closed before the tick.

    At tick T the response holds the candle for [T-60 s, T-1 ms], which has closed, and
    the one forming since T. Only the closed candle is final, and it is the one that
    Polymarket's "candle closing at 12:00 ET" rule refers to when T is noon ET.
    """
    candles = json.loads(env.body)
    symbol = env.params["symbol"]
    out = ParseResult()
    closed = [k for k in candles if int(k[_K_CLOSE_TIME]) < env.snapshot_ts]
    if not closed:
        out.reject("reference_prices", {"candles": candles}, "no candle closed before tick")
        return out
    k = max(closed, key=lambda c: int(c[_K_OPEN_TIME]))
    out.add(
        "reference_prices",
        {
            "snapshot_ts": env.snapshot_ts,
            "recv_ts": env.recv_ts,
            "exchange_ts": int(k[_K_CLOSE_TIME]),
            "asset": symbol.removesuffix("USDT"),
            "source": "binance_vision",
            "symbol": symbol,
            "price": _num(k[_K_CLOSE]),
            "bid": None,
            "ask": None,
        },
    )
    return out


@parser("reference", "coinbase_ticker")
def parse_coinbase_ticker(env: Envelope) -> ParseResult:
    doc = json.loads(env.body)
    product = env.params["product"]  # e.g. BTC-USD
    out = ParseResult()
    out.add(
        "reference_prices",
        {
            "snapshot_ts": env.snapshot_ts,
            "recv_ts": env.recv_ts,
            "exchange_ts": _iso_ms(doc.get("time")),  # time of the last trade
            "asset": product.split("-")[0],
            "source": "coinbase",
            "symbol": product,
            "price": _num(doc.get("price")),
            "bid": _num(doc.get("bid")),
            "ask": _num(doc.get("ask")),
        },
    )
    return out
