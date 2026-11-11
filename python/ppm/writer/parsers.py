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
from datetime import UTC, date, datetime
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


# --- Polymarket ------------------------------------------------------------------

POLYMARKET_SLUG_ASSET = {"bitcoin": "BTC", "ethereum": "ETH"}
# groupItemTitle: "66,000" (above ladder), "<68,000", ">86,000", "68,000-70,000"
_PM_TITLE_RE = re.compile(r"^(?P<op>[<>])?(?P<low>[\d,.]+)(?:-(?P<high>[\d,.]+))?$")
BOOK_DEPTH = 5


def _pm_asset(slug: str) -> str | None:
    return POLYMARKET_SLUG_ASSET.get(slug.split("-")[0])


def _pm_strikes(slug: str, title: str) -> tuple[str, float | None, float | None] | None:
    """(contract_type, strike_low, strike_high) from the event slug and bracket title."""
    m = _PM_TITLE_RE.match(title.replace(" ", ""))
    if not m:
        return None
    low = float(m["low"].replace(",", ""))
    if m["high"]:
        return "range", low, float(m["high"].replace(",", ""))
    if m["op"] == "<":
        return "below", None, low
    if m["op"] == ">" or "-above-on-" in slug:
        return "above", low, None
    return None


@parser("polymarket", "events")
def parse_polymarket_events(env: Envelope) -> ParseResult:
    out = ParseResult()
    for event in json.loads(env.body):
        slug = event["slug"]
        for m in event.get("markets", []):
            asset = _pm_asset(slug)
            strikes = _pm_strikes(slug, m.get("groupItemTitle", ""))
            if asset is None or strikes is None:
                reason = f"unrecognised contract {slug!r} / {m.get('groupItemTitle')!r}"
                out.reject("polymarket_contracts", m, reason)
                continue
            outcomes = json.loads(m["outcomes"])
            tokens = json.loads(m["clobTokenIds"])
            if sorted(outcomes) != ["No", "Yes"] or len(tokens) != 2:
                out.reject("polymarket_contracts", m, f"unexpected outcomes {outcomes!r}")
                continue
            yes = outcomes.index("Yes")
            kind, low, high = strikes
            schedule = m.get("feeSchedule")
            out.add(
                "polymarket_contracts",
                {
                    "snapshot_ts": env.snapshot_ts,
                    "recv_ts": env.recv_ts,
                    "exchange_ts": None,
                    "asset": asset,
                    "yes_token_id": tokens[yes],
                    "no_token_id": tokens[1 - yes],
                    "condition_id": m.get("conditionId"),
                    "event_slug": slug,
                    "question": m.get("question"),
                    "contract_type": kind,
                    "strike_low": low,
                    "strike_high": high,
                    "settle_ts": _iso_ms(m.get("endDate")),
                    "closed": m.get("closed"),
                    "fees_enabled": m.get("feesEnabled"),
                    "fee_type": m.get("feeType"),
                    "fee_schedule": json.dumps(schedule, sort_keys=True) if schedule else None,
                    "tick_size": _num(m.get("orderPriceMinTickSize")),
                    "min_order_size": _num(m.get("orderMinSize")),
                    "neg_risk": m.get("negRisk"),
                },
            )
    return out


def _levels(raw: list[dict[str, str]], best_first_desc: bool) -> list[dict[str, float]]:
    """Order-book levels sorted best first. The CLOB sends bids ascending and asks
    descending (best price LAST), so never trust the order it arrives in."""
    levels = [{"price": float(x["price"]), "size": float(x["size"])} for x in raw]
    return sorted(levels, key=lambda lv: lv["price"], reverse=best_first_desc)


@parser("polymarket", "books")
def parse_polymarket_books(env: Envelope) -> ParseResult:
    slug = env.params["slug"]
    asset = _pm_asset(slug)
    out = ParseResult()
    for book in json.loads(env.body):
        if asset is None:
            out.reject("polymarket_books", book, f"unknown asset in slug {slug!r}")
            continue
        bids = _levels(book.get("bids") or [], best_first_desc=True)
        asks = _levels(book.get("asks") or [], best_first_desc=False)
        out.add(
            "polymarket_books",
            {
                "snapshot_ts": env.snapshot_ts,
                "recv_ts": env.recv_ts,
                "exchange_ts": int(book["timestamp"]) if book.get("timestamp") else None,
                "asset": asset,
                "yes_token_id": book.get("asset_id"),
                "condition_id": book.get("market"),
                "event_slug": slug,
                "best_bid": bids[0]["price"] if bids else None,
                "best_ask": asks[0]["price"] if asks else None,
                "bid_size": bids[0]["size"] if bids else None,
                "ask_size": asks[0]["size"] if asks else None,
                "bids_top5": bids[:BOOK_DEPTH],
                "asks_top5": asks[:BOOK_DEPTH],
                "last_trade": _num(book.get("last_trade_price")),
                "book_hash": book.get("hash"),
                "tick_size": _num(book.get("tick_size")),
            },
        )
    return out


# --- Settlements -----------------------------------------------------------------

_SETTLED = {"settled", "finalized"}


def _settlement_row(env: Envelope, **fields: Any) -> Row:
    return {"snapshot_ts": env.snapshot_ts, "recv_ts": env.recv_ts, **fields}


@parser("settlements", "kalshi_settled")
def parse_kalshi_settled(env: Envelope) -> ParseResult:
    series = env.params["series"]
    asset = KALSHI_SERIES_ASSET.get(series)
    out = ParseResult()
    for m in json.loads(env.body)["markets"]:
        if m.get("status") not in _SETTLED or m.get("result") not in ("yes", "no"):
            continue  # closed but not settled yet: the next hourly run picks it up
        if asset is None:
            out.reject("settlements", m, f"unknown series {series!r}")
            continue
        out.add(
            "settlements",
            _settlement_row(
                env,
                exchange_ts=_iso_ms(m.get("settlement_ts")),
                asset=asset,
                venue="kalshi",
                contract_id=m.get("ticker"),
                event_id=m.get("event_ticker"),
                settle_ts=_iso_ms(m.get("close_time")),
                result=m["result"],
                settlement_value=_num(m.get("expiration_value")),
            ),
        )
    return out


@parser("settlements", "polymarket_resolved")
def parse_polymarket_resolved(env: Envelope) -> ParseResult:
    out = ParseResult()
    for event in json.loads(env.body):
        slug = event["slug"]
        asset = _pm_asset(slug)
        for m in event.get("markets", []):
            if not m.get("closed"):
                continue
            outcomes = json.loads(m["outcomes"])
            prices = json.loads(m.get("outcomePrices") or "[]")
            final = dict(zip(outcomes, prices, strict=False))
            if sorted(final.values()) != ["0", "1"]:
                continue  # closed but not resolved yet (or disputed)
            if asset is None:
                out.reject("settlements", m, f"unknown asset in slug {slug!r}")
                continue
            tokens = json.loads(m["clobTokenIds"])
            out.add(
                "settlements",
                _settlement_row(
                    env,
                    exchange_ts=None,
                    asset=asset,
                    venue="polymarket",
                    contract_id=tokens[outcomes.index("Yes")],
                    event_id=slug,
                    settle_ts=_iso_ms(m.get("endDate")),
                    result="yes" if final["Yes"] == "1" else "no",
                    settlement_value=None,
                ),
            )
    return out


@parser("settlements", "binance_settle_candle")
def parse_binance_settle_candle(env: Envelope) -> ParseResult:
    """The 1m candle closing at noon ET. Binance returns the next candle that exists if
    the requested minute has none, so check it really is the minute we asked for."""
    from ppm.recorders.settlements import noon_et_ms  # local import: avoid a cycle

    symbol = env.params["symbol"]
    settle = noon_et_ms(date.fromisoformat(env.params["date"]))
    out = ParseResult()
    candles = json.loads(env.body)
    if not candles or int(candles[0][_K_OPEN_TIME]) != settle - 60_000:
        out.reject("settlements", {"candles": candles}, f"no candle opening at {settle - 60_000}")
        return out
    k = candles[0]
    out.add(
        "settlements",
        _settlement_row(
            env,
            exchange_ts=int(k[_K_CLOSE_TIME]),
            asset=symbol.removesuffix("USDT"),
            venue="binance",
            contract_id=f"{symbol}@{env.params['date']}",
            event_id=symbol,
            settle_ts=settle,
            result=None,
            settlement_value=_num(k[_K_CLOSE]),
        ),
    )
    return out
