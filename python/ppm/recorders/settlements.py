"""Settlement collector: what each contract actually settled on. Runs hourly.

Every run refetches the last 48 hours (decided 2026-11-11). Rows are deduplicated on
(venue, contract_id), so the overlap is harmless, and it covers late settlements and
laptop sleeps or outages of up to two days with no extra state. It also runs once at
start-up, so a restart backfills straight away.

Sources (checked 2026-11-11, all public):
- Kalshi `GET /markets?series_ticker=S&status=settled&min_close_ts=…`: per contract
  `result` and `expiration_value`, the actual 60 s BRTI average it settled on. A series
  has ~9,000 settled markets in 48 h, so this is the first paginated fetch: each page's
  `cursor` points at the next, so pages are sequential, and each page is one message.
- Polymarket Gamma `/events?slug=…` for the last 3 ET dates: resolved outcomes.
- Binance (vision) 1m candle that closes at noon ET on each of those dates: the exact
  price Polymarket settles on, even if the live reference recorder missed that minute.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from ppm.pipeline.bus import PublishResult
from ppm.recorders.base import FetchSpec, Recorder
from ppm.recorders.kalshi import BASE_URL as KALSHI_URL
from ppm.recorders.kalshi import KalshiRecorder
from ppm.recorders.polymarket import ET, GAMMA_URL, event_slugs
from ppm.recorders.reference import BINANCE_URL, ReferenceRecorder

log = logging.getLogger(__name__)

HOUR_MS = 3_600_000
LOOKBACK_MS = 48 * HOUR_MS
LOOKBACK_DAYS = 3  # ET dates for Polymarket and the Binance noon candle
MAX_PAGES = 50  # safety stop for a runaway cursor (48 h was 9-15 pages per series)
PAGE_LIMIT = 1000
# Pause between Kalshi page requests. Paging 4 series in parallel (~15-20 req/s) got
# HTTP 429 "too many requests" on 2026-11-11; one series at a time with this pause
# stays at or below ~4 req/s. A run takes ~20-30 s, once an hour.
PAGE_INTERVAL_S = 0.25


def noon_et_ms(day: date) -> int:
    """Polymarket settlement time: 12:00 America/New_York on `day`, in UTC ms."""
    return int(datetime.combine(day, time(12), tzinfo=ET).timestamp() * 1000)


class SettlementRecorder(Recorder):
    venue = "settlements"
    tick_ms = HOUR_MS
    run_on_start = True

    def __init__(self, *args: Any, page_interval_s: float = PAGE_INTERVAL_S, **kwargs: Any) -> None:
        # Settlement rows carry their own timestamps, so a late wake-up (laptop sleep)
        # can't mislabel anything: collect whenever we wake, up to a whole tick late.
        kwargs.setdefault("max_lateness_ms", HOUR_MS)
        super().__init__(*args, **kwargs)
        self._page_interval_s = page_interval_s

    async def run_tick(self, snapshot_ts: int) -> list[PublishResult]:
        results: list[PublishResult] = []
        for series in KalshiRecorder.series:  # one at a time: see PAGE_INTERVAL_S
            results += await self._kalshi_series(series, snapshot_ts)
        return results + await super().run_tick(snapshot_ts)  # Polymarket + Binance

    async def _kalshi_series(self, series: str, snapshot_ts: int) -> list[PublishResult]:
        results: list[PublishResult] = []
        cursor = ""
        min_close_s = str((snapshot_ts - LOOKBACK_MS) // 1000)
        for page in range(MAX_PAGES):
            if page:
                await asyncio.sleep(self._page_interval_s)
            query = {
                "series_ticker": series,
                "status": "settled",
                "min_close_ts": min_close_s,
                "limit": str(PAGE_LIMIT),
            }
            if cursor:
                query["cursor"] = cursor
            spec = FetchSpec(
                endpoint="kalshi_settled",
                url=f"{KALSHI_URL}/markets",
                params={"series": series, "page": f"{page:02d}"},
                query=query,
            )
            env = await self._fetch(spec, snapshot_ts)
            results.append(await self._bus.publish(env))
            if not env.ok:
                break  # failure already published (logged as a gap); next hour retries
            cursor = json.loads(env.body).get("cursor") or ""
            if not cursor:
                return results
        else:
            log.warning("settlements: %s hit the %d-page limit", series, MAX_PAGES)
        return results

    async def fetch_specs(self, snapshot_ts: int) -> list[FetchSpec]:
        today = datetime.fromtimestamp(snapshot_ts / 1000, UTC).astimezone(ET).date()
        first = today - timedelta(days=LOOKBACK_DAYS - 1)
        specs = [
            FetchSpec(
                endpoint="polymarket_resolved",
                url=GAMMA_URL,
                query={"slug": event_slugs(first, days=LOOKBACK_DAYS)},
            )
        ]
        for i in range(LOOKBACK_DAYS):
            day = first + timedelta(days=i)
            settle = noon_et_ms(day)
            if settle > snapshot_ts:
                continue  # that candle hasn't closed yet
            for symbol in ReferenceRecorder.binance_symbols:
                specs.append(
                    FetchSpec(
                        endpoint="binance_settle_candle",
                        url=BINANCE_URL,
                        params={"symbol": symbol, "date": day.isoformat()},
                        # the candle that opens at 11:59 and closes at 12:00 ET
                        query={
                            "symbol": symbol,
                            "interval": "1m",
                            "startTime": str(settle - 60_000),
                            "limit": "1",
                        },
                    )
                )
        return specs
