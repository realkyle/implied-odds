"""Kalshi recorder: every open market in the BTC and ETH price series.

One public REST call per series per tick (no API key). `GET /markets?series_ticker=…`
returns all open events of the series with top-of-book quotes, so each series is one
atomic snapshot. Order-book depth would need one call per market (~1,400/min) or the
authenticated websocket, so Stage 1 records top of book only.

Series (checked 2026-11-11):
  KXBTCD / KXETHD   "above K" contracts (hourly events plus the 5 pm ET daily one)
  KXBTC  / KXETH    range contracts ("between", with "greater"/"less" tails)
All settle on a 60-second average of the CF Benchmarks real-time index before close_time.
"""

from __future__ import annotations

from ppm.recorders.base import FetchSpec, Recorder

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
# One page covers a series with lots of headroom (318-390 open markets on 2026-11-11).
# The parser warns if a response says there is a further page.
PAGE_LIMIT = 1000


class KalshiRecorder(Recorder):
    venue = "kalshi"
    series: tuple[str, ...] = ("KXBTCD", "KXBTC", "KXETHD", "KXETH")

    async def fetch_specs(self, snapshot_ts: int) -> list[FetchSpec]:
        return [
            FetchSpec(
                endpoint="markets",
                url=f"{BASE_URL}/markets",
                params={"series": s},
                query={"series_ticker": s, "status": "open", "limit": str(PAGE_LIMIT)},
            )
            for s in self.series
        ]
