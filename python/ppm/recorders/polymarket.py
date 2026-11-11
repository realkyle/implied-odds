"""Polymarket recorder: BTC/ETH "above" and "price on" (range) ladders.

Two kinds of request (checked 2026-11-11, public, no key):
- Discovery, every 10 minutes: ONE Gamma call `GET /events?slug=…&slug=…` for the
  predictable daily slugs (`bitcoin-above-on-november-12-2026`, `bitcoin-price-on-…`,
  and the `ethereum-…` versions) over the next week. Events are listed ~5 days ahead,
  so a 10-minute refresh never misses one. The raw response is published like any
  other fetch; the writer parses it into `polymarket_contracts`.
- Books, every minute: one CLOB `POST /books` per event (a ladder of ~11 contracts),
  so each ladder is an atomic snapshot. The writer parses it into `polymarket_books`.

The recorder reads only token ids and end times out of the discovery response, the
minimum it needs to know what to fetch. All row parsing stays in the writer
(design decision A1). Touch markets ("what price will Bitcoin hit") are never
requested: they pay on a touch, not at expiry.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from ppm.pipeline.bus import PublishResult
from ppm.recorders.base import FetchSpec, Recorder

log = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com/events"
BOOKS_URL = "https://clob.polymarket.com/books"
ET = ZoneInfo("America/New_York")  # event dates are ET calendar dates
ASSET_SLUGS = ("bitcoin", "ethereum")
LADDER_KINDS = ("above", "price")  # "price on" = range brackets
DISCOVERY_DAYS = 7
DISCOVERY_INTERVAL_MS = 10 * 60_000


@dataclass(frozen=True, slots=True)
class Ladder:
    """One event's open contracts: what the books request needs."""

    slug: str
    end_ts: int  # ms; no point fetching books after this
    token_ids: tuple[str, ...]  # YES tokens


def event_slugs(on: date, days: int = DISCOVERY_DAYS) -> list[str]:
    """Slugs for `days` ET dates starting at `on`, e.g. bitcoin-above-on-november-12-2026."""
    slugs = []
    for i in range(days):
        d = on + timedelta(days=i)
        suffix = f"on-{d.strftime('%B').lower()}-{d.day}-{d.year}"
        slugs += [f"{a}-{k}-{suffix}" for a in ASSET_SLUGS for k in LADDER_KINDS]
    return slugs


def ladders_from_events(body: bytes) -> list[Ladder]:
    """Extract each event's open YES token ids from a Gamma /events response."""
    ladders = []
    for event in json.loads(body):
        tokens: list[str] = []
        end_ts = 0
        for m in event.get("markets", []):
            if m.get("closed") or not m.get("enableOrderBook"):
                continue
            outcomes = json.loads(m["outcomes"])
            token_ids = json.loads(m["clobTokenIds"])
            tokens.append(token_ids[outcomes.index("Yes")])
            end = datetime.fromisoformat(m["endDate"])
            end_ts = max(end_ts, int(end.timestamp() * 1000))
        if tokens:
            ladders.append(Ladder(event["slug"], end_ts, tuple(tokens)))
    return ladders


class PolymarketRecorder(Recorder):
    venue = "polymarket"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._ladders: list[Ladder] = []
        self._discovered_at: int | None = None

    def discovery_spec(self, snapshot_ts: int) -> FetchSpec:
        today_et = datetime.fromtimestamp(snapshot_ts / 1000, UTC).astimezone(ET).date()
        return FetchSpec(
            endpoint="events",
            url=GAMMA_URL,
            query={"slug": event_slugs(today_et)},
        )

    async def run_tick(self, snapshot_ts: int) -> list[PublishResult]:
        results: list[PublishResult] = []
        due = (
            self._discovered_at is None
            or snapshot_ts - self._discovered_at >= DISCOVERY_INTERVAL_MS
        )
        if due:
            env = await self._fetch(self.discovery_spec(snapshot_ts), snapshot_ts)
            results.append(await self._bus.publish(env))
            if env.ok:
                try:
                    self._ladders = ladders_from_events(env.body)
                    self._discovered_at = snapshot_ts
                    log.info("polymarket discovered %d ladders", len(self._ladders))
                except (ValueError, KeyError, TypeError):
                    # Keep the previous ladders and retry discovery next tick. The raw
                    # response is already published, so the writer quarantines it too.
                    log.exception("polymarket discovery response not understood")
        return results + await super().run_tick(snapshot_ts)

    async def fetch_specs(self, snapshot_ts: int) -> list[FetchSpec]:
        return [
            FetchSpec(
                endpoint="books",
                url=BOOKS_URL,
                params={"slug": ladder.slug},
                method="POST",
                json=[{"token_id": t} for t in ladder.token_ids],
            )
            for ladder in self._ladders
            if ladder.end_ts > snapshot_ts  # settled ladders have no live book
        ]
