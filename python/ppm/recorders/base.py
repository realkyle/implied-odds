"""Recorder base class: fetch a venue's endpoints every tick and publish raw envelopes.

A recorder never parses venue data (design decision A1). Its only jobs are to fetch on
time, stamp accurate timestamps and hand the bytes to the bus. Keeping it this small is
what makes it hard to break.
"""

from __future__ import annotations

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, ClassVar

import httpx

from ppm.pipeline.bus import PublishResult, RedisBus
from ppm.pipeline.clock import Clock, next_tick, now_ms, sleep_until
from ppm.pipeline.envelope import STATUS_ERROR, Envelope

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FetchSpec:
    """One HTTP request to make each tick. `params` names the fetch (it becomes part of
    the envelope key); `query` and `json` are what is actually sent to the venue. A
    query value may be a list to repeat a parameter (`?slug=a&slug=b`)."""

    endpoint: str
    url: str
    params: Mapping[str, str] = field(default_factory=dict)
    query: Mapping[str, str | list[str]] = field(default_factory=dict)
    method: str = "GET"
    json: Any = None  # request body for POST


class Recorder(ABC):
    venue: ClassVar[str]

    def __init__(
        self,
        client: httpx.AsyncClient,
        bus: RedisBus,
        *,
        clock: Clock = now_ms,
        timeout_s: float = 20.0,
        retries: int = 1,
        retry_base_s: float = 1.0,
        max_lateness_ms: int = 5_000,
    ) -> None:
        self._client = client
        self._bus = bus
        self._clock = clock
        self._timeout_s = timeout_s
        self._retries = retries
        self._retry_base_s = retry_base_s
        self._max_lateness_ms = max_lateness_ms

    @abstractmethod
    async def fetch_specs(self, snapshot_ts: int) -> list[FetchSpec]:
        """The requests to make for this tick (may depend on discovered state)."""

    async def run_forever(self, stop: asyncio.Event) -> None:
        log.info("%s recorder started", self.venue)
        while not stop.is_set():
            tick = next_tick(self._clock())
            if not await sleep_until(tick, stop, self._clock):
                break
            lateness = self._clock() - tick
            if lateness > self._max_lateness_ms:
                # Woke far too late (machine asleep, process paused). Fetching now would
                # label current prices with an old snapshot_ts, so skip this tick instead.
                log.warning(
                    "%s missed tick %d: woke %.1f s late", self.venue, tick, lateness / 1000
                )
                continue
            try:
                results = await self.run_tick(tick)
                log.info("%s tick %d: %s", self.venue, tick, _summarise(results))
            except Exception:  # never let one bad tick kill the recorder
                log.exception("%s tick %d failed", self.venue, tick)
        log.info("%s recorder stopped", self.venue)

    async def run_tick(self, snapshot_ts: int) -> list[PublishResult]:
        specs = await self.fetch_specs(snapshot_ts)
        # All fetches start together, so every endpoint is captured within the same
        # fraction of a second. That is the point of using async here.
        envelopes = await asyncio.gather(*(self._fetch(spec, snapshot_ts) for spec in specs))
        return [await self._bus.publish(env) for env in envelopes]

    async def _fetch(self, spec: FetchSpec, snapshot_ts: int) -> Envelope:
        error, status = "not attempted", STATUS_ERROR
        request_ts = recv_ts = self._clock()
        for attempt in range(self._retries + 1):
            request_ts = self._clock()
            try:
                response = await self._client.request(
                    spec.method,
                    spec.url,
                    params=dict(spec.query),
                    json=spec.json,
                    timeout=self._timeout_s,
                )
                recv_ts = self._clock()
                if response.status_code == 200:
                    return self._envelope(
                        spec, snapshot_ts, request_ts, recv_ts, "200", response.content
                    )
                error = f"HTTP {response.status_code}: {response.text[:500]}"
                status = str(response.status_code)
            except httpx.HTTPError as exc:
                recv_ts = self._clock()
                error = f"{type(exc).__name__}: {exc}"
                status = STATUS_ERROR
            if attempt < self._retries:
                # Exponential backoff with jitter: random spread stops many clients from
                # retrying in lockstep and hammering a struggling server together.
                await asyncio.sleep(self._retry_base_s * 2**attempt * random.uniform(0.5, 1.5))
        log.warning(
            "%s %s failed after %d attempts: %s",
            self.venue,
            spec.endpoint,
            self._retries + 1,
            error,
        )
        # Publish the failure too: the writer logs it as a gap with its cause.
        return self._envelope(spec, snapshot_ts, request_ts, recv_ts, status, error.encode())

    def _envelope(
        self,
        spec: FetchSpec,
        snapshot_ts: int,
        request_ts: int,
        recv_ts: int,
        status: str,
        body: bytes,
    ) -> Envelope:
        return Envelope(
            venue=self.venue,
            endpoint=spec.endpoint,
            params=dict(spec.params),
            snapshot_ts=snapshot_ts,
            request_ts=request_ts,
            recv_ts=recv_ts,
            status=status,
            body=body,
        )


def _summarise(results: list[PublishResult]) -> str:
    counts = {r.value: results.count(r) for r in PublishResult if r in results}
    return ", ".join(f"{n} {k}" for k, n in counts.items()) or "nothing to fetch"
