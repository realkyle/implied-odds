"""The `ppm monitor` service: peek at every venue stream and feed the `VenueWatch`es.

It reads the same Redis streams as the writer but with plain `XREAD`, which is read-only:
unlike `XREADGROUP` it doesn't touch the writer's consumer group (no pending entries, no
acks). It is a separate process on purpose: a dead recorder can't report its own death,
and inside the writer, writer lag would look like every venue going silent.

    recorders --XADD--> ppm:{venue} --XREADGROUP--> writer   (unchanged)
                             \\--XREAD (peek)--> monitor --> VenueWatch --> Notifier
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping

from redis import exceptions as redis_errors
from redis.asyncio import Redis

from ppm.monitor.notify import Notifier
from ppm.monitor.watch import Event, Recovered, RedisUnreachable, VenueWatch
from ppm.pipeline.bus import STREAM_PREFIX, stream_name
from ppm.pipeline.clock import Clock, now_ms
from ppm.pipeline.envelope import ENVELOPE_VERSION

log = logging.getLogger("ppm.monitor")

_REDIS_DOWN = (redis_errors.ConnectionError, redis_errors.TimeoutError, OSError)


def build_watches(
    ticks: Mapping[str, int], silent_ticks: float, failing_ticks: int, started_ms: int
) -> list[VenueWatch]:
    """One watch per venue; thresholds scale with each venue's tick length."""
    return [
        VenueWatch(venue, tick, int(silent_ticks * tick), failing_ticks, started_ms)
        for venue, tick in sorted(ticks.items())
    ]


def header(fields: Mapping[bytes, bytes]) -> tuple[int, bool] | None:
    """`(snapshot_ts, ok)` from a stream message's header fields, without decompressing
    the body. None if the message isn't a readable envelope."""
    try:
        if int(fields[b"v"]) != ENVELOPE_VERSION:
            return None
        return int(fields[b"snapshot_ts"]), fields[b"status"] == b"200"
    except (KeyError, ValueError):
        return None


class Monitor:
    def __init__(
        self,
        redis: Redis,
        watches: Iterable[VenueWatch],
        notifier: Notifier,
        *,
        clock: Clock = now_ms,
        block_ms: int | None = 5_000,  # None: don't block (tests)
        batch: int = 500,
        redis_alert_after_ms: int = 60_000,
        retry_delay_s: float = 2.0,
        max_retry_delay_s: float = 10.0,
        replay_grace_ms: int = 30_000,
    ) -> None:
        self._redis = redis
        self._watches = {w.venue: w for w in watches}
        self._notifier = notifier
        self._clock = clock
        self._block_ms = block_ms
        self._batch = batch
        self._redis_alert_after_ms = redis_alert_after_ms
        self._retry_delay_s = retry_delay_s
        self._max_retry_delay_s = max_retry_delay_s
        self._replay_grace_ms = replay_grace_ms
        self._last_ids: dict[str, str] = {}
        self._down_since: int | None = None
        self._down_alerted = False

    async def seed(self) -> None:
        """Start each stream after its newest message and remember that message's tick."""
        for venue, watch in self._watches.items():
            newest = await self._redis.xrevrange(stream_name(venue), "+", "-", count=1)
            if not newest:
                self._last_ids[venue] = "0-0"  # empty stream: read from the beginning
                continue
            message_id, fields = newest[0]
            self._last_ids[venue] = _text(message_id)
            if (h := header(fields)) is not None:
                watch.seed(h[0])
        log.info(
            "monitor seeded: %s",
            ", ".join(f"{v} last={w.last_tick}" for v, w in self._watches.items()),
        )

    async def read(self) -> list[Event]:
        """One blocking XREAD across every stream; feed each message to its watch."""
        response = await self._redis.xread(
            {stream_name(v): i for v, i in self._last_ids.items()},
            count=self._batch,
            block=self._block_ms,
        )
        events: list[Event] = []
        for stream, messages in response or []:
            venue = _text(stream).removeprefix(STREAM_PREFIX)
            for message_id, fields in messages:
                self._last_ids[venue] = _text(message_id)
                h = header(fields)
                if h is None:
                    log.warning("unreadable message %s on %s", _text(message_id), venue)
                    continue
                events += self._watches[venue].saw(h[0], h[1], self._clock())
        return events

    def check(self) -> list[Event]:
        now = self._clock()
        return [e for w in self._watches.values() for e in w.check(now)]

    async def step(self) -> list[Event]:
        """Seed if needed, read once, then judge silence. Raises if Redis is down."""
        if len(self._last_ids) < len(self._watches):
            await self.seed()
        events = await self.read()
        # Redis handling goes before `check`: right after an outage the streams look
        # stale until the recorders replay their spool, which isn't a venue going silent.
        return self._redis_ok() + events + self.check()

    async def poll_once(self) -> tuple[list[Event], bool]:
        """One `step`, turning a Redis failure into (maybe) an alert. Returns the events
        and whether Redis answered."""
        try:
            return await self.step(), True
        except _REDIS_DOWN as exc:
            log.warning("redis unavailable (%r)", exc)
            return self._redis_failed(exc), False
        except Exception:  # a bug or odd reply must not end alerting; retry after backoff
            log.exception("monitor step failed")
            return [], False

    async def run_forever(self, stop: asyncio.Event) -> None:
        log.info("monitor started: %s", ", ".join(sorted(self._watches)))
        failures = 0
        while not stop.is_set():
            events, ok = await self.poll_once()
            for event in events:
                try:
                    await self._notifier.notify(event)
                except Exception:
                    log.exception("notifier failed for %s", event.format())
            if ok:
                failures = 0
                continue
            # Capped exponential backoff, like the writer (but capped at 10 s, so the
            # 60 s "unreachable" alert and the recovery are both noticed promptly).
            failures += 1
            delay = min(self._retry_delay_s * 2 ** (failures - 1), self._max_retry_delay_s)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass
        log.info("monitor stopped")

    def _redis_failed(self, exc: BaseException) -> list[Event]:
        now = self._clock()
        if self._down_since is None:
            self._down_since = now
        down_for = now - self._down_since
        if not self._down_alerted and down_for >= self._redis_alert_after_ms:
            self._down_alerted = True
            return [RedisUnreachable(down_for, type(exc).__name__)]
        return []

    def _redis_ok(self) -> list[Event]:
        if self._down_since is None:
            return []
        now = self._clock()
        events: list[Event] = []
        if self._down_alerted:
            events.append(Recovered("redis", "unreachable", now - self._down_since))
            # Only after an outage long enough to alert: shorter ones can't make a stream
            # look 2.5 ticks stale, and deferring on every blip would hide real silence.
            for w in self._watches.values():
                w.defer_silence(now + w.tick_ms + self._replay_grace_ms)
        self._down_since, self._down_alerted = None, False
        return events


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value
