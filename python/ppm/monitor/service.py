"""The `ppm monitor` service: peek at every venue stream and feed the `VenueWatch`es.

It reads the same Redis streams as the writer but with plain `XREAD`, which is read-only:
unlike `XREADGROUP` it doesn't touch the writer's consumer group (no pending entries, no
acks). It is a separate process on purpose: a dead recorder can't report its own death,
and inside the writer, writer lag would look like every venue going silent.

    recorders --XADD--> ppm:{venue} --XREADGROUP--> writer   (unchanged)
                             \\--XREAD (peek)--> monitor --> VenueWatch --> Notifier
                              \\--XINFO/XPENDING (group state)--> WriterWatch --/

Peeking only proves the venues publish. Whether the writer takes the messages off is a
separate check (`writer_backlog`): it reads the writer's consumer-group state, also
read-only, and judges the age of the oldest message the writer hasn't finished.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping, Sequence

from redis import exceptions as redis_errors
from redis.asyncio import Redis

from ppm.monitor.notify import Notifier
from ppm.monitor.watch import (
    Backlog,
    Event,
    Recovered,
    RedisUnreachable,
    VenueWatch,
    WriterWatch,
)
from ppm.pipeline.bus import STREAM_PREFIX, WRITER_GROUP, stream_name
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


async def writer_backlog(redis: Redis, venues: Iterable[str], group: str = WRITER_GROUP) -> Backlog:
    """The writer's worst backlog: the stream whose oldest unfinished message is oldest.

    Per stream, the oldest unfinished message is the older of
    - the oldest *pending* one (handed to the writer, never acked): `XPENDING` summary;
    - the first *undelivered* one (after the group's cursor, `last-delivered-id`):
      `XRANGE (cursor + COUNT 1`.
    Its age is Redis `TIME` minus the millisecond part of its ID. A missing stream is
    skipped; a stream without the writer's group (writer never ran) counts from `0-0`.
    Raises the usual connection errors if Redis is down.
    """
    streams = [stream_name(v) for v in venues]
    # Two round trips: the XRANGE needs each group's cursor from the first.
    pipe = redis.pipeline(transaction=False)
    pipe.time()
    for s in streams:
        pipe.xinfo_groups(s)
        pipe.xpending(s, group)
    # raise_on_error=False: a missing stream (or group) comes back as a ResponseError
    # in its slot instead of failing the whole check. Connection errors still raise.
    replies = await pipe.execute(raise_on_error=False)
    seconds, micros = replies[0]
    now = int(seconds) * 1000 + int(micros) // 1000

    tracked: list[tuple[str, str, int, int | None, str | None]] = []
    for i, s in enumerate(streams):
        groups, summary = replies[1 + 2 * i], replies[2 + 2 * i]
        if isinstance(groups, redis_errors.ResponseError):
            continue  # no such stream yet: nothing queued for the writer
        g = next((g for g in groups if _text(g["name"]) == group), None)
        if g is None:
            tracked.append((s, "0-0", 0, None, None))
            continue
        oldest_pending = None
        if not isinstance(summary, redis_errors.ResponseError) and summary.get("min"):
            oldest_pending = _text(summary["min"])
        tracked.append(
            (s, _text(g["last-delivered-id"]), int(g["pending"]), g.get("lag"), oldest_pending)
        )

    pipe = redis.pipeline(transaction=False)
    for s, cursor, *_ in tracked:
        pipe.xrange(s, f"({cursor}", "+", count=1)
    firsts = await pipe.execute() if tracked else []

    worst = Backlog(stream="none", age_ms=0, pending=0, undelivered=0, now_ms=now)
    for (s, _, pending, lag, oldest_pending), first in zip(tracked, firsts, strict=True):
        ids = [oldest_pending] if oldest_pending else []
        if first:
            ids.append(_text(first[0][0]))
        # max(0, ...): an entry added after our TIME reading would come out negative.
        age = max(0, now - min(_id_ms(i) for i in ids)) if ids else 0
        if age > worst.age_ms or worst.stream == "none":
            worst = Backlog(s, age, pending, None if lag is None else int(lag), now)
    return worst


class Monitor:
    def __init__(
        self,
        redis: Redis,
        watches: Iterable[VenueWatch],
        notifier: Notifier,
        *,
        writer_watch: WriterWatch | None = None,
        writer_venues: Sequence[str] = (),
        writer_check_every_ms: int = 5_000,
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
        self._writer_watch = writer_watch
        self._writer_venues = list(writer_venues)
        self._writer_check_every_ms = writer_check_every_ms
        self._writer_checked_at: int | None = None

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

    async def fetch_writer_backlog(self) -> Backlog | None:
        """The writer's backlog, at most every `writer_check_every_ms` (the loop spins
        faster whenever messages arrive); None when skipped. Redis connection errors
        propagate so `poll_once` handles them like any outage; any other failure is
        logged and skipped, so a bug here can't stop the venue checks."""
        if self._writer_watch is None:
            return None
        now = self._clock()
        last = self._writer_checked_at
        # `now < last`: the host clock stepped back; check now rather than stall.
        if last is not None and 0 <= now - last < self._writer_check_every_ms:
            return None
        self._writer_checked_at = now
        try:
            return await writer_backlog(self._redis, self._writer_venues)
        except _REDIS_DOWN:
            raise
        except Exception:
            log.exception("writer backlog check failed")
            return None

    async def step(self) -> list[Event]:
        """Seed if needed, fetch the writer's backlog, read once, then judge the writer
        and silence. Raises if Redis is down."""
        if len(self._last_ids) < len(self._watches):
            await self.seed()
        # Fetch before `read`, judge after it: a Redis failure in either raises before
        # any state changes. Judging first would lose the writer's ALERT/RECOVERED if
        # `read` then raised (the watch would already count as alerted); fetching after
        # `read` would lose the events `read` had taken off the streams.
        backlog = await self.fetch_writer_backlog()
        events = await self.read()
        writer_events = (
            self._writer_watch.observe(backlog)
            if self._writer_watch is not None and backlog is not None
            else []
        )
        # Redis handling goes before `check`: right after an outage the streams look
        # stale until the recorders replay their spool, which isn't a venue going silent.
        return self._redis_ok() + writer_events + events + self.check()

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


def _id_ms(message_id: str) -> int:
    """The millisecond part of a stream ID `<ms>-<seq>`: when Redis added it."""
    return int(message_id.split("-", 1)[0])
