from __future__ import annotations

import pytest
from ppm.monitor import service
from ppm.monitor.service import Monitor, build_watches, header, writer_backlog
from ppm.monitor.watch import Gap, Recovered, RedisUnreachable, Silent, WriterBehind, WriterWatch
from ppm.pipeline.bus import StreamConsumer, stream_name
from ppm.pipeline.clock import TICK_MS
from ppm.tests.conftest import make_envelope
from redis import exceptions as redis_errors

pytestmark = pytest.mark.anyio

T0 = 1_794_500_000_000 - 1_794_500_000_000 % TICK_MS


class FakeClock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


class Collect:
    def __init__(self) -> None:
        self.events: list = []

    async def notify(self, event) -> None:
        self.events.append(event)


async def publish(redis, venue: str, tick: int, status: str = "200") -> None:
    env = make_envelope(venue=venue, snapshot_ts=tick, status=status)
    await redis.xadd(stream_name(venue), env.to_fields())


def monitor(redis, clock, venues=("deribit", "kalshi"), **kw) -> Monitor:
    watches = build_watches({v: TICK_MS for v in venues}, 2.5, 3, clock())
    return Monitor(redis, watches, Collect(), clock=clock, block_ms=None, **kw)


def test_header_reads_tick_and_status_without_the_body():
    fields = {k.encode(): v for k, v in make_envelope(snapshot_ts=T0).to_fields().items()}
    assert header(fields) == (T0, True)
    assert header({**fields, b"status": b"error"}) == (T0, False)
    assert header({**fields, b"v": b"99"}) is None
    assert header({b"junk": b"1"}) is None


async def test_reads_every_stream_and_finds_a_gap(redis):
    clock = FakeClock(T0 + 1_000)
    m = monitor(redis, clock)
    await m.seed()
    for venue in ("deribit", "kalshi"):
        await publish(redis, venue, T0)
    await publish(redis, "kalshi", T0 + 2 * TICK_MS)
    clock.now = T0 + 2 * TICK_MS + 1_000
    events = await m.step()
    assert events == [Gap("kalshi", T0 + TICK_MS, T0 + TICK_MS, 1, TICK_MS + 1_000)]


async def test_seeding_finds_a_gap_that_spans_a_monitor_restart(redis):
    await publish(redis, "deribit", T0)
    await publish(redis, "deribit", T0 + TICK_MS)
    # Monitor (re)starts after the recorder missed T0+2 and T0+3.
    clock = FakeClock(T0 + 4 * TICK_MS)
    m = monitor(redis, clock, venues=("deribit",))
    await m.seed()
    await publish(redis, "deribit", T0 + 4 * TICK_MS)
    clock.now = T0 + 4 * TICK_MS + 500
    events = await m.step()
    gaps = [e for e in events if isinstance(e, Gap)]
    assert gaps == [Gap("deribit", T0 + 2 * TICK_MS, T0 + 3 * TICK_MS, 2, 2 * TICK_MS + 500)]
    # Old messages already on the stream were skipped, not re-judged.
    assert all(not isinstance(e, Gap) or e.first > T0 for e in events)


async def test_seeded_stale_stream_is_silent_straight_away(redis):
    await publish(redis, "deribit", T0)
    clock = FakeClock(T0 + 10 * TICK_MS)
    m = monitor(redis, clock, venues=("deribit",))
    events = await m.step()
    assert events == [Silent("deribit", T0, 10 * TICK_MS)]


async def test_peeking_leaves_the_writer_group_untouched(redis):
    consumer = StreamConsumer(redis, ["deribit"])
    await consumer.ensure_groups()
    for i in range(3):
        await publish(redis, "deribit", T0 + i * TICK_MS)
    await consumer.read(count=1, block_ms=None)  # writer holds one unacked message
    before = await redis.xinfo_groups(stream_name("deribit"))

    m = monitor(redis, FakeClock(T0 + 2 * TICK_MS), venues=("deribit",))
    await m.seed()
    await publish(redis, "deribit", T0 + 3 * TICK_MS)
    await m.step()

    after = await redis.xinfo_groups(stream_name("deribit"))
    keep = ("name", "pending", "last-delivered-id", "consumers")
    assert [{k: g[k] for k in keep} for g in after] == [{k: g[k] for k in keep} for g in before]
    assert after[0]["pending"] == 1


class FlakyRedis:
    """Wraps fakeredis; raises ConnectionError while `down` is set."""

    def __init__(self, inner) -> None:
        self._inner = inner
        self.down = False

    def __getattr__(self, name):
        attr = getattr(self._inner, name)

        async def call(*a, **kw):
            if self.down:
                raise redis_errors.ConnectionError("Connection refused")
            return await attr(*a, **kw)

        return call


async def test_redis_outage_alerts_once_after_a_minute_then_recovers_without_false_silence(
    redis,
):
    flaky = FlakyRedis(redis)
    await publish(redis, "deribit", T0)
    clock = FakeClock(T0 + 1_000)
    m = monitor(flaky, clock, venues=("deribit",))
    assert await m.poll_once() == ([], True)

    flaky.down = True
    events = []
    for t in (5_000, 30_000, 64_999, 65_000, 120_000, 300_000):  # failed attempts
        clock.now = T0 + t
        got, ok = await m.poll_once()
        assert not ok
        events += got
    assert events == [RedisUnreachable(60_000, "ConnectionError")]  # down since +5 s

    # Back up 5 min later: the stream is stale until recorders replay their spool.
    flaky.down = False
    clock.now = T0 + 310_000
    assert await m.poll_once() == (
        [Recovered("redis", "unreachable", 305_000)],
        True,
    )
    clock.now = T0 + 6 * TICK_MS - 1  # still before the replay: no false silence
    assert await m.poll_once() == ([], True)
    # Replay arrives at the next tick: contiguous ticks, no gap, no silence.
    for i in range(1, 7):
        await publish(redis, "deribit", T0 + i * TICK_MS)
    clock.now = T0 + 6 * TICK_MS + 1_000
    assert await m.poll_once() == ([], True)


async def test_short_redis_blip_is_not_an_alert(redis):
    flaky = FlakyRedis(redis)
    clock = FakeClock(T0)
    m = monitor(flaky, clock, venues=("deribit",))
    flaky.down = True
    for t in (0, 30_000, 59_999):
        clock.now = T0 + t
        assert await m.poll_once() == ([], False)
    flaky.down = False
    clock.now = T0 + 61_000
    assert await m.poll_once() == ([], True)


async def test_run_forever_sends_events_to_the_notifier_and_stops(redis):
    import asyncio

    await publish(redis, "deribit", T0)
    clock = FakeClock(T0 + 10 * TICK_MS)
    notes = Collect()
    watches = build_watches({"deribit": TICK_MS}, 2.5, 3, clock())
    m = Monitor(redis, watches, notes, clock=clock, block_ms=10)
    stop = asyncio.Event()
    task = asyncio.create_task(m.run_forever(stop))
    for _ in range(100):
        if notes.events:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert notes.events == [Silent("deribit", T0, 10 * TICK_MS)]


async def test_short_blip_does_not_postpone_real_silence(redis):
    flaky = FlakyRedis(redis)
    await publish(redis, "deribit", T0)
    clock = FakeClock(T0 + 1_000)
    m = monitor(flaky, clock, venues=("deribit",))
    await m.poll_once()
    flaky.down = True
    clock.now = T0 + 100_000
    await m.poll_once()
    flaky.down = False
    clock.now = T0 + 150_000  # 2.5 ticks after the last tick: silence must not be deferred
    assert await m.poll_once() == ([Silent("deribit", T0, 150_000)], True)


class BrokenRedis(FlakyRedis):
    async def xread(self, *a, **kw):
        raise redis_errors.ResponseError("WRONGTYPE Operation against a key")


async def test_an_unexpected_error_does_not_stop_the_monitor(redis, caplog):
    m = monitor(BrokenRedis(redis), FakeClock(T0), venues=("deribit",))
    assert await m.poll_once() == ([], False)
    assert "monitor step failed" in caplog.text


async def test_a_raising_notifier_does_not_stop_the_loop(redis):
    import asyncio

    class Boom:
        calls = 0

        async def notify(self, event) -> None:
            Boom.calls += 1
            raise RuntimeError("boom")

    await publish(redis, "deribit", T0)
    await publish(redis, "kalshi", T0)
    clock = FakeClock(T0 + 10 * TICK_MS)
    watches = build_watches({"deribit": TICK_MS, "kalshi": TICK_MS}, 2.5, 3, clock())
    m = Monitor(redis, watches, Boom(), clock=clock, block_ms=10)
    stop = asyncio.Event()
    task = asyncio.create_task(m.run_forever(stop))
    for _ in range(100):
        if Boom.calls >= 2:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert Boom.calls == 2  # both alerts attempted although the first raised


# --- Writer lag ----------------------------------------------------------------------------
# fakeredis's TIME is the real clock, so these tests stamp message IDs relative to it and
# allow a little slack for the time the test itself takes.

LAG = 5 * TICK_MS
SLACK = 2_000


async def redis_now(redis) -> int:
    seconds, micros = await redis.time()
    return seconds * 1000 + micros // 1000


async def add_aged(redis, venue: str, age_ms: int, now: int) -> str:
    """XADD with an ID stamped `age_ms` before `now`, as if Redis had added it then."""
    env = make_envelope(venue=venue, snapshot_ts=T0)
    return (await redis.xadd(stream_name(venue), env.to_fields(), id=f"{now - age_ms}-0")).decode()


async def test_backlog_is_zero_when_the_writer_has_acked_everything(redis):
    now = await redis_now(redis)
    await add_aged(redis, "deribit", 600_000, now)
    consumer = StreamConsumer(redis, ["deribit"])
    await consumer.ensure_groups()
    for d in await consumer.read(block_ms=None):
        await consumer.ack(d)
    b = await writer_backlog(redis, ["deribit"])
    assert (b.stream, b.age_ms, b.pending, b.undelivered) == ("ppm:deribit", 0, 0, 0)


async def test_backlog_ages_an_undelivered_message_like_a_paused_writer(redis):
    now = await redis_now(redis)
    await StreamConsumer(redis, ["deribit"]).ensure_groups()
    await add_aged(redis, "deribit", 400_000, now)
    await add_aged(redis, "deribit", 100_000, now)
    b = await writer_backlog(redis, ["deribit"])
    assert 400_000 <= b.age_ms < 400_000 + SLACK  # the oldest of the two counts
    assert (b.pending, b.undelivered) == (0, 2)


async def test_backlog_ages_a_delivered_but_unacked_message_like_a_stuck_writer(redis):
    """The disk-full retry loop: delivered, never acked, newer ones still delivered."""
    now = await redis_now(redis)
    await add_aged(redis, "deribit", 500_000, now)
    consumer = StreamConsumer(redis, ["deribit"])
    await consumer.ensure_groups()
    assert len(await consumer.read(block_ms=None)) == 1  # delivered, not acked
    b = await writer_backlog(redis, ["deribit"])
    assert 500_000 <= b.age_ms < 500_000 + SLACK
    assert (b.pending, b.undelivered) == (1, 0)


async def test_backlog_counts_from_the_start_when_the_writer_never_made_its_group(redis):
    now = await redis_now(redis)
    await add_aged(redis, "kalshi", 700_000, now)
    b = await writer_backlog(redis, ["kalshi"])
    assert 700_000 <= b.age_ms < 700_000 + SLACK
    assert (b.stream, b.undelivered) == ("ppm:kalshi", None)


async def test_backlog_skips_missing_streams_and_reports_the_worst_one(redis):
    now = await redis_now(redis)
    await StreamConsumer(redis, ["deribit", "kalshi"]).ensure_groups()
    await add_aged(redis, "deribit", 100_000, now)
    await add_aged(redis, "kalshi", 900_000, now)
    b = await writer_backlog(redis, ["deribit", "kalshi", "polymarket"])  # polymarket: none
    assert b.stream == "ppm:kalshi" and 900_000 <= b.age_ms < 900_000 + SLACK
    assert (await writer_backlog(redis, ["polymarket"])).age_ms == 0


async def test_backlog_check_does_not_touch_the_writers_group(redis):
    now = await redis_now(redis)
    await StreamConsumer(redis, ["deribit"]).ensure_groups()
    await add_aged(redis, "deribit", 400_000, now)
    before = await redis.xinfo_groups(stream_name("deribit"))
    await writer_backlog(redis, ["deribit"])
    assert await redis.xinfo_groups(stream_name("deribit")) == before


def writer_monitor(redis, clock, **kw) -> Monitor:
    return monitor(
        redis,
        clock,
        venues=("deribit",),
        writer_watch=WriterWatch(LAG, 12 * 3_600_000),
        writer_venues=["deribit"],
        **kw,
    )


def writer_events(events) -> list:
    return [e for e in events if isinstance(e, WriterBehind) or getattr(e, "venue", "") == "writer"]


async def test_monitor_alerts_once_on_writer_lag_then_recovers_and_throttles_checks(redis):
    now = await redis_now(redis)
    consumer = StreamConsumer(redis, ["deribit"])
    await consumer.ensure_groups()
    await add_aged(redis, "deribit", LAG + 10_000, now)
    clock = FakeClock(T0)
    m = writer_monitor(redis, clock)

    first = writer_events(await m.step())
    assert len(first) == 1 and isinstance(first[0], WriterBehind)
    assert first[0].stream == "ppm:deribit" and first[0].undelivered == 1

    # Writer catches up, but the next check is throttled to 5 s after the last one.
    for d in await consumer.read(block_ms=None):
        await consumer.ack(d)
    clock.now = T0 + 4_999
    assert writer_events(await m.step()) == []
    clock.now = T0 + 5_000
    back = writer_events(await m.step())
    assert len(back) == 1 and back[0].kind == "writer-lag"
    assert LAG + 10_000 <= back[0].down_for_ms < LAG + 10_000 + SLACK


class FlakyPipelineRedis(FlakyRedis):
    """FlakyRedis whose pipelines fail at `execute()` while down, which is where
    redis-py raises a connection error (even with `raise_on_error=False`)."""

    def pipeline(self, *a, **kw):
        pipe = self._inner.pipeline(*a, **kw)
        real_execute = pipe.execute

        async def execute(*ea, **ekw):
            if self.down:
                raise redis_errors.ConnectionError("Connection refused")
            return await real_execute(*ea, **ekw)

        pipe.execute = execute
        return pipe


class DropsOnRead(FlakyPipelineRedis):
    """Up for the writer check, down for the XREAD right after it, once."""

    def __init__(self, inner) -> None:
        super().__init__(inner)
        self.drop_next_read = False

    async def xread(self, *a, **kw):
        if self.drop_next_read:
            self.drop_next_read = False
            raise redis_errors.ConnectionError("Connection reset")
        return await self._inner.xread(*a, **kw)


async def test_writer_alert_is_not_lost_when_redis_drops_between_check_and_read(redis):
    """Review finding: judging before `read` set the watch to "behind" and then lost the
    ALERT when `read` raised, so it was never sent. Judging after `read` keeps it."""
    now = await redis_now(redis)
    await StreamConsumer(redis, ["deribit"]).ensure_groups()
    await add_aged(redis, "deribit", LAG + 10_000, now)
    flaky = DropsOnRead(redis)
    clock = FakeClock(T0)
    m = writer_monitor(flaky, clock)
    await m.seed()
    flaky.drop_next_read = True
    events, ok = await m.poll_once()
    assert not ok and writer_events(events) == []
    clock.now = T0 + 5_000
    events, ok = await m.poll_once()
    assert ok and [type(e) for e in writer_events(events)] == [WriterBehind]


async def test_a_backward_clock_step_does_not_stall_the_writer_check(redis):
    now = await redis_now(redis)
    await StreamConsumer(redis, ["deribit"]).ensure_groups()
    clock = FakeClock(T0)
    m = writer_monitor(redis, clock)
    assert writer_events(await m.step()) == []  # caught up; checked at T0
    await add_aged(redis, "deribit", LAG + 10_000, now)
    clock.now = T0 - 3_600_000  # NTP stepped the host clock back an hour
    assert [type(e) for e in writer_events(await m.step())] == [WriterBehind]


async def test_redis_outage_freezes_the_writer_watch(redis):
    """While Redis is unreachable the monitor can't judge the writer: no writer ALERT and
    no false RECOVERED, just the Redis alert. Judging resumes when Redis is back."""
    now = await redis_now(redis)
    await StreamConsumer(redis, ["deribit"]).ensure_groups()
    await add_aged(redis, "deribit", LAG + 10_000, now)
    flaky = FlakyPipelineRedis(redis)
    clock = FakeClock(T0)
    m = writer_monitor(flaky, clock)
    events, ok = await m.poll_once()
    assert ok and len(writer_events(events)) == 1

    flaky.down = True
    seen = []
    for t in range(5_000, 125_000, 5_000):
        clock.now = T0 + t
        got, ok = await m.poll_once()
        assert not ok
        seen += got
    assert writer_events(seen) == []
    assert [type(e) for e in seen] == [RedisUnreachable]

    flaky.down = False
    clock.now = T0 + 130_000
    events, ok = await m.poll_once()
    assert ok and writer_events(events) == []  # still behind: no repeat, no recovery


async def test_a_bug_in_the_writer_check_does_not_stop_venue_monitoring(redis, monkeypatch):
    async def broken(*a, **kw):
        raise KeyError("odd reply")

    monkeypatch.setattr(service, "writer_backlog", broken)
    clock = FakeClock(T0 + 1_000)
    m = writer_monitor(redis, clock)
    await m.seed()
    await publish(redis, "deribit", T0)
    await publish(redis, "deribit", T0 + 2 * TICK_MS)
    clock.now = T0 + 2 * TICK_MS + 1_000
    events, ok = await m.poll_once()
    assert ok and [type(e) for e in events] == [Gap]
