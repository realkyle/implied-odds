from __future__ import annotations

import pytest
from ppm.monitor.service import Monitor, build_watches, header
from ppm.monitor.watch import Gap, Recovered, RedisUnreachable, Silent
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
