from __future__ import annotations

import pytest
from ppm.pipeline.bus import DiskSpool, PublishResult, RedisBus, StreamConsumer, stream_name
from ppm.pipeline.clock import now_ms
from ppm.pipeline.envelope import Envelope
from redis import exceptions as redis_errors

pytestmark = pytest.mark.anyio

HOUR = 3_600_000


class FlakyRedis:
    """Wraps a fake Redis; while `down` is True every command fails like a lost connection.
    `trim_down` fails only XTRIM (connection lost between the XADD and the trim)."""

    def __init__(self, inner):
        self.inner = inner
        self.down = False
        self.trim_down = False

    async def xadd(self, *args, **kwargs):
        if self.down:
            raise redis_errors.ConnectionError("connection refused")
        return await self.inner.xadd(*args, **kwargs)

    async def xtrim(self, *args, **kwargs):
        if self.down or self.trim_down:
            raise redis_errors.ConnectionError("connection refused")
        return await self.inner.xtrim(*args, **kwargs)


async def stream_envelopes(redis, venue="deribit") -> list[Envelope]:
    entries = await redis.xrange(stream_name(venue))
    return [Envelope.from_fields(fields) for _, fields in entries]


async def test_publish_goes_to_venue_stream(redis, tmp_path, make):
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**6), retain_ms=HOUR)
    env = make()
    assert await bus.publish(env) is PublishResult.PUBLISHED
    assert await stream_envelopes(redis) == [env]


async def test_redis_outage_spools_then_replays_in_order(redis, tmp_path, make):
    flaky = FlakyRedis(redis)
    spool = DiskSpool(tmp_path, 10**6)
    bus = RedisBus(flaky, spool, retain_ms=HOUR)
    envs = [make(snapshot_ts=1_794_330_000_000 + i * 60_000) for i in range(3)]

    flaky.down = True
    assert [await bus.publish(e) for e in envs[:2]] == [PublishResult.SPOOLED] * 2
    assert len(spool) == 2 and await stream_envelopes(redis) == []

    flaky.down = False
    assert await bus.publish(envs[2]) is PublishResult.PUBLISHED
    assert len(spool) == 0
    assert await stream_envelopes(redis) == envs  # oldest first: arrival order preserved


async def test_spool_survives_a_process_restart(redis, tmp_path, make):
    flaky = FlakyRedis(redis)
    flaky.down = True
    await RedisBus(flaky, DiskSpool(tmp_path, 10**6), HOUR).publish(make())

    # A new process starts with a fresh bus over the same spool directory.
    bus2 = RedisBus(redis, DiskSpool(tmp_path, 10**6), HOUR)
    await bus2.publish(make(snapshot_ts=1_794_330_060_000))
    assert len(await stream_envelopes(redis)) == 2


async def test_full_spool_drops_and_reports_it(redis, tmp_path, make):
    flaky = FlakyRedis(redis)
    flaky.down = True
    bus = RedisBus(flaky, DiskSpool(tmp_path, max_bytes=10), retain_ms=HOUR)
    assert await bus.publish(make()) is PublishResult.DROPPED


async def test_unacked_messages_are_redelivered_after_a_crash(redis, tmp_path, make):
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**6), HOUR)
    await bus.publish(make())

    first = StreamConsumer(redis, ["deribit"])
    await first.ensure_groups()
    delivered = await first.read(block_ms=None)
    assert len(delivered) == 1
    # ...the writer "crashes" here, before acking.

    restarted = StreamConsumer(redis, ["deribit"])  # same consumer name, fresh process
    await restarted.ensure_groups()
    again = await restarted.read(block_ms=None)
    assert [d.message_id for d in again] == [delivered[0].message_id]

    await restarted.ack(again[0])
    assert await restarted.read(block_ms=None) == []


async def test_queue_drained_waits_for_unread_and_unacked_messages(redis, make):
    from ppm.pipeline.bus import queue_drained

    consumer = StreamConsumer(redis, venues=["deribit"])
    await consumer.ensure_groups()
    assert await queue_drained(redis, ["deribit", "kalshi"])  # kalshi stream doesn't exist
    await redis.xadd(stream_name("deribit"), make().to_fields())
    assert not await queue_drained(redis, ["deribit"])  # lag: not yet delivered
    (delivery,) = await consumer.read(block_ms=None)
    assert not await queue_drained(redis, ["deribit"])  # pending: delivered, not acked
    await consumer.ack(delivery)
    assert await queue_drained(redis, ["deribit"])


# --- retention window -----------------------------------------------------------------


async def _add_aged(redis, env, age_ms: int, venue="deribit") -> str:
    """Put `env` on the stream as if it had arrived `age_ms` ago (explicit stream ID)."""
    arrived = now_ms() - age_ms
    await redis.xadd(stream_name(venue), env.to_fields(), id=f"{arrived}-0")
    return f"{arrived}-0"


async def test_publish_trims_messages_older_than_the_window(redis, tmp_path, make):
    old, recent, new = (make(snapshot_ts=1_794_330_000_000 + i * 60_000) for i in range(3))
    await _add_aged(redis, old, age_ms=HOUR + 60_000)  # just past the window
    await _add_aged(redis, recent, age_ms=HOUR - 60_000)  # just inside it
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**6), retain_ms=HOUR)
    assert await bus.publish(new) is PublishResult.PUBLISHED
    assert await stream_envelopes(redis) == [recent, new]


async def test_trim_is_per_stream(redis, tmp_path, make):
    old_kalshi = make(venue="kalshi")
    await _add_aged(redis, old_kalshi, age_ms=2 * HOUR, venue="kalshi")
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**6), retain_ms=HOUR)
    await bus.publish(make())  # deribit: must not trim the kalshi stream
    assert await stream_envelopes(redis, "kalshi") == [old_kalshi]


async def test_unread_message_older_than_the_window_is_lost_and_reported(
    redis, tmp_path, make, caplog
):
    consumer = StreamConsumer(redis, ["deribit"])
    await consumer.ensure_groups()
    await _add_aged(redis, make(), age_ms=2 * HOUR)
    (stale,) = await consumer.read(block_ms=None)  # delivered, then the writer "crashes"

    bus = RedisBus(redis, DiskSpool(tmp_path, 10**6), retain_ms=HOUR)
    fresh = make(snapshot_ts=1_794_330_060_000)
    await bus.publish(fresh)  # trims the unacked message's data

    restarted = StreamConsumer(redis, ["deribit"])
    await restarted.ensure_groups()
    redelivered = await restarted.read(block_ms=None)
    assert [Envelope.from_fields(d.fields) for d in redelivered] == [fresh]
    assert "DATA LOSS" in caplog.text and stale.message_id in caplog.text
    # The lost entry was acked, so it is reported once, not on every read.
    await restarted.ack(redelivered[0])
    assert (await redis.xpending(stream_name("deribit"), "writer"))["pending"] == 0


async def test_replay_after_an_outage_longer_than_the_window_is_kept(redis, tmp_path, make):
    flaky = FlakyRedis(redis)
    spool = DiskSpool(tmp_path, 10**6)
    bus = RedisBus(flaky, spool, retain_ms=HOUR)
    spooled = make()
    flaky.down = True
    assert await bus.publish(spooled) is PublishResult.SPOOLED
    # Redis comes back hours later. The replayed envelope gets a fresh stream ID, so the
    # window counts from the replay, not from when it was first fetched.
    flaky.down = False
    later = make(snapshot_ts=spooled.snapshot_ts + 3 * HOUR)
    assert await bus.publish(later) is PublishResult.PUBLISHED
    assert await stream_envelopes(redis) == [spooled, later]


async def test_failed_trim_still_counts_as_published(redis, tmp_path, make):
    flaky = FlakyRedis(redis)
    spool = DiskSpool(tmp_path, 10**6)
    bus = RedisBus(flaky, spool, retain_ms=HOUR)
    await _add_aged(redis, make(), age_ms=2 * HOUR)
    flaky.trim_down = True
    env = make(snapshot_ts=1_794_330_060_000)
    assert await bus.publish(env) is PublishResult.PUBLISHED
    assert len(spool) == 0  # not spooled again: it is already on the stream
    flaky.trim_down = False
    await bus.publish(make(snapshot_ts=1_794_330_120_000))  # next publish does the trim
    assert len(await stream_envelopes(redis)) == 2


@pytest.mark.parametrize("bad", [0, -1])
def test_retention_must_be_positive(redis, tmp_path, bad):
    with pytest.raises(ValueError, match="retain_ms"):
        RedisBus(redis, DiskSpool(tmp_path, 10**6), retain_ms=bad)
