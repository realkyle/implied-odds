from __future__ import annotations

import pytest
from ppm.pipeline.bus import DiskSpool, PublishResult, RedisBus, StreamConsumer, stream_name
from ppm.pipeline.envelope import Envelope
from redis import exceptions as redis_errors

pytestmark = pytest.mark.anyio


class FlakyRedis:
    """Wraps a fake Redis; while `down` is True every command fails like a lost connection."""

    def __init__(self, inner):
        self.inner = inner
        self.down = False

    async def xadd(self, *args, **kwargs):
        if self.down:
            raise redis_errors.ConnectionError("connection refused")
        return await self.inner.xadd(*args, **kwargs)


async def stream_envelopes(redis, venue="deribit") -> list[Envelope]:
    entries = await redis.xrange(stream_name(venue))
    return [Envelope.from_fields(fields) for _, fields in entries]


async def test_publish_goes_to_venue_stream(redis, tmp_path, make):
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**6), stream_maxlen=1000)
    env = make()
    assert await bus.publish(env) is PublishResult.PUBLISHED
    assert await stream_envelopes(redis) == [env]


async def test_redis_outage_spools_then_replays_in_order(redis, tmp_path, make):
    flaky = FlakyRedis(redis)
    spool = DiskSpool(tmp_path, 10**6)
    bus = RedisBus(flaky, spool, stream_maxlen=1000)
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
    await RedisBus(flaky, DiskSpool(tmp_path, 10**6), 1000).publish(make())

    # A new process starts with a fresh bus over the same spool directory.
    bus2 = RedisBus(redis, DiskSpool(tmp_path, 10**6), 1000)
    await bus2.publish(make(snapshot_ts=1_794_330_060_000))
    assert len(await stream_envelopes(redis)) == 2


async def test_full_spool_drops_and_reports_it(redis, tmp_path, make):
    flaky = FlakyRedis(redis)
    flaky.down = True
    bus = RedisBus(flaky, DiskSpool(tmp_path, max_bytes=10), stream_maxlen=1000)
    assert await bus.publish(make()) is PublishResult.DROPPED


async def test_unacked_messages_are_redelivered_after_a_crash(redis, tmp_path, make):
    bus = RedisBus(redis, DiskSpool(tmp_path, 10**6), 1000)
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
