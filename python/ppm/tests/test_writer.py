from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from ppm.pipeline.bus import DiskSpool, RedisBus, StreamConsumer, stream_name
from ppm.tests.conftest import fixture_bytes, make_envelope
from ppm.writer.storage import DataStore
from ppm.writer.writer import Writer

pytestmark = pytest.mark.anyio


CHAIN_ROWS = len(json.loads(fixture_bytes("deribit", "book_summary_BTC_option.json"))["result"])


def chain_env(**kw):
    return make_envelope(fixture_bytes("deribit", "book_summary_BTC_option.json"), **kw)


async def setup(redis, tmp_path):
    bus = RedisBus(redis, DiskSpool(tmp_path / "spool", 10**8), 1000)
    consumer = StreamConsumer(redis, ["deribit"])
    store = DataStore(tmp_path / "data")
    writer = Writer(consumer, store, retry_delay_s=0)
    await consumer.ensure_groups()
    return bus, consumer, store, writer


async def pending_count(redis) -> int:
    return (await redis.xpending(stream_name("deribit"), "writer"))["pending"]


async def test_end_to_end_writes_tables_raw_and_fetch_log_then_acks(redis, tmp_path):
    bus, _, store, writer = await setup(redis, tmp_path)
    env = chain_env()
    await bus.publish(env)

    assert await writer.run_once(block_ms=None) == 1
    table = pq.read_table(store.staging_path("deribit_options", env))
    assert table.num_rows == CHAIN_ROWS
    assert store.raw_path(env).exists()
    log = pq.read_table(store.staging_path("fetch_log", env)).to_pylist()[0]
    assert log["status"] == "200" and log["rows"] == CHAIN_ROWS and log["latency_ms"] == 240
    assert await pending_count(redis) == 0  # acked only after everything was written


async def test_redelivered_message_does_not_duplicate_rows(redis, tmp_path):
    bus, _, store, writer = await setup(redis, tmp_path)
    env = chain_env()
    await bus.publish(env)
    await bus.publish(env)  # e.g. spool replay after a crash between XADD and cleanup

    await writer.run_once(block_ms=None)
    files = list((tmp_path / "data" / "staging" / "deribit_options").rglob("*.parquet"))
    assert len(files) == 1 and pq.read_table(files[0]).num_rows == CHAIN_ROWS


async def test_crash_before_ack_is_recovered_by_a_new_writer(redis, tmp_path):
    bus, consumer, store, _ = await setup(redis, tmp_path)
    env = chain_env()
    await bus.publish(env)
    await consumer.read(block_ms=None)  # delivered; writer dies before processing/acking
    assert await pending_count(redis) == 1

    _, _, _, restarted = await setup(redis, tmp_path)
    assert await restarted.run_once(block_ms=None) == 1
    assert pq.read_table(store.staging_path("deribit_options", env)).num_rows == CHAIN_ROWS
    assert await pending_count(redis) == 0


async def test_failed_fetch_is_logged_as_a_gap_with_its_cause(redis, tmp_path):
    bus, _, store, writer = await setup(redis, tmp_path)
    env = make_envelope(b"ConnectTimeout: timed out", status="error")
    await bus.publish(env)
    await writer.run_once(block_ms=None)

    log = pq.read_table(store.staging_path("fetch_log", env)).to_pylist()[0]
    assert log["status"] == "error" and "ConnectTimeout" in log["error"] and log["rows"] is None
    assert not store.staging_path("deribit_options", env).exists()


async def test_garbage_payload_is_quarantined_and_acked(redis, tmp_path):
    bus, _, store, writer = await setup(redis, tmp_path)
    env = make_envelope(b"<html>maintenance</html>")
    await bus.publish(env)
    await writer.run_once(block_ms=None)

    assert store.quarantine_path("_unparsed", env).exists()
    assert store.raw_path(env).exists()  # raw kept: can be replayed after a parser fix
    assert await pending_count(redis) == 0  # doesn't block the queue forever


async def test_undecodable_message_is_quarantined(redis, tmp_path):
    _, _, _, writer = await setup(redis, tmp_path)
    await redis.xadd(stream_name("deribit"), {"v": "1", "nonsense": "x"})
    await writer.run_once(block_ms=None)
    assert list((tmp_path / "data" / "quarantine" / "_envelope").iterdir())
    assert await pending_count(redis) == 0


async def test_disk_error_leaves_message_unacked_for_retry(redis, tmp_path, monkeypatch):
    bus, _, store, writer = await setup(redis, tmp_path)
    await bus.publish(chain_env())

    def disk_full(*_args, **_kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(store, "write_raw", disk_full)
    assert await writer.run_once(block_ms=None) == 0
    assert await pending_count(redis) == 1

    monkeypatch.undo()  # disk space freed
    assert await writer.run_once(block_ms=None) == 1
    assert await pending_count(redis) == 0


class FlakyConsumer:
    """Fails like a dropped Redis connection a few times, then stops the writer."""

    def __init__(self, failures: int, stop):
        self.failures = failures
        self.reads = 0
        self.rewinds = 0
        self.stop = stop

    async def ensure_groups(self):
        pass

    async def read(self, **_kw):
        self.reads += 1
        if self.reads <= self.failures:
            from redis import exceptions

            raise exceptions.ConnectionError("Connection reset by peer")
        self.stop.set()
        return []

    def rewind(self):
        self.rewinds += 1


async def test_writer_survives_redis_outages(tmp_path):
    import asyncio

    stop = asyncio.Event()
    consumer = FlakyConsumer(failures=3, stop=stop)
    writer = Writer(consumer, DataStore(tmp_path), retry_delay_s=0.001)
    await asyncio.wait_for(writer.run_forever(stop), timeout=5)
    assert consumer.reads == 4  # three failures, then a successful read
    assert consumer.rewinds == 3  # pending messages re-read after each reconnect
