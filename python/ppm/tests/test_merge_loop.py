"""The hourly merge loop's decision (`cli._merge_when_drained`), against fake Redis."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import pytest
from ppm import cli
from ppm.pipeline.bus import StreamConsumer, stream_name
from redis import exceptions as redis_errors

pytestmark = pytest.mark.anyio
log = logging.getLogger("ppm.merge")


class FakeMerger:
    """Stands in for `DailyMerger`: `days` are pending until merged; `fail` makes every
    merge raise (e.g. a full disk). Reports nothing (the summary isn't under test)."""

    def __init__(self, days: list[str], fail: bool = False) -> None:
        self.days = list(days)
        self.fail = fail
        self.merged = 0

    def pending_days(self, now: int) -> list[str]:
        return list(self.days)

    def merge_due(self, now: int) -> list:
        if self.fail:
            raise OSError("No space left on device")
        self.merged += 1
        self.days = []
        return []


class DownRedis:
    """Every command fails as if Redis were unreachable."""

    async def xinfo_groups(self, *args, **kwargs):
        raise redis_errors.ConnectionError("connection refused")


OLD = "2026-01-01"  # months past its grace


def _overdue_alerts(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if "merge-overdue" in r.getMessage()]


@pytest.fixture
def fast_drain(monkeypatch):
    monkeypatch.setattr(cli, "DRAIN_WINDOW_MS", 300)
    monkeypatch.setattr(cli, "DRAIN_POLL_MS", 50)


async def _busy_queue(redis, make):
    consumer = StreamConsumer(redis, venues=["deribit"])
    await consumer.ensure_groups()
    await redis.xadd(stream_name("deribit"), make().to_fields())
    (delivery,) = await consumer.read(block_ms=None)
    return consumer, delivery


async def test_a_burst_at_the_check_still_merges(redis, make, fast_drain, caplog):
    consumer, delivery = await _busy_queue(redis, make)

    async def writer_finishes_the_burst():
        await asyncio.sleep(0.1)
        await consumer.ack(delivery)

    task = asyncio.create_task(writer_finishes_the_burst())
    merger = FakeMerger([OLD])
    with caplog.at_level(logging.INFO, logger="ppm.merge"):
        await cli._merge_hour(merger, redis, asyncio.Event(), log)
    await task
    assert merger.merged == 1
    assert "queue drained after" in caplog.text
    assert _overdue_alerts(caplog) == []  # merged: nothing left to flag


async def test_a_stuck_queue_skips_and_flags_only_overdue_days(redis, make, fast_drain, caplog):
    await _busy_queue(redis, make)  # never acked
    fresh = datetime.now(UTC).date().isoformat()  # today: grace not over, never overdue
    merger = FakeMerger([OLD, fresh])
    with caplog.at_level(logging.INFO, logger="ppm.merge"):
        await cli._merge_hour(merger, redis, asyncio.Event(), log)
    assert merger.merged == 0
    assert "still had queued messages" in caplog.text
    alerts = _overdue_alerts(caplog)
    assert len(alerts) == 1 and f"day={OLD}" in alerts[0]


async def test_redis_down_still_flags_an_overdue_day(caplog):
    """Review finding: a Redis error used to end the hour as just `merge failed`."""
    merger = FakeMerger([OLD])
    with caplog.at_level(logging.INFO, logger="ppm.merge"):
        await cli._merge_hour(merger, DownRedis(), asyncio.Event(), log)
    assert merger.merged == 0
    assert "merge failed" in caplog.text
    assert len(_overdue_alerts(caplog)) == 1


async def test_a_failing_merge_still_flags_an_overdue_day(redis, caplog):
    merger = FakeMerger([OLD], fail=True)  # queue empty, but every merge raises
    with caplog.at_level(logging.INFO, logger="ppm.merge"):
        await cli._merge_hour(merger, redis, asyncio.Event(), log)
    assert "merge failed" in caplog.text
    assert len(_overdue_alerts(caplog)) == 1


async def test_stop_during_the_wait_neither_merges_nor_alerts(redis, make, caplog):
    await _busy_queue(redis, make)
    stop = asyncio.Event()
    stop.set()
    merger = FakeMerger([OLD])
    with caplog.at_level(logging.INFO, logger="ppm.merge"):
        await cli._merge_hour(merger, redis, stop, log)
    assert merger.merged == 0
    assert _overdue_alerts(caplog) == []


async def test_stop_set_as_the_queue_drains_does_not_start_a_merge(redis, caplog):
    """Docker allows 30 s after SIGTERM; a merge takes ~150 s on the VM."""
    stop = asyncio.Event()
    stop.set()
    merger = FakeMerger([OLD])  # queue already empty: drained on the first check
    await cli._merge_hour(merger, redis, stop, log)
    assert merger.merged == 0
