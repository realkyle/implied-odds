"""Command-line entry points.

ppm record <venue>    run one venue recorder (one OS process per venue)
ppm writer            run the Parquet writer
ppm merge [--loop]    merge finished days into hash-chained daily files
ppm verify            check the manifest hash chain and every file hash
ppm faulttest         inject faults into an isolated Compose stack; prove 0 lost / 0 duplicated
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time

import httpx
from redis.asyncio import Redis

from ppm.config import Settings
from ppm.faulttest.runner import SCENARIOS as FAULT_SCENARIOS
from ppm.faulttest.runner import main as faulttest_main
from ppm.integrity.manifest import Manifest
from ppm.pipeline.bus import DiskSpool, RedisBus, StreamConsumer, queue_drained
from ppm.pipeline.clock import floor_tick, next_tick, now_ms, sleep_until
from ppm.recorders.base import Recorder
from ppm.recorders.deribit import DeribitRecorder
from ppm.recorders.kalshi import KalshiRecorder
from ppm.recorders.polymarket import PolymarketRecorder
from ppm.recorders.reference import ReferenceRecorder
from ppm.recorders.settlements import SettlementRecorder
from ppm.writer.merge import DailyMerger
from ppm.writer.storage import DataStore
from ppm.writer.writer import Writer

RECORDERS: dict[str, type[Recorder]] = {
    r.venue: r
    for r in (
        DeribitRecorder,
        KalshiRecorder,
        PolymarketRecorder,
        ReferenceRecorder,
        SettlementRecorder,
    )
}


def _configure_logging() -> None:
    logging.Formatter.converter = time.gmtime  # log timestamps in UTC, like the data
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)sZ %(levelname)s %(name)s: %(message)s"
    )
    # httpx logs every request at INFO (thousands of lines a day); failures are
    # already logged by the recorder with their cause.
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _stop_event() -> asyncio.Event:
    """An event set on Ctrl-C or SIGTERM, so processes finish the current step cleanly."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    return stop


async def _record(venue: str, settings: Settings) -> None:
    stop = _stop_event()
    redis = Redis.from_url(settings.redis_url, socket_timeout=5, socket_connect_timeout=5)
    spool = DiskSpool(settings.spool_dir / venue, settings.spool_max_bytes)
    bus = RedisBus(redis, spool, settings.stream_maxlen)
    headers = {"User-Agent": "ppm-recorder/0.1 (research, read-only)"}
    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        await RECORDERS[venue](client, bus).run_forever(stop)
    await redis.aclose()


async def _write(settings: Settings) -> None:
    stop = _stop_event()
    # The socket timeout must exceed the consumer's blocking-read time (5 s). Without an
    # explicit value redis-py 8.1 aborted 5 s blocking reads with TimeoutError (2026-11-10).
    redis = Redis.from_url(settings.redis_url, socket_timeout=30, socket_connect_timeout=5)
    consumer = StreamConsumer(redis, venues=sorted(RECORDERS))
    await Writer(consumer, DataStore(settings.data_dir)).run_forever(stop)
    await redis.aclose()


MERGE_EVERY_MS = 3_600_000
# Run a few minutes past the hour, away from the busiest moment (every recorder at :00).
MERGE_OFFSET_MS = 7 * 60_000


async def _merge(settings: Settings, day: str | None, loop: bool) -> None:
    merger = DailyMerger(settings.data_dir)
    if day:
        merger.merge_day(day)
        return
    if not loop:
        merger.merge_due(now_ms())
        return
    stop = _stop_event()
    redis = Redis.from_url(settings.redis_url, socket_timeout=5, socket_connect_timeout=5)
    log = logging.getLogger("ppm.merge")
    log.info("merger started")
    run_at = floor_tick(now_ms(), MERGE_EVERY_MS) + MERGE_OFFSET_MS  # also once at start
    while not stop.is_set():
        if now_ms() >= run_at:
            try:
                due = merger.pending_days(now_ms())
                if due and not await queue_drained(redis, sorted(RECORDERS)):
                    log.info("days %s due, but the writer still has queued messages", due)
                elif due:
                    for report in merger.merge_due(now_ms()):
                        log.info("merged %s: %s", report.day, _merge_summary(report))
            except Exception:  # one failed merge must not kill the service; retry next hour
                log.exception("merge failed")
            run_at = next_tick(now_ms(), MERGE_EVERY_MS) + MERGE_OFFSET_MS
        await sleep_until(min(run_at, now_ms() + 60_000), stop)
    await redis.aclose()


def _merge_summary(report) -> str:
    files = sum(t["files_in"] for t in report.tables.values()) + sum(
        r["files_in"] for r in report.raw.values()
    )
    out = sum(t["bytes_out"] for t in report.tables.values()) + sum(
        r["bytes_out"] for r in report.raw.values()
    )
    return (
        f"{files} files -> {len(report.entries)} files ({out / 1e6:.1f} MB), {report.seconds:.1f} s"
    )


def _verify(settings: Settings, expect_head: str | None) -> int:
    manifest = Manifest(settings.data_dir)
    problems = manifest.verify(expect_head=expect_head)
    for p in problems:
        print(f"FAIL seq={p.seq} path={p.path}: {p.message}")
    entries = sum(1 for _ in manifest.entries())
    if problems:
        print(f"VERIFICATION FAILED: {len(problems)} problem(s) in {entries} entries")
        return 1
    print(f"verified {entries} entries; head {manifest.head()}")
    return 0


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="ppm")
    sub = ap.add_subparsers(dest="command", required=True)
    rec = sub.add_parser("record", help="run a venue recorder")
    rec.add_argument("venue", choices=sorted(RECORDERS))
    sub.add_parser("writer", help="run the Parquet writer")
    mrg = sub.add_parser("merge", help="merge finished days into daily files")
    mrg.add_argument("--date", help="merge this day (YYYY-MM-DD) now, ignoring the grace period")
    mrg.add_argument("--loop", action="store_true", help="run hourly as a service")
    ver = sub.add_parser("verify", help="verify the manifest hash chain and file hashes")
    ver.add_argument("--expect-head", help="fail unless the chain ends at this value")
    flt = sub.add_parser("faulttest", help="failure test on an isolated ppmtest Compose stack")
    flt.add_argument(
        "--scenario",
        action="append",
        choices=FAULT_SCENARIOS,
        help="run only this scenario (repeatable; default: all)",
    )
    flt.add_argument("--outage-s", type=float, default=300.0, help="Redis/network outage length")
    args = ap.parse_args(argv)

    if args.command == "faulttest":  # drives Docker; needs no Settings or service logging
        sys.exit(faulttest_main(args.scenario, args.outage_s))

    _configure_logging()
    settings = Settings.from_env()
    if args.command == "record":
        asyncio.run(_record(args.venue, settings))
    elif args.command == "writer":
        asyncio.run(_write(settings))
    elif args.command == "merge":
        asyncio.run(_merge(settings, args.date, args.loop))
    elif args.command == "verify":
        sys.exit(_verify(settings, args.expect_head))


if __name__ == "__main__":
    main()
