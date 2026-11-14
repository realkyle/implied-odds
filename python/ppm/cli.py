"""Command-line entry points.

ppm record <venue>    run one venue recorder (one OS process per venue)
ppm writer            run the Parquet writer
ppm merge [--loop]    merge finished days into hash-chained daily files
ppm verify            check the manifest hash chain and every file hash
ppm monitor           alert when a venue goes silent or only fails; log every missing tick
ppm backup [--loop]   copy new merged files, the manifest and the chain head to S3 (write-once)
ppm restore --to DIR  rebuild a data directory from the S3 backup (then `ppm verify` it)
ppm faulttest         inject faults into an isolated Compose stack; prove 0 lost / 0 duplicated
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

import httpx
from redis.asyncio import Redis

from ppm.backup.check import check_permissions
from ppm.backup.job import BackupJob, BackupRefused, BackupReport
from ppm.backup.restore import RestoreError, restore
from ppm.backup.store import S3Store
from ppm.config import Settings
from ppm.faulttest.runner import SCENARIOS as FAULT_SCENARIOS
from ppm.faulttest.runner import main as faulttest_main
from ppm.integrity.manifest import Manifest
from ppm.monitor.notify import FanoutNotifier, LogNotifier, Notifier, NtfyNotifier
from ppm.monitor.service import Monitor, build_watches
from ppm.monitor.watch import BackupFailed, ProbeAlert, Recovered
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
    bus = RedisBus(redis, spool, settings.stream_retain_ms)
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


async def _monitor(settings: Settings, test_notify: bool) -> int:
    async with httpx.AsyncClient() as client:
        ntfy = NtfyNotifier(settings.ntfy_topic, client) if settings.ntfy_topic else None
        notifier = _notifier(ntfy)
        if test_notify:
            await LogNotifier().notify(ProbeAlert())
            if ntfy is None:
                print("PPM_NTFY_TOPIC is not set: the alert was only logged")
                return 1
            delivered = await ntfy.send(ProbeAlert())
            print("ntfy accepted the test push" if delivered else "ntfy push FAILED (see log)")
            return 0 if delivered else 1

        stop = _stop_event()
        # Socket timeout above the 5 s blocking read (see `_write`).
        redis = Redis.from_url(settings.redis_url, socket_timeout=30, socket_connect_timeout=5)
        venues = settings.monitor_venues or tuple(RECORDERS)
        unknown = sorted(set(venues) - set(RECORDERS))
        if unknown:
            print(f"PPM_MONITOR_VENUES: unknown venue(s) {unknown}", file=sys.stderr)
            return 2
        watches = build_watches(
            {venue: RECORDERS[venue].tick_ms for venue in venues},
            settings.monitor_silent_ticks,
            settings.monitor_failing_ticks,
            now_ms(),
        )
        logging.getLogger("ppm.monitor").info(
            "thresholds: silent after %.1f ticks, failing after %d ticks, ntfy %s",
            settings.monitor_silent_ticks,
            settings.monitor_failing_ticks,
            "on" if ntfy else "off",
        )
        await Monitor(redis, watches, notifier).run_forever(stop)
        await redis.aclose()
    return 0


def _notifier(ntfy: NtfyNotifier | None) -> Notifier:
    """Always log; also push to the phone when PPM_NTFY_TOPIC is set."""
    return FanoutNotifier([LogNotifier(), *([ntfy] if ntfy else [])])


def _s3_client(settings: Settings):
    import boto3  # deferred: only backup/restore need it, and it's slow to import
    from botocore.config import Config

    retries = Config(retries={"mode": "standard", "max_attempts": 5})
    return boto3.client("s3", region_name=settings.backup_region, config=retries)


# Hourly at :37, half an hour after the merger's :07 run. The pass is a catch-up, not a
# fixed nightly job: a late merge, a reboot or an S3 outage is picked up on the next pass.
BACKUP_EVERY_MS = 3_600_000
BACKUP_OFFSET_MS = 37 * 60_000


def next_backup_at(now: int) -> int:
    """The next :37 strictly after `now`. (`next_tick` + offset would skip one: after a
    startup pass at 10:20 it gives 11:37, not 10:37.)"""
    at = floor_tick(now, BACKUP_EVERY_MS) + BACKUP_OFFSET_MS
    return at if at > now else at + BACKUP_EVERY_MS


def _alert_key(alert: BackupFailed) -> tuple[str, str]:
    """What makes two backup alerts 'the same problem' for de-duplication. S3/network
    errors compare by exception type only: their text carries request IDs that change
    every hour, which would otherwise push the same outage hourly."""
    if alert.reason == "error":
        return alert.reason, alert.detail.split(":", 1)[0]
    return alert.reason, alert.detail


def _backup_summary(report: BackupReport) -> str:
    if report.head is None:
        return f"nothing new ({report.seconds:.1f} s)"
    return (
        f"head seq={report.head.seq} {report.head.head[:12]}…: {len(report.new_entries)} new "
        f"entries, {report.files} files uploaded ({report.bytes / 1e6:.1f} MB), "
        f"{report.already_there} already there, {report.seconds:.1f} s"
    )


async def _backup(settings: Settings, loop: bool, check: bool) -> int:
    if not settings.backup_bucket:
        print("PPM_BACKUP_BUCKET is not set", file=sys.stderr)
        return 2
    client = _s3_client(settings)
    if check:
        probes = check_permissions(client, settings.backup_bucket, settings.backup_prefix)
        for p in probes:
            print(f"{'PASS' if p.ok else 'FAIL'}  expected {p.expected}, got {p.actual}: {p.name}")
        passed = len(probes) > 1 and all(p.ok for p in probes)
        print("permission check PASSED" if passed else "permission check FAILED")
        return 0 if passed else 1

    job = BackupJob(
        settings.data_dir, S3Store(settings.backup_bucket, client), settings.backup_prefix
    )
    log = logging.getLogger("ppm.backup")
    if not loop:
        try:
            log.info("backup: %s", _backup_summary(job.run_once()))
        except BackupRefused as exc:
            log.error("backup REFUSED (%s): %s", exc.reason, exc.detail)
            return 1
        return 0

    async with httpx.AsyncClient() as http:
        ntfy = NtfyNotifier(settings.ntfy_topic, http) if settings.ntfy_topic else None
        notifier = _notifier(ntfy)
        stop = _stop_event()
        log.info("backup started: s3://%s/%s", settings.backup_bucket, settings.backup_prefix)
        failing_since: int | None = None
        last_alert: tuple[str, str] | None = None
        # First pass right away, so every deploy shows at once whether key and bucket
        # work; then hourly at :37. (The pass is idempotent, so an extra one is harmless.)
        run_at = now_ms()
        while not stop.is_set():
            if now_ms() >= run_at:
                alert: BackupFailed | None = None
                try:
                    # boto3 is blocking; a thread keeps SIGTERM handling responsive. If the
                    # container is killed mid-upload, nothing half-written becomes visible.
                    report = await asyncio.to_thread(job.run_once)
                    log.info("backup: %s", _backup_summary(report))
                except BackupRefused as exc:
                    alert = BackupFailed(exc.reason, exc.detail)
                except Exception as exc:  # S3, network, credentials: retry next pass
                    log.exception("backup pass failed")
                    alert = BackupFailed("error", f"{type(exc).__name__}: {exc}")
                if alert is not None:
                    failing_since = failing_since or now_ms()
                    if _alert_key(alert) != last_alert:  # one push per problem, not hourly
                        await notifier.notify(alert)
                    last_alert = _alert_key(alert)
                elif failing_since is not None:
                    await notifier.notify(Recovered("s3", "backup", now_ms() - failing_since))
                    failing_since, last_alert = None, None
                run_at = next_backup_at(now_ms())
            await sleep_until(min(run_at, now_ms() + 60_000), stop)
    return 0


def _restore(settings: Settings, dest: str, seq: int | None) -> int:
    if not settings.backup_bucket:
        print("PPM_BACKUP_BUCKET is not set", file=sys.stderr)
        return 2
    store = S3Store(settings.backup_bucket, _s3_client(settings))
    started = time.monotonic()
    try:
        head = restore(store, settings.backup_prefix, Path(dest), seq)
    except RestoreError as exc:
        print(f"restore failed: {exc}", file=sys.stderr)
        return 1
    print(
        f"restored head seq={head.seq} into {dest} in {time.monotonic() - started:.1f} s; "
        f"now run: PPM_DATA_DIR={dest} ppm verify --expect-head {head.head}"
    )
    return 0


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
    mon = sub.add_parser("monitor", help="alert on silent or failing venues; log missing ticks")
    mon.add_argument("--test-notify", action="store_true", help="send one test alert and exit")
    bak = sub.add_parser("backup", help="copy new merged data and the chain head to S3")
    bmode = bak.add_mutually_exclusive_group()
    bmode.add_argument("--loop", action="store_true", help="run hourly (at :37) as a service")
    bmode.add_argument(
        "--check-permissions",
        action="store_true",
        help="prove this key can add but never overwrite or delete (writes one probe object)",
    )
    rst = sub.add_parser("restore", help="rebuild a data directory from the S3 backup")
    rst.add_argument("--to", required=True, help="new, empty directory to restore into")
    rst.add_argument("--seq", type=int, help="restore the chain as of this head (default newest)")
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
    elif args.command == "monitor":
        sys.exit(asyncio.run(_monitor(settings, args.test_notify)))
    elif args.command == "backup":
        sys.exit(asyncio.run(_backup(settings, args.loop, args.check_permissions)))
    elif args.command == "restore":
        sys.exit(_restore(settings, args.to, args.seq))


if __name__ == "__main__":
    main()
