"""Command-line entry points.

ppm record <venue>    run one venue recorder (one OS process per venue)
ppm writer            run the Parquet writer
ppm merge [--loop]    merge finished days into hash-chained daily files
ppm verify            check the manifest hash chain and every file hash
ppm monitor           alert when a venue goes silent or only fails; log every missing tick
ppm backup [--loop]   copy new merged files, the manifest and the chain head to S3 (write-once)
ppm backup --status   (laptop, owner SSO) is the S3 backup fresh, and is every key 1 version?
ppm restore --to DIR  rebuild a data directory from the S3 backup (then `ppm verify` it)
ppm faulttest         inject faults into an isolated Compose stack; prove 0 lost / 0 duplicated
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from pathlib import Path

import httpx
from redis.asyncio import Redis

from ppm.analysis.coverage import (
    CoverageError,
    compare,
    load_store,
    pinned_entries,
    render,
    to_json,
)
from ppm.backup.audit import S3VersionLister, VersionLister, audit
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
from ppm.monitor.watch import BackupAudit, BackupFailed, ProbeAlert, Recovered, WriterWatch
from ppm.pipeline.bus import DiskSpool, RedisBus, StreamConsumer, wait_until_drained
from ppm.pipeline.clock import floor_tick, next_tick, now_ms, sleep_until
from ppm.recorders.base import Recorder
from ppm.recorders.deribit import DeribitRecorder
from ppm.recorders.kalshi import KalshiRecorder
from ppm.recorders.polymarket import PolymarketRecorder
from ppm.recorders.reference import ReferenceRecorder
from ppm.recorders.settlements import SettlementRecorder
from ppm.writer.merge import DailyMerger, overdue_ms
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
# A single queue check at :07:00 lands in the minute's publish burst (0.2-0.7 s on the VM)
# and sees a busy queue (2026-11-16, PLAN 2h). Re-check every second for up to a minute;
# a writer that keeps up is seen drained within a second or two.
DRAIN_WINDOW_MS = 60_000
DRAIN_POLL_MS = 1_000
# Two skipped hourly checks past the 02:00 grace (i.e. from the 04:07 check on).
MERGE_OVERDUE_MS = 2 * 3_600_000


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
            await _merge_hour(merger, redis, stop, log)
            run_at = next_tick(now_ms(), MERGE_EVERY_MS) + MERGE_OFFSET_MS
        await sleep_until(min(run_at, now_ms() + 60_000), stop)
    await redis.aclose()


async def _merge_hour(
    merger: DailyMerger, redis: Redis, stop: asyncio.Event, log: logging.Logger
) -> None:
    """One hourly run of the merge loop: merge the due days if the queue drains, and
    whatever stopped them (busy queue, Redis down, a failing merge), flag any day still
    unmerged MERGE_OVERDUE_MS past its grace."""
    due: list[str] = []
    try:
        due = merger.pending_days(now_ms())
        if due:
            await _merge_when_drained(merger, redis, due, stop, log)
    except Exception:  # one failed merge must not kill the service; retry next hour
        log.exception("merge failed")
    if due and not stop.is_set():
        _flag_overdue(merger, due, log)


async def _merge_when_drained(
    merger: DailyMerger, redis: Redis, due: list[str], stop: asyncio.Event, log: logging.Logger
) -> None:
    """Merge the due days once the writer's queue is seen empty within DRAIN_WINDOW_MS;
    otherwise skip this hour (the caller flags overdue days)."""
    waited = await wait_until_drained(
        redis, sorted(RECORDERS), window_ms=DRAIN_WINDOW_MS, every_ms=DRAIN_POLL_MS, stop=stop
    )
    if stop.is_set():  # shutting down: don't start a ~150 s merge Docker would SIGKILL
        return
    if waited is None:
        log.info(
            "days %s due, but the writer still had queued messages for %d s",
            due,
            DRAIN_WINDOW_MS // 1000,
        )
        return
    log.info("queue drained after %.1f s; merging %s", waited / 1000, due)
    for report in merger.merge_due(now_ms()):
        log.info("merged %s: %s", report.day, _merge_summary(report))


def _flag_overdue(merger: DailyMerger, due: list[str], log: logging.Logger) -> None:
    """Log `ALERT kind=merge-overdue` for each day still unmerged MERGE_OVERDUE_MS past its
    grace. Log-only: the merger has no notifier (see PLAN 2h)."""
    now = now_ms()
    try:
        still = merger.pending_days(now)
    except Exception:  # can't list the days (e.g. disk trouble): judge the ones we had
        still = due
    for day in still:
        late = overdue_ms(day, now)
        if late >= MERGE_OVERDUE_MS:
            log.warning("ALERT kind=merge-overdue day=%s hours_late=%.1f", day, late / 3.6e6)


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
        # The writer reads every recorder's stream, whatever PPM_MONITOR_VENUES says. One
        # global threshold, in ticks of the fastest venue (the writer is one process).
        writer_venues = sorted(RECORDERS)
        fastest_tick_ms = min(RECORDERS[v].tick_ms for v in writer_venues)
        try:
            writer_watch = WriterWatch(
                int(settings.monitor_writer_lag_ticks * fastest_tick_ms),
                settings.stream_retain_ms,
            )
        except ValueError as exc:
            print(f"PPM_MONITOR_WRITER_LAG_TICKS: {exc}", file=sys.stderr)
            return 2
        logging.getLogger("ppm.monitor").info(
            "thresholds: silent after %.1f ticks, failing after %d ticks, "
            "writer lag after %.0f s (retention %.0f s), ntfy %s",
            settings.monitor_silent_ticks,
            settings.monitor_failing_ticks,
            writer_watch.threshold_ms / 1000,
            writer_watch.retain_ms / 1000,
            "on" if ntfy else "off",
        )
        monitor = Monitor(
            redis, watches, notifier, writer_watch=writer_watch, writer_venues=writer_venues
        )
        await monitor.run_forever(stop)
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


async def _backup_status(settings: Settings, lister: VersionLister | None = None) -> int:
    """Exit 0: fresh and write-once held. 1: a problem (logged, pushed to ntfy).
    2: couldn't check (no bucket, expired SSO, S3 error); printed only, since this runs
    on demand and the owner is looking at the terminal."""
    if not settings.backup_bucket:
        print("PPM_BACKUP_BUCKET is not set", file=sys.stderr)
        return 2
    started = time.monotonic()
    try:
        # Building the client is inside the try: a missing profile fails here, not at
        # the first request, and must still be exit 2, never a traceback with exit 1.
        if lister is None:
            lister = S3VersionLister(settings.backup_bucket, _s3_client(settings))
        versions = await asyncio.to_thread(lister.list_versions, settings.backup_prefix)
        versioning = await asyncio.to_thread(lister.versioning_status)
    except Exception as exc:  # profile, credentials, SSO token, network, AccessDenied
        print(f"cannot check: {type(exc).__name__}: {exc}", file=sys.stderr)
        print("(expired SSO? run `aws sso login --profile ppm`)", file=sys.stderr)
        return 2
    report = audit(versions, now_ms(), settings.backup_prefix, versioning=versioning)
    seconds = time.monotonic() - started
    for line in report.lines():
        print(line)
    for problem in report.problems:
        print(f"PROBLEM  {problem}")
    print(f"backup status {'OK' if report.ok else 'PROBLEM'} ({seconds:.1f} s)")
    if report.ok:
        return 0
    async with httpx.AsyncClient() as http:
        ntfy = NtfyNotifier(settings.ntfy_topic, http) if settings.ntfy_topic else None
        await _notifier(ntfy).notify(BackupAudit(_audit_detail(report.problems)))
    return 1


def _audit_detail(problems: tuple[str, ...], shown: int = 3) -> str:
    """The alert text: a count plus the first few problems. A bulk overwrite could flag
    hundreds of keys, and ntfy turns a body over 4096 bytes into an attachment, so the
    phone would show no text. The terminal output still lists every problem."""
    head = "; ".join(problems[:shown])
    more = len(problems) - shown
    return f"{len(problems)} problems: {head}" + (f"; … {more} more" if more > 0 else "")


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


def _coverage(
    day: str, baseline: str, candidate: str, candidate_head: str, labels: str, json_out: str | None
) -> int:
    """Verify both stores, then compare their coverage of `day` (PLAN 2d1).

    Exit 0 = the automatic part of the pass bar holds, 1 = the report says FAIL,
    2 = can't compare (bad arguments, a store fails verification, day not merged);
    the same convention as `ppm backup --status`.
    """
    names = labels.split(",")
    if len(names) != 2 or not all(names):
        print("--labels must be two names, e.g. laptop,vm", file=sys.stderr)
        return 2
    stores = ((names[0], Path(baseline), None), (names[1], Path(candidate), candidate_head))
    entries: dict[str, list] = {}
    for label, root, head in stores:
        # Never compare data the chain doesn't vouch for; the candidate must also end
        # at the head recorded independently (the restore printed it from S3).
        entries[label], problems = pinned_entries(root, head)
        for p in problems:
            print(f"FAIL {label} seq={p.seq} path={p.path}: {p.message}")
        if problems:
            print(f"{label}: VERIFICATION FAILED, not comparing")
            return 2
        print(f"{label}: verified {root}" + (f" (head {head[:12]}…)" if head else ""))
    try:
        report = compare(
            *(load_store(label, root, day, entries[label]) for label, root, _ in stores)
        )
    except CoverageError as exc:
        print(f"coverage failed: {exc}", file=sys.stderr)
        return 2
    print(render(report))
    if json_out:
        Path(json_out).write_text(json.dumps(to_json(report), indent=2) + "\n")
    return 1 if report.problems() else 0


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
    bmode.add_argument(
        "--status",
        action="store_true",
        help="(owner SSO) newest head age and 1-version-per-key check; exit 0 ok, 1 problem, "
        "2 cannot check",
    )
    rst = sub.add_parser("restore", help="rebuild a data directory from the S3 backup")
    rst.add_argument("--to", required=True, help="new, empty directory to restore into")
    rst.add_argument("--seq", type=int, help="restore the chain as of this head (default newest)")
    cov = sub.add_parser("coverage", help="compare two stores' tick coverage of one UTC day")
    cov.add_argument("--date", required=True, help="UTC day, YYYY-MM-DD (merged in both stores)")
    cov.add_argument("--baseline", required=True, help="data root of the baseline (laptop)")
    cov.add_argument("--candidate", required=True, help="data root of the candidate (VM restore)")
    cov.add_argument(
        "--candidate-head", required=True, help="chain head the candidate must end at (from S3)"
    )
    cov.add_argument("--labels", default="laptop,vm", help="names for baseline,candidate")
    cov.add_argument("--json", dest="json_out", help="also write a JSON summary here")
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
    elif args.command == "backup" and args.status:
        sys.exit(asyncio.run(_backup_status(settings)))
    elif args.command == "backup":
        sys.exit(asyncio.run(_backup(settings, args.loop, args.check_permissions)))
    elif args.command == "coverage":
        sys.exit(
            _coverage(
                args.date,
                args.baseline,
                args.candidate,
                args.candidate_head,
                args.labels,
                args.json_out,
            )
        )
    elif args.command == "restore":
        sys.exit(_restore(settings, args.to, args.seq))


if __name__ == "__main__":
    main()
