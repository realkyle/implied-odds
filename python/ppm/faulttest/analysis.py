"""Pure analysis for the failure test: no Docker, no clock, no I/O except reading staging.

The question every scenario answers is the same: did every message a recorder handed to
the bus end up as exactly one row on disk? Two independent sources are compared:

- **What recorders say they sent**: each tick logs `"{venue} tick {T}: N published, M
  spooled"`. Spooled envelopes count too: they must arrive later, once Redis is back.
- **What the writer stored**: one `fetch_log` row per fetch in staging, keyed by
  `(venue, endpoint_key, snapshot_ts)`.

From these: **lost** = sent − stored for a (venue, tick); **duplicated** = a key stored
more than once; **gap** = a minute tick with neither a log line nor a row. A gap is fine
only inside a fault window that allows it (a killed recorder can't fetch while dead); a
network cut must not create gaps, because a failed fetch still publishes an error row.

The venue monitor (`ppm monitor`) is checked against the same evidence: its GAP lines must
name exactly the harness's gaps, and each fault must raise exactly its expected ALERT
(and RECOVERED), with no alert anywhere else.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ppm.faulttest.stream_census import UNDECODABLE
from ppm.pipeline.clock import TICK_MS, floor_tick

TEST_PROJECT = "ppmtest"
# The monitor's defaults (ppm.config): silent after 2.5 ticks, failing after 3 closed ticks,
# Redis unreachable alert after 60 s.
MONITOR_SILENT_MS = int(2.5 * TICK_MS)
MONITOR_FAILING_TICKS = 3
MONITOR_REDIS_ALERT_MS = 60_000

# "... INFO ppm.recorders.base: deribit tick 1794493440000: 4 published, 2 spooled"
_TICK_LINE = re.compile(r"ppm\.recorders\.base: (\w+) tick (\d+): (.+?)\s*$")
_TICK_FAILED = re.compile(r"ppm\.recorders\.base: (\w+) tick (\d+) failed")
_COUNT = re.compile(r"(\d+) (published|spooled|dropped)")
# "2026-11-12 18:18:00,373Z ERROR ppm.monitor: ALERT kind=silent venue=kalshi last=... "
_MONITOR_LINE = re.compile(
    r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3})Z \w+ ppm\.monitor: (GAP|ALERT|RECOVERED) (.*)$"
)
_KV = re.compile(r"(\w+)=('[^']*'|\S+)")


def assert_test_project(name: str) -> None:
    """Refuse any Compose project but the test one. The runner tears its stack down with
    `down -v`; on the live project that would delete the queued Redis data."""
    if name != TEST_PROJECT:
        raise ValueError(f"fault tests only run on project {TEST_PROJECT!r}, not {name!r}")


@dataclass(frozen=True, slots=True)
class TickLog:
    """One recorder tick as the recorder logged it."""

    venue: str
    tick: int
    published: int = 0
    spooled: int = 0
    dropped: int = 0
    failed: bool = False  # the tick raised; how much it sent is unknown

    @property
    def sent(self) -> int:
        """Envelopes that must reach disk: on the stream now, or in the spool for later."""
        return self.published + self.spooled


def parse_recorder_logs(lines: Iterable[str]) -> list[TickLog]:
    """Extract tick summaries from recorder logs (any mix of venues, any prefix)."""
    out: list[TickLog] = []
    for line in lines:
        if m := _TICK_FAILED.search(line):
            out.append(TickLog(m.group(1), int(m.group(2)), failed=True))
        elif m := _TICK_LINE.search(line):
            counts = {kind: int(n) for n, kind in _COUNT.findall(m.group(3))}
            out.append(TickLog(m.group(1), int(m.group(2)), **counts))
    return out


@dataclass(frozen=True, slots=True)
class FetchRow:
    """The `fetch_log` columns the analysis needs (timestamps in epoch ms)."""

    venue: str
    endpoint_key: str
    snapshot_ts: int
    status: str
    recv_ts: int
    error: str | None = None


def read_fetch_log(store: Path) -> list[FetchRow]:
    """Every fetch_log row in a data store's staging area (one small file per fetch)."""
    rows: list[FetchRow] = []
    for path in sorted((store / "staging" / "fetch_log").glob("date=*/*.parquet")):
        table = pq.read_table(path)
        cols = {name: table.column(name).to_pylist() for name in table.column_names}
        for i in range(table.num_rows):
            rows.append(
                FetchRow(
                    venue=cols["venue"][i],
                    endpoint_key=cols["endpoint_key"][i],
                    snapshot_ts=_ms(cols["snapshot_ts"][i]),
                    status=cols["status"][i],
                    recv_ts=_ms(cols["recv_ts"][i]),
                    error=cols["error"][i],
                )
            )
    return rows


def _ms(value: datetime | int) -> int:
    return value if isinstance(value, int) else int(value.timestamp() * 1000)


@dataclass(frozen=True, slots=True)
class FaultWindow:
    """When a fault affected some venues. Gaps for those venues are allowed in
    [start_ms - one tick, end_ms] only if `allow_gaps` (the tick in flight when a recorder
    is killed is lost along with the process; that is a gap, not data loss)."""

    scenario: str
    venues: frozenset[str]
    start_ms: int
    end_ms: int
    allow_gaps: bool

    def covers(self, venue: str, tick: int) -> bool:
        return venue in self.venues and self.start_ms - TICK_MS <= tick <= self.end_ms


@dataclass(slots=True)
class Integrity:
    """Run-wide verdict on the data itself."""

    sent: int = 0
    stored: int = 0
    lost: dict[tuple[str, int], int] = field(default_factory=dict)  # (venue, tick) -> n
    duplicated: list[tuple[str, str, int]] = field(default_factory=list)
    dropped: int = 0  # recorder gave up (spool full): data loss by definition
    failed_ticks: list[tuple[str, int]] = field(default_factory=list)
    allowed_gaps: dict[str, list[tuple[str, int]]] = field(default_factory=dict)  # by scenario
    unexpected_gaps: list[tuple[str, int]] = field(default_factory=list)
    # Rows but no log line: the recorder died after publishing some envelopes but before
    # logging the tick. Inside its kill window the tick can't be checked for completeness
    # (reported, not failed); outside any window nothing explains it, so it fails.
    unverified: list[tuple[str, int]] = field(default_factory=list)
    unlogged: list[tuple[str, int]] = field(default_factory=list)
    # Stream census (third witness). None when it couldn't be taken.
    delivered: int | None = None  # distinct keys that went through Redis
    extra_deliveries: int = 0  # deliveries beyond the first: absorbed by idempotent writes
    delivered_not_stored: list[tuple[str, str, int]] = field(default_factory=list)
    stored_not_delivered: list[tuple[str, str, int]] = field(default_factory=list)
    undecodable: int = 0  # stream messages that aren't valid envelopes

    @property
    def passed(self) -> bool:
        return self.delivered is not None and not (
            self.lost
            or self.duplicated
            or self.dropped
            or self.unexpected_gaps
            or self.failed_ticks
            or self.unlogged
            or self.delivered_not_stored
            or self.stored_not_delivered
            or self.undecodable
        )


def check_integrity(
    logs: list[TickLog],
    rows: list[FetchRow],
    venues: Iterable[str],
    first_tick: int,
    last_tick: int,
    windows: Iterable[FaultWindow] = (),
    deliveries: Counter[tuple[str, str, int]] | None = None,
) -> Integrity:
    """Compare what recorders sent (logs), what went through Redis (`deliveries`, from
    the stream census) and what was stored (`rows`), for ticks in [first, last].

    Note that `duplicated` can only fail if the writer's file naming breaks: staging
    files are named by (tick, venue, key), so a redelivery overwrites rather than adds.
    The census is what shows redeliveries actually happened (`extra_deliveries`)."""
    windows = list(windows)
    in_range = [t for t in logs if first_tick <= t.tick <= last_tick]
    rows = [r for r in rows if first_tick <= r.snapshot_ts <= last_tick]
    result = Integrity()

    stored = Counter((r.venue, r.snapshot_ts) for r in rows)
    keys = Counter((r.venue, r.endpoint_key, r.snapshot_ts) for r in rows)
    result.stored = len(rows)
    result.duplicated = sorted(k for k, n in keys.items() if n > 1)

    for t in in_range:
        result.sent += t.sent
        result.dropped += t.dropped
        if t.failed:
            result.failed_ticks.append((t.venue, t.tick))
        # Rows can exceed the log's count (a process killed after publishing but before
        # logging); only a shortfall is loss.
        missing = t.sent - stored[(t.venue, t.tick)]
        if missing > 0:
            result.lost[(t.venue, t.tick)] = missing

    logged = {(t.venue, t.tick) for t in in_range}
    for venue in sorted(venues):
        for tick in range(floor_tick(first_tick), last_tick + 1, TICK_MS):
            if (venue, tick) in logged:
                continue
            window = next((w for w in windows if w.allow_gaps and w.covers(venue, tick)), None)
            if stored[(venue, tick)]:
                (result.unverified if window else result.unlogged).append((venue, tick))
            elif window:
                result.allowed_gaps.setdefault(window.scenario, []).append((venue, tick))
            else:
                result.unexpected_gaps.append((venue, tick))

    if deliveries is not None:
        result.undecodable = sum(n for k, n in deliveries.items() if k[0] == UNDECODABLE)
        delivered = {k: n for k, n in deliveries.items() if first_tick <= k[2] <= last_tick}
        result.delivered = len(delivered)
        result.extra_deliveries = sum(n - 1 for n in delivered.values())
        result.delivered_not_stored = sorted(set(delivered) - set(keys))
        result.stored_not_delivered = sorted(set(keys) - set(delivered))
    return result


def parse_census(output: str) -> Counter[tuple[str, str, int]]:
    """Parse `stream_census` output (`venue<TAB>key<TAB>ts<TAB>count` lines)."""
    counts: Counter[tuple[str, str, int]] = Counter()
    for line in output.splitlines():
        if line.strip():
            venue, key, ts, n = line.split("\t")
            counts[(venue, key, int(ts))] = int(n)
    return counts


def gap_ticks(rows: list[FetchRow], logs: list[TickLog], venue: str, start: int, end: int) -> int:
    """Number of minute ticks in [start, end] with no trace of `venue`."""
    seen = {r.snapshot_ts for r in rows if r.venue == venue}
    seen |= {t.tick for t in logs if t.venue == venue}
    return sum(1 for t in range(floor_tick(start), end + 1, TICK_MS) if t not in seen)


def error_ticks(rows: list[FetchRow], venue: str, start: int, end: int) -> int:
    """Ticks in [start, end] where every fetch of `venue` failed, each with a cause."""
    by_tick: dict[int, list[FetchRow]] = {}
    for r in rows:
        if r.venue == venue and start <= r.snapshot_ts <= end:
            by_tick.setdefault(r.snapshot_ts, []).append(r)
    return sum(1 for rs in by_tick.values() if all(r.status != "200" and r.error for r in rs))


def first_ok_after(rows: list[FetchRow], venue: str, after_ms: int) -> FetchRow | None:
    """The earliest successful fetch of `venue` received after `after_ms`."""
    ok = [r for r in rows if r.venue == venue and r.status == "200" and r.recv_ts > after_ms]
    return min(ok, key=lambda r: r.recv_ts, default=None)


def parse_docker_time(value: str) -> int:
    """Docker's RFC 3339 timestamps carry nanoseconds (`...T14:24:02.583123456Z`), which
    `datetime.fromisoformat` rejects before Python 3.11 and truncates oddly; cut to µs."""
    value = value.strip().replace("Z", "+00:00")
    if "." in value:
        head, rest = value.split(".", 1)
        frac, tz = re.match(r"(\d+)(.*)", rest).groups()  # type: ignore[union-attr]
        value = f"{head}.{frac[:6].ljust(6, '0')}{tz}"
    return int(datetime.fromisoformat(value).timestamp() * 1000)


def parse_queue_state(output: str) -> tuple[int, int]:
    """Sum `pending` and `lag` over `redis-cli XINFO GROUPS` output for several streams.

    redis-cli prints each reply as alternating key / value lines. Redis can't always
    compute `lag` (it prints an empty line); then lag is returned as -1 ("unknown"), so a
    caller waiting for a drained queue keeps waiting instead of seeing a false 0.
    """
    tokens = [t.strip() for t in output.splitlines()]
    pending = lag = 0
    for i, tok in enumerate(tokens[:-1]):
        if tok == "pending":
            pending += int(tokens[i + 1])
        elif tok == "lag":
            value = tokens[i + 1]
            if not value.isdigit():
                return pending, -1
            lag += int(value)
    return pending, lag


@dataclass(slots=True)
class ScenarioResult:
    """One scenario's verdict and its measured numbers (the WorkLog figures)."""

    name: str
    passed: bool
    measured: dict[str, object] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def format_table(results: list[ScenarioResult], integrity: Integrity) -> str:
    lines = [f"{'scenario':<16}{'result':<8}measured"]
    for r in results:
        measured = ", ".join(f"{k}={v}" for k, v in r.measured.items())
        lines.append(f"{r.name:<16}{'PASS' if r.passed else 'FAIL':<8}{measured}")
        lines.extend(f"{'':<24}! {n}" for n in r.notes)
    gaps = sum(len(v) for v in integrity.allowed_gaps.values())
    lines.append(
        f"{'integrity':<16}{'PASS' if integrity.passed else 'FAIL':<8}"
        f"sent={integrity.sent}, stored={integrity.stored}, lost={sum(integrity.lost.values())}, "
        f"dup={len(integrity.duplicated)}, dropped={integrity.dropped}, "
        f"allowed_gaps={gaps}, unexpected_gaps={len(integrity.unexpected_gaps)}, "
        f"failed_ticks={len(integrity.failed_ticks)}, unverified={len(integrity.unverified)}, "
        f"unlogged={len(integrity.unlogged)}"
    )
    lines.append(
        f"{'stream census':<16}{'':<8}"
        + (
            "NOT TAKEN"
            if integrity.delivered is None
            else f"delivered={integrity.delivered}, extra_deliveries={integrity.extra_deliveries}, "
            f"delivered_not_stored={len(integrity.delivered_not_stored)}, "
            f"stored_not_delivered={len(integrity.stored_not_delivered)}, "
            f"undecodable={integrity.undecodable}"
        )
    )
    return "\n".join(lines)


# --- Scenario events (recorded by the runner) and their pure evaluation -----------------


@dataclass(frozen=True, slots=True)
class WriterKill:
    kill_ms: int
    pending_at_kill: int  # delivered but unacked when it died: will be redelivered
    lag_at_kill: int  # published but not yet read: queued while the writer is down
    restarted_ms: int | None  # Docker's StartedAt for the new process
    drained_ms: int | None  # queue back to pending = lag = 0
    attempts: int = 1  # ticks it took to freeze the writer holding unacked messages


@dataclass(frozen=True, slots=True)
class RecorderKill:
    venue: str
    kill_ms: int
    restarted_ms: int | None


@dataclass(frozen=True, slots=True)
class RecorderStop:
    """A recorder stopped cleanly (`compose stop`) for long enough to count as silent."""

    venue: str
    stop_ms: int
    start_ms: int


@dataclass(frozen=True, slots=True)
class RedisOutage:
    stop_ms: int
    start_ms: int
    spool_files: int  # sampled just before Redis came back
    spool_bytes: int
    spool_empty_ms: int | None
    drained_ms: int | None


@dataclass(frozen=True, slots=True)
class NetCut:
    venues: tuple[str, ...]
    cut_ms: int
    restore_ms: int


def _s(start: int, end: int | None) -> float | None:
    return None if end is None else round((end - start) / 1000, 1)


def windows_for(events: Iterable[object]) -> list[FaultWindow]:
    """Fault windows for the integrity check. Only a dead recorder (killed or stopped)
    may leave gaps."""
    out: list[FaultWindow] = []
    for e in events:
        if isinstance(e, RecorderKill):
            end = e.restarted_ms if e.restarted_ms is not None else e.kill_ms + 5 * TICK_MS
            out.append(FaultWindow("recorder-kill", frozenset({e.venue}), e.kill_ms, end, True))
        elif isinstance(e, RecorderStop):
            venues = frozenset({e.venue})
            out.append(FaultWindow("recorder-stop", venues, e.stop_ms, e.start_ms, True))
    return out


def evaluate_writer_kill(e: WriterKill, integrity: Integrity) -> ScenarioResult:
    r = ScenarioResult(
        "writer-kill",
        passed=e.restarted_ms is not None and e.drained_ms is not None and e.pending_at_kill > 0,
        measured={
            "restart_s": _s(e.kill_ms, e.restarted_ms),
            "drain_s": _s(e.kill_ms, e.drained_ms),
            "redelivered": e.pending_at_kill,
            "queued": e.lag_at_kill,
            "attempts": e.attempts,
        },
    )
    if e.pending_at_kill <= 0:
        r.notes.append("nothing delivered-but-unacked at the kill: redelivery not exercised")
    if e.restarted_ms is None:
        r.notes.append("writer was not restarted")
    if e.drained_ms is None:
        r.notes.append("queue never drained")
    if integrity.lost or integrity.duplicated:
        r.passed = False
        r.notes.append("lost or duplicated rows (see integrity)")
    return r


def evaluate_recorder_kills(
    kills: list[RecorderKill], logs: list[TickLog], rows: list[FetchRow]
) -> ScenarioResult:
    """Each recorder must come back and record the first tick after its restart; the
    gap it leaves is measured against its downtime."""
    r = ScenarioResult("recorder-kill", passed=True)
    for k in kills:
        if k.restarted_ms is None:
            r.passed = False
            r.notes.append(f"{k.venue} was not restarted")
            continue
        resume_tick = floor_tick(k.restarted_ms) + TICK_MS
        resumed = any(t.venue == k.venue and t.tick == resume_tick for t in logs) or any(
            row.venue == k.venue and row.snapshot_ts == resume_tick for row in rows
        )
        gap = gap_ticks(rows, logs, k.venue, floor_tick(k.kill_ms), resume_tick - TICK_MS)
        r.measured[k.venue] = f"down {_s(k.kill_ms, k.restarted_ms)}s/gap {gap} tick"
        if not resumed:
            r.passed = False
            r.notes.append(f"{k.venue} did not record tick {resume_tick} after restarting")
    return r


def evaluate_redis_outage(
    e: RedisOutage, logs: list[TickLog], monitor: list[MonitorLine] | None = None
) -> ScenarioResult:
    spooled = sum(t.spooled for t in logs if e.stop_ms - TICK_MS <= t.tick <= e.start_ms)
    r = ScenarioResult(
        "redis-outage",
        passed=e.spool_empty_ms is not None and e.drained_ms is not None,
        measured={
            "outage_s": _s(e.stop_ms, e.start_ms),
            "spooled": f"{e.spool_files} env/{e.spool_bytes / 1e6:.2f} MB",
            "replay_s": _s(e.start_ms, e.spool_empty_ms),
            "drain_s": _s(e.start_ms, e.drained_ms),
        },
    )
    if e.spool_files == 0:
        r.passed = False
        r.notes.append("nothing was spooled: the outage was not exercised")
    if spooled and spooled < e.spool_files:
        r.notes.append(f"logs report {spooled} spooled but {e.spool_files} files were seen")
    if e.spool_empty_ms is None:
        r.notes.append("spool never emptied")
    if e.drained_ms is None:
        r.notes.append("queue never drained")
    # Too short to need an alert (the monitor only alerts after 60 s down; allow its 10 s
    # retry backoff on top).
    if monitor is not None and e.start_ms - e.stop_ms >= MONITOR_REDIS_ALERT_MS + 10_000:
        window = [m for m in monitor if e.stop_ms <= m.time_ms <= e.start_ms + 3 * TICK_MS]
        alert = _first(window, "ALERT", "unreachable", "redis")
        back = _first(window, "RECOVERED", "unreachable", "redis")
        r.measured["alert_s"] = None if alert is None else _s(e.stop_ms, alert.time_ms)
        if alert is None or back is None:
            r.passed = False
            r.notes.append("monitor: no ALERT/RECOVERED kind=unreachable for redis")
    return r


def evaluate_net_cut(
    e: NetCut,
    logs: list[TickLog],
    rows: list[FetchRow],
    monitor: list[MonitorLine] | None = None,
) -> ScenarioResult:
    """During the cut every tick must still leave error rows with a cause (no silent
    gap), and each venue must recover on its own after the network is back."""
    r = ScenarioResult("net-cut", passed=True, measured={"cut_s": _s(e.cut_ms, e.restore_ms)})
    first, last = floor_tick(e.cut_ms) + TICK_MS, floor_tick(e.restore_ms)
    for venue in e.venues:
        errors = error_ticks(rows, venue, first, last)
        gaps = gap_ticks(rows, logs, venue, first, last)
        ok = first_ok_after(rows, venue, e.restore_ms)
        resume = None if ok is None else _s(e.restore_ms, ok.recv_ts)
        r.measured[venue] = f"err {errors} tick/gap {gaps}/resume {resume}s"
        if errors == 0:
            r.passed = False
            r.notes.append(f"{venue}: no error rows during the cut")
        if gaps:
            r.passed = False
            r.notes.append(f"{venue}: {gaps} tick(s) with no row and no cause")
        if ok is None:
            r.passed = False
            r.notes.append(f"{venue}: never recovered")
        # "failing" needs MONITOR_FAILING_TICKS whole error ticks inside the cut, plus a
        # tick for the last one's fetches to fail and be judged.
        long_enough = e.restore_ms >= first + MONITOR_FAILING_TICKS * TICK_MS
        if monitor is not None and long_enough:
            after = [m for m in monitor if m.time_ms >= e.cut_ms]
            alert = _first(after, "ALERT", "failing", venue)
            back = _first(after, "RECOVERED", "failing", venue)
            if alert is None or back is None or back.time_ms < e.restore_ms:
                r.passed = False
                r.notes.append(f"monitor: {venue} has no ALERT then RECOVERED kind=failing")
            else:
                worst = r.measured.get("failing_alert_s") or 0.0
                r.measured["failing_alert_s"] = max(worst, _s(e.cut_ms, alert.time_ms) or 0.0)
    return r


# --- the venue monitor (`ppm monitor` log lines) ---------------------------------------


@dataclass(frozen=True, slots=True)
class MonitorLine:
    """One GAP / ALERT / RECOVERED line from the monitor's log."""

    time_ms: int  # when the monitor logged it
    label: str
    fields: dict[str, str]

    @property
    def kind(self) -> str | None:
        return self.fields.get("kind")

    @property
    def venue(self) -> str | None:
        return self.fields.get("venue")

    def gap_ticks(self, tick_ms: int = TICK_MS) -> list[int]:
        first, last = iso_ms(self.fields["first"]), iso_ms(self.fields["last"])
        return list(range(first, last + 1, tick_ms))


def iso_ms(value: str) -> int:
    """`2026-11-12T17:27:00Z` -> epoch ms."""
    when = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    return int(when.timestamp()) * 1000


def parse_monitor_logs(lines: Iterable[str]) -> list[MonitorLine]:
    out: list[MonitorLine] = []
    for line in lines:
        if m := _MONITOR_LINE.search(line):
            when = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=UTC)
            fields = {k: v.strip("'") for k, v in _KV.findall(m.group(3))}
            out.append(MonitorLine(int(when.timestamp() * 1000), m.group(2), fields))
    return out


def _first(lines: list[MonitorLine], label: str, kind: str, venue: str) -> MonitorLine | None:
    return next(
        (m for m in lines if m.label == label and m.kind == kind and m.venue == venue), None
    )


def evaluate_recorder_stop(e: RecorderStop, monitor: list[MonitorLine]) -> ScenarioResult:
    """While the recorder is stopped the monitor must alert `silent` (not before the
    threshold), then report RECOVERED and the missing ticks once it is back."""
    last_tick = floor_tick(e.stop_ms)
    window = [m for m in monitor if e.stop_ms <= m.time_ms]
    alert = _first(window, "ALERT", "silent", e.venue)
    back = _first(window, "RECOVERED", "silent", e.venue)
    gap = next((m for m in window if m.label == "GAP" and m.venue == e.venue), None)
    r = ScenarioResult(
        "recorder-stop",
        passed=alert is not None and back is not None and gap is not None,
        measured={
            "stopped_s": _s(e.stop_ms, e.start_ms),
            "alert_after_last_tick_s": None if alert is None else _s(last_tick, alert.time_ms),
            "alert_after_stop_s": None if alert is None else _s(e.stop_ms, alert.time_ms),
            "gap_ticks": None if gap is None else int(gap.fields["ticks"]),
            "gap_reported_after_restart_s": None if gap is None else _s(e.start_ms, gap.time_ms),
        },
    )
    if alert is None:
        r.notes.append(f"monitor: no ALERT kind=silent for {e.venue}")
    elif alert.time_ms > e.start_ms:
        r.passed = False
        r.notes.append("monitor: silent alert came after the recorder was back")
    elif alert.time_ms - last_tick < MONITOR_SILENT_MS:
        r.passed = False
        r.notes.append("monitor: silent alert fired before the threshold")
    if back is None:
        r.notes.append(f"monitor: no RECOVERED kind=silent for {e.venue}")
    elif back.time_ms < e.start_ms:
        r.passed = False
        r.notes.append("monitor: RECOVERED before the recorder was restarted")
    if gap is None:
        r.notes.append(f"monitor: no GAP for {e.venue}")
    return r


def expected_alert(m: MonitorLine, events: Iterable[object]) -> bool:
    """Is this ALERT explained by a fault the harness injected?"""
    for e in events:
        if isinstance(e, RecorderStop):
            if (m.kind, m.venue) == ("silent", e.venue) and e.stop_ms <= m.time_ms <= e.start_ms:
                return True
        elif isinstance(e, NetCut):
            if m.kind == "failing" and m.venue in e.venues:
                if e.cut_ms <= m.time_ms <= e.restore_ms + 2 * TICK_MS:
                    return True
        elif isinstance(e, RedisOutage):
            if (m.kind, m.venue) == ("unreachable", "redis"):
                if e.stop_ms <= m.time_ms <= e.start_ms + 3 * TICK_MS:
                    return True
    return False


def evaluate_monitor(
    monitor: list[MonitorLine],
    integrity: Integrity,
    events: Iterable[object],
    first_tick: int,
    last_tick: int,
) -> ScenarioResult:
    """Run-wide check of the monitor: GAP lines must equal the harness's gaps exactly
    (none missed, none invented), and every ALERT must be one a fault explains."""
    events = list(events)
    harness = {g for gaps in integrity.allowed_gaps.values() for g in gaps}
    harness |= set(integrity.unexpected_gaps)
    reported: set[tuple[str, int]] = set()
    detect: list[float] = []
    for m in monitor:
        if m.label == "GAP" and m.venue:
            ticks = [t for t in m.gap_ticks() if first_tick <= t <= last_tick]
            reported |= {(m.venue, t) for t in ticks}
            if ticks:
                detect.append(float(m.fields["detected_after_s"]))
    missed, invented = sorted(harness - reported), sorted(reported - harness)
    alerts = [m for m in monitor if m.label == "ALERT" and first_tick <= m.time_ms]
    unexplained = [m for m in alerts if not expected_alert(m, events)]
    r = ScenarioResult(
        "monitor",
        passed=not (missed or invented or unexplained),
        measured={
            "gap_ticks": len(reported),
            "harness_gap_ticks": len(harness),
            "max_gap_detect_s": max(detect, default=None),
            "alerts": len(alerts),
        },
    )
    if missed:
        r.notes.append(f"gaps the monitor missed: {missed}")
    if invented:
        r.notes.append(f"gaps the monitor reported that the harness didn't see: {invented}")
    for m in unexplained:
        r.notes.append(f"unexplained alert: {m.label} {m.fields}")
    return r
