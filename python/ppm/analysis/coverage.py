"""`ppm coverage`: did each of two data stores record every tick of one UTC day? (PLAN 2d1)

Used to decide whether the VM can become the canonical recorder. The two stores are
the laptop's `data/` (the *baseline*) and the VM's chain restored from S3 (the
*candidate*). Both are measured against the **expected tick grid**, not against each
other, because the laptop is not a gold standard: it sleeps on battery
(`caffeinate -s` only holds on AC), which cost it 1270 of 1440 minutes on 2026-11-13.

What it reads (only merged, chain-covered files: the latest manifest entry per table
and day, the same "latest part wins" rule the merge uses):

- **fetch_log** → per venue, each expected tick is *ok* (≥1 fetch, every one 2xx with
  no error), *error* (a fetch failed) or *missing* (no fetch at all). Coverage is per
  `(venue, tick)`; the endpoint keys inside a tick are compared between the stores
  separately. Polymarket's `events` (discovery) is left out of that comparison because
  it runs on a 10-minute cadence that starts whenever the recorder started, so the two
  machines legitimately fetch it in different minutes. Its longest gap is reported.
- **tick tables** → rows per tick in each store. Only counts are compared: the two
  machines fetch milliseconds apart, so prices legitimately differ.
- **settlements** (every merged day up to D) → for each `(venue, contract_id)` both
  stores have, `settle_ts`, `result` and `settlement_value` must be identical. A
  settled value shouldn't depend on when it was fetched, but Kalshi can revise one
  after settling, so each difference is printed with both stores' fetch times.

The pass bar (owner decision 2026-11-15) is partly human: code checks that the
candidate's ok count is ≥ the baseline's for every venue and that settlements match;
every candidate tick that isn't ok is *listed*, and a person writes its cause down.
So is a tick where the candidate fetched fewer pages of a paged endpoint
(`PAGED_ENDPOINTS`): a late fetch sees more results, so page counts legitimately vary.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ppm.integrity.manifest import Manifest, ManifestEntry, Problem
from ppm.recorders.deribit import DeribitRecorder
from ppm.recorders.kalshi import KalshiRecorder
from ppm.recorders.polymarket import DISCOVERY_INTERVAL_MS, PolymarketRecorder
from ppm.recorders.reference import ReferenceRecorder
from ppm.recorders.settlements import SettlementRecorder

# Taken from the recorders themselves so the grid can't drift from what they do.
CADENCE_MS: dict[str, int] = {
    r.venue: r.tick_ms
    for r in (
        DeribitRecorder,
        KalshiRecorder,
        PolymarketRecorder,
        ReferenceRecorder,
        SettlementRecorder,
    )
}
DISCOVERY = ("polymarket", "events")  # (venue, endpoint_key) on its own 10-minute cadence
TICK_TABLES = (
    "deribit_options",
    "deribit_futures",
    "kalshi_markets",
    "polymarket_books",
    "reference_prices",
)
TABLE_VENUE = {
    "deribit_options": "deribit",
    "deribit_futures": "deribit",
    "kalshi_markets": "kalshi",
    "polymarket_books": "polymarket",
    "reference_prices": "reference",
}
DAY_MS = 86_400_000
# Endpoints that page through results. Their key is "<endpoint>-<page NN>-<series>"
# (`Envelope.key` joins params sorted by name: page before series). How many pages a
# fetch needs depends on how many results exist *when* it runs, so a late fetch of the
# same tick can need one more page. 2026-11-15 02:00: the laptop woke from battery sleep
# and fetched at 02:22 (15 pages of settled KXETH), the VM on time (14 pages).
PAGED_ENDPOINTS = frozenset({("settlements", "kalshi_settled")})


class CoverageError(Exception):
    """A store can't be compared (e.g. it has no merged fetch_log for the day)."""


@dataclass(frozen=True, slots=True)
class Settlement:
    settle_ts: int
    result: str | None
    value: float | None
    fetched_at: int = 0  # snapshot_ts of the fetch it came from; not compared

    def values(self) -> tuple[int, str | None, float | None]:
        return (self.settle_ts, self.result, self.value)


@dataclass(frozen=True, slots=True)
class StoreSnapshot:
    """What one store recorded for one day, reduced to what the comparison needs."""

    label: str
    day: str
    # (venue, tick ms) -> endpoint_key -> ok?
    fetches: Mapping[tuple[str, int], Mapping[str, bool]]
    # table -> tick ms -> rows
    tick_rows: Mapping[str, Mapping[int, int]]
    # (venue, contract_id) -> settled values, from every merged day up to `day`
    settlements: Mapping[tuple[str, str], Settlement]


def day_start_ms(day: str) -> int:
    start = datetime.fromisoformat(day).replace(tzinfo=UTC)
    return int(start.timestamp() * 1000)


def expected_ticks(day: str, venue: str) -> range:
    """Every tick the venue's recorder should have fired on `day` (UTC)."""
    start = day_start_ms(day)
    return range(start, start + DAY_MS, CADENCE_MS[venue])


def _ms(column: pa.ChunkedArray) -> list[int]:
    return column.cast(pa.int64()).to_pylist()


def load_store(
    label: str, root: Path, day: str, entries: Iterable[ManifestEntry] | None = None
) -> StoreSnapshot:
    """Read the merged files listed in `root`'s manifest.

    Pass the `entries` that were verified, so a merge appending an amendment between
    `verify` and this read can't slip in an unchecked file (`pinned_entries`).
    """
    latest: dict[tuple[str, str], str] = {}
    for e in Manifest(root).entries() if entries is None else entries:
        if e.kind == "table":
            latest[(e.date, e.name)] = e.path  # later entries (amendments) win
    if (day, "fetch_log") not in latest:
        raise CoverageError(f"{label}: no merged fetch_log for {day} in {root}")

    log = pq.read_table(
        root / latest[(day, "fetch_log")],
        columns=["snapshot_ts", "venue", "endpoint_key", "status", "error"],
    )
    fetches: dict[tuple[str, int], dict[str, bool]] = defaultdict(dict)
    for ts, venue, key, status, error in zip(
        _ms(log["snapshot_ts"]),
        log["venue"].to_pylist(),
        log["endpoint_key"].to_pylist(),
        log["status"].to_pylist(),
        log["error"].to_pylist(),
        strict=True,
    ):
        fetches[(venue, ts)][key] = status.startswith("2") and error is None

    tick_rows: dict[str, dict[int, int]] = {}
    for table in TICK_TABLES:
        if (day, table) in latest:
            ts = pq.read_table(root / latest[(day, table)], columns=["snapshot_ts"])
            counts = pc.value_counts(ts["snapshot_ts"].cast(pa.int64()))
            tick_rows[table] = {c["values"].as_py(): c["counts"].as_py() for c in counts}

    settlements: dict[tuple[str, str], tuple[int, Settlement]] = {}
    for (date, table), path in sorted(latest.items()):
        if table != "settlements" or date > day:
            continue
        t = pq.read_table(root / path)
        for snap, venue, cid, settle, result, value in zip(
            _ms(t["snapshot_ts"]),
            t["venue"].to_pylist(),
            t["contract_id"].to_pylist(),
            _ms(t["settle_ts"]),
            t["result"].to_pylist(),
            t["settlement_value"].to_pylist(),
            strict=True,
        ):
            old = settlements.get((venue, cid))
            if old is None or snap >= old[0]:  # same rule as the merge: latest fetch wins
                settlements[(venue, cid)] = (snap, Settlement(settle, result, value, snap))

    return StoreSnapshot(
        label=label,
        day=day,
        fetches=dict(fetches),
        tick_rows=tick_rows,
        settlements={k: v for k, (_, v) in settlements.items()},
    )


@dataclass(frozen=True, slots=True)
class VenueCoverage:
    """One store, one venue, one day."""

    venue: str
    expected: int
    ok: int
    error_ticks: tuple[int, ...]
    missing_runs: tuple[tuple[int, int], ...]  # (first, last) tick ms, inclusive

    @property
    def error(self) -> int:
        return len(self.error_ticks)

    @property
    def missing(self) -> int:
        return self.expected - self.ok - self.error

    @property
    def pct(self) -> float:
        return 100.0 * self.ok / self.expected


def venue_coverage(snap: StoreSnapshot, venue: str) -> VenueCoverage:
    ticks = expected_ticks(snap.day, venue)
    ok = 0
    errors: list[int] = []
    missing: list[int] = []
    for tick in ticks:
        keys = {
            k: v for k, v in snap.fetches.get((venue, tick), {}).items() if (venue, k) != DISCOVERY
        }
        if not keys:
            missing.append(tick)
        elif all(keys.values()):
            ok += 1
        else:
            errors.append(tick)
    return VenueCoverage(venue, len(ticks), ok, tuple(errors), _runs(missing, ticks.step))


def _runs(ticks: list[int], step: int) -> tuple[tuple[int, int], ...]:
    runs: list[list[int]] = []
    for t in ticks:
        if runs and t - runs[-1][1] == step:
            runs[-1][1] = t
        else:
            runs.append([t, t])
    return tuple((a, b) for a, b in runs)


def ok_ticks(snap: StoreSnapshot, venue: str) -> set[int]:
    """Ticks where every fetch (discovery aside) of `venue` succeeded."""
    out = set()
    for (v, tick), keys in snap.fetches.items():
        rest = [ok for k, ok in keys.items() if (v, k) != DISCOVERY]
        if v == venue and rest and all(rest):
            out.add(tick)
    return out


def discovery_max_gap_ms(snap: StoreSnapshot) -> int | None:
    """Longest wait between polymarket discovery fetches, counting from 00:00 and to 24:00."""
    venue, key = DISCOVERY
    ticks = sorted(t for (v, t), keys in snap.fetches.items() if v == venue and keys.get(key))
    if not ticks:
        return None
    start = day_start_ms(snap.day)
    edges = [start, *ticks, start + DAY_MS]
    return max(b - a for a, b in zip(edges, edges[1:], strict=False))


def page_group(venue: str, key: str) -> str | None:
    """`key` without its page number ("kalshi_settled-KXETH"), or None if not paged."""
    endpoint, _, rest = key.partition("-")
    page, _, series = rest.partition("-")
    if (venue, endpoint) not in PAGED_ENDPOINTS or not page.isdigit():
        return None
    return f"{endpoint}-{series}"


@dataclass(frozen=True, slots=True)
class EndpointDiff:
    venue: str
    tick: int
    endpoint_key: str
    only_in: str  # label of the store that has it


@dataclass(frozen=True, slots=True)
class PageShortfall:
    """At one tick the candidate fetched fewer pages of a paged series than the baseline,
    but at least one page of it without error (otherwise it's a missing endpoint)."""

    venue: str
    tick: int
    series: str  # page_group(): "kalshi_settled-KXETH"
    baseline_pages: int
    candidate_pages: int
    candidate_tick_ok: bool  # if not, the tick is already counted as an error


@dataclass(frozen=True, slots=True)
class CountDiff:
    table: str
    compared: int  # ticks present in both stores
    differing: tuple[tuple[int, int, int], ...]  # (tick, baseline rows, candidate rows)


@dataclass(frozen=True, slots=True)
class SettlementDiff:
    both: int
    differing: tuple[tuple[str, str, Settlement, Settlement], ...]
    only_baseline: int
    only_candidate: int


@dataclass(frozen=True, slots=True)
class CoverageReport:
    day: str
    baseline: str
    candidate: str
    coverage: Mapping[str, Mapping[str, VenueCoverage]]  # label -> venue -> coverage
    discovery_gap_ms: Mapping[str, int | None]  # label -> longest discovery gap
    endpoint_diffs: tuple[EndpointDiff, ...]
    counts: tuple[CountDiff, ...]
    settlements: SettlementDiff
    # (venue, endpoint_key) -> label -> ticks it was fetched in (discovery aside)
    presence: Mapping[tuple[str, str], Mapping[str, int]]
    # Paged series the candidate fetched in fewer pages (their baseline-only page keys
    # are still listed in `endpoint_diffs`, but don't FAIL).
    page_shortfalls: tuple[PageShortfall, ...] = ()

    def problems(self) -> list[str]:
        """The parts of the pass bar code can decide. Empty = those parts pass.

        "ok" only means a tick was fetched without errors, so the cross-store checks
        are what catch a candidate that systematically skips an endpoint (owner
        decision 2026-11-15, after review). A missing *page* of a series the candidate
        fetched is not a skipped endpoint: it needs a stated cause instead (owner
        decision 2026-11-16, `page_shortfalls`).
        """
        out = []
        for venue, base in self.coverage[self.baseline].items():
            cand = self.coverage[self.candidate][venue]
            if cand.ok < base.ok:
                out.append(f"{venue}: {self.candidate} ok {cand.ok} < {self.baseline} ok {base.ok}")
        short = {(p.venue, p.tick, p.series) for p in self.page_shortfalls}
        lacking = Counter(
            (d.venue, d.endpoint_key)
            for d in self.endpoint_diffs
            if d.only_in == self.baseline
            and (d.venue, d.tick, page_group(d.venue, d.endpoint_key)) not in short
        )
        for (venue, key), n in sorted(lacking.items()):
            out.append(f"{venue}: {self.candidate} lacks {key} at {n} shared tick(s)")
        if self.settlements.differing:
            out.append(f"{len(self.settlements.differing)} settlement(s) differ")
        return out

    def fewer_rows(self) -> list[tuple[str, int, int, int]]:
        """(table, tick, baseline rows, candidate rows) where the candidate kept fewer."""
        return [(c.table, t, a, b) for c in self.counts for t, a, b in c.differing if b < a]

    def unexplained(self) -> int:
        """Items a person must write a cause for: candidate ticks not ok, ticks with
        fewer rows, and (tick, series) pairs with fewer pages on an otherwise ok tick
        (a not-ok tick is already counted once)."""
        not_ok = sum(c.error + c.missing for c in self.coverage[self.candidate].values())
        pages = sum(p.candidate_tick_ok for p in self.page_shortfalls)
        return not_ok + len(self.fewer_rows()) + pages


def compare(baseline: StoreSnapshot, candidate: StoreSnapshot) -> CoverageReport:
    if baseline.day != candidate.day:
        raise CoverageError(f"different days: {baseline.day} vs {candidate.day}")
    snaps = (baseline, candidate)
    coverage = {s.label: {v: venue_coverage(s, v) for v in CADENCE_MS} for s in snaps}

    diffs: list[EndpointDiff] = []
    for venue, tick in sorted(baseline.fetches.keys() & candidate.fetches.keys()):
        a = set(baseline.fetches[(venue, tick)]) - {DISCOVERY[1]}
        b = set(candidate.fetches[(venue, tick)]) - {DISCOVERY[1]}
        diffs += [EndpointDiff(venue, tick, k, baseline.label) for k in sorted(a - b)]
        diffs += [EndpointDiff(venue, tick, k, candidate.label) for k in sorted(b - a)]
    shortfalls = []
    for venue, tick, group in sorted(
        {
            (d.venue, d.tick, g)
            for d in diffs
            if d.only_in == baseline.label and (g := page_group(d.venue, d.endpoint_key))
        }
    ):
        base = baseline.fetches[(venue, tick)]
        cand = candidate.fetches[(venue, tick)]
        cand_ok_pages = [k for k, ok in cand.items() if ok and page_group(venue, k) == group]
        if cand_ok_pages:  # no ok page of the series at all -> stays a missing endpoint
            shortfalls.append(
                PageShortfall(
                    venue,
                    tick,
                    group,
                    baseline_pages=sum(page_group(venue, k) == group for k in base),
                    candidate_pages=sum(page_group(venue, k) == group for k in cand),
                    candidate_tick_ok=all(cand.values()),
                )
            )

    counts = []
    for table in TICK_TABLES:
        a = baseline.tick_rows.get(table, {})
        b = candidate.tick_rows.get(table, {})
        # Every tick both stores fetched ok, including ones where a store kept 0 rows
        # (e.g. its parser rejected them all); such a tick is absent from `tick_rows`.
        venue = TABLE_VENUE[table]
        both = sorted(ok_ticks(baseline, venue) & ok_ticks(candidate, venue))
        pairs = [(t, a.get(t, 0), b.get(t, 0)) for t in both]
        counts.append(CountDiff(table, len(both), tuple(p for p in pairs if p[1] != p[2])))

    sa, sb = baseline.settlements, candidate.settlements
    shared = sorted(sa.keys() & sb.keys())
    settlements = SettlementDiff(
        both=len(shared),
        differing=tuple(
            (v, c, sa[(v, c)], sb[(v, c)])
            for v, c in shared
            if sa[(v, c)].values() != sb[(v, c)].values()
        ),
        only_baseline=len(sa.keys() - sb.keys()),
        only_candidate=len(sb.keys() - sa.keys()),
    )
    return CoverageReport(
        day=baseline.day,
        baseline=baseline.label,
        candidate=candidate.label,
        coverage=coverage,
        discovery_gap_ms={s.label: discovery_max_gap_ms(s) for s in snaps},
        endpoint_diffs=tuple(diffs),
        counts=tuple(counts),
        settlements=settlements,
        presence=_presence(snaps),
        page_shortfalls=tuple(shortfalls),
    )


def _presence(snaps: tuple[StoreSnapshot, ...]) -> dict[tuple[str, str], dict[str, int]]:
    """How many ticks each endpoint key appears in, per store (a key one store never
    fetched shows up as 0 because the key set is the union of both)."""
    seen: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for snap in snaps:
        for (venue, _), keys in snap.fetches.items():
            for key in keys:
                if (venue, key) != DISCOVERY:
                    seen[(venue, key)][snap.label] += 1
    return {k: {s.label: c[s.label] for s in snaps} for k, c in sorted(seen.items())}


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%MZ")


def _hhmm(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%H:%M")


def _run_text(run: tuple[int, int], step: int) -> str:
    a, b = run
    n = (b - a) // step + 1
    return f"{_hhmm(a)}" if n == 1 else f"{_hhmm(a)}–{_hhmm(b)} ({n})"


def render(report: CoverageReport, shown: int = 10) -> str:
    """Plain-text report for the terminal and the WorkLog."""
    labels = (report.baseline, report.candidate)
    lines = [
        f"coverage {report.day} (UTC): baseline {report.baseline}, candidate {report.candidate}",
        "",
    ]
    lines.append(
        f"{'venue':<12}{'expected':>9}" + "".join(f"{lb + ' ok/err/miss':>26}" for lb in labels)
    )
    for venue in CADENCE_MS:
        row = f"{venue:<12}{report.coverage[report.baseline][venue].expected:>9}"
        for lb in labels:
            c = report.coverage[lb][venue]
            row += f"{f'{c.ok}/{c.error}/{c.missing} ({c.pct:.2f} %)':>26}"
        lines.append(row)

    for lb in labels:
        for venue, c in report.coverage[lb].items():
            step = CADENCE_MS[venue]
            if c.missing_runs:
                runs = ", ".join(_run_text(r, step) for r in c.missing_runs[:shown])
                more = (
                    f" … +{len(c.missing_runs) - shown} runs" if len(c.missing_runs) > shown else ""
                )
                lines.append(f"  {lb} {venue} missing: {runs}{more}")
            if c.error_ticks:
                ticks = ", ".join(_hhmm(t) for t in c.error_ticks[:shown])
                lines.append(f"  {lb} {venue} error: {ticks}")

    gaps = ", ".join(
        f"{lb} {g / 60_000:.0f} min" if g is not None else f"{lb} none"
        for lb, g in report.discovery_gap_ms.items()
    )
    lines += [
        "",
        f"polymarket discovery, longest gap "
        f"(expected ≤ {DISCOVERY_INTERVAL_MS // 60_000} min): {gaps}",
    ]

    lines.append(
        f"endpoint keys present in only one store (ticks both have): {len(report.endpoint_diffs)}"
    )
    for d in report.endpoint_diffs[:shown]:
        lines.append(f"  {d.venue} {_hhmm(d.tick)} {d.endpoint_key}: only {d.only_in}")
    uneven = [(k, c) for k, c in report.presence.items() if len(set(c.values())) > 1]
    lines.append(f"endpoint keys fetched in a different number of ticks: {len(uneven)}")
    for (venue, key), c in uneven[:shown]:
        lines.append(f"  {venue} {key}: " + ", ".join(f"{lb} {n}" for lb, n in c.items()))

    lines.append("rows per tick (ticks both have; counts only, prices differ by fetch time):")
    for c in report.counts:
        lines.append(f"  {c.table:<18} compared {c.compared:>5}, differing {len(c.differing):>4}")
        for t, a, b in c.differing[:3]:
            lines.append(f"    {_hhmm(t)}: {report.baseline} {a}, {report.candidate} {b}")

    s = report.settlements
    lines.append(
        f"settlements: {s.both} in both, {len(s.differing)} differ, "
        f"{s.only_baseline} only {report.baseline}, {s.only_candidate} only {report.candidate}"
    )
    for venue, cid, a, b in s.differing[:shown]:
        # The fetch times matter: Kalshi can revise a value after settling, so stores
        # whose latest fetch differs may legitimately disagree.
        lines.append(
            f"  {venue} {cid}: {report.baseline} {a.values()} (fetched {_iso(a.fetched_at)}) "
            f"vs {report.candidate} {b.values()} (fetched {_iso(b.fetched_at)})"
        )

    problems = report.problems()
    lines.append("")
    lines += [f"FAIL {p}" for p in problems]
    for table, t, a, b in report.fewer_rows()[:shown]:
        lines.append(
            f"needs a cause: {table} {_hhmm(t)} {report.baseline} {a} rows, {report.candidate} {b}"
        )
    for p in report.page_shortfalls[:shown]:
        lines.append(
            f"needs a cause: {p.venue} {_hhmm(p.tick)} {p.series}: {report.baseline} "
            f"{p.baseline_pages} pages, {report.candidate} {p.candidate_pages}"
            + ("" if p.candidate_tick_ok else f" ({report.candidate} tick is an error)")
        )
    lines.append(
        f"{'FAIL' if problems else 'PASS (automatic part)'}; "
        f"{report.unexplained()} {report.candidate} item(s) "
        "(ticks not ok, fewer rows or fewer pages), each needs a stated cause"
    )
    return "\n".join(lines)


def to_json(report: CoverageReport) -> dict:
    """Machine-readable summary (keys are strings; ticks are ISO times)."""

    def iso(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, UTC).isoformat().replace("+00:00", "Z")

    return {
        "day": report.day,
        "baseline": report.baseline,
        "candidate": report.candidate,
        "coverage": {
            lb: {
                v: {
                    "expected": c.expected,
                    "ok": c.ok,
                    "error": c.error,
                    "missing": c.missing,
                    "pct": round(c.pct, 4),
                    "missing_runs": [[iso(a), iso(b)] for a, b in c.missing_runs],
                    "error_ticks": [iso(t) for t in c.error_ticks],
                }
                for v, c in per.items()
            }
            for lb, per in report.coverage.items()
        },
        "discovery_max_gap_min": {
            lb: (g / 60_000 if g is not None else None) for lb, g in report.discovery_gap_ms.items()
        },
        "endpoint_diffs": len(report.endpoint_diffs),
        "row_counts": {
            c.table: {"compared": c.compared, "differing": len(c.differing)} for c in report.counts
        },
        "settlements": {
            "both": report.settlements.both,
            "differing": len(report.settlements.differing),
            "only_baseline": report.settlements.only_baseline,
            "only_candidate": report.settlements.only_candidate,
        },
        "problems": report.problems(),
        "candidate_ticks_needing_a_cause": report.unexplained(),
        "fewer_rows": [[t, iso(tick), a, b] for t, tick, a, b in report.fewer_rows()],
        "fewer_pages": [
            {
                "venue": p.venue,
                "tick": iso(p.tick),
                "series": p.series,
                "baseline_pages": p.baseline_pages,
                "candidate_pages": p.candidate_pages,
                "candidate_tick_ok": p.candidate_tick_ok,
            }
            for p in report.page_shortfalls
        ],
        "endpoint_presence": {
            f"{venue} {key}": dict(c) for (venue, key), c in report.presence.items()
        },
    }


def pinned_entries(
    root: Path, expect_head: str | None
) -> tuple[list[ManifestEntry], list[Problem]]:
    """Verify `root` and return the exact entry list that was verified.

    The entries are read first, and `verify` must end at their last chain value (and
    that must equal `expect_head` if given), so an amendment appended in between makes
    verification fail instead of going unchecked.
    """
    manifest = Manifest(root)
    entries = list(manifest.entries())
    pin = entries[-1].chain if entries else None
    if expect_head is not None and pin != expect_head:
        msg = f"head {str(pin)[:12]}… does not match expected {expect_head[:12]}…"
        return entries, [Problem(None, None, msg)]
    return entries, manifest.verify(expect_head=pin)
