"""`ppm coverage` (PLAN 2d1): tick coverage of two stores against the expected grid.

Each test builds tiny merged stores (real Parquet files + a real hash-chained manifest),
so `load_store` reads them exactly the way it reads `data/` or a restore from S3.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from ppm import cli
from ppm.analysis.coverage import (
    CADENCE_MS,
    DAY_MS,
    CoverageError,
    compare,
    day_start_ms,
    expected_ticks,
    load_store,
    pinned_entries,
    render,
)
from ppm.integrity.manifest import Manifest

DAY = "2026-11-15"
T0 = day_start_ms(DAY)
MIN = 60_000
HOUR = 3_600_000
KEYS = {
    "deribit": ["book_summary-BTC-option", "index_price-btc_usd"],
    "kalshi": ["markets-KXBTC", "markets-KXBTCD"],
    "polymarket": ["books-bitcoin_above"],
    "reference": ["binance_klines-BTCUSDT"],
    "settlements": ["kalshi_settled-00-KXBTC"],
}
TS = pa.timestamp("ms", tz="UTC")
FETCH = pa.schema(
    [
        ("snapshot_ts", TS),
        ("venue", pa.string()),
        ("endpoint_key", pa.string()),
        ("status", pa.string()),
        ("error", pa.string()),
    ]
)
SETTLE = pa.schema(
    [
        ("snapshot_ts", TS),
        ("venue", pa.string()),
        ("contract_id", pa.string()),
        ("settle_ts", TS),
        ("result", pa.string()),
        ("settlement_value", pa.float64()),
    ]
)


def fetches(
    drop: set[tuple[str, int]] = frozenset(),
    fail: set[tuple[str, int]] = frozenset(),
    skip_key: set[tuple[str, int, str]] = frozenset(),
    events_minute: int | None = 0,
) -> list[dict]:
    """A full day of fetch_log rows, minus `drop`; `fail` ticks get HTTP 500.

    Polymarket discovery (`events`) runs every 10 minutes at `events_minute` past.
    """
    rows = []
    for venue, step in CADENCE_MS.items():
        for tick in range(T0, T0 + DAY_MS, step):
            if (venue, tick) in drop:
                continue
            status = "500" if (venue, tick) in fail else "200"
            for key in KEYS[venue]:
                if (venue, tick, key) not in skip_key:
                    rows.append(_fetch(tick, venue, key, status))
            minute = (tick - T0) // MIN
            if venue == "polymarket" and events_minute is not None and minute % 10 == events_minute:
                rows.append(_fetch(tick, venue, "events", "200"))
    return rows


def _fetch(tick: int, venue: str, key: str, status: str) -> dict:
    return {"snapshot_ts": tick, "venue": venue, "endpoint_key": key, "status": status}


def settlement(cid: str, value: float, snap: int = T0, venue: str = "kalshi") -> dict:
    return {
        "snapshot_ts": snap,
        "venue": venue,
        "contract_id": cid,
        "settle_ts": T0 - HOUR,
        "result": "yes",
        "settlement_value": value,
    }


def add_table(root: Path, day: str, name: str, table: pa.Table, part: int = 1) -> None:
    path = root / "tables" / name / f"date={day}" / f"part-{part}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    Manifest(root).append(
        date=day, kind="table", name=name, path=path, count=table.num_rows, created_ts=T0
    )


def make_store(
    root: Path,
    fetch_rows: list[dict] | None = None,
    settle_rows: list[dict] | None = None,
    books_per_tick: dict[int, int] | None = None,
) -> str:
    """Write a merged store; returns its chain head."""
    root.mkdir(parents=True, exist_ok=True)
    rows = fetches() if fetch_rows is None else fetch_rows
    add_table(root, DAY, "fetch_log", pa.Table.from_pylist(rows, schema=FETCH))
    if books_per_tick is not None:
        ts = [t for t, n in books_per_tick.items() for _ in range(n)]
        add_table(root, DAY, "polymarket_books", pa.table({"snapshot_ts": pa.array(ts, TS)}))
    if settle_rows is not None:
        add_table(root, DAY, "settlements", pa.Table.from_pylist(settle_rows, schema=SETTLE))
    return Manifest(root).head()


def report_for(tmp_path: Path, base: dict | None = None, cand: dict | None = None):
    make_store(tmp_path / "laptop", **(base or {}))
    make_store(tmp_path / "vm", **(cand or {}))
    return compare(
        load_store("laptop", tmp_path / "laptop", DAY), load_store("vm", tmp_path / "vm", DAY)
    )


def test_grid_comes_from_the_recorders():
    assert len(expected_ticks(DAY, "deribit")) == 1440
    assert len(expected_ticks(DAY, "settlements")) == 24
    assert expected_ticks(DAY, "kalshi")[0] == T0  # 2026-11-15T00:00Z
    assert set(CADENCE_MS) == {"deribit", "kalshi", "polymarket", "reference", "settlements"}


def test_full_day_in_both_stores_passes_with_nothing_to_explain(tmp_path):
    r = report_for(tmp_path)
    for label in ("laptop", "vm"):
        for c in r.coverage[label].values():
            assert (c.ok, c.error, c.missing) == (c.expected, 0, 0)
    assert r.problems() == []
    assert r.unexplained() == 0
    assert r.endpoint_diffs == ()
    assert "PASS" in render(r)


def test_missing_ticks_are_counted_and_grouped_into_runs(tmp_path):
    gone = {("deribit", T0 + m * MIN) for m in (5, 6, 7, 100)}
    r = report_for(tmp_path, cand={"fetch_rows": fetches(drop=gone)})
    c = r.coverage["vm"]["deribit"]
    assert (c.ok, c.missing) == (1436, 4)
    assert c.missing_runs == ((T0 + 5 * MIN, T0 + 7 * MIN), (T0 + 100 * MIN, T0 + 100 * MIN))
    assert r.unexplained() == 4
    assert r.problems() == ["deribit: vm ok 1436 < laptop ok 1440"]
    assert "00:05–00:07 (3), 01:40" in render(r)


def test_failed_fetch_makes_the_tick_an_error_not_ok(tmp_path):
    r = report_for(tmp_path, cand={"fetch_rows": fetches(fail={("kalshi", T0 + 3 * MIN)})})
    c = r.coverage["vm"]["kalshi"]
    assert (c.ok, c.error, c.missing) == (1439, 1, 0)
    assert c.error_ticks == (T0 + 3 * MIN,)


def test_candidate_better_than_baseline_passes_but_baseline_gaps_are_shown(tmp_path):
    # The laptop sleeping on battery is the normal case, not a candidate problem.
    asleep = {(v, T0 + m * MIN) for v in ("deribit", "kalshi") for m in range(60, 90)}
    r = report_for(tmp_path, base={"fetch_rows": fetches(drop=asleep)})
    assert r.problems() == []
    assert r.unexplained() == 0
    assert "laptop deribit missing: 01:00–01:29 (30)" in render(r)


def test_discovery_cadence_is_not_an_endpoint_difference(tmp_path):
    # The two machines started at different times, so `events` lands in different minutes.
    r = report_for(
        tmp_path,
        base={"fetch_rows": fetches(events_minute=0)},
        cand={"fetch_rows": fetches(events_minute=7)},
    )
    assert r.endpoint_diffs == ()
    assert r.discovery_gap_ms == {"laptop": 10 * MIN, "vm": 10 * MIN}
    assert r.coverage["vm"]["polymarket"].ok == 1440


def test_discovery_gap_counts_from_midnight_and_to_the_end_of_day(tmp_path):
    r = report_for(tmp_path, cand={"fetch_rows": fetches(events_minute=None)})
    assert r.discovery_gap_ms["vm"] is None
    rows = [x for x in fetches(events_minute=None)] + [
        _fetch(T0 + 600 * MIN, "polymarket", "events", "200")
    ]
    r = report_for(tmp_path / "b", cand={"fetch_rows": rows})
    assert r.discovery_gap_ms["vm"] == 840 * MIN  # 10:00 → 24:00


def test_tick_with_only_discovery_counts_as_missing(tmp_path):
    t = T0 + 20 * MIN  # minute 20 is a discovery minute
    rows = fetches(skip_key={("polymarket", t, "books-bitcoin_above")})
    r = report_for(tmp_path, cand={"fetch_rows": rows})
    assert r.coverage["vm"]["polymarket"].missing == 1


def test_endpoint_present_in_only_one_store_is_listed(tmp_path):
    t = T0 + 9 * MIN
    rows = fetches(skip_key={("kalshi", t, "markets-KXBTCD")})
    r = report_for(tmp_path, cand={"fetch_rows": rows})
    assert [(d.venue, d.tick, d.endpoint_key, d.only_in) for d in r.endpoint_diffs] == [
        ("kalshi", t, "markets-KXBTCD", "laptop")
    ]
    assert r.coverage["vm"]["kalshi"].ok == 1440  # the tick itself was recorded


def test_row_counts_compared_on_ticks_both_fetched_ok_including_empty_ones(tmp_path):
    t1, t2, t3, t4 = T0, T0 + MIN, T0 + 2 * MIN, T0 + 3 * MIN
    r = report_for(
        tmp_path,
        base={"books_per_tick": {t1: 11, t2: 11, t3: 11, t4: 11}},
        # t3: the VM fetched ok but kept 0 rows (all rejected); t4: the VM never fetched
        cand={
            "fetch_rows": fetches(drop={("polymarket", t4)}),
            "books_per_tick": {t1: 11, t2: 10},
        },
    )
    books = next(c for c in r.counts if c.table == "polymarket_books")
    assert books.compared == 1439  # every tick both fetched ok; t4 is a coverage miss instead
    assert books.differing == ((t2, 11, 10), (t3, 11, 0))


def test_failed_discovery_fetches_do_not_shorten_the_gap(tmp_path):
    rows = fetches(events_minute=None) + [
        {**_fetch(T0 + m * MIN, "polymarket", "events", "500")} for m in range(1, 1440)
    ]
    r = report_for(tmp_path, cand={"fetch_rows": rows})
    assert r.discovery_gap_ms["vm"] is None  # retried every minute, never succeeded


def test_amendment_appended_after_pinning_is_not_read_unverified(tmp_path):
    root = tmp_path / "s"
    make_store(root, fetch_rows=fetches(drop={("deribit", T0)}))
    entries, problems = pinned_entries(root, None)
    assert problems == []
    # a merge appends an amendment between verify and the read
    add_table(root, DAY, "fetch_log", pa.Table.from_pylist(fetches(), schema=FETCH), part=2)
    snap = load_store("s", root, DAY, entries)
    assert ("deribit", T0) not in snap.fetches  # still the verified part-1
    _, problems = pinned_entries(root, entries[-1].chain)
    assert [p.message[:4] for p in problems] == ["head"]  # the old pin no longer matches
    assert "does not match expected" in problems[0].message


def test_failure_injected_missing_tick_and_settlement_value_are_both_reported(tmp_path):
    """PLAN 2d1 failure test."""
    r = report_for(
        tmp_path,
        base={"settle_rows": [settlement("KXBTC-A", 1.0), settlement("KXBTC-B", 2.0)]},
        cand={
            "fetch_rows": fetches(drop={("reference", T0 + 42 * MIN)}),
            "settle_rows": [settlement("KXBTC-A", 1.0), settlement("KXBTC-B", 2.5)],
        },
    )
    assert r.coverage["vm"]["reference"].missing_runs == ((T0 + 42 * MIN, T0 + 42 * MIN),)
    assert r.settlements.both == 2
    assert [(v, c) for v, c, _, _ in r.settlements.differing] == [("kalshi", "KXBTC-B")]
    assert r.problems() == [
        "reference: vm ok 1439 < laptop ok 1440",
        "1 settlement(s) differ",
    ]
    assert "FAIL" in render(r)


def test_settlements_in_only_one_store_are_counted_not_failed(tmp_path):
    r = report_for(
        tmp_path,
        base={"settle_rows": [settlement("A", 1.0), settlement("B", 1.0)]},
        cand={"settle_rows": [settlement("A", 1.0), settlement("C", 1.0)]},
    )
    s = r.settlements
    assert (s.both, s.only_baseline, s.only_candidate, s.differing) == (1, 1, 1, ())
    assert r.problems() == []


def test_settlements_from_earlier_days_count_and_latest_fetch_wins(tmp_path):
    for name, early_value in (("laptop", 1.0), ("vm", 9.0)):
        root = tmp_path / name
        make_store(root, settle_rows=[settlement("A", 1.0, snap=T0 + HOUR)])
        # an older day's file with a different, earlier value for the same contract
        old = pa.Table.from_pylist([settlement("A", early_value, snap=T0 - DAY_MS)], schema=SETTLE)
        add_table(root, "2026-11-14", "settlements", old)
    r = compare(
        load_store("laptop", tmp_path / "laptop", DAY), load_store("vm", tmp_path / "vm", DAY)
    )
    assert r.settlements.both == 1
    assert r.settlements.differing == ()


def test_amended_table_uses_the_latest_part(tmp_path):
    root = tmp_path / "s"
    make_store(root, fetch_rows=fetches(drop={("deribit", T0)}))
    add_table(root, DAY, "fetch_log", pa.Table.from_pylist(fetches(), schema=FETCH), part=2)
    assert load_store("s", root, DAY).fetches.get(("deribit", T0)) is not None


def test_store_without_the_day_cannot_be_compared(tmp_path):
    root = tmp_path / "s"
    make_store(root)
    with pytest.raises(CoverageError, match="no merged fetch_log for 2026-11-16"):
        load_store("s", root, "2026-11-16")


def _cli(tmp_path, head: str, *extra: str) -> int:
    with pytest.raises(SystemExit) as exc:
        cli.main(
            [
                "coverage",
                "--date",
                DAY,
                "--baseline",
                str(tmp_path / "laptop"),
                "--candidate",
                str(tmp_path / "vm"),
                "--candidate-head",
                head,
                *extra,
            ]
        )
    return exc.value.code


def test_cli_prints_the_report_and_writes_json(tmp_path, capsys):
    make_store(tmp_path / "laptop")
    head = make_store(tmp_path / "vm", fetch_rows=fetches(drop={("kalshi", T0)}))
    out = tmp_path / "cov.json"
    assert _cli(tmp_path, head, "--json", str(out)) == 1  # VM worse than laptop: FAIL
    assert "kalshi missing: 00:00" in capsys.readouterr().out
    summary = json.loads(out.read_text())
    assert summary["coverage"]["vm"]["kalshi"]["missing"] == 1
    assert summary["coverage"]["vm"]["kalshi"]["missing_runs"] == [
        ["2026-11-15T00:00:00Z", "2026-11-15T00:00:00Z"]
    ]
    assert summary["candidate_ticks_needing_a_cause"] == 1


def test_cli_refuses_a_candidate_that_does_not_end_at_the_expected_head(tmp_path, capsys):
    make_store(tmp_path / "laptop")
    make_store(tmp_path / "vm")
    assert _cli(tmp_path, "0" * 64) == 2
    out = capsys.readouterr().out
    assert "does not match expected" in out
    assert "coverage 2026-11-15" not in out  # no report from unverified data


def test_cli_refuses_a_tampered_baseline(tmp_path, capsys):
    make_store(tmp_path / "laptop")
    head = make_store(tmp_path / "vm")
    victim = tmp_path / "laptop" / "tables" / "fetch_log" / f"date={DAY}" / "part-1.parquet"
    victim.write_bytes(victim.read_bytes() + b"x")
    assert _cli(tmp_path, head) == 2
    assert "laptop: VERIFICATION FAILED" in capsys.readouterr().out


def test_cli_exits_0_when_the_automatic_part_passes(tmp_path, capsys):
    make_store(tmp_path / "laptop", fetch_rows=fetches(drop={("kalshi", T0)}))
    head = make_store(tmp_path / "vm")
    assert _cli(tmp_path, head) == 0
    assert "PASS (automatic part)" in capsys.readouterr().out


def test_cli_exits_2_when_the_day_is_not_merged(tmp_path):
    make_store(tmp_path / "laptop")
    head = make_store(tmp_path / "vm")
    with pytest.raises(SystemExit) as exc:
        cli.main(
            [
                "coverage",
                "--date",
                "2026-11-16",
                "--baseline",
                str(tmp_path / "laptop"),
                "--candidate",
                str(tmp_path / "vm"),
                "--candidate-head",
                head,
            ]
        )
    assert exc.value.code == 2


def test_candidate_never_fetching_an_endpoint_fails_although_every_tick_is_ok(tmp_path):
    """Review finding: "ok" alone would give the VM 1440/1440 here."""
    skip = {("kalshi", t, "markets-KXBTCD") for t in expected_ticks(DAY, "kalshi")}
    r = report_for(tmp_path, cand={"fetch_rows": fetches(skip_key=skip)})
    assert r.coverage["vm"]["kalshi"].ok == 1440
    assert r.problems() == ["kalshi: vm lacks markets-KXBTCD at 1440 shared tick(s)"]
    assert r.presence[("kalshi", "markets-KXBTCD")] == {"laptop": 1440, "vm": 0}
    assert "kalshi markets-KXBTCD: laptop 1440, vm 0" in render(r)


def test_extra_endpoint_on_the_candidate_is_reported_not_failed(tmp_path):
    skip = {("kalshi", T0, "markets-KXBTCD")}
    r = report_for(tmp_path, base={"fetch_rows": fetches(skip_key=skip)})
    assert r.problems() == []
    assert len(r.endpoint_diffs) == 1


def test_candidate_with_fewer_rows_needs_a_cause_but_more_rows_does_not(tmp_path):
    t1, t2 = T0, T0 + MIN
    r = report_for(
        tmp_path,
        base={"books_per_tick": {t1: 11, t2: 11}},
        cand={"books_per_tick": {t1: 10, t2: 12}},
    )
    assert r.fewer_rows() == [("polymarket_books", t1, 11, 10)]
    assert r.unexplained() == 1
    assert r.problems() == []
    assert "needs a cause: polymarket_books 00:00 laptop 11 rows, vm 10" in render(r)
