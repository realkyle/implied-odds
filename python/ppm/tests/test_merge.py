"""Daily merge: dedup, archive, manifest, amendments and crash re-runs."""

from __future__ import annotations

import tarfile
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from ppm.integrity.manifest import Manifest, sha256_file
from ppm.tests.conftest import make_envelope
from ppm.writer.merge import GRACE_MS, POLICIES, DailyMerger, DedupPolicy, dedup
from ppm.writer.schemas import SCHEMAS
from ppm.writer.storage import DataStore

DAY = "2026-11-11"


def ms(*args) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


def price_rows(tick: int, recv_offset: int = 300, price: float = 70_000.0):
    return [
        {
            "snapshot_ts": tick,
            "recv_ts": tick + recv_offset,
            "exchange_ts": tick,
            "asset": a,
            "source": "coinbase",
            "symbol": f"{a}-USD",
            "price": price,
            "bid": None,
            "ask": None,
        }
        for a in ("BTC", "ETH")
    ]


def write_tick(store: DataStore, tick: int, **kw) -> None:
    env = make_envelope(
        b'{"price": "1"}',
        venue="reference",
        endpoint="coinbase_ticker",
        params={"product": "ALL"},
        snapshot_ts=tick,
    )
    table = pa.Table.from_pylist(price_rows(tick, **kw), schema=SCHEMAS["reference_prices"])
    store.write_table("reference_prices", env, table)
    store.write_raw(env)


def fill_day(tmp_path, hours=(0, 1, 23)) -> DataStore:
    store = DataStore(tmp_path)
    for h in hours:
        for minute in (0, 1):
            write_tick(store, ms(2026, 11, 11, h, minute))
    return store


def merged(tmp_path, table="reference_prices", part=1) -> pa.Table:
    return pq.read_table(tmp_path / "tables" / table / f"date={DAY}" / f"part-{part}.parquet")


# --- dedup ---------------------------------------------------------------------------


def test_dedup_keeps_the_first_received_copy():
    tick = ms(2026, 11, 11, 12)
    rows = price_rows(tick, recv_offset=900, price=1.0) + price_rows(
        tick, recv_offset=300, price=2.0
    )
    t = pa.Table.from_pylist(rows, schema=SCHEMAS["reference_prices"])
    out = dedup(t, POLICIES["reference_prices"])
    assert out.num_rows == 2 and set(out["price"].to_pylist()) == {2.0}  # the earlier copy


def test_dedup_latest_keeps_the_most_recent_collection():
    policy = DedupPolicy(("source", "symbol"), keep="latest", by_tick=False)
    rows = price_rows(ms(2026, 11, 11, 1), price=1.0) + price_rows(ms(2026, 11, 11, 2), price=2.0)
    out = dedup(pa.Table.from_pylist(rows, schema=SCHEMAS["reference_prices"]), policy)
    assert out["price"].to_pylist() == [2.0, 2.0]


# --- a normal day ------------------------------------------------------------------


def test_merge_day_produces_one_verified_file_per_table_and_venue(tmp_path):
    fill_day(tmp_path)
    raw_before = {
        p.name: p.read_bytes() for p in (tmp_path / "raw/reference" / f"date={DAY}").glob("*")
    }
    report = DailyMerger(tmp_path).merge_day(DAY)

    t = merged(tmp_path)
    assert t.num_rows == 3 * 2 * 2  # 3 hours x 2 ticks x 2 assets
    assert t["snapshot_ts"].to_pylist() == sorted(t["snapshot_ts"].to_pylist())
    assert report.tables["reference_prices"]["files_in"] == 6
    # inputs are gone, outputs are in the manifest, and verification passes
    assert not (tmp_path / "staging" / "reference_prices" / f"date={DAY}").exists()
    assert not (tmp_path / "raw" / "reference" / f"date={DAY}").exists()
    assert Manifest(tmp_path).verify() == []
    assert [(e.kind, e.name) for e in report.entries] == [
        ("table", "reference_prices"),
        ("raw", "reference"),
    ]
    # the archive holds the exact original bytes
    with tarfile.open(tmp_path / "archive/reference" / f"date={DAY}" / "raw-1.tar") as tar:
        assert {m.name: tar.extractfile(m).read() for m in tar.getmembers()} == raw_before


def test_merging_identical_inputs_gives_identical_bytes(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    for root in (a, b):
        fill_day(root)
        DailyMerger(root).merge_day(DAY)
    rel = f"tables/reference_prices/date={DAY}/part-1.parquet"
    assert sha256_file(a / rel) == sha256_file(b / rel)
    rel = f"archive/reference/date={DAY}/raw-1.tar"
    assert sha256_file(a / rel) == sha256_file(b / rel)


def test_hour_by_hour_merge_equals_merging_everything_at_once(tmp_path):
    fill_day(tmp_path, hours=(0, 5, 13, 23))
    DailyMerger(tmp_path).merge_day(DAY)
    whole = pa.Table.from_pylist(
        [r for h in (0, 5, 13, 23) for m in (0, 1) for r in price_rows(ms(2026, 11, 11, h, m))],
        schema=SCHEMAS["reference_prices"],
    )
    expected = dedup(whole, DedupPolicy(POLICIES["reference_prices"].keys, by_tick=False))
    assert merged(tmp_path).equals(expected)


# --- late data and crashes ---------------------------------------------------------


def test_late_data_becomes_an_amendment_that_supersedes(tmp_path):
    store = fill_day(tmp_path)
    merger = DailyMerger(tmp_path)
    merger.merge_day(DAY)
    write_tick(store, ms(2026, 11, 11, 12, 30))  # arrives after the merge
    report = merger.merge_day(DAY)

    (entry,) = [e for e in report.entries if e.kind == "table"]
    assert entry.path.endswith("part-2.parquet")
    assert entry.supersedes.endswith("part-1.parquet") and entry.reason == "late staging files"
    assert merged(tmp_path, part=2).num_rows == merged(tmp_path, part=1).num_rows + 2
    assert (tmp_path / entry.supersedes).exists()  # history kept, never rewritten
    raw = [e for e in report.entries if e.kind == "raw"]
    assert raw[0].path.endswith("raw-2.tar") and raw[0].count == 1  # only the new response
    assert Manifest(tmp_path).verify() == []


def test_leftover_staging_after_a_crash_adds_no_entry(tmp_path):
    """Crash after the manifest entry but before staging was deleted: re-run is a no-op."""
    store = fill_day(tmp_path)
    merger = DailyMerger(tmp_path)
    merger.merge_day(DAY)
    for h in (0, 1, 23):  # put the same staging back, as if deletion never happened
        for minute in (0, 1):
            write_tick(store, ms(2026, 11, 11, h, minute))
    report = merger.merge_day(DAY)
    assert [e for e in report.entries if e.kind == "table"] == []
    assert not (tmp_path / "staging" / "reference_prices" / f"date={DAY}").exists()
    assert len(list(Manifest(tmp_path).entries())) == 2  # unchanged


def test_output_written_but_not_registered_is_overwritten_on_rerun(tmp_path):
    """Crash after writing part-1 but before the manifest append."""
    fill_day(tmp_path)
    out = tmp_path / "tables" / "reference_prices" / f"date={DAY}" / "part-1.parquet"
    out.parent.mkdir(parents=True)
    out.write_bytes(b"half-finished garbage")
    DailyMerger(tmp_path).merge_day(DAY)
    assert merged(tmp_path).num_rows == 12 and Manifest(tmp_path).verify() == []


def test_only_days_past_the_grace_period_are_merged(tmp_path):
    fill_day(tmp_path)
    merger = DailyMerger(tmp_path)
    end_of_day = ms(2026, 11, 12)
    assert merger.pending_days(end_of_day + GRACE_MS - 1) == []
    assert merger.pending_days(end_of_day + GRACE_MS) == [DAY]
