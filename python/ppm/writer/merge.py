"""Daily merge: staging's many small files → one deduplicated, hash-chained file per table.

For each finished UTC day D (decisions of 2026-11-11):
  staging/{table}/date=D/*.parquet → tables/{table}/date=D/part-N.parquet  (dedup, sorted)
  raw/{venue}/date=D/*.env         → archive/{venue}/date=D/raw-N.tar       (exact bytes)
Each output gets a manifest entry (integrity/manifest.py); then, and only after the
output is re-read and checked, the inputs are deleted.

Every step can be re-run after a crash:
- an output written but not yet in the manifest is simply overwritten (same name);
- staging left behind after its entry was written re-merges to identical bytes, which
  is detected by hash and adds no new entry;
- raw files already inside an earlier archive are not archived twice.

Late data for an already-merged day becomes an amendment: part-(N+1) holds the union,
names the part it supersedes, and says why. Nothing already recorded is rewritten.
"""

from __future__ import annotations

import logging
import os
import shutil
import tarfile
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from ppm.integrity.manifest import Manifest, ManifestEntry, sha256_file
from ppm.writer.schemas import SCHEMAS

log = logging.getLogger(__name__)

DAY_MS = 86_400_000
GRACE_MS = 2 * 3_600_000  # merge day D from D+1 02:00 UTC, once the queue is drained


@dataclass(frozen=True, slots=True)
class DedupPolicy:
    """Which rows are the same observation, and which copy to keep.

    keep="first":  earliest recv_ts (e.g. laptop and VM both recorded the same tick)
    keep="latest": latest snapshot_ts, then recv_ts (re-collected or re-discovered data)
    by_tick: the key contains snapshot_ts, so duplicates can only share a tick and the
             day can be merged one hour at a time (bounded memory on a 2 GB VM).
    """

    keys: tuple[str, ...]
    keep: str = "first"
    by_tick: bool = True


POLICIES: dict[str, DedupPolicy] = {
    "deribit_options": DedupPolicy(("snapshot_ts", "instrument")),
    "deribit_futures": DedupPolicy(("snapshot_ts", "instrument")),
    "kalshi_markets": DedupPolicy(("snapshot_ts", "ticker")),
    "polymarket_books": DedupPolicy(("snapshot_ts", "yes_token_id")),
    "reference_prices": DedupPolicy(("snapshot_ts", "source", "symbol")),
    "fetch_log": DedupPolicy(("snapshot_ts", "venue", "endpoint_key")),
    "polymarket_contracts": DedupPolicy(("yes_token_id",), keep="latest", by_tick=False),
    "settlements": DedupPolicy(("venue", "contract_id"), keep="latest", by_tick=False),
}


def dedup(table: pa.Table, policy: DedupPolicy) -> pa.Table:
    """One row per key, chosen by the policy, sorted by key (deterministic output)."""
    if table.num_rows == 0:
        return table
    tiebreak = (
        [("recv_ts", "ascending")]
        if policy.keep == "first"
        else [("snapshot_ts", "descending"), ("recv_ts", "descending")]
    )
    order = [(k, "ascending") for k in policy.keys] + tiebreak
    ranked = table.sort_by(order)
    ranked = ranked.append_column("__i", pa.array(range(ranked.num_rows), pa.int64()))
    # After sorting, the row to keep is the first of its key group: the minimum index.
    winners = ranked.group_by(list(policy.keys), use_threads=False).aggregate([("__i", "min")])
    keep = pc.sort_indices(winners["__i_min"])
    return ranked.take(pc.take(winners["__i_min"], keep)).drop_columns(["__i"])


@dataclass(slots=True)
class MergeReport:
    day: str
    tables: dict[str, dict[str, int]] = field(default_factory=dict)
    raw: dict[str, dict[str, int]] = field(default_factory=dict)
    entries: list[ManifestEntry] = field(default_factory=list)
    seconds: float = 0.0


class DailyMerger:
    def __init__(self, data_root: Path, manifest: Manifest | None = None) -> None:
        self.root = data_root
        self.manifest = manifest or Manifest(data_root)

    # --- which days ---------------------------------------------------------------

    def pending_days(self, now_ms: int) -> list[str]:
        """Days with staging or raw files left whose grace period has passed."""
        days: set[str] = set()
        for base in ("staging", "raw"):
            for d in (self.root / base).glob("*/date=*"):
                if any(d.iterdir()):
                    days.add(d.name.removeprefix("date="))
        cutoff = now_ms - GRACE_MS
        return sorted(d for d in days if _day_end_ms(d) <= cutoff)

    def merge_due(self, now_ms: int) -> list[MergeReport]:
        return [self.merge_day(d) for d in self.pending_days(now_ms)]

    # --- one day -----------------------------------------------------------------

    def merge_day(self, day: str) -> MergeReport:
        start = time.monotonic()
        report = MergeReport(day)
        for table in sorted(SCHEMAS):
            files = sorted((self.root / "staging" / table / f"date={day}").glob("*.parquet"))
            if files:
                self._merge_table(day, table, files, report)
        for venue_dir in sorted((self.root / "raw").glob("*")):
            files = sorted((venue_dir / f"date={day}").glob("*.env"))
            if files:
                self._archive_raw(day, venue_dir.name, files, report)
        report.seconds = time.monotonic() - start
        log.info(
            "merged %s in %.1f s: %d manifest entries", day, report.seconds, len(report.entries)
        )
        return report

    def _entries_for(self, day: str, kind: str, name: str) -> list[ManifestEntry]:
        return [e for e in self.manifest.entries() if (e.date, e.kind, e.name) == (day, kind, name)]

    def _merge_table(self, day: str, table: str, files: list[Path], report: MergeReport) -> None:
        policy = POLICIES[table]
        schema = SCHEMAS[table]
        previous = self._entries_for(day, "table", table)
        latest = self.root / previous[-1].path if previous else None
        out = self.root / "tables" / table / f"date={day}" / f"part-{len(previous) + 1}.parquet"

        rows_in = 0
        rows_out = 0
        tmp = out.with_name(f".{out.name}.tmp-{os.getpid()}")
        out.parent.mkdir(parents=True, exist_ok=True)
        with pq.ParquetWriter(tmp, schema, compression="zstd") as writer:
            for chunk in self._chunks(files, latest, policy, schema):
                rows_in += chunk.num_rows
                merged = dedup(chunk, policy)
                rows_out += merged.num_rows
                if merged.num_rows:
                    writer.write_table(merged)  # one row group per hour (by_tick tables)
        _fsync(tmp)

        if latest is not None and sha256_file(tmp) == sha256_file(latest):
            # Leftover staging (crash after the manifest entry) or a re-run with nothing
            # new: identical output, so no amendment. Just clean up the inputs.
            tmp.unlink()
            log.info("%s %s: staging already merged; removing %d files", day, table, len(files))
        else:
            os.replace(tmp, out)
            if pq.ParquetFile(out).metadata.num_rows != rows_out:  # re-read before deleting
                raise RuntimeError(f"{out}: row count mismatch after write; inputs kept")
            entry = self.manifest.append(
                date=day,
                kind="table",
                name=table,
                path=out,
                count=rows_out,
                created_ts=_now_ms(),
                supersedes=previous[-1].path if previous else None,
                reason="late staging files" if previous else None,
            )
            report.entries.append(entry)
        bytes_in = sum(f.stat().st_size for f in files)
        for f in files:
            f.unlink()
        _remove_empty(files[0].parent)
        report.tables[table] = {
            "files_in": len(files),
            "bytes_in": bytes_in,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "bytes_out": out.stat().st_size if out.exists() else 0,
        }

    def _chunks(
        self, files: list[Path], latest: Path | None, policy: DedupPolicy, schema: pa.Schema
    ) -> Iterator[pa.Table]:
        """Staging rows (plus the current merged part, for an amendment) in merge order."""
        if not policy.by_tick:
            sources = [*files, *([latest] if latest else [])]
            yield ds.dataset([str(p) for p in sources], schema=schema).to_table()
            return
        by_hour: dict[str, list[Path]] = {}
        for f in files:
            by_hour.setdefault(f.name[:2], []).append(f)  # file names start with HHMM
        hours = set(by_hour)
        if latest:
            ts = pq.read_table(latest, columns=["snapshot_ts"])["snapshot_ts"]
            hours |= {f"{h:02d}" for h in pc.unique(pc.hour(ts)).to_pylist()}
        for hour in sorted(hours):
            parts = [ds.dataset([str(p) for p in by_hour.get(hour, [])], schema=schema).to_table()]
            if latest:
                old = ds.dataset(str(latest), schema=schema)
                parts.append(old.to_table(filter=pc.hour(ds.field("snapshot_ts")) == int(hour)))
            yield pa.concat_tables(parts)

    def _archive_raw(self, day: str, venue: str, files: list[Path], report: MergeReport) -> None:
        previous = self._entries_for(day, "raw", venue)
        archived: set[str] = set()
        for e in previous:  # never archive the same response twice (crash-safe re-run)
            with tarfile.open(self.root / e.path) as tar:
                archived.update(tar.getnames())
        new = [f for f in files if f.name not in archived]
        out = self.root / "archive" / venue / f"date={day}" / f"raw-{len(previous) + 1}.tar"
        if new:
            _write_tar(out, new)
            with tarfile.open(out) as tar:  # re-read before deleting the inputs
                if len(tar.getnames()) != len(new):
                    raise RuntimeError(f"{out}: member count mismatch; raw files kept")
            entry = self.manifest.append(
                date=day,
                kind="raw",
                name=venue,
                path=out,
                count=len(new),
                created_ts=_now_ms(),
                reason="late raw files" if previous else None,
            )
            report.entries.append(entry)
        bytes_in = sum(f.stat().st_size for f in files)
        for f in files:
            f.unlink()
        _remove_empty(files[0].parent)
        report.raw[venue] = {
            "files_in": len(files),
            "bytes_in": bytes_in,
            "archived": len(new),
            "bytes_out": out.stat().st_size if new else 0,
        }


def _write_tar(out: Path, files: list[Path]) -> None:
    """Uncompressed tar (members are already gzipped) with fixed metadata, so the same
    inputs always give the same bytes and the same hash."""
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.name}.tmp-{os.getpid()}")
    with tarfile.open(tmp, "w", format=tarfile.PAX_FORMAT) as tar:
        for f in sorted(files, key=lambda p: p.name):
            info = tarfile.TarInfo(f.name)
            info.size = f.stat().st_size
            info.mtime, info.mode, info.uid, info.gid = 0, 0o644, 0, 0
            with open(f, "rb") as fh:
                tar.addfile(info, fh)
    _fsync(tmp)
    os.replace(tmp, out)


def _fsync(path: Path) -> None:
    with open(path, "rb+") as fh:
        os.fsync(fh.fileno())


def _remove_empty(directory: Path) -> None:
    if directory.exists() and not any(directory.iterdir()):
        shutil.rmtree(directory)


def _day_end_ms(day: str) -> int:
    start = datetime.combine(date.fromisoformat(day), datetime.min.time(), tzinfo=UTC)
    return int((start + timedelta(days=1)).timestamp() * 1000)


def _now_ms() -> int:
    return time.time_ns() // 1_000_000
