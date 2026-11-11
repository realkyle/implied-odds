"""On-disk layout and atomic writes.

Every file name is derived from *what* was fetched and *when* (venue, envelope key,
snapshot tick), never from when it was processed. Reprocessing a redelivered message
therefore overwrites the same file with identical content: idempotent by construction.

data/
  raw/{venue}/date=D/{HHMM}_{key}.env                   exact bytes received (replayable)
  staging/{table}/date=D/{HHMM}_{venue}_{key}.parquet   one file per fetch per table
  quarantine/{table}/date=D/{HHMM}_{venue}_{key}.jsonl  rows that failed validation
  quarantine/_envelope/{stream}_{message_id}.bin        messages that could not be decoded
(D = YYYY-MM-DD of the snapshot tick, UTC)
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ppm.pipeline.envelope import Envelope
from ppm.writer.schemas import Rejected


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write to a temp file, fsync, then rename over the target.

    os.replace is atomic on POSIX: readers see the old file or the new one, never a
    partially written one, even if we crash mid-write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def atomic_write_table(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    pq.write_table(table, tmp, compression="zstd")
    with open(tmp, "rb+") as fh:
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def _tick_parts(snapshot_ts: int) -> tuple[str, str]:
    dt = datetime.fromtimestamp(snapshot_ts / 1000, tz=UTC)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H%M")


class DataStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def raw_path(self, env: Envelope) -> Path:
        date, hhmm = _tick_parts(env.snapshot_ts)
        return self.root / "raw" / env.venue / f"date={date}" / f"{hhmm}_{env.key}.env"

    def staging_path(self, table: str, env: Envelope) -> Path:
        date, hhmm = _tick_parts(env.snapshot_ts)
        return (
            self.root / "staging" / table / f"date={date}" / f"{hhmm}_{env.venue}_{env.key}.parquet"
        )

    def quarantine_path(self, table: str, env: Envelope) -> Path:
        date, hhmm = _tick_parts(env.snapshot_ts)
        return (
            self.root
            / "quarantine"
            / table
            / f"date={date}"
            / f"{hhmm}_{env.venue}_{env.key}.jsonl"
        )

    def write_raw(self, env: Envelope) -> None:
        atomic_write_bytes(self.raw_path(env), env.encode())

    def write_table(self, table: str, env: Envelope, data: pa.Table) -> None:
        atomic_write_table(self.staging_path(table, env), data)

    def write_quarantine(self, table: str, env: Envelope, rejected: list[Rejected]) -> None:
        lines = [json.dumps({"reason": r.reason, "row": r.row}, default=str) for r in rejected]
        atomic_write_bytes(self.quarantine_path(table, env), ("\n".join(lines) + "\n").encode())

    def write_undecodable(
        self, stream: str, message_id: str, fields: Mapping[bytes, bytes], reason: str
    ) -> Path:
        safe_stream = stream.replace(":", "_")
        path = self.root / "quarantine" / "_envelope" / f"{safe_stream}_{message_id}.bin"
        blob = json.dumps(
            {
                "reason": reason,
                "fields": {
                    k.decode(errors="replace"): v.decode(errors="replace")
                    for k, v in fields.items()
                },
            }
        ).encode()
        atomic_write_bytes(path, blob)
        return path
