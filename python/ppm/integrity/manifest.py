"""Hash-chained manifest of every finalized data file: tamper evidence for the dataset.

`data/manifest.jsonl` holds one JSON line per file the daily merge produced. Each entry
stores the file's SHA-256 *and* a chain value:

    chain = SHA-256(previous entry's chain + this entry's canonical JSON)

Changing, deleting or swapping any file changes its hash, and editing any manifest line
changes its chain value and every one after it. `verify` recomputes all of it and
reports every mismatch. It detects tampering; it cannot prevent it. A truncated
manifest (the last entries removed) is only detectable against a previously recorded
head value, which is why `verify` prints the head and accepts `expect_head`.

The manifest is append-only (decided 2026-11-11). A corrected day gets a new file and a
new entry that names the file it supersedes and why; history is never rewritten.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from pathlib import Path

GENESIS = "0" * 64  # `prev` of the first entry
_CHUNK = 1 << 20


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(_CHUNK):
            h.update(block)
    return h.hexdigest()


def _canonical(fields: dict) -> bytes:
    """Stable JSON: sorted keys, no whitespace, so the same entry always hashes the same."""
    return json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    seq: int  # 0, 1, 2, ... with no gaps
    date: str  # data day, YYYY-MM-DD (UTC)
    kind: str  # "table" (merged Parquet) or "raw" (daily archive of raw responses)
    name: str  # table name or venue
    path: str  # relative to the data root
    count: int  # rows (table) or archived responses (raw)
    bytes: int
    sha256: str
    created_ts: int  # ms, when the merge wrote it
    supersedes: str | None  # path of the table file this replaces, if an amendment
    reason: str | None  # why an amendment was needed
    prev: str  # chain value of the previous entry (GENESIS for the first)
    chain: str

    def body(self) -> dict:
        """Everything the chain value covers (all fields except `chain` itself)."""
        fields = asdict(self)
        del fields["chain"]
        return fields

    def expected_chain(self) -> str:
        return hashlib.sha256(self.prev.encode() + _canonical(self.body())).hexdigest()


@dataclass(frozen=True, slots=True)
class Problem:
    """One verification failure, in words a person can act on."""

    seq: int | None
    path: str | None
    message: str


class Manifest:
    def __init__(self, data_root: Path) -> None:
        self.root = data_root
        self.path = data_root / "manifest.jsonl"

    def entries(self) -> Iterator[ManifestEntry]:
        if not self.path.exists():
            return
        with open(self.path, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield ManifestEntry(**json.loads(line))

    def head(self) -> str:
        """Chain value of the last entry: one hash that commits to the whole dataset."""
        last = None
        for last in self.entries():  # noqa: B007 - we only want the final one
            pass
        return last.chain if last else GENESIS

    def append(
        self,
        *,
        date: str,
        kind: str,
        name: str,
        path: Path,
        count: int,
        created_ts: int,
        supersedes: str | None = None,
        reason: str | None = None,
    ) -> ManifestEntry:
        """Hash `path`, chain it to the previous entry, and append durably (fsync)."""
        existing = list(self.entries())
        prev = existing[-1].chain if existing else GENESIS
        draft = ManifestEntry(
            seq=len(existing),
            date=date,
            kind=kind,
            name=name,
            path=str(path.relative_to(self.root)),
            count=count,
            bytes=path.stat().st_size,
            sha256=sha256_file(path),
            created_ts=created_ts,
            supersedes=supersedes,
            reason=reason,
            prev=prev,
            chain="",
        )
        entry = ManifestEntry(**{**draft.body(), "chain": draft.expected_chain()})
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "ab") as fh:
            fh.write(_canonical(asdict(entry)) + b"\n")
            fh.flush()
            os.fsync(fh.fileno())
        return entry

    def verify(
        self, managed_dirs: tuple[str, ...] = ("tables", "archive"), expect_head: str | None = None
    ) -> list[Problem]:
        """Recompute the chain and every file hash. Empty list = verified."""
        problems: list[Problem] = []
        prev = GENESIS
        listed: set[str] = set()
        try:
            entries = list(self.entries())
        except (ValueError, TypeError) as exc:
            return [Problem(None, None, f"manifest is unreadable: {exc}")]
        for i, e in enumerate(entries):
            if e.seq != i:
                problems.append(Problem(e.seq, e.path, f"sequence gap: expected seq {i}"))
            if e.prev != prev:
                problems.append(Problem(e.seq, e.path, "prev does not match the previous chain"))
            if e.chain != e.expected_chain():
                problems.append(Problem(e.seq, e.path, "chain mismatch: entry was edited"))
            prev = e.chain
            listed.add(e.path)
            file = self.root / e.path
            if not file.exists():
                problems.append(Problem(e.seq, e.path, "file is missing"))
            elif sha256_file(file) != e.sha256:
                problems.append(Problem(e.seq, e.path, "file hash mismatch: contents changed"))
        for d in managed_dirs:
            for file in sorted((self.root / d).rglob("*")):
                rel = str(file.relative_to(self.root))
                if file.is_file() and not file.name.startswith(".") and rel not in listed:
                    problems.append(Problem(None, rel, "file is not in the manifest"))
        if expect_head is not None and prev != expect_head:
            problems.append(
                Problem(
                    None, None, f"head {prev[:12]}… does not match expected {expect_head[:12]}…"
                )
            )
        return problems
