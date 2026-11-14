"""One pass of the off-machine backup: copy what's new in the hash chain to the store.

S3 layout under `prefix` (default `vm/`), every key write-once:

    <prefix><entry.path>             each merged file, at its path relative to the data root
    <prefix>manifest/000025.jsonl    the manifest as it was when its last entry had seq 25
    <prefix>heads/000025.json        {"seq": 25, "head": <chain>, "manifest_sha256": ...}

One pass, in this order:

1. Snapshot the manifest once (complete lines only: the merger may be appending) and
   check its chain. A broken chain is refused (`integrity`).
2. Read the newest head already in the store. The local chain must *extend* it: same
   chain value at that seq. If not, history was rewritten, so the pass is refused
   (`fork`) and nothing is uploaded.
3. Upload each new entry's file after re-hashing it against the manifest (`integrity` on
   a mismatch). A key that already exists (a pass crashed halfway) must carry the same
   SHA-256, otherwise `fork`.
4. Upload the manifest snapshot, then the head, last. So every head in the store refers
   only to objects that are already there: a crash at any point leaves the store
   consistent, and the next pass finishes the job.

The VM can't overwrite or delete anything in the store (bucket policy), so the newest
head is a fixed point that a later truncated or rewritten manifest can be checked
against: `ppm verify --expect-head <head>`.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path

from ppm.backup.store import ObjectStore, sha256_bytes
from ppm.integrity.manifest import ManifestEntry, chain_problems, parse_entries, sha256_file

MANIFEST_NAME = "manifest.jsonl"


class BackupRefused(Exception):
    """The pass stopped without writing a head. `reason` is "fork" or "integrity"."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class Head:
    """The record that pins the chain off-machine. Deterministic JSON (no timestamp; S3
    records when it was written), so a rerun produces byte-identical content."""

    seq: int
    head: str
    manifest_sha256: str

    def to_bytes(self) -> bytes:
        body = {"seq": self.seq, "head": self.head, "manifest_sha256": self.manifest_sha256}
        return json.dumps(body, sort_keys=True, separators=(",", ":")).encode() + b"\n"

    @classmethod
    def from_bytes(cls, data: bytes) -> Head:
        d = json.loads(data)
        return cls(
            seq=int(d["seq"]), head=str(d["head"]), manifest_sha256=str(d["manifest_sha256"])
        )


@dataclass(slots=True)
class BackupReport:
    head: Head | None = None  # the head this pass wrote, None if nothing was new
    files: int = 0
    bytes: int = 0
    already_there: int = 0  # files a crashed earlier pass had already uploaded
    seconds: float = 0.0
    new_entries: list[int] = field(default_factory=list)


def head_key(prefix: str, seq: int) -> str:
    return f"{prefix}heads/{seq:06d}.json"  # zero-padded: lexical order = numeric order


def manifest_key(prefix: str, seq: int) -> str:
    return f"{prefix}manifest/{seq:06d}.jsonl"


def latest_head(store: ObjectStore, prefix: str) -> Head | None:
    keys = sorted(store.list_keys(f"{prefix}heads/"))
    return Head.from_bytes(store.get_bytes(keys[-1])) if keys else None


def snapshot_manifest(data_root: Path) -> tuple[bytes, list[ManifestEntry]]:
    """Manifest bytes up to the last complete line, and the entries they hold."""
    path = data_root / MANIFEST_NAME
    if not path.exists():
        return b"", []
    raw = path.read_bytes()
    complete = raw[: raw.rfind(b"\n") + 1]  # drop a line the merger is still writing
    try:
        entries = parse_entries(complete.decode())
    except (ValueError, TypeError) as exc:
        raise BackupRefused("integrity", f"manifest is unreadable: {exc}") from exc
    return complete, entries


class BackupJob:
    """Copies new manifest entries, the manifest and the head to a write-once store."""

    def __init__(self, data_root: Path, store: ObjectStore, prefix: str = "vm/") -> None:
        if not prefix.endswith("/"):
            raise ValueError(f"prefix must end with '/', got {prefix!r}")
        self.root = data_root
        self.store = store
        self.prefix = prefix

    def run_once(self) -> BackupReport:
        start = time.monotonic()
        report = BackupReport()
        snapshot, entries = snapshot_manifest(self.root)
        if problems := chain_problems(entries):
            p = problems[0]
            raise BackupRefused("integrity", f"seq={p.seq} {p.message}")
        if not entries:
            return report

        stored = latest_head(self.store, self.prefix)
        if stored is not None:
            self._check_extends(stored, entries)
            if stored.seq == entries[-1].seq:
                report.seconds = time.monotonic() - start
                return report  # nothing new
        first_new = 0 if stored is None else stored.seq + 1

        for entry in entries[first_new:]:
            self._upload_entry(entry, report)
            report.new_entries.append(entry.seq)

        head = Head(entries[-1].seq, entries[-1].chain, sha256_bytes(snapshot))
        self._put_once(manifest_key(self.prefix, head.seq), snapshot, head.manifest_sha256)
        head_bytes = head.to_bytes()
        self._put_once(head_key(self.prefix, head.seq), head_bytes, sha256_bytes(head_bytes))
        report.head = head
        report.seconds = time.monotonic() - start
        return report

    def _check_extends(self, stored: Head, entries: list[ManifestEntry]) -> None:
        if stored.seq >= len(entries):
            raise BackupRefused(
                "fork",
                f"local manifest has {len(entries)} entries but the store's head is at "
                f"seq {stored.seq}: entries were removed (truncated manifest?)",
            )
        local = entries[stored.seq].chain
        if local != stored.head:
            raise BackupRefused(
                "fork",
                f"chain at seq {stored.seq} is {local[:12]}… locally but {stored.head[:12]}… "
                "in the store: history was rewritten",
            )

    def _upload_entry(self, entry: ManifestEntry, report: BackupReport) -> None:
        path = self.root / entry.path
        if not path.is_file():
            raise BackupRefused("integrity", f"seq={entry.seq} file is missing: {entry.path}")
        actual = sha256_file(path)
        if actual != entry.sha256:
            raise BackupRefused(
                "integrity", f"seq={entry.seq} file hash mismatch (contents changed): {entry.path}"
            )
        key = self.prefix + entry.path
        # HEAD first: after a crashed pass, re-sending a 400 MB tar just to get 412 back
        # would waste the whole upload. put_*_if_absent still guards the rare race.
        if self.store.sha256_of(key) is None and self.store.put_file_if_absent(
            key, path, entry.sha256
        ):
            report.files += 1
            report.bytes += entry.bytes
        else:
            self._same_or_fork(key, entry.sha256)
            report.already_there += 1

    def _put_once(self, key: str, data: bytes, sha256: str) -> None:
        if not self.store.put_bytes_if_absent(key, data, sha256):
            self._same_or_fork(key, sha256)

    def _same_or_fork(self, key: str, sha256: str) -> None:
        existing = self.store.sha256_of(key)
        if existing != sha256:
            raise BackupRefused(
                "fork",
                f"{key} already exists with different contents "
                f"(store sha256 {str(existing)[:12]}…, local {sha256[:12]}…)",
            )
