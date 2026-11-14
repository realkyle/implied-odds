"""Rebuild a data directory from the backup store, then let `ppm verify` judge it.

Restore trusts nothing it downloads: the manifest snapshot must hash to the value in the
head record, and the caller then runs `Manifest(dest).verify(expect_head=head.head)`,
which re-hashes every file and the whole chain. Restore never writes into a non-empty
directory, so it can't be pointed at live data by mistake.
"""

from __future__ import annotations

from pathlib import Path

from ppm.backup.job import MANIFEST_NAME, Head, head_key, latest_head, manifest_key
from ppm.backup.store import ObjectStore, sha256_bytes
from ppm.integrity.manifest import parse_entries


class RestoreError(Exception):
    pass


def restore(store: ObjectStore, prefix: str, dest: Path, seq: int | None = None) -> Head:
    """Download the chain as of head `seq` (default: the newest) into `dest`."""
    if dest.exists() and any(dest.iterdir()):
        raise RestoreError(f"{dest} is not empty; restore only into a new directory")
    if seq is None:
        head = latest_head(store, prefix)
        if head is None:
            raise RestoreError(f"no heads under {prefix}heads/")
    else:
        head = Head.from_bytes(store.get_bytes(head_key(prefix, seq)))
    snapshot = store.get_bytes(manifest_key(prefix, head.seq))
    if sha256_bytes(snapshot) != head.manifest_sha256:
        raise RestoreError(f"manifest snapshot {head.seq} does not match its head record")
    dest.mkdir(parents=True, exist_ok=True)
    (dest / MANIFEST_NAME).write_bytes(snapshot)
    for entry in parse_entries(snapshot.decode()):
        store.download(prefix + entry.path, dest / entry.path)
    return head
