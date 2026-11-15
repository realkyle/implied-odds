"""`ppm backup --status`: is the off-machine backup fresh, and is the bucket untouched?

Runs on the laptop as the owner's SSO session, never with the VM's key. Two things only
someone outside the VM can see:

- **Freshness.** The VM writes a new `<prefix>heads/NNNNNN.json` every night (~02:37Z,
  after the merge). If the newest head is older than `max_age_h` (26 h: one night plus
  margin), the backup has stopped, and a dead VM can't alert about itself. The head JSON
  has no timestamp on purpose (`job.Head`), so the age comes from S3's `LastModified`.
- **Write-once held.** Every key under the prefix must have exactly 1 version and no
  delete marker. A 2nd version means something overwrote an object; a delete marker
  means something deleted one. The bucket policy should make both impossible for the
  VM, so either one means the policy or the bucket changed.
- **Versioning still on.** The version count only proves anything while bucket
  versioning is `Enabled`. If it were suspended, an overwrite would replace the single
  version in place and the count would still be 1, so anything else is a problem.

Seeing versions needs `s3:ListBucketVersions`, which the VM's key deliberately lacks.
That's why this lives behind its own `VersionLister` instead of `ObjectStore`: the type
says which credentials a piece of code needs.

Limit: a *permanent* delete of one version (by version id, owner/root only) removes it
from the listing too, so this can't see that. It catches overwrites and plain deletes.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Protocol

HOUR_MS = 3_600_000
MAX_HEAD_AGE_H = 26.0


@dataclass(frozen=True, slots=True)
class ObjectVersion:
    """One entry of a versioned listing: a real object version or a delete marker."""

    key: str
    version_id: str
    last_modified_ms: int
    is_latest: bool
    is_delete_marker: bool = False


class VersionLister(Protocol):
    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        """Every version and delete marker under `prefix`. Needs `s3:ListBucketVersions`
        (owner credentials only)."""
        ...

    def versioning_status(self) -> str:
        """The bucket's versioning status: "Enabled", "Suspended", or "" if it was
        never turned on."""
        ...


class S3VersionLister:
    """`VersionLister` on one S3 bucket. The boto3 client is passed in, so the caller
    picks the credentials (`AWS_PROFILE=ppm`) and tests can stub it."""

    def __init__(self, bucket: str, client: Any) -> None:
        self.bucket = bucket
        self._s3 = client

    def list_versions(self, prefix: str) -> list[ObjectVersion]:
        out: list[ObjectVersion] = []
        for page in self._s3.get_paginator("list_object_versions").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            out += (_version(v, marker=False) for v in page.get("Versions", []))
            out += (_version(v, marker=True) for v in page.get("DeleteMarkers", []))
        return out

    def versioning_status(self) -> str:
        # S3 omits `Status` entirely for a bucket that never had versioning.
        return self._s3.get_bucket_versioning(Bucket=self.bucket).get("Status", "")


def _version(item: dict, marker: bool) -> ObjectVersion:
    return ObjectVersion(
        key=item["Key"],
        version_id=item["VersionId"],
        last_modified_ms=int(item["LastModified"].timestamp() * 1000),
        is_latest=bool(item["IsLatest"]),
        is_delete_marker=marker,
    )


@dataclass(frozen=True, slots=True)
class AuditReport:
    prefix: str
    max_age_h: float
    versioning: str  # "Enabled" is the only healthy value
    head_key: str | None  # newest live head, None if there is none
    head_age_ms: int | None
    keys: int
    versions: int  # real versions, not counting delete markers
    delete_markers: int
    problems: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.problems

    def lines(self) -> list[str]:
        """Human-readable summary: versioning, the head, the version check."""
        on = self.versioning == "Enabled"
        versioning = (
            f"versioning: {self.versioning or 'never enabled'}: {'OK' if on else 'PROBLEM'}"
        )
        if self.head_key is None or self.head_age_ms is None:
            head = f"no head under {self.prefix}heads/: PROBLEM"
        else:
            fresh = self.head_age_ms <= self.max_age_h * HOUR_MS
            head = (
                f"newest head {self.head_key} age {self.head_age_ms / HOUR_MS:.1f} h "
                f"(max {self.max_age_h:g} h): {'OK' if fresh else 'PROBLEM'}"
            )
        clean = self.versions == self.keys and self.delete_markers == 0
        counts = (
            f"{self.keys} keys, {self.versions} versions, {self.delete_markers} delete "
            f"markers: {'OK' if clean else 'PROBLEM'}"
        )
        return [versioning, head, counts]


def audit(
    versions: Iterable[ObjectVersion],
    now_ms: int,
    prefix: str = "vm/",
    max_age_h: float = MAX_HEAD_AGE_H,
    *,
    versioning: str,
) -> AuditReport:
    """Judge a versioned listing. Pure: the clock, the listing and the bucket's
    versioning status are inputs. `versioning` has no default on purpose, so no caller
    can skip that check by accident."""
    by_key: dict[str, list[ObjectVersion]] = defaultdict(list)
    for v in versions:
        by_key[v.key].append(v)

    problems: list[str] = []
    if versioning != "Enabled":
        problems.append(
            f"bucket versioning is {versioning or 'never enabled'!r}, not 'Enabled': "
            "an overwrite would leave no trace, so the version check proves nothing"
        )
    n_versions = n_markers = 0
    for key in sorted(by_key):
        real = sum(not v.is_delete_marker for v in by_key[key])
        markers = len(by_key[key]) - real
        n_versions += real
        n_markers += markers
        if real != 1 or markers:
            problems.append(f"{key}: {real} versions, {markers} delete markers (expected 1, 0)")

    # Newest head by key name (zero-padded seq, so lexical order is numeric order),
    # among heads whose latest entry is a real object. A deleted head can't vouch for
    # freshness; the delete marker is already a problem above.
    live_heads = sorted(
        (v for v in _live_latest(by_key) if v.key.startswith(f"{prefix}heads/")),
        key=lambda v: v.key,
    )
    head_key: str | None = None
    head_age_ms: int | None = None
    if not live_heads:
        problems.append(f"no head under {prefix}heads/: the backup never ran or was deleted")
    else:
        newest = live_heads[-1]
        head_key, head_age_ms = newest.key, now_ms - newest.last_modified_ms
        if head_age_ms > max_age_h * HOUR_MS:
            problems.append(
                f"newest head {head_key} is {head_age_ms / HOUR_MS:.1f} h old "
                f"(max {max_age_h:g} h): the backup has stopped"
            )

    return AuditReport(
        prefix=prefix,
        max_age_h=max_age_h,
        versioning=versioning,
        head_key=head_key,
        head_age_ms=head_age_ms,
        keys=len(by_key),
        versions=n_versions,
        delete_markers=n_markers,
        problems=tuple(problems),
    )


def _live_latest(by_key: dict[str, list[ObjectVersion]]) -> Iterable[ObjectVersion]:
    """The latest entry of each key, if it is a real object (not a delete marker)."""
    for entries in by_key.values():
        for v in entries:
            if v.is_latest and not v.is_delete_marker:
                yield v
