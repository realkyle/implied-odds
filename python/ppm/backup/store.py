"""Write-once object storage for the off-machine backup.

The backup only ever *adds* objects. Every upload is conditional, `If-None-Match: *`
("create this key only if it doesn't exist yet"), and the bucket policy denies any upload
by the VM's key that doesn't carry that header (`infra/terraform/s3.tf`). So the VM can
never replace something it uploaded earlier, even with a bug: S3 answers 412 Precondition
Failed and `put_*` returns False.

Every object carries the SHA-256 of its contents as user metadata (`x-amz-meta-sha256`),
so "is the object already there the one I meant to upload?" is one HEAD request.

- `ObjectStore`: the interface the backup job and restore use (dependency injection).
- `S3Store`: boto3 implementation. boto3's managed `upload_file` silently drops
  `IfNoneMatch` (checked 2026-11-14 with s3transfer 0.19.3), so large files go up as our
  own sequential multipart upload, with the condition on `CompleteMultipartUpload`.
- `MemoryStore`: in-memory implementation for tests, with the same write-once rule.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import ClientError

# Files above this go up in parts. Below it, one PUT streams the file from disk.
MULTIPART_THRESHOLD = 64 * 1024 * 1024
PART_SIZE = 16 * 1024 * 1024  # one part in memory at a time: bounded RAM on a 2 GB VM


class ObjectStore(Protocol):
    def list_keys(self, prefix: str) -> set[str]:
        """Every key under `prefix`."""
        ...

    def sha256_of(self, key: str) -> str | None:
        """The SHA-256 recorded when `key` was uploaded, or None if there is no such key."""
        ...

    def put_bytes_if_absent(self, key: str, data: bytes, sha256: str) -> bool:
        """Create `key`. False (and nothing changed) if it already exists."""
        ...

    def put_file_if_absent(self, key: str, path: Path, sha256: str) -> bool:
        """Create `key` from a local file. False (and nothing changed) if it already exists."""
        ...

    def get_bytes(self, key: str) -> bytes: ...

    def download(self, key: str, dest: Path) -> None: ...


class S3Store:
    """`ObjectStore` on one S3 bucket. The boto3 client is passed in (credentials, region
    and retries are the caller's choice), which also lets tests substitute it."""

    def __init__(self, bucket: str, client: Any) -> None:
        self.bucket = bucket
        self._s3 = client

    def list_keys(self, prefix: str) -> set[str]:
        keys: set[str] = set()
        for page in self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        ):
            keys.update(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def sha256_of(self, key: str) -> str | None:
        try:
            head = self._s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _status(exc) == 404:
                return None
            raise
        return head.get("Metadata", {}).get("sha256", "")

    def put_bytes_if_absent(self, key: str, data: bytes, sha256: str) -> bool:
        return self._conditional(
            lambda: self._s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                IfNoneMatch="*",
                Metadata={"sha256": sha256},
                ChecksumAlgorithm="SHA256",  # S3 rejects the upload if bytes change in transit
            )
        )

    def put_file_if_absent(self, key: str, path: Path, sha256: str) -> bool:
        if path.stat().st_size <= MULTIPART_THRESHOLD:
            with open(path, "rb") as body:
                return self._conditional(
                    lambda: self._s3.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=body,
                        IfNoneMatch="*",
                        Metadata={"sha256": sha256},
                        ChecksumAlgorithm="SHA256",
                    )
                )
        return self._multipart(key, path, sha256)

    def _multipart(self, key: str, path: Path, sha256: str) -> bool:
        # Parts are invisible until completed. Only the final CompleteMultipartUpload can
        # carry If-None-Match, so that's where write-once is decided; the bucket policy
        # exempts the part uploads (s3:ObjectCreationOperation) for the same reason.
        upload_id = self._s3.create_multipart_upload(
            Bucket=self.bucket, Key=key, Metadata={"sha256": sha256}, ChecksumAlgorithm="SHA256"
        )["UploadId"]
        try:
            parts = []
            with open(path, "rb") as fh:
                number = 1
                while chunk := fh.read(PART_SIZE):
                    resp = self._s3.upload_part(
                        Bucket=self.bucket,
                        Key=key,
                        UploadId=upload_id,
                        PartNumber=number,
                        Body=chunk,
                        ChecksumAlgorithm="SHA256",
                    )
                    parts.append(
                        {
                            "PartNumber": number,
                            "ETag": resp["ETag"],
                            "ChecksumSHA256": resp["ChecksumSHA256"],
                        }
                    )
                    number += 1
            created = self._conditional(
                lambda: self._s3.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                    IfNoneMatch="*",
                )
            )
        except BaseException:
            self._abort(key, upload_id)
            raise
        if not created:
            self._abort(key, upload_id)
        return created

    def _abort(self, key: str, upload_id: str) -> None:
        # Best effort: the bucket's lifecycle rule removes abandoned parts after 7 days.
        try:
            self._s3.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
        except Exception:  # noqa: BLE001 - never mask the original error
            pass

    def _conditional(self, call) -> bool:
        try:
            call()
        except ClientError as exc:
            if _status(exc) == 412:  # PreconditionFailed: the key already exists
                return False
            raise
        return True

    def get_bytes(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def download(self, key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        self._s3.download_file(self.bucket, key, str(dest))


def _status(exc: ClientError) -> int | None:
    return exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")


class MemoryStore:
    """In-memory `ObjectStore` for tests. Same write-once rule as the real bucket, and it
    records the order of successful uploads so tests can assert it."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}  # key -> (data, recorded sha256)
        self.uploads: list[str] = []
        self.fail_after: int | None = None  # simulate a crash after N successful uploads

    def _put(self, key: str, data: bytes, sha256: str) -> bool:
        if key in self.objects:
            return False
        if self.fail_after is not None and len(self.uploads) >= self.fail_after:
            raise ConnectionError("simulated network failure")
        self.objects[key] = (data, sha256)
        self.uploads.append(key)
        return True

    def list_keys(self, prefix: str) -> set[str]:
        return {k for k in self.objects if k.startswith(prefix)}

    def sha256_of(self, key: str) -> str | None:
        return self.objects[key][1] if key in self.objects else None

    def put_bytes_if_absent(self, key: str, data: bytes, sha256: str) -> bool:
        return self._put(key, data, sha256)

    def put_file_if_absent(self, key: str, path: Path, sha256: str) -> bool:
        return self._put(key, path.read_bytes(), sha256)

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key][0]

    def download(self, key: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.objects[key][0])


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
