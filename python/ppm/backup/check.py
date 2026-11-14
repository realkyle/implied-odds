"""`ppm backup --check-permissions`: prove, with the VM's own key, that it can add objects
but never replace or delete them.

Run on the VM. Each probe is a real request whose expected answer is fixed. A refusal
only counts if the key demonstrably works (probe 1 succeeds); otherwise every 403 could
just mean "bad credentials". The probes touch only `<prefix>probe/<timestamp>…` keys, never
real data, so a broken policy can at worst overwrite a probe object. Probe objects stay
behind (the key can't delete them); each run adds one ~20-byte object.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import ClientError


@dataclass(frozen=True, slots=True)
class Probe:
    name: str
    expected: int  # HTTP status: 200 allowed, 412 precondition failed, 403 denied
    actual: int | str

    @property
    def ok(self) -> bool:
        return self.actual == self.expected


def _status(call: Callable[[], Any]) -> int | str:
    try:
        resp = call()
    except ClientError as exc:
        code = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code if code is not None else exc.response.get("Error", {}).get("Code", "?")
    return resp.get("ResponseMetadata", {}).get("HTTPStatusCode", 200)


def check_permissions(client: Any, bucket: str, prefix: str = "vm/") -> list[Probe]:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    key = f"{prefix}probe/{stamp}.txt"
    body = f"probe {stamp}\n".encode()
    put = client.put_object
    probes = [
        Probe(
            "create a new key with If-None-Match (allowed)",
            200,
            _status(lambda: put(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*")),
        ),
    ]
    if not probes[0].ok:  # the key itself doesn't work: refusals below would prove nothing
        return probes
    etag = client.head_object(Bucket=bucket, Key=key)["ETag"]
    probes += [
        Probe(
            "re-create the same key with If-None-Match (S3: already exists)",
            412,
            _status(lambda: put(Bucket=bucket, Key=key, Body=b"x", IfNoneMatch="*")),
        ),
        Probe(
            "PUT without If-None-Match (policy: denied)",
            403,
            _status(lambda: put(Bucket=bucket, Key=f"{prefix}probe/{stamp}-plain.txt", Body=b"x")),
        ),
        Probe(
            "overwrite with If-Match <etag> (policy: denied)",
            403,
            _status(lambda: put(Bucket=bucket, Key=key, Body=b"x", IfMatch=etag)),
        ),
        Probe(
            "DELETE the key (policy: denied)",
            403,
            _status(lambda: client.delete_object(Bucket=bucket, Key=key)),
        ),
        Probe(
            "create a key outside the prefix (policy: denied)",
            403,
            _status(
                lambda: put(
                    Bucket=bucket, Key=f"laptop/probe/{stamp}.txt", Body=b"x", IfNoneMatch="*"
                )
            ),
        ),
    ]
    return probes
