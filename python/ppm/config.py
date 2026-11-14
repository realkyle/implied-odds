"""Runtime settings, read from environment variables with safe local defaults.

No secrets live here. Stage 1 uses only public venue APIs. The S3 backup's access key is
not a setting: boto3 reads it from AWS_SHARED_CREDENTIALS_FILE (a Compose secret on the VM).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    redis_url: str
    data_dir: Path
    spool_dir: Path
    spool_max_bytes: int
    stream_retain_ms: int
    # Venue monitor: silent after this many ticks with no message; failing after this
    # many ticks in a row with only error messages. Optional ntfy.sh topic for pushes.
    monitor_silent_ticks: float = 2.5
    monitor_failing_ticks: int = 3
    ntfy_topic: str | None = None
    monitor_venues: tuple[str, ...] | None = None  # None: every recorder
    # Off-machine backup (S3). The bucket name comes from `terraform output backup_bucket`.
    backup_bucket: str | None = None
    backup_prefix: str = "vm/"
    backup_region: str = "us-east-1"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            data_dir=Path(os.environ.get("PPM_DATA_DIR", "data")),
            spool_dir=Path(os.environ.get("PPM_SPOOL_DIR", "spool")),
            # 2 GiB of disk spool rides out roughly a day of Redis downtime for all venues.
            spool_max_bytes=int(os.environ.get("PPM_SPOOL_MAX_BYTES", str(2 * 1024**3))),
            # Stream retention window = how long the writer may be down before messages
            # are trimmed unread (lost). 12 h of every venue is ~205 MB of Redis memory
            # (measured 2026-11-14), which fits the 2 GB VM; a count cap did not.
            stream_retain_ms=_hours_to_ms(os.environ.get("PPM_STREAM_RETAIN_H", "12")),
            monitor_silent_ticks=float(os.environ.get("PPM_MONITOR_SILENT_TICKS", "2.5")),
            monitor_failing_ticks=int(os.environ.get("PPM_MONITOR_FAILING_TICKS", "3")),
            ntfy_topic=os.environ.get("PPM_NTFY_TOPIC") or None,
            monitor_venues=_csv(os.environ.get("PPM_MONITOR_VENUES")),
            backup_bucket=os.environ.get("PPM_BACKUP_BUCKET") or None,
            backup_prefix=os.environ.get("PPM_BACKUP_PREFIX", "vm/"),
            backup_region=os.environ.get("PPM_BACKUP_REGION", "us-east-1"),
        )


def _hours_to_ms(value: str) -> int:
    hours = float(value)
    if not hours > 0:  # also rejects NaN
        raise ValueError(f"PPM_STREAM_RETAIN_H must be a positive number of hours, got {value!r}")
    return int(hours * 3_600_000)


def _csv(value: str | None) -> tuple[str, ...] | None:
    items = tuple(v.strip() for v in (value or "").split(",") if v.strip())
    return items or None
