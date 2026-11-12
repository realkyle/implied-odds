"""Runtime settings, read from environment variables with safe local defaults.

No secrets live here. Stage 1 uses only public venue APIs.
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
    stream_maxlen: int
    # Venue monitor: silent after this many ticks with no message; failing after this
    # many ticks in a row with only error messages. Optional ntfy.sh topic for pushes.
    monitor_silent_ticks: float = 2.5
    monitor_failing_ticks: int = 3
    ntfy_topic: str | None = None
    monitor_venues: tuple[str, ...] | None = None  # None: every recorder

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            data_dir=Path(os.environ.get("PPM_DATA_DIR", "data")),
            spool_dir=Path(os.environ.get("PPM_SPOOL_DIR", "spool")),
            # 2 GiB of disk spool rides out roughly a day of Redis downtime for all venues.
            spool_max_bytes=int(os.environ.get("PPM_SPOOL_MAX_BYTES", str(2 * 1024**3))),
            # Per-stream cap. Redis trims the oldest entries past this even if they are
            # unacknowledged, so it is a last-resort memory guard, not a buffer to rely on.
            # ~20k Deribit messages is about 2 days of writer downtime.
            stream_maxlen=int(os.environ.get("PPM_STREAM_MAXLEN", "20000")),
            monitor_silent_ticks=float(os.environ.get("PPM_MONITOR_SILENT_TICKS", "2.5")),
            monitor_failing_ticks=int(os.environ.get("PPM_MONITOR_FAILING_TICKS", "3")),
            ntfy_topic=os.environ.get("PPM_NTFY_TOPIC") or None,
            monitor_venues=_csv(os.environ.get("PPM_MONITOR_VENUES")),
        )


def _csv(value: str | None) -> tuple[str, ...] | None:
    items = tuple(v.strip() for v in (value or "").split(",") if v.strip())
    return items or None
