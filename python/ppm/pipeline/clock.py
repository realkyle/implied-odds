"""Tick scheduling aligned to the wall clock.

Every recorder process computes ticks from the clock itself (not from when it started),
so separate processes agree on snapshot_ts without talking to each other.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

TICK_MS = 60_000

Clock = Callable[[], int]


def now_ms() -> int:
    """Current UTC time in integer milliseconds since the epoch."""
    return time.time_ns() // 1_000_000


def floor_tick(ts_ms: int, period_ms: int = TICK_MS) -> int:
    return ts_ms - ts_ms % period_ms


def next_tick(ts_ms: int, period_ms: int = TICK_MS) -> int:
    """The first tick strictly after ts_ms."""
    return floor_tick(ts_ms, period_ms) + period_ms


async def sleep_until(target_ms: int, stop: asyncio.Event, clock: Clock = now_ms) -> bool:
    """Sleep until target_ms. Returns False if `stop` was set first."""
    delay_s = max(0.0, (target_ms - clock()) / 1000)
    try:
        await asyncio.wait_for(stop.wait(), timeout=delay_s)
        return False
    except TimeoutError:
        return True
