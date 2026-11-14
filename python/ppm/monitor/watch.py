"""Per-venue health state machine. Pure: no Redis, no clock, no I/O.

`VenueWatch` is fed two things and answers with events:

- `saw(snapshot_ts, ok, now_ms)`: one envelope header arrived from the venue's stream.
- `check(now_ms)`: time passed (called after every read, even an empty one).

It detects three conditions:

- **Gap**: a tick that never arrived. Ticks are aligned to the wall clock, so if tick T
  is followed by T + 3·tick, ticks T + tick and T + 2·tick are missing. Older or repeated
  ticks never make a gap: a tick has several messages, a spool replay can resend one,
  and settlements re-collects the current hour when it restarts.
- **Silent**: nothing at all for `silent_after_ms` after the last tick. Fires once
  (edge-triggered), then `Recovered(kind="silent")` on the next message.
- **Failing**: `failing_after_ticks` ticks in a row where every message was an error.
  A failed fetch still publishes an error envelope (so an internet cut never looks
  silent); this is the condition that catches it. One ok message clears it.

A tick is judged "all errors" only once it is closed: when a newer tick arrives, or one
tick length after it started (its messages are all published within seconds of each
other, well before the next tick's).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar


def iso(ts_ms: int | None) -> str:
    """Compact UTC time for log lines: `2026-11-12T17:27:00Z`."""
    if ts_ms is None:
        return "none"
    return datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _secs(ms: int) -> str:
    return f"{ms / 1000:.1f}"


# Events. Each is a frozen dataclass with a `format()` that gives one key=value log line,
# so `ppm faulttest` (and grep) can parse them back. `label` is the first word.


@dataclass(frozen=True, slots=True)
class Gap:
    """Ticks `first`..`last` (inclusive) never arrived."""

    label: ClassVar[str] = "GAP"
    venue: str
    first: int
    last: int
    ticks: int
    detected_after_ms: int  # from the first missing tick to when the gap was noticed

    def format(self) -> str:
        return (
            f"GAP venue={self.venue} first={iso(self.first)} last={iso(self.last)} "
            f"ticks={self.ticks} detected_after_s={_secs(self.detected_after_ms)}"
        )


@dataclass(frozen=True, slots=True)
class Silent:
    """No message for at least the silence threshold."""

    label: ClassVar[str] = "ALERT"
    venue: str
    last_tick: int | None  # None: nothing seen since the monitor started
    silent_for_ms: int

    def format(self) -> str:
        return (
            f"ALERT kind=silent venue={self.venue} last={iso(self.last_tick)} "
            f"silent_for_s={_secs(self.silent_for_ms)}"
        )


@dataclass(frozen=True, slots=True)
class Failing:
    """`ticks` closed ticks in a row, from `first`, had only error envelopes."""

    label: ClassVar[str] = "ALERT"
    venue: str
    first: int
    ticks: int

    def format(self) -> str:
        return f"ALERT kind=failing venue={self.venue} first={iso(self.first)} ticks={self.ticks}"


@dataclass(frozen=True, slots=True)
class RedisUnreachable:
    """The monitor itself can't read Redis, so it can't judge any venue."""

    label: ClassVar[str] = "ALERT"
    down_for_ms: int
    error: str

    def format(self) -> str:
        return (
            f"ALERT kind=unreachable venue=redis down_for_s={_secs(self.down_for_ms)} "
            f"error={self.error!r}"
        )


@dataclass(frozen=True, slots=True)
class ProbeAlert:
    """Sent by `ppm monitor --test-notify` to prove notifications get delivered."""

    label: ClassVar[str] = "ALERT"

    def format(self) -> str:
        return "ALERT kind=test venue=monitor (test notification from ppm monitor --test-notify)"


@dataclass(frozen=True, slots=True)
class Recovered:
    """An earlier alert of `kind` (silent / failing / unreachable) has cleared."""

    label: ClassVar[str] = "RECOVERED"
    venue: str
    kind: str
    down_for_ms: int

    def format(self) -> str:
        return f"RECOVERED kind={self.kind} venue={self.venue} down_for_s={_secs(self.down_for_ms)}"


@dataclass(frozen=True, slots=True)
class BackupFailed:
    """The S3 backup refused or failed a pass. `reason`: "fork" (the local chain no longer
    extends the head already in S3), "integrity" (a local file or the manifest doesn't
    match its hashes) or "error" (S3/network/credentials; retried next pass)."""

    label: ClassVar[str] = "ALERT"
    reason: str
    detail: str

    def format(self) -> str:
        return f"ALERT kind=backup venue=s3 reason={self.reason} detail={self.detail!r}"


Event = Gap | Silent | Failing | RedisUnreachable | ProbeAlert | BackupFailed | Recovered


class VenueWatch:
    """Health of one venue's stream. See the module docstring for the rules."""

    def __init__(
        self,
        venue: str,
        tick_ms: int,
        silent_after_ms: int,
        failing_after_ticks: int,
        started_ms: int,
    ) -> None:
        if tick_ms <= 0 or silent_after_ms <= 0 or failing_after_ticks < 1:
            raise ValueError("tick, silence threshold and failing ticks must be positive")
        self.venue = venue
        self.tick_ms = tick_ms
        self.silent_after_ms = silent_after_ms
        self.failing_after_ticks = failing_after_ticks
        self._started_ms = started_ms  # reference for silence until the first tick
        self._last_tick: int | None = None
        self._silent = False
        self._silence_deferred_until = 0
        # The newest tick, while its messages may still be arriving.
        self._open_tick: int | None = None
        self._open_ok = False
        self._error_streak = 0
        self._streak_start = 0
        self._failing = False

    @property
    def last_tick(self) -> int | None:
        return self._last_tick

    def seed(self, snapshot_ts: int) -> None:
        """Start from the newest tick already on the stream (at monitor start-up), so a
        gap that spans a monitor restart is still found. Its ok/error state is unknown,
        so it doesn't count towards `failing`."""
        self._last_tick = snapshot_ts

    def defer_silence(self, until_ms: int) -> None:
        """Don't judge silence before `until_ms`. Used after Redis comes back: recorders
        replay their disk spool only at their next tick, so the stream looks stale for up
        to a tick even though nothing was lost (the Redis alert already covers it)."""
        self._silence_deferred_until = max(self._silence_deferred_until, until_ms)

    def saw(self, snapshot_ts: int, ok: bool, now_ms: int) -> list[Event]:
        events: list[Event] = []
        if self._silent:
            self._silent = False
            events.append(Recovered(self.venue, "silent", now_ms - self._reference()))

        last = self._last_tick
        if last is None or snapshot_ts > last:
            if last is not None and snapshot_ts > last + self.tick_ms:
                # Error ticks on either side of a hole aren't "in a row": restart the count.
                self._error_streak = 0
                first = last + self.tick_ms
                events.append(
                    Gap(
                        self.venue,
                        first=first,
                        last=snapshot_ts - self.tick_ms,
                        ticks=(snapshot_ts - last) // self.tick_ms - 1,
                        detected_after_ms=now_ms - first,
                    )
                )
            events += self._close_open_tick()
            self._last_tick = snapshot_ts
            self._open_tick, self._open_ok = snapshot_ts, False

        if ok and snapshot_ts == self._open_tick:
            self._open_ok = True
            self._error_streak = 0
            if self._failing:
                self._failing = False
                events.append(Recovered(self.venue, "failing", now_ms - self._streak_start))
        return events

    def check(self, now_ms: int) -> list[Event]:
        events: list[Event] = []
        if self._open_tick is not None and now_ms >= self._open_tick + self.tick_ms:
            events += self._close_open_tick()
        silent_for = now_ms - self._reference()
        if (
            not self._silent
            and now_ms >= self._silence_deferred_until
            and silent_for >= self.silent_after_ms
        ):
            self._silent = True
            events.append(Silent(self.venue, self._last_tick, silent_for))
        return events

    def _reference(self) -> int:
        return self._last_tick if self._last_tick is not None else self._started_ms

    def _close_open_tick(self) -> list[Event]:
        if self._open_tick is None:
            return []
        tick, ok = self._open_tick, self._open_ok
        self._open_tick = None
        if ok:
            return []
        self._error_streak += 1
        if self._error_streak == 1:
            self._streak_start = tick
        if self._error_streak >= self.failing_after_ticks and not self._failing:
            self._failing = True
            return [Failing(self.venue, first=self._streak_start, ticks=self._error_streak)]
        return []
