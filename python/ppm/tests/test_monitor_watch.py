from __future__ import annotations

import pytest
from ppm.monitor.watch import Failing, Gap, Recovered, Silent, VenueWatch, iso
from ppm.pipeline.clock import TICK_MS

T0 = 1_794_500_000_000 - 1_794_500_000_000 % TICK_MS  # a minute tick
HOUR = 3_600_000
SILENT = int(2.5 * TICK_MS)


def watch(tick: int = TICK_MS, silent: int = SILENT, failing: int = 3) -> VenueWatch:
    return VenueWatch("deribit", tick, silent, failing, started_ms=T0)


def feed(w: VenueWatch, tick: int, ok: bool = True, delay_ms: int = 1_000) -> list:
    """One message for `tick` arriving `delay_ms` after it, followed by a check."""
    now = tick + delay_ms
    return w.saw(tick, ok, now) + w.check(now)


def test_contiguous_ticks_make_no_events():
    w = watch()
    events = [e for i in range(10) for e in feed(w, T0 + i * TICK_MS)]
    assert events == []


def test_one_missing_tick_is_one_gap_with_exact_bounds():
    w = watch()
    feed(w, T0)
    events = feed(w, T0 + 2 * TICK_MS, delay_ms=1_200)
    assert events == [Gap("deribit", T0 + TICK_MS, T0 + TICK_MS, 1, TICK_MS + 1_200)]
    assert events[0].format() == (
        f"GAP venue=deribit first={iso(T0 + TICK_MS)} last={iso(T0 + TICK_MS)} "
        "ticks=1 detected_after_s=61.2"
    )


def test_several_missing_ticks_are_one_gap():
    w = watch()
    feed(w, T0)
    gap = w.saw(T0 + 4 * TICK_MS, True, T0 + 4 * TICK_MS + 500)[0]
    assert (gap.first, gap.last, gap.ticks) == (T0 + TICK_MS, T0 + 3 * TICK_MS, 3)


def test_multi_message_ticks_duplicates_and_older_ticks_make_no_gap():
    w = watch()
    for _ in range(5):  # one tick, five endpoints
        assert w.saw(T0, True, T0 + 300) == []
    assert feed(w, T0 + TICK_MS) == []
    assert w.saw(T0, True, T0 + TICK_MS + 2_000) == []  # spool replay of an old tick
    assert w.saw(T0 + TICK_MS, True, T0 + TICK_MS + 2_000) == []  # settlements restart
    assert feed(w, T0 + 2 * TICK_MS) == []


def test_silent_fires_once_at_the_threshold_not_before():
    w = watch()
    feed(w, T0)
    assert w.check(T0 + SILENT - 1) == []
    assert w.check(T0 + SILENT) == [Silent("deribit", T0, SILENT)]
    assert w.check(T0 + SILENT + 10_000) == []  # edge-triggered: no repeat
    assert w.check(T0 + 10 * SILENT) == []


def test_recovered_once_with_down_for_then_the_gap():
    w = watch()
    feed(w, T0)
    w.check(T0 + SILENT)
    back = T0 + 4 * TICK_MS
    events = w.saw(back, True, back + 800)
    assert events == [
        Recovered("deribit", "silent", 4 * TICK_MS + 800),
        Gap("deribit", T0 + TICK_MS, T0 + 3 * TICK_MS, 3, 3 * TICK_MS + 800),
    ]
    assert w.saw(back, True, back + 900) == []  # only once
    assert w.check(back + 1_000) == []


def test_nothing_ever_seen_is_silent_from_the_monitor_start():
    w = watch()
    events = w.check(T0 + SILENT)
    assert events == [Silent("deribit", None, SILENT)]
    assert events[0].format() == "ALERT kind=silent venue=deribit last=none silent_for_s=150.0"


def test_hourly_venue_uses_its_own_tick():
    w = VenueWatch("settlements", HOUR, int(2.5 * HOUR), 3, started_ms=T0)
    for _ in range(50):  # paginated: many messages per tick
        w.saw(T0, True, T0 + 20_000)
    assert w.check(T0 + 2 * HOUR) == []
    assert w.saw(T0 + HOUR, True, T0 + HOUR + 20_000) == []
    gap = w.saw(T0 + 3 * HOUR, True, T0 + 3 * HOUR + 5_000)
    assert gap == [Gap("settlements", T0 + 2 * HOUR, T0 + 2 * HOUR, 1, HOUR + 5_000)]
    assert w.check(T0 + 3 * HOUR + int(2.5 * HOUR)) == [
        Silent("settlements", T0 + 3 * HOUR, int(2.5 * HOUR))
    ]


def test_failing_after_exactly_three_all_error_ticks():
    w = watch()
    feed(w, T0)
    assert feed(w, T0 + TICK_MS, ok=False) == []
    assert feed(w, T0 + 2 * TICK_MS, ok=False) == []
    assert feed(w, T0 + 3 * TICK_MS, ok=False) == []  # third tick not closed yet
    # Closed one tick length after it started, even before the next tick arrives.
    assert w.check(T0 + 4 * TICK_MS - 1) == []
    assert w.check(T0 + 4 * TICK_MS) == [Failing("deribit", T0 + TICK_MS, 3)]
    assert feed(w, T0 + 4 * TICK_MS, ok=False) == []  # no repeat
    assert w.check(T0 + 6 * TICK_MS) == []


def test_failing_recovers_on_the_first_ok_message():
    w = watch()
    feed(w, T0)
    for i in (1, 2, 3, 4):
        feed(w, T0 + i * TICK_MS, ok=False)
    assert w.saw(T0 + 5 * TICK_MS, False, T0 + 5 * TICK_MS + 100) == []
    events = w.saw(T0 + 5 * TICK_MS, True, T0 + 5 * TICK_MS + 200)
    assert events == [Recovered("deribit", "failing", 4 * TICK_MS + 200)]
    assert w.check(T0 + 6 * TICK_MS) == []


def test_one_ok_message_in_a_tick_resets_the_streak():
    w = watch()
    events = feed(w, T0)
    events += feed(w, T0 + TICK_MS, ok=False)
    events += feed(w, T0 + 2 * TICK_MS, ok=False)
    # Third tick: two endpoints fail, one succeeds -> not an all-error tick.
    events += w.saw(T0 + 3 * TICK_MS, False, T0 + 3 * TICK_MS + 100)
    events += w.saw(T0 + 3 * TICK_MS, True, T0 + 3 * TICK_MS + 110)
    events += w.saw(T0 + 3 * TICK_MS, False, T0 + 3 * TICK_MS + 120)
    events += w.check(T0 + 4 * TICK_MS)
    events += feed(w, T0 + 4 * TICK_MS, ok=False)
    events += feed(w, T0 + 5 * TICK_MS, ok=False)
    events += w.check(T0 + 6 * TICK_MS)
    assert events == []  # streak is 2 after the reset, not 4


def test_an_internet_cut_is_failing_not_silent():
    w = watch()
    feed(w, T0)
    events = []
    for i in range(1, 6):  # errors arrive ~41 s late (timeout + one retry)
        events += feed(w, T0 + i * TICK_MS, ok=False, delay_ms=41_000)
    events += feed(w, T0 + 6 * TICK_MS, delay_ms=1_000)
    assert [type(e).__name__ for e in events] == ["Failing", "Recovered"]


def test_seed_finds_a_gap_across_a_restart_and_is_silent_if_stale():
    w = watch()
    w.seed(T0)
    assert w.check(T0 + SILENT) == [Silent("deribit", T0, SILENT)]
    events = w.saw(T0 + 3 * TICK_MS, True, T0 + 3 * TICK_MS + 500)
    assert [type(e) for e in events] == [Recovered, Gap]
    assert events[1].ticks == 2


def test_deferred_silence_waits_for_the_spool_replay():
    w = watch()
    feed(w, T0)
    w.defer_silence(T0 + 5 * TICK_MS)
    assert w.check(T0 + 4 * TICK_MS) == []
    # Replayed ticks arrive (old snapshot_ts), contiguous: no gap, no alert.
    events = [e for i in range(1, 5) for e in w.saw(T0 + i * TICK_MS, True, T0 + 5 * TICK_MS)]
    assert events == []
    assert w.check(T0 + 5 * TICK_MS) == []


@pytest.mark.parametrize("args", [(0, 1, 1), (1, 0, 1), (1, 1, 0)])
def test_rejects_nonsense_thresholds(args):
    with pytest.raises(ValueError):
        VenueWatch("x", *args, started_ms=0)


def test_error_ticks_separated_by_a_gap_are_not_in_a_row():
    w = watch()
    feed(w, T0)
    events = feed(w, T0 + TICK_MS, ok=False)
    events += w.check(T0 + 2 * TICK_MS)  # closed: streak 1
    # Recorder dead for two ticks, then two more error ticks.
    events += feed(w, T0 + 4 * TICK_MS, ok=False)
    events += feed(w, T0 + 5 * TICK_MS, ok=False)
    events += w.check(T0 + 6 * TICK_MS)
    assert [type(e).__name__ for e in events] == ["Gap"]  # streak restarted: 2, not 3
