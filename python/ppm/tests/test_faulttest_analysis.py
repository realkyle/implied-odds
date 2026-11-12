"""Failure-test analysis: the counting that decides PASS/FAIL, tested without Docker."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime

import pyarrow as pa
import pytest
from ppm.faulttest import analysis as a
from ppm.monitor.watch import iso
from ppm.pipeline.clock import TICK_MS
from ppm.tests.conftest import make_envelope
from ppm.writer.schemas import SCHEMAS
from ppm.writer.storage import DataStore

T0 = 1_794_493_440_000  # a minute tick (2026-11-12 14:24 UTC)
T = [T0 + i * TICK_MS for i in range(10)]


def row(venue: str, tick: int, key: str = "k1", status: str = "200", recv: int | None = None):
    error = None if status == "200" else "ConnectError: no route"
    return a.FetchRow(venue, key, tick, status, recv if recv is not None else tick + 400, error)


def ticks_ok(venue: str, ticks: list[int], keys=("k1", "k2")):
    logs = [a.TickLog(venue, t, published=len(keys)) for t in ticks]
    rows = [row(venue, t, k) for t in ticks for k in keys]
    return logs, rows


def check(logs, rows, venues, first, last, windows=(), deliveries="from_rows"):
    """check_integrity with, by default, a census saying each stored key went through
    Redis exactly once (the normal case)."""
    if deliveries == "from_rows":
        deliveries = Counter((r.venue, r.endpoint_key, r.snapshot_ts) for r in rows)
    return a.check_integrity(logs, rows, venues, first, last, windows, deliveries)


# --- log parsing ------------------------------------------------------------------------


def test_parses_tick_summaries_with_any_prefix():
    lines = [
        "2026-11-12 14:24:02,583Z INFO ppm.recorders.base: deribit tick 1794493440000: 6 published",
        "ppmtest-kalshi-1 | x INFO ppm.recorders.base: kalshi tick 1794493500000: "
        "2 published, 2 spooled",
        "x ERROR ppm.recorders.base: polymarket tick 1794493560000 failed",
        "x WARNING ppm.pipeline.bus: redis unavailable (...); spooled deribit/abc",  # ignored
        "x ERROR ppm.recorders.base: reference tick 1794493620000: 1 spooled, 3 dropped",
    ]
    assert a.parse_recorder_logs(lines) == [
        a.TickLog("deribit", 1794493440000, published=6),
        a.TickLog("kalshi", 1794493500000, published=2, spooled=2),
        a.TickLog("polymarket", 1794493560000, failed=True),
        a.TickLog("reference", 1794493620000, spooled=1, dropped=3),
    ]


def test_nothing_to_fetch_is_a_tick_with_zero_sent():
    [t] = a.parse_recorder_logs(["ppm.recorders.base: kalshi tick 60000: nothing to fetch"])
    assert t.sent == 0 and not t.failed


# --- integrity -------------------------------------------------------------------------


def test_clean_run_passes():
    logs, rows = ticks_ok("deribit", T[:5])
    result = check(logs, rows, ["deribit"], T[0], T[4])
    assert result.passed
    assert (result.sent, result.stored) == (10, 10)


def test_missing_row_is_lost():
    logs, rows = ticks_ok("deribit", T[:3])
    rows = [r for r in rows if not (r.snapshot_ts == T[1] and r.endpoint_key == "k2")]
    result = check(logs, rows, ["deribit"], T[0], T[2])
    assert result.lost == {("deribit", T[1]): 1}
    assert not result.passed


def test_spooled_envelopes_must_arrive_too():
    logs = [a.TickLog("kalshi", T[0], spooled=2)]
    assert not check(logs, [row("kalshi", T[0])], ["kalshi"], T[0], T[0]).passed
    rows = [row("kalshi", T[0], "k1"), row("kalshi", T[0], "k2")]
    assert check(logs, rows, ["kalshi"], T[0], T[0]).passed


def test_same_key_twice_is_duplicated():
    logs, rows = ticks_ok("deribit", T[:2])
    rows.append(row("deribit", T[1], "k1"))
    result = check(logs, rows, ["deribit"], T[0], T[1])
    assert result.duplicated == [("deribit", "k1", T[1])]
    assert not result.passed


def test_extra_rows_beyond_the_log_are_not_loss():
    # Killed after publishing but before logging: rows exist without a log line.
    logs = [a.TickLog("deribit", T[0], published=1)]
    rows = [row("deribit", T[0], "k1"), row("deribit", T[0], "k2")]
    assert check(logs, rows, ["deribit"], T[0], T[0]).passed


def test_dropped_and_failed_ticks_fail():
    dropped = [a.TickLog("deribit", T[0], dropped=1)]
    assert not check(dropped, [], ["deribit"], T[0], T[0]).passed
    failed = [a.TickLog("deribit", T[0], failed=True)]
    result = check(failed, [], ["deribit"], T[0], T[0])
    assert result.failed_ticks == [("deribit", T[0])] and not result.passed


def test_ticks_outside_the_range_are_ignored():
    logs, rows = ticks_ok("deribit", T[2:4])
    logs.append(a.TickLog("deribit", T[6], published=5))  # after the range, not stored
    assert check(logs, rows, ["deribit"], T[2], T[3]).passed


def test_gap_outside_any_window_is_unexpected():
    logs, rows = ticks_ok("deribit", [T[0], T[2]])
    result = check(logs, rows, ["deribit"], T[0], T[2])
    assert result.unexpected_gaps == [("deribit", T[1])]
    assert not result.passed


def test_gap_inside_a_kill_window_is_allowed_for_that_venue_only():
    logs, rows = ticks_ok("deribit", [T[0], T[2]])
    k_logs, k_rows = ticks_ok("kalshi", [T[0], T[2]])
    kill = a.RecorderKill("deribit", T[1] + 200, T[1] + 1_500)
    result = check(
        logs + k_logs, rows + k_rows, ["deribit", "kalshi"], T[0], T[2], a.windows_for([kill])
    )
    assert result.allowed_gaps == {"recorder-kill": [("deribit", T[1])]}
    assert result.unexpected_gaps == [("kalshi", T[1])]


def test_rows_without_a_log_line_are_unverified_in_a_kill_window_else_fail():
    # Killed after publishing 1 of 2 envelopes, before logging the tick.
    logs, rows = ticks_ok("deribit", [T[0], T[2]])
    rows.append(row("deribit", T[1], "k1"))
    kill = a.RecorderKill("deribit", T[1] + 200, T[1] + 1_500)
    inside = check(logs, rows, ["deribit"], T[0], T[2], a.windows_for([kill]))
    assert inside.unverified == [("deribit", T[1])] and inside.passed
    outside = check(logs, rows, ["deribit"], T[0], T[2])
    assert outside.unlogged == [("deribit", T[1])] and not outside.passed


def test_census_missing_fails():
    logs, rows = ticks_ok("deribit", T[:2])
    assert not check(logs, rows, ["deribit"], T[0], T[1], deliveries=None).passed


def test_census_extra_deliveries_are_absorbed_not_failed():
    logs, rows = ticks_ok("deribit", T[:2])
    deliveries = Counter((r.venue, r.endpoint_key, r.snapshot_ts) for r in rows)
    deliveries[("deribit", "k1", T[1])] += 2  # e.g. replayed from the spool after a crash
    result = check(logs, rows, ["deribit"], T[0], T[1], deliveries=deliveries)
    assert result.passed and result.extra_deliveries == 2 and result.delivered == 4


def test_census_catches_loss_the_logs_cannot_see():
    # A tick with no log line (killed before logging) whose delivered message never
    # got stored: the logs can't see it, the census can.
    logs, rows = ticks_ok("deribit", [T[0]])
    deliveries = Counter((r.venue, r.endpoint_key, r.snapshot_ts) for r in rows)
    deliveries[("deribit", "k1", T[1])] = 1
    kill = a.RecorderKill("deribit", T[1] + 200, T[1] + 1_500)
    result = check(logs, rows, ["deribit"], T[0], T[1], a.windows_for([kill]), deliveries)
    assert result.delivered_not_stored == [("deribit", "k1", T[1])] and not result.passed


def test_census_catches_rows_that_never_went_through_redis():
    logs, rows = ticks_ok("deribit", [T[0]])
    result = check(logs, rows, ["deribit"], T[0], T[0], deliveries=Counter())
    assert len(result.stored_not_delivered) == 2 and not result.passed


def test_census_ignores_keys_outside_the_range():
    logs, rows = ticks_ok("deribit", [T[1]])
    deliveries = Counter((r.venue, r.endpoint_key, r.snapshot_ts) for r in rows)
    deliveries[("deribit", "k1", T[0])] = 3  # warm-up tick, before the checked range
    assert check(logs, rows, ["deribit"], T[1], T[1], deliveries=deliveries).passed


def test_stream_census_counts_every_message_across_pages():
    import fakeredis
    from ppm.faulttest.stream_census import census

    redis = fakeredis.FakeRedis()
    a_env = make_envelope(snapshot_ts=T0)
    b_env = make_envelope(snapshot_ts=T[1])
    k_env = make_envelope(venue="kalshi", endpoint="markets", params={}, snapshot_ts=T0)
    for env in (a_env, b_env, a_env, b_env, a_env):  # a three times, b twice (replays)
        redis.xadd("ppm:deribit", env.to_fields())
    redis.xadd("ppm:kalshi", k_env.to_fields())
    redis.set("ppm:not-a-stream", b"x")  # other key types are skipped
    assert census(redis, page=2) == Counter(
        {
            ("deribit", a_env.key, T0): 3,
            ("deribit", b_env.key, T[1]): 2,
            ("kalshi", k_env.key, T0): 1,
        }
    )


def test_undecodable_stream_messages_are_counted_and_fail():
    import fakeredis
    from ppm.faulttest.stream_census import UNDECODABLE, census

    redis = fakeredis.FakeRedis()
    redis.xadd("ppm:deribit", {"garbage": b"1"})
    counts = census(redis)
    assert counts == Counter({(UNDECODABLE, "ppm:deribit", 0): 1})
    logs, rows = ticks_ok("deribit", [T[0]])
    deliveries = counts + Counter((r.venue, r.endpoint_key, r.snapshot_ts) for r in rows)
    result = check(logs, rows, ["deribit"], T[0], T[0], deliveries=deliveries)
    assert result.undecodable == 1 and not result.passed


def test_parse_census():
    out = "deribit\tbook_summary-abc\t1794493440000\t1\nkalshi\tmarkets-x\t1794493440000\t2\n"
    assert a.parse_census(out) == Counter(
        {("deribit", "book_summary-abc", T0): 1, ("kalshi", "markets-x", T0): 2}
    )


def test_net_cut_and_redis_windows_never_allow_gaps():
    events = [a.NetCut(("deribit",), T[1], T[5]), a.RedisOutage(T[1], T[5], 3, 10, None, None)]
    assert a.windows_for(events) == []


# --- per-scenario evaluation -------------------------------------------------------------


def test_writer_kill_measures_restart_and_drain():
    e = a.WriterKill(
        kill_ms=T[1] + 800,
        pending_at_kill=1,
        lag_at_kill=20,
        restarted_ms=T[1] + 1_700,
        drained_ms=T[1] + 5_000,
    )
    r = a.evaluate_writer_kill(e, a.Integrity())
    assert r.passed
    assert r.measured == {
        "restart_s": 0.9,
        "drain_s": 4.2,
        "redelivered": 1,
        "queued": 20,
        "attempts": 1,
    }


def test_writer_kill_fails_without_restart_or_with_loss():
    e = a.WriterKill(T[1], 0, 0, None, None)
    assert not a.evaluate_writer_kill(e, a.Integrity()).passed
    ok = a.WriterKill(T[1], 1, 0, T[1] + 900, T[1] + 2_000)
    only_queued = a.WriterKill(T[1], 0, 34, T[1] + 900, T[1] + 2_000)
    r = a.evaluate_writer_kill(only_queued, a.Integrity())
    assert not r.passed and "redelivery not exercised" in r.notes[0]
    lossy = a.Integrity(lost={("deribit", T[1]): 1})
    assert not a.evaluate_writer_kill(ok, lossy).passed


def test_recorder_kill_measures_gap_and_requires_resume():
    logs, rows = ticks_ok("deribit", [T[0], T[2], T[3]])
    kill = a.RecorderKill("deribit", T[1] + 200, T[1] + 1_400)
    r = a.evaluate_recorder_kills([kill], logs, rows)
    assert r.passed and r.measured == {"deribit": "down 1.2s/gap 1 tick"}
    # Never came back for the tick after its restart:
    logs, rows = ticks_ok("deribit", [T[0]])
    assert not a.evaluate_recorder_kills([kill], logs, rows).passed
    assert not a.evaluate_recorder_kills([a.RecorderKill("x", T[1], None)], [], []).passed


def test_redis_outage_needs_spooling_and_replay():
    logs = [a.TickLog("deribit", T[1], spooled=4)]
    e = a.RedisOutage(T[0] + 10_000, T[5] + 10_000, 4, 2_000_000, T[6] + 3_000, T[6] + 4_000)
    r = a.evaluate_redis_outage(e, logs)
    assert r.passed and r.measured["replay_s"] == 53.0 and r.measured["spooled"] == "4 env/2.00 MB"
    nothing_spooled = a.RedisOutage(T[0], T[5], 0, 0, T[6], T[6])
    assert not a.evaluate_redis_outage(nothing_spooled, []).passed
    never_empty = a.RedisOutage(T[0], T[5], 4, 10, None, T[6])
    assert not a.evaluate_redis_outage(never_empty, logs).passed


def test_net_cut_needs_error_rows_no_gaps_and_recovery():
    cut, restore = T[1] + 10_000, T[4] + 10_000  # ticks T2..T4 are inside the cut
    logs = [a.TickLog("deribit", t, published=1) for t in T[:7]]
    rows = [
        row("deribit", t, status="200" if t in (T[0], T[1], T[5], T[6]) else "error") for t in T[:7]
    ]
    r = a.evaluate_net_cut(a.NetCut(("deribit",), cut, restore), logs, rows)
    assert r.passed, r.notes
    assert r.measured["deribit"] == "err 3 tick/gap 0/resume 50.4s"

    silent = [x for x in rows if x.snapshot_ts != T[3]]
    silent_logs = [t for t in logs if t.tick != T[3]]
    r = a.evaluate_net_cut(a.NetCut(("deribit",), cut, restore), silent_logs, silent)
    assert not r.passed and any("no row and no cause" in n for n in r.notes)

    never_back = [x for x in rows if x.snapshot_ts <= T[4]]
    assert not a.evaluate_net_cut(a.NetCut(("deribit",), cut, restore), logs, never_back).passed


# --- venue monitor ---------------------------------------------------------------------


def mline(time_ms: int, text: str, level: str = "ERROR") -> str:
    """A monitor log line exactly as `ppm monitor` writes it (UTC asctime + 'Z')."""
    when = datetime.fromtimestamp(time_ms / 1000, UTC)
    stamp = when.strftime("%Y-%m-%d %H:%M:%S,") + f"{when.microsecond // 1000:03d}"
    return f"{stamp}Z {level} ppm.monitor: {text}"


def gap_line(venue: str, first: int, last: int, at: int) -> str:
    n = (last - first) // TICK_MS + 1
    return mline(
        at,
        f"GAP venue={venue} first={iso(first)} last={iso(last)} ticks={n} "
        f"detected_after_s={(at - first) / 1000:.1f}",
        "WARNING",
    )


def test_parse_monitor_logs_reads_time_label_and_fields():
    lines = [
        "noise",
        mline(
            T[2] + 1_250,
            "ALERT kind=unreachable venue=redis down_for_s=60.0 error='ConnectionError'",
        ),
        gap_line("kalshi", T[1], T[3], T[4] + 900),
        "2026-11-12 14:30:00,000Z INFO ppm.monitor: monitor seeded: deribit last=1",
    ]
    out = a.parse_monitor_logs(lines)
    assert [(m.time_ms, m.label, m.kind, m.venue) for m in out] == [
        (T[2] + 1_250, "ALERT", "unreachable", "redis"),
        (T[4] + 900, "GAP", None, "kalshi"),
    ]
    assert out[0].fields["error"] == "ConnectionError"
    assert out[1].gap_ticks() == [T[1], T[2], T[3]]


def monitor_check(monitor_lines, gaps, events=()):
    integrity = a.Integrity(allowed_gaps={"recorder-kill": gaps} if gaps else {})
    return a.evaluate_monitor(a.parse_monitor_logs(monitor_lines), integrity, events, T[0], T[9])


def test_monitor_gaps_must_equal_the_harness_gaps():
    gaps = [("deribit", T[3]), ("kalshi", T[5])]
    lines = [
        gap_line("deribit", T[3], T[3], T[4] + 800),
        gap_line("kalshi", T[5], T[5], T[6] + 900),
    ]
    r = monitor_check(lines, gaps)
    assert r.passed, r.notes
    assert r.measured["gap_ticks"] == 2 and r.measured["max_gap_detect_s"] == 60.9

    missed = monitor_check(lines[:1], gaps)
    assert not missed.passed and "missed" in missed.notes[0]
    invented = monitor_check([*lines, gap_line("reference", T[7], T[7], T[8])], gaps)
    assert not invented.passed and "didn't see" in invented.notes[0]


def test_gap_ticks_outside_the_checked_range_are_ignored():
    early = gap_line("deribit", T[0] - TICK_MS, T[0] - TICK_MS, T[0] + 500)
    assert monitor_check([early], []).passed


def test_an_alert_no_fault_explains_fails_the_monitor():
    stop = a.RecorderStop("kalshi", T[1] + 10_000, T[4] + 30_000)
    silent = mline(T[3] + 32_000, "ALERT kind=silent venue=kalshi last=x silent_for_s=152.0")
    assert monitor_check([silent], [], [stop]).passed
    wrong_venue = silent.replace("kalshi", "deribit")
    r = monitor_check([wrong_venue], [], [stop])
    assert not r.passed and "unexplained alert" in r.notes[0]
    assert not monitor_check([silent], []).passed  # no fault at all


def test_recorder_stop_needs_silent_alert_recovery_and_gap():
    stop, start = T[1] + 10_000, T[4] + 30_000
    e = a.RecorderStop("kalshi", stop, start)
    lines = [
        mline(T[1] + 152_000, "ALERT kind=silent venue=kalshi last=x silent_for_s=152.0"),
        mline(T[5] + 800, "RECOVERED kind=silent venue=kalshi down_for_s=240.8", "WARNING"),
        gap_line("kalshi", T[2], T[4], T[5] + 800),
    ]
    r = a.evaluate_recorder_stop(e, a.parse_monitor_logs(lines))
    assert r.passed, r.notes
    assert r.measured["alert_after_last_tick_s"] == 152.0
    assert r.measured["alert_after_stop_s"] == 142.0
    assert r.measured["gap_ticks"] == 3
    assert r.measured["gap_reported_after_restart_s"] == 30.8
    for drop in range(3):
        partial = a.parse_monitor_logs(lines[:drop] + lines[drop + 1 :])
        assert not a.evaluate_recorder_stop(e, partial).passed
    late = a.RecorderStop("kalshi", stop, T[1] + 100_000)  # back before the alert
    assert not a.evaluate_recorder_stop(late, a.parse_monitor_logs(lines)).passed


def test_net_cut_needs_a_failing_alert_and_recovery_per_venue():
    cut, restore = T[1] + 10_000, T[6] + 10_000
    logs = [a.TickLog("deribit", t, published=1) for t in T[:9]]
    ok = (T[0], T[1], T[7], T[8])
    rows = [row("deribit", t, status="200" if t in ok else "error") for t in T[:9]]
    e = a.NetCut(("deribit",), cut, restore)
    lines = [
        mline(T[5] + 2_000, "ALERT kind=failing venue=deribit first=x ticks=3"),
        mline(T[7] + 900, "RECOVERED kind=failing venue=deribit down_for_s=300.9", "WARNING"),
    ]
    r = a.evaluate_net_cut(e, logs, rows, a.parse_monitor_logs(lines))
    assert r.passed, r.notes
    assert r.measured["failing_alert_s"] == 232.0
    r = a.evaluate_net_cut(e, logs, rows, a.parse_monitor_logs(lines[:1]))
    assert not r.passed and "kind=failing" in r.notes[-1]
    assert a.evaluate_net_cut(e, logs, rows).passed  # monitor not given: not checked


def test_redis_outage_needs_the_unreachable_alert_and_recovery():
    logs = [a.TickLog("deribit", T[1], spooled=4)]
    e = a.RedisOutage(T[0] + 10_000, T[5] + 10_000, 4, 2_000_000, T[6] + 3_000, T[6] + 4_000)
    lines = [
        mline(T[1] + 12_000, "ALERT kind=unreachable venue=redis down_for_s=62.0 error='x'"),
        mline(T[5] + 15_000, "RECOVERED kind=unreachable venue=redis down_for_s=303.0", "WARNING"),
    ]
    r = a.evaluate_redis_outage(e, logs, a.parse_monitor_logs(lines))
    assert r.passed and r.measured["alert_s"] == 62.0
    assert not a.evaluate_redis_outage(e, logs, a.parse_monitor_logs(lines[1:])).passed


def test_recorder_stop_window_allows_gaps_for_that_venue():
    e = a.RecorderStop("kalshi", T[1] + 10_000, T[4] + 30_000)
    assert a.windows_for([e]) == [
        a.FaultWindow("recorder-stop", frozenset({"kalshi"}), e.stop_ms, e.start_ms, True)
    ]


# --- helpers ---------------------------------------------------------------------------


def test_only_the_test_project_is_allowed():
    a.assert_test_project("ppmtest")
    for name in ("quantproj1", "", "ppmtest2", "PPMTEST"):
        with pytest.raises(ValueError):
            a.assert_test_project(name)


def test_parse_docker_time_handles_nanoseconds():
    assert a.parse_docker_time("2026-11-12T14:24:02.583123456Z\n") == T0 + 2_583
    assert a.parse_docker_time("2026-11-12T14:24:00Z") == T0


def test_parse_queue_state_sums_streams_and_flags_unknown_lag():
    one = (
        "name\nwriter\nconsumers\n1\npending\n2\nlast-delivered-id\n1-0\nentries-read\n5\nlag\n3\n"
    )
    two = "name\nwriter\nconsumers\n1\npending\n0\nlag\n0\n"
    assert a.parse_queue_state(one + two) == (2, 3)
    assert a.parse_queue_state("pending\n0\nlag\n\n") == (0, -1)
    assert a.parse_queue_state("") == (0, 0)


def test_read_fetch_log_from_staging(tmp_path):
    store = DataStore(tmp_path)
    env = make_envelope(snapshot_ts=T0, recv_ts=T0 + 250)
    table = pa.Table.from_pylist(
        [
            {
                "snapshot_ts": T0,
                "venue": env.venue,
                "endpoint_key": env.key,
                "request_ts": T0 + 10,
                "recv_ts": T0 + 250,
                "latency_ms": 240,
                "status": "200",
                "error": None,
                "server_ts": None,
                "rows": 3,
                "rejected": 0,
            }
        ],
        schema=SCHEMAS["fetch_log"],
    )
    store.write_table("fetch_log", env, table)
    assert a.read_fetch_log(tmp_path) == [a.FetchRow(env.venue, env.key, T0, "200", T0 + 250, None)]


def test_recorder_stop_fails_on_an_early_alert_or_early_recovery():
    stop, start = T[1] + 10_000, T[4] + 30_000
    e = a.RecorderStop("kalshi", stop, start)
    early = [
        mline(T[1] + 100_000, "ALERT kind=silent venue=kalshi last=x silent_for_s=100.0"),
        mline(T[5] + 800, "RECOVERED kind=silent venue=kalshi down_for_s=240.8", "WARNING"),
        gap_line("kalshi", T[2], T[4], T[5] + 800),
    ]
    r = a.evaluate_recorder_stop(e, a.parse_monitor_logs(early))
    assert not r.passed and "before the threshold" in r.notes[0]
    back_early = [
        mline(T[1] + 152_000, "ALERT kind=silent venue=kalshi last=x silent_for_s=152.0"),
        mline(T[3] + 900, "RECOVERED kind=silent venue=kalshi down_for_s=120.9", "WARNING"),
        gap_line("kalshi", T[2], T[4], T[5] + 800),
    ]
    r = a.evaluate_recorder_stop(e, a.parse_monitor_logs(back_early))
    assert not r.passed and "before the recorder was restarted" in r.notes[0]


def test_short_outages_do_not_require_monitor_alerts():
    cut, restore = T[1] + 10_000, T[4] + 10_000  # only 3 error ticks, last not judged in time
    logs = [a.TickLog("deribit", t, published=1) for t in T[:7]]
    rows = [
        row("deribit", t, status="200" if t in (T[0], T[1], T[5], T[6]) else "error") for t in T[:7]
    ]
    assert a.evaluate_net_cut(a.NetCut(("deribit",), cut, restore), logs, rows, []).passed
    short = a.RedisOutage(T[0] + 10_000, T[0] + 50_000, 4, 10, T[1] + 3_000, T[1] + 4_000)
    assert a.evaluate_redis_outage(short, [a.TickLog("deribit", T[0], spooled=4)], []).passed
