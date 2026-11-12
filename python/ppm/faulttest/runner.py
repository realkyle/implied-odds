"""Drives the failure test against an isolated Compose stack (`ppm faulttest`).

All numbers are judged by `analysis.py`; this module only starts the stack, injects
faults at chosen moments, timestamps what it did and collects the evidence. It shells out
to the `docker` CLI rather than using a Docker SDK: one fewer dependency, and every
action is a command the owner could type by hand to reproduce a step.

Safety: the Compose project is always `ppmtest` (checked by `assert_test_project`), with
its own Redis volume and a fresh `.faulttest/run-<ts>/` for data and spool. The live
recording (laptop processes or the default Compose project) is never touched.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from ppm.faulttest import analysis as a
from ppm.pipeline.clock import TICK_MS, floor_tick, next_tick, now_ms

RECORDERS = ("deribit", "kalshi", "polymarket", "reference")
# settlements is left out: it refetches ~53 Kalshi pages on every start, and a second copy
# could get the *live* stack rate-limited. It shares its kill/restart code (Recorder base).
SERVICES = ("redis", "writer", "monitor", *RECORDERS)
SCENARIOS = ("writer-kill", "recorder-kill", "recorder-stop", "redis-outage", "net-cut")
# recorder-stop: long enough to pass the monitor's silence threshold (2.5 ticks = 150 s
# after the last tick) with margin, short enough to keep the run quick.
STOP_VENUE = "kalshi"
STOP_S = 200.0
ALL_STREAMS = ("deribit", "kalshi", "polymarket", "reference", "settlements")


def _say(msg: str) -> None:
    print(f"[{datetime.now(UTC):%H:%M:%S}] {msg}", flush=True)


class Stack:
    """The `ppmtest` Compose stack plus the docker commands the scenarios need."""

    def __init__(self, repo: Path, run_dir: Path, project: str = a.TEST_PROJECT) -> None:
        a.assert_test_project(project)
        self.project = project
        self.run_dir = run_dir
        self.store = run_dir / "store"
        self.queue = run_dir / "queue"
        self._env = {
            **os.environ,
            "PPM_HOST_DATA": str(self.store),
            "PPM_HOST_SPOOL": str(self.queue),
        }
        self._compose = [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            str(repo / "docker-compose.yml"),
            "-f",
            str(repo / "docker-compose.faulttest.yml"),
        ]

    def compose(self, *args: str, check: bool = True) -> str:
        done = subprocess.run(
            [*self._compose, *args], env=self._env, capture_output=True, text=True, check=False
        )
        if check and done.returncode != 0:
            raise RuntimeError(f"docker compose {' '.join(args)} failed:\n{done.stderr}")
        return done.stdout

    def up(self) -> None:
        self.store.mkdir(parents=True, exist_ok=True)
        self.queue.mkdir(parents=True, exist_ok=True)
        self.compose("up", "-d", "--build", "--wait", *SERVICES)

    def down(self) -> None:
        a.assert_test_project(self.project)  # `-v` deletes this project's Redis volume
        self.compose("down", "-v", "--remove-orphans", check=False)

    def container(self, service: str) -> str:
        return self.compose("ps", "-q", "--all", service).strip()

    def started_at(self, service: str) -> int:
        out = _docker("inspect", "-f", "{{.State.StartedAt}}", self.container(service))
        return a.parse_docker_time(out)

    def signal(self, service: str, sig: str = "KILL") -> None:
        """Send `sig` to the service's Python process from inside the container. KILL is
        a real crash: `docker kill` would count as a *manual* stop, which restart
        policies ignore. As the `ppm` user, `kill -1` reaches every process except PID 1
        (the init) and the shell itself; after a KILL the init exits and Docker restarts
        the container."""
        self.compose("exec", "-T", service, "sh", "-c", f"kill -{sig} -1", check=False)

    def queue_state(self) -> tuple[int, int] | None:
        streams = " ".join(f"ppm:{v}" for v in ALL_STREAMS)
        script = f"for s in {streams}; do redis-cli XINFO GROUPS $s; done"
        try:
            return a.parse_queue_state(self.compose("exec", "-T", "redis", "sh", "-c", script))
        except (RuntimeError, ValueError):
            return None  # Redis down or restarting

    def spool(self) -> tuple[int, int]:
        files = list(self.queue.glob("*/*.env"))
        return len(files), sum(f.stat().st_size for f in files)

    def network(self, action: str, service: str) -> None:
        _docker("network", action, f"{self.project}_egress", self.container(service))

    def census(self) -> str | None:
        """Every stream message counted by key, read inside the writer container."""
        try:
            return self.compose(
                "exec", "-T", "writer", "python", "-m", "ppm.faulttest.stream_census"
            )
        except RuntimeError as exc:
            (self.run_dir / "census.err").write_text(str(exc))
            _say(f"stream census failed (saved to census.err): {str(exc)[-800:]}")
            return None

    def logs(self, *services: str) -> list[str]:
        return self.compose("logs", "--no-color", "--no-log-prefix", *services).splitlines()


def _docker(*args: str) -> str:
    done = subprocess.run(["docker", *args], capture_output=True, text=True, check=False)
    if done.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed:\n{done.stderr}")
    return done.stdout


def _sleep_to(target_ms: int) -> None:
    time.sleep(max(0.0, (target_ms - now_ms()) / 1000))


def _at_tick(offset_s: float) -> int:
    """Sleep until `offset_s` after the next minute tick; return that tick."""
    tick = next_tick(now_ms())
    _sleep_to(tick + int(offset_s * 1000))
    return tick


def _poll(check: Callable[[], bool], timeout_s: float, every_s: float = 0.25) -> int | None:
    """Time (epoch ms) when `check` first held, or None on timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if check():
            return now_ms()
        time.sleep(every_s)
    return None


def _drained(stack: Stack) -> bool:
    return stack.queue_state() == (0, 0)


# --- scenarios: each injects one fault and returns what happened, with timestamps -----


# Freeze offsets after the tick, tried in turn. The writer is fast (on 2026-11-12 it had
# often drained a tick by +1.0 s), so a single fixed offset can miss every time.
FREEZE_OFFSETS_S = (0.4, 0.6, 0.8, 1.0, 1.2, 1.5)


def writer_kill(stack: Stack) -> a.WriterKill:
    """Kill the writer while it holds delivered-but-unacked messages, deterministically.

    Racing a timer doesn't work: the writer stores a tick's ~34 messages 0.3–2 s after
    the tick and Docker restarts it in ~0.3 s, so a kill at a fixed offset often lands
    between batches, and a sample taken after the kill sees the *restarted* writer
    (2026-11-12, second run: 0 pending, 0 queued). Instead, freeze it with SIGSTOP just
    after a tick, while the batch arrives. A frozen writer can't ack, so the queue sample
    is exact. If it holds nothing pending (it was between reads), resume it (SIGCONT) and
    retry on the next tick at the next offset. Then SIGKILL: its pending messages must be
    redelivered to the new process. If every attempt misses, the scenario reports FAIL.
    """
    before = stack.started_at("writer")
    pending = lag = -1
    attempts = 0
    offset = 0.0
    for offset in FREEZE_OFFSETS_S:
        attempts += 1
        _at_tick(offset)
        stack.signal("writer", "STOP")
        state = stack.queue_state()
        pending, lag = state if state else (-1, -1)
        if pending > 0:
            break
        stack.signal("writer", "CONT")
        _say(f"  froze at +{offset} s: nothing pending (lag {lag}); retrying next tick")
    kill = now_ms()
    stack.signal("writer", "KILL")
    restarted = _poll(lambda: _safe_started(stack, "writer") != before, 30)
    restarted_at = stack.started_at("writer") if restarted else None
    drained = _poll(lambda: _drained(stack), 120)
    _say(f"  killed on attempt {attempts}, frozen at tick +{offset} s")
    return a.WriterKill(kill, pending, lag, restarted_at, drained, attempts)


def recorder_kills(stack: Stack) -> list[a.RecorderKill]:
    kills = []
    for venue in RECORDERS:
        before = stack.started_at(venue)
        _at_tick(0.2)  # fetches are in flight: the tick dies with the process
        kill = now_ms()
        stack.signal(venue, "KILL")
        restarted = _poll(lambda v=venue, b=before: _safe_started(stack, v) != b, 30)
        kills.append(a.RecorderKill(venue, kill, stack.started_at(venue) if restarted else None))
        _say(f"  killed {venue}; restarted={'yes' if restarted else 'NO'}")
    # Let each recorder log its first tick after the restart.
    _sleep_to(next_tick(now_ms()) + 30_000)
    return kills


def recorder_stop(stack: Stack, venue: str = STOP_VENUE, down_s: float = STOP_S) -> a.RecorderStop:
    """Stop one recorder cleanly (as `docker compose stop` would on a hung or crashed-
    and-not-restarted process) so the monitor sees real silence, then start it again."""
    _at_tick(10)  # its tick is logged; it is stopped between ticks
    stop = now_ms()
    stack.compose("stop", venue)
    _sleep_to(stop + int(down_s * 1000))
    start = now_ms()
    stack.compose("start", venue)
    # The monitor reports RECOVERED and the GAP when the first tick after the restart lands.
    _sleep_to(next_tick(now_ms()) + 30_000)
    return a.RecorderStop(venue, stop, start)


def redis_outage(stack: Stack, outage_s: float) -> a.RedisOutage:
    _at_tick(10)  # between ticks, so the outage starts cleanly
    stop = now_ms()
    stack.compose("stop", "redis")
    _sleep_to(stop + int(outage_s * 1000))
    files, size = stack.spool()
    start = now_ms()
    stack.compose("start", "redis")
    # Recorders replay their spool on their next publish, i.e. at the next tick.
    empty = _poll(lambda: stack.spool()[0] == 0, 180, every_s=0.5)
    drained = _poll(lambda: _drained(stack), 120)
    return a.RedisOutage(stop, start, files, size, empty, drained)


def net_cut(stack: Stack, outage_s: float) -> a.NetCut:
    _at_tick(10)
    cut = now_ms()
    for venue in RECORDERS:
        stack.network("disconnect", venue)
    _sleep_to(cut + int(outage_s * 1000))
    restore = now_ms()
    for venue in RECORDERS:
        stack.network("connect", venue)
    _sleep_to(next_tick(now_ms()) + TICK_MS + 30_000)  # two ticks to show recovery
    return a.NetCut(RECORDERS, cut, restore)


def _safe_started(stack: Stack, service: str) -> int | None:
    try:
        return stack.started_at(service)
    except (RuntimeError, ValueError):
        return None  # mid-restart


# --- the whole run --------------------------------------------------------------------


def run(repo: Path, scenarios: list[str], outage_s: float) -> int:
    """Run the chosen scenarios on a fresh `ppmtest` stack; print and save a report.
    Returns a process exit code: 0 if everything passed."""
    run_dir = repo / ".faulttest" / f"run-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    stack = Stack(repo, run_dir)
    events: list[object] = []
    try:
        _say(f"starting {stack.project} stack in {run_dir} (builds the image if needed)")
        stack.up()
        _say("warm-up: waiting for two full ticks")
        _sleep_to(next_tick(now_ms()) + TICK_MS + 30_000)
        start_ms = next_tick(now_ms()) - TICK_MS  # first tick checked: fully warmed up

        if "writer-kill" in scenarios:
            _say("writer-kill: freeze the writer mid-batch, then SIGKILL it")
            events.append(writer_kill(stack))
        if "recorder-kill" in scenarios:
            _say("recorder-kill: SIGKILL each recorder mid-fetch, one per tick")
            events.extend(recorder_kills(stack))
        if "recorder-stop" in scenarios:
            _say(f"recorder-stop: stop {STOP_VENUE} for {STOP_S:.0f} s (monitor must alert)")
            events.append(recorder_stop(stack))
        if "redis-outage" in scenarios:
            _say(f"redis-outage: stop Redis for {outage_s:.0f} s")
            events.append(redis_outage(stack, outage_s))
        if "net-cut" in scenarios:
            _say(f"net-cut: disconnect recorders from the internet for {outage_s:.0f} s")
            events.append(net_cut(stack, outage_s))

        _say("final tick and drain")
        _sleep_to(next_tick(now_ms()) + 30_000)
        last_tick = floor_tick(now_ms())
        if _poll(lambda: _drained(stack), 120) is None:
            _say("WARNING: queue not drained at the end; unstored rows will show as lost")
        census_text = stack.census()
        if census_text is not None:
            (run_dir / "census.tsv").write_text(census_text)
        logs_text = stack.logs(*RECORDERS)
        (run_dir / "recorders.log").write_text("\n".join(logs_text))
        (run_dir / "writer.log").write_text("\n".join(stack.logs("writer")))
        monitor_text = stack.logs("monitor")
        (run_dir / "monitor.log").write_text("\n".join(monitor_text))
    finally:
        _say("tearing down the ppmtest stack (data in the run directory is kept)")
        stack.down()

    logs = a.parse_recorder_logs(logs_text)
    rows = a.read_fetch_log(stack.store)
    integrity = a.check_integrity(
        logs,
        rows,
        RECORDERS,
        start_ms,
        last_tick,
        a.windows_for(events),
        a.parse_census(census_text) if census_text is not None else None,
    )
    monitor = a.parse_monitor_logs(monitor_text)
    results = _evaluate(events, logs, rows, integrity, monitor)
    results.append(a.evaluate_monitor(monitor, integrity, events, start_ms, last_tick))
    table = a.format_table(results, integrity)
    print(table)
    report = {
        "run_dir": str(run_dir),
        "first_tick": start_ms,
        "last_tick": last_tick,
        "events": [{"type": type(e).__name__, **asdict(e)} for e in events],  # type: ignore[call-overload]
        "results": [asdict(r) for r in results],
        "integrity": {
            "passed": integrity.passed,
            "sent": integrity.sent,
            "stored": integrity.stored,
            "lost": [[v, t, n] for (v, t), n in integrity.lost.items()],
            "duplicated": integrity.duplicated,
            "dropped": integrity.dropped,
            "failed_ticks": integrity.failed_ticks,
            "allowed_gaps": integrity.allowed_gaps,
            "unexpected_gaps": integrity.unexpected_gaps,
            "unverified": integrity.unverified,
            "unlogged": integrity.unlogged,
            "delivered": integrity.delivered,
            "extra_deliveries": integrity.extra_deliveries,
            "delivered_not_stored": integrity.delivered_not_stored,
            "stored_not_delivered": integrity.stored_not_delivered,
            "undecodable": integrity.undecodable,
        },
        "table": table,
    }
    (run_dir / "report.json").write_text(json.dumps(report, indent=2))
    _say(f"report: {run_dir / 'report.json'}")
    return 0 if integrity.passed and all(r.passed for r in results) else 1


def _evaluate(
    events: list[object],
    logs: list[a.TickLog],
    rows: list[a.FetchRow],
    integrity: a.Integrity,
    monitor: list[a.MonitorLine],
) -> list[a.ScenarioResult]:
    results: list[a.ScenarioResult] = []
    kills = [e for e in events if isinstance(e, a.RecorderKill)]
    for e in events:
        if isinstance(e, a.WriterKill):
            results.append(a.evaluate_writer_kill(e, integrity))
        elif isinstance(e, a.RecorderStop):
            results.append(a.evaluate_recorder_stop(e, monitor))
        elif isinstance(e, a.RedisOutage):
            results.append(a.evaluate_redis_outage(e, logs, monitor))
        elif isinstance(e, a.NetCut):
            results.append(a.evaluate_net_cut(e, logs, rows, monitor))
    if kills:
        results.append(a.evaluate_recorder_kills(kills, logs, rows))
    order = {name: i for i, name in enumerate(SCENARIOS)}
    return sorted(results, key=lambda r: order[r.name])


def main(scenarios: list[str] | None, outage_s: float) -> int:
    repo = Path.cwd()
    if not (repo / "docker-compose.faulttest.yml").exists():
        print("run `ppm faulttest` from the repository root", file=sys.stderr)
        return 2
    return run(repo, scenarios or list(SCENARIOS), outage_s)
