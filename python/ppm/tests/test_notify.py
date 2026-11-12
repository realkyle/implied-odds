from __future__ import annotations

import logging

import httpx
import pytest
from ppm.monitor.notify import FanoutNotifier, LogNotifier, NtfyNotifier
from ppm.monitor.watch import Failing, Gap, ProbeAlert, Recovered, Silent

pytestmark = pytest.mark.anyio

T0 = 1_794_507_420_000  # 2026-11-12T18:17:00Z
SILENT = Silent("kalshi", T0, 150_000)


def ntfy(handler) -> tuple[NtfyNotifier, httpx.AsyncClient]:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return NtfyNotifier("ppm-test-topic", client, base_url="https://ntfy.example"), client


async def test_log_notifier_levels_and_lines(caplog):
    caplog.set_level(logging.INFO, logger="ppm.monitor")
    n = LogNotifier()
    for e in (
        Gap("deribit", T0, T0, 1, 61_200),
        SILENT,
        Failing("polymarket", T0, 3),
        Recovered("kalshi", "silent", 240_800),
    ):
        await n.notify(e)
    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        (
            "WARNING",
            "GAP venue=deribit first=2026-11-12T18:17:00Z last=2026-11-12T18:17:00Z "
            "ticks=1 detected_after_s=61.2",
        ),
        ("ERROR", "ALERT kind=silent venue=kalshi last=2026-11-12T18:17:00Z silent_for_s=150.0"),
        ("ERROR", "ALERT kind=failing venue=polymarket first=2026-11-12T18:17:00Z ticks=3"),
        ("WARNING", "RECOVERED kind=silent venue=kalshi down_for_s=240.8"),
    ]


async def test_ntfy_posts_alerts_and_recoveries_but_not_gaps():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    n, client = ntfy(handler)
    async with client:
        await n.notify(Gap("deribit", T0, T0, 1, 61_000))
        await n.notify(SILENT)
        await n.notify(Recovered("kalshi", "silent", 1_000))
    assert [str(r.url) for r in seen] == ["https://ntfy.example/ppm-test-topic"] * 2
    assert seen[0].content == SILENT.format().encode()
    assert seen[0].headers["Priority"] == "high"
    assert seen[1].headers["Priority"] == "default"


@pytest.mark.parametrize(
    "handler",
    [
        lambda r: httpx.Response(500, text="boom"),
        lambda r: (_ for _ in ()).throw(httpx.ConnectTimeout("timed out", request=r)),
    ],
    ids=["http-500", "timeout"],
)
async def test_ntfy_failure_is_logged_not_raised(handler, caplog):
    caplog.set_level(logging.WARNING, logger="ppm.monitor")
    n, client = ntfy(handler)
    async with client:
        assert await n.send(ProbeAlert()) is False
        await n.notify(SILENT)  # must not raise
    assert all("ntfy push failed" in r.getMessage() for r in caplog.records)
    assert len(caplog.records) == 2


def test_ntfy_rejects_an_empty_topic():
    with pytest.raises(ValueError):
        NtfyNotifier("", httpx.AsyncClient())


async def test_fanout_sends_to_every_notifier():
    got: list[list] = [[], []]

    class Into:
        def __init__(self, out: list) -> None:
            self.out = out

        async def notify(self, event) -> None:
            self.out.append(event)

    await FanoutNotifier([Into(got[0]), Into(got[1])]).notify(SILENT)
    assert got == [[SILENT], [SILENT]]
