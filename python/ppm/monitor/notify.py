"""Where monitor events go. Behind one small interface, so the channel can change (e.g.
Prometheus Alertmanager later) without touching the detection logic.

- `LogNotifier`: always on. GAP → WARNING, ALERT → ERROR, RECOVERED → WARNING.
- `NtfyNotifier`: optional phone push via ntfy.sh, only for alerts and recoveries (a gap
  is a log line, not a reason to wake someone). The topic name works like a password:
  anyone who knows it can read the pushes, so it lives in `PPM_NTFY_TOPIC`, never in git.
- `FanoutNotifier`: sends each event to several notifiers.

A notifier never raises: a failed push is logged, and monitoring carries on.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Protocol

import httpx

from ppm.monitor.watch import Event, Gap, Recovered

log = logging.getLogger("ppm.monitor")

NTFY_URL = "https://ntfy.sh"
NTFY_TIMEOUT_S = 5.0


class Notifier(Protocol):
    async def notify(self, event: Event) -> None: ...


class LogNotifier:
    def __init__(self, logger: logging.Logger = log) -> None:
        self._log = logger

    async def notify(self, event: Event) -> None:
        level = logging.ERROR if event.label == "ALERT" else logging.WARNING
        self._log.log(level, event.format())


class NtfyNotifier:
    """POST alerts and recoveries to `https://ntfy.sh/{topic}`. ntfy takes the message as
    the request body and optional `Title` / `Priority` / `Tags` headers."""

    def __init__(self, topic: str, client: httpx.AsyncClient, base_url: str = NTFY_URL) -> None:
        if not topic:
            raise ValueError("ntfy topic must not be empty")
        self._url = f"{base_url.rstrip('/')}/{topic}"
        self._client = client

    async def notify(self, event: Event) -> None:
        if isinstance(event, Gap):
            return
        await self.send(event)

    async def send(self, event: Event) -> bool:
        """Push one event. True if ntfy accepted it; failures are logged, never raised."""
        recovered = isinstance(event, Recovered)
        headers = {
            "Title": f"ppm {event.label.lower()}",
            "Priority": "default" if recovered else "high",
            "Tags": "white_check_mark" if recovered else "warning",
        }
        try:
            response = await self._client.post(
                self._url, content=event.format().encode(), headers=headers, timeout=NTFY_TIMEOUT_S
            )
        except Exception as exc:  # network, bad topic/URL, encoding: never crash the monitor
            log.warning("ntfy push failed (%s): %s", type(exc).__name__, event.format())
            return False
        if response.status_code >= 400:
            log.warning("ntfy push failed (HTTP %d): %s", response.status_code, event.format())
            return False
        return True


class FanoutNotifier:
    def __init__(self, notifiers: Iterable[Notifier]) -> None:
        self._notifiers = list(notifiers)

    async def notify(self, event: Event) -> None:
        for n in self._notifiers:
            await n.notify(event)
