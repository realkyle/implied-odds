from __future__ import annotations

from ppm.config import Settings


def test_monitor_settings_from_env(monkeypatch):
    monkeypatch.setenv("PPM_MONITOR_VENUES", " deribit, kalshi ,")
    monkeypatch.setenv("PPM_NTFY_TOPIC", "")
    s = Settings.from_env()
    assert s.monitor_venues == ("deribit", "kalshi")
    assert s.ntfy_topic is None  # empty means off
    assert (s.monitor_silent_ticks, s.monitor_failing_ticks) == (2.5, 3)
    monkeypatch.delenv("PPM_MONITOR_VENUES")
    assert Settings.from_env().monitor_venues is None
