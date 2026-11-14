from __future__ import annotations

import pytest
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


def test_stream_retention_from_env(monkeypatch):
    monkeypatch.delenv("PPM_STREAM_RETAIN_H", raising=False)
    assert Settings.from_env().stream_retain_ms == 12 * 3_600_000  # default 12 h
    monkeypatch.setenv("PPM_STREAM_RETAIN_H", "1.5")
    assert Settings.from_env().stream_retain_ms == 5_400_000


@pytest.mark.parametrize("bad", ["0", "-3", "nan", "twelve"])
def test_stream_retention_rejects_non_positive_or_garbage(monkeypatch, bad):
    monkeypatch.setenv("PPM_STREAM_RETAIN_H", bad)
    with pytest.raises(ValueError):
        Settings.from_env()
