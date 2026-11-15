"""`ppm backup --status`: head freshness and the 1-version-per-key rule.

`audit()` is pure, so most tests hand it a listing and a clock. `S3VersionLister` runs
against botocore's `Stubber` (real client, no network) to prove the S3 response is
mapped right, including delete markers and a second page.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import boto3
import pytest
from botocore.stub import Stubber
from ppm import cli
from ppm.backup.audit import HOUR_MS, ObjectVersion, S3VersionLister, audit
from ppm.config import Settings
from ppm.monitor.watch import BackupAudit

P = "vm/"
NOW = 1_794_740_000_000  # 2026-11-15 ~10:53Z


def v(key: str, age_h: float = 1.0, vid: str = "v1", latest=True, marker=False):
    return ObjectVersion(key, vid, NOW - int(age_h * HOUR_MS), latest, marker)


def healthy(head_age_h: float = 6.0) -> list[ObjectVersion]:
    """What the bucket looks like after two nights: 2 data files, 2 snapshots, 2 heads,
    2 probe objects (kept on purpose, see WorkLog 2026-11-15)."""
    return [
        v("vm/tables/a/date=2026-11-13/part-1.parquet", 30),
        v("vm/tables/a/date=2026-11-14/part-1.parquet", head_age_h),
        v("vm/manifest/000005.jsonl", 30),
        v("vm/manifest/000012.jsonl", head_age_h),
        v("vm/heads/000005.json", 30),
        v("vm/heads/000012.json", head_age_h),
        v("vm/probe/20261114T030800Z.txt", 31),
        v("vm/probe/20261114T045600Z.txt", 29),
    ]


def test_fresh_head_and_one_version_per_key_is_ok():
    r = audit(healthy(), NOW, P, versioning="Enabled")
    assert r.ok and r.problems == ()
    assert r.head_key == "vm/heads/000012.json"  # newest by seq, not the older head
    assert r.head_age_ms == 6 * HOUR_MS
    assert (r.keys, r.versions, r.delete_markers) == (8, 8, 0)
    assert r.lines()[0] == "versioning: Enabled: OK"
    assert r.lines()[2] == "8 keys, 8 versions, 0 delete markers: OK"


def test_probe_objects_are_ordinary_keys():
    r = audit(healthy(), NOW, P, versioning="Enabled")
    assert r.ok  # 2 probe objects, 1 version each, raise nothing


def test_head_exactly_at_the_limit_is_ok_one_second_more_is_stale():
    at_limit = audit(healthy(head_age_h=26.0), NOW, P, versioning="Enabled")
    assert at_limit.ok
    stale = audit(healthy(head_age_h=26.0 + 1 / 3600), NOW, P, versioning="Enabled")
    assert not stale.ok
    assert "vm/heads/000012.json is 26.0 h old" in stale.problems[0]
    assert stale.lines()[1].endswith("PROBLEM")


def test_head_age_uses_the_newest_seq_not_the_newest_timestamp():
    # The older head re-uploaded later would be a 2nd version; here only the timing
    # differs, to pin that "newest" means highest seq.
    listing = [v("vm/heads/000005.json", 1), v("vm/heads/000012.json", 40)]
    r = audit(listing, NOW, P, versioning="Enabled")
    assert r.head_key == "vm/heads/000012.json" and not r.ok


def test_no_head_at_all_is_a_problem():
    r = audit([v("vm/tables/a/date=2026-11-14/part-1.parquet")], NOW, P, versioning="Enabled")
    assert not r.ok
    assert r.head_key is None
    assert "no head under vm/heads/" in r.problems[0]
    assert r.lines()[1] == "no head under vm/heads/: PROBLEM"


def test_empty_bucket_is_a_problem():
    assert not audit([], NOW, P, versioning="Enabled").ok


def test_a_second_version_is_flagged_by_key():
    key = "vm/tables/a/date=2026-11-14/part-1.parquet"
    listing = healthy() + [v(key, 0.5, vid="v2")]
    listing[1] = replace(listing[1], is_latest=False)  # the original is now noncurrent
    r = audit(listing, NOW, P, versioning="Enabled")
    assert r.problems == (f"{key}: 2 versions, 0 delete markers (expected 1, 0)",)
    assert (r.keys, r.versions) == (8, 9)


def test_a_delete_marker_is_flagged():
    key = "vm/manifest/000005.jsonl"
    listing = [replace(x, is_latest=False) if x.key == key else x for x in healthy()]
    listing.append(v(key, 0.1, vid="dm", marker=True))
    r = audit(listing, NOW, P, versioning="Enabled")
    assert r.problems == (f"{key}: 1 versions, 1 delete markers (expected 1, 0)",)
    assert r.delete_markers == 1


def test_deleted_newest_head_falls_back_to_the_previous_head():
    # A deleted head can't vouch for freshness: the age comes from the older live head.
    listing = [
        replace(x, is_latest=False) if x.key == "vm/heads/000012.json" else x for x in healthy()
    ]
    listing.append(v("vm/heads/000012.json", 0.1, vid="dm", marker=True))
    r = audit(listing, NOW, P, versioning="Enabled")
    assert r.head_key == "vm/heads/000005.json"
    assert len(r.problems) == 2  # the delete marker and the 30 h old head


def test_failure_stale_head_and_two_version_key_are_both_flagged():
    """PLAN 2f failure test: both problems in one listing, both reported."""
    key = "vm/tables/a/date=2026-11-13/part-1.parquet"
    listing = healthy(head_age_h=40) + [v(key, 0.2, vid="v2")]
    r = audit(listing, NOW, P, versioning="Enabled")
    assert not r.ok
    assert len(r.problems) == 2
    assert any(key in p and "2 versions" in p for p in r.problems)
    assert any("vm/heads/000012.json is 40.0 h old" in p for p in r.problems)


@pytest.mark.parametrize("status", ["Suspended", ""])
def test_versioning_not_enabled_is_a_problem_even_if_every_key_has_one_version(status):
    # Review finding: with versioning suspended an overwrite replaces the only version,
    # so "1 version per key" would pass while write-once is broken.
    r = audit(healthy(), NOW, P, versioning=status)
    assert not r.ok and len(r.problems) == 1
    assert "bucket versioning is" in r.problems[0]
    assert r.lines()[0].endswith("PROBLEM")


def test_audit_requires_the_versioning_status():
    with pytest.raises(TypeError):
        audit(healthy(), NOW, P)  # type: ignore[call-arg]


def test_other_prefixes_heads_dont_count():
    r = audit([v("laptop/heads/000099.json")], NOW, P, versioning="Enabled")
    assert r.head_key is None and not r.ok


# --- S3VersionLister against a stubbed client -------------------------------------


@pytest.fixture
def s3():
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x"
    )
    with Stubber(client) as stub:
        yield client, stub


def _item(key, vid, latest, when):
    return {"Key": key, "VersionId": vid, "IsLatest": latest, "LastModified": when}


def test_s3_lister_maps_versions_and_delete_markers_across_pages(s3):
    client, stub = s3
    t1 = datetime(2026, 11, 15, 2, 37, 14, tzinfo=UTC)
    t2 = datetime(2026, 11, 15, 9, 0, 0, tzinfo=UTC)
    stub.add_response(
        "list_object_versions",
        {
            "IsTruncated": True,
            "NextKeyMarker": "vm/heads/000012.json",
            "NextVersionIdMarker": "h1",
            "Versions": [_item("vm/heads/000012.json", "h1", True, t1)],
        },
        {"Bucket": "b", "Prefix": P},
    )
    stub.add_response(
        "list_object_versions",
        {
            "IsTruncated": False,
            "Versions": [_item("vm/x", "x1", False, t1)],
            "DeleteMarkers": [_item("vm/x", "dm", True, t2)],
        },
        {
            "Bucket": "b",
            "Prefix": P,
            "KeyMarker": "vm/heads/000012.json",
            "VersionIdMarker": "h1",
        },
    )
    got = S3VersionLister("b", client).list_versions(P)
    stub.assert_no_pending_responses()
    assert got == [
        ObjectVersion("vm/heads/000012.json", "h1", int(t1.timestamp() * 1000), True, False),
        ObjectVersion("vm/x", "x1", int(t1.timestamp() * 1000), False, False),
        ObjectVersion("vm/x", "dm", int(t2.timestamp() * 1000), True, True),
    ]


@pytest.mark.parametrize(("response", "expected"), [({"Status": "Enabled"}, "Enabled"), ({}, "")])
def test_s3_lister_versioning_status(s3, response, expected):
    client, stub = s3
    stub.add_response("get_bucket_versioning", response, {"Bucket": "b"})
    assert S3VersionLister("b", client).versioning_status() == expected


def test_s3_lister_raises_access_denied(s3):
    # The VM's key gets exactly this; the CLI turns it into exit 2 "cannot check".
    client, stub = s3
    stub.add_client_error("list_object_versions", "AccessDenied", http_status_code=403)
    with pytest.raises(Exception, match="AccessDenied"):
        S3VersionLister("b", client).list_versions(P)


# --- CLI: exit codes and alerts -----------------------------------------------------


class FakeLister:
    def __init__(self, versions=None, error: Exception | None = None, versioning="Enabled"):
        self.versions, self.error, self.versioning = versions or [], error, versioning

    def versioning_status(self):
        return self.versioning

    def list_versions(self, prefix):
        if self.error:
            raise self.error
        return self.versions


class RecordingNotifier:
    def __init__(self):
        self.events = []

    async def notify(self, event):
        self.events.append(event)


@pytest.fixture
def settings(tmp_path):
    return replace(Settings.from_env(), data_dir=tmp_path, backup_bucket="b", ntfy_topic=None)


@pytest.fixture
def sent(monkeypatch):
    notifier = RecordingNotifier()
    monkeypatch.setattr(cli, "_notifier", lambda ntfy: notifier)
    monkeypatch.setattr(cli, "now_ms", lambda: NOW)
    return notifier.events


def test_cli_ok_exits_0_and_sends_nothing(settings, sent, capsys):
    code = asyncio.run(cli._backup_status(settings, FakeLister(healthy())))
    assert code == 0 and sent == []
    assert "backup status OK" in capsys.readouterr().out


def test_cli_problem_exits_1_and_alerts_once_with_every_problem(settings, sent, capsys):
    key = "vm/tables/a/date=2026-11-13/part-1.parquet"
    lister = FakeLister(healthy(head_age_h=40) + [v(key, 0.2, vid="v2")])
    code = asyncio.run(cli._backup_status(settings, lister))
    assert code == 1
    assert len(sent) == 1 and isinstance(sent[0], BackupAudit)
    assert key in sent[0].detail and "40.0 h old" in sent[0].detail
    assert sent[0].format().startswith("ALERT kind=backup-audit venue=s3")
    out = capsys.readouterr().out
    assert out.count("PROBLEM  ") == 2 and "backup status PROBLEM" in out


def test_cli_suspended_versioning_exits_1(settings, sent):
    lister = FakeLister(healthy(), versioning="Suspended")
    assert asyncio.run(cli._backup_status(settings, lister)) == 1
    assert "versioning" in sent[0].detail


def test_alert_text_is_capped_but_terminal_lists_everything(settings, sent, capsys):
    # Review finding: a bulk overwrite must not produce a push over ntfy's 4096 bytes.
    many = (
        healthy()
        + [v(f"vm/tables/x/date=2026-11-{d:02d}/part-1.parquet", 1, vid="v2") for d in range(1, 30)]
        + [v(f"vm/tables/x/date=2026-11-{d:02d}/part-1.parquet") for d in range(1, 30)]
    )
    assert asyncio.run(cli._backup_status(settings, FakeLister(many))) == 1
    detail = sent[0].detail
    assert detail.startswith("29 problems: ") and detail.endswith("; … 26 more")
    assert len(sent[0].format().encode()) < 4096
    assert capsys.readouterr().out.count("PROBLEM  ") == 29


def test_cli_cannot_check_exits_2_without_alert(settings, sent, capsys):
    code = asyncio.run(cli._backup_status(settings, FakeLister(error=RuntimeError("token"))))
    assert code == 2 and sent == []
    assert "aws sso login --profile ppm" in capsys.readouterr().err


def test_cli_bad_profile_while_building_the_client_exits_2(settings, sent, monkeypatch, capsys):
    # Found in the live run: ProfileNotFound is raised by boto3.client(), before any
    # request, and escaped as a traceback with exit 1 ("problem") instead of 2.
    from botocore.exceptions import ProfileNotFound

    def broken(_settings):
        raise ProfileNotFound(profile="does-not-exist")

    monkeypatch.setattr(cli, "_s3_client", broken)
    assert asyncio.run(cli._backup_status(settings)) == 2
    assert sent == [] and "ProfileNotFound" in capsys.readouterr().err


def test_cli_without_bucket_exits_2(settings, sent):
    no_bucket = replace(settings, backup_bucket=None)
    assert asyncio.run(cli._backup_status(no_bucket, FakeLister(healthy()))) == 2


def test_cli_parses_status_and_rejects_it_with_loop(monkeypatch):
    called = {}

    async def fake_status(settings, lister=None):
        called["yes"] = True
        return 0

    monkeypatch.setattr(cli, "_backup_status", fake_status)
    monkeypatch.setenv("PPM_BACKUP_BUCKET", "b")
    with pytest.raises(SystemExit) as exit_:
        cli.main(["backup", "--status"])
    assert exit_.value.code == 0 and called
    with pytest.raises(SystemExit) as exit_:
        cli.main(["backup", "--status", "--loop"])
    assert exit_.value.code == 2  # argparse: mutually exclusive
