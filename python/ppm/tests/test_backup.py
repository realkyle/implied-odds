"""Off-machine backup: what gets uploaded, in what order, and what is refused.

The job runs against `MemoryStore`, which enforces write-once like the real bucket, so a
test can't pass by overwriting. `S3Store` and the permission check run against
botocore's `Stubber`: real boto3 client, no network, and it asserts the exact request
parameters (e.g. that `IfNoneMatch="*"` is sent).
"""

from __future__ import annotations

import io
import json

import boto3
import pytest
from botocore.stub import ANY, Stubber
from ppm.backup.check import check_permissions
from ppm.backup.job import BackupJob, BackupRefused, Head, head_key, manifest_key
from ppm.backup.restore import RestoreError, restore
from ppm.backup.store import MemoryStore, S3Store, sha256_bytes
from ppm.config import Settings
from ppm.integrity.manifest import Manifest

P = "vm/"


def add_day(m: Manifest, day: str, names=("a", "b")) -> None:
    for i, name in enumerate(names):
        f = m.root / "tables" / name / f"date={day}" / "part-1.parquet"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(f"{name} {day} ".encode() * 50)
        m.append(date=day, kind="table", name=name, path=f, count=i, created_ts=i)


@pytest.fixture
def data(tmp_path):
    m = Manifest(tmp_path / "data")
    add_day(m, "2026-11-11")
    return m


def test_first_pass_uploads_files_then_manifest_then_head(data):
    store = MemoryStore()
    report = BackupJob(data.root, store, P).run_once()

    entries = list(data.entries())
    assert store.uploads == [
        P + entries[0].path,
        P + entries[1].path,
        manifest_key(P, 1),
        head_key(P, 1),  # last: a head only ever points at objects already stored
    ]
    assert report.head == Head(1, data.head(), sha256_bytes(data.path.read_bytes()))
    assert report.files == 2
    assert store.get_bytes(manifest_key(P, 1)) == data.path.read_bytes()
    assert Head.from_bytes(store.get_bytes(head_key(P, 1))).head == data.head()


def test_rerun_with_nothing_new_is_a_noop(data):
    store = MemoryStore()
    BackupJob(data.root, store, P).run_once()
    before = list(store.uploads)
    report = BackupJob(data.root, store, P).run_once()
    assert report.head is None and store.uploads == before


def test_next_day_uploads_only_the_new_entries(data):
    store = MemoryStore()
    job = BackupJob(data.root, store, P)
    job.run_once()
    add_day(data, "2026-11-12")
    report = job.run_once()
    assert report.new_entries == [2, 3]
    assert store.uploads[-4:] == [
        P + "tables/a/date=2026-11-12/part-1.parquet",
        P + "tables/b/date=2026-11-12/part-1.parquet",
        manifest_key(P, 3),
        head_key(P, 3),
    ]
    assert store.list_keys(P + "heads/") == {head_key(P, 1), head_key(P, 3)}


def test_crash_mid_pass_writes_no_head_and_the_next_pass_finishes(data):
    store = MemoryStore()
    store.fail_after = 1  # network dies after the first file
    with pytest.raises(ConnectionError):
        BackupJob(data.root, store, P).run_once()
    assert store.list_keys(P + "heads/") == set()  # no head pointing at missing data

    store.fail_after = None
    report = BackupJob(data.root, store, P).run_once()
    assert report.already_there == 1 and report.files == 1  # first file: HEAD, not re-sent
    assert report.head is not None and report.head.seq == 1


def test_crash_after_files_before_head_resumes_with_existing_objects(data):
    store = MemoryStore()
    store.fail_after = 3  # files + manifest uploaded, head not
    with pytest.raises(ConnectionError):
        BackupJob(data.root, store, P).run_once()
    store.fail_after = None
    report = BackupJob(data.root, store, P).run_once()
    # No head yet, so all entries count as new; their objects exist with the right hash.
    assert report.already_there == 2 and report.files == 0
    assert head_key(P, 1) in store.objects


def test_file_changed_after_merge_is_refused_before_upload(data):
    store = MemoryStore()
    (data.root / next(data.entries()).path).write_bytes(b"tampered")
    with pytest.raises(BackupRefused) as exc:
        BackupJob(data.root, store, P).run_once()
    assert exc.value.reason == "integrity"
    assert store.uploads == []


def test_missing_file_is_refused(data):
    (data.root / next(data.entries()).path).unlink()
    with pytest.raises(BackupRefused, match="file is missing"):
        BackupJob(data.root, MemoryStore(), P).run_once()


def test_edited_manifest_chain_is_refused(data):
    lines = data.path.read_text().splitlines()
    first = json.loads(lines[0])
    first["count"] = 999
    lines[0] = json.dumps(first, sort_keys=True, separators=(",", ":"))
    data.path.write_text("\n".join(lines) + "\n")
    with pytest.raises(BackupRefused) as exc:
        BackupJob(data.root, MemoryStore(), P).run_once()
    assert exc.value.reason == "integrity"


def test_truncated_manifest_after_backup_is_a_fork(data):
    store = MemoryStore()
    add_day(data, "2026-11-12")
    BackupJob(data.root, store, P).run_once()
    lines = data.path.read_bytes().splitlines(keepends=True)
    data.path.write_bytes(b"".join(lines[:2]))  # drop the last two entries
    with pytest.raises(BackupRefused) as exc:
        BackupJob(data.root, store, P).run_once()
    assert exc.value.reason == "fork" and "truncated" in exc.value.detail


def test_rewritten_history_is_a_fork_and_nothing_is_overwritten(data, tmp_path):
    store = MemoryStore()
    BackupJob(data.root, store, P).run_once()
    stored_head = store.get_bytes(head_key(P, 1))
    # Same length, different history: rebuild the chain from different file contents.
    other = Manifest(tmp_path / "other")
    for name in ("a", "b"):
        f = other.root / "tables" / name / "date=2026-11-11" / "part-1.parquet"
        f.parent.mkdir(parents=True)
        f.write_bytes(b"different")
        other.append(date="2026-11-11", kind="table", name=name, path=f, count=0, created_ts=0)
    add_day(other, "2026-11-12")
    with pytest.raises(BackupRefused) as exc:
        BackupJob(other.root, store, P).run_once()
    assert exc.value.reason == "fork"
    assert store.get_bytes(head_key(P, 1)) == stored_head


def test_existing_object_with_other_contents_is_a_fork(data):
    store = MemoryStore()
    first = next(data.entries())
    store.put_bytes_if_absent(P + first.path, b"something else", sha256_bytes(b"something else"))
    with pytest.raises(BackupRefused) as exc:
        BackupJob(data.root, store, P).run_once()
    assert exc.value.reason == "fork"
    assert store.list_keys(P + "heads/") == set()


def test_partial_last_line_is_left_for_the_next_pass(data):
    with open(data.path, "ab") as fh:
        fh.write(b'{"seq": 2, "half-written')  # the merger is mid-append
    report = BackupJob(data.root, MemoryStore(), P).run_once()
    assert report.head is not None and report.head.seq == 1


def test_empty_data_dir_uploads_nothing(tmp_path):
    store = MemoryStore()
    assert BackupJob(tmp_path, store, P).run_once().head is None
    assert store.uploads == []


def test_prefix_must_end_with_slash(tmp_path):
    with pytest.raises(ValueError):
        BackupJob(tmp_path, MemoryStore(), "vm")


def test_memory_store_is_write_once():
    store = MemoryStore()
    assert store.put_bytes_if_absent("k", b"1", sha256_bytes(b"1"))
    assert not store.put_bytes_if_absent("k", b"2", sha256_bytes(b"2"))
    assert store.get_bytes("k") == b"1"


# --- restore: the logic behind 2c2's tamper and restore tests


def test_restore_round_trip_verifies_and_truncation_fails(data, tmp_path):
    store = MemoryStore()
    add_day(data, "2026-11-12")
    BackupJob(data.root, store, P).run_once()
    dest = tmp_path / "restored"
    head = restore(store, P, dest)

    restored = Manifest(dest)
    assert restored.verify(expect_head=head.head) == []

    lines = restored.path.read_bytes().splitlines(keepends=True)
    restored.path.write_bytes(b"".join(lines[:-1]))  # truncate one entry
    problems = [p.message for p in restored.verify(expect_head=head.head)]
    assert any("does not match expected" in m for m in problems)
    # Without the off-machine head the truncated chain would look fine (only the file
    # left behind is flagged) - which is why the head must live elsewhere.
    assert not any("does not match expected" in p.message for p in restored.verify())


def test_restore_older_head(data, tmp_path):
    store = MemoryStore()
    job = BackupJob(data.root, store, P)
    job.run_once()
    old_head = data.head()
    add_day(data, "2026-11-12")
    job.run_once()
    head = restore(store, P, tmp_path / "r", seq=1)
    assert head.head == old_head
    assert Manifest(tmp_path / "r").verify(expect_head=old_head) == []


def test_restore_refuses_non_empty_dir(data, tmp_path):
    store = MemoryStore()
    BackupJob(data.root, store, P).run_once()
    with pytest.raises(RestoreError, match="not empty"):
        restore(store, P, data.root)  # e.g. pointed at live data by mistake


def test_restore_rejects_manifest_that_does_not_match_its_head(data, tmp_path):
    store = MemoryStore()
    BackupJob(data.root, store, P).run_once()
    store.objects[manifest_key(P, 1)] = (b"forged\n", "x")  # bypass write-once on purpose
    with pytest.raises(RestoreError, match="does not match"):
        restore(store, P, tmp_path / "r")


def test_backup_credentials_are_never_under_the_data_dir():
    # The VM mounts the key at /run/secrets/…; the data dir is /data. The job uploads only
    # manifest-listed paths, but keep the two apart by construction as well.
    settings = Settings.from_env()
    assert "secrets" not in str(settings.data_dir)


# --- S3Store against a stubbed boto3 client


@pytest.fixture
def s3():
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="x"
    )
    with Stubber(client) as stub:
        yield client, stub


def test_s3_put_sends_if_none_match_and_returns_false_on_412(s3):
    client, stub = s3
    params = {
        "Bucket": "b",
        "Key": "vm/k",
        "Body": b"data",
        "IfNoneMatch": "*",
        "Metadata": {"sha256": "abc"},
        "ChecksumAlgorithm": "SHA256",
    }
    stub.add_response("put_object", {}, params)
    stub.add_client_error(
        "put_object", "PreconditionFailed", http_status_code=412, expected_params=params
    )
    store = S3Store("b", client)
    assert store.put_bytes_if_absent("vm/k", b"data", "abc") is True
    assert store.put_bytes_if_absent("vm/k", b"data", "abc") is False
    stub.assert_no_pending_responses()


def test_s3_put_reraises_other_errors(s3):
    client, stub = s3
    stub.add_client_error("put_object", "AccessDenied", http_status_code=403)
    with pytest.raises(Exception, match="AccessDenied"):
        S3Store("b", client).put_bytes_if_absent("vm/k", b"d", "abc")


def test_s3_multipart_puts_condition_on_complete_and_aborts_on_412(s3, tmp_path, monkeypatch):
    import ppm.backup.store as store_mod

    monkeypatch.setattr(store_mod, "MULTIPART_THRESHOLD", 10)
    monkeypatch.setattr(store_mod, "PART_SIZE", 8)
    f = tmp_path / "big.tar"
    f.write_bytes(b"0123456789abcdef!")  # 17 bytes -> 3 parts
    client, stub = s3
    stub.add_response(
        "create_multipart_upload",
        {"UploadId": "u1"},
        {
            "Bucket": "b",
            "Key": "vm/big",
            "Metadata": {"sha256": "h"},
            "ChecksumAlgorithm": "SHA256",
        },
    )
    for n in (1, 2, 3):
        stub.add_response(
            "upload_part",
            {"ETag": f'"e{n}"', "ChecksumSHA256": f"c{n}"},
            {
                "Bucket": "b",
                "Key": "vm/big",
                "UploadId": "u1",
                "PartNumber": n,
                "Body": ANY,
                "ChecksumAlgorithm": "SHA256",
            },
        )
    stub.add_client_error(
        "complete_multipart_upload",
        "PreconditionFailed",
        http_status_code=412,
        expected_params={
            "Bucket": "b",
            "Key": "vm/big",
            "UploadId": "u1",
            "MultipartUpload": {
                "Parts": [
                    {"PartNumber": n, "ETag": f'"e{n}"', "ChecksumSHA256": f"c{n}"}
                    for n in (1, 2, 3)
                ]
            },
            "IfNoneMatch": "*",
        },
    )
    stub.add_response(
        "abort_multipart_upload", {}, {"Bucket": "b", "Key": "vm/big", "UploadId": "u1"}
    )
    assert S3Store("b", client).put_file_if_absent("vm/big", f, "h") is False
    stub.assert_no_pending_responses()


def test_s3_sha256_of_missing_key_is_none(s3):
    client, stub = s3
    stub.add_client_error("head_object", "404", http_status_code=404)
    stub.add_response("head_object", {"Metadata": {"sha256": "abc"}})
    store = S3Store("b", client)
    assert store.sha256_of("vm/nope") is None
    assert store.sha256_of("vm/yes") == "abc"


def test_s3_get_bytes(s3):
    client, stub = s3
    from botocore.response import StreamingBody

    stub.add_response("get_object", {"Body": StreamingBody(io.BytesIO(b"hi"), 2)})
    assert S3Store("b", client).get_bytes("vm/k") == b"hi"


# --- permission check


def _meta(code: int) -> dict:
    return {"ResponseMetadata": {"HTTPStatusCode": code}}


def test_check_permissions_all_refusals_pass(s3):
    client, stub = s3
    stub.add_response("put_object", _meta(200))
    stub.add_response("head_object", {"ETag": '"e"'})
    stub.add_client_error("put_object", "PreconditionFailed", http_status_code=412)
    stub.add_client_error("put_object", "AccessDenied", http_status_code=403)
    stub.add_client_error("put_object", "AccessDenied", http_status_code=403)
    stub.add_client_error("delete_object", "AccessDenied", http_status_code=403)
    stub.add_client_error("put_object", "AccessDenied", http_status_code=403)
    stub.add_client_error("head_object", "404", http_status_code=404)
    probes = check_permissions(client, "b")
    assert len(probes) == 7 and all(p.ok for p in probes)


def test_check_permissions_flags_an_allowed_overwrite_and_a_403_head(s3):
    client, stub = s3
    stub.add_response("put_object", _meta(200))
    stub.add_response("head_object", {"ETag": '"e"'})
    stub.add_client_error("put_object", "PreconditionFailed", http_status_code=412)
    stub.add_response("put_object", _meta(200))  # policy missing: unconditional PUT allowed
    stub.add_client_error("put_object", "AccessDenied", http_status_code=403)
    stub.add_client_error("delete_object", "AccessDenied", http_status_code=403)
    stub.add_client_error("put_object", "AccessDenied", http_status_code=403)
    stub.add_client_error("head_object", "403", http_status_code=403)  # no list permission
    probes = check_permissions(client, "b")
    assert [p.ok for p in probes] == [True, True, False, True, True, True, False]


# --- review fixes (2026-11-14): empty manifest, schedule, alert de-duplication


def test_manifest_gone_after_a_backup_is_a_fork_not_nothing_new(data):
    store = MemoryStore()
    BackupJob(data.root, store, P).run_once()
    data.path.write_bytes(b"")  # deleted/emptied, or an empty dir mounted at /data
    with pytest.raises(BackupRefused) as exc:
        BackupJob(data.root, store, P).run_once()
    assert exc.value.reason == "fork"


def test_next_backup_never_skips_the_coming_37():
    from ppm.cli import next_backup_at

    hour = 3_600_000
    t0 = 1794618000000  # 2026-11-14 01:00:00 UTC, a whole hour
    m = 60_000
    assert next_backup_at(t0 + 20 * m) == t0 + 37 * m  # startup pass at :20 -> same hour
    assert next_backup_at(t0 + 37 * m) == t0 + hour + 37 * m  # just ran at :37 -> next hour
    assert next_backup_at(t0 + 50 * m) == t0 + hour + 37 * m


def test_alert_key_ignores_changing_error_text_but_not_fork_details():
    from ppm.cli import _alert_key
    from ppm.monitor.watch import BackupFailed

    a = BackupFailed("error", "EndpointConnectionError: request id 1")
    b = BackupFailed("error", "EndpointConnectionError: request id 2")
    c = BackupFailed("error", "ClientError: AccessDenied")
    assert _alert_key(a) == _alert_key(b) != _alert_key(c)
    assert _alert_key(BackupFailed("fork", "seq 3")) != _alert_key(BackupFailed("fork", "seq 4"))


def test_check_permissions_stops_if_the_key_cannot_write_at_all(s3):
    client, stub = s3
    stub.add_client_error("put_object", "InvalidAccessKeyId", http_status_code=403)
    probes = check_permissions(client, "b")
    assert len(probes) == 1 and not probes[0].ok  # 403s below would prove nothing
