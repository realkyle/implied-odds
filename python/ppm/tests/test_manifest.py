"""Tampering tests: every kind of change to finalized data must fail verification."""

from __future__ import annotations

import json
import shutil

import pytest
from ppm.integrity.manifest import GENESIS, Manifest


@pytest.fixture
def chained(tmp_path):
    """A manifest with three entries over three real files."""
    m = Manifest(tmp_path)
    for i, name in enumerate(["a", "b", "c"]):
        f = tmp_path / "tables" / name / "date=2026-11-11" / "part-1.parquet"
        f.parent.mkdir(parents=True)
        f.write_bytes(f"contents of {name}".encode() * 100)
        m.append(date="2026-11-11", kind="table", name=name, path=f, count=i, created_ts=i)
    return m


def files(m: Manifest):
    return [m.root / e.path for e in m.entries()]


def messages(m: Manifest, **kw) -> list[str]:
    return [p.message for p in m.verify(**kw)]


def test_untouched_data_verifies_and_the_head_commits_to_everything(chained):
    assert chained.verify() == []
    entries = list(chained.entries())
    assert entries[0].prev == GENESIS
    assert [e.prev for e in entries[1:]] == [e.chain for e in entries[:-1]]
    assert chained.head() == entries[-1].chain


def test_modified_file_is_caught(chained):
    target = files(chained)[1]
    target.write_bytes(target.read_bytes().replace(b"b", b"B", 1))  # one byte
    assert messages(chained) == ["file hash mismatch: contents changed"]


def test_deleted_file_is_caught(chained):
    files(chained)[0].unlink()
    assert messages(chained) == ["file is missing"]


def test_swapped_files_are_caught(chained):
    a, b, _ = files(chained)
    tmp = a.with_suffix(".swap")
    shutil.move(a, tmp)
    shutil.move(b, a)
    shutil.move(tmp, b)
    assert messages(chained).count("file hash mismatch: contents changed") == 2


def test_extra_file_is_caught(chained):
    extra = chained.root / "tables" / "a" / "date=2026-11-12" / "part-1.parquet"
    extra.parent.mkdir(parents=True)
    extra.write_bytes(b"sneaked in")
    assert messages(chained) == ["file is not in the manifest"]


def edit_lines(m: Manifest, fn) -> None:
    lines = m.path.read_text().splitlines()
    m.path.write_text("\n".join(fn(lines)) + "\n")


def test_edited_manifest_entry_is_caught(chained):
    def bump_count(lines):
        e = json.loads(lines[1])
        e["count"] += 1  # e.g. hide dropped rows
        lines[1] = json.dumps(e)
        return lines

    edit_lines(chained, bump_count)
    assert "chain mismatch: entry was edited" in messages(chained)


def test_edit_that_also_recomputes_the_file_hash_still_breaks_the_chain(chained):
    """An attacker who swaps a file AND updates its sha256 in the manifest is still caught."""
    target = files(chained)[0]
    target.write_bytes(b"forged")

    def forge(lines):
        import hashlib

        e = json.loads(lines[0])
        e["sha256"] = hashlib.sha256(b"forged").hexdigest()
        lines[0] = json.dumps(e)
        return lines

    edit_lines(chained, forge)
    assert messages(chained) == ["chain mismatch: entry was edited"]


def test_reordered_manifest_is_caught(chained):
    edit_lines(chained, lambda lines: [lines[1], lines[0], lines[2]])
    msgs = messages(chained)
    assert any("sequence gap" in m for m in msgs) and any("prev does not match" in m for m in msgs)


def test_deleted_middle_entry_is_caught(chained):
    edit_lines(chained, lambda lines: [lines[0], lines[2]])
    msgs = messages(chained)
    assert "prev does not match the previous chain" in msgs
    assert "file is not in the manifest" in msgs  # its file is now unaccounted for


def test_truncated_tail_needs_the_recorded_head(chained):
    head = chained.head()
    last = files(chained)[-1]
    edit_lines(chained, lambda lines: lines[:-1])
    last.unlink()  # remove the evidence too
    assert chained.verify() == []  # self-consistent: undetectable without an anchor...
    assert any("does not match expected" in m for m in messages(chained, expect_head=head))
