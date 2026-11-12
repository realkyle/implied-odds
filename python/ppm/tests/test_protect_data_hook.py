"""Tests for .claude/hooks/protect_data.py, the PreToolUse hook that protects live data.

The hook is the hard block behind "never delete data/ or the Redis volume" in CLAUDE.md, so
these tests are its failure test: each dangerous command must exit 2, and everyday commands
must still pass (a hook that blocks too much gets switched off).
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
HOOK = REPO / ".claude" / "hooks" / "protect_data.py"
ROOT = Path("/proj")


def _load_hook():
    spec = importlib.util.spec_from_file_location("protect_data", HOOK)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hook = _load_hook()


@pytest.mark.parametrize(
    "command",
    [
        "rm -rf data/",
        "rm -rf data",
        "rm -r ./data/tables",
        "rm /proj/data/manifest.jsonl",
        "cd data && rm -rf staging",
        "mv data/manifest.jsonl /tmp/",
        "truncate -s 0 data/manifest.jsonl",
        "sed -i '' 's/a/b/' data/manifest.jsonl",
        "sed -n 1p x && sed -i.bak 1d data/manifest.jsonl",
        "find data -name '*.parquet' -delete",
        "echo x > data/manifest.jsonl",
        "echo x >> ./data/manifest.jsonl",
        "rm -rf spool/",
        "docker compose down -v",
        "docker compose -p ppm down --volumes",
        "docker volume rm ppm_redis-data",
        "docker volume prune -f",
        "docker system prune -a --volumes",
        "docker compose exec redis redis-cli FLUSHALL",
        "redis-cli flushdb",
        "git clean -fdx",
        "git clean -fX",
        # Gaps found in review (2026-11-11):
        "cp x data/manifest.jsonl",
        "cp -r foo data/",
        "ln -sf x data/manifest.jsonl",
        "rsync -a tmp/ data/",
        "tar -xf a.tar -C data",
        "unzip -o x.zip -d data",
        "perl -pi -e 's/a/b/' data/manifest.jsonl",
        "echo x >| data/manifest.jsonl",
        "rm -rf *",
        "rm -rf .",
        "rm -rf ./*",
        "rm -rf ~/code/proj/data",
        "rm -rf ../proj/data",
        'rm -rf "$PWD/data"',
        "rm -rf $CLAUDE_PROJECT_DIR/data",
        "rm -rf data*",
        "docker compose -p x down -vt 5",
    ],
)
def test_blocks_destructive_commands(command):
    assert hook.check_bash(command, ROOT) is not None


def test_blocks_destructive_command_when_shell_is_inside_data():
    assert hook.check_bash("rm manifest.jsonl", ROOT, cwd="/proj/data") is not None
    assert hook.check_bash("rm -rf part-0.parquet", ROOT, cwd="/proj/data/tables/x") is not None


def test_allows_reads_when_shell_is_inside_data():
    assert hook.check_bash("ls -la && du -sh .", ROOT, cwd="/proj/data") is None


def test_hook_fails_closed_when_checking_crashes(monkeypatch):
    def boom(*_args):
        raise RuntimeError("bad regex")

    monkeypatch.setattr(hook, "decide", boom)
    monkeypatch.setattr(hook.sys, "stdin", __import__("io").StringIO('{"tool_name": "Bash"}'))
    assert hook.main() == 2


@pytest.mark.parametrize(
    "command",
    [
        "scripts/recording.sh status",
        "scripts/recording.sh stop",
        "ls -la data/",
        "du -sh data/tables",
        "uv run ppm verify --expect-head abc",
        "uv run ppm merge --date 2026-11-12",
        "cat data/manifest.jsonl | head",
        "rm -rf build/",
        "rm -rf /tmp/scratch/data",
        "rm python/ppm/data_quality.py",
        "docker compose down",
        "docker compose up -d --build",
        "git clean -fd",
        "uv run pytest",
        "rm -rf .venv",
        "rm *.pyc",
        "tar -czf /tmp/backup.tgz data/",
        "rm -rf /tmp/proj-other/data",
    ],
)
def test_allows_everyday_commands(command):
    assert hook.check_bash(command, ROOT) is None


@pytest.mark.parametrize(
    ("file_path", "cwd"),
    [
        ("/proj/data/manifest.jsonl", ""),
        ("data/tables/x/part-0.parquet", "/proj"),
        ("../data/manifest.jsonl", "/proj/docs"),
        ("/proj/spool/deribit/1.json", ""),
    ],
)
def test_blocks_edits_inside_protected_dirs(file_path, cwd):
    assert hook.check_file_edit(file_path, cwd, ROOT) is not None


@pytest.mark.parametrize(
    "file_path",
    ["/proj/docs/WorkLog.md", "/proj/python/ppm/data_quality.py", "/proj/database/x"],
)
def test_allows_edits_outside_protected_dirs(file_path):
    assert hook.check_file_edit(file_path, "", ROOT) is None


def _run_hook(event: dict) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(HOOK)],
        input=json.dumps(event),
        capture_output=True,
        text=True,
        env={"CLAUDE_PROJECT_DIR": str(ROOT)},
        check=False,
    )


def test_end_to_end_block_exits_2_with_reason():
    result = _run_hook({"tool_name": "Bash", "tool_input": {"command": "rm -rf data/"}})
    assert result.returncode == 2
    assert "protect_data.py" in result.stderr


def test_end_to_end_write_into_data_is_blocked():
    event = {
        "tool_name": "Write",
        "cwd": "/proj",
        "tool_input": {"file_path": "data/manifest.jsonl", "content": ""},
    }
    assert _run_hook(event).returncode == 2


def test_end_to_end_allow_exits_0():
    result = _run_hook({"tool_name": "Bash", "tool_input": {"command": "ls data/"}})
    assert result.returncode == 0
    assert result.stderr == ""


def test_unreadable_input_is_allowed():
    result = subprocess.run(
        [sys.executable, str(HOOK)], input="not json", capture_output=True, text=True, check=False
    )
    assert result.returncode == 0
