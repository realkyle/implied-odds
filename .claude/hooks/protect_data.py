#!/usr/bin/env python3
"""Claude Code PreToolUse hook: block tool calls that would delete or overwrite live data.

Claude Code pipes each pending tool call to this script as JSON on stdin. Exit code 2
blocks the call and shows stderr to Claude; exit code 0 lets it run.

Protected: data/ (merged tables, archive, manifest.jsonl hash chain, staging) and spool/
(envelopes buffered while Redis is down), plus the Redis queue itself (compose volume,
FLUSHALL). The merger deleting its own staging files is unaffected: it is a Python
process, not a Claude tool call, so this hook never sees it.

This catches mistakes, not deliberate bypasses: a command like `python -c "shutil.rmtree(...)"`
is not parsed. Standard library only, because jq is not installed on the dev Mac.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

PROTECTED_DIRS = ("data", "spool")

# Commands that delete, move, overwrite or rewrite files in place. Copy-like verbs are here
# too: `cp x data/manifest.jsonl` overwrites. The cost is that copying *out* of data/ is also
# blocked (use the Read tool or `cat data/x > /tmp/y` instead).
_DESTRUCTIVE_VERB = re.compile(
    r"(?:^|[\s;&|(])(?:rm|rmdir|mv|cp|ln|install|rsync|truncate|shred|unlink|dd|tee)\b"
    r"|\bsed\s+(?:-\S*\s+)*-i"
    r"|\bperl\b.*\s-\w*i"
    r"|\bfind\b.*\s-delete\b"
    r"|\btar\b.*\s-(?:\w*x|-extract)"
    r"|\bunzip\b"
)
# `rm` given a bare glob or the current dir: from the repo root this deletes data/ too.
_RM_EVERYTHING = re.compile(r"(?:^|[\s;&|(])rm\b[^;&|]*\s(?:\./?)?(?:\*|\.)(?:/?\*)?(?:\s|$|[;&|])")
# Destructive no matter which path they name.
_ALWAYS_BLOCKED = [
    (re.compile(r"\bcompose\b.*\bdown\b.*(?:\s-[a-zA-Z]*v|--volumes)"), "removes the Redis volume"),
    (re.compile(r"\bdocker\s+volume\s+(?:rm|prune)\b"), "removes Docker volumes"),
    (re.compile(r"\bdocker\s+system\s+prune\b.*--volumes"), "removes Docker volumes"),
    (re.compile(r"\b(?:flushall|flushdb)\b", re.IGNORECASE), "wipes the Redis queue"),
    (re.compile(r"\bgit\s+clean\b.*\s-\w*[xX]"), "deletes git-ignored files, including data/"),
]


def _project_root() -> Path:
    env = os.environ.get("CLAUDE_PROJECT_DIR")
    return Path(env) if env else Path(__file__).resolve().parents[2]


def _path_prefix(root: Path) -> str:
    """Optional prefix before data/ or spool/: ./, the absolute root, any path ending in the
    project folder's name (covers ~/… and ../quantproj1/), or $PWD / $CLAUDE_PROJECT_DIR."""
    return (
        rf"(?:\./|{re.escape(str(root))}/|\S*/{re.escape(root.name)}/"
        r"|\$\{?(?:PWD|CLAUDE_PROJECT_DIR)\}?/)?"
    )


def _protected_path_pattern(root: Path) -> re.Pattern[str]:
    """Matches a path token that starts with data or spool (incl. globs like data*)."""
    dirs = "|".join(PROTECTED_DIRS)
    return re.compile(rf"(?:^|[\s'\"=<>;&|(]){_path_prefix(root)}(?:{dirs})(?:/|\*|$|[\s'\";&|)])")


def _inside_protected(path: Path, root: Path) -> str | None:
    for name in PROTECTED_DIRS:
        protected = (root / name).resolve()
        if path == protected or protected in path.parents:
            return name
    return None


def check_bash(command: str, root: Path, cwd: str = "") -> str | None:
    """Returns why the command is blocked, or None if it may run.

    cwd is the shell's working directory: Claude's `cd` persists between calls, so after
    `cd data` a plain `rm x` must be caught even though it names no protected path."""
    for pattern, reason in _ALWAYS_BLOCKED:
        if pattern.search(command):
            return reason
    destructive = _DESTRUCTIVE_VERB.search(command)
    if destructive and cwd and _inside_protected(Path(cwd).resolve(), root):
        return "runs a destructive command while the shell is inside data/ or spool/"
    if _RM_EVERYTHING.search(command):
        return (
            "deletes everything in the current directory (from the repo root that includes data/)"
        )
    if destructive and _protected_path_pattern(root).search(command):
        return "deletes, moves or rewrites files under data/ or spool/"
    redirect = rf">[>|]?\s*['\"]?{_path_prefix(root)}(?:data|spool)/"
    if re.search(redirect, command):
        return "redirects output over a file under data/ or spool/"
    return None


def check_file_edit(file_path: str, cwd: str, root: Path) -> str | None:
    """Returns why an Edit/Write to file_path is blocked, or None if allowed."""
    target = Path(cwd, file_path).resolve() if cwd else Path(file_path).resolve()
    name = _inside_protected(target, root)
    return f"{name}/ is append-only live data" if name else None


def decide(event: dict, root: Path) -> str | None:
    """Returns the block reason for one PreToolUse event, or None to allow it."""
    tool_input = event.get("tool_input") or {}
    if event.get("tool_name") == "Bash":
        return check_bash(tool_input.get("command", ""), root, event.get("cwd", ""))
    file_path = tool_input.get("file_path") or tool_input.get("notebook_path")
    if file_path:
        return check_file_edit(file_path, event.get("cwd", ""), root)
    return None


def main() -> int:
    try:
        event = json.load(sys.stdin)
    except json.JSONDecodeError:
        return 0  # Not a tool call we can read; let Claude Code's own checks apply.
    try:
        reason = decide(event, _project_root())
    except Exception as exc:  # noqa: BLE001 - fail closed: a crashing guard must not let calls through
        reason = f"call could not be checked ({type(exc).__name__}: {exc}), so it is blocked"
    if reason:
        print(
            f"Blocked by .claude/hooks/protect_data.py: this {reason}. "
            "Live recording data is irreplaceable; ask the owner to run it by hand if intended.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
