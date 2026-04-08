#!/usr/bin/env python3

# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_SLOT_COUNT = 4
DEFAULT_WORKTREE_ROOT = ".worktrees/agent-pool"
DEFAULT_STATE_ROOT = ".agent-pool"
DEFAULT_BRANCH_PREFIX = "agent-pool"
DEFAULT_THIRDPARTY_ROOT = "thirdparty"
DEFAULT_PYTHON = "python3"
DEFAULT_BUILD_TYPE = "Release"
DEFAULT_CCACHE_SLOPPINESS = "pch_defines,time_macros,include_file_mtime,include_file_ctime"


class AgentPoolError(RuntimeError):
    pass


@dataclass(frozen=True)
class PoolLayout:
    repo_root: Path
    worktree_root: Path
    state_root: Path
    locks_root: Path
    config_path: Path


@dataclass(frozen=True)
class SlotPaths:
    slot_name: str
    worktree_path: Path
    state_path: Path
    lock_path: Path
    lock_info_path: Path
    meta_path: Path
    build_prefix: Path


def _run(
    args: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    capture_output: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        capture_output=capture_output,
        check=False,
        text=True,
    )
    if check and completed.returncode != 0:
        stderr = completed.stderr.strip() if completed.stderr else ""
        stdout = completed.stdout.strip() if completed.stdout else ""
        detail = stderr or stdout or f"{args[0]} exited with status {completed.returncode}"
        raise AgentPoolError(detail)
    return completed


def _git(
    repo_root: Path,
    *git_args: str,
    cwd: Path | None = None,
    capture_output: bool = True,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return _run(
        ["git", *git_args],
        cwd=cwd or repo_root,
        capture_output=capture_output,
        check=check,
    )


def _load_json(path: Path, *, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text())


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _resolve_repo_root(cwd: Path) -> Path:
    result = _run(["git", "rev-parse", "--show-toplevel"], cwd=cwd, capture_output=True)
    return Path(result.stdout.strip()).resolve()


def _resolve_ccache_dir(env: dict[str, str]) -> str | None:
    explicit = env.get("CCACHE_DIR")
    if explicit:
        return explicit
    ccache_bin = shutil.which("ccache")
    if ccache_bin is None:
        return None
    try:
        result = _run([ccache_bin, "--get-config", "cache_dir"], env=env, capture_output=True)
    except AgentPoolError:
        return None
    resolved = result.stdout.strip()
    return resolved or None


def _normalize_slot_name(value: str) -> str:
    if value.startswith("slot-"):
        suffix = value[5:]
    else:
        suffix = value
    try:
        slot_number = int(suffix)
    except ValueError as exc:
        raise AgentPoolError(f"invalid slot selector: {value}") from exc
    if slot_number < 1:
        raise AgentPoolError(f"slot number must be >= 1: {value}")
    return f"slot-{slot_number:02d}"


def _slot_names(slot_count: int) -> list[str]:
    return [f"slot-{index:02d}" for index in range(1, slot_count + 1)]


def _validate_slot_name(slot_name: str, slot_count: int) -> None:
    slot_number = int(slot_name.split("-", 1)[1])
    if slot_number > slot_count:
        raise AgentPoolError(f"{slot_name} is outside the configured pool of {slot_count} slots")


def _infer_slot_name_from_cwd(layout: PoolLayout, cwd: Path) -> str | None:
    resolved_cwd = cwd.resolve()
    try:
        relative = resolved_cwd.relative_to(layout.worktree_root.resolve())
    except ValueError:
        return None
    if not relative.parts:
        return None
    candidate = relative.parts[0]
    if candidate.startswith("slot-"):
        return candidate
    return None


def _registered_worktrees(repo_root: Path) -> set[Path]:
    result = _git(repo_root, "worktree", "list", "--porcelain", capture_output=True)
    worktrees: set[Path] = set()
    for line in result.stdout.splitlines():
        if line.startswith("worktree "):
            worktrees.add(Path(line.split(" ", 1)[1]).resolve())
    return worktrees


def _resolve_layout(args: argparse.Namespace, cwd: Path) -> PoolLayout:
    repo_root = _resolve_repo_root(cwd)
    worktree_root = Path(
        args.worktree_root
        or os.environ.get("STARROCKS_AGENT_POOL_WORKTREE_ROOT")
        or (repo_root / DEFAULT_WORKTREE_ROOT)
    ).expanduser()
    state_root = Path(
        args.state_root
        or os.environ.get("STARROCKS_AGENT_POOL_STATE_ROOT")
        or (repo_root / DEFAULT_STATE_ROOT)
    ).expanduser()
    if not worktree_root.is_absolute():
        worktree_root = (repo_root / worktree_root).resolve()
    else:
        worktree_root = worktree_root.resolve()
    if not state_root.is_absolute():
        state_root = (repo_root / state_root).resolve()
    else:
        state_root = state_root.resolve()
    return PoolLayout(
        repo_root=repo_root,
        worktree_root=worktree_root,
        state_root=state_root,
        locks_root=state_root / "locks",
        config_path=state_root / "config.json",
    )


def _read_slot_count(layout: PoolLayout, args: argparse.Namespace) -> int:
    if getattr(args, "slots", None):
        return int(args.slots)
    config = _load_json(layout.config_path, default={})
    if isinstance(config, dict) and "slots" in config:
        return int(config["slots"])
    discovered: set[str] = set()
    if layout.worktree_root.exists():
        discovered.update(path.name for path in layout.worktree_root.iterdir() if path.is_dir() and path.name.startswith("slot-"))
    if layout.state_root.exists():
        discovered.update(path.name for path in layout.state_root.iterdir() if path.is_dir() and path.name.startswith("slot-"))
    if discovered:
        return max(int(name.split("-", 1)[1]) for name in discovered)
    return DEFAULT_SLOT_COUNT


def _write_config(layout: PoolLayout, slot_count: int) -> None:
    layout.state_root.mkdir(parents=True, exist_ok=True)
    _write_json(
        layout.config_path,
        {
            "slots": slot_count,
            "state_root": str(layout.state_root),
            "worktree_root": str(layout.worktree_root),
        },
    )


def _slot_paths(layout: PoolLayout, slot_name: str) -> SlotPaths:
    state_path = layout.state_root / slot_name
    return SlotPaths(
        slot_name=slot_name,
        worktree_path=layout.worktree_root / slot_name,
        state_path=state_path,
        lock_path=layout.locks_root / f"{slot_name}.lock",
        lock_info_path=layout.locks_root / f"{slot_name}.lock" / "owner.json",
        meta_path=state_path / "meta.json",
        build_prefix=state_path / "be",
    )


def _ensure_slot_state(paths: SlotPaths) -> None:
    paths.state_path.mkdir(parents=True, exist_ok=True)
    paths.build_prefix.mkdir(parents=True, exist_ok=True)


def _ensure_worktree(layout: PoolLayout, paths: SlotPaths, base_commit: str) -> None:
    layout.worktree_root.mkdir(parents=True, exist_ok=True)
    registered = _registered_worktrees(layout.repo_root)
    if paths.worktree_path.exists():
        if paths.worktree_path.resolve() not in registered:
            raise AgentPoolError(
                f"{paths.worktree_path} exists but is not a registered git worktree; repair or remove it first"
            )
        return
    _git(
        layout.repo_root,
        "worktree",
        "add",
        "--detach",
        str(paths.worktree_path),
        base_commit,
    )


def _resolve_base_commit(layout: PoolLayout, revision: str) -> str:
    result = _git(
        layout.repo_root,
        "rev-parse",
        "--verify",
        f"{revision}^{{commit}}",
        capture_output=True,
    )
    return result.stdout.strip()


def _slot_branch_name(slot_name: str) -> str:
    return f"{DEFAULT_BRANCH_PREFIX}/{slot_name}"


def _prepare_slot(layout: PoolLayout, paths: SlotPaths, base_commit: str) -> None:
    _ensure_worktree(layout, paths, base_commit)
    _ensure_slot_state(paths)
    branch_name = _slot_branch_name(paths.slot_name)
    _git(layout.repo_root, "checkout", "--detach", base_commit, cwd=paths.worktree_path)
    _git(layout.repo_root, "reset", "--hard", base_commit, cwd=paths.worktree_path)
    _git(layout.repo_root, "clean", "-fdx", cwd=paths.worktree_path)
    _git(layout.repo_root, "checkout", "-B", branch_name, base_commit, cwd=paths.worktree_path)


def _slot_dirty(layout: PoolLayout, paths: SlotPaths) -> bool:
    result = _git(layout.repo_root, "status", "--porcelain", cwd=paths.worktree_path, capture_output=True)
    return bool(result.stdout.strip())


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _lock_payload(paths: SlotPaths, *, base_commit: str, env: dict[str, str], command: list[str] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "slot": paths.slot_name,
        "acquired_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "user": env.get("USER") or env.get("USERNAME") or "unknown",
        "base_commit": base_commit,
        "worktree_path": str(paths.worktree_path),
        "state_path": str(paths.state_path),
        "build_prefix": str(paths.build_prefix),
    }
    if command:
        payload["command"] = command
    return payload


def _slot_env(layout: PoolLayout, paths: SlotPaths, env: dict[str, str]) -> dict[str, str]:
    slot_env = {
        "BUILD_TYPE": env.get("BUILD_TYPE") or DEFAULT_BUILD_TYPE,
        "CMAKE_BUILD_PREFIX": str(paths.build_prefix),
        "CCACHE_BASEDIR": str(layout.repo_root),
        "CCACHE_NOHASHDIR": "1",
        "CCACHE_SLOPPINESS": env.get("CCACHE_SLOPPINESS") or DEFAULT_CCACHE_SLOPPINESS,
        "PYTHON": env.get("PYTHON") or DEFAULT_PYTHON,
        "STARROCKS_AGENT_SLOT": paths.slot_name,
        "STARROCKS_AGENT_POOL_WORKTREE_ROOT": str(layout.worktree_root),
        "STARROCKS_AGENT_POOL_STATE_ROOT": str(layout.state_root),
        "STARROCKS_THIRDPARTY": env.get("STARROCKS_THIRDPARTY") or str(layout.repo_root / DEFAULT_THIRDPARTY_ROOT),
    }
    ccache_dir = _resolve_ccache_dir(env)
    if ccache_dir:
        slot_env["CCACHE_DIR"] = ccache_dir
    return slot_env


def _slot_status(layout: PoolLayout, paths: SlotPaths, env: dict[str, str]) -> dict[str, Any]:
    locked = paths.lock_path.exists()
    lock_info = _load_json(paths.lock_info_path, default={}) if locked else {}
    stale = False
    if isinstance(lock_info, dict) and lock_info.get("pid") and lock_info.get("host") == socket.gethostname():
        stale = not _pid_alive(int(lock_info["pid"]))
    branch = ""
    head = ""
    dirty = False
    if paths.worktree_path.exists():
        branch_result = _git(
            layout.repo_root,
            "rev-parse",
            "--abbrev-ref",
            "HEAD",
            cwd=paths.worktree_path,
            capture_output=True,
            check=False,
        )
        head_result = _git(
            layout.repo_root,
            "rev-parse",
            "--short",
            "HEAD",
            cwd=paths.worktree_path,
            capture_output=True,
            check=False,
        )
        branch = branch_result.stdout.strip()
        head = head_result.stdout.strip()
        dirty = _slot_dirty(layout, paths)
    return {
        "slot": paths.slot_name,
        "locked": locked,
        "stale_lock": stale,
        "branch": branch,
        "head": head,
        "dirty": dirty,
        "worktree_exists": paths.worktree_path.exists(),
        "state_exists": paths.state_path.exists(),
        "worktree_path": str(paths.worktree_path),
        "state_path": str(paths.state_path),
        "build_prefix": str(paths.build_prefix),
        "ccache_dir": _resolve_ccache_dir(env),
        "lock_info": lock_info if isinstance(lock_info, dict) else {},
    }


def _print_status_table(items: list[dict[str, Any]]) -> None:
    for item in items:
        state = "locked" if item["locked"] else "free"
        stale = " stale-lock" if item["stale_lock"] else ""
        dirty = " dirty" if item["dirty"] else ""
        print(f"{item['slot']}: {state}{stale}{dirty} branch={item['branch'] or '-'} head={item['head'] or '-'}")
        print(f"  worktree: {item['worktree_path']}")
        print(f"  state:    {item['state_path']}")
        if item["ccache_dir"]:
            print(f"  ccache:   {item['ccache_dir']}")
        if item["locked"] and item["lock_info"]:
            owner = item["lock_info"].get("user", "unknown")
            pid = item["lock_info"].get("pid", "?")
            print(f"  owner:    {owner} pid={pid}")


def _acquire_slot(
    layout: PoolLayout,
    slot_count: int,
    *,
    slot_name: str | None,
    base_commit: str,
    env: dict[str, str],
    command: list[str] | None = None,
) -> dict[str, Any]:
    if slot_name:
        _validate_slot_name(slot_name, slot_count)
    candidate_names = [slot_name] if slot_name else _slot_names(slot_count)
    for candidate in candidate_names:
        paths = _slot_paths(layout, candidate)
        try:
            paths.lock_path.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            continue
        try:
            _prepare_slot(layout, paths, base_commit)
            slot_env = _slot_env(layout, paths, env)
            lock_info = _lock_payload(paths, base_commit=base_commit, env=env, command=command)
            _write_json(paths.lock_info_path, lock_info)
            meta = {
                **lock_info,
                "branch": _slot_branch_name(paths.slot_name),
                "env": slot_env,
            }
            _write_json(paths.meta_path, meta)
            return {
                "slot": paths.slot_name,
                "base_commit": base_commit,
                "branch": _slot_branch_name(paths.slot_name),
                "worktree_path": str(paths.worktree_path),
                "state_path": str(paths.state_path),
                "build_prefix": str(paths.build_prefix),
                "meta_path": str(paths.meta_path),
                "env": slot_env,
            }
        except Exception:
            shutil.rmtree(paths.lock_path, ignore_errors=True)
            raise
    if slot_name:
        raise AgentPoolError(f"{slot_name} is already locked")
    raise AgentPoolError("no free agent-pool slots are available")


def _release_slot(layout: PoolLayout, paths: SlotPaths, *, force: bool) -> None:
    if not paths.lock_path.exists():
        raise AgentPoolError(f"{paths.slot_name} is not locked")
    if not force and paths.worktree_path.exists() and _slot_dirty(layout, paths):
        raise AgentPoolError(f"{paths.slot_name} is dirty; rerun release with --force")
    shutil.rmtree(paths.lock_path, ignore_errors=True)
    if paths.meta_path.exists():
        paths.meta_path.unlink()


def _warm_slot(layout: PoolLayout, paths: SlotPaths, env: dict[str, str]) -> None:
    command_env = os.environ.copy()
    command_env.update(env)
    print(f"[agent-pool] warming {paths.slot_name}: backend build", file=sys.stderr)
    _run(
        ["./build.sh", "--be", "--without-java-ext"],
        cwd=paths.worktree_path,
        env=command_env,
    )
    print(f"[agent-pool] warming {paths.slot_name}: base_test UT", file=sys.stderr)
    _run(
        ["./run-be-ut.sh", "--build-target", "base_test", "--module", "base_test", "--without-java-ext"],
        cwd=paths.worktree_path,
        env=command_env,
    )


def _cmd_bootstrap(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    layout = _resolve_layout(args, cwd)
    slot_count = _read_slot_count(layout, args)
    _write_config(layout, slot_count)
    layout.worktree_root.mkdir(parents=True, exist_ok=True)
    layout.locks_root.mkdir(parents=True, exist_ok=True)
    base_commit = _resolve_base_commit(layout, args.base)
    slot_env_base = os.environ.copy()
    for slot_name in _slot_names(slot_count):
        paths = _slot_paths(layout, slot_name)
        _prepare_slot(layout, paths, base_commit)
        if not args.skip_warmup:
            _warm_slot(layout, paths, _slot_env(layout, paths, slot_env_base))
    print(f"Bootstrapped {slot_count} slots under {layout.worktree_root}")
    return 0


def _cmd_acquire(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    layout = _resolve_layout(args, cwd)
    slot_count = _read_slot_count(layout, args)
    _write_config(layout, slot_count)
    slot_name = _normalize_slot_name(args.slot) if args.slot else None
    base_commit = _resolve_base_commit(layout, args.base)
    acquired = _acquire_slot(
        layout,
        slot_count,
        slot_name=slot_name,
        base_commit=base_commit,
        env=os.environ.copy(),
    )
    if args.json:
        print(json.dumps(acquired, indent=2, sort_keys=True))
    else:
        print(f"Acquired {acquired['slot']}")
        print(f"worktree: {acquired['worktree_path']}")
        print(f"state:    {acquired['state_path']}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    layout = _resolve_layout(args, cwd)
    slot_count = _read_slot_count(layout, args)
    _write_config(layout, slot_count)
    slot_name = _normalize_slot_name(args.slot) if args.slot else None
    base_commit = _resolve_base_commit(layout, args.base)
    run_command = args.run_command
    if run_command and run_command[0] == "--":
        run_command = run_command[1:]
    acquired = _acquire_slot(
        layout,
        slot_count,
        slot_name=slot_name,
        base_commit=base_commit,
        env=os.environ.copy(),
        command=run_command,
    )
    worktree_path = Path(acquired["worktree_path"])
    paths = _slot_paths(layout, acquired["slot"])
    child_env = os.environ.copy()
    child_env.update(acquired["env"])
    print(f"[agent-pool] running in {acquired['slot']} ({worktree_path})", file=sys.stderr)
    try:
        result = subprocess.run(run_command, cwd=str(worktree_path), env=child_env, check=False)
        return int(result.returncode)
    finally:
        if worktree_path.exists() and not _slot_dirty(layout, paths):
            _release_slot(layout, paths, force=True)
        else:
            print(f"[agent-pool] {acquired['slot']} kept locked because the worktree is dirty", file=sys.stderr)


def _cmd_status(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    layout = _resolve_layout(args, cwd)
    slot_count = _read_slot_count(layout, args)
    items = [_slot_status(layout, _slot_paths(layout, slot_name), os.environ.copy()) for slot_name in _slot_names(slot_count)]
    if args.json:
        print(json.dumps(items, indent=2, sort_keys=True))
    else:
        _print_status_table(items)
    return 0


def _cmd_release(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    layout = _resolve_layout(args, cwd)
    slot_name = args.slot
    if slot_name is None:
        slot_name = _infer_slot_name_from_cwd(layout, cwd)
    if slot_name is None:
        raise AgentPoolError("release requires --slot when not running inside a slot worktree")
    slot_name = _normalize_slot_name(slot_name)
    _release_slot(layout, _slot_paths(layout, slot_name), force=args.force)
    print(f"Released {slot_name}")
    return 0


def _cmd_recycle(args: argparse.Namespace) -> int:
    cwd = Path.cwd()
    layout = _resolve_layout(args, cwd)
    slot_count = _read_slot_count(layout, args)
    if args.all:
        slot_names = _slot_names(slot_count)
    elif args.slot:
        slot_names = [_normalize_slot_name(args.slot)]
    else:
        raise AgentPoolError("recycle requires --slot or --all")

    for slot_name in slot_names:
        paths = _slot_paths(layout, slot_name)
        if paths.lock_path.exists() and not args.force:
            raise AgentPoolError(f"{slot_name} is locked; rerun recycle with --force after release if needed")
        if paths.state_path.exists():
            shutil.rmtree(paths.state_path)
        _ensure_slot_state(paths)
        if paths.meta_path.exists():
            paths.meta_path.unlink()
        print(f"Recycled {slot_name}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage StarRocks agent-pool worktrees and build state.")
    parser.add_argument("--worktree-root", help="Override the worktree root path.")
    parser.add_argument("--state-root", help="Override the persistent state root path.")

    subparsers = parser.add_subparsers(dest="subcommand", required=True)

    bootstrap = subparsers.add_parser("bootstrap", help="Create pool slots and optional warm build state.")
    bootstrap.add_argument("--slots", type=int, default=DEFAULT_SLOT_COUNT, help="Number of slots to create.")
    bootstrap.add_argument("--base", default="HEAD", help="Base revision for new or reset slots.")
    bootstrap.add_argument("--skip-warmup", action="store_true", help="Create slots without warming BE or UT builds.")
    bootstrap.set_defaults(func=_cmd_bootstrap)

    acquire = subparsers.add_parser("acquire", help="Lock and prepare one slot for agent use.")
    acquire.add_argument("--slot", help="Slot number or name, for example 2 or slot-02.")
    acquire.add_argument("--slots", type=int, help="Override the configured slot count when auto-selecting.")
    acquire.add_argument("--base", default="HEAD", help="Base revision for the prepared slot.")
    acquire.add_argument("--json", action="store_true", help="Print machine-readable slot metadata.")
    acquire.set_defaults(func=_cmd_acquire)

    run = subparsers.add_parser("run", help="Run a command inside an acquired slot.")
    run.add_argument("--slot", help="Slot number or name, for example 2 or slot-02.")
    run.add_argument("--slots", type=int, help="Override the configured slot count when auto-selecting.")
    run.add_argument("--base", default="HEAD", help="Base revision for the prepared slot.")
    run.add_argument("run_command", nargs=argparse.REMAINDER, help="Command to run after --.")
    run.set_defaults(func=_cmd_run)

    status = subparsers.add_parser("status", help="Show slot usage and resolved paths.")
    status.add_argument("--slots", type=int, help="Override the configured slot count.")
    status.add_argument("--json", action="store_true", help="Print machine-readable slot status.")
    status.set_defaults(func=_cmd_status)

    release = subparsers.add_parser("release", help="Release a slot lock.")
    release.add_argument("--slot", help="Slot number or name. Defaults to the current slot worktree when possible.")
    release.add_argument("--force", action="store_true", help="Release even if the slot worktree is dirty.")
    release.set_defaults(func=_cmd_release)

    recycle = subparsers.add_parser("recycle", help="Delete one or more slot state trees without touching ccache.")
    recycle_group = recycle.add_mutually_exclusive_group(required=True)
    recycle_group.add_argument("--slot", help="Slot number or name.")
    recycle_group.add_argument("--all", action="store_true", help="Recycle every configured slot.")
    recycle.add_argument("--slots", type=int, help="Override the configured slot count when using --all.")
    recycle.add_argument("--force", action="store_true", help="Recycle even if a slot is currently locked.")
    recycle.set_defaults(func=_cmd_recycle)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "func", None) is _cmd_run:
        if args.run_command and args.run_command[0] == "--":
            args.run_command = args.run_command[1:]
        if not args.run_command:
            raise AgentPoolError("run requires a command after --")
    return int(args.func(args))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AgentPoolError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
