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
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


INTERNAL_INCLUDE_PREFIXES = {
    "agent",
    "base",
    "cache",
    "column",
    "common",
    "connector",
    "exec",
    "exprs",
    "formats",
    "fs",
    "gen_cpp",
    "geo",
    "gutil",
    "http",
    "io",
    "runtime",
    "script",
    "serde",
    "service",
    "staros_integration",
    "storage",
    "testutil",
    "tools",
    "types",
    "udf",
    "util",
}
CODE_EXTENSIONS = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx", ".inl"}
HEADER_CODE_EXTENSIONS = {".h", ".hh", ".hpp", ".hxx", ".inl"}
LINK_SCOPE_KEYWORDS = {"PUBLIC", "PRIVATE", "INTERFACE"}
DEFAULT_MANIFEST = "be/module_boundary_manifest.json"
DEFAULT_BASELINE = "build-support/be_module_boundary_baseline.json"
DEFAULT_EXEC_ENV_HEADER_ALLOWLIST = "build-support/exec_env_header_include_allowlist.txt"
DEFAULT_EXEC_ENV_SINGLETON_ALLOWLIST = "build-support/exec_env_singleton_allowlist.txt"
BASELINE_VIOLATION_KEYS = ("include_violations", "target_link_violations", "test_link_violations")
DEFAULT_CHANGED_FULL_CHECK_PATHS = {
    "AGENTS.md",
    "be/AGENTS.md",
    "be/src/common/AGENTS.md",
    "be/module_boundary_manifest.json",
    "build-support/be_module_boundary_baseline.json",
    "build-support/check_be_module_boundaries.py",
    "build-support/exec_env_header_include_allowlist.txt",
    "build-support/exec_env_singleton_allowlist.txt",
    "build-support/render_be_agents.py",
    "build-support/runtime_state_header_include_allowlist.txt",
}
EXEC_ENV_INCLUDE_PATTERN = re.compile(r'#include\s*[<"]runtime/exec_env\.h[>"]')
EXEC_ENV_SINGLETON_PATTERN = re.compile(r"\bExecEnv::GetInstance\s*\(")
SERVICE_LAYERING_REMEDIATION = (
    "Keep Service as the shared service layer and ServiceBE as the BE-specific top layer; move the dependency upward "
    "or include generated RPC/proto types directly."
)


@dataclass(frozen=True)
class ModuleSpec:
    id: str
    doc_label: str
    owned_targets: tuple[str, ...]
    owned_roots: tuple[str, ...]
    owned_globs: tuple[str, ...]
    additional_owned_globs: tuple[str, ...]
    excluded_globs: tuple[str, ...]
    allowed_include_prefixes: tuple[str, ...]
    forbidden_include_prefixes: tuple[str, ...]
    forbidden_includes: tuple[str, ...]
    allowed_target_deps: tuple[str, ...]
    allowed_test_targets: tuple[str, ...]
    allowed_test_link_deps: tuple[str, ...]
    remediation: str
    summary: str = ""


@dataclass
class CMakeState:
    target_sources: dict[str, list[str]]
    target_definition_paths: dict[str, str]
    target_links: dict[str, list[str]]
    test_target_links: dict[str, list[str]]


@dataclass(frozen=True)
class Violation:
    module: str
    path: str
    edge: str
    detail: str
    remediation: str


@dataclass
class Violations:
    include_violations: list[Violation]
    target_link_violations: list[Violation]
    test_link_violations: list[Violation]

    def all(self) -> list[Violation]:
        return self.include_violations + self.target_link_violations + self.test_link_violations

    def is_empty(self) -> bool:
        return not self.all()


def load_manifest(path: Path) -> dict:
    payload = json.loads(path.read_text())
    modules = []
    for raw in payload.get("modules", []):
        modules.append(
            ModuleSpec(
                id=raw["id"],
                doc_label=raw.get("doc_label", raw["id"]),
                owned_targets=tuple(raw.get("owned_targets", [])),
                owned_roots=tuple(raw.get("owned_roots", [])),
                owned_globs=tuple(raw.get("owned_globs", [])),
                additional_owned_globs=tuple(raw.get("additional_owned_globs", [])),
                excluded_globs=tuple(raw.get("excluded_globs", [])),
                allowed_include_prefixes=tuple(raw.get("allowed_include_prefixes", [])),
                forbidden_include_prefixes=tuple(raw.get("forbidden_include_prefixes", [])),
                forbidden_includes=tuple(raw.get("forbidden_includes", [])),
                allowed_target_deps=tuple(raw.get("allowed_target_deps", [])),
                allowed_test_targets=tuple(raw.get("allowed_test_targets", [])),
                allowed_test_link_deps=tuple(raw.get("allowed_test_link_deps", [])),
                remediation=raw.get("remediation", "Update the module boundary or move the code to an allowed layer."),
                summary=raw.get("summary", ""),
            )
        )
    return {"modules": modules}


def load_baseline(path: Path) -> dict[str, set[tuple[str, str, str]]]:
    if not path.exists():
        return _empty_baseline()
    return _normalize_baseline_payload(json.loads(path.read_text()))


def load_path_allowlist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def find_baseline_expansions(
    previous: dict[str, set[tuple[str, str, str]]],
    current: dict[str, set[tuple[str, str, str]]],
) -> dict[str, set[tuple[str, str, str]]]:
    return {
        key: current.get(key, set()) - previous.get(key, set())
        for key in BASELINE_VIOLATION_KEYS
    }


def parse_cmake_state(repo_root: Path) -> CMakeState:
    target_sources: dict[str, list[str]] = {}
    target_definition_paths: dict[str, str] = {}
    raw_target_links: dict[str, list[str]] = {}
    raw_test_links: dict[str, list[str]] = {}
    cmake_files = sorted(repo_root.glob("be/src/**/CMakeLists.txt")) + sorted(repo_root.glob("be/test/**/CMakeLists.txt"))
    for cmake_path in cmake_files:
        definition_path = cmake_path.relative_to(repo_root).as_posix()
        variables: dict[str, list[str]] = {}
        for command_name, tokens in _iter_cmake_commands(cmake_path):
            if not tokens:
                continue
            lower_name = command_name.lower()
            if lower_name == "set":
                variables[tokens[0]] = _expand_tokens(tokens[1:], variables)
                continue
            if lower_name == "list" and len(tokens) >= 3 and tokens[0].upper() == "APPEND":
                variables.setdefault(tokens[1], [])
                variables[tokens[1]].extend(_expand_tokens(tokens[2:], variables))
                continue
            if command_name == "ADD_BE_LIB":
                target_name = tokens[0]
                source_tokens = _expand_tokens(tokens[1:], variables)
                target_sources[target_name] = _resolve_source_tokens(source_tokens, cmake_path.parent, repo_root)
                target_definition_paths[target_name] = definition_path
                continue
            if lower_name == "target_link_libraries":
                target_name = tokens[0]
                link_tokens = _expand_tokens([token for token in tokens[1:] if token not in LINK_SCOPE_KEYWORDS], variables)
                if "be/test/" in cmake_path.as_posix():
                    raw_test_links[target_name] = link_tokens
                    target_definition_paths.setdefault(target_name, definition_path)
                else:
                    raw_target_links[target_name] = link_tokens

    known_targets = set(target_sources)
    target_links = {target: [token for token in links if _is_internal_link_token(token, known_targets)] for target, links in raw_target_links.items()}
    test_target_links = {
        target: [token for token in links if _is_internal_link_token(token, known_targets)]
        for target, links in raw_test_links.items()
        if target.endswith("_test")
    }
    return CMakeState(
        target_sources=target_sources,
        target_definition_paths=target_definition_paths,
        target_links=target_links,
        test_target_links=test_target_links,
    )


def select_modules_for_changed_paths(
    manifest: dict,
    cmake_state: CMakeState,
    changed_paths: set[str],
    repo_root: Path | None = None,
) -> set[str]:
    if not changed_paths:
        return {module.id for module in manifest["modules"]}
    if changed_paths & DEFAULT_CHANGED_FULL_CHECK_PATHS:
        return {module.id for module in manifest["modules"]}
    if any(path.startswith(".github/workflows/") for path in changed_paths):
        return {module.id for module in manifest["modules"]}

    owned_files_by_module = {}
    for module in manifest["modules"]:
        owned_files_by_module[module.id] = collect_owned_files(module, cmake_state, repo_root) if repo_root else set()

    selected = set()
    for module in manifest["modules"]:
        owned = owned_files_by_module[module.id]
        if any(path in owned for path in changed_paths):
            selected.add(module.id)
            continue
        if any(
            cmake_state.target_definition_paths.get(target_name) in changed_paths
            for target_name in module.owned_targets + module.allowed_test_targets
        ):
            selected.add(module.id)
            continue
        if any(path.startswith(root.rstrip("/") + "/") or path == root.rstrip("/") for root in module.owned_roots for path in changed_paths):
            selected.add(module.id)
            continue
        for glob in module.owned_globs + module.additional_owned_globs:
            if "*" not in glob and glob in changed_paths:
                selected.add(module.id)
                break
    return selected or {module.id for module in manifest["modules"]}


def collect_violations(
    repo_root: Path,
    manifest: dict,
    cmake_state: CMakeState,
    selected_modules: set[str] | None = None,
) -> Violations:
    selected_modules = selected_modules or {module.id for module in manifest["modules"]}
    target_file_owners = build_target_file_owners(repo_root, cmake_state)

    include_violations: list[Violation] = []
    target_link_violations: list[Violation] = []
    test_link_violations: list[Violation] = []

    for module in manifest["modules"]:
        if module.id not in selected_modules:
            continue
        owned_files = collect_owned_files(module, cmake_state, repo_root)
        include_violations.extend(check_includes_for_module(repo_root, module, owned_files, target_file_owners))
        target_link_violations.extend(check_target_links_for_module(module, cmake_state))
        test_link_violations.extend(check_test_links_for_module(module, cmake_state))

    return Violations(
        include_violations=sorted(include_violations, key=lambda item: (item.module, item.path, item.edge)),
        target_link_violations=sorted(target_link_violations, key=lambda item: (item.module, item.path, item.edge)),
        test_link_violations=sorted(test_link_violations, key=lambda item: (item.module, item.path, item.edge)),
    )


def apply_baseline(violations: Violations, baseline: dict[str, set[tuple[str, str, str]]]) -> Violations:
    return Violations(
        include_violations=_filter_violations(violations.include_violations, baseline["include_violations"]),
        target_link_violations=_filter_violations(violations.target_link_violations, baseline["target_link_violations"]),
        test_link_violations=_filter_violations(violations.test_link_violations, baseline["test_link_violations"]),
    )


def check_service_layering(repo_root: Path) -> list[Violation]:
    violations: list[Violation] = []
    src_root = repo_root / "be/src"
    if not src_root.exists():
        return violations
    for file_path in sorted(path for path in src_root.rglob("*") if path.is_file() and path.suffix in CODE_EXTENSIONS):
        rel_path = file_path.relative_to(repo_root).as_posix()
        in_service = rel_path.startswith("be/src/service/")
        in_service_be = rel_path.startswith("be/src/service/service_be/")
        for include_path in _parse_all_includes(file_path):
            if in_service and not in_service_be and (
                include_path.startswith("service/service_be/") or include_path.startswith("service_be/")
            ):
                violations.append(
                    Violation(
                        module="service",
                        path=rel_path,
                        edge=include_path,
                        detail="shared Service code must not include ServiceBE",
                        remediation=SERVICE_LAYERING_REMEDIATION,
                    )
                )
                continue
            if not in_service and (include_path.startswith("service/") or include_path.startswith("service_be/")):
                violations.append(
                    Violation(
                        module="service",
                        path=rel_path,
                        edge=include_path,
                        detail="non-service production code must not include top-level Service headers",
                        remediation=SERVICE_LAYERING_REMEDIATION,
                    )
                )
    return violations


def collect_owned_files(module: ModuleSpec, cmake_state: CMakeState, repo_root: Path | None) -> set[str]:
    if repo_root is None:
        return set()
    owned: set[str] = set()
    for root in module.owned_roots:
        absolute_root = repo_root / root
        if not absolute_root.exists():
            continue
        for path in absolute_root.rglob("*"):
            if path.is_file() and path.suffix in CODE_EXTENSIONS:
                owned.add(path.relative_to(repo_root).as_posix())
    for glob in module.owned_globs + module.additional_owned_globs:
        for path in _iter_manifest_glob_files(repo_root, glob):
            owned.add(path.relative_to(repo_root).as_posix())
    for target_name in module.owned_targets:
        for source in cmake_state.target_sources.get(target_name, []):
            owned.add(source)
            owned.update(_companion_headers(repo_root, Path(source)))
    for glob in module.excluded_globs:
        for path in _iter_manifest_glob_files(repo_root, glob):
            owned.discard(path.relative_to(repo_root).as_posix())
    return {path for path in owned if Path(path).suffix in CODE_EXTENSIONS}


def build_target_file_owners(repo_root: Path, cmake_state: CMakeState) -> dict[str, set[str]]:
    owners: dict[str, set[str]] = {}
    for target_name, source_files in cmake_state.target_sources.items():
        for source in source_files:
            owners.setdefault(source, set()).add(target_name)
            for header in _companion_headers(repo_root, Path(source)):
                owners.setdefault(header, set()).add(target_name)
    return owners


def check_includes_for_module(
    repo_root: Path,
    module: ModuleSpec,
    owned_files: set[str],
    target_file_owners: dict[str, set[str]],
) -> list[Violation]:
    violations: list[Violation] = []
    allowed_owners = set(module.owned_targets) | set(module.allowed_target_deps)
    for rel_path in sorted(owned_files):
        file_path = repo_root / rel_path
        for include_path in _parse_internal_includes(file_path):
            owners = target_file_owners.get(f"be/src/{include_path}", set())
            if owners:
                if not owners & allowed_owners:
                    violations.append(
                        Violation(
                            module=module.id,
                            path=rel_path,
                            edge=include_path,
                            detail=f"include resolves to target(s) {', '.join(sorted(owners))}",
                            remediation=module.remediation,
                        )
                    )
                continue
            if include_path in module.forbidden_includes or (
                _matches_any_prefix(include_path, module.forbidden_include_prefixes)
                and not _matches_any_prefix(include_path, module.allowed_include_prefixes)
            ):
                violations.append(
                    Violation(
                        module=module.id,
                        path=rel_path,
                        edge=include_path,
                        detail=f"forbidden include for {module.doc_label}",
                        remediation=module.remediation,
                    )
                )
                continue
            if _matches_any_prefix(include_path, module.allowed_include_prefixes):
                continue
            violations.append(
                Violation(
                    module=module.id,
                    path=rel_path,
                    edge=include_path,
                    detail=f"internal include is outside the allowed prefixes for {module.doc_label}",
                    remediation=module.remediation,
                )
            )
    return violations


def check_target_links_for_module(module: ModuleSpec, cmake_state: CMakeState) -> list[Violation]:
    violations: list[Violation] = []
    allowed_edges = set(module.allowed_target_deps)
    for target_name in module.owned_targets:
        for dep in cmake_state.target_links.get(target_name, []):
            if dep not in allowed_edges:
                violations.append(
                    Violation(
                        module=module.id,
                        path=f"target:{target_name}",
                        edge=dep,
                        detail=f"target link is outside the allowed deps for {module.doc_label}",
                        remediation=module.remediation,
                    )
                )
    return violations


def check_test_links_for_module(module: ModuleSpec, cmake_state: CMakeState) -> list[Violation]:
    violations: list[Violation] = []
    allowed_test_links = set(module.allowed_test_link_deps)
    for test_target in module.allowed_test_targets:
        for dep in cmake_state.test_target_links.get(test_target, []):
            if dep not in allowed_test_links:
                violations.append(
                    Violation(
                        module=module.id,
                        path=f"test:{test_target}",
                        edge=dep,
                        detail=f"test link is outside the allowed deps for {module.doc_label}",
                        remediation=module.remediation,
                    )
                )
    return violations


def _filter_violations(violations: list[Violation], allowed: set[tuple[str, str, str]]) -> list[Violation]:
    return [violation for violation in violations if (violation.module, violation.path, violation.edge) not in allowed]


def _empty_baseline() -> dict[str, set[tuple[str, str, str]]]:
    return {key: set() for key in BASELINE_VIOLATION_KEYS}


def _normalize_baseline_payload(payload: dict) -> dict[str, set[tuple[str, str, str]]]:
    normalized = _empty_baseline()
    for key in BASELINE_VIOLATION_KEYS:
        normalized[key] = {
            (item["module"], item["path"], item["edge"])
            for item in payload.get(key, [])
        }
    return normalized


def collect_exec_env_include_paths(repo_root: Path) -> set[str]:
    return _collect_matching_paths(
        repo_root,
        search_roots=("be/src", "be/test"),
        extensions=HEADER_CODE_EXTENSIONS,
        pattern=EXEC_ENV_INCLUDE_PATTERN,
    )


def collect_exec_env_singleton_paths(repo_root: Path) -> set[str]:
    return _collect_matching_paths(
        repo_root,
        search_roots=("be/src",),
        extensions=CODE_EXTENSIONS,
        pattern=EXEC_ENV_SINGLETON_PATTERN,
    )


def diff_path_allowlist(current: set[str], allowlist: set[str]) -> tuple[set[str], set[str]]:
    return current - allowlist, allowlist - current


def _collect_matching_paths(
    repo_root: Path,
    search_roots: tuple[str, ...],
    extensions: set[str],
    pattern: re.Pattern[str],
) -> set[str]:
    matched_paths: set[str] = set()
    be_root = repo_root / "be"
    for relative_root in search_roots:
        root = repo_root / relative_root
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in extensions:
                continue
            if pattern.search(path.read_text(errors="ignore")):
                matched_paths.add(path.relative_to(be_root).as_posix())
    return matched_paths


def _iter_cmake_commands(path: Path) -> Iterable[tuple[str, list[str]]]:
    lines = path.read_text().splitlines()
    current: list[str] = []
    balance = 0
    command_name = ""
    for raw_line in lines:
        line = _strip_cmake_comment(raw_line)
        if not line.strip() and not current:
            continue
        if not current:
            match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*\(", line)
            if match is None:
                continue
            command_name = match.group(1)
        current.append(line)
        balance += line.count("(") - line.count(")")
        if balance > 0:
            continue
        command_text = "\n".join(current)
        args = command_text[command_text.find("(") + 1 : command_text.rfind(")")]
        yield command_name, _tokenize_cmake_args(args)
        current = []
        balance = 0
        command_name = ""


def _tokenize_cmake_args(args: str) -> list[str]:
    return [token.strip('"') for token in re.findall(r'"[^"]*"|\$\{[^}]+\}|[^\s()]+', args)]


def _expand_tokens(tokens: list[str], variables: dict[str, list[str]]) -> list[str]:
    expanded: list[str] = []
    for token in tokens:
        variable_match = re.fullmatch(r"\$\{([^}]+)\}", token)
        if variable_match and variable_match.group(1) in variables:
            expanded.extend(_expand_tokens(variables[variable_match.group(1)], variables))
        else:
            expanded.append(token)
    return expanded


def _resolve_source_tokens(tokens: list[str], base_dir: Path, repo_root: Path) -> list[str]:
    resolved: list[str] = []
    normalized_repo_root = repo_root.resolve()
    for token in tokens:
        token_path = Path(token)
        if token_path.suffix not in CODE_EXTENSIONS:
            continue
        absolute = (base_dir / token_path).resolve()
        try:
            resolved.append(absolute.relative_to(normalized_repo_root).as_posix())
        except ValueError:
            continue
    return resolved


def _iter_manifest_glob_files(repo_root: Path, pattern: str) -> Iterable[Path]:
    if pattern.endswith("/**"):
        root = repo_root / pattern.removesuffix("/**")
        if not root.exists():
            return
        yield from (path for path in root.rglob("*") if path.is_file())
        return
    yield from (path for path in repo_root.glob(pattern) if path.is_file())


def _companion_headers(repo_root: Path, source_path: Path) -> set[str]:
    companions = set()
    for suffix in (".h", ".hh", ".hpp", ".hxx", ".inl"):
        candidate = source_path.with_suffix(suffix)
        absolute = repo_root / candidate
        if absolute.exists():
            companions.add(candidate.as_posix())
    return companions


def _parse_all_includes(path: Path) -> list[str]:
    includes: list[str] = []
    pattern = re.compile(r'^\s*#include\s*[<"]([^">]+)[">]')
    for line in path.read_text().splitlines():
        match = pattern.match(line)
        if match is None:
            continue
        includes.append(match.group(1))
    return includes


def _parse_internal_includes(path: Path) -> list[str]:
    return [include_path for include_path in _parse_all_includes(path) if _is_internal_include(include_path)]


def _is_internal_include(include_path: str) -> bool:
    return include_path.split("/", 1)[0] in INTERNAL_INCLUDE_PREFIXES


def _matches_any_prefix(include_path: str, prefixes: Iterable[str]) -> bool:
    for prefix in prefixes:
        if include_path == prefix.rstrip("/") or include_path.startswith(prefix):
            return True
    return False


def _is_internal_link_token(token: str, known_targets: set[str]) -> bool:
    if token in known_targets:
        return True
    return bool(re.fullmatch(r"[A-Z][A-Za-z0-9_]*", token))


def _strip_cmake_comment(line: str) -> str:
    quote_open = False
    for index, char in enumerate(line):
        if char == '"':
            quote_open = not quote_open
        if char == "#" and not quote_open:
            return line[:index]
    return line


def _git_changed_paths(repo_root: Path, base: str) -> set[str]:
    command = ["git", "diff", "--name-only", f"{base}...HEAD"]
    result = subprocess.run(command, cwd=repo_root, check=True, capture_output=True, text=True)
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def _git_read_text(repo_root: Path, ref: str, repo_path: str) -> str | None:
    command = ["git", "show", f"{ref}:{repo_path}"]
    result = subprocess.run(command, cwd=repo_root, check=False, capture_output=True, text=True)
    if result.returncode == 0:
        return result.stdout
    missing_markers = (
        "exists on disk, but not in",
        "does not exist in",
        "pathspec",
    )
    if any(marker in result.stderr for marker in missing_markers):
        return None
    result.check_returncode()
    return None


def _load_baseline_from_git(repo_root: Path, ref: str, baseline_path: Path) -> dict[str, set[tuple[str, str, str]]]:
    repo_path = baseline_path.as_posix()
    text = _git_read_text(repo_root, ref, repo_path)
    if text is None:
        return _empty_baseline()
    return _normalize_baseline_payload(json.loads(text))


def _print_violations(violations: Violations) -> None:
    for label, items in (
        ("include", violations.include_violations),
        ("target-link", violations.target_link_violations),
        ("test-link", violations.test_link_violations),
    ):
        for violation in items:
            print(
                f"[{label}] module={violation.module} path={violation.path} edge={violation.edge}\n"
                f"  {violation.detail}\n"
                f"  remediation: {violation.remediation}"
            )


def _print_baseline_expansions(expansions: dict[str, set[tuple[str, str, str]]]) -> None:
    print("Reviewed baseline entries may only shrink. Remove fixed debt instead of adding or editing baseline entries.")
    for label in BASELINE_VIOLATION_KEYS:
        for module, path, edge in sorted(expansions[label]):
            print(f"[baseline] kind={label} module={module} path={path} edge={edge}")


def _print_path_allowlist_diff(label: str, extra_paths: set[str], stale_paths: set[str], allowlist_path: str) -> None:
    if extra_paths:
        print(f"ERROR: new {label} paths require allowlist review")
        print(f"  allowlist: {allowlist_path}")
        print(f"  action: remove the new dependency or add the path to {allowlist_path} if it is intentional.")
        for path in sorted(extra_paths):
            print(f"  [{label}] new path={path}")
    if stale_paths:
        print(f"ERROR: stale {label} allowlist entries should be removed")
        print(f"  allowlist: {allowlist_path}")
        print(f"  action: delete the stale entries from {allowlist_path}.")
        for path in sorted(stale_paths):
            print(f"  [{label}] stale path={path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check BE module boundary rules from the manifest.")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST, help="Path to be/module_boundary_manifest.json")
    parser.add_argument("--baseline", default=DEFAULT_BASELINE, help="Path to the reviewed baseline JSON")
    parser.add_argument("--mode", choices=("full", "changed"), default="full", help="Check the whole repo or only changed modules")
    parser.add_argument("--base", help="Git base ref for --mode changed")
    parser.add_argument(
        "--enforce-baseline-shrink",
        action="store_true",
        help="Fail if the baseline file adds or edits entries relative to --base; only deletions are allowed.",
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    manifest = load_manifest(repo_root / args.manifest)
    baseline = load_baseline(repo_root / args.baseline)
    exec_env_header_allowlist = load_path_allowlist(repo_root / DEFAULT_EXEC_ENV_HEADER_ALLOWLIST)
    exec_env_singleton_allowlist = load_path_allowlist(repo_root / DEFAULT_EXEC_ENV_SINGLETON_ALLOWLIST)
    cmake_state = parse_cmake_state(repo_root)

    selected_modules = None
    if args.mode == "changed":
        if not args.base:
            parser.error("--base is required when --mode changed")
        changed_paths = _git_changed_paths(repo_root, args.base)
        selected_modules = select_modules_for_changed_paths(manifest, cmake_state, changed_paths, repo_root)

    if args.enforce_baseline_shrink:
        if not args.base:
            parser.error("--base is required when --enforce-baseline-shrink is set")
        previous_baseline = _load_baseline_from_git(repo_root, args.base, Path(args.baseline))
        expansions = find_baseline_expansions(previous_baseline, baseline)
        if any(expansions[key] for key in BASELINE_VIOLATION_KEYS):
            _print_baseline_expansions(expansions)
            return 1

    violations = collect_violations(repo_root, manifest, cmake_state, selected_modules=selected_modules)
    violations = apply_baseline(violations, baseline)
    violations.include_violations.extend(check_service_layering(repo_root))
    violations.include_violations.sort(key=lambda item: (item.module, item.path, item.edge))
    if not violations.is_empty():
        _print_violations(violations)
        return 1

    exec_env_include_extra, exec_env_include_stale = diff_path_allowlist(
        collect_exec_env_include_paths(repo_root),
        exec_env_header_allowlist,
    )
    exec_env_singleton_extra, exec_env_singleton_stale = diff_path_allowlist(
        collect_exec_env_singleton_paths(repo_root),
        exec_env_singleton_allowlist,
    )
    if exec_env_include_extra or exec_env_include_stale or exec_env_singleton_extra or exec_env_singleton_stale:
        _print_path_allowlist_diff(
            "exec-env-include",
            exec_env_include_extra,
            exec_env_include_stale,
            DEFAULT_EXEC_ENV_HEADER_ALLOWLIST,
        )
        _print_path_allowlist_diff(
            "exec-env-singleton",
            exec_env_singleton_extra,
            exec_env_singleton_stale,
            DEFAULT_EXEC_ENV_SINGLETON_ALLOWLIST,
        )
        return 1

    checked = "all modules" if selected_modules is None else ", ".join(sorted(selected_modules))
    print(f"OK: BE module boundaries clean for {checked}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
