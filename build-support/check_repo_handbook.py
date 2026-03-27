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

import re
import sys
from pathlib import Path


REQUIRED_FILES = (
    "handbook/index.md",
    "handbook/architecture/index.md",
    "handbook/architecture/repo-topology.md",
    "handbook/architecture/be-boundary-harness.md",
    "handbook/plans/index.md",
    "handbook/plans/completed/README.md",
    "handbook/plans/templates/execution-plan.md",
)
INDEX_LINK_REQUIREMENTS = {
    "handbook/index.md": (
        "handbook/architecture/index.md",
        "handbook/plans/index.md",
    ),
    "handbook/architecture/index.md": (
        "handbook/architecture/repo-topology.md",
        "handbook/architecture/be-boundary-harness.md",
    ),
    "handbook/plans/index.md": (
        "handbook/plans/templates/execution-plan.md",
        "handbook/plans/completed/README.md",
    ),
}
ENTRYPOINT_LINK_REQUIREMENTS = {
    "AGENTS.md": ("handbook/index.md",),
    "CLAUDE.md": ("handbook/index.md",),
    "be/AGENTS.md": (
        "handbook/index.md",
        "handbook/architecture/be-boundary-harness.md",
    ),
}
PLAN_METADATA_REQUIREMENTS = (
    "- Status:",
    "- Owner:",
    "- Last Updated:",
    "## Summary",
    "## Acceptance Criteria",
    "## Decision Log",
)
LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def _repo_relative(path: Path, repo_root: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def _resolve_link(source_path: Path, link_target: str, repo_root: Path) -> str | None:
    target = link_target.strip()
    if not target or target.startswith(("http://", "https://", "mailto:", "#")):
        return None
    target = target.split("#", 1)[0].strip()
    if not target:
        return None
    resolved = (source_path.parent / target).resolve(strict=False)
    try:
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return None


def extract_links(path: Path, repo_root: Path) -> set[str]:
    links = set()
    for raw_target in LINK_PATTERN.findall(path.read_text()):
        resolved = _resolve_link(path, raw_target, repo_root)
        if resolved is not None:
            links.add(resolved)
    return links


def extract_section_links(path: Path, heading: str, repo_root: Path) -> set[str]:
    lines = path.read_text().splitlines()
    section_lines: list[str] = []
    in_section = False
    target_heading = f"## {heading}"
    for line in lines:
        if line.startswith("## "):
            if line.strip() == target_heading:
                in_section = True
                continue
            if in_section:
                break
        if in_section:
            section_lines.append(line)

    if not in_section:
        return set()

    section_text = "\n".join(section_lines)
    links = set()
    for raw_target in LINK_PATTERN.findall(section_text):
        resolved = _resolve_link(path, raw_target, repo_root)
        if resolved is not None:
            links.add(resolved)
    return links


def collect_errors(repo_root: Path) -> list[str]:
    repo_root = repo_root.resolve()
    errors: list[str] = []

    for required in REQUIRED_FILES:
        if not (repo_root / required).exists():
            errors.append(f"Missing required handbook file: {required}")

    for source, required_targets in ENTRYPOINT_LINK_REQUIREMENTS.items():
        source_path = repo_root / source
        if not source_path.exists():
            errors.append(f"Missing required entrypoint: {source}")
            continue
        links = extract_links(source_path, repo_root)
        for target in required_targets:
            if target not in links:
                errors.append(f"{source} must link to {target}")

    for source, required_targets in INDEX_LINK_REQUIREMENTS.items():
        source_path = repo_root / source
        if not source_path.exists():
            continue
        links = extract_links(source_path, repo_root)
        for target in required_targets:
            if target not in links:
                errors.append(f"{source} must link to {target}")

    plans_index_path = repo_root / "handbook" / "plans" / "index.md"
    if plans_index_path.exists():
        active_links = extract_section_links(plans_index_path, "Active Plans", repo_root)
        template_links = extract_section_links(plans_index_path, "Templates", repo_root)
        completed_links = extract_section_links(plans_index_path, "Completed Plans", repo_root)
        if not active_links and any((repo_root / "handbook" / "plans" / "active").glob("*.md")):
            errors.append("handbook/plans/index.md must list active plans under ## Active Plans")
        if "handbook/plans/templates/execution-plan.md" not in template_links:
            errors.append("handbook/plans/index.md must link to handbook/plans/templates/execution-plan.md under ## Templates")
        if "handbook/plans/completed/README.md" not in completed_links:
            errors.append("handbook/plans/index.md must link to handbook/plans/completed/README.md under ## Completed Plans")

        active_dir = repo_root / "handbook" / "plans" / "active"
        for plan_path in sorted(active_dir.glob("*.md")):
            rel_path = _repo_relative(plan_path, repo_root)
            if rel_path not in active_links:
                errors.append(f"Unindexed active plan: {rel_path}")
            plan_text = plan_path.read_text()
            for requirement in PLAN_METADATA_REQUIREMENTS:
                if requirement not in plan_text:
                    errors.append(f"{rel_path} is missing required plan metadata or section: {requirement}")

        completed_dir = repo_root / "handbook" / "plans" / "completed"
        for completed_path in sorted(completed_dir.glob("*.md")):
            if completed_path.name == "README.md":
                continue
            rel_path = _repo_relative(completed_path, repo_root)
            if rel_path in active_links:
                errors.append(f"Completed plan must not appear in Active Plans: {rel_path}")
            if rel_path not in completed_links:
                errors.append(f"Completed plan is not indexed under ## Completed Plans: {rel_path}")

    return errors


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    errors = collect_errors(repo_root)
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1
    print("OK: repo handbook structure and entrypoints are in sync.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
