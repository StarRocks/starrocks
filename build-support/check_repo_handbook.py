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
    "handbook/domains/index.md",
    "handbook/domains/backend.md",
    "handbook/domains/frontend.md",
    "handbook/domains/sql-integration.md",
    "handbook/domains/docs-and-translation.md",
    "handbook/domains/ci-and-tooling.md",
    "handbook/domains/generated-and-extensions.md",
    "handbook/plans/index.md",
    "handbook/plans/completed/README.md",
    "handbook/plans/templates/execution-plan.md",
    "handbook/policies/index.md",
    "handbook/policies/reliability-first.md",
    "handbook/policies/internal-vs-public-docs.md",
    "handbook/policies/agent-change-flow.md",
    "handbook/quality/index.md",
    "handbook/quality/scorecard.md",
    "handbook/templates/domain-map.md",
    "handbook/templates/policy.md",
)
INDEX_LINK_REQUIREMENTS = {
    "handbook/index.md": (
        "handbook/architecture/index.md",
        "handbook/domains/index.md",
        "handbook/policies/index.md",
        "handbook/quality/index.md",
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
    "handbook/domains/index.md": (
        "handbook/domains/backend.md",
        "handbook/domains/frontend.md",
        "handbook/domains/sql-integration.md",
        "handbook/domains/docs-and-translation.md",
        "handbook/domains/ci-and-tooling.md",
        "handbook/domains/generated-and-extensions.md",
    ),
    "handbook/policies/index.md": (
        "handbook/policies/reliability-first.md",
        "handbook/policies/internal-vs-public-docs.md",
        "handbook/policies/agent-change-flow.md",
    ),
    "handbook/quality/index.md": ("handbook/quality/scorecard.md",),
}
ENTRYPOINT_LINK_REQUIREMENTS = {
    "AGENTS.md": ("handbook/index.md",),
    "CLAUDE.md": ("handbook/index.md",),
    "be/AGENTS.md": (
        "handbook/index.md",
        "handbook/domains/backend.md",
        "handbook/architecture/be-boundary-harness.md",
    ),
    "be/src/common/AGENTS.md": (
        "handbook/index.md",
        "handbook/domains/backend.md",
        "be/AGENTS.md",
    ),
    "docs/AGENTS.md": (
        "handbook/index.md",
        "handbook/domains/docs-and-translation.md",
    ),
    "fe/AGENTS.md": (
        "handbook/index.md",
        "handbook/domains/frontend.md",
    ),
    "gensrc/AGENTS.md": (
        "handbook/index.md",
        "handbook/domains/generated-and-extensions.md",
    ),
    "java-extensions/AGENTS.md": (
        "handbook/index.md",
        "handbook/domains/generated-and-extensions.md",
    ),
    "test/AGENTS.md": (
        "handbook/index.md",
        "handbook/domains/sql-integration.md",
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
DOMAIN_SECTION_REQUIREMENTS = (
    "## Purpose",
    "## Entrypoints",
    "## Commands",
    "## Guardrails",
    "## Test and Validation",
    "## Open Gaps",
)
POLICY_SECTION_REQUIREMENTS = (
    "## Intent",
    "## Applies To",
    "## Enforcement",
    "## Exceptions",
)
QUALITY_SCORECARD_REQUIREMENTS = (
    "## Rating Scale",
    "## Domain Scores",
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


def _check_required_headings(path: Path, requirements: tuple[str, ...], repo_root: Path) -> list[str]:
    text = path.read_text()
    errors: list[str] = []
    for requirement in requirements:
        if requirement not in text:
            errors.append(f"{_repo_relative(path, repo_root)} is missing required section heading: {requirement}")
    return errors


def _collect_unindexed_pages(
    index_path: Path,
    pages_dir: Path,
    section_heading: str,
    page_label: str,
    repo_root: Path,
) -> list[str]:
    errors: list[str] = []
    indexed_links = extract_section_links(index_path, section_heading, repo_root)
    pages = sorted(path for path in pages_dir.glob("*.md") if path.name != "index.md")
    if not indexed_links and pages:
        errors.append(f"{_repo_relative(index_path, repo_root)} must list {page_label} pages under ## {section_heading}")

    for page_path in pages:
        rel_path = _repo_relative(page_path, repo_root)
        if rel_path not in indexed_links:
            errors.append(f"Unindexed {page_label} page: {rel_path}")
    return errors


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

    domains_index_path = repo_root / "handbook" / "domains" / "index.md"
    if domains_index_path.exists():
        errors.extend(
            _collect_unindexed_pages(
                domains_index_path,
                repo_root / "handbook" / "domains",
                "Domains",
                "domain",
                repo_root,
            )
        )
        for domain_path in sorted((repo_root / "handbook" / "domains").glob("*.md")):
            if domain_path.name == "index.md":
                continue
            errors.extend(
                _check_required_headings(
                    domain_path,
                    DOMAIN_SECTION_REQUIREMENTS,
                    repo_root,
                )
            )

    policies_index_path = repo_root / "handbook" / "policies" / "index.md"
    if policies_index_path.exists():
        errors.extend(
            _collect_unindexed_pages(
                policies_index_path,
                repo_root / "handbook" / "policies",
                "Policies",
                "policy",
                repo_root,
            )
        )
        for policy_path in sorted((repo_root / "handbook" / "policies").glob("*.md")):
            if policy_path.name == "index.md":
                continue
            errors.extend(
                _check_required_headings(
                    policy_path,
                    POLICY_SECTION_REQUIREMENTS,
                    repo_root,
                )
            )

    scorecard_path = repo_root / "handbook" / "quality" / "scorecard.md"
    if scorecard_path.exists():
        errors.extend(
            _check_required_headings(
                scorecard_path,
                QUALITY_SCORECARD_REQUIREMENTS,
                repo_root,
            )
        )

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
