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
import posixpath
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path


LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
LOCAL_INDEX_HEADER = """# Local Handbook Plans

Use this directory for machine-local execution plans that should stay out of git. Agents should read this index after `handbook/plans/index.md` when it exists.

## Active Plans
"""


@dataclass(frozen=True)
class PlanRecord:
    title: str
    rel_path: str
    absolute_path: Path
    overrides: str | None = None
    source: str = "tracked"


def repo_root_from_script(script_path: Path) -> Path:
    return script_path.resolve().parent.parent


def tracked_index_path(repo_root: Path) -> Path:
    return repo_root / "handbook" / "plans" / "index.md"


def local_root_path(repo_root: Path) -> Path:
    return repo_root / "handbook" / "plans" / "local"


def local_index_path(repo_root: Path) -> Path:
    return local_root_path(repo_root) / "index.md"


def local_active_dir(repo_root: Path) -> Path:
    return local_root_path(repo_root) / "active"


def template_path(repo_root: Path) -> Path:
    return repo_root / "handbook" / "plans" / "templates" / "execution-plan.md"


def rel_path(path: Path, repo_root: Path) -> str:
    return path.relative_to(repo_root).as_posix()


def resolve_link(source_path: Path, raw_target: str, repo_root: Path) -> str | None:
    target = raw_target.strip().split("#", 1)[0].strip()
    if not target or target.startswith(("http://", "https://", "mailto:", "#")):
        return None
    resolved = (source_path.parent / target).resolve(strict=False)
    try:
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return None


def extract_section_links(index_path: Path, heading: str, repo_root: Path) -> list[str]:
    lines = index_path.read_text().splitlines()
    in_section = False
    links: list[str] = []
    for line in lines:
        if line.startswith("## "):
            if line.strip() == f"## {heading}":
                in_section = True
                continue
            if in_section:
                break
        if not in_section:
            continue
        for raw_target in LINK_PATTERN.findall(line):
            resolved = resolve_link(index_path, raw_target, repo_root)
            if resolved is not None:
                links.append(resolved)
    return links


def parse_title(plan_path: Path) -> str:
    for line in plan_path.read_text().splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    raise ValueError(f"{plan_path} is missing a top-level title")


def parse_metadata(plan_path: Path) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for line in plan_path.read_text().splitlines():
        if line.startswith("## "):
            break
        if line.startswith("- ") and ":" in line:
            key, value = line[2:].split(":", 1)
            metadata[key.strip()] = value.strip()
    return metadata


def extract_sections(markdown_text: str) -> str:
    for marker in ("\n## Summary", "\n## "):
        idx = markdown_text.find(marker)
        if idx != -1:
            return markdown_text[idx + 1 :].strip()
    raise ValueError("plan body is missing markdown sections")


def slugify(title: str) -> str:
    normalized = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", normalized.lower()).strip("-")
    if not slug:
        raise ValueError(f"unable to derive a filename slug from title: {title!r}")
    return slug


def today_string() -> str:
    return date.today().isoformat()


def render_plan(title: str, owner: str, sections: str, overrides: str | None) -> str:
    lines = [
        f"# {title}",
        "",
        "- Status: active",
        f"- Owner: {owner}",
        f"- Last Updated: {today_string()}",
    ]
    if overrides is not None:
        lines.append(f"- Overrides: {overrides}")
    lines.extend(["", sections.strip(), ""])
    return "\n".join(lines)


def ensure_local_tree(repo_root: Path) -> None:
    local_active_dir(repo_root).mkdir(parents=True, exist_ok=True)
    if not local_index_path(repo_root).exists():
        local_index_path(repo_root).write_text(f"{LOCAL_INDEX_HEADER}\n")


def collect_tracked_plans(repo_root: Path) -> list[PlanRecord]:
    plans: list[PlanRecord] = []
    index_path = tracked_index_path(repo_root)
    for rel_target in extract_section_links(index_path, "Active Plans", repo_root):
        absolute_path = repo_root / rel_target
        plans.append(
            PlanRecord(
                title=parse_title(absolute_path),
                rel_path=rel_target,
                absolute_path=absolute_path,
                overrides=None,
                source="tracked",
            )
        )
    return plans


def collect_local_plans(repo_root: Path) -> list[PlanRecord]:
    active_dir = local_active_dir(repo_root)
    if not active_dir.exists():
        return []
    plans: list[PlanRecord] = []
    for absolute_path in sorted(active_dir.glob("*.md")):
        metadata = parse_metadata(absolute_path)
        plans.append(
            PlanRecord(
                title=parse_title(absolute_path),
                rel_path=rel_path(absolute_path, repo_root),
                absolute_path=absolute_path,
                overrides=metadata.get("Overrides"),
                source="local",
            )
        )
    return plans


def validate_override_collisions(local_plans: list[PlanRecord]) -> None:
    seen: dict[str, str] = {}
    for plan in local_plans:
        if plan.overrides is None:
            continue
        existing = seen.get(plan.overrides)
        if existing is not None:
            raise ValueError(
                f"multiple local plans override {plan.overrides}: {existing} and {plan.rel_path}"
            )
        seen[plan.overrides] = plan.rel_path


def rewrite_local_index(repo_root: Path) -> None:
    ensure_local_tree(repo_root)
    entries = collect_local_plans(repo_root)
    lines = [LOCAL_INDEX_HEADER.rstrip(), ""]
    for plan in entries:
        link_target = plan.absolute_path.relative_to(local_root_path(repo_root)).as_posix()
        lines.append(f"- [{plan.title}]({link_target})")
    lines.append("")
    local_index_path(repo_root).write_text("\n".join(lines))


def next_local_plan_path(repo_root: Path, title: str) -> Path:
    base_name = f"{today_string()}-{slugify(title)}"
    candidate = local_active_dir(repo_root) / f"{base_name}.md"
    suffix = 2
    while candidate.exists():
        candidate = local_active_dir(repo_root) / f"{base_name}-{suffix}.md"
        suffix += 1
    return candidate


def render_template_plan(repo_root: Path, title: str, owner: str) -> str:
    sections = extract_sections(template_path(repo_root).read_text())
    return render_plan(title=title, owner=owner, sections=sections, overrides=None)


def render_local_override(repo_root: Path, source_rel_path: str, title: str | None, owner: str | None) -> str:
    source_path = (repo_root / source_rel_path).resolve()
    if not source_path.exists():
        raise ValueError(f"tracked plan does not exist: {source_rel_path}")
    metadata = parse_metadata(source_path)
    title = title or parse_title(source_path)
    owner = owner or metadata.get("Owner")
    if owner is None:
        raise ValueError(f"tracked plan is missing Owner metadata: {source_rel_path}")
    sections = extract_sections(source_path.read_text())
    return render_plan(title=title, owner=owner, sections=sections, overrides=source_rel_path)


def canonical_tracked_plan_path(repo_root: Path, source_rel_path: str) -> str:
    source_path = (repo_root / source_rel_path.strip()).resolve(strict=False)
    try:
        canonical_rel_path = source_path.relative_to(repo_root.resolve()).as_posix()
    except ValueError as err:
        raise ValueError("--from must point to handbook/plans/active/<plan>.md") from err
    if not canonical_rel_path.startswith("handbook/plans/active/"):
        raise ValueError("--from must point to handbook/plans/active/<plan>.md")
    return canonical_rel_path


def create_local_plan(repo_root: Path, title: str | None, owner: str | None, source_rel_path: str | None) -> Path:
    ensure_local_tree(repo_root)
    existing_local = collect_local_plans(repo_root)
    validate_override_collisions(existing_local)
    if source_rel_path is not None:
        source_rel_path = canonical_tracked_plan_path(repo_root, source_rel_path)
        if any(plan.overrides == source_rel_path for plan in existing_local):
            raise ValueError(f"a local override already exists for {source_rel_path}")
        content = render_local_override(repo_root, source_rel_path, title=title, owner=owner)
        title = title or parse_title(repo_root / source_rel_path)
    else:
        if not title or not owner:
            raise ValueError("--title and --owner are required when --from is not provided")
        content = render_template_plan(repo_root, title=title, owner=owner)

    destination = next_local_plan_path(repo_root, title)
    destination.write_text(content)
    rewrite_local_index(repo_root)
    return destination


def resolve_local_plan(repo_root: Path, plan_ref: str) -> Path:
    active_dir = local_active_dir(repo_root)
    if not active_dir.exists():
        raise ValueError("no local handbook plans exist")

    candidates = sorted(active_dir.glob("*.md"))
    if not candidates:
        raise ValueError("no local handbook plans exist")

    requested_ref = plan_ref.strip()
    normalized_ref = posixpath.normpath(requested_ref) if requested_ref else requested_ref
    matches = [
        candidate
        for candidate in candidates
        if normalized_ref
        in {
            candidate.name,
            candidate.stem,
            candidate.relative_to(active_dir).as_posix(),
            candidate.relative_to(local_root_path(repo_root)).as_posix(),
            rel_path(candidate, repo_root),
        }
    ]
    if not matches:
        raise ValueError(f"no local handbook plan matched {plan_ref!r}")
    if len(matches) > 1:
        joined = ", ".join(rel_path(match, repo_root) for match in matches)
        raise ValueError(f"ambiguous local plan reference {plan_ref!r}: {joined}")
    return matches[0]


def complete_local_plan(repo_root: Path, plan_ref: str) -> Path:
    plan_path = resolve_local_plan(repo_root, plan_ref)
    plan_path.unlink()
    rewrite_local_index(repo_root)
    return plan_path


def effective_active_plans(repo_root: Path) -> list[PlanRecord]:
    tracked = collect_tracked_plans(repo_root)
    local = collect_local_plans(repo_root)
    validate_override_collisions(local)

    tracked_paths = {plan.rel_path for plan in tracked}
    overrides = {plan.overrides: plan for plan in local if plan.overrides is not None}
    seen_local: set[str] = set()
    effective: list[PlanRecord] = []

    for tracked_plan in tracked:
        replacement = overrides.get(tracked_plan.rel_path)
        if replacement is None:
            effective.append(tracked_plan)
            continue
        effective.append(replacement)
        seen_local.add(replacement.rel_path)

    for local_plan in local:
        if local_plan.rel_path in seen_local:
            continue
        if local_plan.overrides is not None and local_plan.overrides in tracked_paths:
            continue
        effective.append(local_plan)
    return effective


def print_effective_plans(repo_root: Path) -> None:
    print("Effective active plans:")
    for plan in effective_active_plans(repo_root):
        if plan.source == "local" and plan.overrides is not None:
            print(f"- {plan.rel_path} (local override for {plan.overrides})")
        elif plan.source == "local":
            print(f"- {plan.rel_path} (local)")
        else:
            print(f"- {plan.rel_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage local handbook plans.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List effective tracked and local active plans.")

    create_parser = subparsers.add_parser("create", help="Create a local handbook plan.")
    create_parser.add_argument("--local", action="store_true", help="Create a gitignored local plan.")
    create_parser.add_argument("--title", help="Plan title.")
    create_parser.add_argument("--owner", help="Plan owner.")
    create_parser.add_argument("--from", dest="source", help="Tracked plan to fork into a local override.")

    complete_parser = subparsers.add_parser("complete", help="Delete a finished local handbook plan.")
    complete_parser.add_argument("--local", action="store_true", help="Complete a gitignored local plan.")
    complete_parser.add_argument("--plan", required=True, help="Local plan path, filename, or slug.")
    return parser


def main(argv: list[str] | None = None, repo_root: Path | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = repo_root or repo_root_from_script(Path(__file__))

    try:
        if args.command == "list":
            print_effective_plans(repo_root)
            return 0

        if not args.local:
            parser.error("only --local is supported")

        if args.command == "create":
            created_path = create_local_plan(
                repo_root=repo_root,
                title=args.title,
                owner=args.owner,
                source_rel_path=args.source,
            )
            print(rel_path(created_path, repo_root))
            return 0

        if args.command == "complete":
            removed_path = complete_local_plan(repo_root=repo_root, plan_ref=args.plan)
            print(rel_path(removed_path, repo_root))
            return 0
    except ValueError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 1

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
