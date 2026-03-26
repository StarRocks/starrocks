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
import sys
from pathlib import Path


BEGIN_MARKER = "<!-- BEGIN GENERATED: BE MODULE HARNESSES -->"
END_MARKER = "<!-- END GENERATED: BE MODULE HARNESSES -->"
DEFAULT_MANIFEST = "be/module_boundary_manifest.json"
DEFAULT_AGENTS = "be/AGENTS.md"


class GeneratedSectionMismatchError(RuntimeError):
    pass


def render_module_boundaries_section(manifest: dict) -> str:
    lines = [
        "## Module Harness",
        "",
        "This section is generated from `be/module_boundary_manifest.json`.",
        "Run `python3 build-support/render_be_agents.py --write` after changing the manifest.",
        "Run `python3 build-support/check_be_module_boundaries.py --mode full` to validate the same rules mechanically.",
        "",
    ]
    for module in manifest["modules"]:
        lines.append(f"### {module['doc_label']} (`{module['id']}`)")
        if module.get("summary"):
            lines.append(module["summary"])
        if module.get("owned_targets"):
            lines.append(f"- Targets: {', '.join(f'`{target}`' for target in module['owned_targets'])}")
        if module.get("allowed_include_prefixes"):
            lines.append(
                "- Allowed internal include prefixes: "
                + ", ".join(f"`{prefix}`" for prefix in module["allowed_include_prefixes"])
            )
        if module.get("allowed_target_deps"):
            lines.append("- Allowed target deps: " + ", ".join(f"`{dep}`" for dep in module["allowed_target_deps"]))
        if module.get("allowed_test_targets"):
            lines.append("- Core tests: " + ", ".join(f"`{target}`" for target in module["allowed_test_targets"]))
        lines.append(f"- Remediation: {module['remediation']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def replace_generated_section(content: str, rendered_section: str) -> str:
    replacement = f"{BEGIN_MARKER}\n{rendered_section}{END_MARKER}"
    if BEGIN_MARKER in content and END_MARKER in content:
        start = content.index(BEGIN_MARKER)
        end = content.index(END_MARKER) + len(END_MARKER)
        return content[:start] + replacement + content[end:]
    trimmed = content.rstrip()
    if trimmed:
        return trimmed + "\n\n" + replacement + "\n"
    return replacement + "\n"


def check_agents_file(agents_path: Path, manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_text())
    rendered_section = render_module_boundaries_section(manifest)
    current = agents_path.read_text()
    expected = replace_generated_section(current, rendered_section)
    if current != expected:
        raise GeneratedSectionMismatchError(
            f"{agents_path} is out of date with {manifest_path}; run python3 build-support/render_be_agents.py --write"
        )


def write_agents_file(agents_path: Path, manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_text())
    rendered_section = render_module_boundaries_section(manifest)
    current = agents_path.read_text()
    updated = replace_generated_section(current, rendered_section)
    if current != updated:
        agents_path.write_text(updated)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render the generated BE module section inside be/AGENTS.md.")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST, help="Path to be/module_boundary_manifest.json")
    parser.add_argument("--agents", default=DEFAULT_AGENTS, help="Path to be/AGENTS.md")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="Fail if the generated section is stale")
    mode.add_argument("--write", action="store_true", help="Rewrite the generated section in place")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parent.parent
    agents_path = repo_root / args.agents
    manifest_path = repo_root / args.manifest

    try:
        if args.check:
            check_agents_file(agents_path, manifest_path)
            print(f"OK: {agents_path} matches {manifest_path}.")
        else:
            write_agents_file(agents_path, manifest_path)
            print(f"Updated {agents_path} from {manifest_path}.")
    except GeneratedSectionMismatchError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
