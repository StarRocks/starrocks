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

"""
Compare StarRocks config parameters and session variables against documentation.

Parses three source-of-truth files and compares their parameters against the
hand-written Markdown documentation to find undocumented (missing) and removed
(stale) entries.

Usage:
  # Full gap report — all undocumented params across all categories
  python3 build-support/extract_and_diff_params.py

  # Only report params added in the current git branch (for CI on PRs)
  python3 build-support/extract_and_diff_params.py --new-params-only

  # JSON output (used by ci-doc-needs-update.yml to build the issue body)
  python3 build-support/extract_and_diff_params.py --output json

  # GitHub-comment Markdown (used by ci-doc-param-drift.yml to post a PR comment)
  python3 build-support/extract_and_diff_params.py --new-params-only --output github-comment
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# ── Source-of-truth paths ────────────────────────────────────────────────────

FE_CONFIG_PATH = (
    REPO_ROOT / "fe/fe-core/src/main/java/com/starrocks/common/Config.java"
)
SESSION_VAR_PATH = (
    REPO_ROOT / "fe/fe-core/src/main/java/com/starrocks/qe/SessionVariable.java"
)
GLOBAL_VAR_PATH = (
    REPO_ROOT / "fe/fe-core/src/main/java/com/starrocks/qe/GlobalVariable.java"
)
BE_CONFIG_PATH = REPO_ROOT / "be/src/common/config.h"

# ── Documentation paths ──────────────────────────────────────────────────────

FE_PARAM_DOCS = [
    REPO_ROOT / "docs/en/administration/management/FE_parameters/log_server_meta.md",
    REPO_ROOT / "docs/en/administration/management/FE_parameters/user_query_loading.md",
    REPO_ROOT / "docs/en/administration/management/FE_parameters/stats_storage.md",
    REPO_ROOT / "docs/en/administration/management/FE_parameters/shared_lake_other.md",
]
BE_PARAM_DOCS = [
    REPO_ROOT / "docs/en/administration/management/BE_parameters/log_server_meta.md",
    REPO_ROOT / "docs/en/administration/management/BE_parameters/query_loading.md",
    REPO_ROOT / "docs/en/administration/management/BE_parameters/stats_storage.md",
    REPO_ROOT / "docs/en/administration/management/BE_parameters/shared_lake_other.md",
]
SESSION_VAR_DOCS = [
    REPO_ROOT / "docs/en/sql-reference/System_variable.md",
]

ALL_SOURCE_PATHS = [FE_CONFIG_PATH, BE_CONFIG_PATH, SESSION_VAR_PATH, GLOBAL_VAR_PATH]


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass
class ParamInfo:
    name: str
    param_type: str       # 'fe_config' | 'be_config' | 'session_var'
    code_type: str = ""   # Java type or C++ macro type suffix
    default: str = ""
    mutable: bool = False
    description: str = ""
    suggested_doc: str = ""


# ── Comment extraction helpers ───────────────────────────────────────────────

def _clean_block_comment(lines: list[str]) -> str:
    """Strip /** ... */ decoration and return clean prose."""
    cleaned = []
    for line in lines:
        line = re.sub(r"^\s*/\*+\s*", "", line)   # /** or /*
        line = re.sub(r"\s*\*+/\s*$", "", line)   # */
        line = re.sub(r"^\s*\*\s?", "", line)      # leading * on body lines
        line = line.strip()
        if line and not line.startswith("@"):       # skip Javadoc tags
            cleaned.append(line)
    text = " ".join(cleaned)
    # Collapse runs of whitespace
    return re.sub(r"\s{2,}", " ", text).strip()


def _clean_line_comments(lines: list[str]) -> str:
    """Strip // prefix and return clean prose."""
    cleaned = []
    for line in lines:
        line = re.sub(r"^\s*//+\s*", "", line).strip()
        if line:
            cleaned.append(line)
    return " ".join(cleaned)


def _preceding_comment(lines: list[str], annotation_idx: int) -> str:
    """
    Walk backwards from a @ConfField or CONF_* annotation line and return
    any Javadoc block (/** ... */) or run of // line comments that sit
    directly above it (ignoring blank lines and @Deprecated).
    """
    j = annotation_idx - 1
    while j >= 0 and lines[j].strip() in ("", "@Deprecated"):
        j -= 1
    if j < 0:
        return ""

    prev = lines[j].strip()

    if prev.endswith("*/"):
        # Collect the full block comment walking backwards
        block: list[str] = []
        while j >= 0:
            block.insert(0, lines[j].strip())
            if lines[j].strip().startswith("/*"):
                break
            j -= 1
        return _clean_block_comment(block)

    if prev.startswith("//"):
        # Collect consecutive // lines walking backwards
        block = []
        while j >= 0 and lines[j].strip().startswith("//"):
            block.insert(0, lines[j].strip())
            j -= 1
        return _clean_line_comments(block)

    return ""


# ── AI description generation ─────────────────────────────────────────────────

_AI_SYSTEM_PROMPT = """\
You write technical reference documentation for StarRocks, an open-source \
analytical database. Given a configuration parameter or session variable, its \
metadata, and code snippets showing where it is used, write a single concise \
description sentence.

Rules:
- One sentence only, under 160 characters.
- Start with a verb: Controls, Specifies, Sets, Enables, Defines, Limits, \
Determines.
- Describe what the parameter controls or represents — not what value to set.
- Do not restate the parameter name, type, or default value.
- Base the description only on the context provided. Do not speculate.
- If the context is insufficient for an accurate description, respond with \
exactly the word: INSUFFICIENT_CONTEXT\
"""


def _gather_code_context(name: str, param_type: str) -> str:
    """Grep for the top usages of a parameter in the relevant source tree."""
    if param_type == "fe_config":
        pattern = f"Config\\.{name}"
        paths = [str(REPO_ROOT / "fe/fe-core/src/main/java")]
        includes = ["*.java"]
    elif param_type == "be_config":
        pattern = name
        paths = [str(REPO_ROOT / "be/src")]
        includes = ["*.cpp", "*.h"]
    else:
        pattern = f'"{name}"'
        paths = [str(REPO_ROOT / "fe/fe-core/src/main/java")]
        includes = ["*.java"]

    snippets: list[str] = []
    for include in includes:
        try:
            r = subprocess.run(
                ["grep", "-rn", "--include", include, "-A", "3", "-B", "1", pattern]
                + paths,
                capture_output=True,
                text=True,
                cwd=REPO_ROOT,
                timeout=10,
            )
            if r.stdout.strip():
                snippets.append(r.stdout.strip()[:3000])
        except subprocess.SubprocessError:
            pass

    return "\n---\n".join(snippets[:3])


def generate_ai_descriptions(
    params: list[ParamInfo],
    api_key: str,
    model: str = "claude-haiku-4-5-20251001",
) -> dict[str, str]:
    """
    Call the Anthropic API to generate one-sentence descriptions for params
    that currently have no description. Returns {name: description}.

    Requires the 'anthropic' package: pip install anthropic
    """
    try:
        import anthropic  # type: ignore
    except ImportError:
        print(
            "error: 'anthropic' package not installed. Run: pip install anthropic",
            file=sys.stderr,
        )
        return {}

    client = anthropic.Anthropic(api_key=api_key)
    results: dict[str, str] = {}

    for idx, param in enumerate(params, 1):
        print(
            f"  [{idx}/{len(params)}] {param.name} ...",
            file=sys.stderr,
            end="",
            flush=True,
        )
        context = _gather_code_context(param.name, param.param_type)
        category = {
            "fe_config": "FE Configuration (Config.java)",
            "be_config": "BE Configuration (config.h)",
            "session_var": "Session / Global Variable",
        }[param.param_type]

        user_msg = (
            f"Parameter: {param.name}\n"
            f"Category: {category}\n"
            f"Type: {param.code_type}\n"
            f"Default: {param.default}\n"
            f"Mutable: {param.mutable}\n\n"
            f"Code context:\n"
            f"{context if context else '(no usages found in source tree)'}\n\n"
            "Write a one-sentence description for this parameter."
        )

        try:
            resp = client.messages.create(
                model=model,
                max_tokens=200,
                system=[
                    {
                        "type": "text",
                        "text": _AI_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_msg}],
            )
            desc = resp.content[0].text.strip()
            if desc == "INSUFFICIENT_CONTEXT":
                print(" (insufficient context)", file=sys.stderr)
            else:
                results[param.name] = desc
                print(f" ✓", file=sys.stderr)
        except Exception as exc:
            print(f" error: {exc}", file=sys.stderr)

    return results


# ── FE Config.java parser ────────────────────────────────────────────────────

_FIELD_RE = re.compile(
    r"public\s+static\s+(?:final\s+)?(\S+(?:\[\])?)\s+([a-z_][a-z0-9_]*)\s*="
)


def parse_fe_configs(path: Path) -> dict[str, ParamInfo]:
    """
    Extract @ConfField-annotated parameters from Config.java.

    Handles:
    - @ConfField with no args (immutable)
    - @ConfField(mutable = true, comment = "...")
    - @Deprecated @ConfField (excluded from output)
    - Multi-line field initializers
    """
    params: dict[str, ParamInfo] = {}
    lines = path.read_text().splitlines()
    n = len(lines)
    i = 0

    while i < n:
        stripped = lines[i].strip()

        if not stripped.startswith("@ConfField"):
            i += 1
            continue

        # Check if @Deprecated appears on the immediately preceding non-blank line
        prev = i - 1
        while prev >= 0 and not lines[prev].strip():
            prev -= 1
        deprecated = prev >= 0 and lines[prev].strip() == "@Deprecated"

        # Collect full annotation (may span lines when it has arguments)
        ann_text = stripped
        while ann_text.count("(") > ann_text.count(")"):
            i += 1
            if i >= n:
                break
            ann_text += " " + lines[i].strip()

        # Advance past any other annotations between @ConfField and the field declaration
        i += 1
        while i < n and lines[i].strip().startswith("@"):
            i += 1

        if i >= n:
            break

        # Collect field declaration (may span lines when the initializer is complex)
        field_text = lines[i].strip()
        while "public static" in field_text and ";" not in field_text:
            i += 1
            if i >= n:
                break
            field_text += " " + lines[i].strip()

        if "public static" not in field_text:
            i += 1
            continue

        m = _FIELD_RE.search(field_text)
        if m and not deprecated:
            code_type = m.group(1)
            name = m.group(2)

            # Simplified default extraction: everything between '=' and first ';'
            default_m = re.search(r"=\s*(.+?)(?:\s*//.*)?;", field_text)
            default = default_m.group(1).strip() if default_m else ""

            comment_m = re.search(r'comment\s*=\s*"([^"]*)"', ann_text)
            if comment_m:
                description = comment_m.group(1)
            else:
                description = _preceding_comment(lines, i - 1)

            params[name] = ParamInfo(
                name=name,
                param_type="fe_config",
                code_type=code_type,
                default=default,
                mutable=bool(re.search(r"mutable\s*=\s*true", ann_text)),
                description=description,
                suggested_doc="docs/en/administration/management/FE_parameters/",
            )

        i += 1

    return params


# ── BE config.h parser ───────────────────────────────────────────────────────

_CONF_MACRO_RE = re.compile(
    r'^\s*CONF_(m?)([A-Za-z0-9]+)\(\s*([a-z_][a-z0-9_]+)\s*,\s*"([^"]*)"\s*\)\s*;'
)
_INCLUDE_RE = re.compile(r'^\s*#include\s+"(config_[^"]+\.h)"\s*$')


def _be_config_files(path: Path) -> list[Path]:
    """Return config.h plus any local config_*.h includes (non-fwd)."""
    files = [path]
    for line in path.read_text().splitlines():
        m = _INCLUDE_RE.match(line)
        if m and not m.group(1).endswith("_fwd.h"):
            candidate = path.parent / m.group(1)
            if candidate.exists():
                files.append(candidate)
    return files


def parse_be_configs(path: Path) -> dict[str, ParamInfo]:
    """
    Extract CONF_* macro parameters from config.h and its local includes.

    The 'm' prefix on a macro (e.g. CONF_mBool vs CONF_Bool) indicates mutability.
    """
    params: dict[str, ParamInfo] = {}
    for scan_path in _be_config_files(path):
        lines = scan_path.read_text().splitlines()
        for idx, line in enumerate(lines):
            m = _CONF_MACRO_RE.match(line)
            if m:
                mutable_flag, code_type, name, default = m.groups()
                params[name] = ParamInfo(
                    name=name,
                    param_type="be_config",
                    code_type=code_type,
                    default=default,
                    mutable=bool(mutable_flag),
                    description=_preceding_comment(lines, idx),
                    suggested_doc="docs/en/administration/management/BE_parameters/",
                )
    return params


# ── SessionVariable / GlobalVariable parser ──────────────────────────────────

# Matches single-line:  public static final String FOO = "actual_name";
_CONST_RE = re.compile(
    r'public\s+static\s+final\s+String\s+(\w+)\s*=\s*"([a-z][a-z0-9_]*)"'
)
# Matches the opening of a two-line constant: public static final String FOO =
_CONST_MULTILINE_START_RE = re.compile(
    r'public\s+static\s+final\s+String\s+(\w+)\s*=\s*$'
)
# Matches the continuation line:     "actual_name";
_CONST_MULTILINE_VALUE_RE = re.compile(r'^\s*"([a-z][a-z0-9_]*)"\s*;')
_VARATTR_START_RE = re.compile(r'@(?:VarAttr|VariableMgr\.VarAttr)\s*\(')
_FIELD_DECL_RE = re.compile(
    r'(?:private|public|protected)\s+(?:static\s+)?(\w+(?:\[\])?)\s+\w+\s*=\s*(.+?)(?:;|$)'
)


def parse_session_vars(session_path: Path, global_path: Path) -> dict[str, ParamInfo]:
    """
    Extract annotated session and global variables from SessionVariable.java
    and GlobalVariable.java.

    Two-pass approach:
      1. Build a dict of String constants (SCREAMING_SNAKE → 'actual_name').
      2. For each @VarAttr annotation, resolve the name constant and record metadata.

    Variables with flag = VariableMgr.INVISIBLE are internal and excluded.
    """
    params: dict[str, ParamInfo] = {}

    for src_path in [session_path, global_path]:
        lines = src_path.read_text().splitlines()
        n = len(lines)

        # Pass 1: String constants (handles both single-line and two-line definitions)
        const_map: dict[str, str] = {}
        for idx, line in enumerate(lines):
            m = _CONST_RE.search(line)
            if m:
                const_map[m.group(1)] = m.group(2)
                continue
            m = _CONST_MULTILINE_START_RE.search(line.strip())
            if m and idx + 1 < n:
                vm = _CONST_MULTILINE_VALUE_RE.match(lines[idx + 1])
                if vm:
                    const_map[m.group(1)] = vm.group(1)

        # Pass 2: @VarAttr annotations
        i = 0
        while i < n:
            stripped = lines[i].strip()

            if not _VARATTR_START_RE.match(stripped):
                i += 1
                continue

            # Collect multi-line annotation
            ann_text = stripped
            while ann_text.count("(") > ann_text.count(")") and i + 1 < n:
                i += 1
                ann_text += " " + lines[i].strip()

            # Skip internal variables
            if "INVISIBLE" in ann_text:
                i += 1
                continue

            # Resolve name= (internal storage name)
            name_m = re.search(r'\bname\s*=\s*(\w+|"[^"]+")', ann_text)
            if not name_m:
                i += 1
                continue

            def _resolve(raw: str) -> str:
                if raw.startswith('"'):
                    return raw.strip('"')
                return const_map.get(raw, raw.lower())

            primary_name = _resolve(name_m.group(1))

            # Resolve show= (user-visible name used in SHOW VARIABLES and docs).
            # Many variables rename internals with a _v2 suffix while keeping the
            # original name as the show/alias value — docs always use the show name.
            show_m = re.search(r'\bshow\s*=\s*(\w+|"[^"]+")', ann_text)
            show_name = _resolve(show_m.group(1)) if show_m else None

            # Resolve alias= (backward-compat names, also user-visible)
            alias_names = [
                _resolve(m.group(1))
                for m in re.finditer(r'\balias\s*=\s*(\w+|"[^"]+")', ann_text)
            ]

            # Collect user-visible names for doc comparison.
            # When show= is present it is the name users see in SHOW VARIABLES and
            # docs — the primary name is an internal implementation detail (_v2 etc.)
            # and does not need its own doc entry.
            all_names: list[str] = []
            if show_name and re.fullmatch(r"[a-z_][a-z0-9_]*", show_name):
                all_names.append(show_name)
            elif re.fullmatch(r"[a-z_][a-z0-9_]*", primary_name):
                all_names.append(primary_name)
            for alias in alias_names:
                if re.fullmatch(r"[a-z_][a-z0-9_]*", alias) and alias not in all_names:
                    all_names.append(alias)

            if not all_names:
                i += 1
                continue

            # Find the field declaration on the following lines
            code_type = ""
            default = ""
            for j in range(i + 1, min(i + 6, n)):
                fd = _FIELD_DECL_RE.search(lines[j].strip())
                if fd:
                    code_type = fd.group(1)
                    default = fd.group(2).strip().rstrip(";")
                    break

            for var_name in all_names:
                params[var_name] = ParamInfo(
                    name=var_name,
                    param_type="session_var",
                    code_type=code_type,
                    default=default,
                    mutable=True,
                    description="",
                    suggested_doc="docs/en/sql-reference/System_variable.md",
                )
            i += 1

    return params


# ── Documentation parser ─────────────────────────────────────────────────────

# Matches:
#   ### `param_name`        (FE params — backtick-wrapped)
#   ### param_name          (BE params — plain)
#   ### param_name (global) (session vars — with scope qualifier)
_DOC_HEADING_RE = re.compile(r"^#{2,4}\s+[`']?([a-z_][a-z0-9_]*)[`']?")


def parse_doc_params(doc_paths: list[Path]) -> set[str]:
    """Extract documented parameter names from Markdown heading lines."""
    names: set[str] = set()
    for path in doc_paths:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            m = _DOC_HEADING_RE.match(line)
            if m:
                names.add(m.group(1).lower())
    return names


# ── Git diff filter ───────────────────────────────────────────────────────────

def get_new_param_names_from_diff(
    source_paths: list[Path], diff_base: str = "origin/main"
) -> set[str]:
    """
    Return parameter names that appear in the added lines (+) of
    'git diff <diff_base>' for the given source files.

    Used by --new-params-only to restrict the report to params
    introduced in the current branch, avoiding noise from the pre-existing backlog.
    Pass --diff-base to override the comparison ref (e.g. origin/branch-4.1 for
    release-branch PRs, or HEAD^1 when running against a merge commit).
    """
    try:
        result = subprocess.run(
            ["git", "diff", diff_base, "--"] + [str(p) for p in source_paths],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=30,
        )
    except (subprocess.SubprocessError, FileNotFoundError):
        return set()

    names: set[str] = set()
    for line in result.stdout.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        # Java: public static [final] Type field_name =
        for m in re.finditer(
            r"public\s+static\s+(?:final\s+)?\S+\s+([a-z_][a-z0-9_]+)\s*=", line
        ):
            names.add(m.group(1))
        # Java: single-line constant  = "actual_var_name"
        for m in re.finditer(r'=\s*"([a-z_][a-z0-9_]+)"', line):
            names.add(m.group(1))
        # Java: two-line constant continuation — the added line is just "name";
        # e.g. +            "enable_optimize_skew_join_v1";
        for m in re.finditer(r'^\s*"([a-z_][a-z0-9_]+)"\s*;', line[1:]):
            names.add(m.group(1))
        # C++ CONF macro first argument
        for m in re.finditer(
            r"CONF_m?[A-Za-z0-9_]+\(\s*([a-z_][a-z0-9_]+)\s*,", line
        ):
            names.add(m.group(1))
    return names


# ── Output formatters ─────────────────────────────────────────────────────────

def _fmt_text(
    fe_missing: list[ParamInfo],
    fe_stale: set[str],
    be_missing: list[ParamInfo],
    be_stale: set[str],
    sv_missing: list[ParamInfo],
    sv_stale: set[str],
    new_params_only: bool,
) -> str:
    out: list[str] = []

    def section(title: str, missing: list[ParamInfo], stale: set[str]) -> None:
        out.append(f"\n=== {title} ===")
        if missing:
            out.append(
                f"MISSING FROM DOCS ({len(missing)} in code, not documented):"
            )
            for p in sorted(missing, key=lambda x: x.name):
                parts = [f"  {p.name}"]
                if p.code_type:
                    parts.append(f"[{p.code_type}]")
                if p.default:
                    parts.append(f"default={p.default!r}")
                if p.mutable:
                    parts.append("(mutable)")
                if p.description:
                    parts.append(f"— {p.description}")
                out.append(" ".join(parts))
        else:
            out.append("MISSING FROM DOCS: none")

        if not new_params_only:
            if stale:
                out.append(
                    f"\nSTALE IN DOCS ({len(stale)} in docs, not in code):"
                )
                for name in sorted(stale):
                    out.append(f"  {name}")
            else:
                out.append("\nSTALE IN DOCS: none")

    section(
        "FE Configuration  (Config.java → FE_parameters/)",
        fe_missing,
        fe_stale,
    )
    section(
        "BE Configuration  (config.h → BE_parameters/)",
        be_missing,
        be_stale,
    )
    section(
        "Session / Global Variables  (SessionVariable.java + GlobalVariable.java → System_variable.md)",
        sv_missing,
        sv_stale,
    )
    return "\n".join(out)


def _fmt_github_comment(
    fe_missing: list[ParamInfo],
    be_missing: list[ParamInfo],
    sv_missing: list[ParamInfo],
) -> str:
    total = len(fe_missing) + len(be_missing) + len(sv_missing)
    if total == 0:
        return ""

    out: list[str] = [
        "## New parameters without documentation",
        "",
        f"This PR introduces **{total} parameter(s)** not yet documented. "
        "Please add entries before or shortly after merge.",
        "",
    ]

    def section(title: str, params: list[ParamInfo]) -> None:
        if not params:
            return
        out.append("<details>")
        out.append(f"<summary>{title} — {len(params)} undocumented</summary>")
        out.append("")
        out.append(
            "| Parameter | Type | Default | Mutable | Description | Suggested doc |"
        )
        out.append(
            "|-----------|------|---------|---------|-------------|---------------|"
        )
        for p in sorted(params, key=lambda x: x.name):
            desc = p.description or "—"
            mut = "Yes" if p.mutable else "No"
            out.append(
                f"| `{p.name}` | {p.code_type} | `{p.default}` | {mut} | {desc} | {p.suggested_doc} |"
            )
        out.append("")
        out.append("</details>")
        out.append("")

    section("FE Configuration", fe_missing)
    section("BE Configuration", be_missing)
    section("Session / Global Variables", sv_missing)

    out.append(
        "_Auto-generated by `build-support/extract_and_diff_params.py`. "
        "See `docs/en/administration/management/` for the doc files to update._"
    )
    return "\n".join(out)


def _fmt_json(
    fe_missing: list[ParamInfo],
    fe_stale: set[str],
    be_missing: list[ParamInfo],
    be_stale: set[str],
    sv_missing: list[ParamInfo],
    sv_stale: set[str],
) -> str:
    return json.dumps(
        {
            "fe_config": {
                "missing_from_docs": [asdict(p) for p in fe_missing],
                "stale_in_docs": sorted(fe_stale),
            },
            "be_config": {
                "missing_from_docs": [asdict(p) for p in be_missing],
                "stale_in_docs": sorted(be_stale),
            },
            "session_var": {
                "missing_from_docs": [asdict(p) for p in sv_missing],
                "stale_in_docs": sorted(sv_stale),
            },
        },
        indent=2,
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Compare StarRocks source-code config params against documentation."
    )
    ap.add_argument(
        "--new-params-only",
        action="store_true",
        help="Only report params added in the current git branch (CI mode)",
    )
    ap.add_argument(
        "--output",
        choices=["text", "json", "github-comment"],
        default="text",
        help="Output format (default: text)",
    )
    ap.add_argument(
        "--diff-base",
        default="origin/main",
        help=(
            "Git ref to diff against for --new-params-only "
            "(default: origin/main). Use origin/<base_ref> for release-branch PRs "
            "or HEAD^1 when running against a merge commit."
        ),
    )
    ap.add_argument(
        "--fe-config", type=Path, default=FE_CONFIG_PATH,
        help="Path to Config.java",
    )
    ap.add_argument(
        "--be-config", type=Path, default=BE_CONFIG_PATH,
        help="Path to be/src/common/config.h",
    )
    ap.add_argument(
        "--session-var", type=Path, default=SESSION_VAR_PATH,
        help="Path to SessionVariable.java",
    )
    ap.add_argument(
        "--global-var", type=Path, default=GLOBAL_VAR_PATH,
        help="Path to GlobalVariable.java",
    )
    ap.add_argument(
        "--with-ai-descriptions",
        action="store_true",
        help=(
            "Use the Anthropic API to generate descriptions for params that have "
            "no description in their source annotation or Javadoc. "
            "Requires ANTHROPIC_API_KEY env var or --api-key."
        ),
    )
    ap.add_argument(
        "--api-key",
        default=None,
        help="Anthropic API key (overrides ANTHROPIC_API_KEY env var)",
    )
    ap.add_argument(
        "--param",
        default=None,
        help="Only process this single parameter name (useful for testing AI descriptions)",
    )
    args = ap.parse_args()

    # Extract from source code
    fe_params = parse_fe_configs(args.fe_config)
    be_params = parse_be_configs(args.be_config)
    sv_params = parse_session_vars(args.session_var, args.global_var)

    # Extract from documentation
    fe_doc = parse_doc_params(FE_PARAM_DOCS)
    be_doc = parse_doc_params(BE_PARAM_DOCS)
    sv_doc = parse_doc_params(SESSION_VAR_DOCS)

    # Compute gaps
    fe_missing_names = set(fe_params) - fe_doc
    fe_stale = fe_doc - set(fe_params)
    be_missing_names = set(be_params) - be_doc
    be_stale = be_doc - set(be_params)
    sv_missing_names = set(sv_params) - sv_doc
    sv_stale = sv_doc - set(sv_params)

    # In --new-params-only mode, restrict to params visible in the current git diff
    if args.new_params_only:
        new_names = get_new_param_names_from_diff(
            [args.fe_config, args.be_config, args.session_var, args.global_var],
            diff_base=args.diff_base,
        )
        fe_missing_names &= new_names
        be_missing_names &= new_names
        sv_missing_names &= new_names
        fe_stale = set()
        be_stale = set()
        sv_stale = set()

    fe_missing = sorted(
        [fe_params[n] for n in fe_missing_names], key=lambda x: x.name
    )
    be_missing = sorted(
        [be_params[n] for n in be_missing_names], key=lambda x: x.name
    )
    sv_missing = sorted(
        [sv_params[n] for n in sv_missing_names], key=lambda x: x.name
    )

    # ── Optional AI description generation ───────────────────────────────────
    if args.with_ai_descriptions:
        api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            print(
                "error: --with-ai-descriptions requires ANTHROPIC_API_KEY env var "
                "or --api-key",
                file=sys.stderr,
            )
            return 1

        all_missing = fe_missing + be_missing + sv_missing
        needs_desc = [
            p for p in all_missing
            if not p.description
            and (args.param is None or p.name == args.param)
        ]
        if needs_desc:
            print(
                f"Generating AI descriptions for {len(needs_desc)} params "
                f"with no existing description ...",
                file=sys.stderr,
            )
            ai_descs = generate_ai_descriptions(needs_desc, api_key)
            # Patch descriptions back into the ParamInfo objects in-place
            for p in all_missing:
                if p.name in ai_descs:
                    p.description = ai_descs[p.name]
        else:
            print("All params already have descriptions — nothing to generate.", file=sys.stderr)

    # ── Apply --param filter to output ───────────────────────────────────────
    if args.param:
        fe_missing = [p for p in fe_missing if p.name == args.param]
        be_missing = [p for p in be_missing if p.name == args.param]
        sv_missing = [p for p in sv_missing if p.name == args.param]
        fe_stale = {n for n in fe_stale if n == args.param}
        be_stale = {n for n in be_stale if n == args.param}
        sv_stale = {n for n in sv_stale if n == args.param}

    has_gaps = bool(fe_missing or be_missing or sv_missing)

    if args.output == "json":
        print(
            _fmt_json(
                fe_missing, fe_stale,
                be_missing, be_stale,
                sv_missing, sv_stale,
            )
        )
    elif args.output == "github-comment":
        comment = _fmt_github_comment(fe_missing, be_missing, sv_missing)
        if comment:
            print(comment)
            return 1
        return 0
    else:
        print(
            _fmt_text(
                fe_missing, fe_stale,
                be_missing, be_stale,
                sv_missing, sv_stale,
                args.new_params_only,
            )
        )
        if not args.new_params_only:
            print(
                f"\nSummary: {len(fe_missing)} FE config, "
                f"{len(be_missing)} BE config, "
                f"{len(sv_missing)} session/global var missing from docs."
            )

    return 1 if has_gaps else 0


if __name__ == "__main__":
    sys.exit(main())
