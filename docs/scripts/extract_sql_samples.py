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
Audit SQL code samples in the StarRocks English documentation.

Walks docs/en/, finds all ```sql fenced blocks, extracts each sample with its
source file and line number, categorizes by SQL statement type, and flags
whether the sample looks runnable (no placeholder patterns) or illustrative.

Usage:
  python3 docs/scripts/extract_sql_samples.py
  python3 docs/scripts/extract_sql_samples.py --format json
  python3 docs/scripts/extract_sql_samples.py --format csv
  python3 docs/scripts/extract_sql_samples.py --filter-runnable
  python3 docs/scripts/extract_sql_samples.py --filter-category SELECT,DDL
  python3 docs/scripts/extract_sql_samples.py --docs-root docs/en/sql-reference
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DOCS_ROOT = REPO_ROOT / "docs/en"

# ── SQL category detection ───────────────────────────────────────────────────

_CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("DDL",        ["CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "COMMENT"]),
    ("DML",        ["INSERT", "UPDATE", "DELETE", "LOAD", "STREAM", "BROKER", "ROUTINE"]),
    ("SHOW_ADMIN", ["SHOW", "ADMIN", "DESCRIBE", "DESC", "EXPLAIN", "ANALYZE"]),
    ("SET",        ["SET"]),
    ("SELECT",     ["SELECT", "WITH"]),
]


def _categorize(body: str) -> str:
    first_line = ""
    for line in body.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--") and not stripped.startswith("#"):
            first_line = stripped.upper()
            break
    for category, keywords in _CATEGORY_KEYWORDS:
        for kw in keywords:
            if first_line.startswith(kw):
                return category
    return "OTHER"


# ── Runnability detection ────────────────────────────────────────────────────

_PLACEHOLDER_RE = re.compile(r"<[a-zA-Z_][a-zA-Z0-9_ ]*>")
_ELLIPSIS_RE = re.compile(r"^\s*\.\.\.\s*$", re.MULTILINE)
_COMMENT_PLACEHOLDER_RE = re.compile(r"--\s*(add|insert|replace|your|todo|example)", re.IGNORECASE)


def _is_runnable(body: str) -> bool:
    if _PLACEHOLDER_RE.search(body):
        return False
    if _ELLIPSIS_RE.search(body):
        return False
    if _COMMENT_PLACEHOLDER_RE.search(body):
        return False
    # Must have at least one real statement token
    return bool(re.search(r"\b(SELECT|CREATE|INSERT|ALTER|DROP|SET|SHOW|ADMIN|WITH|DELETE|UPDATE)\b", body, re.IGNORECASE))


# ── Data model ───────────────────────────────────────────────────────────────

@dataclass
class SqlSample:
    file: str         # relative to repo root
    line_start: int   # 1-indexed line of the opening ```sql fence
    category: str     # DDL | DML | SHOW_ADMIN | SET | SELECT | OTHER
    runnable: bool    # True if no placeholder patterns detected
    body: str         # raw SQL text


# ── Extraction ───────────────────────────────────────────────────────────────

_FENCE_OPEN_RE = re.compile(r"^```\s*sql\s*$", re.IGNORECASE)
_FENCE_CLOSE_RE = re.compile(r"^```\s*$")


def extract_samples(docs_root: Path) -> list[SqlSample]:
    samples: list[SqlSample] = []
    for path in sorted(docs_root.rglob("*.md")) + sorted(docs_root.rglob("*.mdx")):
        rel = str(path.relative_to(REPO_ROOT))
        lines = path.read_text(errors="replace").splitlines()
        n = len(lines)
        i = 0
        while i < n:
            if _FENCE_OPEN_RE.match(lines[i].strip()):
                start = i + 1  # 1-indexed: fence is at line i, content starts at i+1
                i += 1
                body_lines: list[str] = []
                while i < n and not _FENCE_CLOSE_RE.match(lines[i].strip()):
                    body_lines.append(lines[i])
                    i += 1
                body = "\n".join(body_lines).strip()
                if body:
                    samples.append(
                        SqlSample(
                            file=rel,
                            line_start=start,
                            category=_categorize(body),
                            runnable=_is_runnable(body),
                            body=body,
                        )
                    )
            i += 1
    return samples


# ── Output formatters ────────────────────────────────────────────────────────

CATEGORIES = ["SELECT", "DDL", "SHOW_ADMIN", "SET", "DML", "OTHER"]


def _fmt_text(samples: list[SqlSample]) -> str:
    out: list[str] = []
    out.append("SQL Sample Extraction Report")
    out.append("=" * 60)
    out.append(f"Total SQL blocks found: {len(samples)}")
    out.append("")

    # Summary table
    out.append(f"{'Category':<12} {'Total':>6} {'Runnable':>9} {'Illustrative':>13}")
    out.append("-" * 44)
    for cat in CATEGORIES:
        cat_samples = [s for s in samples if s.category == cat]
        runnable = sum(1 for s in cat_samples if s.runnable)
        out.append(
            f"{cat:<12} {len(cat_samples):>6} {runnable:>9} {len(cat_samples) - runnable:>13}"
        )
    runnable_total = sum(1 for s in samples if s.runnable)
    out.append(
        f"{'TOTAL':<12} {len(samples):>6} {runnable_total:>9} {len(samples) - runnable_total:>13}"
    )
    out.append("")

    # Top files by SQL block count
    from collections import Counter
    file_counts = Counter(s.file for s in samples)
    out.append("Files with the most SQL blocks (top 10):")
    for path, count in file_counts.most_common(10):
        out.append(f"  {count:>4}  {path}")
    out.append("")

    # Sample runnable blocks (first 5)
    runnable_samples = [s for s in samples if s.runnable]
    if runnable_samples:
        out.append(f"Runnable samples (first 5 of {len(runnable_samples)}):")
        for s in runnable_samples[:5]:
            out.append(f"  {s.file}:{s.line_start}  [{s.category}]")
            for line in s.body.splitlines()[:3]:
                out.append(f"    {line}")
            if len(s.body.splitlines()) > 3:
                out.append("    ...")
            out.append("")

    # Sample illustrative blocks (first 5)
    illustrative = [s for s in samples if not s.runnable]
    if illustrative:
        out.append(f"Illustrative samples (first 5 of {len(illustrative)}):")
        for s in illustrative[:5]:
            out.append(f"  {s.file}:{s.line_start}  [{s.category}]")
            for line in s.body.splitlines()[:3]:
                out.append(f"    {line}")
            if len(s.body.splitlines()) > 3:
                out.append("    ...")
            out.append("")

    return "\n".join(out)


def _fmt_json(samples: list[SqlSample]) -> str:
    return json.dumps([asdict(s) for s in samples], indent=2)


def _fmt_csv(samples: list[SqlSample]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=["file", "line_start", "category", "runnable", "body"],
        lineterminator="\n",
    )
    writer.writeheader()
    for s in samples:
        writer.writerow(asdict(s))
    return buf.getvalue()


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Audit SQL code samples in StarRocks English documentation."
    )
    ap.add_argument(
        "--docs-root",
        type=Path,
        default=DEFAULT_DOCS_ROOT,
        help="Root directory to scan (default: docs/en/)",
    )
    ap.add_argument(
        "--format",
        choices=["text", "json", "csv"],
        default="text",
        help="Output format (default: text)",
    )
    ap.add_argument(
        "--filter-runnable",
        action="store_true",
        help="Only output runnable samples",
    )
    ap.add_argument(
        "--filter-category",
        help="Comma-separated list of categories to include (e.g. SELECT,DDL)",
    )
    ap.add_argument(
        "--output",
        type=Path,
        help="Write output to this file instead of stdout",
    )
    args = ap.parse_args()

    if not args.docs_root.exists():
        print(f"error: docs root not found: {args.docs_root}", file=sys.stderr)
        return 1

    samples = extract_samples(args.docs_root)

    # Apply filters
    if args.filter_runnable:
        samples = [s for s in samples if s.runnable]
    if args.filter_category:
        cats = {c.strip().upper() for c in args.filter_category.split(",")}
        samples = [s for s in samples if s.category in cats]

    # Format
    if args.format == "json":
        output = _fmt_json(samples)
    elif args.format == "csv":
        output = _fmt_csv(samples)
    else:
        output = _fmt_text(samples)

    # Write
    if args.output:
        args.output.write_text(output)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
