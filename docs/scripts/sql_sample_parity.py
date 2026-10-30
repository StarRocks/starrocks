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
Cross-language parity report for SQL doc examples (en / zh / ja).

This is a DIFFERENT signal from the doc-rot checker (run_sql_samples.py). Rot
asks "does this example still run?"; parity asks "does this example EXIST in the
other languages?". A missing example can never fail a rot run, so translation
drift — "the Chinese page gained three examples and English never followed" — is
invisible to the rot checker by construction. This script is what catches it.

Examples are paired by `skeleton_fingerprint` (comments stripped, whitespace
collapsed, lowercased), so the same statement carrying a translated `-- comment`
counts as PRESENT in both languages rather than as a spurious difference.

Output is a translation/backfill task list, NOT SQL fixes. Nothing here should be
auto-applied: a difference may be deliberate (a page restructured in one language
only), so the report is advisory and wants a human triage pass.

Usage:
  python3 docs/scripts/sql_sample_parity.py --repo ../sr-branch-4.1
  python3 docs/scripts/sql_sample_parity.py --repo . --format json > parity.json
  python3 docs/scripts/sql_sample_parity.py --repo . --subtree sql-reference
  python3 docs/scripts/sql_sample_parity.py --repo . --base zh   # compare against zh
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_sql_samples import extract_samples  # noqa: E402

LANGS = ("en", "zh", "ja")


def collect(repo: Path, lang: str, subtree: str) -> dict[str, list]:
    """{page-relative-path: [samples]} for one language, keyed by the path with the
    docs/<lang>/ prefix removed so the same page lines up across languages."""
    root = repo / "docs" / lang / subtree
    if not root.is_dir():
        return {}
    prefix = f"docs/{lang}/"
    pages: dict[str, list] = {}
    for s in extract_samples(root, repo_root=repo):
        if not s.runnable:
            continue
        key = s.file[len(prefix):] if s.file.startswith(prefix) else s.file
        pages.setdefault(key, []).append(s)
    return pages


def build_report(repo: Path, subtree: str, base: str) -> dict:
    by_lang = {lang: collect(repo, lang, subtree) for lang in LANGS}
    present = [l for l in LANGS if by_lang[l]]
    others = [l for l in present if l != base]

    totals = {l: sum(len(v) for v in by_lang[l].values()) for l in present}
    all_pages = sorted({p for l in present for p in by_lang[l]})

    missing_pages: list[dict] = []
    page_diffs: list[dict] = []

    for page in all_pages:
        have = [l for l in present if page in by_lang[l]]
        lack = [l for l in present if page not in by_lang[l]]
        if lack and have:
            missing_pages.append({"page": page, "has_samples_in": have,
                                  "no_samples_in": lack})
        if base not in by_lang or page not in by_lang.get(base, {}):
            # Page has runnable SQL in some other language but none in the base
            # language — already captured as a missing page above.
            continue

        base_sk = Counter(s.skeleton for s in by_lang[base][page])
        base_by_sk = {s.skeleton: s for s in by_lang[base][page]}
        entry = {"page": page, "counts": {base: len(by_lang[base][page])},
                 "only_in_base": [], "missing_from_base": []}

        # Group by example, not by (example, language) pair: one example absent
        # from both zh and ja is ONE backfill task, not two.
        lacking: dict[str, list[str]] = {}
        extra: dict[str, tuple[str, object]] = {}
        for lang in others:
            samples = by_lang[lang].get(page, [])
            entry["counts"][lang] = len(samples)
            other_sk = Counter(s.skeleton for s in samples)
            for sk in (base_sk - other_sk):
                lacking.setdefault(sk, []).append(lang)
            for sk in (other_sk - base_sk):
                if sk not in extra:
                    extra[sk] = (lang, {s.skeleton: s for s in samples}[sk])

        for sk, langs in lacking.items():
            s = base_by_sk[sk]
            entry["only_in_base"].append(
                {"missing_from": langs, "line": s.line_start,
                 "snippet": " ".join(s.body.split())[:110]})
        for sk, (lang, s) in extra.items():
            entry["missing_from_base"].append(
                {"present_in": lang, "line": s.line_start,
                 "snippet": " ".join(s.body.split())[:110]})
        if entry["only_in_base"] or entry["missing_from_base"]:
            page_diffs.append(entry)

    n_only = sum(len(e["only_in_base"]) for e in page_diffs)
    n_missing = sum(len(e["missing_from_base"]) for e in page_diffs)
    return {
        "meta": {
            "repo": str(repo), "subtree": subtree, "base": base,
            "languages": present,
            "runnable_samples": totals,
            "pages_with_runnable_sql": {l: len(by_lang[l]) for l in present},
            "pages_compared": len(all_pages),
            "pages_differing": len(page_diffs),
            f"examples_in_{base}_missing_elsewhere": n_only,
            f"examples_elsewhere_missing_from_{base}": n_missing,
            "note": "Paired on skeleton_fingerprint, so a translated comment is not "
                    "a difference. Advisory only — some differences are deliberate.",
        },
        "missing_pages": missing_pages,
        "page_diffs": page_diffs,
    }


def report_markdown(rep: dict) -> str:
    m = rep["meta"]
    base = m["base"]
    out = [f"# SQL example parity — base `{base}`  ({m['subtree']})", ""]
    out.append("| language | runnable samples | pages with SQL |")
    out.append("|---|---|---|")
    for l in m["languages"]:
        out.append(f"| `{l}` | {m['runnable_samples'][l]} | {m['pages_with_runnable_sql'][l]} |")
    out += ["",
            f"- pages compared: **{m['pages_compared']}**, differing: **{m['pages_differing']}**",
            f"- examples in `{base}` missing from another language: "
            f"**{m[f'examples_in_{base}_missing_elsewhere']}**",
            f"- examples in another language missing from `{base}`: "
            f"**{m[f'examples_elsewhere_missing_from_{base}']}**",
            "",
            "Paired on the comment-stripped skeleton hash, so translated comments are "
            "not counted as differences. Advisory: some gaps are deliberate.", ""]

    if rep["missing_pages"]:
        out += ["## Pages with runnable SQL in some languages but not others", ""]
        for e in rep["missing_pages"]:
            out.append(f"- `{e['page']}` — has SQL in {', '.join('`'+l+'`' for l in e['has_samples_in'])}; "
                       f"none in {', '.join('`'+l+'`' for l in e['no_samples_in'])}")
        out.append("")

    if rep["page_diffs"]:
        out += ["## Per-page example differences", ""]
        for e in sorted(rep["page_diffs"],
                        key=lambda x: -(len(x["only_in_base"]) + len(x["missing_from_base"]))):
            counts = "  ".join(f"`{l}`={n}" for l, n in e["counts"].items())
            out.append(f"### `{e['page']}`")
            out.append(f"{counts}")
            out.append("")
            for d in e["only_in_base"]:
                langs = ", ".join(f"`{l}`" for l in d["missing_from"])
                out.append(f"- missing from {langs} "
                           f"(`{base}`:{d['line']}) — `{d['snippet']}`")
            for d in e["missing_from_base"]:
                out.append(f"- missing from `{base}` "
                           f"(present in `{d['present_in']}`:{d['line']}) — `{d['snippet']}`")
            out.append("")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo", default=".", help="docs checkout root (contains docs/en, docs/zh, docs/ja)")
    ap.add_argument("--subtree", default="sql-reference",
                    help="path under docs/<lang>/ to compare (default: sql-reference)")
    ap.add_argument("--base", default="en", choices=LANGS,
                    help="language to compare the others against (default: en)")
    ap.add_argument("--format", choices=["md", "json"], default="md")
    args = ap.parse_args()

    repo = Path(args.repo).resolve()
    if not (repo / "docs" / args.base / args.subtree).is_dir():
        print(f"error: {repo}/docs/{args.base}/{args.subtree} not found", file=sys.stderr)
        return 2

    rep = build_report(repo, args.subtree, args.base)
    print(json.dumps(rep, indent=2, ensure_ascii=False) if args.format == "json"
          else report_markdown(rep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
