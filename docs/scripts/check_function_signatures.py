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
Detect drift between documented function signatures and the registered source of truth.

Readers report *missing fields in the documented syntax*. The motivating case: `abs` is
registered with 12 overloads (BIGINT, DECIMAL32/64/128/256, DECIMALV2, DOUBLE, FLOAT,
INT, LARGEINT, SMALLINT) while its doc page documents `ABS(x);` — stating no types at all.

Source of truth is `gensrc/script/functions.py`, which is a plain Python list literal, so
it is read with ast.literal_eval rather than a custom parser.

SCOPE — scalar functions only. Aggregate and table functions are registered
imperatively in fe/fe-core/.../catalog/FunctionSet.java and are NOT covered; pages for
them surface as NOT_IN_SOURCE, which is why that finding is off by default. Two live-
cluster routes were investigated and ruled out: `SHOW BUILTIN FUNCTIONS` returns only
`Function Name` with no signature, and `information_schema.routines` is an unimplemented
placeholder view. Enumeration therefore has to come from the source file.

Report-only by design. Rewriting a synopsis is normative prose, so a human authors the
fix; this tool only says which types and overloads the source declares and the doc omits.
There is no live-cluster mode yet (return-type probing against a versioned cluster is a
possible later addition).

Usage:
  python3 docs/scripts/check_function_signatures.py
  python3 docs/scripts/check_function_signatures.py --format json
  python3 docs/scripts/check_function_signatures.py --include-not-in-source
  python3 docs/scripts/check_function_signatures.py \
      --functions-py ../sr-branch-4.0/gensrc/script/functions.py --docs-base ../sr-branch-4.0/docs
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_sql_samples import skeleton_fingerprint          # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FUNCTIONS_PY = REPO_ROOT / "gensrc/script/functions.py"
DEFAULT_DOCS_BASE = REPO_ROOT / "docs"
FN_SUBPATH = "sql-reference/sql-functions"
DEFAULT_SUPPRESSIONS = Path(__file__).resolve().parent / "function_signature_suppressions.json"

# ── Type vocabulary ──────────────────────────────────────────────────────────

# Collapse the source's precision/element variants to a comparable family. Docs write
# "DECIMAL" where the source registers five decimal widths, and "ARRAY<DOUBLE>" where the
# source registers ARRAY_DOUBLE, so comparing raw names would be almost entirely noise.
_FAMILY = {
    "DECIMAL32": "DECIMAL", "DECIMAL64": "DECIMAL", "DECIMAL128": "DECIMAL",
    "DECIMAL256": "DECIMAL", "DECIMALV2": "DECIMAL", "DECIMAL": "DECIMAL",
    "STRING": "VARCHAR", "CHAR": "VARCHAR", "VARCHAR": "VARCHAR",
    "BOOL": "BOOLEAN", "BOOLEAN": "BOOLEAN",
    "INTEGER": "INT", "INT": "INT",
    "MAP_VARCHAR_VARCHAR": "MAP",
    "BINARY": "VARBINARY",
}
# Source generics match any documented type — they are placeholders, not claims.
_WILDCARD_SOURCE = {"ANY_ELEMENT", "ANY_ARRAY", "ANY_MAP", "ANY_STRUCT", "VARIANT",
                    "FUNCTION", "..."}


def family(t: str) -> str:
    t = t.strip().upper()
    if t.startswith("ARRAY_") or t.startswith("ARRAY<") or t == "ARRAY":
        return "ARRAY"
    if t.startswith("MAP_") or t.startswith("MAP<") or t == "MAP":
        return "MAP"
    if t.startswith("STRUCT"):
        return "STRUCT"
    return _FAMILY.get(t, t)


# Type tokens a doc synopsis may name. Used both to normalise and to decide whether the
# synopsis states any types at all.
_DOC_TYPES = sorted({
    "BOOLEAN", "BOOL", "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "LARGEINT",
    "FLOAT", "DOUBLE", "DECIMAL", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256",
    "DECIMALV2", "VARCHAR", "CHAR", "STRING", "DATE", "DATETIME", "TIME", "JSON", "HLL",
    "BITMAP", "ARRAY", "MAP", "STRUCT", "VARBINARY", "BINARY", "PERCENTILE",
}, key=len, reverse=True)
_TYPE_RE = re.compile(r"\b(" + "|".join(_DOC_TYPES) + r")\b", re.I)
_TYPE_RE_STRICT = re.compile(r"\b(" + "|".join(_DOC_TYPES) + r")\b")

# `ARRAY<BIGINT>` / `array(bigint)` / `MAP<VARCHAR,INT>` name ONE type, not two or three.
# Collapsing the parameter list before tokenising stops the inner type from being read as
# a separate claim — the bug that made bitmap_to_array's `ARRAY<BIGINT>` look like it
# claimed a BIGINT return.
_PARAMETERIZED = re.compile(r"\b(ARRAY|MAP|STRUCT)\s*[<(][^<>()]*[>)]", re.I)


def collapse_parameterized(line: str) -> str:
    prev = None
    while prev != line:                       # repeat for nesting, e.g. ARRAY<ARRAY<INT>>
        prev = line
        line = _PARAMETERIZED.sub(lambda m: m.group(1), line)
    return line

# `## Syntax` in en, `## 语法` in zh, `## 構文` in ja (ja also uses the English word).
_SYNTAX_HEADING = re.compile(r"^#{2,4}\s*(Syntax|语法|構文|文法)\s*$", re.M | re.I)
_FENCE = re.compile(r"^```[^\n]*\n(.*?)^```", re.S | re.M)


# ── Source of truth ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Overload:
    ret: str
    args: tuple[str, ...]


def load_source(path: Path) -> tuple[dict[str, list[Overload]], int]:
    """Parse gensrc/script/functions.py into {name: [Overload, ...]}.

    Entry shape is [id, name, exception_safe, check_overflow, return_type, [args], fn, ...].
    The args list is located positionally rather than by a fixed index so a future extra
    leading field cannot silently shift the return type by one.
    """
    src = path.read_text(errors="replace")
    m = re.search(r"vectorized_functions\s*=\s*\[(.*?)\n\]", src, re.S)
    if not m:
        raise SystemExit(f"error: could not find vectorized_functions list in {path}")
    entries = ast.literal_eval("[" + re.sub(r"#[^\n]*", "", m.group(1)) + "]")
    out: dict[str, list[Overload]] = defaultdict(list)
    for e in entries:
        args_idx = next((i for i, x in enumerate(e) if isinstance(x, list)), None)
        if args_idx is None or args_idx < 2 or not isinstance(e[1], str):
            continue                      # shape we do not recognise — skip, do not guess
        out[e[1].lower()].append(
            Overload(ret=str(e[args_idx - 1]), args=tuple(str(a) for a in e[args_idx])))
    return dict(out), len(entries)


# ── Doc side ─────────────────────────────────────────────────────────────────

@dataclass
class DocPage:
    lang: str
    file: str
    stem: str
    syntax: str = ""                 # raw first fenced block under the Syntax heading
    skeleton: str = ""               # pairs the en/zh/ja copies of one signature
    doc_types: list[str] = field(default_factory=list)     # loose: every type mentioned
    ret_types: list[str] = field(default_factory=list)     # strict: leading type before name(
    arg_types: list[str] = field(default_factory=list)     # strict: types inside the parens
    has_syntax_block: bool = False


def extract_syntax_block(text: str) -> str:
    m = _SYNTAX_HEADING.search(text)
    if not m:
        return ""
    rest = text[m.end():]
    nxt = re.search(r"^#{2,4}\s+\S", rest, re.M)      # stop at the next heading, if any
    section = rest[:nxt.start()] if nxt else rest
    f = _FENCE.search(section)
    return f.group(1).strip() if f else ""


def parse_doc_signature(block: str) -> tuple[list[str], list[str]]:
    """Return (types mentioned anywhere — loose, types claimed as a return type — strict).

    Two passes on purpose, because the two are used for opposite questions and want
    opposite error directions:

    * LOOSE (case-insensitive) answers "does the synopsis mention this type?", feeding
      MISSING_OVERLOADS. Being generous here avoids claiming a type is undocumented when
      the page wrote it in lower case, e.g. `array_cum_sum(array(bigint))`.
    * STRICT (upper case only) answers "does the synopsis *claim* this type?", feeding
      RETURN_TYPE_MISMATCH. Being strict here avoids reading an argument NAME as a type —
      `ADD_MONTHS(date, months)` names an argument `date`, it does not claim a DATE.
    """
    loose, ret_types, arg_types = [], [], []
    for raw in block.splitlines():
        line = collapse_parameterized(raw.strip().rstrip(";"))
        if not line:
            continue
        loose += [t.upper() for t in _TYPE_RE.findall(line)]
        # leading return type: one or more union-separated types before `name(`
        m = re.match(r"\s*([A-Za-z0-9_<>,\s|]+?)\s+([A-Za-z_]\w*)\s*\(", line)
        if m:
            head = m.group(1)
            found = _TYPE_RE_STRICT.findall(head)
            # only a return type if the head is types (plus separators), not prose
            if found and not _TYPE_RE_STRICT.sub("", head).strip(" |<>,"):
                ret_types += found
        # argument types: strictly-cased tokens inside the parameter list only, so the
        # leading return type does not leak into the argument comparison
        inner = re.search(r"\(([^()]*)\)", line)
        if inner:
            arg_types += _TYPE_RE_STRICT.findall(inner.group(1))
    return loose, ret_types, arg_types


def load_pages(docs_base: Path, langs: list[str]) -> tuple[list[DocPage], dict[str, str]]:
    """Also return {identifier: first file naming it}, built from the full page text.

    Needed because several registered names have no page of their own but ARE documented
    as an alias on a sibling page — `dceil` on `ceil.md`, `current_date` on `curdate.md`.
    Matching only page stems reports those as undocumented, which is wrong.
    """
    pages: list[DocPage] = []
    mentions: dict[str, str] = {}
    for lang in langs:
        root = docs_base / lang / FN_SUBPATH
        if not root.is_dir():
            continue
        for p in sorted(root.rglob("*.md")):
            text = p.read_text(errors="replace")
            block = extract_syntax_block(text)
            all_t, ret_t, arg_t = parse_doc_signature(block)
            try:
                rel = str(p.resolve().relative_to(docs_base.resolve().parent))
            except ValueError:
                rel = str(p)
            if lang == "en":
                for tok in re.findall(r"\b[a-z_][a-z0-9_]{2,}\b", text.lower()):
                    mentions.setdefault(tok, rel)
            pages.append(DocPage(lang=lang, file=rel, stem=p.stem.lower(),
                                 syntax=block,
                                 skeleton=skeleton_fingerprint(block) if block else "",
                                 doc_types=all_t, ret_types=ret_t, arg_types=arg_t,
                                 has_syntax_block=bool(block)))
    return pages, mentions


# ── Findings ─────────────────────────────────────────────────────────────────

KINDS = ["UNDOCUMENTED_FUNCTION", "MISSING_OVERLOADS", "RETURN_TYPE_MISMATCH",
         "ARG_TYPE_MISMATCH", "NO_TYPES_STATED", "NO_SYNTAX_BLOCK", "ALIAS_ONLY",
         "NOT_IN_SOURCE"]

KIND_MEANING = {
    "UNDOCUMENTED_FUNCTION": "registered in functions.py and named nowhere in the docs",
    "ALIAS_ONLY": "no page of its own, but named on another function's page — usually a "
                  "documented alias (dceil on ceil.md), so lower priority than a true gap",
    "MISSING_OVERLOADS": "source declares return types the synopsis never names",
    "RETURN_TYPE_MISMATCH": "synopsis states a return type no overload declares",
    "ARG_TYPE_MISMATCH": "synopsis names an argument type no *registered* overload accepts "
                         "— unsound on its own, see the note below",
    "NO_TYPES_STATED": "synopsis names no types at all (e.g. `ABS(x);`)",
    "NO_SYNTAX_BLOCK": "no fenced block found under a Syntax heading",
    "NOT_IN_SOURCE": "page name not in functions.py — Java-registered aggregate, or renamed/removed",
}


@dataclass
class Finding:
    name: str
    kinds: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    langs: list[str] = field(default_factory=list)
    doc_syntax: str = ""
    source_ret_families: list[str] = field(default_factory=list)
    source_ret_raw: list[str] = field(default_factory=list)
    missing_ret_families: list[str] = field(default_factory=list)
    bad_ret: list[str] = field(default_factory=list)
    bad_args: list[str] = field(default_factory=list)
    overloads: int = 0
    partial: bool = False        # MISSING_OVERLOADS where the doc names some types
    skeletons_agree: bool = True


def load_suppressions(path: Path) -> set[tuple[str, str]]:
    if not path.is_file():
        return set()
    data = json.loads(path.read_text())
    out = set()
    for e in data.get("suppressions", []):
        fn, kind = e.get("function"), e.get("kind")
        if fn and kind:
            out.add((fn.lower(), kind))
    return out


def analyse(source: dict[str, list[Overload]], pages: list[DocPage],
            name_aliases: dict[str, str], mentions: dict[str, str]) -> list[Finding]:
    by_name: dict[str, list[DocPage]] = defaultdict(list)
    for p in pages:
        by_name[name_aliases.get(p.stem, p.stem)].append(p)

    findings: list[Finding] = []

    # documented pages
    for name, ps in sorted(by_name.items()):
        f = Finding(name=name,
                    files=[p.file for p in ps],
                    langs=sorted({p.lang for p in ps}))
        en = next((p for p in ps if p.lang == "en"), ps[0])
        f.doc_syntax = " / ".join(dict.fromkeys(
            l.strip() for p in ps for l in p.syntax.splitlines() if l.strip()))[:300]
        skels = {p.skeleton for p in ps if p.skeleton}
        f.skeletons_agree = len(skels) <= 1

        overloads = source.get(name)
        if overloads is None:
            f.kinds.append("NOT_IN_SOURCE")
            findings.append(f)
            continue

        f.overloads = len(overloads)
        f.source_ret_raw = sorted({o.ret for o in overloads})
        f.source_ret_families = sorted({family(o.ret) for o in overloads}
                                       - _WILDCARD_SOURCE)
        src_arg_families = {family(a) for o in overloads for a in o.args} - _WILDCARD_SOURCE
        src_all_families = set(f.source_ret_families) | src_arg_families

        if not en.has_syntax_block:
            f.kinds.append("NO_SYNTAX_BLOCK")
        doc_families = {family(t) for t in en.doc_types}
        if en.has_syntax_block and not doc_families:
            f.kinds.append("NO_TYPES_STATED")

        # missing overloads: return families the source declares that the doc never names
        missing = sorted(set(f.source_ret_families) - doc_families)
        if missing and f.source_ret_families:
            f.missing_ret_families = missing
            # partial = the synopsis names SOME types but not all. Independent signal.
            # When it names none, NO_TYPES_STATED already says so and every type is
            # trivially "missing", so that subset carries no extra information.
            f.partial = bool(doc_families)
            f.kinds.append("MISSING_OVERLOADS")

        # Mismatches only where BOTH sides make a checkable claim. When every source
        # overload is a generic (ANY_MAP, ANY_ELEMENT, FUNCTION), the source states no
        # concrete type, so nothing the doc says can contradict it — reporting a mismatch
        # against an empty set is what made map_apply/map_filter/map_from_arrays fire.
        if en.ret_types and f.source_ret_families:
            bad = sorted({family(t) for t in en.ret_types} - set(f.source_ret_families))
            if bad:
                f.bad_ret = bad
                f.kinds.append("RETURN_TYPE_MISMATCH")
        doc_arg_families = {family(t) for t in en.arg_types}
        if doc_arg_families and src_arg_families:
            bad_a = sorted(doc_arg_families - src_arg_families)
            if bad_a:
                f.bad_args = bad_a
                f.kinds.append("ARG_TYPE_MISMATCH")

        if f.kinds:
            findings.append(f)

    # registered but undocumented. Underscore-prefixed names are internal — the
    # __iceberg_transform_* family exists to implement partition transforms and is not
    # user-callable, so absence of a doc page is correct, not a gap.
    documented = set(by_name)
    for name in sorted(set(source) - documented):
        if name.startswith("_"):
            continue
        where = mentions.get(name)
        findings.append(Finding(name=name,
                                kinds=["ALIAS_ONLY" if where else "UNDOCUMENTED_FUNCTION"],
                                files=[where] if where else [],
                                overloads=len(source[name]),
                                source_ret_raw=sorted({o.ret for o in source[name]}),
                                source_ret_families=sorted(
                                    {family(o.ret) for o in source[name]} - _WILDCARD_SOURCE)))
    return findings


# ── Report ───────────────────────────────────────────────────────────────────

def fmt_md(findings: list[Finding], stats: dict, include_nis: bool,
           include_arg: bool) -> str:
    L = ["# Function signature drift", "",
         "Generated by `docs/scripts/check_function_signatures.py`. Source of truth is "
         "`gensrc/script/functions.py` (**scalar functions only** — aggregates are "
         "registered in `FunctionSet.java` and are not covered). Report-only: rewriting a "
         "synopsis is normative prose, so a human authors every fix.", ""]
    L += [f"- source entries parsed: **{stats['entries']}** "
          f"({stats['names']} distinct scalar names, {stats['multi']} with >1 overload)",
          f"- doc pages scanned: **{stats['pages']}** across {', '.join(stats['langs'])}",
          f"- pages mapped to a source name: **{stats['mapped']}**", ""]
    if not include_nis:
        L += [f"> `NOT_IN_SOURCE` is suppressed ({stats['not_in_source']} pages). It is "
              f"dominated by Java-registered aggregates until that source is added. "
              f"Re-run with `--include-not-in-source` to see it.", ""]
    if not include_arg:
        L += [f"> `ARG_TYPE_MISMATCH` is suppressed ({stats['arg_mismatch']} pages). "
              f"`functions.py` lists the physical overloads the BE implements, not the "
              f"logical types the FE accepts after implicit casting — `date_add` registers "
              f"`(DATETIME, INT)` yet accepts DATE, so the documented `DATETIME|DATE` is "
              f"correct. The check cannot distinguish that from real drift. Re-run with "
              f"`--include-arg-mismatch` to review it by hand.", ""]

    counts = Counter(k for f in findings for k in f.kinds)
    L += ["## Findings", "", "| finding | count | meaning |", "|---|---|---|"]
    for k in KINDS:
        if counts.get(k):
            L.append(f"| `{k}` | {counts[k]} | {KIND_MEANING[k]} |")
    L.append("")

    for k in KINDS:
        sel = [f for f in findings if k in f.kinds]
        if not sel:
            continue
        L += [f"## {k} ({len(sel)})", "", KIND_MEANING[k], ""]
        if k in ("UNDOCUMENTED_FUNCTION", "ALIAS_ONLY"):
            hdr = ("| function | overloads | return types | named on |"
                   if k == "ALIAS_ONLY" else "| function | overloads | return types |")
            L += [hdr, "|---|---|---|---|" if k == "ALIAS_ONLY" else "|---|---|---|"]
            for f in sel:
                row = (f"| `{f.name}` | {f.overloads} | "
                       f"{', '.join('`%s`' % t for t in f.source_ret_families)} |")
                if k == "ALIAS_ONLY":
                    row += f" `{f.files[0] if f.files else ''}` |"
                L.append(row)
            L.append("")
            continue
        for f in sel:
            L.append(f"### `{f.name}`  ({', '.join(f.langs)})")
            if f.doc_syntax:
                L.append(f"- documented: `{f.doc_syntax}`")
            if k == "MISSING_OVERLOADS":
                if not f.partial:
                    L.append("- the synopsis names no types at all, so every type below is "
                             "trivially absent; `NO_TYPES_STATED` is the actionable finding")
                L.append(f"- source declares **{f.overloads} overloads**, return types "
                         f"{', '.join('`%s`' % t for t in f.source_ret_raw)}")
                L.append(f"- **not named in the synopsis:** "
                         f"{', '.join('`%s`' % t for t in f.missing_ret_families)}")
            if k == "RETURN_TYPE_MISMATCH":
                L.append(f"- doc states `{'`, `'.join(f.bad_ret)}`; source returns "
                         f"{', '.join('`%s`' % t for t in f.source_ret_families)}")
            if k == "ARG_TYPE_MISMATCH":
                L.append(f"- doc names `{'`, `'.join(f.bad_args)}`, accepted by no overload")
            if not f.skeletons_agree:
                L.append("- note: the language copies of this synopsis differ")
            for fl in f.files:
                L.append(f"  - `{fl}`")
            L.append("")
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--functions-py", type=Path, default=DEFAULT_FUNCTIONS_PY)
    ap.add_argument("--docs-base", type=Path, default=DEFAULT_DOCS_BASE,
                    help="Directory containing en/, zh/, ja/ (default: docs/)")
    ap.add_argument("--languages", default="en,zh,ja")
    ap.add_argument("--include-arg-mismatch", action="store_true",
                    help="Include ARG_TYPE_MISMATCH. Off by default: functions.py lists "
                         "the PHYSICAL overloads the BE implements, not the logical types "
                         "the FE accepts after implicit casting, so a documented DATE arg "
                         "on a DATETIME-registered function is correct, not drift.")
    ap.add_argument("--include-not-in-source", action="store_true",
                    help="Include pages absent from functions.py; dominated by "
                         "Java-registered aggregates, so off by default.")
    ap.add_argument("--suppressions", type=Path, default=DEFAULT_SUPPRESSIONS)
    ap.add_argument("--format", choices=["md", "json"], default="md")
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()

    if not args.functions_py.is_file():
        print(f"error: source not found: {args.functions_py}", file=sys.stderr)
        return 1

    source, n_entries = load_source(args.functions_py)
    langs = [l.strip() for l in args.languages.split(",") if l.strip()]
    pages, mentions = load_pages(args.docs_base, langs)
    if not pages:
        print(f"error: no function pages under {args.docs_base}/<lang>/{FN_SUBPATH}",
              file=sys.stderr)
        return 1

    suppressed = load_suppressions(args.suppressions)
    aliases: dict[str, str] = {}
    if args.suppressions.is_file():
        aliases = {k.lower(): v.lower() for k, v in
                   json.loads(args.suppressions.read_text()).get("name_aliases", {}).items()}

    findings = analyse(source, pages, aliases, mentions)
    for f in findings:
        f.kinds = [k for k in f.kinds if (f.name, k) not in suppressed]
    findings = [f for f in findings if f.kinds]

    n_arg_mismatch = sum(1 for f in findings if "ARG_TYPE_MISMATCH" in f.kinds)
    if not args.include_arg_mismatch:
        for f in findings:
            f.kinds = [k for k in f.kinds if k != "ARG_TYPE_MISMATCH"]
        findings = [f for f in findings if f.kinds]

    n_not_in_source = sum(1 for f in findings if "NOT_IN_SOURCE" in f.kinds)
    if not args.include_not_in_source:
        for f in findings:
            f.kinds = [k for k in f.kinds if k != "NOT_IN_SOURCE"]
        findings = [f for f in findings if f.kinds]

    en_stems = {aliases.get(p.stem, p.stem) for p in pages if p.lang == "en"}
    stats = dict(entries=n_entries, names=len(source),
                 multi=sum(1 for v in source.values() if len(v) > 1),
                 pages=len(pages), langs=langs,
                 mapped=len(en_stems & set(source)),
                 not_in_source=n_not_in_source, arg_mismatch=n_arg_mismatch)

    out = (json.dumps(dict(stats={k: v for k, v in stats.items()},
                           findings=[asdict(f) for f in findings]), indent=2)
           if args.format == "json"
           else fmt_md(findings, stats, args.include_not_in_source,
                       args.include_arg_mismatch))
    if args.output:
        args.output.write_text(out)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
