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
PROTOTYPE — detect column drift between documented query output and a live cluster.

Motivation: readers report *missing columns* in documented output. A release adds a
column to `SHOW ...` or an `information_schema` table, the pasted result table in the
docs is never regenerated, and the doc silently under-reports the schema.

This checks the one part of pasted output that is deterministic and machine-checkable —
the **column header**. It deliberately does NOT compare row values: IDs, paths,
timestamps, byte counts and row counts are cluster-specific, and a doc showing
realistic example data is *better* than one showing single-node Docker noise.

Why this is not just a flag on run_sql_samples.py: that checker *skips* every
output-bearing block (`output-sample` / `cli-transcript` / `client-output` skip
reasons), because a pasted `+----+` table is not runnable SQL. So the blocks most
likely to carry column drift are exactly the ones the rot check never executes. This
tool recovers that coverage.

Two block shapes are recognized:
  inline    — statement and its pasted output in the SAME fence (the dominant shape)
  adjacent  — a ```sql fence followed by a separate output fence

Pairing is deliberately conservative: a pair is emitted only when a statement is
immediately followed by its result set. A wrong statement->output pairing produces a
confident, wrong finding, which is worse than no finding.

Usage:
  # offline: parse and report what is checkable, no cluster needed
  python3 docs/scripts/check_output_columns.py --offline

  # live: diff documented headers against a running cluster
  python3 docs/scripts/check_output_columns.py --host 127.0.0.1 --port 9030 --user root

  python3 docs/scripts/check_output_columns.py --scope all --format json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_sql_samples import skeleton_fingerprint          # noqa: E402
from run_sql_samples import split_statements                  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DOCS_ROOT = REPO_ROOT / "docs/en/sql-reference"

# ── Fence + output recognition ───────────────────────────────────────────────

# A fence line carrying an info string OPENS a block; a bare ``` CLOSES it. The info
# string may contain spaces — ```Plain Text is used ~450 times in sql-reference, and
# treating it as neither open nor close makes the previous block swallow the rest of
# the page (silently capturing prose as a "statement").
_FENCE_OPEN = re.compile(r"^```+[ \t]*([^\s`][^`]*?)[ \t]*$")
_FENCE_CLOSE = re.compile(r"^```+[ \t]*$")

# `+----+------+` result-table border.
_BORDER = re.compile(r"^\s*\+[-+]{3,}\+\s*$")
# `*************************** 1. row ***************************` (\G output).
_VERT_ROW = re.compile(r"^\s*\*{3,}\s*(\d+)\.\s*row\s*\*{3,}\s*$")
# `field: value` inside a \G row block.
_VERT_FIELD = re.compile(r"^\s*([A-Za-z_][\w $.]*)\s*:\s?(.*)$")
# Client chrome that trails a result set.
_CHROME = re.compile(r"(?im)^\s*(Query OK|Empty set|\d+ rows?\s+in set|\d+ rows?\s+affected)")
# Client prompts. Two forms, kept separate on purpose:
#  - a KNOWN client name may have spaces before '>' (`MySQL > `, `MySQL [db] > `)
#  - any other identifier must NOT, and must be followed by a space or tab, so that
#    neither `col > 5` nor an unspaced `col>5` on a wrapped WHERE line is eaten. An
#    unspaced bare prompt (`mydb>SELECT 1`) is therefore left alone: that costs one
#    ERROR row, where mis-stripping would silently corrupt the statement instead.
_PROMPT_NAMED = re.compile(
    r"^\s*(mysql|starrocks|hive|spark-sql|clickhouse|psql)\b[^>\n]*>\s?", re.I)
_PROMPT_BARE = re.compile(r"^\s*[A-Za-z][\w.\-]*(\s*\[[^\]]*\])?>[ \t]")
# Continuation prompt inside a transcript.
_CONT_PROMPT = re.compile(r"^\s*(->|\.\.\.)\s?")

# Synopsis notation, not an executable statement: `<placeholder>`, `[OPTIONAL KEYWORD]`,
# `[A|B]`, `[db_name.]`. A synopsis block sitting above an example's output is the main
# source of false adjacent pairings, so reject it at extraction time.
_SYNOPSIS = re.compile(
    r"<[A-Za-z_][\w ]*>|\[[A-Z][A-Z ]{2,}\]|\[[^\]\n]*\|[^\]\n]*\]|\[[A-Za-z_]\w*\.\]")
# A candidate statement must contain a real SQL verb — guards against prose that lands
# in the statement buffer (`Example 2: Display the amount of ...`).
_LOOKS_SQL = re.compile(
    r"\b(SELECT|SHOW|DESC|DESCRIBE|EXPLAIN|WITH|CREATE|ALTER|DROP|INSERT|UPDATE|"
    r"DELETE|SET|ADMIN|ANALYZE)\b", re.I)
# MySQL-client vertical-output terminator; not valid over the wire.
_G_TERMINATOR = re.compile(r"\\G\s*;?\s*$")

# Statements whose result-set shape is worth checking and that are safe to run
# read-only against a shared cluster. Anything else is reported as out-of-scope
# rather than executed — a prototype must not mutate a cluster to check a header.
_CHECKABLE_STMT = re.compile(
    r"^\s*(SHOW|DESC|DESCRIBE|SELECT|WITH|EXPLAIN|ADMIN\s+SHOW)\b", re.I)


def _is_output_start(line: str) -> bool:
    return bool(_BORDER.match(line) or _VERT_ROW.match(line))


@dataclass
class Pair:
    """One documented statement paired with the column header of its pasted output."""
    file: str
    line_start: int          # 1-indexed line of the statement's fence
    shape: str               # inline | adjacent
    style: str               # ascii | vertical
    statement: str
    doc_columns: list[str]
    skeleton: str = ""
    # filled in by the live pass
    status: str = ""         # MATCH | ORDER | CASE | DOC_MISSING | DOC_EXTRA | BOTH
                             # | ERROR | NO_RESULTSET | OUT_OF_SCOPE
    live_columns: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)   # live has, doc lacks
    extra: list[str] = field(default_factory=list)     # doc has, live lacks
    detail: str = ""


# ── Header parsing ───────────────────────────────────────────────────────────

def _parse_ascii_header(lines: list[str], i: int) -> tuple[list[str], int]:
    """At lines[i] == a `+---+` border, return (column names, index after the output).

    Shape:  +----+----+   <- i
            | a  | b  |   <- header
            +----+----+
            | 1  | 2  |   ... rows
            +----+----+
            2 rows in set (0.01 sec)
    """
    cols: list[str] = []
    if i + 1 < len(lines):
        header = lines[i + 1]
        if header.lstrip().startswith("|"):
            cols = [c.strip() for c in header.strip().strip("|").split("|")]
            cols = [c for c in cols if c]
    # Consume the whole output region: table lines plus trailing client chrome.
    j = i
    while j < len(lines):
        s = lines[j].strip()
        if s.startswith("|") or s.startswith("+") or _CHROME.match(lines[j]):
            j += 1
            continue
        break
    return cols, j


def _parse_vertical_header(lines: list[str], i: int) -> tuple[list[str], int]:
    """At lines[i] == `*** 1. row ***`, return (field names of that row, index after).

    Only the FIRST row block defines the column set; later rows repeat it.
    """
    cols: list[str] = []
    j = i + 1
    first_row = True
    while j < len(lines):
        if _VERT_ROW.match(lines[j]):
            first_row = False
            j += 1
            continue
        m = _VERT_FIELD.match(lines[j])
        if m:
            if first_row:
                cols.append(m.group(1).strip())
            j += 1
            continue
        if _CHROME.match(lines[j]) or not lines[j].strip():
            j += 1
            continue
        break
    return cols, j


def _clean_statement(raw_lines: list[str]) -> str:
    """Strip client prompts/continuations, then keep the LAST statement — the one that
    produced the output that follows. Returns "" when the buffer holds no executable
    statement (prose, or synopsis notation)."""
    cleaned = []
    for ln in raw_lines:
        ln = _PROMPT_NAMED.sub("", ln) if _PROMPT_NAMED.match(ln) else _PROMPT_BARE.sub("", ln)
        cleaned.append(_CONT_PROMPT.sub("", ln))
    text = "\n".join(cleaned).strip()
    if not text:
        return ""
    stmts = split_statements(text)
    if not stmts:
        return ""
    stmt = _G_TERMINATOR.sub("", stmts[-1].strip()).strip().rstrip(";").strip()
    if not stmt or not _LOOKS_SQL.search(stmt) or _SYNOPSIS.search(stmt):
        return ""
    return stmt


def _pairs_in_block(body: list[str], file: str, open_line: int, shape: str) -> list[Pair]:
    """Walk a block, emitting a Pair each time a statement is followed by output.

    A block may hold several statement/output pairs; each gets the line number of its
    OWN statement, not the fence's, so findings point at the right place.
    """
    out: list[Pair] = []
    buf: list[str] = []
    buf_at = 0                      # index in `body` where the current buffer began
    i, n = 0, len(body)
    while i < n:
        if _is_output_start(body[i]):
            style = "ascii" if _BORDER.match(body[i]) else "vertical"
            cols, j = (_parse_ascii_header(body, i) if style == "ascii"
                       else _parse_vertical_header(body, i))
            stmt = _clean_statement(buf)
            if stmt and cols:
                # body[0] is the line after the opening fence.
                lead = next((k for k, l in enumerate(buf) if l.strip()), 0)
                out.append(Pair(file=file, line_start=open_line + 1 + buf_at + lead,
                                shape=shape, style=style, statement=stmt,
                                doc_columns=cols,
                                skeleton=skeleton_fingerprint(stmt)))
            buf = []
            i = j
            buf_at = j
            continue
        buf.append(body[i])
        i += 1
    return out


# ── Extraction ───────────────────────────────────────────────────────────────

def _blocks(lines: list[str]):
    """Yield (lang, body_lines, open_line_1indexed, close_line_1indexed)."""
    i, n = 0, len(lines)
    while i < n:
        m = _FENCE_OPEN.match(lines[i].rstrip())
        if m:
            lang, start, body = m.group(1), i + 1, []
            i += 1
            while i < n and not _FENCE_CLOSE.match(lines[i].rstrip()):
                body.append(lines[i])
                i += 1
            yield lang, body, start, i + 1
        i += 1


def extract_pairs(docs_root: Path, repo_root: Path) -> list[Pair]:
    pairs: list[Pair] = []
    for path in sorted(docs_root.rglob("*.md")) + sorted(docs_root.rglob("*.mdx")):
        try:
            rel = str(path.resolve().relative_to(repo_root))
        except ValueError:
            rel = str(path)
        lines = path.read_text(errors="replace").splitlines()
        blocks = list(_blocks(lines))
        for idx, (lang, body, start, end) in enumerate(blocks):
            if any(_is_output_start(ln) for ln in body):
                # inline: statement + output share one fence (the dominant shape)
                pairs.extend(_pairs_in_block(body, rel, start, "inline"))
                continue
            if lang.lower() != "sql" or idx + 1 >= len(blocks):
                continue
            # adjacent: this sql fence, then a separate output fence.
            nlang, nbody, nstart, _ = blocks[idx + 1]
            if nlang.lower() == "sql" or not any(_is_output_start(l) for l in nbody):
                continue
            # The two fences must be CONTIGUOUS (blank line only). A prose paragraph or
            # heading in between means the output belongs to a later example — that gap
            # is what produced pairings like a CREATE VIEW block against SHOW CREATE
            # VIEW's output, or a synopsis block against an example's output.
            if nstart - end > 2:
                continue
            stmt = _clean_statement(body)
            if not stmt or not _CHECKABLE_STMT.match(stmt):
                continue
            k = next(i for i, l in enumerate(nbody) if _is_output_start(l))
            style = "ascii" if _BORDER.match(nbody[k]) else "vertical"
            cols, _ = (_parse_ascii_header(nbody, k) if style == "ascii"
                       else _parse_vertical_header(nbody, k))
            if cols:
                pairs.append(Pair(file=rel, line_start=start, shape="adjacent",
                                  style=style, statement=stmt, doc_columns=cols,
                                  skeleton=skeleton_fingerprint(stmt)))
    return pairs


# ── Live diff ────────────────────────────────────────────────────────────────

# Some column names embed the database being queried — `SHOW TABLES` returns
# `Tables_in_<db>`. The doc's db and the probe db never match, so compare a normalized
# form or every such example reports a spurious rename.
_DB_QUALIFIED_COL = re.compile(r"^(tables_in|views_in)_.+$", re.I)


def _normalize_col(name: str) -> str:
    m = _DB_QUALIFIED_COL.match(name.strip())
    return (m.group(1).casefold() + "_*") if m else name.strip().casefold()


def _classify(doc: list[str], live: list[str]) -> tuple[str, list[str], list[str]]:
    d_fold = [_normalize_col(c) for c in doc]
    l_fold = [_normalize_col(c) for c in live]
    d_set, l_set = set(d_fold), set(l_fold)
    missing = [c for c in live if _normalize_col(c) not in d_set]
    extra = [c for c in doc if _normalize_col(c) not in l_set]
    if missing and extra:
        return "BOTH", missing, extra
    if missing:
        return "DOC_MISSING", missing, extra
    if extra:
        return "DOC_EXTRA", missing, extra
    if d_fold != l_fold:
        return "ORDER", [], []
    # A db-qualified name (`Tables_in_<db>`) legitimately reads differently in the doc
    # than on the probe cluster — that is not a casing defect.
    if any(c.casefold() != l.casefold() and not _DB_QUALIFIED_COL.match(c)
           for c, l in zip(doc, live)):
        return "CASE", [], []
    return "MATCH", [], []


def run_live(pairs: list[Pair], conn_kwargs: dict) -> None:
    import pymysql  # lazy: only needed for live runs

    conn = pymysql.connect(**conn_kwargs, autocommit=True,
                           connect_timeout=30, read_timeout=120)
    # Unique per run, and created rather than recreated: a fixed name lets two
    # concurrent runs drop each other's scratch DB, and turns a pre-existing DB that
    # happens to share the name into collateral damage. This only ever drops a DB it
    # created itself, so a name collision fails loudly at CREATE instead.
    scratch = f"doccolumns_probe_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE {scratch}")
        cur.execute(f"USE {scratch}")

    try:
        for p in pairs:
            if not _CHECKABLE_STMT.match(p.statement):
                p.status = "OUT_OF_SCOPE"
                p.detail = "not a read-only result-returning statement"
                continue
            try:
                with conn.cursor() as cur:
                    cur.execute(p.statement)
                    if not cur.description:
                        p.status = "NO_RESULTSET"
                        continue
                    p.live_columns = [d[0] for d in cur.description]
                    cur.fetchall()
            except Exception as exc:                  # noqa: BLE001
                p.status = "ERROR"
                p.detail = str(exc).strip()[:200]
                continue
            p.status, p.missing, p.extra = _classify(p.doc_columns, p.live_columns)
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {scratch}")
        conn.close()


# ── Reporting ────────────────────────────────────────────────────────────────

_ORDER = ["DOC_MISSING", "BOTH", "DOC_EXTRA", "ORDER", "CASE", "MATCH",
          "ERROR", "NO_RESULTSET", "OUT_OF_SCOPE", ""]


def _fmt_md(pairs: list[Pair], offline: bool) -> str:
    from collections import Counter
    L = ["# Documented output — column drift", ""]
    L.append("Generated by `docs/scripts/check_output_columns.py` (PROTOTYPE). "
             "Compares the **column header** of pasted output against a live cluster; "
             "row values are never compared.")
    L.append("")
    L.append(f"**Checkable statement/output pairs found:** {len(pairs)}")
    shapes = Counter(p.shape for p in pairs)
    styles = Counter(p.style for p in pairs)
    L.append(f"**Shape:** " + ", ".join(f"`{k}` {v}" for k, v in shapes.most_common()))
    L.append(f"**Output style:** " + ", ".join(f"`{k}` {v}" for k, v in styles.most_common()))
    L.append("")

    if offline:
        L += ["## Offline mode — no cluster consulted", "",
              "Sampled pairs (first 15), showing the header this tool would diff:", ""]
        for p in pairs[:15]:
            stmt = " ".join(p.statement.split())[:90]
            L.append(f"- `{p.file}:{p.line_start}` [{p.shape}/{p.style}]  "
                     f"`{stmt}`")
            L.append(f"  - doc columns ({len(p.doc_columns)}): "
                     f"{', '.join('`%s`' % c for c in p.doc_columns)}")
        by_file = Counter(p.file for p in pairs)
        L += ["", "### Pages with the most checkable pairs", ""]
        for f, c in by_file.most_common(15):
            L.append(f"- {c:>3}  `{f}`")
        return "\n".join(L)

    counts = Counter(p.status for p in pairs)
    L += ["## Verdicts", "", "| status | count | meaning |", "|---|---|---|"]
    meaning = {
        "DOC_MISSING": "**live has columns the doc lacks — the reader complaint**",
        "DOC_EXTRA": "doc shows columns the cluster does not return (triage)",
        "BOTH": "differs in both directions (triage)",
        "ORDER": "same columns, different order (nit)",
        "CASE": "same columns, casing differs (nit)",
        "MATCH": "header matches",
        "ERROR": "statement did not execute here",
        "NO_RESULTSET": "statement returned no result set",
        "OUT_OF_SCOPE": "not run (not read-only)",
    }
    for st in _ORDER:
        if counts.get(st):
            L.append(f"| `{st}` | {counts[st]} | {meaning.get(st, '')} |")
    L.append("")

    for st in ("DOC_MISSING", "BOTH", "DOC_EXTRA", "ORDER", "CASE"):
        sel = [p for p in pairs if p.status == st]
        if not sel:
            continue
        L += [f"## {st} ({len(sel)})", ""]
        for p in sel:
            stmt = " ".join(p.statement.split())[:100]
            L.append(f"### `{p.file}:{p.line_start}`")
            L.append(f"`{stmt}`")
            if p.missing:
                L.append(f"- **missing from doc:** {', '.join('`%s`' % c for c in p.missing)}")
            if p.extra:
                L.append(f"- **in doc, not in cluster:** {', '.join('`%s`' % c for c in p.extra)}")
            if st in ("ORDER", "CASE"):
                L.append(f"- doc:  {', '.join('`%s`' % c for c in p.doc_columns)}")
                L.append(f"- live: {', '.join('`%s`' % c for c in p.live_columns)}")
            L.append("")
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    ap.add_argument("--scope", choices=["show-and-infoschema", "all"],
                    default="show-and-infoschema",
                    help="show-and-infoschema (default): SHOW_*.md + information_schema/ "
                         "— fixed output schemas, highest drift, smallest surface to "
                         "shake the design out on. all: the whole --docs-root tree.")
    ap.add_argument("--offline", action="store_true",
                    help="Parse and report only; do not connect to a cluster.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9030)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="")
    ap.add_argument("--format", choices=["md", "json"], default="md")
    ap.add_argument("--limit", type=int, help="Check at most N pairs.")
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()

    if not args.docs_root.exists():
        print(f"error: docs root not found: {args.docs_root}", file=sys.stderr)
        return 1

    repo_root = REPO_ROOT
    for parent in (args.docs_root.resolve(), *args.docs_root.resolve().parents):
        if (parent / "docs" / "en").is_dir():
            repo_root = parent
            break

    pairs = extract_pairs(args.docs_root, repo_root)
    if args.scope == "show-and-infoschema":
        pairs = [p for p in pairs
                 if Path(p.file).name.startswith("SHOW_")
                 or "/information_schema/" in p.file]
    if args.limit:
        pairs = pairs[:args.limit]

    if not args.offline:
        run_live(pairs, dict(host=args.host, port=args.port,
                             user=args.user, password=args.password))

    out = (json.dumps([asdict(p) for p in pairs], indent=2) if args.format == "json"
           else _fmt_md(pairs, args.offline))
    if args.output:
        args.output.write_text(out)
        print(f"Written to {args.output}", file=sys.stderr)
    else:
        print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
