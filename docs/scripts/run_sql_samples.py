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
Run the runnable SQL samples from the docs against a live StarRocks and report
which ones fail — catching doc rot (renamed functions, changed signatures,
removed/changed syntax).

Builds on extract_sql_samples.py (same dir): that finds and classifies every
```sql block; this one executes the runnable ones.

Execution model
---------------
- Samples are grouped by source file and run **in document order** inside a
  fresh scratch database per file, so intra-page sequences (CREATE -> INSERT ->
  SELECT) resolve. Pages are isolated from each other.
- Samples that need resources a bare cluster doesn't have (object storage,
  external catalogs, Kafka/routine load, broker/backup, ...) are SKIPPED with a
  reason, not failed. So are shared-data-only samples unless --profile shared-data.
- "Pass" = the statement executes without a server error. (Result-content
  checking is a later phase.)

Connection is over the MySQL protocol to an FE. Point it at whatever your
docker-compose brings up:

  python3 docs/scripts/run_sql_samples.py --host 127.0.0.1 --port 9030 --user root
  python3 docs/scripts/run_sql_samples.py --dry-run            # no cluster: show the plan
  python3 docs/scripts/run_sql_samples.py --docs-root docs/en/sql-reference --format md

Requires `pymysql` for live runs (pip install pymysql); --dry-run is stdlib-only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract_sql_samples import SqlSample, extract_samples  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DOCS_ROOT = REPO_ROOT / "docs/en/sql-reference"   # Phase 1 scope

# ── Skip rules: samples a bare cluster can't or shouldn't run ────────────────
# Each entry: (reason, compiled regex). Matched against the sample body.
_SKIP_RULES: list[tuple[str, re.Pattern]] = [
    ("external-storage", re.compile(r"\b(s3|oss|obs|cos|ks3|gs|hdfs|wasbs?|abfss?)://", re.I)),
    ("external-storage", re.compile(r"\bFILES\s*\(", re.I)),
    ("external-catalog", re.compile(r"\bCREATE\s+(EXTERNAL\s+)?(CATALOG|RESOURCE)\b", re.I)),
    ("external-catalog", re.compile(r"\b(hive|iceberg|hudi|delta|paimon|jdbc|elasticsearch)\s*[._]", re.I)),
    ("load-job",         re.compile(r"\b(BROKER|ROUTINE|STREAM)\s+LOAD\b|\bLOAD\s+LABEL\b|\bCREATE\s+PIPE\b", re.I)),
    ("kafka",            re.compile(r"\bkafka_", re.I)),
    ("backup-restore",   re.compile(r"\b(BACKUP|RESTORE|CREATE\s+REPOSITORY)\b", re.I)),
    ("cluster-op",       re.compile(r"\b(ALTER\s+SYSTEM|ADD\s+BACKEND|DROP\s+BACKEND|DECOMMISSION)\b", re.I)),
    ("async-or-session", re.compile(r"\b(SUBMIT\s+TASK|KILL|CANCEL)\b", re.I)),
]
_SHARED_DATA_RULES: list[re.Pattern] = [
    re.compile(r"\b(STORAGE\s+VOLUME|cloud_native|datacache\.)\b", re.I),
]


@dataclass
class Result:
    sample: SqlSample
    status: str               # PASS | FAIL | SKIP
    reason: str = ""          # skip reason or error message
    statement: str = ""       # the specific statement that failed


def classify(body: str, profile: str) -> tuple[str, str]:
    """Return (action, reason): action is 'run' or 'skip'."""
    for reason, rx in _SKIP_RULES:
        if rx.search(body):
            return "skip", reason
    if profile != "shared-data":
        for rx in _SHARED_DATA_RULES:
            if rx.search(body):
                return "skip", "shared-data-only"
    return "run", ""


# ── Statement splitting (quote/comment aware, minimal) ───────────────────────
def split_statements(sql: str) -> list[str]:
    out, buf, i, n = [], [], 0, len(sql)
    quote = None
    while i < n:
        c = sql[i]
        if quote:
            buf.append(c)
            if c == "\\" and i + 1 < n:
                buf.append(sql[i + 1]); i += 2; continue
            if c == quote:
                quote = None
            i += 1; continue
        if c in ("'", '"', "`"):
            quote = c; buf.append(c); i += 1; continue
        if c == "-" and sql[i:i + 2] == "--":                 # line comment
            j = sql.find("\n", i); j = n if j == -1 else j
            buf.append(sql[i:j]); i = j; continue
        if c == "/" and sql[i:i + 2] == "/*":                 # block comment
            j = sql.find("*/", i); j = n if j == -1 else j + 2
            buf.append(sql[i:j]); i = j; continue
        if c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []; i += 1; continue
        buf.append(c); i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


# ── Execution ────────────────────────────────────────────────────────────────
def run_live(by_file: dict[str, list[SqlSample]], conn_kwargs: dict, profile: str) -> list[Result]:
    import pymysql  # lazy: only needed for live runs

    results: list[Result] = []
    conn = pymysql.connect(**conn_kwargs, autocommit=True, connect_timeout=30)
    try:
        for idx, (rel, samples) in enumerate(sorted(by_file.items())):
            scratch = f"docverify_{idx}"
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {scratch}")
                cur.execute(f"CREATE DATABASE {scratch}")
                cur.execute(f"USE {scratch}")
            for s in samples:
                action, reason = classify(s.body, profile)
                if action == "skip":
                    results.append(Result(s, "SKIP", reason)); continue
                failed = None
                for stmt in split_statements(s.body):
                    try:
                        with conn.cursor() as cur:
                            cur.execute(stmt)
                            if cur.description:      # drain any result set
                                cur.fetchall()
                    except Exception as exc:         # noqa: BLE001
                        failed = (stmt, str(exc).strip()); break
                if failed:
                    results.append(Result(s, "FAIL", failed[1], statement=failed[0]))
                else:
                    results.append(Result(s, "PASS"))
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {scratch}")
    finally:
        conn.close()
    return results


def plan_only(by_file: dict[str, list[SqlSample]], profile: str) -> list[Result]:
    results = []
    for _, samples in sorted(by_file.items()):
        for s in samples:
            action, reason = classify(s.body, profile)
            results.append(Result(s, "SKIP" if action == "skip" else "RUN", reason))
    return results


# ── Reporting ────────────────────────────────────────────────────────────────
def summarize(results: list[Result]) -> dict:
    c = {}
    for r in results:
        c[r.status] = c.get(r.status, 0) + 1
    return c


def report_text(results: list[Result], dry: bool) -> str:
    L = []
    for r in results:
        if r.status == "FAIL":
            L.append(f"FAIL {r.sample.file}:{r.sample.line_start}\n"
                     f"     stmt: {r.statement[:120]}\n"
                     f"     err:  {r.reason[:200]}")
    counts = summarize(results)
    head = "PLAN" if dry else "RESULTS"
    L.append(f"\n{head}: " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if dry:
        skips = {}
        for r in results:
            if r.status == "SKIP":
                skips[r.reason] = skips.get(r.reason, 0) + 1
        if skips:
            L.append("skips by reason: " + "  ".join(f"{k}={v}" for k, v in sorted(skips.items())))
    return "\n".join(L)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    ap.add_argument("--dry-run", action="store_true",
                    help="classify + show the execution plan without connecting")
    ap.add_argument("--profile", choices=["shared-nothing", "shared-data"],
                    default="shared-nothing")
    ap.add_argument("--host", default=os.environ.get("SR_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("SR_PORT", "9030")))
    ap.add_argument("--user", default=os.environ.get("SR_USER", "root"))
    ap.add_argument("--password", default=os.environ.get("SR_PASSWORD", ""))
    ap.add_argument("--format", choices=["text", "json"], default="text")
    args = ap.parse_args()

    samples = [s for s in extract_samples(args.docs_root) if s.runnable]
    by_file: dict[str, list[SqlSample]] = {}
    for s in samples:
        by_file.setdefault(s.file, []).append(s)
    for v in by_file.values():
        v.sort(key=lambda x: x.line_start)

    if args.dry_run:
        results = plan_only(by_file, args.profile)
    else:
        results = run_live(by_file, dict(host=args.host, port=args.port,
                                         user=args.user, password=args.password),
                           args.profile)

    if args.format == "json":
        print(json.dumps([{"file": r.sample.file, "line": r.sample.line_start,
                           "status": r.status, "reason": r.reason,
                           "statement": r.statement} for r in results], indent=2))
    else:
        print(report_text(results, args.dry_run))

    return 1 if any(r.status == "FAIL" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
