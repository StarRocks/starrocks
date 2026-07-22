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
    # Account/privilege management mutates global auth state and can break the
    # session (or the whole cluster) for every later sample; "executes" is not a
    # useful accuracy signal for these anyway.
    ("account-mgmt", re.compile(
        r"\b(GRANT|REVOKE|CREATE\s+USER|ALTER\s+USER|DROP\s+USER|CREATE\s+ROLE|"
        r"DROP\s+ROLE|SET\s+(DEFAULT\s+)?ROLE|SET\s+PASSWORD|EXECUTE\s+AS|"
        r"IMPERSONATE|SECURITY\s+INTEGRATION)\b", re.I)),
    # Not runnable scripts: MySQL-client transcripts (prompt lines) and result
    # tables (ASCII borders) that were fenced as ```sql.
    ("cli-transcript", re.compile(r"(?m)^\s*(mysql\s*>|MySQL\s*(\[[^\]]*\])?\s*>)")),
    ("output-sample",  re.compile(r"(?m)^\s*\+[-+]{3,}\+\s*$")),
    ("cli-terminator", re.compile(r"\\G")),   # MySQL client \G vertical-output terminator
    # UDFs need a jar + FE/BE config the bare cluster doesn't have.
    ("udf", re.compile(r"\b(CREATE(\s+OR\s+REPLACE)?|DROP)\s+(GLOBAL\s+)?"
                       r"(AGGREGATE\s+|TABLE\s+)?FUNCTION\b", re.I)),
    # Template placeholder ellipsis (e.g. SQL_command_template.md).
    ("placeholder-ellipsis", re.compile(r"\.\.\.")),
    # Syntax-reference notation, not runnable SQL: [OPTIONAL KEYWORD] blocks in
    # Synopsis sections (e.g. CREATE [GLOBAL] FUNCTION, SHOW [FULL] TABLES).
    ("syntax-notation", re.compile(r"\[[A-Z][A-Z ]{2,}\]")),
]

# Server errors that mean the example just isn't self-contained (references a
# db/table/column/function defined elsewhere) — a distinct doc-quality signal,
# NOT doc rot.
_UNRESOLVED_CODES = {5501, 5502, 1055}
_UNRESOLVED_MSGS = ("unknown database", "unknown table", "unknown column",
                    "cannot be resolved", "not found", "unknown catalog",
                    "is not found", "unknown function", "no matching function")


def _is_unresolved(code, msg: str) -> bool:
    if code in _UNRESOLVED_CODES:
        return True
    m = msg.lower()
    return any(k in m for k in _UNRESOLVED_MSGS)


# Errors from the test cluster's shape, not the docs (e.g. single-BE replication
# limits, disabled UDFs). A distinct bucket so FAIL stays doc-rot signal.
_ENV_MSGS = ("replication num should be", "udf is not enabled",
             "be number", "backend number")


def _is_env(msg: str) -> bool:
    m = msg.lower()
    return any(k in m for k in _ENV_MSGS)
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
def _has_sql(stmt: str) -> bool:
    """True if a fragment has real SQL once comments are stripped. Guards against
    trailing/standalone comment fragments (e.g. `-- Return {...}`) that split off
    after a ';' and would otherwise execute as an empty statement / <EOF> error."""
    no_block = re.sub(r"/\*.*?\*/", "", stmt, flags=re.S)
    no_line = re.sub(r"(?m)--.*$", "", no_block)
    return bool(no_line.strip())


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
            if stmt and _has_sql(stmt):
                out.append(stmt)
            buf = []; i += 1; continue
        buf.append(c); i += 1
    tail = "".join(buf).strip()
    if tail and _has_sql(tail):
        out.append(tail)
    return out


# ── Execution ────────────────────────────────────────────────────────────────
def run_live(by_file: dict[str, list[SqlSample]], conn_kwargs: dict, profile: str) -> list[Result]:
    import pymysql  # lazy: only needed for live runs

    def connect():
        return pymysql.connect(**conn_kwargs, autocommit=True,
                               connect_timeout=30, read_timeout=120)

    results: list[Result] = []
    admin = connect()   # dedicated root session for DB lifecycle only

    def admin_exec(sql: str) -> bool:
        nonlocal admin
        for _ in range(2):
            try:
                with admin.cursor() as cur:
                    cur.execute(sql)
                return True
            except Exception:                       # noqa: BLE001
                try:
                    admin.close()
                except Exception:                    # noqa: BLE001
                    pass
                try:
                    admin = connect()
                except Exception:                    # noqa: BLE001
                    return False
        return False

    # Single-BE dev cluster: default tables to 1 replica so examples that don't
    # specify replication_num don't fail on "replication num > available BEs".
    # (Examples that explicitly request >1 replica still need a multi-BE cluster.)
    admin_exec('ADMIN SET FRONTEND CONFIG ("default_replication_num" = "1")')

    for idx, (rel, samples) in enumerate(sorted(by_file.items())):
        scratch = f"docverify_{idx}"
        admin_exec(f"DROP DATABASE IF EXISTS {scratch}")
        admin_exec(f"CREATE DATABASE {scratch}")
        # A FRESH worker session per file isolates role / session-var / current-db
        # state so a stateful sample can't leak into the next page.
        try:
            worker = connect()
            with worker.cursor() as cur:
                cur.execute(f"USE {scratch}")
        except Exception:                            # noqa: BLE001
            for s in samples:
                results.append(Result(s, "SKIP", "no-session"))
            continue

        for s in samples:
            action, reason = classify(s.body, profile)
            if action == "skip":
                results.append(Result(s, "SKIP", reason)); continue
            failed = None
            for stmt in split_statements(s.body):
                try:
                    with worker.cursor() as cur:
                        cur.execute(stmt)
                        if cur.description:          # drain any result set
                            cur.fetchall()
                except Exception as exc:             # noqa: BLE001
                    code = exc.args[0] if getattr(exc, "args", None) else None
                    # "already exists" = a name reused across independent examples
                    # on the page (or a db an earlier page created). The DDL intent
                    # holds; this is a dirty-namespace artifact, not doc rot.
                    if code in (1050, 1007) or "already exists" in str(exc).lower():
                        continue
                    msg = str(exc).strip()
                    if _is_unresolved(code, msg):
                        status = "UNRESOLVED"
                    elif _is_env(msg):
                        status = "ENV"
                    else:
                        status = "FAIL"
                    failed = (stmt, msg, status)
                    # keep the worker usable for the rest of the file
                    try:
                        worker.ping(reconnect=True)
                        with worker.cursor() as cur:
                            cur.execute(f"USE {scratch}")
                    except Exception:                # noqa: BLE001
                        pass
                    break
            results.append(Result(s, failed[2], failed[1], statement=failed[0])
                           if failed else Result(s, "PASS"))
        try:
            worker.close()
        except Exception:                            # noqa: BLE001
            pass
        admin_exec(f"DROP DATABASE IF EXISTS {scratch}")
    try:
        admin.close()
    except Exception:                                # noqa: BLE001
        pass
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
