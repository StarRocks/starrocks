#!/usr/bin/env python3
"""
Prepare auto-fix candidates for the sql-doc-autofix skill.

Reads the SQL-sample checker's JSON output (run_sql_samples.py --format json),
keeps the FAIL items (the doc-rot candidates), and attaches the surrounding
documentation prose for each — so the agent can judge the example's INTENT, not
just its SQL, before proposing a fix.

Deterministic (stdlib only). It does NOT classify or fix — that's the agent's job.

Usage:
  python3 run_sql_samples.py --docs-root <4.1-checkout>/docs/en/sql-reference \
      --host 127.0.0.1 --port 9030 --user root --format json > run.json
  python3 autofix_candidates.py --run-json run.json \
      --repo /Users/you/GitHub/starrocks-4.1 [--context 12] [--limit N] > candidates.json
"""
import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-json", required=True, help="run_sql_samples.py --format json output")
    ap.add_argument("--repo", required=True, help="docs checkout root (to resolve file paths)")
    ap.add_argument("--context", type=int, default=12, help="lines of prose around each example")
    ap.add_argument("--limit", type=int, default=0, help="max candidates (0 = all)")
    args = ap.parse_args()

    run = json.load(open(args.run_json))
    meta = run.get("meta", {})
    fails = [r for r in run.get("results", []) if r.get("status") == "FAIL"]
    repo = Path(args.repo)

    out = []
    for r in fails:
        src = repo / r["file"]
        prose = ""
        if src.exists():
            lines = src.read_text(errors="replace").splitlines()
            i = max(0, r["line"] - 1 - args.context)
            j = min(len(lines), r["line"] - 1 + args.context)
            prose = "\n".join(lines[i:j])
        out.append({
            "file": r["file"],
            "line": r["line"],
            "statement": r["statement"],
            "error": r["reason"],
            "doc_context": prose,
        })
    if args.limit:
        out = out[:args.limit]

    print(json.dumps({
        "meta": meta,                       # docs + cluster versions + alignment verdict
        "note": "FAIL items with surrounding doc prose. Classify before fixing; "
                "preserve each example's intent; version/build-gated items are not fixes.",
        "candidates": out,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
