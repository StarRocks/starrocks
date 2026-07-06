#!/usr/bin/env python3
"""Readback sweep: SELECT MAX(amount) WHERE user_id <range>.
`amount` is NOT in the index (user_id, town), so the secondary index produces a
rowid filter and BE must gather `amount` from the BASE segments (回表).
This is the query family the sidx-scan-orderby goal targets.

Env: FE_HOST, FE_PORT(9030), USER(root), PASSWORD, DB(bench_sidx)
Args: --table (orders | orders_ob), --label
15 runs, drop 3 warmups, p50 (matches the PoC report method).
"""
import argparse, os, sys, time
try:
    import pymysql
except ImportError:
    sys.exit("pip install pymysql")

# selectivity -> varchar user_id predicate (VARCHAR schema = harshest case)
RANGES = [
    ("0.0005%", "user_id = '12345'"),
    ("0.55%",   "user_id BETWEEN '12345' AND '12444'"),
    ("5.5%",    "user_id BETWEEN '14000' AND '14999'"),
    ("11.1%",   "user_id BETWEEN '13000' AND '14999'"),
    ("27.8%",   "user_id BETWEEN '10000' AND '14999'"),
]

def pctl(v, q):
    v = sorted(v); k = (len(v)-1)*q; f = int(k); c = min(f+1, len(v)-1)
    return v[f] if f == c else v[f] + (v[c]-v[f])*(k-f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--table", default="orders")
    ap.add_argument("--runs", type=int, default=15)
    ap.add_argument("--warmup", type=int, default=3)
    ap.add_argument("--verify", action="store_true", help="print MAX value for correctness check")
    a = ap.parse_args()
    c = pymysql.connect(host=os.environ["FE_HOST"], port=int(os.environ.get("FE_PORT","9030")),
                        user=os.environ.get("USER","root"), password=os.environ.get("PASSWORD",""),
                        db=os.environ.get("DB","bench_sidx")).cursor()
    for sel, pred in RANGES:
        sql = f"SELECT MAX(amount) FROM {a.table} WHERE {pred}"
        s = []; val = None
        for i in range(a.runs):
            t = time.perf_counter(); c.execute(sql); rows = c.fetchall()
            if i >= a.warmup: s.append((time.perf_counter()-t)*1000)
            val = rows[0][0] if rows else None
        extra = f"  max={val}" if a.verify else ""
        print(f"[{a.label}] {sel:8s} p50={pctl(s,0.50):8.1f}ms  p95={pctl(s,0.95):8.1f}ms{extra}", flush=True)

if __name__ == "__main__":
    sys.exit(main())
