#!/usr/bin/env python3
"""Covering-query sweep: queries whose predicate AND output are all inside the
index (user_id, town), so the secondary index can answer WITHOUT base readback.
Contrast with the readback sweep (MAX(amount), amount not in index).

Two covered query shapes per selectivity:
  cnt   : SELECT COUNT(*)              -- index-only aggregate, no readback
  town  : SELECT town  (2nd index col) -- covering projection, no readback

Aggregate town with MAX() so output stays one row (avoid transferring millions
of rows), while still forcing the town column to be produced from the index.

Env: FE_HOST, FE_PORT(9030), USER(root), PASSWORD, DB(bench_sidx)
Args: --table (orders | orders_bi | orders_ob | orders_bi_ob), --bigint (numeric literals)
"""
import argparse, os, sys, time
try:
    import pymysql
except ImportError:
    sys.exit("pip install pymysql")

# selectivity -> (varchar_range, bigint_range)
RANGES = [
    ("0.0005%", ("user_id = '12345'",                         "user_id = 12345")),
    ("0.55%",   ("user_id BETWEEN '12345' AND '12444'",       "user_id BETWEEN 12345 AND 13444")),
    ("5.5%",    ("user_id BETWEEN '14000' AND '14999'",       "user_id BETWEEN 12345 AND 23444")),
    ("11.1%",   ("user_id BETWEEN '13000' AND '14999'",       "user_id BETWEEN 12345 AND 34544")),
    ("27.8%",   ("user_id BETWEEN '10000' AND '14999'",       "user_id BETWEEN 12345 AND 67944")),
]

def pctl(v, q):
    v = sorted(v); k = (len(v)-1)*q; f = int(k); c = min(f+1, len(v)-1)
    return v[f] if f == c else v[f] + (v[c]-v[f])*(k-f)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True)
    ap.add_argument("--table", default="orders")
    ap.add_argument("--bigint", action="store_true")
    ap.add_argument("--runs", type=int, default=15)
    ap.add_argument("--warmup", type=int, default=3)
    a = ap.parse_args()
    c = pymysql.connect(host=os.environ["FE_HOST"], port=int(os.environ.get("FE_PORT","9030")),
                        user=os.environ.get("USER","root"), password=os.environ.get("PASSWORD",""),
                        db=os.environ.get("DB","bench_sidx")).cursor()
    for sel, (vr, br) in RANGES:
        pred = br if a.bigint else vr
        for shape, sql in (("cnt",  f"SELECT COUNT(*) FROM {a.table} WHERE {pred}"),
                           ("town", f"SELECT MAX(town) FROM {a.table} WHERE {pred}")):
            s = []
            for i in range(a.runs):
                t = time.perf_counter(); c.execute(sql); c.fetchall()
                if i >= a.warmup: s.append((time.perf_counter()-t)*1000)
            print(f"[{a.label}] {sel:8s} {shape:4s} p50={pctl(s,0.50):8.1f}ms")

if __name__ == "__main__":
    sys.exit(main())
