#!/usr/bin/env python3
"""Drive the query side of the secondary-index benchmark.

Runs each query in QUERIES N times, drops the warm-up runs, and emits
one CSV row per query with p50 / p95 / p99 latency in milliseconds.

Required env:
    FE_HOST, FE_PORT (default 9030), USER (default root), PASSWORD, DB (default bench_sidx)
"""

import argparse
import os
import statistics
import sys
import time

try:
    import pymysql  # type: ignore
except ImportError:
    sys.exit("pip install pymysql to run this benchmark")


QUERIES = [
    ("eq_town_high_sel",        "SELECT COUNT(*) FROM orders WHERE town = 'Brighton'"),
    ("eq_town_low_sel",         "SELECT COUNT(*) FROM orders WHERE town = 'London'"),
    ("eq_town_with_proj",       "SELECT order_id, amount FROM orders WHERE town = 'Edinburgh' LIMIT 1000"),
    ("range_ts_one_day",        "SELECT COUNT(*) FROM orders WHERE create_ts >= '2025-06-01 00:00:00' AND create_ts < '2025-06-02 00:00:00'"),
    ("range_ts_one_week",       "SELECT COUNT(*) FROM orders WHERE create_ts >= '2025-06-01 00:00:00' AND create_ts < '2025-06-08 00:00:00'"),
    ("composite_town_type",     "SELECT COUNT(*) FROM orders WHERE town = 'Manchester' AND type = 'flat'"),
    ("baseline_full_count",     "SELECT COUNT(*) FROM orders"),  # control -- index should NOT help
    ("pk_eq",                   "SELECT * FROM orders WHERE order_id = 17424219"),  # control -- pindex path
]


def percentile(values, q):
    if not values:
        return float("nan")
    values = sorted(values)
    k = (len(values) - 1) * q
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    return values[f] + (values[c] - values[f]) * (k - f)


def run_one(cursor, sql, runs, warmup):
    samples_ms = []
    for i in range(runs):
        t0 = time.perf_counter()
        cursor.execute(sql)
        cursor.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if i >= warmup:
            samples_ms.append(elapsed_ms)
    return samples_ms


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True, help="run label, e.g. 'index_off' or 'index_on'")
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--out", default="-", help="output CSV path, '-' for stdout")
    args = parser.parse_args()

    host = os.environ["FE_HOST"]
    port = int(os.environ.get("FE_PORT", "9030"))
    user = os.environ.get("USER", "root")
    password = os.environ.get("PASSWORD", "")
    db = os.environ.get("DB", "bench_sidx")

    conn = pymysql.connect(host=host, port=port, user=user, password=password, db=db)
    cursor = conn.cursor()

    out = sys.stdout if args.out == "-" else open(args.out, "w")
    out.write("label,query,runs,p50_ms,p95_ms,p99_ms,min_ms,max_ms\n")
    for name, sql in QUERIES:
        samples = run_one(cursor, sql, args.runs, args.warmup)
        if not samples:
            continue
        out.write(
            f"{args.label},{name},{len(samples)},"
            f"{percentile(samples, 0.50):.2f},"
            f"{percentile(samples, 0.95):.2f},"
            f"{percentile(samples, 0.99):.2f},"
            f"{min(samples):.2f},{max(samples):.2f}\n"
        )
        out.flush()
        print(
            f"[{args.label}] {name:24s} p50={percentile(samples, 0.50):8.2f}ms "
            f"p95={percentile(samples, 0.95):8.2f}ms p99={percentile(samples, 0.99):8.2f}ms",
            file=sys.stderr,
        )
    if out is not sys.stdout:
        out.close()
    cursor.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
