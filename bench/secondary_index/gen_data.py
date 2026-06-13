#!/usr/bin/env python3
"""Generate a deterministic CSV dataset for the secondary-index benchmark.

Schema lines up with bench/secondary_index/setup_table.sql. Column
distributions are chosen so the indexed columns have predictable
selectivities, which makes p99 / scanned-rows numbers reproducible across
runs.
"""

import argparse
import datetime
import random
import sys


TOWNS = [
    "London", "Manchester", "Birmingham", "Leeds", "Liverpool",
    "Bristol", "Sheffield", "Newcastle", "Nottingham", "Leicester",
    "Edinburgh", "Glasgow", "Cardiff", "Belfast", "Aberdeen",
    "Plymouth", "Southampton", "Coventry", "Brighton", "Hull",
]
TYPES = ["flat", "terraced", "semi-detached", "detached", "other"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=30_000_000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", required=True)
    p.add_argument("--start-date", default="2024-01-01")
    p.add_argument("--end-date", default="2026-05-22")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    start = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
    span_seconds = int((end - start).total_seconds())

    with open(args.out, "w", buffering=1024 * 1024) as f:
        # NB: no header row -- Stream Load is column-positional in this bench
        for i in range(args.rows):
            order_id = i + 1
            ts = start + datetime.timedelta(seconds=rng.randint(0, span_seconds))
            user_id = rng.randint(1, 200_000)
            town = TOWNS[rng.randrange(len(TOWNS))]
            type_ = TYPES[rng.randrange(len(TYPES))]
            amount = round(rng.uniform(1_000, 5_000_000), 2)
            desc = f"order-{order_id}-{rng.randrange(1 << 24):x}"
            extra1 = f"e1-{rng.randrange(1 << 20):x}"
            extra2 = f"e2-{rng.randrange(1 << 20):x}"
            extra3 = f"e3-{rng.randrange(1 << 20):x}"
            f.write(
                f"{order_id},{ts.strftime('%Y-%m-%d %H:%M:%S')},{user_id},"
                f"{town},{type_},{amount},{desc},{extra1},{extra2},{extra3}\n"
            )
            if i and i % 1_000_000 == 0:
                print(f"  ... wrote {i:,} rows", file=sys.stderr)

    print(f"wrote {args.rows:,} rows to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
