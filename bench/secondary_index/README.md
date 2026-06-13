# Secondary Index PoC Benchmark

End-to-end benchmark scripts that measure the cost / benefit of the
rowset-level secondary index on a Lake (shared-data) PK table.

## What gets measured

| Phase | Metric | How |
|-------|--------|-----|
| Load   | rows/s + total seconds | Time a fixed-volume Stream Load with index OFF then ON |
| Load   | index file count + bytes per tablet | `ls` against the fileset / Lake metadata |
| Query  | p50/p95/p99 latency | Repeat each query N=50 times, drop top-3, take percentiles |
| Query  | rows scanned by segment iterator | BE metric `secondary_index.candidate_rows` etc. |

## Schema

```sql
CREATE TABLE orders (
    order_id   BIGINT  NOT NULL,
    create_ts  DATETIME NOT NULL,
    user_id    BIGINT  NOT NULL,
    town       VARCHAR(64) NOT NULL,
    type       VARCHAR(16) NOT NULL,
    amount     DECIMAL(18,2),
    desc_blob  VARCHAR(256),   -- pad row width
    extra1     VARCHAR(64),
    extra2     VARCHAR(64),
    extra3     VARCHAR(64)
)
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES (
    "storage_type" = "lake",
    "replication_num" = "1"
);
```

Default scale: **30M rows** (~7 GB at this width). One bucket therefore
holds ~7.5M rows split across multiple segments.

## Files

| File | Purpose |
|------|---------|
| `gen_data.py` | Pseudo-random CSV generator. Deterministic via `--seed`. |
| `run_load.sh`     | Drives one Stream Load and reports throughput. Wraps curl. |
| `run_query.py`    | Repeats query batches and emits CSV `query,p50,p95,p99,scanned_rows`. |
| `setup_table.sql` | DDL + the `set_config` HTTP calls that enable the index. |
| `bench.sh`        | Full driver: load (off) → load (on) → query (off) → query (on). |

## Running

```bash
# 0. Build BE + deploy to a Lake cluster (out of scope here).
#    Once the cluster is up, set FE_HOST, BE_HTTP and TABLET_ID env vars.

export FE_HOST=fe-1.example          # for mysql + stream load
export FE_PORT=9030
export BE_HTTP=be-1.example:8040     # for /api/update_config + stream load redirects
export TABLET_ID=$(./tablet_id_of.sh orders)

# 1. DDL + initial config
mysql -h $FE_HOST -P $FE_PORT -u root < setup_table.sql

# 2. Generate dataset (3M default, can pass --rows)
python3 gen_data.py --rows 30000000 --seed 42 --out /tmp/orders.csv

# 3. Run the full matrix
./bench.sh /tmp/orders.csv

# Results land in ./out/<timestamp>/
```

## Knobs the benchmark exercises

- `enable_secondary_index_write` (BE)
- `enable_secondary_index_read`  (BE)
- `secondary_index_defs`         (BE, the per-tablet registry)
- `secondary_index_build_mem_limit_mb` (BE, sanity check)
- `TabletReaderParams::use_secondary_index` — opted in via a session var hack
  on `params.use_secondary_index` that we plumb through OlapScanNode for the
  benchmark queries.

## Limitations of the current PoC for this benchmark

- DDL has no `CREATE INDEX` syntax yet; the per-tablet registry has to be
  manually populated via `/api/update_config?secondary_index_defs=...`
  after the table is created and we have the tablet IDs.
- The session var that flips `use_secondary_index` is also temporary;
  bench drives queries through a small wrapper that sets the BE config
  `enable_secondary_index_read` globally for the read window.
