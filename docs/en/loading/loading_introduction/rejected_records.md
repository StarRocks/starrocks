---
displayed_sidebar: docs
sidebar_label: "Rejected records"
keywords: ['rejected', 'records', 'max_filter_ratio', 'replay']
---

# Rejected records

When a load tolerates a non-zero `max_filter_ratio`, StarRocks persists
every filtered row into the **`_statistics_.rejected_records`** system table
so operators can inspect bad data and replay it into the target table
without re-running the whole load. This page shows how to enable the
feature, query the table, and replay rejected rows with SQL.

The system table covers row-level rejections from:

- Stream Load, Routine Load, Broker Load, and `INSERT` (including
  `INSERT INTO ... SELECT ... FROM FILES()`)
- Scanner parse failures (CSV column-count mismatches, type conversions,
  strict-mode filtering)
- Sink constraint violations (NOT NULL, partition-range misses, VARCHAR
  length, decimal precision)
- ORC reader row rejections (columnar formats, before the filter is applied)

## Enable rejected-record capture

Rejected-record capture is off by default so that new clusters do not
produce system-table writes until you explicitly opt in. To enable the
feature, two knobs must both be set:

1. **Per-load**: set `log_rejected_record_num` to a positive number (a
   cap) or `-1` (unlimited) on the load itself.

   ```sql
   -- Session-level for INSERT / INSERT ... SELECT
   SET log_rejected_record_num = -1;

   -- Broker Load property
   LOAD LABEL mydb.my_label ( ... )
   PROPERTIES (
       "log_rejected_record_num" = "10000"
   );

   -- Routine Load property
   CREATE ROUTINE LOAD mydb.my_job ON my_table ...
   PROPERTIES (
       "log_rejected_record_num" = "10000",
       ...
   );

   -- Stream Load header
   curl -H "log_rejected_record_num: 10000" ...
   ```

2. **Cluster-level**: set the BE config `enable_rejected_record_sync` to
   `true`. Without this, rejected rows are still captured to BE local
   files but the sync daemon does not ship them to
   `_statistics_.rejected_records`:

   ```bash
   curl -X POST "http://<be>:<be_http_port>/api/update_config?enable_rejected_record_sync=true"
   ```

   Or set it permanently in `be.conf` before restart:

   ```
   enable_rejected_record_sync=true
   ```

## Query the table

Run `DESCRIBE _statistics_.rejected_records;` to see the full schema.
The most useful columns for day-to-day triage:

- `raw_record` (JSON) – the offending row, keyed by column name
- `error_code`, `error_message`, `error_column` – why the row was rejected
- `load_label`, `load_type`, `txn_id` – who produced it. `user_name` is
  left NULL on Broker/Routine/INSERT loads (the BE plan fragment does
  not receive the submitting user today); join
  `_statistics_.loads_history` by `load_label` to recover it
- `source_info` (JSON) – file + line for file loads, topic/partition/offset for Routine Load
- `created_at` – partition key; always filter on this first

```sql
-- Rejected rows for a specific load, most-recent-first
SELECT created_at, error_code, error_column, error_message, raw_record
FROM _statistics_.rejected_records
WHERE load_label = 'load_orders_20260327'
ORDER BY created_at DESC
LIMIT 100;

-- Error distribution for a target table over the last 24 hours
SELECT error_code, error_column, COUNT(*) AS cnt
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND created_at >= NOW() - INTERVAL 1 DAY
GROUP BY error_code, error_column
ORDER BY cnt DESC;

-- All rejected rows for a load, joined with the load record. Note that
-- `information_schema.loads` does not expose `txn_id`, so use the load
-- label as the join key (both sides use it as the stable per-load
-- handle).
SELECT r.created_at, r.error_code, r.raw_record, l.state, l.scan_rows
FROM _statistics_.rejected_records AS r
JOIN information_schema.loads AS l
  ON r.load_label = l.label
WHERE r.load_label = 'my_load_label_2026_04_28';
```

## Replay rejected rows

The `raw_record` column is JSON with the offending row's column values
keyed by name. Use the `->>` operator to extract a value as a string
and `CAST(... AS <type>)` to recover the target type.

```sql
-- Fix VARCHAR length violations by truncating before re-insert.
INSERT INTO mydb.orders (order_id, customer_name, amount, created_at)
SELECT
    CAST(raw_record->>'order_id'      AS BIGINT),
    LEFT(raw_record->>'customer_name', 64),
    CAST(raw_record->>'amount'        AS DECIMAL(10,2)),
    CAST(raw_record->>'created_at'    AS DATETIME)
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND error_code    = 'VALUE_OUT_OF_RANGE'
  AND created_at    > '2026-03-27';
```

When the scanner could not split the row at all (for example a CSV with
a column-count mismatch), `raw_record` carries a single key `_raw` with
the raw line:

```sql
-- Show the first 20 unparseable lines for diagnosis.
SELECT raw_record->>'_raw' AS raw_line
FROM _statistics_.rejected_records
WHERE error_code = 'PARSE_ERROR'
ORDER BY created_at DESC
LIMIT 20;
```

## Retention and cleanup

The table is partitioned by day and auto-expires partitions via
`partition_live_number = 7`. Adjust the retention by changing the FE
config `rejected_records_retained_days` (default `7`); the table-keeper
daemon reconciles the live table property on its next tick. For
one-off cleanup:

```sql
DELETE FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table    = 'orders'
  AND created_at      < '2026-03-01';
```

## Permissions

Access to `_statistics_.rejected_records` is controlled by a built-in
row access policy:

- **Admin users** (e.g. `root`, or any role with the admin privilege)
  see all rows.
- **Non-admin users** see only rows whose `target_database.target_table`
  they can `SELECT` on. Rows for tables the user has no SELECT privilege
  on are filtered out of the result set.
- If the policy cannot resolve or validate the target for a row, that
  row is hidden (fail-closed).

For operator-run dashboards, either use an admin account if full
visibility is required, or grant `SELECT` on the relevant target tables
to the reporting role.

## Limitations

- **Parquet rejected records carry an anchor, not the full row.**
  Parquet loads record the offending column's raw value in a
  single-column `raw_record` fragment **plus** a source anchor in
  `source_info`:

  ```json
  {
    "format": "parquet",
    "file": "gs://bucket/orders.parquet",
    "row_in_file": 1817542,
    "file_size": 12345678,
    "file_mtime_ms": 1711531331000
  }
  ```

  `raw_record` alone is enough to diagnose what went wrong column by
  column. For full-row replay a follow-up commit will ship a
  `parquet_read_rows(file, anchors)` TVF that rehydrates the full row
  by re-reading the source file using the anchor. Until that TVF
  lands, the anchor is still useful for pointing users at the exact
  row in the original Parquet file (`row_in_file` is 0-based) and for
  validating that the source file has not changed (`file_size` and
  `file_mtime_ms` are snapshotted at scan open and should be compared
  before attempting any manual rehydration).
- **`information_schema.loads.rejected_record_path` is deprecated.**
  The BE-local tab-delimited rejected-record file it used to point at
  was removed; the column is kept for upgrade compatibility but is
  always `NULL`. Query `_statistics_.rejected_records` directly by
  `load_label` or `txn_id` instead.
- **Second-level delay.** Rejected rows become queryable in the system
  table within `rejected_record_sync_interval_sec` (default 30 s) after
  the load completes, not immediately.

## Related configuration

| Scope | Name | Default | Notes |
| --- | --- | --- | --- |
| Session | `log_rejected_record_num` | `0` | `0` = disabled, `-1` = unlimited, `N` = cap to `N` |
| FE | `rejected_records_retained_days` | `7` | Daily partitions retained in the system table |
| BE | `enable_rejected_record_sync` | `false` | Master switch for the sync daemon |
| BE | `rejected_record_sync_interval_sec` | `30` | Sync tick interval |
| BE | `rejected_record_sync_max_batch_rows` | `10000` | Soft cap on rows per merge-commit batch |
| BE | `rejected_record_local_retention_hours` | `24` | Local file GC for un-syncable records |
| BE | `rejected_record_sync_post_timeout_sec` | `60` | Per-request Stream Load timeout |
