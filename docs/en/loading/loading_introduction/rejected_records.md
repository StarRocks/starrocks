---
sidebar_position: 60
displayed_sidebar: docs
sidebar_label: "Rejected records"
keywords: ['rejected', 'records', 'max_filter_ratio', 'replay']
description: "How StarRocks persists filtered rows into _statistics_.rejected_records when max_filter_ratio > 0, and how to query and replay rejected records."
---

# Rejected records

For load jobs with a non-zero `max_filter_ratio`, the system can be configured to persist every filtered row into the system table **`_statistics_.rejected_records`**, allowing you to inspect bad data and replay it into the target table without re-running the whole job. This topic shows how to enable the rejected-record capture feature, query the table, and replay rejected rows with SQL.

`_statistics_.rejected_records` provides information of rejected data from:

- Stream Load, Routine Load, Broker Load, and `INSERT` (including `INSERT INTO ... SELECT ... FROM FILES()`)
- Scanner parse failures (CSV column-count mismatches, type conversions, strict-mode filtering)
- Sink constraint violations (NOT NULL, partition-range misses, VARCHAR length, decimal precision)
- ORC reader row rejections (columnar formats, before the filter is applied)

## Enable rejected-record capture

Rejected-record capture is disabled by default so that new clusters do not write to the system table until you explicitly enable it. 

To enable this feature, follow these steps:

1. Enable the rejected record synchronization (into the system table) by setting the BE configuration `enable_rejected_record_sync` to `true`.

   - To change it dynamically, execute the following SQL:

     ```SQL
     UPDATE information_schema.be_configs SET VALUE = "true" WHERE name = "enable_rejected_record_sync";
     ```

   - To change it permanently, add the following configuration in `be.conf`, and restart the BE service:

     ```Properties
     enable_rejected_record_sync=true
     ```

2. Set `log_rejected_record_num` to a positive number (a cap) or `-1` (unlimited) for your session or your load job.

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

The rejected records will be synchronized to `_statistics_.rejected_records` for further operations.

## Query `_statistics_.rejected_records`

You can first view the table schema of `_statistics_.rejected_records` by executing the following SQL:

```SQL
DESC _statistics_.rejected_records;
```

You can focus on these columns for day-to-day triage:

| Column       | Description                             |
| ------------ | --------------------------------------- |
| `raw_record` | The rejected row, in JSON format, keyed by column name. |
| `error_code`<br />`error_message`<br />`error_column` | The reason that the row was rejected. |
| `load_label`<br />`load_type`<br />`txn_id` | The label, type, and transaction ID of the load job that produced the rejected row. `user_name` is left NULL on Broker Load, Routine Load, and INSERT jobs. You can join `_statistics_.rejected_records` and `_statistics_.loads_history` on `load_label` to obtain `user_name`. |
| `source_info` | Information on the load source. `file` and `line` for file loading, and `topic`, `partition`, and `offset` for Routine Load. |
| `created_at` | The time when the record is generated. Partition key of the system table. You are recommended to filter on this column first. |

The following examples shows some basic usages of the table:

- View the rejected rows for a specific load, and lists the most recent record first.

  ```SQL
  SELECT created_at, error_code, error_column, error_message, raw_record
  FROM _statistics_.rejected_records
  WHERE load_label = 'load_orders_20260327'
  ORDER BY created_at DESC
  LIMIT 100;
  ```

- View the error distribution for a target table over the last 24 hours.

```SQL
SELECT error_code, error_column, COUNT(*) AS cnt
FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table = 'orders'
  AND created_at >= NOW() - INTERVAL 1 DAY
GROUP BY error_code, error_column
ORDER BY cnt DESC;
```

- View all rejected rows for a load job based on a Join query with `information_schema.loads`. You must join them on the load label because `information_schema.loads` does not expose `txn_id`.

```SQL
SELECT r.created_at, r.error_code, r.raw_record, l.state, l.scan_rows
FROM _statistics_.rejected_records AS r
JOIN information_schema.loads AS l
  ON r.load_label = l.label
WHERE r.load_label = 'my_load_label_2026_04_28';
```

## Replay rejected rows

The `raw_record` column is a JSON string of the rejected row's column values keyed by name. Use the `->>` operator to extract a value as a string and `CAST(... AS <type>)` to recover the target type.

The following example fixes VARCHAR length violations by truncating, and INSERT the value to the target table.

```SQL
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

When the scanner could not split the row at all (for example a CSV with a column-count mismatch), `raw_record` carries a single key `_raw` with the raw line.

The following example shows the first 20 unparsable lines for diagnosis.

```SQL
SELECT raw_record->>'_raw' AS raw_line
FROM _statistics_.rejected_records
WHERE error_code = 'PARSE_ERROR'
ORDER BY created_at DESC
LIMIT 20;
```

## Data retention and cleanup

`_statistics_.rejected_records` is partitioned by day and auto-expires partitions based on the property `partition_live_number = 7`. You can adjust the retention by changing the FE configuration `rejected_records_retained_days` (default `7`). The table-keeper
daemon reconciles the live table property on its next tick.

The following example deletes the records on a specific target table that are earlier than a specific date:

```sql
DELETE FROM _statistics_.rejected_records
WHERE target_database = 'mydb'
  AND target_table    = 'orders'
  AND created_at      < '2026-03-01';
```

## Permissions

Access to `_statistics_.rejected_records` is controlled by a built-in row access policy:

- The built-in **`root`** user sees every row in the table (the policy applies no filter).
- **All other users** -- including those with the `db_admin`, `cluster_admin`, `user_admin`, and `security_admin` roles -- see only rows whose `target_database.target_table` they have the `SELECT` privilege. Rows for tables the user has no SELECT privilege on are filtered out of the result set.
- If the policy cannot resolve or validate the target for a row, that row is hidden (fail-closed).

For operator-run dashboards, either use an admin account if full visibility is required, or grant `SELECT` on the relevant target tables to the reporting role.

## Limitations

- **Parquet rejected records carry an anchor, not the full row.**

  Parquet loads record the offending column's raw value in a single-column `raw_record` fragment **plus** a source anchor in `source_info`:

  ```json
  {
    "format": "parquet",
    "file": "gs://bucket/orders.parquet",
    "row_in_file": 1817542,
    "file_size": 12345678,
    "file_mtime_ms": 1711531331000
  }
  ```

  `raw_record` alone is enough to diagnose what went wrong column by column. For full-row replay a follow-up commit will ship a `parquet_read_rows(file, anchors)` TVF that rehydrates the full row by re-reading the source file using the anchor. Until that TVF lands, the anchor is still useful for pointing users at the exact row in the original Parquet file (`row_in_file` is 0-based) and for validating that the source file has not changed (`file_size` and `file_mtime_ms` are snapshotted at scan open and should be compared before attempting any manual rehydration).

- **`information_schema.loads.rejected_record_path` is deprecated.**

  The BE-local tab-delimited rejected-record file it used to point at was removed; the column is kept for upgrade compatibility but is always `NULL`. Query `_statistics_.rejected_records` directly by `load_label` or `txn_id` instead.

- **Second-level delay.**

  Rejected rows become queryable in the system table within `rejected_record_sync_interval_sec` (default 30 s) after the load completes, not immediately.

## Related configuration

| Scope            | Parameter                               | Default | Description                                                |
| ---------------- | --------------------------------------- | ------- | ---------------------------------------------------------- |
| Session Variable | `log_rejected_record_num`               | `0`     | The number of rejected records to log. `0` indicates to disable this feature. `-1` indicates to log unlimited records. |
| FE Configuration | `rejected_records_retained_days`        | `7`     | Daily partitions retained in `_statistics_.rejected_records`. |
| BE Configuration | `enable_rejected_record_sync`           | `false` | Master switch of the daemon that synchronize rejected rows to `_statistics_.rejected_records`. |
| BE Configuration | `rejected_record_sync_interval_sec`     | `30`    | The tick interval for the synchronization.                 |
| BE Configuration | `rejected_record_sync_max_batch_rows`   | `10000` | The soft cap on rows per Merge Commit batch.               |
| BE Configuration | `rejected_record_local_retention_hours` | `24`    | Local file GC for the records that cannot be synchronized. |
| BE Configuration | `rejected_record_sync_post_timeout_sec` | `60`    | Per-request Stream Load timeout.                           |
