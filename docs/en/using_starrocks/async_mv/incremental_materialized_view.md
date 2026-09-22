---
displayed_sidebar: docs
description: "How to create, use, refresh, and monitor incremental materialized views on native tables, for fresh results at delta-sized refresh cost."
sidebar_position: 2
keywords: ['zengliang', 'wuhua', 'IMV', 'zengliangshuaxin']
---

# Incremental Materialized Views

Create, use, refresh, monitor, and troubleshoot incremental materialized views (IMVs) in shared-data clusters.

An incremental materialized view is a **strict incremental refresh mode** of asynchronous materialized views: the initial refresh establishes a full baseline, and each subsequent refresh consumes only the changes since the last successful refresh and merges the results into the materialized view. If a change or query shape cannot be safely maintained incrementally, the system explicitly returns an error instead of silently falling back to a full recomputation.

:::note

- Incremental materialized views are supported only on cloud-native tables and and Iceberg append-only external tables in shared-data clusters. Shared-nothing clusters are not supported.
- Incremental materialized views do not participate in automatic query rewrite. You must query the materialized view directly.
- Incremental View Maintenance (IVM) is used as the internal name in source code and error messages, while IMV is used in system table fields and product documentation. Both refer to the same capability.
:::

## Background

### Why incremental materialized views are needed

Asynchronous materialized views use PCT (Partition Change Tracking) refresh by default. When data in a partition of a base table changes, the entire partition is recomputed.

This creates a structural problem: **refresh cost is proportional to the total amount of data in the affected partitions, rather than to the amount of changed data**. For example, if a daily partition contains 30 million rows, adding only 10,000 rows still requires all 30 million rows to be recomputed. As historical data accumulates, refresh time continuously increases. Eventually, you may have to reduce the refresh frequency to control the cost, which comes at the expense of data freshness.

Incremental materialized views flatten this curve: **refresh cost depends only on the amount of data changed in the current batch and is decoupled from the total amount of historical data**.

Incremental materialized views are especially useful in the following scenarios:

- Existing asynchronous materialized views whose refresh cost continuously increases with data volume.
- Dashboards with strict freshness requirements.
- Operational, advertising, transaction, or IoT aggregation reports over large detail tables where the base table is primarily append-only.

### Differences from Other materialized views

|                                | Single-table aggregation        | Multi-table join      | Query rewrite | Refresh cost                                | Refresh strategy                                             | Base tables                                                  |
| ------------------------------ | ------------------------------- | --------------------- | ------------- | ------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Incremental materialized view  | Supported (with restrictions)   | INNER/CROSS JOIN only | Not supported | ∝ current batch of changes                  | <ul><li>Triggered by base-table changes</li><li>Scheduled refresh</li><li>Manual refresh</li></ul> | Shared-data native tables (DUPLICATE / PRIMARY / AGGREGATE KEY), Iceberg append-only tables |
| Asynchronous materialized view | Supported                       | Supported             | Supported     | ∝ total data in affected partitions         | <ul><li>Scheduled refresh</li><li>Manual refresh</li></ul>   | Supports multi-table definitions; can be based on the Default Catalog, External Catalog, existing materialized views, and existing views |
| Synchronous materialized view  | Only some aggregation functions | Not supported         | Supported     | Refreshed synchronously with data ingestion | Synchronous refresh during ingestion                         | Single-table definitions based only on the Default Catalog   |

### Related concepts

- **Change Data Capture (CDC)**: A mechanism that derives row-level changes from tablet metadata of shared-data tables. DUPLICATE KEY and AGGREGATE KEY tables can derive changes natively without additional configuration or write overhead. PRIMARY KEY tables must explicitly set the table property `enable_change_data_capture = 'true'` so that lightweight metadata required to locate changes is recorded during each ingestion operation.

- **Delta window**: The range of changes consumed by one incremental refresh, represented as `(previous refresh position, current position]`. For native tables, the position is a **bookmark ID**; for Iceberg tables, it is the snapshot ID. Delta windows are contiguous, with no gaps or overlaps.

- **Bookmark**: A version reference held by the materialized view for an native table. It ensures that incremental refreshes read from a consistent historical version. The trade-off is that **old base-table files cannot be reclaimed until they are no longer referenced**. See [Costs and Overheads](#costs-and-overheads).

- **Net change**: When the same row is modified multiple times within one window, only the pair consisting of "deletion of the value at the beginning of the window + insertion of the value at the end of the window" is retained. Rows that are created and deleted within the same window do not appear at all. Incremental refresh **always consumes net changes**.

- **Retraction**: Reflecting `DELETE` / `UPDATE` operations on the base table in the materialized view. Because the materialized view is always a PRIMARY KEY table, corresponding rows can be precisely updated using UPSERT / DELETE semantics.

### Major limitations

To guarantee strict incremental semantics, incremental materialized views have limitations in several areas. Verify these limitations during solution design before adopting the feature. See [Capability Boundaries](#capability-boundaries) for the complete list.

- **Does not participate in query rewrite**

  Query rewrite is disabled regardless of rewrite-related configuration and cannot be enabled by a switch. If you want existing business SQL to be accelerated without modification, evaluate the migration cost: all consumers must explicitly query the materialized view. If this cost is too high, continue using asynchronous materialized views (PCT).

- **Supported only in shared-data clusters**

  Shared-nothing clusters are not supported and there is no workaround. Real-time report acceleration on shared-nothing clusters requires planning a different cluster architecture or continuing to use asynchronous materialized views (PCT).

- **Partition-level or table-level destructive operations permanently break the incremental chain**

  `INSERT OVERWRITE`, `DROP PARTITION`, and `TRUNCATE` cause refresh failures and deactivate the materialized view. The materialized view must be dropped and recreated. If the base table has scheduled retention-based `DROP PARTITION` jobs or uses `INSERT OVERWRITE` for backfills, design a compatible workflow first, such as separating the affected partition ranges, or accept periodic materialized-view rebuilds. **Row-level `DELETE` / `UPDATE` on a PRIMARY KEY base table with CDC enabled is supported normally** and is not affected by this limitation.

- **Limited SQL support**

  OUTER / SEMI / ANTI JOIN, window functions, CTEs, multi-level nested subqueries, `LIMIT`, `HAVING` containing aggregates, `GROUPING SETS` / `ROLLUP` / `CUBE`, global aggregation without `GROUP BY`, and other unsupported operators are rejected. Only 13 aggregation functions are allowlisted. If reports use `count(distinct)`, `stddev`, `variance`, `percentile_approx`, `min_by`, and similar functions, rewrite the queries. Use `bitmap_union(to_bitmap(col))` for exact distinct counting and `hll_union(hll_hash(col))` for approximate distinct counting. Expensive statistics such as percentiles and standard deviation should generally remain in drill-down queries over the raw table. Unsupported queries should continue to use asynchronous materialized views (PCT).

- **Restricted base-table type**

  UNIQUE KEY tables are not supported. AGGREGATE KEY tables support strict rollup only. Materialized views cannot be used as base tables (MV on MV).For acceleration on UNIQUE KEY tables, consider migrating to the PRIMARY KEY model. Existing multi-level materialized-view chains must be flattened so that each materialized view directly references the base table.

- **Cannot switch between PCT and INCREMENTAL in place**

  `refresh_mode` cannot be changed across modes using `ALTER`. You cannot "try it at low cost first and switch back later." Validate the design in a test environment or with a shadow materialized view before changing production workloads.

## Create an incremental materialized view

### Syntax

To create an incremental materialized view, specify `refresh_mode` as `INCREMENTAL` in `PROPERTIES`:

```SQL
CREATE MATERIALIZED VIEW <mv_name>
[PARTITION BY <expr>]
[DISTRIBUTED BY HASH(<col>) [BUCKETS <n>]]
REFRESH [IMMEDIATE | DEFERRED]
    { ASYNC
    | { ASYNC | SCHEDULE } [START('<start_time>')] EVERY(INTERVAL <n> <unit>)
    | MANUAL }
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS <query>;
```

:::note

- Valid `refresh_mode` values are `PCT` (default) and `INCREMENTAL`. Any other value returns `Invalid refresh_mode: <v>. Only INCREMENTAL, PCT are supported.`
- `START('<start_time>')` **can be used only together with `EVERY(...)`** and cannot appear by itself.
- `IMMEDIATE` / `DEFERRED` apply to the entire refresh configuration, including `ASYNC`; they are not limited to `MANUAL`.
- If you use PRIMARY KEY tables as the base tables, they must have CDC enabled by setting the table property `enable_change_data_capture` to `true`.
:::

### Example 1: Single-table filter and projection (DUPLICATE KEY base table)

```SQL
CREATE TABLE base_dup (
    k INT,
    v INT
) DUPLICATE KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 1;

INSERT INTO base_dup VALUES (1, 10), (2, 20), (3, 30);

CREATE MATERIALIZED VIEW mv_dup
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT k, v FROM base_dup WHERE v > 15;

REFRESH MATERIALIZED VIEW mv_dup WITH SYNC MODE;
SELECT k, v FROM mv_dup ORDER BY k;
```

```Plain
+------+------+
| k    | v    |
+------+------+
|    2 |   20 |
|    3 |   30 |
+------+------+
```

After additional data is inserted, only new rows satisfying the predicate are incrementally written:

```SQL
INSERT INTO base_dup VALUES (4, 40), (5, 50), (6, 5);
REFRESH MATERIALIZED VIEW mv_dup WITH SYNC MODE;
SELECT k, v FROM mv_dup ORDER BY k;
```

```Plain
+------+------+
| k    | v    |
+------+------+
|    2 |   20 |
|    3 |   30 |
|    4 |   40 |
|    5 |   50 |
+------+------+
```

### Example 2: Single-table aggregation (DUPLICATE KEY base table)

The following example covers most of the allowlisted aggregation functions:

```SQL
CREATE TABLE base_orders (
    order_id       BIGINT,
    region         VARCHAR(8),
    amount         DECIMAL(10,2),
    score          BIGINT,
    nullable_score INT,
    flag           BOOLEAN,
    uid            INT
) DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1;

INSERT INTO base_orders VALUES
    (1001, 'CN', 99.90, 10,    5, FALSE,  1),
    (1002, 'US', 20.50, 20, NULL, FALSE, 10),
    (1003, 'CN', 15.50, 30, NULL, TRUE,   1),
    (1004, 'US', 75.00, 40,    8, FALSE, 10);

CREATE MATERIALIZED VIEW mv_orders
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT region,
          SUM(amount)                AS s,
          COUNT(*)                   AS c_all,
          COUNT(nullable_score)      AS c_non_null,
          AVG(amount)                AS avg_dec,
          MIN(amount)                AS mn,
          MAX(amount)                AS mx,
          APPROX_COUNT_DISTINCT(uid) AS acd,
          BOOL_OR(flag)              AS any_flag
   FROM base_orders
   GROUP BY region;

REFRESH MATERIALIZED VIEW mv_orders WITH SYNC MODE;

-- Add a new JP group and append data to the existing CN group
INSERT INTO base_orders VALUES
    (1005, 'JP', 100.00, 50,   12, FALSE, 99),
    (1006, 'CN',  25.00, 60, NULL, FALSE,  2);
REFRESH MATERIALIZED VIEW mv_orders WITH SYNC MODE;
SELECT * FROM mv_orders ORDER BY region;
```

Only the `CN` and `JP` groups are recomputed. The `US` group is not touched.

### Example 3: INNER JOIN (two DUPLICATE KEY base tables)

```SQL
CREATE TABLE orders (
    order_id BIGINT, uid BIGINT, amount DECIMAL(10,2)
) DUPLICATE KEY(order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 1;

CREATE TABLE users (
    user_id BIGINT, country VARCHAR(8)
) DUPLICATE KEY(user_id) DISTRIBUTED BY HASH(user_id) BUCKETS 1;

INSERT INTO users  VALUES (1, 'CN'), (2, 'US');
INSERT INTO orders VALUES (1001, 1, 99.90), (1002, 2, 20.50);

CREATE MATERIALIZED VIEW mv_join
DISTRIBUTED BY HASH(order_id) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT o.order_id, u.country, o.amount
   FROM orders o JOIN users u ON o.uid = u.user_id;

REFRESH MATERIALIZED VIEW mv_join WITH SYNC MODE;

-- Incrementally update both the dimension and fact tables
INSERT INTO users  VALUES (3, 'JP');
INSERT INTO orders VALUES (1003, 1, 15.50), (1004, 3, 200.00);
REFRESH MATERIALIZED VIEW mv_join WITH SYNC MODE;
SELECT * FROM mv_join ORDER BY order_id;
```

```Plain
+----------+---------+--------+
| order_id | country | amount |
+----------+---------+--------+
|     1001 | CN      |  99.90 |
|     1002 | US      |  20.50 |
|     1003 | CN      |  15.50 |
|     1004 | JP      | 200.00 |
+----------+---------+--------+
```

### Example 4: Exact distinct counting with Bitmap

```SQL
CREATE TABLE uv_base (
    k INT, uid INT
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 2;

INSERT INTO uv_base VALUES (1, 10), (1, 20), (1, 20), (2, 30);

CREATE MATERIALIZED VIEW mv_uv
DISTRIBUTED BY HASH(k) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT k, bitmap_union(to_bitmap(uid)) AS uv FROM uv_base GROUP BY k;

REFRESH MATERIALIZED VIEW mv_uv WITH SYNC MODE;

INSERT INTO uv_base VALUES (1, 30), (2, 30), (2, 40);
REFRESH MATERIALIZED VIEW mv_uv WITH SYNC MODE;

SELECT k, bitmap_count(uv) AS dc FROM mv_uv ORDER BY k;
```

```Plain
+------+------+
| k    | dc   |
+------+------+
|    1 |    3 |
|    2 |    2 |
+------+------+
```

The result is equivalent to `count(distinct)`:

```SQL
SELECT COUNT(*) FROM (
    SELECT k, bitmap_count(uv) FROM mv_uv
    EXCEPT SELECT k, COUNT(DISTINCT uid) FROM uv_base GROUP BY k) t;   -- 0
```

For approximate distinct counting, replace `bitmap_union(to_bitmap(uid))` with `hll_union(hll_hash(uid))` and use `hll_cardinality()` at query time.

### Example 5: Strict rollup on an AGGREGATE KEY base table

```SQL
CREATE TABLE base_agg (
    region VARCHAR(8),
    city   VARCHAR(8),
    s      BIGINT SUM,
    mx     BIGINT MAX,
    mn     BIGINT MIN
) AGGREGATE KEY(region, city)
DISTRIBUTED BY HASH(region) BUCKETS 1;

INSERT INTO base_agg VALUES ('CN','BJ',10,100,5), ('CN','BJ',20,50,8), ('US','NY',30,200,1);

CREATE MATERIALIZED VIEW mv_agg
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT region, city, SUM(s) AS s_sum, MAX(mx) AS mx_max, MIN(mn) AS mn_min
   FROM base_agg
   GROUP BY region, city;

REFRESH MATERIALIZED VIEW mv_agg WITH SYNC MODE;

INSERT INTO base_agg VALUES ('CN','BJ',5,150,2), ('JP','TK',7,7,7);
REFRESH MATERIALIZED VIEW mv_agg WITH SYNC MODE;
SELECT * FROM mv_agg ORDER BY region, city;
```

```Plain
+--------+------+-------+--------+--------+
| region | city | s_sum | mx_max | mn_min |
+--------+------+-------+--------+--------+
| CN     | BJ   |    35 |    150 |      2 |
| JP     | TK   |     7 |      7 |      7 |
| US     | NY   |    30 |    200 |      1 |
+--------+------+-------+--------+--------+
```

`GROUP BY region` (rollup to a subset of the key columns) is also valid. However, any expression other than `GROUP BY region, city`, as well as `COUNT(*)` / `AVG()`, is rejected.

### Example 6: UPDATE / DELETE retraction on a PRIMARY KEY base table

This is one of the most differentiated capabilities of incremental materialized views on native tables.

```SQL
CREATE TABLE pk_base (
    id INT NOT NULL,
    g  INT,
    v  BIGINT
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('enable_change_data_capture' = 'true');

INSERT INTO pk_base VALUES (1,10,100),(2,10,200),(3,20,300),(4,20,400),(5,30,500);

CREATE MATERIALIZED VIEW mv_pk
DISTRIBUTED BY HASH(g) BUCKETS 2
REFRESH DEFERRED MANUAL
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT g, SUM(v) AS sv, MAX(v) AS mx, MIN(v) AS mn, COUNT(*) AS c
   FROM pk_base
   GROUP BY g;

REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
SELECT * FROM mv_pk ORDER BY g;
```

```Plain
+------+------+------+------+------+
| g    | sv   | mx   | mn   | c    |
+------+------+------+------+------+
|   10 |  300 |  200 |  100 |    2 |
|   20 |  700 |  400 |  300 |    2 |
|   30 |  500 |  500 |  500 |    1 |
+------+------+------+------+------+
```

An `UPDATE` correctly retracts the old value and recalculates `MAX`:

```SQL
UPDATE pk_base SET v = 250 WHERE id = 2;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=10 becomes sv=350, mx=250, mn=100, c=2
```

A `DELETE` correctly removes the row. If an entire group becomes empty, the corresponding row is removed from the materialized view:

```SQL
DELETE FROM pk_base WHERE id = 4;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- g=20 becomes sv=300, mx=300, mn=300, c=1

DELETE FROM pk_base WHERE id = 5;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
-- The g=30 row is removed entirely
```

When a grouping column is modified and the row moves between groups, both the old and new groups are updated:

```SQL
UPDATE pk_base SET g = 20 WHERE id = 1;
REFRESH MATERIALIZED VIEW mv_pk WITH SYNC MODE;
```

### Example 7: Partitioned materialized view with base-table change trigger

```SQL
CREATE TABLE events (
    id BIGINT, dt DATE, v BIGINT
) DUPLICATE KEY(id, dt)
PARTITION BY RANGE(dt) (
    PARTITION p1 VALUES LESS THAN ('2026-02-01'),
    PARTITION p2 VALUES LESS THAN ('2026-03-01'),
    PARTITION p3 VALUES LESS THAN ('2026-04-01')
)
DISTRIBUTED BY HASH(id) BUCKETS 2;

CREATE MATERIALIZED VIEW mv_events
PARTITION BY dt
DISTRIBUTED BY HASH(id) BUCKETS 2
REFRESH ASYNC                                   -- Automatically trigger incremental refresh after each base-table ingestion
PROPERTIES ("refresh_mode" = "INCREMENTAL")
AS SELECT id, dt, v FROM events;

INSERT INTO events VALUES (1,'2026-01-10',10), (2,'2026-02-10',20);
-- No manual REFRESH is required. The refresh is triggered after the ingestion transaction commits.
```

Verify the trigger mode:

```SQL
SELECT TABLE_NAME, REFRESH_TRIGGER, REFRESH_POLICY
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mv_events';
```

```Plain
+-----------+----------------------+----------------------+
| TABLE_NAME| REFRESH_TRIGGER      | REFRESH_POLICY       |
+-----------+----------------------+----------------------+
| mv_events | ON_BASE_TABLE_CHANGE | ON_BASE_TABLE_CHANGE |
+-----------+----------------------+----------------------+
```

## Refresh an incremental materialized view

### The initial refresh is a full baseline

**The initial refresh performs a full PCT bootstrap** to establish the incremental starting point. True incremental refresh begins with the second refresh.

Therefore, seeing both `PCT SUCCESS` and `INCREMENTAL SUCCESS` in `information_schema.task_runs` is an **expected behavior**, not an error:

```SQL
SELECT DISTINCT get_json_string(EXTRA_MESSAGE, '$.refreshMode') AS executed_mode, STATE
FROM information_schema.task_runs
WHERE TASK_NAME = (SELECT TASK_NAME FROM information_schema.materialized_views
                   WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '<mv>')
  AND get_json_double(EXTRA_MESSAGE, '$.processStartTime') > 0
ORDER BY 1, 2;
```

```Plain
+---------------+---------+
| executed_mode | STATE   |
+---------------+---------+
| INCREMENTAL   | SUCCESS |
| PCT           | SUCCESS |
+---------------+---------+
```

Concurrent writes during the bootstrap are not counted twice. The system pins reads to a frozen position to provide exactly-once semantics.

### Three refresh trigger modes

There are three refresh trigger modes:

**Base-table change trigger**

- **DDL Syntax**: `REFRESH ASYNC` without `EVERY`
- **Description**: Automatically triggers an incremental refresh after each base-table transaction commits. **This is specific to native tables.** Iceberg base tables do not support it. The `excluded_trigger_tables` materialized-view property can be used to exclude specific base tables.

**Scheduled refresh**

- **DDL Syntax**: `REFRESH ASYNC [START('<t>')] EVERY(INTERVAL <n> <unit>)`
- **Description**: The minimum interval is controlled by the FE configuration `materialized_view_min_refresh_interval` (60 seconds by default).

**Manual refresh**

- **DDL Syntax**: `REFRESH [DEFERRED] MANUAL` + `REFRESH MATERIALIZED VIEW <mv>`
- **Description**: Add `WITH SYNC MODE` to wait synchronously for completion.


### Skip refresh when there are no changes

If none of the base tables has changed, no refresh plan is generated. The task run state is `SKIPPED` with reason `MV_UP_TO_DATE`, while the freshness timestamp is still advanced.

### Transactions and retries

Incremental refresh is essentially executing the following statement:

```SQL
INSERT INTO <mv> SELECT <incrementally rewritten query>
```

**Advancing the incremental position and writing the data are committed in the same transaction.** Therefore:

- Refresh failure ⇒ the position does not advance ⇒ the next refresh consumes the same window again, providing **idempotent retry with no duplication or data loss**.
- Incremental refresh has exactly one retry. `max_mv_refresh_failure_retry_times` applies only to PCT refresh. Lock timeout has a separate `max_mv_refresh_try_lock_failure_retry_times` setting, which defaults to 3.
- After consecutive failures exceed `max_task_consecutive_fail_count` (10 by default), the refresh task is suspended and the materialized view is deactivated.

### Batching (Currently Ineffective for native Tables)

The FE configurations `mv_max_rows_per_refresh` (100 million rows by default) and `mv_max_bytes_per_refresh` (20 GB by default) are intended to split accumulated incremental changes into multiple batches.

## Query an incremental materialized view

Incremental materialized views **must be queried directly**:

```SQL
SELECT * FROM mv_orders WHERE region = 'CN';
```

## Monitoring and troubleshooting

The observability of incremental materialized views has three levels.

### Level 1: Materialized-view level

```SQL
SELECT TABLE_NAME, REFRESH_MODE, REFRESH_TRIGGER, REFRESH_POLICY,
       IS_ACTIVE, INACTIVE_REASON,
       QUERY_REWRITE_STATUS, QUERY_REWRITE_STATUS_REASON,
       LAST_REFRESH_STATE, LAST_REFRESH_FINISHED_TIME,
       LAST_FRESHNESS_CONFIRMED_AT, BASE_TABLE_REFRESH_VERSION_TIMES,
       LAST_REFRESH_ERROR_MESSAGE, TASK_NAME
FROM information_schema.materialized_views
WHERE TABLE_SCHEMA = DATABASE() AND REFRESH_MODE = 'INCREMENTAL';
```

- `LAST_FRESHNESS_CONFIRMED_AT`: The most recent time at which freshness was confirmed. It also advances when a refresh is skipped because there are no changes.
- `BASE_TABLE_REFRESH_VERSION_TIMES`: The data-version timestamp for each base table, useful for evaluating end-to-end freshness latency.

### Level 2: Refresh-job level

```SQL
SELECT JOB_ID, REFRESH_TRIGGER, REFRESH_STATE,
       SUBMIT_TIME, FINISH_TIME, DURATION_TIME,
       IMV_SOURCE_VERSION_RANGE,
       IMV_SOURCE_TIMESTAMP_RANGE,
       IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP,
       ERROR_CODE, ERROR_MESSAGE, FAILED_QUERY_ID
FROM information_schema.materialized_view_refresh_jobs
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '<mv>'
ORDER BY SUBMIT_TIME DESC LIMIT 20;
```

- `IMV_SOURCE_VERSION_RANGE`: The position range consumed by this job for each base table. JSON keys use fully qualified readable names, including the catalog. For example:

  ```Plain
  {"default_catalog.mydb.base_tbl":{"start":"1024","end":"1088"}}
  ```

  `start == end` indicates that the base table had no changes in this job. For the initial refresh, `start` is rendered as the literal `MIN`.

- `IMV_SOURCE_TIMESTAMP_RANGE`: The commit timestamps, in epoch milliseconds, corresponding to the two endpoints above. **If a bookmark has already been reclaimed, the timestamp for the corresponding endpoint cannot be resolved and the entry for that base table is omitted.** For long-term auditing, rely on `task_runs`.

- When no incremental range is consumed, such as during a PCT bootstrap job, all three columns are `NULL`, rather than `{}`.

### Level 3: Individual run level

```SQL
SELECT QUERY_ID, STATE, PROCESS_TIME, FINISH_TIME,
       get_json_string(EXTRA_MESSAGE, '$.refreshMode')           AS executed_mode,
       get_json_string(EXTRA_MESSAGE, '$.imvSourceVersionRange') AS ver_range,
       ERROR_MESSAGE
FROM information_schema.task_runs
WHERE JOB_ID = '<job_id>'
ORDER BY CREATE_TIME;
```

To verify that incremental windows are contiguous and contain no gaps:

```SQL
WITH r AS (
  SELECT regexp_extract(get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange'),'"start": *"([0-9]+)"',1) AS s,
         regexp_extract(get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange'),'"end": *"([0-9]+)"',1)   AS e
  FROM information_schema.task_runs
  WHERE TASK_NAME = '<task_name>'
    AND get_json_string(EXTRA_MESSAGE,'$.refreshMode') = 'INCREMENTAL'
    AND get_json_string(EXTRA_MESSAGE,'$.imvSourceVersionRange') <> '{}'
)
SELECT (SELECT COUNT(*) FROM r)                         AS incremental_runs,
       (SELECT COUNT(*) FROM r a JOIN r b ON a.e = b.s) AS adjacent_pairs,
       (SELECT COUNT(*) FROM r WHERE s = e)             AS degenerate_windows;
```

### Determine whether a failure is recoverable

```SQL
SELECT QUERY_ID, CREATE_TIME, STATE, ERROR_CODE,
       ERROR_MESSAGE LIKE '%CDC-ERROR-1 (CHANGE_NOT_TRACKABLE):%'        AS permanently_broken_be,
       ERROR_MESSAGE LIKE '%do not support non-append-only base changes%' AS permanently_broken_fe,
       ERROR_MESSAGE
FROM information_schema.task_runs
WHERE `DATABASE` = DATABASE()
  AND (DEFINITION LIKE '%<mv>%' OR EXTRA_MESSAGE LIKE '%<mv>%')
  AND STATE = 'FAILED'
ORDER BY CREATE_TIME DESC LIMIT 20;
```

- If either marker is `1`, the incremental chain is permanently broken. The materialized view has been deactivated and **must be dropped and recreated**.
- If both are `0`, the failure is normally retryable, such as an out-of-memory error or lock timeout. The materialized view remains active. Resolve the underlying issue and refresh again.

### Related metrics

- `mv_global_count{refresh_mode, status}`: Counts asynchronous materialized views by refresh mode and active status.
- `mv_global_refresh_jobs_total` / `..._success_jobs_total` / `..._failed_jobs_total` / `mv_global_refresh_duration` / `mv_global_refresh_pending_jobs`.
- `mv_global_query_mv_usage_total{usage_type, refresh_mode}`: `usage_type = DIRECT` specifically represents the usage pattern of incremental materialized views and can be used to verify that business queries have actually switched to the materialized view.
- **Bookmark metrics**, which are specific to incremental materialized views on native tables and are used to monitor retention costs: `bookmark_count`, `bookmark_reference_count`, `bookmark_max_active_age_ms`, and so on. A continuously increasing `bookmark_max_active_age_ms` indicates that a materialized view has not been refreshed for a long time and that old base-table files cannot be reclaimed.

## Management and recovery

### Supported operations

| Operation                                                    | Supported                                         | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------ |
| `REFRESH MATERIALIZED VIEW <mv>`                             | Yes                                               | Asynchronously submits a refresh                             |
| `REFRESH MATERIALIZED VIEW <mv> WITH SYNC MODE`              | Yes                                               | Waits synchronously for completion                           |
| `REFRESH ... PARTITION START(...) END(...)`                  | **No**                                            | `Partition refresh is not supported for materialized views with refresh_mode=INCREMENTAL. Please refresh the whole materialized view instead.` |
| `REFRESH ... FORCE`                                          | **No**                                            | `FORCE refresh is not supported for materialized views with refresh_mode=INCREMENTAL. Please drop and re-create the materialized view instead.` |
| `ALTER MATERIALIZED VIEW ... SET("refresh_mode"=...)`        | **Same value only**                               | Switching between modes is rejected                          |
| `ALTER MATERIALIZED VIEW ... REFRESH ASYNC EVERY(...)` / `... REFRESH MANUAL` | Yes                                               | Preserves the incremental position; the next refresh remains incremental |
| `ALTER MATERIALIZED VIEW ... REFRESH ASYNC` (switch to base-table change trigger) | Supported, but **loses the incremental position** | Rebuilds the refresh context and clears the recorded incremental position. The first refresh triggered after the `ALTER` is a **full PCT bootstrap**. Reserve a full-refresh window when switching to base-table change triggers. |
| `ALTER MATERIALIZED VIEW ... ACTIVE / INACTIVE`              | Yes                                               | See the explanation below                                    |
| `DROP MATERIALIZED VIEW`                                     | Yes                                               | Releases all bookmarks held by the materialized view on base tables, allowing old versions to be reclaimed |
| Multiple incremental materialized views on the same base table | Yes                                               | Each materialized view maintains an independent position     |

### Base-table operations that break the incremental chain

| Base-table operation                                         | Result                                                       | Key error message                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `DELETE` on a DUPLICATE KEY base table                       | Refresh fails + materialized view deactivated (**irrecoverable**) | `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete` |
| `ALTER TABLE ... DROP PARTITION`                             | Refresh fails (**irrecoverable**)                            | `non-append-only change on base table`                       |
| `TRUNCATE TABLE [PARTITION p]`                               | Refresh fails (**irrecoverable**)                            | Same as above                                                |
| `INSERT OVERWRITE`                                           | Refresh fails (**irrecoverable**)                            | Same as above                                                |
| `DROP TABLE` (base table)                                    | Materialized view deactivated                                | `INACTIVE_REASON` contains `base-table dropped`              |
| `ALTER TABLE ... RENAME`                                     | Materialized view deactivated                                | Contains `base-table renamed`                                |
| `ALTER TABLE a SWAP WITH b`                                  | Materialized view deactivated                                | Contains `base-table swapped`                                |
| `ALTER TABLE ... MODIFY COLUMN` (change type)                | Refresh fails + materialized view deactivated                | `column schema not compatible`                               |
| Modify distribution columns / sort keys                      | Materialized view deactivated                                | `INACTIVE_REASON` starts with `base-table optimized:`        |
| PRIMARY KEY table recovery (`recover`)                       | The corresponding window permanently fails                   | `Change data capture does not support primary key recover`   |
| Cross-cluster replication writes                             | The corresponding window permanently fails                   | `Change data capture does not support replication`           |
| A window in which CDC was disabled                           | The corresponding window permanently fails                   | `CHANGES window on tablet <id> spans version <v> which was not recorded` |
| Version ancestor chain already reclaimed                     | The corresponding window permanently fails                   | `CHANGES ancestor chain on tablet <id> cannot reach base version <v>` |
| **`DELETE` / `UPDATE` on a PRIMARY KEY base table with CDC enabled** | **Normal incremental maintenance**                           | —                                                            |
| **`ADD COLUMN` / `DROP COLUMN` when the materialized view does not reference the column** | **Does not break the chain; incremental refresh continues**  | —                                                            |
| **Changing only the bucket count (`DISTRIBUTED BY ... BUCKETS n`)** | **Does not break the chain; incremental refresh continues**  | —                                                            |

### Recovery

Once the incremental chain is permanently broken:

```Plain
Cannot incrementally refresh materialized view <mv>: <reason>.
INCREMENTAL materialized views do not support non-append-only base changes
(DELETE / OVERWRITE / DROP PARTITION / snapshot expiration / table replacement).
Drop and recreate the materialized view to recover.
```

The materialized view is deactivated, and `INACTIVE_REASON` is set to:

```Plain
incremental refresh broken by non-append-only base change: <mv>
```

The system does **not** automatically reactivate the materialized view in the background.

## Costs and overheads

| Cost                           | Description                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| **Storage amplification**      | Hidden columns `__ROW_ID__` and `__AGG_STATE_*` increase storage usage. The benchmark showed approximately 2.7× amplification. |
| **Delayed garbage collection** | Bookmarks held by the materialized view lower the minimum retained version of the base table to the oldest version still referenced. **As long as the materialized view exists and has not been refreshed, old base-table files cannot be reclaimed.** Long periods without refresh can therefore cause continuous storage growth. Monitor `bookmark_max_active_age_ms`. |
| **PRIMARY KEY write overhead** | With CDC enabled, each ingestion records additional metadata pages used to locate changes. These structures do not accumulate across ingestions and represent incremental metadata overhead per ingestion. CDC has zero overhead when disabled. |
| **Limited tracking horizon**   | The BE configuration `cloud_native_tablet_metadata_ancestors_recorded` (5 by default) determines how many historical metadata versions are retained for each tablet and therefore how far incremental refresh can look back. Increasing it extends the tracking horizon at the cost of additional metadata storage. |
| **Migration cost**             | Query rewrite is not supported, so business SQL must be changed to query the materialized view directly. |

## Decision Tree

```Plain
Is the cluster a shared-data cluster?
├─ No → Use an asynchronous materialized view (PCT)
└─ Yes → Can existing business SQL be changed to query the materialized view directly?
         ├─ No → Use an asynchronous materialized view (PCT, supports query rewrite)
         └─ Yes → Does the base table have scheduled DROP PARTITION / TRUNCATE / INSERT OVERWRITE operations?
                  ├─ Yes → Use an asynchronous materialized view (PCT), or redesign those operations
                  └─ No → Does the query use only supported operators and aggregation functions?
                           ├─ No → Use an asynchronous materialized view (PCT)
                           └─ Yes → What is the base-table data model?
                                    ├─ UNIQUE KEY → Not supported; change the model or use PCT
                                    ├─ AGGREGATE KEY → Can the query be expressed as strict rollup?
                                    │                   ├─ Yes → Use an incremental materialized view
                                    │                   └─ No → Use PCT
                                    ├─ PRIMARY KEY → Enable CDC and use an incremental materialized view
                                    └─ DUPLICATE KEY → Use an incremental materialized view
```

## Common misconceptions

| Misconception                                                | Actual behavior                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| "An incremental materialized view is an upgraded version of an asynchronous materialized view and can replace it entirely." | No. It is a specialized mode with a significantly narrower SQL support surface than PCT and does not support query rewrite. |
| "Once the materialized view is created, existing queries automatically become faster." | No. Business SQL must explicitly be rewritten to query the materialized view. |
| "`DELETE` on a PRIMARY KEY base table breaks the incremental chain." | No. Row-level `DELETE` / `UPDATE` is supported normally on a PRIMARY KEY base table with CDC enabled. Partition-level / table-level destructive operations break the chain. |
| "Seeing `PCT` in task runs means the incremental configuration did not take effect." | No. The initial refresh is a full bootstrap and therefore uses `PCT`. Seeing both `PCT` and `INCREMENTAL` is expected. |
| "`materialized_view_refresh_jobs.REFRESH_MODE` indicates the mode actually executed." | No. It is the configured mode. The actual execution mode is in `task_runs.EXTRA_MESSAGE.$.refreshMode`. |
| "A materialized view can first be created as PCT and then changed to INCREMENTAL with `ALTER`." | No. `refresh_mode` cannot be changed across modes with `ALTER`. Drop and recreate the materialized view instead. |
| "Changing `default_mv_refresh_mode` to `incremental` makes all newly created materialized views incremental." | No. The creation path reads only the explicitly specified `refresh_mode` property. Changing this configuration can make a materialized view "report itself as incremental but actually use PCT", while also disabling query rewrite. |
| "Changing a materialized view from scheduled refresh to base-table change trigger using `ALTER ... REFRESH ASYNC` is lossless." | No. The operation clears the incremental position and triggers a full bootstrap on the subsequent refresh. |
| "Increasing `mv_max_rows_per_refresh` controls the amount of data processed in each refresh." | This currently has no effect for internal-table base tables. Each refresh consumes the entire accumulated window. |
| "Incremental refresh is always cheaper."                     | It depends on the workload. If writes are naturally distributed across many small partitions or historical partitions no longer change, PCT may already be inexpensive. Benchmark your workload. |

## Parameter Reference

### Table Properties (Base Tables)

#### `enable_change_data_capture`

- Type: Boolean
- Default: `false`
- Description: Can be set only on PRIMARY KEY tables in shared-data clusters. Enables the table to be used as a base table for an incremental materialized view and supports `UPDATE` / `DELETE` retraction. Can be enabled online using `ALTER TABLE ... SET(...)` as an asynchronous metadata job.

### Materialized View Properties

#### `refresh_mode`

- Type: Enum
- Default: `PCT`
- Description: Set it to `INCREMENTAL` to declare an incremental materialized view. It must be explicitly specified during creation, and cannot be switched across modes using `ALTER` statements.

#### `excluded_trigger_tables`

- Type: String
- Default: An empty string
- Description: The list of table names of base tables that are excluded from base-table change triggers. Valid only for `REFRESH ASYNC`.

#### `partition_refresh_number` / `partition_refresh_strategy` / `auto_refresh_partitions_limit`

- Description: Those properties have no effect on incremental materialized views.

#### `enable_query_rewrite` and other rewrite-related properties

- Description: Those properties have no effect on incremental materialized views. Query rewrite is always disabled.

### FE Configurations

#### `default_mv_refresh_mode`

- Type: String
- Default: `pct`
- Description: Valid values are `PCT` / `INCREMENTAL`. **Does not determine whether newly created materialized views actually use incremental maintenance**; the creation path checks only the explicit `refresh_mode` property. Changing it to `incremental` can cause materialized views without an explicit property to "report themselves as incremental while actually using PCT." Keep the default value.


#### `mv_max_rows_per_refresh`

- Type: long
- Default: `100000000`
- Description: Maximum number of rows per incremental batch. **Currently has no effect on native tables.**

#### `mv_max_bytes_per_refresh`

- Type: long
- Default: `21474836480`
- Description: Maximum bytes per incremental batch (20 GB). **Currently has no effect on native tables.**

#### `mv_refresh_try_lock_timeout_ms`

- Type: int
- Default: `30000`
- Description: Lock acquisition timeout when generating a refresh plan.

#### `max_mv_refresh_try_lock_failure_retry_times`

- Type: int
- Default: `3`
- Description: Number of retries after lock timeout.

#### `max_mv_refresh_failure_retry_times`

- Type: int
- Default: `1`
- Description: **Applies only to PCT refresh**. Incremental refresh always retries once.

#### `max_task_consecutive_fail_count`

- Type: int
- Default: `10`
- Description: Suspends the task and deactivates the materialized view after consecutive failures exceed this value.

#### `max_mv_task_run_meta_message_values_length`

- Type: int
- Default: `8`
- Description: Truncation length for collection-type fields in `EXTRA_MESSAGE`. **If there are more than 8 base tables, the observability data is incomplete.**

#### `materialized_view_min_refresh_interval`

- Type: int
- Default: `60` seconds
- Description: Minimum interval for `EVERY(INTERVAL ...)`.

#### `enable_mv_automatic_active_check`

- Type: boolean
- Default: `true`
- Description: Enables background automatic reactivation. Incremental-chain-break reasons are excluded.

#### `bookmark_reference_max_ttl_ms`

- Type: long
- Default: `-1` (disabled)
- Description: Maximum lifetime of bookmark references at the cluster level. **Setting a positive value may reclaim bookmarks required by materialized views and cause refresh failures. Use with caution.**

#### `enable_bookmark_meta_functions`

- Type: boolean
- Default: `false`
- Description: Enables diagnostic functions such as `bookmark_create()` / `bookmark_release()`. Intended only for troubleshooting.

### BE Configurations

#### `cloud_native_tablet_metadata_ancestors_recorded`

- Type: int
- Default: `5`
- Description: Number of historical metadata ancestor versions retained for each tablet. Determines the tracking horizon of incremental refresh. Increasing it allows refreshes to look further back at the cost of additional metadata storage.

## Capability Boundaries

### Prerequisites

Creating an incremental materialized view requires all of the following:

- A **shared-data cluster**, with the base table being a cloud-native table or an Iceberg table.
- A **supported base-table type**: DUPLICATE KEY tables are directly supported; CDC is required for PRIMARY KEY tables; AGGREGATE KEY tables support strict rollup only; UNIQUE KEY is not supported.
- The query uses only supported operators. See [Supported Operators](#supported-operators).
- All aggregation functions are included in the allowlist. See [Supported Aggregation Functions](#supported-aggregation-functions).

### Base Table Types

| Base table type                           | Supported | Description                                                  |
| ----------------------------------------- | --------- | ------------------------------------------------------------ |
| Cloud-native table in shared-data cluster | Yes       | Primary focus of this document.                              |
| Native table in shared-nothing cluster    | No        | Error: `IVMAnalyzer does not support table type: OLAP`.      |
| Iceberg table (append-only)               | Yes       | See [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md). |
| Hive / Hudi / Delta Lake / Paimon / JDBC  | No        | Error: `IVMAnalyzer does not support table type: <TYPE>`.    |
| Logical view                              | No        | Error: `IVMAnalyzer does not support inner relation type: ViewRelation`. |
| Materialized view (MV on MV)              | No        | Error: `IVMAnalyzer does not support table type: CLOUD_NATIVE_MATERIALIZED_VIEW`. |
| CTE (WITH clause)                         | No        | Error: `IVMAnalyzer does not support inner relation type: CTERelation`. |
| Temporary table                           | No        | Error: `Materialized view can't base on temporary table`.    |

### Data Models

#### DUPLICATE KEY Base Tables

**Append-only writes are supported.** No additional configuration is required.

Executing `DELETE` on the base table causes refresh to fail and deactivates the materialized view. The error message contains:

```Plain
CDC-ERROR-1 (CHANGE_NOT_TRACKABLE): CDC for DUP_KEYS does not support delete
```

The reason is that DUPLICATE KEY tables do not have delete vectors. Rows do not have addressable row identifiers, so the system cannot propagate the disappearance of an individual row to the materialized view.

#### PRIMARY KEY Base Tables

**Row-level incremental maintenance for `INSERT` / `UPDATE` / `DELETE` is supported.** This is the most important capability that differentiates incremental materialized views on native tables from Iceberg.

The base table must have CDC enabled by setting the table property `enable_change_data_capture` to `true`:

```SQL
-- Enable CDC when creating the table
CREATE TABLE pk_base (
    id INT NOT NULL,
    g  INT,
    v  BIGINT
) PRIMARY KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('enable_change_data_capture' = 'true');

-- Or enable it for an existing table (asynchronous metadata change job; wait for completion)
ALTER TABLE pk_base SET ('enable_change_data_capture' = 'true');
```

If CDC is not enabled, creating an incremental materialized view fails with:

```Plain
IVM on cloud-native PRIMARY KEY base 'pk_base' requires change data capture to be
enabled on the base table: ... Enable it with
ALTER TABLE pk_base SET ('enable_change_data_capture' = 'true').
```

PRIMARY KEY base tables have the following additional restrictions:

| Restriction                                                  | Key error message                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Cannot be mixed with non-PRIMARY KEY base tables for a pure projection/filter query | `IVM over a cloud-native PRIMARY KEY base requires every base to be a cloud-native PRIMARY KEY table, but '<t>' is not` |
| Cannot be mixed with non-PRIMARY KEY base tables and then aggregated | `IVM retractable aggregate requires every base to be a cloud-native PRIMARY KEY table` |
| `ORDER BY (...)` is not supported in the materialized view definition | `IVMAnalyzer does not yet support ORDER BY for a materialized view over a cloud-native PRIMARY KEY base` |
| JOIN materialized-view output columns must be orderable; JSON / MAP / BITMAP / HLL / PERCENTILE / VARIANT are not supported | `IVMAnalyzer does not support a join materialized view with a non-orderable output column type` |
| `SELECT *` is not supported over derived tables              | `IVMAnalyzer does not support SELECT * over a derived table on a cloud-native PRIMARY KEY base; list the columns explicitly` |
| Explicit column alias lists such as `t(a, b)` are not supported for derived tables | `IVMAnalyzer does not support a derived table with an explicit column alias list` |
| `UNION ALL` cannot mix retractable and append-only branches  | `IVMAnalyzer does not support a UNION ALL that mixes a retractable cloud-native PRIMARY KEY branch with an append-only branch` |
| `GROUP BY` / `DISTINCT` cannot appear inside `UNION ALL` branches | `IVMAnalyzer does not support a GROUP BY or DISTINCT branch in a UNION ALL materialized view` |

The following PRIMARY KEY shapes have been verified as supported: single-table queries, PK INNER JOIN PK, PK CROSS JOIN PK, aggregation over PK tables, one-level derived tables over PK tables, and PK UNION ALL PK.

#### AGGREGATE KEY Base Tables

**Only strict rollup is supported.** This data model has the most restrictions.

The reason is that the incremental data stream contains the raw rows **before aggregation**, while normal queries see the merged result. Incremental maintenance is safe only when every materialized-view group maps to a complete merged group and the aggregation remains invariant under the merge operation.

The following eight constraints apply:

| Constraint                                                   | Key error message                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| Must be the only `FROM` source; JOIN / UNION / subqueries are not allowed | `is only supported when the base table is the sole FROM source (no JOIN, UNION, or subquery)` |
| Must contain `GROUP BY`; pure projection materialized views are not supported | `requires GROUP BY: the CDC delta stream emits raw pre-merge rowset rows` |
| `GROUP BY` can contain only **plain column references**      | `only supports GROUP BY plain column references, got: <expr>` |
| `GROUP BY` columns must be a **subset of the aggregate-key columns** | `requires GROUP BY columns to be a subset of the aggregate-key columns` |
| `WHERE` can filter only key columns                          | `does not support a WHERE predicate on value column '<c>': the predicate is evaluated on raw pre-merge delta rows` |
| Aggregation arguments must be a **single plain column reference** (`SUM(s * 2)` is rejected even though it is valid on DUPLICATE KEY tables) | `requires aggregate over a single plain column reference, got: <expr>` |
| The aggregate function must match the base column's aggregation type column by column | `requires MV aggregate to match the base column's aggregation type` |
| Aggregation cannot be applied to key columns or `REPLACE` / `REPLACE_IF_NOT_NULL` columns | `does not support aggregate on aggregate-key column '<c>'`; `does not support aggregate on REPLACE/REPLACE_IF_NOT_NULL column '<c>': replace semantics is order-dependent` |

Only the following six aggregation functions are supported on AGGREGATE KEY base tables: `sum`, `max`, `min`, `bitmap_union`, `hll_union`, and `percentile_union`.

`count` / `count(*)` / `avg` / `ndv` / `approx_count_distinct` / `array_agg` / `bool_or` / `bitmap_agg` are rejected on AGGREGATE KEY base tables even though they are supported on DUPLICATE KEY base tables:

```Plain
IVM on cloud-native AGGREGATE KEY base '<t>' does not support aggregate 'count';
AGG_KEYS base only accepts delta-rollup-compatible aggregates [sum, max, min,
bitmap_union, hll_union, percentile_union]
```

#### UNIQUE KEY Base Tables

**Not supported**

```Plain
IVM on cloud-native UNIQUE_KEYS base '<t>' is not supported: the CDC delta stream is
append-only and cannot maintain replace/upsert semantics, so the MV would retain
stale rows after a key update
```

#### Mixed Data-Model JOINs

| Combination                                       | Supported |
| ------------------------------------------------- | --------- |
| PRIMARY KEY ⋈ PRIMARY KEY (both with CDC enabled) | Yes       |
| DUPLICATE KEY ⋈ DUPLICATE KEY                     | Yes       |
| PRIMARY KEY ⋈ DUPLICATE KEY                       | No        |
| PRIMARY KEY ⋈ DUPLICATE KEY + aggregation         | No        |
| AGGREGATE KEY ⋈ any table                         | No        |

### Supported Operators

| Operator                                                     | Supported | Description                                                  |
| ------------------------------------------------------------ | --------- | ------------------------------------------------------------ |
| Scalar expressions in `SELECT`                               | Yes       | `CAST`, `CASE WHEN`, arithmetic, string/date functions, and so on |
| `WHERE` filtering                                            | Yes       | Limited to key columns for AGGREGATE KEY base tables         |
| `GROUP BY` aggregation                                       | Yes       | `GROUP BY` is required; global aggregation without `GROUP BY` is not supported |
| `GROUP BY` ordinal (`GROUP BY 1`)                            | Yes       | Handled automatically                                        |
| `GROUP BY` expressions                                       | Yes       | Limited to plain column references for AGGREGATE KEY base tables |
| `GROUP BY` without aggregation (deduplication)               | Yes       | Equivalent to deduplication by key                           |
| `SELECT DISTINCT`                                            | Yes       | Automatically rewritten to equivalent `GROUP BY`, except for AGGREGATE KEY base tables |
| `HAVING` referencing only grouping keys                      | Yes       |                                                              |
| INNER JOIN                                                   | Yes       | Correctly handles changes on either or both sides            |
| CROSS JOIN                                                   | Yes       |                                                              |
| UNION ALL                                                    | Yes       | Branches cannot contain aggregation                          |
| Derived tables / subqueries                                  | Partially | Only one level is supported for all base-table models. The inner query must contain only projection / filtering / JOIN. Aggregation, `DISTINCT`, or `UNION` inside the inner query is rejected |
| **LEFT / RIGHT / FULL OUTER JOIN**                           | No        | `IVMAnalyzer does not support join type: LEFT OUTER JOIN`    |
| **SEMI / ANTI JOIN**                                         | No        | `IVMAnalyzer does not support join type: LEFT SEMI JOIN`     |
| **UNION (distinct)**                                         | No        | `IVMAnalyzer only supports UNION ALL, but got: DISTINCT`     |
| **INTERSECT / EXCEPT**                                       | No        | `IVMAnalyzer can only handle UnionRelation, but got: IntersectRelation` |
| **Window / analytic functions**                              | No        | `IVMAnalyzer does not support window functions`              |
| **`ORDER BY` in the query**                                  | No        | `IVMAnalyzer does not support order by clause`               |
| **`LIMIT`**                                                  | No        | Creation fails with `IVM rewrite failed to fully resolve incremental markers` |
| **`HAVING` containing aggregate functions**                  | No        | `IVMAnalyzer does not support HAVING with aggregate functions` |
| **GROUPING SETS / ROLLUP / CUBE / GROUP BY ALL**             | No        | `IVMAnalyzer does not support <GROUPING_SETS｜ROLLUP｜CUBE｜GROUP_BY_ALL> for incremental view maintenance` |
| **Global aggregation without `GROUP BY`**                    | No        | `IVMAnalyzer requires group by expressions for incremental view maintenance.` |
| **Distinct aggregation** (`count(distinct x)`, etc.)         | No        | `IVMAnalyzer does not support distinct aggregate functions`  |
| **CTE (`WITH`)**                                             | No        | `IVMAnalyzer does not support inner relation type: CTERelation` |
| **Non-deterministic functions** (`rand()` / `now()` / `uuid()`) | No        | General materialized-view restriction                        |
| **Time travel queries**                                      | No        | General materialized-view restriction                        |

### Supported Aggregation Functions

| Function                | Supported argument types                                     |
| ----------------------- | ------------------------------------------------------------ |
| `count`                 | `count(*)` or `count(col)` (any type, at most one argument)  |
| `sum`                   | Integer, floating-point, DECIMAL                             |
| `avg`                   | Integer, floating-point, DECIMAL                             |
| `min` / `max`           | Integer, floating-point, DECIMAL, DATE, DATETIME, string     |
| `array_agg`             | One argument; `array_agg(col ORDER BY k)` is **not supported** |
| `bool_or`               | BOOLEAN                                                      |
| `approx_count_distinct` | One argument                                                 |
| `ndv`                   | One argument                                                 |
| `bitmap_agg`            | One argument                                                 |
| `bitmap_union`          | BITMAP, such as `bitmap_union(to_bitmap(col))`               |
| `hll_union`             | HLL, such as `hll_union(hll_hash(col))`                      |
| `percentile_union`      | PERCENTILE, such as `percentile_union(percentile_hash(col))` |

Common unsupported functions include `count(distinct x)`, `multi_distinct_count`, `stddev`, `variance`, `stddev_samp`, `percentile_approx`, `group_concat`, `any_value`, `max_by`, `min_by`, `retention`, `window_funnel`, `bitmap_union_count`, `hll_union_agg`, `covar_*`, `corr`, and all aggregate UDFs.

The error format is:

```Plain
IVMAnalyzer does not support aggregate function: stddev. Supported functions:
[count, sum, avg, min, max, array_agg, bool_or, approx_count_distinct, ndv,
bitmap_agg, bitmap_union, hll_union, percentile_union]
```

### Structure of the Materialized View

An incremental materialized view is **always a PRIMARY KEY table**. Users cannot choose another model. This introduces several user-visible side effects:

- **Hidden column `__ROW_ID__`**: The primary key of the materialized view, identifying each row. It has two possible sources:

  - **Query-derived**: When `GROUP BY` / `DISTINCT` is present, or when the base table is PRIMARY KEY, the value is encoded from grouping keys or primary-key columns and has type `VARCHAR`.
  - **Auto-incremented**: In pure append-only projection/filter scenarios, it is `BIGINT AUTO_INCREMENT` and is populated by the storage engine.
- **Hidden columns `__AGG_STATE_<aggregate expression>`**: Each finalizable aggregation function stores an additional intermediate-state column. `bitmap_union`, `hll_union`, and `percentile_union` are folded when directly used as output columns and do not create additional columns.
- **Storage amplification**: These hidden columns increase storage usage. In the benchmark, storage was 318 MB versus 117 MB for PCT, approximately **2.7×**. Reserve additional capacity accordingly.

Partitioning, distribution, and sort keys:

| Clause           | Description                                                  |
| ---------------- | ------------------------------------------------------------ |
| `PARTITION BY`   | Supported. Uses the standard materialized-view partition validation and can use a different granularity from the base table, such as daily partitions on the base table and monthly partitions on the materialized view. |
| `DISTRIBUTED BY` | **Automatically normalized**. If omitted and `enable_range_distribution` is enabled in a shared-data cluster, Range distribution is used. Range distribution has no user-writable syntax. Otherwise, it is normalized to HASH distribution over **all key columns**, while preserving only the explicitly specified bucket count. Explicit hash columns and `RANDOM` are ignored. |
| `ORDER BY (...)` | Supported for non-PRIMARY KEY base tables; **not supported for PRIMARY KEY base tables**. |

## Error Reference

| Error message contains                                       | Meaning and action                                           |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| `does not support table type: OLAP`                          | Shared-nothing clusters are not supported. There is no workaround. |
| `does not support table type: CLOUD_NATIVE_MATERIALIZED_VIEW` | MV on MV is not supported.                                   |
| `does not support inner relation type: ViewRelation` / `CTERelation` | Logical views / CTEs cannot be used as base relations. Expand them into table references. |
| `requires change data capture to be enabled`                 | PRIMARY KEY base tables require `ALTER TABLE ... SET ('enable_change_data_capture' = 'true')`. |
| `UNIQUE_KEYS base ... is not supported`                      | UNIQUE KEY base tables are not supported. Consider the PRIMARY KEY model instead. |
| `AGGREGATE KEY base ... requires GROUP BY`                   | AGGREGATE KEY base tables must use rollup; pure projection materialized views are not supported. |
| `AGGREGATE KEY base ... does not support aggregate 'count'`  | AGGREGATE KEY base tables support only `sum` / `max` / `min` / `bitmap_union` / `hll_union` / `percentile_union`. |
| `does not support join type: LEFT OUTER JOIN`                | Only INNER / CROSS JOIN are supported.                       |
| `only supports UNION ALL`                                    | `UNION` (distinct), `INTERSECT`, and `EXCEPT` are not supported. |
| `does not support window functions`                          | Window functions are not supported.                          |
| `does not support order by clause`                           | `ORDER BY` cannot appear in the query body.                  |
| `does not support HAVING with aggregate functions`           | `HAVING` can reference only grouping keys.                   |
| `requires group by expressions`                              | Aggregation requires `GROUP BY`; global aggregation is not supported. |
| `does not support distinct aggregate functions`              | Use `bitmap_union(to_bitmap(col))` instead of `count(distinct col)`. |
| `does not support aggregate function: <name>`                | The function is not on the allowlist. See [Supported Aggregation Functions](#supported-aggregation-functions). |
| `every base to be a cloud-native PRIMARY KEY table`          | PRIMARY KEY base tables cannot be mixed with non-PRIMARY KEY base tables. |
| `IVM rewrite failed to fully resolve incremental markers`    | The query contains an operator that cannot be maintained incrementally, such as `LIMIT` or an unresolved correlated subquery. |
| `Failed to generate IVM refresh plan at CREATE time`         | Compilation failed during creation. The actual reason appears after this message. |
| `Invalid refresh_mode`                                       | `refresh_mode` accepts only `PCT` / `INCREMENTAL`.           |
| `Altering refresh_mode from ... is not supported`            | Cross-mode `ALTER` is not supported. Drop and recreate the materialized view. |
| `Partition refresh is not supported` / `FORCE refresh is not supported` | Incremental materialized views do not support partition refresh or FORCE refresh. |
| `do not support non-append-only base changes`                | The incremental chain is permanently broken. Drop and recreate the materialized view. |
| `CDC-ERROR-1 (CHANGE_NOT_TRACKABLE)`                         | The change cannot be tracked, such as `DELETE` on a DUPLICATE KEY table, a CDC gap, or a reclaimed ancestor chain. The incremental chain is permanently broken. Drop and recreate the materialized view. |
| `column schema not compatible`                               | A base-table column type change caused schema incompatibility. Drop and recreate the materialized view. |

## Related Documentation

- [CREATE MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)
- [REFRESH MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW.md)
- [ALTER MATERIALIZED VIEW](../../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md)
- [Asynchronous Materialized Views](./async_mv.mdx)
- [information_schema.materialized_views](../../sql-reference/information_schema/materialized_views.md)
- [information_schema.materialized_view_refresh_jobs](../../sql-reference/information_schema/materialized_view_refresh_jobs.md)
