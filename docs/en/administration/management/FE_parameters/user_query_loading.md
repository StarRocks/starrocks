---
displayed_sidebar: docs
sidebar_label: "Authentication, Query, and Loading"
---

# FE Configuration - Authentication, Query, and Loading

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

<FEConfigMethod />

## View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

For detailed description of the returned fields, see [`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md).

:::note
You must have administrator privileges to run cluster administration-related commands.
:::

## Configure FE parameters

### Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### Configure FE static parameters

<StaticFEConfigNote />

---

This topic introduces the following types of FE configurations:
- [User, role, and privilege](#user-role-and-privilege)
- [Query](#query-engine)
- [Loading and unloading](#loading-and-unloading)

## User, role, and privilege

### `enable_task_info_mask_credential`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When true, StarRocks redacts credentials from task SQL definitions before returning them in `information_schema.tasks` and `information_schema.task_runs` by applying SqlCredentialRedactor.redact to the DEFINITION column. In `information_schema.task_runs` the same redaction is applied whether the definition comes from the task run status or, when empty, from the task definition lookup. When false, raw task definitions are returned (may expose credentials). Masking is CPU/string-processing work and can be time-consuming when the number of tasks or `task_runs` is large; disable only if you need unredacted definitions and accept the security risk.
- Introduced in: v3.5.6

### `privilege_max_role_depth`

- Default: 16
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum role depth (level of inheritance) of a role.
- Introduced in: v3.0.0

### `privilege_max_total_roles_per_user`

- Default: 64
- Type: Int
- Unit:
- Is mutable: Yes
- Description: The maximum number of roles a user can have.
- Introduced in: v3.0.0

## Query engine

### `brpc_send_plan_fragment_timeout_ms`

- Default: 60000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Timeout in milliseconds applied to the BRPC TalkTimeoutController before sending a plan fragment. `BackendServiceClient.sendPlanFragmentAsync` sets this value prior to calling the backend `execPlanFragmentAsync`. It governs how long BRPC will wait when borrowing an idle connection from the connection pool and while performing the send; if exceeded, the RPC will fail and may trigger the method's retry logic. Set this lower to fail fast under contention, or raise it to tolerate transient pool exhaustion or slow networks. Be cautious: very large values can delay failure detection and block request threads.
- Introduced in: v3.3.11, v3.4.1, v3.5.0

### `connector_table_query_trigger_analyze_large_table_interval`

- Default: 12 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval for query-trigger ANALYZE tasks of large tables.
- Introduced in: v3.4.0

### `connector_table_query_trigger_analyze_max_pending_task_num`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of query-trigger ANALYZE tasks that are in Pending state on the FE.
- Introduced in: v3.4.0

### `connector_table_query_trigger_analyze_max_running_task_num`

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of query-trigger ANALYZE tasks that are in Running state on the FE.
- Introduced in: v3.4.0

### `connector_table_query_trigger_analyze_small_table_interval`

- Default: 2 * 3600
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval for query-trigger ANALYZE tasks of small tables.
- Introduced in: v3.4.0

### `connector_table_query_trigger_analyze_small_table_rows`

- Default: 10000000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The threshold for determining whether a table is a small table for query-trigger ANALYZE tasks.
- Introduced in: v3.4.0

### `connector_table_query_trigger_task_schedule_interval`

- Default: 30
- Type: Int
- Unit: Second
- Is mutable: Yes
- Description: The interval at which the Scheduler thread schedules the query-trigger background tasks. This item is to replace `connector_table_query_trigger_analyze_schedule_interval` introduced in v3.4.0. Here, the background tasks refer `ANALYZE` tasks in v3.4，and the collection task of low-cardinality columns' dictionary in versions later than v3.4.  
- Introduced in: v3.4.2

### `create_table_max_serial_replicas`

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of replicas to create serially. If actual replica count exceeds this value, replicas will be created concurrently. Try to reduce this value if table creation is taking a long time to complete.
- Introduced in: -

### `default_mv_partition_refresh_number`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: When a materialized view refresh involves multiple partitions, this parameter controls how many partitions are refreshed in a single batch by default.
Starting from version 3.3.0, the system defaults to refreshing one partition at a time to avoid potential out-of-memory (OOM) issues. In earlier versions, all partitions were refreshed at once by default, which could lead to memory exhaustion and task failure. However, note that when a materialized view refresh involves a large number of partitions, refreshing only one partition at a time may lead to excessive scheduling overhead, longer overall refresh time, and a large number of refresh records. In such cases, it is recommended to adjust this parameter appropriately to improve refresh efficiency and reduce scheduling costs.
- Introduced in: v3.3.0

### `default_mv_refresh_immediate`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to refresh an asynchronous materialized view immediately after creation. When this item is set to `true`, newly created materialized view will be refreshed immediately.
- Introduced in: v3.2.3

### `dynamic_partition_check_interval_seconds`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which new data is checked. If new data is detected, StarRocks automatically creates partitions for the data.
- Introduced in: -

### `dynamic_partition_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the dynamic partitioning feature. When this feature is enabled, StarRocks dynamically creates partitions for new data and automatically deletes expired partitions to ensure the freshness of data.
- Introduced in: -

### `enable_active_materialized_view_schema_strict_check`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to strictly check the length consistency of data types when activating an inactive materialized view. When this item is set to `false`, the activation of the materialized view is not affected if the length of the data types has changed in the base table.
- Introduced in: v3.3.4

### `mv_fast_schema_change_mode`

- Default: strict
- Type: String
- Unit: -
- Is mutable: Yes
- Description: Controls the behavior of Materialized View (MV) Fast Schema Evolution (FSE). Valid values are: `strict` (default) - only allow FSE when `isSupportFastSchemaEvolutionInDanger` is true and clear affected partition entries from the version map; `force` - allow FSE even when `isSupportFastSchemaEvolutionInDanger` is false and clear affected partition entries to trigger recomputation on refresh; `force_no_clear` - allow FSE even when `isSupportFastSchemaEvolutionInDanger` is false but do not clear partition entries.
- Introduced in: v4.1.0

### `enable_auto_collect_array_ndv`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable automatic collection for the NDV information of the ARRAY type.
- Introduced in: v4.0

### `enable_backup_materialized_view`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the BACKUP and RESTORE of asynchronous materialized views when backing up or restoring a specific database. If this item is set to `false`, StarRocks will skip backing up asynchronous materialized views.
- Introduced in: v3.2.0

### `enable_collect_full_statistic`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable automatic full statistics collection. This feature is enabled by default.
- Introduced in: -

### `enable_colocate_mv_index`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to support colocating the synchronous materialized view index with the base table when creating a synchronous materialized view. If this item is set to `true`, tablet sink will speed up the write performance of synchronous materialized views.
- Introduced in: v3.2.0

### `enable_decimal_v3`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to support the DECIMAL V3 data type.
- Introduced in: -

### `enable_experimental_mv`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the asynchronous materialized view feature. TRUE indicates this feature is enabled. From v2.5.2 onwards, this feature is enabled by default. For versions earlier than v2.5.2, this feature is disabled by default.
- Introduced in: v2.4

### `enable_local_replica_selection`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to select local replicas for queries. Local replicas reduce the network transmission cost. If this parameter is set to TRUE, the CBO preferentially selects tablet replicas on BEs that have the same IP address as the current FE. If this parameter is set to `FALSE`, both local replicas and non-local replicas can be selected.
- Introduced in: -

### `enable_manual_collect_array_ndv`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable manual collection for the NDV information of the ARRAY type.
- Introduced in: v4.0

### `enable_materialized_view`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the creation of materialized views.
- Introduced in: -

### `enable_materialized_view_external_table_precise_refresh`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Set this item to `true` to enable an internal optimization for materialized view refresh when a base table is an external (non-cloud-native) table. When enabled, the materialized view refresh processor computes candidate partitions and refreshes only the affected base-table partitions instead of all partitions, reducing I/O and refresh cost. Set it to `false` to force full-partition refresh of external tables.
- Introduced in: v3.2.9

### `enable_materialized_view_metrics_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect monitoring metrics for asynchronous materialized views by default.
- Introduced in: v3.1.11, v3.2.5

### `enable_materialized_view_spill`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Intermediate Result Spilling for materialized view refresh tasks.
- Introduced in: v3.1.1

### `enable_materialized_view_text_based_rewrite`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable text-based query rewrite by default. If this item is set to `true`, the system builds the abstract syntax tree while creating an asynchronous materialized view.
- Introduced in: v3.2.5

### `enable_mv_automatic_active_check`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the system to automatically check and re-activate the asynchronous materialized views that are set inactive because their base tables (views) had undergone Schema Change or had been dropped and re-created. Please note that this feature will not re-activate the materialized views that are manually set inactive by users.
- Introduced in: v3.1.6

### `enable_mv_automatic_repairing_for_broken_base_tables`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, StarRocks will attempt to automatically repair materialized view base-table metadata when a base external table is dropped and recreated or its table identifier changes. The repair flow can update the materialized view's base table information, collect partition-level repair information for external table partitions, and drive partition refresh decisions for async auto-refresh materialized views while honoring `autoRefreshPartitionsLimit`. Currently the automated repair supports Hive external tables; unsupported table types will cause the materialized view to be set inactive and a repair exception. Partition information collection is non-blocking and failures are logged.
- Introduced in: v3.3.19, v3.4.8, v3.5.6

### `enable_predicate_columns_collection`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable predicate columns collection. If disabled, predicate columns will not be recorded during query optimization.
- Introduced in: -

### `enable_query_queue_v2`

- Default: true
- Type: boolean
- Unit: -
- Is mutable: No
- Description: When true, switches the FE slot-based query scheduler to Query Queue V2. The flag is read by the slot manager and trackers (for example, `BaseSlotManager.isEnableQueryQueueV2` and `SlotTracker#createSlotSelectionStrategy`) to choose `SlotSelectionStrategyV2` instead of the legacy strategy. `query_queue_v2_xxx` configuration options and `QueryQueueOptions` take effect only when this flag is enabled. From v4.1 onwards, the default value is changed from `false` to `true`.
- Introduced in: v3.3.4, v3.4.0, v3.5.0

### `enable_sql_blacklist`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable blacklist check for SQL queries. When this feature is enabled, queries in the blacklist cannot be executed.
- Introduced in: -

### `enable_statistic_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect statistics for the CBO. This feature is enabled by default.
- Introduced in: -

### `enable_statistic_collect_on_first_load`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls automatic statistics collection and maintenance triggered by data loading operations. This includes:
  - Statistics collection when data is first loaded into a partition (partition version equals 2).
  - Statistics collection when data is loaded into empty partitions of multi-partition tables.
  - Statistics copying and updating for INSERT OVERWRITE operations.

  **Decision Policy for Statistics Collection Type:**
  
  - For INSERT OVERWRITE: `deltaRatio = |targetRows - sourceRows| / (sourceRows + 1)`
    - If `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (Default: 0.1), statistics collection will not be performed. Only the existing statistics will be copied.
    - Else, if `targetRows > statistic_sample_collect_rows` (Default: 200000), SAMPLE statistics collection is used.
    - Else, FULL statistics collection is used.
  
  - For First Load: `deltaRatio = loadRows / (totalRows + 1)`
    - If `deltaRatio < statistic_sample_collect_ratio_threshold_of_first_load` (Default: 0.1), statistics collection will not be performed.
    - Else, if `loadRows > statistic_sample_collect_rows` (Default: 200000), SAMPLE statistics collection is used.
    - Else, FULL statistics collection is used.
  
  **Synchronization Behavior:**
  
  - For DML statements (INSERT INTO/INSERT OVERWRITE): Synchronous mode with table lock. The load operation waits for statistics collection to complete (up to `semi_sync_collect_statistic_await_seconds`).
  - For Stream Load and Broker Load: Asynchronous mode without lock. Statistics collection runs in background without blocking the load operation.
  
  :::note
  Disabling this configuration will prevent all loading-triggered statistics operations, including statistics maintenance for INSERT OVERWRITE, which may result in tables lacking statistics. If new tables are frequently created and data is frequently loaded, enabling this feature will increase memory and CPU overhead.
  :::

- Introduced in: v3.1

### `enable_statistic_collect_on_update`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether UPDATE statements can trigger automatic statistics collection. When enabled, UPDATE operations that modify table data may schedule statistics collection through the same ingestion-based statistics framework controlled by `enable_statistic_collect_on_first_load`. Disabling this configuration skips statistics collection for UPDATE statements while keeping load-triggered statistics collection behavior unchanged.
- Introduced in: v3.5.11, v4.0.4

### `enable_udf`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to enable UDF.
- Introduced in: -

### `expr_children_limit`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of child expressions allowed in an expression.
- Introduced in: -

### `histogram_buckets_size`

- Default: 64
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The default bucket number for a histogram.
- Introduced in: -

### `histogram_max_sample_row_count`

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rows to collect for a histogram.
- Introduced in: -

### `histogram_mcv_size`

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The number of most common values (MCV) for a histogram.
- Introduced in: -

### `histogram_sample_ratio`

- Default: 0.1
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The sampling ratio for a histogram.
- Introduced in: -

### `http_slow_request_threshold_ms`

- Default: 5000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: If the response time for an HTTP request exceeds the value specified by this parameter, a log is generated to track this request.
- Introduced in: v2.5.15, v3.1.5

### `lock_checker_enable_deadlock_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, the LockChecker thread performs JVM-level deadlock detection using ThreadMXBean.findDeadlockedThreads() and logs the offending threads' stack traces. The check runs inside the LockChecker daemon (whose frequency is controlled by `lock_checker_interval_second`) and writes detailed stack information to the log, which may be CPU- and I/O-intensive. Enable this option only for troubleshooting live or reproducible deadlock issues; leaving it enabled in normal operation can increase overhead and log volume.
- Introduced in: v3.2.0

### `low_cardinality_threshold`

- Default: 255
- Type: Int
- Unit: -
- Is mutable: No
- Description: Threshold of low cardinality dictionary.
- Introduced in: v3.5.0

### `materialized_view_min_refresh_interval`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum allowed refresh interval (in seconds) for ASYNC materialized view schedules. When a materialized view is created with a time-based interval, the interval is converted to seconds and must not be less tham this value; otherwise the CREATE/ALTER operation fails with a DDL error. If this value is greater than 0, the check is enforced; set it to 0 or a negative value to disable the limit, which prevents excessive TaskManager scheduling and high FE memory/CPU usage from overly frequent refreshes. This item does not apply to `EVENT_TRIGGERED` refreshes.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### `materialized_view_refresh_ascending`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, materialized view partition refresh will iterate partitions in ascending partition-key order (oldest to newest). When it is set to `false` (default), the system iterates in descending order (newest to oldest). StarRocks uses this item in both list- and range-partitioned materialized view refresh logic to choose which partitions to process when partition refresh limits apply and to compute the next start/end partition boundaries for subsequent TaskRun executions. Changing this item alters which partitions are refreshed first and how the next partition range is derived; for range-partitioned materialized views, the scheduler validates new start/end and will raise an error if a change would create a repeated boundary (dead-loop), so set this item with care.
- Introduced in: v3.3.1, v3.4.0, v3.5.0

### `max_allowed_in_element_num_of_delete`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of elements allowed for the IN predicate in a DELETE statement.
- Introduced in: -

### `max_create_table_timeout_second`

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum timeout duration for creating a table.
- Introduced in: -

### `max_distribution_pruner_recursion_depth`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description:: The maximum recursion depth allowed by the partition pruner. Increasing the recursion depth can prune more elements but also increases CPU consumption.
- Introduced in: -

### `max_partitions_in_one_batch`

- Default: 4096
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of partitions that can be created when you bulk create partitions.
- Introduced in: -

### `max_planner_scalar_rewrite_num`

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum number of times that the optimizer can rewrite a scalar operator.
- Introduced in: -

### `max_query_queue_history_slots_number`

- Default: 0
- Type: Int
- Unit: Slots
- Is mutable: Yes
- Description: Controls how many recently released (history) allocated slots are retained per query queue for monitoring and observability. When `max_query_queue_history_slots_number` is set to a value `> 0`, BaseSlotTracker keeps up to that many most-recently released LogicalSlot entries in an in-memory queue, evicting the oldest when the limit is exceeded. Enabling this causes getSlots() to include these history entries (newest first), allows BaseSlotTracker to attempt registering slots with the ConnectContext for richer ExtraMessage data, and lets LogicalSlot.ConnectContextListener attach query finish metadata to history slots. When `max_query_queue_history_slots_number` `<= 0` the history mechanism is disabled (no extra memory used). Use a reasonable value to balance observability and memory overhead.
- Introduced in: v3.5.0

### `max_query_retry_time`

- Default: 2
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of query retries on an FE.
- Introduced in: -

### `max_running_rollup_job_num_per_table`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of rollup jobs can run in parallel for a table.
- Introduced in: -

### `max_scalar_operator_flat_children`

- Default：10000
- Type：Int
- Unit：-
- Is mutable: Yes
- Description：The maximum number of flat children for ScalarOperator. You can set this limit to prevent the optimizer from using too much memory.
- Introduced in: -

### `max_scalar_operator_optimize_depth`

- Default：256
- Type：Int
- Unit：-
- Is mutable: Yes
- Description: The maximum depth that ScalarOperator optimization can be applied.
- Introduced in: -

### `mv_active_checker_interval_seconds`

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: When the background `active_checker` thread is enabled, the system will periodically detect and automatically reactivate materialized views that became Inactive due to schema changes or rebuilds of their base tables (or views). This parameter controls the scheduling interval of the checker thread, in seconds. The default value is system-defined.
- Introduced in: v3.1.6

### `mv_rewrite_consider_data_layout_mode`

- Default: `enable`
- Type: String
- Unit: -
- Is mutable: Yes
- Description: Controls whether materialized view rewrite should take the base table data layout into account when selecting the best materialized view. Valid values:
  - `disable`: Never use data-layout criteria when choosing between candidate materialized views.
  - `enable`: Use data-layout criteria only when the query is recognized as layout-sensitive.
  - `force`: Always apply data-layout criteria when selecting the best materialized view.
  Changing this item affects `BestMvSelector` behavior and can improve or broaden rewrite applicability depending on whether physical layout matters for plan correctness or performance.
- Introduced in: -

### `publish_version_interval_ms`

- Default: 10
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The time interval at which release validation tasks are issued.
- Introduced in: -

### `query_queue_slots_estimator_strategy`

- Default: MAX
- Type: String
- Unit: -
- Is mutable: Yes
- Description: Selects the slot estimation strategy used for queue-based queries when `enable_query_queue_v2` is true. Valid values: MBE (memory-based), PBE (parallelism-based), MAX (take max of MBE and PBE) and MIN (take min of MBE and PBE). MBE estimates slots from predicted memory or plan costs divided by the per-slot memory target and is capped by `totalSlots`. PBE derives slots from fragment parallelism (scan range counts or cardinality / rows-per-slot) and a CPU-cost based calculation (using CPU costs per slot), then bounds the result within [numSlots/2, numSlots]. MAX and MIN combine MBE and PBE by taking their maximum or minimum respectively. If the configured value is invalid, the default (`MAX`) is used.
- Introduced in: v3.5.0

### `query_queue_v2_concurrency_level`

- Default: 4
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls how many logical concurrency "layers" are used when computing the system's total query slots. In shared-nothing mode the total slots = `query_queue_v2_concurrency_level` * number_of_BEs * cores_per_BE (derived from BackendResourceStat). In multi-warehouse mode the effective concurrency is scaled down to max(1, `query_queue_v2_concurrency_level` / 4). If the configured value is non-positive it is treated as `4`. Changing this value increases or decreases totalSlots (and therefore concurrent query capacity) and affects per-slot resources: memBytesPerSlot is derived by dividing per-worker memory by (cores_per_worker * concurrency), and CPU accounting uses `query_queue_v2_cpu_costs_per_slot`. Set it proportional to cluster size; very large values may reduce per-slot memory and cause resource fragmentation. 
- Introduced in: v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_cpu_costs_per_slot`

- Default: 1000000000
- Type: Long
- Unit: planner CPU cost units
- Is mutable: Yes
- Description: Per-slot CPU cost threshold used to estimate how many slots a query needs from its planner CPU cost. The scheduler computes slots as integer(`plan_cpu_costs` / `query_queue_v2_cpu_costs_per_slot`) and then clamps the result to the range [1, totalSlots] (totalSlots is derived from the query queue V2 `V2` parameters). The V2 code normalizes non-positive settings to 1 (Math.max(1, value)), so a non-positive value effectively becomes `1`. Increasing this value reduces slots allocated per query (favoring fewer, larger-slot queries); decreasing it increases slots per query. Tune together with `query_queue_v2_num_rows_per_slot` and concurrency settings to control parallelism vs. resource granularity.
- Introduced in: v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_num_rows_per_slot`

- Default: 4096
- Type: Int
- Unit: Rows
- Is mutable: Yes
- Description: The target number of source-row records assigned to a single scheduling slot when estimating per-query slot count. StarRocks computes `estimated_slots` = (cardinality of the Source Node) / `query_queue_v2_num_rows_per_slot`, then clamps the result to the range [1, totalSlots] and enforces a minimum of 1 if the computed value is non-positive. totalSlots is derived from available resources (roughly DOP * `query_queue_v2_concurrency_level` * number_of_workers/BE) and therefore depends on cluster/core counts. Increase this value to reduce slot count (each slot handles more rows) and lower scheduling overhead; decrease it to increase parallelism (more, smaller slots), up to the resource limit.
- Introduced in: v3.3.4, v3.4.0, v3.5.0

### `query_queue_v2_schedule_strategy`

- Default: SWRR
- Type: String
- Unit: -
- Is mutable: Yes
- Description: Selects the scheduling policy used by Query Queue V2 to order pending queries. Supported values (case-insensitive) are `SWRR` (Smooth Weighted Round Robin) — the default, suitable for mixed/hybrid workloads that need fair weighted sharing — and `SJF` (Short Job First + Aging) — prioritizes short jobs while using aging to avoid starvation. The value is parsed with case-insensitive enum lookup; an unrecognized value is logged as an error and the default policy is used. This configuration only affects behavior when Query Queue V2 is enabled and interacts with V2 sizing settings such as `query_queue_v2_concurrency_level`.
- Introduced in: v3.3.12, v3.4.2, v3.5.0

### `semi_sync_collect_statistic_await_seconds`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Maximum wait time for semi-synchronous statistics collection during DML operations (INSERT INTO and INSERT OVERWRITE statements). Stream Load and Broker Load use asynchronous mode and are not affected by this configuration. If statistics collection time exceeds this value, the load operation continues without waiting for collection to complete. This configuration works in conjunction with `enable_statistic_collect_on_first_load`.
- Introduced in: v3.1

### `slow_query_analyze_threshold`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:: The execution time threshold for queries to trigger the analysis of Query Feedback.
- Introduced in: v3.4.0

### `statistic_analyze_status_keep_second`

- Default: 3 * 24 * 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The duration to retain the history of collection tasks. The default value is 3 days.
- Introduced in: -

### `statistic_auto_analyze_end_time`

- Default: 23:59:59
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The end time of automatic collection. Value range: `00:00:00` - `23:59:59`.
- Introduced in: -

### `statistic_auto_analyze_start_time`

- Default: 00:00:00
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The start time of automatic collection. Value range: `00:00:00` - `23:59:59`.
- Introduced in: -

### `statistic_auto_collect_ratio`

- Default: 0.8
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The threshold for determining whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered.
- Introduced in: -

### `statistic_auto_collect_small_table_rows`

- Default: 10000000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: Threshold to determine whether a table in an external data source (Hive, Iceberg, Hudi) is a small table during automatic collection. If the table has rows less than this value, the table is considered a small table.
- Introduced in: v3.2

### `statistic_cache_columns`

- Default: 100000
- Type: Long
- Unit: -
- Is mutable: No
- Description: The number of rows that can be cached for the statistics table.
- Introduced in: -

### `statistic_cache_thread_pool_size`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the thread-pool which will be used to refresh statistic caches.
- Introduced in: -

### `statistic_collect_interval_sec`

- Default: 5 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval for checking data updates during automatic collection.
- Introduced in: -

### `statistic_max_full_collect_data_size`

- Default: 100 * 1024 * 1024 * 1024
- Type: Long
- Unit: bytes
- Is mutable: Yes
- Description: The data size threshold for the automatic collection of statistics. If the total size exceeds this value, then sampled collection is performed instead of full.
- Introduced in: -

### `statistic_sample_collect_rows`

- Default: 200000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The row count threshold for deciding between SAMPLE and FULL statistics collection during loading-triggered statistics operations. If the number of loaded or changed rows exceeds this threshold (default 200,000), SAMPLE statistics collection is used; otherwise, FULL statistics collection is used. This setting works in conjunction with `enable_statistic_collect_on_first_load` and `statistic_sample_collect_ratio_threshold_of_first_load`.
- Introduced in: -

### `statistic_update_interval_sec`

- Default: 24 * 60 * 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which the cache of statistical information is updated.
- Introduced in: -

### `task_check_interval_second`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval between executions of task background jobs. GlobalStateMgr uses this value to schedule the TaskCleaner FrontendDaemon which invokes `doTaskBackgroundJob()`; the value is multiplied by 1000 to set the daemon interval in milliseconds. Decreasing the value makes background maintenance (task cleanup, checks) run more frequently and react faster but increases CPU/IO overhead; increasing it reduces overhead but delays cleanup and detection of stale tasks. Tune this value to balance maintenance responsiveness and resource usage.
- Introduced in: v3.2.0

### `task_min_schedule_interval_s`

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Minimum allowed schedule interval (in seconds) for task schedules checked by the SQL layer. When a task is submitted, TaskAnalyzer converts the schedule period to seconds and rejects the submission with `ERR_INVALID_PARAMETER` if the period is smaller than `task_min_schedule_interval_s`. This prevents creating tasks that run too frequently and protects the scheduler from high-frequency tasks. If a schedule has no explicit start time, TaskAnalyzer sets the start time to the current epoch seconds.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### `task_runs_timeout_second`

- Default: 4 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Default execution timeout (in seconds) for a TaskRun. This item is used by TaskRun execution as the baseline timeout. If the task run's properties include session variables `query_timeout` or `insert_timeout` with a positive integer value, the runtime uses the larger value between that session timeout and `task_runs_timeout_second`. The effective timeout is then bounded to not exceed the configured `task_runs_ttl_second` and `task_ttl_second`. Set this item to limit how long a task run may execute. Very large values may be clipped by the task/task-run TTL settings.
- Introduced in: -

## Loading and unloading

### `broker_load_default_timeout_second`

- Default: 14400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a Broker Load job.
- Introduced in: -

### `desired_max_waiting_jobs`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of pending jobs in an FE. The number refers to all jobs, such as table creation, loading, and schema change jobs. If the number of pending jobs in an FE reaches this value, the FE will reject new load requests. This parameter takes effect only for asynchronous loading. From v2.5 onwards, the default value is changed from 100 to 1024.
- Introduced in: -

### `disable_load_job`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable loading when the cluster encounters an error. This prevents any loss caused by cluster errors. The default value is `FALSE`, indicating that loading is not disabled. `TRUE` indicates loading is disabled and the cluster is in read-only state.
- Introduced in: -

### `empty_load_as_error`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to return an error message "all partitions have no load data" if no data is loaded. Valid values:
  - `true`: If no data is loaded, the system displays a failure message and returns an error "all partitions have no load data".
  - `false`: If no data is loaded, the system displays a success message and returns OK, instead of an error.
- Introduced in: -

### `enable_file_bundling`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the File Bundling optimization for the cloud-native table. When this feature is enabled (set to `true`), the system automatically bundles the data files generated by loading, Compaction, or Publish operations, thereby reducing the API cost caused by high-frequency access to the external storage system. You can also control this behavior on the table level using the CREATE TABLE property `file_bundling`. For detailed instructions, see CREATE TABLE.
- Introduced in: v4.0

### `enable_routine_load_lag_metrics`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to collect Routine Load Kafka partition offset lag metrics. Please note that set this item to `true` will call the Kafka API to get the partition's latest offset.
- Introduced in: -

### `enable_sync_publish`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to synchronously execute the apply task at the publish phase of a load transaction. This parameter is applicable only to Primary Key tables. Valid values:
  - `TRUE` (default): The apply task is synchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful only after the apply task is completed, and the loaded data can truly be queried. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `true` can improve query performance and stability, but may increase load latency.
  - `FALSE`: The apply task is asynchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful after the apply task is submitted, but the loaded data cannot be immediately queried. In this case, concurrent queries need to wait for the apply task to complete or time out before they can continue. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `false` may affect query performance and stability.
- Introduced in: v3.2.0

### `export_checker_interval_second`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which load jobs are scheduled.
- Introduced in: -

### `export_max_bytes_per_be_per_task`

- Default: 268435456
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum amount of data that can be exported from a single BE by a single data unload task.
- Introduced in: -

### `export_running_job_num_limit`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of data exporting tasks that can run in parallel.
- Introduced in: -

### `export_task_default_timeout_second`

- Default: 2 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a data exporting task.
- Introduced in: -

### `export_task_pool_size`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: No
- Description: The size of the unload task thread pool.
- Introduced in: -

### `external_table_commit_timeout_ms`

- Default: 10000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration for committing (publishing) a write transaction to a StarRocks external table. The default value `10000` indicates a 10-second timeout duration.
- Introduced in: -

### `finish_transaction_default_lock_timeout_ms`

- Default: 1000
- Type: Int
- Unit: MilliSeconds
- Is mutable: Yes
- Description: The default timeout for acquiring the db and table lock during finishing transaction.
- Introduced in: v4.0.0, v3.5.8

### `history_job_keep_max_second`

- Default: 7 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration a historical job can be retained, such as schema change jobs.
- Introduced in: -

### `insert_load_default_timeout_second`

- Default: 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for the INSERT INTO statement that is used to load data.
- Introduced in: -

### `label_clean_interval_second`

- Default: 4 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which labels are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that historical labels can be cleaned up in a timely manner.
- Introduced in: -

### `label_keep_max_num`

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of load jobs that can be retained within a period of time. If this number is exceeded, the information of historical jobs will be deleted.
- Introduced in: -

### `label_keep_max_second`

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration in seconds to keep the labels of load jobs that have been completed and are in the FINISHED or CANCELLED state. The default value is 3 days. After this duration expires, the labels will be deleted. This parameter applies to all types of load jobs. A value too large consumes a lot of memory.
- Introduced in: -

### `load_checker_interval_second`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which load jobs are processed on a rolling basis.
- Introduced in: -

### `load_parallel_instance_num`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls the number of parallel load fragment instances created on a single host for broker and stream loads. LoadPlanner uses this value as the per-host degree of parallelism unless the session enables adaptive sink DOP; if the session variable `enable_adaptive_sink_dop` is true, the session`s `sink_degree_of_parallelism` overrides this configuration. When shuffle is required, this value is applied to fragment parallel execution (scan fragment and sink fragment parallel exec instances). When no shuffle is needed, it is used as the sink pipeline DOP. Note: loads from local files are forced to a single instance (pipeline DOP = 1, parallel exec = 1) to avoid local disk contention. Increasing this number raises per-host concurrency and throughput but may increase CPU, memory and I/O contention.
- Introduced in: v3.2.0

### `load_straggler_wait_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum loading lag that can be tolerated by a BE replica. If this value is exceeded, cloning is performed to clone data from other replicas.
- Introduced in: -

### `loads_history_retained_days`

- Default: 30
- Type: Int
- Unit: Days
- Is mutable: Yes
- Description: Number of days to retain load history in the internal `_statistics_.loads_history` table. This value is used for table creation to set the table property `partition_live_number` and is passed to `TableKeeper` (clamped to a minimum of 1) to determine how many daily partitions to keep. Increasing or decreasing this value adjusts how long completed load jobs are retained in daily partitions; it affects new table creation and the keeper's pruning behavior but does not automatically recreate past partitions. The `LoadsHistorySyncer` relies on this retention when managing the loads history lifecycle; its sync cadence is controlled by `loads_history_sync_interval_second`.
- Introduced in: v3.3.6, v3.4.0, v3.5.0

### `loads_history_sync_interval_second`

- Default: 60
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Interval (in seconds) used by LoadsHistorySyncer to schedule periodic syncs of finished load jobs from `information_schema.loads` into the internal `_statistics_.loads_history` table. The value is multiplied by 1000 in the constructor to set the FrontendDaemon interval. The syncer skips the first run (to allow table creation) and only imports loads that finished more than one minute ago; small values increase DML and executor load, while larger values delay availability of historical load records. See `loads_history_retained_days` for retention/partitioning behavior of the target table.
- Introduced in: v3.3.6, v3.4.0, v3.5.0

### `max_broker_load_job_concurrency`

- Default: 5
- Alias: `async_load_task_pool_size`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Broker Load jobs allowed within the StarRocks cluster. This parameter is valid only for Broker Load. The value of this parameter must be less than the value of `max_running_txn_num_per_db`. From v2.5 onwards, the default value is changed from `10` to `5`.
- Introduced in: -

### `max_load_timeout_second`

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum timeout duration allowed for a load job. The load job fails if this limit is exceeded. This limit applies to all types of load jobs.
- Introduced in: -

### `max_routine_load_batch_size`

- Default: 4294967296
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum amount of data that can be loaded by a Routine Load task.
- Introduced in: -

### `max_routine_load_task_concurrent_num`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent tasks for each Routine Load job.
- Introduced in: -

### `max_routine_load_task_num_per_be`

- Default: 16
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Routine Load tasks on each BE. Since v3.1.0, the default value for this parameter is increased to 16 from 5, and no longer needs to be less than or equal to the value of BE static parameter `routine_load_thread_pool_size` (deprecated).
- Introduced in: -

### `max_running_txn_num_per_db`

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of load transactions allowed to be running for each database within a StarRocks cluster. The default value is `1000`. From v3.1 onwards, the default value is changed to `1000` from `100`. When the actual number of load transactions running for a database exceeds the value of this parameter, new load requests will not be processed. New requests for synchronous load jobs will be denied, and new requests for asynchronous load jobs will be placed in queue. We do not recommend you increase the value of this parameter because this will increase system load.
- Introduced in: -

### `max_stream_load_timeout_second`

- Default: 259200
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum allowed timeout duration for a Stream Load job.
- Introduced in: -

### `max_tolerable_backend_down_num`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of faulty BE nodes allowed. If this number is exceeded, Routine Load jobs cannot be automatically recovered.
- Introduced in: -

### `min_bytes_per_broker_scanner`

- Default: 67108864
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: The minimum allowed amount of data that can be processed by a Broker Load instance.
- Introduced in: -

### `min_load_timeout_second`

- Default: 1
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum timeout duration allowed for a load job. This limit applies to all types of load jobs.
- Introduced in: -

### `min_routine_load_lag_for_metrics`

- Default: 10000
- Type: INT
- Unit: -
- Is mutable: Yes
- Description: The minimum offset lag of Routine Load jobs to be shown in monitoring metrics. Routine Load jobs whose offset lags are greater than this value will be displayed in the metrics.
- Introduced in: -

### `period_of_auto_resume_min`

- Default: 5
- Type: Int
- Unit: Minutes
- Is mutable: Yes
- Description: The interval at which Routine Load jobs are automatically recovered.
- Introduced in: -

### `prepared_transaction_default_timeout_second`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The default timeout duration for a prepared transaction.
- Introduced in: -

### `routine_load_task_consume_second`

- Default: 15
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum time for each Routine Load task within the cluster to consume data. Since v3.1.0, Routine Load job supports a new parameter `task_consume_second` in `job_properties`. This parameter applies to individual load tasks within a Routine Load job, which is more flexible.
- Introduced in: -

### `routine_load_task_timeout_second`

- Default: 60
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for each Routine Load task within the cluster. Since v3.1.0, Routine Load job supports a new parameter `task_timeout_second` in `job_properties`. This parameter applies to individual load tasks within a Routine Load job, which is more flexible.
- Introduced in: -

### `routine_load_unstable_threshold_second`

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: Routine Load job is set to the UNSTABLE state if any task within the Routine Load job lags. To be specific, the difference between the timestamp of the message being consumed and the current time exceeds this threshold, and unconsumed messages exist in the data source.
- Introduced in: -

### `spark_dpp_version`

- Default: 1.0.0
- Type: String
- Unit: -
- Is mutable: No
- Description: The version of Spark Dynamic Partition Pruning (DPP) used.
- Introduced in: -

### `spark_home_default_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/spark2x"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of a Spark client.
- Introduced in: -

### `spark_launcher_log_dir`

- Default: `sys_log_dir` + "/spark_launcher_log"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores Spark log files.
- Introduced in: -

### `spark_load_default_timeout_second`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for each Spark Load job.
- Introduced in: -

### `spark_load_submit_timeout_second`

- Default: 300
- Type: long
- Unit: Seconds
- Is mutable: No
- Description: Maximum time in seconds to wait for a YARN response after submitting a Spark application. `SparkLauncherMonitor.LogMonitor` converts this value to milliseconds and will stop monitoring and forcibly kill the spark launcher process if the job remains in UNKNOWN/CONNECTED/SUBMITTED longer than this timeout. `SparkLoadJob` reads this configuration as the default and allows a per-load override via the `LoadStmt.SPARK_LOAD_SUBMIT_TIMEOUT` property. Set it high enough to accommodate YARN queueing delays; setting it too low may abort legitimately queued jobs, while setting it too high may delay failure handling and resource cleanup.
- Introduced in: v3.2.0

### `spark_resource_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of the Spark dependency package.
- Introduced in: -

### `stream_load_default_timeout_second`

- Default: 600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The default timeout duration for each Stream Load job.
- Introduced in: -

### `stream_load_max_txn_num_per_be`

- Default: -1
- Type: Int
- Unit: Transactions
- Is mutable: Yes
- Description: Limits the number of concurrent stream-load transactions accepted from a single BE (backend) host. When set to a non-negative integer, FrontendServiceImpl checks the current transaction count for the BE (by client IP) and rejects new stream-load begin requests if the count `>=` this limit. A value of `< 0` disables the limit (unlimited). This check occurs during stream load begin and may cause a `streamload txn num per be exceeds limit` error when exceeded. Related runtime behavior uses `stream_load_default_timeout_second` for request timeout fallback.
- Introduced in: v3.3.0, v3.4.0, v3.5.0

### `stream_load_task_keep_max_num`

- Default: 1000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of Stream Load tasks that StreamLoadMgr keeps in memory (global across all databases). When the number of tracked tasks (`idToStreamLoadTask`) exceeds this threshold, StreamLoadMgr first calls `cleanSyncStreamLoadTasks()` to remove completed synchronous stream-load tasks; if the size still remains greater than half of this threshold, it invokes `cleanOldStreamLoadTasks(true)` to force removal of older or finished tasks. Increase this value to retain more task history in memory; decrease it to reduce memory usage and make cleanup more aggressive. This value controls in-memory retention only and does not affect persisted/replayed tasks.
- Introduced in: v3.2.0

### `stream_load_task_keep_max_second`

- Default: 3 * 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Retention window for finished or cancelled Stream Load tasks. After a task reaches a final state and its end timestamp is ealier than this threshold (`currentMs - endTimeMs > stream_load_task_keep_max_second * 1000`), it becomes eligible for removal by `StreamLoadMgr.cleanOldStreamLoadTasks` and is discarded when loading persisted state. Applies to both `StreamLoadTask` and `StreamLoadMultiStmtTask`. If total task count exceeds `stream_load_task_keep_max_num`, cleanup may be triggered earlier (synchronous tasks are prioritized by `cleanSyncStreamLoadTasks`). Set this to balance history/debugability against memory usage.
- Introduced in: v3.2.0

### `transaction_clean_interval_second`

- Default: 30
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which finished transactions are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that finished transactions can be cleaned up in a timely manner.
- Introduced in: -

### `transaction_stream_load_coordinator_cache_capacity`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The capacity of the cache that stores the mapping from transaction label to coordinator node.
- Introduced in: -

### `transaction_stream_load_coordinator_cache_expire_seconds`

- Default: 900
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The time to keep the coordinator mapping in the cache before it's evicted(TTL).
- Introduced in: -

### `yarn_client_path`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-client/hadoop/bin/yarn"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of the Yarn client package.
- Introduced in: -

### `yarn_config_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/lib/yarn-config"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores the Yarn configuration file.
- Introduced in: -
