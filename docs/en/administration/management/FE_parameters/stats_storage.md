---
displayed_sidebar: docs
sidebar_label: "Statistics and Storage"
---

# FE Configuration - Statistics and Storage

This topic introduces the following types of FE configurations:
- [Statistic report](#statistic-report)
- [Storage](#storage)

## Statistic report

### `enable_collect_warehouse_metrics`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, the system will collect and export per-warehouse metrics. Enabling it adds warehouse-level metrics (slot/usage/availability) to the metric output and increases metric cardinality and collection overhead. Disable it to omit warehouse-specific metrics and reduce CPU/network and monitoring storage cost.
- Introduced in: v3.5.0

### `enable_http_detail_metrics`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: When true, the HTTP server computes and exposes detailed HTTP worker metrics (notably the `HTTP_WORKER_PENDING_TASKS_NUM` gauge). Enabling this causes the server to iterate over Netty worker executors and call `pendingTasks()` on each `NioEventLoop` to sum pending task counts; when disabled the gauge returns 0 to avoid that cost. This extra collection can be CPU- and latency-sensitive — enable only for debugging or detailed investigation.
- Introduced in: v3.2.3

### `proc_profile_collect_time_s`

- Default: 120
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Duration in seconds for a single process profile collection. When `proc_profile_cpu_enable` or `proc_profile_mem_enable` is set to `true`, AsyncProfiler is started, the collector thread sleeps for this duration, then the profiler is stopped and the profile is written. Larger values increase sample coverage and file size but prolong profiler runtime and delay subsequent collections; smaller values reduce overhead but may produce insufficient samples. Ensure this value aligns with retention settings such as `proc_profile_file_retained_days` and `proc_profile_file_retained_size_bytes`.
- Introduced in: v3.2.12

## Storage

### `alter_table_timeout_second`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for the schema change operation (ALTER TABLE).
- Introduced in: -

### `capacity_used_percent_high_water`

- Default: 0.75
- Type: double
- Unit: Fraction (0.0–1.0)
- Is mutable: Yes
- Description: The high-water threshold of disk capacity used percent (fraction of total capacity) used when computing backend load scores. `BackendLoadStatistic.calcSore` uses `capacity_used_percent_high_water` to set `LoadScore.capacityCoefficient`: if a backend's used percent less than 0.5 the coefficient equal to 0.5; if used percent `>` `capacity_used_percent_high_water` the coefficient = 1.0; otherwise the coefficient transitions linearly with used percent via (2 * usedPercent - 0.5). When the coefficient is 1.0, the load score is driven entirely by capacity proportion; lower values increase the weight of replica count. Adjusting this value changes how aggressively the balancer penalizes backends with high disk utilization.
- Introduced in: v3.2.0

### `catalog_trash_expire_second`

- Default: 86400
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The longest duration the metadata can be retained after a database, table, or partition is dropped. If this duration expires, the data will be deleted and cannot be recovered through the RECOVER command.
- Introduced in: -

### `catalog_recycle_bin_erase_min_latency_ms`

- Default: 600000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The minimum delay in milliseconds before the metadata is erased when a database, table, or partition is dropped. This avoids the erase log being written ahead of the drop log.
- Introduced in: -

### `catalog_recycle_bin_erase_max_operations_per_cycle`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of erase operations per cycle for actually deleting databases, tables, or partitions from the recycle bin. The erase operation holds a lock, so one batch should not be too large.
- Introduced in: -

### `catalog_recycle_bin_erase_fail_retry_interval_ms`

- Default: 60000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The retry interval in milliseconds when an erase operation in the recycle bin fails.
- Introduced in: -

### `check_consistency_default_timeout_second`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for a replica consistency check. You can set this parameter based on the size of your tablet.
- Introduced in: -

### `consistency_check_cooldown_time_second`

- Default: 24 * 3600
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Controls the minimal interval (in seconds) required between consistency checks of the same tablet. During tablet selection, a tablet is considered eligible only if `tablet.getLastCheckTime()` is less than `(currentTimeMillis - consistency_check_cooldown_time_second * 1000)`. The default value (24 * 3600) enforces roughly one check per tablet per day to reduce backend disk I/O. Lowering this value increases check frequency and resource usage; raising it reduces I/O at the cost of slower detection of inconsistencies. The value is applied globally when filtering cooldowned tablets from an index's tablet list.
- Introduced in: v3.5.5

### `consistency_check_end_time`

- Default: "4"
- Type: String
- Unit: Hour of day (0-23)
- Is mutable: No
- Description: Specifies the end hour (hour-of-day) of the ConsistencyChecker work window. The value is parsed with SimpleDateFormat("HH") in the system time zone and accepted as 0–23 (single or two-digit). StarRocks uses it with `consistency_check_start_time` to decide when to schedule and add consistency-check jobs. When `consistency_check_start_time` is greater than `consistency_check_end_time`, the window spans midnight (for example, default is `consistency_check_start_time` = "23" to `consistency_check_end_time` = "4"). When `consistency_check_start_time` is equal to `consistency_check_end_time`, the checker never runs. Parsing failure will cause FE startup to log an error and exit, so provide a valid hour string.
- Introduced in: v3.2.0

### `consistency_check_start_time`

- Default: "23"
- Type: String
- Unit: Hour of day (00-23)
- Is mutable: No
- Description: Specifies the start hour (hour-of-day) of the ConsistencyChecker work window. The value is parsed with SimpleDateFormat("HH") in the system time zone and accepted as 0–23 (single or two-digit). StarRocks uses it with `consistency_check_end_time` to decide when to schedule and add consistency-check jobs. When `consistency_check_start_time` is greater than `consistency_check_end_time`, the window spans midnight (for example, default is `consistency_check_start_time` = "23" to `consistency_check_end_time` = "4"). When `consistency_check_start_time` is equal to `consistency_check_end_time`, the checker never runs. Parsing failure will cause FE startup to log an error and exit, so provide a valid hour string.
- Introduced in: v3.2.0

### `consistency_tablet_meta_check_interval_ms`

- Default: 2 * 3600 * 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: Interval used by the ConsistencyChecker to run a full tablet-meta consistency scan between `TabletInvertedIndex` and `LocalMetastore`. The daemon in `runAfterCatalogReady` triggers checkTabletMetaConsistency when `current time - lastTabletMetaCheckTime` exceeds this value. When an invalid tablet is first detected, its `toBeCleanedTime` is set to `now + (consistency_tablet_meta_check_interval_ms / 2)` so actual deletion is delayed until a subsequent scan. Increase this value to reduce scan frequency and load (slower cleanup); decrease it to detect and remove stale tablets faster (higher overhead).
- Introduced in: v3.2.0

### `default_replication_num`

- Default: 3
- Type: Short
- Unit: -
- Is mutable: Yes
- Description: Sets the default number of replicas for each data partition when creating a table in StarRocks. This setting can be overridden when creating a table by specifying `replication_num=x` in the CREATE TABLE DDL.
- Introduced in: -

### `enable_auto_tablet_distribution`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to automatically set the number of buckets.
  - If this parameter is set to `TRUE`, you don't need to specify the number of buckets when you create a table or add a partition. StarRocks automatically determines the number of buckets.
  - If this parameter is set to `FALSE`, you need to manually specify the number of buckets when you create a table or add a partition. If you do not specify the bucket count when adding a new partition to a table, the new partition inherits the bucket count set at the creation of the table. However, you can also manually specify the number of buckets for the new partition.
- Introduced in: v2.5.7

### `enable_experimental_rowstore`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the hybrid row-column storage feature.
- Introduced in: v3.2.3

### `enable_fast_schema_evolution`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Fast Schema Evolution for all tables within the StarRocks cluster. Valid values are `TRUE` and `FALSE` (default). Enabling Fast Schema Evolution can increase the speed of schema changes and reduce resource usage when columns are added or dropped.
- Introduced in: v3.2.0

> **NOTE**
>
> - StarRocks shared-data clusters supports this parameter from v3.3.0.
> - If you need to configure the Fast Schema Evolution for a specific table, such as disabling Fast Schema Evolution for a specific table, you can set the table property `fast_schema_evolution` at table creation.

### `enable_online_optimize_table`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether StarRocks will use the non-blocking online optimization path when creating an optimize job. When `enable_online_optimize_table` is true and the target table meets compatibility checks (no partition/keys/sort specification, distribution is not `RandomDistributionDesc`, storage type is not `COLUMN_WITH_ROW`, replicated storage enabled, and the table is not a cloud-native table or materialized view), the planner creates an `OnlineOptimizeJobV2` to perform optimization without blocking writes. If false or any compatibility condition fails, StarRocks falls back to `OptimizeJobV2`, which may block write operations during optimization.
- Introduced in: v3.3.3, v3.4.0, v3.5.0

### `enable_strict_storage_medium_check`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether the FE strictly checks the storage medium of BEs when users create tables. If this parameter is set to `TRUE`, the FE checks the storage medium of BEs when users create tables and returns an error if the storage medium of the BE is different from the `storage_medium` parameter specified in the CREATE TABLE statement. For example, the storage medium specified in the CREATE TABLE statement is SSD but the actual storage medium of BEs is HDD. As a result, the table creation fails. If this parameter is `FALSE`, the FE does not check the storage medium of BEs when users create a table.
- Introduced in: -

### `max_bucket_number_per_partition`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of buckets can be created in a partition.
- Introduced in: v3.3.2

### `max_column_number_per_table`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of columns can be created in a table.
- Introduced in: v3.3.2

### `max_dynamic_partition_num`

- Default: 500
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Limits the maximum number of partitions that can be created at once when analyzing or creating a dynamic-partitioned table. During dynamic partition property validation, the `systemtask_runs_max_history_number` computes expected partitions (end offset + history partition number) and throws a DDL error if that total exceeds `max_dynamic_partition_num`. Raise this value only when you expect legitimately large partition ranges; increasing it allows more partitions to be created but can increase metadata size, scheduling work, and operational complexity.
- Introduced in: v3.2.0

### `max_partition_number_per_table`

- Default: 100000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of partitions can be created in a table.
- Introduced in: v3.3.2

### `max_task_consecutive_fail_count`

- Default: 10
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of consecutive failures a task may have before the scheduler automatically suspends it. When `TaskSource.MV.equals(task.getSource())` and `max_task_consecutive_fail_count` are greater than 0, if a task's consecutive failure counter reaches or exceeds `max_task_consecutive_fail_count`, the task is suspended via the TaskManager and, for materialized-view tasks, the materialized view is inactivated. An exception is thrown indicating suspension and how to reactivate (for example, `ALTER MATERIALIZED VIEW <mv_name> ACTIVE`). Set this item to 0 or a negative value to disable automatic suspension.
- Introduced in: -

### `partition_recycle_retention_period_secs`

- Default: 1800
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The metadata retention time for the partition that is dropped by INSERT OVERWRITE or materialized view refresh operations. Note that such metadata cannot be recovered by executing RECOVER.
- Introduced in: v3.5.9

### `recover_with_empty_tablet`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to replace a lost or corrupted tablet replica with an empty one. If a tablet replica is lost or corrupted, data queries on this tablet or other healthy tablets may fail. Replacing the lost or corrupted tablet replica with an empty tablet ensures that the query can still be executed. However, the result may be incorrect because data is lost. The default value is `FALSE`, which means lost or corrupted tablet replicas are not replaced with empty ones, and the query fails.
- Introduced in: -

### `storage_usage_hard_limit_percent`

- Default: 95
- Alias: `storage_flood_stage_usage_percent`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Hard limit of the storage usage percentage in a BE directory. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_hard_limit_reserve_bytes`, Load and Restore jobs are rejected. You need to set this item together with the BE configuration item `storage_flood_stage_usage_percent` to allow the configurations to take effect.
- Introduced in: -

### `storage_usage_hard_limit_reserve_bytes`

- Default: 100 * 1024 * 1024 * 1024
- Alias: `storage_flood_stage_left_capacity_bytes`
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Hard limit of the remaining storage space in a BE directory. If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_hard_limit_percent`, Load and Restore jobs are rejected. You need to set this item together with the BE configuration item `storage_flood_stage_left_capacity_bytes` to allow the configurations to take effect.
- Introduced in: -

### `storage_usage_soft_limit_percent`

- Default: 90
- Alias: `storage_high_watermark_usage_percent`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Soft limit of the storage usage percentage in a BE directory. If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_soft_limit_reserve_bytes`, tablets cannot be cloned into this directory.
- Introduced in: -

### `storage_usage_soft_limit_reserve_bytes`

- Default: 200 * 1024 * 1024 * 1024
- Alias: `storage_min_left_capacity_bytes`
- Type: Long
- Unit: Bytes
- Is mutable: Yes
- Description: Soft limit of the remaining storage space in a BE directory. If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_soft_limit_percent`, tablets cannot be cloned into this directory.
- Introduced in: -

### `tablet_checker_lock_time_per_cycle_ms`

- Default: 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The maximum lock hold time per cycle for tablet checker before releasing and reacquiring the table lock. Values less than 100 will be treated as 100.
- Introduced in: v3.5.9, v4.0.2

### `tablet_create_timeout_second`

- Default: 10
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for creating a tablet. The default value is changed from 1 to 10 from v3.1 onwards.
- Introduced in: -

### `tablet_delete_timeout_second`

- Default: 2
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for deleting a tablet.
- Introduced in: -

### `tablet_sched_balance_load_disk_safe_threshold`

- Default: 0.5
- Alias: `balance_load_disk_safe_threshold`
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The percentage threshold for determining whether the disk usage of BEs is balanced. If the disk usage of all BEs is lower than this value, it is considered balanced. If the disk usage is greater than this value and the difference between the highest and lowest BE disk usage is greater than 10%, the disk usage is considered unbalanced and a tablet re-balancing is triggered.
- Introduced in: -

### `tablet_sched_balance_load_score_threshold`

- Default: 0.1
- Alias: `balance_load_score_threshold`
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The percentage threshold for determining whether the load of a BE is balanced. If a BE has a lower load than the average load of all BEs and the difference is greater than this value, this BE is in a low load state. On the contrary, if a BE has a higher load than the average load and the difference is greater than this value, this BE is in a high load state.
- Introduced in: -

### `tablet_sched_be_down_tolerate_time_s`

- Default: 900
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The maximum duration the scheduler allows for a BE node to remain inactive. After the time threshold is reached, tablets on that BE node will be migrated to other active BE nodes.
- Introduced in: v2.5.7

### `tablet_sched_disable_balance`

- Default: false
- Alias: `disable_balance`
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable tablet balancing. `TRUE` indicates that tablet balancing is disabled. `FALSE` indicates that tablet balancing is enabled.
- Introduced in: -

### `tablet_sched_disable_colocate_balance`

- Default: false
- Alias: `disable_colocate_balance`
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to disable replica balancing for Colocate Table. `TRUE` indicates replica balancing is disabled. `FALSE` indicates replica balancing is enabled.
- Introduced in: -

### `tablet_sched_max_balancing_tablets`

- Default: 500
- Alias: `max_balancing_tablets`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablets that can be balanced at the same time. If this value is exceeded, tablet re-balancing will be skipped.
- Introduced in: -

### `tablet_sched_max_clone_task_timeout_sec`

- Default: 2 * 60 * 60
- Alias: `max_clone_task_timeout_sec`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description:The maximum timeout duration for cloning a tablet.
- Introduced in: -

### `tablet_sched_max_not_being_scheduled_interval_ms`

- Default: 15 * 60 * 1000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: When the tablet clone tasks are being scheduled, if a tablet has not been scheduled for the specified time in this parameter, StarRocks gives it a higher priority to schedule it as soon as possible.
- Introduced in: -

### `tablet_sched_max_scheduling_tablets`

- Default: 10000
- Alias: `max_scheduling_tablets`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablets that can be scheduled at the same time. If the value is exceeded, tablet balancing and repair checks will be skipped.
- Introduced in: -

### `tablet_sched_min_clone_task_timeout_sec`

- Default: 3 * 60
- Alias: `min_clone_task_timeout_sec`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum timeout duration for cloning a tablet.
- Introduced in: -

### `tablet_sched_num_based_balance_threshold_ratio`

- Default: 0.5
- Alias: -
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: Doing num based balance may break the disk size balance, but the maximum gap between disks cannot exceed `tablet_sched_num_based_balance_threshold_ratio` * `tablet_sched_balance_load_score_threshold`. If there are tablets in the cluster that are constantly balancing from A to B and B to A, reduce this value. If you want the tablet distribution to be more balanced, increase this value.
- Introduced in: - 3.1

### `tablet_sched_repair_delay_factor_second`

- Default: 60
- Alias: `tablet_repair_delay_factor_second`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which replicas are repaired, in seconds.
- Introduced in: -

### `tablet_sched_slot_num_per_path`

- Default: 8
- Alias: `schedule_slot_num_per_path`
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablet-related tasks that can run concurrently in a BE storage directory. From v2.5 onwards, the default value of this parameter is changed from `4` to `8`.
- Introduced in: -

### `tablet_sched_storage_cooldown_second`

- Default: -1
- Alias: `storage_cooldown_second`
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The latency of automatic cooling starting from the time of table creation. The default value `-1` specifies that automatic cooling is disabled. If you want to enable automatic cooling, set this parameter to a value greater than `-1`.
- Introduced in: -

### `tablet_stat_update_interval_second`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the FE retrieves tablet statistics from each BE.
- Introduced in: -

### `enable_range_distribution`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the Range-based Distribution semantic for table creation.
- Introduced in: v4.1.0

### `tablet_reshard_max_parallel_tablets`

- Default: 10240
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablets that can be split or merged in parallel.
- Introduced in: v4.1.0

### `tablet_reshard_target_size`

- Default: 1073741824 (1 GB)
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The target size of the tablets after the SPLIT or MERGE operation.
- Introduced in: v4.1.0

### `tablet_reshard_max_split_count`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of new tablets that an old tablet can be split into.
- Introduced in: v4.1.0

### `tablet_reshard_history_job_max_keep_ms`

- Default: 259200000 (72 hours)
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The maximum retention time of historical tablet SPLIT/MERGE jobs.
- Introduced in: v4.1.0
