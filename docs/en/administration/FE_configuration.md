---
displayed_sidebar: "English"
---

# FE configuration

FE parameters are classified into dynamic parameters and static parameters.

- Dynamic parameters can be configured and adjusted by running SQL commands, which is very convenient. But the configurations become invalid if you restart your FE. Therefore, we recommend that you also modify the configuration items in the `fe.conf` file to prevent the loss of modifications.

- Static parameters can only be configured and adjusted in the FE configuration file **fe.conf**. **After you modify this file, you must restart your FE for the changes to take effect.**

Whether a parameter is a dynamic parameter is indicated by the `IsMutable` column in the output of [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md). `TRUE` indicates a dynamic parameter.

Note that both dynamic and static FE parameters can be configured in the **fe.conf** file.

## View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
 ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
 ```

 For detailed description of the returned fields, see [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md).

> **NOTE**
>
> You must have administrator's privilege to run cluster administration-related commands.

## Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

> **NOTE**
>
> The configurations will be restored to the default values in the `fe.conf` file after the FE restarts. Therefore, we recommend that you also modify the configuration items in `fe.conf` to prevent the loss of modifications.

### Logging

#### qe_slow_log_ms

- Unit: ms
- Default: 5000
- Description: The threshold used to determine whether a query is a slow query. If the response time of a query exceeds this threshold, it is recorded as a slow query in `fe.audit.log`.

### Metadata and cluster management

#### catalog_try_lock_timeout_ms

- **Unit**: ms
- **Default**: 5000
- **Description**: The timeout duration to obtain the global lock.

#### edit_log_roll_num

- **Unit**: -
- **Default**: 50000
- **Description**: The maximum number of metadata log entries that can be written before a log file is created for these log entries. This parameter is used to control the size of log files. The new log file is written to the BDBJE database.

#### ignore_unknown_log_id

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether to ignore an unknown log ID. When an FE is rolled back, the FEs of the earlier version may be unable to recognize some log IDs. If the value is `TRUE`, the FE ignores unknown log IDs. If the value is `FALSE`, the FE exits.

#### ignore_materialized_view_error

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether FE ignores the metadata exception caused by materialized view errors. If FE fails to start due to the metadata exception caused by materialized view errors, you can set this parameter to `true` to allow FE to ignore the exception. This parameter is supported from v2.5.10 onwards.

#### ignore_meta_check

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether non-leader FEs ignore the metadata gap from the leader FE. If the value is TRUE, non-leader FEs ignore the metadata gap from the leader FE and continue providing data reading services. This parameter ensures continuous data reading services even when you stop the leader FE for a long period of time. If the value is FALSE, non-leader FEs do not ignore the metadata gap from the leader FE and stop providing data reading services.

#### meta_delay_toleration_second

- **Unit**: s
- **Default**: 300
- **Description**: The maximum duration by which the metadata on the follower and observer FEs can lag behind that on the leader FE. Unit: seconds. If this duration is exceeded, the non-leader FEs stops providing services.

#### drop_backend_after_decommission

- **Unit**: -
- **Default**: TRUE
- **Description**: Whether to delete a BE after the BE is decommissioned. `TRUE` indicates that the BE is deleted immediately after it is decommissioned. `FALSE` indicates that the BE is not deleted after it is decommissioned.

#### enable_collect_query_detail_info

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether to collect the profile of a query. If this parameter is set to `TRUE`, the system collects the profile of the query. If this parameter is set to `FALSE`, the system does not collect the profile of the query.

#### enable_background_refresh_connector_metadata

- **Unit**: -
- **Default**: `true` in v3.0<br />`false` in v2.5
- **Description**: Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. `true` indicates to enable the Hive metadata cache refresh, and `false` indicates to disable it. This parameter is supported from v2.5.5 onwards.

#### background_refresh_metadata_interval_millis

- **Unit**: ms
- **Default**: 600000
- **Description**: The interval between two consecutive Hive metadata cache refreshes. This parameter is supported from v2.5.5 onwards.

#### background_refresh_metadata_time_secs_since_last_access_secs

- **Unit**: s
- **Default**: 86400
- **Description**: The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cached metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata. This parameter is supported from v2.5.5 onwards.

#### enable_statistics_collect_profile

- **Unit**: N/A
- **Default**: false
- **Description**: Whether to generate profiles for statistics queries. You can set this item to `true` to allow StarRocks to generate query profiles for queries on system statistics. This parameter is supported from v3.1.5 onwards.

### Query engine

#### max_allowed_in_element_num_of_delete

- Unit: -
- Default: 10000
- Description: The maximum number of elements allowed for the IN predicate in a DELETE statement.

#### enable_materialized_view

- Unit: -
- Default: TRUE
- Description: Whether to enable the creation of materialized views.

#### enable_decimal_v3

- Unit: -
- Default: TRUE
- Description: Whether to support the DECIMAL V3 data type.

#### enable_sql_blacklist

- Unit: -
- Default: FALSE
- Description: Whether to enable blacklist check for SQL queries. When this feature is enabled, queries in the blacklist cannot be executed.

#### dynamic_partition_check_interval_seconds

- Unit: s
- Default: 600
- Description: The interval at which new data is checked. If new data is detected, StarRocks automatically creates partitions for the data.

#### dynamic_partition_enable

- Unit: -
- Default: TRUE
- Description: Whether to enable the dynamic partitioning feature. When this feature is enabled, StarRocks dynamically creates partitions for new data and automatically deletes expired partitions to ensure the freshness of data.

#### http_slow_request_threshold_ms

- Unit: ms
- Default: 5000
- Description: If the response time for an HTTP request exceeds the value specified by this parameter, a log is generated to track this request.
- Introduced in: 2.5.15，3.1.5

#### max_partitions_in_one_batch

- Unit: -
- Default: 4096
- Description: The maximum number of partitions that can be created when you bulk create partitions.

#### max_query_retry_time

- Unit: -
- Default: 2
- Description: The maximum number of query retries on an FE.

#### max_create_table_timeout_second

- Unit: s
- Default: 600
- Description: The maximum timeout duration for creating a table.

#### create_table_max_serial_replicas

- Unit: -
- Default: 128
- Description: The maximum number of replicas to create serially. If actual replica count exceeds this, replicas will be created concurrently. Try to reduce this config if table creation is taking a long time to complete.

#### max_running_rollup_job_num_per_table

- Unit: -
- Default: 1
- Description: The maximum number of rollup jobs can run in parallel for a table.

#### max_planner_scalar_rewrite_num

- Unit: -
- Default: 100000
- Description: The maximum number of times that the optimizer can rewrite a scalar operator.

#### enable_statistic_collect

- Unit: -
- Default: TRUE
- Description: Whether to collect statistics for the CBO. This feature is enabled by default.

#### enable_collect_full_statistic

- Unit: -
- Default: TRUE
- Description: Whether to enable automatic full statistics collection. This feature is enabled by default.

#### statistic_auto_collect_ratio

- Unit: -
- Default: 0.8
- Description: The threshold for determining whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered.

#### statistic_max_full_collect_data_size

- Unit: LONG
- Default: 107374182400
- Description: The size, in bytes, of the largest partition for the automatic collection of statistics. If a partition exceeds this value, then sampled collection is performed instead of full.

#### statistic_collect_max_row_count_per_query

- Unit: INT
- Default: 5000000000
- Description: The maximum number of rows to query for a single analyze task. An analyze task will be split into multiple queries if this value is exceeded.

#### statistic_collect_interval_sec

- Unit: s
- Default: 300
- Description: The interval for checking data updates during automatic collection.

#### statistic_auto_analyze_start_time

- Unit: STRING
- Default: 00:00:00
- Description: The start time of automatic collection. Value range: `00:00:00` - `23:59:59`.

#### statistic_auto_analyze_end_time

- Unit: STRING
- Default: 23:59:59
- Description: The end time of automatic collection. Value range: `00:00:00` - `23:59:59`.

#### statistic_sample_collect_rows

- Unit: -
- Default: 200000
- Description: The minimum number of rows to collect for sampled collection. If the parameter value exceeds the actual number of rows in your table, full collection is performed.

#### histogram_buckets_size

- Unit: -
- Default: 64
- Description: The default bucket number for a histogram.

#### histogram_mcv_size

- Unit: -
- Default: 100
- Description: The number of most common values (MCV) for a histogram.

#### histogram_sample_ratio

- Unit: -
- Default: 0.1
- Description: The sampling ratio for a histogram.

#### histogram_max_sample_row_count

- Unit: -
- Default: 10000000
- Description: The maximum number of rows to collect for a histogram.

#### statistics_manager_sleep_time_sec

- Unit: s
- Default: 60
- Description: The interval at which metadata is scheduled. The system performs the following operations based on this interval:
  - Create tables for storing statistics.
  - Delete statistics that have been deleted.
  - Delete expired statistics.

#### statistic_update_interval_sec

- Unit: s
- Default: `24 * 60 * 60`
- Description: The interval at which the cache of statistical information is updated. Unit: seconds.

#### statistic_analyze_status_keep_second

- Unit: s
- Default: 259200
- Description: The duration to retain the history of collection tasks. The default value is 3 days.

#### statistic_collect_concurrency

- Unit: -
- Default: 3
- Description: The maximum number of manual collection tasks that can run in parallel. The value defaults to 3, which means you can run a maximum of three manual collection tasks in parallel. If the value is exceeded, incoming tasks will be in the PENDING state, waiting to be scheduled.

#### statistic_auto_collect_small_table_rows

- Unit: -
- Default: 10000000
- Description: Threshold to determine whether a table in an external data source (Hive, Iceberg, Hudi) is a small table during automatic collection. If the table has rows less than this value, the table is considered a small table.
- Introduced in: v3.2

#### enable_local_replica_selection

- Unit: -
- Default: FALSE
- Description: Whether to select local replicas for queries. Local replicas reduce the network transmission cost. If this parameter is set to TRUE, the CBO preferentially selects tablet replicas on BEs that have the same IP address as the current FE. If this parameter is set to `FALSE`, both local replicas and non-local replicas can be selected. The default value is FALSE.

#### max_distribution_pruner_recursion_depth

- Unit: -
- Default: 100
- Description: The maximum recursion depth allowed by the partition pruner. Increasing the recursion depth can prune more elements but also increases CPU consumption.

#### enable_udf

- Unit: -
- Default: FALSE
- Description: Whether to enable UDF.

### Loading and unloading

#### max_broker_load_job_concurrency

- **Unit**: -
- **Default**: 5
- **Description**: The maximum number of concurrent Broker Load jobs allowed within the StarRocks cluster. This parameter is valid only for Broker Load. The value of this parameter must be less than the value of `max_running_txn_num_per_db`. From v2.5 onwards, the default value is changed from `10` to `5`. The alias of this parameter is `async_load_task_pool_size`.

#### load_straggler_wait_second

- **Unit**: s
- **Default**: 300
- **Description**: The maximum loading lag that can be tolerated by a BE replica. If this value is exceeded, cloning is performed to clone data from other replicas. Unit: seconds.

#### desired_max_waiting_jobs

- **Unit**: -
- **Default**: 1024
- **Description**: The maximum number of pending jobs in an FE. The number refers to all jobs, such as table creation, loading, and schema change jobs. If the number of pending jobs in an FE reaches this value, the FE will reject new load requests. This parameter takes effect only for asynchronous loading. From v2.5 onwards, the default value is changed from 100 to 1024.

#### max_load_timeout_second

- **Unit**: s
- **Default**: 259200
- **Description**: The maximum timeout duration allowed for a load job. The load job fails if this limit is exceeded. This limit applies to all types of load jobs.

#### min_load_timeout_second

- **Unit**: s
- **Default**: 1
- **Description**: The minimum timeout duration allowed for a load job. This limit applies to all types of load jobs. Unit: seconds.

#### max_running_txn_num_per_db

- **Unit**: -
- **Default**: 100
- **Description**: The maximum number of load transactions allowed to be running for each database within a StarRocks cluster. The default value is `100`. When the actual number of load transactions running for a database exceeds the value of this parameter, new load requests will not be processed. New requests for synchronous load jobs will be denied, and new requests for asynchronous load jobs will be placed in queue. We do not recommend you increase the value of this parameter because this will increase system load.

#### load_parallel_instance_num

- **Unit**: -
- **Default**: 1
- **Description**: The maximum number of concurrent loading instances for each load job on a BE.

#### disable_load_job

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether to disable loading when the cluster encounters an error. This prevents any loss caused by cluster errors. The default value is `FALSE`, indicating that loading is not disabled.

#### history_job_keep_max_second

- **Unit**: s
- **Default**: 604800
- **Description**: The maximum duration a historical job can be retained, such as schema change jobs, in seconds.

#### label_keep_max_num

- **Unit**: -
- **Default**: 1000
- **Description**: The maximum number of load jobs that can be retained within a period of time. If this number is exceeded, the information of historical jobs will be deleted.

#### label_keep_max_second

- **Unit**: s
- **Default**: 259200
- **Description**: The maximum duration in seconds to keep the labels of load jobs that have been completed and are in the FINISHED or CANCELLED state. The default value is 3 days. After this duration expires, the labels will be deleted. This parameter applies to all types of load jobs. A value too large consumes a lot of memory.

#### max_routine_load_job_num

- **Unit**: -
- **Default**: 100
- **Description**: The maximum number of Routine Load jobs in a StarRocks cluster. This parameter is deprecated since v3.1.0.

#### max_routine_load_task_concurrent_num

- **Unit**: -
- **Default**: 5
- **Description**: The maximum number of concurrent tasks for each Routine Load job.

#### max_routine_load_task_num_per_be

- **Unit**: -
- **Default**: 16
- **Description**: The maximum number of concurrent Routine Load tasks on each BE. Since v3.1.0, the default value for this parameter is increased to 16 from 5, and no longer needs to be less than or equal to the value of BE static parameter `routine_load_thread_pool_size` (deprecated).

#### max_routine_load_batch_size

- **Unit**: Byte
- **Default**: 4294967296
- **Description**: The maximum amount of data that can be loaded by a Routine Load task, in bytes.

#### routine_load_task_consume_second

- **Unit**: s
- **Default**: 15
- **Description**: The maximum time for each Routine Load task within the cluster to consume data. Since v3.1.0, Routine Load job supports a new parameter `task_consume_second` in [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible.

#### routine_load_task_timeout_second

- **Unit**: s
- **Default**: 60
- **Description**: The timeout duration for each Routine Load task within the cluster. Since v3.1.0, Routine Load job supports a new parameter `task_timeout_second` in [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible.

#### routine_load_unstable_threshold_second
- **Unit**: s
- **Default**: 3600
- **Description**: Routine Load job is set to the UNSTABLE state if any task within the Routine Load job lags. To be specific:
  - The difference between the timestamp of the message being consumed and the current time exceeds this threshold.
  - Unconsumed messages exist in the data source.

#### max_tolerable_backend_down_num

- **Unit**: -
- **Default**: 0
- **Description**: The maximum number of faulty BE nodes allowed. If this number is exceeded, Routine Load jobs cannot be automatically recovered.

#### period_of_auto_resume_min

- **Unit**: Min
- **Default**: 5
- **Description**: The interval at which Routine Load jobs are automatically recovered.

#### spark_load_default_timeout_second

- **Unit**: s
- **Default**: 86400
- **Description**: The timeout duration for each Spark Load job, in seconds.

#### spark_home_default_dir

- **Unit**: -
- **Default**: StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x"
- **Description**: The root directory of a Spark client.

#### stream_load_default_timeout_second

- **Unit**: s
- **Default**: 600
- **Description**: The default timeout duration for each Stream Load job, in seconds.

#### max_stream_load_timeout_second

- **Unit**: s
- **Default**: 259200
- **Description**: The maximum allowed timeout duration for a Stream Load job, in seconds.

#### insert_load_default_timeout_second

- **Unit**: s
- **Default**: 3600
- **Description**: The timeout duration for the INSERT INTO statement that is used to load data, in seconds.

#### broker_load_default_timeout_second

- **Unit**: s
- **Default**: 14400
- **Description**: The timeout duration for a Broker Load job, in seconds.

#### min_bytes_per_broker_scanner

- **Unit**: Byte
- **Default**: 67108864
- **Description**: The minimum allowed amount of data that can be processed by a Broker Load instance, in bytes.

#### max_broker_concurrency

- **Unit**: -
- **Default**: 100
- **Description**: The maximum number of concurrent instances for a Broker Load task. This parameter is deprecated from v3.1 onwards.

#### export_max_bytes_per_be_per_task

- **Unit**: Byte
- **Default**: 268435456
- **Description**: The maximum amount of data that can be exported from a single BE by a single data unload task, in bytes.

#### export_running_job_num_limit

- **Unit**: -
- **Default**: 5
- **Description**: The maximum number of data exporting tasks that can run in parallel.

#### export_task_default_timeout_second

- **Unit**: s
- **Default**: 7200
- **Description**: The timeout duration for a data exporting task, in seconds.

#### empty_load_as_error

- **Unit**: -
- **Default**: TRUE
- **Description**: Whether to return an error message "all partitions have no load data" if no data is loaded. Values:
  - TRUE: If no data is loaded, the system displays a failure message and returns an error "all partitions have no load data".
  - FALSE: If no data is loaded, the system displays a success message and returns OK, instead of an error.

#### external_table_commit_timeout_ms

- **Unit**: ms
- **Default**: 10000
- **Description**: The timeout duration for committing (publishing) a write transaction to a StarRocks external table. The default value `10000` indicates a 10-second timeout duration.

#### enable_sync_publish

- **Unit**: -
- **Default**: TRUE
- **Description**: Whether to synchronously execute the apply task at the publish phase of a load transaction. This parameter is applicable only to Primary Key tables. Valid values:
  - `TRUE` (default): The apply task is synchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful only after the apply task is completed, and the loaded data can truly be queried. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `true` can improve query performance and stability, but may increase load latency.
  - `FALSE`: The apply task is asynchronously executed at the publish phase of a load transaction. It means that the load transaction is reported as successful after the apply task is submitted, but the loaded data cannot be immediately queried. In this case, concurrent queries need to wait for the apply task to complete or time out before they can continue. When a task loads a large volume of data at a time or loads data frequently, setting this parameter to `false` may affect query performance and stability.
- **Introduced in**: v3.2.0

### Storage

#### default_replication_num

- **Unit**: -
- **Default**: 3
- **Description**: `default_replication_num` sets the default number of replicas for each data partition when creating a table in StarRocks. This setting can be overridden when creating a table by specifying `replication_num=x` in the CREATE TABLE DDL.

#### enable_strict_storage_medium_check

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether the FE strictly checks the storage medium of BEs when users create tables. If this parameter is set to `TRUE`, the FE checks the storage medium of BEs when users create tables and returns an error if the storage medium of the BE is different from the `storage_medium` parameter specified in the CREATE TABLE statement. For example, the storage medium specified in the CREATE TABLE statement is SSD but the actual storage medium of BEs is HDD. As a result, the table creation fails. If this parameter is `FALSE`, the FE does not check the storage medium of BEs when users create a table.

#### enable_auto_tablet_distribution

- **Unit**: -
- **Default**: TRUE
- **Description**: Whether to automatically set the number of buckets.
  - If this parameter is set to `TRUE`, you don't need to specify the number of buckets when you create a table or add a partition. StarRocks automatically determines the number of buckets.
  - If this parameter is set to `FALSE`, you need to manually specify the number of buckets when you create a table or add a partition. If you do not specify the bucket count when adding a new partition to a table, the new partition inherits the bucket count set at the creation of the table. However, you can also manually specify the number of buckets for the new partition. Starting from version 2.5.7, StarRocks supports setting this parameter.

#### enable_experimental_rowstore
- **Description:** Whether to enable hybrid row-column storage (this feature is currrently in preview). If this parameter is set to `TRUE` and `"STORE_TYPE" = "column_with_row"` is configured in the `PROPERTIES` clause at table creation, data in that table is stored in both row-by-row and column-by-column fashions. This feature can improve performance for high-concurrency point queries and partial data updates，while retaining the efficient analytical capabilities of columnar storage. For detailed user guide, see [Hybrid row-column storage](../table_design/hybrid_table.md).
- **Default**: FALSE
- **Introduced in**: v3.2.x

#### storage_usage_soft_limit_percent

- **Unit**: %
- **Default**: 90
- **Description**: If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_soft_limit_reserve_bytes`, tablets cannot be cloned into this directory.

#### storage_usage_soft_limit_reserve_bytes

- **Unit**: Byte
- **Default**: `200 * 1024 * 1024 * 1024`
- **Description**: If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_soft_limit_percent`, tablets cannot be cloned into this directory.

#### catalog_trash_expire_second

- **Unit**: s
- **Default**: 86400
- **Description**: The longest duration the metadata can be retained after a table or database is deleted. If this duration expires, the data will be deleted and cannot be recovered. Unit: seconds.

#### alter_table_timeout_second

- **Unit**: s
- **Default**: 86400
- **Description**: The timeout duration for the schema change operation (ALTER TABLE). Unit: seconds.

#### enable_fast_schema_evolution

- **Default**: FALSE
- **Description**: Whether to enable fast schema evolution for all tables within the StarRocks cluster. Valid values are `TRUE` and `FALSE` (default). Enabling fast schema evolution can increase the speed of schema changes and reduce resource usage when columns are added or dropped.
  > **NOTE**
  >
  > - StarRocks shared-data clusters do not support this parameter.
  > - If you need to configure the fast schema evolution for a specific table, such as disabling fast schema evolution for a specific table, you can set the table property [`fast_schema_evolution`](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md#set-fast-schema-evolution) at table creation.
- **Introduced in**: v3.2.0

#### recover_with_empty_tablet

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether to replace a lost or corrupted tablet replica with an empty one. If a tablet replica is lost or corrupted, data queries on this tablet or other healthy tablets may fail. Replacing the lost or corrupted tablet replica with an empty tablet ensures that the query can still be executed. However, the result may be incorrect because data is lost. The default value is `FALSE`, which means lost or corrupted tablet replicas are not replaced with empty ones, and the query fails.

#### tablet_create_timeout_second

- **Unit**: s
- **Default**: 10
- **Description**: The timeout duration for creating a tablet, in seconds.

#### tablet_delete_timeout_second

- **Unit**: s
- **Default**: 2
- **Description**: The timeout duration for deleting a tablet, in seconds.

#### check_consistency_default_timeout_second

- **Unit**: s
- **Default**: 600
- **Description**: The timeout duration for a replica consistency check. You can set this parameter based on the size of your tablet.

#### tablet_sched_slot_num_per_path

- **Unit**: -
- **Default**: 8
- **Description**: The maximum number of tablet-related tasks that can run concurrently in a BE storage directory. The alias is `schedule_slot_num_per_path`. From v2.5 onwards, the default value of this parameter is changed from `4` to `8`.

#### tablet_sched_max_scheduling_tablets

- **Unit**: -
- **Default**: 10000
- **Description**: The maximum number of tablets that can be scheduled at the same time. If the value is exceeded, tablet balancing and repair checks will be skipped.

#### tablet_sched_disable_balance

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether to disable tablet balancing. `TRUE` indicates that tablet balancing is disabled. `FALSE` indicates that tablet balancing is enabled. The alias is `disable_balance`.

#### tablet_sched_disable_colocate_balance

- **Unit**: -
- **Default**: FALSE
- **Description**: Whether to disable replica balancing for Colocate Table. `TRUE` indicates replica balancing is disabled. `FALSE` indicates replica balancing is enabled. The alias is `disable_colocate_balance`.

#### tablet_sched_max_balancing_tablets

- **Unit**: -
- **Default**: 500
- **Description**: The maximum number of tablets that can be balanced at the same time. If this value is exceeded, tablet re-balancing will be skipped. The alias is `max_balancing_tablets`.

#### tablet_sched_balance_load_disk_safe_threshold

- **Unit**: -
- **Default**: 0.5
- **Description**: The percentage threshold for determining whether the disk usage of BEs is balanced. If the disk usage of all BEs is lower than this value, it is considered balanced. If the disk usage is greater than this value and the difference between the highest and lowest BE disk usage is greater than 10%, the disk usage is considered unbalanced and a tablet re-balancing is triggered. The alias is `balance_load_disk_safe_threshold`.

#### tablet_sched_balance_load_score_threshold

- **Unit**: -
- **Default**: 0.1
- **Description**: The percentage threshold for determining whether the load of a BE is balanced. If a BE has a lower load than the average load of all BEs and the difference is greater than this value, this BE is in a low load state. On the contrary, if a BE has a higher load than the average load and the difference is greater than this value, this BE is in a high load state. The alias is `balance_load_score_threshold`.

#### tablet_sched_repair_delay_factor_second

- **Unit**: s
- **Default**: 60
- **Description**: The interval at which replicas are repaired, in seconds. The alias is `tablet_repair_delay_factor_second`.

#### tablet_sched_min_clone_task_timeout_sec

- **Unit**: s
- **Default**: 3 * 60
- **Description**: The minimum timeout duration for cloning a tablet, in seconds.

#### tablet_sched_max_clone_task_timeout_sec

- **Unit**: s
- **Default**: `2 * 60 * 60`
- **Description**: The maximum timeout duration for cloning a tablet, in seconds. The alias is `max_clone_task_timeout_sec`.

#### tablet_sched_max_not_being_scheduled_interval_ms

- **Unit**: ms
- **Default**: `15 * 60 * 100`
- **Description**: When the tablet clone tasks are being scheduled, if a tablet has not been scheduled for the specified time in this parameter, StarRocks gives it a higher priority to schedule it as soon as possible.

### Shared-data specific

#### lake_compaction_score_selector_min_score

- **Default**: 10.0
- **Description**: The Compaction Score threshold that triggers Compaction operations. When the Compaction Score of a partition is greater than or equal to this value, the system performs Compaction on that partition.
- **Introduced in**: v3.1.0

The Compaction Score indicates whether a partition needs Compaction and is scored based on the number of files in the partition. Excessive number of files can impact query performance, so the system periodically performs Compaction to merge small files and reduce the file count. You can check the Compaction Score for a partition based on the `MaxCS` column in the result returned by running [SHOW PARTITIONS](../sql-reference/sql-statements/data-manipulation/SHOW_PARTITIONS.md).

#### lake_compaction_max_tasks

- **Default**: -1
- **Description**: The maximum number of concurrent Compaction tasks allowed.
- **Introduced in**: v3.1.0

The system calculates the number of Compaction tasks based on the number of tablets in a partition. For example, if a partition has 10 tablets, performing one Compaction on that partition creates 10 Compaction tasks. If the number of concurrently executing Compaction tasks exceeds this threshold, the system will not create new Compaction tasks. Setting this item to `0` disables Compaction, and setting it to `-1` allows the system to automatically calculate this value based on an adaptive strategy.

#### lake_compaction_history_size

- **Default**: 12
- **Description**: The number of recent successful Compaction task records to keep in the memory of the Leader FE node. You can view recent successful Compaction task records using the `SHOW PROC '/compactions'` command. Note that the Compaction history is stored in the FE process memory, and it will be lost if the FE process is restarted.
- **Introduced in**: v3.1.0

#### lake_compaction_fail_history_size

- **Default**: 12
- **Description**: The number of recent failed Compaction task records to keep in the memory of the Leader FE node. You can view recent failed Compaction task records using the `SHOW PROC '/compactions'` command. Note that the Compaction history is stored in the FE process memory, and it will be lost if the FE process is restarted.
- **Introduced in**: v3.1.0

#### lake_publish_version_max_threads

- **Default**: 512
- **Description**: The maximum number of threads for Version Publish tasks.
- **Introduced in**: v3.2.0

#### lake_autovacuum_parallel_partitions

- **Default**: 8
- **Description**: The maximum number of partitions that can undergo AutoVacuum simultaneously. AutoVaccum is the Garbage Collection after Compactions.
- **Introduced in**: v3.1.0

#### lake_autovacuum_partition_naptime_seconds

- **Unit**: Seconds
- **Default**: 180
- **Description**: The minimum interval between AutoVacuum operations on the same partition.
- **Introduced in**: v3.1.0

#### lake_autovacuum_grace_period_minutes

- **Unit**: Minutes
- **Default**: 5
- **Description**: The time range for retaining historical data versions. Historical data versions within this time range are not automatically cleaned via AutoVacuum after Compactions. You need to set this value greater than the maximum query time to avoid that the data accessed by running queries get deleted before the queries finish.
- **Introduced in**: v3.1.0

#### lake_autovacuum_stale_partition_threshold

- **Unit**: Hours
- **Default**: 12
- **Description**: If a partition has no updates (loading, DELETE, or Compactions) within this time range, the system will not perform AutoVacuum on this partition.
- **Introduced in**: v3.1.0

#### lake_enable_ingest_slowdown

- **Default**: false
- **Description**: Whether to enable Data Ingestion Slowdown. When Data Ingestion Slowdown is enabled, if the Compaction Score of a partition exceeds `lake_ingest_slowdown_threshold`, loading tasks on that partition will be throttled down.
- **Introduced in**: v3.2.0

#### lake_ingest_slowdown_threshold

- **Default**: 100
- **Description**: The Compaction Score threshold that triggers Data Ingestion Slowdown. This configuration only takes effect when `lake_enable_ingest_slowdown` is set to `true`.
- **Introduced in**: v3.2.0

> **Note**
>
> When `lake_ingest_slowdown_threshold` is less than `lake_compaction_score_selector_min_score`, the effective threshold will be `lake_compaction_score_selector_min_score`.

#### lake_ingest_slowdown_ratio

- **Default**: 0.1
- **Description**: The ratio of the loading rate slowdown when Data Ingestion Slowdown is triggered.
- **Introduced in**: v3.2.0

Data loading tasks consist of two phases: data writing and data committing (COMMIT). Data Ingestion Slowdown is achieved by delaying data committing. The delay ratio is calculated using the following formula: `(compaction_score - lake_ingest_slowdown_threshold) * lake_ingest_slowdown_ratio`. For example, if the data writing phase takes 5 minutes, `lake_ingest_slowdown_ratio` is 0.1, and the Compaction Score is 10 higher than `lake_ingest_slowdown_threshold`, the delay in data committing time is `5 * 10 * 0.1 = 5` minutes, which means the average loading speed is halved.

> **Note**
>
> - If a loading task writes to multiple partitions simultaneously, the maximum Compaction Score among all partitions is used to calculate the delay in committing time.
> - The delay in committing time is calculated during the first attempt to commit. Once set, it will not change. Once the delay time is up, as long as the Compaction Score is not above `lake_compaction_score_upper_bound`, the system will perform the data committing operation.
> - If the delay in committing time exceeds the timeout of the loading task, the task will fail directly.

#### lake_compaction_score_upper_bound

- **Default**: 0
- **Description**: The upper limit of the Compaction Score for a partition. `0` indicates no upper limit. This item only takes effect when `lake_enable_ingest_slowdown` is set to `true`. When the Compaction Score of a partition reaches or exceeds this upper limit, all loading tasks on that partition will be indefinitely delayed until the Compaction Score drops below this value or the task times out.
- **Introduced in**: v3.2.0

### Other FE dynamic parameters

#### plugin_enable

- **Unit**: -
- **Default**: TRUE
- **Description**: Whether plugins can be installed on FEs. Plugins can be installed or uninstalled only on the Leader FE.

#### max_small_file_number

- **Unit**: -
- **Default**: 100
- **Description**: The maximum number of small files that can be stored on an FE directory.

#### max_small_file_size_bytes

- **Unit**: Byte
- **Default**: 1024 * 1024
- **Description**: The maximum size of a small file, in bytes.

#### agent_task_resend_wait_time_ms

- **Unit**: ms
- **Default**: 5000
- **Description**: The duration the FE must wait before it can resend an agent task. An agent task can be resent only when the gap between the task creation time and the current time exceeds the value of this parameter. This parameter is used to prevent repetitive sending of agent tasks. Unit: ms.

#### backup_job_default_timeout_ms

- **Unit**: ms
- **Default**: 86400*1000
- **Description**: The timeout duration of a backup job. If this value is exceeded, the backup job fails.

#### report_queue_size

- **Unit**: -
- **Default**: 100
- **Description**: The maximum number of jobs that can wait in a report queue. The report is about disk, task, and tablet information of BEs. If too many report jobs are piling up in a queue, OOM will occur.

#### enable_experimental_mv

- **Unit**: -
- **Default**: TRUE
- **Description**: Whether to enable the asynchronous materialized view feature. TRUE indicates this feature is enabled. From v2.5.2 onwards, this feature is enabled by default. For versions earlier than v2.5.2, this feature is disabled by default.

#### authentication_ldap_simple_bind_base_dn

- **Unit**: -
- **Default**: Empty string
- **Description**: The base DN, which is the point from which the LDAP server starts to search for users' authentication information.

#### authentication_ldap_simple_bind_root_dn

- **Unit**: -
- **Default**: Empty string
- **Description**: The administrator DN used to search for users' authentication information.

#### authentication_ldap_simple_bind_root_pwd

- **Unit**: -
- **Default**: Empty string
- **Description**: The password of the administrator used to search for users' authentication information.

#### authentication_ldap_simple_server_host

- **Unit**: -
- **Default**: Empty string
- **Description**: The host on which the LDAP server runs.

#### authentication_ldap_simple_server_port

- **Unit**: -
- **Default**: 389
- **Description**: The port of the LDAP server.

#### authentication_ldap_simple_user_search_attr

- **Unit**: -
- **Default**: uid
- **Description**: The name of the attribute that identifies users in LDAP objects.

#### max_upload_task_per_be

- **Unit**: -
- **Default**: 0
- **Description**: In each BACKUP operation, the maximum number of upload tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number. This item is supported from v3.1.0 onwards.

#### max_download_task_per_be

- **Unit**: -
- **Default**: 0
- **Description**: In each RESTORE operation, the maximum number of download tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number. This item is supported from v3.1.0 onwards.

#### allow_system_reserved_names

- **Default**: FALSE
- **Description**: Whether to allow users to create columns whose names are initiated with `__op` and `__row`. To enable this feaure, set this parameter to `TRUE`. Please note that these name formats are reserved for special purposes in StarRocks and creating such columns may result in undefined behavior. Therefore this feature is disabled by default. This item is supported from v3.2.0 onwards.

#### enable_backup_materialized_view

- **Default**: TRUE
- **Description**: Whehter to enable the BACKUP and RESTORE of asynchronous materialized views when backing up or restoring a specific database. If this item is set to `false`, StarRocks will skip backing up asynchronized materialized views. This item is supported from v3.2.0 onwards.

#### enable_colocate_mv_index

- **Default**: TRUE
- **Description**: Whether to support colocating the synchronous materialized view index with the base table when creating a synchronous materialized view. If this item is set to `true`, tablet sink will speed up the write performance of synchronous materialized views. This item is supported from v3.2.0 onwards.

#### enable_mv_automatic_active_check

- **Default**: TRUE
- **Description**: Whether to enable the system to automatically check and re-activate the asynchronous materialized views that are set inactive because their base tables (views) had undergone Schema Change or had been dropped and re-created. Please note that this feature will not re-activate the materialized views that are manually set inactive by users. This item is supported from v3.1.6 onwards.

##### jdbc_meta_default_cache_enable

- **Unit**: -
- **Default**: FALSE
- **Description**: The default value for whether the JDBC Catalog metadata cache is enabled. When set to True, newly created JDBC Catalogs will default to metadata caching enabled.

##### jdbc_meta_default_cache_expire_sec

- **Unit**: Second
- **Default**: 600
- **Description**: The default expiration time for the JDBC Catalog metadata cache. When jdbc_meta_default_cache_enable is set to true, newly created JDBC Catalogs will default to setting the expiration time of the metadata cache.

##### default_mv_refresh_immediate

- **Unit**: -
- **Default**: TRUE
- **Description**: Whether to refresh an asynchronous materialized view immediately after creation. When this item is set to `true`, newly created materialized view will be refreshed immediately.
- **Introduced in**: v3.2.3

## Configure FE static parameters

This section provides an overview of the static parameters that you can configure in the FE configuration file **fe.conf**. After you reconfigure these parameters for an FE, you must restart the FE for the changes to take effect.

### Logging

#### log_roll_size_mb

- **Default:** 1024 MB
- **Description:** The size per log file. Unit: MB. The default value `1024` specifies the size per log file as 1 GB.

#### sys_log_dir

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/log"
- **Description:** The directory that stores system log files.

#### sys_log_level

- **Default:** INFO
- **Description:** The severity levels into which system log entries are classified. Valid values: `INFO`, `WARN`, `ERROR`, and `FATAL`.

#### sys_log_verbose_modules

- **Default:** Empty string
- **Description:** The modules for which StarRocks generates system logs. If this parameter is set to `org.apache.starrocks.catalog`, StarRocks generates system logs only for the catalog module.

#### sys_log_roll_interval

- **Default:** DAY
- **Description:** The time interval at which StarRocks rotates system log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of system log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of system log files.

#### sys_log_delete_age

- **Default:** 7d
- **Description:** The retention period of system log files. The default value `7d` specifies that each system log file can be retained for 7 days. StarRocks checks each system log file and deletes those that were generated 7 days ago.

#### sys_log_roll_num

- **Default:** 10
- **Description:** The maximum number of system log files that can be retained within each retention period specified by the `sys_log_roll_interval` parameter.

#### audit_log_dir

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/log"
- **Description:** The directory that stores audit log files.

#### audit_log_roll_num

- **Default:** 90
- **Description:** The maximum number of audit log files that can be retained within each retention period specified by the `audit_log_roll_interval` parameter.

#### audit_log_modules

- **Default:** slow_query, query
- **Description:** The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the slow_query module and the query module. Separate the module names with a comma (,) and a space.

#### audit_log_roll_interval

- **Default:** DAY
- **Description:** The time interval at which StarRocks rotates audit log entries. Valid values: `DAY` and `HOUR`.
- If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of audit log files.
- If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of audit log files.

#### audit_log_delete_age

- **Default:** 30d
- **Description:** The retention period of audit log files. The default value `30d` specifies that each audit log file can be retained for 30 days. StarRocks checks each audit log file and deletes those that were generated 30 days ago.

#### dump_log_dir

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/log"
- **Description:** The directory that stores dump log files.

#### dump_log_modules

- **Default:** query
- **Description:** The modules for which StarRocks generates dump log entries. By default, StarRocks generates dump logs for the query module. Separate the module names with a comma (,) and a space.

#### dump_log_roll_interval

- **Default:** DAY
- **Description:** The time interval at which StarRocks rotates dump log entries. Valid values: `DAY` and `HOUR`.
  - If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of dump log files.
  - If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of dump log files.

#### dump_log_roll_num

- **Default:** 10
- **Description:** The maximum number of dump log files that can be retained within each retention period specified by the `dump_log_roll_interval` parameter.

#### dump_log_delete_age

- **Default:** 7d
- **Description:** The retention period of dump log files. The default value `7d` specifies that each dump log file can be retained for 7 days. StarRocks checks each dump log file and deletes those that were generated 7 days ago.

### Server

#### frontend_address

- **Default:** 0.0.0.0
- **Description:** The IP address of the FE node.

#### priority_networks

- **Default:** Empty string
- **Description:** Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as 10.10.10.0/24. If no IP address matches the entries in this list, an IP address will be randomly selected.

#### http_port

- **Default:** 8030
- **Description:** The port on which the HTTP server in the FE node listens.

#### http_worker_threads_num

- **Default:** 0
- **Description:** Number of worker threads for http server to deal with http requests. For a negative or 0 value, the number of threads will be twice the number of cpu cores.
- Introduced in: 2.5.18，3.0.10，3.1.7，3.2.2

#### http_backlog_num

- **Default:** 1024
- **Description:** The length of the backlog queue held by the HTTP server in the FE node.

#### cluster_name

- **Default:** StarRocks Cluster
- **Description:** The name of the StarRocks cluster to which the FE belongs. The cluster name is displayed for `Title` on the web page.

#### rpc_port

- **Default:** 9020
- **Description:** The port on which the Thrift server in the FE node listens.

#### thrift_backlog_num

- **Default:** 1024
- **Description:** The length of the backlog queue held by the Thrift server in the FE node.

#### thrift_server_max_worker_threads

- **Default:** 4096
- **Description:** The maximum number of worker threads that are supported by the Thrift server in the FE node.

#### thrift_client_timeout_ms

- **Default:** 5000
- **Description:** The length of time after which idle client connections time out. Unit: ms.

#### thrift_server_queue_size

- **Default:** 4096
- **Description:** The length of queue where requests are pending. If the number of threads that are being processed in the thrift server exceeds the value specified in `thrift_server_max_worker_threads`, new requests are added to the pending queue.

#### brpc_idle_wait_max_time

- **Default:** 10000
- **Description:** The maximum length of time for which bRPC clients wait as in the idle state. Unit: ms.

#### query_port

- **Default:** 9030
- **Description:** The port on which the MySQL server in the FE node listens.

#### mysql_service_nio_enabled

- **Default:** TRUE
- **Description:** Specifies whether asynchronous I/O is enabled for the FE node.

#### mysql_service_io_threads_num

- **Default:** 4
- **Description:** The maximum number of threads that can be run by the MySQL server in the FE node to process I/O events.

#### mysql_nio_backlog_num

- **Default:** 1024
- **Description:** The length of the backlog queue held by the MySQL server in the FE node.

#### max_mysql_service_task_threads_num

- **Default:** 4096
- **Description:** The maximum number of threads that can be run by the MySQL server in the FE node to process tasks.

#### mysql_server_version

- **Default:** 5.1.0
- **Description:** The MySQL server version returned to the client. Modifying this parameter will affect the version information in the following situations:
  1. `select version();`
  2. Handshake packet version
  3. Value of the global variable `version` (`show variables like 'version';`)

#### max_connection_scheduler_threads_num

- **Default:** 4096
- **Description:** The maximum number of threads that are supported by the connection scheduler.

#### qe_max_connection

- **Default:** 1024
- **Description:** The maximum number of connections that can be established by all users to the FE node.

#### check_java_version

- **Default:** TRUE
- **Description:** Specifies whether to check version compatibility between the executed and compiled Java programs. If the versions are incompatible, StarRocks reports errors and aborts the startup of Java programs.

### Metadata and cluster management

#### meta_dir

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/meta"
- **Description:** The directory that stores metadata.

#### heartbeat_mgr_threads_num

- **Default:** 8
- **Description:** The number of threads that can be run by the Heartbeat Manager to run heartbeat tasks.

#### heartbeat_mgr_blocking_queue_size

- **Default:** 1024
- **Description:** The size of the blocking queue that stores heartbeat tasks run by the Heartbeat Manager.

#### bdbje_reset_election_group

- **Default:** FALSE
- **Description:** Whether to reset the BDBJE replication group. If this parameter is set to `TRUE`, the FE will reset the BDBJE replication group (that is, remove the information of all electable FE nodes) and start as the leader FE. After the reset, this FE will be the only member in the cluster, and other FEs can rejoin this cluster by using `ALTER SYSTEM ADD/DROP FOLLOWER/OBSERVER 'xxx'`. Use this setting only when no leader FE can be elected because the data of most follower FEs have been damaged. `reset_election_group` is used to replace `metadata_failure_recovery`.

#### metadata_journal_ignore_replay_failure

- **Default:** FALSE
- **Description:** Whether to ignore journal replay failures. `TRUE` indicates journal replay failures will be ignored. However, this configuration does not take effect for failures that will damage cluster data.

#### edit_log_port

- **Default:** 9010
- **Description:** The port that is used for communication among the leader, follower, and observer FEs in the StarRocks cluster.

#### edit_log_type

- **Default:** BDB
- **Description:** The type of edit log that can be generated. Set the value to `BDB`.

#### bdbje_heartbeat_timeout_second

- **Default:** 30
- **Description:** The amount of time after which the heartbeats among the leader, follower, and observer FEs in the StarRocks cluster time out. Unit: second.

#### bdbje_lock_timeout_second

- **Default:** 1
- **Description:** The amount of time after which a lock in the BDB JE-based FE times out. Unit: second.

#### max_bdbje_clock_delta_ms

- **Default:** 5000
- **Description:** The maximum clock offset that is allowed between the leader FE and the follower or observer FEs in the StarRocks cluster. Unit: ms.

#### txn_rollback_limit

- **Default:** 100
- **Description:** The maximum number of transactions that can be rolled back.

#### bdbje_replica_ack_timeout_second

- **Default:** 10
- **Description:** The maximum amount of time for which the leader FE can wait for ACK messages from a specified number of follower FEs when metadata is written from the leader FE to the follower FEs. Unit: second. If a large amount of metadata is being written, the follower FEs require a long time before they can return ACK messages to the leader FE, causing ACK timeout. In this situation, metadata writes fail, and the FE process exits. We recommend that you increase the value of this parameter to prevent this situation.

#### master_sync_policy

- **Default:** SYNC
- **Description:** The policy based on which the leader FE flushes logs to disk. This parameter is valid only when the current FE is a leader FE. Valid values:
  - `SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.
  - `NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.
  - `WRITE_NO_SYNC`: When a transaction is commited, a log entry is generated simultaneously but is not flushed to disk. If you have deployed only one follower FE, we recommend that you set this parameter to `SYNC`. If you have deployed three or more follower FEs, we recommend that you set this parameter and the `replica_sync_policy` both to `WRITE_NO_SYNC`.

#### replica_sync_policy

- **Default:** SYNC
- **Description:** The policy based on which the follower FE flushes logs to disk. This parameter is valid only when the current FE is a follower FE. Valid values:
  - `SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.
  - `NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.
  - `WRITE_NO_SYNC`: When a transaction is committed, a log entry is generated simultaneously but is not flushed to disk.

#### replica_ack_policy

- **Default:** SIMPLE_MAJORITY
- **Description:** The policy based on which a log entry is considered valid. The default value `SIMPLE_MAJORITY` specifies that a log entry is considered valid if a majority of follower FEs return ACK messages.

#### cluster_id

- **Default:** -1
- **Description:** The ID of the StarRocks cluster to which the FE belongs. FEs or BEs that have the same cluster ID belong to the same StarRocks cluster. Valid values: any positive integer. The default value `-1` specifies that StarRocks will generate a random cluster ID for the StarRocks cluster at the time when the leader FE of the cluster is started for the first time.

### Query engine

#### publish_version_interval_ms

- **Default:** 10
- **Description:** The time interval at which release validation tasks are issued. Unit: ms.

#### statistic_cache_columns

- **Default:** 100000
- **Description:** The number of rows that can be cached for the statistics table.

#### statistic_cache_thread_pool_size

- **Default:** 10
- **Description:** The size of the thread-pool which will be used to refresh statistic caches.

### Loading and unloading

#### load_checker_interval_second

- **Default:** 5
- **Description:** The time interval at which load jobs are processed on a rolling basis. Unit: second.

#### transaction_clean_interval_second

- **Default:** 30
- **Description:** The time interval at which finished transactions are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that finished transactions can be cleaned up in a timely manner.

#### label_clean_interval_second

- **Default:** 14400
- **Description:** The time interval at which labels are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that historical labels can be cleaned up in a timely manner.

#### spark_dpp_version

- **Default:** 1.0.0
- **Description:** The version of Spark Dynamic Partition Pruning (DPP) used.

#### spark_resource_path

- **Default:** Empty string
- **Description:** The root directory of the Spark dependency package.

#### spark_launcher_log_dir

- **Default:** sys_log_dir + "/spark_launcher_log"
- **Description:** The directory that stores Spark log files.

#### yarn_client_path

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn"
- **Description:** The root directory of the Yarn client package.

#### yarn_config_dir

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"
- **Description:** The directory that stores the Yarn configuration file.

#### export_checker_interval_second

- **Default:** 5
- **Description:** The time interval at which load jobs are scheduled.

#### export_task_pool_size

- **Default:** 5
- **Description:** The size of the unload task thread pool.

### Storage

#### default_storage_medium

- **Default:** HDD
- **Description:** The default storage media that is used for a table or partition at the time of table or partition creation if no storage media is specified. Valid values: `HDD` and `SSD`. When you create a table or partition, the default storage media specified by this parameter is used if you do not specify a storage media type for the table or partition.

#### tablet_sched_storage_cooldown_second

- **Default:** -1
- **Description:** The latency of automatic cooling starting from the time of table creation. The alias of this parameter is `storage_cooldown_second`. Unit: second. The default value `-1` specifies that automatic cooling is disabled. If you want to enable automatic cooling, set this parameter to a value greater than `-1`.

#### tablet_stat_update_interval_second

- **Default:** 300
- **Description:** The time interval at which the FE retrieves tablet statistics from each BE. Unit: second.

### StarRocks shared-data cluster

#### run_mode

- **Default**: shared_nothing
- **Description**: The running mode of the StarRocks cluster. Valid values: shared_data and shared_nothing (Default).

  - shared_data indicates running StarRocks in shared-data mode.
  - shared_nothing indicates running StarRocks in shared-nothing mode.
CAUTION
You cannot adopt the shared_data and shared_nothing modes simultaneously for a StarRocks cluster. Mixed deployment is not supported.
DO NOT change run_mode after the cluster is deployed. Otherwise, the cluster fails to restart. The transformation from a shared-nothing cluster to a shared-data cluster or vice versa is not supported.

#### cloud_native_meta_port

- **Default**: 6090
- **Description**: The cloud-native meta service RPC port.

#### cloud_native_storage_type

- **Default**: S3
- **Description**: The type of object storage you use. In shared-data mode, StarRocks supports storing data in Azure Blob (supported from v3.1.1 onwards), and object storages that are compatible with the S3 protocol (such as AWS S3, Google GCP, and MinIO). Valid value: S3 (Default) and AZBLOB. If you specify this parameter as S3, you must add the parameters prefixed by aws_s3. If you specify this parameter as AZBLOB, you must add the parameters prefixed by azure_blob.

#### aws_s3_path

- **Default**: N/A
- **Description**: The S3 path used to store data. It consists of the name of your S3 bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.

#### aws_s3_endpoint

- **Default**: N/A
- **Description**: The endpoint used to access your S3 bucket, for example, `https://s3.us-west-2.amazonaws.com`.

#### aws_s3_region

- **Default**: N/A
- **Description**: The region in which your S3 bucket resides, for example, `us-west-2`.

#### aws_s3_use_aws_sdk_default_behavior

- **Default**: false
- **Description**: Whether to use the default authentication credential of AWS SDK. Valid values: true and false (Default).

#### aws_s3_use_instance_profile

- **Default**: false
- **Description**: Whether to use Instance Profile and Assumed Role as credential methods for accessing S3. Valid values: true and false (Default).
  - If you use IAM user-based credential (Access Key and Secret Key) to access S3, you must specify this item as false, and specify aws_s3_access_key and aws_s3_secret_key.
  - If you use Instance Profile to access S3, you must specify this item as true.
  - If you use Assumed Role to access S3, you must specify this item as true, and specify aws_s3_iam_role_arn.
  - And if you use an external AWS account, you must also specify aws_s3_external_id.

#### aws_s3_access_key

- **Default**: N/A
- **Description**: The Access Key ID used to access your S3 bucket.

#### aws_s3_secret_key

- **Default**: N/A
- **Description**: The Secret Access Key used to access your S3 bucket.

#### aws_s3_iam_role_arn

- **Default**: N/A
- **Description**: The ARN of the IAM role that has privileges on your S3 bucket in which your data files are stored.

#### aws_s3_external_id

- **Default**: N/A
- **Description**: The external ID of the AWS account that is used for cross-account access to your S3 bucket.

#### azure_blob_path

- **Default**: N/A
- **Description**: The Azure Blob Storage path used to store data. It consists of the name of the container within your storage account and the sub-path (if any) under the container, for example, testcontainer/subpath.

#### azure_blob_endpoint

- **Default**: N/A
- **Description**: The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`.

#### azure_blob_shared_key

- **Default**: N/A
- **Description**: The Shared Key used to authorize requests for your Azure Blob Storage.

#### azure_blob_sas_token

- **Default**: N/A
- **Description**: The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage.

### Other FE static parameters

#### plugin_dir

- **Default:** STARROCKS_HOME_DIR/plugins
- **Description:** The directory that stores plugin installation packages.

#### small_file_dir

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/small_files"
- **Description:** The root directory of small files.

#### max_agent_task_threads_num

- **Default:** 4096
- **Description:** The maximum number of threads that are allowed in the agent task thread pool.

#### auth_token

- **Default:** Empty string
- **Description:** The token that is used for identity authentication within the StarRocks cluster to which the FE belongs. If this parameter is left unspecified, StarRocks generates a random token for the cluster at the time when the leader FE of the cluster is started for the first time.

#### tmp_dir

- **Default:** StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"
- **Description:** The directory that stores temporary files such as files generated during backup and restore procedures. After these procedures finish, the generated temporary files are deleted.

#### locale

- **Default:** zh_CN.UTF-8
- **Description:** The character set that is used by the FE.

#### hive_meta_load_concurrency

- **Default:** 4
- **Description:** The maximum number of concurrent threads that are supported for Hive metadata.

#### hive_meta_cache_refresh_interval_s

- **Default:** 7200
- **Description:** The time interval at which the cached metadata of Hive external tables is updated. Unit: second.

#### hive_meta_cache_ttl_s

- **Default:** 86400
- **Description:** The amount of time after which the cached metadata of Hive external tables expires. Unit: second.

#### hive_meta_store_timeout_s

- **Default:** 10
- **Description:** The amount of time after which a connection to a Hive metastore times out. Unit: second.

#### es_state_sync_interval_second

- **Default:** 10
- **Description:** The time interval at which the FE obtains Elasticsearch indexes and synchronizes the metadata of StarRocks external tables. Unit: second.

#### enable_auth_check

- **Default:** TRUE
- **Description:** Specifies whether to enable the authentication check feature. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature.

#### enable_metric_calculator

- **Default:** TRUE
- **Description:** Specifies whether to enable the feature that is used to periodically collect metrics. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature.

#### jdbc_connection_pool_size

- **Default:** 8
- **Description:** The maximum capacity of the JDBC connection pool for accessing JDBC catalogs.

#### jdbc_minimum_idle_connections

- **Default:** 1
- **Description:** The minimum number of idle connections in the JDBC connection pool for accessing JDBC catalogs.

#### jdbc_connection_idle_timeout_ms

- **Unit**: ms
- **Default:** 600000
- **Description:** The maximum amount of time after which a connection for accessing a JDBC catalog times out. Timed-out connections are considered idle.
