# Parameter configuration

This topic describes FE, BE, and system parameters. It also provides suggestions on how to configure and tune these parameters.

## FE configuration items

FE parameters are classified into dynamic parameters and static parameters.

- Dynamic parameters can be configured and adjusted by running SQL commands, which is very convenient. But the configurations become invalid if you restart your FE. Therefore, we recommend that you also modify the configuration items in the `fe.conf` file to prevent the loss of modifications.

- Static parameters can only be configured and adjusted in the FE configuration file **fe.conf**. **After you modify this file, you must restart your FE for the changes to take effect.**

Whether a parameter is a dynamic parameter is indicated by the `IsMutable` column in the output of [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SHOW%20CONFIG.md). `TRUE` indicates a dynamic parameter.

Note that both dynamic and static FE parameters can be configured in the **fe.conf** file.

### View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
 ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
 ```

 For detailed description of the returned fields, see [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SHOW%20CONFIG.md).

> **NOTE**
>
> You must have administrator's privilege to run cluster administration-related commands.

### Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SET%20CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

> **NOTE**
>
> The configurations will be restored to the default values in the `fe.conf` file after the FE restarts. Therefore, we recommend that you also modify the configuration items in `fe.conf` to prevent the loss of modifications.

#### Logging

| Parameter      | Unit | Default | Description                                                  |
| -------------- | ---- | ------- | ------------------------------------------------------------ |
| qe_slow_log_ms | ms   | 5000    | The threshold used to determine whether a query is a slow query. If the response time of a query exceeds this threshold, it is recorded as a slow query in `fe.audit.log`. |

#### Metadata and cluster management

| Parameter                        | Unit | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| -------------------------------- | ---- | ------- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| catalog_try_lock_timeout_ms      | ms   | 5000    | The timeout duration to obtain the global lock.                                                                                                                                                                                                                                                                                                                                                                                                               |
| edit_log_roll_num                | -    | 50000   | The maximum number of metadata log entries that can be written before a log file is created for these log entries. <br>This parameter is used to control the size of log files. The new log file is written to the BDBJE database.                                                                                                                                                                                                                            |
| ignore_unknown_log_id            | -    | FALSE   | Whether to ignore an unknown log ID. When an FE is rolled back, the BEs of the earlier version may be unable to recognize some log IDs.<br>If the value is `TRUE`, the FE ignores unknown log IDs. If the value is `FALSE`, the FE exits.                                                                                                                                                                                                                     |
| ignore_materialized_view_error   | -    | FALSE   | Whether FE ignores the metadata exception caused by materialized view errors. If FE fails to start due to the metadata exception caused by materialized view errors, you can set this parameter to `true` to allow FE to ignore the exception. This parameter is supported from v2.5.10 onwards.                                                                                                                                                                                                                                                   |
| ignore_meta_check                | -    | FALSE   | Whether non-leader FEs ignore the metadata gap from the leader FE. If the value is TRUE, non-leader FEs ignore the metadata gap from the leader FE and continue providing data reading services.<br>This parameter ensures continuous data reading services even when you stop the leader FE for a long period of time.<br>If the value is FALSE, non-leader FEs do not ignore the metadata gap from the leader FE and stop providing data reading services. |
|meta_delay_toleration_second      | s    | 300     | The maximum duration by which the metadata on the follower and observer FEs can lag behind that on the leader FE. Unit: seconds.<br> If this duration is exceeded, the non-leader FE stops providing services.                                                                                                                                                                                                                                                |
| drop_backend_after_decommission  | -    | TRUE    | Whether to delete a BE after the BE is decommissioned. `TRUE` indicates that the BE is deleted immediately after it is decommissioned.<br>`FALSE` indicates that the BE is not deleted after it is decommissioned.                                                                                                                                                                                                                                            |
| enable_collect_query_detail_info | -    | FALSE   | Whether to view the profile of a query. If this parameter is set to `TRUE`, the system collects the profile of the query.<br>If this parameter is set to `FALSE`, the system does not collect the profile of the query.                                                                                                                                                                                                                                       |
| enable_background_refresh_connector_metadata | -    | `true` in v3.0<br />`false` in v2.5  | Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. `true` indicates to enable the Hive metadata cache refresh, and `false` indicates to disable it. This parameter is supported from v2.5.5 onwards.                                      |
| background_refresh_metadata_interval_millis         | ms   | 600000 | The interval between two consecutive Hive metadata cache refreshes. This parameter is supported from v2.5.5 onwards.                                                                                                                                                                                                                                                                                                                                          |
| background_refresh_metadata_time_secs_since_last_access_secs | s    | 86400  | The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cahced metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata. This parameter is supported from v2.5.5 onwards.                                                                                       |

#### Query engine

| Parameter                                | Unit | Default      | Description                                                  |
| ---------------------------------------- | ---- | ------------ | ------------------------------------------------------------ |
| max_allowed_in_element_num_of_delete     | -    | 10000        | The maximum number of elements allowed for the IN predicate in a DELETE statement. |
| enable_materialized_view                 | -    | TRUE         | Whether to enable the creation of materialized views.        |
| enable_decimal_v3                        | -    | TRUE         | Whether to support the DECIMAL V3 data type.                 |
| enable_sql_blacklist                     | -    | FALSE        | Whether to enable blacklist check for SQL queries. When this feature is enabled, queries in the blacklist cannot be executed. |
| dynamic_partition_check_interval_seconds | s    | 600          | The interval at which new data is checked. If new data is detected, StarRocks automatically creates partitions for the data. |
| dynamic_partition_enable                 | -    | TRUE         | Whether to enable the dynamic partitioning feature. When this feature is enabled, StarRocks dynamically creates partitions for new data and automatically deletes expired partitions to ensure the freshness of data. |
| max_partitions_in_one_batch              | -    | 4096         | The maximum number of partitions that can be created when you bulk create partitions. |
| max_query_retry_time                     | -    | 2            | The maximum number of query retries on an FE.                |
| max_create_table_timeout_second          | s    | 600          | The maximum timeout duration for creating a table, in seconds. |
| create_table_max_serial_replicas         | -    | 128          | The maximum number of replicas to create serially. If actual replica count exceeds this, replicas will be created concurrently. Try to reduce this config if table creation is taking a long time to complete. |
| max_running_rollup_job_num_per_table     | -    | 1            | The maximum number of rollup jobs can run in parallel for a table. |
| max_planner_scalar_rewrite_num           | -    | 100000       | The maximum number of times that the optimizer can rewrite a scalar operator. |
| enable_statistic_collect                 | -    | TRUE         | Whether to collect statistics for the CBO. This feature is enabled by default. |
| enable_collect_full_statistic            | -    | TRUE         | Whether to enable automatic full statistics collection. This feature is enabled by default. |
| statistic_auto_collect_ratio             | -    | 0.8          | The threshold for determining whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered. |
| statistic_max_full_collect_data_size | LONG      | 107374182400      | The size of the largest partition for automatic collection to collect data. Unit: Byte. If a partition exceeds this value, full collection is discarded and sampled collection is performed instead. |
| statistic_collect_max_row_count_per_query | INT  | 5000000000        | The maximum number of rows to query for a single analyze task. An analyze task will be split into multiple queries if this value is exceeded. |
| statistic_collect_interval_sec           | s    | 300          | The interval for checking data updates during automatic collection. Unit: seconds. |
| statistic_auto_analyze_start_time | STRING      | 00:00:00   | The start time of automatic collection. Value range: `00:00:00` - `23:59:59`. |
| statistic_auto_analyze_end_time | STRING      | 23:59:59  | The end time of automatic collection. Value range: `00:00:00` - `23:59:59`. |
| statistic_sample_collect_rows            | -    | 200000       | The minimum number of rows to collect for sampled collection. If the parameter value exceeds the actual number of rows in your table, full collection is performed. |
| histogram_buckets_size                   | -    | 64           | The default bucket number for a histogram.                   |
| histogram_mcv_size                       | -    | 100          | The number of most common values (MCV) for a histogram.      |
| histogram_sample_ratio                   | -    | 0.1          | The sampling ratio for a histogram.                          |
| histogram_max_sample_row_count           | -    | 10000000     | The maximum number of rows to collect for a histogram.       |
| statistics_manager_sleep_time_sec        | s    | 60           | The interval at which metadata is scheduled. Unit: seconds. The system performs the following operations based on this interval:<ul><li>Create tables for storing statistics. </li><li>Delete statistics that have been deleted.</li><li>Delete expired statistics.</li></ul>|
| statistic_update_interval_sec            | s    | 24 \* 60 \* 60 | The interval at which the cache of statistical information is updated. Unit: seconds. |
| statistic_analyze_status_keep_second     | s    | 259200       | The duration to retain the history of collection tasks. The default value is 3 days. Unit: seconds. |
|statistic_collect_concurrency             | -    |  3  | The maximum number of manual collection tasks that can run in parallel. The value defaults to 3, which means you can run a maximum of three manual collections tasks in parallel. <br>If the value is exceeded, incoming tasks will be in the PENDING state, waiting to be scheduled.|
| enable_local_replica_selection           | -    | FALSE        | Whether to select local replicas for queries. Local replicas reduce the network transmission cost. If this parameter is set to TRUE, the CBO preferentially selects tablet replicas on BEs that have the same IP address as the current FE. If this parameter is set to FALSE, both local replicas and non-local replicas can be selected. The default value is FALSE. |
| max_distribution_pruner_recursion_depth  | -    | 100          | The maximum recursion depth allowed by the partition pruner. Increasing the recursion depth can prune more elements but also increases CPU consumption. |
|enable_udf                                |  -   |    FALSE     | Whether to enable UDF .                            |

#### Loading and unloading

| Parameter                               | Unit | Default                                         | Description                                                  |
| --------------------------------------- | ---- | ----------------------------------------------- | ------------------------------------------------------------ |
| max_broker_load_job_concurrency         | -    | 5                                               | The maximum number of concurrent Broker Load jobs allowed within the StarRocks cluster. This parameter is valid only for Broker Load. The value of this parameter must be less than the value of `max_running_txn_num_per_db`. From v2.5 onwards, the default value is changed from `10` to `5`. The alias of this parameter is `async_load_task_pool_size`.|
| load_straggler_wait_second              | s    | 300                                             | The maximum loading lag that can be tolerated by a BE replica. If this value is exceeded, cloning is performed to clone data from other replicas. Unit: seconds. |
| desired_max_waiting_jobs                | -    | 1024                                            | The maximum number of pending jobs in an FE. The number refers to all jobs, such as table creation, loading, and schema change jobs. If the number of pending jobs in an FE reaches this value, the FE will reject new load requests. This parameter takes effect only for asynchronous loading. From v2.5 onwards, the default value is changed from 100 to 1024.|
| max_load_timeout_second                 | s    | 259200                                          | The maximum timeout duration allowed for a load job. The load job fails if this limit is exceeded. This limit applies to all types of load jobs. Unit: seconds. |
| min_load_timeout_second                 | s    | 1                                               | The minimum timeout duration allowed for a load job. This limit applies to all types of load jobs. Unit: seconds. |
| max_running_txn_num_per_db              | -    | 100                                             | The maximum number of load transactions allowed to be running for each database within a StarRocks cluster. The default value is `100`.<br>When the actual number of load transactions running for a database exceeds the value of this parameter, new load requests will not be processed. New requests for synchronous load jobs will be denied, and new requests for asynchronous load jobs will be placed in queue. We do not recommend you increase the value of this parameter because this will increase system load. |
| load_parallel_instance_num              | -    | 1                                               | The maximum number of concurrent loading instances for each load job on a BE. |
| disable_load_job                        | -    | FALSE                                           | Whether to disable loading when the cluster encounters an error. This prevents any loss caused by cluster errors. The default value is `FALSE`, indicating that loading is not disabled. |
| history_job_keep_max_second             | s    | 604800                                          | The maximum duration a historical job can be retained, such as schema change jobs, in seconds. |
| label_keep_max_num                      | -    | 1000                                            | The maximum number of load jobs that can be retained within a period of time. If this number is exceeded, the information of historical jobs will be deleted. |
| label_keep_max_second                   | s    | 259200                                          | The maximum duration the labels of load jobs that have been completed and are in the FINISHED or CANCELLED state can be retained in the StarRocks system. The default value is 3 days. After this duration expires, the labels will be deleted. This parameter applies to all types of load jobs. Unit: seconds. A value too large consumes a lot of memory. |
| max_routine_load_job_num                | -    | 100                                             | The maximum number of Routine Load jobs in a StarRocks cluster. This parameter is deprecated since v3.1.0. |
| max_routine_load_task_concurrent_num    | -    | 5                                               | The maximum number of concurrent tasks for each Routine Load job. |
| max_routine_load_task_num_per_be        | -    | 16                                               | The maximum number of concurrent Routine Load tasks on each BE. Since v3.1.0, the default value for this parameter is increased to 16 from 5, and no longer needs to be less than or equal to the value of BE static parameter `routine_load_thread_pool_size` (deprecated). |
| max_routine_load_batch_size             | Byte | 4294967296                                      | The maximum amount of data that can be loaded by a Routine Load task, in bytes. |
| routine_load_task_consume_second        | s    | 15                                              | The maximum time for each Routine Load task within the cluster to consume data .<br>Since v3.1.0, Routine Load job supports a new parameter `task_consume_second` in [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE%20ROUTINE%20LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible. |
| routine_load_task_timeout_second        | s    | 60                                              | The timeout duration for each Routine Load task within the cluster.<br>Since v3.1.0, Routine Load job supports a new parameter `task_timeout_second` in [job_properties](../sql-reference/sql-statements/data-manipulation/CREATE%20ROUTINE%20LOAD.md#job_properties). This parameter applies to individual load tasks within a Routine Load job, which is more flexible. |
| max_tolerable_backend_down_num          | -    | 0                                               | The maximum number of faulty BE nodes allowed. If this number is exceeded, Routine Load jobs cannot be automatically recovered. |
| period_of_auto_resume_min               | Min  | 5                                               | The interval at which Routine Load jobs are automatically recovered, in minutes. |
| spark_load_default_timeout_second       | s    | 86400                                           | The timeout duration for each Spark Load job, in seconds.    |
| spark_home_default_dir                  | -    | StarRocksFE.STARROCKS_HOME_DIR + "/lib/spark2x" | The root directory of a Spark client.                        |
| stream_load_default_timeout_second      | s    | 600                                             | The default timeout duration for each Stream Load job, in seconds. |
| max_stream_load_timeout_second          | s    | 259200                                          | The maximum allowed timeout duration for a Stream Load job, in seconds. |
| insert_load_default_timeout_second      | s    | 3600                                            | The timeout duration for the INSERT INTO statement that is used to load data, in seconds. |
| broker_load_default_timeout_second      | s    | 14400                                           | The timeout duration for a Broker Load job, in seconds.      |
| min_bytes_per_broker_scanner            | Byte | 67108864                                        | The minimum allowed amount of data that can be processed by a Broker Load instance, in bytes. |
| max_broker_concurrency                  | -    | 100                                             | The maximum number of concurrent instances for a Broker Load task. This parameter is deprecated from v3.1 onwards. |
| export_max_bytes_per_be_per_task        | Byte | 268435456                                       | The maximum amount of data that can be exported from a single BE by a single data unload task, in bytes. |
| export_running_job_num_limit            | -    | 5                                               | The maximum number of data exporting tasks that can run in parallel. |
| export_task_default_timeout_second      | s    | 7200                                            | The timeout duration for a data exporting task, in seconds.  |
| empty_load_as_error                     | -    | TRUE                                            | Whether to return an error message "all partitions have no load data" if no data is loaded. Values:<br> - TRUE: If no data is loaded, the system displays a failure message and returns an error "all partitions have no load data". <br> - FALSE: If no data is loaded, the system displays a success message and returns OK, instead of an error. |
| external_table_commit_timeout_ms        | ms    | 10000                                          | The timeout duration for committing (publishing) a write transaction to a StarRocks external table. The default value `10000` indicates a 10-second timeout duration. |

#### Storage

| Parameter                                     | Unit | Default                | Description                                                  |
| --------------------------------------------- | ---- | ---------------------- | ------------------------------------------------------------ |
| enable_strict_storage_medium_check            | -    | FALSE                  | Whether the FE strictly checks the storage medium of BEs when users create tables. If this parameter is set to `TRUE`, the FE checks the storage medium of BEs when users create tables and returns an error if the storage medium of the BE is different from the `storage_medium` parameter specified in the CREATE TABLE statement. For example, the storage medium specified in the CREATE TABLE statement is SSD but the actual storage medium of BEs is HDD. As a result, the table creation fails. If this parameter is `FALSE`, the FE does not check the storage medium of BEs when users create table. |
| enable_auto_tablet_distribution               | -    | TRUE                   | Whether to automatically set the number of buckets. <ul><li> If this parameter is set to `TRUE`, you don't need to specify the number of buckets when you create a table or add a partition. StarRocks automatically determines the number of buckets. For the strategy of automatically setting the number of buckets, see [Determine the number of buckets](../table_design/Data_distribution.md#determine-the-number-of-buckets).</li><li>If this parameter is set to `FALSE`, you need to manually specify the the number of buckets when you create a table or add a partition. If you do not specify the bucket count when adding a new partition to a table, the new partition inherits the bucket count set at the creation of the table. However, you can also manually specify the number of buckets for the new partition.</li></ul>Starting from version 2.5.7, StarRocks supports setting this parameter.|
| storage_usage_soft_limit_percent              | %    | 90                     | If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_usage_soft_limit_reserve_bytes`, tablets cannot be cloned into this directory. |
| storage_usage_soft_limit_reserve_bytes        | Byte | 200 \* 1024 \* 1024 \* 1024 | If the remaining storage space in the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_usage_soft_limit_percent`, tablets cannot be cloned into this directory. |
| catalog_trash_expire_second                   | s    | 86400                  | The longest duration the metadata can be retained after a table or database is deleted. If this duration expires, the data will be deleted and cannot be recovered. Unit: seconds. |
| alter_table_timeout_second                    | s    | 86400                  | The timeout duration for the schema change operation (ALTER TABLE). Unit: seconds. |
| recover_with_empty_tablet                     | -    | FALSE                  | Whether to replace a lost or corrupted tablet replica with an empty one. If a tablet replica is lost or corrupted, data queries on this tablet or other healthy tablets may fail. Replacing the lost or corrupted tablet replica with an empty tablet ensures that the query can still be executed. However, the result may be incorrect because data is lost. The default value is `FALSE`, which means lost or corrupted tablet replicas are not replaced with empty ones and the query fails. |
| tablet_create_timeout_second                  | s    | 10                      | The timeout duration for creating a tablet, in seconds.       |
| tablet_delete_timeout_second                  | s    | 2                      | The timeout duration for deleting a tablet, in seconds.      |
| check_consistency_default_timeout_second      | s    | 600                    | The timeout duration for a replica consistency check. You can set this parameter based on the size of your tablet. |
| tablet_sched_slot_num_per_path                | -    | 8                      | The maximum number of tablet-related tasks that can run concurrently in a BE storage directory. The alias is `schedule_slot_num_per_path`. From v2.5 onwards, the default value of this parameter is changed from `4` to `8`.|
| tablet_sched_max_scheduling_tablets           | -    | 10000                  | The maximum number of tablets that can be scheduled at the same time. If the value is exceeded, tablet balancing and repair checks will be skipped. |
| tablet_sched_disable_balance                  | -    | FALSE                  | Whether to disable tablet balancing. `TRUE` indicates that tablet balancing is disabled. `FALSE` indicates that tablet balancing is enabled. The alias is `disable_balance`. |
| tablet_sched_disable_colocate_balance         | -    | FALSE                  | Whether to disable replica balancing for Colocate Table. `TRUE` indicates replica balancing is disabled. `FALSE` indicates replica balancing is enabled. The alias is `disable_colocate_balance`. |
| tablet_sched_max_balancing_tablets            | -    | 500                    | The maximum number of tablets that can be balanced at the same time. If this value is exceeded, tablet re-balancing will be skipped. The alias is `max_balancing_tablets`. |
| tablet_sched_balance_load_disk_safe_threshold | -    | 0.5                    | The threshold for determining whether the BE disk usage is balanced. This parameter takes effect only when `tablet_sched_balancer_strategy` is set to `disk_and_tablet`. If the disk usage of all BEs is lower than 50%, disk usage is considered balanced. For the `disk_and_tablet` policy, if the difference between the highest and lowest BE disk usage is greater than 10%, disk usage is considered unbalanced and tablet re-balancing is triggered. The alias is `balance_load_disk_safe_threshold`. |
| tablet_sched_balance_load_score_threshold     | -    | 0.1                    | The threshold for determining whether the BE load is balanced. This parameter takes effect only when `tablet_sched_balancer_strategy` is set to `be_load_score`. A BE whose load is 10% lower than the average load is in low load state, and a BE whose load is 10% higher than the average load is in high load state. The alias is `balance_load_score_threshold`. |
| tablet_sched_repair_delay_factor_second       | s    | 60                     | The interval at which replicas are repaired, in seconds. The alias is `tablet_repair_delay_factor_second`. |
| tablet_sched_min_clone_task_timeout_sec       | s    | 3 \* 60                | The minimum timeout duration for cloning a tablet, in seconds. |
| tablet_sched_max_clone_task_timeout_sec       | s    | 2 \* 60 \* 60          | The maximum timeout duration for cloning a tablet, in seconds. The alias is `max_clone_task_timeout_sec`. |

#### Other FE dynamic parameters

| Parameter                                | Unit | Default     | Description                                                  |
| ---------------------------------------- | ---- | ----------- | ------------------------------------------------------------ |
| plugin_enable                            | -    | TRUE        | Whether plugins can be installed on FEs. Plugins can be installed or uninstalled only on the Leader FE. |
| max_small_file_number                    | -    | 100         | The maximum number of small files that can be stored on an FE directory. |
| max_small_file_size_bytes                | Byte | 1024 * 1024 | The maximum size of a small file, in bytes.                  |
| agent_task_resend_wait_time_ms           | ms   | 5000        | The duration the FE must wait before it can resend an agent task. An agent task can be resent only when the gap between the task creation time and the current time exceeds the value of this parameter. This parameter is used to prevent repetitive sending of agent tasks. Unit: ms. |
| backup_job_default_timeout_ms            | ms   | 86400*1000  | The timeout duration of a backup job, in ms. If this value is exceeded, the backup job fails. |
| report_queue_size                        | -    | 100         | The maximum number of jobs that can wait in a report queue. <br>The report is about disk, task, and tablet information of BEs. If too many report jobs are piling up in a queue, OOM will occur. |
| enable_experimental_mv                   | -    | TRUE       | Whether to enable the asynchronous materialized view feature. `TRUE` indicates this feature is enabled. From v2.5.2 onwards, this feature is enabled by default. For versions earlier than v2.5.2, this feature is disabled by default. |
| authentication_ldap_simple_bind_base_dn  | -  | Empty string | The base DN, which is the point from which the LDAP server starts to search for users' authentication information.|
| authentication_ldap_simple_bind_root_dn  |  -  | Empty string | The administrator DN used to search for users' authentication information.|
| authentication_ldap_simple_bind_root_pwd |  -  | Empty string | The password of the administrator used to search for users' authentication information.|
| authentication_ldap_simple_server_host   |  -  | Empty string |  The host on which the LDAP server runs.                               |
| authentication_ldap_simple_server_port   |  -  | 389     | The port of the LDAP server.                                       |
| authentication_ldap_simple_user_search_attr|  -  | uid     | The name of the attribute that identifies users in LDAP objects.|
|max_upload_task_per_be                    |  -  | 0       | In each BACKUP operation, the maximum number of upload tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number. This item is supported from v3.1.0 onwards.     |
|max_download_task_per_be                  |  -  | 0       | In each RESTORE operation, the maximum number of download tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number. This item is supported from v3.1.0 onwards.     |

### Configure FE static parameters

This section provides an overview of the static parameters that you can configure in the FE configuration file **fe.conf**. After you reconfigure these parameters for an FE, you must restart the FE for the changes to take effect.

#### Logging

| Parameter               | Default                                 | Description                                                  |
| ----------------------- | --------------------------------------- | ------------------------------------------------------------ |
| log_roll_size_mb        | 1024                                    | The size per log file. Unit: MB. The default value `1024` specifies the size per log file as 1 GB. |
| sys_log_dir             | StarRocksFE.STARROCKS_HOME_DIR + "/log" | The directory that stores system log files.                  |
| sys_log_level           | INFO                                    | The severity levels into which system log entries are classified. Valid values: `INFO`, `WARN`, `ERROR`, and `FATAL`. |
| sys_log_verbose_modules | Empty string                            | The modules for which StarRocks generates system logs. If this parameter is set to `org.apache.starrocks.catalog`, StarRocks generates system logs only for the catalog module. |
| sys_log_roll_interval   | DAY                                     | The time interval at which StarRocks rotates system log entries. Valid values: `DAY` and `HOUR`.<ul><li>If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of system log files.</li><li>If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of system log files.</li></ul> |
| sys_log_delete_age      | 7d                                      | The retention period of system log files. The default value `7d` specifies that each system log file can be retained for 7 days. StarRocks checks each system log file and deletes those that were generated 7 days ago. |
| sys_log_roll_num        | 10                                      | The maximum number of system log files that can be retained within each retention period specified by the `sys_log_roll_interval` parameter. |
| audit_log_dir           | StarRocksFE.STARROCKS_HOME_DIR + "/log" | The directory that stores audit log files.                   |
| audit_log_roll_num      | 90                                      | The maximum number of audit log files that can be retained within each retention period specified by the `audit_log_roll_interval` parameter. |
| audit_log_modules       | slow_query, query                       | The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the slow_query module and the query module. Separate the module names with a comma (,) and a space. |
| audit_log_roll_interval | DAY                                     | The time interval at which StarRocks rotates audit log entries. Valid values: `DAY` and `HOUR`.<ul><li>If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of audit log files.</li><li>If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of audit log files.</li></ul> |
| audit_log_delete_age    | 30d                                     | The retention period of audit log files. The default value `30d` specifies that each audit log file can be retained for 30 days. StarRocks checks each audit log file and deletes those that were generated 30 days ago. |
| dump_log_dir            | StarRocksFE.STARROCKS_HOME_DIR + "/log" | The directory that stores dump log files.                    |
| dump_log_modules        | query                                   | The modules for which StarRocks generates dump log entries. By default, StarRocks generates dump logs for the the query module. Separate the module names with a comma (,) and a space. |
| dump_log_roll_interval  | DAY                                     | The time interval at which StarRocks rotates dump log entries. Valid values: `DAY` and `HOUR`.<ul><li>If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of dump log files.</li><li>If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of dump log files.</li></ul> |
| dump_log_roll_num       | 10                                      | The maximum number of dump log files that can be retained within each retention period specified by the `dump_log_roll_interval` parameter. |
| dump_log_delete_age     | 7d                                      | The retention period of dump log files. The default value `7d` specifies that each dump log file can be retained for 7 days. StarRocks checks each dump log file and deletes those that were generated 7 days ago. |

#### Server

| Parameter                            | Default           | Description                                                  |
| ------------------------------------ | ----------------- | ------------------------------------------------------------ |
| frontend_address                     | 0.0.0.0           | The IP address of the FE node.                               |
| priority_networks                    | Empty string      | Declares a selection strategy for servers that have multiple IP addresses. Note that at most one IP address must match the list specified by this parameter. The value of this parameter is a list that consists of entries, which are separated with semicolons (;) in CIDR notation, such as 10.10.10.0/24. If no IP address matches the entries in this list, an IP address will be randomly selected. |
| http_port                            | 8030              | The port on which the HTTP server in the FE node listens.    |
| http_backlog_num                     | 1024              | The length of the backlog queue held by the HTTP server in the FE node. |
| cluster_name                         | StarRocks Cluster | The name of the StarRocks cluster to which the FE belongs. The cluster name is displayed for `Title` on the web page. |
| rpc_port                             | 9020              | The port on which the Thrift server in the FE node listens.  |
| thrift_backlog_num                   | 1024              | The length of the backlog queue held by the Thrift server in the FE node. |
| thrift_server_max_worker_threads     | 4096              | The maximum number of worker threads that are supported by the Thrift server in the FE node. |
| thrift_client_timeout_ms             | 5000                 | The length of time after which idle client connections time out. Unit: ms. |
| thrift_server_queue_size             | 4096              | The length of queue where requests are pending. If the number of threads that are being processed in the thrift server exceeds the value specified in `thrift_server_max_worker_threads`, new requests are added to the pending queue. |
| brpc_idle_wait_max_time              | 10000             | The maximum length of time for which BRPC clients wait as in the idle state. Unit: ms. |
| query_port                           | 9030              | The port on which the MySQL server in the FE node listens.   |
| mysql_service_nio_enabled            | TRUE              | Specifies whether asynchronous I/O is enabled for the FE node. |
| mysql_service_io_threads_num         | 4                 | The maximum number of threads that can be run by the MySQL server in the FE node to process I/O events. |
| mysql_nio_backlog_num                | 1024              | The length of the backlog queue held by the MySQL server in the FE node. |
| max_mysql_service_task_threads_num   | 4096              | The maximum number of threads that can be run by the MySQL server in the FE node to process tasks. |
| max_connection_scheduler_threads_num | 4096              | The maximum number of threads that are supported by the connection scheduler. |
| qe_max_connection                    | 1024              | The maximum number of connections that can be established by all users to the FE node. |
| check_java_version                   | TRUE              | Specifies whether to check version compatibility between the executed and compiled Java programs. If the versions are incompatible, StarRocks reports errors and aborts the startup of Java programs. |

#### Metadata and cluster management

| Parameter                         | Default                                  | Description                                                  |
| --------------------------------- | ---------------------------------------- | ------------------------------------------------------------ |
| meta_dir                          | StarRocksFE.STARROCKS_HOME_DIR + "/meta" | The directory that stores metadata.                          |
| heartbeat_mgr_threads_num         | 8                                        | The number of threads that can be run by the Heartbeat Manager to run heartbeat tasks. |
| heartbeat_mgr_blocking_queue_size | 1024                                     | The size of the blocking queue that stores heartbeat tasks run by the Heartbeat Manager. |
| metadata_failure_recovery         | FALSE                                    | Specifies whether to forcibly reset the metadata of the FE. Exercise caution when you set this parameter. |
| edit_log_port                     | 9010                                     | The port that is used for communication among the leader, follower, and observer FEs in the StarRocks cluster. |
| edit_log_type                     | BDB                                      | The type of edit log that can be generated. Set the value to `BDB`. |
| bdbje_heartbeat_timeout_second    | 30                                       | The amount of time after which the heartbeats among the leader, follower, and observer FEs in the StarRocks cluster time out. Unit: second. |
| bdbje_lock_timeout_second         | 1                                        | The amount of time after which a lock in the BDB JE-based FE times out. Unit: second. |
| max_bdbje_clock_delta_ms          | 5000                                     | The maximum clock offset that is allowed between the leader FE and the follower or observer FEs in the StarRocks cluster. Unit: ms. |
| txn_rollback_limit                | 100                                      | The maximum number of transactions that can be rolled back.  |
| bdbje_replica_ack_timeout_second  | 10                                       | The maximum amount of time for which the leader FE can wait for ACK messages from a specified number of follower FEs when metadata is written from the leader FE to the follower FEs. Unit: second. If a large amount of metadata is being written, the follower FEs require a long time before they can return ACK messages to the leader FE, causing ACK timeout. In this situation, metadata writes fail, and the FE process exits. We recommend that you increase the value of this parameter to prevent this situation. |
| master_sync_policy                | SYNC                                     | The policy based on which the leader FE flushes logs to disk. This parameter is valid only when the current FE is a leader FE. Valid values:<ul><li>`SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.</li><li>`NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.</li><li>`WRITE_NO_SYNC`: When a transaction is commited, a log entry is generated simultaneously but is not flushed to disk.</li></ul>If you have deployed only one follower FE, we recommend that you set this parameter to `SYNC`. If you have deployed three or more follower FEs, we recommend that you set this parameter and the `replica_sync_policy` both to `WRITE_NO_SYNC`. |
| replica_sync_policy               | SYNC                                     | The policy based on which the follower FE flushes logs to disk. This parameter is valid only when the current FE is a follower FE. Valid values:<ul><li>`SYNC`: When a transaction is committed, a log entry is generated and flushed to disk simultaneously.</li><li>`NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.</li><li>`WRITE_NO_SYNC`: When a transaction is committed, a log entry is generated simultaneously but is not flushed to disk.</li></ul> |
| replica_ack_policy                | SIMPLE_MAJORITY                          | The policy based on which a log entry is considered valid. The default value `SIMPLE_MAJORITY` specifies that a log entry is considered valid if a majority of follower FEs return ACK messages. |
| cluster_id                        | -1                                       | The ID of the StarRocks cluster to which the FE belongs. FEs or BEs that have the same cluster ID belong to the same StarRocks cluster. Valid values: any positive integer. The default value `-1` specifies that StarRocks will generate a random cluster ID for the StarRocks cluster at the time when the leader FE of the cluster is started for the first time. |

#### Query engine

| Parameter                   | Default | Description                                                  |
| --------------------------- |---------| ------------------------------------------------------------ |
| publish_version_interval_ms | 10      | The time interval at which release validation tasks are issued. Unit: ms. |
| statistic_cache_columns     | 100000  | The number of rows that can be cached for the statistics table. |
| statistic_cache_thread_pool_size     | 10      | The size of the thread-pool which will be used to refresh statistic caches. |

#### Loading and unloading

| Parameter                         | Default                                                      | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| load_checker_interval_second      | 5                                                            | The time interval at which load jobs are processed on a rolling basis. Unit: second. |
| transaction_clean_interval_second | 30                                                           | The time interval at which finished transactions are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that finished transactions can be cleaned up in a timely manner. |
| label_clean_interval_second       | 14400                                                        | The time interval at which labels are cleaned up. Unit: second. We recommend that you specify a short time interval to ensure that historical labels can be cleaned up in a timely manner. |
| spark_dpp_version                 | 1.0.0                                                        | The version of Spark Dynamic Partition Pruning (DPP) used.   |
| spark_resource_path               | Empty string                                                 | The root directory of the Spark dependency package.          |
| spark_launcher_log_dir            | sys_log_dir + "/spark_launcher_log"                          | The directory that stores Spark log files.                   |
| yarn_client_path                  | StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-client/hadoop/bin/yarn" | The root directory of the Yarn client package.               |
| yarn_config_dir                   | StarRocksFE.STARROCKS_HOME_DIR + "/lib/yarn-config"          | The directory that stores the Yarn configuration file.       |
| export_checker_interval_second    | 5                                                            | The time interval at which load jobs are scheduled.          |
| export_task_pool_size             | 5                                                            | The size of the unload task thread pool.                     |

#### Storage

| Parameter                            | Default         | Description                                                  |
| ------------------------------------ | --------------- | ------------------------------------------------------------ |
| default_storage_medium               | HDD             | The default storage media that is used for a table or partition at the time of table or partition creation if no storage media is specified. Valid values: `HDD` and `SSD`. When you create a table or partition, the default storage media specified by this parameter is used if you do not specify a storage media type for the table or partition. |
| tablet_sched_balancer_strategy       | disk_and_tablet | The policy based on which load balancing is implemented among tablets. The alias of this parameter is `tablet_balancer_strategy`. Valid values: `disk_and_tablet` and `be_load_score`. |
| tablet_sched_storage_cooldown_second | -1              | The latency of automatic cooling starting from the time of table creation. The alias of this parameter is `storage_cooldown_second`. Unit: second. The default value `-1` specifies that automatic cooling is disabled. If you want to enable automatic cooling, set this parameter to a value greater than `-1`. |
| tablet_stat_update_interval_second   | 300             | The time interval at which the FE retrieves tablet statistics from each BE. Unit: second. |

#### StarRocks shared-data cluster

| Parameter                           | Default         | Description                                                  |
| ----------------------------------- | --------------- | ------------------------------------------------------------ |
| run_mode                            | shared_nothing  | The running mode of the StarRocks cluster. Valid values: `shared_data` and `shared_nothing` (Default). <br>`shared_data` indicates running StarRocks in shared-data mode. `shared_nothing` indicates running StarRocks in classic mode.<br />**CAUTION**<br />You cannot adopt the `shared_data` and `shared_nothing` modes simultaneously for a StarRocks cluster. Mixed deployment is not supported.<br />DO NOT change `run_mode` after the cluster is deployed. Otherwise, the cluster fails to restart. The transformation from a classic cluster to a shared-data cluster or vice versa is not supported. |
| cloud_native_meta_port              | 6090            | The cloud-native meta service RPC port. Default: `6090`.     |
| cloud_native_storage_type           | S3              | The type of object storage you use. In shared-data mode, StarRocks supports storing data in <!--Azure Blob (Preview feature, supported from v3.2 onwards), and -->object storages that are compatible with the S3 protocol (such as AWS S3, Google GCP, and MinIO). Valid value: `S3` (Default)<!-- and `AZBLOB`-->. If you specify this parameter as `S3`, you must add the parameters prefixed by `aws_s3`. <!--If you specify this parameter as `AZBLOB`, you must add the parameters prefixed by `azure_blob`.--> |
| aws_s3_path                         | N/A             | The S3 path used to store data. It consists of the name of your S3 bucket and the sub-path (if any) under it, for example, `testbucket/subpath`. |
| aws_s3_endpoint                     | N/A             | The endpoint used to access your S3 bucket, for example, `https://s3.us-west-2.amazonaws.com`. |
| aws_s3_region                       | N/A             | The region in which your S3 bucket resides, for example, `us-west-2`. |
| aws_s3_use_aws_sdk_default_behavior | false           | Whether to use the default authentication credential of AWS SDK. Valid values: `true` and `false` (Default). |
| aws_s3_use_instance_profile         | false           | Whether to use Instance Profile and Assumed Role as credential methods for accessing S3. Valid values: `true` and `false` (Default). <ul><li>If you use IAM user-based credential (Access Key and Secret Key) to access S3, you must specify this item as `false`, and specify `aws_s3_access_key` and `aws_s3_secret_key`. </li><li>If you use Instance Profile to access S3, you must specify this item as `true`. </li><li>If you use Assumed Role to access S3, you must specify this item as `true`, and specify `aws_s3_iam_role_arn`. </li><li>And if you use an external AWS account,  you must also specify `aws_s3_external_id`.</li></ul> |
| aws_s3_access_key                   | N/A             | The Access Key ID used to access your S3 bucket.             |
| aws_s3_secret_key                   | N/A             | The Secret Access Key used to access your S3 bucket.         |
| aws_s3_iam_role_arn                 | N/A             | The ARN of the IAM role that has privileges on your S3 bucket in which your data files are stored. |
| aws_s3_external_id                  | N/A             | The external ID of the AWS account that is used for cross-account access to your S3 bucket. |
<!--| azure_blob_path                     | N/A             | The Azure Blob Storage path used to store data. It consists of the name of the container within your storage account and the sub-path (if any) under the container, for example, `testcontainer/subpath`. |
| azure_blob_endpoint                 | N/A             | The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`. |
| azure_blob_shared_key               | N/A             | The Shared Key used to authorize requests for your Azure Blob Storage.                     |
| azure_blob_sas_token                | N/A             | The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage.                |-->

#### Other FE static parameters

| Parameter                          | Default                                         | Description                                                  |
| ---------------------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| plugin_dir                         | STARROCKS_HOME_DIR/plugins                      | The directory that stores plugin installation packages.      |
| small_file_dir                     | StarRocksFE.STARROCKS_HOME_DIR + "/small_files" | The root directory of small files.                           |
| max_agent_task_threads_num         | 4096                                            | The maximum number of threads that are allowed in the agent task thread pool. |
| auth_token                         | Empty string                                    | The token that is used for identity authentication within the StarRocks cluster to which the FE belongs. If this parameter is left unspecified, StarRocks generates a random token for the cluster at the time when the leader FE of the cluster is started for the first time. |
| tmp_dir                            | StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"    | The directory that stores temporary files such as files generated during backup and restore procedures. After these procedures finish, the generated temporary files are deleted. |
| locale                             | zh_CN.UTF-8                                     | The character set that is used by the FE.                    |
| hive_meta_load_concurrency         | 4                                               | The maximum number of concurrent threads that are supported for Hive metadata. |
| hive_meta_cache_refresh_interval_s | 7200                                            | The time interval at which the cached metadata of Hive external tables is updated. Unit: second. |
| hive_meta_cache_ttl_s              | 86400                                           | The amount of time after which the cached metadata of Hive external tables expires. Unit: second. |
| hive_meta_store_timeout_s          | 10                                              | The amount of time after which a connection to a Hive metastore times out. Unit: second. |
| es_state_sync_interval_second      | 10                                              | The time interval at which the FE obtains Elasticsearch indexes and synchronizes the metadata of StarRocks external tables. Unit: second. |
| enable_auth_check                  | TRUE                                            | Specifies whether to enable the authentication check feature. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature. |
| enable_metric_calculator           | TRUE                                            | Specifies whether to enable the feature that is used to periodically collect metrics. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature. |

## BE configuration items

Some BE configuration items are dynamic parameters which you can set them by commands when BE nodes are still online. The rest of them are static parameters. You can only set the static parameters of a BE node by changing them in the corresponding configuration file **be.conf**, and restart the BE node to allow the change to take effect.

### View BE configuration items

You can view the BE configuration items using the following command:

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

### Configure BE dynamic parameters

You can configure a dynamic parameter of a BE node by using the `curl` command.

```Shell
curl -XPOST http://be_host:http_port/api/update_config?<configuration_item>=<value>
```

BE dynamic parameters are as follows.

| Configuration item | Default | Unit | Description |
| ------------------ | ------- | ---- | ----------- |
| report_task_interval_seconds | 10 | Second | The time interval at which to report the state of a task. A task can be creating a table, dropping a table, loading data, or changing a table schema. |
| report_disk_state_interval_seconds | 60 | Second | The time interval at which to report the storage volume state, which includes the size of data within the volume. |
| report_tablet_interval_seconds | 60 | Second | The time interval at which to report the most updated version of all tablets. |
| report_workgroup_interval_seconds | 5 | Second | The time interval at which to report the most updated version of all workgroups. |
| max_download_speed_kbps | 50000 | KB/s | The maximum download speed of each HTTP request. This value affects the performance of data replica synchronization across BE nodes. |
| download_low_speed_limit_kbps | 50 | KB/s | The download speed lower limit of each HTTP request. An HTTP request aborts when it constantly runs with a lower speed than this value within the time span specified in the configuration item download_low_speed_time. |
| download_low_speed_time | 300 | Second | The maximum time that an HTTP request can run with a download speed lower than the limit. An HTTP request aborts when it constantly runs with a lower speed than the value of download_low_speed_limit_kbps within the time span specified in this configuration item. |
| status_report_interval | 5 | Second | The time interval at which a query reports its profile, which can be used for query statistics collection by FE. |
| scanner_thread_pool_thread_num | 48 | N/A | The number of threads which the storage engine used for concurrent storage volume scanning. All threads are managed in the thread pool. |
| thrift_client_retry_interval_ms | 100 | ms | The time interval at which a thrift client retries. |
| scanner_thread_pool_queue_size | 102400 | N/A | The number of scan tasks supported by the storage engine. |
| scanner_row_num | 16384 | N/A | The maximum row count returned by each scan thread in a scan. |
| max_scan_key_num | 1024 | N/A | The maximum number of scan key segmented by each query. |
| max_pushdown_conditions_per_column | 1024 | N/A | The maximum number of conditions that allow pushdown in each column. If the number of conditions exceeds this limit, the predicates are not pushed down to the storage layer. |
| exchg_node_buffer_size_bytes | 10485760 | Byte | The maximum buffer size on the receiver end of an exchange node for each query. This configuration item is a soft limit. A backpressure is triggered when data is sent to the receiver end with an excessive speed. |
| memory_limitation_per_thread_for_schema_change | 2 | GB | The maximum memory size allowed for each schema change task. |
| update_cache_expire_sec | 360 | Second | The expiration time of Update Cache. |
| file_descriptor_cache_clean_interval | 3600 | Second | The time interval at which to clean file descriptors that have not been used for a certain period of time. |
| disk_stat_monitor_interval | 5 | Second | The time interval at which to monitor health status of disks. |
| unused_rowset_monitor_interval | 30 | Second | The time interval at which to clean the expired rowsets. |
| max_percentage_of_error_disk | 0 | % | The maximum percentage of error that is tolerable in a storage volume before the corresponding BE node quits. |
| default_num_rows_per_column_file_block | 1024 | N/A | The maximum number of rows that can be stored in each row block. |
| pending_data_expire_time_sec | 1800 | Second | The expiration time of the pending data in the storage engine. |
| inc_rowset_expired_sec | 1800 | Second | The expiration time of the incoming data. This configuration item is used in incremental clone. |
| tablet_rowset_stale_sweep_time_sec | 1800 | Second | The time interval at which to sweep the stale rowsets in tablets. |
| snapshot_expire_time_sec | 172800 | Second | The expiration time of snapshot files. |
| trash_file_expire_time_sec | 259200 | Second | The time interval at which to clean trash files. |
| base_compaction_check_interval_seconds | 60 | Second | The time interval of thread polling for a Base Compaction. |
| min_base_compaction_num_singleton_deltas | 5 | N/A | The minimum number of segments that trigger a Base Compaction. |
| max_base_compaction_num_singleton_deltas | 100 | N/A | The maximum number of segments that can be compacted in each Base Compaction. |
| base_compaction_interval_seconds_since_last_operation | 86400 | Second | The time interval since the last Base Compaction. This configuration item is one of the conditions that trigger a Base Compaction. |
| cumulative_compaction_check_interval_seconds | 1 | Second | The time interval of thread polling for a Cumulative Compaction. |
| update_compaction_check_interval_seconds | 60 | Second | The time interval at which to check the Update Compaction of the Primary Key table. |
| min_compaction_failure_interval_sec | 120 | Second | The minimum time interval that a Tablet Compaction can be scheduled since the last compaction failure. |
| max_compaction_concurrency | -1 | N/A | The maximum concurrency of compactions (both Base Compaction and Cumulative Compaction). The value -1 indicates that no limit is imposed on the concurrency. |
| periodic_counter_update_period_ms | 500 | ms | The time interval at which to collect the Counter statistics. |
| load_error_log_reserve_hours | 48 | Hour | The time for which data loading logs are reserved. |
| streaming_load_max_mb | 10240 | MB | The maximum size of a file that can be streamed into StarRocks. |
| streaming_load_max_batch_size_mb | 100 | MB | The maximum size of a JSON file that can be streamed into StarRocks. |
| memory_maintenance_sleep_time_s | 10 | Second | The time interval at which ColumnPool GC is triggered. StarRocks executes GC periodically, and returns the released memory memory to the operating system. |
| write_buffer_size | 104857600 | Byte | The buffer size of MemTable in the memory. This configuration item is the threshold to trigger a flush. |
| tablet_stat_cache_update_interval_second | 300 | Second | The time interval at which to update Tablet Stat Cache. |
| result_buffer_cancelled_interval_time | 300 | Second | The wait time before BufferControlBlock release data. |
| thrift_rpc_timeout_ms | 5000 | ms | The timeout for a thrift RPC. |
| txn_commit_rpc_timeout_ms | 20000 | ms | The timeout for a transaction commit RPC. |
| max_consumer_num_per_group | 3 | N/A | The maximum number of consumers in a consumer group of Routine Load. |
| max_memory_sink_batch_count | 20 | N/A | The maximum number of Scan Cache batches. |
| scan_context_gc_interval_min | 5 | Minute | The time interval at which to clean the Scan Context. |
| path_gc_check_step | 1000 | N/A | The maximum number of files that can be scanned continuously each time. |
| path_gc_check_step_interval_ms | 10 | ms | The time interval between file scans. |
| path_scan_interval_second | 86400 | Second | The time interval at which GC cleans expired data. |
| storage_flood_stage_usage_percent | 95 | % | If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_flood_stage_left_capacity_bytes`, Load and Restore jobs are rejected. |
| storage_flood_stage_left_capacity_bytes | 107374182400 | Byte | If the remaining storage space of the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_flood_stage_usage_percent`, Load and Restore jobs are rejected. |
| tablet_meta_checkpoint_min_new_rowsets_num | 10 | N/A | The minimum number of rowsets to create since the last TabletMeta Checkpoint. |
| tablet_meta_checkpoint_min_interval_secs | 600 | Second | The time interval of thread polling for a TabletMeta Checkpoint. |
| max_runnings_transactions_per_txn_map | 100 | N/A | The maximum number of transactions that can run concurrently in each partition. |
| tablet_max_pending_versions | 1000 | N/A | The maximum number of pending versions that are tolerable on a Primary Key tablet. Pending versions refer to versions that are committed but not applied yet. |
| tablet_max_versions | 1000 | N/A | The maximum number of versions allowed on a tablet. If the number of versions exceeds this value, new write requests will fail. |
| max_hdfs_file_handle | 1000 | N/A | The maximum number of HDFS file descriptors that can be opened. |
| be_exit_after_disk_write_hang_second | 60 | second | The length of time that the BE waits to exit after the disk hangs. |
| min_cumulative_compaction_failure_interval_sec | 30 | second | The minimum time interval at which Cumulative Compaction retries upon failures. |
| size_tiered_level_num | 7 | N/A | The number of levels for the Size-tiered Compaction strategy. At most one rowset is reserved for each level. Therefore, under a stable condition, there are, at most, as many rowsets as the level number specified in this configuration item. |
| size_tiered_level_multiple | 5 | N/A | The multiple of data size between two contiguous levels in the Size-tiered Compaction strategy. |
| size_tiered_min_level_size | 131072 | Byte | The data size of the minimum level in the Size-tiered Compaction strategy. Rowsets smaller than this value immediately trigger the data compaction. |
| storage_page_cache_limit | 20% | N/A | The PageCache size. STRING. It can be specified as size, for example, `20G`, `20480M`, `20971520K`, or `21474836480B`. It can also be specified as the ratio (percentage) to the memory size, for example, `20%`. It takes effect only when `disable_storage_page_cache` is set to `false`. |
| internal_service_async_thread_num | 10 | N/A | The thread pool size allowed on each BE for interacting with Kafka. Currently, the FE responsible for processing Routine Load requests depends on BEs to interact with Kafka, and each BE in StarRocks has its own thread pool for interactions with Kafka. If a large number of Routine Load tasks are distributed to a BE, the BE's thread pool for interactions with Kafka may be too busy to process all tasks in a timely manner. In this situation, you can adjust the value of this parameter to suit your needs. |

### Configure BE static parameters

You can only set the static parameters of a BE by changing them in the corresponding configuration file **be.conf**, and restart the BE to allow the changes to take effect.

BE static parameters are as follows.

| Configuration item | Default | Unit | Description |
| -------------------------------------------------- | ------------------------------------------------------------ | ------ | ------------------------------------------------------------ |
| hdfs_client_enable_hedged_read           | false         | Specifies whether to enable the hedged read feature. This parameter is supported from v3.0 onwards.                                    |
| hdfs_client_hedged_read_threadpool_size  | 128           | Specifies the size of the Hedged Read thread pool on your HDFS client. The thread pool size limits the number of threads to dedicate to the running of hedged reads in your HDFS client. This parameter is supported from v3.0 onwards. It is equivalent to the `dfs.client.hedged.read.threadpool.size` parameter in the `hdfs-site.xml` file of your HDFS cluster. |
| hdfs_client_hedged_read_threshold_millis | 2500          | Specifies the number of milliseconds to wait before starting up a hedged read. For example, you have set this parameter to `30`. In this situation, if a read from a block has not returned within 30 milliseconds, your HDFS client immediately starts up a new read against a different block replica. This parameter is supported from v3.0 onwards. It is equivalent to the `dfs.client.hedged.read.threshold.millis` parameter in the `hdfs-site.xml` file of your HDFS cluster. |
| be_port | 9060 | N/A | The BE thrift server port, which is used to receive requests from FEs. |
| brpc_port | 8060 | N/A | The BE BRPC port, which is used to view the network statistics of BRPCs. |
| brpc_num_threads | -1 | N/A | The number of bthreads of a BRPC. The value -1 indicates the same number with the CPU threads. |
| priority_networks | Empty string | N/A | The CIDR-formatted IP address that is used to specify the priority IP address of a BE node if the machine that hosts the BE node has multiple IP addresses. |
| heartbeat_service_port | 9050 | N/A | The BE heartbeat service port, which is used to receive heartbeats from FEs. |
| starlet_port | 9070 | N/A | The BE heartbeat service port for the StarRocks shared-data cluster. |
| heartbeat_service_thread_count | 1 | N/A | The thread count of the BE heartbeat service. |
| create_tablet_worker_count | 3 | N/A | The number of threads used to create a tablet. |
| drop_tablet_worker_count | 3 | N/A | The number of threads used to drop a tablet. |
| push_worker_count_normal_priority | 3 | N/A | The number of threads used to handle a load task with NORMAL priority. |
| push_worker_count_high_priority | 3 | N/A | The number of threads used to handle a load task with HIGH priority. |
| transaction_publish_version_worker_count | 0 | N/A | The maximum number of threads used to publish a version. When this value is set to less than or equal to `0`, the system uses half of the CPU core count as the value, so as to avoid insufficient thread resources when import concurrency is high but only a fixed number of threads are used. From v2.5, the default value has been changed from `8` to `0`.  |
| clear_transaction_task_worker_count | 1 | N/A | The number of threads used for clearing transaction. |
| alter_tablet_worker_count | 3 | N/A | The number of threads used for schema change. |
| clone_worker_count | 3 | N/A | The number of threads used for clone. |
| storage_medium_migrate_count | 1 | N/A | The number of threads used for storage medium migration (from SATA to SSD). |
| check_consistency_worker_count | 1 | N/A | The number of threads used for check the consistency of tablets. |
| sys_log_dir | ${STARROCKS_HOME}/log | N/A | The directory that stores system logs (including INFO, WARNING, ERROR, and FATAL). |
| user_function_dir | ${STARROCKS_HOME}/lib/udf | N/A | The directory used to store the User-defined Functions (UDFs). |
| small_file_dir | ${STARROCKS_HOME}/lib/small_file | N/A | The directory used to store the files downloaded by the file manager. |
| sys_log_level | INFO | N/A | The severity levels into which system log entries are classified. Valid values: INFO, WARN, ERROR, and FATAL. |
| sys_log_roll_mode | SIZE-MB-1024 | N/A | The mode how system logs are segmented into log rolls. Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-<size>`. The default value indicates that logs are segmented into rolls which are 1 GB each. |
| sys_log_roll_num | 10 | N/A | The number of log rolls to reserve. |
| sys_log_verbose_modules | Empty string | N/A | The module of the logs to be printed. For example, if you set this configuration item to OLAP, StarRocks only prints the logs of the OLAP module. Valid values are namespaces in BE, including `starrocks`, `starrocks::debug`, `starrocks::fs`, `starrocks::io`, `starrocks::lake`, `starrocks::pipeline`, `starrocks::query_cache`, `starrocks::stream`, and `starrocks::workgroup`. |
| sys_log_verbose_level | 10 | N/A | The level of the logs to be printed. This configuration item is used to control the output of logs initiated with VLOG in codes. |
| log_buffer_level | Empty string | N/A | The strategy how logs are flushed. The default value indicates that logs are buffered in memory. Valid values are `-1` and `0`. `-1` indicates that logs are not buffering in memory. |
| num_threads_per_core | 3 | N/A | The number threads started in each CPU core. |
| compress_rowbatches | TRUE | N/A | The boolean value to control if to compress the row batches in RPCs between BEs. This configuration item is used for the data transmission between query layers. The value true indicates to compress the row batches. The value false indicates not to compress the row batches. |
| serialize_batch | FALSE | N/A | The boolean value to control if to serialize the row batches in RPCs between BEs. This configuration item is used for the data transmission between query layers. The value true indicates to serialize the row batches. The value false indicates not to serialize the row batches. |
| storage_root_path | ${STARROCKS_HOME}/storage | N/A | The directory and medium of the storage volume. Multiple volumes are separated by semicolon (;). If the storage medium is SSD, add `,medium:ssd` at the end of the directory. If the storage medium is HDD, add `,medium:hdd` at the end of the directory. Example: `/data1,medium:hdd;/data2,medium:ssd`. |
|max_length_for_bitmap_function|1000000|Byte|The maximum length of input values for bitmap functions.|
|max_length_for_to_base64|200000|Byte|The maximum length of input values for to_base6() function.|
| max_tablet_num_per_shard | 1024 | N/A | The maximum number of tablets in each shard. This configuration item is used to restrict the number of tablet child directories under each storage directory. |
| max_garbage_sweep_interval | 3600 | Second | The maximum time interval for garbage collection on storage volumes. |
| min_garbage_sweep_interval | 180 | Second | The minimum time interval for garbage collection on storage volumes. |
| file_descriptor_cache_capacity | 16384 | N/A | The number of file descriptors that can be cached. |
| min_file_descriptor_number | 60000 | N/A | The minimum number of file descriptors in the BE process. |
| index_stream_cache_capacity | 10737418240 | Byte | The cache capacity for the statistical information of BloomFilter, Min, and Max. |
| disable_storage_page_cache | FALSE | N/A | The boolean value to control if to disable PageCache. When PageCache is enabled, StarRocks caches the query results. PageCache can significantly improve the query performance when similar queries are repeated frequently. `true` indicates to disable PageCache. The value of this item has been changed from `true` to `false` since StarRocks v2.4.|
| base_compaction_num_threads_per_disk | 1 | N/A | The number of threads used for Base Compaction on each storage volume. |
| base_cumulative_delta_ratio | 0.3 | N/A | The ratio of cumulative file size to base file size. The ratio reaching this value is one of the conditions that trigger the Base Compaction. |
| compaction_trace_threshold | 60 | Second | The time threshold for each compaction. If a compaction takes more time than the time threshold, StarRocks prints the corresponding trace. |
| be_http_port | 8040 | N/A | The HTTP server port. |
| be_http_num_workers | 48 | N/A | The number of threads used by the HTTP server. |
| load_data_reserve_hours | 4 | Hour | The reservation time for the files produced by small-scale loadings. |
| number_tablet_writer_threads | 16 | N/A | The number of threads used for Stream Load. |
| streaming_load_rpc_max_alive_time_sec | 1200 | Second | The RPC timeout for Stream Load. |
| fragment_pool_thread_num_min | 64 | N/A | The minimum number of threads used for query. |
| fragment_pool_thread_num_max | 4096 | N/A | The maximum number of threads used for query. |
| fragment_pool_queue_size | 2048 | N/A | The upper limit of query number that can be processed on each BE node. |
| enable_token_check | TRUE | N/A | The boolean value to control if to enable the token check. The value true indicates to enable the token check. The value false indicates to disable the token check. |
| enable_prefetch | TRUE | N/A | The boolean value to control if to enable the pre-fetch of the query. The value true indicates to enable the pre-fetch. The value false indicates to disable the pre-fetch. |
| load_process_max_memory_limit_bytes | 107374182400 | Byte | The maximum size limit of memory resources can be taken up by all load process on a BE node. |
| load_process_max_memory_limit_percent | 30 | % | The maximum percentage limit of memory resources can be taken up by all load process on a BE node. |
| sync_tablet_meta | FALSE | N/A | The boolean value to control if to enable the synchronization of the tablet metadata. The value true indicates to enable the synchronization. The value false indicates to disable the synchronization. |
| routine_load_thread_pool_size | 10 | N/A | The thread pool size for Routine Load on each BE. Since v3.1.0, this parameter is deprecated. The thread pool size for Routine Load on each BE is now controlled by the FE dynamic parameter `max_routine_load_task_num_per_be`. |
| brpc_max_body_size | 2147483648 | Byte | The maximum body size of a BRPC. |
| tablet_map_shard_size | 32 | N/A | The tablet map shard size. The value must be the power of two. |
| enable_bitmap_union_disk_format_with_set | FALSE | N/A | The boolean value to control if to enable the new storage format of the BITMAP type, which can improve the performance of bitmap_union. The value true indicates to enable the new storage format. The value false indicates to disable the new storage format. |
| mem_limit | 90% | N/A | BE process memory upper limit. You can set it as a percentage ("80%") or a physical limit ("100GB"). |
| flush_thread_num_per_store | 2 | N/A | Number of threads that are used for flushing MemTable in each store. |
| block_cache_enable  | false | N/A   | Whether to enable Data Cache.<ul><li>`true`: Data Cache is enabled.</li><li>`false`: Data Cache is disabled. </li></ul>To enable Data Cache, set the value of this parameter to `true`. |
| block_cache_disk_path | N/A | N/A  | The paths of disks. We recommend that the number of paths you configured for this parameter is the same as the number of disks of your BE machine. Multiple paths need to be separated with semicolons (;). After you add this parameter, StarRocks automatically creates a file named **cachelib_data** to cache blocks. |
| block_cache_meta_path | N/A  | N/A   | The storage path of block metadata. You can customize the storage path. We recommend that you store the metadata under the **$STARROCKS_HOME** path. |
| block_cache_mem_size   | 2147483648 | Bytes | The maximum amount of data that can be cached in the memory. Unit: bytes. The default value is `2147483648`, which is 2 GB. We recommend that you set the value of this parameter to at least 20 GB. If StarRocks reads a large amount of data from disks after Data Cache is enabled, consider increasing the value. |
| block_cache_disk_size  | 0 | Bytes | The maximum amount of data that can be cached in a single disk. For example, if you configure two disk paths for the `block_cache_disk_path` parameter and set the value of the `block_cache_disk_size` parameter as `21474836480` (20 GB), a maximum of 40 GB data can be cached in these two disks. The default value is `0`, which indicates that only the memory is used to cache data. Unit: bytes. |
| jdbc_connection_pool_size  | 8 | The JDBC connection pool size. On each BE node, queries which access the external table with the same `jdbc_url` share the same connection pool. |
| jdbc_minimum_idle_connections  | 1 | The minimum number of idle connections in the JDBC connection pool. |
| jdbc_connection_idle_timeout_ms  | 600000 | The length of time after which an idle connection in the JDBC connection pool expires. If the connection idle time in the JDBC connection pool exceeds this value, the connection pool closes idle connections of more than the number specified in the configuration item `jdbc_minimum_idle_connections`. |
| query_cache_capacity  | 536870912 | The size of the query cache in the BE. Unit: bytes. The default size is 512 MB. The size cannot be less than 4 MB. If the memory capacity of the BE is insufficient to provision your expected query cache size, you can increase the memory capacity of the BE. |
| enable_event_based_compaction_framework  | TRUE | Whether to enable Event-based Compaction Framework.<ul><li>`true`: Event-based Compaction Framework is enabled.</li><li>`false`: Event-based Compaction Framework is disabled. </li></ul> Enabling Event-based Compaction Framework can greatly reduce the overhead of compaction in scenarios where there are many tablets or a single tablet has a large amount of data. |
| enable_size_tiered_compaction_strategy  | TRUE |  Whether to enable the Size-tiered Compaction strategy.<ul><li>`true`: The size-tiered Compaction strategy is enabled.</li><li>`false`: The size-tiered Compaction strategy is disabled. </li></ul> |

<!--| aws_sdk_logging_trace_enabled | 0 | N/A | |
| be_exit_after_disk_write_hang_second | 60 | N/A | |
| be_service_threads | 64 | N/A | |
| bitmap_filter_enable_not_equal | 0 | N/A | |
| bitmap_max_filter_items | 30 | N/A | |
| bitmap_max_filter_ratio | 1000 | N/A | |
| bitmap_serialize_version | 1 | N/A | |
| block_cache_block_size | 1048576 | N/A | |
| block_cache_disk_path |  | N/A | |
| block_cache_disk_size | 21474836480 | N/A | |
| block_cache_enable | 0 | N/A | |
| block_cache_mem_size | 2147483648 | N/A | |
| broker_write_timeout_seconds | 30 | N/A | |
| brpc_socket_max_unwritten_bytes | 1073741824 | N/A | |
| cardinality_of_inject | 10 | N/A | |
| chunk_reserved_bytes_limit | 2147483648 | N/A | |
| cluster_id | -1 | N/A | |
| column_dictionary_key_size_threshold | 0 | N/A | |
| compact_thread_pool_queue_size | 100 | N/A | |
| compact_threads | 4 | N/A | |
| compaction_max_memory_limit | -1 | N/A | |
| compaction_max_memory_limit_percent | 100 | N/A | |
| compaction_memory_limit_per_worker | 2147483648 | N/A | |
| consistency_max_memory_limit | 10G | N/A | |
| consistency_max_memory_limit_percent | 20 | N/A | |
| cumulative_compaction_num_threads_per_disk | 1 | N/A | |
| default_query_options | | N/A | |
| delete_worker_count_high_priority | 1 | N/A | |
| delete_worker_count_normal_priority | 2 | N/A | |
| deliver_broadcast_rf_passthrough_bytes_limit | 131072 | N/A | |
| deliver_broadcast_rf_passthrough_inflight_num | 10 | N/A | |
| dependency_librdkafka_debug | all | N/A | |
| dependency_librdkafka_debug_enable | 0 | N/A | |
| dictionary_encoding_ratio | 0.7 | N/A | |
| dictionary_speculate_min_chunk_size | 10000 | N/A | |
| directory_of_inject |  | N/A | |
| disable_column_pool | 0 | N/A | |
| disable_mem_pools | 0 | N/A | |
| download_worker_count | 1 | N/A | |
| enable_check_string_lengths | 1 | N/A | |
| enable_event_based_compaction_framework | 0 | N/A | |
| enable_load_colocate_mv | 0 | N/A | |
| enable_metric_calculator | 0 | N/A | |
| enable_new_load_on_memory_limit_exceeded | 1 | N/A | |
| enable_orc_late_materialization | 1 | N/A | |
| enable_schema_change_v2 | 1 | N/A | |
| enable_segment_overflow_read_chunk | 1 | N/A | |
| enable_system_metrics | 1 | N/A | |
| es_http_timeout_ms | 5000 | N/A | |
| es_index_max_result_window | 10000 | N/A | |
| es_scroll_keepalive | 5m | N/A | |
| experimental_s3_max_single_part_size | 16777216 | N/A | |
| experimental_s3_min_upload_part_size | 16777216 | N/A | |
| ignore_broken_disk | 0 | N/A | |
| ignore_load_tablet_failure | 0 | N/A | |
| ignore_rowset_stale_unconsistent_delete | 0 | N/A | |
| internal_service_async_thread_num | 10 | N/A | |
| io_coalesce_read_max_buffer_size | 8388608 | N/A | |
| io_coalesce_read_max_distance_size | 1048576 | N/A | |
| jaeger_endpoint | | N/A | |
| jdbc_connection_pool_size | 8 | N/A | |
| l0_l1_merge_ratio | 10 | N/A | |
| lake_gc_segment_expire_seconds | 259200 | N/A | |
| lake_metadata_cache_limit | 2147483648 | N/A | |
| late_materialization_ratio | 10 | N/A | |
| local_library_dir |  | N/A | |
| loop_count_wait_fragments_finish | 0 | N/A | |
| madvise_huge_pages | 0 | N/A | |
| make_snapshot_rpc_timeout_ms | 20000 | N/A | |
| make_snapshot_worker_count | 5 | N/A | |
| manual_compact_before_data_dir_load | 0 | N/A | |
| max_batch_publish_latency_ms | 100 | N/A | |
| max_client_cache_size_per_host | 10 | N/A | |
| max_cumulative_compaction_num_singleton_deltas | 1000 | N/A | |
| max_free_io_buffers | 128 | N/A | |
| max_hdfs_scanner_num | 50 | N/A | |
| max_length_for_bitmap_function | 1000000 | N/A | |
| max_length_for_to_base64 | 200000 | N/A | |
| max_load_dop | 16 | N/A | |
| max_pulsar_consumer_num_per_group | 10 | N/A | |
| max_row_source_mask_memory_bytes | 209715200 | N/A | |
| max_segment_file_size | 1073741824 | N/A | |
| max_transmit_batched_bytes | 262144 | N/A | |
| memory_max_alignment | 16 | N/A | |
| memory_ratio_for_sorting_schema_change | 0.8 | N/A | |
| meta_threshold_to_manual_compact | 10737418240 | N/A | |
| metric_late_materialization_ratio | 1000 | N/A | |
| min_base_compaction_size | 21474836480 | N/A | |
| min_cumulative_compaction_failure_interval_sec | 30 | N/A | |
| min_cumulative_compaction_num_singleton_deltas | 5 | N/A | |
| min_cumulative_compaction_size | 5368709120 | N/A | |
| mmap_buffers | 0 | N/A | |
| null_encoding | 0 | N/A | |
| num_cores | 0 | N/A | |
| object_storage_access_key_id |  | N/A | |
| object_storage_endpoint |  | N/A | |
| object_storage_endpoint_path_style_access | 0 | N/A | |
| object_storage_endpoint_use_https | 0 | N/A | |
| object_storage_max_connection | 102400 | N/A | |
| object_storage_region | | N/A | |
| object_storage_secret_access_key |  | N/A | |
| orc_coalesce_read_enable | 1 | N/A | |
| orc_file_cache_max_size | 8388608 | N/A | |
| orc_natural_read_size | 8388608 | N/A | |
| parallel_clone_task_per_path | 2 | N/A | |
| parquet_coalesce_read_enable | 1 | N/A | |
| parquet_header_max_size | 16384 | N/A | |
| parquet_late_materialization_enable | 1 | N/A | |
| path_gc_check | 1 | N/A | |
| path_gc_check_interval_second | 86400 | N/A | |
| pipeline_exec_thread_pool_thread_num | 0 | N/A | |
| pipeline_connector_scan_thread_num_per_cpu | 8 | N/A | The thread number per cpu of connector scanner  |
| pipeline_max_num_drivers_per_exec_thread | 10240 | N/A | |
| pipeline_prepare_thread_pool_queue_size | 102400 | N/A | |
| pipeline_prepare_thread_pool_thread_num | 0 | N/A | |
| pipeline_print_profile | 0 | N/A | |
| pipeline_scan_thread_pool_queue_size | 102400 | N/A | |
| pipeline_scan_thread_pool_thread_num | 0 | N/A | |
| pipeline_sink_brpc_dop | 64 | N/A | |
| pipeline_sink_buffer_size | 64 | N/A | |
| pipeline_sink_io_thread_pool_queue_size | 102400 | N/A | |
| pipeline_sink_io_thread_pool_thread_num | 0 | N/A | |
| plugin_path |  | N/A | |
| port | 20001 | N/A | |
| pprof_profile_dir |  | N/A | |
| pre_aggregate_factor | 80 | N/A | |
| priority_queue_remaining_tasks_increased_frequency | 512 | N/A | |
| profile_report_interval | 30 | N/A | |
| pull_load_task_dir |  | N/A | |
| query_cache_capacity | 536870912 | N/A | |
| query_debug_trace_dir |  | N/A | |
| query_scratch_dirs |  | N/A | |
| release_snapshot_worker_count | 5 | N/A | |
| repair_compaction_interval_seconds | 600 | N/A | |
| rewrite_partial_segment | 1 | N/A | |
| routine_load_kafka_timeout_second | 10 | N/A | |
| routine_load_pulsar_timeout_second | 10 | N/A | |
| rpc_compress_ratio_threshold | 1.1 | N/A | |
| scan_use_query_mem_ratio | 0.25 | N/A | |
| scratch_dirs | /tmp | N/A | |
| send_rpc_runtime_filter_timeout_ms | 1000 | N/A | |
| sleep_five_seconds | 5 | N/A | |
| sleep_one_second | 1 | N/A | |
| sorter_block_size | 8388608 | N/A | |
| storage_format_version | 2 | N/A | |
| sys_minidump_dir |  | N/A | |
| sys_minidump_enable | 0 | N/A | |
| sys_minidump_interval | 600 | N/A | |
| sys_minidump_limit | 20480 | N/A | |
| sys_minidump_max_files | 16 | N/A | |
| tablet_internal_parallel_max_splitted_scan_bytes | 536870912 | N/A | |
| tablet_internal_parallel_max_splitted_scan_rows | 1048576 | N/A | |
| tablet_internal_parallel_min_scan_dop | 4 | N/A | |
| tablet_internal_parallel_min_splitted_scan_rows | 16384 | N/A | |
| tablet_max_versions | 15000 | N/A | |
| tablet_writer_open_rpc_timeout_sec | 60 | N/A | |
| tc_max_total_thread_cache_bytes | 1073741824 | N/A | |
| thrift_connect_timeout_seconds | 3 | N/A | |
| thrift_port | 9060 | N/A | |
| txn_map_shard_size | 128 | N/A | |
| txn_shard_size | 1024 | N/A | |
| udf_thread_pool_size | 1 | N/A | |
| update_compaction_num_threads_per_disk | 1 | N/A | |
| update_compaction_per_tablet_min_interval_seconds | 120 | N/A | |
| update_memory_limit_percent | 60 | N/A | |
| upload_worker_count | 1 | N/A | |
| use_mmap_allocate_chunk | 0 | N/A | |
| vector_chunk_size | 4096 | N/A | |
| vertical_compaction_max_columns_per_group | 5 | N/A | |
| web_log_bytes | 1048576 | N/A | |-->
