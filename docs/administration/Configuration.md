# Parameter Configuration

This topic describes FE, BE, Broker, and system parameters. It also provides suggestions on how to configure and tune these parameters.

## FE configuration items

### FE static parameters

This section provides an overview of the static parameters that you can configure in the FE configuration file **fe.conf**. After you reconfigure these parameters for an FE, you must restart the FE to make the new parameter settings take effect.

#### Logging

| Parameter               | Default                                 | Description                                                  |
| ----------------------- | --------------------------------------- | ------------------------------------------------------------ |
| log_roll_size_mb        | 1024                                    | The size per log file. Unit: MB. The default value `1024` specifies the size per log file as 1 GB. |
| sys_log_dir             | StarRocksFE.STARROCKS_HOME_DIR + "/log" | The directory that stores system log files.                  |
| sys_log_level           | INFO                                    | The severity levels into which system log entries are classified. Valid values: `INFO`, `WARN`, `ERROR`, and `FATAL`. |
| sys_log_verbose_modules | Empty string                            | The modules for which StarRocks generates system logs. If this parameter is set to `org.apache.starrocks.catalog`, StarRocks generates system logs only for the catalog module. |
| sys_log_roll_interval   | DAY                                     | The time interval at which StarRocks rotates system log entries. Valid values: `DAY` and `HOUR`.<ul><li>If this parameter is set to `DAY`, a suffix in the `yyyyMMdd` format is added to the names of system log files.</li><li>If this parameter is set to `HOUR`, a suffix in the `yyyyMMddHH` format is added to the names of system log files.</li></ul> |
| sys_log_delete_age      | 7d                                      | The retention period of system log files. The default value `7d` specifies that each system log file can be retained for 7 days. StarRocks checks each system log file and deletes those that were generated 7 days ago. |
| sys_log_roll_num        | 10                                      | The maximum number of system log files that can be retaind within each retention period specified by the `sys_log_roll_interval` parameter. |
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
| thrift_client_timeout_ms             | 0                 | The length of time after which requests from clients time out. Unit: ms. The default value `0` specifies that requests from clients never time out. |
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
| master_sync_policy                | SYNC                                     | The policy based on which the leader FE flushes logs to disk. This parameter is valid only when the current FE is a leader FE. Valid values:<ul><li>`SYNC`: When a transaction is commited, a log entry is generated and flushed to disk simultaneously.</li><li>`NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.</li><li>`WRITE_NO_SYNC`: When a transaction is commited, a log entry is generated simultaneously but is not flushed to disk.</li></ul>If you have deployed only one follower FE, we recommend that you set this parameter to `SYNC`. If you have deployed three or more follower FEs, we recommend that you set this parameter and the `replica_sync_policy` both to `WRITE_NO_SYNC`. |
| replica_sync_policy               | SYNC                                     | The policy based on which the follower FE flushes logs to disk. This parameter is valid only when the current FE is a follower FE. Valid values:<ul><li>`SYNC`: When a transaction is commited, a log entry is generated and flushed to disk simultaneously.</li><li>`NO_SYNC`: The generation and flushing of a log entry do not occur at the same time when a transaction is committed.</li><li>`WRITE_NO_SYNC`: When a transaction is commited, a log entry is generated simultaneously but is not flushed to disk.</li></ul> |
| replica_ack_policy                | SIMPLE_MAJORITY                          | The policy based on which a log entry is considered valid. The default value `SIMPLE_MAJORITY` specifies that a log entry is considered valid if a majority of follower FEs return ACK messages. |
| cluster_id                        | -1                                       | The ID of the StarRocks cluster to which the FE belongs. FEs or BEs that have the same cluster ID belong to the same StarRocks cluster. Valid values: any positive integer. The default value `-1` specifies that StarRocks will generate a random cluster ID for the StarRocks cluster at the time when the leader FE of the cluster is started for the first time. |

#### Query engine

| Parameter                   | Default | Description                                                  |
| --------------------------- | ------- | ------------------------------------------------------------ |
| publish_version_interval_ms | 10      | The time interval at which release validation tasks are issued. Unit: ms. |
| statistic_cache_columns     | 100000  | The number of rows that can be cached for the statistics table. |

#### Loading and unloading

| Parameter                         | Default                                                      | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| async_load_task_pool_size         | 10                                                           | The size of the load task thread pool. This parameter is valid only for Broker Load. |
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
| enable_pipeline_load_for_insert   | FALSE                                                        | Specifies whether to use the pipeline engine to execute `INSERT INTO` statements. |

#### Storage

| Parameter                            | Default         | Description                                                  |
| ------------------------------------ | --------------- | ------------------------------------------------------------ |
| default_storage_medium               | HDD             | The default storage media that is used for a table or partition at the time of table or partition creation if no storage media is specified. Valid values: `HDD` and `SSD`. When you create a table or partition, the default storage media specified by this parameter is used if you do not specify a storage media type for the table or partition. |
| tablet_sched_balancer_strategy       | disk_and_tablet | The policy based on which load balancing is implemented among tablets. The alias of this parameter is `tablet_balancer_strategy`. Valid values: `disk_and_tablet` and `be_load_score`. |
| tablet_sched_storage_cooldown_second | -1              | The latency of automatic cooling starting from the time of table creation. The alias of this parameter is `storage_cooldown_second`. Unit: second. The default value `-1` specifies that automatic cooling is disabled. If you want to enable automatic cooling, set this parameter to a value greater than `-1`. |
| tablet_stat_update_interval_second   | 300             | The time interval at which the FE retrieves tablet statistics from each BE. Unit: second. |

#### Other

| Parameter                          | Default                                         | Description                                                  |
| ---------------------------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| plugin_dir                         | STARROCKS_HOME_DIR/plugins                      | The directory that stores plugin installation packages.      |
| small_file_dir                     | StarRocksFE.STARROCKS_HOME_DIR + "/small_files" | The root directory of small files.                           |
| max_agent_task_threads_num         | 4096                                            | The maximum number of threads that are allowed in the agent task thread pool. |
| auth_token                         | Empty string                                    | The token that is used for identity authentication whitin the StarRocks cluster to which the FE belongs. If this parameter is left unspecified, StarRocks generates a random token for the cluster at the time when the leader FE of the cluster is started for the first time. |
| tmp_dir                            | StarRocksFE.STARROCKS_HOME_DIR + "/temp_dir"    | The directory that stores temporary files such as files generated during backup and restore procedures. After these procedures finish, the generated temporary files are deleted. |
| locale                             | zh_CN.UTF-8                                     | The character set that is used by the FE.                    |
| hive_meta_load_concurrency         | 4                                               | The maximum number of concurrent threads that are supported for Hive metadata. |
| hive_meta_cache_refresh_interval_s | 7200                                            | The time interval at which the cached metadata of Hive external tables is updated. Unit: second. |
| hive_meta_cache_ttl_s              | 86400                                           | The amount of time after which the cached metadata of Hive external tables expires. Unit: second. |
| hive_meta_store_timeout_s          | 10                                              | The amount of time after which a connection to a Hive metastore times out. Unit: second. |
| es_state_sync_interval_second      | 10                                              | The time interval at which the FE obtains Elasticsearch indexes and synchronizes the metadata of StarRocks external tables. Unit: second. |
| enable_auth_check                  | TRUE                                            | Specifies whether to enable the authentication chek feature. Valide values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature. |
| enable_metric_calculator           | TRUE                                            | Specifies whether to enable the feature that is used to periodically collect metrics. Valide values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature. |

## BE configuration items

Some BE configuration items are dynamic parameters which you can set them by commands when BE nodes are still online. The rest of them are static parameters. You can only set the static parameters of a BE node by changing them in the corresponding configuration file **be.conf**, and restart the BE node to allow the change to take effect.

### View BE configuration items

You can view the BE configuration items using the following command:

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

### Configure BE dynamic parameters

You can configure a dynamic parameter of a BE node by using `curl` command.

```Shell
curl -XPOST http://be_host:http_port/api/update_config?<configuration_item>=<value>
```

BE dynamic parameters are as follows:

| Configuration item | Default | Unit | Description |
| ------------------ | ------- | ---- | ----------- |
| tc_use_memory_min | 0 | Byte | The minimum size of the TCMalloc-reserved memory. StarRocks does not return the released memory resource to the operating system if the size of the memory resource is less than this value. |
| tc_free_memory_rate | 0 | % | The maximum ratio of the TCMalloc-reserved memory size to the total memory size occupied by TCMalloc. StarRocks does not return the released memory resource to the operating system if the size ratio of the released memory to the total memory used by TCMalloc is less than this value. Range: [0,100]. |
| tc_gc_period | 60 | Second | The duration of a TCMalloc garbage collection (GC) cycle. |
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
| update_compaction_check_interval_seconds | 60 | Second | The time interval at which to check the Update Compaction of the Primary Key data model. |
| min_compaction_failure_interval_sec | 120 | Second | The minimum time interval that a Tablet Compaction can be scheduled since the last compaction failure. |
| periodic_counter_update_period_ms | 500 | ms | The time interval at which to collect the Counter statistics. |
| load_error_log_reserve_hours | 48 | Hour | The time for which data loading logs are reserved. |
| streaming_load_max_mb | 10240 | MB | The maximum size of a file that can be streamed into StarRocks. |
| streaming_load_max_batch_size_mb | 100 | MB | The maximum size of a JSON file that can be streamed into StarRocks. |
| memory_maintenance_sleep_time_s | 10 | Second | The time interval at which TCMalloc GC is triggered. StarRocks executes GC periodically, and returns the released memory memory to the operating system. |
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
| storage_flood_stage_usage_percent | 95 | % | The storage usage threshold (in percentage) that can trigger the rejection of a Load or Restore job if it is reached. |
| storage_flood_stage_left_capacity_bytes | 1073741824 | Byte | The minimum left capacity of the storage before the rejection of a Load or Restore job is triggered. |
| tablet_meta_checkpoint_min_new_rowsets_num | 10 | N/A | The minimum number of rowsets to create since the last TabletMeta Checkpoint. |
| tablet_meta_checkpoint_min_interval_secs | 600 | Second | The time interval of thread polling for a TabletMeta Checkpoint. |
| max_runnings_transactions_per_txn_map | 100 | N/A | The maximum number of transactions that can run concurrently in each partition. |
| tablet_max_pending_versions | 1000 | N/A | The maximum number of pending versions that are tolerable in a Primary Key table. Pending versions refer to versions that are committed but not applied yet. |
| max_hdfs_file_handle | 1000 | N/A | The maximum number of HDFS file descriptors that can be opened. |
| parquet_buffer_stream_reserve_size | 1048576 | Byte | The size of buffer that Parquet reader reserves for each column while reading data. |
| internal_service_async_thread_num | 10 | N/A | The thread pool size allowed on each BE for interacting with Kafka. Currently, the FE responsible for processing Routine Load requests depends on BEs to interact with Kafka, and each BE in StarRocks has its own thread pool for interactions with Kafka. If a large number of Routine Load tasks are distributed to a BE, the BE's thread pool for interactions with Kafka may be too busy to process all tasks in a timely manner. In this situation, you can adjust the value of this parameter to suit your needs. |

### Configure BE static parameters

You can only set the static parameters of a BE node by changing them in the corresponding configuration file **be.conf**, and restart the BE node to allow the change to take effect.

BE static parameters are as follows:

| Configuration item | Default | Unit | Description |
| -------------------------------------------------- | ------------------------------------------------------------ | ------ | ------------------------------------------------------------ |
| be_port | 9060 | N/A | The BE thrift server port, which is used to receive requests from FEs. |
| brpc_port | 8060 | N/A | The BE BRPC port, which is used to view the network statistics of BRPCs. |
| brpc_num_threads | -1 | N/A | The number of bthreads of a BRPC. The value -1 indicates the same number with the CPU threads. |
| priority_networks | Empty string | N/A | The CIDR-formatted IP address that is used to specify the priority IP address of a BE node if the machine that hosts the BE node has multiple IP addresses. |
| heartbeat_service_port | 9050 | N/A | The BE heartbeat service port, which is used to receive heartbeats from FEs. |
| heartbeat_service_thread_count | 1 | N/A | The thread count of the BE heartbeat service. |
| create_tablet_worker_count | 3 | N/A | The number of threads used to create a tablet. |
| drop_tablet_worker_count | 3 | N/A | The number of threads used to drop a tablet. |
| push_worker_count_normal_priority | 3 | N/A | The number of threads used to handle a load task with NORMAL priority. |
| push_worker_count_high_priority | 3 | N/A | The number of threads used to handle a load task with HIGH priority. |
| transaction_publish_version_worker_count | 8 | N/A | The number of threads used to publish a version. |
| clear_transaction_task_worker_count | 1 | N/A | The number of threads used for clearing transaction. |
| alter_tablet_worker_count | 3 | N/A | The number of threads used for schema change. |
| clone_worker_count | 3 | N/A | The number of threads used for clone. |
| storage_medium_migrate_count | 1 | N/A | The number of threads used for storage medium migration (from SATA to SSD). |
| check_consistency_worker_count | 1 | N/A | The number of threads used for check the consistency of tablets. |
| sys_log_dir | `${STARROCKS_HOME}/log` | N/A | The directory that stores system logs (including INFO, WARNING, ERROR, and FATAL). |
| user_function_dir | `${STARROCKS_HOME}/lib/udf` | N/A | The directory used to store the User-defined Functions (UDFs). |
| small_file_dir | `${STARROCKS_HOME}/lib/small_file` | N/A | The directory used to store the files downloaded by the file manager. |
| sys_log_level | INFO | N/A | The severity levels into which system log entries are classified. Valid values: INFO, WARN, ERROR, and FATAL. |
| sys_log_roll_mode | SIZE-MB-1024 | N/A | The mode how system logs are segmented into log rolls. Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-<size>`. The default value indicates that logs are segmented into rolls which are 1 GB each. |
| sys_log_roll_num | 10 | N/A | The number of log rolls to reserve. |
| sys_log_verbose_modules | Empty string | N/A | The module of the logs to be printed. For example, if you set this configuration item to OLAP, StarRocks only prints the logs of the OLAP module. Valid values are namespaces in BE, including `starrocks`, `starrocks::vectorized`, and `pipeline`. |
| sys_log_verbose_level | 10 | N/A | The level of the logs to be printed. This configuration item is used to control the ouput of logs initiated with VLOG in codes. |
| log_buffer_level | Empty string | N/A | The strategy how logs are flushed. The default value indicates that logs are buffered in memory. Valid values are `-1` and `0`. `-1` indicates that logs are not buffering in memory. |
| num_threads_per_core | 3 | N/A | The number threads started in each CPU core. |
| compress_rowbatches | TRUE | N/A | The boolean value to control if to compress the row batches in RPCs between BEs. This configuration item is used for the data transmission between query layers. The value true indicates to compress the row batches. The value false indicates not to compress the row batches. |
| serialize_batch | FALSE | N/A | The boolean value to control if to serialize the row batches in RPCs between BEs. This configuration item is used for the data transmission between query layers. The value true indicates to serialize the row batches. The value false indicates not to serialize the row batches. |
| storage_root_path | `${STARROCKS_HOME}/storage` | N/A | The directory and medium of the storage volume. Multiple volumes are separated by semicolon (;). If the storage medium is SSD, add `,medium:ssd` at the end of the directory. If the storage medium is HDD, add `,medium:hdd` at the end of the directory. Example: `/data1,medium:hdd;/data2,medium:ssd`. |
| max_tablet_num_per_shard | 1024 | N/A | The maximum number of tablets in each shard. This configuration item is used to restrict the number of tablet child directories under each storage directory. |
| max_garbage_sweep_interval | 3600 | Second | The maximum time interval for garbage collection on storage volumes. |
| min_garbage_sweep_interval | 180 | Second | The minimum time interval for garbage collection on storage volumes. |
| row_nums_check | TRUE | N/A | The boolean value to control if to check the row counts before and after the compaction. The value true indicates to enable the row count check. The value false indicates disable the row count check. |
| file_descriptor_cache_capacity | 16384 | N/A | The number of file descriptors that can be cached. |
| min_file_descriptor_number | 60000 | N/A | The minimum number of file descriptors in the BE process. |
| index_stream_cache_capacity | 10737418240 | Byte | The cache capacity for the statistical information of BloomFilter, Min, and Max. |
| storage_page_cache_limit | 0 | | The capacity of page cache. You can set it as a percentage ("20%") or a physical value ("100MB"). |
| disable_storage_page_cache | TRUE | N/A | The boolean value to control if to disable the Page Cache. The value true indicates to disable the Page Cache. The value false indicates to enable the Page Cache. |
| base_compaction_num_threads_per_disk | 1 | N/A | The number of threads used for Base Compaction on each storage volume. |
| base_cumulative_delta_ratio | 0.3 | N/A | The ratio of cumulative file size to base file size. The ratio reaching this value is one of the conditions that trigger the Base Compaction. |
| max_compaction_concurrency | -1 | N/A | The maximum concurrency of compactions (both Base Compaction and Cumulative Compaction). The value -1 indicates that no limit is imposed on the concurrency. |
| compaction_trace_threshold | 60 | Second | The time threshold for each compaction. If a compaction takes more time than the time threshold, StarRocks prints the corresponding trace. |
| webserver_port | 8040 | N/A | The HTTP server port. |
| webserver_num_workers | 48 | N/A | The number of threads used by the HTTP server. |
| load_data_reserve_hours | 4 | Hour | The reservation time for the files produced by small-scale loadings. |
| number_tablet_writer_threads | 16 | N/A | The number of threads used for Stream Load. |
| streaming_load_rpc_max_alive_time_sec | 1200 | Second | The RPC timeout for Stream Load. |
| fragment_pool_thread_num_min | 64 | N/A | The minimum number of threads used for query. |
| fragment_pool_thread_num_max | 4096 | N/A | The maximum number of threads used for query. |
| fragment_pool_queue_size | 2048 | N/A | The upper limit of query number that can be processed on each BE node. |
| enable_partitioned_aggregation | TRUE | N/A | The boolean value to control if to enable the Partition Aggregation. The value true indicates to enable the Partition Aggregation. The value false indicates to disable the Partition Aggregation. |
| enable_token_check | TRUE | N/A | The boolean value to control if to enable the token check. The value true indicates to enable the token check. The value false indicates to disable the token check. |
| enable_prefetch | TRUE | N/A | The boolean value to control if to enable the pre-fetch of the query. The value true indicates to enable the pre-fetch. The value false indicates to disable the pre-fetch. |
| load_process_max_memory_limit_bytes | 107374182400 | Byte | The maximum size limit of memory resources can be taken up by all load process on a BE node. |
| load_process_max_memory_limit_percent | 30 | % | The maximum percentage limit of memory resources can be taken up by all load process on a BE node. |
| sync_tablet_meta | FALSE | N/A | The boolean value to control if to enable the synchronization of the tablet metadata. The value true indicates to enable the synchronization. The value false indicates to disable the synchronization. |
| routine_load_thread_pool_size | 10 | N/A | The thread pool size of Routine Load. |
| brpc_max_body_size | 2147483648 | Byte | The maximum body size of a BRPC. |
| tablet_map_shard_size | 32 | N/A | The tablet map shard size. The value must be the power of two. |
| enable_bitmap_union_disk_format_with_set | FALSE | N/A | The boolean value to control if to enable the new storage format of the BITMAP type, which can improve the performance of bitmap_union. The value true indicates to enable the new storage format. The value false indicates to disable the new storage format. |
| mem_limit | 90% | N/A | BE process memory upper limit. You can set it as a percentage ("80%") or a physical limit ("100GB"). |
| flush_thread_num_per_store | 2 | N/A | Number of threads that are used for flushing MemTable in each store. |

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
| connector_io_tasks_per_scan_operator | 16 | N/A | |
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
| enable_new_load_on_memory_limit_exceeded | 0 | N/A | |
| enable_orc_late_materialization | 1 | N/A | |
| enable_quadratic_probing | 0 | N/A | |
| enable_schema_change_v2 | 1 | N/A | |
| enable_segment_overflow_read_chunk | 1 | N/A | |
| enable_system_metrics | 1 | N/A | |
| es_http_timeout_ms | 5000 | N/A | |
| es_index_max_result_window | 10000 | N/A | |
| es_scroll_keepalive | 5m | N/A | |
| etl_thread_pool_queue_size | 256 | N/A | |
| etl_thread_pool_size | 8 | N/A | |
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
| lake_gc_metadata_check_interval | 1800 | N/A | |
| lake_gc_metadata_max_versions | 10 | N/A | |
| lake_gc_segment_check_interval | 3600 | N/A | |
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
| min_cmumulative_compaction_failure_interval_sec | 30 | N/A | |
| min_cumulative_compaction_num_singleton_deltas | 5 | N/A | |
| min_cumulative_compaction_size | 5368709120 | N/A | |
| mmap_buffers | 0 | N/A | |
| null_encoding | 0 | N/A | |
| num_cores | 0 | N/A | |
| num_disks | 0 | N/A | |
| num_threads_per_disk | 0 | N/A | |
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
| pipeline_hdfs_scan_thread_pool_thread_num | 48 | N/A | |
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
| read_size | 8388608 | N/A | |
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

## Configure broker

You can only set the configuration items of a broker by changing them in the corresponding configuration file **broker.conf**, and restart the broker to allow the change to take effect.

| Configuration item | Default | Unit | Description |
| ------------------------- | ------------------ | ------ | ------------------------------------------------------------ |
| hdfs_read_buffer_size_kb | 8192 | KB | Size of the buffer that is used to read data from HDFS. |
| hdfs_write_buffer_size_kb | 1024 | KB | Size of the buffer that is used to write data into HDFS. |
| client_expire_seconds | 300 | Second | Client sessions will be deleted if they do not receive any ping after the specified time. |
| broker_ipc_port | 8000 | N/A | The HDFS thrift RPC port. |
| sys_log_dir | `${BROKER_HOME}/log` | N/A | The directory used to store system logs (including INFO, WARNING, ERROR, and FATAL). |
| sys_log_level | INFO | N/A | The log level. Valid values include INFO, WARNING, ERROR, and FATAL. |
| sys_log_roll_mode | SIZE-MB-1024 | N/A | The mode how system logs are segmented into log rolls. Valid values include TIME-DAY, TIME-HOUR, and SIZE-MB-nnn. The default value indicates that logs are segmented into rolls which are 1 GB each. |
| sys_log_roll_num | 30 | N/A | The number of log rolls to reserve. |
| audit_log_dir | `${BROKER_HOME}/log` | N/A | The directory that stores audit log files. |
| audit_log_modules | Empty string | N/A | The modules for which StarRocks generates audit log entries. By default, StarRocks generates audit logs for the slow_query module and the query module. You can specify multiple modules, whose names must be separated by a comma (,) and a space. |
| audit_log_roll_mode | TIME-DAY | N/A | Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-<size>`. |
| audit_log_roll_num | 10 | N/A | This configuration does not work if the audit_log_roll_mode is set to `SIZE-MB-<size>`. |
| sys_log_verbose_modules | com.starrocks | N/A | The modules for which StarRocks generates system logs. Valid values are namespaces in BE, including `starrocks`, `starrocks::vectorized`, and `pipeline`. |

## Set system configurations

### Linux Kernel

Linux kernel 3.10 or later is recommended.

### CPU configurations

| Configuration item | Description | Recommended value | How to set |
| ------------------ | ------------------------------------------------------------ | ----------------- | ------------------------------------------------------------ |
| scaling_governor | The parameter scaling_governor is used to control the CPU power mode. The default value is on-demand. The performance mode consumes more energy, produces better performance, and thereby is recommended in the deployment of StarRocks. | performance | echo 'performance' \| sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor |

### Memory configurations

| Configuration item | Description | Recommended value | How to set |
| -------------------- | ------------------------------------------------------------ | ----------------- | ------------------------------------------------------------ |
| overcommit_memory | Memory Overcommit allows the operating system to overcommit memory resource to processes. We recommend you enable Memory Overcommit. | 1 | echo 1 \| sudo tee /proc/sys/vm/overcommit_memory |
| transparent_hugepage | Transparent Huge Pages is enabled by default. We recommend you disable this feature because it can interfere the memory allocator, and thereby lead to a drop in performance. | madvise | echo 'madvise' \| sudo tee /sys/kernel/mm/transparent_hugepage/enabled |
| swappiness | We recommend you disable the swappiness to eliminate its affects on the performance. | 0 | echo 0 \| sudo tee /proc/sys/vm/swappiness |

### Storage configurations

We recommend you set different scheduler algorithms in accordance with the medium of your storage volumes.

| Configuration item | Description | Recommended value | How to set |
| ------------------ | ------------------------------------------------------------ | ----------------- | ----------------------------------------------------------- |
| scheduler | mq-deadline scheduler algorithm suits SATA disks. | mq-deadline | echo mq-deadline \| sudo tee /sys/block/vdb/queue/scheduler |
| scheduler | kyber scheduler algorithm suits NVME or SSD disks. | kyber | echo kyber \| sudo tee /sys/block/vdb/queue/scheduler |
| scheduler | If your system does not support kyber scheduler algorithm, we recommend you use none scheduler algorithm. | none | echo none \| sudo tee /sys/block/vdb/queue/scheduler |

### Network configurations

We recommend you use 10GB network in your StarRocks cluster. Otherwise, StarRocks will fail to achieve the expected performance. You can use iPerf to check the bandwidth of your cluster.

### File system configurations

We recommend you use the ext4 journaling file system. You can run the following command to check the mount type:

```Shell
df -Th
```

### High concurrency configurations

If your StarRocks cluster has a high load concurrency, we recommend you set the following configurations.

```Shell
echo 120000 > /proc/sys/kernel/threads-max
echo 60000 > /proc/sys/vm/max_map_count
echo 200000 > /proc/sys/kernel/pid_max
```

### User process configuration

You can set the maximum number of user processes by running the following command:

```Shell
ulimit -u 40960
```

### File descriptor configuration

Run the following command to the maximum number of file descriptors to `65535`.

```Shell
ulimit -n 65535
```

If this configuration becomes invalid after you re-connect to the cluster, you can set the `UsePAM` configuration item under **/etc/ssh/sshd_config** to `yes`, and restart the SSHD service.

### Others

| Configuration item | Recommended value | How to set |
| --------------------- | ----------------- | ----------------------------------------------------------- |
| tcp abort on overflow | 1 | echo 1 \| sudo tee /proc/sys/net/ipv4/tcp_abort_on_overflow |
| somaxconn | 1024 | echo 1024 \| sudo tee /proc/sys/net/core/somaxconn |
