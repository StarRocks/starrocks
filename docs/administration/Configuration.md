# Parameter Configuration

After the service is started, you may adjust the configuration parameters to meet the business requirements. You need to reboot the BE and Fe to apply the changes.

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
| thrift_server_type                   | THREAD_POOL       | The service model that is used by the Thrift server in the FE node. Valid values: `SIMPLE`, `THREADED`, and `THREAD_POOL`. |
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

## BE Configuration Items

|Configuration item|Default|Role|
|---|---|---|
|be_port|9060|Port of thrift server on BE, used to receive requests from FE|
|brpc_port|8060|Port of BRPC to view some network statistics of BRPC|
|brpc_num_threads|-1|The number of bthreads of BRPC, -1 means the same as the number of CPU cores|
|priority_networks|empty string|Specify BE IP address in the form of CIDR 10.10.10.0/24 for machines with multiple IPs|
|heartbeat_service_port|9050|The heartbeat service port (thrift) where users receive heartbeats from FE|
|heartbeat_service_thread_count|1|The number of heartbeat threads|
|create_tablet_worker_count|3|The number of threads creating a tablet|
|drop_tablet_worker_count|3| The number of threads deleting a tablet |
|push_worker_count_normal_priority|3| The number of threads importing and processing NORMAL priority tasks |
|push_worker_count_high_priority|3|The number of threads importing and processing HIGH priority tasks|
|publish_version_worker_count|2|The number of threads taking effect|
|clear_transaction_task_worker_count|1|The number of threads cleaning up transactions|
|alter_tablet_worker_count|3|The number of threads processing schema change|
|clone_worker_count|3|The number of threads cloning|
|storage_medium_migrate_count|1|Then number of threads migrating SATA to SSD|
|check_consistency_worker_count|1|Calculate checksum for tablet (checksum) |
|report_task_interval_seconds|10|The interval of reporting individual tasks, including table creation, table deletion, import, and schema change |
|report_disk_state_interval_seconds|60| The interval of reporting the state of each disk, the amount of data on it, etc. |
|report_tablet_interval_seconds|60| The interval of reporting the state of each tablet. Report the latest version of each tablet |
|alter_tablet_timeout_seconds|86400|Timeout of schema change |
|sys_log_dir|${STARROCKS_HOME}/log| The directory where logs, including INFO, WARNING, ERROR, FATAL, etc. are stored |
|user_function_dir|${STARROKCS_HOME}/lib/udf| The directory where UDF programs are stored |
|sys_log_level|INFO| log level，INFO < WARNING < ERROR < FATAL|
|sys_log_roll_mode|SIZE-MB-1024| The size of the log split, per GB |
|sys_log_roll_num|10| The number of logs to keep |
|sys_log_verbose_modules|*|Modules for log printing, write olap to print only the logs that are under olap modules|
|sys_log_verbose_level|10|The level of the log display, used to control log output starting with VLOG in the code|
|log_buffer_level|empty string|Policy for log flushing. The default is to keep it in memory|
|num_threads_per_core|3|Number of threads started per CPU core|
|compress_rowbatches|true|Whether to compress RowBatch (which supports data transfer between query layers) for RPC communication between BEs|
|serialize_batch|false| Whether to serialize batches for RPC communication between BEss|
|status_report_interval|5|Interval for reporting query profile, used by FE to collect query statistics|
|starrocks_scanner_thread_pool_thread_num|48| The number of threads in the storage engine that concurrently scan the disk. Threads are uniformly managed in the thread pool |
| starrocks _scanner_thread_pool_queue_size|102400|The maximum number of tasks received by the storage engine|
| starrocks _scan_range_row_count|524288|The granularity of the storage engine to split query tasks, default by 512K|
| starrocks _scanner_queue_size|1024|The number of scan tasks supported by the storage engine|
| starrocks _scanner_row_num|16384|The maximum number of data rows to be returned in a single execution per scan thread|
| starrocks _max_scan_key_num|1024|The maximum number of scan keys that can be split by a query|
|column_dictionary_key_ratio_threshold|0|The ratio of string type, less than which the dictionary compression algorithm will be applied|
|column_dictionary_key_size_threshold|0| The size of the column, less than which the dictionary compression algorithm will be applied |
|memory_limitation_per_thread_for_schema_change|2| The maximum amount of memory allowed for a single schema change task |
|max_unpacked_row_block_size|104857600| The maximum number of bytes for a single block, default by 100MB |
|file_descriptor_cache_clean_interval|3600| Interval for the file descriptor to clean up cache |
|disk_stat_monitor_interval|5|Interval for disk status detection|
|unused_rowset_monitor_interval|30|Interval for expired Rowset cleanup|
|storage_root_path|empty string|Directory where data is stored|
|max_percentage_of_error_disk|0|Disk errors reach a certain percentage then BE exits|
|default_num_rows_per_data_block|1024|Number of data rows per block|
|max_tablet_num_per_shard|1024|Number of tablets per shard, used to divide tablets and prevent too many tablet subdirectories in a single directory|
|pending_data_expire_time_sec|1800|The maximum amount of time that pending data can be kept in the storage engine|
|inc_rowset_expired_sec|1800|The maximum amount of time that valid data can be kept in the storage engine for incremental cloning |
|max_garbage_sweep_interval|3600|Maximum interval for disk to perform garbage cleanup|
|min_garbage_sweep_interval|180|Minimum interval for disk to perform garbage cleanup|
|snapshot_expire_time_sec|172800|Interval for snapshot file cleanup, default by 48 hours|
|trash_file_expire_time_sec|259200|Interval for recycle bin cleanup, default by 72 hours|
|row_nums_check|true| Comparison of Rowset rows that are before and after compaction |
|file_descriptor_cache_capacity|32768| Cache capacity of the file descriptor|
|min_file_descriptor_number|60000| The lower limit requirement of the file descriptor for the BE process |
|index_stream_cache_capacity|10737418240| Cache capacity of BloomFilter/Min/Max statistics |
|storage_page_cache_limit|20G| Capacity of PageCache |
|disable_storage_page_cache|false|Whether PageCache is enabled|
|base_compaction_start_hour|20| Start time of BaseCompaction |
|base_compaction_end_hour|7| End time of BaseCompaction|
|base_compaction_check_interval_seconds|60|Interval of BaseCompaction thread polling|
|base_compaction_num_cumulative_deltas|5|BaseCompaction trigger: the number of Cumulative files needed to be reached|
|base_compaction_num_threads_per_disk|1|Number of BaseCompaction threads per disk|
|base_cumulative_delta_ratio|0.3|BaseCompaction trigger: The target ratio between cumulative and base files |
|base_compaction_interval_seconds_since_last_operation|86400|BaseCompaction trigger: The interval for triggering the next BaseCompaction|
|base_compaction_write_mbytes_per_sec|5| Speed limit of BaseCompaction to write disk |
|cumulative_compaction_check_interval_seconds|10| Interval of CumulativeCompaction thread polling |
|min_cumulative_compaction_num_singleton_deltas|5| CumulativeCompaction trigger: the lower limit on the number of Singleton files to be reached |
|max_cumulative_compaction_num_singleton_deltas|1000| CumulativeCompaction trigger: the upper limit on the number of Singleton files to be reached |
|cumulative_compaction_num_threads_per_disk|1| Number of CumulativeCompaction threads per disk |
|cumulative_compaction_budgeted_bytes|104857600|BaseCompaction trigger: The sum limit of Singleton file size, default by 100MB |
|cumulative_compaction_write_mbytes_per_sec|100| Speed limit of CumulativeCompaction to write disk |
|min_compaction_failure_interval_sec|600| Interval for Tablet Compaction to be scheduled again after a failure. |
|max_compaction_concurrency|-1| Maximum concurrency for BaseCompaction and CumulativeCompaction.-1 indicates no limit |
|webserver_port|8040| Http Server port |
|webserver_num_workers|5| Number of Http Server threads |
|periodic_counter_update_period_ms|500| Interval for getting counter statistics |
|load_data_reserve_hours|4|Length of time to retain files generated by small batch import|
|load_error_log_reserve_hours|48|Length of time to retain imported data information|
|number_tablet_writer_threads|16|Number of threads for streaming imports|
|streaming_load_max_mb|10240|The maximum size of a single file for stream load|
|streaming_load_rpc_max_alive_time_sec|1200|Timeout for RPC of stream load |
|tablet_writer_rpc_timeout_sec|600| Timeout for RPC of TabletWriter |
|fragment_pool_thread_num|64| Number of query threads. Default by 64 threads, subsequent queries will dynamically create threads |
|fragment_pool_queue_size|1024|The maximum number of queries that can be processed on a single node |
|enable_partitioned_hash_join|false|Use PartitionHashJoin |
|enable_partitioned_aggregation|true|Use PartitionAggregation|
|enable_token_check|true|Token check is enabled |
|enable_prefetch|true|Query prefetching |
|load_process_max_memory_limit_bytes|107374182400|The maximum amount of memory occupied by all import threads on a single node, 100GB by default |
|load_process_max_memory_limit_percent|30|The maximum percentage  of the memory occupied by all import threads on a single node |
|sync_tablet_meta|false|Whether the storage engine is on sync retention to disk. |
|thrift_rpc_timeout_ms|5000| Timeout of Thrift|
|txn_commit_rpc_timeout_ms|10000|Number of thread pools routinely imported|
|routine_load_thread_pool_size|10|Number of thread pools of routine load |
|tablet_meta_checkpoint_min_new_rowsets_num|10|Minimum number of Rowsets for TabletMeta Checkpoint|
|tablet_meta_checkpoint_min_interval_secs|600|Interval of TabletMeta Checkpoint thread polling |
|default_rowset_type|ALPHA|The format of the storage engine, ALPHA by default, will be replaced with BETA|
|brpc_max_body_size|209715200|The maximum packet size of BRPC, 200MB by default|
|max_runnings_transactions|2000|The maximum number of transactions supported by the storage engine|
|tablet_map_shard_size|1|Size of tablet map shard|
|enable_bitmap_union_disk_format_with_set | False | New storage format for Bitmap, which can optimize the performance of `bitmap_union`|

## Broker configuration parameters

Reference [Broker load import](... /loading/BrokerLoad.md)

## System parameters

Linux Kernel

Recommended kernel is 3.10 or higher.

CPU

`Scaling governor` is used to control the CPU consumption. The default mode is `on-demand`. The `performance` mode has the highest consumption rate and the best performance, which is recommended for StarRocks deployment.

~~~shell
echo 'performance' | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
~~~

memory

* **Overcommit**

It is recommended to use `Overcommit`.
It is recommended to set `cat /proc/sys/vm/overcommit_memory` to 1.

~~~shell
echo 1 | sudo tee /proc/sys/vm/overcommit_memory
~~~

* **Huge Pages**

Do not transparent huge pages, which will interfere with the memory allocator and cause performance degradation.

~~~shell
echo 'madvise' | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
~~~

* **Swappiness**

Turn off the swap area to eliminate performance disturbances when swapping memory to virtual memory.

~~~shell
echo 0 | sudo tee /proc/sys/vm/swappiness
~~~

Disk

* **SATA**

The mq-deadline scheduling algorithm will sort and merge I/O requests, which is suitable for SATA disks.

~~~shell
echo mq-deadline | sudo tee /sys/block/vdb/queue/scheduler
~~~

* **SSD/NVME**

The kyber scheduling algorithm is suitable for devices with low latency, such as NVME/SSD.

~~~shell
echo kyber | sudo tee /sys/block/vdb/queue/scheduler
~~~

If the system does not support kyber, it is recommended to use the none scheduling algorithm

~~~shell
echo none | sudo tee /sys/block/vdb/queue/scheduler
~~~

Network

Please use at least a 10 GB network. A 1 GB network will work but won’t achieve expected performance. You can use iperf to test the system bandwidth.

File System

It is recommended to use the Ext4 file system. You can check the mount type with relevant commands.

~~~shell
df -Th
Filesystem Type Size Used Avail Use% Mounted on
/dev/vdb1 ext4 1008G 903G 55G 95% /home/disk1
~~~

Other System Configuration

* **tcp abort on overflow**

~~~shell
echo 1 | sudo tee /proc/sys/net/ipv4/tcp_abort_on_overflow
~~~

* **somaxconn**

~~~shell
echo 1024 | sudo tee /proc/sys/net/core/somaxconn
~~~

System resources

System resources are the maximum resources the system can use. It’s configured in `/etc/security/limits.conf`. The resources include file descriptors, maximum number of processes, maximum memory usage, etc.

* **File descriptors**

Run `ulimit -n 65535` on the deployed machine to set the file descriptor to 65535. If the ulimit value fails after login, change `UsePAM yes` in `/etc/ssh/sshd_config` and restart the sshd service.

High concurrency configuration

If the cluster load is highly concurrent, it is recommended to add the following configuration

~~~shell
echo 120000 > /proc/sys/kernel/threads-max
echo 60000 > /proc/sys/vm/max_map_count
echo 200000 > /proc/sys/kernel/pid_max
~~~

* **max user processes**

ulimit -u 40960
