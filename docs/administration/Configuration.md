# Parameter Configuration

After the service is started, you may adjust the configuration parameters to meet the business requirements. You need to reboot the BE and Fe to apply the changes.

## FE configuration items

|Configuration Item|Default|Role|
|---|---|---|
|log_roll_size_mb|1024|Size of log split, per 1G|
|sys_log_dir|StarRocksFe.STARROCKS_HOME_DIR/log|Directory where logs are kept|
|sys_log_level|INFO|log level，INFO < WARNING < ERROR < FATAL|sys_log_roll_num|10| The number of logs to keep |
|sys_log_verbose_modules| empty string | Modules for log printing, write `org.starrocks.catalog` to print only the logs that are under the catalog module |
|sys_log_roll_interval|DAY| The interval for log splitting |
|sys_log_delete_age|7d| The interval for log deletion |
|sys_log_roll_mode|1024| The size of the log split, per 1G |
|audit_log_dir|starrocksFe.STARROCKS_HOME_DIR/log| Directory where audit logs are kept |
|audit_log_roll_num|90| The number of audit logs to keep |
|audit_log_modules|"slow_query", "query"| Modules for audit log printing, default retains `slow_query` and `query` |
|qe_slow_log_ms|5000| |Length of time for Slow query. The default value is 5000ms |
|audit_log_roll_interval|DAY| The interval for audit log splitting |
|audit_log_delete_age|30d| The interval for audit log deletion |
|audit_log_roll_mode|TIME-DAY| The interval for audit log splitting |
|label_keep_max_second|259200|The time to keep the label, with a default value of 3 days. The longer the keep time, the more memory to consume |
|history_job_keep_max_second|604800| Maximum retention time for historical jobs, such as schema change jobs, 7 days by default |
|label_clean_interval_second|14400|The interval for label cleaning|
|transaction_clean_interval_second|30| The interval for transaction cleaning |
|meta_dir|StarRocksFe.STARROCKS_HOME_DIR/meta| Directory for metadata |
|tmp_dir|starrocksFe.STARROCKS_HOME_DIR/temp_ddir| Directory where temporary files are kept, such as backup/restore, etc. |
|edit_log_port|9010| The port used for communication between FE Groups (Master, Follower, Observer) |
|edit_log_roll_num|50000| Split size of image log |
|meta_delay_toleration_second|300| Maximum metadata lag time tolerated by non-leader nodes |
|master_sync_policy|SYNC| Swipe method for leader’s log, SYNC by default |
|replica_sync_policy|SYNC| Swipe method for follower’s log,  SYNC by default |
|replica_ack_policy|SIMPLE_MAJORITY| The form in which logs are considered valid. The default is for the majority to return a confirmation message, which is considered to be in effect |
|bdbje_heartbeat_timeout_second|30|The interval for BDBJE heartbeat timeout|
|bdbje_lock_timeout_second|1| The interval for BDBJE lock timeout |
|txn_rollback_limit|100| the upper limit of transaction rollback |
|frontend_address|0.0.0.0| FE IP address |
|priority_networks| empty string | Specify BE IP address in the form of CIDR 10.10.10.0/24 for machines with multiple IPs |
|metadata_failure_recovery|false| Forced reset of FE metadata. Use with caution |
|ignore_meta_check|false| Ignore the metadata lag |
|max_bdbje_clock_delta_ms|5000| Maximum tolerated time offset between leader and non-leader |
|http_port|8030| Port of Http Server |
|http_backlog_num|1024|HttpServer port backlog|
|thrift_backlog_num|1024|ThriftServer port backlog|
|rpc_port|9020| Thrift server port of FE |
|query_port|9030| MySQL server port of FE |
|mysql_service_nio_enabled|false| Whether the nio is enabled for the service connected to FE |
|mysql_service_io_threads_num|false| The number of threads of the service connected to FE|
|auth_token|empty string|Whether the token is enabled automatically |
|tablet_create_timeout_second|1| Timeout for table creation |
|max_create_table_timeout_second|60| Maximum timeout for table creation |
|publish_version_timeout_second|30| Timeout for version to be published |
|publish_version_interval_ms|10| Interval for version to be published |
|load_straggler_wait_second|300| Maximum tolerated import lag time for BE replications, beyond which cloning will be performed |
|max_layout_length_per_row|100000|maximum length of a single row, 100KB|
|load_checker_interval_second|5|Interval for import polling|
|broker_load_default_timeout_second|14400|Timeout for Broker Load, 4 hours by default |
|mini_load_default_timeout_second|3600|Timeout for small batch import, 1 hour by default |
|insert_load_default_timeout_second|3600|Timeout for Insert Into statement, 1 hour by default |
|stream_load_default_timeout_second|600|Timeout for StreamLoad, 10 minutes by default |
|max_load_timeout_second|259200| Applicable to all imports, maximum timeout, 3 days by default |
|min_load_timeout_second|1| Applicable to all imports, minimum timeout, 1 second by default |
|desired_max_waiting_jobs|100| Max_waiting_jobs for all tasks, including table creation, import, schema change |
|max_running_txn_num_per_db|100| The number of concurrent import jobs |
|async_load_task_pool_size|10| The size of the thread pool for import job execution |
|tablet_delete_timeout_second|2| Timeout for table deletion |
|capacity_used_percent_high_water|0.75|Measurements of disk capacity used on Backend. Try not to send creation or clone tasks to this tablet when this parameter exceeds 0.75, until it is back to normal |
|alter_table_timeout_second|86400|Timeout for schema change|
|max_backend_down_time_second|3600| Maximum time for BE to rejoin after it disconnects to FE|
|storage_cooldown_second|2592000| Duration of media migration, 30 days by default |
|catalog_trash_expire_second|86400| Length of time that metadata remains in the recycle bin after deleting a table/database, beyond which data cannot be recovered|
|min_bytes_per_broker_scanner|67108864|Minimum amount of data to be processed by a single instance, 64MB by default |
|max_broker_concurrency|100|Maximum number of concurrent instances for a single task, 10 by default |
|load_parallel_instance_num|1|Number of concurrent instances on a single BE, 1 by default |
|export_checker_interval_second|5| Interval for exporting thread polling |
|export_running_job_num_limit|5| Maximum number of exporting jobs |
|export_task_default_timeout_second|7200| Timeout for export job, 2 hours by default |
|export_max_bytes_per_be_per_task|268435456| Maximum amount of data exported by a single export job on a single be, 256M by default |
|export_task_pool_size|5| Size of export task thread pool, 5 by default |
|consistency_check_start_time|23| The start time for FE to initiate replica consistency check |
|consistency_check_end_time|4| The end time for FE to initiate replica consistency check |
|check_consistency_default_timeout_second|600| Timeout for replica consistency check |
|qe_max_connection|1024| Maximum number of connections received on the FE, for all users |
|max_conn_per_user|100| Maximum number of connections that a single user can handle |
|query_colocate_join_memory_limit_penalty_factor|8| Memory limit for Colocate Join |
|disable_colocate_join|false| Colocate Join is not enabled|
|expr_children_limit|10000| The number of in's that can be involved in a query |
|expr_depth_limit|3000| |the number of nestings in the query |
|locale|zh_CN.UTF-8| Character set |
|remote_fragment_exec_timeout_ms|5000| RPC timeout for FE sending query planning, not involving task execution |
|max_query_retry_time|2| The number of query retries on FE |
|catalog_try_lock_timeout_ms|5000| Timeout for Catalog Lock fetch |
|disable_load_job|false|No import job is received, which is a stopgap measure when the cluster fails |
|es_state_sync_interval_second|10| Interval for FE to fetch Elastic Search Index |
|tablet_repair_delay_factor_second|60| Interval for replica repair controlled by FE |
|max_routine_load_job_num|100| maximum number of routine load jobs |
|max_routine_load_task_concurrent_num|5| Maximum number of concurrent execution tasks per routine load job |
|max_routine_load_task_num_per_be|5| Maximum number of concurrent  routine load tasks per BE, which needs to be less than or equal to the number specified in the configuration |
|max_routine_load_batch_size|524288000| The maximum amount of data to import per routine load task, default by 500M |
|routine_load_task_consume_second|3|Maximum time to consume data per routine load task, default by 3s|
|routine_load_task_timeout_second|15|Timeout for  routine load task, default by 15s|

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
|sys_log_dir|`${STARROCKS_HOME}/log`| The directory where logs, including INFO, WARNING, ERROR, FATAL, etc. are stored |
|user_function_dir|`${STARROCKS_HOME}/lib/udf`| The directory where UDF programs are stored |
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
