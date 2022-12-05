# Parameter configuration

This topic describes FE, BE, Broker, and system parameters. It also provides suggestions on how to configure and tune these parameters.

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
|replica_sync_policy|SYNC| Swipe method for follower’s log, SYNC by default |
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
|empty_load_as_error|TRUE|Switch value to control if to return error `all partitions have no load data` when the data to load is empty. If this parameter is set as `false`, the system returns `OK` instead of the error when the data to load is empty.|
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
|enable_statistic_collect|TRUE|Whether to collect statistics. This parameter is turned on by default.|
|enable_collect_full_statistic|TRUE|Whether to enable automatic full statistics collection. This parameter is turned on by default.|
|statistic_auto_collect_ratio|0.8|The threshold for determining whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered.|
|statistic_max_full_collect_data_size|100|The size of the largest partition for automatic collection to collect data. Unit: GB.If a partition exceeds this value, full collection is discarded and sampled collection is performed instead.|
|statistic_collect_interval_sec|300|The interval for checking data updates during automatic collection. Unit: seconds.|
|statistic_sample_collect_rows|200000|The minimum number of rows to collect for sampled collection. If the parameter value exceeds the actual number of rows in your table, full collection is performed.|
|histogram_buckets_size|64|The default bucket number for a histogram.|
|histogram_mcv_size|100|The number of most common values (MVC) for a histogram.|
|histogram_sample_ratio|0.1|The sampling ratio for a histogram.|
|histogram_max_sample_row_count|10000000|The maximum number of rows to collect for a histogram.|
|statistics_manager_sleep_time_sec|60|The interval at which metadata is scheduled. Unit: seconds. The system performs the following operations based on this interval: create tables for storing statistics, delete statistics that have been deleted, delete expired statistics.|
|statistic_update_interval_sec|24 \* 60 \* 60|The interval at which the cache of statistical information is updated. Unit: seconds.|
|statistic_analyze_status_keep_second|259200|The duration to retain the history of collection tasks. The default value is 3 days. Unit: seconds.|
|statistic_collect_concurrency|3|The maximum number of manual collection tasks that can run in parallel. The value defaults to 3, which means you can run a maximum of three manual collections tasks in parallel. If the value is exceeded, incoming tasks will be in the PENDING state, waiting to be scheduled. You can only modify this parameter in the **fe.conf** file. You must restart the FE for the modification to take effect.|
|max_routine_load_job_num|100| maximum number of routine load jobs |
|max_routine_load_task_concurrent_num|5| Maximum number of concurrent execution tasks per routine load job |
|max_routine_load_task_num_per_be|5| Maximum number of concurrent routine load tasks per BE, which needs to be less than or equal to the number specified in the configuration |
|max_routine_load_batch_size|524288000| The maximum amount of data to import per routine load task, default by 500M |
|routine_load_task_consume_second|3|Maximum time to consume data per routine load task, default by 3s|
|routine_load_task_timeout_second|15|Timeout for routine load task, default by 15s|
|enable_strict_storage_medium_check|TRUE|Whether the FE checks available storage space.|
|storage_cooldown_second|-1|The delay of cooldown from HDD storage to SSD storage. Unit: seconds. The default value indicates to disable the auto-cooldown.|

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
| | | | |
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
| sys_log_dir | ${STARROCKS_HOME}/log | N/A | The directory that stores system logs (including INFO, WARNING, ERROR, and FATAL). |
| user_function_dir | ${STARROCKS_HOME}/lib/udf | N/A | The directory used to store the User-defined Functions (UDFs). |
| small_file_dir | ${STARROCKS_HOME}/lib/small_file | N/A | The directory used to store the files downloaded by the file manager. |
| sys_log_level | INFO | N/A | The severity levels into which system log entries are classified. Valid values: INFO, WARN, ERROR, and FATAL. |
| sys_log_roll_mode | SIZE-MB-1024 | N/A | The mode how system logs are segmented into log rolls. Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-<size>`. The default value indicates that logs are segmented into rolls which are 1 GB each. |
| sys_log_roll_num | 10 | N/A | The number of log rolls to reserve. |
| sys_log_verbose_modules | Empty string | N/A | The module of the logs to be printed. For example, if you set this configuration item to OLAP, StarRocks only prints the logs of the OLAP module. Valid values are namespaces in BE, including `starrocks`, `starrocks::vectorized`, and `pipeline`. |
| sys_log_verbose_level | 10 | N/A | The level of the logs to be printed. This configuration item is used to control the ouput of logs initiated with VLOG in codes. |
| log_buffer_level | Empty string | N/A | The strategy how logs are flushed. The default value indicates that logs are buffered in memory. Valid values are `-1` and `0`. `-1` indicates that logs are not buffering in memory. |
| num_threads_per_core | 3 | N/A | The number threads started in each CPU core. |
| compress_rowbatches | TRUE | N/A | The boolean value to control if to compress the row batches in RPCs between BEs. This configuration item is used for the data transmission between query layers. The value true indicates to compress the row batches. The value false indicates not to compress the row batches. |
| serialize_batch | FALSE | N/A | The boolean value to control if to serialize the row batches in RPCs between BEs. This configuration item is used for the data transmission between query layers. The value true indicates to serialize the row batches. The value false indicates not to serialize the row batches. |
| storage_root_path | ${STARROCKS_HOME}/storage | N/A | The directory and medium of the storage volume. Multiple volumes are separated by semicolon (;). If the storage medium is SSD, add `,medium:ssd` at the end of the directory. If the storage medium is HDD, add `,medium:hdd` at the end of the directory. Example: `/data1,medium:hdd;/data2,medium:ssd`. |
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
