---
displayed_sidebar: "English"
---

# BE configuration

Some BE configuration items are dynamic parameters which you can set them by commands when BE nodes are still online. The rest of them are static parameters. You can only set the static parameters of a BE node by changing them in the corresponding configuration file **be.conf**, and restart the BE node to allow the change to take effect.

## View BE configuration items

You can view the BE configuration items using the following command:

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

## Configure BE dynamic parameters

You can configure a dynamic parameter of a BE node by using the `curl` command.

```Shell
curl -XPOST http://be_host:http_port/api/update_config?<configuration_item>=<value>
```

BE dynamic parameters are as follows.

#### enable_stream_load_verbose_log

- **Default:** false
- **Description:** Specifies whether to log the HTTP requests and responses for Stream Load jobs.
- **Introduced in:** 2.5.17, 3.0.9, 3.1.6, 3.2.1

#### report_task_interval_seconds

- **Default:** 10 seconds
- **Description:** The time interval at which to report the state of a task. A task can be creating a table, dropping a table, loading data, or changing a table schema.

#### report_disk_state_interval_seconds

- **Default:** 60 seconds
- **Description:** The time interval at which to report the storage volume state, which includes the size of data within the volume.

#### report_tablet_interval_seconds

- **Default:** 60 seconds
- **Description:** The time interval at which to report the most updated version of all tablets.

#### report_workgroup_interval_seconds

- **Default:** 5 seconds
- **Description:** The time interval at which to report the most updated version of all workgroups.

#### max_download_speed_kbps

- **Default:** 50,000 KB/s
- **Description:** The maximum download speed of each HTTP request. This value affects the performance of data replica synchronization across BE nodes.

#### download_low_speed_limit_kbps

- **Default:** 50 KB/s
- **Description:** The download speed lower limit of each HTTP request. An HTTP request aborts when it constantly runs with a lower speed than this value within the time span specified in the configuration item `download_low_speed_time`.

#### download_low_speed_time

- **Default:** 300 seconds
- **Description:** The maximum time that an HTTP request can run with a download speed lower than the limit. An HTTP request aborts when it constantly runs with a lower speed than the value of `download_low_speed_limit_kbps` within the time span specified in this configuration item.

#### status_report_interval

- **Default:** 5 seconds
- **Description:** The time interval at which a query reports its profile, which can be used for query statistics collection by FE.

#### scanner_thread_pool_thread_num

- **Default:** 48 (Number of Threads)
- **Description:** The number of threads which the storage engine used for concurrent storage volume scanning. All threads are managed in the thread pool.

#### thrift_client_retry_interval_ms

- **Default:** 100 ms
- **Description:** The time interval at which a thrift client retries.

#### scanner_thread_pool_queue_size

- **Default:** 102,400
- **Description:** The number of scan tasks supported by the storage engine.

#### scanner_row_num

- **Default:** 16,384
- **Description:** The maximum row count returned by each scan thread in a scan.

#### max_scan_key_num

- **Default:** 1,024 (Maximum Number of Scan Keys per Query)
- **Description:** The maximum number of scan keys segmented by each query.

#### max_pushdown_conditions_per_column

- **Default:** 1,024
- **Description:** The maximum number of conditions that allow pushdown in each column. If the number of conditions exceeds this limit, the predicates are not pushed down to the storage layer.

#### exchg_node_buffer_size_bytes

- **Default:** 10,485,760 Bytes
- **Description:** The maximum buffer size on the receiver end of an exchange node for each query. This configuration item is a soft limit. A backpressure is triggered when data is sent to the receiver end with an excessive speed.

#### memory_limitation_per_thread_for_schema_change

- **Default:** 2 GB
- **Description:** The maximum memory size allowed for each schema change task.

#### update_cache_expire_sec

- **Default:** 360 seconds
- **Description:** The expiration time of Update Cache.

#### file_descriptor_cache_clean_interval

- **Default:** 3,600 seconds
- **Description:** The time interval at which to clean file descriptors that have not been used for a certain period of time.

#### disk_stat_monitor_interval

- **Default:** 5 seconds
- **Description:** The time interval at which to monitor health status of disks.

#### unused_rowset_monitor_interval

- **Default:** 30 seconds
- **Description:** The time interval at which to clean the expired rowsets.

#### max_percentage_of_error_disk

- **Default:** 0%
- **Description:** The maximum percentage of error that is tolerable in a storage volume before the corresponding BE node quits.

#### default_num_rows_per_column_file_block

- **Default:** 1,024
- **Description:** The maximum number of rows that can be stored in each row block.

#### pending_data_expire_time_sec

- **Default:** 1,800 seconds
- **Description:** The expiration time of the pending data in the storage engine.

#### inc_rowset_expired_sec

- **Default:** 1,800 seconds
- **Description:** The expiration time of the incoming data. This configuration item is used in incremental clone.

#### tablet_rowset_stale_sweep_time_sec

- **Default:** 1,800 seconds
- **Description:** The time interval at which to sweep the stale rowsets in tablets.

#### snapshot_expire_time_sec

- **Default:** 172,800 seconds
- **Description:** The expiration time of snapshot files.

#### trash_file_expire_time_sec

- **Default:** 86,400 seconds
- **Description:** The time interval at which to clean trash files. The default value has been changed from 259,200 to 86,400 since v2.5.17, v3.0.9, and v3.1.6.

#### base_compaction_check_interval_seconds

- **Default:** 60 seconds
- **Description:** The time interval of thread polling for a Base Compaction.

#### min_base_compaction_num_singleton_deltas

- **Default:** 5
- **Description:** The minimum number of segments that trigger a Base Compaction.

#### max_base_compaction_num_singleton_deltas

- **Default:** 100
- **Description:** The maximum number of segments that can be compacted in each Base Compaction.

#### base_compaction_interval_seconds_since_last_operation

- **Default:** 86,400 seconds
- **Description:** The time interval since the last Base Compaction. This configuration item is one of the conditions that trigger a Base Compaction.

#### cumulative_compaction_check_interval_seconds

- **Default:** 1 second
- **Description:** The time interval of thread polling for a Cumulative Compaction.

#### update_compaction_check_interval_seconds

- **Default:** 60 seconds
- **Description:** The time interval at which to check the Update Compaction of the Primary Key table.

#### min_compaction_failure_interval_sec

- **Default:** 120 seconds
- **Description:** The minimum time interval that a Tablet Compaction can be scheduled since the last compaction failure.

#### max_compaction_concurrency

- **Default:** -1
- **Description:** The maximum concurrency of compactions (both Base Compaction and Cumulative Compaction). The value -1 indicates that no limit is imposed on the concurrency.

#### periodic_counter_update_period_ms

- **Default:** 500 ms
- **Description:** The time interval at which to collect the Counter statistics.

#### load_error_log_reserve_hours

- **Default:** 48 hours
- **Description:** The time for which data loading logs are reserved.

#### streaming_load_max_mb

- **Default:** 10,240 MB
- **Description:** The maximum size of a file that can be streamed into StarRocks.

#### streaming_load_max_batch_size_mb

- **Default:** 100 MB
- **Description:** The maximum size of a JSON file that can be streamed into StarRocks.

#### memory_maintenance_sleep_time_s

- **Default:** 10 seconds
- **Description:** The time interval at which ColumnPool GC is triggered. StarRocks executes GC periodically and returns the released memory to the operating system.

#### write_buffer_size

- **Default:** 104,857,600 Bytes
- **Description:** The buffer size of MemTable in the memory. This configuration item is the threshold to trigger a flush.

#### tablet_stat_cache_update_interval_second

- **Default:** 300 seconds
- **Description:** The time interval at which to update Tablet Stat Cache.

#### result_buffer_cancelled_interval_time

- **Default:** 300 seconds
- **Description:** The wait time before BufferControlBlock releases data.

#### thrift_rpc_timeout_ms

- **Default:** 5,000 ms
- **Description:** The timeout for a thrift RPC.

#### max_consumer_num_per_group

- **Default:** 3 (Maximum Number of Consumers in a Consumer Group of Routine Load)
- **Description:** The maximum number of consumers in a consumer group of Routine Load.

#### max_memory_sink_batch_count

- **Default:** 20 (Maximum Number of Scan Cache Batches)
- **Description:** The maximum number of Scan Cache batches.

#### scan_context_gc_interval_min

- **Default:** 5 minutes
- **Description:** The time interval at which to clean the Scan Context.

#### path_gc_check_step

- **Default:** 1,000 (Maximum Number of Files Scanned Continuously Each Time)
- **Description:** The maximum number of files that can be scanned continuously each time.

#### path_gc_check_step_interval_ms

- **Default:** 10 ms
- **Description:** The time interval between file scans.

#### path_scan_interval_second

- **Default:** 86,400 seconds
- **Description:** The time interval at which GC cleans expired data.

#### storage_flood_stage_usage_percent

- **Default:** 95
- **Description:** If the storage usage (in percentage) of the BE storage directory exceeds this value and the remaining storage space is less than `storage_flood_stage_left_capacity_bytes`, Load and Restore jobs are rejected.

#### storage_flood_stage_left_capacity_bytes

- **Default:** 107,374,182,400 Bytes (Remaining Storage Space Threshold for Load and Restore Job Rejection)
- **Description:** If the remaining storage space of the BE storage directory is less than this value and the storage usage (in percentage) exceeds `storage_flood_stage_usage_percent`, Load and Restore jobs are rejected.

#### tablet_meta_checkpoint_min_new_rowsets_num

- **Default:** 10 (Minimum Number of Rowsets to Trigger a TabletMeta Checkpoint)
- **Description:** The minimum number of rowsets to create since the last TabletMeta Checkpoint.

#### tablet_meta_checkpoint_min_interval_secs

- **Default:** 600 seconds
- **Description:** The time interval of thread polling for a TabletMeta Checkpoint.

#### max_runnings_transactions_per_txn_map

- **Default:** 100 (Maximum Number of Concurrent Transactions per Partition)
- **Description:** The maximum number of transactions that can run concurrently in each partition.

#### tablet_max_pending_versions

- **Default:** 1,000 (Maximum Number of Pending Versions on a Primary Key Tablet)
- **Description:** The maximum number of pending versions that are tolerable on a Primary Key tablet. Pending versions refer to versions that are committed but not applied yet.

#### tablet_max_versions

- **Default:** 1,000 (Maximum Number of Versions Allowed on a Tablet)
- **Description:** The maximum number of versions allowed on a tablet. If the number of versions exceeds this value, new write requests will fail.

#### max_hdfs_file_handle

- **Default:** 1,000 (Maximum Number of HDFS File Descriptors)
- **Description:** The maximum number of HDFS file descriptors that can be opened.

#### be_exit_after_disk_write_hang_second

- **Default:** 60 seconds
- **Description:** The length of time that the BE waits to exit after the disk hangs.

#### min_cumulative_compaction_failure_interval_sec

- **Default:** 30 seconds
- **Description:** The minimum time interval at which Cumulative Compaction retries upon failures.

#### size_tiered_level_num

- **Default:** 7 (Number of Levels for Size-tiered Compaction)
- **Description:** The number of levels for the Size-tiered Compaction strategy. At most one rowset is reserved for each level. Therefore, under a stable condition, there are, at most, as many rowsets as the level number specified in this configuration item.

#### size_tiered_level_multiple

- **Default:** 5 (Multiple of Data Size Between Contiguous Levels in Size-tiered Compaction)
- **Description:** The multiple of data size between two contiguous levels in the Size-tiered Compaction strategy.

#### size_tiered_min_level_size

- **Default:** 131,072 Bytes
- **Description:** The data size of the minimum level in the Size-tiered Compaction strategy. Rowsets smaller than this value immediately trigger the data compaction.

#### storage_page_cache_limit

- **Default:** 20%
- **Description:** The PageCache size. It can be specified as size, for example, `20G`, `20,480M`, `20,971,520K`, or `21,474,836,480B`. It can also be specified as the ratio (percentage) to the memory size, for example, `20%`. It takes effect only when `disable_storage_page_cache` is set to `false`.

#### internal_service_async_thread_num

- **Default:** 10 (Number of Threads)
- **Description:** The thread pool size allowed on each BE for interacting with Kafka. Currently, the FE responsible for processing Routine Load requests depends on BEs to interact with Kafka, and each BE in StarRocks has its own thread pool for interactions with Kafka. If a large number of Routine Load tasks are distributed to a BE, the BE's thread pool for interactions with Kafka may be too busy to process all tasks in a timely manner. In this situation, you can adjust the value of this parameter to suit your needs.

#### lake_enable_vertical_compaction_fill_data_cache

- **Default:** false
- **Description:** Whether to allow compaction tasks to cache data on local disks in a shared-data cluster.
- **Introduced in:** 3.1.7, 3.2.3

#### update_compaction_ratio_threshold

- **Default:** 0.5
- **Description:** The maximum proportion of data that a compaction can merge for a Primary Key table in a shared-data cluster. We recommend shrinking this value if a single tablet becomes excessively large. This parameter is supported from v3.1.5 onwards.

## Configure BE static parameters

You can only set the static parameters of a BE by changing them in the corresponding configuration file **be.conf**, and restart the BE to allow the changes to take effect.

BE static parameters are as follows.

#### hdfs_client_enable_hedged_read

- **Default**: false
- **Unit**: N/A
- **Description**: Specifies whether to enable the hedged read feature. This parameter is supported from v3.0 onwards.

#### hdfs_client_hedged_read_threadpool_size

- **Default**: 128
- **Unit**: N/A
- **Description**: Specifies the size of the Hedged Read thread pool on your HDFS client. The thread pool size limits the number of threads to dedicate to the running of hedged reads in your HDFS client. This parameter is supported from v3.0 onwards. It is equivalent to the dfs.client.hedged.read.threadpool.size parameter in the hdfs-site.xml file of your HDFS cluster.

#### hdfs_client_hedged_read_threshold_millis

- **Default**: 2500
- **Unit**: Millisecond
- **Description**: Specifies the number of milliseconds to wait before starting up a hedged read. For example, you have set this parameter to 30. In this situation, if a read from a block has not returned within 30 milliseconds, your HDFS client immediately starts up a new read against a different block replica. This parameter is supported from v3.0 onwards. It is equivalent to the dfs.client.hedged.read.threshold.millis parameter in the hdfs-site.xml file of your HDFS cluster.

#### be_port

- **Default**: 9060
- **Unit**: N/A
- **Description**: The BE thrift server port, which is used to receive requests from FEs.

#### brpc_port

- **Default**: 8060
- **Unit**: N/A
- **Description**: The BE bRPC port, which is used to view the network statistics of bRPCs.

#### brpc_num_threads

- **Default**: -1
- **Unit**: N/A
- **Description**: The number of bthreads of a bRPC. The value -1 indicates the same number with the CPU threads.

#### priority_networks

- **Default**: Empty string
- **Unit**: N/A
- **Description**: The CIDR-formatted IP address that is used to specify the priority IP address of a BE node if the machine that hosts the BE node has multiple IP addresses.

#### heartbeat_service_port

- **Default**: 9050
- **Unit**: N/A
- **Description**: The BE heartbeat service port, which is used to receive heartbeats from FEs.

#### starlet_port

- **Default**: 9070
- **Unit**: N/A
- **Description**: An extra agent service port for CN (BE in v3.0) in a shared-data cluster.

#### heartbeat_service_thread_count

- **Default**: 1
- **Unit**: N/A
- **Description**: The thread count of the BE heartbeat service.

#### create_tablet_worker_count

- **Default**: 3
- **Unit**: N/A
- **Description**: The number of threads used to create a tablet.

#### drop_tablet_worker_count

- **Default**: 3
- **Unit**: N/A
- **Description**: The number of threads used to drop a tablet.

#### push_worker_count_normal_priority

- **Default**: 3
- **Unit**: N/A
- **Description**: The number of threads used to handle a load task with NORMAL priority.

#### push_worker_count_high_priority

- **Default**: 3
- **Unit**: N/A
- **Description**: The number of threads used to handle a load task with HIGH priority.

#### transaction_publish_version_worker_count

- **Default**: 0
- **Unit**: N/A
- **Description**: The maximum number of threads used to publish a version. When this value is set to less than or equal to 0, the system uses half of the CPU core count as the value, so as to avoid insufficient thread resources when import concurrency is high but only a fixed number of threads are used. From v2.5, the default value has been changed from 8 to 0.

#### clear_transaction_task_worker_count

- **Default**: 1
- **Unit**: N/A
- **Description**: The number of threads used for clearing transaction.

#### alter_tablet_worker_count

- **Default**: 3
- **Unit**: N/A
- **Description**: The number of threads used for schema change.

#### clone_worker_count

- **Default**: 3
- **Unit**: N/A
- **Description**: The number of threads used for clone.

#### storage_medium_migrate_count

- **Default**: 1
- **Unit**: N/A
- **Description**: The number of threads used for storage medium migration (from SATA to SSD).

#### check_consistency_worker_count

- **Default**: 1
- **Unit**: N/A
- **Description**: The number of threads used for checking the consistency of tablets.

#### sys_log_dir

- **Default**: `${STARROCKS_HOME}/log`
- **Unit**: N/A
- **Description**: The directory that stores system logs (including INFO, WARNING, ERROR, and FATAL).

#### user_function_dir

- **Default**: `${STARROCKS_HOME}/lib/udf`
- **Unit**: N/A
- **Description**: The directory used to store User-defined Functions (UDFs).

#### small_file_dir

- **Default**: `${STARROCKS_HOME}/lib/small_file`
- **Unit**: N/A
- **Description**: The directory used to store the files downloaded by the file manager.

#### sys_log_level

- **Default**: INFO
- **Unit**: N/A
- **Description**: The severity levels into which system log entries are classified. Valid values: INFO, WARN, ERROR, and FATAL.

#### sys_log_roll_mode

- **Default**: SIZE-MB-1024
- **Unit**: N/A
- **Description**: The mode in which system logs are segmented into log rolls. Valid values include `TIME-DAY`, `TIME-HOUR`, and `SIZE-MB-`size. The default value indicates that logs are segmented into rolls, each of which is 1 GB.

#### sys_log_roll_num

- **Default**: 10
- **Unit**: N/A
- **Description**: The number of log rolls to reserve.

#### sys_log_verbose_modules

- **Default**: Empty string
- **Unit**: N/A
- **Description**: The module of the logs to be printed. For example, if you set this configuration item to OLAP, StarRocks only prints the logs of the OLAP module. Valid values are namespaces in BE, including starrocks, starrocks::debug, starrocks::fs, starrocks::io, starrocks::lake, starrocks::pipeline, starrocks::query_cache, starrocks::stream, and starrocks::workgroup.

#### sys_log_verbose_level

- **Default**: 10
- **Unit**: N/A
- **Description**: The level of the logs to be printed. This configuration item is used to control the output of logs initiated with VLOG in codes.

#### log_buffer_level

- **Default**: Empty string
- **Unit**: N/A
- **Description**: The strategy for flushing logs. The default value indicates that logs are buffered in memory. Valid values are -1 and 0. -1 indicates that logs are not buffered in memory.

#### num_threads_per_core

- **Default**: 3
- **Unit**: N/A
- **Description**: The number of threads started on each CPU core.

#### compress_rowbatches

- **Default**: TRUE
- **Unit**: N/A
- **Description**: A boolean value to control whether to compress the row batches in RPCs between BEs. TRUE indicates compressing the row batches, and FALSE indicates not compressing them.

#### serialize_batch

- **Default**: FALSE
- **Unit**: N/A
- **Description**: A boolean value to control whether to serialize the row batches in RPCs between BEs. TRUE indicates serializing the row batches, and FALSE indicates not serializing them.

#### storage_root_path

- **Default**: `${STARROCKS_HOME}/storage`
- **Unit**: N/A
- **Description**: The directory and medium of the storage volume.
  - Multiple volumes are separated by semicolons (`;`).
  - If the storage medium is SSD, add `medium:ssd` at the end of the directory.
  - If the storage medium is HDD, add ,medium:hdd at the end of the directory.

#### max_length_for_bitmap_function

- **Default**: 1000000
- **Unit**: Byte
- **Description**: The maximum length of input values for bitmap functions.

#### max_length_for_to_base64

- **Default**: 200000
- **Unit**: Byte
- **Description**: The maximum length of input values for the to_base64() function.

#### max_tablet_num_per_shard

- **Default**: 1024
- **Unit**: N/A
- **Description**: The maximum number of tablets in each shard. This configuration item is used to restrict the number of tablet child directories under each storage directory.

#### max_garbage_sweep_interval

- **Default**: 3600
- **Unit**: Second
- **Description**: The maximum time interval for garbage collection on storage volumes.

#### min_garbage_sweep_interval

- **Default**: 180
- **Unit**: Second
- **Description**: The minimum time interval for garbage collection on storage volumes.

#### file_descriptor_cache_capacity

- **Default**: 16384
- **Unit**: N/A
- **Description**: The number of file descriptors that can be cached.

#### min_file_descriptor_number

- **Default**: 60000
- **Unit**: N/A
- **Description**: The minimum number of file descriptors in the BE process.

#### index_stream_cache_capacity

- **Default**: 10737418240
- **Unit**: Byte
- **Description**: The cache capacity for the statistical information of BloomFilter, Min, and Max.

#### disable_storage_page_cache

- **Default**: FALSE
- **Unit**: N/A
- **Description**: A boolean value to control whether to disable PageCache.
  - When PageCache is enabled, StarRocks caches the recently scanned data.
  - PageCache can significantly improve the query performance when similar queries are repeated frequently.
  - TRUE indicates disabling PageCache.
  - The default value of this item has been changed from TRUE to FALSE since StarRocks v2.4.

#### base_compaction_num_threads_per_disk

- **Default**: 1
- **Unit**: N/A
- **Description**: The number of threads used for Base Compaction on each storage volume.

#### base_cumulative_delta_ratio

- **Default**: 0.3
- **Unit**: N/A
- **Description**: The ratio of cumulative file size to base file size. The ratio reaching this value is one of the conditions that trigger the Base Compaction.

#### compaction_trace_threshold

- **Default**: 60
- **Unit**: Second
- **Description**: The time threshold for each compaction. If a compaction takes more time than the time threshold, StarRocks prints the corresponding trace.

#### be_http_port

- **Default**: 8040
- **Unit**: N/A
- **Description**: The HTTP server port.

#### be_http_num_workers

- **Default**: 48
- **Unit**: N/A
- **Description**: The number of threads used by the HTTP server.

#### load_data_reserve_hours

- **Default**: 4
- **Unit**: Hour
- **Description**: The reservation time for the files produced by small-scale loadings.

#### number_tablet_writer_threads

- **Default**: 16
- **Unit**: N/A
- **Description**: The number of threads used for Stream Load.

#### streaming_load_rpc_max_alive_time_sec

- **Default**: 1200
- **Unit**: Second
- **Description**: The RPC timeout for Stream Load.

#### fragment_pool_thread_num_min

- **Default**: 64
- **Unit**: N/A
- **Description**: The minimum number of threads used for query.

#### fragment_pool_thread_num_max

- **Default**: 4096
- **Unit**: N/A
- **Description**: The maximum number of threads used for query.

#### fragment_pool_queue_size

- **Default**: 2048
- **Unit**: N/A
- **Description**: The upper limit of the query number that can be processed on each BE node.

#### enable_token_check

- **Default**: TRUE
- **Unit**: N/A
- **Description**: A boolean value to control whether to enable the token check. TRUE indicates enabling the token check, and FALSE indicates disabling it.

#### enable_prefetch

- **Default**: TRUE
- **Unit**: N/A
- **Description**: A boolean value to control whether to enable the pre-fetch of the query. TRUE indicates enabling pre-fetch, and FALSE indicates disabling it.

#### load_process_max_memory_limit_bytes

- **Default**: 107374182400
- **Unit**: Byte
- **Description**: The maximum size limit of memory resources that can be taken up by all load processes on a BE node.

#### load_process_max_memory_limit_percent

- **Default**: 30
- **Unit**: %
- **Description**: The maximum percentage limit of memory resources that can be taken up by all load processes on a BE node.

#### sync_tablet_meta

- **Default**: FALSE
- **Unit**: N/A
- **Description**: A boolean value to control whether to enable the synchronization of the tablet metadata. TRUE indicates enabling synchronization, and FALSE indicates disabling it.

#### routine_load_thread_pool_size

- **Default**: 10
- **Unit**: N/A
- **Description**: The thread pool size for Routine Load on each BE. Since v3.1.0, this parameter is deprecated. The thread pool size for Routine Load on each BE is now controlled by the FE dynamic parameter max_routine_load_task_num_per_be.

#### brpc_max_body_size

- **Default**: 2147483648
- **Unit**: Byte
- **Description**: The maximum body size of a bRPC.

#### tablet_map_shard_size

- **Default**: 32
- **Unit**: N/A
- **Description**: The tablet map shard size. The value must be a power of two.

#### enable_bitmap_union_disk_format_with_set

- **Default**: FALSE
- **Unit**: N/A
- **Description**: A boolean value to control whether to enable the new storage format of the BITMAP type, which can improve the performance of bitmap_union. TRUE indicates enabling the new storage format, and FALSE indicates disabling it.

#### mem_limit

- **Default**: 90%
- **Unit**: N/A
- **Description**: BE process memory upper limit. You can set it as a percentage ("80%") or a physical limit ("100G"). The default hard limit is 90% of the server's memory size, and the soft limit is 80%. You need to configure this parameter if you want to deploy StarRocks with other memory-intensive services on a same server.

#### flush_thread_num_per_store

- **Default**: 2
- **Unit**: N/A
- **Description**: Number of threads that are used for flushing MemTable in each store.

#### datacache_enable

- **Default**: false
- **Unit**: N/A
- **Description**: Whether to enable Data Cache. TRUE indicates Data Cache is enabled, and FALSE indicates Data Cache is disabled.

#### datacache_disk_path

- **Default**: N/A
- **Unit**: N/A
- **Description**: The paths of disks. We recommend that the number of paths you configure for this parameter is the same as the number of disks on your BE machine. Multiple paths need to be separated with semicolons (;).

#### datacache_meta_path

- **Default**: N/A
- **Unit**: N/A
- **Description**: The storage path of block metadata. You can customize the storage path. We recommend that you store the metadata under the $STARROCKS_HOME path.

#### datacache_mem_size

- **Default**: 10%
- **Unit**: N/A
- **Description**: The maximum amount of data that can be cached in memory. You can set it as a percentage (for example, `10%`) or a physical limit (for example, `10G`, `21474836480`). The default value is `10%`. We recommend that you set the value of this parameter to at least 10 GB.

#### datacache_disk_size

- **Default**: 0
- **Unit**: N/A
- **Description**: The maximum amount of data that can be cached on a single disk. You can set it as a percentage (for example, `80%`) or a physical limit (for example, `2T`, `500G`). For example, if you configure two disk paths for the `datacache_disk_path` parameter and set the value of the `datacache_disk_size` parameter as `21474836480` (20 GB), a maximum of 40 GB data can be cached on these two disks. The default value is `0`, which indicates that only memory is used to cache data. Unit: bytes.

#### jdbc_connection_pool_size

- **Default**: 8
- **Unit**: N/A
- **Description**: The JDBC connection pool size. On each BE node, queries that access the external table with the same jdbc_url share the same connection pool.

#### jdbc_minimum_idle_connections

- **Default**: 1
- **Unit**: N/A
- **Description**: The minimum number of idle connections in the JDBC connection pool.

#### jdbc_connection_idle_timeout_ms

- **Default**: 600000
- **Unit**: N/A
- **Description**: The length of time after which an idle connection in the JDBC connection pool expires. If the connection idle time in the JDBC connection pool exceeds this value, the connection pool closes idle connections beyond the number specified in the configuration item jdbc_minimum_idle_connections.

#### query_cache_capacity

- **Default**: 536870912
- **Unit**: N/A
- **Description**: The size of the query cache in the BE. Unit: bytes. The default size is 512 MB. The size cannot be less than 4 MB. If the memory capacity of the BE is insufficient to provision your expected query cache size, you can increase the memory capacity of the BE.

#### enable_event_based_compaction_framework

- **Default**: TRUE
- **Unit**: N/A
- **Description**: Whether to enable the Event-based Compaction Framework. TRUE indicates Event-based Compaction Framework is enabled, and FALSE indicates it is disabled. Enabling Event-based Compaction Framework can greatly reduce the overhead of compaction in scenarios where there are many tablets or a single tablet has a large amount of data.

#### enable_size_tiered_compaction_strategy

- **Default**: TRUE
- **Unit**: N/A
- **Description**: Whether to enable the Size-tiered Compaction strategy. TRUE indicates the Size-tiered Compaction strategy is enabled, and FALSE indicates it is disabled.

#### lake_service_max_concurrency

- **Default**: 0
- **Unit**: N/A
- **Description**: The maximum concurrency of RPC requests in a shared-data cluster. Incoming requests will be rejected when this threshold is reached. When this item is set to 0, no limit is imposed on the concurrency.
