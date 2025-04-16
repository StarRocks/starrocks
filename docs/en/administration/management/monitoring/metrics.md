---
displayed_sidebar: docs
---

# General Monitoring Metrics

This topic introduces some important general metrics of StarRocks.

For dedicated metrics for materialized views and shared-data clusters, please refer to the corresponding sections:

- [Metrics for asynchronous materialized view metrics](./metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](./metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](./Monitor_and_Alert.md).

## Metric items

### be_broker_count

- Unit: Count
- Type: Average
- Description: Number of brokers.

### be_brpc_endpoint_count

- Unit: Count
- Type: Average
- Description: Number of StubCache in bRPC.

### be_bytes_read_per_second

- Unit: Bytes/s
- Type: Average
- Description: Read speed of BE.

### be_bytes_written_per_second

- Unit: Bytes/s
- Type: Average
- Description: Write speed of BE.

### be_base_compaction_bytes_per_second

- Unit: Bytes/s
- Type: Average
- Description: Base compaction speed of BE.

### be_cumulative_compaction_bytes_per_second

- Unit: Bytes/s
- Type: Average
- Description: Cumulative compaction speed of BE.

### be_base_compaction_rowsets_per_second

- Unit: Count
- Type: Average
- Description: Base compaction speed of BE rowsets.

### be_cumulative_compaction_rowsets_per_second

- Unit: Count
- Type: Average
- Description: Cumulative compaction speed of BE rowsets.

### be_base_compaction_failed

- Unit: Count/s
- Type: Average
- Description: Base compaction failure of BE.

### be_clone_failed

- Unit: Count/s
- Type: Average
- Description: BE clone failure.

### be_create_rollup_failed

- Unit: Count/s
- Type: Average
- Description: Materialized view creation failure of BE.

### be_create_tablet_failed

- Unit: Count/s
- Type: Average
- Description: Tablet creation failure of BE.

### be_cumulative_compaction_failed

- Unit: Count/s
- Type: Average
- Description: Cumulative compaction failure of BE.

### be_delete_failed

- Unit: Count/s
- Type: Average
- Description: Delete failure of BE.

### be_finish_task_failed

- Unit: Count/s
- Type: Average
- Description: Task failure of BE.

### be_publish_failed

- Unit: Count/s
- Type: Average
- Description: Version release failure of BE.

### be_report_tables_failed

- Unit: Count/s
- Type: Average
- Description: Table report failure of BE.

### be_report_disk_failed

- Unit: Count/s
- Type: Average
- Description: Disk report failure of BE.

### be_report_tablet_failed

- Unit: Count/s
- Type: Average
- Description: Tablet report failure of BE.

### be_report_task_failed

- Unit: Count/s
- Type: Average
- Description: Task report failure of BE.

### be_schema_change_failed

- Unit: Count/s
- Type: Average
- Description: Schema change failure of BE.

### be_base_compaction_requests

- Unit: Count/s
- Type: Average
- Description: Base compaction request of BE.

### be_clone_total_requests

- Unit: Count/s
- Type: Average
- Description: Clone request of BE.

### be_create_rollup_requests

- Unit: Count/s
- Type: Average
- Description: Materialized view creation request of BE.

### be_create_tablet_requests

- Unit: Count/s
- Type: Average
- Description: Tablet creation request of BE.

### be_cumulative_compaction_requests

- Unit: Count/s
- Type: Average
- Description: Cumulative compaction request of BE.

### be_delete_requests

- Unit: Count/s
- Type: Average
- Description: Delete request of BE.

### be_finish_task_requests

- Unit: Count/s
- Type: Average
- Description: Task finish request of BE.

### be_publish_requests

- Unit: Count/s
- Type: Average
- Description: Version publish request of BE.

### be_report_tablets_requests

- Unit: Count/s
- Type: Average
- Description: Tablet report request of BE.

### be_report_disk_requests

- Unit: Count/s
- Type: Average
- Description: Disk report request of BE.

### be_report_tablet_requests

- Unit: Count/s
- Type: Average
- Description: Tablet report request of BE.

### be_report_task_requests

- Unit: Count/s
- Type: Average
- Description: Task report request of BE.

### be_schema_change_requests

- Unit: Count/s
- Type: Average
- Description: Schema change report request of BE.

### be_storage_migrate_requests

- Unit: Count/s
- Type: Average
- Description: Migration request of BE.

### be_fragment_endpoint_count

- Unit: Count
- Type: Average
- Description: Number of BE DataStream.

### be_fragment_request_latency_avg

- Unit: ms
- Type: Average
- Description: Latency of fragment requests.

### be_fragment_requests_per_second

- Unit: Count/s
- Type: Average
- Description: Number of fragment requests.

### be_http_request_latency_avg

- Unit: ms
- Type: Average
- Description: Latency of HTTP requests.

### be_http_requests_per_second

- Unit: Count/s
- Type: Average
- Description: Number of HTTP requests.

### be_http_request_send_bytes_per_second

- Unit: Bytes/s
- Type: Average
- Description: Number of bytes sent for HTTP requests.

### fe_connections_per_second

- Unit: Count/s
- Type: Average
- Description: New connection rate of FE.

### fe_connection_total

- Unit: Count
- Type: Cumulative
- Description: Total number of FE connections.

### fe_edit_log_read

- Unit: Count/s
- Type: Average
- Description: Read speed of FE edit log.

### fe_edit_log_size_bytes

- Unit: Bytes/s
- Type: Average
- Description: Size of FE edit log.

### fe_edit_log_write

- Unit: Bytes/s
- Type: Average
- Description: Write speed of FE edit log.

### fe_checkpoint_push_per_second

- Unit: Count/s
- Type: Average
- Description: Number of FE checkpoints.

### fe_pending_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of pending hadoop jobs.

### fe_committed_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of committed hadoop jobs.

### fe_loading_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of loading hadoop jobs.

### fe_finished_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of completedhadoop jobs.

### fe_cancelled_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled hadoop jobs.

### fe_pending_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of pending insert jobs.

### fe_loading_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of loading insert jobs.

### fe_committed_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of committed insert jobs.

### fe_finished_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of completed insert jobs.

### fe_cancelled_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled insert jobs.

### fe_pending_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of pending broker jobs.

### fe_loading_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of loading broker jobs.

### fe_committed_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of committed broker jobs.

### fe_finished_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of finished broker jobs.

### fe_cancelled_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled broker jobs.

### fe_pending_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of pending delete jobs.

### fe_loading_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of loading delete jobs.

### fe_committed_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of committed delete jobs.

### fe_finished_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of finished delete jobs.

### fe_cancelled_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled delete jobs.

### fe_rollup_running_alter_job

- Unit: Count
- Type: Average
- Description: Number of jobs created in rollup.

### fe_schema_change_running_job

- Unit: Count
- Type: Average
- Description: Number of jobs in schema change.

### cpu_util

- Unit: -
- Type: Average
- Description: CPU usage rate.

### cpu_system

- Unit: -
- Type: Average
- Description: cpu_system usage rate.

### cpu_user

- Unit: -
- Type: Average
- Description: cpu_user usage rate.

### cpu_idle

- Unit: -
- Type: Average
- Description: cpu_idle usage rate.

### cpu_guest

- Unit: -
- Type: Average
- Description: cpu_guest usage rate.

### cpu_iowait

- Unit: -
- Type: Average
- Description: cpu_iowait usage rate.

### cpu_irq

- Unit: -
- Type: Average
- Description: cpu_irq usage rate.

### cpu_nice

- Unit: -
- Type: Average
- Description: cpu_nice usage rate.

### cpu_softirq

- Unit: -
- Type: Average
- Description: cpu_softirq usage rate.

### cpu_steal

- Unit: -
- Type: Average
- Description: cpu_steal usage rate.

### disk_free

- Unit: Bytes
- Type: Average
- Description: Free disk capacity.

### disk_io_svctm

- Unit: ms
- Type: Average
- Description: Disk IO service time.

### disk_io_util

- Unit: -
- Type: Average
- Description: Disk usage.

### disk_used

- Unit: Bytes
- Type: Average
- Description: Used disk capacity.

### encryption_keys_created

- Unit: Count
- Type: Cumulative
- Description: number of file encryption keys created for file encryption

### encryption_keys_unwrapped

- Unit: Count
- Type: Cumulative
- Description: number of encryption meta unwrapped for file decryption

### encryption_keys_in_cache

- Unit: Count
- Type: Instantaneous
- Description: number of encryption keys currently in key cache

### starrocks_fe_meta_log_count

- Unit: Count
- Type: Instantaneous
- Description: The number of Edit Logs without a checkpoint. A value within `100000` is considered reasonable..

### starrocks_fe_query_resource_group

- Unit: Count
- Type: Cumulative
- Description: The number of queries for each resource group.

### starrocks_fe_query_resource_group_latency

- Unit: Seconds
- Type: Average
- Description: the query latency percentile for each resource group.

### starrocks_fe_query_resource_group_err

- Unit: Count
- Type: Cumulative
- Description: The number of incorrect queries for each resource group.

### starrocks_be_resource_group_cpu_limit_ratio

- Unit: -
- Type: Instantaneous
- Description: Instantaneous value of resource group cpu quota ratio.

### starrocks_be_resource_group_cpu_use_ratio

- Unit: -
- Type: Average
- Description: The ratio of CPU time used by the resource group to the CPU time of all resource groups.

### starrocks_be_resource_group_mem_limit_bytes

- Unit: Bytes
- Type: Instantaneous
- Description: Instantaneous value of resource group memory quota.

### starrocks_be_resource_group_mem_allocated_bytes

- Unit: Bytes
- Type: Instantaneous
- Description: Instantaneous value of resource group memory usage.

### starrocks_be_pipe_prepare_pool_queue_len

- Unit: Count
- Type: Instantaneous
- Description: Instantaneous value of pipeline prepare thread pool task queue length.

### starrocks_fe_safe_mode

- Unit: -
- Type: Instantaneous
- Description: Indicates whether Safe Mode is enabled. Valid values: `0` (disabled) and `1` (enabled). When Safe Mode is enabled, the cluster no longer accepts any loading requests.

### starrocks_fe_unfinished_backup_job

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of running BACKUP tasks under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse. For a shared-data cluster, this value is always `0`.

### starrocks_fe_unfinished_restore_job

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of running RESTORE tasks under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse. For a shared-data cluster, this value is always `0`.

### starrocks_fe_memory

- Unit: Bytes or Count
- Type: Instantaneous
- Description: Indicates the memory statistics for various modules under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

### starrocks_fe_unfinished_query

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of queries currently running under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

### starrocks_fe_last_finished_job_timestamp

- Unit: ms
- Type: Instantaneous
- Description: Indicates the end time of the last query or loading under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

### starrocks_fe_query_resource_group

- Unit: Count
- Type: Cumulative
- Description: Indicates the total number of queries executed under the specific resource group.

### starrocks_fe_query_resource_group_err

- Unit: Count
- Type: Cumulative
- Description: Indicates the number of failed queries under the specific resource group.

### starrocks_fe_query_resource_group_latency

- Unit: ms
- Type: Cumulative
- Description: Indicates the latency statistics for queries under the specific resource group.

### starrocks_fe_tablet_num

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of tablets on each BE node.

### starrocks_fe_tablet_max_compaction_score

- Unit: Count
- Type: Instantaneous
- Description: Indicates the highest Compaction Score on each BE node.


### update_compaction_outputs_total

- Unit: Count
- Description: Total number of Primary Key table compactions.

### update_del_vector_bytes_total

- Unit: Bytes
- Description: Total memory used for caching DELETE vectors in Primary Key tables.

### push_request_duration_us

- Unit: us
- Description: Total time spent on Spark Load.

### writable_blocks_total (Deprecated)

### disks_data_used_capacity

- Description: Used capacity of each disk (represented by a storage path).

### query_scan_rows

- Unit: Count
- Description: Total number of scanned rows.

### update_primary_index_num

- Unit: Count
- Description: Number of Primary Key indexes cached in memory.

### result_buffer_block_count

- Unit: Count
- Description: Number of blocks in the result buffer.

### query_scan_bytes

- Unit: Bytes
- Description: Total number of scanned bytes.

### disk_reads_completed

- Unit: Count
- Description: Number of successfully completed disk reads.

### query_cache_hit_count

- Unit: Count
- Description: Number of query cache hits.

### jemalloc_resident_bytes

- Unit: Bytes
- Description: Maximum number of bytes in physically resident data pages mapped by the allocator, comprising all pages dedicated to allocator metadata, pages backing active allocations, and unused dirty pages.

### blocks_open_writing (Deprecated)

### disk_io_time_weigthed

- Unit: ms
- Description: Weighted time spent on I/Os.

### update_compaction_task_byte_per_second

- Unit: Bytes/s
- Description: Estimated rate of Primary Key table compactions.

### blocks_open_reading (Deprecated)

### tablet_update_max_compaction_score

- Unit: -
- Description: Highest compaction score of tablets in Primary Key tables in the current BE.

### segment_read

- Unit: Count
- Description: Total number of segment reads.

### disk_io_time_ms

- Unit: ms
- Description: Time spent on I/Os.

### load_mem_bytes

- Unit: Bytes
- Description: Memory cost of data loading.

### delta_column_group_get_non_pk_total

- Unit: Count
- Description: Total number of times to get delta column group (for non-Primary Key tables only).

### query_scan_bytes_per_second

- Unit: Bytes/s
- Description: Estimated rate of scanned bytes per second.

### active_scan_context_count

- Unit: Count
- Description: Total number of scan tasks created by Flink/Spark SQL.

### fd_num_limit

- Unit: Count
- Description: Maximum number of file descriptors.

### update_compaction_task_cost_time_ns

- Unit: ns
- Description: Total time spent on the Primary Key table compactions.

### delta_column_group_get_hit_cache

- Unit: Count
- Description: Total number of delta column group cache hits (for Primary Key tables only).

### data_stream_receiver_count

- Unit: Count
- Description: Cumulative number of instances serving as Exchange receivers in BE.

### bytes_written_total

- Unit: Bytes
- Description: Total bytes written (sectors write * 512).

### transaction_streaming_load_bytes

- Unit: Bytes
- Description: Total loading bytes of transaction load.

### running_cumulative_compaction_task_num

- Unit: Count
- Description: Total number of running cumulative compactions.

### transaction_streaming_load_requests_total

- Unit: Count
- Description: Total number of transaction load requests.

### cpu

- Unit: -
- Description: CPU usage information returned by `/proc/stat`.

### update_del_vector_num

- Unit: Count
- Description: Number of the DELETE vector cache items in Primary Key tables.

### disks_avail_capacity

- Description: Available capacity of a specific disk.

### clone_mem_bytes

- Unit: Bytes
- Description: Memory used for replica clone.

### fragment_requests_total

- Unit: Count
- Description: Total fragment instances executing on a BE (for non-pipeline engine).

### disk_write_time_ms

- Unit: ms
- Description: Time spent on disk writing. Unit: ms.

### schema_change_mem_bytes

- Unit: Bytes
- Description: Memory used for Schema Change.

### thrift_connections_total

- Unit: Count
- Description: Total number of thrift connections (including finished connections).

### thrift_opened_clients

- Unit: Count
- Description: Number of currently opened thrift clients.

### thrift_used_clients

- Unit: Count
- Description: Number of thrift clients in use currently.

### thrift_current_connections (Deprecated)

### disk_bytes_read

- Unit: Bytes
- Description: Total bytes read from the disk(sectors read * 512).

### base_compaction_task_cost_time_ms

- Unit: ms
- Description: Total time spent on base compactions.

### update_primary_index_bytes_total

- Unit: Bytes
- Description: Total memory cost of the Primary Key index.

### compaction_deltas_total

- Unit: Count
- Description: Total number of merged rowsets from base compactions and cumulative compactions.

### max_network_receive_bytes_rate

- Unit: Bytes
- Description: Total bytes received over the network (maximum value among all network interfaces).

### max_network_send_bytes_rate

- Unit: Bytes
- Description: Total bytes sent over the network (maximum value among all network interfaces).

### chunk_allocator_mem_bytes

- Unit: Bytes
- Description: Memory used by the chunk allocator.

### query_cache_usage_ratio

- Unit: -
- Description: Current query cache usage ratio.

### process_fd_num_limit_soft

- Unit: Count
- Description: Soft limit on the maximum number of file descriptors. Please note that this item indicates the soft limit. You can set the hard limit using the `ulimit` command.

### query_cache_hit_ratio

- Unit: -
- Description: Hit ratio of query cache.

### tablet_metadata_mem_bytes

- Unit: Bytes
- Description: Memory used by tablet metadata.

### jemalloc_retained_bytes

- Unit: Bytes
- Description: Total bytes of virtual memory mappings that were retained rather than returned to the operating system via operations like munmap(2).

### unused_rowsets_count

- Unit: Count
- Description: Total number of unused rowsets. Please note that these rowsets will be reclaimed later.

### update_rowset_commit_apply_total

- Unit: Count
- Description: Total number of COMMIT and APPLY for Primary Key tables.

### segment_zonemap_mem_bytes

- Unit: Bytes
- Description: Memory used by segment zonemap.

### base_compaction_task_byte_per_second

- Unit: Bytes/s
- Description: Estimated rate of base compactions.

### transaction_streaming_load_duration_ms

- Unit: ms
- Description: Total time spent on Stream Load transaction Interface.

### memory_pool_bytes_total

- Unit: Bytes
- Description: Memory used by the memory pool.

### short_key_index_mem_bytes

- Unit: Bytes
- Description: Memory used by the short key index.

### disk_bytes_written

- Unit: Bytes
- Description: Total bytes written to disk.

### segment_flush_queue_count

- Unit: Count
- Description: Number of queued tasks in the segment flush thread pool.

### jemalloc_metadata_bytes

- Unit: Bytes
- Description: Total number of bytes dedicated to metadata, comprising base allocations used for bootstrap-sensitive allocator metadata structures and internal allocations. The usage of transparent huge pages is not included in this item.

### network_send_bytes

- Unit: Bytes
- Description: Number of bytes sent over the network.

### update_mem_bytes

- Unit: Bytes
- Description: Memory used by Primary Key table APPLY tasks and Primary Key index.

### blocks_created_total (Deprecated)

### query_cache_usage

- Unit: Bytes
- Description: Current query cache usages.

### memtable_flush_duration_us

- Unit: us
- Description: Total time spent on memtable flush.

### push_request_write_bytes

- Unit: Bytes
- Description: Total bytes written via Spark Load.

### jemalloc_active_bytes

- Unit: Bytes
- Description: Total bytes in active pages allocated by the application.

### load_rpc_threadpool_size

- Unit: Count
- Description: The current size of the RPC thread pool, which is used for handling Routine Load and loading via table functions. The default value is 10, with a maximum value of 1000. This value is dynamically adjusted based on the usage of the thread pool.

### publish_version_queue_count

- Unit: Count
- Description: Queued task count in the Publish Version thread pool.

### chunk_pool_system_free_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

### chunk_pool_system_free_cost_ns

- Unit: ns
- Description: Memory chunk allocation/cache metrics.

### chunk_pool_system_alloc_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

### chunk_pool_system_alloc_cost_ns

- Unit: ns
- Description: Memory chunk allocation/cache metrics.

### chunk_pool_local_core_alloc_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

### chunk_pool_other_core_alloc_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

### fragment_endpoint_count

- Unit: Count
- Description: Cumulative number of instances serving as Exchange senders in BE.

### disk_sync_total (Deprecated)

### max_disk_io_util_percent

- Unit: -
- Description: Maximum disk I/O utilization percentage.

### network_receive_bytes

- Unit: Bytes
- Description: Total bytes received via network.

### storage_page_cache_mem_bytes

- Unit: Bytes
- Description: Memory used by storage page cache.

### jit_cache_mem_bytes

- Unit: Bytes
- Description: Memory used by jit compiled function cache.

### column_partial_update_apply_total

- Unit: Count
- Description: Total number of APPLY for partial updates by column (Column mode)

### disk_writes_completed

- Unit: Count
- Description: Total number of successfully completed disk writes.

### memtable_flush_total

- Unit: Count
- Description: Total number of memtable flushes.

### page_cache_hit_count

- Unit: Count
- Description: Total number of hits in the storage page cache.

### bytes_read_total (Deprecated)

### update_rowset_commit_request_total

- Unit: Count
- Description: Total number of rowset COMMIT requests in Primary Key tables.

### tablet_cumulative_max_compaction_score

- Unit: -
- Description: Highest cumulative compaction score of tablets in this BE.

### column_zonemap_index_mem_bytes

- Unit: Bytes
- Description: Memory used by column zonemaps.

### push_request_write_bytes_per_second

- Unit: Bytes/s
- Description: Data writing rate of Spark Load.

### process_thread_num

- Unit: Count
- Description: Total number of threads in this process.

### query_cache_lookup_count

- Unit: Count
- Description: Total number of query cache lookups.

### http_requests_total (Deprecated)

### http_request_send_bytes (Deprecated)

### cumulative_compaction_task_cost_time_ms

- Unit: ms
- Description: Total time spent on cumulative compactions.

### column_metadata_mem_bytes

- Unit: Bytes
- Description: Memory used by column metadata.

### plan_fragment_count

- Unit: Count
- Description: Number of currently running query plan fragments.

### page_cache_lookup_count

- Unit: Count
- Description: Total number of storage page cache lookups.

### query_mem_bytes

- Unit: Bytes
- Description: Memory used by queries.

### load_channel_count

- Unit: Count
- Description: Total number of loading channels.

### push_request_write_rows

- Unit: Count
- Description: Total rows written via Spark Load.

### running_base_compaction_task_num

- Unit: Count
- Description: Total number of running base compaction tasks.

### fragment_request_duration_us

- Unit: us
- Description: Cumulative execution time of fragment instances (for non-pipeline engine).

### process_mem_bytes

- Unit: Bytes
- Description: Memory used by this process.

### broker_count

- Unit: Count
- Description: Total number of filesystem brokers (by host address) created.

### segment_replicate_queue_count

- Unit: Count
- Description: Queued task count in the Segment Replicate thread pool.

### brpc_endpoint_stub_count

- Unit: Count
- Description: Total number of bRPC stubs (by address).

### readable_blocks_total (Deprecated)

### delta_column_group_get_non_pk_hit_cache

- Unit: Count
- Description: Total number of hits in the delta column group cache (for non-Primary Key tables).

### update_del_vector_deletes_new

- Unit: Count
- Description: Total number of newly generated DELETE vectors used in Primary Key tables.

### compaction_bytes_total

- Unit: Bytes
- Description: Total merged bytes from base compactions and cumulative compactions.

### segment_metadata_mem_bytes

- Unit: Bytes
- Description: Memory used by segment metadata.

### column_partial_update_apply_duration_us

- Unit: us
- Description: Total time spent on partial updates for columns' APPLY tasks (Column mode).

### bloom_filter_index_mem_bytes

- Unit: Bytes
- Description: Memory used by Bloomfilter indexes.

### routine_load_task_count

- Unit: Count
- Description: Number of currently running Routine Load tasks.

### delta_column_group_get_total

- Unit: Count
- Description: Total number of times to get delta column groups (for Primary Key tables).

### disk_read_time_ms

- Unit: ms
- Description: Time spent on reading from disk. Unit: ms.

### update_compaction_outputs_bytes_total

- Unit: Bytes
- Description: Total bytes written by Primary Key table compactions.

### memtable_flush_queue_count

- Unit: Count
- Description: Queued task count in the memtable flush thread pool.

### query_cache_capacity

- Description: Capacity of query cache.

### streaming_load_duration_ms

- Unit: ms
- Description: Total time spent on Stream Load.

### streaming_load_current_processing

- Unit: Count
- Description: Number of currently running Stream Load tasks.

### streaming_load_bytes

- Unit: Bytes
- Description: Total bytes loaded by Stream Load.

### load_rows

- Unit: Count
- Description: Total loaded rows.

### load_bytes

- Unit: Bytes
- Description: Total loaded bytes.

### meta_request_total

- Unit: Count
- Description: Total number of meta read/write requests.

### meta_request_duration

- Unit: us
- Description: Total meta read/write duration.

### tablet_base_max_compaction_score

- Unit: -
- Description: Highest base compaction score of tablets in this BE.

### page_cache_capacity

- Description: Capacity of the storage page cache.

### cumulative_compaction_task_byte_per_second

- Unit: Bytes/s
- Description: Rate of bytes processed during cumulative compactions.

### stream_load

- Unit: -
- Description: Total loaded rows and received bytes.

### stream_load_pipe_count

- Unit: Count
- Description: Number of currently running Stream Load tasks.

### binary_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the BINARY column pool.

### int16_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT16 column pool.

### decimal_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DECIMAL column pool.

### double_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DOUBLE column pool.

### int128_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT128 column pool.

### date_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DATE column pool.

### int64_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT64 column pool.

### int8_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT8 column pool.

### datetime_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DATETIME column pool.

### int32_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT32 column pool.

### float_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the FLOAT column pool.

### uint8_column_pool_bytes

- Unit: Bytes
- Description: Bytes used by the UINT8 column pool.

### column_pool_mem_bytes

- Unit: Bytes
- Description: Memory used by the column pools.

### local_column_pool_bytes (Deprecated)

### total_column_pool_bytes (Deprecated)

### central_column_pool_bytes (Deprecated)

### wait_base_compaction_task_num

- Unit: Count
- Description: Number of base compaction tasks waiting for execution.

### wait_cumulative_compaction_task_num

- Unit: Count
- Description: Number of cumulative compaction tasks waiting for execution.

### jemalloc_allocated_bytes

- Unit: Bytes
- Description: Total number of bytes allocated by the application.

### pk_index_compaction_queue_count

- Unit: Count
- Description: Queued task count in the Primary Key index compaction thread pool.

### disks_total_capacity

- Description: Total capacity of the disk.

### disks_state

- Unit: -
- Description: State of each disk. `1` indicates that the disk is in use, and `0` indicates that it is not in use.

### update_del_vector_deletes_total (Deprecated)

### update_del_vector_dels_num (Deprecated)

### result_block_queue_count

- Unit: Count
- Description: Number of results in the result block queue.

### engine_requests_total

- Unit: Count
- Description: Total count of all types of requests between BE and FE, including CREATE TABLE, Publish Version, and tablet clone.

### snmp

- Unit: -
- Description: Metrics returned by `/proc/net/snmp`.

### compaction_mem_bytes

- Unit: Bytes
- Description: Memory used by compactions.

### txn_request

- Unit: -
- Description: Transaction requests of BEGIN, COMMIT, ROLLBACK, and EXEC.

### small_file_cache_count

- Unit: Count
- Description: Number of small file caches.

### rowset_metadata_mem_bytes

- Unit: Bytes
- Description: Total number of bytes for rowset metadata.

### update_apply_queue_count

- Unit: Count
- Description: Queued task count in the Primary Key table transaction APPLY thread pool.

### async_delta_writer_queue_count

- Unit: Count
- Description: Queued task count in the tablet delta writer thread pool.

### update_compaction_duration_us

- Unit: us
- Description: Total time spent on Primary Key table compactions.

### transaction_streaming_load_current_processing

- Unit: Count
- Description: Number of currently running transactional Stream Load tasks.

### ordinal_index_mem_bytes

- Unit: Bytes
- Description: Memory used by ordinal indexes.

### consistency_mem_bytes

- Unit: Bytes
- Description: Memory used by replica consistency checks.

### tablet_schema_mem_bytes

- Unit: Bytes
- Description: Memory used by tablet schema.

### fd_num_used

- Unit: Count
- Description: Number of file descriptors currently in use.

### running_update_compaction_task_num

- Unit: Count
- Description: Total number of currently running Primary Key table compaction tasks.

### rowset_count_generated_and_in_use

- Unit: Count
- Description: Number of rowset IDs currently in use.

### bitmap_index_mem_bytes

- Unit: Bytes
- Description: Memory used by bitmap indexes.

### update_rowset_commit_apply_duration_us

- Unit: us
- Description: Total time spent on Primary Key table APPLY tasks.

### process_fd_num_used

- Unit: Count
- Description: Number of file descriptors currently in use in this BE process.

### network_send_packets

- Unit: Count
- Description: Total number of packets sent through the network.

### network_receive_packets

- Unit: Count
- Description: Total number of packets received through the network.

### metadata_mem_bytes (Deprecated)

### push_requests_total

- Unit: Count
- Description: Total number of successful and failed Spark Load requests.

### blocks_deleted_total (Deprecated)

### jemalloc_mapped_bytes

- Unit: Bytes
- Description: Total number of bytes in active extents mapped by the allocator.

### process_fd_num_limit_hard

- Unit: Count
- Description: Hard limit on the maximum number of file descriptors.

### jemalloc_metadata_thp

- Unit: Count
- Description: Number of Transparent Huge Pages used for metadata.

### update_rowset_commit_request_failed

- Unit: Count
- Description: Total number of failed rowset COMMIT requests in Primary Key tables.

### streaming_load_requests_total

- Unit: Count
- Description: Total number of Stream Load requests.

### resource_group_running_queries

- Unit: Count
- Description: Number of queries currently running in each resource group. This is an instantaneous value.

### resource_group_total_queries

- Unit: Count
- Description: Total number of queries executed in each resource group, including those currently running. This is an instantaneous value.

### resource_group_bigquery_count

- Unit: Count
- Description: Number of queries in each resource group that have triggered the limit for big queries. This is an instantaneous value.

### resource_group_concurrency_overflow_count

- Unit: Count
- Description: Number of queries in each resource group that have triggered the concurrency limit. This is an instantaneous value.

### resource_group_mem_limit_bytes

- Unit: Bytes
- Description: Memory limit for each resource group, measured in bytes. This is an instantaneous value.

### resource_group_mem_inuse_bytes

- Unit: Bytes
- Description: Currently used memory by each resource group, measured in bytes. This is an instantaneous value.

### resource_group_cpu_limit_ratio

- Unit: -
- Description: Ratio of the CPU core limit for each resource group to the total CPU core limit for all resource groups. This is an instantaneous value.

### resource_group_inuse_cpu_cores

- Unit: Count
- Description: Estimated number of CPU cores currently in use by each resource group. This is an average value over the time interval between two metric retrievals.

### resource_group_cpu_use_ratio (Deprecated)

- Unit: -
- Description: Ratio of pipeline thread time slices used by each resource group to the total used by all resource groups. This is an average value over the time interval between two metric retrievals.

### resource_group_connector_scan_use_ratio (Deprecated)

- Unit: -
- Description: Ratio of external table scan thread time slices used by each resource group to the total used by all resource groups. This is an average value over the time interval between two metric retrievals.

### resource_group_scan_use_ratio (Deprecated)

- Unit: -
- Description: Ratio of internal table scan thread time slices used by each resource group to the total used by all resource groups. This is an average value over the time interval between two metric retrievals.

### pipe_poller_block_queue_len

- Unit: Count
- Description: Current length of the block queue of PipelineDriverPoller in the pipeline engine.

### pip_query_ctx_cnt

- Unit: Count
- Description: Total number of currently running queries in the BE.

### pipe_driver_schedule_count

- Unit: Count
- Description: Cumulative number of driver scheduling times for pipeline executors in the BE.

### pipe_scan_executor_queuing

- Unit: Count
- Description: Current number of pending asynchronous I/O tasks launched by Scan Operators.

### pipe_driver_queue_len

- Unit: Count
- Description: Current number of ready drivers in the ready queue waiting for scheduling in BE.

### pipe_driver_execution_time

- Description: Cumulative time spent by PipelineDriver executors on processing PipelineDrivers.

### pipe_prepare_pool_queue_len

- Unit: Count
- Description: Queued task count in the pipeline PREPARE thread pool. This is an instantaneous value.

### starrocks_fe_routine_load_jobs

- Unit: Count
- Description: The total number of Routine Load jobs in different states. For example:

  ```plaintext
  starrocks_fe_routine_load_jobs{state="NEED_SCHEDULE"} 0
  starrocks_fe_routine_load_jobs{state="RUNNING"} 1
  starrocks_fe_routine_load_jobs{state="PAUSED"} 0
  starrocks_fe_routine_load_jobs{state="STOPPED"} 0
  starrocks_fe_routine_load_jobs{state="CANCELLED"} 1
  starrocks_fe_routine_load_jobs{state="UNSTABLE"} 0
  ```

### starrocks_fe_routine_load_paused

- Unit: Count
- Description: The total number of times Routine Load jobs are paused.

### starrocks_fe_routine_load_rows

- Unit: Count
- Description: The total number of rows loaded by all Routine Load jobs.

### starrocks_fe_routine_load_receive_bytes

- Unit: Byte
- Description: The total amount of data loaded by all Routine Load jobs.

### starrocks_fe_routine_load_error_rows

- Unit: Count
- Description: The total number of error rows encountered during data loading by all Routine Load jobs.
