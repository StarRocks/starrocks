---
displayed_sidebar: docs
---

# Table, ingest, and query metrics

This topic introduces some important general metrics of StarRocks.

For dedicated metrics for materialized views and shared-data clusters, please refer to the corresponding sections:

- [Metrics for asynchronous materialized view metrics](../metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](../metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../Monitor_and_Alert.md).

## Metric items

### Table and ingest processing metrics

#### update_compaction_outputs_total

- Unit: Count
- Description: Total number of Primary Key table compactions.

#### update_del_vector_bytes_total

- Unit: Bytes
- Description: Total memory used for caching DELETE vectors in Primary Key tables.

#### push_request_duration_us

- Unit: us
- Description: Total time spent on Spark Load.

### Query metrics

#### query_scan_rows

- Unit: Count
- Description: Total number of scanned rows.

#### update_primary_index_num

- Unit: Count
- Description: Number of Primary Key indexes cached in memory.

#### result_buffer_block_count

- Unit: Count
- Description: Number of blocks in the result buffer.

#### query_scan_bytes

- Unit: Bytes
- Description: Total number of scanned bytes.

#### starrocks_be_files_scan_num_files_read

- Unit: Count
- Description: Number of files read from external storage (CSV, Parquet, ORC, JSON, Avro). Labels: `file_format`, `scan_type`.

#### starrocks_be_files_scan_num_bytes_read

- Unit: Bytes
- Description: Total bytes read from external storage. Labels: `file_format`, `scan_type`.

#### starrocks_be_files_scan_num_raw_rows_read

- Unit: Count
- Description: Total raw rows read from external storage before format validation and predicate filtering. Labels: `file_format`, `scan_type`.

#### starrocks_be_files_scan_num_valid_rows_read

- Unit: Count
- Description: Number of valid rows read (excluding rows with invalid format). Labels: `file_format`, `scan_type`.

#### starrocks_be_files_scan_num_rows_return

- Unit: Count
- Description: Number of rows returned after predicate filtering. Labels: `file_format`, `scan_type`.

#### disk_reads_completed

- Unit: Count
- Description: Number of successfully completed disk reads.

#### query_cache_hit_count

- Unit: Count
- Description: Number of query cache hits.

#### jemalloc_resident_bytes

- Unit: Bytes
- Description: Maximum number of bytes in physically resident data pages mapped by the allocator, comprising all pages dedicated to allocator metadata, pages backing active allocations, and unused dirty pages.

#### blocks_open_writing (Deprecated)

#### disk_io_time_weigthed

- Unit: ms
- Description: Weighted time spent on I/Os.

#### update_compaction_task_byte_per_second

- Unit: Bytes/s
- Description: Estimated rate of Primary Key table compactions.

#### blocks_open_reading (Deprecated)

#### tablet_update_max_compaction_score

- Unit: -
- Description: Highest compaction score of tablets in Primary Key tables in the current BE.

#### segment_read

- Unit: Count
- Description: Total number of segment reads.

#### disk_io_time_ms

- Unit: ms
- Description: Time spent on I/Os.

#### load_mem_bytes

- Unit: Bytes
- Description: Memory cost of data loading.

#### delta_column_group_get_non_pk_total

- Unit: Count
- Description: Total number of times to get delta column group (for non-Primary Key tables only).

#### query_scan_bytes_per_second

- Unit: Bytes/s
- Description: Estimated rate of scanned bytes per second.

#### active_scan_context_count

- Unit: Count
- Description: Total number of scan tasks created by Flink/Spark SQL.

#### fd_num_limit

- Unit: Count
- Description: Maximum number of file descriptors.

#### update_compaction_task_cost_time_ns

- Unit: ns
- Description: Total time spent on the Primary Key table compactions.

#### delta_column_group_get_hit_cache

- Unit: Count
- Description: Total number of delta column group cache hits (for Primary Key tables only).

#### data_stream_receiver_count

- Unit: Count
- Description: Cumulative number of instances serving as Exchange receivers in BE.

#### bytes_written_total

- Unit: Bytes
- Description: Total bytes written (sectors write * 512).

#### transaction_streaming_load_bytes

- Unit: Bytes
- Description: Total loading bytes of transaction load.

#### running_cumulative_compaction_task_num

- Unit: Count
- Description: Total number of running cumulative compactions.

#### transaction_streaming_load_requests_total

- Unit: Count
- Description: Total number of transaction load requests.

#### cpu

- Unit: -
- Description: CPU usage information returned by `/proc/stat`.

#### update_del_vector_num

- Unit: Count
- Description: Number of the DELETE vector cache items in Primary Key tables.

#### disks_avail_capacity

- Description: Available capacity of a specific disk.

#### clone_mem_bytes

- Unit: Bytes
- Description: Memory used for replica clone.

#### fragment_requests_total

- Unit: Count
- Description: Total fragment instances executing on a BE (for non-pipeline engine).

#### disk_write_time_ms

- Unit: ms
- Description: Time spent on disk writing. Unit: ms.

#### schema_change_mem_bytes

- Unit: Bytes
- Description: Memory used for Schema Change.

#### thrift_connections_total

- Unit: Count
- Description: Total number of thrift connections (including finished connections).

#### thrift_opened_clients

- Unit: Count
- Description: Number of currently opened thrift clients.

#### thrift_used_clients

- Unit: Count
- Description: Number of thrift clients in use currently.

#### thrift_current_connections (Deprecated)

#### disk_bytes_read

- Unit: Bytes
- Description: Total bytes read from the disk(sectors read * 512).

#### base_compaction_task_cost_time_ms

- Unit: ms
- Description: Total time spent on base compactions.

#### update_primary_index_bytes_total

- Unit: Bytes
- Description: Total memory cost of the Primary Key index.

#### compaction_deltas_total

- Unit: Count
- Description: Total number of merged rowsets from base compactions and cumulative compactions.

#### max_network_receive_bytes_rate

- Unit: Bytes
- Description: Total bytes received over the network (maximum value among all network interfaces).

#### max_network_send_bytes_rate

- Unit: Bytes
- Description: Total bytes sent over the network (maximum value among all network interfaces).

#### chunk_allocator_mem_bytes

- Unit: Bytes
- Description: Memory used by the chunk allocator.

#### query_cache_usage_ratio

- Unit: -
- Description: Current query cache usage ratio.

#### process_fd_num_limit_soft

- Unit: Count
- Description: Soft limit on the maximum number of file descriptors. Please note that this item indicates the soft limit. You can set the hard limit using the `ulimit` command.

#### query_cache_hit_ratio

- Unit: -
- Description: Hit ratio of query cache.

#### tablet_metadata_mem_bytes

- Unit: Bytes
- Description: Memory used by tablet metadata.

#### jemalloc_retained_bytes

- Unit: Bytes
- Description: Total bytes of virtual memory mappings that were retained rather than returned to the operating system via operations like munmap(2).

#### unused_rowsets_count

- Unit: Count
- Description: Total number of unused rowsets. Please note that these rowsets will be reclaimed later.

#### update_rowset_commit_apply_total

- Unit: Count
- Description: Total number of COMMIT and APPLY for Primary Key tables.

#### segment_zonemap_mem_bytes

- Unit: Bytes
- Description: Memory used by segment zonemap.

#### base_compaction_task_byte_per_second

- Unit: Bytes/s
- Description: Estimated rate of base compactions.

#### transaction_streaming_load_duration_ms

- Unit: ms
- Description: Total time spent on Stream Load transaction Interface.

#### memory_pool_bytes_total

- Unit: Bytes
- Description: Memory used by the memory pool.

#### short_key_index_mem_bytes

- Unit: Bytes
- Description: Memory used by the short key index.

#### disk_bytes_written

- Unit: Bytes
- Description: Total bytes written to disk.

#### segment_flush_queue_count

- Unit: Count
- Description: Number of queued tasks in the segment flush thread pool.

#### starrocks_be_segment_file_not_found_total

- Unit: Count
- Description: Total number of times a segment file was not found (file missing) during segment open. A continuously increasing value may indicate data loss or storage inconsistency.

#### jemalloc_metadata_bytes

- Unit: Bytes
- Description: Total number of bytes dedicated to metadata, comprising base allocations used for bootstrap-sensitive allocator metadata structures and internal allocations. The usage of transparent huge pages is not included in this item.

#### network_send_bytes

- Unit: Bytes
- Description: Number of bytes sent over the network.

#### update_mem_bytes

- Unit: Bytes
- Description: Memory used by Primary Key table APPLY tasks and Primary Key index.

#### blocks_created_total (Deprecated)

#### query_cache_usage

- Unit: Bytes
- Description: Current query cache usages.

#### memtable_flush_duration_us

- Unit: us
- Description: Total time spent on memtable flush.

#### push_request_write_bytes

- Unit: Bytes
- Description: Total bytes written via Spark Load.

#### jemalloc_active_bytes

- Unit: Bytes
- Description: Total bytes in active pages allocated by the application.

#### load_rpc_threadpool_size

- Unit: Count
- Description: The current size of the RPC thread pool, which is used for handling Routine Load and loading via table functions. The default value is 10, with a maximum value of 1000. This value is dynamically adjusted based on the usage of the thread pool.

#### publish_version_queue_count

- Unit: Count
- Description: Queued task count in the Publish Version thread pool.

#### chunk_pool_system_free_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

#### chunk_pool_system_free_cost_ns

- Unit: ns
- Description: Memory chunk allocation/cache metrics.

#### chunk_pool_system_alloc_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

#### chunk_pool_system_alloc_cost_ns

- Unit: ns
- Description: Memory chunk allocation/cache metrics.

#### chunk_pool_local_core_alloc_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

#### chunk_pool_other_core_alloc_count

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

#### fragment_endpoint_count

- Unit: Count
- Description: Cumulative number of instances serving as Exchange senders in BE.

#### disk_sync_total (Deprecated)

#### max_disk_io_util_percent

- Unit: -
- Description: Maximum disk I/O utilization percentage.

#### network_receive_bytes

- Unit: Bytes
- Description: Total bytes received via network.

#### storage_page_cache_mem_bytes

- Unit: Bytes
- Description: Memory used by storage page cache.

#### jit_cache_mem_bytes

- Unit: Bytes
- Description: Memory used by jit compiled function cache.

#### column_partial_update_apply_total

- Unit: Count
- Description: Total number of APPLY for partial updates by column (Column mode)

#### disk_writes_completed

- Unit: Count
- Description: Total number of successfully completed disk writes.

#### memtable_flush_total

- Unit: Count
- Description: Total number of memtable flushes.

#### page_cache_hit_count

- Unit: Count
- Description: Total number of hits in the storage page cache.

#### page_cache_insert_count

- Unit: Count
- Description: Total number of insert operations in the storage page cache.

#### page_cache_insert_evict_count

- Unit: Count
- Description: Total number of cache entries evicted during insert operations due to capacity constraints.

#### page_cache_release_evict_count

- Unit: Count
- Description: Total number of cache entries evicted during release operations when cache usage exceeds capacity.

#### bytes_read_total (Deprecated)

#### update_rowset_commit_request_total

- Unit: Count
- Description: Total number of rowset COMMIT requests in Primary Key tables.

#### tablet_cumulative_max_compaction_score

- Unit: -
- Description: Highest cumulative compaction score of tablets in this BE.

#### column_zonemap_index_mem_bytes

- Unit: Bytes
- Description: Memory used by column zonemaps.

#### push_request_write_bytes_per_second

- Unit: Bytes/s
- Description: Data writing rate of Spark Load.

#### process_thread_num

- Unit: Count
- Description: Total number of threads in this process.

#### query_cache_lookup_count

- Unit: Count
- Description: Total number of query cache lookups.

#### http_requests_total (Deprecated)

#### http_request_send_bytes (Deprecated)

#### cumulative_compaction_task_cost_time_ms

- Unit: ms
- Description: Total time spent on cumulative compactions.

#### column_metadata_mem_bytes

- Unit: Bytes
- Description: Memory used by column metadata.

#### plan_fragment_count

- Unit: Count
- Description: Number of currently running query plan fragments.

#### page_cache_lookup_count

- Unit: Count
- Description: Total number of storage page cache lookups.

#### query_mem_bytes

- Unit: Bytes
- Description: Memory used by queries.

#### load_channel_count

- Unit: Count
- Description: Total number of loading channels.

#### push_request_write_rows

- Unit: Count
- Description: Total rows written via Spark Load.

#### running_base_compaction_task_num

- Unit: Count
- Description: Total number of running base compaction tasks.

#### fragment_request_duration_us

- Unit: us
- Description: Cumulative execution time of fragment instances (for non-pipeline engine).

#### process_mem_bytes

- Unit: Bytes
- Description: Memory used by this process.

#### broker_count

- Unit: Count
- Description: Total number of filesystem brokers (by host address) created.

#### segment_replicate_queue_count

- Unit: Count
- Description: Queued task count in the Segment Replicate thread pool.

#### brpc_endpoint_stub_count

- Unit: Count
- Description: Total number of bRPC stubs (by address).

#### readable_blocks_total (Deprecated)

#### delta_column_group_get_non_pk_hit_cache

- Unit: Count
- Description: Total number of hits in the delta column group cache (for non-Primary Key tables).

#### update_del_vector_deletes_new

- Unit: Count
- Description: Total number of newly generated DELETE vectors used in Primary Key tables.

#### compaction_bytes_total

- Unit: Bytes
- Description: Total merged bytes from base compactions and cumulative compactions.

#### segment_metadata_mem_bytes

- Unit: Bytes
- Description: Memory used by segment metadata.

#### column_partial_update_apply_duration_us

- Unit: us
- Description: Total time spent on partial updates for columns' APPLY tasks (Column mode).

#### bloom_filter_index_mem_bytes

- Unit: Bytes
- Description: Memory used by Bloomfilter indexes.

#### routine_load_task_count

- Unit: Count
- Description: Number of currently running Routine Load tasks.

#### delta_column_group_get_total

- Unit: Count
- Description: Total number of times to get delta column groups (for Primary Key tables).

#### disk_read_time_ms

- Unit: ms
- Description: Time spent on reading from disk. Unit: ms.

#### update_compaction_outputs_bytes_total

- Unit: Bytes
- Description: Total bytes written by Primary Key table compactions.

#### memtable_flush_queue_count

- Unit: Count
- Description: Queued task count in the memtable flush thread pool.

#### query_cache_capacity

- Description: Capacity of query cache.

#### streaming_load_duration_ms

- Unit: ms
- Description: Total time spent on Stream Load.

#### streaming_load_current_processing

- Unit: Count
- Description: Number of currently running Stream Load tasks.

#### streaming_load_bytes

- Unit: Bytes
- Description: Total bytes loaded by Stream Load.

#### load_rows

- Unit: Count
- Description: Total loaded rows.

#### load_bytes

- Unit: Bytes
- Description: Total loaded bytes.

#### meta_request_total

- Unit: Count
- Description: Total number of meta read/write requests.

#### meta_request_duration

- Unit: us
- Description: Total meta read/write duration.

#### tablet_base_max_compaction_score

- Unit: -
- Description: Highest base compaction score of tablets in this BE.

#### page_cache_capacity

- Description: Capacity of the storage page cache.

#### cumulative_compaction_task_byte_per_second

- Unit: Bytes/s
- Description: Rate of bytes processed during cumulative compactions.

#### stream_load

- Unit: -
- Description: Total loaded rows and received bytes.

#### stream_load_pipe_count

- Unit: Count
- Description: Number of currently running Stream Load tasks.

#### binary_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the BINARY column pool.

#### int16_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT16 column pool.

#### decimal_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DECIMAL column pool.

#### double_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DOUBLE column pool.

#### int128_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT128 column pool.

#### date_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DATE column pool.

#### int64_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT64 column pool.

#### int8_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT8 column pool.

#### datetime_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the DATETIME column pool.

#### int32_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the INT32 column pool.

#### float_column_pool_bytes

- Unit: Bytes
- Description: Memory used by the FLOAT column pool.

#### uint8_column_pool_bytes

- Unit: Bytes
- Description: Bytes used by the UINT8 column pool.

#### column_pool_mem_bytes

- Unit: Bytes
- Description: Memory used by the column pools.

#### local_column_pool_bytes (Deprecated)

#### total_column_pool_bytes (Deprecated)

#### central_column_pool_bytes (Deprecated)

#### wait_base_compaction_task_num

- Unit: Count
- Description: Number of base compaction tasks waiting for execution.

#### wait_cumulative_compaction_task_num

- Unit: Count
- Description: Number of cumulative compaction tasks waiting for execution.

#### jemalloc_allocated_bytes

- Unit: Bytes
- Description: Total number of bytes allocated by the application.

#### pk_index_compaction_queue_count

- Unit: Count
- Description: Queued task count in the Primary Key index compaction thread pool.

#### pk_index_sst_read_error_total

- Type: Counter
- Unit: Count
- Description: Total number of SST file read failures in the lake Primary Key persistent index. Incremented when SST multi-get (read) operations fail.

#### pk_index_sst_write_error_total

- Type: Counter
- Unit: Count
- Description: Total number of SST file write failures in the lake Primary Key persistent index. Incremented when SST file build fails.

#### disks_total_capacity

- Description: Total capacity of the disk.

#### disks_state

- Unit: -
- Description: State of each disk. `1` indicates that the disk is in use, and `0` indicates that it is not in use.

#### update_del_vector_deletes_total (Deprecated)

#### update_del_vector_dels_num (Deprecated)

#### result_block_queue_count

- Unit: Count
- Description: Number of results in the result block queue.

#### engine_requests_total

- Unit: Count
- Description: Total count of all types of requests between BE and FE, including CREATE TABLE, Publish Version, and tablet clone.

#### snmp

- Unit: -
- Description: Metrics returned by `/proc/net/snmp`.

#### compaction_mem_bytes

- Unit: Bytes
- Description: Memory used by compactions.

#### txn_request

- Unit: -
- Description: Transaction requests of BEGIN, COMMIT, ROLLBACK, and EXEC.

#### small_file_cache_count

- Unit: Count
- Description: Number of small file caches.

#### rowset_metadata_mem_bytes

- Unit: Bytes
- Description: Total number of bytes for rowset metadata.

#### update_apply_queue_count

- Unit: Count
- Description: Queued task count in the Primary Key table transaction APPLY thread pool.

#### async_delta_writer_queue_count

- Unit: Count
- Description: Queued task count in the tablet delta writer thread pool.

#### update_compaction_duration_us

- Unit: us
- Description: Total time spent on Primary Key table compactions.

#### transaction_streaming_load_current_processing

- Unit: Count
- Description: Number of currently running transactional Stream Load tasks.

#### ordinal_index_mem_bytes

- Unit: Bytes
- Description: Memory used by ordinal indexes.

#### consistency_mem_bytes

- Unit: Bytes
- Description: Memory used by replica consistency checks.

#### tablet_schema_mem_bytes

- Unit: Bytes
- Description: Memory used by tablet schema.

#### fd_num_used

- Unit: Count
- Description: Number of file descriptors currently in use.

#### running_update_compaction_task_num

- Unit: Count
- Description: Total number of currently running Primary Key table compaction tasks.

#### rowset_count_generated_and_in_use

- Unit: Count
- Description: Number of rowset IDs currently in use.

#### bitmap_index_mem_bytes

- Unit: Bytes
- Description: Memory used by bitmap indexes.

#### builtin_inverted_index_mem_bytes

- Unit: Bytes
- Description: Memory used by builtin inverted indexes.

#### update_rowset_commit_apply_duration_us

- Unit: us
- Description: Total time spent on Primary Key table APPLY tasks.

#### process_fd_num_used

- Unit: Count
- Description: Number of file descriptors currently in use in this BE process.

#### network_send_packets

- Unit: Count
- Description: Total number of packets sent through the network.

#### network_receive_packets

- Unit: Count
- Description: Total number of packets received through the network.

#### metadata_mem_bytes (Deprecated)

#### push_requests_total

- Unit: Count
- Description: Total number of successful and failed Spark Load requests.

#### blocks_deleted_total (Deprecated)

#### jemalloc_mapped_bytes

- Unit: Bytes
- Description: Total number of bytes in active extents mapped by the allocator.

#### process_fd_num_limit_hard

- Unit: Count
- Description: Hard limit on the maximum number of file descriptors.

#### jemalloc_metadata_thp

- Unit: Count
- Description: Number of Transparent Huge Pages used for metadata.

#### update_rowset_commit_request_failed

- Unit: Count
- Description: Total number of failed rowset COMMIT requests in Primary Key tables.

#### streaming_load_requests_total

- Unit: Count
- Description: Total number of Stream Load requests.

