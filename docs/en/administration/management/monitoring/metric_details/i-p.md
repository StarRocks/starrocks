---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical i - p"
---

# Metrics i through p

:::note

Metrics for materialized views and shared-data clusters are detailed in the corresponding sections:

- [Metrics for asynchronous materialized view metrics](../metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](../metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../Monitor_and_Alert.md).

:::

## `iceberg_compaction_duration_ms_total`

- Unit: Millisecond
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total time spent running Iceberg compaction tasks.

## `iceberg_compaction_input_files_total`

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of data files read by Iceberg compaction tasks.

## `iceberg_compaction_output_files_total`

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of data files produced by Iceberg compaction tasks.

## `iceberg_compaction_removed_delete_files_total`

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of delete files removed by Iceberg manual compaction tasks.

## `iceberg_compaction_total`

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of Iceberg compaction (`rewrite_data_files`) tasks.

## `iceberg_delete_bytes`

- Unit: Bytes
- Type: Cumulative
- Labels: `delete_type` (`position` or `metadata`)
- Description: Total deleted bytes from Iceberg `DELETE` tasks. For `metadata` delete, this represents the size of deleted data files. For `position` delete, this represents the size of position delete files created.

## `iceberg_delete_duration_ms_total`

- Unit: Millisecond
- Type: Cumulative
- Labels: `delete_type` (`position` or `metadata`)
- Description: Total execution time of Iceberg `DELETE` tasks in milliseconds. The duration of each task is added after it ends. `delete_type` distinguishes between two delete methods.

## `iceberg_delete_rows`

- Unit: Rows
- Type: Cumulative
- Labels: `delete_type` (`position` or `metadata`)
- Description: Total deleted rows from Iceberg `DELETE` tasks. For `metadata` delete, this represents the number of rows in deleted data files. For `position` delete, this represents the number of position deletes created.

## `iceberg_delete_total`

- Unit: Count
- Type: Cumulative
- Labels:
  - `status` (`success` or `failed`)
  - `reason` (`none`, `timeout`, `oom`, `access_denied`, `unknown`)
  - `delete_type` (`position` or `metadata`)
- Description: Total number of `DELETE` tasks that target Iceberg tables. The metric is incremented by 1 after each task ends, regardless of success or failure. `delete_type` distinguishes between two delete methods: `position` (generates position delete files) and `metadata` (metadata-level delete).

## `iceberg_metadata_table_query_total`

- Unit: Count
- Type: Cumulative
- Labels: `metadata_table` (`refs`, `history`, `metadata_log_entries`, `snapshots`, `manifests`, `files`, `partitions`, or `properties`)
- Description: Total number of SQL queries that access Iceberg metadata tables. Each query is counted under the `metadata_table` label that identifies the metadata table being accessed.

## `iceberg_time_travel_query_total`

- Unit: Count
- Type: Cumulative
- Labels: `time_travel_type` (`branch`, `tag`, `snapshot`, or `timestamp`) for the categorized series.
- Description: Total number of Iceberg time travel queries. The unlabeled series counts each time travel query once. The labeled series count each distinct time travel type used by the query. `snapshot` means `FOR VERSION AS OF <snapshot_id>`, `branch` and `tag` mean `FOR VERSION AS OF <reference_name>`, and `timestamp` means `FOR TIMESTAMP AS OF ...`.

## `iceberg_write_bytes`

- Unit: Bytes
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total written bytes from Iceberg write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`). This represents the total size of data files written to the Iceberg table. `write_type` distinguishes between the operation types.

## `iceberg_write_duration_ms_total`

- Unit: Millisecond
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total execution time of Iceberg write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`) in milliseconds. The duration of each task is added after it ends. `write_type` distinguishes between the operation types.

## `iceberg_write_files`

- Unit: Count
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total number of data files written to Iceberg from write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`). This represents the count of data files written to the Iceberg table. `write_type` distinguishes between the operation types.

## `iceberg_write_rows`

- Unit: Rows
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total written rows from Iceberg write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`). This represents the number of rows written to the Iceberg table. `write_type` distinguishes between the operation types.

## `iceberg_write_total`

- Unit: Count
- Type: Cumulative
- Labels:
  - `status` (`success` or `failed`)
  - `reason` (`none`, `timeout`, `oom`, `access_denied`, `unknown`)
  - `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total number of `INSERT`, `INSERT OVERWRITE`, or `CTAS` tasks that target Iceberg tables. The metric is incremented by 1 after each task ends, regardless of success or failure. `write_type` distinguishes between the operation types.

## `int128_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the INT128 column pool.

## `int16_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the INT16 column pool.

## `int32_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the INT32 column pool.

## `int64_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the INT64 column pool.

## `int8_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the INT8 column pool.

## `jemalloc_active_bytes`

- Unit: Bytes
- Description: Total bytes in active pages allocated by the application.

## `jemalloc_allocated_bytes`

- Unit: Bytes
- Description: Total number of bytes allocated by the application.

## `jemalloc_mapped_bytes`

- Unit: Bytes
- Description: Total number of bytes in active extents mapped by the allocator.

## `jemalloc_metadata_bytes`

- Unit: Bytes
- Description: Total number of bytes dedicated to metadata, comprising base allocations used for bootstrap-sensitive allocator metadata structures and internal allocations. The usage of transparent huge pages is not included in this item.

## `jemalloc_metadata_thp`

- Unit: Count
- Description: Number of Transparent Huge Pages used for metadata.

## `jemalloc_resident_bytes`

- Unit: Bytes
- Description: Maximum number of bytes in physically resident data pages mapped by the allocator, comprising all pages dedicated to allocator metadata, pages backing active allocations, and unused dirty pages.

## `jemalloc_retained_bytes`

- Unit: Bytes
- Description: Total bytes of virtual memory mappings that were retained rather than returned to the operating system via operations like munmap(2).

## `jit_cache_mem_bytes`

- Unit: Bytes
- Description: Memory used by jit compiled function cache.

## `load_bytes`

- Unit: Bytes
- Description: Total loaded bytes.

## `load_channel_count`

- Unit: Count
- Description: Total number of loading channels.

## `load_mem_bytes`

- Unit: Bytes
- Description: Memory cost of data loading.

## `load_rows`

- Unit: Count
- Description: Total loaded rows.

## `load_rpc_threadpool_size`

- Unit: Count
- Description: The current size of the RPC thread pool, which is used for handling Routine Load and loading via table functions. The default value is 10, with a maximum value of 1000. This value is dynamically adjusted based on the usage of the thread pool.

## `local_column_pool_bytes (Deprecated)`

## `max_disk_io_util_percent`

- Unit: -
- Description: Maximum disk I/O utilization percentage.

## `max_network_receive_bytes_rate`

- Unit: Bytes
- Description: Total bytes received over the network (maximum value among all network interfaces).

## `max_network_send_bytes_rate`

- Unit: Bytes
- Description: Total bytes sent over the network (maximum value among all network interfaces).

## `memory_pool_bytes_total`

- Unit: Bytes
- Description: Memory used by the memory pool.

## `memtable_flush_duration_us`

- Unit: us
- Description: Total time spent on memtable flush.

## `memtable_flush_queue_count`

- Unit: Count
- Description: Queued task count in the memtable flush thread pool.

## `memtable_flush_total`

- Unit: Count
- Description: Total number of memtable flushes.

## `merge_commit_append_pipe`

- Unit: microsecond
- Type: Summary
- Description: Time spent appending data to the stream load pipe during merge commit.

## `merge_commit_fail_total`

- Unit: Count
- Type: Cumulative
- Description: Merge commit requests that failed.

## `merge_commit_pending`

- Unit: microsecond
- Type: Summary
- Description: Time merge commit tasks spend waiting in the pending queue before execution.

## `merge_commit_pending_bytes`

- Unit: Bytes
- Type: Instantaneous
- Description: Total bytes of data held by pending merge commit tasks.

## `merge_commit_pending_total`

- Unit: Count
- Type: Instantaneous
- Description: Merge commit tasks currently waiting in the execution queue.

## `merge_commit_register_pipe_total`

- Unit: Count
- Type: Cumulative
- Description: Stream load pipes registered for merge commit operations.

## `merge_commit_request`

- Unit: microsecond
- Type: Summary
- Description: End-to-end processing latency for merge commit requests.

## `merge_commit_request_bytes`

- Unit: Bytes
- Type: Cumulative
- Description: Total bytes of data received across merge commit requests.

## `merge_commit_request_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of merge commit requests received by BE.

## `merge_commit_send_rpc_total`

- Unit: Count
- Type: Cumulative
- Description: RPC requests sent to FE for starting merge commit operations.

## `merge_commit_success_total`

- Unit: Count
- Type: Cumulative
- Description: Merge commit requests that finished successfully.

## `merge_commit_unregister_pipe_total`

- Unit: Count
- Type: Cumulative
- Description: Stream load pipes unregistered from merge commit operations.

Latency metrics expose percentile series such as `merge_commit_request_latency_99` and `merge_commit_request_latency_90`, reported in microseconds. The end-to-end latency obeys:

`merge_commit_request = merge_commit_pending + merge_commit_wait_plan + merge_commit_append_pipe + merge_commit_wait_finish`

> **Note**: Before v3.4.11, v3.5.12, and v4.0.4, these latency metrics were reported in nanoseconds.

## `merge_commit_wait_finish`

- Unit: microsecond
- Type: Summary
- Description: Time spent waiting for merge commit load operations to finish.

## `merge_commit_wait_plan`

- Unit: microsecond
- Type: Summary
- Description: Combined latency for the RPC request and waiting for the stream load pipe to become available.

## `meta_request_duration`

- Unit: us
- Description: Total meta read/write duration.

## `meta_request_total`

- Unit: Count
- Description: Total number of meta read/write requests.

## `metadata_mem_bytes (Deprecated)`

## `network_receive_bytes`

- Unit: Bytes
- Description: Total bytes received via network.

## `network_receive_packets`

- Unit: Count
- Description: Total number of packets received through the network.

## `network_send_bytes`

- Unit: Bytes
- Description: Number of bytes sent over the network.

## `network_send_packets`

- Unit: Count
- Description: Total number of packets sent through the network.

## `ordinal_index_mem_bytes`

- Unit: Bytes
- Description: Memory used by ordinal indexes.

## `page_cache_capacity`

- Description: Capacity of the storage page cache.

## `page_cache_hit_count`

- Unit: Count
- Description: Total number of hits in the storage page cache.

## `page_cache_insert_count`

- Unit: Count
- Description: Total number of insert operations in the storage page cache.

## `page_cache_insert_evict_count`

- Unit: Count
- Description: Total number of cache entries evicted during insert operations due to capacity constraints.

## `page_cache_lookup_count`

- Unit: Count
- Description: Total number of storage page cache lookups.

## `page_cache_release_evict_count`

- Unit: Count
- Description: Total number of cache entries evicted during release operations when cache usage exceeds capacity.

## `pip_query_ctx_cnt`

- Unit: Count
- Description: Total number of currently running queries in the BE.

## `pipe_driver_execution_time`

- Description: Cumulative time spent by PipelineDriver executors on processing PipelineDrivers.

## `pipe_driver_queue_len`

- Unit: Count
- Description: Current number of ready drivers in the ready queue waiting for scheduling in BE.

## `pipe_driver_schedule_count`

- Unit: Count
- Description: Cumulative number of driver scheduling times for pipeline executors in the BE.

## `pipe_poller_block_queue_len`

- Unit: Count
- Description: Current length of the block queue of PipelineDriverPoller in the pipeline engine.

## `pipe_prepare_pool_queue_len`

- Unit: Count
- Description: Queued task count in the pipeline PREPARE thread pool. This is an instantaneous value.

## `pipe_scan_executor_queuing`

- Unit: Count
- Description: Current number of pending asynchronous I/O tasks launched by Scan Operators.

## `pk_index_compaction_queue_count`

- Unit: Count
- Description: Queued task count in the Primary Key index compaction thread pool.

## `pk_index_sst_read_error_total`

- Type: Counter
- Unit: Count
- Description: Total number of SST file read failures in the lake Primary Key persistent index. Incremented when SST multi-get (read) operations fail.

## `pk_index_sst_write_error_total`

- Type: Counter
- Unit: Count
- Description: Total number of SST file write failures in the lake Primary Key persistent index. Incremented when SST file build fails.

## `plan_fragment_count`

- Unit: Count
- Description: Number of currently running query plan fragments.

## `process_fd_num_limit_hard`

- Unit: Count
- Description: Hard limit on the maximum number of file descriptors.

## `process_fd_num_limit_soft`

- Unit: Count
- Description: Soft limit on the maximum number of file descriptors. Please note that this item indicates the soft limit. You can set the hard limit using the `ulimit` command.

## `process_fd_num_used`

- Unit: Count
- Description: Number of file descriptors currently in use in this BE process.

## `process_mem_bytes`

- Unit: Bytes
- Description: Memory used by this process.

## `process_thread_num`

- Unit: Count
- Description: Total number of threads in this process.

## `publish_version_queue_count`

- Unit: Count
- Description: Queued task count in the Publish Version thread pool.

## `push_request_duration_us`

- Unit: us
- Description: Total time spent on Spark Load.

## `push_request_write_bytes`

- Unit: Bytes
- Description: Total bytes written via Spark Load.

## `push_request_write_bytes_per_second`

- Unit: Bytes/s
- Description: Data writing rate of Spark Load.

## `push_request_write_rows`

- Unit: Count
- Description: Total rows written via Spark Load.

## `push_requests_total`

- Unit: Count
- Description: Total number of successful and failed Spark Load requests.

