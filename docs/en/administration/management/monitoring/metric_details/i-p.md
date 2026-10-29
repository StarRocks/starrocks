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

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../monitoring.md).

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

## `jemalloc_dirty_bytes`

- Unit: Bytes
- Description: Total number of bytes in unused dirty pages, which have not yet been `madvise`d back to the operating system and can be reused for new allocations without a page fault.

## `jemalloc_mapped_bytes`

- Unit: Bytes
- Description: Total number of bytes in active extents mapped by the allocator.

## `jemalloc_metadata_bytes`

- Unit: Bytes
- Description: Total number of bytes dedicated to metadata, comprising base allocations used for bootstrap-sensitive allocator metadata structures and internal allocations. The usage of transparent huge pages is not included in this item.

## `jemalloc_metadata_thp`

- Unit: Count
- Description: Number of Transparent Huge Pages used for metadata.

## `jemalloc_muzzy_bytes`

- Unit: Bytes
- Description: Total number of bytes in unused muzzy pages, an intermediate decay state between dirty and retained where the pages have been `madvise`d (for example with `MADV_FREE`) but the mapping is still retained.

## `jemalloc_resident_bytes`

- Unit: Bytes
- Description: Maximum number of bytes in physically resident data pages mapped by the allocator, comprising all pages dedicated to allocator metadata, pages backing active allocations, and unused dirty pages.

## `jemalloc_retained_bytes`

- Unit: Bytes
- Description: Total bytes of virtual memory mappings that were retained rather than returned to the operating system via operations like munmap(2).

## `jit_cache_mem_bytes`

- Unit: Bytes
- Description: Memory used by jit compiled function cache.

<<<<<<< HEAD
=======
## `jvm_heap_size_bytes`

- Unit: Bytes
- Type: Instantaneous
- Labels: `type` (`used`, `committed`, or `max`)
- Description: Heap memory of the JVM embedded in the BE process. `used` is the memory occupied by objects, `committed` is the memory the JVM has reserved from the operating system, and `max` is the maximum heap size. FE nodes export metrics with the same name for their own JVM; this entry describes the BE metric.
  - The BE JVM metrics (all `jvm_*_size_bytes` metrics in this section) are exported only when the BE configuration item `enable_jvm_metrics` is `true` (default) and the environment variable `JAVA_HOME` is set when BE starts.
  - The BE does not start a JVM to collect these metrics. All values stay `0` until a JVM is created in the BE process, for example by HDFS access, a JNI-based scanner, or a Java UDF.
  - The values are refreshed every 15 seconds when the BE configuration item `enable_metric_calculator` is `true` (default), and when the metrics are scraped otherwise.
  - The committed heap and non-heap memory is also counted in the `jvm` memory tracker of the BE when `enable_jvm_memory_tracker` is `true` (default).

## `jvm_non_heap_size_bytes`

- Unit: Bytes
- Type: Instantaneous
- Labels: `type` (`used` or `committed`)
- Description: Non-heap memory of the JVM embedded in the BE process, such as Metaspace and the code cache. No `max` value is exported. See [`jvm_heap_size_bytes`](#jvm_heap_size_bytes) for when the BE JVM metrics are exported.

## `jvm_old_size_bytes`

- Unit: Bytes
- Type: Instantaneous
- Labels: `type` (`used`, `committed`, `max`, `peak_used`, or `peak_max`)
- Description: Old generation memory pool of the JVM embedded in the BE process. See [`jvm_young_size_bytes`](#jvm_young_size_bytes) for the meaning of each `type` and the supported garbage collectors.

## `jvm_perm_size_bytes`

- Unit: Bytes
- Type: Instantaneous
- Labels: `type` (`used`, `committed`, `max`, `peak_used`, or `peak_max`)
- Description: Metaspace (permanent generation on Java 7 and earlier) of the JVM embedded in the BE process. Metaspace has no defined maximum unless the JVM option `-XX:MaxMetaspaceSize` is set, so `max` and `peak_max` are usually `-1`. See [`jvm_young_size_bytes`](#jvm_young_size_bytes) for the meaning of each `type`.

## `jvm_survivor_size_bytes`

- Unit: Bytes
- Type: Instantaneous
- Labels: `type` (`used`, `committed`, `max`, `peak_used`, or `peak_max`)
- Description: Survivor space memory pool of the JVM embedded in the BE process. With the default G1 collector, `max` and `peak_max` are `-1`. See [`jvm_young_size_bytes`](#jvm_young_size_bytes) for the meaning of each `type` and the supported garbage collectors.

## `jvm_young_size_bytes`

- Unit: Bytes
- Type: Instantaneous
- Labels: `type` (`used`, `committed`, `max`, `peak_used`, or `peak_max`)
- Description: Young generation (Eden space) memory pool of the JVM embedded in the BE process. With the default G1 collector, `max` and `peak_max` are `-1`. See [`jvm_heap_size_bytes`](#jvm_heap_size_bytes) for when the BE JVM metrics are exported.
  - `used`, `committed`, and `max` are the current usage of the pool. `peak_used` and `peak_max` come from the peak usage that the JVM records for the pool (`MemoryPoolMXBean.getPeakUsage()`): `peak_used` is the highest `used` value, and `peak_max` is the `max` field of that peak usage record. `peak_max` is not a separately tracked high-water mark of `max`, so use `max`, not `peak_max`, to monitor the capacity of the pool. The peak usage is recorded since the JVM started or since it was last reset. The BE never resets it, so it is reset only when the BE restarts.
  - A `max` or `peak_max` of `-1` means that the pool has no defined maximum size. It does not indicate an error.
  - `jvm_young_size_bytes`, `jvm_survivor_size_bytes`, and `jvm_old_size_bytes` are populated only when the JVM uses the Serial, Parallel, CMS, or G1 garbage collector, because they are matched by the memory pool names of these collectors. Under ZGC or Shenandoah, they stay `0`. `jvm_heap_size_bytes` and `jvm_non_heap_size_bytes` are not affected.

## `lake_compaction_held_segment_bytes`

- Unit: Bytes
- Type: Instantaneous
- Description: Segment metadata currently pinned by running lake compaction tasks that hold their input segments (`lake_compaction_hold_input_segments`). Unlike the metadata cache, this memory is not managed by an LRU and is released when the holding task ends, so a persistently high value indicates long-running compactions rather than a cache that needs resizing.

>>>>>>> b7ff6bd ([Doc] Document BE JVM memory metrics (#81845))
## `lake_compaction_failed`

- Unit: Count
- Description: Counter of failed lake compaction jobs.

## `lake_compaction_partial_success`

- Unit: Count
- Description: Counter of partially successful lake compaction jobs.

## `lake_compaction_running`

- Unit: Count
- Description: Number of currently running lake compaction jobs.

## `lake_compaction_running_tasks`

- Unit: Count
- Description: Number of tablets currently being compacted across all running shared-data (lake) compaction jobs. This is the same unit the scheduler caps with the `lake_compaction_max_tasks` config, and is finer-grained than `lake_compaction_running`, which counts compaction jobs (one per partition) — a single job fans out into one tablet-level task per tablet. Carries an `is_leader` label; follower FEs export the metric with `is_leader="false"` and value 0, so dashboards should filter on `is_leader="true"`.

## `lake_compaction_score_at_trigger`

- Unit: Score
- Type: Gauge
- Description: Compaction score of the most recent partition that triggered a lake compaction job, rounded to the nearest integer. The value is the partition's *max* tablet-level score (`Quantiles.getMax()`), matching the criterion the scheduler uses to pick partitions for compaction. Updated once per partition per trigger; the gauge holds the value of the most recent update. This gauge does not decay: on the leader FE, when no compactions are running it retains the last trigger's value (it is not reset to 0). The value is process-local (an in-memory counter on the leader, not persisted), so after an FE leader failover the newly promoted leader starts at 0 and reports 0 until its first compaction trigger — it does not inherit the previous leader's value. Alert on it together with `lake_compaction_running > 0` rather than reading it in isolation. Carries an `is_leader` label; follower FEs export `is_leader="false"` and return 0, so dashboards should filter on `is_leader="true"`.

## `lake_compaction_success`

- Unit: Count
- Description: Counter of successful lake compaction jobs.

## `lake_vacuum_del_file_batch_size_minute`

- Unit: Count (files per batch)
- Type: Gauge
- Description: Recent (60s) mean of files per `DeleteObjects` batch sent by Vacuum on shared-data clusters.

## `lake_vacuum_del_file_retries_minute`

- Unit: Count
- Type: Gauge
- Description: Number of Vacuum delete retries triggered in the last 60s on shared-data clusters. Surfaces transient object-storage throttling (SlowDown / try-again).

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

## `load_sink_close_active_threads`

- Unit: Count
- Type: Instantaneous
- Description: The number of load sink closes that are running in the load sink close thread pool. In a shared-data cluster, each close writes the combined transaction log of a load to object storage.

## `load_sink_close_execute_time_ns_total`

- Unit: Nanoseconds
- Type: Cumulative
- Description: The total execution time of completed load sink close tasks, including waits and object-storage writes performed during close. Updated when each task finishes.

## `load_sink_close_executed_tasks_total`

- Unit: Count
- Type: Cumulative
- Description: The number of load sink close tasks completed by the thread pool, including closes that return an error.

## `load_sink_close_pending_time_ns_total`

- Unit: Nanoseconds
- Type: Cumulative
- Description: The total time completed load sink close tasks spent waiting in the thread pool queue. Updated when each task finishes; currently queued tasks are not yet included.

## `load_sink_close_queue_count`

- Unit: Count
- Type: Instantaneous
- Description: The number of load sink closes waiting in the queue of the load sink close thread pool. A value that keeps growing means object-storage writes are slower than loads finish.

## `load_sink_close_running_threads`

- Unit: Count
- Type: Instantaneous
- Description: The number of threads in the load sink close thread pool.

## `load_sink_close_threadpool_size`

- Unit: Count
- Type: Instantaneous
- Description: The maximum number of threads in the load sink close thread pool, set by `load_sink_close_thread_pool_num_max`.

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

## `meta_replay_lag_second`

- Unit: Seconds
- Type: Gauge
- Description: How far the metadata replayed by this FE lags behind the Leader's clock. The Leader FE writes a timestamp into the journal every 10 seconds, and this metric is the age of the most recent timestamp that this node has replayed. Unlike `max_journal_replay_lag`, which only the Leader reports, this metric is reported by the lagging node itself, and it keeps growing while a single journal entry is stuck in replay. The Leader FE always reports `0`, because it writes the timestamps instead of replaying them. Once this value exceeds `meta_delay_toleration_second`, the node stops serving reads from its own metadata and forwards its queries to the Leader. Two cases are exempt: reads continue while `ignore_meta_check` is `true`, and a node that has replayed nothing since the previous check keeps whatever read availability it already had, unless it has also lost contact with the Leader, because falling behind a Leader that is writing nothing says nothing about this node. This second case only stops a node from being taken out of service; it never returns one that has already stopped serving.

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

## `pipe_drivers`

- Unit: Count
- Description: Current logical driver capacity reserved by BE admission control. Includes reservations held while a fragment instantiates drivers and operators, as well as reservations retained after instantiation until they are released. The value follows logical DOP accounting, including colocate execution groups, and is not a count of instantiated driver objects or running threads. Releasing a reservation, including on preparation failure or fragment completion, decreases the value.

## `pipe_poller_block_queue_len`

- Unit: Count
- Description: Current length of the block queue of PipelineDriverPoller in the pipeline engine.

## `pipe_poller_max_driver_check`

- Unit: microsecond
- Type: Summary
- Description: Time taken by the slowest single driver check in one pass of PipelineDriverPoller. A large value means that checking one blocked driver held up the whole pass.

## `pipe_poller_pass`

- Unit: microsecond
- Type: Summary
- Description: Time taken by one pass of PipelineDriverPoller over its blocked drivers. Each executor set has a single poller thread, so a long pass delays every driver that waits on it. The maximum in the recent window is exposed as `pipe_poller_pass_max_latency`.

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
- Description: Total number of SST file read failures in the lake Primary Key persistent index. Incremented when SST multi-get (read) operations fail, or when compaction detects data corruption while reading input SST files.

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

This metric includes local pass-through buffers tracked by `passthrough_mem_bytes`. The pass-through metric is a component of the process total, not additional memory; do not add the two metrics when calculating total usage. This accounting metric is not container RSS.

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
