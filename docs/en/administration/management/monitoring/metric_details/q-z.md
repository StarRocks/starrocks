---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical q - z"
---

# Metrics q through z

:::note

Metrics for materialized views and shared-data clusters are detailed in the corresponding sections:

- [Metrics for asynchronous materialized view metrics](../metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](../metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../Monitor_and_Alert.md).

:::

## `query_cache_capacity`

- Description: Capacity of query cache.

## `query_cache_hit_count`

- Unit: Count
- Description: Number of query cache hits.

## `query_cache_hit_ratio`

- Unit: -
- Description: Hit ratio of query cache.

## `query_cache_lookup_count`

- Unit: Count
- Description: Total number of query cache lookups.

## `query_cache_usage`

- Unit: Bytes
- Description: Current query cache usages.

## `query_cache_usage_ratio`

- Unit: -
- Description: Current query cache usage ratio.

## `query_mem_bytes`

- Unit: Bytes
- Description: Memory used by queries.

## `query_scan_bytes`

- Unit: Bytes
- Description: Total number of scanned bytes.

## `query_scan_bytes_per_second`

- Unit: Bytes/s
- Description: Estimated rate of scanned bytes per second.

## `query_scan_rows`

- Unit: Count
- Description: Total number of scanned rows.

## `query_spill_trigger_total`

- Unit: Count
- Labels: `storage_type`
- Description: Number of spillable operator instances that triggered at least one spill, broken down by storage backend (`local`, `remote`). Incremented once per operator instance at the first flush callback.

## `query_spill_bytes_write_total`

- Unit: Bytes
- Labels: `storage_type`
- Description: Cumulative payload bytes written by spillable operators to spill storage, broken down by storage backend (`local`, `remote`).

## `query_spill_bytes_read_total`

- Unit: Bytes
- Labels: `storage_type`
- Description: Cumulative payload bytes read back from spill storage during restore, broken down by storage backend.

## `query_spill_blocks_write_total`

- Unit: Count
- Labels: `storage_type`
- Description: Number of spill blocks allocated for writing, broken down by storage backend. Useful for estimating IO count scale on the write path.

## `query_spill_blocks_read_total`

- Unit: Count
- Labels: `storage_type`
- Description: Number of spill blocks opened for reading, broken down by storage backend. Useful for estimating IO count scale on the read path.

## `query_spill_write_io_duration_ns_total`

- Unit: Nanoseconds
- Labels: `storage_type`
- Description: Cumulative wall-clock time spent in write-side spill IO (block append and flush), broken down by storage backend. Useful for tracking write-side spill performance.

## `query_spill_read_io_duration_ns_total`

- Unit: Nanoseconds
- Labels: `storage_type`
- Description: Cumulative wall-clock time spent in read-side spill IO (block reads during restore), broken down by storage backend. Useful for tracking read-side spill performance.

## `readable_blocks_total (Deprecated)`

## `resource_group_bigquery_count`

- Unit: Count
- Description: Number of queries in each resource group that have triggered the limit for big queries. This is an instantaneous value.

## `resource_group_concurrency_overflow_count`

- Unit: Count
- Description: Number of queries in each resource group that have triggered the concurrency limit. This is an instantaneous value.

## `resource_group_connector_scan_use_ratio (Deprecated)`

- Unit: -
- Description: Ratio of external table scan thread time slices used by each resource group to the total used by all resource groups. This is an average value over the time interval between two metric retrievals.

## `resource_group_cpu_limit_ratio`

- Unit: -
- Description: Ratio of the CPU core limit for each resource group to the total CPU core limit for all resource groups. This is an instantaneous value.

## `resource_group_cpu_use_ratio (Deprecated)`

- Unit: -
- Description: Ratio of pipeline thread time slices used by each resource group to the total used by all resource groups. This is an average value over the time interval between two metric retrievals.

## `resource_group_inuse_cpu_cores`

- Unit: Count
- Description: Estimated number of CPU cores currently in use by each resource group. This is an average value over the time interval between two metric retrievals.

## `resource_group_mem_inuse_bytes`

- Unit: Bytes
- Description: Currently used memory by each resource group, measured in bytes. This is an instantaneous value.

## `resource_group_mem_limit_bytes`

- Unit: Bytes
- Description: Memory limit for each resource group, measured in bytes. This is an instantaneous value.

## `resource_group_running_queries`

- Unit: Count
- Description: Number of queries currently running in each resource group. This is an instantaneous value.

## `resource_group_scan_use_ratio (Deprecated)`

- Unit: -
- Description: Ratio of internal table scan thread time slices used by each resource group to the total used by all resource groups. This is an average value over the time interval between two metric retrievals.

## `resource_group_total_queries`

- Unit: Count
- Description: Total number of queries executed in each resource group, including those currently running. This is an instantaneous value.

## `result_block_queue_count`

- Unit: Count
- Description: Number of results in the result block queue.

## `result_buffer_block_count`

- Unit: Count
- Description: Number of blocks in the result buffer.

## `routine_load_task_count`

- Unit: Count
- Description: Number of currently running Routine Load tasks.

## `rowset_count_generated_and_in_use`

- Unit: Count
- Description: Number of rowset IDs currently in use.

## `rowset_metadata_mem_bytes`

- Unit: Bytes
- Description: Total number of bytes for rowset metadata.

## `running_base_compaction_task_num`

- Unit: Count
- Description: Total number of running base compaction tasks.

## `running_cumulative_compaction_task_num`

- Unit: Count
- Description: Total number of running cumulative compactions.

## `running_update_compaction_task_num`

- Unit: Count
- Description: Total number of currently running Primary Key table compaction tasks.

## `schema_change_mem_bytes`

- Unit: Bytes
- Description: Memory used for Schema Change.

## `segment_flush_queue_count`

- Unit: Count
- Description: Number of queued tasks in the segment flush thread pool.

## `segment_metadata_mem_bytes`

- Unit: Bytes
- Description: Memory used by segment metadata.

## `segment_read`

- Unit: Count
- Description: Total number of segment reads.

## `segment_replicate_queue_count`

- Unit: Count
- Description: Queued task count in the Segment Replicate thread pool.

## `segment_zonemap_mem_bytes`

- Unit: Bytes
- Description: Memory used by segment zonemap.

## `short_key_index_mem_bytes`

- Unit: Bytes
- Description: Memory used by the short key index.

## `small_file_cache_count`

- Unit: Count
- Description: Number of small file caches.

## `spill_disk_bytes_used`

- Unit: Bytes
- Labels: `storage_type`
- Description: Current disk bytes reserved across all spill storage directories. The `storage_type=local` variant aggregates the live reserved bytes across every directory managed by the BE's spill `DirManager`. The `storage_type=remote` variant is reported for completeness and is currently always 0 because remote spill storage is tracked per-query rather than globally.

## `snmp`

- Unit: -
- Description: Metrics returned by `/proc/net/snmp`.

## `starrocks_be_clone_task_copy_bytes`

- Unit: Bytes
- Type: Cumulative
- Description: The total file size copied by Clone tasks in the BE node, including both INTER_NODE and INTRA_NODE types.

## `starrocks_be_clone_task_copy_duration_ms`

- Unit: ms
- Type: Cumulative
- Description: The total time for copy consumed by Clone tasks in the BE node, including both INTER_NODE and INTRA_NODE types.

## `starrocks_be_exec_state_report_active_threads`

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks being executed in the thread pool that reports the execution status of the Fragment instance.

## `starrocks_be_exec_state_report_queue_count`

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks queued in the thread pool that reports the execution status of the Fragment instance, up to a maximum of 1000.

## `starrocks_be_exec_state_report_running_threads`

- Unit: Count
- Type: Instantaneous
- Description: The number of threads in the thread pool that reports the execution status of the Fragment instance, with a minimum of 1 and a maximum of 2.

## `starrocks_be_exec_state_report_threadpool_size`

- Unit: Count
- Type: Instantaneous
- Description: The maximum number of threads in the thread pool that reports the execution status of the Fragment instance, defaults to 2.

## `starrocks_be_files_scan_num_bytes_read`

- Unit: Bytes
- Description: Total bytes read from external storage. Labels: `file_format`, `scan_type`.

## `starrocks_be_files_scan_num_files_read`

- Unit: Count
- Description: Number of files read from external storage (CSV, Parquet, ORC, JSON, Avro). Labels: `file_format`, `scan_type`.

## `starrocks_be_files_scan_num_raw_rows_read`

- Unit: Count
- Description: Total raw rows read from external storage before format validation and predicate filtering. Labels: `file_format`, `scan_type`.

## `starrocks_be_files_scan_num_rows_return`

- Unit: Count
- Description: Number of rows returned after predicate filtering. Labels: `file_format`, `scan_type`.

## `starrocks_be_files_scan_num_valid_rows_read`

- Unit: Count
- Description: Number of valid rows read (excluding rows with invalid format). Labels: `file_format`, `scan_type`.

## `starrocks_be_flat_json_access_hit_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of flat JSON sub-column access hits observed during scan. Aggregated from the per-scan `flat_json_hits` and `merge_json_hits` statistics.

## `starrocks_be_flat_json_access_miss_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of flat JSON sub-column access misses observed during scan. Aggregated from the per-scan `dynamic_json_hits` statistics (paths not materialized as flat columns).

## `starrocks_be_flat_json_cast_duration_ns_total`

- Unit: Nanosecond
- Type: Cumulative
- Description: Total time spent casting flat JSON sub-column values during scan.

## `starrocks_be_flat_json_compaction_schema_change_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of times `HyperJsonTransformer` is re-initialized for compaction input with a different flat JSON schema than the previous input. A high rate indicates schema churn across segments being compacted.

## `starrocks_be_flat_json_compaction_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of compaction invocations that flatten JSON columns via `FlatJsonColumnCompactor`.

## `starrocks_be_flat_json_flatten_duration_ns_total`

- Unit: Nanosecond
- Type: Cumulative
- Description: Total time spent flattening JSON values during scan.

## `starrocks_be_flat_json_merge_duration_ns_total`

- Unit: Nanosecond
- Type: Cumulative
- Description: Total time spent merging flat JSON sub-columns back to full JSON during scan.

## `starrocks_be_flat_json_paths_discovered_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of JSON paths discovered by `JsonPathDeriver` during flat JSON segment writes.

## `starrocks_be_flat_json_paths_extracted_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of JSON paths materialized as sub-columns by `FlatJsonColumnWriter` (includes the synthetic null/remain columns).

## `starrocks_be_flat_json_segment_write_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of segment writes that invoke flat JSON column extraction.

## `starrocks_be_flat_json_write_rows_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of rows appended to `FlatJsonColumnWriter` (counted at `append()`, before actual flattening).

## `starrocks_be_mem_pool_mem_limit_bytes`

- Unit: Bytes
- Type: Instantaneous
- Description: Memory limit for each memory pool, measured in bytes.

## `starrocks_be_mem_pool_mem_usage_bytes`

- Unit: Bytes
- Type: Instantaneous
- Description: Currently used total memory by each memory pool, measured in bytes.

## `starrocks_be_mem_pool_mem_usage_ratio`

- Unit: -
- Type: Instantaneous
- Description: Ratio of the memory usage of the memory pool to the memory limit of the memory pool.

## `starrocks_be_mem_pool_workgroup_count`

- Unit: Count
- Type: Instantaneous
- Description: Number of resource groups assigned to each memory pool.

## `starrocks_be_pipe_prepare_pool_queue_len`

- Unit: Count
- Type: Instantaneous
- Description: Instantaneous value of pipeline prepare thread pool task queue length.

## `starrocks_be_priority_exec_state_report_active_threads`

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks being executed in the thread pool that reports the final execution state of the Fragment instance.

## `starrocks_be_priority_exec_state_report_queue_count`

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks queued in the thread pool that reports the final execution status of the Fragment instance, up to a maximum of 2147483647.

## `starrocks_be_priority_exec_state_report_running_threads`

- Unit: Count
- Type: Instantaneous
- Description: The number of threads in the thread pool that reports the final execution status of the Fragment instance, with a minimum of 1 and a maximum of 2.

## `starrocks_be_priority_exec_state_report_threadpool_size`

- Unit: Count
- Type: Instantaneous
- Description: The maximum number of threads in the thread pool that reports the final execution status of the Fragment instance, defaults to 2.

## `starrocks_be_resource_group_cpu_limit_ratio`

- Unit: -
- Type: Instantaneous
- Description: Instantaneous value of resource group cpu quota ratio.

## `starrocks_be_resource_group_cpu_use_ratio`

- Unit: -
- Type: Average
- Description: The ratio of CPU time used by the resource group to the CPU time of all resource groups.

## `starrocks_be_resource_group_mem_inuse_bytes`

- Unit: Bytes
- Type: Instantaneous
- Description: Instantaneous value of resource group memory usage.

## `starrocks_be_resource_group_mem_limit_bytes`

- Unit: Bytes
- Type: Instantaneous
- Description: Instantaneous value of resource group memory quota.

## `starrocks_be_segment_file_not_found_total`

- Unit: Count
- Description: Total number of times a segment file was not found (file missing) during segment open. A continuously increasing value may indicate data loss or storage inconsistency.

## `starrocks_be_staros_shard_info_fallback_total`

- Unit: Count
- Type: Cumulative
- Description: Shared-data only. Total number of actual starmgr RPCs (`g_starlet->get_shard_info()`) that the BE's StarOSWorker had to issue because the requested shard info was not in the local cache (i.e. the FE had not pushed the shard to this BE before a query/compaction/lake operation referenced it). Only counted when the starlet readiness check passes and the RPC is actually dispatched; starlet-not-ready timeouts are not included. Should normally be near zero. A sustained or rising rate is a strong signal that FE-side task or node selection is scheduling work on a BE that does not yet have the shard, or that shard push propagation from FE is lagging. Recommended alert: high per-BE rate over a 5-minute window.

## `starrocks_be_staros_shard_info_fallback_failed_total`

- Unit: Count
- Type: Cumulative
- Description: Shared-data only. Subset of `starrocks_be_staros_shard_info_fallback_total` where the starmgr RPC returned a non-OK status. Use the ratio `failed_total / fallback_total` to alert on transient starmgr errors separately from routine successful fallbacks.

## `starrocks_be_staros_shard_count`

- Unit: Count
- Type: Instantaneous
- Description: Shared-data only. Number of shards currently assigned to this BE's StarOSWorker (size of the worker's local shard table). Updated synchronously inside `StarOSWorker::add_shard` and `StarOSWorker::remove_shard` (push-on-mutation), so the value reflects the last shard table mutation rather than being recomputed at scrape time. The gauge is not reset on BE shutdown and will retain its last value until the next mutation. Use it to observe shard distribution balance across BEs and to detect drift from the FE-side placement.

## `starrocks_fe_clone_task_copy_bytes`

- Unit: Bytes
- Type: Cumulative
- Description: The total file size copied by Clone tasks in the cluster, including both INTER_NODE and INTRA_NODE types.

## `starrocks_fe_clone_task_copy_duration_ms`

- Unit: ms
- Type: Cumulative
- Description: The total time for copy consumed by Clone tasks in the cluster, including both INTER_NODE and INTRA_NODE types.

## `starrocks_fe_clone_task_success`

- Unit: Count
- Type: Cumulative
- Description: The number of successfully executed Clone tasks in the cluster.

## `starrocks_fe_clone_task_total`

- Unit: Count
- Type: Cumulative
- Description: The total number of Clone tasks in the cluster.

## `starrocks_fe_last_finished_job_timestamp`

- Unit: ms
- Type: Instantaneous
- Description: Indicates the end time of the last query or loading under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

## `starrocks_fe_memory_usage`

- Unit: Bytes or Count
- Type: Instantaneous
- Description: Indicates the memory statistics for various modules under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

## `starrocks_fe_meta_log_count`

- Unit: Count
- Type: Instantaneous
- Description: The number of Edit Logs without a checkpoint. A value within `100000` is considered reasonable..

## `starrocks_fe_publish_version_daemon_loop_total`

- Unit: Count
- Type: Cumulative
- Description: Total number of `publish-version-daemon` loop runs on this FE node.

The following metrics are `summary`-type metrics that provide latency distributions for different phases of a transaction. These metrics are reported exclusively by the Leader FE node.

Each metric includes the following outputs:
- **Quantiles**: Latency values at different percentile boundaries. These are exposed via the `quantile` label, which can have values of `0.75`, `0.95`, `0.98`, `0.99`, and `0.999`.
- **`<metric_name>_sum`**: The total cumulative time spent in this phase, for example, `starrocks_fe_txn_total_latency_ms_sum`.
- **`<metric_name>_count`**: The total number of transactions recorded for this phase, for example, `starrocks_fe_txn_total_latency_ms_count`.

All transaction metrics share the following labels:
- `type`: Categorizes transactions by their load job source type (for example, `all`, `stream_load`, `routine_load`). This allows for monitoring both overall transaction performance and the performance of specific load types. The reported groups can be configured via the FE parameter [`txn_latency_metric_report_groups`](../../FE_configuration.md#txn_latency_metric_report_groups).
- `is_leader`: Indicates whether the reporting FE node is the Leader. Only the Leader FE (`is_leader="true"`) reports actual metric values. Followers will have `is_leader="false"` and report no data.

## `starrocks_fe_query_resource_group`

- Unit: Count
- Type: Cumulative
- Description: Indicates the total number of queries executed under the specific resource group.

## `starrocks_fe_query_resource_group`

- Unit: Count
- Type: Cumulative
- Description: The number of queries for each resource group.

## `starrocks_fe_query_resource_group_err`

- Unit: Count
- Type: Cumulative
- Description: Indicates the number of failed queries under the specific resource group.

## `starrocks_fe_query_resource_group_err`

- Unit: Count
- Type: Cumulative
- Description: The number of incorrect queries for each resource group.

## `starrocks_fe_query_resource_group_latency`

- Unit: ms
- Type: Cumulative
- Description: Indicates the latency statistics for queries under the specific resource group.

## `starrocks_fe_query_resource_group_latency`

- Unit: Seconds
- Type: Average
- Description: the query latency percentile for each resource group.

## `starrocks_fe_routine_load_error_rows`

- Unit: Count
- Description: The total number of error rows encountered during data loading by all Routine Load jobs.

## `starrocks_fe_routine_load_jobs`

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

## `starrocks_fe_routine_load_max_lag_of_partition`

- Unit: -
- Description: The maximum Kafka partition offset lag for each Routine Load job. It is collected only when the FE configuration `enable_routine_load_lag_metrics` is set to `true` and the offset lag is greater than or equal to the FE configuration `min_routine_load_lag_for_metrics`. By default, `enable_routine_load_lag_metrics` is `false`, and `min_routine_load_lag_for_metrics` is `10000`.

## `starrocks_fe_routine_load_max_lag_time_of_partition`

- Unit: Seconds
- Description: The maximum Kafka partition offset timestamp lag for each Routine Load job. It is collected only when the FE configuration `enable_routine_load_lag_time_metrics` is set to `true`. By default, `enable_routine_load_lag_time_metrics` is `false`.

## `starrocks_fe_routine_load_paused`

- Unit: Count
- Description: The total number of times Routine Load jobs are paused.

## `starrocks_fe_routine_load_receive_bytes`

- Unit: Byte
- Description: The total amount of data loaded by all Routine Load jobs.

## `starrocks_fe_routine_load_rows`

- Unit: Count
- Description: The total number of rows loaded by all Routine Load jobs.

## `starrocks_fe_safe_mode`

- Unit: -
- Type: Instantaneous
- Description: Indicates whether Safe Mode is enabled. Valid values: `0` (disabled) and `1` (enabled). When Safe Mode is enabled, the cluster no longer accepts any loading requests.

## `starrocks_fe_scheduled_pending_tablet_num`

- Unit: Count
- Type: Instantaneous
- Description: The number of Clone tasks in Pending state FE scheduled, including both BALANCE and REPAIR types.

## `starrocks_fe_scheduled_running_tablet_num`

- Unit: Count
- Type: Instantaneous
- Description: The number of Clone tasks in Running state FE scheduled, including both BALANCE and REPAIR types.

## `starrocks_fe_slow_lock_held_time_ms`

- Unit: ms
- Type: Summary
- Description: Histogram tracking the lock held time (in milliseconds) when slow locks are detected. This metric is updated when lock wait time exceeds the `slow_lock_threshold_ms` configuration parameter. It tracks the maximum lock held time among all lock owners when a slow lock event is detected. Each metric includes quantile values (0.75, 0.95, 0.98, 0.99, 0.999), `_sum`, and `_count` outputs. Note: This metric may not accurately reflect the exact lock held time under high contention, because the metric is updated once the wait time exceeds the threshold, but the held time may continue to increase until the owner completes its operation and releases the lock. However, this metric can still be updated even when deadlock occurs.

## `starrocks_fe_slow_lock_wait_time_ms`

- Unit: ms
- Type: Summary
- Description: Histogram tracking the lock wait time (in milliseconds) when slow locks are detected. This metric is updated when lock wait time exceeds the `slow_lock_threshold_ms` configuration parameter. It accurately tracks how long threads wait to acquire locks during lock contention scenarios. Each metric includes quantile values (0.75, 0.95, 0.98, 0.99, 0.999), `_sum`, and `_count` outputs. This metric provides precise wait time measurements. Note: This metric cannot be updated when deadlock occurs, hence it cannot be used to detect deadlock situations.

## `starrocks_fe_sql_block_hit_count`

- Unit: Count
- Description: The number of times blacklisted SQL has been intercepted.

## `starrocks_fe_tablet_max_compaction_score`

- Unit: Count
- Type: Instantaneous
- Description: Indicates the highest Compaction Score on each BE node.

## `starrocks_fe_tablet_num`

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of tablets on each BE node.

## `starrocks_fe_txn_publish_ack_latency_ms`

- Unit: ms
- Type: Summary
- Description: The final acknowledgment latency, from `ready-to-finish` time to the final `finish` time when the transaction is marked as `VISIBLE`. This metric includes final acknowledgment steps after the transaction is ready to finish.

## `starrocks_fe_txn_publish_can_finish_latency_ms`

- Unit: ms
- Type: Summary
- Description: The latency from `publish` task completion to the moment `canTxnFinish()` first returns true, measured from `publish version finish` time to `ready-to-finish` time.

## `starrocks_fe_txn_publish_execute_latency_ms`

- Unit: ms
- Type: Summary
- Description: The active execution time of the `publish` task, from when the task is picked up to when it finishes. This metric represents the actual time being spent to make the transaction's changes visible.

## `starrocks_fe_txn_publish_latency_ms`

- Unit: ms
- Type: Summary
- Description: The latency of the `publish` phase, from `commit` time to `finish` time. This is the duration it takes for a committed transaction to become visible to queries. It is the sum of the `schedule`, `execute`, `can_finish`, and `ack` sub-phases.

## `starrocks_fe_txn_publish_schedule_latency_ms`

- Unit: ms
- Type: Summary
- Description: The time a transaction spends waiting to be published after it has been committed, measured from `commit` time to when the publish task is picked up. This metric reflects scheduling delays or queueing time in the `publish` pipeline.

## `starrocks_fe_txn_total_latency_ms`

- Unit: ms
- Type: Summary
- Description: The total latency for a transaction to complete, measured from the `prepare` time to the `finish` time. This metric represents the full end-to-end duration of a transaction.

## `starrocks_fe_txn_write_latency_ms`

- Unit: ms
- Type: Summary
- Description: The latency of the `write` phase of a transaction, from `prepare` time to `commit` time. This metric isolates the performance of the data writing and preparation stage before the transaction is ready to be published.

## `starrocks_fe_unfinished_backup_job`

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of running BACKUP tasks under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse. For a shared-data cluster, this value is always `0`.

## `starrocks_fe_unfinished_query`

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of queries currently running under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

## `starrocks_fe_unfinished_restore_job`

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of running RESTORE tasks under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse. For a shared-data cluster, this value is always `0`.

## `storage_page_cache_mem_bytes`

- Unit: Bytes
- Description: Memory used by storage page cache.

## `stream_load`

- Unit: -
- Description: Total loaded rows and received bytes.

## `stream_load_pipe_count`

- Unit: Count
- Description: Number of currently running Stream Load tasks.

## `streaming_load_bytes`

- Unit: Bytes
- Description: Total bytes loaded by Stream Load.

## `streaming_load_current_processing`

- Unit: Count
- Description: Number of currently running Stream Load tasks.

## `streaming_load_duration_ms`

- Unit: ms
- Description: Total time spent on Stream Load.

## `streaming_load_requests_total`

- Unit: Count
- Description: Total number of Stream Load requests.

##### SPLIT

## `tablet_base_max_compaction_score`

- Unit: -
- Description: Highest base compaction score of tablets in this BE.

## `tablet_cumulative_max_compaction_score`

- Unit: -
- Description: Highest cumulative compaction score of tablets in this BE.

## `tablet_metadata_mem_bytes`

- Unit: Bytes
- Description: Memory used by tablet metadata.

## `tablet_schema_mem_bytes`

- Unit: Bytes
- Description: Memory used by tablet schema.

## `tablet_update_max_compaction_score`

- Unit: -
- Description: Highest compaction score of tablets in Primary Key tables in the current BE.

## `thrift_connections_total`

- Unit: Count
- Description: Total number of thrift connections (including finished connections).

## `thrift_current_connections (Deprecated)`

## `thrift_opened_clients`

- Unit: Count
- Description: Number of currently opened thrift clients.

## `thrift_used_clients`

- Unit: Count
- Description: Number of thrift clients in use currently.

## `total_column_pool_bytes (Deprecated)`

## `transaction_streaming_load_bytes`

- Unit: Bytes
- Description: Total loading bytes of transaction load.

## `transaction_streaming_load_current_processing`

- Unit: Count
- Description: Number of currently running transactional Stream Load tasks.

## `transaction_streaming_load_duration_ms`

- Unit: ms
- Description: Total time spent on Stream Load transaction Interface.

## `transaction_streaming_load_requests_total`

- Unit: Count
- Description: Total number of transaction load requests.

## `txn_request`

- Unit: -
- Description: Transaction requests of BEGIN, COMMIT, ROLLBACK, and EXEC.

## `uint8_column_pool_bytes`

- Unit: Bytes
- Description: Bytes used by the UINT8 column pool.

## `unused_rowsets_count`

- Unit: Count
- Description: Total number of unused rowsets. Please note that these rowsets will be reclaimed later.

## `update_apply_queue_count`

- Unit: Count
- Description: Queued task count in the Primary Key table transaction APPLY thread pool.

## `update_compaction_duration_us`

- Unit: us
- Description: Total time spent on Primary Key table compactions.

## `update_compaction_outputs_bytes_total`

- Unit: Bytes
- Description: Total bytes written by Primary Key table compactions.

## `update_compaction_outputs_total`

- Unit: Count
- Description: Total number of Primary Key table compactions.

## `update_compaction_task_byte_per_second`

- Unit: Bytes/s
- Description: Estimated rate of Primary Key table compactions.

## `update_compaction_task_cost_time_ns`

- Unit: ns
- Description: Total time spent on the Primary Key table compactions.

## `update_del_vector_bytes_total`

- Unit: Bytes
- Description: Total memory used for caching DELETE vectors in Primary Key tables.

## `update_del_vector_deletes_new`

- Unit: Count
- Description: Total number of newly generated DELETE vectors used in Primary Key tables.

## `update_del_vector_deletes_total (Deprecated)`

## `update_del_vector_dels_num (Deprecated)`

## `update_del_vector_num`

- Unit: Count
- Description: Number of the DELETE vector cache items in Primary Key tables.

## `update_mem_bytes`

- Unit: Bytes
- Description: Memory used by Primary Key table APPLY tasks and Primary Key index.

## `update_primary_index_bytes_total`

- Unit: Bytes
- Description: Total memory cost of the Primary Key index.

## `update_primary_index_num`

- Unit: Count
- Description: Number of Primary Key indexes cached in memory.

## `update_rowset_commit_apply_duration_us`

- Unit: us
- Description: Total time spent on Primary Key table APPLY tasks.

## `update_rowset_commit_apply_total`

- Unit: Count
- Description: Total number of COMMIT and APPLY for Primary Key tables.

## `update_rowset_commit_request_failed`

- Unit: Count
- Description: Total number of failed rowset COMMIT requests in Primary Key tables.

## `update_rowset_commit_request_total`

- Unit: Count
- Description: Total number of rowset COMMIT requests in Primary Key tables.

## `wait_base_compaction_task_num`

- Unit: Count
- Description: Number of base compaction tasks waiting for execution.

## `wait_cumulative_compaction_task_num`

- Unit: Count
- Description: Number of cumulative compaction tasks waiting for execution.

## `writable_blocks_total (Deprecated)`

