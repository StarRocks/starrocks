---
displayed_sidebar: docs
---

# Monitoring Metrics

This topic introduces some important general metrics of StarRocks.

For dedicated metrics for materialized views and shared-data clusters, please refer to the corresponding sections:

- [Metrics for asynchronous materialized view metrics](../metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](../metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../Monitor_and_Alert.md).

## Resource group metrics

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

## Pipe metrics

### pipe_poller_block_queue_len

- Unit: Count
- Description: Current length of the block queue of PipelineDriverPoller in the pipeline engine.

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

## BE thread pool metrics

### starrocks_be_exec_state_report_active_threads

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks being executed in the thread pool that reports the execution status of the Fragment instance.

### starrocks_be_exec_state_report_running_threads

- Unit: Count
- Type: Instantaneous
- Description: The number of threads in the thread pool that reports the execution status of the Fragment instance, with a minimum of 1 and a maximum of 2.

### starrocks_be_exec_state_report_threadpool_size

- Unit: Count
- Type: Instantaneous
- Description: The maximum number of threads in the thread pool that reports the execution status of the Fragment instance, defaults to 2.

### starrocks_be_exec_state_report_queue_count

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks queued in the thread pool that reports the execution status of the Fragment instance, up to a maximum of 1000.

### starrocks_be_priority_exec_state_report_active_threads

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks being executed in the thread pool that reports the final execution state of the Fragment instance.

### starrocks_be_priority_exec_state_report_running_threads

- Unit: Count
- Type: Instantaneous
- Description: The number of threads in the thread pool that reports the final execution status of the Fragment instance, with a minimum of 1 and a maximum of 2.

### starrocks_be_priority_exec_state_report_threadpool_size

- Unit: Count
- Type: Instantaneous
- Description: The maximum number of threads in the thread pool that reports the final execution status of the Fragment instance, defaults to 2.

### starrocks_be_priority_exec_state_report_queue_count

- Unit: Count
- Type: Instantaneous
- Description: The number of tasks queued in the thread pool that reports the final execution status of the Fragment instance, up to a maximum of 2147483647.

## Routine load metrics

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

### starrocks_fe_routine_load_max_lag_of_partition

- Unit: -
- Description: The maximum Kafka partition offset lag for each Routine Load job. It is collected only when the FE configuration `enable_routine_load_lag_metrics` is set to `true` and the offset lag is greater than or equal to the FE configuration `min_routine_load_lag_for_metrics`. By default, `enable_routine_load_lag_metrics` is `false`, and `min_routine_load_lag_for_metrics` is `10000`.

### starrocks_fe_routine_load_max_lag_time_of_partition

- Unit: Seconds
- Description: The maximum Kafka partition offset timestamp lag for each Routine Load job. It is collected only when the FE configuration `enable_routine_load_lag_time_metrics` is set to `true`. By default, `enable_routine_load_lag_time_metrics` is `false`.

## Blocked SQL metrics

### starrocks_fe_sql_block_hit_count

- Unit: Count
- Description: The number of times blacklisted SQL has been intercepted.

## Cloning metrics

### starrocks_fe_scheduled_pending_tablet_num

- Unit: Count
- Type: Instantaneous
- Description: The number of Clone tasks in Pending state FE scheduled, including both BALANCE and REPAIR types.

### starrocks_fe_scheduled_running_tablet_num

- Unit: Count
- Type: Instantaneous
- Description: The number of Clone tasks in Running state FE scheduled, including both BALANCE and REPAIR types.

### starrocks_fe_clone_task_total

- Unit: Count
- Type: Cumulative
- Description: The total number of Clone tasks in the cluster.

### starrocks_fe_clone_task_success

- Unit: Count
- Type: Cumulative
- Description: The number of successfully executed Clone tasks in the cluster.

### starrocks_fe_clone_task_copy_bytes

- Unit: Bytes
- Type: Cumulative
- Description: The total file size copied by Clone tasks in the cluster, including both INTER_NODE and INTRA_NODE types.

### starrocks_fe_clone_task_copy_duration_ms

- Unit: ms
- Type: Cumulative
- Description: The total time for copy consumed by Clone tasks in the cluster, including both INTER_NODE and INTRA_NODE types.

### starrocks_be_clone_task_copy_bytes

- Unit: Bytes
- Type: Cumulative
- Description: The total file size copied by Clone tasks in the BE node, including both INTER_NODE and INTRA_NODE types.

### starrocks_be_clone_task_copy_duration_ms

- Unit: ms
- Type: Cumulative
- Description: The total time for copy consumed by Clone tasks in the BE node, including both INTER_NODE and INTRA_NODE types.

## Transaction Latency Metrics

### starrocks_fe_publish_version_daemon_loop_total

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

### starrocks_fe_txn_total_latency_ms

- Unit: ms
- Type: Summary
- Description: The total latency for a transaction to complete, measured from the `prepare` time to the `finish` time. This metric represents the full end-to-end duration of a transaction.

### starrocks_fe_txn_write_latency_ms

- Unit: ms
- Type: Summary
- Description: The latency of the `write` phase of a transaction, from `prepare` time to `commit` time. This metric isolates the performance of the data writing and preparation stage before the transaction is ready to be published.

### starrocks_fe_txn_publish_latency_ms

- Unit: ms
- Type: Summary
- Description: The latency of the `publish` phase, from `commit` time to `finish` time. This is the duration it takes for a committed transaction to become visible to queries. It is the sum of the `schedule`, `execute`, `can_finish`, and `ack` sub-phases.

### starrocks_fe_txn_publish_schedule_latency_ms

- Unit: ms
- Type: Summary
- Description: The time a transaction spends waiting to be published after it has been committed, measured from `commit` time to when the publish task is picked up. This metric reflects scheduling delays or queueing time in the `publish` pipeline.

### starrocks_fe_txn_publish_execute_latency_ms

- Unit: ms
- Type: Summary
- Description: The active execution time of the `publish` task, from when the task is picked up to when it finishes. This metric represents the actual time being spent to make the transaction's changes visible.

### starrocks_fe_txn_publish_can_finish_latency_ms

- Unit: ms
- Type: Summary
- Description: The latency from `publish` task completion to the moment `canTxnFinish()` first returns true, measured from `publish version finish` time to `ready-to-finish` time.

### starrocks_fe_txn_publish_ack_latency_ms

- Unit: ms
- Type: Summary
- Description: The final acknowledgment latency, from `ready-to-finish` time to the final `finish` time when the transaction is marked as `VISIBLE`. This metric includes final acknowledgment steps after the transaction is ready to finish.

## Merge Commit BE Metrics

### merge_commit_request_total

- Unit: Count
- Type: Cumulative
- Description: Total number of merge commit requests received by BE.

### merge_commit_request_bytes

- Unit: Bytes
- Type: Cumulative
- Description: Total bytes of data received across merge commit requests.

### merge_commit_success_total

- Unit: Count
- Type: Cumulative
- Description: Merge commit requests that finished successfully.

### merge_commit_fail_total

- Unit: Count
- Type: Cumulative
- Description: Merge commit requests that failed.

### merge_commit_pending_total

- Unit: Count
- Type: Instantaneous
- Description: Merge commit tasks currently waiting in the execution queue.

### merge_commit_pending_bytes

- Unit: Bytes
- Type: Instantaneous
- Description: Total bytes of data held by pending merge commit tasks.

### merge_commit_send_rpc_total

- Unit: Count
- Type: Cumulative
- Description: RPC requests sent to FE for starting merge commit operations.

### merge_commit_register_pipe_total

- Unit: Count
- Type: Cumulative
- Description: Stream load pipes registered for merge commit operations.

### merge_commit_unregister_pipe_total

- Unit: Count
- Type: Cumulative
- Description: Stream load pipes unregistered from merge commit operations.

Latency metrics expose percentile series such as `merge_commit_request_latency_99` and `merge_commit_request_latency_90`, reported in microseconds. The end-to-end latency obeys:

`merge_commit_request = merge_commit_pending + merge_commit_wait_plan + merge_commit_append_pipe + merge_commit_wait_finish`

> **Note**: Before v3.4.11, v3.5.12, and v4.0.4, these latency metrics were reported in nanoseconds.

### merge_commit_request

- Unit: microsecond
- Type: Summary
- Description: End-to-end processing latency for merge commit requests.

### merge_commit_pending

- Unit: microsecond
- Type: Summary
- Description: Time merge commit tasks spend waiting in the pending queue before execution.

### merge_commit_wait_plan

- Unit: microsecond
- Type: Summary
- Description: Combined latency for the RPC request and waiting for the stream load pipe to become available.

### merge_commit_append_pipe

- Unit: microsecond
- Type: Summary
- Description: Time spent appending data to the stream load pipe during merge commit.

### merge_commit_wait_finish

- Unit: microsecond
- Type: Summary
- Description: Time spent waiting for merge commit load operations to finish.

## Iceberg query FE metrics

### iceberg_time_travel_query_total

- Unit: Count
- Type: Cumulative
- Labels: `time_travel_type` (`branch`, `tag`, `snapshot`, or `timestamp`) for the categorized series.
- Description: Total number of Iceberg time travel queries. The unlabeled series counts each time travel query once. The labeled series count each distinct time travel type used by the query. `snapshot` means `FOR VERSION AS OF <snapshot_id>`, `branch` and `tag` mean `FOR VERSION AS OF <reference_name>`, and `timestamp` means `FOR TIMESTAMP AS OF ...`.

### iceberg_metadata_table_query_total

- Unit: Count
- Type: Cumulative
- Labels: `metadata_table` (`refs`, `history`, `metadata_log_entries`, `snapshots`, `manifests`, `files`, `partitions`, or `properties`)
- Description: Total number of SQL queries that access Iceberg metadata tables. Each query is counted under the `metadata_table` label that identifies the metadata table being accessed.

## Iceberg delete FE metrics

### iceberg_delete_total

- Unit: Count
- Type: Cumulative
- Labels:
  - `status` (`success` or `failed`)
  - `reason` (`none`, `timeout`, `oom`, `access_denied`, `unknown`)
  - `delete_type` (`position` or `metadata`)
- Description: Total number of `DELETE` tasks that target Iceberg tables. The metric is incremented by 1 after each task ends, regardless of success or failure. `delete_type` distinguishes between two delete methods: `position` (generates position delete files) and `metadata` (metadata-level delete).

### iceberg_delete_duration_ms_total

- Unit: Millisecond
- Type: Cumulative
- Labels: `delete_type` (`position` or `metadata`)
- Description: Total execution time of Iceberg `DELETE` tasks in milliseconds. The duration of each task is added after it ends. `delete_type` distinguishes between two delete methods.

### iceberg_delete_bytes

- Unit: Bytes
- Type: Cumulative
- Labels: `delete_type` (`position` or `metadata`)
- Description: Total deleted bytes from Iceberg `DELETE` tasks. For `metadata` delete, this represents the size of deleted data files. For `position` delete, this represents the size of position delete files created.

### iceberg_delete_rows

- Unit: Rows
- Type: Cumulative
- Labels: `delete_type` (`position` or `metadata`)
- Description: Total deleted rows from Iceberg `DELETE` tasks. For `metadata` delete, this represents the number of rows in deleted data files. For `position` delete, this represents the number of position deletes created.

### iceberg_compaction_total

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of Iceberg compaction (`rewrite_data_files`) tasks.

### iceberg_compaction_duration_ms_total

- Unit: Millisecond
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total time spent running Iceberg compaction tasks.

### iceberg_compaction_input_files_total

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of data files read by Iceberg compaction tasks.

### iceberg_compaction_output_files_total

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of data files produced by Iceberg compaction tasks.

### iceberg_compaction_removed_delete_files_total

- Unit: Count
- Type: Cumulative
- Labels: `compaction_type` (`manual` or `auto`)
- Description: Total number of delete files removed by Iceberg manual compaction tasks.

## Iceberg write FE metrics

### iceberg_write_total

- Unit: Count
- Type: Cumulative
- Labels:
  - `status` (`success` or `failed`)
  - `reason` (`none`, `timeout`, `oom`, `access_denied`, `unknown`)
  - `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total number of `INSERT`, `INSERT OVERWRITE`, or `CTAS` tasks that target Iceberg tables. The metric is incremented by 1 after each task ends, regardless of success or failure. `write_type` distinguishes between the operation types.

### iceberg_write_duration_ms_total

- Unit: Millisecond
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total execution time of Iceberg write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`) in milliseconds. The duration of each task is added after it ends. `write_type` distinguishes between the operation types.

### iceberg_write_bytes

- Unit: Bytes
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total written bytes from Iceberg write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`). This represents the total size of data files written to the Iceberg table. `write_type` distinguishes between the operation types.

### iceberg_write_rows

- Unit: Rows
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total written rows from Iceberg write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`). This represents the number of rows written to the Iceberg table. `write_type` distinguishes between the operation types.

### iceberg_write_files

- Unit: Count
- Type: Cumulative
- Labels: `write_type` (`insert`, `overwrite`, or `ctas`)
- Description: Total number of data files written to Iceberg from write tasks (`INSERT`, `INSERT OVERWRITE`, `CTAS`). This represents the count of data files written to the Iceberg table. `write_type` distinguishes between the operation types.

## Hive write FE metrics

### hive_write_total

- Unit: Count
- Type: Cumulative
- Labels:
  - `status` (`success` or `failed`)
  - `reason` (`none`, `timeout`, `oom`, `access_denied`, `unknown`)
  - `write_type` (`insert` or `overwrite`)
- Description: Total number of `INSERT` or `INSERT OVERWRITE` tasks that target Hive tables. The metric is incremented by 1 after each task ends, regardless of success or failure. `write_type` distinguishes between the operation types.

### hive_write_duration_ms_total

- Unit: Millisecond
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total execution time of Hive write tasks (`INSERT`, `INSERT OVERWRITE`) in milliseconds. The duration of each task is added after it ends. `write_type` distinguishes between the operation types.

### hive_write_bytes

- Unit: Bytes
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total written bytes from Hive write tasks (`INSERT`, `INSERT OVERWRITE`). This represents the total size of data files written to the Hive table. `write_type` distinguishes between the operation types.

### hive_write_rows

- Unit: Rows
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total written rows from Hive write tasks (`INSERT`, `INSERT OVERWRITE`). This represents the number of rows written to the Hive table. `write_type` distinguishes between the operation types.

### hive_write_files

- Unit: Count
- Type: Cumulative
- Labels: `write_type` (`insert` or `overwrite`)
- Description: Total number of data files written to Hive from write tasks (`INSERT`, `INSERT OVERWRITE`). This represents the count of data files written to the Hive table. `write_type` distinguishes between the operation types.

## DataCache metrics

DataCache metrics provide visibility into cache capacity, usage, and hit rate for data caching.

The following metrics are exposed on the BE Prometheus endpoint (`/metrics`):

### datacache_mem_quota_bytes

- Unit: Bytes
- Type: Gauge
- Description: The configured memory quota for datacache.

### datacache_mem_used_bytes

- Unit: Bytes
- Type: Gauge
- Description: The current memory usage of datacache.

### datacache_disk_quota_bytes

- Unit: Bytes
- Type: Gauge
- Description: The configured disk quota for datacache.

### datacache_disk_used_bytes

- Unit: Bytes
- Type: Gauge
- Description: The current disk usage of datacache.

### datacache_meta_used_bytes

- Unit: Bytes
- Type: Gauge
- Description: The memory usage for datacache metadata.

### block_cache_hit_bytes

- Unit: Bytes
- Type: Counter
- Description: Cumulative bytes of block cache hits. For now, only the cache hit bytes for external table is being counted.

### block_cache_miss_bytes

- Unit: Bytes
- Type: Counter
- Description: Cumulative bytes of block cache misses. For now, only the cache miss bytes for external table is being counted.

