---
displayed_sidebar: docs
---

# Server metrics

This topic introduces some important general metrics of StarRocks.

For dedicated metrics for materialized views and shared-data clusters, please refer to the corresponding sections:

- [Metrics for asynchronous materialized view metrics](../metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](../metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../Monitor_and_Alert.md).

## Metric items

### BE and FE server metrics

#### pip_query_ctx_cnt

- Unit: Count
- Description: Total number of currently running queries in the BE.

#### be_broker_count

- Unit: Count
- Type: Average
- Description: Number of brokers.

#### be_brpc_endpoint_count

- Unit: Count
- Type: Average
- Description: Number of StubCache in bRPC.

#### be_bytes_read_per_second

- Unit: Bytes/s
- Type: Average
- Description: Read speed of BE.

#### be_bytes_written_per_second

- Unit: Bytes/s
- Type: Average
- Description: Write speed of BE.

#### be_base_compaction_bytes_per_second

- Unit: Bytes/s
- Type: Average
- Description: Base compaction speed of BE.

#### be_cumulative_compaction_bytes_per_second

- Unit: Bytes/s
- Type: Average
- Description: Cumulative compaction speed of BE.

#### be_base_compaction_rowsets_per_second

- Unit: Count
- Type: Average
- Description: Base compaction speed of BE rowsets.

#### be_cumulative_compaction_rowsets_per_second

- Unit: Count
- Type: Average
- Description: Cumulative compaction speed of BE rowsets.

#### be_base_compaction_failed

- Unit: Count/s
- Type: Average
- Description: Base compaction failure of BE.

#### be_clone_failed

- Unit: Count/s
- Type: Average
- Description: BE clone failure.

#### be_create_rollup_failed

- Unit: Count/s
- Type: Average
- Description: Materialized view creation failure of BE.

#### be_create_tablet_failed

- Unit: Count/s
- Type: Average
- Description: Tablet creation failure of BE.

#### be_cumulative_compaction_failed

- Unit: Count/s
- Type: Average
- Description: Cumulative compaction failure of BE.

#### be_delete_failed

- Unit: Count/s
- Type: Average
- Description: Delete failure of BE.

#### be_finish_task_failed

- Unit: Count/s
- Type: Average
- Description: Task failure of BE.

#### be_publish_failed

- Unit: Count/s
- Type: Average
- Description: Version release failure of BE.

#### be_report_tables_failed

- Unit: Count/s
- Type: Average
- Description: Table report failure of BE.

#### be_report_disk_failed

- Unit: Count/s
- Type: Average
- Description: Disk report failure of BE.

#### be_report_tablet_failed

- Unit: Count/s
- Type: Average
- Description: Tablet report failure of BE.

#### be_report_task_failed

- Unit: Count/s
- Type: Average
- Description: Task report failure of BE.

#### be_schema_change_failed

- Unit: Count/s
- Type: Average
- Description: Schema change failure of BE.

#### be_base_compaction_requests

- Unit: Count/s
- Type: Average
- Description: Base compaction request of BE.

#### be_clone_total_requests

- Unit: Count/s
- Type: Average
- Description: Clone request of BE.

#### be_create_rollup_requests

- Unit: Count/s
- Type: Average
- Description: Materialized view creation request of BE.

#### be_create_tablet_requests

- Unit: Count/s
- Type: Average
- Description: Tablet creation request of BE.

#### be_cumulative_compaction_requests

- Unit: Count/s
- Type: Average
- Description: Cumulative compaction request of BE.

#### be_delete_requests

- Unit: Count/s
- Type: Average
- Description: Delete request of BE.

#### be_finish_task_requests

- Unit: Count/s
- Type: Average
- Description: Task finish request of BE.

#### be_publish_requests

- Unit: Count/s
- Type: Average
- Description: Version publish request of BE.

#### be_report_tablets_requests

- Unit: Count/s
- Type: Average
- Description: Tablet report request of BE.

#### be_report_disk_requests

- Unit: Count/s
- Type: Average
- Description: Disk report request of BE.

#### be_report_tablet_requests

- Unit: Count/s
- Type: Average
- Description: Tablet report request of BE.

#### be_report_task_requests

- Unit: Count/s
- Type: Average
- Description: Task report request of BE.

#### be_schema_change_requests

- Unit: Count/s
- Type: Average
- Description: Schema change report request of BE.

#### be_storage_migrate_requests

- Unit: Count/s
- Type: Average
- Description: Migration request of BE.

#### be_fragment_endpoint_count

- Unit: Count
- Type: Average
- Description: Number of BE DataStream.

#### be_fragment_request_latency_avg

- Unit: ms
- Type: Average
- Description: Latency of fragment requests.

#### be_fragment_requests_per_second

- Unit: Count/s
- Type: Average
- Description: Number of fragment requests.

#### be_http_request_latency_avg

- Unit: ms
- Type: Average
- Description: Latency of HTTP requests.

#### be_http_requests_per_second

- Unit: Count/s
- Type: Average
- Description: Number of HTTP requests.

#### be_http_request_send_bytes_per_second

- Unit: Bytes/s
- Type: Average
- Description: Number of bytes sent for HTTP requests.

#### fe_connections_per_second

- Unit: Count/s
- Type: Average
- Description: New connection rate of FE.

#### fe_connection_total

- Unit: Count
- Type: Cumulative
- Description: Total number of FE connections.

#### fe_edit_log_read

- Unit: Count/s
- Type: Average
- Description: Read speed of FE edit log.

#### fe_edit_log_size_bytes

- Unit: Bytes/s
- Type: Average
- Description: Size of FE edit log.

#### fe_edit_log_write

- Unit: Bytes/s
- Type: Average
- Description: Write speed of FE edit log.

#### fe_checkpoint_push_per_second

- Unit: Count/s
- Type: Average
- Description: Number of FE checkpoints.

#### fe_pending_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of pending hadoop jobs.

#### fe_committed_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of committed hadoop jobs.

#### fe_loading_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of loading hadoop jobs.

#### fe_finished_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of completedhadoop jobs.

#### fe_cancelled_hadoop_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled hadoop jobs.

#### fe_pending_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of pending insert jobs.

#### fe_loading_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of loading insert jobs.

#### fe_committed_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of committed insert jobs.

#### fe_finished_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of completed insert jobs.

#### fe_cancelled_insert_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled insert jobs.

#### fe_pending_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of pending broker jobs.

#### fe_loading_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of loading broker jobs.

#### fe_committed_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of committed broker jobs.

#### fe_finished_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of finished broker jobs.

#### fe_cancelled_broker_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled broker jobs.

#### fe_pending_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of pending delete jobs.

#### fe_loading_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of loading delete jobs.

#### fe_committed_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of committed delete jobs.

#### fe_finished_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of finished delete jobs.

#### fe_cancelled_delete_load_job

- Unit: Count
- Type: Average
- Description: Number of cancelled delete jobs.

#### fe_rollup_running_alter_job

- Unit: Count
- Type: Average
- Description: Number of jobs created in rollup.

#### fe_schema_change_running_job

- Unit: Count
- Type: Average
- Description: Number of jobs in schema change.

#### starrocks_fe_meta_log_count

- Unit: Count
- Type: Instantaneous
- Description: The number of Edit Logs without a checkpoint. A value within `100000` is considered reasonable..

#### starrocks_fe_query_resource_group

- Unit: Count
- Type: Cumulative
- Description: The number of queries for each resource group.

#### starrocks_fe_query_resource_group_latency

- Unit: Seconds
- Type: Average
- Description: the query latency percentile for each resource group.

#### starrocks_fe_query_resource_group_err

- Unit: Count
- Type: Cumulative
- Description: The number of incorrect queries for each resource group.

#### starrocks_be_resource_group_cpu_limit_ratio

- Unit: -
- Type: Instantaneous
- Description: Instantaneous value of resource group cpu quota ratio.

#### starrocks_be_resource_group_cpu_use_ratio

- Unit: -
- Type: Average
- Description: The ratio of CPU time used by the resource group to the CPU time of all resource groups.

#### starrocks_be_resource_group_mem_limit_bytes

- Unit: Bytes
- Type: Instantaneous
- Description: Instantaneous value of resource group memory quota.

#### starrocks_be_resource_group_mem_inuse_bytes

- Unit: Bytes
- Type: Instantaneous
- Description: Instantaneous value of resource group memory usage.

#### starrocks_be_mem_pool_mem_limit_bytes

- Unit: Bytes
- Type: Instantaneous
- Description: Memory limit for each memory pool, measured in bytes.

#### starrocks_be_mem_pool_mem_usage_bytes

- Unit: Bytes
- Type: Instantaneous
- Description: Currently used total memory by each memory pool, measured in bytes.

#### starrocks_be_mem_pool_mem_usage_ratio

- Unit: -
- Type: Instantaneous
- Description: Ratio of the memory usage of the memory pool to the memory limit of the memory pool.

#### starrocks_be_mem_pool_workgroup_count

- Unit: Count
- Type: Instantaneous
- Description: Number of resource groups assigned to each memory pool.

#### starrocks_be_pipe_prepare_pool_queue_len

- Unit: Count
- Type: Instantaneous
- Description: Instantaneous value of pipeline prepare thread pool task queue length.

#### starrocks_fe_safe_mode

- Unit: -
- Type: Instantaneous
- Description: Indicates whether Safe Mode is enabled. Valid values: `0` (disabled) and `1` (enabled). When Safe Mode is enabled, the cluster no longer accepts any loading requests.

#### starrocks_fe_unfinished_backup_job

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of running BACKUP tasks under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse. For a shared-data cluster, this value is always `0`.

#### starrocks_fe_unfinished_restore_job

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of running RESTORE tasks under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse. For a shared-data cluster, this value is always `0`.

#### starrocks_fe_memory_usage

- Unit: Bytes or Count
- Type: Instantaneous
- Description: Indicates the memory statistics for various modules under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

#### starrocks_fe_unfinished_query

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of queries currently running under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

#### starrocks_fe_last_finished_job_timestamp

- Unit: ms
- Type: Instantaneous
- Description: Indicates the end time of the last query or loading under the specific warehouse. For a shared-nothing cluster, this item only monitors the default warehouse.

#### starrocks_fe_query_resource_group

- Unit: Count
- Type: Cumulative
- Description: Indicates the total number of queries executed under the specific resource group.

#### starrocks_fe_query_resource_group_err

- Unit: Count
- Type: Cumulative
- Description: Indicates the number of failed queries under the specific resource group.

#### starrocks_fe_query_resource_group_latency

- Unit: ms
- Type: Cumulative
- Description: Indicates the latency statistics for queries under the specific resource group.

#### starrocks_fe_tablet_num

- Unit: Count
- Type: Instantaneous
- Description: Indicates the number of tablets on each BE node.

#### starrocks_fe_tablet_max_compaction_score

- Unit: Count
- Type: Instantaneous
- Description: Indicates the highest Compaction Score on each BE node.

#### starrocks_fe_slow_lock_held_time_ms

- Unit: ms
- Type: Summary
- Description: Histogram tracking the lock held time (in milliseconds) when slow locks are detected. This metric is updated when lock wait time exceeds the `slow_lock_threshold_ms` configuration parameter. It tracks the maximum lock held time among all lock owners when a slow lock event is detected. Each metric includes quantile values (0.75, 0.95, 0.98, 0.99, 0.999), `_sum`, and `_count` outputs. Note: This metric may not accurately reflect the exact lock held time under high contention, because the metric is updated once the wait time exceeds the threshold, but the held time may continue to increase until the owner completes its operation and releases the lock. However, this metric can still be updated even when deadlock occurs.

#### starrocks_fe_slow_lock_wait_time_ms

- Unit: ms
- Type: Summary
- Description: Histogram tracking the lock wait time (in milliseconds) when slow locks are detected. This metric is updated when lock wait time exceeds the `slow_lock_threshold_ms` configuration parameter. It accurately tracks how long threads wait to acquire locks during lock contention scenarios. Each metric includes quantile values (0.75, 0.95, 0.98, 0.99, 0.999), `_sum`, and `_count` outputs. This metric provides precise wait time measurements. Note: This metric cannot be updated when deadlock occurs, hence it cannot be used to detect deadlock situations.

### Server infrastructure metrics

#### cpu_util

- Unit: -
- Type: Average
- Description: CPU usage rate.

#### cpu_system

- Unit: -
- Type: Average
- Description: cpu_system usage rate.

#### cpu_user

- Unit: -
- Type: Average
- Description: cpu_user usage rate.

#### cpu_idle

- Unit: -
- Type: Average
- Description: cpu_idle usage rate.

#### cpu_guest

- Unit: -
- Type: Average
- Description: cpu_guest usage rate.

#### cpu_iowait

- Unit: -
- Type: Average
- Description: cpu_iowait usage rate.

#### cpu_irq

- Unit: -
- Type: Average
- Description: cpu_irq usage rate.

#### cpu_nice

- Unit: -
- Type: Average
- Description: cpu_nice usage rate.

#### cpu_softirq

- Unit: -
- Type: Average
- Description: cpu_softirq usage rate.

#### cpu_steal

- Unit: -
- Type: Average
- Description: cpu_steal usage rate.

#### disk_free

- Unit: Bytes
- Type: Average
- Description: Free disk capacity.

#### disk_io_svctm

- Unit: ms
- Type: Average
- Description: Disk IO service time.

#### disk_io_util

- Unit: -
- Type: Average
- Description: Disk usage.

#### disk_used

- Unit: Bytes
- Type: Average
- Description: Used disk capacity.

#### encryption_keys_created

- Unit: Count
- Type: Cumulative
- Description: number of file encryption keys created for file encryption

#### encryption_keys_unwrapped

- Unit: Count
- Type: Cumulative
- Description: number of encryption meta unwrapped for file decryption

#### encryption_keys_in_cache

- Unit: Count
- Type: Instantaneous
- Description: number of encryption keys currently in key cache

#### writable_blocks_total (Deprecated)

#### disks_data_used_capacity

- Description: Used capacity of each disk (represented by a storage path).

