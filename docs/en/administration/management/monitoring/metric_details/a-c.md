---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical a - c"
---

# Metrics a through c

:::note

Metrics for materialized views and shared-data clusters are detailed in the corresponding sections:

- [Metrics for asynchronous materialized view metrics](../metrics-materialized_view.md)
- [Metrics for Shared-data Dashboard metrics, and Starlet Dashboard metrics](../metrics-shared-data.md)

For more information on how to build a monitoring service for your StarRocks cluster, see [Monitor and Alert](../Monitor_and_Alert.md).

:::

## `active_scan_context_count`

- Unit: Count
- Description: Total number of scan tasks created by Flink/Spark SQL.

## `async_delta_writer_queue_count`

- Unit: Count
- Description: Queued task count in the tablet delta writer thread pool.

## `base_compaction_task_byte_per_second`

- Unit: Bytes/s
- Description: Estimated rate of base compactions.

## `base_compaction_task_cost_time_ms`

- Unit: ms
- Description: Total time spent on base compactions.

## `be_base_compaction_bytes_per_second`

- Unit: Bytes/s
- Type: Average
- Description: Base compaction speed of BE.

## `be_base_compaction_failed`

- Unit: Count/s
- Type: Average
- Description: Base compaction failure of BE.

## `be_base_compaction_requests`

- Unit: Count/s
- Type: Average
- Description: Base compaction request of BE.

## `be_base_compaction_rowsets_per_second`

- Unit: Count
- Type: Average
- Description: Base compaction speed of BE rowsets.

## `be_broker_count`

- Unit: Count
- Type: Average
- Description: Number of brokers.

## `be_brpc_endpoint_count`

- Unit: Count
- Type: Average
- Description: Number of StubCache in bRPC.

## `be_bytes_read_per_second`

- Unit: Bytes/s
- Type: Average
- Description: Read speed of BE.

## `be_bytes_written_per_second`

- Unit: Bytes/s
- Type: Average
- Description: Write speed of BE.

## `be_clone_failed`

- Unit: Count/s
- Type: Average
- Description: BE clone failure.

## `be_clone_total_requests`

- Unit: Count/s
- Type: Average
- Description: Clone request of BE.

## `be_create_rollup_failed`

- Unit: Count/s
- Type: Average
- Description: Materialized view creation failure of BE.

## `be_create_rollup_requests`

- Unit: Count/s
- Type: Average
- Description: Materialized view creation request of BE.

## `be_create_tablet_failed`

- Unit: Count/s
- Type: Average
- Description: Tablet creation failure of BE.

## `be_create_tablet_requests`

- Unit: Count/s
- Type: Average
- Description: Tablet creation request of BE.

## `be_cumulative_compaction_bytes_per_second`

- Unit: Bytes/s
- Type: Average
- Description: Cumulative compaction speed of BE.

## `be_cumulative_compaction_failed`

- Unit: Count/s
- Type: Average
- Description: Cumulative compaction failure of BE.

## `be_cumulative_compaction_requests`

- Unit: Count/s
- Type: Average
- Description: Cumulative compaction request of BE.

## `be_cumulative_compaction_rowsets_per_second`

- Unit: Count
- Type: Average
- Description: Cumulative compaction speed of BE rowsets.

## `be_delete_failed`

- Unit: Count/s
- Type: Average
- Description: Delete failure of BE.

## `be_delete_requests`

- Unit: Count/s
- Type: Average
- Description: Delete request of BE.

## `be_finish_task_failed`

- Unit: Count/s
- Type: Average
- Description: Task failure of BE.

## `be_finish_task_requests`

- Unit: Count/s
- Type: Average
- Description: Task finish request of BE.

## `be_fragment_endpoint_count`

- Unit: Count
- Type: Average
- Description: Number of BE DataStream.

## `be_fragment_request_latency_avg`

- Unit: ms
- Type: Average
- Description: Latency of fragment requests.

## `be_fragment_requests_per_second`

- Unit: Count/s
- Type: Average
- Description: Number of fragment requests.

## `be_http_request_latency_avg`

- Unit: ms
- Type: Average
- Description: Latency of HTTP requests.

## `be_http_request_send_bytes_per_second`

- Unit: Bytes/s
- Type: Average
- Description: Number of bytes sent for HTTP requests.

## `be_http_requests_per_second`

- Unit: Count/s
- Type: Average
- Description: Number of HTTP requests.

## `be_publish_failed`

- Unit: Count/s
- Type: Average
- Description: Version release failure of BE.

## `be_publish_requests`

- Unit: Count/s
- Type: Average
- Description: Version publish request of BE.

## `be_report_disk_failed`

- Unit: Count/s
- Type: Average
- Description: Disk report failure of BE.

## `be_report_disk_requests`

- Unit: Count/s
- Type: Average
- Description: Disk report request of BE.

## `be_report_tables_failed`

- Unit: Count/s
- Type: Average
- Description: Table report failure of BE.

## `be_report_tablet_failed`

- Unit: Count/s
- Type: Average
- Description: Tablet report failure of BE.

## `be_report_tablet_requests`

- Unit: Count/s
- Type: Average
- Description: Tablet report request of BE.

## `be_report_tablets_requests`

- Unit: Count/s
- Type: Average
- Description: Tablet report request of BE.

## `be_report_task_failed`

- Unit: Count/s
- Type: Average
- Description: Task report failure of BE.

## `be_report_task_requests`

- Unit: Count/s
- Type: Average
- Description: Task report request of BE.

## `be_schema_change_failed`

- Unit: Count/s
- Type: Average
- Description: Schema change failure of BE.

## `be_schema_change_requests`

- Unit: Count/s
- Type: Average
- Description: Schema change report request of BE.

## `be_storage_migrate_requests`

- Unit: Count/s
- Type: Average
- Description: Migration request of BE.

## `binary_column_pool_bytes`

- Unit: Bytes
- Description: Memory used by the BINARY column pool.

## `bitmap_index_mem_bytes`

- Unit: Bytes
- Description: Memory used by bitmap indexes.

## `block_cache_hit_bytes`

- Unit: Bytes
- Type: Counter
- Description: Cumulative bytes of block cache hits. For now, only the cache hit bytes for external table is being counted.

## `block_cache_miss_bytes`

- Unit: Bytes
- Type: Counter
- Description: Cumulative bytes of block cache misses. For now, only the cache miss bytes for external table is being counted.

## `blocks_created_total (Deprecated)`

## `blocks_deleted_total (Deprecated)`

## `blocks_open_reading (Deprecated)`

## `blocks_open_writing (Deprecated)`

## `bloom_filter_index_mem_bytes`

- Unit: Bytes
- Description: Memory used by Bloomfilter indexes.

## `broker_count`

- Unit: Count
- Description: Total number of filesystem brokers (by host address) created.

## `brpc_endpoint_stub_count`

- Unit: Count
- Description: Total number of bRPC stubs (by address).

## `builtin_inverted_index_mem_bytes`

- Unit: Bytes
- Description: Memory used by builtin inverted indexes.

## `bytes_read_total (Deprecated)`

## `bytes_written_total`

- Unit: Bytes
- Description: Total bytes written (sectors write * 512).

## `central_column_pool_bytes (Deprecated)`

## `chunk_allocator_mem_bytes`

- Unit: Bytes
- Description: Memory used by the chunk allocator.

## `chunk_pool_local_core_alloc_count`

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

## `chunk_pool_other_core_alloc_count`

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

## `chunk_pool_system_alloc_cost_ns`

- Unit: ns
- Description: Memory chunk allocation/cache metrics.

## `chunk_pool_system_alloc_count`

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

## `chunk_pool_system_free_cost_ns`

- Unit: ns
- Description: Memory chunk allocation/cache metrics.

## `chunk_pool_system_free_count`

- Unit: Count
- Description: Memory chunk allocation/cache metrics.

## `clone_mem_bytes`

- Unit: Bytes
- Description: Memory used for replica clone.

## `column_metadata_mem_bytes`

- Unit: Bytes
- Description: Memory used by column metadata.

## `column_partial_update_apply_duration_us`

- Unit: us
- Description: Total time spent on partial updates for columns' APPLY tasks (Column mode).

## `column_partial_update_apply_total`

- Unit: Count
- Description: Total number of APPLY for partial updates by column (Column mode)

## `column_pool_mem_bytes`

- Unit: Bytes
- Description: Memory used by the column pools.

## `column_zonemap_index_mem_bytes`

- Unit: Bytes
- Description: Memory used by column zonemaps.

## `compaction_bytes_total`

- Unit: Bytes
- Description: Total merged bytes from base compactions and cumulative compactions.

## `compaction_deltas_total`

- Unit: Count
- Description: Total number of merged rowsets from base compactions and cumulative compactions.

## `compaction_mem_bytes`

- Unit: Bytes
- Description: Memory used by compactions.

## `consistency_mem_bytes`

- Unit: Bytes
- Description: Memory used by replica consistency checks.

## `cpu`

- Unit: -
- Description: CPU usage information returned by `/proc/stat`.

## `cpu_guest`

- Unit: -
- Type: Average
- Description: cpu_guest usage rate.

## `cpu_idle`

- Unit: -
- Type: Average
- Description: cpu_idle usage rate.

## `cpu_iowait`

- Unit: -
- Type: Average
- Description: cpu_iowait usage rate.

## `cpu_irq`

- Unit: -
- Type: Average
- Description: cpu_irq usage rate.

## `cpu_nice`

- Unit: -
- Type: Average
- Description: cpu_nice usage rate.

## `cpu_softirq`

- Unit: -
- Type: Average
- Description: cpu_softirq usage rate.

## `cpu_steal`

- Unit: -
- Type: Average
- Description: cpu_steal usage rate.

## `cpu_system`

- Unit: -
- Type: Average
- Description: cpu_system usage rate.

## `cpu_user`

- Unit: -
- Type: Average
- Description: cpu_user usage rate.

## `cpu_util`

- Unit: -
- Type: Average
- Description: CPU usage rate.

## `cumulative_compaction_task_byte_per_second`

- Unit: Bytes/s
- Description: Rate of bytes processed during cumulative compactions.

## `cumulative_compaction_task_cost_time_ms`

- Unit: ms
- Description: Total time spent on cumulative compactions.

