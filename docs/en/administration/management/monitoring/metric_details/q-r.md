---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical q - r"
---

# Metrics q through r

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

## `recycle_bin_database_num`

- Unit: Count
- Description: Number of databases currently held in the FE catalog recycle bin.

## `recycle_bin_partition_num`

- Unit: Count
- Description: Number of partitions currently held in the FE catalog recycle bin.

## `recycle_bin_table_num`

- Unit: Count
- Description: Number of tables currently held in the FE catalog recycle bin.

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

