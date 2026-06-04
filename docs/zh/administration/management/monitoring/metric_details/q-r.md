---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical q - r"
---

# 指标 q 到 r

## `query_cache_capacity`

- 描述：查询缓存的容量。

## `query_cache_hit_count`

- 单位：计数
- 描述：查询缓存命中次数。

## `query_cache_hit_ratio`

- 单位：-
- 描述：查询缓存命中率。

## `query_cache_lookup_count`

- 单位：计数
- 描述：查询缓存查找总次数。

## `query_cache_usage`

- 单位：字节
- 描述：当前查询缓存使用量。

## `query_cache_usage_ratio`

- 单位：-
- 描述：当前查询缓存使用率。

## `query_mem_bytes`

- 单位：字节
- 描述：查询使用的内存。

## `query_scan_bytes`

- 单位：字节
- 描述：扫描的总字节数。

## `query_scan_bytes_per_second`

- 单位：字节/秒
- 描述：每秒扫描字节数的估计速率。

## `query_scan_rows`

- 单位：计数
- 描述：扫描的总行数。

## `query_spill_trigger_total`

- 单位：计数
- 标签：`storage_type`
- 描述：按存储后端（`local`、`remote`）细分的、触发过至少一次落盘的 spillable 算子实例数量。每个算子实例首次执行 flush 回调时累加一次。

## `query_spill_bytes_write_total`

- 单位：字节
- 标签：`storage_type`
- 描述：按存储后端细分的、spillable 算子累计写入溢出存储的有效负载字节数。

## `query_spill_bytes_read_total`

- 单位：字节
- 标签：`storage_type`
- 描述：按存储后端细分的、恢复阶段从溢出存储累计读回的有效负载字节数。

## `query_spill_blocks_write_total`

- 单位：计数
- 标签：`storage_type`
- 描述：按存储后端细分的、为写入分配的溢出 block 数量。用于估算写路径的 IO 次数规模。

## `query_spill_blocks_read_total`

- 单位：计数
- 标签：`storage_type`
- 描述：按存储后端细分的、打开用于读取的溢出 block 数量。用于估算读路径的 IO 次数规模。

## `query_spill_write_io_duration_ns_total`

- 单位：纳秒
- 标签：`storage_type`
- 描述：按存储后端细分的、写侧溢出 IO（block append 与 flush）累计耗时。用于跟踪写盘性能。

## `query_spill_read_io_duration_ns_total`

- 单位：纳秒
- 标签：`storage_type`
- 描述：按存储后端细分的、恢复阶段读侧溢出 IO（block 读取）累计耗时。用于跟踪读盘性能。

## `readable_blocks_total (Deprecated)`

## `resource_group_bigquery_count`

- 单位：计数
- 描述：每个资源组中触发大查询限制的查询数量。这是一个瞬时值。

## `resource_group_concurrency_overflow_count`

- 单位：计数
- 描述：每个资源组中触发并发限制的查询数量。这是一个瞬时值。

## `resource_group_connector_scan_use_ratio (Deprecated)`

- 单位：-
- 描述：每个资源组使用的外部表扫描线程时间片占所有资源组使用总量的比例。这是两次指标检索之间时间间隔内的平均值。

## `resource_group_cpu_limit_ratio`

- 单位：-
- 描述：每个资源组的 CPU 核限制占所有资源组 CPU 核限制总量的比例。这是一个瞬时值。

## `resource_group_cpu_use_ratio (Deprecated)`

- 单位：-
- 描述：每个资源组使用的管道线程时间片占所有资源组使用总量的比例。这是两次指标检索之间时间间隔内的平均值。

## `resource_group_inuse_cpu_cores`

- 单位：计数
- 描述：每个资源组当前使用的 CPU 核的估计数量。这是两次指标检索之间时间间隔内的平均值。

## `resource_group_mem_inuse_bytes`

- 单位：字节
- 描述：每个资源组当前使用的内存，以字节为单位。这是一个瞬时值。

## `resource_group_mem_limit_bytes`

- 单位：字节
- 描述：每个资源组的内存限制，以字节为单位。这是一个瞬时值。

## `resource_group_running_queries`

- 单位：计数
- 描述：每个资源组中当前正在运行的查询数量。这是一个瞬时值。

## `resource_group_scan_use_ratio (Deprecated)`

- 单位：-
- 描述：每个资源组使用的内部表扫描线程时间片占所有资源组总使用量的比例。这是两次指标检索之间时间间隔内的平均值。

## `resource_group_total_queries`

- 单位：计数
- 描述：每个资源组中执行的查询总数，包括当前正在运行的查询。这是一个瞬时值。

## `result_block_queue_count`

- 单位：计数
- 描述：结果块队列中的结果数量。

## `result_buffer_block_count`

- 单位：计数
- 描述：结果缓冲区中的块数量。

## `routine_load_task_count`

- 单位：计数
- 描述：当前正在运行的Routine Load任务数量。

## `rowset_count_generated_and_in_use`

- 单位：计数
- 描述：当前正在使用的rowset ID数量。

## `rowset_metadata_mem_bytes`

- 单位：字节
- 描述：rowset元数据的总字节数。

## `running_base_compaction_task_num`

- 单位：计数
- 描述：正在运行的base compaction任务总数。

## `running_cumulative_compaction_task_num`

- 单位：计数
- 描述：正在运行的cumulative compaction任务总数。

## `running_update_compaction_task_num`

- 单位：计数
- 描述：当前正在运行的Primary Key表compaction任务总数。

