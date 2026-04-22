---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical a - c"
---

# 指标 a 到 c

:::note

物化视图和存算分离集群的指标在相应章节中详细说明：

- [异步物化视图指标](../metrics-materialized_view.md)
- [存算分离仪表盘指标和 Starlet 仪表盘指标](../metrics-shared-data.md)

有关如何为 StarRocks 集群构建监控服务的更多信息，请参阅 [监控和告警](../Monitor_and_Alert.md)。

:::

## `active_scan_context_count`

- 单位：计数
- 描述：Flink/Spark SQL 创建的扫描任务总数。

## `async_delta_writer_queue_count`

- 单位：计数
- 描述：tablet delta writer 线程池中排队的任务数量。

## `base_compaction_task_byte_per_second`

- 单位：字节/秒
- 描述：基础合并的估计速率。

## `base_compaction_task_cost_time_ms`

- 单位：毫秒
- 描述：基础合并的总耗时。

## `be_base_compaction_bytes_per_second`

- 单位：字节/秒
- 类型：平均值
- 描述：BE 的基础合并速度。

## `be_base_compaction_failed`

- 单位：计数/秒
- 类型：平均值
- 描述：BE 的基础合并失败次数。

## `be_base_compaction_requests`

- 单位：计数/秒
- 类型：平均值
- 描述：BE 的基础合并请求次数。

## `be_base_compaction_rowsets_per_second`

- 单位：计数
- 类型：平均值
- 描述：BE rowset 的基础合并速度。

## `be_broker_count`

- 单位：计数
- 类型：平均值
- 描述：broker 数量。

## `be_brpc_endpoint_count`

- 单位：计数
- 类型：平均值
- 描述：bRPC 中 StubCache 的数量。

## `be_bytes_read_per_second`

- 单位：字节/秒
- 类型：平均值
- 描述：BE 的读取速度。

## `be_bytes_written_per_second`

- 单位：字节/秒
- 类型：平均值
- 描述：BE 的写入速度。

## `be_clone_failed`

- 单位：计数/秒
- 类型：平均
- 描述：BE 克隆失败。

## `be_clone_total_requests`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的克隆请求。

## `be_create_rollup_failed`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的物化视图创建失败。

## `be_create_rollup_requests`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的物化视图创建请求。

## `be_create_tablet_failed`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的 Tablet 创建失败。

## `be_create_tablet_requests`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的 Tablet 创建请求。

## `be_cumulative_compaction_bytes_per_second`

- 单位：字节/秒
- 类型：平均
- 描述：BE 的累积合并速度。

## `be_cumulative_compaction_failed`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的累积合并失败。

## `be_cumulative_compaction_requests`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的累积合并请求。

## `be_cumulative_compaction_rowsets_per_second`

- 单位：计数
- 类型：平均
- 描述：BE rowsets 的累积合并速度。

## `be_delete_failed`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的删除失败。

## `be_delete_requests`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的删除请求。

## `be_finish_task_failed`

- 单位：计数/秒
- 类型：平均
- 描述：BE 的任务失败。

## `be_finish_task_requests`

- 单位: 次/秒
- 类型: 平均值
- 描述: BE 的任务完成请求。

## `be_fragment_endpoint_count`

- 单位: 次
- 类型: 平均值
- 描述: BE DataStream 的数量。

## `be_fragment_request_latency_avg`

- 单位: 毫秒
- 类型: 平均值
- 描述: 片段请求的延迟。

## `be_fragment_requests_per_second`

- 单位: 次/秒
- 类型: 平均值
- 描述: 片段请求的数量。

## `be_http_request_latency_avg`

- 单位: 毫秒
- 类型: 平均值
- 描述: HTTP 请求的延迟。

## `be_http_request_send_bytes_per_second`

- 单位: 字节/秒
- 类型: 平均值
- 描述: HTTP 请求发送的字节数。

## `be_http_requests_per_second`

- 单位: 次/秒
- 类型: 平均值
- 描述: HTTP 请求的数量。

## `be_publish_failed`

- 单位: 次/秒
- 类型: 平均值
- 描述: BE 的版本发布失败。

## `be_publish_requests`

- 单位: 次/秒
- 类型: 平均值
- 描述: BE 的版本发布请求。

## `be_report_disk_failed`

- 单位: 次/秒
- 类型: 平均值
- 描述: BE 的磁盘报告失败。

## `be_report_disk_requests`

- 单位: 次/秒
- 类型: 平均值
- 描述: BE 的磁盘报告请求。

## `be_report_tables_failed`

- 单位: 次/秒
- 类型: 平均值
- 描述: BE 的表报告失败。

## `be_report_tablet_failed`

- 单位: 次/秒
- 类型: 平均值
- 描述: BE 的 Tablet 报告失败。

## `be_report_tablet_requests`

- 单位: 次/秒
- 类型：平均
- 描述：BE 的 Tablet 报告请求。

## `be_report_tablets_requests`

- 单位：次/秒
- 类型：平均
- 描述：BE 的 Tablet 报告请求。

## `be_report_task_failed`

- 单位：次/秒
- 类型：平均
- 描述：BE 的任务报告失败。

## `be_report_task_requests`

- 单位：次/秒
- 类型：平均
- 描述：BE 的任务报告请求。

## `be_schema_change_failed`

- 单位：次/秒
- 类型：平均
- 描述：BE 的 Schema 变更失败。

## `be_schema_change_requests`

- 单位：次/秒
- 类型：平均
- 描述：BE 的 Schema 变更报告请求。

## `be_storage_migrate_requests`

- 单位：次/秒
- 类型：平均
- 描述：BE 的迁移请求。

## `binary_column_pool_bytes`

- 单位：字节
- 描述：BINARY 列池使用的内存。

## `bitmap_index_mem_bytes`

- 单位：字节
- 描述：位图索引使用的内存。

## `block_cache_hit_bytes`

- 单位：字节
- 类型：计数器
- 描述：块缓存命中累计字节数。目前，仅计算外部表的缓存命中字节数。

## `block_cache_miss_bytes`

- 单位：字节
- 类型：计数器
- 描述：块缓存未命中累计字节数。目前，仅计算外部表的缓存未命中字节数。

## `blocks_created_total (Deprecated)`

## `blocks_deleted_total (Deprecated)`

## `blocks_open_reading (Deprecated)`

## `blocks_open_writing (Deprecated)`

## `bloom_filter_index_mem_bytes`

- 单位：字节
- 描述：布隆过滤器索引使用的内存。

## `broker_count`

- 单位：计数
- 描述：已创建的文件系统代理（按主机地址）总数。

## `brpc_endpoint_stub_count`

- 单位：计数
- 描述：bRPC 存根（按地址）总数。

## `builtin_inverted_index_mem_bytes`

- 单位：字节
- 描述：内置倒排索引使用的内存。

## `bytes_read_total (Deprecated)`

## `bytes_written_total`

- 单位：字节
- 描述：写入的总字节数（扇区写入 * 512）。

## `central_column_pool_bytes (Deprecated)`

## `chunk_allocator_mem_bytes`

- 单位：字节
- 描述：块分配器使用的内存。

## `chunk_pool_local_core_alloc_count`

- 单位：计数
- 描述：内存块分配/缓存指标。

## `chunk_pool_other_core_alloc_count`

- 单位：计数
- 描述：内存块分配/缓存指标。

## `chunk_pool_system_alloc_cost_ns`

- 单位：ns
- 描述：内存块分配/缓存指标。

## `chunk_pool_system_alloc_count`

- 单位：计数
- 描述：内存块分配/缓存指标。

## `chunk_pool_system_free_cost_ns`

- 单位：ns
- 描述：内存块分配/缓存指标。

## `chunk_pool_system_free_count`

- 单位：计数
- 描述：内存块分配/缓存指标。

## `clone_mem_bytes`

- 单位：字节
- 描述：副本克隆使用的内存。

## `column_metadata_mem_bytes`

- 单位：字节
- 描述：列元数据使用的内存。

## `column_partial_update_apply_duration_us`

- 单位：us
- 描述：列的APPLY任务（列模式）部分更新所花费的总时间。

## `column_partial_update_apply_total`

- 单位：计数
- 描述：按列（列模式）部分更新的APPLY总数

## `column_pool_mem_bytes`

- 单位：字节
- 描述：列池使用的内存。

## `column_zonemap_index_mem_bytes`

- 单位：字节
- 描述：列分区图使用的内存。

## `compaction_bytes_total`

- 单位：字节
- 描述：来自基础合并和累积合并的总合并字节数。

## `compaction_deltas_total`

- 单位：计数
- 描述：来自基础合并和累积合并的总合并行集数。

## `compaction_mem_bytes`

- 单位：字节
- 描述：合并操作使用的内存。

## `consistency_mem_bytes`

- 单位：字节
- 描述：副本一致性检查使用的内存。

## `cpu`

- 单位：-
- 描述：`/proc/stat` 返回的 CPU 使用信息。

## `cpu_guest`

- 单位：-
- 类型：平均值
- 描述：cpu_guest 使用率。

## `cpu_idle`

- 单位：-
- 类型: 平均
- 描述: cpu_idle 使用率。

## `cpu_iowait`

- 单位: -
- 类型: 平均
- 描述: cpu_iowait 使用率。

## `cpu_irq`

- 单位: -
- 类型: 平均
- 描述: cpu_irq 使用率。

## `cpu_nice`

- 单位: -
- 类型: 平均
- 描述: cpu_nice 使用率。

## `cpu_softirq`

- 单位: -
- 类型: 平均
- 描述: cpu_softirq 使用率。

## `cpu_steal`

- 单位: -
- 类型: 平均
- 描述: cpu_steal 使用率。

## `cpu_system`

- 单位: -
- 类型: 平均
- 描述: cpu_system 使用率。

## `cpu_user`

- 单位: -
- 类型: 平均
- 描述: cpu_user 使用率。

## `cpu_util`

- 单位: -
- 类型: 平均
- 描述: CPU 使用率。

## `cumulative_compaction_task_byte_per_second`

- 单位: 字节/秒
- 描述: 累积压缩期间处理的字节速率。

## `cumulative_compaction_task_cost_time_ms`

- 单位: 毫秒
- 描述: 累积压缩所花费的总时间。
