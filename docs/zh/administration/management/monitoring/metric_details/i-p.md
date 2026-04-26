---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical i - p"
---

# 指标 i 到 p

:::note

物化视图和存算分离集群的指标在相应章节中详细说明：

- [异步物化视图指标](../metrics-materialized_view.md)
- [存算分离仪表盘指标和 Starlet 仪表盘指标](../metrics-shared-data.md)

有关如何为 StarRocks 集群构建监控服务的更多信息，请参阅 [监控与告警](../Monitor_and_Alert.md)。

:::

## `iceberg_compaction_duration_ms_total`

- 单位：毫秒
- 类型：累积
- 标签：`compaction_type` (`manual` 或 `auto`)
- 描述：运行 Iceberg 压缩任务的总耗时。

## `iceberg_compaction_input_files_total`

- 单位：计数
- 类型：累积
- 标签：`compaction_type` (`manual` 或 `auto`)
- 描述：Iceberg 压缩任务读取的数据文件总数。

## `iceberg_compaction_output_files_total`

- 单位：计数
- 类型：累积
- 标签：`compaction_type` (`manual` 或 `auto`)
- 描述：Iceberg 压缩任务生成的数据文件总数。

## `iceberg_compaction_removed_delete_files_total`

- 单位：计数
- 类型：累积
- 标签：`compaction_type` (`manual` 或 `auto`)
- 描述：Iceberg 手动压缩任务删除的删除文件总数。

## `iceberg_compaction_total`

- 单位：计数
- 类型：累积
- 标签：`compaction_type` (`manual` 或 `auto`)
- 描述：Iceberg 压缩 (`rewrite_data_files`) 任务的总数。

## `iceberg_delete_bytes`

- 单位：字节
- 类型：累积
- 标签：`delete_type` (`position` 或 `metadata`)
- 描述：Iceberg `DELETE` 任务删除的总字节数。对于 `metadata` 删除，这表示已删除数据文件的大小。对于 `position` 删除，这表示创建的位置删除文件的大小。

## `iceberg_delete_duration_ms_total`

- 单位：毫秒
- 类型：累积
- 标签：`delete_type` (`position` 或 `metadata`)
- 描述：Iceberg `DELETE` 任务的总执行时间（毫秒）。每个任务的持续时间在其结束后添加。`delete_type` 区分两种删除方法。

## `iceberg_delete_rows`

- 单位：行
- 类型：累积
- 标签：`delete_type` (`position` 或 `metadata`)
- 描述：Iceberg `DELETE` 任务删除的总行数。对于 `metadata` 删除，这表示已删除数据文件中的行数。对于 `position` 删除，这表示创建的位置删除数。

## `iceberg_delete_total`

- 单位：计数
- 类型：累积
- 标签：
  - `status` (`success` 或 `failed`)
  - `reason` (`none`、`timeout`、`oom`、`access_denied`、`unknown`)
  - `delete_type` (`position` 或 `metadata`)
- 描述：针对 Iceberg 表的 `DELETE` 任务总数。无论任务成功或失败，每当任务结束时，该指标都会增加 1。`delete_type` 区分两种删除方法：`position`（生成位置删除文件）和 `metadata`（元数据级别删除）。

## `iceberg_metadata_table_query_total`

- 单位：计数
- 类型：累积
- 标签：`metadata_table` (`refs`、`history`、`metadata_log_entries`、`snapshots`、`manifests`、`files`、`partitions` 或 `properties`)
- 描述：访问 Iceberg 元数据表的 SQL 查询总数。每个查询都计入标识所访问元数据表的 `metadata_table` 标签下。

## `iceberg_time_travel_query_total`

- 单位：计数
- 类型：累积
- 标签：`time_travel_type` (`branch`、`tag`、`snapshot` 或 `timestamp`) 用于分类系列。
- 描述：Iceberg 时间旅行查询总数。未标记的系列对每个时间旅行查询计数一次。标记的系列对查询使用的每种不同时间旅行类型计数。`snapshot` 表示 `FOR VERSION AS OF <snapshot_id>`，`branch` 和 `tag` 表示 `FOR VERSION AS OF <reference_name>`，`timestamp` 表示 `FOR TIMESTAMP AS OF ...`。

## `iceberg_write_bytes`

- 单位：字节
- 类型：累积
- 标签：`write_type` (`insert`、`overwrite` 或 `ctas`)
- 描述：Iceberg 写入任务 (`INSERT`、`INSERT OVERWRITE`、`CTAS`) 写入的总字节数。这表示写入 Iceberg 表的数据文件的总大小。`write_type` 区分操作类型。

## `iceberg_write_duration_ms_total`

- 单位：毫秒
- 类型：累积
- 标签：`write_type` (`insert`、`overwrite` 或 `ctas`)
- 描述：Iceberg 写入任务 (`INSERT`、`INSERT OVERWRITE`、`CTAS`) 的总执行时间（毫秒）。每个任务的持续时间在其结束后累加。`write_type` 区分操作类型。

## `iceberg_write_files`

- 单位：计数
- 类型：累积
- 标签：`write_type` (`insert`、`overwrite` 或 `ctas`)
- 描述：Iceberg 写入任务 (`INSERT`、`INSERT OVERWRITE`、`CTAS`) 写入的数据文件总数。这表示写入 Iceberg 表的数据文件计数。`write_type` 区分操作类型。

## `iceberg_write_rows`

- 单位：行
- 类型：累积
- 标签：`write_type` (`insert`、`overwrite` 或 `ctas`)
- 描述：Iceberg 写入任务 (`INSERT`、`INSERT OVERWRITE`、`CTAS`) 写入的总行数。这表示写入 Iceberg 表的行数。`write_type` 区分操作类型。

## `iceberg_write_total`

- 单位：计数
- 类型：累积
- 标签：
  - `status` (`success` 或 `failed`)
  - `reason` (`none`、`timeout`、`oom`、`access_denied`、`unknown`)
  - `write_type` (`insert`、`overwrite` 或 `ctas`)
- 描述：针对 Iceberg 表的 `INSERT`、`INSERT OVERWRITE` 或 `CTAS` 任务总数。无论任务成功或失败，每当任务结束时，该指标都会增加 1。`write_type` 区分操作类型。

## `int128_column_pool_bytes`

- 单位：字节
- 描述：INT128 列池使用的内存。

## `int16_column_pool_bytes`

- 单位：字节
- 描述：INT16 列池使用的内存。

## `int32_column_pool_bytes`

- 单位：字节
- 描述：INT32 列池使用的内存。

## `int64_column_pool_bytes`

- 单位：字节
- 描述：INT64 列池使用的内存。

## `int8_column_pool_bytes`

- 单位：字节
- 描述：INT8 列池使用的内存。

## `jemalloc_active_bytes`

- 单位：字节
- 描述：应用程序分配的活动页面中的总字节数。

## `jemalloc_allocated_bytes`

- 单位：字节
- 描述：应用程序分配的总字节数。

## `jemalloc_mapped_bytes`

- 单位：字节
- 描述：分配器映射的活动扩展中的总字节数。

## `jemalloc_metadata_bytes`

- 单位：字节
- 描述：专用于元数据的总字节数，包括用于引导敏感分配器元数据结构的基础分配和内部分配。此项不包括透明巨页的使用。

## `jemalloc_metadata_thp`

- 单位：计数
- 描述：用于元数据的透明巨页数量。

## `jemalloc_resident_bytes`

- 单位：字节
- 描述：分配器映射的物理驻留数据页中的最大字节数，包括所有专用于分配器元数据的页面、支持活动分配的页面以及未使用的脏页面。

## `jemalloc_retained_bytes`

- 单位：字节
- 描述：通过 munmap(2) 等操作保留而非返回给操作系统的虚拟内存映射总字节数。

## `jit_cache_mem_bytes`

- 单位：字节
- 描述：JIT 编译函数缓存使用的内存。

## `load_bytes`

- 单位：字节
- 描述：总加载字节数。

## `load_channel_count`

- 单位：计数
- 描述：加载通道总数。

## `load_mem_bytes`

- 单位：字节
- 描述：数据加载的内存开销。

## `load_rows`

- 单位：计数
- 描述：总加载行数。

## `load_rpc_threadpool_size`

- 单位：计数
- 描述：RPC 线程池的当前大小，用于处理 Routine Load 和通过表函数加载。默认值为 10，最大值为 1000。此值根据线程池的使用情况动态调整。

## `local_column_pool_bytes (Deprecated)`

## `max_disk_io_util_percent`

- 单位：-
- 描述：最大磁盘 I/O 利用率百分比。

## `max_network_receive_bytes_rate`

- 单位：字节
- 描述：通过网络接收的总字节数（所有网络接口中的最大值）。

## `max_network_send_bytes_rate`

- 单位：字节
- 描述：通过网络发送的总字节数（所有网络接口中的最大值）。

## `memory_pool_bytes_total`

- 单位：字节
- 描述：内存池使用的内存。

## `memtable_flush_duration_us`

- 单位：微秒
- 描述：memtable 刷盘的总耗时。

## `memtable_flush_queue_count`

- 单位：计数
- 描述：memtable 刷盘线程池中排队的任务数量。

## `memtable_flush_total`

- 单位：计数
- 描述：memtable 刷盘的总次数。

## `merge_commit_append_pipe`

- 单位：微秒
- 类型：汇总
- 描述：在合并提交期间，向流式加载管道追加数据所花费的时间。

## `merge_commit_fail_total`

- 单位：计数
- 类型：累计
- 描述：失败的合并提交请求。

## `merge_commit_pending`

- 单位：微秒
- 类型：汇总
- 描述：合并提交任务在执行前在待处理队列中等待的时间。

## `merge_commit_pending_bytes`

- 单位：字节
- 类型：瞬时
- 描述：待处理合并提交任务持有的数据总字节数。

## `merge_commit_pending_total`

- 单位：计数
- 类型：瞬时
- 描述：当前在执行队列中等待的合并提交任务。

## `merge_commit_register_pipe_total`

- 单位：计数
- 类型：累计
- 描述：为合并提交操作注册的流式加载管道。

## `merge_commit_request`

- 单位：微秒
- 类型：汇总
- 描述：合并提交请求的端到端处理延迟。

## `merge_commit_request_bytes`

- 单位：字节
- 类型：累计
- 描述：通过合并提交请求接收到的数据总字节数。

## `merge_commit_request_total`

- 单位：计数
- 类型：累计
- 描述：BE 接收到的合并提交请求总数。

## `merge_commit_send_rpc_total`

- 单位：计数
- 类型：累计
- 描述：发送给 FE 以启动合并提交操作的 RPC 请求。

## `merge_commit_success_total`

- 单位：计数
- 类型：累积
- 描述：成功完成的合并提交请求。

## `merge_commit_unregister_pipe_total`

- 单位：计数
- 类型：累积
- 描述：从合并提交操作中注销的流式加载管道。

延迟指标公开了百分位数序列，例如 `merge_commit_request_latency_99` 和 `merge_commit_request_latency_90`，以微秒为单位报告。端到端延迟遵循：

`merge_commit_request = merge_commit_pending + merge_commit_wait_plan + merge_commit_append_pipe + merge_commit_wait_finish`

> **注意**：在 v3.4.11、v3.5.12 和 v4.0.4 之前，这些延迟指标以纳秒为单位报告。

## `merge_commit_wait_finish`

- 单位：微秒
- 类型：摘要
- 描述：等待合并提交加载操作完成所花费的时间。

## `merge_commit_wait_plan`

- 单位：微秒
- 类型：摘要
- 描述：RPC 请求和等待流式加载管道可用性的总延迟。

## `meta_request_duration`

- 单位：us
- 描述：元数据读/写总持续时间。

## `meta_request_total`

- 单位：计数
- 描述：元数据读/写请求总数。

## `metadata_mem_bytes (Deprecated)`

## `network_receive_bytes`

- 单位：字节
- 描述：通过网络接收的总字节数。

## `network_receive_packets`

- 单位：计数
- 描述：通过网络接收的数据包总数。

## `network_send_bytes`

- 单位：字节
- 描述：通过网络发送的字节数。

## `network_send_packets`

- 单位：计数
- 描述：通过网络发送的数据包总数。

## `ordinal_index_mem_bytes`

- 单位：字节
- 描述：序数索引使用的内存。

## `page_cache_capacity`

- 描述：存储页缓存的容量。

## `page_cache_hit_count`

- 单位：计数
- 描述：存储页缓存中的总命中数。

## `page_cache_insert_count`

- 单位：计数
- 描述：存储页缓存中的总插入操作数。

## `page_cache_insert_evict_count`

- 单位：计数
- 描述：由于容量限制，在插入操作期间逐出的缓存条目总数。

## `page_cache_lookup_count`

- 单位：计数
- 描述：存储页缓存的总查找次数。

## `page_cache_release_evict_count`

- 单位：计数
- 描述：当缓存使用量超出容量时，在释放操作期间逐出的缓存条目总数。

## `pip_query_ctx_cnt`

- 单位：计数
- 描述：BE 中当前正在运行的查询总数。

## `pipe_driver_execution_time`

- 描述：PipelineDriver 执行器处理 PipelineDriver 所花费的累计时间。

## `pipe_driver_queue_len`

- 单位：计数
- 描述：BE 中就绪队列中等待调度的就绪驱动程序当前数量。

## `pipe_driver_schedule_count`

- 单位：计数
- 描述：BE 中管道执行器驱动程序调度的累计次数。

## `pipe_poller_block_queue_len`

- 单位：计数
- 描述：管道引擎中 PipelineDriverPoller 的阻塞队列当前长度。

## `pipe_prepare_pool_queue_len`

- 单位：计数
- 描述：管道 PREPARE 线程池中排队的任务数量。这是一个瞬时值。

## `pipe_scan_executor_queuing`

- 单位：计数
- 描述：Scan Operators 启动的待处理异步 I/O 任务的当前数量。

## `pk_index_compaction_queue_count`

- 单位：计数
- 描述：主键索引压缩线程池中排队的任务数量。

## `pk_index_sst_read_error_total`

- 类型：计数器
- 单位：计数
- 描述：湖主键持久化索引中 SST 文件读取失败的总次数。当 SST 多次获取（读取）操作失败时增加。

## `pk_index_sst_write_error_total`

- 类型：计数器
- 单位：计数
- 描述：湖主键持久化索引中 SST 文件写入失败的总次数。当 SST 文件构建失败时增加。

## `plan_fragment_count`

- 单位：计数
- 描述：当前正在运行的查询计划片段数量。

## `process_fd_num_limit_hard`

- 单位：计数
- 描述：文件描述符最大数量的硬限制。

## `process_fd_num_limit_soft`

- 单位：计数
- 描述：文件描述符最大数量的软限制。请注意，此项表示软限制。您可以使用 `ulimit` 命令设置硬限制。

## `process_fd_num_used`

- 单位：计数
- 描述：此 BE 进程中当前正在使用的文件描述符数量。

## `process_mem_bytes`

- 单位：字节
- 描述：此进程使用的内存。

## `process_thread_num`

- 单位：计数
- 描述：此进程中的线程总数。

## `publish_version_queue_count`

- 单位：计数
- 描述：发布版本线程池中排队的任务数量。

## `push_request_duration_us`

- 单位：us
- 描述：Spark Load 上花费的总时间。

## `push_request_write_bytes`

- 单位：字节
- 描述：通过 Spark Load 写入的总字节数。

## `push_request_write_bytes_per_second`

- 单位：字节/秒
- 描述：Spark Load 的数据写入速率。

## `push_request_write_rows`

- 单位: 计数
- 描述: 通过Spark Load写入的总行数。

## `push_requests_total`

- 单位: 计数
- 描述: 成功和失败的Spark Load请求总数。
