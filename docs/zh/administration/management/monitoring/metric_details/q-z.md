---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical q - z"
---

# 指标 q 到 z

:::note

物化视图和共享数据集群的指标在相应章节中详细介绍：

- [异步物化视图指标](../metrics-materialized_view.md)
- [共享数据仪表盘指标和 Starlet 仪表盘指标](../metrics-shared-data.md)

有关如何为 StarRocks 集群构建监控服务的更多信息，请参阅 [监控和告警](../Monitor_and_Alert.md)。

:::

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

## `schema_change_mem_bytes`

- 单位：字节
- 描述：Schema Change使用的内存。

## `segment_flush_queue_count`

- 单位：计数
- 描述：segment flush线程池中排队的任务数量。

## `segment_metadata_mem_bytes`

- 单位：字节
- 描述：segment元数据使用的内存。

## `segment_read`

- 单位：计数
- 描述：segment读取总数。

## `segment_replicate_queue_count`

- 单位：计数
- 描述：Segment Replicate线程池中排队的任务数量。

## `segment_zonemap_mem_bytes`

- 单位：字节
- 描述：segment zonemap使用的内存。

## `short_key_index_mem_bytes`

- 单位：字节
- 描述：短键索引使用的内存。

## `small_file_cache_count`

- 单位: 计数
- 描述: 小文件缓存的数量。

## `snmp`

- 单位: -
- 描述: `/proc/net/snmp` 返回的指标。

## `starrocks_be_clone_task_copy_bytes`

- 单位: 字节
- 类型: 累积
- 描述: BE 节点中 Clone 任务复制的总文件大小，包括 INTER_NODE 和 INTRA_NODE 类型。

## `starrocks_be_clone_task_copy_duration_ms`

- 单位: 毫秒
- 类型: 累积
- 描述: BE 节点中 Clone 任务复制消耗的总时间，包括 INTER_NODE 和 INTRA_NODE 类型。

## `starrocks_be_exec_state_report_active_threads`

- 单位: 计数
- 类型: 瞬时
- 描述: 报告 Fragment 实例执行状态的线程池中正在执行的任务数量。

## `starrocks_be_exec_state_report_queue_count`

- 单位: 计数
- 类型: 瞬时
- 描述: 报告 Fragment 实例执行状态的线程池中排队的任务数量，最多 1000 个。

## `starrocks_be_exec_state_report_running_threads`

- 单位: 计数
- 类型: 瞬时
- 描述: 报告 Fragment 实例执行状态的线程池中的线程数量，最少 1 个，最多 2 个。

## `starrocks_be_exec_state_report_threadpool_size`

- 单位: 计数
- 类型: 瞬时
- 描述: 报告 Fragment 实例执行状态的线程池中的最大线程数量，默认为 2。

## `starrocks_be_files_scan_num_bytes_read`

- 单位: 字节
- 描述: 从外部存储读取的总字节数。标签: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_files_read`

- 单位: 计数
- 描述: 从外部存储读取的文件数量 (CSV, Parquet, ORC, JSON, Avro)。标签: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_raw_rows_read`

- 单位: 计数
- 描述: 在格式验证和谓词过滤之前从外部存储读取的总原始行数。标签: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_rows_return`

- 单位: 计数
- 描述: 谓词过滤后返回的行数。标签: `file_format`, `scan_type`。

## `starrocks_be_files_scan_num_valid_rows_read`

- 单位: 计数
- 描述: 读取的有效行数（不包括格式无效的行）。标签: `file_format`, `scan_type`。

## `starrocks_be_mem_pool_mem_limit_bytes`

- 单位: 字节
- 类型: 瞬时
- 描述: 每个内存池的内存限制，以字节为单位。

## `starrocks_be_mem_pool_mem_usage_bytes`

- 单位: 字节
- 类型: 瞬时
- 描述: 每个内存池当前使用的总内存，以字节为单位。

## `starrocks_be_mem_pool_mem_usage_ratio`

- 单位: -
- 类型: 瞬时
- 描述：内存池的内存使用量与内存池的内存限制之比。

## `starrocks_be_mem_pool_workgroup_count`

- 单位：计数
- 类型：瞬时
- 描述：分配给每个内存池的资源组数量。

## `starrocks_be_pipe_prepare_pool_queue_len`

- 单位：计数
- 类型：瞬时
- 描述：管道准备线程池任务队列长度的瞬时值。

## `starrocks_be_priority_exec_state_report_active_threads`

- 单位：计数
- 类型：瞬时
- 描述：报告Fragment实例最终执行状态的线程池中正在执行的任务数量。

## `starrocks_be_priority_exec_state_report_queue_count`

- 单位：计数
- 类型：瞬时
- 描述：报告Fragment实例最终执行状态的线程池中排队等待的任务数量，最大值为2147483647。

## `starrocks_be_priority_exec_state_report_running_threads`

- 单位：计数
- 类型：瞬时
- 描述：报告Fragment实例最终执行状态的线程池中的线程数量，最小值为1，最大值为2。

## `starrocks_be_priority_exec_state_report_threadpool_size`

- 单位：计数
- 类型：瞬时
- 描述：报告Fragment实例最终执行状态的线程池中的最大线程数量，默认为2。

## `starrocks_be_resource_group_cpu_limit_ratio`

- 单位：-
- 类型：瞬时
- 描述：资源组CPU配额比的瞬时值。

## `starrocks_be_resource_group_cpu_use_ratio`

- 单位：-
- 类型：平均
- 描述：资源组使用的CPU时间与所有资源组CPU时间之比。

## `starrocks_be_resource_group_mem_inuse_bytes`

- 单位：字节
- 类型：瞬时
- 描述：资源组内存使用量的瞬时值。

## `starrocks_be_resource_group_mem_limit_bytes`

- 单位：字节
- 类型：瞬时
- 描述：资源组内存配额的瞬时值。

## `starrocks_be_segment_file_not_found_total`

- 单位：计数
- 描述：在段打开期间未找到段文件（文件丢失）的总次数。持续增加的值可能表示数据丢失或存储不一致。

## `starrocks_be_staros_shard_info_fallback_total`

- 单位：计数
- 类型：累积
- 描述：仅存算分离模式。BE 的 StarOSWorker 因本地缓存中没有所需的 shard 信息（即 FE 在查询 / 合并 / lake 操作引用该 shard 之前尚未将其推送到该 BE）而实际向 starmgr 发起的 RPC（`g_starlet->get_shard_info()`）总次数。仅在 starlet 就绪检查通过且 RPC 实际发出后才计入；starlet 尚未就绪的超时不包含在内。正常情况下该值应接近零。持续或上升的速率是一个强烈信号，表明 FE 端的任务或节点选择正把任务调度到尚未拥有该 shard 的 BE，或者 FE 端的 shard 推送存在滞后。建议告警：单个 BE 在 5 分钟窗口内的速率过高。

## `starrocks_be_staros_shard_info_fallback_failed_total`

- 单位：计数
- 类型：累积
- 描述：仅存算分离模式。`starrocks_be_staros_shard_info_fallback_total` 中 starmgr RPC 返回非 OK 状态的子集。可使用比率 `failed_total / fallback_total` 将 starmgr 的瞬时错误与正常的成功回退区分开来进行告警。

## `starrocks_fe_clone_task_copy_bytes`

- 单位：字节
- 类型：累积
- 描述：集群中Clone任务复制的总文件大小，包括INTER_NODE和INTRA_NODE类型。

## `starrocks_fe_clone_task_copy_duration_ms`

- 单位：毫秒
- 类型：累积
- 描述：集群中Clone任务消耗的总复制时间，包括INTER_NODE和INTRA_NODE类型。

## `starrocks_fe_clone_task_success`

- 单位：计数
- 类型：累积
- 描述：集群中成功执行的克隆任务数量。

## `starrocks_fe_clone_task_total`

- 单位：计数
- 类型：累积
- 描述：集群中克隆任务的总数。

## `starrocks_fe_last_finished_job_timestamp`

- 单位：毫秒
- 类型：瞬时
- 描述：表示特定仓库下最后一次查询或加载的结束时间。对于无共享集群，此项仅监控默认仓库。

## `starrocks_fe_memory_usage`

- 单位：字节或计数
- 类型：瞬时
- 描述：表示特定仓库下各种模块的内存统计信息。对于无共享集群，此项仅监控默认仓库。

## `starrocks_fe_meta_log_count`

- 单位：计数
- 类型：瞬时
- 描述：没有检查点的编辑日志数量。`100000`范围内的值被认为是合理的。

## `starrocks_fe_publish_version_daemon_loop_total`

- 单位：计数
- 类型：累积
- 描述：此FE节点上`publish-version-daemon`循环的总运行次数。

以下指标是`summary`类型的指标，提供事务不同阶段的延迟分布。这些指标仅由Leader FE节点报告。

每个指标包括以下输出：

- **分位数**：不同百分位边界的延迟值。这些通过`quantile`标签公开，其值可以是`0.75`、`0.95`、`0.98`、`0.99`和`0.999`。
- **`<metric_name>_sum`**：此阶段花费的总累积时间，例如`starrocks_fe_txn_total_latency_ms_sum`。
- **`<metric_name>_count`**：此阶段记录的事务总数，例如`starrocks_fe_txn_total_latency_ms_count`。

所有事务指标共享以下标签：

- `type`：按加载作业源类型（例如，`all`、`stream_load`、`routine_load`）对事务进行分类。这允许监控整体事务性能和特定加载类型的性能。报告组可以通过FE参数配置[`txn_latency_metric_report_groups`](../../FE_configuration.md#txn_latency_metric_report_groups)。
- `is_leader`：指示报告的FE节点是否为Leader。只有Leader FE (`is_leader="true"`) 报告实际指标值。Follower将具有`is_leader="false"`并报告无数据。

## `starrocks_fe_query_resource_group`

- 单位：计数
- 类型：累积
- 描述：表示在特定资源组下执行的查询总数。

## `starrocks_fe_query_resource_group`

- 单位：计数
- 类型：累积
- 描述：每个资源组的查询数量。

## `starrocks_fe_query_resource_group_err`

- 单位：计数
- 类型：累积
- 描述：表示在特定资源组下失败的查询数量。

## `starrocks_fe_query_resource_group_err`

- 单位：计数
- 类型：累积
- 描述：每个资源组的错误查询数量。

## `starrocks_fe_query_resource_group_latency`

- 单位：毫秒
- 类型：累积
- 描述：表示特定资源组下查询的延迟统计信息。

## `starrocks_fe_query_resource_group_latency`

- 单位：秒
- 类型：平均
- 描述：每个资源组的查询延迟百分位数。

## `starrocks_fe_routine_load_error_rows`

- 单位：计数
- 描述：所有例行加载作业在数据加载过程中遇到的错误行总数。

## `starrocks_fe_routine_load_jobs`

- 单位：计数
- 描述：处于不同状态的例行加载作业总数。例如：

  ```plaintext
  starrocks_fe_routine_load_jobs{state="NEED_SCHEDULE"} 0
  starrocks_fe_routine_load_jobs{state="RUNNING"} 1
  starrocks_fe_routine_load_jobs{state="PAUSED"} 0
  starrocks_fe_routine_load_jobs{state="STOPPED"} 0
  starrocks_fe_routine_load_jobs{state="CANCELLED"} 1
  starrocks_fe_routine_load_jobs{state="UNSTABLE"} 0
  ```

## `starrocks_fe_routine_load_max_lag_of_partition`

- 单位：-
- 描述：每个例行加载作业的最大 Kafka 分区偏移量滞后。仅当 FE 配置 `enable_routine_load_lag_metrics` 设置为 `true` 且偏移量滞后大于或等于 FE 配置 `min_routine_load_lag_for_metrics` 时才收集。默认情况下，`enable_routine_load_lag_metrics` 为 `false`，`min_routine_load_lag_for_metrics` 为 `10000`。

## `starrocks_fe_routine_load_max_lag_time_of_partition`

- 单位：秒
- 描述：每个例行加载作业的最大 Kafka 分区偏移时间戳滞后。仅当 FE 配置 `enable_routine_load_lag_time_metrics` 设置为 `true` 时才收集。默认情况下，`enable_routine_load_lag_time_metrics` 为 `false`。

## `starrocks_fe_routine_load_paused`

- 单位：计数
- 描述：例行加载作业暂停的总次数。

## `starrocks_fe_routine_load_receive_bytes`

- 单位：字节
- 描述：所有例行加载作业加载的数据总量。

## `starrocks_fe_routine_load_rows`

- 单位：计数
- 描述：所有例行加载作业加载的总行数。

## `starrocks_fe_safe_mode`

- 单位：-
- 类型：瞬时
- 描述：指示是否启用安全模式。有效值：`0`（禁用）和 `1`（启用）。当安全模式启用时，集群不再接受任何加载请求。

## `starrocks_fe_scheduled_pending_tablet_num`

- 单位：计数
- 类型：瞬时
- 描述：FE 调度的处于 Pending 状态的克隆任务数量，包括 BALANCE 和 REPAIR 类型。

## `starrocks_fe_scheduled_running_tablet_num`

- 单位：计数
- 类型：瞬时
- 描述：FE 调度的处于 Running 状态的克隆任务数量，包括 BALANCE 和 REPAIR 类型。

## `starrocks_fe_slow_lock_held_time_ms`

- 单位：毫秒
- 类型：摘要
- 描述：当检测到慢锁时，直方图跟踪锁持有时间（以毫秒为单位）。当锁等待时间超过 `slow_lock_threshold_ms` 配置参数时，此指标会更新。它跟踪在检测到慢锁事件时所有锁持有者中的最大锁持有时间。每个指标包括分位数（0.75、0.95、0.98、0.99、0.999）、`_sum` 和 `_count` 输出。注意：在高争用情况下，此指标可能无法准确反映确切的锁持有时间，因为一旦等待时间超过阈值，指标就会更新，但持有时间可能会继续增加，直到持有者完成其操作并释放锁。但是，即使发生死锁，此指标仍然可以更新。

## `starrocks_fe_slow_lock_wait_time_ms`

- 单位：毫秒
- 类型：摘要
- 描述：当检测到慢锁时，直方图跟踪锁等待时间（以毫秒为单位）。当锁等待时间超过 `slow_lock_threshold_ms` 配置参数时，此指标会更新。它准确跟踪在锁争用场景中线程等待获取锁的时间。每个指标包括分位数（0.75、0.95、0.98、0.99、0.999）、`_sum` 和 `_count` 输出。此指标提供精确的等待时间测量。注意：当发生死锁时，此指标无法更新，因此不能用于检测死锁情况。

## `starrocks_fe_sql_block_hit_count`

- 单位：计数
- 描述：被拦截的黑名单 SQL 的次数。

## `starrocks_fe_tablet_max_compaction_score`

- 单位：计数
- 类型：瞬时
- 描述：表示每个 BE 节点上的最高 Compaction Score。

## `starrocks_fe_tablet_num`

- 单位：计数
- 类型：瞬时
- 描述：表示每个BE节点上的tablet数量。

## `starrocks_fe_txn_publish_ack_latency_ms`

- 单位：毫秒
- 类型：汇总
- 描述：最终确认延迟，从 `ready-to-finish` 时间到事务被标记为 `VISIBLE` 的最终 `finish` 时间。此指标包括事务准备完成后的最终确认步骤。

## `starrocks_fe_txn_publish_can_finish_latency_ms`

- 单位：毫秒
- 类型：汇总
- 描述：从 `publish` 任务完成到 `canTxnFinish()` 首次返回 true 的延迟，测量时间从 `publish version finish` 到 `ready-to-finish`。

## `starrocks_fe_txn_publish_execute_latency_ms`

- 单位：毫秒
- 类型：汇总
- 描述：`publish` 任务的实际执行时间，从任务被选取到完成。此指标表示使事务更改可见所花费的实际时间。

## `starrocks_fe_txn_publish_latency_ms`

- 单位：毫秒
- 类型：汇总
- 描述：`publish` 阶段的延迟，从 `commit` 时间到 `finish` 时间。这是已提交事务对查询可见所需的时间。它是 `schedule`、`execute`、`can_finish` 和 `ack` 子阶段的总和。

## `starrocks_fe_txn_publish_schedule_latency_ms`

- 单位：毫秒
- 类型：汇总
- 描述：事务提交后等待发布的时间，从 `commit` 时间到发布任务被选取的时间。此指标反映了 `publish` 管道中的调度延迟或排队时间。

## `starrocks_fe_txn_total_latency_ms`

- 单位：毫秒
- 类型：汇总
- 描述：事务完成的总延迟，从 `prepare` 时间到 `finish` 时间。此指标表示事务的完整端到端持续时间。

## `starrocks_fe_txn_write_latency_ms`

- 单位：毫秒
- 类型：汇总
- 描述：事务 `write` 阶段的延迟，从 `prepare` 时间到 `commit` 时间。此指标隔离了事务准备发布之前的数据写入和准备阶段的性能。

## `starrocks_fe_unfinished_backup_job`

- 单位：计数
- 类型：瞬时
- 描述：表示特定仓库下正在运行的BACKUP任务数量。对于无共享集群，此项仅监控默认仓库。对于共享数据集群，此值始终为 `0`。

## `starrocks_fe_unfinished_query`

- 单位：计数
- 类型：瞬时
- 描述：表示特定仓库下当前正在运行的查询数量。对于无共享集群，此项仅监控默认仓库。

## `starrocks_fe_unfinished_restore_job`

- 单位：计数
- 类型：瞬时
- 描述：表示特定仓库下正在运行的RESTORE任务数量。对于无共享集群，此项仅监控默认仓库。对于共享数据集群，此值始终为 `0`。

## `storage_page_cache_mem_bytes`

- 单位：字节
- 描述：存储页缓存使用的内存。

## `stream_load`

- 单位：-
- 描述：总加载行数和接收字节数。

## `stream_load_pipe_count`

- 单位：计数
- 描述：当前正在运行的Stream Load任务数量。

## `streaming_load_bytes`

- 单位：字节
- 描述：Stream Load加载的总字节数。

## `streaming_load_current_processing`

- 单位: 计数
- 描述: 当前正在运行的Stream Load任务数量。

## `streaming_load_duration_ms`

- 单位: 毫秒
- 描述: Stream Load的总耗时。

## `streaming_load_requests_total`

- 单位: 计数
- 描述: Stream Load请求的总数量。

##### 分割

## `tablet_base_max_compaction_score`

- 单位: -
- 描述: 此BE中tablet的最高基础合并分数。

## `tablet_cumulative_max_compaction_score`

- 单位: -
- 描述: 此BE中tablet的最高累积合并分数。

## `tablet_metadata_mem_bytes`

- 单位: 字节
- 描述: tablet元数据使用的内存。

## `tablet_schema_mem_bytes`

- 单位: 字节
- 描述: tablet schema使用的内存。

## `tablet_update_max_compaction_score`

- 单位: -
- 描述: 当前BE中主键表tablet的最高合并分数。

## `thrift_connections_total`

- 单位: 计数
- 描述: thrift连接的总数量（包括已完成的连接）。

## `thrift_current_connections (Deprecated)`

## `thrift_opened_clients`

- 单位: 计数
- 描述: 当前打开的thrift客户端数量。

## `thrift_used_clients`

- 单位: 计数
- 描述: 当前正在使用的thrift客户端数量。

## `total_column_pool_bytes (Deprecated)`

## `transaction_streaming_load_bytes`

- 单位: 字节
- 描述: 事务加载的总加载字节数。

## `transaction_streaming_load_current_processing`

- 单位: 计数
- 描述: 当前正在运行的事务性Stream Load任务数量。

## `transaction_streaming_load_duration_ms`

- 单位: 毫秒
- 描述: Stream Load事务接口的总耗时。

## `transaction_streaming_load_requests_total`

- 单位: 计数
- 描述: 事务加载请求的总数量。

## `txn_request`

- 单位: -
- 描述: BEGIN、COMMIT、ROLLBACK和EXEC的事务请求。

## `uint8_column_pool_bytes`

- 单位: 字节
- 描述: UINT8列池使用的字节数。

## `unused_rowsets_count`

- 单位: 计数
- 描述: 未使用的rowset总数量。请注意，这些rowset稍后将被回收。

## `update_apply_queue_count`

- 单位: 计数
- 描述: 主键表事务APPLY线程池中排队的任务数量。

## `update_compaction_duration_us`

- 单位: 微秒
- 描述：主键表压缩所花费的总时间。

## `update_compaction_outputs_bytes_total`

- 单位：字节
- 描述：主键表压缩写入的总字节数。

## `update_compaction_outputs_total`

- 单位：计数
- 描述：主键表压缩的总次数。

## `update_compaction_task_byte_per_second`

- 单位：字节/秒
- 描述：主键表压缩的估计速率。

## `update_compaction_task_cost_time_ns`

- 单位：纳秒
- 描述：主键表压缩所花费的总时间。

## `update_del_vector_bytes_total`

- 单位：字节
- 描述：主键表中用于缓存 DELETE 向量的总内存。

## `update_del_vector_deletes_new`

- 单位：计数
- 描述：主键表中使用的最新生成的 DELETE 向量总数。

## `update_del_vector_deletes_total (Deprecated)`

## `update_del_vector_dels_num (Deprecated)`

## `update_del_vector_num`

- 单位：计数
- 描述：主键表中 DELETE 向量缓存项的数量。

## `update_mem_bytes`

- 单位：字节
- 描述：主键表 APPLY 任务和主键索引使用的内存。

## `update_primary_index_bytes_total`

- 单位：字节
- 描述：主键索引的总内存开销。

## `update_primary_index_num`

- 单位：计数
- 描述：内存中缓存的主键索引数量。

## `update_rowset_commit_apply_duration_us`

- 单位：微秒
- 描述：主键表 APPLY 任务所花费的总时间。

## `update_rowset_commit_apply_total`

- 单位：计数
- 描述：主键表的 COMMIT 和 APPLY 总数。

## `update_rowset_commit_request_failed`

- 单位：计数
- 描述：主键表中失败的行集 COMMIT 请求总数。

## `update_rowset_commit_request_total`

- 单位：计数
- 描述：主键表中行集 COMMIT 请求总数。

## `wait_base_compaction_task_num`

- 单位：计数
- 描述：等待执行的基础压缩任务数量。

## `wait_cumulative_compaction_task_num`

- 单位：计数
- 描述：等待执行的累积压缩任务数量。

## `writable_blocks_total (Deprecated)`
