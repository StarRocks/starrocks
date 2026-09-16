---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical t - z"
---

# 指标 t 到 z

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

## `threadpool_task_exception_total`

- 单位: 计数
- 描述: BE 进程内所有 ThreadPool 工作线程捕获并吞掉的任务异常累计次数。仅当 [`enable_threadpool_catch_task_exception`](../../../configuration/BE_parameters/log_server_meta.md#enable_threadpool_catch_task_exception) 为 `true` 时才会增加；该配置为 `false`（默认）时没有外层 catch，该指标不会变化。可在开启 catch 模式时用于告警；具体线程池名称和异常详情仍记录在 BE ERROR 日志中。

## `thrift_connections_total`

- 单位: 计数
- 描述: thrift连接的总数量（包括已完成的连接）。

## `thrift_current_connections (Deprecated)`

## `thrift_opened_clients`

- 单位: 计数
- 描述: 当前打开的thrift客户端数量。

<<<<<<< HEAD
=======
## `thrift_server_acceptor_stall_ms`

- 单位: 毫秒
- 类型: 瞬时
- 描述: FE Thrift 接收循环（accept loop）上次返回连接至今的时长（毫秒）。接收循环卡住会使该值上升，但没有 Thrift 流量的 FE 同样会上升，因此应结合连接到达速率一起判断，不要仅凭该指标告警。此外，当接收循环根本没有运行时（Thrift 服务器启动之前、停止之后）该值同样为 `0`，因此基于阈值的告警无法区分 Thrift 服务已停止与服务健康这两种情况。

## `thrift_server_expired_connections_total`

- 单位：计数
- 类型：累积
- 描述：因在等待队列中的滞留时间超过 `thrift_server_queue_timeout_ms` 而被 FE Thrift 服务器直接关闭、未予处理的连接总数。该检查默认关闭，因此在运维人员启用该超时之前，本指标恒为 0。该值上升表示 FE 当时正在处理一批调用方几乎肯定已经放弃的积压连接；可结合 `thrift_server_queue_wait_ms` 判断队列落后的程度。

## `thrift_server_queue_wait_ms`

- 单位：毫秒
- 类型：瞬时
- 描述：连接在被工作线程取走之前，在 FE Thrift 服务器等待队列中滞留时长的分位数。每个出队连接都会采样，包括随后因超时而被丢弃的连接，因此该分布不会被 `thrift_server_queue_timeout_ms` 截断。这是 Thrift 饱和的先行指标：在工作线程池尚能跟上时它就会上升，远早于 `thrift_server_rejected_connections_total` 开始变化。

## `thrift_server_rejected_connections_total`

- 单位: 计数
- 类型: 累积
- 描述: FE Thrift 服务器因工作线程池饱和而立即关闭的连接总数。每次拒绝都会计数，包括因日志限流而未打印告警的那些。该值上升表示客户端正在被拒绝；请结合 `thrift-server-pool` 的 `thread_pool` 指标一起查看，其增长速率可反映工作线程容量的缺口。

>>>>>>> 3323784 ([BugFix] FailFast: drop stale queued thrift connections (#79173))
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
