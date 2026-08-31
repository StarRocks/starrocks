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

## `tablet_merge_sstable_fallback_cohort_mismatch_total`

- 单位: 计数
- 描述: 因各源 tablet 的 SST 队列在数量、顺序或语义元数据上不一致而选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_fallback_duplicate_physical_file_total`

- 单位: 计数
- 描述: 因候选 SST 队列中存在重复物理 SST 文件名而选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_fallback_embedded_delvec_total`

- 单位: 计数
- 描述: 因 SST 投影后无法解析所需内嵌删除向量而选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_fallback_nonuniform_mapping_total`

- 单位: 计数
- 描述: 因源到目标的 RSSID 映射或所有权不满足元数据安全复用的一致性要求而选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_fallback_projected_domain_total`

- 单位: 计数
- 描述: 因投影后的 SST 所有者、RSSID 偏移或水位超出可复用的存活域或支持域而选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_fallback_rowset_layout_mismatch_total`

- 单位: 计数
- 描述: 因各源 rowset 的物理布局不一致而选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_fallback_shared_or_mixed_total`

- 单位: 计数
- 描述: 因源 SST 为共享所有权或混合所有权而无法安全复用元数据并选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_fallback_unsupported_sst_form_total`

- 单位: 计数
- 描述: 因源范围或 SST 元数据不符合支持的复用形式而选择延迟重建主键索引的 tablet 合并累计次数。

## `tablet_merge_sstable_meta_identical_total`

- 单位: 计数
- 描述: 在合并后 tablet 元数据中复用一组完整且完全相同的继承 SST 队列的 tablet 合并累计次数。

## `tablet_merge_sstable_meta_lazy_rebuild_total`

- 单位: 计数
- 描述: 省略源 SST 元数据，使下一次加载主键索引的操作或使用方重建该索引的 tablet 合并累计次数。

## `tablet_merge_sstable_meta_private_total`

- 单位: 计数
- 描述: 投影并复用一组完整私有源 SST 元数据的 tablet 合并累计次数。

## `tablet_merge_sstable_omitted_bytes_total`

- 单位: Bytes
- 描述: 延迟重建回退期间从合并索引元数据中省略并记录为孤立文件的唯一源 SST 文件累计大小。

## `tablet_merge_sstable_omitted_file_total`

- 单位: 计数
- 描述: 延迟重建回退期间从合并索引元数据中省略并记录为孤立文件的唯一源 SST 文件累计数量。

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

## `vector_index_cache_async_load_failure`

- 类型：累计
- 单位：计数
- 描述：已经开始执行、但在加载或写入缓存过程中失败的向量索引缓存后台加载任务累计数。不包括执行前被取消的任务。

## `vector_index_cache_async_load_inflight`

- 类型：瞬时
- 单位：计数
- 描述：当前正在后台 worker 中运行的向量索引缓存加载任务数。

## `vector_index_cache_async_load_ns`

- 类型：累计
- 单位：纳秒
- 描述：已经开始执行的向量索引缓存后台加载任务累计执行时间，包括成功和失败的任务，不包括队列等待时间和被拒绝的任务。

## `vector_index_cache_async_load_queued`

- 类型：瞬时
- 单位：计数
- 描述：后台线程池已经接受、但尚未开始运行的向量索引缓存加载任务数。

## `vector_index_cache_async_load_rejected`

- 类型：累计
- 单位：计数
- 描述：执行前被拒绝的向量索引缓存后台加载请求累计数。例如，缓存容量为零、线程池已经停止或任务队列无法接受任务时，该指标会增加。

## `vector_index_cache_async_load_success`

- 类型：累计
- 单位：计数
- 描述：成功加载索引并将其写入缓存的后台任务累计数。如果缓存无法继续保留该条目，容量淘汰可能会立即移除已经成功写入的索引。

## `vector_index_cache_loading_wait_timeout`

- 类型：累计
- 单位：计数
- 描述：同步缓存调用方等待正在进行的向量索引加载达到 `vector_index_cache_loading_wait_timeout_ms` 的累计次数。该指标按调用方而不是唯一索引计数；等待超时后，已经开始的 loader 会继续执行。

## `wait_base_compaction_task_num`

- 单位：计数
- 描述：等待执行的基础压缩任务数量。

## `wait_cumulative_compaction_task_num`

- 单位：计数
- 描述：等待执行的累积压缩任务数量。

## `writable_blocks_total (Deprecated)`
