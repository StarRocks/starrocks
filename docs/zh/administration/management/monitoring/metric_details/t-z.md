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
