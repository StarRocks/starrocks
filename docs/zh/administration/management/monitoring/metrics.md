---
displayed_sidebar: docs
---

# 通用监控指标

本文介绍了监控 StarRocks 的重要通用指标。

有关物化视图和存算分离集群专属监控指标，请参考对应章节：

- [异步物化视图监控项](./metrics-materialized_view.md)
- [存算分离集群监控项](./metrics-shared-data.md)

关于为您的 StarRocks 集群设置监控报警服务的详细说明，请参阅 [监控警报](./Monitor_and_Alert.md)。

## 监控项说明

### be_broker_count

- 单位：个
- 类型：平均值
- 描述：Broker的数量。

### be_brpc_endpoint_count

- 单位：个
- 类型：平均值
- 描述：bRPC 中 StubCache 的数量。

### be_bytes_read_per_second

- 单位：Byte/秒
- 类型：平均值
- 描述：BE 读取速度。

### be_bytes_written_per_second

- 单位：Byte/秒
- 类型：平均值
- 描述：BE 写入速度。

### be_base_compaction_bytes_per_second

- 单位：Byte/秒
- 类型：平均值
- 描述：BE 的基线合并速率。

### be_cumulative_compaction_bytes_per_second

- 单位：Byte/秒
- 类型：平均值
- 描述：BE 的增量合并速率。

### be_base_compaction_rowsets_per_second

- 单位：个/秒
- 类型：平均值
- 描述：BE 的基线合并 rowsets 合并速率。

### be_cumulative_compaction_rowsets_per_second

- 单位：个/秒
- 类型：平均值
- 描述：BE 的增量合并 rowsets 合并速率。

### be_base_compaction_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 基线合并失败。

### be_clone_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 克隆失败

### be_create_rollup_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 创建物化视图失败。

### be_create_tablet_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 创建 tablet 失败。

### be_cumulative_compaction_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 增量合并失败。

### be_delete_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 删除失败。

### be_finish_task_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE task 失败。

### be_publish_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 版本发布失败。

### be_report_tables_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 表上报失败。

### be_report_disk_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 磁盘上报失败。

### be_report_tablet_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 分片上报失败。

### be_report_task_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 任务上报失败。

### be_schema_change_failed

- 单位：个/秒
- 类型：平均值
- 描述：BE 修改表结构失败。

### be_base_compaction_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 基线合并请求。

### be_clone_total_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 克隆请求。

### be_create_rollup_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 创建物化视图请求。

### be_create_tablet_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 创建分片请求。

### be_cumulative_compaction_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 增量合并请求。

### be_delete_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 删除请求。

### be_finish_task_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 完成任务请求。

### be_publish_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 版本发布请求。

### be_report_tablets_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 分片上报请求。

### be_report_disk_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 磁盘上报请求。

### be_report_tablet_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 任务上报请求。

### be_report_task_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 任务上报请求。

### be_schema_change_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 表结构修改请求。

### be_storage_migrate_requests

- 单位：个/秒
- 类型：平均值
- 描述：BE 迁移请求。

### be_fragment_endpoint_count

- 单位：个
- 类型：平均值
- 描述：BE DataStream 数量。

### be_fragment_request_latency_avg

- 单位：m s
- 类型：平均值
- 描述：fragment 请求响应时间。

### be_fragment_requests_per_second

- 单位：个/秒
- 类型：平均值
- 描述：fragment 请求数。

### be_http_request_latency_avg

- 单位：毫秒
- 类型：平均值
- 描述：HTTP 请求响应时间。

### be_http_requests_per_second

- 单位：个/秒
- 类型：平均值
- 描述：HTTP 请求数。

### be_http_request_send_bytes_per_second

- 单位：Byte/秒
- 类型：平均值
- 描述：HTTP 请求发送字节数。

### fe_connections_per_second

- 单位：个/秒
- 类型：平均值
- 描述：FE 的新增连接速率。

### fe_connection_total

- 单位：个
- 类型：累计值
- 描述：FE 的总连接数量。

### fe_edit_log_read

- 单位：个/秒
- 类型：平均值
- 描述：FE edit log 读取速率。

### fe_edit_log_size_bytes

- 单位：Byte/秒
- 类型：平均值
- 描述：FE edit log 大小。

### fe_edit_log_write

- 单位：Byte/秒
- 类型：平均值
- 描述：FE edit log 写入速率。

### fe_checkpoint_push_per_second

- 单位：个/秒
- 类型：平均值
- 描述：FE checkpoint 数。

### fe_pending_hadoop_load_job

- 单位：个
- 类型：平均值
- 描述：Pending 的 hadoop job 数量。

### fe_committed_hadoop_load_job

- 单位：个
- 类型：平均值
- 描述：提交的 hadoop job 数量。

### fe_loading_hadoop_load_job

- 单位：个
- 类型：平均值
- 描述：加载中的 hadoop job 数量。

### fe_finished_hadoop_load_job

- 单位：个
- 类型：平均值
- 描述：完成的 hadoop job 数量。

### fe_cancelled_hadoop_load_job

- 单位：个
- 类型：平均值
- 描述：取消的 hadoop job 数量。

### fe_pending_insert_load_job

- 单位：个
- 类型：平均值
- 描述：Pending 的 insert job 数量。

### fe_loading_insert_load_job

- 单位：个
- 类型：平均值
- 描述：提交的 insert job 数量。

### fe_committed_insert_load_job

- 单位：个
- 类型：平均值
- 描述：加载中的 insert job 数量。

### fe_finished_insert_load_job

- 单位：个
- 类型：平均值
- 描述：完成的 insert job 数量。

### fe_cancelled_insert_load_job

- 单位：个
- 类型：平均值
- 描述：取消的 insert job 数量。

### fe_pending_broker_load_job

- 单位：个
- 类型：平均值
- 描述：Pending 的 broker job 数量。

### fe_loading_broker_load_job

- 单位：个
- 类型：平均值
- 描述：提交的 broker job 数量。

### fe_committed_broker_load_job

- 单位：个
- 类型：平均值
- 描述：加载中的 broker job 数量。

### fe_finished_broker_load_job

- 单位：个
- 类型：平均值
- 描述：完成的 broker job 数量。

### fe_cancelled_broker_load_job

- 单位：个
- 类型：平均值
- 描述：取消的 broker job 数量。

### fe_pending_delete_load_job

- 单位：个
- 类型：平均值
- 描述：Pending 的 delete job 数量。

### fe_loading_delete_load_job

- 单位：个
- 类型：平均值
- 描述：提交的 delete job 数量。

### fe_committed_delete_load_job

- 单位：个
- 类型：平均值
- 描述：加载中的 delete job 数量。

### fe_finished_delete_load_job

- 单位：个
- 类型：平均值
- 描述：完成的 delete job 数量。

### fe_cancelled_delete_load_job

- 单位：个
- 类型：平均值
- 描述：取消的 delete job 数量。

### fe_rollup_running_alter_job

- 单位：个
- 类型：平均值
- 描述：rollup 创建中的 job 数量。

### fe_schema_change_running_job

- 单位：个
- 类型：平均值
- 描述：表结构变更中的 job 数量。

### cpu_util

- 单位：-
- 类型：平均值
- 描述：CPU 百分比使用率。

### cpu_system

- 单位：-
- 类型：平均值
- 描述：cpu_system 百分比使用率。

### cpu_user

- 单位：-
- 类型：平均值
- 描述：cpu_user 百分比使用率。

### cpu_idle

- 单位：-
- 类型：平均值
- 描述：cpu_idle 百分比使用率。

### cpu_guest

- 单位：-
- 类型：平均值
- 描述：cpu_guest 百分比使用率。

### cpu_iowait

- 单位：-
- 类型：平均值
- 描述：cpu_iowait 百分比使用率。

### cpu_irq

- 单位：-
- 类型：平均值
- 描述：cpu_irq 百分比使用率。

### cpu_nice

- 单位：-
- 类型：平均值
- 描述：cpu_nice 百分比使用率。

### cpu_softirq

- 单位：-
- 类型：平均值
- 描述：cpu_softirq 百分比使用率。

### cpu_steal

- 单位：-
- 类型：平均值
- 描述：cpu_steal 百分比使用率。

### disk_free

- 单位：Byte
- 类型：平均值
- 描述：空闲磁盘容量。

### disk_io_svctm

- 单位：毫秒
- 类型：平均值
- 描述：磁盘 IO 服务时间。

### disk_io_util

- 单位：-
- 类型：平均值
- 描述：磁盘百分比使用率。

### disk_used

- 单位：Byte
- 类型：平均值
- 描述：已用磁盘容量。

### encryption_keys_created

- 单位：个
- 类型：累计值
- 描述：加密文件时创建密钥的次数

### encryption_keys_unwrapped

- 单位：个
- 类型：累计值
- 描述：解密文件时解码密钥的次数

### encryption_keys_in_cache

- 单位：个
- 类型：瞬时值
- 描述：当前密钥缓存中密钥的个数

### starrocks_fe_query_resource_group

- 单位：个
- 类型：累计值
- 描述：该资源组中查询任务的数量

### starrocks_fe_query_resource_group_latency

- 单位：秒
- 类型：平均值
- 描述：该资源组的查询延迟百分位数

### starrocks_fe_query_resource_group_err

- 单位：个
- 类型：累计值
- 描述：该资源组中报错的查询任务的数量

### starrocks_fe_meta_log_count

- 单位：个
- 类型：瞬时值
- 描述：未做 Checkpoint 的 Edit Log 数量，该值在 `100000` 以内为合理

### starrocks_be_resource_group_cpu_limit_ratio

- 单位：-
- 类型：瞬时值
- 描述：该资源组 CPU 百分比配额比率的瞬时值

### starrocks_be_resource_group_cpu_use_ratio

- 单位：-
- 类型：平均值
- 描述：该资源组 CPU 使用时间占所有资源组 CPU 时间的百分比

### starrocks_be_resource_group_mem_limit_bytes

- 单位：Byte
- 类型：瞬时值
- 描述：该资源组内存配额比率的瞬时值

### starrocks_be_resource_group_mem_allocated_bytes

- 单位：Byte
- 类型：瞬时值
- 描述：该资源组内存使用率瞬时值

### starrocks_be_pipe_prepare_pool_queue_len

- 单位：个
- 类型：瞬时值
- 描述：Pipeline 准备线程池任务队列长度的瞬时值

### starrocks_fe_safe_mode

- 单位：-
- 类型：瞬时值
- 描述：Safe Mode 是否被开启 。取值：`0`（关闭）或 `1`（开启）。Safe Mode 开启时，集群不再接受任何数据导入请求。

### starrocks_fe_unfinished_backup_job

- 单位：个
- 类型：瞬时值
- 描述：Warehouse 下运行中的 BACKUP 任务的数量。对于存算一体集群，此项仅监控 Default Warehouse。对于存算分离集群，该值始终为 `0`。

### starrocks_fe_unfinished_restore_job

- 单位：个
- 类型：瞬时值
- 描述：Warehouse 下运行中的 RESTORE 任务的数量。对于存算一体集群，此项仅监控 Default Warehouse。对于存算分离集群，该值始终为 `0`。

### starrocks_fe_memory

- 单位：Byte 或个
- 类型：瞬时值
- 描述：Warehouse 下各个模块内存统计信息。对于存算一体集群，此项仅监控 Default Warehouse。

### starrocks_fe_unfinished_query

- 单位：个
- 类型：瞬时值
- 描述：Warehouse 下运行中的查询数量。对于存算一体集群，此项仅监控 Default Warehouse。

### starrocks_fe_last_finished_job_timestamp

- 单位：毫秒
- 类型：瞬时值
- 描述：Warehouse 下最后一次查询或导入的结束时间。对于存算一体集群，此项仅监控 Default Warehouse。

### starrocks_fe_query_resource_group

- 单位：个
- 类型：累计值
- 描述：Resource Group 下执行的查询总量。

### starrocks_fe_query_resource_group_err

- 单位：个
- 类型：累计值
- 描述：Resource Group 下失败的查询数量。

### starrocks_fe_query_resource_group_latency

- 单位：毫秒
- 类型：累计值
- 描述：Resource Group 下查询的延时统计信息。

### starrocks_fe_tablet_num

- 单位：个
- 类型：瞬时值
- 描述：每个 BE 节点上的 Tablet 数量。

### starrocks_fe_tablet_max_compaction_score

- 单位：个
- 类型：瞬时值
- 描述：每个 BE 节点上最高的 Compaction Score。

### update_compaction_outputs_total

- 单位：个
- 描述：主键表 Compaction 时的总数量。

### update_del_vector_bytes_total

- 单位：Byte
- 描述：主键表 DELETE Vector 缓存的内存成本。

### push_request_duration_us

- 单位：微秒
- 描述：Spark Load 总耗时。

### writable_blocks_total（已弃用）

### disks_data_used_capacity

- 描述：每个磁盘（由存储路径表示）的已用容量。

### query_scan_rows

- 单位：个
- 描述：扫描的总行数。

### update_primary_index_num

- 单位：个
- 描述：内存中主键索引的数量。

### result_buffer_block_count

- 单位：个
- 描述：结果缓存中的 Block 数。

### query_scan_bytes

- 单位：Byte
- 描述：扫描的总字节数。

### disk_reads_completed

- 单位：个
- 描述：成功完成的磁盘读取次数。

### query_cache_hit_count

- 单位：个
- 描述：查询命中缓存次数。

### jemalloc_resident_bytes

- 单位：Byte
- 描述：Allocator 映射的物理驻留数据页中的最大字节数，包括 Allocator 元数据、活跃分配和未使用的 Dirty Page。

### blocks_open_writing（已弃用）

### disk_io_time_weigthed

- 单位：毫秒
- 描述：I/O 操作的加权时间。

### update_compaction_task_byte_per_second

- 单位：Byte/秒
- 描述：主键表 Compaction 任务的速率（估计值）。

### blocks_open_reading（已弃用）

### tablet_update_max_compaction_score

- 单位：-
- 描述：当前 BE 中主键表中各个 Tablet 的最高 Compaction Score。

### segment_read

- 单位：个
- 描述：Segment 读取的总次数。

### disk_io_time_ms

- 单位：毫秒
- 描述：I/O 操作的总时间。单位：毫秒。

### load_mem_bytes

- 单位：Byte
- 描述：数据导入的内存成本。

### delta_column_group_get_non_pk_total

- 单位：个
- 描述：获取 Delta Column Group 的总次数（非主键表）。

### query_scan_bytes_per_second

- 单位：Byte/秒
- 描述：每秒扫描字节数（估计值）。

### active_scan_context_count

- 单位：个
- 描述：Flink/Spark SQL 创建的扫描任务总数。

### fd_num_limit

- 单位：个
- 描述：文件描述符的最大数量。

### update_compaction_task_cost_time_ns

- 单位：纳秒
- 描述：主键表 Compaction 任务的总耗时。

### delta_column_group_get_hit_cache

- 单位：个
- 描述：主键表中 Delta Column Group 缓存的总命中次数。

### data_stream_receiver_count

- 单位：个
- 描述：在 BE 中充当 Exchange Receiver 的实例的累积数量。

### bytes_written_total

- 单位：Byte
- 描述：总写入字节数（Sector Write * 512）。

### transaction_streaming_load_bytes

- 单位：Byte
- 描述：通过 Stream Load 事务接口导入的总字节数。

### running_cumulative_compaction_task_num

- 单位：个
- 描述：运行中的 Cumulative Compaction 任务总数。

### transaction_streaming_load_requests_total

- 单位：个
- 描述：事务导入请求的总数量。

### cpu

- 单位：-
- 描述：`/proc/stat` 返回的 CPU 使用信息。

### update_del_vector_num

- 单位：个
- 描述：主键表中 DELETE Vector 缓存项的数量。

### disks_avail_capacity

- 描述：磁盘的可用容量。

### clone_mem_bytes

- 单位：Byte
- 描述：Tablet Clone 的内存使用量。

### fragment_requests_total

- 单位：个
- 描述：在 BE 上执行的非 Pipeline Engine 的 Fragment 实例总数。

### disk_write_time_ms

- 单位：毫秒
- 描述：磁盘写入的总时间。单位：毫秒。

### schema_change_mem_bytes

- 单位：Byte
- 描述：Schema Change 的内存使用量。

### thrift_connections_total

- 单位：个
- 描述：Thrift 连接的总数（包括已完成的连接）。

### thrift_opened_clients

- 单位：个
- 描述：当前已打开的 Thrift 客户端数。

### thrift_used_clients

- 单位：个
- 描述：当前使用的 Thrift 客户端数。

### thrift_current_connections（已弃用）

### disk_bytes_read

- 单位：Byte
- 描述：从磁盘读取的总字节数（Sector Read * 512）。

### base_compaction_task_cost_time_ms

- 单位：毫秒
- 描述：Base Compaction 任务的总耗时。

### update_primary_index_bytes_total

- 单位：Byte
- 描述：主键索引的总内存成本。

### compaction_deltas_total

- 单位：个
- 描述：Base Compaction 和 Cumulative Compaction 中合并的 Rowset 的总数。

### max_network_receive_bytes_rate

- 单位：Byte
- 描述：通过网络接收的总字节数（所有网络接口中的最大值）。

### max_network_send_bytes_rate

- 单位：Byte
- 描述：通过网络发送的总字节数（所有网络接口中的最大值）。

### chunk_allocator_mem_bytes

- 单位：Byte
- 描述：Chunk Allocator 使用的内存。

### query_cache_usage_ratio

- 单位：-
- 描述：当前 Query Cache 使用比例。

### process_fd_num_limit_soft

- 单位：个
- 描述：文件描述符最大数量的软限制。请注意，此项表示软限制。您可以使用 `ulimit` 命令设置硬限制。

### query_cache_hit_ratio

- 单位：-
- 描述：Query Cache命中比率。

### tablet_metadata_mem_bytes

- 单位：Byte
- 描述：Tablet 元数据使用的内存。

### jemalloc_retained_bytes

- 单位：Byte
- 描述：通过保留而不是通过操作（如 munmap(2)）返回到操作系统的虚拟内存映射的总字节数。

### unused_rowsets_count

- 单位：个
- 描述：未使用的 Rowset 的总数。请注意，此后系统将会回收这些 Rowset 。

### update_rowset_commit_apply_total

- 单位：个
- 描述：主键表的 COMMIT 和 APPLY 的总数。

### segment_zonemap_mem_bytes

- 单位：Byte
- 描述：Segment Zonemap 使用的内存。

### base_compaction_task_byte_per_second

- 单位：Byte/秒
- 描述：Base Compaction 的速率（估计值）。

### transaction_streaming_load_duration_ms

- 单位：毫秒
- 描述：事务导入的总时间。

### memory_pool_bytes_total

- 单位：Byte
- 描述：内存池使用的内存。

### short_key_index_mem_bytes

- 单位：Byte
- 描述：Short Key 索引使用的内存。

### disk_bytes_written

- 单位：Byte
- 描述：写入磁盘的总字节数。

### segment_flush_queue_count

- 单位：个
- 描述：Segment Flush 线程池中排队的任务数量。

### jemalloc_metadata_bytes

- 单位：Byte
- 描述：用于元数据的总字节数，包括用于 Bootstrap-sensitive Allocator 元数据结构和内部分配的基本分配。此项不包括 Transparent Huge Pages 的使用。

### network_send_bytes

- 单位：Byte
- 描述：通过网络发送的字节数。

### update_mem_bytes

- 单位：Byte
- 描述：主键表 APPLY 任务和主键索引使用的内存。

### blocks_created_total（已弃用）

### query_cache_usage

- 单位：Byte
- 描述：当前 Query Cache 使用的字节数。

### memtable_flush_duration_us

- 单位：微秒
- 描述：Memtable Flush 的总时间。

### push_request_write_bytes

- 单位：Byte
- 描述：通过 Spark Load 写入的总字节数。

### jemalloc_active_bytes

- 单位：Byte
- 描述：应用分配的活动页面中的总字节数。

### load_rpc_threadpool_size

- 单位：个
- 描述：当前导入 RPC 线程池大小。该线程池仅用于处理 Routine Load 导入和表函数 导入。默认值为 10 ，最大值为 1000。该值将根据线程池使用情况动态调整。

### publish_version_queue_count

- 单位：个
- 描述：Publish Version 线程池中的排队任务数。

### chunk_pool_system_free_count

- 单位：个
- 描述：内存块分配/缓存指标。

### chunk_pool_system_free_cost_ns

- 单位：纳秒
- 描述：内存块分配/缓存指标。

### chunk_pool_system_alloc_count

- 单位：个
- 描述：内存块分配/缓存指标。

### chunk_pool_system_alloc_cost_ns

- 单位：纳秒
- 描述：内存块分配/缓存指标。

### chunk_pool_local_core_alloc_count

- 单位：个
- 描述：内存块分配/缓存指标。

### chunk_pool_other_core_alloc_count

- 单位：个
- 描述：内存块分配/缓存指标。

### fragment_endpoint_count

- 单位：个
- 描述：在 BE 中充当 Exchange Sender 的实例数（累积值）。

### disk_sync_total（已弃用）

### max_disk_io_util_percent

- 单位：-
- 描述：最大磁盘 I/O 利用率百分比。

### network_receive_bytes

- 单位：Byte
- 描述：通过网络接收的总字节数。

### storage_page_cache_mem_bytes

- 单位：Byte
- 描述：Storage Page Cache 使用的内存。

### jit_cache_mem_bytes

- 单位：Byte
- 描述：jit 编译函数使用的内存。

### column_partial_update_apply_total

- 单位：个
- 描述：列的 Partial Update （列模式）的 APPLY 的总数。

### disk_writes_completed

- 单位：个
- 描述：成功完成的磁盘写入次数。

### memtable_flush_total

- 单位：个
- 描述：Memtable Flush 的总次数。

### page_cache_hit_count

- 单位：个
- 描述：Storage Page Cache 中的命中总次数。

### bytes_read_total（已弃用）

### update_rowset_commit_request_total

- 单位：个
- 描述：主键表中 RowsetCOMMIT 请求的总数。

### tablet_cumulative_max_compaction_score

- 单位：-
- 描述：此 BE 中各 Tablet 的最高 Cumulative Compaction Score。

### column_zonemap_index_mem_bytes

- 单位：Byte
- 描述：Column Zonemap 索引使用的内存。

### push_request_write_bytes_per_second

- 单位：Byte/秒
- 描述：Spark Load 的写入速率。

### process_thread_num

- 单位：个
- 描述：此进程中的线程总数。

### query_cache_lookup_count

- 单位：个
- 描述：Query Cache 查询的总次数。

### http_requests_total（已弃用）

### http_request_send_bytes（已弃用）

### cumulative_compaction_task_cost_time_ms

- 单位：毫秒
- 描述：Cumulative Compaction 任务的总耗时。

### column_metadata_mem_bytes

- 单位：Byte
- 描述：列元数据使用的内存。

### plan_fragment_count

- 单位：个
- 描述：当前运行的查询计划 Fragment 数量。

### page_cache_lookup_count

- 单位：个
- 描述：Storage Page Cache 查询的总次数。

### query_mem_bytes

- 单位：Byte
- 描述：查询使用的内存。

### load_channel_count

- 单位：个
- 描述：导入通道的总数。

### push_request_write_rows

- 单位：个
- 描述：通过 Spark Load 写入的总行数。

### running_base_compaction_task_num

- 单位：个
- 描述：运行中的 Base Compaction 任务总数。

### fragment_request_duration_us

- 单位：微秒
- 描述：Fragment 实例的累积执行时间（非 Pipeline Engine ）。

### process_mem_bytes

- 单位：Byte
- 描述：此进程使用的内存。

### broker_count

- 单位：个
- 描述：创建的文件系统 Broker 的总数（按主机地址计）。

### segment_replicate_queue_count

- 单位：个
- 描述：Segment Replicate 线程池中排队的任务数。

### brpc_endpoint_stub_count

- 单位：个
- 描述：bRPC Stub 的总数（按主机地址计）。

### readable_blocks_total（已弃用）

### delta_column_group_get_non_pk_hit_cache

- 单位：个
- 描述：非主键表中 Delta Column Group 缓存的命中总次数。

### update_del_vector_deletes_new

- 单位：个
- 描述：主键表中新生成的 DELETE 向量的总数。

### compaction_bytes_total

- 单位：Byte
- 描述：Base Compaction 和 Cumulative Compaction 中合并的总字节数。

### segment_metadata_mem_bytes

- 单位：Byte
- 描述：Segment 元数据使用的内存。

### column_partial_update_apply_duration_us

- 单位：微秒
- 描述：列的 Partial Update（列模式）的 APPLY 任务的总时间。

### bloom_filter_index_mem_bytes

- 单位：Byte
- 描述：Bloom Filter 使用的内存。

### routine_load_task_count

- 单位：个
- 描述：当前运行的 Routine Load 任务的数量。

### delta_column_group_get_total

- 单位：个
- 描述：获取主键表 Delta Column Group 的总次数。

### disk_read_time_ms

- 单位：毫秒
- 描述：从磁盘读取的总时间。单位：毫秒。

### update_compaction_outputs_bytes_total

- 单位：Byte
- 描述：主键表 Compaction 时写入的总字节数。

### memtable_flush_queue_count

- 单位：个
- 描述：Memtable Flush 线程池中排队的任务数量。

### query_cache_capacity

- 描述：Query Cache 的容量。

### streaming_load_duration_ms

- 单位：毫秒
- 描述：Stream Load 的总耗时。

### streaming_load_current_processing

- 单位：个
- 描述：当前运行的 Stream Load 任务数量。

### streaming_load_bytes

- 单位：Byte
- 描述：Stream Load 导入的总字节数。

### load_rows

- 单位：个
- 描述：总导入行数。

### load_bytes

- 单位：Byte
- 描述：总导入字节数。

### meta_request_total

- 单位：个
- 描述：元数据读/写请求的总数量。

### meta_request_duration

- 单位：微秒
- 描述：元数据读/写的总持续时间。单位：微秒。

### tablet_base_max_compaction_score

- 单位：-
- 描述：此 BE 中各 Tablet 的最高 Base Compaction Score。

### page_cache_capacity

- 描述：Storage Page Cache 的容量。

### cumulative_compaction_task_byte_per_second

- 单位：Byte/秒
- 描述：Cumulative Compaction 任务的每秒处理字节数。

### stream_load

- 单位：-
- 描述：导入的总行数和接收的总字节数。

### stream_load_pipe_count

- 单位：个
- 描述：当前运行的 Stream Load 任务数量。

### binary_column_pool_bytes

- 单位：Byte
- 描述：BINARY 列池使用的内存。

### int16_column_pool_bytes

- 单位：Byte
- 描述：INT16 列池使用的内存。

### decimal_column_pool_bytes

- 单位：Byte
- 描述：DECIMAL 列池使用的内存。

### double_column_pool_bytes

- 单位：Byte
- 描述：DOUBLE 列池使用的内存。

### int128_column_pool_bytes

- 单位：Byte
- 描述：INT128 列池使用的内存。

### date_column_pool_bytes

- 单位：Byte
- 描述：DATE 列池使用的内存。

### int64_column_pool_bytes

- 单位：Byte
- 描述：INT64 列池使用的内存。

### int8_column_pool_bytes

- 单位：Byte
- 描述：INT8 列池使用的内存。

### datetime_column_pool_bytes

- 单位：Byte
- 描述：DATETIME 列池使用的内存。

### int32_column_pool_bytes

- 单位：Byte
- 描述：INT32 列池使用的内存。

### float_column_pool_bytes

- 单位：Byte
- 描述：FLOAT 列池使用的内存。

### uint8_column_pool_bytes

- 单位：Byte
- 描述：UINT8 列池使用的字节数。

### column_pool_mem_bytes

- 单位：Byte
- 描述：列池使用的内存。

### local_column_pool_bytes（已弃用）

### total_column_pool_bytes（已弃用）

### central_column_pool_bytes（已弃用）

### wait_base_compaction_task_num

- 单位：个
- 描述：正在等待执行的 Base Compaction 任务数量。

### wait_cumulative_compaction_task_num

- 单位：个
- 描述：正在等待执行的 Cumulative Compaction 任务数量。

### jemalloc_allocated_bytes

- 单位：Byte
- 描述：应用分配的总字节数。

### pk_index_compaction_queue_count

- 单位：个
- 描述：主键索引 Compaction 线程池中排队的任务数量。

### disks_total_capacity

- 描述：磁盘的总容量。

### disks_state

- 单位：-
- 描述：磁盘的状态。`1` 表示磁盘正在使用，`0` 表示磁盘未被使用。

### update_del_vector_deletes_total（已弃用）

### update_del_vector_dels_num（已弃用）

### result_block_queue_count

- 单位：个
- 描述：结果 Block 队列中的结果数量。

### engine_requests_total

- 单位：个
- 描述：BE 和 FE 之间各种请求的总数，包括 CREATE TABLE、Publish Version 和 Tablet Clone。

### snmp

- 单位：-
- 描述：由 `/proc/net/snmp` 返回的指标。

### compaction_mem_bytes

- 单位：Byte
- 描述：Compaction 使用的内存。

### txn_request

- 单位：-
- 描述：BEGIN、COMMIT、ROLLBACK 和 EXEC 的事务请求。

### small_file_cache_count

- 单位：个
- 描述：Small File Cache 的数量。

### rowset_metadata_mem_bytes

- 单位：Byte
- 描述：Rowset 元数据的总字节数。

### update_apply_queue_count

- 单位：个
- 描述：主键表事务 APPLY 线程池中排队的任务数量。

### async_delta_writer_queue_count

- 单位：个
- 描述：Tablet Delta Writer 线程池中排队的任务数量。

### update_compaction_duration_us

- 单位：微秒
- 描述：主键表 Compaction 任务的总耗时。

### transaction_streaming_load_current_processing

- 单位：个
- 描述：当前运行的 Stream Load 事务导入任务数量。

### ordinal_index_mem_bytes

- 单位：Byte
- 描述：Ordinal 索引使用的内存。

### consistency_mem_bytes

- 单位：Byte
- 描述：副本一致性检查使用的内存。

### tablet_schema_mem_bytes

- 单位：Byte
- 描述：Tablet Schema 使用的内存。

### fd_num_used

- 单位：个
- 描述：当前正在使用的文件描述符数量。

### running_update_compaction_task_num

- 单位：个
- 描述：当前运行的主键表 Compaction 任务总数。

### rowset_count_generated_and_in_use

- 单位：个
- 描述：当前正在使用的 Rowset ID 的数量。

### bitmap_index_mem_bytes

- 单位：Byte
- 描述：Bitmap 索引使用的内存。

### update_rowset_commit_apply_duration_us

- 单位：微秒
- 描述：主键表 APPLY 任务的总耗时。

### process_fd_num_used

- 单位：个
- 描述：此 BE 进程中当前正在使用的文件描述符数量。

### network_send_packets

- 单位：个
- 描述：通过网络发送的总数据包数量。

### network_receive_packets

- 单位：个
- 描述：通过网络接收的总数据包数量。

### metadata_mem_bytes（已弃用）

### push_requests_total

- 单位：个
- 描述：Spark Load 请求总数（包括成功和失败的请求）。

### blocks_deleted_total（已弃用）

### jemalloc_mapped_bytes

- 单位：Byte
- 描述：由 Allocator 映射的活动范围中的总字节数。

### process_fd_num_limit_hard

- 单位：个
- 描述：文件描述符的硬限制最大数量。

### jemalloc_metadata_thp

- 单位：个
- 描述：用于元数据的 Transparent Huge Pages 的数量。

### update_rowset_commit_request_failed

- 单位：个
- 描述：主键表中 RowsetCOMMIT 请求失败的总数。

### streaming_load_requests_total

- 单位：个
- 描述：Stream Load 请求的总数。

### resource_group_running_queries

- 单位：个
- 描述：每个资源组当前正在运行的查询数量（瞬时值）。

### resource_group_total_queries

- 单位：个
- 描述：每个资源组执行的查询总数，包括当前正在运行的查询（瞬时值）。

### resource_group_bigquery_count

- 单位：个
- 描述：每个资源组触发大查询限制的查询数量（瞬时值）。

### resource_group_concurrency_overflow_count

- 单位：个
- 描述：每个资源组触发并发限制的查询数量（瞬时值）。

### resource_group_mem_limit_bytes

- 单位：Byte
- 描述：每个资源组的内存限制（瞬时值）。单位：字节。

### resource_group_mem_inuse_bytes

- 单位：Byte
- 描述：每个资源组当前使用的内存（瞬时值）。单位：字节。

### resource_group_cpu_limit_ratio

- 单位：-
- 描述：每个资源组 CPU 核心限制与所有资源组总 CPU 核心限制的比值（瞬时值）。

### resource_group_inuse_cpu_cores

- 单位：个
- 描述：每个资源组当前正在使用的 CPU 核心的估计数量（两次统计之间的平均值）。

### resource_group_cpu_use_ratio（已弃用）

- 单位：-
- 描述：每个资源组使用的流水线线程时间片与所有资源组总使用时间片的比值（两次统计之间的平均值）。

### resource_group_connector_scan_use_ratio（已弃用）

- 单位：-
- 描述：每个资源组的外部表扫描线程时间片与所有资源组总使用时间片的比值（两次统计之间的平均值）。

### resource_group_scan_use_ratio（已弃用）

- 单位：-
- 描述：每个资源组的内部表扫描线程时间片与所有资源组总使用时间片的比值（两次统计之间的平均值）。

### pipe_poller_block_queue_len

- 单位：个
- 描述：Pipeline Engine 中 PipelineDriverPoller 的 Block 队列的当前长度。

### pip_query_ctx_cnt

- 单位：个
- 描述：BE 中当前正在运行的查询的总数。

### pipe_driver_schedule_count

- 单位：个
- 描述：BE 中 Pipeline Executor 的 Driver 调度次数（累积值）。

### pipe_scan_executor_queuing

- 单位：个
- 描述：当前由 Scan Operator 发起待执行的异步 I/O 任务的数量。

### pipe_driver_queue_len

- 单位：个
- 描述：当前在 BE 中等待调度的 Ready Driver 的数量。

### pipe_driver_execution_time

- 描述：PipelineDriver Executor 处理 PipelineDriver 所用的总时间。

### pipe_prepare_pool_queue_len

- 单位：个
- 描述：Pipeline Prepare 线程池中排队的任务数量（瞬时值）。

### starrocks_fe_routine_load_jobs

- 单位：个
- 描述：按 Routine Load 作业状态统计作业数量。例如：

  ```plaintext
  starrocks_fe_routine_load_jobs{state="NEED_SCHEDULE"} 0
  starrocks_fe_routine_load_jobs{state="RUNNING"} 1
  starrocks_fe_routine_load_jobs{state="PAUSED"} 0
  starrocks_fe_routine_load_jobs{state="STOPPED"} 0
  starrocks_fe_routine_load_jobs{state="CANCELLED"} 1
  ```

### starrocks_fe_routine_load_paused

- 单位：个
- 描述：所有 Routine Load 作业暂停总次数。

### starrocks_fe_routine_load_rows

- 单位：个
- 描述：所有 Routine Load 作业导入数据的总行数。

### starrocks_fe_routine_load_receive_bytes

- 单位：Byte
- 描述：所有 Routine Load 作业导入数据的总量。

### starrocks_fe_routine_load_error_rows

- 单位：个
- 描述：所有 Routine Load 作业导入错误数据的总行数。
