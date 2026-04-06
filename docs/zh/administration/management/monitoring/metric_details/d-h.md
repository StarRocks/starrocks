---
displayed_sidebar: docs
hide_table_of_contents: true
description: "Alphabetical d - h"
---

# 指标 d 到 h

:::note

物化视图和共享数据集群的指标在相应章节中详细介绍：

- [异步物化视图指标](../metrics-materialized_view.md)
- [共享数据仪表盘指标和 Starlet 仪表盘指标](../metrics-shared-data.md)

有关如何为 StarRocks 集群构建监控服务的更多信息，请参阅[监控和告警](../Monitor_and_Alert.md)。

:::

## `data_stream_receiver_count`

- 单位：计数
- 描述：BE 中作为 Exchange 接收器的实例的累计数量。

## `datacache_disk_quota_bytes`

- 单位：字节
- 类型：Gauge
- 描述：为 datacache 配置的磁盘配额。

## `datacache_disk_used_bytes`

- 单位：字节
- 类型：Gauge
- 描述：datacache 当前的磁盘使用量。

## `datacache_mem_quota_bytes`

- 单位：字节
- 类型：Gauge
- 描述：为 datacache 配置的内存配额。

## `datacache_mem_used_bytes`

- 单位：字节
- 类型：Gauge
- 描述：datacache 当前的内存使用量。

## `datacache_meta_used_bytes`

- 单位：字节
- 类型：Gauge
- 描述：datacache 元数据的内存使用量。

## `date_column_pool_bytes`

- 单位：字节
- 描述：DATE 列池使用的内存。

## `datetime_column_pool_bytes`

- 单位：字节
- 描述：DATETIME 列池使用的内存。

## `decimal_column_pool_bytes`

- 单位：字节
- 描述：DECIMAL 列池使用的内存。

## `delta_column_group_get_hit_cache`

- 单位：计数
- 描述：delta 列组缓存命中总数（仅适用于主键表）。

## `delta_column_group_get_non_pk_hit_cache`

- 单位：计数
- 描述：delta 列组缓存命中总数（适用于非主键表）。

## `delta_column_group_get_non_pk_total`

- 单位：计数
- 描述：获取 delta 列组的总次数（仅适用于非主键表）。

## `delta_column_group_get_total`

- 单位：计数
- 描述：获取 delta 列组的总次数（适用于主键表）。

## `disk_bytes_read`

- 单位：字节
- 描述：从磁盘读取的总字节数（扇区读取数 * 512）。

## `disk_bytes_written`

- 单位：字节
- 描述：写入磁盘的总字节数。

## `disk_free`

- 单位：字节
- 类型：平均值
- 描述：空闲磁盘容量。

## `disk_io_svctm`

- 单位：毫秒
- 类型：平均值
- 描述：磁盘IO服务时间。

## `disk_io_time_ms`

- 单位：毫秒
- 描述：I/O花费的时间。

## `disk_io_time_weigthed`

- 单位：毫秒
- 描述：I/O花费的加权时间。

## `disk_io_util`

- 单位：-
- 类型：平均值
- 描述：磁盘使用率。

## `disk_read_time_ms`

- 单位：毫秒
- 描述：从磁盘读取花费的时间。单位：毫秒。

## `disk_reads_completed`

- 单位：计数
- 描述：成功完成的磁盘读取次数。

## `disk_sync_total (Deprecated)`

## `disk_used`

- 单位：字节
- 类型：平均值
- 描述：已用磁盘容量。

## `disk_write_time_ms`

- 单位：毫秒
- 描述：磁盘写入花费的时间。单位：毫秒。

## `disk_writes_completed`

- 单位：计数
- 描述：成功完成的磁盘写入总次数。

## `disks_avail_capacity`

- 描述：特定磁盘的可用容量。

## `disks_data_used_capacity`

- 描述：每个磁盘的已用容量（由存储路径表示）。

## `disks_state`

- 单位：-
- 描述：每个磁盘的状态。`1` 表示磁盘正在使用中，`0` 表示磁盘未在使用中。

## `disks_total_capacity`

- 描述：磁盘总容量。

## `double_column_pool_bytes`

- 单位：字节
- 描述：DOUBLE列池使用的内存。

## `encryption_keys_created`

- 单位：计数
- 类型：累积
- 描述：为文件加密创建的文件加密密钥数量

## `encryption_keys_in_cache`

- 单位：计数
- 类型：瞬时
- 描述：当前密钥缓存中的加密密钥数量

## `encryption_keys_unwrapped`

- 单位：计数
- 类型：累积
- 描述：为文件解密而解包的加密元数据数量

## `engine_requests_total`

- 单位：计数
- 描述：BE和FE之间所有类型请求的总计数，包括CREATE TABLE、Publish Version和tablet clone。

## `fd_num_limit`

- 单位：计数
- 描述：文件描述符的最大数量。

## `fd_num_used`

- 单位：计数
- 描述：当前正在使用的文件描述符数量。

## `fe_cancelled_broker_load_job`

- 单位：计数
- 类型：平均
- 描述：已取消的broker作业数量。

## `fe_cancelled_delete_load_job`

- 单位：计数
- 类型：平均
- 描述：已取消的删除作业数量。

## `fe_cancelled_hadoop_load_job`

- 单位：计数
- 类型：平均
- 描述：已取消的hadoop作业数量。

## `fe_cancelled_insert_load_job`

- 单位：计数
- 类型：平均
- 描述：已取消的插入作业数量。

## `fe_checkpoint_push_per_second`

- 单位：计数/秒
- 类型：平均
- 描述：FE检查点数量。

## `fe_committed_broker_load_job`

- 单位：计数
- 类型：平均
- 描述：已提交的broker作业数量。

## `fe_committed_delete_load_job`

- 单位：计数
- 类型：平均
- 描述：已提交的删除作业数量。

## `fe_committed_hadoop_load_job`

- 单位：计数
- 类型：平均
- 描述：已提交的hadoop作业数量。

## `fe_committed_insert_load_job`

- 单位：计数
- 类型：平均
- 描述：已提交的插入作业数量。

## `fe_connection_total`

- 单位：计数
- 类型：累积
- 描述：FE连接总数。

## `fe_connections_per_second`

- 单位: 次/秒
- 类型: 平均值
- 描述: FE 的新连接速率。

## `fe_edit_log_read`

- 单位: 次/秒
- 类型: 平均值
- 描述: FE 编辑日志的读取速度。

## `fe_edit_log_size_bytes`

- 单位: 字节/秒
- 类型: 平均值
- 描述: FE 编辑日志的大小。

## `fe_edit_log_write`

- 单位: 字节/秒
- 类型: 平均值
- 描述: FE 编辑日志的写入速度。

## `fe_finished_broker_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 已完成的 broker 任务数量。

## `fe_finished_delete_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 已完成的删除任务数量。

## `fe_finished_hadoop_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 已完成的 hadoop 任务数量。

## `fe_finished_insert_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 已完成的插入任务数量。

## `fe_loading_broker_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 正在加载的 broker 任务数量。

## `fe_loading_delete_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 正在加载的删除任务数量。

## `fe_loading_hadoop_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 正在加载的 hadoop 任务数量。

## `fe_loading_insert_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 正在加载的插入任务数量。

## `fe_pending_broker_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 待处理的 broker 任务数量。

## `fe_pending_delete_load_job`

- 单位: 次数
- 类型: 平均值
- 描述: 待删除作业的数量。

## `fe_pending_hadoop_load_job`

- 单位: 计数
- 类型: 平均值
- 描述: 待处理 Hadoop 作业的数量。

## `fe_pending_insert_load_job`

- 单位: 计数
- 类型: 平均值
- 描述: 待插入作业的数量。

## `fe_rollup_running_alter_job`

- 单位: 计数
- 类型: 平均值
- 描述: 在 Rollup 中创建的作业数量。

## `fe_schema_change_running_job`

- 单位: 计数
- 类型: 平均值
- 描述: 模式变更中的作业数量。

## `float_column_pool_bytes`

- 单位: 字节
- 描述: FLOAT 列池使用的内存。

## `fragment_endpoint_count`

- 单位: 计数
- 描述: 在 BE 中作为 Exchange 发送方的实例累计数量。

## `fragment_request_duration_us`

- 单位: 微秒
- 描述: 片段实例的累计执行时间（适用于非流水线引擎）。

## `fragment_requests_total`

- 单位: 计数
- 描述: 在 BE 上执行的片段实例总数（适用于非流水线引擎）。

## `hive_write_bytes`

- 单位: 字节
- 类型: 累计
- 标签: `write_type` (`insert` 或 `overwrite`)
- 描述: Hive 写入任务 (`INSERT`, `INSERT OVERWRITE`) 写入的总字节数。这表示写入 Hive 表的数据文件的总大小。`write_type` 用于区分操作类型。

## `hive_write_duration_ms_total`

- 单位: 毫秒
- 类型: 累计
- 标签: `write_type` (`insert` 或 `overwrite`)
- 描述: Hive 写入任务 (`INSERT`, `INSERT OVERWRITE`) 的总执行时间（毫秒）。每个任务的持续时间在其结束后添加。`write_type` 用于区分操作类型。

## `hive_write_files`

- 单位: 计数
- 类型: 累计
- 标签: `write_type` (`insert` 或 `overwrite`)
- 描述: Hive 写入任务 (`INSERT`, `INSERT OVERWRITE`) 写入的数据文件总数。这表示写入 Hive 表的数据文件的数量。`write_type` 用于区分操作类型。

## `hive_write_rows`

- 单位: 行
- 类型: 累计
- 标签: `write_type` (`insert` 或 `overwrite`)
- 描述: Hive 写入任务 (`INSERT`, `INSERT OVERWRITE`) 写入的总行数。这表示写入 Hive 表的行数。`write_type` 用于区分操作类型。

## `hive_write_total`

- 单位: 计数
- 类型: 累计
- 标签：
  - `status`（`success` 或 `failed`）
  - `reason`（`none`、`timeout`、`oom`、`access_denied`、`unknown`）
  - `write_type`（`insert` 或 `overwrite`）
- 描述：针对 Hive 表的 `INSERT` 或 `INSERT OVERWRITE` 任务的总数。无论任务成功或失败，每当任务结束时，该指标都会增加 1。`write_type` 用于区分操作类型。

## `http_request_send_bytes (Deprecated)`

## `http_requests_total (Deprecated)`
