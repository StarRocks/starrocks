---
displayed_sidebar: docs
---

# 存算分离

本主题解答了一些关于存算分离集群的常见问题。

## 为什么表创建失败？

检查 BE 日志 (`be.INFO`) 以确定具体原因。常见原因包括：

- 对象存储设置错误（例如，`aws_s3_path`、`endpoint`、`authentication`）。
- 对象存储服务不稳定或异常。

其他错误：

错误信息："Error 1064 (HY000): Unexpected exception: Failed to create shards. INVALID_ARGUMENT: shard info cannot be empty"

原因：这通常发生在使用自动桶推断时，没有 CN 或 BE 节点存活。此问题在 v3.2 中已修复。

## 为什么表创建时间过长？

过多的桶数量（尤其是在分区表中）导致 StarRocks 创建许多 tablets。系统需要为对象存储中的每个 tablet 写入一个 tablet 元数据文件，其高延迟可能会显著增加总创建时间。您可以考虑：

- 减少桶的数量。
- 通过 BE 配置 `create_tablet_worker_count` 增加 tablet 创建线程池大小。
- 检查并解决对象存储中的高写入延迟问题。

## 为什么删除表后对象存储中的数据没有被清理？

StarRocks 支持两种 DROP TABLE 模式：

- `DROP TABLE xxx`：将表元数据移动到 FE 回收站（数据未删除）。
- `DROP TABLE xxx FORCE`：立即删除表元数据和数据。

如果清理失败，请检查：

- 是否使用了 `DROP TABLE xxx FORCE`。
- 回收站保留参数是否设置过高。参数包括：
  - FE 配置 `catalog_trash_expire_second`
  - BE 配置 `trash_file_expire_time_sec`
- FE 日志中的删除错误（例如，RPC 超时）。如有需要，增加 RPC 超时时间。

## 如何找到对象存储中表数据的存储路径？

运行以下命令以获取存储路径。

```SQL
SHOW PROC '/dbs/<database_name>';
```

示例：

```SQL
mysql> SHOW PROC '/dbs/load_benchmark';
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+--------------------------------------------------------------------------------------------------------------+
| TableId | TableName   | IndexNum | PartitionColumnName | PartitionNum | State  | Type         | LastConsistencyCheckTime | ReplicaCount | PartitionType | StoragePath                                                                                                  |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+--------------------------------------------------------------------------------------------------------------+
| 17152   | store_sales | 1        | NULL                | 1            | NORMAL | CLOUD_NATIVE | NULL                     | 64           | UNPARTITIONED | s3://starrocks-common/xxxxxxxxx-xxxx_load_benchmark-1699408425544/5ce4ee2c-98ba-470c-afb3-8d0bf4795e48/17152 |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+--------------------------------------------------------------------------------------------------------------+
1 row in set (0.18 sec)
```

在 v3.1.4 之前的版本中，表数据分散在单个目录下。

从 v3.1.4 开始，数据按分区组织。相同命令显示表的根路径，现在包含以分区 ID 命名的子目录，每个分区目录包含 `data/`（segment 数据文件）和 `meta/`（tablet 元数据文件）子目录。

## 为什么在存算分离集群中的查询速度慢？

常见原因包括：

- 缓存未命中。
- 缺乏足够的 Compaction，导致过多的小 segment 文件，从而导致过多的 I/O。
- 并行度不佳（例如，tablet 数量过少）。
- 不当的 `datacache.partition_duration` 设置，导致缓存失败。

您需要首先分析 Query Profile 以确定根本原因。

### 缓存未命中

在存算分离集群中，数据存储在远程，因此 Data Cache 至关重要。如果查询速度意外变慢，请检查 Query Profile 指标，如 `CompressedBytesReadRemote` 和 `IOTimeRemote`。

缓存未命中可能由以下原因导致：

- 在表创建时禁用了 Data Cache。
- 本地缓存空间不足。
- 由于弹性扩展导致的 tablet 迁移。
- 不当的 `datacache.partition_duration` 设置导致无法缓存。

### 缺乏足够的 Compaction

如果没有足够的 Compaction，许多历史数据版本会保留，增加查询时访问的 segment 文件数量。这会增加 I/O 并减慢查询速度。

您可以通过以下方式诊断 Compaction 不足：

- 检查相关分区的 Compaction Score。

  Compaction Score 应保持在 ~10 以下。过高的 Compaction Score 通常表示 Compaction 失败。

- 查看 Query Profile 指标，如 `SegmentsReadCount`。

  如果 Segment 数量很高，可能是 Compaction 滞后或卡住。

### 不当的 tablet 设置

Tablets 将数据分布在计算节点上。糟糕的分桶或不均衡的桶键可能导致查询仅在部分节点上运行。

建议：

- 选择能确保均衡分布的桶列。
- 设置合理的桶数量（公式：`total data size / (1–5 GB)`）。

### 不当的 `datacache.partition_duration` 设置

如果此值设置得过小，“冷”分区的数据可能无法缓存，导致重复的远程读取。在 Query Profile 中，如果 `CompressedBytesReadRemote` 或 `IOCountRemote` 非零，这可能是原因。相应调整 `datacache.partition_duration`。

## 为什么一个 Warehouse 下的所有查询都因“Timeout was reached”或“Deadline Exceeded”错误而超时？

检查 Warehouse 下的计算节点是否可以访问对象存储端点。

## 如何在存算分离集群中检索 tablet 元数据？

1. 通过运行以下命令获取可见版本：

   ```SQL
   SHOW PARTITIONS FROM <table_name>`
   ```

2. 执行以下语句以检索 tablet 元数据：

   ```SQL
   admin execute on <backend_id> 
   'System.print(StorageEngine.get_lake_tablet_metadata_json(<tablet_id>, <version>))'
   ```

## 为什么在高频导入时导入速度慢？

StarRocks 序列化事务提交，因此高导入率可能达到限制。

监控以下方面：

- **导入队列**：如果导入队列已满，可以增加 I/O 工作线程。
- **发布版本延迟**：高发布时间导致导入延迟。

## 在存算分离集群中，Compaction 如何工作，为什么会卡住？

关键行为包括：

- Compaction 由 FE 调度并由 CN 执行。
- 每次 Compaction 产生一个新版本，并经过写入、提交和发布的过程。
- FE 不跟踪运行时的 Compaction 任务；BE 上的过时任务可能阻塞新任务。

要清理卡住的 Compaction 任务，请按照以下步骤操作：

1. 检查分区的版本信息，并比较 `CompactVersion` 和 `VisibleVersion`。

   ```SQL
   SHOW PARTITIONS FROM <table_name>
   ```

2. 检查 Compaction 任务状态。

   ```SQL
   SHOW PROC '/compactions'
   ```

   ```SQL
   SELECT * FROM information_schema.be_cloud_native_compactions WHERE TXN_ID = <TxnID>
   ```

3. 取消过期任务。

   a. 禁用 Compaction 和迁移。

      ```SQL
      ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "0");
      ADMIN SET FRONTEND CONFIG ('tablet_sched_disable_balance' = 'true');
      ADMIN SHOW FRONTEND CONFIG LIKE 'lake_compaction_max_tasks';
      ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_sched_disable_balance';
      ```

   b. 重启所有 BE 节点

   c. 验证所有 Compaction 均已失败。

      ```SQL
      SHOW PROC '/compactions'
      ```

   d. 重新启用 Compaction 和迁移。

      ```SQL
      ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "-1");
      ADMIN SET FRONTEND CONFIG ('tablet_sched_disable_balance' = 'false');
      ADMIN SHOW FRONTEND CONFIG LIKE 'lake_compaction_max_tasks';
      ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_sched_disable_balance';
      ```

## 为什么在 Kubernetes 存算分离集群中 Compaction 速度慢？

如果导入发生在一个 Warehouse 中，但 Compaction 在另一个 Warehouse 中运行（例如，`default_warehouse`），则 Compaction 必须跨 Warehouse 拉取数据，没有缓存，从而减慢速度。

解决方案：

- 将 BE 配置 `lake_enable_vertical_compaction_fill_data_cache` 设置为 `true`。
- 在与 Compaction 相同的 Warehouse 中执行写入。

## 为什么在存算分离集群中存储使用量看起来过高？

对象存储上的存储使用量包括所有历史版本，而 `SHOW DATA` 输出仅反映最新版本。

然而，如果差异过大，可能由以下问题引起：

- Compaction 可能滞后。
- Vacuum 任务可能积压（检查队列大小）。

如有必要，您可以考虑调整 Compaction 或 Vacuum 线程池。

## 为什么大查询失败并显示“meta does not exist”？

查询可能正在使用已被 Compaction 和 Vacuum 的数据版本。

要解决此问题，您可以通过修改 FE 配置 `lake_autovacuum_grace_period_minutes` 增加文件保留时间，然后重试查询。

## 什么导致对象存储中出现过多的小文件？

过多的小文件可能导致性能下降。常见原因包括：

- 不当的桶设置导致过多的桶。
- 高导入频率导致非常小的单次导入。

Compaction 最终会合并小文件，但调整桶数量和批量导入有助于防止性能下降。