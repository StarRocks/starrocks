---
displayed_sidebar: docs
---

# 中间结果落盘

本文介绍如何将大算子的中间计算结果落盘 (Spill to disk) 至本地磁盘或对象存储。

## 概述

对于依靠内存计算处理查询的数据库系统，例如 StarRocks，它们在大数据集上处理聚合、排序以及连接等运算时，会消耗大量内存资源。当达到内存限制时，这些查询将被强制终止。

但在某些场景下，您可能希望使用有限的内存资源稳定地完成某些任务，甚至愿意为此牺牲部分性能，例如，构建物化视图或使用 INSERT INTO SELECT 执行轻量级 ETL。这类任务会大量消耗内存资源，进而阻塞集群中运行的其他查询。通常，您需要通过手动调整参数解决这个问题，并且依靠集群资源隔离策略来控制查询并发。这种方式不仅较为不便，并且在极端情况下，可能依然会失败。

从 v3.0.1 开始，StarRocks 支持将一些大算子的中间结果落盘。 使用此功能，您可以在牺牲一部分性能的前提下，大幅降低大规模数据查询上的内存消耗，进而提高整个系统的可用性。

目前，StarRocks 支持将以下算子的中间结果落盘：

- 聚合算子
- 排序算子
- Hash join（LEFT JOIN、RIGHT JOIN、FULL JOIN、OUTER JOIN、SEMI JOIN 以及 INNER JOIN）算子

## 开启中间结果落盘

根据以下步骤开启中间结果落盘：

1. 在 BE 配置文件 **be.conf** 或 CN 配置文件 **cn.conf** 中指定落盘路径 `spill_local_storage_dir`，并重启集群使修改生效。

   ```Properties
   spill_local_storage_dir=/<dir_1>[;/<dir_2>]
   ```

   > **说明**
   >
   > - 您可以指定多个落盘路径 `spill_local_storage_dir`，中间使用分号（`;`）分隔。
   > - 在生产环境中，我们强烈建议您为数据存储和中间结果落盘使用不同的磁盘。当中间结果落盘时，写入负载和磁盘使用量可能会显著增加。如果使用相同的磁盘，这种激增会影响集群中运行的其他查询或任务。

2. 执行以下语句开启中间结果落盘：

   ```SQL
   SET enable_spill = true;
   ```

3. 通过 Session 变量 `spill_mode` 设定中间结果落盘模式：

   ```SQL
   SET spill_mode = { "auto" | "force" };
   ```

   > **说明**
   >
   > 每个涉及中间结果落盘的查询在完成后，StarRocks 会自动清除其产生的落盘数据。如果 BE 在清除数据之前崩溃导致数据残留，StarRocks 会在 BE 重新启动时清除落盘数据。

   | **变量**     | **默认值** | **描述**                                                     |
   | ------------ | ---------- | ------------------------------------------------------------ |
   | enable_spill | false      | 是否启用中间结果落盘。如果将其设置为 `true`，StarRocks 会将中间结果落盘，以减少在查询中处理聚合、排序或连接算子时的内存使用量。 |
   | spill_mode   | auto       | 中间结果落盘的执行方式。有效值：`auto`：达到内存使用阈值时，会自动触发落盘。`force`：无论内存使用情况如何，StarRocks 都会强制落盘所有相关算子的中间结果。此变量仅在变量 `enable_spill` 设置为 `true` 时生效。 |

## [Preview] 将中间结果落盘至对象存储

自 v3.3.0 起，StarRocks 支持将中间结果落盘至对象存储。

:::tip
在启用将对象存储落盘功能之前，您必须创建一个存储卷来指定您想要使用的对象存储。有关创建存储卷的详细说明，请参阅 [CREATE STORAGE VOLUME](../../../sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md)。
:::

在上一步中开启落盘之后，您可以进一步设置以下系统变量，以允许将中间结果落盘至对象存储：

```SQL
SET enable_spill_to_remote_storage = true;

-- 将 <storage_volume_name> 替换为您想要使用的存储卷的名称。
SET spill_storage_volume = '<storage_volume_name>';
```

启用将中间结果落盘至对象存储后，触发落盘的查询的中间结果将首先存储在 BE 或 CN 节点的本地磁盘上，如果本地磁盘的容量限制已达到上限，中间结果将会存储至对象存储。

请注意，如果您在 `spill_storage_volume` 中指定的存储卷不存在，则不会启用将对象存储落盘。

## 使用限制

- 中间结果落盘无法解决所有内存不足问题。例如，StarRocks 无法释放用于表达式计算的内存。
- 通常，涉及中间结果落盘的查询可能表明其查询时间有数量级的增长。建议您通过设置 Session 变量 `query_timeout` 适当延长这些查询的超时时间。
- 相较于落盘至本地磁盘，将中间结果落盘至对象存储存的性能有所下降。
- 每个 BE 或 CN 节点的 `spill_local_storage_dir` 由该节点上运行的所有查询共享。目前，StarRocks 不支持为单个查询单独设置本地磁盘落盘的数据量上限。因此，多个触发落盘的并发查询可能会相互影响。
