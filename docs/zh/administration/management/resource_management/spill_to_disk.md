---
displayed_sidebar: docs
sidebar_position: 50
---

import Beta from '../../../_assets/commonMarkdown/_beta.mdx'

# 中间结果落盘

<Beta />

本文介绍如何将大型运算符的中间计算结果落盘到本地磁盘和对象存储中。

## 概述

对于依赖内存计算进行查询执行的数据库系统，如 StarRocks，在处理包含聚合、排序和连接运算符的大数据集查询时，可能会消耗大量内存资源。当达到内存限制时，这些查询会因内存不足（OOM）而被强制终止。

然而，有时您可能希望某些内存密集型任务能够稳定完成，而性能不是您的首要考虑，例如构建物化视图，或使用 INSERT INTO SELECT 执行轻量级 ETL。这些任务可能会轻易耗尽您的内存资源，从而阻塞集群中运行的其他查询。通常，为了解决这个问题，您只能单独微调这些任务，并依赖资源隔离策略来控制查询并发。这在某些极端情况下可能特别不便且容易失败。

从 StarRocks v3.0.1 开始，StarRocks 支持将一些内存密集型运算符的中间结果落盘。通过此功能，您可以在性能上接受一定程度的下降，以显著减少内存使用，从而提高系统可用性。

目前，StarRocks 的中间结果落盘功能支持以下运算符：

- 聚合运算符
- 排序运算符
- 哈希连接（LEFT JOIN, RIGHT JOIN, FULL JOIN, OUTER JOIN, SEMI JOIN 和 INNER JOIN）运算符
- CTE 运算符（从 v3.3.4 开始支持）

## 启用中间结果落盘

按照以下步骤启用中间结果落盘：

1. 在 BE 配置文件 **be.conf** 或 CN 配置文件 **cn.conf** 中指定本地落盘目录 `spill_local_storage_dir`，用于存储本地磁盘上的中间结果，并重启集群以使修改生效。

   ```Properties
   spill_local_storage_dir=/<dir_1>[;/<dir_2>]
   ```

   > **注意**
   >
   > - 您可以通过分号（`;`）分隔多个 `spill_local_storage_dir`。
   > - 在生产环境中，我们强烈建议您使用不同的磁盘进行数据存储和落盘。当中间结果落盘时，写入负载和磁盘使用可能会显著增加。如果使用相同的磁盘，这种激增可能会影响集群中运行的其他查询或任务。

2. 执行以下语句以启用中间结果落盘：

   ```SQL
   SET enable_spill = true;
   ```

3. 使用会话变量 `spill_mode` 配置中间结果落盘的模式：

   ```SQL
   SET spill_mode = { "auto" | "force" };
   ```

   > **注意**
   >
   > 每次带有落盘的查询完成后，StarRocks 会自动清除查询产生的落盘数据。如果 BE 在清除数据前崩溃，StarRocks 会在 BE 重启时清除这些数据。

   | **变量** | **默认值** | **描述** |
   | ------------ | ----------- | ------------------------------------------------------------ |
   | enable_spill | false       | 是否启用中间结果落盘。如果设置为 `true`，StarRocks 会将中间结果落盘，以减少处理查询中的聚合、排序或连接运算符时的内存使用。 |
   | spill_mode   | auto        | 中间结果落盘的执行模式。有效值：<ul><li>`auto`: 当达到内存使用阈值时自动触发落盘。</li><li>`force`: 无论内存使用情况如何，StarRocks 强制对所有相关运算符执行落盘。</li></ul>此变量仅在 `enable_spill` 变量设置为 `true` 时生效。 |

## [预览] 中间结果落盘到对象存储

从 v3.3.0 开始，StarRocks 支持将中间结果落盘到对象存储。

:::tip
在启用落盘到对象存储之前，您必须创建一个存储卷以定义要使用的对象存储。有关创建存储卷的详细说明，请参见 [CREATE STORAGE VOLUME](../../../sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md)。
:::

在您完成前一步启用落盘后，您可以进一步设置这些系统变量，以允许中间结果落盘到对象存储：

```SQL
SET enable_spill_to_remote_storage = true;

-- 将 <storage_volume_name> 替换为您要使用的存储卷名称。
SET spill_storage_volume = '<storage_volume_name>';
```

启用落盘到对象存储后，触发落盘的查询的中间结果将首先存储在 BE 或 CN 节点的本地磁盘上，然后在本地磁盘容量限制达到时存储到对象存储中。

请注意，如果您为 `spill_storage_volume` 指定的存储卷不存在，则不会启用落盘到对象存储。

## 限制

- 并非所有 OOM 问题都能通过落盘解决。例如，StarRocks 无法释放用于表达式评估的内存。
- 通常，涉及落盘的查询会导致查询延迟增加十倍。我们建议您通过设置会话变量 `query_timeout` 来延长这些查询的超时时间。
- 与落盘到本地磁盘相比，落盘到对象存储的性能显著下降。
- 每个 BE 或 CN 节点的 `spill_local_storage_dir` 在节点上运行的所有查询之间共享。目前，StarRocks 不支持为每个查询单独设置落盘到本地磁盘的数据大小限制。因此，涉及落盘的并发查询可能会相互影响。