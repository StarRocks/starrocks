---
displayed_sidebar: "Chinese"
---

# 查询队列

本文档介绍如何在 StarRocks 中管理查询队列。

自 v2.5 版本起，StarRocks 支持查询队列功能。启用查询队列后，StarRocks 会在并发查询数量或资源使用率达到一定阈值时自动对查询进行排队，从而避免过载加剧。待执行查询将在队列中等待直至有足够的计算资源时开始执行。

您可以为 CPU 使用率、内存使用率和查询并发度设置阈值以触发查询队列。

## 启用查询队列

StarRocks 默认关闭查询队列。您可以通过设置相应的全局会话变量（Global session variable）来为 INSERT 导入、SELECT 查询和统计信息查询启用查询队列。

- 为导入任务启用查询队列：

```SQL
SET GLOBAL enable_query_queue_load = true;
```

- 为 SELECT 查询启用查询队列：

```SQL
SET GLOBAL enable_query_queue_select = true;
```

- 为统计信息查询启用查询队列：

```SQL
SET GLOBAL enable_query_queue_statistic = true;
```

## 指定资源阈值

您可以通过以下全局会话变量设置触发查询队列的阈值：

| **变量**                            | **默认值** | **描述**                                                     |
| ----------------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0          | 单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。      |
| query_queue_mem_used_pct_limit      | 0          | 单个 BE 节点中内存使用百分比上限。仅在设置为大于 `0` 后生效。取值范围：[0, 1] |
| query_queue_cpu_used_permille_limit | 0          | 单个 BE 节点中 CPU 使用千分比上限（即 CPU 使用率 * 1000）。仅在设置为大于 `0` 后生效。取值范围：[0, 1000] |

> **说明**
>
> 默认设置下，BE 每隔一秒向 FE 报告资源使用情况。您可以通过设置 BE 配置项 `report_resource_usage_interval_ms` 来更改此间隔时间。

## 配置查询队列

您可以通过以下全局会话变量设置查询队列的容量和队列中查询的最大超时时间：

| **变量**                           | **默认值** | **描述**                                                     |
| ---------------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024       | 队列中查询数量的上限。当达到此阈值时，新增查询将被拒绝执行。仅在设置为大于 `0` 后生效。 |
| query_queue_pending_timeout_second | 300        | 队列中单个查询的最大超时时间。当达到此阈值时，该查询将被拒绝执行。单位：秒。 |

## 查看查询队列统计信息

您可以通过以下方式查看查询队列的统计信息：

- 通过 [SHOW PROC](../sql-reference/sql-statements/Administration/SHOW_PROC.md) 查看 BE 节点运行查询的数量、内存和 CPU 使用情况：

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

- 通过 [SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW_PROCESSLIST.md) 查看查询是否在队列中（即 `IsPending` 为 `true` 时）：

```Plain
MySQL [(none)]> SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info              | IsPending |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
|    2 | root | xxx.xx.xxx.xx:xxxxx |       | Query   | 2022-11-24 18:08:29 |    0 | OK    | SHOW PROCESSLIST  | false     |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
```

- 查看 FE 审计日志文件 **fe.audit.log**。 其中 `PendingTimeMs` 字段表示查询在队列中等待的时间，单位为毫秒。

- 查看以下 FE Metrics：

| **Metrics**                      | **描述**                   |
| -------------------------------- | -------------------------- |
| starrocks_fe_query_queue_pending | 当前正在队列中的查询数量。 |
| starrocks_fe_query_queue_total   | 被列入队列的查询总数量。   |
| starrocks_fe_query_queue_timeout | 排队超时的查询总数量。     |
