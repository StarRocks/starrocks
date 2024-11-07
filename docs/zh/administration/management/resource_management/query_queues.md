---
displayed_sidebar: docs
---

# 查询队列

本文档介绍如何在 StarRocks 中管理查询队列。

自 v2.5 版本起，StarRocks 支持查询队列功能。启用查询队列后，StarRocks 会在并发查询数量或资源使用率达到一定阈值时自动对查询进行排队，从而避免过载加剧。待执行查询将在队列中等待直至有足够的计算资源时开始执行。自 v3.1.4 版本起，StarRocks 支持设置资源组粒度的查询队列功能。

您可以为 CPU 使用率、内存使用率和查询并发度设置阈值以触发查询队列。

**Roadmap**:

| 版本   | 全局查询队列 | 资源组粒度查询队列 | 并发数量集中管理 | 并发度动态调整 |
| ------ | --------- | --------------- | ------------- | ------------ |
| v2.5   | ✅        | ❌               | ❌            | ❌           |
| v3.1.4 | ✅        | ✅               | ✅            | ✅           |

## 启用查询队列

StarRocks 默认关闭查询队列。您可以通过设置相应的全局会话变量（Global session variable）来为 INSERT 导入、SELECT 查询和统计信息查询启用全局或资源组粒度的查询队列。

### 启用全局查询队列

设置以下全局会话变量来为导入任务、SELECT 查询或统计信息查询启用全局查询队列管理。

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

### 启用资源组粒度查询队列

从 v3.1.4 开始，StarRocks 支持资源组粒度查询队列。

如需启用资源组粒度查询队列，除上述全局会话变量之外，您还需要额外设置 `enable_group_level_query_queue`。

```SQL
SET GLOBAL enable_group_level_query_queue = true;
```

## 指定资源阈值

### 全局粒度的资源阈值

您可以通过以下全局会话变量设置触发查询队列的阈值：

| **变量**                            | **默认值** | **描述**                                                     |
| ----------------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_concurrency_limit       | 0          | 单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。      |
| query_queue_mem_used_pct_limit      | 0          | 单个 BE 节点中内存使用百分比上限。仅在设置为大于 `0` 后生效。取值范围：[0, 1] |
| query_queue_cpu_used_permille_limit | 0          | 单个 BE 节点中 CPU 使用千分比上限（即 CPU 使用率 * 1000）。仅在设置为大于 `0` 后生效。取值范围：[0, 1000] |

> **说明**
>
> 默认设置下，BE 每隔一秒向 FE 报告资源使用情况。您可以通过设置 BE 配置项 `report_resource_usage_interval_ms` 来更改此间隔时间。

### 资源组粒度的资源阈值

从 v3.1.4 开始，您可以在创建资源组时为其设置各自的并发查询上限 `concurrency_limit` 和 CPU 核数上限 `max_cpu_cores`。当发起一个查询时，如果任意一项资源占用超过了全局粒度或资源组粒度的资源阈值，那么查询会进行排队，直到所有资源都没有超过阈值，再执行该查询。

| **属性**          | **默认值** | **描述**                                                     |
| ----------------- | ---------- | ------------------------------------------------------------ |
| concurrency_limit | 0          | 该资源组在单个 BE 节点中并发查询上限。仅在设置为大于 `0` 后生效。 |
| max_cpu_cores     | 0          | 该资源组在单个 BE 节点中使用的 CPU 核数上限。仅在设置为大于 `0` 后生效。取值范围：[0, `avg_be_cpu_cores`]，其中 `avg_be_cpu_cores` 表示所有 BE 的 CPU 核数的平均值。 |

您可以通过 SHOW USAGE RESOURECE GROUPS 来查看每个资源组在每个 BE 上的资源使用信息，参见[查看资源组的使用信息](./resource_group.md#查看资源组的使用信息)。

### 管理查询并发数量

当正在运行的查询数量 `num_running_queries` 超过全局粒度或资源组粒度的 `concurrency_limit`  时，新到来的查询会进行排队。在 &lt; v3.1.4  和 &ge; v3.1.4 版本中，获取 `num_running_queries` 的方式不同。

- &lt; v3.1.4 版本，`num_running_queries` 由 BE 周期性汇报得出正在运行的查询数量，汇报周期为 `report_resource_usage_interval_ms`。所以，系统对于 `num_running_queries` 的变化感知会有一定的延迟。例如，如果当下 BE 汇报的 `num_running_queries` 没有超过全局粒度和资源组粒度的 `concurrency_limit`，但是在下次汇报前如果发起了大量查询，超过了 `concurrency_limit` 的限制，那么这些新查询也都会执行，而不会进行排队。

- &ge; v3.1.4 版本，所有 FE 正在运行的查询数量 `num_running_queries` 由 Leader FE 集中管理。每个 Follower FE 在发起和结束一个查询时，会通知 Leader FE，从而可以应对短时间内查询激增超过了 `concurrency_limit` 的场景。

## 配置查询队列

您可以通过以下全局会话变量设置查询队列的容量和队列中查询的最大超时时间：

| **变量**                           | **默认值** | **描述**                                                     |
| ---------------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_max_queued_queries     | 1024       | 队列中查询数量的上限。当达到此阈值时，新增查询将被拒绝执行。仅在设置为大于 `0` 后生效。 |
| query_queue_pending_timeout_second | 300        | 队列中单个查询的最大超时时间。当达到此阈值时，该查询将被拒绝执行。单位：秒。 |

## 根据查询并发数量动态调整查询并发度

从 v3.1.4 版本起，对于被查询队列管理的由 Pipeline Engine 运行的查询，StarRocks 可以根据当前正在运行的查询数量 `num_running_queries`、Fragment 数量 `num_fragments`、查询并发度 `pipeline_dop` 动态调整新到来查询的并发度 `pipeline_dop`。您可以通过这种方式动态控制查询并发数量，在保证 BE 资源充分利用的基础上，降低调度的开销。关于 Fragment 和查询并发度 `pipeline_dop`，参见[查询管理-调整查询并发度](./Query_management.md#调整查询并发度)。

针对每个查询队列下的查询，StarRocks 会维护一个逻辑 Driver 数量 `num_drivers`，用以表示该查询的所有 Fragment 在一个 BE 上总的并发数量。`num_drivers` 的值等于 `num_fragments*pipeline_dop`。当新到来一个查询时，StarRocks 会根据以下逻辑调整查询并发度 `pipeline_dop`：

- 正在运行的 Driver 数量 `num_drivers` 超过查询并发 Driver 低位上限 `query_queue_driver_low_water` 越多，查询并发度 `pipeline_dop` 越小。
- StarRocks 尽量保证正在运行的 Driver 数量 `num_drivers` 不超过查询并发 Driver 高位上限 `query_queue_driver_high_water`。

您可以通过以下全局会话变量控制查询并发度 `pipeline_dop` 的动态调整：

| **变量**                      | **默认值** | **描述**                                                     |
| ----------------------------- | ---------- | ------------------------------------------------------------ |
| query_queue_driver_high_water | -1         | 查询并发 Driver 高位上限。仅在设置为大于 `0` 后生效。等于 `0` 时，会设置为 `avg_be_cpu_cores*16`，其中 `avg_be_cpu_cores` 表示所有 BE 的 CPU 核数的平均值。大于 `0` 时，会直接使用该值。 |
| query_queue_driver_low_water  | -1         | 查询并发 Driver 低位上限。仅在设置为大于 `0` 后生效。等于 `0` 时，会设置为 `avg_be_cpu_cores*8`。大于 `0` 时，会直接使用该值。 |

## 观测查询队列

您可以通过以下方式查看查询队列相关的信息。

### SHOW PROC

通过 [SHOW PROC](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROC.md) 查看 BE 节点运行查询的数量、内存和 CPU 使用情况：

```Plain
mysql> SHOW PROC '/backends'\G
*************************** 1. row ***************************
...
    NumRunningQueries: 0
           MemUsedPct: 0.79 %
           CpuUsedPct: 0.0 %
```

### SHOW PROCESSLIST

通过 [SHOW PROCESSLIST](../../../sql-reference/sql-statements/cluster-management/nodes_processes/SHOW_PROCESSLIST.md) 查看查询是否在队列中（即 `IsPending` 为 `true` 时）：

```Plain
MySQL [(none)]> SHOW PROCESSLIST;
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
| Id   | User | Host                | Db    | Command | ConnectionStartTime | Time | State | Info              | IsPending |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
|    2 | root | xxx.xx.xxx.xx:xxxxx |       | Query   | 2022-11-24 18:08:29 |    0 | OK    | SHOW PROCESSLIST  | false     |
+------+------+---------------------+-------+---------+---------------------+------+-------+-------------------+-----------+
```

### FE 审计日志

查看 FE 审计日志文件 **fe.audit.log**。 其中 `PendingTimeMs` 字段表示查询在队列中等待的时间，单位为毫秒。

### 监控指标

您可以通过[监控报警](../monitoring/Monitor_and_Alert.md)功能获取相应监控指标观测查询队列。下列 FE 指标为各 FE 节点基于自身的统计数据得出。

| 指标                                            | 单位 | 类型   | 描述                                                         |
| ----------------------------------------------- | ---- | ------ | --------------------------------------------------------- |
| starrocks_fe_query_queue_pending                | 个   | 瞬时值 | 当前正在队列中的查询数量。                                      |
| starrocks_fe_query_queue_total                  | 个   | 瞬时值 | 历史排队过的查询数量（包括正在运行的查询）。                       |
| starrocks_fe_query_queue_timeout                | 个   | 瞬时值 | 排队超时的查询总数量。                                         |
| starrocks_fe_resource_group_query_queue_total   | 个   | 瞬时值 | 该资源组历史排队的查询数量（包括正在运行的查询）。Label `name` 表示该资源组的名称。从 v3.1.4 版本起，StarRocks 支持该指标。 |
| starrocks_fe_resource_group_query_queue_pending | 个   | 瞬时值 | 该资源组正在排队的查询数量。Label `name` 表示该资源组的名称。从 v3.1.4 版本起，StarRocks 支持该指标。 |
| starrocks_fe_resource_group_query_queue_timeout | 个   | 瞬时值 | 该资源组排队超时的查询数量。Label `name` 表示该资源组的名称。从 v3.1.4 版本起，StarRocks 支持该指标。 |

### SHOW RUNNING QUERIES

从 v3.1.4 版本开始，StarRocks 支持 SQL 语句 SHOW RUNNING QUERIES，用于展示每个查询的队列信息。各字段的含义如下：

- `QueryId`：该查询的 Query ID。
- `ResourceGroupId`：该查询命中的资源组 ID。当没有命中用户定义的资源组时，会显示为 “-”。
- `StartTime`：该查询开始时间。
- `PendingTimeout`：PENDING 状态下查询在队列中超时的时间。
- `QueryTimeout`：该查询超时的时间。
- `State`：该查询的排队状态。其中，PENDING 表示在队列中；RUNNING 表示正在执行。
- `Slots`：该查询申请的逻辑资源数量，目前固定为 `1`。
- `Frontend`：发起该查询的 FE 节点。
- `FeStartTime`：发起该查询的 FE 节点的启动时间。

示例：

```Plain
MySQL [(none)]> SHOW RUNNING QUERIES;
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
| QueryId                              | ResourceGroupId | StartTime           | PendingTimeout      | QueryTimeout        |   State   | Slots | Frontend                        | FeStartTime         |
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
| a46f68c6-3b49-11ee-8b43-00163e10863a | -               | 2023-08-15 16:56:37 | 2023-08-15 17:01:37 | 2023-08-15 17:01:37 |  RUNNING  | 1     | 127.00.00.01_9010_1692069711535 | 2023-08-15 16:37:03 |
| a6935989-3b49-11ee-935a-00163e13bca3 | 12003           | 2023-08-15 16:56:40 | 2023-08-15 17:01:40 | 2023-08-15 17:01:40 |  RUNNING  | 1     | 127.00.00.02_9010_1692069658426 | 2023-08-15 16:37:03 |
| a7b5e137-3b49-11ee-8b43-00163e10863a | 12003           | 2023-08-15 16:56:42 | 2023-08-15 17:01:42 | 2023-08-15 17:01:42 |  PENDING  | 1     | 127.00.00.03_9010_1692069711535 | 2023-08-15 16:37:03 |
+--------------------------------------+-----------------+---------------------+---------------------+---------------------+-----------+-------+---------------------------------+---------------------+
```
