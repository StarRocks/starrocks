---
displayed_sidebar: docs
---

# Query Profile 概述

本文介绍如何查看并分析 Query Profile。Query Profile 记录了查询中涉及的所有工作节点的执行信息。您可以通过 Query Profile 快速识别影响 StarRocks 集群查询性能的瓶颈。

自 v3.3.0 起，StarRocks 支持为 INSERT INTO FILES() 和 Broker Load 导入方式提供 Query Profile。相关指标详情，请参考 [OlapTableSink Operator](./query_profile_details.md#olaptablesink-operator)。

## 启用 Query Profile

您可以通过将变量 `enable_profile` 设置为 `true` 以启用 Query Profile：

```SQL
SET enable_profile = true;
```

### 针对慢查询开启 Query Profile

在生产环境中，通常不推荐全面启用 Query Profile 功能。这是因为 Query Profile 的数据采集和处理过程可能会为系统带来额外的负担。然而，如果需要捕捉到耗时的慢查询，就需要巧妙地使用这一功能。为此，您可以选择只对慢查询启用 Query Profile。这可以通过设置变量 `big_query_profile_threshold` 为一个大于 `0s` 的时间来实现。例如，若将此变量设置为 `30s`，意味着只有那些执行时间超过 30 秒的查询会启用 Query Profile 功能。这样既保证了系统性能，又能有效监控到慢查询。

```SQL
-- 30 seconds
SET global big_query_profile_threshold = '30s';

-- 500 milliseconds
SET global big_query_profile_threshold = '500ms';

-- 60 minutes
SET global big_query_profile_threshold = '60m';
```

### 启用 Runtime Query Profile

某些查询可能需要耗费较长时间执行，从数十秒到数小时不等。在查询完成之前，经常无法判断查询是否还在进行或是系统已经死机。为解决这个问题，StarRocks 在 v3.1 及以上版本中引入了 Runtime Query Profile 功能。此功能允许 StarRocks 在查询执行过程中，按固定时间间隔收集并上报 Query Profile 数据。这使您能够实时了解查询的执行进度和潜在的瓶颈点，而不必等到查询完全结束。通过这种方式，您可以更有效地监控和优化查询过程。

当 Query Profile 启用时，该功能会自动启用，默认的上报时间间隔为 10 秒。您可以通过修改变量 `runtime_profile_report_interval` 来调整对应的时间间隔：

```SQL
SET runtime_profile_report_interval = 30;
```

Runtime Query Profile 与普通 Query Profile 格式和内容均相同。您可以像分析常规 Query Profile 一样分析 Runtime Query Profile，以此了解集群内正在运行的查询的性能指标。

### 配置 Query Profile 行为

| 配置方式 | 配置项 | 可选值 | 默认值 | 含义 |
| -- | -- | -- | -- | -- |
| Session 变量 | enable_profile | true/false | false |是否启用 Query Profile 功能。`true` 表示启用。 |
| Session 变量 | pipeline_profile_level | 1/2 | 1 | 设置 Query Profile 的级别。`1` 表示会对 Profile 进行合并展示；`2` 表示保留原始的 Profile，如果选用这一级别，那么所有可视化的分析工具将不再起作用，因此通常不建议修改该参数。 |
| Session 变量 | runtime_profile_report_interval | 正整数 | 10 | 设置 Runtime Query Profile 上报的时间间隔，单位秒。 |
| Session 变量 | big_query_profile_threshold | 字符串 | `0s` | 设置长查询自动开启 Query Profile 的阈值，`0s` 表示关闭该功能。整数结合时间单位表示启用，可以用单位包括：`ms`、`s`、`m`。 |
| FE 动态配置项 | enable_statistics_collect_profile | true/false | false | 是否启用统计信息采集相关查询的 Query Profile。`true` 表示启用。 |

## 获取 Query Profile

### 通过 Web 页面获取

以下步骤获取 Query Profile：

1. 在浏览器中访问 `http://<fe_ip>:<fe_http_port>`。
2. 在显示的页面上，单击顶部导航中的 **queries**。
3. 在 **Finished Queries** 列表中，选择您要分析的查询并单击 **Profile** 列中的链接。

![img](../_assets/profile-1.png)

页面将跳转至相应 Query Profile。

![img](../_assets/profile-2.png)

### 通过 get_query_profile 函数获取

以下示例展示了如何获取 Query Profile：

```
-- Enable the profiling feature.
set enable_profile = true;

-- Run a simple query.
select 1;

-- Get the query_id of the query.
select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| bd3335ce-8dde-11ee-92e4-3269eb8da7d1 |
+--------------------------------------+

-- Obtain the query profile.
select get_query_profile('502f3c04-8f5c-11ee-a41f-b22a2c00f66b')\G
```

## 分析 Query Profile

Query Profile 生成的原始文本内容非常庞大，包含了众多指标。如需详细了解这些指标，请参阅[Query Profile 结构与详细指标](./query_profile_details.md)。

然而，对于非开发者的普通用户来说，直接分析这些原始文本可能并非易事。为了解决这一问题，StarRocks 提供了 [文本可视化 Query Profile](./query_profile_text_based_analysis.md) 的分析方式。您可以通过该功能更直观地理解复杂 Query Profile 的含义。
