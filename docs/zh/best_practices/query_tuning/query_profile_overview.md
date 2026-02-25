---
displayed_sidebar: docs
sidebar_position: 30
keywords: ['profile', 'query']
---

# Query Profile 概述

## 简介

Query Profile 记录了查询中所有工作节点的执行信息，帮助您快速识别影响查询性能的瓶颈。它是诊断和调优 StarRocks 查询性能的强大工具。

> 从 v3.3.0 开始，StarRocks 支持为使用 INSERT INTO FILES() 和 Broker Load 的数据导入提供 Query Profile。有关涉及的指标详情，请参见 [OlapTableSink Operator](./query_profile_operator_metrics.md#olaptablesink-operator)。

## 如何启用 Query Profile

### 启用 Query Profile

您可以通过将变量 `enable_profile` 设置为 `true` 来启用 Query Profile：

```SQL
SET enable_profile = true;
SET GLOBAL enable_profile = true;
```

### 慢查询的 Query Profile

不建议在生产环境中长时间全局启用 Query Profile，因为这可能会增加系统开销。为了仅捕获和分析慢查询，可以将变量 `big_query_profile_threshold` 设置为大于 `0s` 的时间。例如，将其设置为 `30s` 表示只有超过 30 秒的查询才会触发 Query Profile。

```SQL
-- 30 秒
SET global big_query_profile_threshold = '30s';

-- 500 毫秒
SET global big_query_profile_threshold = '500ms';

-- 60 分钟
SET global big_query_profile_threshold = '60m';
```

### 运行时 Query Profile

对于长时间运行的查询，可能难以在完成前确定进度或检测问题。运行时 Query Profile 功能（v3.1+）在执行期间以固定间隔收集并报告 Query Profile 数据，提供关于查询进度和瓶颈的实时洞察。

当启用 Query Profile 时，运行时 Query Profile 会自动激活，默认报告间隔为 10 秒。可以通过 `runtime_profile_report_interval` 调整间隔：

```SQL
SET runtime_profile_report_interval = 30;
```

### 配置

| 配置项                           | 类型         | 有效值          | 默认值 | 描述                                                                                     |
|----------------------------------|--------------|-----------------|---------|------------------------------------------------------------------------------------------|
| enable_profile                   | Session Var  | true/false      | false   | 启用 Query Profile                                                                       |
| pipeline_profile_level           | Session Var  | 1/2             | 1       | 1: 合并指标；2: 保留原始结构（禁用可视化工具）                                           |
| runtime_profile_report_interval  | Session Var  | 正整数          | 10      | 运行时 Query Profile 报告间隔（秒）                                                      |
| big_query_profile_threshold      | Session Var  | 字符串          | 0s      | 启用 Query Profile 的查询超过此持续时间（例如，'30s'，'500ms'，'60m'）                    |
| enable_statistics_collect_profile| FE Dynamic   | true/false      | false   | 启用与统计信息收集相关的查询的 Query Profile                                              |

## 如何获取 Query Profile

### 通过 Web UI

1. 在浏览器中访问 `http://<fe_ip>:<fe_http_port>`。
2. 点击顶部导航中的 **queries**。
3. 在 **Finished Queries** 列表中，选择要分析的查询并点击 **Profile** 列中的链接。

![img](../../_assets/profile-1.png)

您将被重定向到所选 Query Profile 的详细页面。

![img](../../_assets/profile-2.png)

### 通过 SQL 函数 (`get_query_profile`)

示例工作流程：
- `last_query_id()`: 返回会话中最近执行的查询的 ID。用于快速检索上一个查询的 profile。
- `show profilelist;`: 列出最近的查询及其 ID 和状态。使用此功能找到分析 profile 所需的 `query_id`。
- `get_query_profile('<query_id>')`: 返回指定查询的详细执行 profile。用于分析查询的执行方式以及时间或资源的消耗位置。

```sql
-- 启用 profiling 功能。
SET enable_profile = true;

-- 运行一个包含扫描和聚合的查询以生成有意义的 Profile。
-- （使用系统表确保该查询可在任何集群上运行）
SELECT count(*) FROM information_schema.columns;

-- 获取查询的 query_id。
SELECT last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| 019b364f-10c4-704c-b79a-af2cc3a77b89 |
+--------------------------------------+

-- 获取 profile 列表
SHOW PROFILELIST;

-- 获取查询 profile。
SELECT get_query_profile('019b364f-10c4-704c-b79a-af2cc3a77b89')\G
```

### 在托管版本中

在 StarRocks 托管（企业）环境中，您可以方便地从 Web 控制台的查询历史中直接访问查询 profile。托管 UI 提供了每个查询执行 profile 的直观可视化表示，使您无需手动 SQL 命令即可轻松分析性能和识别瓶颈。

## 解读 Query Profile

### Explain Analyze

大多数用户可能会发现直接分析原始文本具有挑战性。StarRocks 提供了一种 [基于文本的 Query Profile 可视化分析](./query_profile_text_based_analysis.md) 方法，以便更直观地理解。

### 托管版本

在 StarRocks 企业版（EE）中，托管版本提供了内置的查询 profile 可视化工具。该工具提供了一个交互式图形界面，与原始文本输出相比，使解释复杂的查询执行细节变得更加容易。

**可视化工具的主要功能包括：**
- **Operator 级别分解：** 以树或图的形式查看执行计划，每个 operator 的指标（时间、行数、内存）清晰显示。
- **瓶颈高亮：** 通过颜色编码的指示器快速识别慢速或资源密集的 operator。
- **深入分析能力：** 点击任意 operator 查看详细统计信息，包括输入/输出行数、CPU 时间、内存使用等。

**如何使用：**
1. 打开 StarRocks 托管 Web 控制台。
2. 导航到 **Query** 或 **Query History** 部分。
3. 选择一个查询并点击 **Profile** 或 **Visualize** 按钮。
4. 探索可视化的 profile 以分析性能并识别优化机会。

此可视化工具仅限于托管/企业版，旨在加速复杂工作负载的故障排除和性能调优。