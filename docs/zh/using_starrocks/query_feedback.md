---
displayed_sidebar: docs
sidebar_position: 11
---

# Query Feedback

本文介绍 Query Feedback 功能的基本概念和使用场景，以及如何利用 Query Plan Advisor 基于执行详情反馈优化查询计划（Query Plan）。

StarRocks 自 v3.4.0 起支持 Query Feedback 功能。

## 概述

Query Feedback 是一个框架，属于 CBO 优化器的重要组成部分。它记录查询执行过程中的执行统计信息，并在具有类似计划的后续查询中重复使用该统计信息，从而帮助 CBO 优化器优化查询计划。当前 CBO 优化器根据估计的统计信息优化查询计划，因此在统计信息更新不及时或估算错误的情况下，CBO 优化器可能会选择质量较差的查询计划（Bad Plan），例如错误地将大表 Braodcast，或左右表顺序选择不当。这些 Bad Plan 会导致查询执行超时或占用大量系统资源，甚至可能引发系统崩溃。

## 工作流程

基于 Query Feedback 的计划优化工作流程包括以下三个阶段：

1. **观测阶段**：BE 或 CN 记录每个查询计划中 PlanNode 的主要指标（包括 InputRows 和 OutputRows）。
2. **分析阶段**：针对超出特定阈值的慢查询和手动标记分析的查询，系统将在查询结束后、结果返回前，结合该查询在关键节点的执行详情，自动分析当前查询计划中是否存在需要优化的部分。FE 将查询计划与执行统计数据进行比较，并确认是由于异常查询计划导致查询慢。FE 在分析不准确统计数据的同时，会为每个查询生成 SQL Tuning Guide，指示 CBO 优化器动态调优，并推荐调优操作。
3. **动态调整阶段**：CBO 优化器生成物理计划后，将搜索适用于该计划的任何现有 Tuning Guide。如果有，CBO 优化器将根据 Tuning Guide 和策略动态优化计划，修正问题部分，从而消除因重复使用 Bad Plan 而对查询性能造成的影响。调优后的查询计划会与原始计划的执行耗时进行对比，从而评估调优效果。

## 使用方法

Query Plan Advisor 功能通过系统变量 `enable_plan_advisor`（默认值：`true`）控制。该功能针对慢查询默认启用，即耗时超过 FE 配置项 `slow_query_analyze_threshold`（默认值：`5` 秒）中配置的阈值的查询。

此外，您还可以手动分析特定查询，或为所有查询启用自动分析。

### 手动分析特定查询

即使特定查询语句的执行时间未超过 `slow_query_analyze_threshold` 中设置的阈值，您也可以为其启用 Query Plan Advisor。

```SQL
ALTER PLAN ADVISOR ADD <query_statement>
```

示例：

```SQL
ALTER PLAN ADVISOR ADD SELECT COUNT(*) FROM (
    SLECT * FROM c1_skew_left_over t1 
    JOIN (SELECT * FROM c1_skew_left_over WHERE c1 = 'c') t2 
    ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t;
```

### 为所有查询启用自动分析

如需为所有查询启用自动分析，需将系统变量 `enable_plan_analyzer`（默认值：`false`）设置为 `true`。

```SQL
SET enable_plan_analyzer = true;
```

### 显示当前 FE 上的 Tuning Guide

每个 FE 独立管理其中的 Tuning Guide。您可以使用以下语句查看当前 FE 中保存的 Tuning Guide：

```SQL
SHOW PLAN ADVISOR
```

### 检查 Tuning Guide 是否生效

针对查询语句执行 [EXPLAIN](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md)，如果在 EXPLAIN 字符串中出现 `Plan had been tuned by Plan Advisor` 消息，则表示 Tuning Guide 已应用于相应查询。

示例：

```SQL
EXPLAIN SELECT COUNT(*) FROM (
    SLECT * FROM c1_skew_left_over t1 
    JOIN (SELECT * FROM c1_skew_left_over WHERE c1 = 'c') t2 
    ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t;
+-----------------------------------------------------------------------------------------------+
| Explain String                                                                                |
+-----------------------------------------------------------------------------------------------+
| Plan had been tuned by Plan Advisor.                                                          |
| Original query id:8e010cf4-b178-11ef-8aa4-8a5075cec65e                                        |
| Original time cost: 148 ms                                                                    |
| 1: LeftChildEstimationErrorTuningGuide                                                        |
| Reason: left child statistics of JoinNode 5 had been overestimated.                           |
| Advice: Adjust the distribution join execution type and join plan to improve the performance. |
|                                                                                               |
| PLAN FRAGMENT 0                                                                               |
|  OUTPUT EXPRS:9: count                                                                        |
|   PARTITION: UNPARTITIONED                                           
```

### 删除特定查询的 Tuning Guide

您可以根据 SHOW PLAN ADVISOR 中返回的 Query ID 删除特定查询的 Tuning Guide。

```SQL
ALTER PLAN ADVISOR DROP <query_id>
```

示例：

```SQL
ALTER PLAN ADVISOR DROP "8e010cf4-b178-11ef-8aa4-8a5075cec65e";
```

### 清除当前 FE 上的所有 Tuning Guide

要清除当前 FE 上的所有 Tuning Guide，请执行以下语句：

```SQL
TRUNCATE PLAN ADVISOR
```

## 应用场景

当前，Query Feedback 主要用于优化以下三种场景：

- 优化局部 Join 节点的左右顺序
- 优化局部 Join 节点的执行方式（例如：从 Broadcast 转为 Shuffle）
- 针对聚合效果较好的情况，强制启用第一阶段预聚合 (`pre_aggregation`)，尽可能在第一阶段完成更多数据聚合

调优策略主要依据 `Runtime Exec Node Input/Output Rows` 的 FE `Statistics Estimated Rows` 指标。 由于当前的调优阈值较为保守，建议您在从 EXPLAIN 返回或 Query Profile 中发现上述问题时，尝试使用 Query Feedback 来验证是否能获得正向优化效果。

以下列举三种常见的用户案例。

### 案例一：错误的 Join 顺序

原始 Bad Plan：

```SQL
small left table inner join large table (broadcast)
```

优化后的查询计划：

```SQL
large left table inner join small right table (broadcast)
```

**问题原因**

可能由于统计信息未采集或更新不及时，CBO 基于错误的统计数据生成了错误的执行计划。

**解决方案**

执行过程中，系统会对 Left 和 Right Child 的 `Input/Output Rows` 以及 `Statistics Estimated Rows` 进行对比，并生成 Tuning Guide。在该 SQL 再次被执行时，系统会自动校正 Join 顺序。

### 案例二：错误的 Join 执行方式

原始 Bad Plan：

```SQL
large left table1 inner join large right table2 (broadcast)
```

优化后的查询计划：

```SQL
large left table1 (shuffle) inner join large right table2 (shuffle)
```

**问题原因**

可能由于存在数据倾斜。当右表存在大量分区且某个分区数据量异常大时，谓词应用后，右表的行数估算值出现严重偏差。

**解决方案**

执行过程中，系统会对 Left 和 Right Child 的 `Input/Output rows` 以及 `Statistics Estimated Rows` 进行对比，并生成 Tuning Guide。优化后会将 Broadcast Join 改为 Shuffle Join。

### 案例三：第一阶段预聚合效果差

**现象**

尽管数据本身具有较好的聚合效果，但对于当前查询，第一阶段预聚合的 `auto` 模式只对少量本地数据进行了聚合，未充分发挥性能优势。

**解决方案**

执行过程中，系统会采集本地 (Local) 和全局 (Global) 聚合的 `Input/Output Rows` 数据，结合历史信息判断聚合列的聚合效果。如果聚合效果优异，则系统会强制将本地聚合设置为 `pre_aggregation` 模式，以在第一阶段尽可能多地聚合数据，从而提升整体查询性能。

## 限制

- Tuning Guide 仅可用于与其生成时相同的查询，不适用于具有相同模式但不同参数的查询。
- 每个 FE 独立管理其 Query Plan Advisor，不支持多节点间的同步。如果同一 SQL 被发送到不同的 FE 节点，可能会得到不同的调优结果。
- Query Plan Advisor 使用基于内存的缓存结构：
  - 当 Tuning Guide 的数量超过限制时，系统会自动淘汰过期数据。
  - Tuning Guide 的默认数量上限为 300。系统不支持对历史 Tuning Guide 进行持久化存储。
