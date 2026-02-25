---
displayed_sidebar: docs
sidebar_position: 11
---

import Experimental from '../_assets/commonMarkdown/_experimental.mdx'

# 查询反馈

<Experimental />

本主题介绍查询反馈功能、其应用场景，以及如何基于执行统计信息使用查询计划顾问优化查询计划。

StarRocks 从 v3.4.0 开始支持查询反馈功能。

## 概述

查询反馈是一个框架，也是CBO优化器的关键组件。它在查询执行期间记录执行统计信息，并在后续具有类似查询计划的查询中重用这些信息，以帮助CBO生成优化的查询计划。CBO基于估计的统计信息优化查询计划，因此当统计信息过时或不准确时，可能会选择低效的查询计划（坏计划），例如广播大表或错误排序左右表。这些坏计划可能导致查询执行超时、资源消耗过多，甚至系统崩溃。

## 工作流程

基于查询反馈的计划优化工作流程包括三个阶段：

1. **观察**：BE或CN记录每个查询计划中PlanNode的主要指标（包括`InputRows`和`OutputRows`）。
2. **分析**：对于超过配置阈值的慢查询和手动标记为需要分析的查询，系统将在查询完成后、结果返回前分析关键节点的执行细节，以识别当前查询计划中的优化机会。FE将查询计划与执行统计信息进行比较，并检查查询是否由于异常查询计划导致慢查询。在分析不准确的统计信息时，FE会为每个查询生成SQL调优指南，指导CBO动态优化查询，并推荐提高性能的策略。
3. **优化**：在CBO生成物理计划后，它将搜索适用于该计划的现有调优指南。如果存在，CBO将根据指南和策略动态优化计划，纠正问题部分，从而消除由于重复使用坏查询计划对查询性能的影响。优化计划的执行时间与原始计划进行比较，以评估调优的有效性。

## 用法

受系统变量`enable_plan_advisor`（默认值：`true`）控制，查询计划顾问默认对慢查询启用，即执行时间超过FE配置项`slow_query_analyze_threshold`（默认值：`5`秒）的查询。

此外，您可以手动分析特定查询或为所有执行的查询启用自动分析。

### 手动分析特定查询

即使执行时间未超过`slow_query_analyze_threshold`，您也可以手动分析特定查询语句。

```SQL
ALTER PLAN ADVISOR ADD <query_statement>
```

示例：

```SQL
ALTER PLAN ADVISOR ADD SELECT COUNT(*) FROM (
    SELECT * FROM c1_skew_left_over t1 
    JOIN (SELECT * FROM c1_skew_left_over WHERE c1 = 'c') t2 
    ON t1.c2 = t2.c2 WHERE t1.c1 > 'c' ) t;
```

### 为所有查询启用自动分析

要为所有查询启用自动分析，您需要将系统变量`enable_plan_analyzer`（默认值：`false`）设置为`true`。

```SQL
SET enable_plan_analyzer = true;
```

### 显示当前FE上的调优指南

每个FE维护其自己的调优指南记录。您可以使用以下语句查看当前FE上为授权查询生成的调优指南：

```SQL
SHOW PLAN ADVISOR
```

### 检查调优指南是否生效

对查询语句执行[EXPLAIN](../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md)。在EXPLAIN字符串中，消息`Plan had been tuned by Plan Advisor`表示已对相应查询应用了调优指南。

示例：

```SQL
EXPLAIN SELECT COUNT(*) FROM (
    SELECT * FROM c1_skew_left_over t1 
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

### 删除特定查询的调优指南

您可以根据`SHOW PLAN ADVISOR`返回的查询ID删除特定查询的调优指南。

```SQL
ALTER PLAN ADVISOR DROP <query_id>
```

示例：

```SQL
ALTER PLAN ADVISOR DROP "8e010cf4-b178-11ef-8aa4-8a5075cec65e";
```

### 清除当前FE上的所有调优指南

要清除当前FE上的所有调优指南，请执行以下语句：

```SQL
TRUNCATE PLAN ADVISOR
```

## 使用案例

目前，查询反馈主要用于优化以下场景：

- 优化本地Join节点中左右两侧的顺序
- 优化本地Join节点的执行方式（例如，从Broadcast切换到Shuffle）
- 对于具有显著聚合潜力的情况，强制使用`pre_aggregation`模式以在第一阶段最大化数据聚合

调优指南主要基于指标`Runtime Exec Node Input/Output Rows`和FE`statistics estimated rows`。由于当前调优阈值相对保守，建议您利用查询反馈检查查询性能概况或EXPLAIN字符串中观察到的问题以获取潜在的性能改进。

以下是三个常见的用户案例。

### 案例1：错误的Join顺序

原始坏计划：

```SQL
small left table inner join large table (broadcast)
```

优化后的计划：

```SQL
large left table inner join small right table (broadcast)
```

**原因** 问题可能是由于统计信息过时或缺失，导致CBO优化器基于不准确的数据生成错误的计划。

**解决方案** 在查询执行期间，系统比较左子节点和右子节点的`input/output rows`和`statistics estimated rows`，生成调优指南。在重新执行时，系统自动调整Join顺序。

### 案例2：错误的Join执行方式

原始坏计划：

```SQL
large left table1 inner join large right table2 (broadcast)
```

优化后的计划：

```SQL
large left table1 (shuffle) inner join large right table2 (shuffle)
```

**原因** 问题可能由数据倾斜引起。当右表有很多分区且其中一个分区包含不成比例的大量数据时，系统可能错误地估计应用谓词后的右表行数。

**解决方案** 在查询执行期间，系统比较左子节点和右子节点的`input/output rows`和`statistics estimated rows`，生成调优指南。优化后，Join方法从Broadcast Join调整为Shuffle Join。

### 案例3：低效的第一阶段预聚合模式

**症状** 对于具有良好聚合潜力的数据，第一阶段聚合的`auto`模式可能仅聚合少量本地数据，错失性能提升的机会。

**解决方案** 在查询执行期间，系统收集本地和全局聚合的`Input/Output Rows`。基于历史数据，评估聚合列的潜力。如果潜力显著，系统在本地聚合中强制使用`pre_aggregation`模式，最大化第一阶段的数据聚合，提高整体查询性能。

## 限制

- 调优指南只能用于生成它的完全相同的查询。不适用于具有相同模式但不同参数的查询。
- 每个FE独立管理其查询计划顾问，不支持FE节点间的同步。如果相同的查询提交到不同的FE节点，调优结果可能会有所不同。
- 查询计划顾问使用内存缓存结构：
  - 当调优指南数量超过限制时，过期的调优指南会自动被驱逐。
  - 调优指南的默认限制为300，不支持历史调优指南的持久化。