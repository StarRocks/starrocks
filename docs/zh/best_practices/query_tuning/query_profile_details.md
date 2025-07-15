---
displayed_sidebar: docs
keywords: ['profile', 'query']
---

# 概述

Query Profile 是一份详细报告，提供了关于在 StarRocks 中执行 SQL 查询的深入见解。它提供了查询性能的全面视图，包括每个操作所花费的时间、处理的数据量以及其他相关指标。这些信息对于优化查询性能、识别瓶颈和排查问题非常宝贵。

:::tip 为什么这很重要
80% 的实际慢查询可以通过识别三个警示指标之一来解决。本速查表可以在您被数据淹没之前帮助您找到问题。
:::

## 快速开始

分析最近的查询：

### 1. 列出最近的查询 ID

需要查询 ID 来分析查询配置文件。使用 `SHOW PROFILELIST;`：

```sql
SHOW PROFILELIST;
```

:::tip
`SHOW PROFILELIST` 的详细信息在 [基于文本的查询配置文件可视化分析](./query_profile_text_based_analysis.md) 中。如果您刚开始，请参阅该页面。
:::

### 2. 将配置文件与您的 SQL 并排打开

运行 `ANALYZE PROFILE FOR <query_id>\G` 或在 CelerData Web UI 中点击 **Profile**。

### 3. 浏览“执行概览”横幅

检查三个高信号指标：**QueryExecutionWallTime**（总运行时间）、**QueryPeakMemoryUsagePerNode**——如果值超过 BE 内存的大约 80%，则暗示可能会溢出或 OOM——以及 **QueryCumulativeCpuTime / WallTime** 的比率。如果该比率低于 CPU 核心数的一半，查询主要在等待 I/O 或网络。

如果没有触发，您的查询通常没问题——到此为止。

### 4. 更深入一层

识别消耗最多时间或内存的 operators，分析其指标，并确定根本原因以找出性能瓶颈。

"Operator Metrics" 部分提供了许多指导方针，以帮助识别性能问题的根本原因。

## 核心概念

### 查询执行流程

SQL 查询的全面执行流程包括以下阶段：
1. **规划**：查询经过解析、分析和优化，最终生成查询计划。
2. **调度**：调度器和协调器协同工作，将查询计划分发到所有参与的后端节点。
3. **执行**：查询计划使用流水线执行引擎执行。

![SQL 执行流程](../../_assets/Profile/execution_flow.png)

### 查询计划结构

StarRocks 计划是分层的：**Fragment** 是工作的顶级切片；每个 fragment 生成多个在不同后端节点上运行的 **FragmentInstances**。在一个实例中，**Pipeline** 将 operators 串联在一起；多个 **PipelineDrivers** 在不同的 CPU 核心上并发运行相同的 pipeline；**Operator** 是实际处理数据的原子步骤——扫描、连接、聚合。

![profile-3](../../_assets/Profile/profile-3.png)

### 流水线执行引擎概念

Pipeline Engine 是 StarRocks 执行引擎的关键组件。它负责以并行和高效的方式执行查询计划。Pipeline Engine 旨在处理复杂的查询计划和大量数据，确保高性能和可扩展性。

在流水线引擎中，一个 **Operator** 实现一个算法，一个 **Pipeline** 是 operators 的有序链，多个 **PipelineDrivers** 并行保持该链的忙碌。轻量级用户空间调度器分配时间，以便驱动程序在不阻塞的情况下前进。

![pipeline_opeartors](../../_assets/Profile/pipeline_operators.png)

### 指标合并策略

默认情况下，StarRocks 合并 FragmentInstance 和 PipelineDriver 层以减少配置文件体积，形成简化的三层结构：
- Fragment
- Pipeline
- Operator

您可以通过会话变量 `pipeline_profile_level` 控制此合并行为：
- `1`（默认）：合并的三层结构
- `2`：原始五层结构
- 其他值：视为 `1`

当配置文件压缩开启时，引擎**平均时间指标**（例如，`OperatorTotalTime`，而 `__MAX_OF_` / `__MIN_OF_` 记录极值），**累加计数器**如 `PullChunkNum`，并保持**常量属性**如 `DegreeOfParallelism` 不变。

MIN 和 MAX 值之间的显著差异通常表明数据倾斜，特别是在聚合和连接操作中。