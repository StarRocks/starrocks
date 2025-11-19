---
displayed_sidebar: docs
sidebar_position: 20
---

# 查询计划

优化查询性能是分析系统中的一个常见挑战。慢查询会影响用户体验和整个集群的性能。在StarRocks中，理解和解释查询计划和查询分析是诊断和改进慢查询的基础。这些工具可以帮助你：
- 识别瓶颈和昂贵的操作
- 发现次优的连接策略或缺失的索引
- 理解数据是如何被过滤、聚合和移动的
- 排查和优化资源使用

**查询计划** 是由StarRocks FE生成的详细路线图，描述了你的SQL语句将如何执行。它将查询分解为一系列操作，如扫描、连接、聚合和排序，并确定执行这些操作的最有效方式。

StarRocks提供了几种查看查询计划的方法：

1. **EXPLAIN 语句**:  
   使用 `EXPLAIN` 显示查询的逻辑或物理执行计划。你可以添加选项来控制输出：
   - `EXPLAIN LOGICAL <query>`: 显示简化的计划。
   - `EXPLAIN <query>`: 显示基本的物理计划。
   - `EXPLAIN VERBOSE <query>`: 显示带有详细信息的物理计划。
   - `EXPLAIN COSTS <query>`: 包含每个操作的估计成本，用于诊断统计问题。

2. **EXPLAIN ANALYZE**:  
   使用 `EXPLAIN ANALYZE <query>` 执行查询并显示实际执行计划以及真实的运行时统计信息。详情请参阅 [Explain Analyze](./query_profile_text_based_analysis.md) 文档。

   示例：
   ```sql
   EXPLAIN ANALYZE SELECT * FROM sales_orders WHERE amount > 1000;
   ```

3. **Query Profile**:  
   在运行查询后，你可以查看其详细的执行分析，包括时间、资源使用和操作级别的统计信息。请参阅 [Query Profile](./query_profile_overview.md) 文档，了解如何访问和解释这些信息。
   - **SQL命令**: `SHOW PROFILELIST` 和 `ANALYZE PROFILE FOR <query_id>`: 可用于检索特定查询的执行分析。
   - **FE HTTP 服务**: 通过StarRocks FE的Web UI访问查询分析，导航到 **Query** 或 **Profile** 部分，在那里你可以搜索和检查查询执行细节。
   - **托管版本**: 在云或托管部署中，使用提供的Web控制台或监控仪表板查看查询计划和分析，通常具有增强的可视化和过滤选项。

通常，查询计划用于诊断与查询计划和优化相关的问题，而查询分析有助于识别查询执行期间的性能问题。在接下来的部分中，我们将探讨查询执行的关键概念，并通过一个具体的示例来分析查询计划。

## 查询执行流程

在StarRocks中，查询的生命周期由三个主要阶段组成：
1. **规划**: 查询经过解析、分析和优化，最终生成查询计划。
2. **调度**: 调度器和协调器将计划分发给所有参与的后端节点。
3. **执行**: 使用管道执行引擎执行计划。

![SQL Execution Flow](../../_assets/Profile/execution_flow.png)

**计划结构**

StarRocks计划是分层的：
- **Fragment**: 顶层工作片段；每个Fragment在不同的后端节点上生成多个 **FragmentInstances**。
- **Pipeline**: 在一个实例中，管道将操作符串联在一起；多个 **PipelineDrivers** 在不同的CPU核心上并发运行相同的管道。
- **Operator**: 原子步骤——扫描、连接、聚合——实际处理数据。

![profile-3](../../_assets/Profile/profile-3.png)

**管道执行引擎**

管道引擎以并行和高效的方式执行查询计划，处理复杂的计划和大数据量，以实现高性能和可扩展性。

![pipeline_opeartors](../../_assets/Profile/pipeline_operators.png)

**指标合并策略**

默认情况下，StarRocks合并FragmentInstance和PipelineDriver层以减少分析体积，形成简化的三层结构：
- Fragment
- Pipeline
- Operator

你可以通过会话变量 `pipeline_profile_level` 控制这种合并行为。

## 示例

### 如何阅读查询计划和分析

1. **理解结构**: 查询计划分为多个片段，每个片段代表一个执行阶段。从下往上阅读：首先是扫描节点，然后是连接、聚合，最后是结果。

2. **整体分析**:
   - 检查总运行时间、内存使用和CPU/墙时间比率。
   - 通过按操作符时间排序找到慢操作符。
   - 确保过滤器尽可能下推。
   - 查找数据倾斜（不均匀的操作符时间或行数）。
   - 监控高内存或磁盘溢出；如有必要，调整连接顺序或使用汇总视图。
   - 根据需要使用物化视图和查询提示（`BROADCAST`、`SHUFFLE`、`COLOCATE`）进行优化。

2. **扫描操作**: 查找 `OlapScanNode` 或类似节点。注意扫描了哪些表，应用了哪些过滤器，以及是否使用了预聚合或物化视图。

3. **连接操作**: 确定连接类型（`HASH JOIN`、`BROADCAST`、`SHUFFLE`、`COLOCATE`、`BUCKET SHUFFLE`）。连接方法影响性能：
   - **Broadcast**: 小表发送到所有节点；适用于小表。
   - **Shuffle**: 行被分区和洗牌；适用于大表。
   - **Colocate**: 表按相同方式分区；支持本地连接。
   - **Bucket Shuffle**: 仅一个表被洗牌以减少网络成本。

4. **聚合和排序**: 查找 `AGGREGATE`、`TOP-N` 或 `ORDER BY`。这些操作在数据量大或高基数时可能开销较大。

5. **数据移动**: `EXCHANGE` 节点显示片段或节点之间的数据传输。过多的数据移动会影响性能。

6. **谓词下推**: 早期应用的过滤器（在扫描时）减少下游数据。检查 `PREDICATES` 或 `PushdownPredicates` 以查看哪些过滤器被下推。

### 示例查询计划

:::tip
这是 TPC-DS 基准测试中的第 96 个查询。
:::

```sql
explain logical
select  count(*)
from store_sales
    ,household_demographics
    ,time_dim
    , store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
order by count(*) limit 100;
```

输出是一个分层计划，显示StarRocks将如何执行查询。计划结构为操作符树，从下往上阅读。逻辑计划显示了带有成本估算的操作序列：

```
- Output => [69:count]
    - TOP-100(FINAL)[69: count ASC NULLS FIRST]
            Estimates: {row: 1, cpu: 8.00, memory: 8.00, network: 8.00, cost: 68669801.20}
        - TOP-100(PARTIAL)[69: count ASC NULLS FIRST]
                Estimates: {row: 1, cpu: 8.00, memory: 8.00, network: 8.00, cost: 68669769.20}
            - AGGREGATE(GLOBAL) []
                    Estimates: {row: 1, cpu: 8.00, memory: 8.00, network: 0.00, cost: 68669737.20}
                    69:count := count(69:count)
                - EXCHANGE(GATHER)
                        Estimates: {row: 1, cpu: 8.00, memory: 0.00, network: 8.00, cost: 68669717.20}
                    - AGGREGATE(LOCAL) []
                            Estimates: {row: 1, cpu: 3141.35, memory: 0.80, network: 0.00, cost: 68669701.20}
                            69:count := count()
                        - HASH/INNER JOIN [9:ss_store_sk = 40:s_store_sk] => [71:auto_fill_col]
                                Estimates: {row: 3490, cpu: 111184.52, memory: 8.80, network: 0.00, cost: 68668128.93}
                                71:auto_fill_col := 1
                            - HASH/INNER JOIN [7:ss_hdemo_sk = 25:hd_demo_sk] => [9:ss_store_sk]
                                    Estimates: {row: 19940, cpu: 1841177.20, memory: 2880.00, network: 0.00, cost: 68612474.92}
                                - HASH/INNER JOIN [4:ss_sold_time_sk = 30:t_time_sk] => [7:ss_hdemo_sk, 9:ss_store_sk]
                                        Estimates: {row: 199876, cpu: 69221191.15, memory: 7077.97, network: 0.00, cost: 67671726.32}
                                    - SCAN [store_sales] => [4:ss_sold_time_sk, 7:ss_hdemo_sk, 9:ss_store_sk]
                                            Estimates: {row: 5501341, cpu: 66016092.00, memory: 0.00, network: 0.00, cost: 33008046.00}
                                            partitionRatio: 1/1, tabletRatio: 192/192
                                            predicate: 7:ss_hdemo_sk IS NOT NULL
                                    - EXCHANGE(BROADCAST)
                                            Estimates: {row: 1769, cpu: 7077.97, memory: 7077.97, network: 7077.97, cost: 38928.81}
                                        - SCAN [time_dim] => [30:t_time_sk]
                                                Estimates: {row: 1769, cpu: 21233.90, memory: 0.00, network: 0.00, cost: 10616.95}
                                                partitionRatio: 1/1, tabletRatio: 5/5
                                                predicate: 33:t_hour = 8 AND 34:t_minute >= 30
                                - EXCHANGE(BROADCAST)
                                        Estimates: {row: 720, cpu: 2880.00, memory: 2880.00, network: 2880.00, cost: 14400.00}
                                    - SCAN [household_demographics] => [25:hd_demo_sk]
                                            Estimates: {row: 720, cpu: 5760.00, memory: 0.00, network: 0.00, cost: 2880.00}
                                            partitionRatio: 1/1, tabletRatio: 1/1
                                            predicate: 28:hd_dep_count = 5
                            - EXCHANGE(BROADCAST)
                                    Estimates: {row: 2, cpu: 8.80, memory: 8.80, network: 8.80, cost: 44.15}
                                - SCAN [store] => [40:s_store_sk]
                                        Estimates: {row: 2, cpu: 17.90, memory: 0.00, network: 0.00, cost: 8.95}
                                        partitionRatio: 1/1, tabletRatio: 1/1
                                        predicate: 45:s_store_name = 'ese'
```

**自下而上阅读计划**

查询计划应该从底部（叶节点）向上到顶部（根节点）阅读，遵循数据流：

1. **扫描操作（底层）**: 底部的 `SCAN` 操作符从基表读取数据：
   - `SCAN [store_sales]` 读取主事实表，带有谓词 `ss_hdemo_sk IS NOT NULL`
   - `SCAN [time_dim]` 读取时间维度表，带有谓词 `t_hour = 8 AND t_minute >= 30`
   - `SCAN [household_demographics]` 读取人口统计表，带有谓词 `hd_dep_count = 5`
   - `SCAN [store]` 读取商店表，带有谓词 `s_store_name = 'ese'`

   每个扫描操作显示：
   - **估算值**: 行数、CPU、内存、网络和成本估算
   - **分区和tablet比率**: 扫描了多少分区/tablet（例如，`partitionRatio: 1/1, tabletRatio: 192/192`）
   - **谓词**: 下推到扫描级别的查询条件，减少读取的数据量

2. **数据交换（广播）**: `EXCHANGE(BROADCAST)` 操作将较小的维度表分发到处理较大事实表的所有节点。当维度表相对于事实表较小时，这种方法很高效，如 `time_dim`、`household_demographics` 和 `store` 被广播的情况。

3. **连接操作（中层）**: 数据通过 `HASH/INNER JOIN` 操作向上流动：
   - 首先，`store_sales` 与 `time_dim` 在 `ss_sold_time_sk = t_time_sk` 上连接
   - 然后，结果与 `household_demographics` 在 `ss_hdemo_sk = hd_demo_sk` 上连接
   - 最后，结果与 `store` 在 `ss_store_sk = s_store_sk` 上连接

   每个连接显示连接条件和结果行数及资源使用的估算值。

4. **聚合（上层）**: 
   - `AGGREGATE(LOCAL)` 在每个节点上执行本地聚合，计算 `count()`
   - `EXCHANGE(GATHER)` 从所有节点收集结果
   - `AGGREGATE(GLOBAL)` 将本地结果合并为最终计数

5. **最终操作（顶层）**: 
   - `TOP-100(PARTIAL)` 和 `TOP-100(FINAL)` 操作处理 `ORDER BY count(*) LIMIT 100` 子句，在排序后选择前100个结果

逻辑计划为每个操作提供成本估算，帮助您了解查询在哪些地方消耗了最多的资源。实际的物理执行计划（来自 `EXPLAIN` 或 `EXPLAIN VERBOSE`）包含有关操作如何在节点间分布和并行执行的更多详细信息。
