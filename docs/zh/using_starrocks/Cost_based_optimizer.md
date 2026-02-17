---
displayed_sidebar: docs
sidebar_position: 10
---

# 收集 CBO 统计信息

本主题介绍 StarRocks 基于成本的优化器 (CBO) 的基本概念，以及如何收集统计信息，以便 CBO 选择最优查询计划。StarRocks 2.4 引入直方图以收集准确的数据分布统计信息。

自 v3.2.0 起，StarRocks 支持收集 Hive、Iceberg 和 Hudi 表的统计信息，减少对其他元数据存储系统的依赖。其语法与收集 StarRocks 内部表的统计信息类似。

## 什么是 CBO

CBO 对查询优化至关重要。当 SQL 查询到达 StarRocks 后，会被解析成逻辑执行计划。CBO 会将逻辑计划重写和转换为多个物理执行计划。然后，CBO 估算计划中每个操作符的执行成本（例如 CPU、内存、网络和 I/O），并选择成本最低的查询路径作为最终的物理计划。

StarRocks CBO 于 StarRocks 1.16.0 版本推出，并从 1.19 版本开始默认启用。StarRocks CBO 基于 Cascades 框架开发，根据各种统计信息估算成本。它能够从数万个执行计划中选择成本最低的执行计划，显著提高复杂查询的效率和性能。

统计信息对 CBO 至关重要。它们决定了成本估算是否准确有用。以下章节将详细介绍统计信息的类型、收集策略以及如何收集统计信息和查看统计信息。

## 统计信息类型

StarRocks 收集多种统计信息作为成本估算的输入。

### 基本统计信息

默认情况下，StarRocks 定期收集表和列的以下基本统计信息：

- row_count: 表中的总行数

- data_size: 列的数据大小

- ndv: 列的基数，即列中不同值的数量

- null_count: 列中 NULL 值的数量

- min: 列中的最小值

- max: 列中的最大值

完整统计信息存储在 `_statistics_` 数据库的 `column_statistics` 表中。您可以查看此表以查询表统计信息。以下是查询此表中的统计数据示例。

```sql
SELECT * FROM _statistics_.column_statistics\G
*************************** 1. row ***************************
      table_id: 10174
  partition_id: 10170
   column_name: is_minor
         db_id: 10169
    table_name: zj_test.source_wiki_edit
partition_name: p06
     row_count: 2
     data_size: 2
           ndv: NULL
    null_count: 0
           max: 1
           min: 0
```

### 直方图

StarRocks 2.4 引入直方图以补充基本统计信息。直方图被认为是有效的数据表示方式。对于数据倾斜的表，直方图可以准确反映数据分布。

StarRocks 使用等高直方图，它构建在多个桶上。每个桶包含等量的数据。对于频繁查询且对选择性有重大影响的数据值，StarRocks 会为它们分配单独的桶。更多的桶意味着更准确的估算，但也可能导致内存使用量略微增加。您可以调整直方图收集任务的桶数和最常见值 (MCV)。

**直方图适用于数据高度倾斜且查询频繁的列。如果您的表数据均匀分布，则无需创建直方图。直方图只能在数值、DATE、DATETIME 或字符串类型的列上创建。**

直方图存储在 `_statistics_` 数据库的 `histogram_statistics` 表中。以下是查询此表中的统计数据示例：

```sql
SELECT * FROM _statistics_.histogram_statistics\G
*************************** 1. row ***************************
   table_id: 10174
column_name: added
      db_id: 10169
 table_name: zj_test.source_wiki_edit
    buckets: NULL
        mcv: [["23","1"],["5","1"]]
update_time: 2023-12-01 15:17:10.274000
```

### 多列联合统计信息

自 v3.5.0 起，StarRocks 支持多列联合统计信息收集。当前，当 StarRocks 执行基数估算时，在大多数场景下，优化器假设多列之间完全相互独立，即它们之间没有关联。然而，如果列之间存在关联，当前的估算方法可能会导致不正确的结果。这会导致优化器生成错误的执行计划。目前，仅支持多列联合 NDV，主要用于以下场景的基数估算。

- 评估多个 AND 连接的等价谓词。
- 评估 Agg 节点。
- 应用于聚合下推策略。

目前，多列联合统计信息收集仅支持手动收集。默认类型为采样收集。多列统计信息存储在每个 StarRocks 集群的 `_statistics_` 数据库的 `multi_column_statistics` 表中。查询将返回类似以下的信息：

```sql
mysql> select * from _statistics_.multi_column_statistics \G
*************************** 1. row ***************************
    table_id: 1695021
  column_ids: 0#1
       db_id: 110099
  table_name: db.test_multi_col_stats
column_names: id,name
         ndv: 11
 update_time: 2025-04-11 15:09:50
```

### 虚拟列统计信息
自 StarRocks 4.0 起，支持虚拟列统计信息。虚拟统计信息是可以对表达式而非仅对物理列进行计算的统计信息。

目前，唯一实现的虚拟统计信息是 `unnest`（针对数组）。虚拟统计信息的计算默认禁用，因为使用它们取决于查询模式。
分析表时，您可以通过传递属性来启用 `unnest` 虚拟统计信息的计算，方法是：
```sql
ANALYZE FULL TABLE `TABLE1` PROPERTIES("unnest_virtual_statistics" = "true");
```
请注意，支持采样统计信息和完整统计信息。

例如，当启用时，将计算使用表达式 `unnest(col1)` 的数组列 `col1` 的统计信息。
当会话变量 `enable_unnest_virtual_statistics` 启用时，优化器将在查询中遇到 `unnest(col1)` 表达式时使用并传播计算出的虚拟统计信息。
如果您经常对数组列进行 unnest 操作并对其发出查询（例如 join），这将有所帮助。优化器可以利用统计信息来例如解释倾斜的 join。

## 收集类型和方法

表中的数据大小和数据分布不断变化。统计信息必须定期更新以反映数据的变化。在创建统计信息收集任务之前，您必须选择最适合您业务需求的收集类型和方法。

StarRocks 支持完整收集和采样收集，两者均可自动和手动执行。默认情况下，StarRocks 会自动收集表的完整统计信息。它每 5 分钟检查一次数据更新。如果检测到数据更改，将自动触发数据收集。如果您不想使用自动完整收集，可以自定义收集任务。

| **收集类型** | **收集方法** | **描述**                                              | **优缺点**                               |
| ------------------- | --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 完整收集     | 自动/手动      | 扫描整个表以收集统计信息。按分区收集统计信息。如果分区没有数据更改，则不会从该分区收集数据，从而减少资源消耗。完整统计信息存储在 `_statistics_.column_statistics` 表中。 | 优点：统计信息准确，有助于 CBO 进行准确估算。缺点：消耗系统资源且速度慢。从 2.5 版本开始，StarRocks 允许您指定自动收集周期，从而减少资源消耗。 |
| 采样收集  | 自动/手动      | 从表的每个分区均匀提取 `N` 行数据。按表收集统计信息。每列的基本统计信息作为一条记录存储。列的基数信息 (ndv) 是根据采样数据估算的，因此不准确。采样统计信息存储在 `_statistics_.table_statistic_v1` 表中。 | 优点：消耗较少的系统资源且速度快。缺点：统计信息不完整，可能会影响成本估算的准确性。 |

自 v3.5.0 起，采样和完整收集的统计信息都存储在 `_statistics_.column_statistics` 表中。这是因为当前的优化器在基数估算时会关注最新收集的统计信息，而每次后台自动收集统计信息时，可能会因表的不同健康状况而采用不同的收集方法。如果存在数据倾斜，全表的采样统计信息的错误率可能会更高，并且同一个查询可能会因不同的收集方法而使用不同的统计信息，这可能会导致优化器生成不正确的执行计划。因此，采样收集和完整收集都假设统计信息是在分区级别收集的。您可以通过将 FE 配置项 `statistic_use_meta_statistics` 修改为 `false` 来调整回以前的收集和存储方法。

## 谓词列

自 v3.5.0 起，StarRocks 支持收集谓词列统计信息。

谓词列是指在查询中经常用作过滤条件（WHERE 子句、JOIN 条件、GROUP BY 列、DISTINCT 列）的列。StarRocks 自动记录查询中涉及的每个谓词列，并将其存储在 `_statistics_. predicate_columns` 表中。查询将返回以下信息：

```sql
select * from _statistics_.predicate_columns \G
*************************** 1. row ***************************
      fe_id: 127.0.0.1_9011_1735874871718
      db_id: 1684773
   table_id: 1685786
  column_id: 1
      usage: normal,predicate,join,group_by
  last_used: 2025-04-11 20:39:32
    created: 2025-04-11 20:39:53
```

您还可以查询 `information_schema.column_stats_usage` 视图以获取更多可观测信息。在极端场景下（列数过多），完整收集的开销很大，而在实际中，对于稳定的工作负载，您通常不需要所有列的统计信息，而只需要涉及一些关键 Filter、Join 和 Aggregation 的列的统计信息。因此，为了权衡成本和准确性，StarRocks 支持手动收集谓词列统计信息，并在自动收集统计信息时根据策略收集谓词列统计信息，避免收集表中所有列的统计信息。集群中的每个 FE 节点会定期将谓词列信息同步更新到 FE 缓存中，以加速使用。

## 收集统计信息

StarRocks 提供灵活的统计信息收集方法。您可以选择自动、手动或自定义收集，以适应您的业务场景。

默认情况下，StarRocks 会定期自动收集表的完整统计信息。默认间隔为 10 分钟。如果系统发现数据更新比例符合条件，将自动触发收集。**收集完整统计信息可能会消耗大量系统资源**。如果您不想使用自动完整统计信息收集，可以将 FE 配置项 `enable_collect_full_statistic` 设置为 `false`，定期收集任务将从完整统计信息收集更改为采样收集。

自 v3.5.0 起，如果在自动收集过程中发现自上次收集到当前表以来数据发生较大变化，StarRocks 会自动将完整收集切换为采样收集。在使用采样收集的情况下，如果表中存在谓词列（PREDICATE/JOIN/GROUP BY/DISTINCT），StarRocks 会将采样任务转换为完整收集，并收集谓词列的统计信息以确保统计信息的准确性，而不会收集表中所有列的统计信息。您可以通过 FE 配置项 `statistic_auto_collect_use_full_predicate_column_for_sample` 进行配置。此外，如果完整收集时表的列数超过 FE 配置项 `statistic_auto_collect_predicate_columns_threshold`，StarRocks 还会将所有列的完整收集切换为谓词列的完整收集。

### 自动收集

对于基本统计信息，StarRocks 默认自动收集表的完整统计信息，无需手动操作。对于尚未收集统计信息的表，StarRocks 会在调度周期内自动收集统计信息。对于已收集统计信息的表，StarRocks 会更新表中的总行数和修改行数，并定期持久化此信息，以判断是否触发自动收集。直方图和多列联合统计信息不被当前的自动收集任务捕获。

从 2.4.5 版本开始，StarRocks 允许您为自动完整收集指定收集周期，这可以防止因自动完整收集引起的集群性能抖动。该周期由 FE 参数 `statistic_auto_analyze_start_time` 和 `statistic_auto_analyze_end_time` 指定。

将触发自动收集的条件：

- 自上次统计信息收集以来，表数据已更改。

- 收集时间落在配置的收集周期范围内。（默认收集周期为全天。）

- 上一个收集作业的更新时间早于分区的最新更新时间。

- 表统计信息的健康度低于指定阈值 (`statistic_auto_collect_ratio`)。

> 统计信息健康度计算公式：
>
> 1. 如果数据已更新的分区数小于 10，公式为 `1 - (自上次收集以来更新的行数/总行数)`。
> 2. 如果数据已更新的分区数大于或等于 10，公式为 `1 - MIN(自上次收集以来更新的行数/总行数, 自上次收集以来更新的分区数/总分区数)`。
> 3. 自 v3.5.0 起，为了判断分区是否健康，StarRocks 不再比较统计信息收集时间与数据更新时间，而是比较分区更新行数比例。您可以通过 FE 配置项 `statistic_partition_health_v2_threshold` 进行配置。此外，您可以将 FE 配置项 `statistic_partition_healthy_v2` 设置为 `false`，以使用之前的健康检查行为。

此外，StarRocks 允许您根据表大小和表更新频率配置收集策略：

- 对于数据量小的表，**即使表数据频繁更新，也会实时收集统计信息，不受限制**。`statistic_auto_collect_small_table_size` 参数可用于确定表是小表还是大表。您还可以使用 `statistic_auto_collect_small_table_interval` 配置小表的收集间隔。

- 对于数据量大的表，适用以下限制：

  - 默认收集间隔不小于 12 小时，可以使用 `statistic_auto_collect_large_table_interval` 进行配置。

  - 当满足收集间隔且统计信息健康度低于自动采样收集阈值时，触发采样收集。您可以使用 FE 配置项 `statistic_auto_collect_sample_threshold` 配置此行为。自 v3.5.0 起，如果满足以下所有条件，采样收集将转换为谓词列的完整收集：
    - 表中存在谓词列，并且谓词列的数量小于 `statistic_auto_collect_max_predicate_column_size_on_sample_strategy`。
    - FE 配置 `statistic_auto_collect_use_full_predicate_column_for_sample` 设置为 `true`。

  - 当满足收集间隔且统计信息健康度高于自动采样收集阈值 (`statistic_auto_collect_sample_threshold`) 并低于自动收集阈值 (`statistic_auto_collect_ratio`) 时，触发完整收集。

  - 当要收集数据的分区大小 (`statistic_max_full_collect_data_size`) 大于 100 GB 时，触发采样收集。

  - 仅收集更新时间晚于上次收集任务时间的 phân 区的统计信息。未更改数据的分区的统计信息不予收集。

- 对于具有谓词列且总列数超过 `statistic_auto_collect_predicate_columns_threshold` 的表，仅收集表中谓词列的统计信息。

:::tip

表数据发生变化后，手动触发该表的采样收集任务，会使采样收集任务的更新时间晚于数据更新时间，从而不会在该调度周期内触发该表的自动完整收集。
:::

自动完整收集默认启用，并由系统使用默认设置运行。

下表描述了默认设置。如果需要修改，请运行 **ADMIN SET CONFIG** 命令。

| **FE 配置项**         | **类型** | **默认值** | **描述**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| enable_statistic_collect              | BOOLEAN  | TRUE              | 是否开启默认和用户定义的自动收集任务。该开关默认开启。 |
| enable_collect_full_statistic         | BOOLEAN  | TRUE              | 是否开启默认自动收集。该开关默认开启。 |
| statistic_collect_interval_sec        | LONG     | 600               | 自动收集过程中检查数据更新的间隔。单位：秒。 |
| statistic_auto_analyze_start_time | STRING      | 00:00:00   | 自动收集的开始时间。取值范围：`00:00:00` - `23:59:59`。 |
| statistic_auto_analyze_end_time | STRING      | 23:59:59  | 自动收集的结束时间。取值范围：`00:00:00` - `23:59:59`。 |
| statistic_auto_collect_small_table_size     | LONG    | 5368709120   | 确定表是否为自动完整收集的小表的阈值。大于此值的表被视为大表，小于或等于此值的表被视为小表。单位：字节。默认值：5368709120 (5 GB)。                         |
| statistic_auto_collect_small_table_interval | LONG    | 0         | 自动收集小表完整统计信息的间隔。单位：秒。                              |
| statistic_auto_collect_large_table_interval | LONG    | 43200        | 自动收集大表完整统计信息的间隔。单位：秒。默认值：43200 (12 小时)。                               |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | 确定自动收集统计信息是否健康的阈值。如果统计信息健康度低于此阈值，则触发自动收集。 |
| statistic_auto_collect_sample_threshold  | DOUBLE | 0.3   | 触发自动采样收集的统计信息健康度阈值。如果统计信息健康度低于此阈值，则触发自动采样收集。 |
| statistic_max_full_collect_data_size | LONG      | 107374182400      | 自动收集任务收集数据的分区数据大小。单位：字节。默认值：107374182400 (100 GB)。如果数据大小超过此值，则放弃完整收集并执行采样收集。 |
| statistic_full_collect_buffer | LONG | 20971520 | 自动收集任务占用的最大缓冲区大小。单位：字节。默认值：20971520 (20 MB)。 |
| statistic_collect_max_row_count_per_query | INT  | 5000000000        | 单个 analyze 任务查询的最大行数。如果超过此值，analyze 任务将被拆分为多个查询。 |
| statistic_collect_too_many_version_sleep | LONG | 600000 | 如果运行收集任务的表数据版本过多，自动收集任务的睡眠时间。单位：毫秒。默认值：600000 (10 分钟)。  |
| statistic_auto_collect_use_full_predicate_column_for_sample | BOOLEAN    | TRUE       | 当自动完整收集任务符合采样收集策略时，是否将其转换为谓词列的完整收集。 |
| statistic_auto_collect_max_predicate_column_size_on_sample_strategy | INT    | 16       | 当自动完整收集任务符合采样收集策略时，如果表的谓词列数量异常多且超过此配置项，任务将不会切换为谓词列的完整收集，而是保持为所有列的采样收集。此配置项控制此行为的谓词列最大值。 |
| statistic_auto_collect_predicate_columns_threshold | INT     | 32       | 如果自动收集过程中表的列数超过此配置，则只收集表中谓词列的统计信息。 |
| statistic_predicate_columns_persist_interval_sec   | LONG    | 60       | FE 同步并持久化谓词列统计信息的间隔。 |
| statistic_predicate_columns_ttl_hours       | LONG    | 24       | FE 中缓存的谓词列统计信息的淘汰时间。 |
| enable_predicate_columns_collection         | BOOLEAN | TRUE     | 是否启用谓词列收集。如果禁用，谓词列在查询优化期间将不会被记录。 |
| enable_manual_collect_array_ndv             | BOOLEAN | FALSE        | 是否启用手动收集 ARRAY 类型的 NDV 信息。 |
| enable_auto_collect_array_ndv               | BOOLEAN | FALSE        | 是否启用自动收集 ARRAY 类型的 NDV 信息。 |

您可以依靠自动作业完成大部分统计信息收集，但如果您有特定需求，可以通过执行 ANALYZE TABLE 语句手动创建任务，或者通过执行 CREATE ANALYZE 语句自定义自动任务。

### 手动收集

您可以使用 ANALYZE TABLE 创建手动收集任务。默认情况下，手动收集是同步操作。您也可以将其设置为异步操作。在异步模式下，运行 ANALYZE TABLE 后，系统会立即返回该语句是否成功。然而，收集任务会在后台运行，您无需等待结果。您可以通过运行 SHOW ANALYZE STATUS 查看任务状态。异步收集适用于数据量大的表，而同步收集适用于数据量小的表。**手动收集任务创建后只运行一次。您无需删除手动收集任务。** 您必须对相应表具有 INSERT 和 SELECT 权限才能执行 ANALYZE TABLE 操作。

#### 手动收集基本统计信息

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name 
    [( col_name [, col_name]... )
    | col_name [, col_name]...
    | ALL COLUMNS
    | PREDICATE COLUMNS
    | MULTIPLE COLUMNS ( col_name [, col_name]... )]
[PARTITION (partition_name [, partition_name]...)]
[WITH [SYNC | ASYNC] MODE]
[PROPERTIES (property [, property]...)]
```

参数说明：

- 收集类型
  - FULL：表示完整收集。
  - SAMPLE：表示采样收集。
  - 如果未指定收集类型，则默认使用完整收集。

- 要收集统计信息的列类型：
  - `col_name`：要收集统计信息的列。多个列之间用逗号 (`,`) 分隔。如果未指定此参数，则收集整个表的统计信息。
  - `ALL COLUMNS`：收集所有列的统计信息。自 v3.5.0 起支持。
  - `PREDICATE COLUMNS`：仅收集谓词列的统计信息。自 v3.5.0 起支持。
  - `MULTIPLE COLUMNS`：从指定的多个列收集联合统计信息。目前，仅支持多列的手动同步收集。手动统计信息收集的列数不能超过 `statistics_max_multi_column_combined_num`，默认值为 `10`。自 v3.5.0 起支持。

- `[WITH SYNC | ASYNC MODE]`：手动收集任务是以同步还是异步模式运行。如果未指定此参数，则默认使用同步收集。

- `PROPERTIES`：自定义参数。如果未指定 `PROPERTIES`，则使用 `fe.conf` 文件中的默认设置。实际使用的属性可以通过 SHOW ANALYZE STATUS 输出中的 `Properties` 列查看。

| **PROPERTIES**                | **类型** | **默认值** | **描述**                                              |
| ----------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000            | 采样收集的最小行数。如果参数值超过表中实际行数，则执行完整收集。 |
| unnest_virtual_statistics     | BOOL     | false             | 是否为适用的列计算虚拟 unnest 统计信息。 |

示例

手动完整收集

```SQL
-- Manually collect full stats of a table using default settings.
ANALYZE TABLE tbl_name;

-- Manually collect full stats of a table using default settings.
ANALYZE FULL TABLE tbl_name;

-- Manually collect stats of specified columns in a table using default settings.
ANALYZE TABLE tbl_name(c1, c2, c3);
```

手动采样收集

```SQL
-- Manually collect partial stats of a table using default settings.
ANALYZE SAMPLE TABLE tbl_name;

-- Manually collect stats of specified columns in a table, with the number of rows to collect specified.
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

- 手动收集多列联合统计信息

```sql
-- Manual sampled collection of multi-column joint statistics
ANALYZE SAMPLE TABLE tbl_name MULTIPLE COLUMNS (v1, v2);

-- Manual full collection of multi-column joint statistics
ANALYZE FULL TABLE tbl_name MULTIPLE COLUMNS (v1, v2);
```

- 手动收集谓词列

```sql
-- Manual sampled collection of Predicate Column
ANALYZE SAMPLE TABLE tbl_name PREDICATE COLUMNS

-- Manual full collection of Predicate Column
ANALYZE FULL TABLE tbl_name PREDICATE COLUMNS
```

#### 手动收集直方图

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

参数说明：

- `col_name`：要收集统计信息的列。多个列之间用逗号 (`,`) 分隔。如果未指定此参数，则收集整个表的统计信息。直方图必须指定此参数。

- [WITH SYNC | ASYNC MODE]：手动收集任务是以同步还是异步模式运行。如果未指定此参数，则默认使用同步收集。

- `WITH N BUCKETS`：`N` 是直方图收集的桶数。如果未指定，则使用 `fe.conf` 中的默认值。

- PROPERTIES：自定义参数。如果未指定 `PROPERTIES`，则使用 `fe.conf` 中的默认设置。

| **PROPERTIES**                    | **类型** | **默认值** | **描述**                                              |
| --------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows     | INT      | 200000            | 收集的最小行数。如果参数值超过表中实际行数，则执行完整收集。 |
| histogram_buckets_size            | LONG     | 64                | 直方图的默认桶数。                   |
| histogram_mcv_size                | INT      | 100               | 直方图的最常见值 (MCV) 数量。      |
| histogram_sample_ratio            | FLOAT    | 0.1               | 直方图的采样比例。                          |
| histogram_max_sample_row_count    | LONG     | 10000000          | 直方图收集的最大行数。       |
| histogram_collect_bucket_ndv_mode | STRING   | none              | 估计每个直方图桶中不同值数量 (NDV) 的模式。`none`（默认，不收集不同计数）、`hll`（使用 HyperLogLog 进行准确估计）或 `sample`（使用低开销的基于采样的估计器）。 |
| unnest_virtual_statistics         | BOOL     | false             | 是否为适用的列计算虚拟 unnest 统计信息。 |

直方图收集的行数由多个参数控制。它是 `statistic_sample_collect_rows` 和表行数 * `histogram_sample_ratio` 之间较大的值。该行数不能超过 `histogram_max_sample_row_count` 指定的值。如果超过该值，则 `histogram_max_sample_row_count` 优先。

实际使用的属性可以通过 SHOW ANALYZE STATUS 输出中的 `Properties` 列查看。

示例

```SQL
-- Manually collect histograms on v1 using the default settings.
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- Manually collect histograms on v1 and v2, with 32 buckets, 32 MCVs, and 50% sampling ratio.
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);

-- Collect histograms on v3 using the 'hll' mode for accurate distinct count estimation per bucket.
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v3
PROPERTIES(
   "histogram_collect_bucket_ndv_mode" = "hll"
);
```

### 自定义收集

#### 自定义自动收集任务

StarRocks 提供的默认收集任务会根据策略自动收集所有数据库和所有表的统计信息，默认情况下，您无需创建自定义收集任务。

如果您想自定义自动收集任务，需要通过 CREATE ANALYZE 语句创建。创建收集任务需要对被收集的表具有 INSERT 和 SELECT 权限。

```SQL
-- Automatically collect stats of all databases.
CREATE ANALYZE [FULL|SAMPLE] ALL [PROPERTIES (property [,property])]

-- Automatically collect stats of all tables in a database.
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
[PROPERTIES (property [,property])]

-- Automatically collect stats of specified columns in a table.
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]

-- Automatically collect histograms of specified columns in a table.
CREATE ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

参数说明：

- 收集类型
  - FULL：表示完整收集。
  - SAMPLE：表示采样收集。
  - 如果未指定收集类型，则默认使用采样收集。

- `col_name`：要收集统计信息的列。多个列之间用逗号 (`,`) 分隔。如果未指定此参数，则收集整个表的统计信息。

- `PROPERTIES`：自定义参数。如果未指定 `PROPERTIES`，则使用 `fe.conf` 中的默认设置。

| **PROPERTIES**                        | **类型** | **默认值** | **描述**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | 确定自动收集统计信息是否健康的阈值。如果统计信息健康度低于此阈值，则触发自动收集。 |
| statistic_sample_collect_rows         | INT      | 200000            | 收集的最小行数。如果参数值超过表中实际行数，则执行完整收集。 |
| statistic_exclude_pattern             | String   | null              | 作业中需要排除的数据库或表的名称。您可以在作业中指定不收集统计信息的数据库和表。请注意，这是一个正则表达式模式，匹配内容为 `database.table`。 |
| statistic_auto_collect_interval       | LONG   |  0      | 自动收集的间隔。单位：秒。默认情况下，StarRocks 根据表大小选择 `statistic_auto_collect_small_table_interval` 或 `statistic_auto_collect_large_table_interval` 作为收集间隔。如果您在创建 analyze job 时指定了 `statistic_auto_collect_interval` 属性，则此设置优先于 `statistic_auto_collect_small_table_interval` 和 `statistic_auto_collect_large_table_interval`。 |

示例

自动完整收集

```SQL
-- Automatically collect full stats of all databases.
CREATE ANALYZE ALL;

-- Automatically collect full stats of a database.
CREATE ANALYZE DATABASE db_name;

-- Automatically collect full stats of all tables in a database.
CREATE ANALYZE FULL DATABASE db_name;

-- Automatically collect full stats of specified columns in a table.
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 

-- Automatically collect stats of all databases, excluding specified database 'db_name'.
CREATE ANALYZE ALL PROPERTIES (
   "statistic_exclude_pattern" = "db_name\."
);

-- Automatically collect histograms of specified database, table, or columns.
CREATE ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON c1,c2;
```

自动采样收集

```SQL
-- Automatically collect stats of all tables in a database with default settings.
CREATE ANALYZE SAMPLE DATABASE db_name;

-- Automatically collect stats of all tables in a database, excluding specified table 'db_name.tbl_name'.
CREATE ANALYZE SAMPLE DATABASE db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);

-- Automatically collect stats of specified columns in a table, with statistics health and the number of rows to collect specified.
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES (
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);

```

**不使用 StarRocks 提供的自动收集任务，而是使用一个不收集 `db_name.tbl_name` 表的用户自定义收集任务。**

```sql
ADMIN SET FRONTEND CONFIG("enable_auto_collect_statistics"="false");
DROP ALL ANALYZE JOB;
CREATE ANALYZE FULL ALL db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name.tbl_name"
);
```

#### 查看自定义收集任务

```SQL
SHOW ANALYZE JOB [WHERE predicate][ORDER BY columns][LIMIT num]
```

您可以使用 WHERE 子句过滤结果。该语句返回以下列。

| **列**   | **描述**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | 收集任务的 ID。                               |
| Database     | 数据库名称。                                           |
| Table        | 表名称。                                              |
| Columns      | 列名称。                                            |
| Type         | 统计信息类型，包括 `FULL` 和 `SAMPLE`。       |
| Schedule     | 调度类型。自动任务的类型为 `SCHEDULE`。 |
| Properties   | 自定义参数。                                           |
| Status       | 任务状态，包括 PENDING、RUNNING、SUCCESS 和 FAILED。 |
| LastWorkTime | 上次收集的时间。                             |
| Reason       | 任务失败的原因。如果任务执行成功，则返回 NULL。 |

示例

```SQL
-- View all the custom collection tasks.
SHOW ANALYZE JOB

-- View custom collection tasks of database `test`.
SHOW ANALYZE JOB where `database` = 'test';
```

#### 删除自定义收集任务

```SQL
DROP ANALYZE <ID>
| DROP ALL ANALYZE JOB
```

任务 ID 可以通过 SHOW ANALYZE JOB 语句获取。

示例

```SQL
DROP ANALYZE 266030;
```

```SQL
DROP ALL ANALYZE JOB;
```

## 查看收集任务状态

您可以通过运行 SHOW ANALYZE STATUS 语句查看所有当前任务的状态。此语句不能用于查看自定义收集任务的状态。要查看自定义收集任务的状态，请使用 SHOW ANALYZE JOB。

```SQL
SHOW ANALYZE STATUS [WHERE predicate];
```

您可以使用 `LIKE 或 WHERE` 过滤要返回的信息。

该语句返回以下列。

| **列表名** | **描述**                                              |
| ------------- | ------------------------------------------------------------ |
| Id            | 收集任务的 ID。                               |
| Database      | 数据库名称。                                           |
| Table         | 表名称。                                              |
| Columns       | 列名称。                                            |
| Type          | 统计信息类型，包括 FULL、SAMPLE 和 HISTOGRAM。 |
| Schedule      | 调度类型。`ONCE` 表示手动，`SCHEDULE` 表示自动。 |
| Status        | 任务状态。                                      |
| StartTime     | 任务开始执行的时间。                     |
| EndTime       | 任务执行结束的时间。                       |
| Properties    | 自定义参数。                                           |
| Reason        | 任务失败的原因。如果执行成功，则返回 NULL。 |

## 查看统计信息

### 查看基本统计信息元数据

```SQL
SHOW STATS META [WHERE predicate][ORDER BY columns][LIMIT num]
```

该语句返回以下列。

| **列** | **描述**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | 数据库名称。                                           |
| Table      | 表名称。                                              |
| Columns    | 列名称。                                            |
| Type       | 统计信息类型。`FULL` 表示完整收集，`SAMPLE` 表示采样收集。 |
| UpdateTime | 当前表的最新统计信息更新时间。     |
| Properties | 自定义参数。                                           |
| Healthy    | 统计信息的健康度。                       |
| ColumnStats  | 列 ANALYZE 类型。                                       |
| TabletStatsReportTime | FE 中表 Tablet 元数据更新的时间。 |
| TableHealthyMetrics    | 统计信息中的健康指标。     |
| TableUpdateTime    | 表更新的时间。                 |

### 查看直方图元数据

```SQL
SHOW HISTOGRAM META [WHERE predicate]
```

该语句返回以下列。

| **列** | **描述**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | 数据库名称。                                           |
| Table      | 表名称。                                              |
| Column     | 列。                                                 |
| Type       | 统计信息类型。直方图的值为 `HISTOGRAM`。 |
| UpdateTime | 当前表的最新统计信息更新时间。     |
| Properties | 自定义参数。                                           |

## 删除统计信息

您可以删除不需要的统计信息。删除统计信息时，统计信息的数据和元数据都会被删除，以及过期缓存中的统计信息。请注意，如果自动收集任务正在进行中，之前删除的统计信息可能会再次被收集。您可以使用 `SHOW ANALYZE STATUS` 查看收集任务的历史记录。

### 删除基本统计信息

以下语句删除存储在 `default_catalog._statistics_.column_statistics` 表中的统计信息，FE 缓存的相应表统计信息也将失效。自 v3.5.0 起，此语句还会删除此表的多列联合统计信息。

```SQL
DROP STATS tbl_name
```

以下语句删除存储在 `default_catalog._statistics_.multi_column_statistics` 表中的多列联合统计信息，FE 缓存的相应表中的多列联合统计信息也将失效。此语句不删除表的基本统计信息。

```SQL
DROP MULTIPLE COLUMNS STATS tbl_name
```

### 删除直方图

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name]
```

## 取消收集任务

您可以使用 KILL ANALYZE 语句取消一个**正在运行的**收集任务，包括手动任务和自定义任务。

```SQL
KILL ANALYZE <ID>
```

手动收集任务的任务 ID 可以从 SHOW ANALYZE STATUS 获取。自定义收集任务的任务 ID 可以从 SHOW ANALYZE SHOW ANALYZE JOB 获取。

## 其他 FE 配置项

| **FE 配置项**        | **类型** | **默认值** | **描述**                                              |
| ------------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_manager_sleep_time_sec     | LONG     | 60                | 元数据调度的间隔。单位：秒。系统根据此间隔执行以下操作：创建用于存储统计信息的表。删除已删除的统计信息。删除过期的统计信息。 |
| statistic_analyze_status_keep_second | LONG     | 259200            | 保留收集任务历史记录的时长。单位：秒。默认值：259200 (3 天)。 |

## 会话变量

`statistic_collect_parallel`：用于调整可在 BE 上运行的统计信息收集任务的并行度。默认值：1。您可以增加此值以加快收集任务。

## 收集外部表统计信息

自 v3.2.0 起，StarRocks 支持收集 Hive、Iceberg 和 Hudi 表的统计信息。其语法与收集 StarRocks 内部表的统计信息类似。**但是，仅支持手动完整收集、手动直方图收集（自 v3.2.7 起）和自动完整收集。不支持采样收集。** 自 v3.3.0 起，StarRocks 支持收集 Delta Lake 表的统计信息以及 STRUCT 中子字段的统计信息。自 v3.4.0 起，StarRocks 支持通过查询触发的 ANALYZE 任务进行自动统计信息收集。

收集的统计信息存储在 `default_catalog` 中 `_statistics_` 数据库的 `external_column_statistics` 表中。它们不存储在 Hive Metastore 中，不能被其他搜索引擎共享。您可以查询 `default_catalog._statistics_.external_column_statistics` 表中的数据，以验证是否已为 Hive/Iceberg/Hudi 表收集了统计信息。

以下是查询 `external_column_statistics` 中统计数据示例。

```sql
SELECT * FROM _statistics_.external_column_statistics\G
*************************** 1. row ***************************
    table_uuid: hive_catalog.tn_test.ex_hive_tbl.1673596430
partition_name: 
   column_name: k1
  catalog_name: hive_catalog
       db_name: tn_test
    table_name: ex_hive_tbl
     row_count: 3
     data_size: 12
           ndv: NULL
    null_count: 0
           max: 3
           min: 2
   update_time: 2023-12-01 14:57:27.137000
```

### 限制

为外部表收集统计信息时，适用以下限制：

- 您只能收集 Hive、Iceberg、Hudi 和 Delta Lake（自 v3.3.0 起）表的统计信息。
- 仅支持手动完整收集、手动直方图收集（自 v3.2.7 起）和自动完整收集。不支持采样收集。
- 系统要自动收集完整统计信息，您必须创建一个 Analyze job，这与收集 StarRocks 内部表统计信息不同，后者系统默认在后台执行。
- 对于自动收集任务：
  - 您只能收集特定表的统计信息。您不能收集数据库中所有表的统计信息或外部目录中所有数据库的统计信息。
  - StarRocks 可以检测 Hive 和 Iceberg 表中的数据是否已更新，如果已更新，则仅收集数据已更新的分区的统计信息。StarRocks 无法感知 Hudi 表中的数据是否已更新，只能执行定期完整收集。
- 对于查询触发的收集任务：
  - 目前，只有 Leader FE 节点可以触发 ANALYZE 任务。
  - 系统仅支持检查 Hive 和 Iceberg 表的分区更改，并且仅收集数据已更改的分区的统计信息。对于 Delta Lake/Hudi 表，系统收集整个表的统计信息。
  - 如果 Partition Transforms 应用于 Iceberg 表，则仅支持 `identity`、`year`、`month`、`day`、`hour` 类型 Transforms 的统计信息收集。
  - 不支持收集 Iceberg 表的 Partition Evolution 统计信息。

以下示例发生在 Hive 外部目录下的数据库中。如果您想从 `default_catalog` 收集 Hive 表的统计信息，请以 `[catalog_name.][database_name.]<table_name>` 格式引用表。

### 查询触发收集

自 v3.4.0 起，系统支持通过查询触发的 ANALYZE 任务自动收集外部表的统计信息。当查询 Hive、Iceberg、Hudi 或 Delta Lake 表时，系统将在后台自动触发 ANALYZE 任务，收集相应表和列的统计信息，并将其用于后续的查询计划优化。

工作流程：

1. 当优化器查询 FE 中缓存的统计信息时，它将根据查询的表和列确定 ANALYZE 任务的对象（ANALYZE 任务将仅收集查询中包含的列的统计信息）。
2. 系统会将任务对象封装为 ANALYZE 任务并将其添加到 PendingTaskQueue。
3. Scheduler 线程会定期从 PendingTaskQueue 中获取任务并将其添加到 RunningTasksQueue。
4. 在 ANALYZE 任务执行期间，它会收集统计信息并将其写入 BE，同时清除 FE 中缓存的过期统计信息。

此功能默认启用。您可以通过以下系统变量和配置项控制上述过程。

#### 系统变量

##### enable_query_trigger_analyze

- 默认值: true
- 类型: Boolean
- 描述: 是否启用查询触发的 ANALYZE 任务。
- 引入版本: v3.4.0

#### FE 配置

##### connector_table_query_trigger_analyze_small_table_rows

- 默认值: 10000000
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: 查询触发的 ANALYZE 任务确定表是否为小表的阈值。
- 引入版本: v3.4.0

##### connector_table_query_trigger_analyze_small_table_interval

- 默认值: 2 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 小表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

##### connector_table_query_trigger_analyze_large_table_interval

- 默认值: 12 * 3600
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: 大表的查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

##### connector_table_query_trigger_analyze_max_pending_task_num

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: FE 上处于 Pending 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

##### connector_table_query_trigger_analyze_schedule_interval

- 默认值: 30
- 类型: Int
- 单位: 秒
- 是否可变: 是
- 描述: Scheduler 线程调度查询触发 ANALYZE 任务的间隔。
- 引入版本: v3.4.0

##### connector_table_query_trigger_analyze_max_running_task_num

- 默认值: 2
- 类型: Int
- 单位: -
- 是否可变: 是
- 描述: FE 上处于 Running 状态的查询触发 ANALYZE 任务的最大数量。
- 引入版本: v3.4.0

### 手动收集

您可以按需创建 Analyze job，该作业在创建后立即运行。

#### 创建手动收集任务

语法：

```sql
-- Manual full collection
ANALYZE [FULL] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES(property [,property])]

-- Manual histogram collection (since v3.3.0)
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
[PROPERTIES (property [,property])]
```

以下是手动完整收集的示例：

```sql
ANALYZE TABLE ex_hive_tbl(k1);
+----------------------------------+---------+----------+----------+
| Table                            | Op      | Msg_type | Msg_text |
+----------------------------------+---------+----------+----------+
| hive_catalog.tn_test.ex_hive_tbl | analyze | status   | OK       |
+----------------------------------+---------+----------+----------+
```

#### 查看任务状态

语法：

```sql
SHOW ANALYZE STATUS [LIKE | WHERE predicate]
```

示例：

```sql
SHOW ANALYZE STATUS where `table` = 'ex_hive_tbl';
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| Id    | Database             | Table       | Columns | Type | Schedule | Status  | StartTime           | EndTime             | Properties | Reason |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| 16400 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:31:42 | 2023-12-04 16:31:42 | {}         |        |
| 16465 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:35 | 2023-12-04 16:37:35 | {}         |        |
| 16467 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:46 | 2023-12-04 16:37:46 | {}         |        |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
```

#### 查看统计信息元数据

语法：

```sql
SHOW STATS META [WHERE predicate]
```

示例：

```sql
SHOW STATS META where `table` = 'ex_hive_tbl';
+----------------------+-------------+---------+------+---------------------+------------+---------+
| Database             | Table       | Columns | Type | UpdateTime          | Properties | Healthy |
+----------------------+-------------+---------+------+---------------------+------------+---------+
| hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | 2023-12-04 16:37:46 | {}         |         |
+----------------------+-------------+---------+------+---------------------+------------+---------+
```

#### 取消收集任务

取消正在运行的收集任务。

语法：

```sql
KILL ANALYZE <ID>
```

您可以在 SHOW ANALYZE STATUS 的输出中查看任务 ID。

### 自动收集

要使系统自动收集外部数据源中表的统计信息，您可以创建一个 Analyze job。StarRocks 会在默认的 5 分钟检查间隔内自动检查是否运行该任务。对于 Hive 和 Iceberg 表，StarRocks 仅在表数据更新时才运行收集任务。

但是，无法感知 Hudi 表中的数据更改，StarRocks 会根据您指定的检查间隔和收集间隔定期收集统计信息。您可以指定以下 FE 配置项来控制收集行为：

- statistic_collect_interval_sec
  自动收集过程中检查数据更新的间隔。单位：秒。默认值：5 分钟。

- statistic_auto_collect_small_table_rows (v3.2 及更高版本)
  自动收集过程中，判断外部数据源（Hive、Iceberg、Hudi）中的表是否为小表的阈值。默认值：10000000。

- statistic_auto_collect_small_table_interval
  小表收集统计信息的间隔。单位：秒。默认值：0。

- statistic_auto_collect_large_table_interval
  大表收集统计信息的间隔。单位：秒。默认值：43200 (12 小时)。

自动收集线程会按照 `statistic_collect_interval_sec` 指定的间隔检查数据更新。如果表中的行数小于 `statistic_auto_collect_small_table_rows`，它将根据 `statistic_auto_collect_small_table_interval` 收集这些表的统计信息。

如果表中的行数超过 `statistic_auto_collect_small_table_rows`，它将根据 `statistic_auto_collect_large_table_interval` 收集这些表的统计信息。它仅在 `上次表更新时间 + 收集间隔 > 当前时间` 时更新大表的统计信息。这可以防止对大表进行频繁的 analyze 任务。

#### 创建自动收集任务

语法：

```sql
CREATE ANALYZE TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

您可以指定属性 `statistic_auto_collect_interval` 来专门设置自动收集任务的收集间隔。FE 配置项 `statistic_auto_collect_small_table_interval` 和 `statistic_auto_collect_large_table_interval` 将不会对此任务生效。

示例：

```sql
CREATE ANALYZE TABLE ex_hive_tbl (k1)
PROPERTIES ("statistic_auto_collect_interval" = "5");

Query OK, 0 rows affected (0.01 sec)
```

#### 查看自动收集任务的状态

与手动收集相同。

#### 查看统计信息元数据

与手动收集相同。

#### 查看自动收集任务

语法：

```sql
SHOW ANALYZE JOB [WHERE predicate]
```

示例：

```sql
SHOW ANALYZE JOB WHERE `id` = '17204';

Empty set (0.00 sec)
```

#### 取消收集任务

与手动收集相同。

#### 删除统计信息

```sql
DROP STATS tbl_name
```

## 参考资料

- 查询 FE 配置项，请运行 [ADMIN SHOW CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

- 修改 FE 配置项，请运行 [ADMIN SET CONFIG](../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md)。
