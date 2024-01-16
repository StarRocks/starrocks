---
displayed_sidebar: "Chinese"
---

# 异步物化视图故障排除

本文介绍了如何检查您的异步物化视图，并解决在使用时遇到的问题。

> **注意**
>
> 以下展示的部分功能仅在 StarRocks v3.1 及以后的版本中支持。

## 检查异步物化视图

为了全面了解您正在使用的异步物化视图，您可以首先检查它们的工作状态、刷新历史和资源消耗情况。

### 检查异步物化视图工作状态

您可以使用 [SHOW MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md) 命令来检查异步物化视图的工作状态。在返回的所有信息中，您可以关注以下字段：

- `is_active`：物化视图的状态是否为 Active 状态。只有处于 Active 状态的物化视图才能用于查询加速和改写。
- `last_refresh_state`：最近一次刷新的状态，包括 PENDING（等待中）、RUNNING（运行中）、FAILED（失败）和 SUCCESS（成功）。
- `last_refresh_error_message`：上次刷新失败的原因（如果物化视图状态不为 Active 状态）。
- `rows`：物化视图中的数据行数。请注意，这个值可能与物化视图的实际行数不同，因为更新可能有延迟。

有关返回的其他字段的详细信息，请参阅 [SHOW MATERIALIZED VIEW - 返回](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md#返回)。

示例：

```Plain
MySQL > SHOW MATERIALIZED VIEW LIKE 'mv_pred_2'\G
***************************[ 1. row ]***************************
id                                   | 112517
database_name                        | ssb_1g
name                                 | mv_pred_2
refresh_type                         | ASYNC
is_active                            | true
inactive_reason                      | <null>
partition_type                       | UNPARTITIONED
task_id                              | 457930
task_name                            | mv-112517
last_refresh_start_time              | 2023-08-04 16:46:50
last_refresh_finished_time           | 2023-08-04 16:46:54
last_refresh_duration                | 3.996
last_refresh_state                   | SUCCESS
last_refresh_force_refresh           | false
last_refresh_start_partition         |
last_refresh_end_partition           |
last_refresh_base_refresh_partitions | {}
last_refresh_mv_refresh_partitions   |
last_refresh_error_code              | 0
last_refresh_error_message           |
rows                                 | 0
text                                 | CREATE MATERIALIZED VIEW `mv_pred_2` (`lo_quantity`, `lo_revenue`, `sum`)
DISTRIBUTED BY HASH(`lo_quantity`, `lo_revenue`) BUCKETS 2
REFRESH ASYNC
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD"
)
AS SELECT `lineorder`.`lo_quantity`, `lineorder`.`lo_revenue`, sum(`lineorder`.`lo_tax`) AS `sum`
FROM `ssb_1g`.`lineorder`
WHERE `lineorder`.`lo_linenumber` = 1
GROUP BY 1, 2;

1 row in set
Time: 0.003s
```

### 查看异步物化视图的刷新历史

您可以通过查询 `information_schema` 数据库中的 `task_runs` 表来查看异步物化视图的刷新历史。在返回的所有信息中，您可以关注以下字段：

- `CREATE_TIME` 和 `FINISH_TIME`：刷新任务的开始和结束时间。
- `STATE`：刷新任务的状态，包括 PENDING（等待中）、RUNNING（运行中）、FAILED（失败）和 SUCCESS（成功）。
- `ERROR_MESSAGE`：刷新任务失败的原因。

示例：

```Plain
MySQL > SELECT * FROM information_schema.task_runs WHERE task_name ='mv-112517' \G
***************************[ 1. row ]***************************
QUERY_ID      | 7434cee5-32a3-11ee-b73a-8e20563011de
TASK_NAME     | mv-112517
CREATE_TIME   | 2023-08-04 16:46:50
FINISH_TIME   | 2023-08-04 16:46:54
STATE         | SUCCESS
DATABASE      | ssb_1g
EXPIRE_TIME   | 2023-08-05 16:46:50
ERROR_CODE    | 0
ERROR_MESSAGE | <null>
PROGRESS      | 100%
EXTRA_MESSAGE | {"forceRefresh":false,"mvPartitionsToRefresh":[],"refBasePartitionsToRefreshMap":{},"basePartitionsToRefreshMap":{}}
PROPERTIES    | {"FORCE":"false"}
***************************[ 2. row ]***************************
QUERY_ID      | 72dd2f16-32a3-11ee-b73a-8e20563011de
TASK_NAME     | mv-112517
CREATE_TIME   | 2023-08-04 16:46:48
FINISH_TIME   | 2023-08-04 16:46:53
STATE         | SUCCESS
DATABASE      | ssb_1g
EXPIRE_TIME   | 2023-08-05 16:46:48
ERROR_CODE    | 0
ERROR_MESSAGE | <null>
PROGRESS      | 100%
EXTRA_MESSAGE | {"forceRefresh":true,"mvPartitionsToRefresh":["mv_pred_2"],"refBasePartitionsToRefreshMap":{},"basePartitionsToRefreshMap":{"lineorder":["lineorder"]}}
PROPERTIES    | {"FORCE":"true"}
```

### 监控异步物化视图的资源消耗情况

您可以在刷新任务执行期间或完成之后监控和分析异步物化视图所消耗的资源。

#### 刷新任务执行期间监控资源消耗

刷新任务执行期间，您可以使用 [SHOW PROC '/current_queries'](../sql-reference/sql-statements/Administration/SHOW_PROC.md) 实时监控其资源消耗情况。

在返回的所有信息中，您可以关注以下字段：

- `ScanBytes`：扫描的数据大小。
- `ScanRows`：扫描的数据行数。
- `MemoryUsage`：使用的内存大小。
- `CPUTime`：CPU 时间成本。
- `ExecTime`：查询的执行时间。

示例：

```Plain
MySQL > SHOW PROC '/current_queries'\G
***************************[ 1. row ]***************************
StartTime     | 2023-08-04 17:01:30
QueryId       | 806eed7d-32a5-11ee-b73a-8e20563011de
ConnectionId  | 0
Database      | ssb_1g
User          | root
ScanBytes     | 70.981 MB
ScanRows      | 6001215 rows
MemoryUsage   | 73.748 MB
DiskSpillSize | 0.000
CPUTime       | 2.515 s
ExecTime      | 2.583 s
```

#### 刷新任务完成后分析资源消耗

在刷新任务完成后，您可以通过 Query Profile 来分析其资源消耗情况。

当异步物化视图正在刷新时，会执行 INSERT OVERWRITE 语句。您可以检查相应的 Query Profile，以分析刷新任务所消耗的时间和资源。

在返回的所有信息中，您可以关注以下指标：

- `Total`：查询消耗的总时间。
- `QueryCpuCost`：查询的总 CPU 时间成本。CPU 时间成本会对并发进程进行聚合。因此，该指标的值可能大于查询的实际执行时间。
- `QueryMemCost`：查询的总内存成本。
- 其他针对各个运算符的特定指标，比如连接运算符和聚合运算符。

有关如何分析 Query Profile 和理解其他指标的详细信息，请参阅 [查看分析 Query Profile](../administration/query_profile.md).

### 验证查询是否被异步物化视图改写

您可以通过使用 [EXPLAIN](../sql-reference/sql-statements/Administration/EXPLAIN.md) 查看查询计划，以检查查询是否可以被异步物化视图重写。

如果查询计划中的 `SCAN` 指标显示了相应物化视图的名称，那么该查询已经被物化视图重写。

示例一：

```Plain
MySQL > SHOW CREATE TABLE mv_agg\G
***************************[ 1. row ]***************************
Materialized View        | mv_agg
Create Materialized View | CREATE MATERIALIZED VIEW `mv_agg` (`c_custkey`)
DISTRIBUTED BY RANDOM
REFRESH ASYNC
PROPERTIES (
"replication_num" = "1",
"replicated_storage" = "true",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`
FROM `ssb_1g`.`customer`
GROUP BY `customer`.`c_custkey`;

MySQL > EXPLAIN LOGICAL SELECT `customer`.`c_custkey`
                      -> FROM `ssb_1g`.`customer`
                      -> GROUP BY `customer`.`c_custkey`;
+-----------------------------------------------------------------------------------+
| Explain String                                                                    |
+-----------------------------------------------------------------------------------+
| - Output => [1:c_custkey]                                                         |
|     - SCAN [mv_agg] => [1:c_custkey]                                              |
|             Estimates: {row: 30000, cpu: ?, memory: ?, network: ?, cost: 15000.0} |
|             partitionRatio: 1/1, tabletRatio: 12/12                               |
|             1:c_custkey := 10:c_custkey                                           |
+-----------------------------------------------------------------------------------+
```

如果禁用了查询重写功能，StarRocks 将采用常规的查询计划。

示例二：

```Plain
MySQL > SET enable_materialized_view_rewrite = false;
MySQL > EXPLAIN LOGICAL SELECT `customer`.`c_custkey`
                      -> FROM `ssb_1g`.`customer`
                      -> GROUP BY `customer`.`c_custkey`;
+---------------------------------------------------------------------------------------+
| Explain String                                                                        |
+---------------------------------------------------------------------------------------+
| - Output => [1:c_custkey]                                                             |
|     - AGGREGATE(GLOBAL) [1:c_custkey]                                                 |
|             Estimates: {row: 15000, cpu: ?, memory: ?, network: ?, cost: 120000.0}    |
|         - SCAN [mv_bitmap] => [1:c_custkey]                                           |
|                 Estimates: {row: 60000, cpu: ?, memory: ?, network: ?, cost: 30000.0} |
|                 partitionRatio: 1/1, tabletRatio: 12/12                               |
+---------------------------------------------------------------------------------------+
```

## 诊断并解决故障

以下列出了在使用异步物化视图时可能遇到的一些常见问题，以及相应的解决方案。

### 创建异步物化视图失败

如果无法创建异步物化视图，即无法执行 CREATE MATERIALIZED VIEW 语句，您可以从以下几个方面着手解决：

- **检查是否误用了创建同步物化视图的 SQL 语句。**

  StarRocks 提供了两种不同的物化视图：同步物化视图和异步物化视图。

  创建同步物化视图时使用的基本 SQL 语句如下：

  ```SQL
  CREATE MATERIALIZED VIEW <mv_name> 
  AS <query>
  ```

  与之相比，创建异步物化视图时使用的 SQL 语句包含更多参数：

  ```SQL
  CREATE MATERIALIZED VIEW <mv_name> 
  REFRESH ASYNC -- 异步物化视图的刷新策略。
  DISTRIBUTED BY HASH(<column>) -- 异步物化视图的数据分布策略。
  AS <query>
  ```

  除了 SQL 语句之外，这两种物化视图之间的主要区别在于，异步物化视图支持 StarRocks 提供的所有查询语法，而同步物化视图只支持有限的聚合函数。

- **检查是否指定了正确的 `Partition By` 列。**

  在创建异步物化视图时，您可以为其指定分区策略，从而在更细粒度的级别上刷新物化视图。

  目前，StarRocks 仅支持单列分区列，且只支持 Range Partition。您可以在分区列使用 date_trunc() 函数进行上卷以更改分区策略的粒度级别。除此之外不支持任何其他表达式。

- **检查您是否具有创建物化视图所需的权限。**

  在创建异步物化视图时，您需要所有查询对象（表、视图、物化视图）的 SELECT 权限。当查询中使用 UDF 时，您还需要函数的 USAGE 权限。

### 物化视图刷新失败

如果物化视图刷新失败，即刷新任务的状态不是 SUCCESS，您可以从以下几个方面着手解决：

- **检查是否采用了不合适的刷新策略。**

  默认情况下，物化视图在创建后会立即刷新。然而，在 v2.5 及早期版本中，采用 MANUAL 刷新策略的物化视图在创建后不会自动刷新。您必须使用 REFRESH MATERIALIZED VIEW 命令手动刷新。

- **检查刷新任务是否超出了内存限制。**

  通常情况下，当异步物化视图涉及大规模的聚合或连接计算时，会大量消耗内存资源。要解决这个问题，您可以：

  - 为物化视图指定分区策略，实现细粒度的刷新。
  - 为刷新任务启用中间结果落盘功能。从 v3.1 版本开始，StarRocks 支持将物化视图刷新任务的部分中间结果落盘。执行以下语句启用中间结果落盘功能：

    ```SQL
    SET enable_spill = true;
    ```

- **检查刷新任务是否超时。**

  较大的物化视图可能因为刷新任务超过超时时间而无法刷新。要解决这个问题，您可以：

  - 为物化视图指定分区策略，实现细粒度的刷新。
  - 增加超时时间。

从 v3.0 版本开始，您可以在创建物化视图时定义以下属性（Session Variable），或者使用 ALTER MATERIALIZED VIEW 命令进行添加：

示例：

```SQL
-- 在创建物化视图时定义属性。
CREATE MATERIALIZED VIEW mv1 
REFRESH ASYNC
PROPERTIES ( 'session.enable_spill'='true' )
AS <query>;

-- 添加属性。
ALTER MATERIALIZED VIEW mv2 
    SET ('session.enable_spill' = 'true');
```

### 物化视图不可用

如果物化视图无法改写查询或刷新，且物化视图的 `is_active` 状态为 `false`，可能是由于基表发生 Schema Change。要解决这个问题，您可以通过执行以下语句手动将物化视图状态设置为 Active：

```SQL
ALTER MATERIALIZED VIEW mv1 ACTIVE;
```

如果设置没有生效，您需要删除该物化视图并重新创建。

### 物化视图刷新任务占用过多资源

如果您发现刷新任务正在使用过多的系统资源，您可以从以下几个方面着手解决：

- **检查创建的物化视图是否过大。**

  如果您 Join 了多张表，导致了大量的计算，刷新任务将占用大量资源。要解决这个问题，您需要评估物化视图的大小并重新规划创建。

- **检查刷新间隔是否过于频繁。**

  如果物化视图采用了固定间隔刷新的策略，您可以调低刷新频率解决问题。如果刷新任务是由基表中的数据变更触发的，频繁地数据导入操作也可能导致此问题。要解决这个问题，您需要为物化视图定义合适的刷新策略。

- **检查物化视图是否已分区。**

  未分区的物化视图在刷新时可能消耗大量资源，因为 StarRocks 每次都会刷新整个物化视图。要解决这个问题，您需要为物化视图指定分区策略，实现细粒度的刷新。

要停止占用过多资源的刷新任务，您可以：

- 将物化视图状态设置为 Inactive，以停止所有刷新任务：

  ```SQL
  ALTER MATERIALIZED VIEW mv1 INACTIVE;
  ```

- 使用 SHOW PROCESSLIST 和 KILL 语句终止正在运行的刷新任务：

  ```SQL
  -- 获取正在运行的刷新任务的 ConnectionId。
  SHOW PROCESSLIST;
  -- 终止正在运行的刷新任务。
  KILL QUERY <ConnectionId>;
  ```

### 物化视图无法改写查询

如果物化视图无法改写相关查询，您可以从以下几个方面着手解决：

- **检查物化视图和查询是否匹配。**

  - StarRocks 使用基于结构而非基于文本的匹配技术来匹配物化视图和查询。因此，并不是当查询与物化视图看起来一样时就一定改写可以查询。
  - 物化视图只支持重写 SPJG（Select/Projection/Join/Aggregation）类型的查询，不支持改写涉及窗口函数、嵌套聚合或 Join 加聚合的查询。
  - 物化视图无法重写 Outer Join 中包含复杂的 Join 谓词的查询。例如，在类似 `A LEFT JOIN B ON A.dt > '2023-01-01' AND A.id = B.id` 的情况下，建议您将 `JOIN ON` 子句中的谓词在 `WHERE` 子句中指定。

  有关物化视图查询重写的限制信息，请参阅 [物化视图查询改写 - 限制](./query_rewrite_with_materialized_views.md#限制)。

- **检查物化视图的状态是否为 Active。**

  StarRocks 在改写查询之前会检查物化视图的状态。只有当物化视图的状态为 Active 时，查询才能被改写。要解决这个问题，您可以通过执行以下语句手动将物化视图的状态设置为 Active：

  ```SQL
  ALTER MATERIALIZED VIEW mv1 ACTIVE;
  ```

- **检查物化视图是否满足数据一致性要求。**

  StarRocks 会检查物化视图与基表数据的一致性。默认情况下，只有当物化视图中的数据为最新时，才能重写查询。要解决这个问题，您可以：

  - 为物化视图添加 `PROPERTIES('query_rewrite_consistency'='LOOSE')` 禁用一致性检查。
  - 为物化视图添加 `PROPERTIES('mv_rewrite_staleness_second'='5')` 来容忍一定程度的数据不一致。只要上次刷新在该时间间隔之内，无论基表中的数据是否发生变化，查询都可以被重写。

- **检查物化视图的查询语句是否缺少输出列。**

  为了改写范围和点查询，您必须在物化视图的查询语句的 SELECT 表达式中指定过滤所用的谓词。您需要检查物化视图的 SELECT 语句，以确保其含有查询中 `WHERE` 和 `ORDER BY` 子句中引用的列。

示例1：物化视图 `mv1` 使用了嵌套聚合，因此无法用于重写查询。

```SQL
CREATE MATERIALIZED VIEW mv1 REFRESH ASYNC AS
select count(distinct cnt) 
from (
    select c_city, count(*) cnt 
    from customer 
    group by c_city
) t;
```

示例2：物化视图 `mv2` 使用了 Join 加聚合，因此无法用于重写查询。要解决这个问题，您可以创建一个带有聚合的物化视图，然后基于该物化视图创建带有 Join 的嵌套物化视图。

```SQL
CREATE MATERIALIZED VIEW mv2 REFRESH ASYNC AS
select *
from (
    select lo_orderkey, lo_custkey, p_partkey, p_name
    from lineorder
    join part on lo_partkey = p_partkey
) lo
join (
    select c_custkey
    from customer
    group by c_custkey
) cust
on lo.lo_custkey = cust.c_custkey;
```

示例3：物化视图 `mv3` 无法改写模式为 `SELECT c_city, sum(tax) FROM tbl WHERE dt='2023-01-01' AND c_city = 'xxx'` 的查询，因为谓词引用的列不在 SELECT 表达式中。

```SQL
CREATE MATERIALIZED VIEW mv3 REFRESH ASYNC AS
SELECT c_city, sum(tax) FROM tbl GROUP BY c_city;
```

要解决这个问题，您可以按照以下方式创建物化视图：

```SQL
CREATE MATERIALIZED VIEW mv3 REFRESH ASYNC AS
SELECT dt, c_city, sum(tax) FROM tbl GROUP BY dt, c_city;
```
