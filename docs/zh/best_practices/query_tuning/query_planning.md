---
displayed_sidebar: docs
---

# 查询分析

如何优化查询性能是一个常见的问题。慢查询会影响用户体验和集群性能。因此，分析和优化查询性能非常重要。

你可以在 `fe/log/fe.audit.log` 中查看查询信息。每个查询对应一个 `QueryID`，可以用来搜索查询的 `QueryPlan` 和 `Profile`。`QueryPlan` 是通过解析 SQL 语句由 FE 生成的执行计划。`Profile` 是 BE 的执行结果，包含每个步骤消耗的时间和处理的数据量等信息。

## 计划分析

在 StarRocks 中，SQL 语句的生命周期可以分为三个阶段：查询解析、查询计划和查询执行。查询解析通常不是瓶颈，因为分析工作负载所需的 QPS 不高。

StarRocks 中的查询性能由查询计划和查询执行决定。查询计划负责协调操作符（Join/Order/Aggregate），而查询执行负责运行具体的操作。

查询计划为 DBA 提供了一个宏观视角来访问查询信息。查询计划是查询性能的关键，也是 DBA 参考的良好资源。以下代码片段以 `TPCDS query96` 为例，展示如何查看查询计划。

使用 [EXPLAIN](../../sql-reference/sql-statements/cluster-management/plan_profile/EXPLAIN.md) 语句查看查询的计划。

```SQL
EXPLAIN select  count(*)
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

查询计划有两种类型——逻辑查询计划和物理查询计划。这里描述的查询计划指的是逻辑查询计划。`TPCDS query96.sql` 对应的查询计划如下所示。

```sql
+------------------------------------------------------------------------------+
| Explain String                                                               |
+------------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                              |
|  OUTPUT EXPRS:<slot 11>                                                      |
|   PARTITION: UNPARTITIONED                                                   |
|   RESULT SINK                                                                |
|   12:MERGING-EXCHANGE                                                        |
|      limit: 100                                                              |
|      tuple ids: 5                                                            |
|                                                                              |
| PLAN FRAGMENT 1                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 12                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   8:TOP-N                                                                    |
|   |  order by: <slot 11> ASC                                                 |
|   |  offset: 0                                                               |
|   |  limit: 100                                                              |
|   |  tuple ids: 5                                                            |
|   |                                                                          |
|   7:AGGREGATE (update finalize)                                              |
|   |  output: count(*)                                                        |
|   |  group by:                                                               |
|   |  tuple ids: 4                                                            |
|   |                                                                          |
|   6:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: left hash join node can not do colocate        |
|   |  equal join conjunct: `ss_store_sk` = `s_store_sk`                       |
|   |  tuple ids: 0 2 1 3                                                      |
|   |                                                                          |
|   |----11:EXCHANGE                                                           |
|   |       tuple ids: 3                                                       |
|   |                                                                          |
|   4:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: left hash join node can not do colocate        |
|   |  equal join conjunct: `ss_hdemo_sk`=`household_demographics`.`hd_demo_sk`|
|   |  tuple ids: 0 2 1                                                        |
|   |                                                                          |
|   |----10:EXCHANGE                                                           |
|   |       tuple ids: 1                                                       |
|   |                                                                          |
|   2:HASH JOIN                                                                |
|   |  join op: INNER JOIN (BROADCAST)                                         |
|   |  hash predicates:                                                        |
|   |  colocate: false, reason: table not in same group                        |
|   |  equal join conjunct: `ss_sold_time_sk` = `time_dim`.`t_time_sk`         |
|   |  tuple ids: 0 2                                                          |
|   |                                                                          |
|   |----9:EXCHANGE                                                            |
|   |       tuple ids: 2                                                       |
|   |                                                                          |
|   0:OlapScanNode                                                             |
|      TABLE: store_sales                                                      |
|      PREAGGREGATION: OFF. Reason: `ss_sold_time_sk` is value column          |
|      partitions=1/1                                                          |
|      rollup: store_sales                                                     |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 0                                                            |
|                                                                              |
| PLAN FRAGMENT 2                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|                                                                              |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 11                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   5:OlapScanNode                                                             |
|      TABLE: store                                                            |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `store`.`s_store_name` = 'ese'                              |
|      partitions=1/1                                                          |
|      rollup: store                                                           |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 3                                                            |
|                                                                              |
| PLAN FRAGMENT 3                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 10                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   3:OlapScanNode                                                             |
|      TABLE: household_demographics                                           |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `household_demographics`.`hd_dep_count` = 5                 |
|      partitions=1/1                                                          |
|      rollup: household_demographics                                          |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 1                                                            |
|                                                                              |
| PLAN FRAGMENT 4                                                              |
|  OUTPUT EXPRS:                                                               |
|   PARTITION: RANDOM                                                          |
|   STREAM DATA SINK                                                           |
|     EXCHANGE ID: 09                                                          |
|     UNPARTITIONED                                                            |
|                                                                              |
|   1:OlapScanNode                                                             |
|      TABLE: time_dim                                                         |
|      PREAGGREGATION: OFF. Reason: null                                       |
|      PREDICATES: `time_dim`.`t_hour` = 8, `time_dim`.`t_minute` >= 30        |
|      partitions=1/1                                                          |
|      rollup: time_dim                                                        |
|      tabletRatio=0/0                                                         |
|      tabletList=                                                             |
|      cardinality=-1                                                          |
|      avgRowSize=0.0                                                          |
|      numNodes=0                                                              |
|      tuple ids: 2                                                            |
+------------------------------------------------------------------------------+
128 rows in set (0.02 sec)
```

查询 96 展示了一个涉及多个 StarRocks 概念的查询计划。

|名称|解释|
|--|--|
|avgRowSize|扫描数据行的平均大小|
|cardinality|扫描表中的数据行总数|
|colocate|表是否处于 colocate 模式|
|numNodes|要扫描的节点数|
|rollup|物化视图|
|preaggregation|预聚合|
|predicates|谓词，查询过滤器|

查询 96 的查询计划被分为五个片段，从 0 到 4 编号。查询计划可以自下而上逐个阅读。

片段 4 负责扫描 `time_dim` 表并提前执行相关的查询条件（即 `time_dim.t_hour = 8 and time_dim.t_minute >= 30`）。这一步也被称为谓词下推。StarRocks 决定是否为聚合表启用 `PREAGGREGATION`。在前面的图中，`time_dim` 的预聚合被禁用。在这种情况下，`time_dim` 的所有维度列都会被读取，如果表中有很多维度列，这可能会对性能产生负面影响。如果 `time_dim` 表选择 `range partition` 进行数据划分，查询计划中会命中几个分区，自动过滤掉不相关的分区。如果存在物化视图，StarRocks 会根据查询自动选择物化视图。如果没有物化视图，查询将自动命中基表（例如，前图中的 `rollup: time_dim`）。

扫描完成后，片段 4 结束。数据将传递给其他片段，如前图中所示的 EXCHANGE ID : 09，传递到标记为 9 的接收节点。

对于查询 96 的查询计划，片段 2、3 和 4 具有相似的功能，但它们负责扫描不同的表。具体来说，查询中的 `Order/Aggregation/Join` 操作在片段 1 中执行。

片段 1 使用 `BROADCAST` 方法执行 `Order/Aggregation/Join` 操作，即将小表广播到大表。如果两个表都很大，建议使用 `SHUFFLE` 方法。目前，StarRocks 仅支持 `HASH JOIN`。`colocate` 字段用于显示两个连接表的分区和分桶方式相同，因此可以在本地执行连接操作而无需迁移数据。当 Join 操作完成后，将执行上层的 `aggregation`、`order by` 和 `top-n` 操作。

通过去除具体的表达式（仅保留操作符），查询计划可以以更宏观的视角呈现，如下图所示。

![8-5](../../_assets/8-5.png)

## 查询提示

查询提示是显式建议查询优化器如何执行查询的指令或注释。目前，StarRocks 支持三种类型的提示：系统变量提示 (`SET_VAR`)、用户定义变量提示 (`SET_USER_VARIABLE`) 和 Join 提示。提示仅在单个查询中生效。

### 系统变量提示

你可以在 SELECT 和 SUBMIT TASK 语句中使用 `SET_VAR` 提示来设置一个或多个 [系统变量](../../sql-reference/System_variable.md)，然后执行这些语句。你还可以在其他语句中包含的 SELECT 子句中使用 `SET_VAR` 提示，例如 CREATE MATERIALIZED VIEW AS SELECT 和 CREATE VIEW AS SELECT。注意，如果在 CTE 的 SELECT 子句中使用 `SET_VAR` 提示，即使语句成功执行，`SET_VAR` 提示也不会生效。

与 [系统变量的一般用法](../../sql-reference/System_variable.md) 相比，`SET_VAR` 提示在语句级别生效，不会影响整个会话。

#### 语法

```SQL
[...] SELECT /*+ SET_VAR(key=value [, key = value]) */ ...
SUBMIT [/*+ SET_VAR(key=value [, key = value]) */] TASK ...
```

#### 示例

要为聚合查询指定聚合模式，可以使用 `SET_VAR` 提示在聚合查询中设置系统变量 `streaming_preaggregation_mode` 和 `new_planner_agg_stage`。

```SQL
SELECT /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming',new_planner_agg_stage = '2') */ SUM(sales_amount) AS total_sales_amount FROM sales_orders;
```

要为 SUBMIT TASK 语句指定执行超时时间，可以在 SUBMIT TASK 语句中使用 `SET_VAR` 提示设置系统变量 `query_timeout`。

```SQL
SUBMIT /*+ SET_VAR(query_timeout=3) */ TASK AS CREATE TABLE temp AS SELECT count(*) AS cnt FROM tbl1;
```

要为创建物化视图的子查询指定执行超时时间，可以在 SELECT 子句中使用 `SET_VAR` 提示设置系统变量 `query_timeout`。

```SQL
CREATE MATERIALIZED VIEW mv 
PARTITION BY dt 
DISTRIBUTED BY HASH(`key`) 
BUCKETS 10 
REFRESH ASYNC 
AS SELECT /*+ SET_VAR(query_timeout=500) */ * from dual;
```

### 用户定义变量提示

你可以在 SELECT 语句或 INSERT 语句中使用 `SET_USER_VARIABLE` 提示来设置一个或多个 [用户定义变量](../../sql-reference/user_defined_variables.md)。如果其他语句包含 SELECT 子句，你也可以在该 SELECT 子句中使用 `SET_USER_VARIABLE` 提示。其他语句可以是 SELECT 语句和 INSERT 语句，但不能是 CREATE MATERIALIZED VIEW AS SELECT 语句和 CREATE VIEW AS SELECT 语句。注意，如果在 CTE 的 SELECT 子句中使用 `SET_USER_VARIABLE` 提示，即使语句成功执行，`SET_USER_VARIABLE` 提示也不会生效。从 v3.2.4 开始，StarRocks 支持用户定义变量提示。

与 [用户定义变量的一般用法](../../sql-reference/user_defined_variables.md) 相比，`SET_USER_VARIABLE` 提示在语句级别生效，不会影响整个会话。

#### 语法

```SQL
[...] SELECT /*+ SET_USER_VARIABLE(@var_name = expr [, @var_name = expr]) */ ...
INSERT /*+ SET_USER_VARIABLE(@var_name = expr [, @var_name = expr]) */ ...
```

#### 示例

以下 SELECT 语句引用了标量子查询 `select max(age) from users` 和 `select min(name) from users`，因此你可以使用 `SET_USER_VARIABLE` 提示将这两个标量子查询设置为用户定义变量，然后运行查询。

```SQL
SELECT /*+ SET_USER_VARIABLE (@a = (select max(age) from users), @b = (select min(name) from users)) */ * FROM sales_orders where sales_orders.age = @a and sales_orders.name = @b;
```

### Join 提示

对于多表 Join 查询，优化器通常会选择最优的 Join 执行方法。在特殊情况下，你可以使用 Join 提示显式建议优化器使用的 Join 执行方法或禁用 Join Reorder。目前，Join 提示支持建议 Shuffle Join、Broadcast Join、Bucket Shuffle Join 或 Colocate Join 作为 Join 执行方法。当使用 Join 提示时，优化器不会执行 Join Reorder。因此，你需要选择较小的表作为右表。此外，当建议 [Colocate Join](../../using_starrocks/Colocate_join.md) 或 Bucket Shuffle Join 作为 Join 执行方法时，请确保连接表的数据分布符合这些 Join 执行方法的要求。否则，建议的 Join 执行方法无法生效。

#### 语法

```SQL
... JOIN { [BROADCAST] | [SHUFFLE] | [BUCKET] | [COLOCATE] | [UNREORDER]} ...
```

:::note
Join 提示不区分大小写。
:::

#### 示例

- Shuffle Join

  如果你需要在执行 Join 操作之前将表 A 和表 B 中具有相同分桶键值的数据行分发到同一台机器上，可以提示 Join 执行方法为 Shuffle Join。

  ```SQL
  select k1 from t1 join [SHUFFLE] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

- Broadcast Join
  
  如果表 A 是一个大表，表 B 是一个小表，你可以提示 Join 执行方法为 Broadcast Join。表 B 的数据被完全广播到表 A 所在的机器上，然后执行 Join 操作。与 Shuffle Join 相比，Broadcast Join 节省了对表 A 数据进行洗牌的成本。

  ```SQL
  select k1 from t1 join [BROADCAST] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

- Bucket Shuffle Join
  
  如果 Join 查询中的等值连接表达式包含表 A 的分桶键，尤其是当表 A 和表 B 都是大表时，你可以提示 Join 执行方法为 Bucket Shuffle Join。表 B 的数据根据表 A 的数据分布被洗牌到表 A 所在的机器上，然后执行 Join 操作。与 Broadcast Join 相比，Bucket Shuffle Join 显著减少了数据传输，因为表 B 的数据只需在全局范围内洗牌一次。
  参与 Bucket Shuffle Join 的表必须是非分区表或 colocated 表。

  ```SQL
  select k1 from t1 join [BUCKET] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

- Colocate Join
  
  如果表 A 和表 B 属于在创建表时指定的同一个 Colocation Group，表 A 和表 B 中具有相同分桶键值的数据行分布在同一个 BE 节点上。当 Join 查询中的等值连接表达式包含表 A 和表 B 的分桶键时，你可以提示 Join 执行方法为 Colocate Join。具有相同键值的数据直接在本地连接，减少了节点间数据传输的时间，提高了查询性能。

  ```SQL
  select k1 from t1 join [COLOCATE] t2 on t1.k1 = t2.k2 group by t2.k2;
  ```

#### 查看 Join 执行方法

使用 `EXPLAIN` 命令查看实际的 Join 执行方法。如果返回的结果显示 Join 执行方法与 Join 提示匹配，则表示 Join 提示生效。

```SQL
EXPLAIN select k1 from t1 join [COLOCATE] t2 on t1.k1 = t2.k2 group by t2.k2;
```

![8-9](../../_assets/8-9.png)

## SQL 指纹

SQL 指纹用于优化慢查询并提高系统资源利用率。StarRocks 使用 SQL 指纹功能对慢查询日志 (`fe.audit.log.slow_query`) 中的 SQL 语句进行标准化，将 SQL 语句分类为不同类型，并计算每种 SQL 类型的 MD5 哈希值以识别慢查询。MD5 哈希值由字段 `Digest` 指定。

```SQL
2021-12-27 15:13:39,108 [slow_query] |Client=172.26.xx.xxx:54956|User=root|Db=default_cluster:test|State=EOF|Time=2469|ScanBytes=0|ScanRows=0|ReturnRows=6|StmtId=3|QueryId=824d8dc0-66e4-11ec-9fdc-00163e04d4c2|IsQuery=true|feIp=172.26.92.195|Stmt=select count(*) from test_basic group by id_bigint|Digest=51390da6b57461f571f0712d527320f4
```

SQL 语句标准化将语句文本转换为更标准化的格式，并仅保留重要的语句结构。

- 保留对象标识符，如数据库和表名。

- 将常量转换为问号（?）。

- 删除注释并格式化空格。

例如，以下两个 SQL 语句在标准化后属于同一类型。

- 标准化前的 SQL 语句

```SQL
SELECT * FROM orders WHERE customer_id=10 AND quantity>20



SELECT * FROM orders WHERE customer_id = 20 AND quantity > 100
```

- 标准化后的 SQL 语句

```SQL
SELECT * FROM orders WHERE customer_id=? AND quantity>?
```