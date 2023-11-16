
# 物化视图

## 背景介绍

物化视图的一般定义是：一种包含一个查询结果的数据库对象，它可以是远端数据的一份本地拷贝，也可以是一个表或一个 join 结果的行/列的一个子集，还可以是使用聚合函数的一个汇总。相对于普通的逻辑视图，将数据「物化」后，能够带来查询性能的提升。

> 系统目前还不支持join，更多注意事项请参看 [注意事项](#注意事项)。

在本系统中，物化视图会被更多地用来当做一种预先计算的技术，同RollUp表，预先计算是为了减少查询时现场计算量，从而降低查询延迟。RollUp 表有两种使用方式：对明细表的任意维度组合进行预先聚合；采用新的维度列排序方式，以命中更多的前缀查询条件。当然也可以两种方式一起使用。物化视图的功能是 RollUp 表的超集，原有的 RollUp 功能都可通过物化视图来实现。

<br/>

物化视图的使用场景有：

* 分析需求覆盖明细数据查询以及固定维度聚合查询两方面。
* 需要做对排序键前缀之外的其他列组合形式做范围条件过滤。
* 需要对明细表的任意维度做粗粒度聚合分析。

<br/>

## 原理

物化视图的数据组织形式和基表、RollUp表相同。用户可以在新建的基表时添加物化视图，也可以对已有表添加物化视图，这种情况下，基表的数据会自动以**异步**方式填充到物化视图中。基表可以拥有多张物化视图，向基表导入数据时，会**同时**更新基表的所有物化视图。数据导入操作具有**原子性**，因此基表和它的物化视图保持数据一致。

物化视图创建成功后，用户的原有的查询基表的 SQL 语句保持不变，StarRocks 会自动选择一个最优的物化视图，从物化视图中读取数据并计算。用户可以通过 EXPLAIN 命令来检查当前查询是否使用了物化视图。

<br/>

物化视图中的聚合和查询中聚合的匹配关系：

| 物化视图聚合 | 查询中聚合 |
| :-: | :-: |
| sum | sum |
| min | min |
| max | max |
| count | count |
| bitmap_union | bitmap_union, bitmap_union_count, count(distinct) |
| hll_union | hll_raw_agg, hll_union_agg, ndv, approx_count_distinct |

其中 bitmap 和 hll 的聚合函数在查询匹配到物化视图后，查询的聚合算子会根据物化视图的表结构进行改写。

<br/>

## 使用方式

### 创建物化视图

通过下面命令就可以创建物化视图。创建物化视图是一个异步的操作，也就是说用户成功提交创建任务后，StarRocks 会在后台对存量的数据进行计算，直到创建成功。

~~~SQL
CREATE MATERIALIZED VIEW
~~~

假设用户有一张销售记录明细表，存储了每个交易的交易id、销售员、售卖门店、销售时间、以及金额。建表语句为：

~~~SQL
CREATE TABLE sales_records(
    record_id int,
    seller_id int,
    store_id int,
    sale_date date,
    sale_amt bigint
) distributed BY hash(record_id)
properties("replication_num" = "1");
~~~

表 sales_records 的结构为:

~~~PlainText
MySQL [test]> desc sales_records;

+-----------+--------+------+-------+---------+-------+
| Field     | Type   | Null | Key   | Default | Extra |
+-----------+--------+------+-------+---------+-------+
| record_id | INT    | Yes  | true  | NULL    |       |
| seller_id | INT    | Yes  | true  | NULL    |       |
| store_id  | INT    | Yes  | true  | NULL    |       |
| sale_date | DATE   | Yes  | false | NULL    | NONE  |
| sale_amt  | BIGINT | Yes  | false | NULL    | NONE  |
+-----------+--------+------+-------+---------+-------+
~~~

如果用户经常对不同门店的销售量做分析，则可以为 sales_records 表创建一张“以售卖门店为分组，对相同售卖门店的销售额求和”的物化视图。创建语句如下：

~~~sql
CREATE MATERIALIZED VIEW store_amt AS
SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
~~~

更详细物化视图创建语法请参看SQL参考手册 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md)。

<br/>

### 查看物化视图构建状态

由于创建物化视图是一个异步的操作，用户在提交完创建物化视图任务后，需要通过命令检查物化视图是否构建完成，命令如下:

~~~SQL
SHOW ALTER MATERIALIZED VIEW FROM db_name;
~~~

或

~~~SQL
SHOW ALTER TABLE ROLLUP FROM db_name;
~~~

> db_name：替换成真实的 db name，比如"test"。

查询结果为:

~~~PlainText
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime          | FinishedTime        | BaseIndexName | RollupIndexName | RollupId | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+----------+------+----------+---------+
| 22324 | sales_records | 2020-09-27 01:02:49 | 2020-09-27 01:03:13 | sales_records | store_amt       | 22325    | 672           | FINISHED |      | NULL     | 86400   |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+----------+------+----------+---------+
~~~

如果 State 为 FINISHED，说明物化视图已经创建完成。

### 查看已创建的物化视图

您可以通过基表名查看一个基表的所有物化视图。例如，运行如下命令查看`sales_records`的所有物化视图。

~~~PlainText
mysql> desc sales_records all;

+---------------+---------------+-----------+--------+------+-------+---------+-------+
| IndexName     | IndexKeysType | Field     | Type   | Null | Key   | Default | Extra |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
| sales_records | DUP_KEYS      | record_id | INT    | Yes  | true  | NULL    |       |
|               |               | seller_id | INT    | Yes  | true  | NULL    |       |
|               |               | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_date | DATE   | Yes  | false | NULL    | NONE  |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | NONE  |
|               |               |           |        |      |       |         |       |
| store_amt     | AGG_KEYS      | store_id  | INT    | Yes  | true  | NULL    |       |
|               |               | sale_amt  | BIGINT | Yes  | false | NULL    | SUM   |
+---------------+---------------+-----------+--------+------+-------+---------+-------+
~~~

<br/>

### 查询命中物化视图

当物化视图创建完成后，用户再查询不同门店的销售量时，就会直接从刚才创建的物化视图 store_amt 中读取聚合好的数据，达到提升查询效率的效果。

用户的查询依旧指定查询 sales_records 表，比如：

~~~SQL
SELECT store_id, SUM(sale_amt) FROM sales_records GROUP BY store_id;
~~~

使用 EXPLAIN 命令查询物化视图是否命中：

~~~SQL
EXPLAIN SELECT store_id, SUM(sale_amt) FROM sales_records GROUP BY store_id;
~~~

结果为:

~~~PlainText

| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:<slot 2> `store_id` | <slot 3> sum(`sale_amt`)                |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   4:EXCHANGE                                                                |
|      use vectorized: true                                                   |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: <slot 2> `store_id`                          |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 04                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   3:AGGREGATE (merge finalize)                                              |
|   |  output: sum(<slot 3> sum(`sale_amt`))                                  |
|   |  group by: <slot 2> `store_id`                                          |
|   |  use vectorized: true                                                   |
|   |                                                                         |
|   2:EXCHANGE                                                                |
|      use vectorized: true                                                   |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 02                                                         |
|     HASH_PARTITIONED: <slot 2> `store_id`                                   |
|                                                                             |
|   1:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(`sale_amt`)                                                |
|   |  group by: `store_id`                                                   |
|   |  use vectorized: true                                                   |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: sales_records                                                   |
|      PREAGGREGATION: ON                                                     |
|      partitions=1/1                                                         |
|      rollup: store_amt                                                      |
|      tabletRatio=10/10                                                      |
|      tabletList=22326,22328,22330,22332,22334,22336,22338,22340,22342,22344 |
|      cardinality=0                                                          |
|      avgRowSize=0.0                                                         |
|      numNodes=1                                                             |
|      use vectorized: true                                                   |
+-----------------------------------------------------------------------------+
~~~

查询计划树中的 OlapScanNode 显示 `PREAGGREGATION: ON` 和 `rollup: store_amt`，说明使用物化视图 store_amt 的预先聚合计算结果。也就是说查询已经命中到物化视图 store_amt，并直接从物化视图中读取数据了。

<br/>

### 删除物化视图

下列两种情形需要删除物化视图:

* 用户误操作创建物化视图，需要撤销该操作。
* 用户创建了大量的物化视图，导致数据导入速度过慢不满足业务需求，并且部分物化视图的相互重复，查询频率极低，可容忍较高的查询延迟，此时需要删除部分物化视图。

删除已经创建完成的物化视图:

~~~SQL
DROP MATERIALIZED VIEW IF EXISTS store_amt;
~~~

删除处于创建中的物化视图，需要先取消异步任务，然后再删除物化视图，以表 `db0.table0` 上的物化视图 mv 为例:

首先获得JobId，执行命令:

~~~SQL
show alter table rollup from db0;
~~~

结果为:

~~~PlainText
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+-------------+------+----------+---------+
| JobId | TableName     | CreateTime          | FinishedTime        | BaseIndexName | RollupIndexName | RollupId | TransactionId | State       | Msg  | Progress | Timeout |
| 22478 | table0        | 2020-09-27 01:46:42 | NULL                | table0        | mv              | 22479    | 676           | WAITING_TXN |      | NULL     | 86400   |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+-------------+------+----------+---------+
~~~

其中JobId为22478，取消该Job，执行命令:

~~~SQL
cancel alter table rollup from db0.table0 (22478);
~~~

<br/>

## 最佳实践

### 精确去重

用户可以在明细表上使用表达式`bitmap_union(to_bitmap(col))`创建物化视图，实现原来聚合表才支持的基于 bitmap 的预先计算的精确去重功能。

比如，用户有一张计算广告业务相关的明细表，每条记录包含的信息有点击日期、点击的是什么广告、通过什么渠道点击、以及点击的用户是谁：

~~~SQL
CREATE TABLE advertiser_view_record(
    TIME date,
    advertiser varchar(10),
    channel varchar(10),
    user_id int
) distributed BY hash(TIME)
properties("replication_num" = "1");
~~~

用户查询广告 UV，使用下面查询语句：

~~~SQL
SELECT advertiser, channel, count(distinct user_id)
FROM advertiser_view_record
GROUP BY advertiser, channel;
~~~

这种情况下，可以创建物化视图，使用 bitmap_union 预先聚合:

~~~SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, bitmap_union(to_bitmap(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
~~~

物化视图创建完毕后，查询语句中的`count(distinct user_id)`，会自动改写为`bitmap_union_count (to_bitmap(user_id))`以命中物化视图。

<br/>

以上文表 `advertiser_view_record` 为例，如果想在查询点击广告的 UV 时实现近似去重查询加速，可基于该明细表创建一张物化视图，并使用 [hll_union()](../sql-reference/sql-functions/aggregate-functions/hll_union.md) 函数预先聚合数据。

用户可以在明细表上使用表达式 `hll_union(hll_hash(col))` 创建物化视图，实现近似去重的预计算。

在同上一样的场景中，用户创建如下物化视图:

~~~SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, hll_union(hll_hash(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
~~~

<br/>

### 匹配更丰富的前缀索引

用户的基表 tableA 有 (k1, k2, k3) 三列。其中 k1, k2 为排序键。这时候如果用户查询条件中包含 `where k1=1 and k2=2`，就能通过 shortkey 索引加速查询。但是用户查询语句中使用条件 `k3=3`，则无法通过 shortkey 索引加速。此时，可创建以 k3 作为第一列的物化视图:

~~~SQL
CREATE MATERIALIZED VIEW mv_1 AS
SELECT k3, k2, k1
FROM tableA
~~~

这时候查询就会直接从刚才创建的 mv_1 物化视图中读取数据。物化视图对 k3 是存在前缀索引的，查询效率也会提升。

<br/>

## 注意事项

<<<<<<< HEAD
1. 物化视图的聚合函数的参数仅支持单列，比如：`sum(a+b)` 不支持。
2. 如果删除语句的条件列，在物化视图中不存在，则不能进行删除操作。如果一定要删除数据，则需要先将物化视图删除，然后方可删除数据。
3. 单表上过多的物化视图会影响导入的效率：导入数据时，物化视图和 base 表数据是同步更新的，如果一张表的物化视图表超过10张，则有可能导致导入速度很慢。这就像单次导入需要同时导入10张表数据是一样的。
4. 相同列，不同聚合函数，不能同时出现在一张物化视图中，比如：`select sum(a), min(a) from table` 不支持。
5. 物化视图的创建语句目前不支持 JOIN 和 WHERE ，也不支持 GROUP BY 的 HAVING 子句。
6. 目前聚合模型、更新模型和明细模型支持创建物化视图，主键模型不支持创建物化视图。
7. 创建物化视图是一个异步的操作。在执行完 CREATE MATERIALIZED VIEW 语句后，创建物化视图的任务即提交成功。只有等待上一个物化视图创建完成后（即`State`为`FINISHED`），才能提交下一个创建物化视图的任务。
8. 当前版本物化视图中使用聚合函数需要与 GROUP BY 语句一起使用，且 SELECT LIST 中至少包含一个分组列。
=======
您可以通过 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md) 语句为特定查询语句创建物化视图。

以下示例根据上述查询语句，基于表 `goods` 和表 `order_list` 创建一个“以订单 ID 为分组，对订单中所有商品价格求和”的异步物化视图，并设定其刷新方式为 ASYNC，每天自动刷新。

```SQL
CREATE MATERIALIZED VIEW order_mv
DISTRIBUTED BY HASH(`order_id`)
REFRESH ASYNC START('2022-09-01 10:00:00') EVERY (interval 1 day)
AS SELECT
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

> **说明**
>
> - 创建异步物化视图时必须至少指定分桶和刷新策略其中之一。
> - 您可以为异步物化视图设置与其基表不同的分区和分桶策略，但异步物化视图的分区列和分桶列必须在查询语句中。
> - 异步物化视图支持分区上卷。例如，基表基于天做分区方式，您可以设置异步物化视图按月做分区。
> - 异步物化视图暂不支持使用 List 分区策略，亦不支持基于使用 List 分区的基表创建。
> - 创建物化视图的查询语句不支持非确定性函数，其中包括 rand()、random()、uuid() 和 sleep()。
> - 异步物化视图支持多种数据类型。有关详细信息，请参阅 [CREATE MATERIALIZED VIEW - 支持数据类型](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md#支持数据类型)。
> - 默认情况下，执行 CREATE MATERIALIZED VIEW 语句后，StarRocks 将立即开始刷新任务，这将会占用一定系统资源。如需推迟刷新时间，请添加 REFRESH DEFERRED 参数。

- **异步物化视图刷新机制**

  目前，StarRocks 支持两种 ON DEMAND 刷新策略，即异步刷新（ASYNC）和手动刷新（MANUAL）。

  在此基础上，异步物化视图支持多种刷新机制控制刷新开销并保证刷新成功率：

  - 支持设置刷新最大分区数。当一张异步物化视图拥有较多分区时，单次刷新将耗费较多资源。您可以通过设置该刷新机制来指定单次刷新的最大分区数量，从而将刷新任务进行拆分，保证数据量多的物化视图能够分批、稳定的完成刷新。
  - 支持为异步物化视图的分区指定 Time to Live（TTL），从而减少异步物化视图占用的存储空间。
  - 支持指定刷新范围，只刷新最新的几个分区，减少刷新开销。
  - 支持设置数据变更不会触发对应物化视图自动刷新的基表。
  - 支持为刷新任务设置资源组。
  
  详细信息，请参阅 [CREATE MATERIALIZED VIEW - 参数](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md#参数) 中的 **PROPERTIES** 部分。您还可以使用 [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW.md) 修改现有异步物化视图的刷新机制。

  > **注意**
  >
  > 为避免全量刷新任务耗尽系统资源导致任务失败，建议您基于分区基表创建分区物化视图，保证基表分区中的数据更新时，只有物化视图对应的分区会被刷新，而非刷新整个物化视图。详细信息，请参考[使用物化视图进行数据建模 - 分区建模](./data_modeling_with_materialized_views.md#分区建模)。

- **嵌套物化视图**

  StarRocks v2.5 及以后版本支持嵌套异步物化视图，即基于异步物化视图构建新的异步物化视图。每个异步物化视图的刷新方式仅影响当前物化视图。当前 StarRocks 不对嵌套层数进行限制。生产环境中建议嵌套层数不超过三层。

- **External Catalog 物化视图**

  StarRocks 支持基于 Hive Catalog（自 v2.5 起）、Hudi Catalog（自 v2.5 起）、Iceberg Catalog（自 v2.5 起）构建异步物化视图。外部数据目录物化视图的创建方式与普通异步物化视图相同，但有使用限制。详细信息，请参阅 [使用物化视图加速数据湖查询](./data_lake_query_acceleration_with_materialized_views.md)。

## 手动刷新异步物化视图

您可以通过 [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH_MATERIALIZED_VIEW.md) 命令手动刷新指定异步物化视图。StarRocks v2.5 版本中，异步物化视图支持手动刷新部分分区。

```SQL
REFRESH MATERIALIZED VIEW order_mv;
```

您可以通过 [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL_REFRESH_MATERIALIZED_VIEW.md) 取消异步调用的刷新任务。

## 直接查询异步物化视图

异步物化视图本质上是一个物理表，其中存储了根据特定查询语句预先计算的完整结果集。在物化视图第一次刷新后，您即可直接查询物化视图。

```Plain
MySQL > SELECT * FROM order_mv;
+----------+--------------------+
| order_id | total              |
+----------+--------------------+
|    10001 |               14.5 |
|    10002 | 10.200000047683716 |
|    10003 |  8.700000047683716 |
+----------+--------------------+
3 rows in set (0.01 sec)
```

> **说明**
>
> 您可以直接查询异步物化视图，但由于异步刷新机制，其结果可能与您从基表上查询的结果不一致。

## 使用异步物化视图改写加速查询

StarRocks v2.5 版本支持 SPJG 类型的异步物化视图查询的自动透明改写。其查询改写包括单表改写，Join 改写，聚合改写，Union 改写和嵌套物化视图的改写。详细内容，请参考[物化视图查询改写](./query_rewrite_with_materialized_views.md)。

目前，StarRocks 支持基于 Default catalog、Hive catalog、Hudi catalog 和 Iceberg catalog 的异步物化视图的查询改写。当查询 Default catalog 数据时，StarRocks 通过排除数据与基表不一致的物化视图，来保证改写之后的查询与原始查询结果的强一致性。当物化视图数据过期时，不会作为候选物化视图。在查询外部目录数据时，由于 StarRocks 无法感知外部目录分区中的数据变化，因此不保证结果的强一致性。关于基于 External Catalog 的异步物化视图，请参考[使用物化视图加速数据湖查询](./data_lake_query_acceleration_with_materialized_views.md)。

## 管理异步物化视图

### 修改异步物化视图

您可以通过 [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW.md) 命令修改异步物化视图属性。

- 启用被禁用的异步物化视图（将物化视图的状态设置为 Active）。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv ACTIVE;
  ```

- 修改异步物化视图名称为 `order_total`。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv RENAME order_total;
  ```

- 修改异步物化视图的最大刷新间隔为 2 天。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv REFRESH ASYNC EVERY(INTERVAL 2 DAY);
  ```

### 查看异步物化视图

您可以使用 [SHOW MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md) 或查询 Information Schema 中的系统元数据表来查看数据库中的异步物化视图。

- 查看当前数据仓库内所有异步物化视图。

  ```SQL
  SHOW MATERIALIZED VIEW;
  ```

- 查看特定异步物化视图。

  ```SQL
  SHOW MATERIALIZED VIEW WHERE NAME = "order_mv";
  ```

- 通过名称匹配查看异步物化视图。

  ```SQL
  SHOW MATERIALIZED VIEW WHERE NAME LIKE "order%";
  ```

- 通过 Information Schema 中的系统元数据表 `materialized_views` 查看所有异步物化视图。详细内容，请参考 [information_schema.materialized_views](../administration/information_schema.md#materialized_views)。

  ```SQL
  SELECT * FROM information_schema.materialized_views;
  ```

### 查看异步物化视图创建语句

您可以通过 [SHOW CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_MATERIALIZED_VIEW.md) 命令查看异步物化视图创建语句。

```SQL
SHOW CREATE MATERIALIZED VIEW order_mv;
```

### 查看异步物化视图的执行状态

您可以通过查询 StarRocks 的 [Information Schema](../administration/information_schema.md) 中的 `tasks` 和 `task_runs` 元数据表来查看异步物化视图的执行（构建或刷新）状态。

以下示例查看最新创建的异步物化视图的执行状态：

1. 查看 `tasks` 表中最新任务的 `TASK_NAME`。

   ```Plain
   mysql> select * from information_schema.tasks  order by CREATE_TIME desc limit 1\G;
   *************************** 1. row ***************************
     TASK_NAME: mv-59299
   CREATE_TIME: 2022-12-12 17:33:51
      SCHEDULE: MANUAL
      DATABASE: ssb_1
    DEFINITION: insert overwrite hive_mv_lineorder_flat_1 SELECT `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_linenumber`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_custkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_partkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderpriority`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_ordtotalprice`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_revenue`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`p_mfgr`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`s_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_city`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate`
   FROM `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`
   WHERE `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate` = '1997-01-01'
   EXPIRE_TIME: NULL
   1 row in set (0.02 sec)
   ```

2. 基于查询到的 `TASK_NAME` 在表 `task_runs` 中查看执行状态。

   ```Plain
   mysql> select * from information_schema.task_runs where task_name='mv-59299' order by CREATE_TIME \G;
   *************************** 1. row ***************************
        QUERY_ID: d9cef11f-7a00-11ed-bd90-00163e14767f
       TASK_NAME: mv-59299
     CREATE_TIME: 2022-12-12 17:39:19
     FINISH_TIME: 2022-12-12 17:39:22
           STATE: SUCCESS
        DATABASE: ssb_1
      DEFINITION: insert overwrite hive_mv_lineorder_flat_1 SELECT `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_linenumber`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_custkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_partkey`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderpriority`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_ordtotalprice`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_revenue`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`p_mfgr`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`s_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_city`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`c_nation`, `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate`
   FROM `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`
   WHERE `hive_ci`.`dla_scan`.`lineorder_flat_1000_1000_orc`.`lo_orderdate` = '1997-01-01'
     EXPIRE_TIME: 2022-12-15 17:39:19
      ERROR_CODE: 0
   ERROR_MESSAGE: NULL
        PROGRESS: 100%
   2 rows in set (0.02 sec)
   ```

### 删除异步物化视图

您可以通过 [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP_MATERIALIZED_VIEW.md) 命令删除已创建的异步物化视图。

```SQL
DROP MATERIALIZED VIEW order_mv;
```

### 相关 Session 变量

以下变量控制物化视图的行为：

- `analyze_mv`：刷新后是否以及如何分析物化视图。有效值为空字符串（即不分析）、`sample`（抽样采集）或 `full`（全量采集）。默认为 `sample`。
- `enable_materialized_view_rewrite`：是否开启物化视图的自动改写。有效值为 `true`（自 2.5 版本起为默认值）和 `false`。
>>>>>>> b8eb50e58 ([Doc] link fixes to 2.5 (#35185))
