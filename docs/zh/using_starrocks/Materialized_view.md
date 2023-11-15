
# 物化视图

## 背景介绍

物化视图的一般定义是：它一种包含一个查询结果的数据库对象，它可以是远端数据的一份本地拷贝，也可以是一个表或一个 join 结果的行/列的一个子集，还可以是使用聚合函数的一个汇总。相对于普通的逻辑视图，将数据「物化」后，能够带来查询性能的提升。

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

具体的语法可以通过下面命令查看：

~~~SQL
HELP CREATE MATERIALIZED VIEW
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

更详细物化视图创建语法请参看SQL参考手册 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md) ，或者在 MySQL 客户端使用命令 `help create materialized view` 获得帮助。

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
DROP MATERIALIZED VIEW IF EXISTS store_amt on sales_records;
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

### 近似去重

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
ORDER BY k3;
~~~

这时候查询就会直接从刚才创建的 mv_1 物化视图中读取数据。物化视图对 k3 是存在前缀索引的，查询效率也会提升。

<br/>

## 注意事项

1. 物化视图的聚合函数的参数仅支持单列，比如：`sum(a+b)` 不支持。
2. 如果删除语句的条件列，在物化视图中不存在，则不能进行删除操作。如果一定要删除数据，则需要先将物化视图删除，然后方可删除数据。
3. 单表上过多的物化视图会影响导入的效率：导入数据时，物化视图和 base 表数据是同步更新的，如果一张表的物化视图表超过10张，则有可能导致导入速度很慢。这就像单次导入需要同时导入10张表数据是一样的。
4. 相同列，不同聚合函数，不能同时出现在一张物化视图中，比如：`select sum(a), min(a) from table` 不支持。
5. 物化视图的创建语句目前不支持 JOIN 和 WHERE ，也不支持 GROUP BY 的 HAVING 子句。
6. 目前聚合模型、更新模型和明细模型支持创建物化视图，主键模型不支持创建物化视图。
7. 创建物化视图是一个异步的操作。在执行完 CREATE MATERIALIZED VIEW 语句后，创建物化视图的任务即提交成功。只有等待上一个物化视图创建完成后（即`State`为`FINISHED`），才能提交下一个创建物化视图的任务。
