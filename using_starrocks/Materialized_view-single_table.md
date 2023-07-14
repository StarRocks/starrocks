# 同步物化视图

本文介绍如何在 StarRocks 中创建、使用以及管理**同步物化视图（Rollup）**。

同步物化视图下，所有对于基表的数据变更都会自动同步更新到物化视图中。您无需手动调用刷新命令，即可实现自动同步刷新物化视图。同步物化视图的管理成本和更新成本都比较低，适合实时场景下单表聚合查询的透明加速。

StarRocks 中的同步物化视图仅能基于 [Default Catalog](../data_source/catalog/default_catalog.md) 中的单个基表创建，是一种特殊的查询加速索引。

自 2.4 版本起，StarRocks 支持**异步物化视图**，可以基于多个基表创建，且支持更丰富的聚合函数。详细信息，请参阅 [异步物化视图](../using_starrocks/Materialized_view.md)。

下表从支持的特性角度比较了 StarRocks 2.5、2.4 中的异步物化视图以及同步物化视图（Rollup）：

|                              | **单表聚合** | **多表关联** | **查询改写** | **刷新策略** | **基表** |
| ---------------------------- | ----------- | ---------- | ----------- | ---------- | -------- |
| **异步物化视图** | 是 | 是 | 是 | <ul><li>异步定时刷新</li><li>手动刷新</li></ul> | 支持多表构建。基表可以来自：<ul><li>Default Catalog</li><li>External Catalog（v2.5）</li><li>已有异步物化视图（v2.5）</li><li>已有视图（v3.1）</li></ul> |
| **同步物化视图（Rollup）** | 仅部分聚合函数 | 否 | 是 | 导入同步刷新 | 仅支持基于 Default Catalog 的单表构建 |

## 相关概念

- **基表（Base Table）**

  物化视图的驱动表。

  对于 StarRocks 的同步物化视图，基表仅可以是 [Default catalog](../data_source/catalog/default_catalog.md) 中的单个内部表。StarRocks 支持在明细模型 (Duplicate Key type)、聚合模型 (Aggregate Key type) 和更新模型 (Unique Key type) 上创建同步物化视图。

- **刷新（Refresh）**

  StarRocks 同步物化视图中的数据将在数据导入基表时自动更新，无需手动调用刷新命令。

- **查询改写（Query Rewrite）**

  查询改写是指在对已构建了物化视图的基表进行查询时，系统自动判断是否可以复用物化视图中的预计算结果处理查询。如果可以复用，系统会直接从相关的物化视图读取预计算结果，以避免重复计算消耗系统资源和时间。

  StarRocks 的同步物化视图支持部分聚合算子的查询改写。详细信息，请参见 [聚合函数匹配关系](#聚合函数匹配关系)。

## 准备工作

创建同步物化视图前，您需要检查数据仓库是否需要通过同步物化视图加速查询。比如，您可以查看数据仓库中的查询是否重复使用特定子查询语句。

以下示例基于表 `sales_records`，其中包含每笔交易的交易 ID `record_id`、销售员 `seller_id`、售卖门店 `store_id`、销售时间 `sale_date` 以及销售额 `sale_amt`。建表并导入如下数据：

```SQL
CREATE TABLE sales_records(
    record_id INT,
    seller_id INT,
    store_id INT,
    sale_date DATE,
    sale_amt BIGINT
) DISTRIBUTED BY HASH(record_id);

INSERT INTO sales_records
VALUES
    (001,01,1,"2022-03-13",8573),
    (002,02,2,"2022-03-14",6948),
    (003,01,1,"2022-03-14",4319),
    (004,03,3,"2022-03-15",8734),
    (005,03,3,"2022-03-16",4212),
    (006,02,2,"2022-03-17",9515);
```

该示例业务场景需要频繁分析不同门店的销售额，则查询需要大量调用 sum() 函数，耗费大量系统资源。您可以运行该查询记录查询消耗时间，并使用 EXPLAIN 命令查看此查询的 Query Profile。

```Plain
MySQL > SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
+----------+-----------------+
| store_id | sum(`sale_amt`) |
+----------+-----------------+
|        2 |           16463 |
|        3 |           12946 |
|        1 |           12892 |
+----------+-----------------+
3 rows in set (0.02 sec)

MySQL > EXPLAIN SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
+-----------------------------------------------------------------------------+
| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:3: store_id | 6: sum                                          |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   4:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: 3: store_id                                  |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 04                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   3:AGGREGATE (merge finalize)                                              |
|   |  output: sum(6: sum)                                                    |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   2:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 02                                                         |
|     HASH_PARTITIONED: 3: store_id                                           |
|                                                                             |
|   1:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(5: sale_amt)                                               |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: sales_records                                                   |
|      PREAGGREGATION: ON                                                     |
|      partitions=1/1                                                         |
|      rollup: sales_records                                                  |
|      tabletRatio=10/10                                                      |
|      tabletList=12049,12053,12057,12061,12065,12069,12073,12077,12081,12085 |
|      cardinality=1                                                          |
|      avgRowSize=2.0                                                         |
|      numNodes=0                                                             |
+-----------------------------------------------------------------------------+
45 rows in set (0.00 sec)
```

可以看到，此时查询时间为 0.02 秒，其 Query Profile 中的 `rollup` 项显示为 `sales_records`（即基表），说明该查询未使用物化视图加速。

## 创建同步物化视图

您可以通过 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md) 语句为特定查询语句创建物化视图。

以下示例根据上述查询语句，为表 `sales_records` 创建一个”以售卖门店为分组，对每一个售卖门店里的所有交易额求和”的同步物化视图。

```SQL
CREATE MATERIALIZED VIEW store_amt AS
SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
```

> **注意**
>
> - 在同步物化视图中使用聚合函数时，查询语句必须使用 GROUP BY 语句，且 SELECT LIST 中至少包含一个分组列。
> - 同步物化视图不支持对多列数据使用单个聚合函数，不支持形如 `sum(a+b)` 的查询语句。
> - 同步物化视图不支持对同列数据使用多个聚合函数，不支持形如 `select sum(a), min(a) from table` 的查询语句。
> - 同步物化视图创建语句不支持 JOIN、WHERE 子句。
> - 使用 ALTER TABLE DROP COLUMN 删除基表中特定列时，需要保证该基表所有同步物化视图中都不包含被删除列，否则无法进行删除操作。如需删除该列，则需要将所有包含该列的同步物化视图删除，然后删除该列。
> - 为一张表创建过多的同步物化视图会影响导入的效率。导入数据时，同步物化视图和基表数据将同步更新，如果一张基表包含 `n` 个同步物化视图，向基表导入数据时，其导入效率大约等同于导入 `n` 张表，数据导入的速度会变慢。
> - 当前不支持同时创建多个同步物化视图。仅当当前创建任务完成时，方可执行下一个创建任务。

## 查看同步物化视图构建状态

创建同步物化视图是一个异步的操作。CREATE MATERIALIZED VIEW 命令执行成功即代表创建同步物化视图的任务提交成功。您可以通过 [SHOW ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW%20ALTER%20MATERIALIZED%20VIEW.md) 命令查看当前数据库中同步物化视图的构建状态。

```Plain
MySQL > SHOW ALTER MATERIALIZED VIEW\G
*************************** 1. row ***************************
          JobId: 12090
      TableName: sales_records
     CreateTime: 2022-08-25 19:41:10
   FinishedTime: 2022-08-25 19:41:39
  BaseIndexName: sales_records
RollupIndexName: store_amt
       RollupId: 12091
  TransactionId: 10
          State: FINISHED
            Msg: 
       Progress: NULL
        Timeout: 86400
1 row in set (0.00 sec)
```

其中，`RollupIndexName` 为同步物化视图名称，`State` 项为 `FINISHED`，代表该同步物化视图构建完成。

## 查询同步物化视图

因为同步物化视图本质上是基表的索引而不是物理表，所以您只能使用 Hint `[_SYNC_MV_]` 查询同步物化视图：

```SQL
-- 请勿省略 Hint 中的括号[]。
SELECT * FROM <mv_name> [_SYNC_MV_];
```

> **注意**
>
> 目前，StarRocks 会自动为同步物化视图中的列生成名称。您为同步物化视图中的列指定的 Alias 将无法生效。

## 使用同步物化视图查询

新建的同步物化视图将预计算并保存上述查询的结果，后续查询将直接调用该结果以加速查询。创建成功后，您可以再次运行同样的查询以测试查询时间。

```Plain
MySQL > SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
+----------+-----------------+
| store_id | sum(`sale_amt`) |
+----------+-----------------+
|        2 |           16463 |
|        3 |           12946 |
|        1 |           12892 |
+----------+-----------------+
3 rows in set (0.01 sec)
```

可以看到，此时查询时间已经缩短为 0.01 秒。

## 验证查询是否命中同步物化视图

您可以再次使用 EXPLAIN 命令查看该查询是否命中同步物化视图。

```Plain
MySQL > EXPLAIN SELECT store_id, SUM(sale_amt) FROM sales_records GROUP BY store_id;
+-----------------------------------------------------------------------------+
| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:3: store_id | 6: sum                                          |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   4:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: 3: store_id                                  |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 04                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   3:AGGREGATE (merge finalize)                                              |
|   |  output: sum(6: sum)                                                    |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   2:EXCHANGE                                                                |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 02                                                         |
|     HASH_PARTITIONED: 3: store_id                                           |
|                                                                             |
|   1:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(5: sale_amt)                                               |
|   |  group by: 3: store_id                                                  |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: sales_records                                                   |
|      PREAGGREGATION: ON                                                     |
|      partitions=1/1                                                         |
|      rollup: store_amt                                                      |
|      tabletRatio=10/10                                                      |
|      tabletList=12092,12096,12100,12104,12108,12112,12116,12120,12124,12128 |
|      cardinality=6                                                          |
|      avgRowSize=2.0                                                         |
|      numNodes=0                                                             |
+-----------------------------------------------------------------------------+
45 rows in set (0.00 sec)
```

可以看到，此时 Query Profile 中的 `rollup` 项显示为 `store_amt`（即同步物化视图），说明该查询已命中同步物化视图。

## 查看同步物化视图的表结构

您可以通过 DESC \<tbl_name\> ALL 命令查看特定表的表结构和其下属所有同步物化视图。

```Plain
MySQL > DESC sales_records ALL;
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
8 rows in set (0.00 sec)
```

## 删除同步物化视图

在以下三种情况下，您需要删除同步物化视图:

- 同步物化视图创建错误，需要删除正在创建中的同步物化视图。
- 创建了大量的同步物化视图，导致数据导入速度过慢，并且部分同步物化视图重复。
- 相关查询频率较低，且业务场景可容忍较高的查询延迟。

### 删除正在创建的同步物化视图

可以通过取消正在进行的同步物化视图创建任务删除正在创建的同步物化视图。首先需要通过 [查看同步物化视图构建状态](#查看同步物化视图构建状态) 获取该同步物化视图的任务 ID `JobID`。得到任务 ID 后，需要通过 CANCEL ALTER 命令取消该创建任务。

```SQL
CANCEL ALTER TABLE ROLLUP FROM sales_records (12090);
```

### 删除已创建的同步物化视图

可以通过 [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md) 命令删除已创建的同步物化视图。

```SQL
DROP MATERIALIZED VIEW store_amt;
```

## 最佳实践

### 精确去重

以下示例基于一张广告业务相关的明细表 `advertiser_view_record`，其中记录了点击日期 `click_time`、广告客户 `advertiser`、点击渠道 `channel` 以及点击用户 ID `user_id`。

```SQL
CREATE TABLE advertiser_view_record(
    click_time DATE,
    advertiser VARCHAR(10),
    channel VARCHAR(10),
    user_id INT
) distributed BY hash(click_time);
```

该场景需要频繁使用如下语句查询点击广告的 UV。

```SQL
SELECT advertiser, channel, count(distinct user_id)
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

如需实现精确去重查询加速，您可以基于该明细表创建一张同步物化视图，并使用 bitmap_union() 函数预先聚合数据。

```SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, bitmap_union(to_bitmap(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

同步物化视图创建完成后，后续查询语句中的子查询 `count(distinct user_id)` 会被自动改写为 `bitmap_union_count (to_bitmap(user_id))` 以便查询命中物化视图。

### 近似去重

以上文表 `advertiser_view_record` 为例，如果想在查询点击广告的 UV 时实现近似去重查询加速，可基于该明细表创建一张同步物化视图，并使用 [hll_union()](../sql-reference/sql-functions/aggregate-functions/hll_union.md) 函数预先聚合数据。

```SQL
CREATE MATERIALIZED VIEW advertiser_uv2 AS
SELECT advertiser, channel, hll_union(hll_hash(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

### 增设前缀索引

假设基表 `tableA` 包含 `k1`、`k2` 和 `k3` 列，其中仅 `k1` 和 `k2` 为排序键。如果业务场景需要在查询语句中包括子查询 `where k3=x` 并通过前缀索引加速查询，那么您可以创建以 `k3` 为第一列的同步物化视图。

```SQL
CREATE MATERIALIZED VIEW k3_as_key AS
SELECT k3, k2, k1
FROM tableA
```

## 聚合函数匹配关系

使用同步物化视图查询时，原始查询语句将会被自动改写并用于查询同步物化视图中保存的中间结果。下表展示了原始查询聚合函数和构建同步物化视图用到的聚合函数的匹配关系。您可以根据业务场景选择对应的聚合函数构建同步物化视图。

| **原始查询聚合函数**                                   | **物化视图构建聚合函数** |
| ------------------------------------------------------ | ------------------------ |
| sum                                                    | sum                      |
| min                                                    | min                      |
| max                                                    | max                      |
| count                                                  | count                    |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union             |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                |
