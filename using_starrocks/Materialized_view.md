# 物化视图

本文介绍如何在 StarRocks 中创建、使用以及管理物化视图。

## 背景介绍

StarRocks 中的物化视图是一种特殊的物理表，其中保存着基于基表 (base table) 的预计算查询结果。一方面，当您对基表进行相关复杂查询时，这些查询可以直接复用预计算结果，避免重复计算，进而提高查询效率。另一方面，您也可以通过物化视图对数据仓库进行建模，从而向上层应用提供统一的数据口径，屏蔽底层实现，或者保护基表明细数据安全。

### 相关概念

- **物化视图**

  物化视图可以分成两个部分：物化和视图。其中「物化」指通过额外的存储空间来缓存建模的中间结果，通过复用计算完成的物化结果来实现加速。而 「视图」是基于其他表构建的新表，其目的是建模。

- **基表**

  物化视图的驱动表。

- **查询重写**

  查询重写是指当对包含物化视图的基表进行查询时，系统会自动判断能否通过查询物化视图来得到结果。如果可以，则避免了聚合或连接操作，而直接从已经计算好的物化视图中读取数据。

- **刷新**

  刷新是指将基表和物化视图同步的过程。一般刷新可分为 ON DEMAND 和 ON COMMIT 两种方式，ON COMMIT 指每次基表更新都刷新物化视图，ON DEMAND 指按需刷新，物化视图通过手动刷新，或者内部定时刷新。

### 应用场景

物化视图可以服务以下应用场景：

- **查询加速**

  物化视图适用于加速可预测且重复的查询。通过物化视图，系统可以直接调用其中的预计算中间结果处理此类查询，降低大量复杂查询带来的负载压力，同时也大幅度缩短了查询处理时间。StarRocks 实现了基于物化视图的透明加速，并且保证直接查询源表的时候，结果一定基于最新数据。

- **数仓建模**

  通过物化视图功能，可以根据（多张）基表数据构建新表，从而实现以下目的：

  - **复用 SQL，统一语义**：向上层提供统一的数据口径，避免重复开发和重复计算。
  - **屏蔽复杂性**：向上层提供一个简单的界面，避免暴露底层实现。
  - **数据安全防护**：通过物化视图屏蔽基表的明细数据，保护基表数据安全。

### 使用案例

- 案例一：加速重复聚合查询

  假设您的数据仓库中存在大量包含相同聚合函数子查询的查询，占用了大量计算资源，您可以根据该子查询建立物化视图，计算并保存该子查询的所有结果。建立成功后，系统将自动改写查询语句，直接查询物化视图中的中间结果，从而降低负载，加速查询。

- 案例二：周期性多表关联

  假设您需要定期将数据仓库中多张表关联，生成一张新的宽表，您可以为这些表建立多表物化视图，并设定一个定期的异步刷新规则，从而避免手动调度关联任务。建立成功后，查询将直接基于物化视图返回结果，从而避免关联操作带来的延迟。

- 案例三：数仓分层

  假设您的基表中包含大量原始数据，查询需要进行复杂的 ETL 操作，您可以通过对数据建立多层物化视图实现数仓分层。如此可以将复杂查询分解为多层简单查询，既可以减少重复计算，又能够帮助维护人员快速定位问题。除此之外，数仓分层还可以将原始数据与统计数据解耦，从而保护敏感性原始数据。

## 使用单表同步刷新物化视图加速查询

StarRocks 支持为单张表构建同步刷新的物化视图。

如果您的数据仓库中存在大量复杂或重复的查询，您可以通过创建物化视图加速查询。

### 准备工作

创建物化视图前，您需要检查数据仓库是否需要通过物化视图加速查询。比如，您可以查看数据仓库中的查询是否重复使用特定子查询语句。

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

可以看到，此时查询时间为 0.02 秒，其 Query Profile 中的 `rollup` 项显示为 `sales_records`（即基表），说明该查询未使用物化视图。

### 创建物化视图

您可以通过 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md) 语句为特定查询语句创建物化视图。

以下示例根据上述查询语句，为表 `sales_records` 创建一个”以售卖门店为分组，对每一个售卖门店里的所有交易额求和”的物化视图。

```SQL
CREATE MATERIALIZED VIEW store_amt AS
SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
```

### 使用物化视图查询

新建的物化视图将预计算并保存上述查询的结果，后续查询将直接调用该结果以加速查询。创建成功后，您可以再次运行同样的查询以测试查询时间。

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

### 验证查询是否命中物化视图

您可以再次使用 EXPLAIN 命令查看该查询是否命中物化视图。

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

可以看到，此时 Query Profile 中的 `rollup` 项显示为 `store_amt`（即物化视图），说明该查询已命中物化视图。

### 查看物化视图构建状态

创建物化视图是一个异步的操作。CREATE MATERIALIZED VIEW 命令执行成功即代表创建物化视图的任务提交成功。您可以通过 [SHOW ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW%20ALTER%20MATERIALIZED%20VIEW.md) 命令查看当前数据库中物化视图的构建状态。

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

其中，`RollupIndexName` 为物化视图名称； `State` 项为 `FINISHED`，代表该物化视图构建完成。

### 查看物化视图的表结构

您可以通过 DESC tbl_name ALL 命令查看特定表和其下属所有物化视图的表结构。

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

### 删除物化视图

在以下三种情况下，您需要删除物化视图:

- 物化视图创建错误，需要删除正在创建中的物化视图。

- 创建了大量的物化视图，导致数据导入速度过慢，并且部分物化视图重复。

- 相关查询频率较低，且业务场景可容忍较高的查询延迟。

#### 删除正在创建的物化视图

可以通过取消正在进行的物化视图创建任务删除正在创建的物化视图。首先需要通过 [查看物化视图构建状态](#查看物化视图构建状态) 获取该物化视图的任务 ID `JobID`。得到任务 ID 后，需要通过 CANCEL ALTER 命令取消该创建任务。

```Plain
CANCEL ALTER TABLE ROLLUP FROM sales_records (12090);
```

#### 删除已创建的物化视图

可以通过 [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md) 命令删除已创建的物化视图。

```SQL
DROP MATERIALIZED VIEW store_amt;
```

### 最佳实践

#### 精确去重

以下示例基于一张广告业务相关的明细表 `advertiser_view_record`，其中记录了点击日期 `click_time`、广告代码 `advertiser`、点击渠道 `channel` 以及点击用户 ID `user_id`。

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

如需实现精确去重查询加速，您可以基于该明细表创建一张物化视图，并使用 bitmap_union() 函数预先聚合数据。

```SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, bitmap_union(to_bitmap(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

物化视图创建完成后，后续查询语句中的子查询 `count(distinct user_id)` 会被自动改写为 `bitmap_union_count (to_bitmap(user_id))` 以便查询命中物化视图。

#### 近似去重

以上文表 `advertiser_view_record` 为例，如果想在查询点击广告的 UV 时实现近似去重查询加速，可基于该明细表创建一张物化视图，并使用 [hll_union()](../sql-reference/sql-functions/aggregate-functions/hll_union.md) 函数预先聚合数据。

```SQL
CREATE MATERIALIZED VIEW advertiser_uv2 AS
SELECT advertiser, channel, hll_union(hll_hash(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

#### 增设前缀索引

假设基表 `tableA` 包含 `k1`、`k2` 和 `k3` 列，其中仅 `k1` 和 `k2` 为排序键。如果业务场景需要在查询语句中包括子查询 `where k3=x` 并通过前缀索引加速查询，那么您可以创建以 `k3` 为第一列的物化视图。

```SQL
CREATE MATERIALIZED VIEW k3_as_key AS
SELECT k3, k2, k1
FROM tableA
```

### 聚合函数匹配关系

使用物化视图搜索时，原始查询语句将会被自动改写并用于查询物化视图中保存的中间结果。下表展示了原始查询聚合函数和构建物化视图用到的聚合函数的匹配关系。您可以根据业务场景选择对应的聚合函数构建物化视图。

| **原始查询聚合函数**                                   | **物化视图构建聚合函数** |
| ------------------------------------------------------ | ------------------------ |
| sum                                                    | sum                      |
| min                                                    | min                      |
| max                                                    | max                      |
| count                                                  | count                    |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union             |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                |

### 注意事项

- 同步物化视图仅支持单列聚合函数，不支持形如 `sum(a+b)` 的查询语句。

- 同步物化视图创建语句不支持 JOIN、WHERE 等子句。

- 当前版本暂时不支持同时创建多个物化视图。仅当当前创建任务完成时，方可执行下一个创建任务。

- 一个物化视图仅支持对同一列数据使用一种聚合函数，不支持形如 `select sum(a), min(a) from table` 的查询语句。

- 使用 ALTER TABLE DROP COLUMN 删除基表中特定列时，需要保证该基表所有物化视图中都包含被删除列，否则无法进行删除操作。如果必须删除该列，则需要将所有未包含该列的物化视图删除，然后进行删除列操作。

- 为一张表创建过多的物化视图会影响导入的效率。导入数据时，物化视图和基表数据将同步更新，如果一张基表包含 n 个物化视图，向基表导入数据时，其导入效率大约等同于导入 n 张表，数据导入的速度会变慢。

- 当前版本物化视图中使用聚合函数需要与 GROUP BY 语句一起使用，且 SELECT LIST 中至少包含一个分组列。

## 使用多表异步刷新物化视图为数仓建模

2.4 版本中，StarRocks 进一步支持多表异步刷新物化视图，方便您通过创建物化视图的方式为数据仓库进行建模。

目前，StarRocks 多表物化视图支持以下刷新方式：

- **异步刷新**：这种刷新方式通过异步刷新任务实现物化视图数据的刷新，不保证物化视图与源表之间的数据强一致。

- **手动刷新**：这种刷新方式通过用户手动调用刷新命令，来实现物化视图的刷新，不保证物化视图与源表之间的数据强一致。

### 准备工作

#### 开启异步物化视图

使用异步物化视图前，您需要使用以下命令设置 FE 配置项 `enable_experimental_mv` 为 `true`：

```SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_mv"="true");
```

#### 创建基表

以下示例基于两张基表：

- 表 `goods` 包含产品 ID，产品名称，和产品价格。

- 表 `order_list` 包含订单 ID，客户 ID，和产品 ID。

其中 `item_id1` 与 `item_id2` 等价。

建表并导入如下数据：

```SQL
CREATE TABLE goods(
    item_id1          INT,
    item_name         STRING,
    price             FLOAT
) DISTRIBUTED BY HASH(item_id1);

INSERT INTO goods
VALUES
    (1001,"apple",6.5),
    (1002,"pear",8.0),
    (1003,"potato",2.2);

CREATE TABLE order_list(
    order_id          INT,
    client_id         INT,
    item_id2          INT,
    order_date        DATE
) DISTRIBUTED BY HASH(order_id);

INSERT INTO order_list
VALUES
    (10001,101,1001,"2022-03-13"),
    (10001,101,1002,"2022-03-13"),
    (10002,103,1002,"2022-03-13"),
    (10002,103,1003,"2022-03-14"),
    (10003,102,1003,"2022-03-14"),
    (10003,102,1001,"2022-03-14");
```

该示例业务场景需要频繁分析订单总额，则查询需要将两张表关联并调用 sum 函数，根据订单 ID 和总额生成一张新表。除此之外，该业务场景需要每天定时刷新订单总额表。其查询语句如下：

```SQL
SELECT
    order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

### 创建物化视图

您可以通过 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md) 语句为特定查询语句创建物化视图。

以下示例根据上述查询语句，基于表 `order_list` 和表 `goods` 创建一个“以订单 ID 为分组，对订单中所有商品价格求和”的物化视图，并设定其刷新方式为异步，以相隔一天的频率自动刷新。

```SQL
CREATE MATERIALIZED VIEW order_mv
DISTRIBUTED BY HASH(`order_id`) BUCKETS 12
REFRESH ASYNC START('2022-09-01 10:00:00') EVERY (interval 1 day)
AS SELECT
    order_list.order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

#### 关于多表异步物化视图刷新策略

StarRocks 2.5 版本中，多表异步刷新物化视图支持多种异步刷新机制。您可以在创建物化视图时添加下列属性（PROPERTIES）以赋予物化视图不同的刷新机制，或通过 ALTER MATERIALIZED VIEW 语句修改已有物化视图的属性。

| **属性**                      | **默认值** | **描述**                                                     |
| ----------------------------- | ---------- | ------------------------------------------------------------ |
| partition_ttl_number          | -1         | 需要保留的最近的物化视图分区数量。分区数量超过该值后，过期分区将被删除。StarRocks 将根据 FE 配置项 `dynamic_partition_check_interval_seconds` 中的时间间隔定期检查物化视图分区，并自动删除过期分区。当值为 `-1` 时，将保留物化视图所有分区。 |
| partition_refresh_number      | -1         | 单次刷新中，最多刷新的分区数量。如果需要刷新的分区数量超过该值，StarRocks 将拆分这次刷新任务，并分批完成。仅当前一批分区刷新成功时，StarRocks 会继续刷新下一批分区，直至所有分区刷新完成。如果其中有分区刷新失败，将不会产生后续的刷新任务。当值为 `-1` 时，将不会拆分刷新任务。 |
| excluded_trigger_tables       | 空字符串   | 在此项属性中列出的基表，其数据产生变化时不会触发对应物化视图自动刷新。该参数仅针对导入触发式刷新，通常需要与属性 `auto_refresh_partitions_limit` 搭配使用。形式：`[db_name.]table_name`。当值为空字符串时，任意的基表数据变化都将触发对应物化视图刷新。 |
| auto_refresh_partitions_limit | -1         | 当触发物化视图刷新时，需要刷新的最近的物化视图分区数量。您可以通过该属性限制刷新的范围，降低刷新代价，但因为仅有部分分区刷新，有可能导致物化视图数据与基表无法保持一致。当值为 `-1` 时，将刷新所有分区。 |

#### 关于嵌套物化视图

StarRocks 2.5 版本中，多表异步刷新物化视图支持嵌套物化视图，即基于物化视图构建物化视图。每个物化视图的刷新方式仅影响当前物化视图。当前 StarRocks 不对嵌套层数进行限制。生产环境中建议嵌套层数不超过三层。

#### 关于外部数据目录物化视图

StarRocks 2.5 版本中，多表异步刷新物化视图支持基于 Hive catalog、Hudi catalog 以及 Iceberg catalog 构建物化视图。外部数据目录物化视图的创建方式与异步刷新物化视图相同，但有以下使用限制：

- 外部数据目录物化视图仅支持异步定时刷新和手动刷新。

- 物化视图中的数据不保证与外部数据目录的数据强一致。

- 目前暂不支持基于资源（Resource）构建物化视图。

- StarRocks 目前无法感知外部数据目录基表数据是否发生变动，所以每次刷新会默认刷新所有分区。您可以通过手动刷新方式指定刷新部分分区。

### 使用物化视图查询

新建的物化视图将预计算并保存上述查询的结果。创建成功后，**您可以直接查询异步物化视图**。

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

### （可选）使用多表物化视图查询改写

StarRocks 2.5 版本中，多表异步刷新物化视图支持 SPJG类型的物化视图查询的自动透明改写。SPJG 类型的物化视图是指在物化视图 Plan 中只包含 Scan、Filter、Project 以及 Aggregate 类型的算子。其查询改写包括单表改写，Join 改写，聚合改写，Union 改写和嵌套物化视图的改写。

当查询内部表数据时，StarRocks 通过排除数据与基表不一致的物化视图，来保证改写之后的查询与原始查询结果的强一致性。当物化视图数据过期的时候，不会作为候选的物化视图。

> **注意**
>
> StarRocks 当前暂不支持基于外部数据目录物化视图的查询改写。

#### 候选物化视图

查询改写时，StarRocks 会从众多的物化视图中粗选出可能符合改写条件的候选物化视图，排除不符合条件的物化视图，以降低改写的代价。

候选物化视图需满足以下条件：

1. 物化视图状态为 active。
2. 物化视图的基表和查询中涉及的表必须有交集。
3. 如果是非分区物化视图，物化视图数据必须为最新的才能作为候选。
4. 如果是分区物化视图，物化视图的部分分区为最新的才能作为候选。
5. 物化视图必须只包含 Select、Filter、Join、Projection 以及 Aggregate 类型算子。
6. 嵌套物化视图如果符合条件 1、3 以及 4，也会作为候选。

#### 启用多表物化视图查询改写

StarRocks 默认开启物化视图查询改写。您可以通过 Session 变量 `enable_materialized_view_rewrite` 开启或关闭该功能。

```SQL
SET GLOBAL enable_materialized_view_rewrite = { true | false };
```

#### 设置多表物化视图查询改写

您可以通过以下 Session 变量设置多表物化视图查询改写。

| **变量**                                    | **默认值** | **描述**                                                     |
| ------------------------------------------- | ---------- | ------------------------------------------------------------ |
| enable_materialized_view_union_rewrite      | true       | 是否开启物化视图 Union 改写。                                |
| enable_rule_based_materialized_view_rewrite | true       | 是否开启基于规则的物化视图查询改写功能，主要用于处理单表查询改写。 |
| nested_mv_rewrite_max_level                 | 3          | 可用于查询改写的嵌套物化视图的最大层数。类型：INT。取值范围：[1, +∞)。取值为 `1` 表示只可使用基于基表创建的物化视图用于查询改写。 |

#### 验证查询改写是否生效

您可以使用 EXPLAIN 语句查看对应 Query Plan。如果其中 `OlapScanNode` 项目下的 `TABLE` 为对应物化视图名称，则表示该查询已基于物化视图改写。

```Plain
mysql> EXPLAIN SELECT order_id, sum(goods.price) as total FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2 GROUP BY order_id;
+------------------------------------+
| Explain String                     |
+------------------------------------+
| PLAN FRAGMENT 0                    |
|  OUTPUT EXPRS:1: order_id | 8: sum |
|   PARTITION: RANDOM                |
|                                    |
|   RESULT SINK                      |
|                                    |
|   1:Project                        |
|   |  <slot 1> : 9: order_id        |
|   |  <slot 8> : 10: total          |
|   |                                |
|   0:OlapScanNode                   |
|      TABLE: order_mv               |
|      PREAGGREGATION: ON            |
|      partitions=1/1                |
|      rollup: order_mv              |
|      tabletRatio=0/12              |
|      tabletList=                   |
|      cardinality=3                 |
|      avgRowSize=4.0                |
|      numNodes=0                    |
+------------------------------------+
20 rows in set (0.01 sec)
```

### 修改物化视图名称

您可以通过 ALTER MATERIALIZED VIEW 命令修改物化视图名称。

```SQL
ALTER MATERIALIZED VIEW order_mv RENAME order_total;
```

### 修改物化视图刷新方式

您可以通过 ALTER MATERIALIZED VIEW 命令修改异步物化视图的刷新方式。

```SQL
ALTER MATERIALIZED VIEW order_mv REFRESH ASYNC EVERY(INTERVAL 2 DAY);
```

### 查看物化视图

您可以通过以下方式查看数据仓库内的物化视图：

- 查看当前数据仓库内所有物化视图。

```SQL
SHOW MATERIALIZED VIEW;
```

- 查看特定物化视图。

```SQL
SHOW MATERIALIZED VIEW WHERE NAME = order_mv;
```

- 通过名称匹配查看物化视图。

```SQL
SHOW MATERIALIZED VIEW WHERE NAME LIKE "order%";
```

- 通过 `information_schema` 查看所有物化视图。

```SQL
SELECT * FROM information_schema.materialized_views;
```

### 查看物化视图创建语句

您可以通过 SHOW CREATE MATERIALIZED VIEW 命令查看物化视图创建语句。

```SQL
SHOW CREATE MATERIALIZED VIEW order_mv;
```

### 手动刷新物化视图

您可以通过 [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md) 命令手动刷新特定物化视图。StarRocks 2.5 版本中，多表异步刷新物化视图支持手动刷新部分分区。

```SQL
REFRESH MATERIALIZED VIEW order_mv;
```

> **注意**
>
> 您可以对异步刷新和手动刷新方式的物化视图手动调用物化视图，但不能通过该命令手动刷新单表同步刷新方式的物化视图。

您可以通过 [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL%20REFRESH%20MATERIALIZED%20VIEW.md) 命令取消异步或手动刷新物化视图的刷新任务。

### 查看多表物化视图的执行状态

您可以通过以下方式查看数据仓库内多表物化视图的执行状态。

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM INFORMATION_SCHEMA.task_runs;
```

> **说明**
>
> 异步刷新的物化视图依赖 Task 框架实现数据刷新，所以您可以通过查询 Task 框架提供的 `tasks` 和 `task_runs` 两张元数据表查看刷新任务。

### 删除物化视图

您可以通过 [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md) 命令删除已创建的物化视图。

```SQL
DROP MATERIALIZED VIEW order_mv;
```

### 注意事项

- 异步刷新物化视图有如下特性：
  - 您可以直接查询异步刷新物化视图，但结果可能与源表不一致。
  - 您可以为异步刷新物化视图设定与基表不同的分区方式和分桶方式。
  - 异步刷新物化视图支持分区上卷。例如，基表基于天做分区方式，您可以设置物化视图按月做分区。
- 在异步刷新和手动刷新方式下，您可以基于多张表构建物化视图。
- 异步刷新和手动刷新的物化视图的分区列和分桶列必须在查询语句中。
- 查询语句不支持非确定性函数，其中包括 rand()、random()、uuid() 和 sleep()。
