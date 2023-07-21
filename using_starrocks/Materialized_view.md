# 异步物化视图

本文介绍如何理解、创建、使用和管理 StarRocks 中的异步物化视图。StarRocks 自 2.4 版本起支持异步物化视图。

相较于同步物化视图，异步物化视图支持多表关联以及更加丰富的聚合算子。异步物化视图可以通过手动调用或定时任务的方式刷新，并且支持刷新部分分区，可以大幅降低刷新成本。除此之外，异步物化视图支持多种查询改写场景，实现自动、透明查询加速。

有关同步物化视图（Rollup）的场景和使用，参见[同步物化视图（Rollup）](../using_starrocks/Materialized_view-single_table.md)。

## 背景介绍

数据仓库环境中的应用程序经常基于多个大表执行复杂查询，通常涉及多表之间数十亿行数据的关联和聚合。处理此类查询通常会大量消耗系统资源和时间，造成极高的查询成本。

您可以通过 StarRocks 中的异步物化视图解决以上问题。异步物化视图是一种特殊的物理表，其中存储了基于基表特定查询语句的预计算结果。当您对基表执行复杂查询时，StarRocks 可以直接复用预计算结果，避免重复计算，进而提高查询性能。查询的频率越高或查询语句越复杂，性能增益就会越很明显。

您还可以通过异步物化视图对数据仓库进行建模，从而向上层应用提供统一的数据口径，屏蔽底层实现，保护基表明细数据安全。

### 理解 StarRocks 物化视图

StarRocks 2.4 之前的版本提供了一种同步更新的同步物化视图（Rollup），可以提供更好的数据新鲜度和更低的刷新成本。但是同步物化视图在场景上有诸多限制，只可基于单一基表创建，且仅支持有限的聚合算子。2.4 版本之后支持异步物化视图，可以基于多个基表创建，且支持更丰富的聚合算子。

下表从支持的特性角度比较了 StarRocks 中的异步物化视图以及同步物化视图（Rollup）：

|                              | **单表聚合** | **多表关联** | **查询改写** | **刷新策略** | **基表** |
| ---------------------------- | ----------- | ---------- | ----------- | ---------- | -------- |
| **异步物化视图** | 是 | 是 | 是 | <ul><li>异步定时刷新</li><li>手动刷新</li></ul> | 支持多表构建。基表可以来自：<ul><li>Default Catalog</li><li>External Catalog（v2.5）</li><li>已有异步物化视图（v2.5）</li><li>已有视图（v3.1）</li></ul> |
| **同步物化视图（Rollup）** | 仅部分聚合函数 | 否 | 是 | 导入同步刷新 | 仅支持基于 Default Catalog 的单表构建 |

### 相关概念

- **基表（Base Table）**

  物化视图的驱动表。

  对于 StarRocks 的异步物化视图，基表可以是 [Default catalog](../data_source/catalog/default_catalog.md) 中的内部表、外部数据目录中的表（自 2.5 版本起支持），甚至是已有的异步物化视图（自 2.5 版本起支持）或视图（自 3.1 版本起支持）。StarRocks 支持在所有 [StarRocks 表类型](../table_design/table_types/table_types.md) 上创建异步物化视图。

- **刷新（Refresh）**

  创建异步物化视图后，其中的数据仅反映创建时刻基表的状态。当基表中的数据发生变化时，需要通过刷新异步物化视图更新数据变化。

  目前 StarRocks 支持两种异步刷新策略：ASYNC（任务定时触发刷新）和 MANUAL（用户手动触发刷新）。

- **查询改写（Query Rewrite）**

  查询改写是指在对已构建了物化视图的基表进行查询时，系统自动判断是否可以复用物化视图中的预计算结果处理查询。如果可以复用，系统会直接从相关的物化视图读取预计算结果，以避免重复计算消耗系统资源和时间。

  自 2.5 版本起，StarRocks 支持基于在 Default Catalog 或 External Catalog（如 Hive catalog、Hudi catalog 或 Iceberg catalog）上创建的 SPJG 类型异步物化视图的自动、透明查询改写。

## 使用场景

如果您的数据仓库环境中有以下需求，我们建议您创建异步物化视图：

- **加速重复聚合查询**

  假设您的数仓环境中存在大量包含相同聚合函数子查询的查询，占用了大量计算资源，您可以根据该子查询建立异步物化视图，计算并保存该子查询的所有结果。建立成功后，系统将自动改写查询语句，直接查询异步物化视图中的中间结果，从而降低负载，加速查询。

- **周期性多表关联查询**

  假设您需要定期将数据仓库中多张表关联，生成一张新的宽表，您可以为这些表建立异步物化视图，并设定定期刷新规则，从而避免手动调度关联任务。异步物化视图建立成功后，查询将直接基于异步物化视图返回结果，从而避免关联操作带来的延迟。

- **数仓分层**

  假设您的基表中包含大量原始数据，查询需要进行复杂的 ETL 操作，您可以通过对数据建立多层异步物化视图实现数仓分层。如此可以将复杂查询分解为多层简单查询，既可以减少重复计算，又能够帮助维护人员快速定位问题。除此之外，数仓分层还可以将原始数据与统计数据解耦，从而保护敏感性原始数据。

## 创建异步物化视图

StarRocks 支持在以下数据源创建异步物化视图：

- StarRocks 内部表。基表支持所有 StarRocks 表类型。
- Hive Catalog、Hudi Catalog 和 Iceberg  Catalog 中的表。
- 已有异步物化视图。
- 已有视图。

### 准备工作

#### 创建基表

以下示例基于两张基表：

- 表 `goods` 包含产品 ID `item_id1`、产品名称 `item_name` 和产品价格 `price`。
- 表 `order_list` 包含订单 ID `order_id`、客户 ID `client_id` 和产品 ID `item_id2`。

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

该示例业务场景需要频繁分析订单总额，则查询需要将两张表关联并调用 sum() 函数，根据订单 ID 和总额生成一张新表。除此之外，该业务场景需要每天定时刷新订单总额表。

其查询语句如下：

```SQL
SELECT
    order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

### 基于查询语句创建异步物化视图

您可以通过 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md) 语句为特定查询语句创建物化视图。

以下示例根据上述查询语句，基于表 `goods` 和表 `order_list` 创建一个“以订单 ID 为分组，对订单中所有商品价格求和”的异步物化视图，并设定其刷新方式为异步，以相隔一天的频率自动刷新。

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
> - 创建异步物化视图时必须指定分桶策略。
> - 您可以为异步物化视图设置与其基表不同的分区和分桶策略。
> - 异步物化视图支持分区上卷。例如，基表基于天做分区方式，您可以设置异步物化视图按月做分区。
> - 异步物化视图的分区列和分桶列必须在查询语句中。
> - 创建物化视图的查询语句不支持非确定性函数，其中包括 rand()、random()、uuid() 和 sleep()。
> - 异步物化视图支持多种数据类型。有关详细信息，请参阅 [CREATE MATERIALIZED VIEW - 支持数据类型](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md#支持数据类型)。

- **异步物化视图刷新机制**

  目前，StarRocks 支持两种 ON DEMAND 刷新策略，即异步定时刷新和手动刷新。

  StarRocks 2.5 版本中，异步物化视图新增支持多种刷新机制：

  - 新增刷新最大分区数。当一张异步物化视图拥有较多分区时，单次刷新将耗费较多资源。您可以通过设置该刷新机制来指定单次刷新的最大分区数量，从而将刷新任务进行拆分，保证数据量多的物化视图能够分批、稳定的完成刷新。
  - 您可以为异步物化视图的分区指定 time to live（TTL），从而减少异步物化视图占用的存储空间。
  - 您可以指定刷新范围，只刷新最新的几个分区，减少刷新开销。
  
  详细信息，请参阅 [CREATE MATERIALIZED VIEW - 参数](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md#参数) 中的 **PROPERTIES** 部分。您还可以使用 [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER%20MATERIALIZED%20VIEW.md) 修改现有异步物化视图的刷新机制。

- **嵌套物化视图**

  StarRocks 2.5 版本支持嵌套异步物化视图，即基于异步物化视图构建新的异步物化视图。每个异步物化视图的刷新方式仅影响当前物化视图。当前 StarRocks 不对嵌套层数进行限制。生产环境中建议嵌套层数不超过三层。

- **外部数据目录物化视图**

  StarRocks 2.5 版本支持基于 Hive catalog、Hudi catalog 以及 Iceberg catalog 构建异步物化视图。外部数据目录物化视图的创建方式与普通异步物化视图相同，但有以下使用限制：

  - 物化视图中的数据不保证与外部数据目录的数据强一致。
  - 目前暂不支持基于资源（Resource）构建物化视图。
  - 对于 Iceberg 和 Hudi 外部数据目录，StarRocks 目前无法感知基表数据是否发生变动，所以每次刷新会默认刷新所有分区。您可以通过 [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md) 命令手动刷新指定分区。
  - 在 2.5.5 版本后，StarRocks 可以周期性刷新经常访问的 Hive 外部数据目录的元数据缓存，达到感知数据更新的效果。您可以通过以下 FE 参数配置 Hive 元数据缓存周期性刷新：

    | 配置名称                                                      | 默认值                        | 说明                                  |
    | ------------------------------------------------------------ | ---------------------------- | ------------------------------------ |
    | enable_background_refresh_connector_metadata                 | v3.0 为 true，v2.5 为 false   | 是否开启 Hive 元数据缓存周期性刷新。开启后，StarRocks 会轮询 Hive 集群的元数据服务（Hive Metastore 或 AWS Glue），并刷新经常访问的 Hive 外部数据目录的元数据缓存，以感知数据更新。`true` 代表开启，`false` 代表关闭。FE 动态参数，可以通过 [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SET%20CONFIG.md) 命令设置。 |
    | background_refresh_metadata_interval_millis                  | 600000（10 分钟）             | 接连两次 Hive 元数据缓存刷新之间的间隔。单位：毫秒。FE 动态参数，可以通过 [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SET%20CONFIG.md) 命令设置。 |
    | background_refresh_metadata_time_secs_since_last_access_secs | 86400（24 小时）              | Hive 元数据缓存刷新任务过期时间。对于已被访问过的 Hive Catalog，如果超过该时间没有被访问，则停止刷新其元数据缓存。对于未被访问过的 Hive Catalog，StarRocks 不会刷新其元数据缓存。单位：秒。FE 动态参数，可以通过 [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SET%20CONFIG.md) 命令设置。 |

## 手动刷新异步物化视图

您可以通过 [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md) 命令手动刷新指定异步物化视图。StarRocks 2.5 版本中，异步物化视图支持手动刷新部分分区。在 3.1 版本中，StarRocks 支持同步调用刷新任务。

```SQL
-- 异步调用刷新任务。
REFRESH MATERIALIZED VIEW order_mv;
-- 同步调用刷新任务。
REFRESH MATERIALIZED VIEW order_mv WITH SYNC MODE;
```

您可以通过 [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL%20REFRESH%20MATERIALIZED%20VIEW.md) 取消异步调用的刷新任务。

## 查询异步物化视图

异步物化视图本质上是一个物理表，其中存储了根据特定查询语句预先计算的完整结果集。第一次刷新物化视图后，您即可直接查询物化视图。

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

## 使用异步物化视图改写查询

StarRocks 2.5 版本支持 SPJG 类型的异步物化视图查询的自动透明改写。SPJG 类型的物化视图是指在物化视图 Plan 中只包含 Scan、Filter、Project 以及 Aggregate 类型的算子。其查询改写包括单表改写，Join 改写，聚合改写，Union 改写和嵌套物化视图的改写。

目前，StarRocks 支持基于 Default catalog、Hive catalog、Hudi catalog 和 Iceberg catalog 的异步物化视图的查询改写。当查询 Default catalog 数据时，StarRocks 通过排除数据与基表不一致的物化视图，来保证改写之后的查询与原始查询结果的强一致性。当物化视图数据过期时，不会作为候选物化视图。在查询外部目录数据时，由于 StarRocks 无法感知外部目录分区中的数据变化，因此不保证结果的强一致性。

### 启用异步物化视图查询改写

- 启用基于 Default catalog 的异步物化视图查询改写

StarRocks 默认开启异步物化视图查询改写。您可以通过 Session 变量 `enable_materialized_view_rewrite` 开启或关闭该功能。

```SQL
SET GLOBAL enable_materialized_view_rewrite = { true | false };
```

- [实验功能] 启用基于 External Catalog 的异步物化视图查询改写

因为不保证结果的强一致性，所以 StarRocks 默认禁用基于外部数据目录的异步物化视图查询改写。您可以通过在创建物化视图时添加 PROPERTY `"force_external_table_query_rewrite" = "true"` 启用该功能。

示例：

```SQL
CREATE MATERIALIZED VIEW ex_mv_par_tbl
PARTITION BY emp_date
DISTRIBUTED BY hash(empid)
PROPERTIES (
"force_external_table_query_rewrite" = "true"
) 
AS
select empid, deptno, emp_date
from `hive_catalog`.`emp_db`.`emps_par_tbl`
where empid < 5;
```

### 通过 Aggregate Rollup 改写查询

StarRocks 支持通过 Aggregate Rollup 改写查询，即 StarRocks 可以使用通过 `GROUP BY a,b` 子句创建的异步物化视图改写带有 `GROUP BY a` 子句的聚合查询。

在以下示例中，StarRocks 可以使用物化视图 `order_agg_mv` 改写查询 Query 1 和 Query 2：

```SQL
CREATE MATERIALIZED VIEW order_agg_mv
DISTRIBUTED BY HASH(`order_id`) BUCKETS 12
REFRESH ASYNC START('2022-09-01 10:00:00') EVERY (interval 1 day)
AS
SELECT
    order_id,
    order_date,
    bitmap_union(to_bitmap(client_id))  -- uv
FROM order_list 
GROUP BY order_id, order_date;
-- Query 1
SELECT
    order_date,
    bitmap_union(to_bitmap(client_id))  -- uv
FROM order_list 
GROUP BY order_date;
-- Query 2
SELECT
    order_date,
    count(distinct client_id) 
FROM order_list 
GROUP BY order_date;
```

仅有部分聚合函数支持通过 Aggregate Rollup 改写查询。在先前的示例中，如果物化视图 `order_agg_mv` 使用 `count(distinct client_id)` 而非 `bitmap_union(to_bitmap(client_id))`，StarRocks 将无法通过 Aggregate Rollup 改写查询。

下表展示了原始查询中的聚合函数与用于构建物化视图的聚合函数之间的对应关系。您可以根据自己的业务场景，选择相应的聚合函数构建物化视图。

| **原始查询聚合函数**                                      | **支持 Aggregate Rollup 的物化视图构建聚合函数**                 |
| ------------------------------------------------------ | ------------------------------------------------------------ |
| sum                                                    | sum                                                          |
| count                                                  | count                                                        |
| min                                                    | min                                                          |
| max                                                    | max                                                          |
| avg                                                    | sum / count                                                  |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union                                                 |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                                                    |
| percentile_approx, percentile_union                    | percentile_union                                             |

没有相应 GROUP BY 列的 DISTINCT 聚合无法使用 Aggregate Rollup 重写。但是，从 StarRocks v3.1 开始，如果使用 Aggregate Rollup 对应 DISTINCT 聚合函数的查询没有 GROUP BY 列，但有等价的谓词，该查询也可以被相关物化视图重写，因为 StarRocks 可以将等价谓词转换为 GROUP BY 常量表达式。

在以下示例中，StarRocks 可以使用物化视图 `order_agg_mv1` 改写对应查询 Query：

```SQL
CREATE MATERIALIZED VIEW order_agg_mv1
DISTRIBUTED BY HASH(`order_id`) BUCKETS 12
REFRESH ASYNC START('2022-09-01 10:00:00') EVERY (interval 1 day)
AS
SELECT
    order_date,
    count(distinct client_id) 
FROM order_list 
GROUP BY order_date;


-- Query
SELECT
    order_date,
    count(distinct client_id) 
FROM order_list WHERE order_date='2023-07-03';
```

### 基于 View Delta Join 场景改写查询

StarRocks 支持基于异步物化视图 Delta Join 场景改写查询，即查询的表是物化视图基表的子集的场景。例如，`table_a INNER JOIN table_b` 形式的查询可以由 `table_a INNER JOIN table_b INNER JOIN/LEFT OUTER JOIN table_c` 形式的物化视图改写，其中 `table_b INNER JOIN/LEFT OUTER JOIN table_c` 是 Delta Join。此功能允许对这类查询进行透明加速，从而保持查询的灵活性并避免构建宽表的巨大成本。自 v3.1 起，StarRocks 支持基于 Hive Catalog 的 View Delta Join 查询改写。

View Delta Join 查询只有在满足以下要求时才能被改写：

- Delta Join 必须是 Inner Join 或 Left Outer Join。
- 如果 Delta Join 是 Inner Join，则需要 Join 的 Key 必须是对应表的 Foreign/Primary/Unique Key 且必须为 NOT NULL。

  例如，物化视图的形式为 `A INNER JOIN B ON (A.a1 = B.b1) INNER JOIN C ON (B.b2 = C.c1)`，查询的形式为 `A INNER JOIN B ON (A.a1 = B.b1)`。 在这种情况下，`B INNER JOIN C ON (B.b2 = C.c1)` 是 Delta Join。`B.b2` 必须是 B 的 Foreign Key 且必须为 NOT NULL，`C.c1` 必须是 C 的 Primary Key 或 Unique Key。

- 如果 Delta Join 是 Left Outer Join，则需要 Join 的 Key 必须是对应表的 Foreign/Primary/Unique Key。

  例如，物化视图的形式为 `A INNER JOIN B ON (A.a1 = B.b1) LEFT OUTER JOIN C ON (B.b2 = C.c1)`，查询的形式为 `A INNER JOIN B ON (A.a1 = B.b1)`。 在这种情况下，`B LEFT OUTER JOIN C ON (B.b2 = C.c1)` 是 Delta Join。`B.b2` 必须是 B 的 Foreign Key，`C.c1` 必须是 C 的 Primary Key 或 Unique Key。

要实现上述约束，您必须在创建表时通过 Property `unique_constraints` 和 `foreign_key_constraints` 定义表的 Unique Key 和外键约束。详细信息，请参阅 [CREATE TABLE - PROPERTIES](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md#参数说明)。

> **注意**
>
> Unique Key 和外键约束仅用于查询改写。导入数据时，不保证进行外键约束校验。您必须确保导入的数据满足约束条件。

以下示例在创建表 `lineorder` 时定义了多个外键：

```SQL
CREATE TABLE `lineorder` (
  `lo_orderkey` int(11) NOT NULL COMMENT "",
  `lo_linenumber` int(11) NOT NULL COMMENT "",
  `lo_custkey` int(11) NOT NULL COMMENT "",
  `lo_partkey` int(11) NOT NULL COMMENT "",
  `lo_suppkey` int(11) NOT NULL COMMENT "",
  `lo_orderdate` int(11) NOT NULL COMMENT "",
  `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
  `lo_shippriority` int(11) NOT NULL COMMENT "",
  `lo_quantity` int(11) NOT NULL COMMENT "",
  `lo_extendedprice` int(11) NOT NULL COMMENT "",
  `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
  `lo_discount` int(11) NOT NULL COMMENT "",
  `lo_revenue` int(11) NOT NULL COMMENT "",
  `lo_supplycost` int(11) NOT NULL COMMENT "",
  `lo_tax` int(11) NOT NULL COMMENT "",
  `lo_commitdate` int(11) NOT NULL COMMENT "",
  `lo_shipmode` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`lo_orderkey`)
PROPERTIES (
-- Define Unique Keys in unique_constraints.
"unique_constraints" = "lo_orderkey,lo_linenumber",
-- Define Foreign Keys in foreign_key_constraints.
"foreign_key_constraints" = "
    (lo_custkey) REFERENCES customer(c_custkey);
    (lo_partkey) REFERENCES ssb.part(p_partkey);
    (lo_suppkey) REFERENCES supplier(s_suppkey);
    (lo_orderdate) REFERENCES dates(d_datekey)
"
);
```

### 基于派生 Join 场景改写查询

自 v3.1.0 起，StarRocks 支持基于派生 Join 场景改写查询，即基于某种 Join 模式构建的异步物化视图可以改写使用不同 Join 的查询，只要满足以下条件：Join 表和 Join 列相同，并且 Join 模式满足一些要求。

下表列出了物化视图中的 Join 模式与可改写查询中的 Join 模式的对应关系（其中 `A` 和 `B` 表示需要 Join 的表，`a1` 表示 `A` 中的 Join 列，`b1` 表示 `B` 中的 Join 列）：

| **物化视图****中的 Join 模式**  | **可改写查询中的 Join 模式**    | **限制说明**                               |
| ------------------------------- | ------------------------------- | ------------------------------------------ |
| LEFT OUTER JOIN ON A.a1 = B.b1  | INNER JOIN ON A.a1 = B.b1       | 无                                         |
| LEFT OUTER JOIN ON A.a1 = B.b1  | LEFT ANTI JOIN ON A.a1 = B.b1   | 无                                         |
| RIGHT OUTER JOIN ON A.a1 = B.b1 | INNER JOIN ON A.a1 = B.b1       | 无                                         |
| RIGHT OUTER JOIN ON A.a1 = B.b1 | RIGHT ANTI JOIN ON A.a1 = B.b1  | 无                                         |
| INNER JOIN ON A.a1 = B.b1       | LEFT SEMI JOIN ON A.a1 = B.b1   | 右表的 Join 列必须有唯一键约束或者主键约束 |
| INNER JOIN ON A.a1 = B.b1       | RIGHT SEMI JOIN ON A.a1 = B.b1  | 左表的 Join 列必须有唯一键约束或者主键约束 |
| FULL OUTER JOIN ON A.a1 = B.b1  | LEFT OUTER JOIN ON A.a1 = B.b1  | 左表中至少有一列为 NOT NULL                |
| FULL OUTER JOIN ON A.a1 = B.b1  | RIGHT OUTER JOIN ON A.a1 = B.b1 | 右表中至少有一列为 NOT NULL                |
| FULL OUTER JOIN ON A.a1 = B.b1  | INNER JOIN ON A.a1 = B.b1       | 左表和右表中至少有一列为 NOT NULL          |

例如，如果您构建以下异步物化视图：

```SQL
CREATE MATERIALIZED VIEW derivable_join_mv
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_custkey, c_name, c_address
FROM lineorder LEFT OUTER JOIN customer
ON lo_custkey = c_custkey;
```

则该物化视图可以改写如下查询：

```SQL
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_name, c_address
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey;
```

此查询将被改写为以下形式：

```SQL
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_name, c_address
FROM derivable_join_mv
WHERE c_custkey IS NOT NULL;
```

### 设置物化视图查询改写

您可以通过以下 Session 变量设置异步物化视图查询改写。

| **变量**                                    | **默认值** | **描述**                                                     |
| ------------------------------------------- | ---------- | ------------------------------------------------------------ |
| enable_materialized_view_union_rewrite      | true       | 是否开启物化视图 Union 改写。                                |
| enable_rule_based_materialized_view_rewrite | true       | 是否开启基于规则的物化视图查询改写功能，主要用于处理单表查询改写。 |
| nested_mv_rewrite_max_level                 | 3          | 可用于查询改写的嵌套物化视图的最大层数。类型：INT。取值范围：[1, +∞)。取值为 `1` 表示只可使用基于基表创建的物化视图用于查询改写。 |

### 验证查询改写是否生效

您可以使用 EXPLAIN 语句查看对应 Query Plan。如果其中 `OlapScanNode` 项目下的 `TABLE` 为对应异步物化视图名称，则表示该查询已基于异步物化视图改写。

```Plain
mysql> EXPLAIN SELECT 
    order_id, sum(goods.price) AS total 
    FROM order_list INNER JOIN goods 
    ON goods.item_id1 = order_list.item_id2 
    GROUP BY order_id;
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

## 管理异步物化视图

### 修改异步物化视图

您可以通过 [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER%20MATERIALIZED%20VIEW.md) 命令修改异步物化视图属性。

- 修改异步物化视图名称为 `order_total`。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv RENAME order_total;
  ```

- 修改异步物化视图刷新方式为每隔两日刷新。

  ```SQL
  ALTER MATERIALIZED VIEW order_mv REFRESH ASYNC EVERY(INTERVAL 2 DAY);
  ```

### 查看异步物化视图

您可以使用 [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md) 或查询 Information Schema 中的系统元数据表来查看数据库中的异步物化视图。

- 查看当前数据仓库内所有异步物化视图。

  ```SQL
  SHOW MATERIALIZED VIEWS;
  ```

- 查看特定异步物化视图。

  ```SQL
  SHOW MATERIALIZED VIEWS WHERE NAME = "order_mv";
  ```

- 通过名称匹配查看异步物化视图。

  ```SQL
  SHOW MATERIALIZED VIEWS WHERE NAME LIKE "order%";
  ```

- 通过 Information Schema 中的系统元数据表查看所有异步物化视图。

  ```SQL
  SELECT * FROM information_schema.materialized_views;
  ```

### 查看异步物化视图创建语句

您可以通过 [SHOW CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20MATERIALIZED%20VIEW.md) 命令查看异步物化视图创建语句。

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

您可以通过 [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md) 命令删除已创建的异步物化视图。

```SQL
DROP MATERIALIZED VIEW order_mv;
```

### 相关 Session 变量

以下变量控制物化视图的行为：

- `analyze_mv`：刷新后是否以及如何分析物化视图。有效值为空字符串（即不分析）、`sample`（抽样采集）或 `full`（全量采集）。默认为 `sample`。
- `enable_materialized_view_rewrite`：是否开启物化视图的自动改写。有效值为 `true`（自 2.5 版本起为默认值）和 `false`。
