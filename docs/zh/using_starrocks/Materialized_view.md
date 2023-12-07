---
displayed_sidebar: "Chinese"
---

# 异步物化视图

本文介绍如何理解、创建、使用和管理 StarRocks 中的异步物化视图。StarRocks 自 2.4 版本起支持异步物化视图。

相较于同步物化视图，异步物化视图支持多表关联以及更加丰富的聚合算子。异步物化视图可以通过手动调用或定时任务的方式刷新，并且支持刷新部分分区，可以大幅降低刷新成本。除此之外，异步物化视图支持多种查询改写场景，实现自动、透明查询加速。

有关同步物化视图（Rollup）的场景和使用，参见[同步物化视图（Rollup）](../using_starrocks/Materialized_view-single_table.md)。

## 背景介绍

数据仓库环境中的应用程序经常基于多个大表执行复杂查询，通常涉及多表之间数十亿行数据的关联和聚合。处理此类查询通常会大量消耗系统资源和时间，造成极高的查询成本。

您可以通过 StarRocks 中的异步物化视图解决以上问题。异步物化视图是一种特殊的物理表，其中存储了基于基表特定查询语句的预计算结果。当您对基表执行复杂查询时，StarRocks 可以直接复用预计算结果，避免重复计算，进而提高查询性能。查询的频率越高或查询语句越复杂，性能增益就会越很明显。

您还可以通过异步物化视图对数据仓库进行建模，从而向上层应用提供统一的数据口径，屏蔽底层实现，保护基表明细数据安全。

### 理解 StarRocks 物化视图

StarRocks v2.4 之前的版本提供了一种同步更新的同步物化视图（Rollup），可以提供更好的数据新鲜度和更低的刷新成本。但是同步物化视图在场景上有诸多限制，只可基于单一基表创建，且仅支持有限的聚合算子。v2.4 版本之后支持异步物化视图，可以基于多个基表创建，且支持更丰富的聚合算子。

下表从支持的特性角度比较了 StarRocks 中的异步物化视图以及同步物化视图（Rollup）：

|                              | **单表聚合** | **多表关联** | **查询改写** | **刷新策略** | **基表** |
| ---------------------------- | ----------- | ---------- | ----------- | ---------- | -------- |
| **异步物化视图** | 是 | 是 | 是 | <ul><li>异步刷新</li><li>手动刷新</li></ul> | 支持多表构建。基表可以来自：<ul><li>Default Catalog</li><li>External Catalog（v2.5）</li><li>已有异步物化视图（v2.5）</li></ul> |
| **同步物化视图（Rollup）** | 仅部分聚合函数 | 否 | 是 | 导入同步刷新 | 仅支持基于 Default Catalog 的单表构建 |

### 相关概念

- **基表（Base Table）**

  物化视图的驱动表。

  对于 StarRocks 的异步物化视图，基表可以是 [Default catalog](../data_source/catalog/default_catalog.md) 中的内部表、外部数据目录中的表（自 2.5 版本起支持），甚至是已有的异步物化视图（自 v2.5 起支持）。StarRocks 支持在所有 [StarRocks 表类型](../table_design/table_types/table_types.md) 上创建异步物化视图。

- **刷新（Refresh）**

  创建异步物化视图后，其中的数据仅反映创建时刻基表的状态。当基表中的数据发生变化时，需要通过刷新异步物化视图更新数据变化。

  目前 StarRocks 支持两种异步刷新策略：

  - ASYNC：异步刷新，每当基表中的数据发生变化时，物化视图根据指定的刷新间隔自动触发刷新任务。
  - MANUAL：手动触发刷新。物化视图不会自动刷新，需要用户手动维护刷新任务。

- **查询改写（Query Rewrite）**

  查询改写是指在对已构建了物化视图的基表进行查询时，系统自动判断是否可以复用物化视图中的预计算结果处理查询。如果可以复用，系统会直接从相关的物化视图读取预计算结果，以避免重复计算消耗系统资源和时间。

  自 v2.5 版本起，StarRocks 支持基于 SPJG 类型异步物化视图的自动、透明查询改写。SPJG 类型的物化视图是指在物化视图 Plan 中只包含 Scan、Filter、Project 以及 Aggregate 类型的算子。

## 使用场景

如果您的数据仓库环境中有以下需求，我们建议您创建异步物化视图：

- **加速重复聚合查询**

  假设您的数仓环境中存在大量包含相同聚合函数子查询的查询，占用了大量计算资源，您可以根据该子查询建立异步物化视图，计算并保存该子查询的所有结果。建立成功后，系统将自动改写查询语句，直接查询异步物化视图中的中间结果，从而降低负载，加速查询。

- **周期性多表关联查询**

  假设您需要定期将数据仓库中多张表关联，生成一张新的宽表，您可以为这些表建立异步物化视图，并设定定期刷新规则，从而避免手动调度关联任务。异步物化视图建立成功后，查询将直接基于异步物化视图返回结果，从而避免关联操作带来的延迟。

- **数仓分层**

  假设您的基表中包含大量原始数据，查询需要进行复杂的 ETL 操作，您可以通过对数据建立多层异步物化视图实现数仓分层。如此可以将复杂查询分解为多层简单查询，既可以减少重复计算，又能够帮助维护人员快速定位问题。除此之外，数仓分层还可以将原始数据与统计数据解耦，从而保护敏感性原始数据。

- **湖仓加速**

  查询数据湖可能由于网络延迟和对象存储的吞吐限制而变慢。您可以通过在数据湖之上构建异步物化视图来提升查询性能。此外，StarRocks 可以智能改写查询以使用现有的物化视图，省去了手动修改查询的麻烦。

关于异步物化视图的具体使用案例，请参考以下内容：

- [数据建模](./data_modeling_with_materialized_views.md)
- [查询改写](./query_rewrite_with_materialized_views.md)
- [湖仓加速](./data_lake_query_acceleration_with_materialized_views.md)

## 创建异步物化视图

StarRocks 支持在以下数据源创建异步物化视图：

- StarRocks 内部表（基表支持所有 StarRocks 表类型）
- External Catalog 中的表

  - Hive Catalog（自 v2.5 起）
  - Hudi Catalog（自 v2.5 起）
  - Iceberg  Catalog（自 v2.5 起）

- 已有异步物化视图（自 v2.5 起）

### 准备工作

以下示例基于 Default Catalog 中的两张基表：

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

该示例业务场景需要频繁分析订单总额，则查询需要将两张表关联并调用 sum() 函数，根据订单 ID 和总额生成一张新表。除此之外，该业务场景需要每天刷新订单总额。

其查询语句如下：

```SQL
SELECT
    order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

### 基于查询语句创建异步物化视图

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
