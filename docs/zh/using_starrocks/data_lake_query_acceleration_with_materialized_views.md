---
displayed_sidebar: "Chinese"
---

# 使用物化视图加速数据湖查询

本文描述了如何使用 StarRocks 的异步物化视图来优化数据湖中的查询性能。

StarRocks 提供了开箱即用的数据湖查询功能，非常适用于对湖中的数据进行探查式查询分析。在大多数情况下，[Data Cache](../data_source/data_cache.md) 可以提供 Block 级文件缓存，避免由远程存储抖动和大量I/O操作引起的性能下降。

然而，当涉及到使用湖中数据构建复杂和高效的报表，或进一步加速这些查询时，您可能仍然会遇到性能挑战。通过使用异步物化视图，您可以为数据湖中的报表和应用实现更高的并发，以及更好的性能。

## 概述

StarRocks 支持基于 External Catalog，如 Hive Catalog、Iceberg Catalog 和 Hudi Catalog，构建异步物化视图。基于 External Catalog 的物化视图在以下情况下特别有用：

- **数据湖报表的透明加速**

  为了确保数据湖报表的查询性能，数据工程师通常需要与数据分析师紧密合作，研究报告加速层的构建逻辑。如果加速层需求更新，他们必须相应地更新构建逻辑、执行计划和查询语句。 通过物化视图的查询改写能力，可以使用户不感知报表加速过程。当识别出慢查询时，数据工程师可以分析慢查询的模式并按需创建物化视图。然后，上层查询会被智能改写，并通过物化视图透明加速，从而实现在不修改业务应用的逻辑或查询语句情况下，快速改善查询性能。

- **实时数据与离线数据关联的增量计算**

  假设您的业务应用需要将 StarRocks 本地表中的实时数据与数据湖中的历史数据关联起来以进行增量计算。在这种情况下，物化视图可以提供一个简单的解决方案。例如，如果实时事实表是 StarRocks 中的本地表，而维度表存储在数据湖中，您可以通过构建物化视图将本地表与外部数据源中的表关联起来，轻松进行增量计算。

- **指标层的快速搭建**

  在处理高维度数据时，计算和处理指标可能会遇到挑战。您可以使用物化视图进行数据预聚合和上卷，以创建一个相对轻量级的指标层。此外，你还可以利用物化视图自动刷新的特性，进一步降低指标计算的复杂性。

物化视图、Data Cache 和 StarRocks 中的本地表都是实现显著查询性能提升的有效方法。下表比较了它们的主要区别：

<table class="comparison">
  <thead>
    <tr>
      <th>&nbsp;</th>
      <th>Data Cache</th>
      <th>物化视图</th>
      <th>本地表</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><b>数据导入和更新</b></td>
      <td>查询会自动触发数据缓存</td>
      <td>自动触发刷新任务</td>
      <td>支持各种导入方法，但需要手动维护导入任务</td>
    </tr>
    <tr>
      <td><b>数据缓存粒度</b></td>
      <td><ul><li>支持 Block 级数据缓存</li><li>遵循 LRU 缓存淘汰机制</li><li>不缓存计算结果</li></ul></td>
      <td>存储预计算的查询结果</td>
      <td>基于表定义存储数据</td>
    </tr>
    <tr>
      <td><b>查询性能</b></td>
      <td colspan="3" style={{textAlign: 'center'}} >Data Cache &le; 物化视图 = 本地表</td>
    </tr>
    <tr>
      <td><b>查询语句</b></td>
      <td><ul><li>无需修改针对湖数据的查询语句</li><li>一旦查询命中缓存，就会进行现场计算。</li></ul></td>
      <td><ul><li>无需修改针对湖数据的查询语句</li><li>利用查询改写重用预先计算的结果</li></ul></td>
      <td>需要修改查询语句以查询本地表</td>
    </tr>
  </tbody>
</table>

<br />

与直接查询数据湖数据或将数据导入到本地表中相比，物化视图提供了几个独特的优势：

- **本地存储加速**：物化视图可以利用 StarRocks 的本地存储加速优势，如索引、分区分桶和 Colocate Group，从而相较直接从数据湖查询数据具有更好的查询性能。
- **无需维护加载任务**：物化视图通过自动刷新任务透明地更新数据，无需维护导入任务。此外，基于 Hive Catalog 的物化视图可以检测数据更改并在分区级别执行增量刷新。
- **智能查询改写**：查询可以被透明改写至物化视图，无需修改应用使用的查询语句即可加速查询。

<br />

因此，我们建议在以下情况下使用物化视图：

- 在启用了 Data Cache 的情况下，查询性能仍不符合您对查询延迟和并发性的要求。
- 查询涉及可复用的部份，如固定的聚合方式、Join 模式。
- 数据以分区方式组织，而查询聚合度较高（例如按天聚合）。

<br />

在以下情况下，我们建议优先通过 Data Cache 来实现加速：

- 查询没有大量可复用的部份，并且可能涉及数据湖中的任何数据。
- 远程存储存在显著的波动或不稳定性，可能会对访问产生潜在影响。

## 创建基于 External Catalog 的物化视图

在 External Catalog 中的表上创建物化视图与在 StarRocks 本地表上创建物化视图类似。您只需根据正在使用的数据源设置合适的刷新策略，并手动启用 External Catalog 物化视图的查询改写功能。

### 选择合适的刷新策略

目前，StarRocks 无法检测 Hudi Catalog、Iceberg Catalog 和 JDBC Catalog 中的分区级别数据更改。因此，一旦触发刷新任务，将执行全量刷新。

对于 Hive Catalog，您可以启用 Hive 元数据缓存刷新功能，允许 StarRocks 在分区级别检测数据更改。请注意，物化视图的分区键必须包含在基表的分区键中。启用此功能后，StarRocks 定期访问 Hive 元数据存储服务（HMS）或 AWS Glue，以检查最近查询的热数据的元数据信息。从而，StarRocks 可以：

- 仅刷新数据有更改的分区，避免全量刷新，减少刷新导致的资源消耗。
- 在查询改写期间在一定程度上确保数据一致性。如果数据湖中的基表发生数据更改，查询将不会被改写至使用物化视图。

  > **说明**
  >
  > 您仍然可以选择在创建物化视图时通过设置属性 `mv_rewrite_staleness_second` 来容忍一定程度的数据不一致。有关更多信息，请参阅 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md)。

要启用 Hive 元数据缓存刷新功能，您可以使用 [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md) 设置以下 FE 动态配置项：

| **配置名称**                                                 | **默认值**                      | **说明**                                                     |
| ------------------------------------------------------------ | ------------------------------- | ------------------------------------------------------------ |
| enable_background_refresh_connector_metadata                 | v3.0 为 `true`，v2.5 为 `false` | 是否开启 Hive 元数据缓存周期性刷新。开启后，StarRocks 会轮询 Hive 集群的元数据服务（HMS 或 AWS Glue），并刷新经常访问的 Hive 外部数据目录的元数据缓存，以感知数据更新。`true` 代表开启，`false` 代表关闭。 |
| background_refresh_metadata_interval_millis                  | 600000（10 分钟）               | 接连两次 Hive 元数据缓存刷新之间的间隔。单位：毫秒。         |
| background_refresh_metadata_time_secs_since_last_access_secs | 86400（24 小时）                | Hive 元数据缓存刷新任务过期时间。对于已被访问过的 Hive Catalog，如果超过该时间没有被访问，则停止刷新其元数据缓存。对于未被访问过的 Hive Catalog，StarRocks 不会刷新其元数据缓存。单位：秒。 |

### 启用 External Catalog 物化视图的查询改写

由于不保证数据的强一致性，StarRocks 默认禁用 Hudi、Iceberg 和 JDBC Catalog 物化视图的查询改写功能。您可以通过在创建物化视图时将 Property `force_external_table_query_rewrite` 设置为 `true` 来启用此功能。对于基于 Hive Catalog 中的表创建的物化视图，查询改写功能默认开启。在涉及查询改写的情况下，如果您使用非常复杂的查询语句来构建物化视图，我们建议您拆分查询语句并以嵌套方式构建多个简单的物化视图。嵌套的物化视图更加灵活，可以适应更广泛的查询模式。

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
from `hudi_catalog`.`emp_db`.`emps_par_tbl`
where empid < 5;
```

## 最佳实践

在实际业务场景中，您可以通过分析 Audit Log 或[大查询日志](../administration/monitor_manage_big_queries.md)来识别执行较慢、资源消耗较高的查询。您还可以使用 [Query Profile](../administration/query_profile.md) 来精确定位查询缓慢的特定阶段。以下各小节提供了如何通过物化视图提高数据湖查询性能的说明和示例。

### 案例一：加速数据湖中的 Join 计算

您可以使用物化视图来加速数据湖中的 Join 查询。

假设以下 Hive catalog 上的查询为慢查询：

```SQL
--Q1
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25;

--Q2
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35;

--Q3 
SELECT SUM(lo_revenue), d_year, p_brand
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates, hive.ssb_1g_csv.part, hive.ssb_1g_csv.supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
    AND s_region = 'ASIA'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;
```

通过分析查询概要，您可能会注意到查询执行时间主要花费在表 `lineorder` 与其他维度表在列 `lo_orderdate` 上的 Hash Join 上。

此处，Q1 和 Q2 在 Join `lineorder` 和 `dates` 后执行聚合，而 Q3 在 Join `lineorder`、`dates`、`part` 和 `supplier` 后执行聚合。

因此，您可以利用 StarRocks 的 [View Delta Join 改写](./query_rewrite_with_materialized_views.md#view-delta-join-改写) 能力来构建物化视图，对 `lineorder`、`dates`、`part` 和 `supplier` 进行 Join。

```SQL
CREATE MATERIALIZED VIEW lineorder_flat_mv
DISTRIBUTED BY HASH(LO_ORDERDATE, LO_ORDERKEY) BUCKETS 48
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES ( 
    -- 指定唯一约束。
    "unique_constraints" = "
    hive.ssb_1g_csv.supplier.s_suppkey;
    hive.ssb_1g_csv.part.p_partkey;
    hive.ssb_1g_csv.dates.d_datekey",
    -- 指定外键约束。
    "foreign_key_constraints" = "
    hive.ssb_1g_csv.lineorder(lo_partkey) REFERENCES hive.ssb_1g_csv.part(p_partkey);
    hive.ssb_1g_csv.lineorder(lo_suppkey) REFERENCES hive.ssb_1g_csv.supplier(s_suppkey);
    hive.ssb_1g_csv.lineorder(lo_orderdate) REFERENCES hive.ssb_1g_csv.dates(d_datekey)",
    -- 启用查询改写。
    "force_external_table_query_rewrite" = "TRUE"
)
AS SELECT
       l.LO_ORDERDATE AS LO_ORDERDATE,
       l.LO_ORDERKEY AS LO_ORDERKEY,
       l.LO_PARTKEY AS LO_PARTKEY,
       l.LO_SUPPKEY AS LO_SUPPKEY,
       l.LO_QUANTITY AS LO_QUANTITY,
       l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
       l.LO_DISCOUNT AS LO_DISCOUNT,
       l.LO_REVENUE AS LO_REVENUE,
       s.S_REGION AS S_REGION,
       p.P_BRAND AS P_BRAND,
       d.D_YEAR AS D_YEAR,
       d.D_YEARMONTH AS D_YEARMONTH
   FROM hive.ssb_1g_csv.lineorder AS l
            INNER JOIN hive.ssb_1g_csv.supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
            INNER JOIN hive.ssb_1g_csv.part AS p ON p.P_PARTKEY = l.LO_PARTKEY
            INNER JOIN hive.ssb_1g_csv.dates AS d ON l.LO_ORDERDATE = d.D_DATEKEY;
```

### 案例二：加速数据湖中的聚合和 Join 后聚合计算

物化视图可用于加速聚合查询，无论是在单个表上还是涉及多个表。

- 单表聚合查询

  对于典型的单表查询，如果 Query Profile 显示 AGGREGATE 节点消耗了大量时间，您可以使用常见的聚合算子构建物化视图。 假设以下查询速度较慢：

  ```SQL
  --Q4
  SELECT
  lo_orderdate, count(distinct lo_orderkey)
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate
  ORDER BY lo_orderdate limit 100;
  ```

  Q4 是一个计算每日去重订单数量的查询，因为 count distinct 的消耗较大，可以创建下列两类物化视图：

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_1 
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  lo_orderdate, count(distinct lo_orderkey)
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate;
  
  CREATE MATERIALIZED VIEW mv_2_2 
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  -- lo_orderkey 必须是 BIGINT 类型，以便可以用于查询改写。
  lo_orderdate, bitmap_union(to_bitmap(lo_orderkey))
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate;
  ```

  请注意，此处不要创建带有 LIMIT 和 ORDER BY 子句的物化视图，以避免改写失败。有关查询改写限制的更多信息，请参阅[物化视图查询改写 - 限制](./query_rewrite_with_materialized_views.md#限制)。

- 多表聚合查询

  在涉及 Join 结果聚合的场景中，您可以在现有 Join 多表的物化视图上创建嵌套物化视图，进一步聚合连接结果。例如，根据案例一中的示例，您可以创建以下物化视图以加速 Q1 和 Q2，因为它们的聚合模式相似：

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_3
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth, SUM(lo_extendedprice * lo_discount) AS REVENUE
  FROM lineorder_flat_mv
  GROUP BY lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth;
  ```

  当然，您也可以在单个物化视图中同时执行 Join 和聚合计算。尽管这类的物化视图改写查询的机会更少（因为涉及的计算更加具体），但在聚合后，其占用存储空间更少。您可以基于您的真实场景选择。

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_4
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  PROPERTIES (
      "force_external_table_query_rewrite" = "TRUE"
  )
  AS
  SELECT lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth, SUM(lo_extendedprice * lo_discount) AS REVENUE
  FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
  WHERE lo_orderdate = d_datekey
  GROUP BY lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth;
  ```

### 案例三：加速数据湖中的聚合后 Join 计算

在某些情况下，您可能需要首先对一个表执行聚合计算，然后再与其他表执行 Join 查询。为了充分利用 StarRocks 的查询改写功能，我们建议您构建嵌套的物化视图。例如：

```SQL
--Q5
SELECT * FROM  (
    SELECT 
      l.lo_orderkey, l.lo_orderdate, c.c_custkey, c_region, sum(l.lo_revenue)
    FROM 
      hive.ssb_1g_csv.lineorder l 
      INNER JOIN (
        SELECT distinct c_custkey, c_region 
        from 
          hive.ssb_1g_csv.customer 
        WHERE 
          c_region IN ('ASIA', 'AMERICA') 
      ) c ON l.lo_custkey = c.c_custkey
      GROUP BY  l.lo_orderkey, l.lo_orderdate, c.c_custkey, c_region
  ) c1 
WHERE 
  lo_orderdate = '19970503'
```

Q5 首先在 `customer` 表上执行聚合，然后在 `lineorder` 表上执行 Join 和聚合。类似的查询可能涉及对 `c_region` 和 `lo_orderdate` 的不同过滤条件。为了利用查询改写功能，您可以创建两个物化视图，一个用于聚合，另一个用于连接。

```SQL
--mv_3_1
CREATE MATERIALIZED VIEW mv_3_1
DISTRIBUTED BY HASH(c_custkey)
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES (
    "force_external_table_query_rewrite" = "TRUE"
)
AS
SELECT distinct c_custkey, c_region from hive.ssb_1g_csv.customer; 

--mv_3_2
CREATE MATERIALIZED VIEW mv_3_2
DISTRIBUTED BY HASH(lo_orderdate)
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES (
    "force_external_table_query_rewrite" = "TRUE"
)
AS
SELECT l.lo_orderdate, l.lo_orderkey, mv.c_custkey, mv.c_region, sum(l.lo_revenue)
FROM hive.ssb_1g_csv.lineorder l 
INNER JOIN mv_3_1 mv
ON l.lo_custkey = mv.c_custkey
GROUP BY l.lo_orderkey, l.lo_orderdate, mv.c_custkey, mv.c_region;
```

### 案例四：对实时数据和数据湖中的历史数据进行冷热分离

试想以下情景：过去三天内的新数据直接写入 StarRocks，三天前的旧数据经过校对后批量写入 Hive。但是查询仍然可能有涉及过去七天数据。在这种情况下，您可以使用物化视图创建一个简单的模型来自动过期数据。

```SQL
CREATE MATERIALIZED VIEW mv_4_1 
DISTRIBUTED BY HASH(lo_orderdate)
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
AS 
SELECT lo_orderkey, lo_orderdate, lo_revenue
FROM hive.ssb_1g_csv.lineorder
WHERE lo_orderdate<=current_date()
AND lo_orderdate>=date_add(current_date(), INTERVAL -4 DAY);
```

您可以根据上层业务逻辑进一步构建视图或物化视图封装计算。
