---
displayed_sidebar: docs
---

# 物化视图查询改写

本文描述了如何利用 StarRocks 的异步物化视图来改写并加速查询。

## 概述

StarRocks 的异步物化视图采用了主流的基于 SPJG（select-project-join-group-by）模式透明查询改写算法。在不修改查询语句的前提下，StarRocks 可以自动将在基表上的查询改写为在物化视图上的查询。通过其中包含的预计算结果，物化视图可以帮助您显著降低计算成本，并大幅加速查询执行。

基于异步物化视图的查询改写功能，在以下场景下特别有用：

- **指标预聚合**

  如果您需要处理高维度数据，可以使用物化视图来创建预聚合指标层。

- **宽表 Join**

  物化视图允许您在复杂场景下下透明加速包含大宽表 Join 的查询。

- **湖仓加速**

  构建基于 External Catalog 的物化视图可以轻松加速针对数据湖中数据的查询。

> **说明**
>
> 基于 JDBC Catalog 表构建的异步物化视图暂不支持查询改写。

### 功能特点

StarRocks 的异步物化视图自动查询改写功能具有以下特点：

- **强数据一致性**：如果基表是 StarRocks 内表，StarRocks 可以保证通过物化视图查询改写获得的结果与直接查询基表的结果一致。
- **Staleness rewrite**：StarRocks 支持 Staleness rewrite，即允许容忍一定程度的数据过期，以应对数据变更频繁的情况。
- **多表 Join**：StarRocks 的异步物化视图支持各种类型的 Join，包括一些复杂的 Join 场景，如 View Delta Join 和 Join 派生改写，可用于加速涉及大宽表的查询场景。
- **聚合改写**：StarRocks 可以改写带有聚合操作的查询，以提高报表性能。
- **嵌套物化视图**：StarRocks 支持基于嵌套物化视图改写复杂查询，扩展了可改写的查询范围。
- **Union 改写**：您可以将 Union 改写特性与物化视图分区的生存时间（TTL）相结合，实现冷热数据的分离，允许您从物化视图查询热数据，从基表查询历史数据。
- **基于视图构建物化视图**：您可以在基于视图建模的情景下加速查询。
- **基于 External Catalog 构建物化视图**：您可以通过该特性加速数据湖中的查询。
- **复杂表达式改写**：支持在表达式中调用函数和算术运算，满足复杂分析和计算需求。

这些特点将在以下各节中详细说明。

## Join 改写

StarRocks 支持改写具有各种类型 Join 的查询，包括 Inner Join、Cross Join、Left Outer Join、Full Outer Join、Right Outer Join、Semi Join 和 Anti Join。 以下示例展示 Join 查询的改写。创建以下基表：

```SQL
CREATE TABLE customer (
  c_custkey     INT(11)     NOT NULL,
  c_name        VARCHAR(26) NOT NULL,
  c_address     VARCHAR(41) NOT NULL,
  c_city        VARCHAR(11) NOT NULL,
  c_nation      VARCHAR(16) NOT NULL,
  c_region      VARCHAR(13) NOT NULL,
  c_phone       VARCHAR(16) NOT NULL,
  c_mktsegment  VARCHAR(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(c_custkey)
DISTRIBUTED BY HASH(c_custkey) BUCKETS 12;

CREATE TABLE lineorder (
  lo_orderkey         INT(11) NOT NULL,
  lo_linenumber       INT(11) NOT NULL,
  lo_custkey          INT(11) NOT NULL,
  lo_partkey          INT(11) NOT NULL,
  lo_suppkey          INT(11) NOT NULL,
  lo_orderdate        INT(11) NOT NULL,
  lo_orderpriority    VARCHAR(16) NOT NULL,
  lo_shippriority     INT(11) NOT NULL,
  lo_quantity         INT(11) NOT NULL,
  lo_extendedprice    INT(11) NOT NULL,
  lo_ordtotalprice    INT(11) NOT NULL,
  lo_discount         INT(11) NOT NULL,
  lo_revenue          INT(11) NOT NULL,
  lo_supplycost       INT(11) NOT NULL,
  lo_tax              INT(11) NOT NULL,
  lo_commitdate       INT(11) NOT NULL,
  lo_shipmode         VARCHAR(11) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(lo_orderkey)
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 48;
```

基于上述基表，创建以下物化视图：

```SQL
CREATE MATERIALIZED VIEW join_mv1
DISTRIBUTED BY HASH(lo_orderkey)
AS
SELECT lo_orderkey, lo_linenumber, lo_revenue, lo_partkey, c_name, c_address
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey;
```

该物化视图可以改写以下查询：

```SQL
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_name, c_address
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey;
```

其原始查询计划和改写后的计划如下：

![Rewrite-1](../_assets/Rewrite-1.png)

StarRocks 支持改写具有复杂表达式的 Join 查询，如算术运算、字符串函数、日期函数、CASE WHEN 表达式和谓词 OR 等。例如，上述物化视图可以改写以下查询：

```SQL
SELECT 
    lo_orderkey, 
    lo_linenumber, 
    (2 * lo_revenue + 1) * lo_linenumber, 
    upper(c_name), 
    substr(c_address, 3)
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey;
```

除了常规场景，StarRocks还支持在更复杂的情景下改写 Join 查询。

### Query Delta Join 改写

Query Delta Join 是指查询中 Join 的表是物化视图中 Join 的表的超集的情况。例如，以下查询 Join 了表 `lineorder`、表 `customer` 和 表 `part`。如果物化视图 `join_mv1` 仅包含 `lineorder` 和 `customer` 的 Join，StarRocks 可以使用 `join_mv1` 来改写查询。

示例：

```SQL
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_name, c_address, p_name
FROM
    lineorder INNER JOIN customer ON lo_custkey = c_custkey
    INNER JOIN part ON lo_partkey = p_partkey;
```

其原始查询计划和改写后的计划如下：

![Rewrite-2](../_assets/Rewrite-2.png)

### View Delta Join 改写

View Delta Join 指的是查询中 Join 的表是物化视图中 Join 的表的子集的情况。通常在涉及大宽表的情景中使用此功能。例如，在 Star Schema Benchmark (SSB) 的背景下，您可以通过创建物化视图，Join 所有表以提高查询性能。测试发现在通过物化视图透明改写查询后，多表 Join 的查询性能可以达到与查询相应大宽表相同的性能水平。

要启用 View Delta Join 改写，必须保证物化视图中包含在查询中不存在的 1:1 的 Cardinality Preservation Join。满足以下约束条件的九种 Join 都被视为 Cardinality Preservation Join，可以用于启用 View Delta Join 改写：

![Rewrite-3](../_assets/Rewrite-3.png)

以 SSB 测试为例，创建以下基表：

```SQL
CREATE TABLE customer (
  c_custkey         INT(11)       NOT NULL,
  c_name            VARCHAR(26)   NOT NULL,
  c_address         VARCHAR(41)   NOT NULL,
  c_city            VARCHAR(11)   NOT NULL,
  c_nation          VARCHAR(16)   NOT NULL,
  c_region          VARCHAR(13)   NOT NULL,
  c_phone           VARCHAR(16)   NOT NULL,
  c_mktsegment      VARCHAR(11)   NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(c_custkey)
DISTRIBUTED BY HASH(c_custkey) BUCKETS 12
PROPERTIES (
"unique_constraints" = "c_custkey"   -- 指定唯一键。
);

CREATE TABLE dates (
  d_datekey          DATE          NOT NULL,
  d_date             VARCHAR(20)   NOT NULL,
  d_dayofweek        VARCHAR(10)   NOT NULL,
  d_month            VARCHAR(11)   NOT NULL,
  d_year             INT(11)       NOT NULL,
  d_yearmonthnum     INT(11)       NOT NULL,
  d_yearmonth        VARCHAR(9)    NOT NULL,
  d_daynuminweek     INT(11)       NOT NULL,
  d_daynuminmonth    INT(11)       NOT NULL,
  d_daynuminyear     INT(11)       NOT NULL,
  d_monthnuminyear   INT(11)       NOT NULL,
  d_weeknuminyear    INT(11)       NOT NULL,
  d_sellingseason    VARCHAR(14)   NOT NULL,
  d_lastdayinweekfl  INT(11)       NOT NULL,
  d_lastdayinmonthfl INT(11)       NOT NULL,
  d_holidayfl        INT(11)       NOT NULL,
  d_weekdayfl        INT(11)       NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(d_datekey)
DISTRIBUTED BY HASH(d_datekey) BUCKETS 1
PROPERTIES (
"unique_constraints" = "d_datekey"   -- 指定唯一键。
);

CREATE TABLE supplier (
  s_suppkey          INT(11)       NOT NULL,
  s_name             VARCHAR(26)   NOT NULL,
  s_address          VARCHAR(26)   NOT NULL,
  s_city             VARCHAR(11)   NOT NULL,
  s_nation           VARCHAR(16)   NOT NULL,
  s_region           VARCHAR(13)   NOT NULL,
  s_phone            VARCHAR(16)   NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(s_suppkey)
DISTRIBUTED BY HASH(s_suppkey) BUCKETS 12
PROPERTIES (
"unique_constraints" = "s_suppkey"   -- 指定唯一键。
);

CREATE TABLE part (
  p_partkey          INT(11)       NOT NULL,
  p_name             VARCHAR(23)   NOT NULL,
  p_mfgr             VARCHAR(7)    NOT NULL,
  p_category         VARCHAR(8)    NOT NULL,
  p_brand            VARCHAR(10)   NOT NULL,
  p_color            VARCHAR(12)   NOT NULL,
  p_type             VARCHAR(26)   NOT NULL,
  p_size             TINYINT(11)   NOT NULL,
  p_container        VARCHAR(11)   NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(p_partkey)
DISTRIBUTED BY HASH(p_partkey) BUCKETS 12
PROPERTIES (
"unique_constraints" = "p_partkey"   -- 指定唯一键。
);

CREATE TABLE lineorder (
  lo_orderdate       DATE          NOT NULL, -- 指定为 NOT NULL。
  lo_orderkey        INT(11)       NOT NULL,
  lo_linenumber      TINYINT       NOT NULL,
  lo_custkey         INT(11)       NOT NULL, -- 指定为 NOT NULL。
  lo_partkey         INT(11)       NOT NULL, -- 指定为 NOT NULL。
  lo_suppkey         INT(11)       NOT NULL, -- 指定为 NOT NULL。
  lo_orderpriority   VARCHAR(100)  NOT NULL,
  lo_shippriority    TINYINT       NOT NULL,
  lo_quantity        TINYINT       NOT NULL,
  lo_extendedprice   INT(11)       NOT NULL,
  lo_ordtotalprice   INT(11)       NOT NULL,
  lo_discount        TINYINT       NOT NULL,
  lo_revenue         INT(11)       NOT NULL,
  lo_supplycost      INT(11)       NOT NULL,
  lo_tax             TINYINT       NOT NULL,
  lo_commitdate      DATE          NOT NULL,
  lo_shipmode        VARCHAR(100)  NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(lo_orderdate,lo_orderkey)
PARTITION BY RANGE(lo_orderdate)
(PARTITION p1 VALUES [("0000-01-01"), ("1993-01-01")),
PARTITION p2 VALUES [("1993-01-01"), ("1994-01-01")),
PARTITION p3 VALUES [("1994-01-01"), ("1995-01-01")),
PARTITION p4 VALUES [("1995-01-01"), ("1996-01-01")),
PARTITION p5 VALUES [("1996-01-01"), ("1997-01-01")),
PARTITION p6 VALUES [("1997-01-01"), ("1998-01-01")),
PARTITION p7 VALUES [("1998-01-01"), ("1999-01-01")))
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 48
PROPERTIES (
"foreign_key_constraints" = "
    (lo_custkey) REFERENCES customer(c_custkey);
    (lo_partkey) REFERENCES part(p_partkey);
    (lo_suppkey) REFERENCES supplier(s_suppkey)" -- 指定外键约束。
);
```

创建 Join 表 `lineorder`、表 `customer`、表 `supplier`、表 `part` 和表 `dates` 的物化视图 `lineorder_flat_mv`：

```SQL
CREATE MATERIALIZED VIEW lineorder_flat_mv
DISTRIBUTED BY HASH(LO_ORDERDATE, LO_ORDERKEY) BUCKETS 48
PARTITION BY LO_ORDERDATE
REFRESH MANUAL
PROPERTIES (
    "partition_refresh_number"="1"
)
AS SELECT /*+ SET_VAR(query_timeout = 7200) */     -- 设置刷新超时时间。
       l.LO_ORDERDATE        AS LO_ORDERDATE,
       l.LO_ORDERKEY         AS LO_ORDERKEY,
       l.LO_LINENUMBER       AS LO_LINENUMBER,
       l.LO_CUSTKEY          AS LO_CUSTKEY,
       l.LO_PARTKEY          AS LO_PARTKEY,
       l.LO_SUPPKEY          AS LO_SUPPKEY,
       l.LO_ORDERPRIORITY    AS LO_ORDERPRIORITY,
       l.LO_SHIPPRIORITY     AS LO_SHIPPRIORITY,
       l.LO_QUANTITY         AS LO_QUANTITY,
       l.LO_EXTENDEDPRICE    AS LO_EXTENDEDPRICE,
       l.LO_ORDTOTALPRICE    AS LO_ORDTOTALPRICE,
       l.LO_DISCOUNT         AS LO_DISCOUNT,
       l.LO_REVENUE          AS LO_REVENUE,
       l.LO_SUPPLYCOST       AS LO_SUPPLYCOST,
       l.LO_TAX              AS LO_TAX,
       l.LO_COMMITDATE       AS LO_COMMITDATE,
       l.LO_SHIPMODE         AS LO_SHIPMODE,
       c.C_NAME              AS C_NAME,
       c.C_ADDRESS           AS C_ADDRESS,
       c.C_CITY              AS C_CITY,
       c.C_NATION            AS C_NATION,
       c.C_REGION            AS C_REGION,
       c.C_PHONE             AS C_PHONE,
       c.C_MKTSEGMENT        AS C_MKTSEGMENT,
       s.S_NAME              AS S_NAME,
       s.S_ADDRESS           AS S_ADDRESS,
       s.S_CITY              AS S_CITY,
       s.S_NATION            AS S_NATION,
       s.S_REGION            AS S_REGION,
       s.S_PHONE             AS S_PHONE,
       p.P_NAME              AS P_NAME,
       p.P_MFGR              AS P_MFGR,
       p.P_CATEGORY          AS P_CATEGORY,
       p.P_BRAND             AS P_BRAND,
       p.P_COLOR             AS P_COLOR,
       p.P_TYPE              AS P_TYPE,
       p.P_SIZE              AS P_SIZE,
       p.P_CONTAINER         AS P_CONTAINER,
       d.D_DATE              AS D_DATE,
       d.D_DAYOFWEEK         AS D_DAYOFWEEK,
       d.D_MONTH             AS D_MONTH,
       d.D_YEAR              AS D_YEAR,
       d.D_YEARMONTHNUM      AS D_YEARMONTHNUM,
       d.D_YEARMONTH         AS D_YEARMONTH,
       d.D_DAYNUMINWEEK      AS D_DAYNUMINWEEK,
       d.D_DAYNUMINMONTH     AS D_DAYNUMINMONTH,
       d.D_DAYNUMINYEAR      AS D_DAYNUMINYEAR,
       d.D_MONTHNUMINYEAR    AS D_MONTHNUMINYEAR,
       d.D_WEEKNUMINYEAR     AS D_WEEKNUMINYEAR,
       d.D_SELLINGSEASON     AS D_SELLINGSEASON,
       d.D_LASTDAYINWEEKFL   AS D_LASTDAYINWEEKFL,
       d.D_LASTDAYINMONTHFL  AS D_LASTDAYINMONTHFL,
       d.D_HOLIDAYFL         AS D_HOLIDAYFL,
       d.D_WEEKDAYFL         AS D_WEEKDAYFL
   FROM lineorder            AS l
       INNER JOIN customer   AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
       INNER JOIN supplier   AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
       INNER JOIN part       AS p ON p.P_PARTKEY = l.LO_PARTKEY
       INNER JOIN dates      AS d ON l.LO_ORDERDATE = d.D_DATEKEY;    
```

SSB Q2.1 涉及四个表的 Join，但与物化视图 `lineorder_flat_mv` 相比，缺少了 `customer` 表。在 `lineorder_flat_mv` 中，`lineorder INNER JOIN customer` 本质上是一个 Cardinality Preservation Join。因此逻辑上，可以消除该 Join 而不影响查询结果。因此，Q2.1 可以使用 `lineorder_flat_mv` 进行改写。

SSB Q2.1：

```SQL
SELECT sum(lo_revenue) AS lo_revenue, d_year, p_brand
FROM lineorder
JOIN dates ON lo_orderdate = d_datekey
JOIN part ON lo_partkey = p_partkey
JOIN supplier ON lo_suppkey = s_suppkey
WHERE p_category = 'MFGR#12' AND s_region = 'AMERICA'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;
```

其原始查询计划和改写后的计划如下：

![Rewrite-4](../_assets/Rewrite-4.png)

同样，SSB 中的其他查询也可以通过使用 `lineorder_flat_mv` 进行透明改写，从而优化查询性能。

### Join 派生改写

Join 派生是指物化视图和查询中的 Join 类型不一致，但物化视图的 Join 结果包含查询 Join 结果的情况。目前支持两种情景：三表或以上 Join 和两表 Join 的情景。

- **情景一：三表或以上 Join**

  假设物化视图包含表 `t1` 和表 `t2` 之间的 Left Outer Join，以及表 `t2` 和表 `t3` 之间的 Inner Join。两个 Join 的条件都包括来自表 `t2` 的列。

  而查询则包含 `t1` 和 `t2` 之间的 Inner Join，以及 `t2` 和 `t3` 之间的 Inner Join。两个 Join 的条件都包括来自表 `t2` 的列。

  在这种情况下，上述查询可以通过物化视图改写。这是因为在物化视图中，首先执行 Left Outer Join，然后执行 Inner Join。Left Outer Join 生成的右表没有匹配结果（即右表中的列为 NULL）。这些结果在执行 Inner Join 期间被过滤掉。因此，物化视图和查询的逻辑是等效的，可以对查询进行改写。

  示例：

  创建物化视图 `join_mv5`：

  ```SQL
  CREATE MATERIALIZED VIEW join_mv5
  PARTITION BY lo_orderdate
  DISTRIBUTED BY hash(lo_orderkey)
  PROPERTIES (
    "partition_refresh_number" = "1"
  )
  AS
  SELECT lo_orderkey, lo_orderdate, lo_linenumber, lo_revenue, c_custkey, c_address, p_name
  FROM customer LEFT OUTER JOIN lineorder
  ON c_custkey = lo_custkey
  INNER JOIN part
  ON p_partkey = lo_partkey;
  ```

  `join_mv5` 可改写以下查询：

  ```SQL
  SELECT lo_orderkey, lo_orderdate, lo_linenumber, lo_revenue, c_custkey, c_address, p_name
  FROM customer INNER JOIN lineorder
  ON c_custkey = lo_custkey
  INNER JOIN part
  ON p_partkey = lo_partkey;
  ```

  其原始查询计划和改写后的计划如下：

  ![Rewrite-5](../_assets/Rewrite-5.png)

  同样，如果物化视图定义为 `t1 INNER JOIN t2 INNER JOIN t3`，而查询为 `LEFT OUTER JOIN t2 INNER JOIN t3`，那么查询也可以被改写。而且，在涉及超过三个表的情况下，也具备上述的改写能力。

- **情景二：两表 Join**

  两表 Join 的派生改写支持以下几种细分场景：

  ![Rewrite-6](../_assets/Rewrite-6.png)

  在场景一至九中，需要向改写结果补偿过滤谓词，以确保语义等效性。例如，创建以下物化视图：

  ```SQL
  CREATE MATERIALIZED VIEW join_mv3
  DISTRIBUTED BY hash(lo_orderkey)
  AS
  SELECT lo_orderkey, lo_linenumber, lo_revenue, c_custkey, c_address
  FROM lineorder LEFT OUTER JOIN customer
  ON lo_custkey = c_custkey;
  ```

  则 `join_mv3` 可以改写以下查询，其查询结果需补偿谓词 `c_custkey IS NOT NULL`：

  ```SQL
  SELECT lo_orderkey, lo_linenumber, lo_revenue, c_custkey, c_address
  FROM lineorder INNER JOIN customer
  ON lo_custkey = c_custkey;
  ```

  其原始查询计划和改写后的计划如下：

  ![Rewrite-7](../_assets/Rewrite-7.png)

  在场景十中， 需要 Left Outer Join 查询中包含右表中 `IS NOT NULL` 的过滤谓词，如 `=`、`<>`、`>`、`<`、`<=`、`>=`、`LIKE`、`IN`、`NOT LIKE` 或 `NOT IN`。例如，创建以下物化视图：

  ```SQL
  CREATE MATERIALIZED VIEW join_mv4
  DISTRIBUTED BY hash(lo_orderkey)
  AS
  SELECT lo_orderkey, lo_linenumber, lo_revenue, c_custkey, c_address
  FROM lineorder INNER JOIN customer
  ON lo_custkey = c_custkey;
  ```

  则 `join_mv4` 可以改写以下查询，其中 `customer.c_address = "Sb4gxKs7"` 为 `IS NOT NULL` 谓词：

  ```SQL
  SELECT lo_orderkey, lo_linenumber, lo_revenue, c_custkey, c_address
  FROM lineorder LEFT OUTER JOIN customer
  ON lo_custkey = c_custkey
  WHERE customer.c_address = "Sb4gxKs7";
  ```

  其原始查询计划和改写后的计划如下：

  ![Rewrite-8](../_assets/Rewrite-8.png)

## 聚合改写

StarRocks 异步物化视图的多表聚合查询改写支持所有聚合函数，包括 bitmap_union、hll_union 和 percentile_union 等。例如，创建以下物化视图：

```SQL
CREATE MATERIALIZED VIEW agg_mv1
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT 
  lo_orderkey, 
  lo_linenumber, 
  c_name, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey
GROUP BY lo_orderkey, lo_linenumber, c_name;
```

该物化视图可以改写以下查询：

```SQL
SELECT 
  lo_orderkey, 
  lo_linenumber, 
  c_name, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey
GROUP BY lo_orderkey, lo_linenumber, c_name;
```

其原始查询计划和改写后的计划如下：

![Rewrite-9](../_assets/Rewrite-9.png)

以下各节详细阐述了聚合改写功能可用的场景。

### 聚合上卷改写

StarRocks 支持通过聚合上卷改写查询，即 StarRocks 可以使用通过 `GROUP BY a,b` 子句创建的异步物化视图改写带有 `GROUP BY a` 子句的聚合查询。例如，`agg_mv1` 可以改写以下查询：

```SQL
SELECT 
  lo_orderkey, 
  c_name, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey
GROUP BY lo_orderkey, c_name;
```

其原始查询计划和改写后的计划如下：

![Rewrite-10](../_assets/Rewrite-10.png)

> **说明**
>
> 当前暂不支持 grouping set、grouping set with rollup 以及 grouping set with cube 的改写。

仅有部分聚合函数支持聚合上卷查询改写。下表展示了原始查询中的聚合函数与用于构建物化视图的聚合函数之间的对应关系。您可以根据自己的业务场景，选择相应的聚合函数构建物化视图。

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

没有相应 GROUP BY 列的 DISTINCT 聚合无法使用聚合上卷查询改写。但是，从 StarRocks v3.1 开始，如果聚合上卷对应 DISTINCT 聚合函数的查询没有 GROUP BY 列，但有等价的谓词，该查询也可以被相关物化视图改写，因为 StarRocks 可以将等价谓词转换为 GROUP BY 常量表达式。

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

### 聚合下推

从 v3.3.0 版本开始，StarRocks 支持物化视图查询改写的聚合下推功能。启用此功能后，聚合函数将在查询执行期间下推至 Scan Operator，并在执行 Join Operator 之前被物化视图改写。此举可以缓解 Join 操作导致的数据膨胀，从而提高查询性能。

系统默认禁用该功能。要启用此功能，必须将系统变量 `enable_materialized_view_agg_pushdown_rewrite` 设置为 `true`。

假设需要加速以下基于 SSB 的查询 `SQL1`：

```sql
-- SQL1
SELECT 
    LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE)
FROM lineorder l JOIN dates d 
ON l.LO_ORDERDATE = d.d_date 
GROUP BY LO_ORDERDATE 
ORDER BY LO_ORDERDATE;
```

`SQL1` 包含 `lineorder` 表内的聚合以及 `lineorder` 和 `dates` 表之间的 Join。聚合发生在 `lineorder` 内部，与 `dates` 的 Join 仅用于数据过滤。所以 `SQL1` 在逻辑上等同于以下 `SQL2`：

```sql
-- SQL2
SELECT 
    LO_ORDERDATE, sum(sum1), max(max1), bitmap_union_count(bitmap1)
FROM 
 (SELECT
  LO_ORDERDATE,  sum(LO_REVENUE) AS sum1, max(LO_REVENUE) AS max1, bitmap_union(to_bitmap(LO_REVENUE)) AS bitmap1
  FROM lineorder 
  GROUP BY LO_ORDERDATE) l JOIN dates d 
ON l.LO_ORDERDATE = d.d_date 
GROUP BY LO_ORDERDATE 
ORDER BY LO_ORDERDATE;
```

`SQL2` 将聚合提前，大量减少 Join 的数据量。您可以基于 `SQL2` 的子查询创建物化视图，并启用聚合下推以改写和加速聚合：

```sql
-- 创建物化视图 mv0
CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS
SELECT
  LO_ORDERDATE, 
  sum(LO_REVENUE) AS sum1, 
  max(LO_REVENUE) AS max1, 
  bitmap_union(to_bitmap(LO_REVENUE)) AS bitmap1
FROM lineorder 
GROUP BY LO_ORDERDATE;

-- 启用聚合下推
SET enable_materialized_view_agg_pushdown_rewrite=true;
```

此时，`SQL1` 将通过物化视图进行改写和加速。改写后的查询如下：

```sql
SELECT 
    LO_ORDERDATE, sum(sum1), max(max1), bitmap_union_count(bitmap1)
FROM 
 (SELECT LO_ORDERDATE, sum1, max1, bitmap1 FROM mv0) l JOIN dates d 
ON l.LO_ORDERDATE = d.d_date 
GROUP BY LO_ORDERDATE
ORDER BY LO_ORDERDATE;
```

请注意，只有部分支持聚合上卷改写的聚合函数可以下推。目前支持下推的聚合函数有：

- MIN
- MAX
- COUNT
- COUNT DISTINCT
- SUM
- BITMAP_UNION
- HLL_UNION
- PERCENTILE_UNION
- BITMAP_AGG
- ARRAY_AGG_DISTINCT

:::note
- 下推后的聚合函数需要进行上卷才能对齐原始语义。有关聚合上卷的更多说明，请参阅 [聚合上卷改写](#聚合上卷改写)。
- 聚合下推支持基于 Bitmap 或 HLL 函数的 Count Distinct 上卷改写。
- 聚合下推仅支持将查询中的聚合函数下推至 Join/Filter/Where Operator 之下的 Scan Operator 之上。
- 聚合下推仅支持基于单张表构建的物化视图进行查询改写和加速。
:::

### COUNT DISTINCT 改写

StarRocks 支持将 COUNT DISTINCT 计算改写为 BITMAP 类型的计算，从而使用物化视图实现高性能、精确的去重。例如，创建以下物化视图：

```SQL
CREATE MATERIALIZED VIEW distinct_mv
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT lo_orderkey, bitmap_union(to_bitmap(lo_custkey)) AS distinct_customer
FROM lineorder
GROUP BY lo_orderkey;
```

该物化视图可以改写以下查询：

```SQL
SELECT lo_orderkey, count(distinct lo_custkey) 
FROM lineorder 
GROUP BY lo_orderkey;
```

## 嵌套物化视图改写

StarRocks 支持使用嵌套物化视图改写查询。例如，创建以下物化视图 `join_mv2`、`agg_mv2` 和 `agg_mv3`：

```SQL
CREATE MATERIALIZED VIEW join_mv2
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_name, c_address
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey;


CREATE MATERIALIZED VIEW agg_mv2
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT 
  lo_orderkey, 
  lo_linenumber, 
  c_name, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM join_mv2
GROUP BY lo_orderkey, lo_linenumber, c_name;

CREATE MATERIALIZED VIEW agg_mv3
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT 
  lo_orderkey, 
  sum(total_revenue) AS total_revenue, 
  max(max_discount) AS max_discount 
FROM agg_mv2
GROUP BY lo_orderkey;
```

其关系如下：

![Rewrite-11](../_assets/Rewrite-11.png)

`agg_mv3` 可改写以下查询：

```SQL
SELECT 
  lo_orderkey, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey
GROUP BY lo_orderkey;
```

其原始查询计划和改写后的计划如下：

![Rewrite-12](../_assets/Rewrite-12.png)

## Union 改写

### 谓词 Union 改写

当物化视图的谓词范围是查询的谓词范围的子集时，可以使用 UNION 操作改写查询。

例如，创建以下物化视图：

```SQL
CREATE MATERIALIZED VIEW agg_mv4
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT 
  lo_orderkey, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder
WHERE lo_orderkey < 300000000
GROUP BY lo_orderkey;
```

该物化视图可以改写以下查询：

```SQL
select 
  lo_orderkey, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder
GROUP BY lo_orderkey;
```

其原始查询计划和改写后的计划如下：

![Rewrite-13](../_assets/Rewrite-13.png)

其中，`agg_mv5` 包含`lo_orderkey < 300000000` 的数据，`lo_orderkey >= 300000000` 的数据通过直接查询表 `lineorder` 得到，最终通过 Union 操作之后再聚合，获取最终结果。

### 分区 Union 改写

假设基于分区表创建了一个分区物化视图。当查询扫描的分区范围是物化视图最新分区范围的超集时，查询可被 UNION 改写。

例如，有如下的物化视图 `agg_mv4`。基表 `lineorder` 当前包含分区 `p1` 至 `p7`，物化视图也包含分区 `p1` 至 `p7`。

```SQL
CREATE MATERIALIZED VIEW agg_mv5
DISTRIBUTED BY hash(lo_orderkey)
PARTITION BY RANGE(lo_orderdate)
REFRESH MANUAL
AS
SELECT 
  lo_orderdate, 
  lo_orderkey, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder
GROUP BY lo_orderkey;
```

如果 `lineorder` 新增分区 `p8`，其范围为 `[("19990101"), ("20000101"))`，则以下查询可被 UNION 改写：

```SQL
SELECT 
  lo_orderdate, 
  lo_orderkey, 
  sum(lo_revenue) AS total_revenue, 
  max(lo_discount) AS max_discount 
FROM lineorder
GROUP BY lo_orderkey;
```

其原始查询计划和改写后的计划如下：

![Rewrite-14](../_assets/Rewrite-14.png)

如上所示，`agg_mv5` 包含来自分区 `p1` 到 `p7` 的数据，而分区 `p8` 的数据来源于 `lineorder`。最后，这两组数据使用 UNION 操作合并。

## 基于视图的物化视图查询改写

自 v3.1.0 起，StarRocks 支持基于视图创建物化视图。如果基于视图的查询为 SPJG 类型，StarRocks 将会内联展开查询，然后进行改写。默认情况下，对视图的查询会自动展开为对视图的基表的查询，然后进行透明匹配和改写。

然而，在实际场景中，数据分析师可能会基于复杂的嵌套视图进行数据建模，这些视图无法直接展开。因此，基于这些视图创建的物化视图无法改写查询。为了改进在上述情况下的能力，从 v3.3.0 开始，StarRock 优化了基于视图的物化视图查询改写逻辑。

### 基本原理

在先前的查询改写逻辑中，StarRocks 会将基于视图的查询展开为针对视图基表的查询。如果展开后查询的执行计划与 SPJG 模式不匹配，物化视图将无法改写查询。

为了解决这个问题，StarRocks 引入了一个新的算子 - LogicalViewScanOperator，该算子用于简化执行计划树的结构，且无需展开查询，使查询执行计划树尽量满足 SPJG 模式，从而优化查询改写。

以下示例展示了一个包含聚合子查询的查询，一个建立在子查询之上的视图，基于视图展开之后的查询，以及建立在视图之上的物化视图：

```SQL
-- 原始查询：
SELECT 
  v1.a,
  t2.b,
  v1.total
FROM(
  SELECT 
    a,
    sum(c) AS total
  FROM t1
  GROUP BY a
) v1
INNER JOIN t2 ON v1.a = t2.a;

-- 视图：
CREATE VIEW view_1 AS
SELECT 
  t1.a,
  sum(t1.c) AS total
FROM t1
GROUP BY t1.a;
    
-- 展开后的查询
SELECT 
  v1.a,
  t2.b,
  v1.total
FROM view_1 v1
JOIN t2 ON v1.a = t2.a;
    
-- 物化视图：
CREATE MATERIALIZED VIEW mv1
DISTRIBUTED BY hash(a)
REFRESH MANUAL
AS
SELECT 
  v1.a,
  t2.b,
  v1.total
FROM view_1 v1
JOIN t2 ON v1.a = t2.a;
```

原始查询的执行计划如下图左侧所示。由于 JOIN 内的 LogicalAggregateOperator 与 SPJG 模式不匹配，StarRocks 不支持这种情况下的查询改写。然而，如果将子查询定义为一个视图，原始查询可以展开为针对该视图的查询。通过 LogicalViewScanOperator，StarRocks 可以将不匹配的部分转换为 SPJG 模式，从而允许改写查询。

![img](../_assets/Rewrite-view-based.png)

### 使用

StarRocks 默认禁用基于视图的物化视图查询改写。

要启用此功能，您必须设置以下变量：

```SQL
SET enable_view_based_mv_rewrite = true;
```

### 使用场景

#### 基于单个视图的物化视图查询改写

StarRocks 支持通过基于单个视图的物化视图进行查询改写，包括聚合查询。

例如，您可以为 TPC-H Query 18 构建以下视图和物化视图：

```SQL
CREATE VIEW q18_view
AS
SELECT
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
FROM
  customer,
  orders,
  lineitem
WHERE
  o_orderkey IN (
    SELECT
      l_orderkey
    FROM
      lineitem
    GROUP BY
      l_orderkey having
        sum(l_quantity) > 315
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice;

CREATE MATERIALIZED VIEW q18_mv
DISTRIBUTED BY hash(c_custkey, o_orderkey)
REFRESH MANUAL
AS
SELECT * FROM q18_view;
```

物化视图可以改写以下两个查询：

```Plain
mysql> EXPLAIN LOGICAL SELECT * FROM q18_view;
+-------------------------------------------------------------------------------------------------------+
| Explain String                                                                                        |
+-------------------------------------------------------------------------------------------------------+
| - Output => [2:c_name, 1:c_custkey, 9:o_orderkey, 10:o_orderdate, 13:o_totalprice, 52:sum]            |
|     - SCAN [q18_mv] => [1:c_custkey, 2:c_name, 52:sum, 9:o_orderkey, 10:o_orderdate, 13:o_totalprice] |
              # highlight-start
|             MaterializedView: true                                                                    |
              # highlight-end
|             Estimates: {row: 9, cpu: 486.00, memory: 0.00, network: 0.00, cost: 243.00}               |
|             partitionRatio: 1/1, tabletRatio: 96/96                                                   |
|             1:c_custkey := 60:c_custkey                                                               |
|             2:c_name := 59:c_name                                                                     |
|             52:sum := 64:sum(l_quantity)                                                              |
|             9:o_orderkey := 61:o_orderkey                                                             |
|             10:o_orderdate := 62:o_orderdate                                                          |
|             13:o_totalprice := 63:o_totalprice                                                        |
+-------------------------------------------------------------------------------------------------------+
```

```Plain
mysql> EXPLAIN LOGICAL SELECT c_name, sum(`sum(l_quantity)`) FROM q18_view GROUP BY c_name;
+-----------------------------------------------------------------------------------------------------+
| Explain String                                                                                      |
+-----------------------------------------------------------------------------------------------------+
| - Output => [2:c_name, 59:sum]                                                                      |
|     - AGGREGATE(GLOBAL) [2:c_name]                                                                  |
|             Estimates: {row: 9, cpu: 306.00, memory: 306.00, network: 0.00, cost: 1071.00}          |
|             59:sum := sum(59:sum)                                                                   |
|         - EXCHANGE(SHUFFLE) [2]                                                                     |
|                 Estimates: {row: 9, cpu: 30.60, memory: 0.00, network: 30.60, cost: 306.00}         |
|             - AGGREGATE(LOCAL) [2:c_name]                                                           |
|                     Estimates: {row: 9, cpu: 61.20, memory: 30.60, network: 0.00, cost: 244.80}     |
|                     59:sum := sum(52:sum)                                                           |
|                 - SCAN [q18_mv] => [2:c_name, 52:sum]                                               |
                          # highlight-start
|                         MaterializedView: true                                                      |
                          # highlight-end
|                         Estimates: {row: 9, cpu: 306.00, memory: 0.00, network: 0.00, cost: 153.00} |
|                         partitionRatio: 1/1, tabletRatio: 96/96                                     |
|                         2:c_name := 60:c_name                                                       |
|                         52:sum := 65:sum(l_quantity)                                                |
+-----------------------------------------------------------------------------------------------------+
```

#### 基于视图的物化视图改写 JOIN 查询

StarRocks 支持对包含视图之间或视图与表之间的 JOIN 的查询进行改写，包括在 JOIN 上进行聚合。

例如，您可以创建以下视图和物化视图：

```SQL
CREATE VIEW view_1 AS
SELECT 
  l_partkey,
  l_suppkey,
  sum(l_quantity) AS total_quantity
FROM lineitem
GROUP BY 
  l_partkey,
  l_suppkey;


CREATE VIEW view_2 AS
SELECT 
  l_partkey,
  l_suppkey,
   sum(l_tax) AS total_tax
FROM lineitem
GROUP BY 
  l_partkey,
  l_suppkey;


CREATE MATERIALIZED VIEW mv_1 
DISTRIBUTED BY hash(l_partkey, l_suppkey) 
REFRESH MANUAL AS
SELECT 
  v1.l_partkey,
  v2.l_suppkey,
  total_quantity,
  total_tax
FROM view_1 v1
JOIN view_2 v2 ON v1.l_partkey = v2.l_partkey
AND v1.l_suppkey = v2.l_suppkey;
```

物化视图可以改写以下两个查询：

```Plain
mysql>  EXPLAIN LOGICAL
    -> SELECT v1.l_partkey,
    ->        v2.l_suppkey,
    ->        total_quantity,
    ->        total_tax
    -> FROM view_1 v1
    -> JOIN view_2 v2 ON v1.l_partkey = v2.l_partkey
    -> AND v1.l_suppkey = v2.l_suppkey;
+--------------------------------------------------------------------------------------------------------+
| Explain String                                                                                         |
+--------------------------------------------------------------------------------------------------------+
| - Output => [4:l_partkey, 25:l_suppkey, 17:sum, 37:sum]                                                |
|     - SCAN [mv_1] => [17:sum, 4:l_partkey, 37:sum, 25:l_suppkey]                                       |
              # highlight-start
|             MaterializedView: true                                                                     |
              # highlight-end
|             Estimates: {row: 799541, cpu: 31981640.00, memory: 0.00, network: 0.00, cost: 15990820.00} |
|             partitionRatio: 1/1, tabletRatio: 96/96                                                    |
|             17:sum := 43:total_quantity                                                                |
|             4:l_partkey := 41:l_partkey                                                                |
|             37:sum := 44:total_tax                                                                     |
|             25:l_suppkey := 42:l_suppkey                                                               |
+--------------------------------------------------------------------------------------------------------+
```

```Plain
mysql> EXPLAIN LOGICAL
    -> SELECT v1.l_partkey,
    ->        sum(total_quantity),
    ->        sum(total_tax)
    -> FROM view_1 v1
    -> JOIN view_2 v2 ON v1.l_partkey = v2.l_partkey
    -> AND v1.l_suppkey = v2.l_suppkey
    -> group by v1.l_partkey;
+--------------------------------------------------------------------------------------------------------------------+
| Explain String                                                                                                     |
+--------------------------------------------------------------------------------------------------------------------+
| - Output => [4:l_partkey, 41:sum, 42:sum]                                                                          |
|     - AGGREGATE(GLOBAL) [4:l_partkey]                                                                              |
|             Estimates: {row: 196099, cpu: 4896864.00, memory: 3921980.00, network: 0.00, cost: 29521223.20}        |
|             41:sum := sum(41:sum)                                                                                  |
|             42:sum := sum(42:sum)                                                                                  |
|         - EXCHANGE(SHUFFLE) [4]                                                                                    |
|                 Estimates: {row: 136024, cpu: 489686.40, memory: 0.00, network: 489686.40, cost: 19228831.20}      |
|             - AGGREGATE(LOCAL) [4:l_partkey]                                                                       |
|                     Estimates: {row: 136024, cpu: 5756695.20, memory: 489686.40, network: 0.00, cost: 18249458.40} |
|                     41:sum := sum(17:sum)                                                                          |
|                     42:sum := sum(37:sum)                                                                          |
|                 - SCAN [mv_1] => [17:sum, 4:l_partkey, 37:sum]                                                     |
                          # highlight-start
|                         MaterializedView: true                                                                     |
                          # highlight-end
|                         Estimates: {row: 799541, cpu: 28783476.00, memory: 0.00, network: 0.00, cost: 14391738.00} |
|                         partitionRatio: 1/1, tabletRatio: 96/96                                                    |
|                         17:sum := 45:total_quantity                                                                |
|                         4:l_partkey := 43:l_partkey                                                                |
|                         37:sum := 46:total_tax                                                                     |
+--------------------------------------------------------------------------------------------------------------------+
```

#### 基于视图的物化视图改写外表查询

您可以在 External Catalog 中的外表上构建视图，然后基于这些视图构建物化视图来改写查询。其使用方式类似于内部表。

## 基于 External Catalog 构建物化视图

StarRocks 支持基于 Hive Catalog、Hudi Catalog、Iceberg Catalog 和 Paimon Catalog 的外部数据源上构建异步物化视图，并支持透明地改写查询。基于 External Catalog 的物化视图支持大多数查询改写功能，但存在以下限制：

- 基于 Hudi、Paimon 和 JDBC Catalog 创建的物化视图不支持 Union 改写。
- 基于 Hudi、Paimon 和 JDBC Catalog 创建的物化视图不支持 View Delta Join 改写。
- 基于 Hudi 和 JDBC Catalog 创建的物化视图不支持分区增量刷新。

## 基于文本的物化视图改写

自 v3.3.0 起，StarRocks 支持基于文本的物化视图改写，极大地拓展了自身的查询改写能力。

### 基本原理

为实现基于文本的物化视图改写，StarRocks 将对查询（或其子查询）的抽象语法树与物化视图定义的抽象语法树进行比较。当双方匹配时，StarRocks 就可以基于物化视图改写该查询。基于文本的物化视图改写简单高效，与常规的 SPJG 类型物化视图查询改写相比限制更少。正确使用此功能可显著增强查询性能。

基于文本的物化视图改写不仅支持 SPJG 类型算子，还支持 Union、Window、Order、Limit 和 CTE 等算子。

### 使用

StarRocks 默认启用基于文本的物化视图改写。您可以通过将变量 `enable_materialized_view_text_match_rewrite` 设置为 `false` 来手动禁用此功能。

FE 配置项 `enable_materialized_view_text_based_rewrite` 用于控制是否在创建异步物化视图时构建抽象语法树。此功能默认启用。将此项设置为 `false` 将在系统级别禁用基于文本的物化视图改写。

变量 `materialized_view_subuqery_text_match_max_count` 用于控制系统比对子查询是否与物化视图定义匹配的最大次数。默认值为 `4`。增加此值同时也会增加优化器的耗时。

请注意，只有当物化视图满足时效性（数据一致性）要求时，才能用于基于文本的查询改写。您可以在创建物化视图时通过属性 `query_rewrite_consistency`手动设置一致性检查规则。更多信息，请参考 [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/materialized_view/CREATE_MATERIALIZED_VIEW.md)。

### **使用场景**

符合以下情况的查询可以被改写：

- 原始查询与物化视图的定义一致。
- 原始查询的子查询与物化视图的定义一致。

与常规的 SPJG 类型物化视图查询改写相比，基于文本的物化视图改写支持更复杂的查询，例如多层聚合。

:::info

- 建议您将需要匹配的查询封装至原始查询的子查询中。
- 请不要在物化视图的定义或原始查询的子查询中封装 ORDER BY 子句，否则查询将无法被改写。这是由于子查询中的 ORDER BY 子句会默认被消除。

:::

例如，您可以构建以下物化视图：

```SQL
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS
SELECT 
  user_id, 
  count(1) 
FROM (
  SELECT 
    user_id, 
    time, 
    bitmap_union(to_bitmap(tag_id)) AS a
  FROM user_tags 
  GROUP BY 
    user_id, 
    time) t
GROUP BY user_id;
```

该物化视图可以改写以下两个查询：

```SQL
SELECT 
  user_id, 
  count(1) 
FROM (
  SELECT 
    user_id, 
    time, 
    bitmap_union(to_bitmap(tag_id)) AS a
  FROM user_tags 
  GROUP BY 
    user_id, 
    time) t
GROUP BY user_id;
SELECT count(1)
FROM
（
    SELECT 
      user_id, 
      count(1) 
    FROM (
      SELECT 
        user_id, 
        time, 
        bitmap_union(to_bitmap(tag_id)) AS a
      FROM user_tags 
      GROUP BY 
        user_id, 
        time) t
    GROUP BY user_id
）m;
```

但是该物化视图无法改写以下包含 ORDER BY 子句的查询：

```SQL
SELECT 
  user_id, 
  count(1) 
FROM (
  SELECT 
    user_id, 
    time, 
    bitmap_union(to_bitmap(tag_id)) AS a
  FROM user_tags 
  GROUP BY 
    user_id, 
    time) t
GROUP BY user_id
ORDER BY user_id;
```

## 设置物化视图查询改写

您可以通过以下 Session 变量设置异步物化视图查询改写。

| **变量**                                    | **默认值** | **描述**                                                     |
| ------------------------------------------- | ---------- | ------------------------------------------------------------ |
| enable_materialized_view_union_rewrite | true | 是否开启物化视图 Union 改写。  |
| enable_rule_based_materialized_view_rewrite | true | 是否开启基于规则的物化视图查询改写功能，主要用于处理单表查询改写。 |
| nested_mv_rewrite_max_level | 3 | 可用于查询改写的嵌套物化视图的最大层数。类型：INT。取值范围：[1, +∞)。取值为 `1` 表示只可使用基于基表创建的物化视图来进行查询改写。 |

## 验证查询改写是否生效

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

## 禁用查询改写

StarRocks 默认开启基于 Default Catalog 创建的异步物化视图查询改写。您可以通过将 Session 变量 `enable_materialized_view_rewrite` 设置为 `false` 禁用该功能。

对于基于 External Catalog 创建的异步物化视图，你可以通过 [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/materialized_view/ALTER_MATERIALIZED_VIEW.md) 将物化视图 Property `force_external_table_query_rewrite` 设置为 `false` 来禁用此功能。

## 限制

单就物化视图查询改写能力，StarRocks 目前存在以下限制：

- StarRocks 不支持非确定性函数的改写，包括 rand、random、uuid 以及 sleep。
- StarRocks 不支持窗口函数的改写。
- 如果物化视图定义语句中包含 LIMIT、ORDER BY、UNION、EXCEPT、INTERSECT、MINUS、GROUPING SETS、WITH CUBE 或 WITH ROLLUP，则无法用于改写。
- 基于 External Catalog 的物化视图不保证查询结果强一致。
- 基于 JDBC Catalog 表构建的异步物化视图暂不支持查询改写。

针对基于视图的物化视图查询改写，StarRocks 目前存在以下限制：

- 目前，StarRocks 不支持分区 Union 改写。
- 如果视图包含随机函数，则不支持查询改写，包括 rand()、random()、uuid() 和 sleep()。
- 如果视图包含具有相同名称的列，则不支持查询改写。您必须为具有相同名称的列设置不同的别名。
- 用于创建物化视图的视图必须至少包含以下数据类型之一的列：整数类型、日期类型和字符串类型。以下示例中，因为 `total_cost` 为 DOUBLE 类型的列，所以无法创建查询该视图的物化视图。

  ```SQL
  CREATE VIEW v1 
  AS 
  SELECT sum(cost) AS total_cost
  FROM t1;
  ```
