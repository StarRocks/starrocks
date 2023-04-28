# Asynchronous materialized views

This topic explains how to understand, create, use, and manage an asynchronous materialized view. Asynchronous materialized view is available in StarRocks v2.4 and later.

> Compared to synchronous materialized views, asynchronous materialized views support multi-table joins and more aggregate functions. Asynchronous materialized views can be refreshed manually or by scheduled tasks, and you can choose to refresh only certain partitions instead of the entire materialized view, significantly reducing the cost of refresh. Additionally, asynchronous materialized views support various query rewrite scenarios, enabling automatic and transparent query acceleration.
> 

> For information on the scenario and usage of synchronous materialized views (Rollup), refer to the Synchronous Materialized View (Rollup) topic.
> 

## Overview

Databases often execute complex queries on large tables, which require multi-table joins and aggregations on tables containing billions of rows. Processing these queries can be expensive in terms of system resources and time.

To address these issues, StarRocks provides asynchronous materialized views. These views are special physical tables that hold pre-computed query results from one or more base tables. When you perform complex queries on the base table, StarRocks returns pre-computed results from the relevant materialized views to process the queries. This avoids repetitive complex calculations and improves query performance, which can be significant for frequently run or sufficiently complex queries.

Asynchronous materialized views are particularly useful for building mathematical models on your data warehouse. This approach provides a unified data specification to upper-layer applications, shields the underlying implementation, and protects the security of the raw data in the base tables.

### Understand materialized views in StarRocks

StarRocks v2.3 and earlier versions offered synchronized materialized views, which could be built only on a single table. Although synchronized materialized views, or Rollup, provide higher data freshness and lower refreshing costs, they have many limitations compared to asynchronous materialized views supported from v2.4 onwards. For instance, when building a synchronized materialized view to accelerate or rewrite queries, the choice of aggregation operators is limited.

The following table compares asynchronous materialized views (ASYNC MV) in StarRocks v2.5 and v2.4 with the synchronized materialized view (SYNC MV) in terms of features they support:

|  | Single-table aggregation | Multi-table join | Query rewrite | Refresh strategy | Base table |
| --- | --- | --- | --- | --- | --- |
| ASYNC MVs in v2.5 | Yes | Yes | Yes | <ul><li>Regularly triggered refresh</li><li>Manual refresh</li></ul> | Multiple tables from:<ul><li>Default catalog</li><li>External catalogs</li><li>Existing materialized views</li></ul> |
| ASYNC MVs in v2.4 | Yes | Yes | No | <ul><li>Regularly triggered refresh</li><li>Manual refresh</li></ul> | Multiple tables from the default catalog |
| SYNC MV (Rollup) | Limited choices of operators | No | Yes | Synchronous refresh during data loading | Single table in the default catalog |

### Basic Concepts

- **Base Table**
    
    Base tables are the tables that drive a materialized view. For StarRocks' asynchronous materialized views, base tables can be StarRocks native tables in the default catalog, tables in external catalogs (supported from v2.5), or even existing asynchronous materialized views (supported from v2.5). StarRocks supports creating asynchronous materialized views on all types of StarRocks tables.
    
- **Refresh**
    
    When you create an asynchronous materialized view, its data reflects only the state of the base tables at that time. When the data in the base tables changes, you need to refresh the materialized view to keep the changes synchronized. Currently, StarRocks supports two generic refreshing strategies: ASYNC (refreshing triggered regularly by tasks) and MANUAL (refreshing triggered manually by users).
    
- **Query Rewrite**
    
    Query rewrite means that when executing a query on base tables with materialized views built on, the system automatically judges whether the pre-computed results in the materialized view can be reused for the query. If they can be reused, the system will load the data directly from the relevant materialized view to avoid the time- and resource-consuming computations or joins. From v2.5, StarRocks supports automatic, transparent query rewrite based on the SPJG-type asynchronous materialized views that are created on the default catalog or an external catalog such as a Hive catalog, Hudi catalog, or Iceberg catalog.
    
    Base tables are the tables that drive a materialized view.
    
    For StarRocks' asynchronous materialized views, base tables can be StarRocks native tables in the default catalog, tables in external catalogs (supported from v2.5), or even existing asynchronous materialized views (supported from v2.5). StarRocks supports creating asynchronous materialized views on all types of StarRocks tables.
    

## Use Case

Materialized View can be used in following cases:

- **Accelerating Heavy Aggregation Queries**

If most queries in your data warehouse include the same sub-query with an aggregate function, and these queries consume a significant proportion of computing resources, you can create an asynchronous materialized view based on this sub-query. The materialized view will compute and store all results of the sub-query. After building the materialized view, StarRocks rewrites all queries containing the sub-query, loads intermediate results from the materialized view, and accelerates these queries.

- **Accelerate Multi-Table Join Queries**

If you need to regularly join multiple tables in your data warehouse to make a new wide table, you can build an asynchronous materialized view for these tables and set the ASYNC refreshing strategy to trigger refreshing tasks at fixed time intervals. After building the materialized view, query results are returned directly from the materialized view, avoiding the latency caused by JOIN operations.

- **Data Warehouse Layering**

If your data warehouse contains a mass of raw data, and queries require a complex set of ETL operations, you can build multiple layers of asynchronous materialized views to stratify the data in your data warehouse and decompose the query into a series of simple sub-queries. This can significantly reduce repetitive computation and, more importantly, help your DBA identify problems with ease and efficiency. Data warehouse layering also helps decouple the raw data and statistical data, protecting the security of sensitive raw data.

- **DataLake Acceleration**

Querying a DataLake can be slow due to network latency and object storage throughput. Building a materialized view on top of the DataLake to clean and filter the raw data can speed up subsequent queries. StarRocks can automatically refresh the materialized view when external data changes, so you don't have to worry about data consistency. Additionally, the SQL optimizer can rewrite queries to use available materialized views, so you don't have to modify your queries.

## Create materialized view

Asynchronous materialized view can be created on top of: 

- OLAP Table: regular StarRocks table with native storage
- DataLake: Hive/Hudi/Iceberg/DeltaLake
- Jdbc Table:
- Materialized View: this pattern is called MV on MV

### Before start

The following examples involve two base tables:

- The table `goods` records the item ID `item_id1`, the item name `item_name`, and the item price `price`.
- The table `order_list` records the order ID `order_id`, client ID `client_id`, item ID `item_id2`, and order date `order_date`.

The column `goods.item_id1` is equivalent to the column `order_list.item_id2`.

Execute the following statements to create the tables and insert data into them:

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

The scenario in the following example demands frequent calculations of the total of each order. It requires frequent joins of the two base tables and intensive usage of the aggregate function `sum()`. Besides, the business scenario demands the data refresh at an interval of one day.

The query statement is as follows:

```SQL
SELECT
    order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

### Create the materialized view

You can create a materialized view based on a specific query statement using [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md).

Based on the table `goods`, `order_list`, and the query statement mentioned above, the following example creates the materialized view `order_mv` to analyze the total of each order. The materialized view is set to refresh itself at an interval of one day.

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

> **NOTE**
>
> - You must specify a bucketing strategy when creating an asynchronous materialized view.
> - You can set different partitioning and bucketing strategies for an asynchronous materialized view from those of its base tables.
> - Asynchronous materialized views support a dynamic partitioning strategy in a longer span. For example, if the base table is partitioned at an interval of one day, you can set the materialized view to be partitioned at an interval of one month.
> - The query statement that is used to create an asynchronous materialized view must include the partition keys and bucket keys of the materialized views.
> - The query statement used to create a materialized view does not support random functions, including rand(), random(), uuid(), and sleep().

- **About refresh mechanisms of asynchronous materialized views**
    
    Currently, StarRocks supports two ON DEMAND refresh strategies: manual refresh and regular refresh at a fixed time interval.
    
    In StarRocks v2.5, asynchronous materialized views support a variety of refreshing mechanisms:
    
    - If an MV has many large partitions, each refresh can consume a significant amount of resources. To address this, StarRocks v2.5 supports splitting refresh tasks. You can specify the maximum number of partitions to be refreshed, and StarRocks performs refresh in batches, with a batch size equal to or smaller than the specified maximum number of partitions. This feature ensures that large asynchronous materialized views are stably refreshed, enhancing the stability and robustness of data modeling.
    - You can specify the time to live (TTL) for partitions of an asynchronous materialized view, reducing the storage size taken by the materialized view.
    - You can specify the refresh range to refresh only the latest few partitions, reducing the refresh overhead.
    
    For more information, see the **PROPERTIES** section in CREATE MATERIALIZED VIEW - Parameters. You can also modify the mechanisms of an existing asynchronous materialized view using ALTER MATERIALIZED VIEW.
    
- **About nested materialized views**
    
    StarRocks v2.5 now supports creating nested asynchronous materialized views. You can build asynchronous materialized views based on existing asynchronous materialized views. The refreshing strategy for each materialized view does not affect the materialized views on the upper or lower layers. Currently, StarRocks does not limit the number of nesting levels. However, in a production environment, we recommend that the number of nesting layers does not exceed three.
    
- **About external catalog materialized views**
    
    StarRocks v2.5 now supports creating asynchronous materialized views based on Hive, Hudi, and Iceberg catalogs. To create an external catalog materialized view, follow the same steps as creating a general asynchronous materialized view, but with the following usage restrictions:
    
    - Strict consistency is not guaranteed between the materialized view and the base tables in the external catalog.
    - Currently, building asynchronous materialized views based on external resources is not supported.
    - Currently, StarRocks cannot perceive if the data of the base tables in the external catalog has changed, so all partitions are refreshed by default every time the refreshing task is triggered. If you want to refresh only some of the partitions, you can manually refresh the materialized view using the [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md) statement and specify the partition you want to refresh.

## Query Materialized View

There are two methods to query the materialized view:

- Query it directly: treat the materialized view as a regular table and query it directly.
- Transparently rewrite: the optimizer will attempt to rewrite your query to use an available materialized view by using pattern match algorithms. In this way, you don't need to change your query.

### Query directly

The asynchronous materialized view you have created is essentially a physical table that contains a complete set of pre-computed results in accordance with the query statement. Therefore, you can directly query the materialized view after it has been refreshed for the first time.

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

> **NOTE**
>
> You can directly query an asynchronous materialized view, but the results may be inconsistent with what you get from the query on its base tables.

### Transparently Rewrite

StarRocks v2.5 now offers automatic and transparent query rewriting based on SPJG-type asynchronous materialized views. These are materialized views whose plans only include Scan, Filter, Project, and Aggregate operators. The query rewriting based on SPJG-type materialized views includes single table query rewriting, join query rewriting, aggregation query rewriting, union query rewriting, and query rewriting based on nested materialized views.

Currently, StarRocks supports query rewriting on asynchronous materialized views created on either the default catalog or an external catalog (e.g., Hive, Hudi, or Iceberg). When querying data in the default catalog, StarRocks ensures strong consistency of the results between the rewritten query and the original query by excluding materialized views whose data is inconsistent with the base table. If the data in a materialized view expires, it will not be used as a candidate materialized view. However, when querying data in external catalogs, StarRocks does not guarantee strong consistency of the results because it cannot detect data changes in external catalogs.

### Enable query rewrite

**Enable query rewrite based on the default catalog materialized views**
    
  StarRocks enables asynchronous materialized view query rewrite by default. You can enable or disable this feature with the session variable enable_materialized_view_rewrite.
  
  ```
  SET GLOBAL enable_materialized_view_rewrite = { true | false };
  ```
    
**[Experimental] Enable query rewrite based on the external catalog materialized views**
    
Because StarRocks does not ensure a strong consistency of the results when you query data in external catalogs using asynchronous materialized views, the query rewrite based on the external catalog materialized views is disabled by default. You can enable this feature for an external catalog materialized view by adding the property `"force_external_table_query_rewrite" = "true"` when creating the materialized view.

  Example:

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

### Rewrite queries in View Delta Join scenarios

StarRocks now supports rewriting queries based on asynchronous materialized views with Delta Join, which allows queried tables to be a subset of the materialized view's base tables. For example, queries of the form `table_a INNER JOIN table_b` can be rewritten by materialized views of the form `table_a INNER JOIN table_b INNER JOIN/LEFT OUTER JOIN table_c`, where `table_b INNER JOIN/LEFT OUTER JOIN table_c` is the Delta Join. This feature provides transparent acceleration for such queries, preserving the query's flexibility and avoiding the high cost of building wide tables.

View Delta Join queries can be rewritten only when the following requirements are met:

- The Delta Join must be Inner Join or Left Outer Join.
- If the Delta Join is Inner Join, the keys to be joined must be the corresponding Foreign/Primary/Unique Key and NOT NULL.

  For example, the materialized view is of the form `A INNER JOIN B ON (A.a1 = B.b1) INNER JOIN C ON (B.b2 = C.c1)`, and the query is of the form `A INNER JOIN B ON (A.a1 = B.b1)`. In this case, `B INNER JOIN C ON (B.b2 = C.c1)` is the Delta Join. `B.b2` must be the Foreign Key of B and must be NOT NULL, and `C.c1` must be the Primary Key or Unique Key of C.

- If the Delta Join is Left Outer Join, the keys to be joined must be the corresponding Foreign/Primary/Unique Key.

  For example, the materialized view is of the form `A INNER JOIN B ON (A.a1 = B.b1) LEFT OUTER JOIN C ON (B.b2 = C.c1)`, and the query is of the form `A INNER JOIN B ON (A.a1 = B.b1)`. In this case, `B LEFT OUTER JOIN C ON (B.b2 = C.c1)` is the Delta Join. `B.b2` must be the Foreign Key of B, and `C.c1` must be the Primary Key or Unique Key of C.

To implement the above constraints, you must define the Foreign Key constraints of a table using the property `foreign_key_constraints` when creating the table. For more information, see [CREATE TABLE - PROPERTIES](../sql-reference/sql-statements/data-definition/CREATE%20TABLE.md#parameters).

> **CAUTION**
>
> The Foreign Key constraints are only used for query rewrite. Foreign Key constraint checks are not guaranteed when data is loaded into the table. You must ensure the data loaded into the table meets the constraints.

The following example defines multiple Foreign Keys when creating the table `lineorder`:

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
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 192
PROPERTIES (
-- Define Foreign Keys in foreign_key_constraints
"foreign_key_constraints" = "
    (lo_custkey) REFERENCES customer(c_custkey);
    (lo_partkey) REFERENCES ssb.part(p_partkey);
    (lo_suppkey) REFERENCES supplier(s_suppkey);
    (lo_orderdate) REFERENCES dates(d_datekey)
"
);
```

### Configure query rewrite

You can configure the asynchronous materialized view query rewrite through the following session variables:

| **Variable**                                | **Default** | **Description**                                              |
| ------------------------------------------- | ----------- | ------------------------------------------------------------ |
| enable_materialized_view_union_rewrite      | true        | Boolean value to control if to enable materialized view Union query rewrite. |
| enable_rule_based_materialized_view_rewrite | true        | Boolean value to control if to enable rule-based materialized view query rewrite. This variable is mainly used in single-table query rewrite. |
| nested_mv_rewrite_max_level                 | 3           | The maximum levels of nested materialized views that can be used for query rewrite. Type: INT. Range: [1, +∞). The value of `1` indicates that materialized views created on other materialized views will not be used for query rewrite. |

### Check if a query is rewritten

You can check if your query is rewritten by viewing its query plan using the EXPLAIN statement. If the field `TABLE` under the section `OlapScanNode` shows the name of the corresponding materialized view, it means that the query has been rewritten based on the materialized view.

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

## Manage Materialized View

These are DDL commands to manage the Materialized View:

- [CREATE MATERIAIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md)
- [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md)
- [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER%20MATERIALIZED%20VIEW.md)
- [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md)
- [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL%20REFRESH%20MATERIALIZED%20VIEW.md)
- [SHOW MATERIALIZED VIEW](../sql-reference//sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md)
- [SHOW CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20MATERIALIZED%20VIEW.md)

### Manually refresh an asynchronous materialized view

You can refresh an asynchronous materialized view that is created with the ASYNC or the MANUAL refreshing strategy via [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md). StarRocks v2.5 supports refreshing specific partitions of an asynchronous materialized view by specifying partition names.

```
REFRESH MATERIALIZED VIEW order_mv;
```

You can cancel a refresh task using [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL%20REFRESH%20MATERIALIZED%20VIEW.md).

### Alter an asynchronous materialized view

You can alter the property of an asynchronous materialized view using [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER%20MATERIALIZED%20VIEW.md).

- Rename an asynchronous materialized view.

  ```SQL
  ALTER MATERIALIZED VIEW order_mv RENAME order_total;
  ```

- Alter the refreshing interval of an asynchronous materialized view to 2 days.

  ```SQL
  ALTER MATERIALIZED VIEW order_mv REFRESH ASYNC EVERY(INTERVAL 2 DAY);
  ```

### Show asynchronous materialized views

You can view the asynchronous materialized views in your database by using [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md) or querying the system metadata table in Information Schema.

- Check all asynchronous materialized views in your database.

  ```SQL
  SHOW MATERIALIZED VIEWS;
  ```

- Check a specific asynchronous materialized view.

  ```SQL
  SHOW MATERIALIZED VIEWS WHERE NAME = "order_mv";
  ```

- Check specific asynchronous materialized views by matching the name.

  ```SQL
  SHOW MATERIALIZED VIEWS WHERE NAME LIKE "order%";
  ```

- Check all asynchronous materialized views by querying the metadata table in Information Schema.

  ```SQL
  SELECT * FROM information_schema.materialized_views;
  ```

### Check the asynchronous materialized view definition

You can check the query used to create an asynchronous materialized view via [SHOW CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20MATERIALIZED%20VIEW.md).

```SQL
SHOW CREATE MATERIALIZED VIEW order_mv;
```

### Check the asynchronous materialized view execution status

You can check the execution (building or refreshing) status of an asynchronous materialized view by querying the `tasks` and `task_runs` metadata tables in StarRocks' [Information Schema](../administration/information_schema.md). 

The following example checks the execution status of the materialized view that was created most recently:

1. Check the `TASK_NAME` of the most recent task in the table `tasks`.

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

2. Check the execution status in the table `task_runs` using the `TASK_NAME` you have found.

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

### Drop an asynchronous materialized view

You can drop an asynchronous materialized view via [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md).