# Asynchronous materialized views

This topic describes how to understand, create, use, and manage an asynchronous materialized view. Asynchronous materialized views are supported from StarRocks v2.4 onwards.

Compared with synchronous materialized views, asynchronous materialized views support multi-table join and more aggregate functions. The refresh of asynchronous materialized views can be triggered manually or by scheduled tasks. You can also refresh some of the partitions instead of the whole materialized view, greatly reducing the cost of refresh. In addition, asynchronous materialized views support a variety of query rewrite scenarios, allowing automatic, transparent query acceleration.

For the scenario and usage of the synchronized materialized views (Rollup), see [Synchronous materialized view (Rollup)](../using_starrocks/Materialized_view-single_table.md).

## Overview

Applications in databases often perform complex queries on large tables. Such queries involve multi-table joins and aggregations on tables that contain billions of rows. Processing these queries can be expensive, in terms of system resources and the time it takes to compute the results.

Asynchronous materialized views in StarRocks are designed to tackle these issues. An asynchronous materialized view is a special physical table that holds pre-computed query results from one or more base tables. When you perform complex queries on the base table, StarRocks returns the pre-computed results from the relevant materialized views to process these queries. This way, query performance can be improved because repetitive complex calculations are avoided. This performance difference can be significant when a query is run frequently or is sufficiently complex.

Additionally, asynchronous materialized views are especially useful for building mathematical models upon your data warehouse. By doing so, you can provide a unified data specification to upper-layer applications, shield the underlying implementation, or protect the raw data security of the base tables.

### Understand materialized views in StarRocks

StarRocks v2.3 and earlier versions provided a synchronized materialized view that can be built only on a single table. Synchronized materialized views, or the Rollup, retain higher data freshness and lower refreshing costs. However, compared to asynchronous materialized views supported from v2.4 onwards, synchronized materialized views have many limitations. You have limited choices of aggregation operators when you want to build a synchronized materialized view to accelerate or rewrite your queries.

The following table compares the asynchronous materialized views (ASYNC MVs) in StarRocks v2.5, v2.4, and the synchronized materialized view (SYNC MV) in the perspective of features that they support:

|                       | **Single-table aggregation** | **Multi-table join** | **Query rewrite** | **Refresh strategy** | **Base table** |
| --------------------- | ---------------------------- | -------------------- | ----------------- | -------------------- | -------------- |
| **ASYNC MVs in v2.5** | Yes | Yes | Yes | <ul><li>Regularly triggered refresh</li><li>Manual refresh</li></ul> | Multiple tables from:<ul><li>Default catalog</li><li>External catalogs</li><li>Existing materialized views</li></ul> |
| **ASYNC MVs in v2.4** | Yes | Yes | No | <ul><li>Regularly triggered refresh</li><li>Manual refresh</li></ul> | Multiple tables from the default catalog |
| **SYNC MV (Rollup)**  | Limited choices of operators | No | Yes | Synchronous refresh during data loading | Single table in the default catalog |

### Basic concepts

- **Base table**

  Base tables are the driving tables of a materialized view.

  For StarRocks' asynchronous materialized views, base tables can be StarRocks native tables in the [default catalog](../data_source/catalog/default_catalog.md), tables in external catalogs (supported from v2.5), or even existing asynchronous materialized views (supported from v2.5). StarRocks supports creating asynchronous materialized views on all [types of StarRocks tables](../table_design/Data_model.md).

- **Refresh**

  When you create an asynchronous materialized view, its data reflects only the state of the base tables at that time. When the data in the base tables change, you need to refresh the materialized view to keep the changes synchronized.

  Currently, StarRocks supports two generic refreshing strategies: ASYNC (refreshing triggered regularly by tasks) and MANUAL (refreshing triggered manually by users).

- **Query rewrite**

  Query rewrite means that when executing a query on base tables with materialized views built on, the system automatically judges whether the pre-computed results in the materialized view can be reused for the query. If they can be reused, the system will load the data directly from the relevant materialized view to avoid the time- and resource-consuming computations or joins.

  From v2.5, StarRocks supports automatic, transparent query rewrite based on the SPJG-type asynchronous materialized views that are created on the default catalog or an external catalog such as a Hive catalog, Hudi catalog, or Iceberg catalog.

## Decide when to create a materialized view

You can create an asynchronous materialized view if you have the following demands in your data warehouse environment:

- **Accelerating queries with repetitive aggregate functions**

  Suppose that most queries in your data warehouse include the same sub-query with an aggregate function, and these queries have consumed a huge proportion of your computing resources. Based on this sub-query, you can create an asynchronous materialized view, which will compute and store all results of the sub-query. After the materialized view is built, StarRocks rewrites all queries that contain the sub-query, loads the intermediate results stored in the materialized view, and thus accelerates these queries.

- **Regular JOIN of multiple tables**

  Suppose that you need to regularly join multiple tables in your data warehouse to make a new wide table. You can build an asynchronous materialized view for these tables, and set the ASYNC refreshing strategy that triggers refreshing tasks at a fixed time interval. After the materialized view is built, query results are returned directly from the materialized view, and thus the latency caused by JOIN operations is avoided.

- **Data warehouse layering**

  Suppose that your data warehouse contains a mass of raw data, and queries in it require a complex set of ETL operations. You can build multiple layers of asynchronous materialized views to stratify the data in your data warehouse, and thus decompose the query into a series of simple sub-queries. It can significantly reduce repetitive computation, and, more importantly, help your DBA identify the problem with ease and efficiency. Beyond that, data warehouse layering helps decouple the raw data and statistical data, protecting the security of sensitive raw data.

## Create an asynchronous materialized view

StarRocks supports creating asynchronous materialized views on one or more base tables from the default catalog and external catalogs. Base tables support all StarRocks table types. You can also build an asynchronous materialized view on existing materialized views.

### Before you begin

#### Enable asynchronous materialized view

To use the asynchronous materialized view feature, you need to set the FE configuration item `enable_experimental_mv` to `true` using the following statement:

```SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_mv"="true");
```

> **NOTE**
>
> From v2.5.2 onwards, this feature is enabled by default. You do not need to manually enable it.

#### Prepare base tables

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

  In StarRocks v2.5, asynchronous materialized views further support a variety of asynchronous refreshing mechanisms:

  - If an MV has many large partitions, each refresh can consume a large amount of resources. In v2.5, StarRocks supports splitting refresh tasks. You can specify the maximum number of partitions to be refreshed, and StarRocks performs refresh in batches, with a batch size smaller or equal to the specified maximum number of partitions. This feature ensures large asynchronous materialized views are stably refreshed, enhancing the stability and robustness of data modeling.
  - You can specify the time to live (TTL) for partitions of an asynchronous materialized view, reducing the storage size taken by the materialized view.
  - You can specify the refresh range to refresh only the latest few partitions, reducing the refresh overhead.

  For more information, see the **PROPERTIES** section in [CREATE MATERIALIZED VIEW - Parameters](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md#parameters). You can also modify the mechanisms of an existing asynchronous materialized view using [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER%20MATERIALIZED%20VIEW.md).

- **About nested materialized views**

  StarRocks v2.5 supports creating nested asynchronous materialized views. You can build asynchronous materialized views based on existing asynchronous materialized views. The refreshing strategy for each materialized view does not affect the materialized views on the upper or lower layers. Currently, StarRocks does not limit the number of nesting levels. In a production environment, we recommend that the number of nesting layers does not exceed THREE.

- **About external catalog materialized views**

  StarRocks v2.5 supports creating asynchronous materialized views based on Hive catalog, Hudi catalog and Iceberg catalog. An external catalog materialized view is created in the same way as a general asynchronous materialized view is created, with the following usage restrictions:

  - Strict consistency is not guaranteed between the materialized view and the base tables in the external catalog.
  - Currently, building asynchronous materialized views based on external resources is not supported.
  - Currently, StarRocks cannot perceive the data changes on the base tables in Iceberg catalogs and Hudi catalogs, so all partitions are refreshed by default every time the refreshing task is triggered. If you want to refresh only some of the partitions, you can manually refresh the materialized view using the [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md) statement and specify the partition you want to refresh.
  - From v2.5.5 onwards, StarRocks can periodically refresh the cached metadata of the frequently accessed Hive catalogs to perceive data changes. You can configure the Hive metadata cache refresh through the following FE dynamic parameters:

    | Configuration item                                           | Default                              | Description                          |
    | ------------------------------------------------------------ | ------------------------------------ | ------------------------------------ |
    | enable_background_refresh_connector_metadata | `true` in v3.0<br />`false` in v2.5  | Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. `true` indicates to enable the Hive metadata cache refresh, and `false` indicates to disable it.  |
    | background_refresh_metadata_interval_millis  | `600000` (10 minutes)  | The interval between two consecutive Hive metadata cache refreshes. Unit: millisecond.  |
    | background_refresh_metadata_time_secs_since_last_access_secs | `86400` (24 hours) | The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cached metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata. Unit: second.  |

    You can modify these FE dynamic parameters using the [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN%20SET%20CONFIG.md) command.

## Manually refresh an asynchronous materialized view

You can refresh an asynchronous materialized view regardless of its refreshing strategy via [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md). StarRocks v2.5 supports refreshing specific partitions of an asynchronous materialized view by specifying partition names. StarRocks v3.1 supports making a synchronous call of the refresh task, and the SQL statement is returned only when the task succeeds or fails.

```SQL
-- Refresh the materialized view via an asynchronous call (default).
REFRESH MATERIALIZED VIEW order_mv;
-- Refresh the materialized view via a synchronous call.
REFRESH MATERIALIZED VIEW order_mv WITH SYNC MODE;
```

You can cancel a refresh task submitted via an asynchronous call using [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL%20REFRESH%20MATERIALIZED%20VIEW.md).

## Query the asynchronous materialized view

The asynchronous materialized view you created is essentially a physical table that contains the complete set of pre-computed results in accordance with the query statement. Therefore, you can directly query the materialized view after the materialized view is refreshed for the first time.

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

## Rewrite queries with the asynchronous materialized view

StarRocks v2.5 supports automatic and transparent query rewrite based on the SPJG-type asynchronous materialized views. The SPJG-type materialized views refer to materialized views whose plan only includes Scan, Filter, Project, and Aggregate types of operators. The SPJG-type materialized views query rewrite includes single table query rewrite, Join query rewrite, aggregation query rewrite, Union query rewrite and query rewrite based on nested materialized views.

Currently, StarRocks supports rewriting queries on asynchronous materialized views that are created on the default catalog or an external catalog such as a Hive catalog, Hudi catalog, or Iceberg catalog. When querying data in the default catalog, StarRocks ensures strong consistency of results between the rewritten query and the original query by excluding materialized views whose data is inconsistent with the base table. When the data in a materialized view expires, the materialized view will not be used as a candidate materialized view. When querying data in external catalogs, StarRocks does not ensure strong consistency of the results because StarRocks cannot perceive the data changes in external catalogs.

### Enable query rewrite

- **Enable query rewrite based on the default catalog materialized views**

  StarRocks enables asynchronous materialized view query rewrite by default. You can enable or disable this feature with the session variable enable_materialized_view_rewrite.

  ```SQL
  SET GLOBAL enable_materialized_view_rewrite = { true | false };
  ```

- **[Experimental] Enable query rewrite based on the external catalog materialized views**

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

### Rewrite queries with Derivable Join

From v2.5.9 onwards, StarRocks supports rewriting queries with a join that can be derived from the corresponding asynchronous materialized view, that is, an asynchronous materialized view with a certain pattern of join can rewrite queries with a different pattern of join as long as both joins have the same joined tables and columns and meet some requirements.

The following table specifies the correspondence of the join pattern in the materialized view and the join pattern in the queries that can be rewritten (`A` and `B` indicate the joined tables, `a1` indicates the joined column in `A`, and `b1` indicates the joined column in `B`):

| **Join in the asynchronous** **materialized view** | **Join in the rewritable** **query** | **Constraints**                                              |
| -------------------------------------------------- | ------------------------------------ | ------------------------------------------------------------ |
| LEFT OUTER JOIN ON A.a1 = B.b1                     | INNER JOIN ON A.a1 = B.b1            | None                                                         |
| LEFT OUTER JOIN ON A.a1 = B.b1                     | LEFT ANTI JOIN ON A.a1 = B.b1        | None                                                         |
| RIGHT OUTER JOIN ON A.a1 = B.b1                    | INNER JOIN ON A.a1 = B.b1            | None                                                         |
| RIGHT OUTER JOIN ON A.a1 = B.b1                    | RIGHT ANTI JOIN ON A.a1 = B.b1       | None                                                         |
| INNER JOIN ON A.a1 = B.b1                          | LEFT SEMI JOIN ON A.a1 = B.b1        | The joined column in the right table must be the Unique Key or Primary Key. |
| INNER JOIN ON A.a1 = B.b1                          | RIGHT SEMI JOIN ON A.a1 = B.b1       | The joined column in the left table must be the Unique Key or Primary Key. |
| FULL OUTER JOIN ON A.a1 = B.b1                     | LEFT OUTER JOIN ON A.a1 = B.b1       | At least one NOT NULL column in the left table.              |
| FULL OUTER JOIN ON A.a1 = B.b1                     | RIGHT OUTER JOIN ON A.a1 = B.b1      | At least one NOT NULL column in the right table.             |
| FULL OUTER JOIN ON A.a1 = B.b1                     | INNER JOIN ON A.a1 = B.b1            | At least one NOT NULL column in both the left and right table. |

For example, if you create an asynchronous materialized view as follows:

```SQL
CREATE MATERIALIZED VIEW derivable_join_mv
DISTRIBUTED BY hash(lo_orderkey)
AS
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_custkey, c_name, c_address
FROM lineorder LEFT OUTER JOIN customer
ON lo_custkey = c_custkey;
```

The materialized view can rewrite queries in the following pattern:

```SQL
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_name, c_address
FROM lineorder INNER JOIN customer
ON lo_custkey = c_custkey;
```

The query is rewritten as follows:

```SQL
SELECT lo_orderkey, lo_linenumber, lo_revenue, c_name, c_address
FROM derivable_join_mv
WHERE c_custkey IS NOT NULL;
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

## Manage an asynchronous materialized view

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

You can view the asynchronous materialized views in your database by using [SHOW MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md) or querying the system metadata table in Information Schema.

- Check all asynchronous materialized views in your database.

  ```SQL
  SHOW MATERIALIZED VIEW;
  ```

- Check a specific asynchronous materialized view.

  ```SQL
  SHOW MATERIALIZED VIEW WHERE NAME = "order_mv";
  ```

- Check specific asynchronous materialized views by matching the name.

  ```SQL
  SHOW MATERIALIZED VIEW WHERE NAME LIKE "order%";
  ```

- Check all asynchronous materialized views by querying the metadata table in Information Schema.

  ```SQL
  SELECT * FROM information_schema.materialized_views;
  ```

### Check the definition of asynchronous materialized view

You can check the query used to create an asynchronous materialized view via [SHOW CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20MATERIALIZED%20VIEW.md).

```SQL
SHOW CREATE MATERIALIZED VIEW order_mv;
```

### Check the execution status of asynchronous materialized view

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

```Plain
DROP MATERIALIZED VIEW order_mv;
```
