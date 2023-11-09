---
displayed_sidebar: "English"
---

# Asynchronous materialized views

This topic describes how to understand, create, use, and manage an asynchronous materialized view. Asynchronous materialized views are supported from StarRocks v2.4 onwards.

Compared with synchronous materialized views, asynchronous materialized views support multi-table join and more aggregate functions. The refresh of asynchronous materialized views can be triggered manually or by scheduled tasks. You can also refresh some of the partitions instead of the whole materialized view, greatly reducing the cost of refresh. In addition, asynchronous materialized views support a variety of query rewrite scenarios, allowing automatic, transparent query acceleration.

For the scenario and usage of the synchronous materialized views (Rollup), see [Synchronous materialized view (Rollup)](../using_starrocks/Materialized_view-single_table.md).

## Overview

Applications in databases often perform complex queries on large tables. Such queries involve multi-table joins and aggregations on tables that contain billions of rows. Processing these queries can be expensive, in terms of system resources and the time it takes to compute the results.

Asynchronous materialized views in StarRocks are designed to tackle these issues. An asynchronous materialized view is a special physical table that holds pre-computed query results from one or more base tables. When you perform complex queries on the base table, StarRocks returns the pre-computed results from the relevant materialized views to process these queries. This way, query performance can be improved because repetitive complex calculations are avoided. This performance difference can be significant when a query is run frequently or is sufficiently complex.

Additionally, asynchronous materialized views are especially useful for building mathematical models upon your data warehouse. By doing so, you can provide a unified data specification to upper-layer applications, shield the underlying implementation, or protect the raw data security of the base tables.

### Understand materialized views in StarRocks

StarRocks v2.3 and earlier versions provided a synchronous materialized view that can be built only on a single table. Synchronous materialized views, or the Rollup, retain higher data freshness and lower refreshing costs. However, compared to asynchronous materialized views supported from v2.4 onwards, synchronous materialized views have many limitations. You have limited choices of aggregation operators when you want to build a synchronous materialized view to accelerate or rewrite your queries.

The following table compares the asynchronous materialized views (ASYNC MV) and the synchronous materialized view (SYNC MV) in StarRocks in the perspective of features that they support:

|                       | **Single-table aggregation** | **Multi-table join** | **Query rewrite** | **Refresh strategy** | **Base table** |
| --------------------- | ---------------------------- | -------------------- | ----------------- | -------------------- | -------------- |
| **ASYNC MV** | Yes | Yes | Yes | <ul><li>Asynchronous refresh</li><li>Manual refresh</li></ul> | Multiple tables from:<ul><li>Default catalog</li><li>External catalogs (v2.5)</li><li>Existing materialized views (v2.5)</li><li>Existing views (v3.1)</li></ul> |
| **SYNC MV (Rollup)**  | Limited choices of aggregate functions | No | Yes | Synchronous refresh during data loading | Single table in the default catalog |

### Basic concepts

- **Base table**

  Base tables are the driving tables of a materialized view.

  For StarRocks' asynchronous materialized views, base tables can be StarRocks native tables in the [default catalog](../data_source/catalog/default_catalog.md), tables in external catalogs (supported from v2.5), or even existing asynchronous materialized views (supported from v2.5) and views (supported from v3.1). StarRocks supports creating asynchronous materialized views on all [types of StarRocks tables](../table_design/table_types/table_types.md).

- **Refresh**

  When you create an asynchronous materialized view, its data reflects only the state of the base tables at that time. When the data in the base tables change, you need to refresh the materialized view to keep the changes synchronized.

  Currently, StarRocks supports two generic refreshing strategies:

  - ASYNC: Asynchronous refresh mode. Each time the base table data changes, the materialized view is automatically refreshed according to the pre-defined refresh interval.
  - MANUAL: Manual refresh mode. The materialized view will not be automatically refreshed. The refresh tasks can only be triggered manually by users.

- **Query rewrite**

  Query rewrite means that when executing a query on base tables with materialized views built on, the system automatically judges whether the pre-computed results in the materialized view can be reused for the query. If they can be reused, the system will load the data directly from the relevant materialized view to avoid the time- and resource-consuming computations or joins.

  From v2.5, StarRocks supports automatic, transparent query rewrite based on the SPJG-type asynchronous materialized views. The SPJG-type materialized views refer to materialized views whose plan only includes Scan, Filter, Project, and Aggregate types of operators.

  > **NOTE**
  >
  > Asynchronous materialized views created on base tables in a [JDBC catalog](../data_source/catalog/jdbc_catalog.md) do not support query rewrite.

## Decide when to create a materialized view

You can create an asynchronous materialized view if you have the following demands in your data warehouse environment:

- **Accelerating queries with repetitive aggregate functions**

  Suppose that most queries in your data warehouse include the same sub-query with an aggregate function, and these queries have consumed a huge proportion of your computing resources. Based on this sub-query, you can create an asynchronous materialized view, which will compute and store all results of the sub-query. After the materialized view is built, StarRocks rewrites all queries that contain the sub-query, loads the intermediate results stored in the materialized view, and thus accelerates these queries.

- **Regular JOIN of multiple tables**

  Suppose that you need to regularly join multiple tables in your data warehouse to make a new wide table. You can build an asynchronous materialized view for these tables, and set the ASYNC refreshing strategy that triggers refreshing tasks at a fixed time interval. After the materialized view is built, query results are returned directly from the materialized view, and thus the latency caused by JOIN operations is avoided.

- **Data warehouse layering**

  Suppose that your data warehouse contains a mass of raw data, and queries in it require a complex set of ETL operations. You can build multiple layers of asynchronous materialized views to stratify the data in your data warehouse, and thus decompose the query into a series of simple sub-queries. It can significantly reduce repetitive computation, and, more importantly, help your DBA identify the problem with ease and efficiency. Beyond that, data warehouse layering helps decouple raw data and statistical data, protecting the security of sensitive raw data.
  
- **Accelerating queries in data lakes**

  Querying a data lake can be slow due to network latency and object storage throughput. You can enhance the query performance by building an asynchronous materialized view on top of the data lake. Moreover, StarRocks can intelligently rewrite queries to use the existing materialized views, saving you the trouble of modifying your queries manually.

For specific use cases of asynchronous materialized views, refer to the following content:

- [Data Modeling](./use_cases/data_modeling_with_materialized_views.md)
- [Query Rewrite](./use_cases/query_rewrite_with_materialized_views.md)
- [Data Lake Query Acceleration](./use_cases/data_lake_query_acceleration_with_materialized_views.md)

## Create an asynchronous materialized view

StarRocks' asynchronous materialized views can be created on the following base tables:

- StarRocks' native tables (all StarRocks table types are supported)

- Tables in external catalogs, including

  - Hive catalog (Since v2.5)
  - Hudi catalog (Since v2.5)
  - Iceberg catalog (Since v2.5)
  - JDBC catalog (Since v3.0)

- Existing asynchronous materialized views (Since v2.5)
- Existing views (Since v3.1)

### Before you begin

The following examples involve two base tables in the default catalog:

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

You can create a materialized view based on a specific query statement using [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md).

Based on the table `goods`, `order_list`, and the query statement mentioned above, the following example creates the materialized view `order_mv` to analyze the total of each order. The materialized view is set to refresh itself at an interval of one day.

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

> **NOTE**
>
> - While creating an asynchronous materialized view, you must specify either the data distribution strategy or the refresh strategy of the materialized view, or both.
> - You can set different partitioning and bucketing strategies for an asynchronous materialized view from those of its base tables, but you must include the partition keys and bucket keys of the materialized views in the query statement used to create the materialized view.
> - Asynchronous materialized views support a dynamic partitioning strategy in a longer span. For example, if the base table is partitioned at an interval of one day, you can set the materialized view to be partitioned at an interval of one month.
> - Currently, StarRocks does not support creating asynchronous materialized views with the list partitioning strategy or based on tables that are created with the list partitioning strategy.
> - The query statement used to create a materialized view does not support random functions, including rand(), random(), uuid(), and sleep().
> - Asynchronous materialized views support a variety of data types. For more information, see [CREATE MATERIALIZED VIEW - Supported data types](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md#supported-data-types).
> - By default, executing a CREATE MATERIALIZED VIEW statement immediately triggers the refresh task, which can consume a certain proportion of the system resources. If you want to defer the refresh task, you can add the REFRESH DEFERRED parameter to your CREATE MATERIALIZED VIEW statement.

- **About refresh mechanisms of asynchronous materialized views**

  Currently, StarRocks supports two ON DEMAND refresh strategies: MANUAL refresh and ASYNC refresh.

  In StarRocks v2.5, asynchronous materialized views further support a variety of asynchronous refreshing mechanisms to control the cost of refresh and increase the success rate:

  - If an MV has many large partitions, each refresh can consume a large amount of resources. In v2.5, StarRocks supports splitting refresh tasks. You can specify the maximum number of partitions to be refreshed, and StarRocks performs refresh in batches, with a batch size smaller or equal to the specified maximum number of partitions. This feature ensures large asynchronous materialized views are stably refreshed, enhancing the stability and robustness of data modeling.
  - You can specify the time to live (TTL) for partitions of an asynchronous materialized view, reducing the storage size taken by the materialized view.
  - You can specify the refresh range to refresh only the latest few partitions, reducing the refresh overhead.
  - You can specify the base tables where data changes will not automatically trigger a refresh of the corresponding materialized view.
  - You can assign a resource group to the refresh task.

  For more information, see the **PROPERTIES** section in [CREATE MATERIALIZED VIEW - Parameters](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md#parameters). You can also modify the mechanisms of an existing asynchronous materialized view using [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW.md).

  > **CAUTION**
  >
  > To prevent full refresh operations from exhausting system resources and causing task failures, it is recommended to create partitioned materialized views based on partitioned base tables. This ensures that when data updates occur within a base table partition, only the corresponding partition of the materialized view are refreshed, rather than refreshing the entire materialized view. For more information, please refer to [Data Modeling with Materialized Views - Partitioned Modeling](./data_modeling_with_materialized_views.md#partitioned-modeling).

- **About nested materialized views**

  StarRocks v2.5 supports creating nested asynchronous materialized views. You can build asynchronous materialized views based on existing asynchronous materialized views. The refreshing strategy for each materialized view does not affect the materialized views on the upper or lower layers. Currently, StarRocks does not limit the number of nesting levels. In a production environment, we recommend that the number of nesting layers does not exceed THREE.

- **About external catalog materialized views**

  StarRocks supports building asynchronous materialized views based on Hive Catalog (since v2.5), Hudi Catalog (since v2.5), Iceberg Catalog (since v2.5), and JDBC Catalog (since v3.0). Creating a materialized view on external catalogs is similar to creating an asynchronous materialized view on the default catalog, but with some usage restrictions. For more information, please refer to [Data lake query acceleration with materialized views](./data_lake_query_acceleration_with_materialized_views.md).

## Manually refresh an asynchronous materialized view

You can refresh an asynchronous materialized view regardless of its refreshing strategy via [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH_MATERIALIZED_VIEW.md). StarRocks v2.5 supports refreshing specific partitions of an asynchronous materialized view by specifying partition names. StarRocks v3.1 supports making a synchronous call of the refresh task, and the SQL statement is returned only when the task succeeds or fails.

```SQL
-- Refresh the materialized view via an asynchronous call (default).
REFRESH MATERIALIZED VIEW order_mv;
-- Refresh the materialized view via a synchronous call.
REFRESH MATERIALIZED VIEW order_mv WITH SYNC MODE;
```

You can cancel a refresh task submitted via an asynchronous call using [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL_REFRESH_MATERIALIZED_VIEW.md).

## Query the asynchronous materialized view directly

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

## Rewrite and accelerate queries with the asynchronous materialized view

StarRocks v2.5 supports automatic and transparent query rewrite based on the SPJG-type asynchronous materialized views. The SPJG-type materialized views query rewrite includes single table query rewrite, Join query rewrite, aggregation query rewrite, Union query rewrite and query rewrite based on nested materialized views. For more information, please refer to [Query Rewrite with Materialized Views](./query_rewrite_with_materialized_views.md).

Currently, StarRocks supports rewriting queries on asynchronous materialized views that are created on the default catalog or an external catalog such as a Hive catalog, Hudi catalog, or Iceberg catalog. When querying data in the default catalog, StarRocks ensures strong consistency of results between the rewritten query and the original query by excluding materialized views whose data is inconsistent with the base table. When the data in a materialized view expires, the materialized view will not be used as a candidate materialized view. When querying data in external catalogs, StarRocks does not ensure a strong consistency of the results because StarRocks cannot perceive the data changes in external catalogs. For more about asynchronous materialized views that are created based on an external catalog, please refer to [Data lake query acceleration with materialized views](./data_lake_query_acceleration_with_materialized_views.md).

> **NOTE**
>
> Asynchronous materialized views created on base tables in a JDBC catalog do not support query rewrite.

## Manage an asynchronous materialized view

### Alter an asynchronous materialized view

You can alter the property of an asynchronous materialized view using [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW.md).

- Enable an inactive materialized view.

  ```SQL
  ALTER MATERIALIZED VIEW order_mv ACTIVE;
  ```

- Rename an asynchronous materialized view.

  ```SQL
  ALTER MATERIALIZED VIEW order_mv RENAME order_total;
  ```

- Alter the refreshing interval of an asynchronous materialized view to 2 days.

  ```SQL
  ALTER MATERIALIZED VIEW order_mv REFRESH ASYNC EVERY(INTERVAL 2 DAY);
  ```

### Show asynchronous materialized views

You can view the asynchronous materialized views in your database by using [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md) or querying the system metadata table in Information Schema.

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

- Check all asynchronous materialized views by querying the metadata table `materialized_views` in Information Schema. For more information, please refer to [information_schema.materialized_views](../administration/information_schema.md#materialized_views).

  ```SQL
  SELECT * FROM information_schema.materialized_views;
  ```

### Check the definition of asynchronous materialized view

You can check the query used to create an asynchronous materialized view via [SHOW CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_MATERIALIZED_VIEW.md).

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

You can drop an asynchronous materialized view via [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP_MATERIALIZED_VIEW.md).

```Plain
DROP MATERIALIZED VIEW order_mv;
```

### Relevant session variables

The following variables control the behaviour of an asynchronous materialized view:

- `analyze_mv`: Whether and how to analyze the materialized view after refresh. Valid values are an empty string (Do not analyze), `sample` (Sampled statistics collection), and `full` (Full statistics collection). Default is `sample`.
- `enable_materialized_view_rewrite`: Whether to enable the automatic rewrite for materialized view. Valid values are `true` (Default since v2.5) and `false`.
