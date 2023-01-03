# Materialized view

This topic describes how to create, use, and manage a materialized view.

## Overview

A materialized view in StarRocks is a special physical table that holds pre-computed query results from a base table. On the one hand, when you perform complex queries on the base table, the relevant pre-computed results can be used directly in the query execution to avoid repeated calculations and improve query efficiency. On the other hand, you can build models based on your data warehouse through materialized views to provide a unified data specification to upper-layer applications, cover the underlying implementation, or protect the raw data security of the base table.

### Basic concepts

- **Materialized view**

  You can understand materialized views from two perspectives: materialization and view. Materialization means storing and reusing the pre-computed results for query acceleration. A view is essentially a table built on the basis of other tables. It is usually used to build mathematical models.

- **Base table**

  Base table is the driving table of its materialized view.

- **Query rewrite**

  Query rewrite means that when executing a query on a base table with materialized views built on, the system automatically judges whether or not the pre-computed results can be reused in processing the query. If they can be reused, the system will load the data directly from the relevant materialized view to avoid the time- and resource-consuming computation or join operation.

- **Refresh**

  Refresh refers to the data synchronization of the materialized view when the data in the base tables changes. There are two generic refresh strategies: ON DEMAND refresh and ON COMMIT refresh. ON DEMAND refresh is triggered manually or regularly. ON COMMIT refresh is triggered each time when data in the base tables changes.

### Scenario

Materialized views are useful in the following situations:

- **Query acceleration**

  Materialized views well serve the needs for accelerating predictable queries and queries that use same sub-query repetitively. Through materialized views, the system can directly use the pre-computed intermediate query result set to process such queries. It significantly reduces the load pressure caused by a large number of complex queries, and also greatly shortens the query processing time. StarRocks implements transparent acceleration based on materialized views, and ensures that, when querying the source table directly, the result must be based on the latest data.

- **Data warehouse modeling**

  Through materialized views, you can build new tables based on one or more base tables to achieve the following:

  - **Structured SQL statements, unified semantics**

    You can offer a unified SQL statement structure and data format to the upper-layer application to avoid repetitive development and computation.

  - **Simple interface**

    You can cover the underlying implementation and ensure the simplicity of the interface for the upper-layer application.

  - **Data security**

    You can shield the raw data of the base table through the materialized view to ensure the security of the sensitive data.

### Use cases

- Accelerating queries with repetitive aggregate functions

  Suppose that most queries in your data warehouse include the same sub-query with an aggregate function, and these queries have consumed a huge proportion of your computing resources. Based on this sub-query, you can create a materialized view, which will compute and store all results of the sub-query. After the materialized view is built, the system will rewrite all queries that contain the sub-query, load the intermediate result stored in the materialized view, and thus accelerate these queries.

- Regular JOIN of multiple tables

  Suppose that you need to regularly join multiple tables in your data warehouse to make a new wide table. You can build a materialized view for these tables, and set an async refresh mechanism that triggers the building regularly, so that you do not have to bother with doing it yourself. After the materialized view is built, query results are returned directly from the materialized view, and thus the latency caused by JOIN operations is avoided.

- Data warehouse layering

  Suppose that your data warehouse contains a mass of raw data, and queries in it require a complex set of ETL operations. You can build multiple layers of materialized views to stratify the data in your data warehouse, and thus decompose the query into a series of simple sub-queries. It will significantly reduce repetitive computation, and, more importantly, help the DBA identify the problem with ease and efficiency. Beyond that, data warehouse layering helps decouple the raw data and statistic data, protecting the security of the sensitive raw data.

## Accelerate queries with a single-table sync materialized view

StarRocks supports creating sync refresh materialized views on single tables.

If there are a considerable number of predictable queries or queries that use a same set of sub-query results repetitively, you can build materialized views to accelerate these queries.

### Preparation

Before creating the materialized view, check if your data warehouse is eligible for query acceleration through materialized views. For example, check if the queries reuse certain subquery statements.

The following example is based on the table `sales_records`, which contains the transaction ID `record_id`, salesperson ID `seller_id`, store ID `store_id`, date `sale_date`, and sales amount `sale_amt` for each transaction. Follow these steps to create the table and insert data into it:

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

The business scenario of this example demands frequent analyses on the sales amounts of different stores. As a result, the sum function is used on each query, consuming a massive amount of computing resources. You can run the query to record its time, and view its query profile by using EXPLAIN command.

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

It can be observed that the query takes about 0.02 seconds, and no materialized view is used to accelerate the query because the output of `rollup` section in the query profile is `sales_records`, which is the base table.

### Create a materialized view

You can create a materialized view based on a specific query statement using [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md).

Based on the table `sales_records` and the query statement mentioned above, the following example creates the materialized view `store_amt` to analyze the sum of sales amount in each store.

```SQL
CREATE MATERIALIZED VIEW store_amt AS
SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
```

### Query with the materialized view

The materialized view you created contains the complete set of pre-computed results in accordance with the query statement. Subsequent queries will use the data within it. You can run the same query to test the query time as you did in the preparation.

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

It can be observed that the query time is reduced to 0.01 seconds.

### Check if a query hits the materialized view

Run EXPLAIN command again to check if the query hits the materialized view.

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

As you can see, now the output of `rollup` section in the query profile is `store_amt`, which is the materialized view you have built. That means this query has hit the materialized view.

### Check the building status of a materialized view

Creating a materialized view is an asynchronous operation. Running CREATE MATERIALIZED VIEW command successfully indicates that the task of creating the materialized view is submitted successfully. You can view the building status of the materialized view in a database via [SHOW ALTER](../sql-reference/sql-statements/data-manipulation/SHOW%20ALTER.md) command.

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

The `RollupIndexName` section indicates the name of the materialized view, and `State` section indicates if the building is completed.

### Check the schema of a materialized view

You can use DESC tbl_name ALL command to check the schema of a table and its subordinate materialized views.

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

### Check the refresh tasks of materialized views

A single-table sync materialized view updates simultaneously when data is loaded into its base table. You can check the refresh tasks of all single-table sync materialized views in the database.

```SQL
SHOW ALTER MATERIALIZED VIEW;
```

### Drop a materialized view

Under the following circumstances, you need to drop a materialized view:

- You have created a wrong materialized view and you need to drop it before the building completed.

- You have created too many materialized views, which results in a huge drop in load performance, and some of the materialized views are duplicate.

- The frequency of the involved queries is low, and you can tolerate a relatively high query latency.

#### Drop an unfinished materialized view

You can drop a materialized view that is being created by canceling the in-progress creation task. First, you need to get the job ID `JobID` of the materialized view creation task by [checking the building status of the materialized view](#check-the-building-status-of-a-materialized-view). After getting the job ID, you need to cancel the creation task with the CANCEL ALTER command.

```Plain
CANCEL ALTER TABLE ROLLUP FROM sales_records (12090);
```

#### Drop an existing materialized view

You can drop an existing materialized view with the [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md) command.

```SQL
DROP MATERIALIZED VIEW store_amt;
```

### Best practices

#### Exact count distinct

The following example is based on an advertisement business analysis table `advertiser_view_record`, which records the date that the ad is viewed `click_time`, the name of the ad `advertiser`, the channel of the ad `channel`, and the ID of the user who viewed the ID `user_id`.

```SQL
CREATE TABLE advertiser_view_record(
    click_time DATE,
    advertiser VARCHAR(10),
    channel VARCHAR(10),
    user_id INT
) distributed BY hash(click_time);
```

Analysis is mainly focused on the UV of the ads.

```SQL
SELECT advertiser, channel, count(distinct user_id)
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

To accelerate exact count distinct, you can create a materialized view based on this table and use the bitmap_union function to pre-aggregate the data.

```SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, bitmap_union(to_bitmap(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

After the materialized view is created, the sub-query `count(distinct user_id)` in the subsequent queries will be automatically rewritten as `bitmap_union_count (to_bitmap(user_id))` so that they can hit the materialized view.

#### Approximate count distinct

Use the table `advertiser_view_record` above as an example again. To accelerate approximate count distinct, you can create a materialized view based on this table and use the [hll_union()](../sql-reference/sql-functions/aggregate-functions/hll_union.md) function to pre-aggregate the data.

```SQL
CREATE MATERIALIZED VIEW advertiser_uv2 AS
SELECT advertiser, channel, hll_union(hll_hash(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
```

#### Set extra sort keys

Suppose that the base table `tableA` contains columns `k1`, `k2` and `k3`, where only `k1` and `k2` are sort keys. If the query including the sub-query `where k3=x` must be accelerated, you can create a materialized view with `k3` as the first column.

```SQL
CREATE MATERIALIZED VIEW k3_as_key AS
SELECT k3, k2, k1
FROM tableA
```

### Correspondence of aggregate functions

When a query is executed with a materialized view, the original query statement will be automatically rewritten and used to query the intermediate results stored in the materialized view. The following table shows the correspondence between the aggregate function in the original query and the aggregate function used to construct the materialized view. You can select the corresponding aggregate function to build a materialized view according to your business scenario.

| **aggregate function in the original query**           | **aggregate function of the materialized view** |
| ------------------------------------------------------ | ----------------------------------------------- |
| sum                                                    | sum                                             |
| min                                                    | min                                             |
| max                                                    | max                                             |
| count                                                  | count                                           |
| bitmap_union, bitmap_union_count, count(distinct)      | bitmap_union                                    |
| hll_raw_agg, hll_union_agg, ndv, approx_count_distinct | hll_union                                       |

### Caution

- Sync materialized views only support aggregate functions on a single column. Query statements in the form of `sum(a+b)` are not supported.

- Clauses such as JOIN, and WHERE are not supported in the sync materialized view creation statements.

- The current version of StarRocks does not support creating multiple materialized views at the same time. A new materialized view can only be created when the one before is completed.

- A materialized view supports only one aggregate function for each column of the base table. Query statements such as `select sum(a), min(a) from table` are not supported.

- When using ALTER TABLE DROP COLUMN to drop a specific column in a base table, you need to ensure that all materialized views of the base table contain the dropped column, otherwise the drop operation cannot be performed. If you have to drop the column, you need to first drop all materialized views that do not contain the column, and then drop the column.

- Creating too many materialized views for a table will affect the data load efficiency. When data is being loaded to the base table, the data in materialized view and base table will be updated synchronously. If a base table contains `n` materialized views, the efficiency of loading data into the base table is about the same as the efficiency of loading data into `n` tables.

- You must use the GROUP BY clause when using aggregate functions and specify the GROUP BY column in your SELECT list.

## Model data warehouse with multi-table async materialized view

StarRocks 2.4 supports creating asynchronous materialized views for multiple base tables to allow modeling data warehouse. Asynchronous materialized views support all [Data Models](../table_design/Data_model.md).

As for the current version, multi-table materialized views support two refresh strategies:

- **Async refresh**

  Async refresh strategy allows materialized views to refresh through asynchronous refresh tasks, and does not guarantee strict consistency between the base table and its subordinate materialized views.

- **Manual refresh**

  You can manually trigger a refresh task for an async materialized view. It does not guarantee strict consistency between the base table and its subordinate materialized views.

### Preparation

#### Enable async materialized view

To use the async materialized view feature, you need to set the configuration item `enable_experimental_mv` as `true` using the following statement:

```SQL
ADMIN SET FRONTEND CONFIG ("enable_experimental_mv"="true");
```

#### Create base tables

The following examples involve two base tables:

- Table `goods` records the item ID `item_id1`, item name `item_name`, and item price `price`.

- Table `order_list` records the order ID `order_id`, client ID `client_id`, item ID `item_id2`, and order date `order_date`.

Column `item_id1` is equivalent to column `item_id2`.

Follow these steps to create the tables and insert data into them:

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

The business scenario of this example demands frequent analyses on the total of each order. Because each query requires JOIN operation on the two base tables, two base tables should be joined a wide table. Besides, the business scenario demands the data refresh at an interval of one day.

The query statement is as follows:

```SQL
SELECT
    order_id,
    sum(goods.price) as total
FROM order_list INNER JOIN goods ON goods.item_id1 = order_list.item_id2
GROUP BY order_id;
```

### Create a materialized view

You can create a materialized view based on a specific query statement using [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE%20MATERIALIZED%20VIEW.md).

Based on the table `goods`, `order_list` and the query statement mentioned above, the following example creates the materialized view `order_mv` to analyze the total of each order. The materialized view is set to refresh asynchronously at an interval of one day.

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

#### About async refresh mechanisms for materialized views

In StarRocks v2.5, multi-table async refresh materialized views support multiple async refresh mechanisms. You can set different refresh mechanisms by specifying the following properties when creating a materialized view, or modify the mechanism of an existing materialized view using [ALTER MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/ALTER%20MATERIALIZED%20VIEW.md).

| **Property**                      | **Default** | **Description**                                                     |
| --------------------------------- | ----------- | ------------------------------------------------------------------- |
| partition_ttl_number          | -1         | The number of most recent materialized view partitions to keep. After the number of partitions exceeds this value, expired partitions will be deleted. StarRocks will periodically check materialized view partitions according to the time interval specified in the FE configuration item `dynamic_partition_enable`, and automatically delete expired partitions. When the value is `-1`, all partitions of the materialized view will be preserved. |
| partition_refresh_number      | -1         | In a single refresh, the maximum number of partitions to refresh. If the number of partitions to be refreshed exceeds this value, StarRocks will split the refresh task and complete it in batches. Only when the previous batch of partitions is refreshed successfully, StarRocks will continue to refresh the next batch of partitions until all partitions are refreshed. If any of the partitions fails to be refreshed, no subsequent refresh tasks will be generated. When the value is `-1`, the refresh task will not be split. |
| excluded_trigger_tables       | An empty string   | If a base table of the materialized view is listed here, automatic refresh task will not be triggered when the data in the base table is changed. This parameter only applies to load-triggered refresh strategy, and is usually used together with the property `auto_refresh_partitions_limit`. Format: `[db_name.]table_name`. When the value is an empty string, any data change in all base tables triggers the refresh of the corresponding materialized view. |
| auto_refresh_partitions_limit | -1         | The number of most recent materialized view partitions that need to be refreshed when a materialized view refresh is triggered. You can use this property to limit the refresh range and reduce the refresh cost. However, because not all the partitions are refreshed, the data in the materialized view may not be consistent with the base table. When the value is `-1`, all partitions will be flushed. |

#### About nested materialized views

StarRocks v2.5 supports creating nested async refresh materialized views. You can build materialized views based on existing materialized views. The refresh strategy for each materialized view only applies to the corresponding materialized view. Currently, StarRocks does not limit the number of nesting levels. In a production environment, we recommend that the number of nesting layers not exceed THREE.

#### About external catalog materialized views

StarRocks v2.5 supports creating async refresh materialized views based on Hive catalog, Hudi catalog and Iceberg catalog. An external catalog materialized view is created in the same way as a general async refresh materialized view is created, with the following usage restrictions:

- External catalog materialized view only support async fixed-interval refresh and manual refresh.
- Strict consistency is not guaranteed between the materialized view and the base tables in the external catalog.
- Currently, building materialized views based on external resources is not supported.
- Currently, StarRocks cannot perceive if the base table data in the external catalog has changed, so all partitions will be refreshed by default every time the base table is refreshed. You can manually refresh only some of partitions using [REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/REFRESH%20MATERIALIZED%20VIEW.md).

### Query with the materialized view

The materialized view you created contains the complete set of pre-computed results in accordance with the query statement. **You can directly query an async materialized view**.

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

### (Optional) Rewrite queries with async materialized view

In StarRocks v2.5, multi-table async refresh materialized views support automatic and transparent query rewrite based on the SPJG-type materialized views. The SPJG-type materialized views refer to materialized views whose plan only includes Scan, Filter, Project, and Aggregate types of operators. The SPJG-type materialized views query rewrite includes single table query rewrite, Join query rewrite, aggregation query rewrite, Union query rewrite and query rewrite based on nested materialized views.

When querying data in the internal tables, StarRocks ensures strong consistency of results between the rewritten query and the original query by excluding materialized views whose data is inconsistent with the base table. When the data in a materialized view data expires, the materialized view will not be used as a candidate materialized view.

> **CAUTION**
>
> Currently, StarRocks does not support query rewrite based on external catalog materialized views.

#### Candidate materialized view for query rewrite

When rewriting a query, StarRocks will roughly select candidate materialized views that may meet the corresponding conditions from all materialized views, so as to reduce the cost of rewriting.

Candidate materialized views must meet the following conditions:

1. The materialized view status is active.
2. There must be an intersection between the base table of the materialized view and the tables involved in the query.
3. If it is a non-partitioned materialized view, the data within it must be the most updated.
4. If it is a partitioned materialized view, only most updated partitions can be used in query rewrite.
5. The materialized view must contain only Select, Filter, Join, Projection, and Aggregate operators.
6. Nested materialized views are also considered as candidates if they meet conditions 1, 3, and 4.

#### Enable query rewrite based on async materialized views

StarRocks enables materialized view query rewrite by default. You can enable or disable this feature through the session variable `enable_materialized_view_rewrite`.

```SQL
SET GLOBAL enable_materialized_view_rewrite = { true | false };
```

#### Configure query rewrite based on async materialized views

You can configure async materialized view query rewrite through the following session variables:

| **Variable**                                | **Default** | **Description**                                              |
| ------------------------------------------- | ----------- | ------------------------------------------------------------ |
| enable_materialized_view_union_rewrite      | true        | Boolean value to control if to enable materialized view Union query rewrite. |
| enable_rule_based_materialized_view_rewrite | true        | Boolean value to control if to enable rule-based materialized view query rewrite. This variable is mainly used in single-table query rewrite. |
| nested_mv_rewrite_max_level                 | 3           | The maximum levels of nested materialized views that can be used for query rewrite. Type: INT. Range: [1, +∞). The value of `1` indicates that only materialized views created on base tables can be used for query rewrite. |

#### Check if a query is rewritten

You can check if your query is rewritten based on materialized views by viewing its query plan using the EXPLAIN statement. If the field `TABLE` under the section `OlapScanNode` shows the name of the corresponding materialized view, it means that the query has been rewritten based on the materialized view.

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

### Rename a materialized view

You can rename a materialized view via ALTER MATERIALIZED VIEW command.

```SQL
ALTER MATERIALIZED VIEW order_mv RENAME order_total;
```

### Alter the refresh strategy of a materialized view

You can also alter the refresh strategy of a materialized view via ALTER MATERIALIZED VIEW command.

```SQL
ALTER MATERIALIZED VIEW order_mv REFRESH ASYNC EVERY(INTERVAL 2 DAY);
```

### Check materialized views

You can check materialized views in your database in the following ways:

- Check all materialized views in your database.

```SQL
SHOW MATERIALIZED VIEW;
```

- Check a specific materialized view.

```SQL
SHOW MATERIALIZED VIEW WHERE NAME = order_mv;
```

- Check specific materialized views by matching the name.

```SQL
SHOW MATERIALIZED VIEW WHERE NAME LIKE "order%";
```

- Check all materialized views via `information_schema`.

```SQL
SELECT * FROM information_schema.materialized_views;
```

### Check the definition of a materialized view

You can check the SQL statement used to create a materialized view via SHOW CREATE MATERIALIZED VIEW command.

```SQL
SHOW CREATE MATERIALIZED VIEW order_mv;
```

### Manually refresh an async materialized view

You can manually refresh an async materialized view via REFRESH MATERIALIZED VIEW command. StarRocks v2.5 supports specifying partitions to be refreshed.

```SQL
REFRESH MATERIALIZED VIEW order_mv;
```

> **CAUTION**
>
> You can refresh a materialized view with async or manual refresh strategy via this command. However, you cannot refresh a single-table sync refresh materialized view via this command.

You can cancel a refresh task by using the [CANCEL REFRESH MATERIALIZED VIEW](../sql-reference/sql-statements/data-manipulation/CANCEL%20REFRESH%20MATERIALIZED%20VIEW.md) statement.

### Check the execution status of a multi-table materialized view

You can check the execution status of a multi-table materialized view via the following ways.

```SQL
SELECT * FROM INFORMATION_SCHEMA.tasks;
SELECT * FROM INFORMATION_SCHEMA.task_runs;
```

> **NOTE**
>
> Async refresh materialized views rely on the Task framework to refresh data, so you can check refresh tasks by querying the `tasks` and `task_runs` metadata tables provided by the Task framework.

### Drop a materialized view

You can drop a materialized view via [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP%20MATERIALIZED%20VIEW.md) command.

```SQL
DROP MATERIALIZED VIEW order_mv;
```

### Caution

- Async refresh materialized views have the following features:
  - You can directly query a async refresh materialized view, but the result may be inconsistent with that from the base tables.
  - You can set different partitioning and bucketing strategies for a async refresh materialized view from that of the base tables.
  - Async refresh materialized views support dynamic partitioning strategy in a longer span. For example, if the base table is partitioned at an interval of one day, you can set the materialized view to be partitioned at an interval of one month.

- You can build a multi-table materialized view under async or manual refresh strategies.

- Partition keys and bucket keys of the async or manual refresh materialized view must be in the query statement.

- The query statement does not support random functions, including rand((), random(), uuid()), and sleep().
