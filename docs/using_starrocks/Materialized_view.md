# Materialized view

This topic describes how to create, use, and manage a materialized view.

## Overview

A materialized view in StarRocks is a special physical table that holds pre-computed query results from a base table. When you perform complex queries on a base table, the relevant pre-computed results can be used directly in the query execution to avoid repeated calculations and improve query efficiency.

### Basic concepts

- **Materialized view**

  You can understand materialized views from two perspectives: materialization and view. Materialization means storing and reusing the pre-computed results for query acceleration. A view is essentially a table built on the basis of other tables. It is usually used to build mathematical models.

- **Base table**

  Base table is the driving table of its materialized view.

- **Query rewrite**

  Query rewrite means that when executing a query on a base table with materialized views built on, the system automatically judges whether or not the pre-computed results can be reused in processing the query. If they can be reused, the system will load the data directly from the relevant materialized view to avoid the time- and resource-consuming computation or join operation.

### Scenario

Materialized views are useful in the following situation:

- **Query acceleration**

  Materialized views well serve the needs for accelerating predictable queries and queries that use same sub-query repetitively. Through materialized views, the system can directly use the pre-computed intermediate query result set to process such queries. It significantly reduces the load pressure caused by a large number of complex queries, and also greatly shortens the query processing time. StarRocks implements transparent acceleration based on materialized views, and ensures that, when querying the source table directly, the result must be based on the latest data.

### Use case

#### Accelerating queries with repetitive aggregate functions

  Suppose that most queries in your data warehouse include the same sub-query with an aggregate function, and these queries have consumed a huge proportion of your computing resources. Based on this sub-query, you can create a materialized view, which will compute and store all results of the sub-query. After the materialized view is built, the system will rewrite all queries that contain the sub-query, load the intermediate result stored in the materialized view, and thus accelerate these queries.

## Accelerate queries with a materialized view

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

You can create a materialized view based on a specific query statement by using the following SQL command.

```SQL
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [database.]mv_name
AS (query)
[PROPERTIES ("key"="value", ...)];
```

For detailed instructions and parameter references, see [SQL Reference - CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md).

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

Creating a materialized view is an asynchronous operation. Running CREATE MATERIALIZED VIEW command successfully indicates that the task of creating the materialized view is submitted successfully. You can view the building status of the materialized view in a database via [SHOW ALTER](../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md) command.

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

You can drop an existing materialized view with the [DROP MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/DROP_MATERIALIZED_VIEW.md) command.

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

- Prior to StarRocks 2.4, materialized views only support aggregate functions on a single column. Query statements in the form of `sum(a+b)` are not supported.

- Prior to StarRocks 2.4, clauses such as JOIN, WHERE, and GROUP BY are not supported in the materialized view creation statements.

- The current version of StarRocks does not support creating multiple materialized views at the same time. A new materialized view can only be created when the one before is completed.

- A materialized view supports only one aggregate function for each column of the base table. Query statements such as `select sum(a), min(a) from table` are not supported.

- When using ALTER TABLE DROP COLUMN to drop a specific column in a base table, you need to ensure that all materialized views of the base table contain the dropped column, otherwise the drop operation cannot be performed. If you have to drop the column, you need to first drop all materialized views that do not contain the column, and then drop the column.

- Creating too many materialized views for a table will affect the data load efficiency. When data is being loaded to the base table, the data in materialized view and base table will be updated synchronously. If a base table contains `n` materialized views, the efficiency of loading data into the base table is about the same as the efficiency of loading data into `n` tables.

- You must use the GROUP BY clause when using aggregate functions and specify the GROUP BY column in your SELECT list.
