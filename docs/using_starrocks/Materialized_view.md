# Materialized view

## Background

In computing, a materialized view is a database object that contains query results. For example, it may be a local copy of the data located remotely, a subset of the rows and/or columns of a table or join result, or a summary using an aggregate function. Compared to normal logical views, "materializing" the data can lead to improved query performance.

In this system, materialized views are used as a precomputation technique, similar to  RollUp tables that are precomputed to reduce the onsite computation at  query time and thus reduce query latency. RollUp tables can be used in two ways: to pre-aggregate any combination of dimensions in a duplicate table; and to use a new dimension column sorting method to hit more prefix query conditions. It is possible to use both ways together.  Materialized views are superset of RollUp tables and  implemented with Rollup functionalities.

The use cases of materialized views are:

* Analysis that needs to cover both detailed data queries and fixed dimension aggregation queries.
* Need to do range condition filtering on a combination of columns other than sort key prefixes.
* Need to do coarse-grained aggregation analysis for any dimension of duplicate tables.

## Principle

The data organization of materialized views is the same as the one of the base and RollUp tables. Users can add materialized views to a newly created base table or an existing table. In the latter case, the data from the base table is automatically populated into the materialized views in an **asynchronous** manner. A base table can have multiple materialized views. When importing data to a base table, all materialized views are updated **simultaneously**. The data import operation is **atomic**, so the base table and its materialized views maintain data consistency.

After the materialized view is created successfully, the user's original SQL statement for querying the base table remains unchanged. StarRocks automatically selects an optimal materialized view, and reads the data from the materialized view for calculation. You can use the `EXPLAIN` command to check whether the current query uses the materialized view or not.

Matching relationship between aggregates in materialized views and aggregates in queries.

| aggregates in materialized views | aggregates in queries |
| :-: | :-: |
| sum | sum |
| min | min |
| max | max |
| count | count |
| bitmap_union | bitmap_union, bitmap_union_count, count(distinct) |
| hll_union | hll_raw_agg, hll_union_agg, ndv, approx_count_distinct |

When the query matches the materialized view, the aggregation operators of the bitmap and hll are rewritten according to the table structure of the materialized view.

## Usage

### Create materialized view

A materialized view can be created with the following command. The creation of a materialized view is an asynchronous operation, meaning that after the job is submitted, StarRocks will calculate the history data until the materialized view is successfully created.

~~~SQL
CREATE MATERIALIZED VIEW
~~~

Suppose the user has a sales record duplicate table that stores the transaction id, salesperson, selling store, sold time, and sold price t for each transaction. The table creation statement is as follows:

~~~SQL
CREATE TABLE sales_records(
    record_id int,
    seller_id int,
    store_id int,
    sale_date date,
    sale_amt bigint
) distributed BY hash(record_id)
properties("replication_num" = "1");
~~~

The structure of the sales_records table is:

~~~PlainText
MySQL [test]> desc sales_records;

+-----------+--------+------+-------+---------+-------+
| Field     | Type   | Null | Key   | Default | Extra |
+-----------+--------+------+-------+---------+-------+
| record_id | INT    | Yes  | true  | NULL    |       |
| seller_id | INT    | Yes  | true  | NULL    |       |
| store_id  | INT    | Yes  | true  | NULL    |       |
| sale_date | DATE   | Yes  | false | NULL    | NONE  |
| sale_amt  | BIGINT | Yes  | false | NULL    | NONE  |
+-----------+--------+------+-------+---------+-------+
~~~

If you need to frequently analyze the sales volume of different stores, you can create a materialized view of the `sales_records` table that `sums the sales of the same stores, grouped by the stores sold`. The creation statement is as follows.

~~~sql
CREATE MATERIALIZED VIEW store_amt AS
SELECT store_id, SUM(sale_amt)
FROM sales_records
GROUP BY store_id;
~~~

### View creation progress of a materialized view

Since creating a materialized view is an asynchronous operation, after submitting the job, the user needs to check whether the materialized view is created or not with the following command:

~~~SQL
SHOW ALTER MATERIALIZED VIEW FROM db_name;
~~~

Or

~~~SQL
SHOW ALTER TABLE ROLLUP FROM db_name;
~~~

> db_name: Replace with the real db name, such as "test".

The query result is:

~~~PlainText
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+----------+------+----------+---------+
| JobId | TableName     | CreateTime          | FinishedTime        | BaseIndexName | RollupIndexName | RollupId | TransactionId | State    | Msg  | Progress | Timeout |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+----------+------+----------+---------+
| 22324 | sales_records | 2020-09-27 01:02:49 | 2020-09-27 01:03:13 | sales_records | store_amt       | 22325    | 672           | FINISHED |      | NULL     | 86400   |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+----------+------+----------+---------+
~~~

If the State is FINISHED, it means the materialized view has been created.

### View all created materialized views

You can view all materialized views of a base table by using the name of a base table. For example, run the following code to view all materialized views of `sales_records`.

~~~PlainText
mysql> desc sales_records all;

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
~~~

### Materialized view hit the query

When the user queries the sales volume of different stores, the aggregated data will be read directly from the newly created materialized view (i.e. `store_amt`) for query efficiency.

For example, to query the `sales_records` table as follows:.

~~~SQL
SELECT store_id, SUM(sale_amt) FROM sales_records GROUP BY store_id;
~~~

Use the `EXPLAIN` command to query whether the materialized view is a hit or not.

~~~SQL
EXPLAIN SELECT store_id, SUM(sale_amt) FROM sales_records GROUP BY store_id;
~~~

Results:

~~~PlainText

| Explain String                                                              |
+-----------------------------------------------------------------------------+
| PLAN FRAGMENT 0                                                             |
|  OUTPUT EXPRS:<slot 2> `store_id` | <slot 3> sum(`sale_amt`)                |
|   PARTITION: UNPARTITIONED                                                  |
|                                                                             |
|   RESULT SINK                                                               |
|                                                                             |
|   4:EXCHANGE                                                                |
|      use vectorized: true                                                   |
|                                                                             |
| PLAN FRAGMENT 1                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: HASH_PARTITIONED: <slot 2> `store_id`                          |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 04                                                         |
|     UNPARTITIONED                                                           |
|                                                                             |
|   3:AGGREGATE (merge finalize)                                              |
|   |  output: sum(<slot 3> sum(`sale_amt`))                                  |
|   |  group by: <slot 2> `store_id`                                          |
|   |  use vectorized: true                                                   |
|   |                                                                         |
|   2:EXCHANGE                                                                |
|      use vectorized: true                                                   |
|                                                                             |
| PLAN FRAGMENT 2                                                             |
|  OUTPUT EXPRS:                                                              |
|   PARTITION: RANDOM                                                         |
|                                                                             |
|   STREAM DATA SINK                                                          |
|     EXCHANGE ID: 02                                                         |
|     HASH_PARTITIONED: <slot 2> `store_id`                                   |
|                                                                             |
|   1:AGGREGATE (update serialize)                                            |
|   |  STREAMING                                                              |
|   |  output: sum(`sale_amt`)                                                |
|   |  group by: `store_id`                                                   |
|   |  use vectorized: true                                                   |
|   |                                                                         |
|   0:OlapScanNode                                                            |
|      TABLE: sales_records                                                   |
|      PREAGGREGATION: ON                                                     |
|      partitions=1/1                                                         |
|      rollup: store_amt                                                      |
|      tabletRatio=10/10                                                      |
|      tabletList=22326,22328,22330,22332,22334,22336,22338,22340,22342,22344 |
|      cardinality=0                                                          |
|      avgRowSize=0.0                                                         |
|      numNodes=1                                                             |
|      use vectorized: true                                                   |
+-----------------------------------------------------------------------------+
~~~

The OlapScanNode in the query plan tree shows `PREAGGREGATION: ON` and `rollup: store_amt`, meaning the results are calculated using the materialized view `store_amt` directly.

### Delete a materialized view

You need to delete a materialized view in the following two cases:

* The user created a materialized view by mistake and needs to undo the operation.
* The user has created many materialized views, which makes the data import speed too slow to meet the business needs. Duplicate materialized views, low query frequency, and high query latency are also the reason for deleting a materialized view..

Delete the materialized views that have been created as follows:

~~~SQL
DROP MATERIALIZED VIEW IF EXISTS store_amt on sales_records;
~~~

To delete a materialized view that is being created, you need to first cancel the asynchronous task and then delete the materialized view. Take the materialized view mv on table `db0.table0` as an example:

First get the JobId and execute the command:

~~~SQL
show alter table rollup from db0;
~~~

Results:

~~~PlainText
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+-------------+------+----------+---------+
| JobId | TableName     | CreateTime          | FinishedTime        | BaseIndexName | RollupIndexName | RollupId | TransactionId | State       | Msg  | Progress | Timeout |
| 22478 | table0        | 2020-09-27 01:46:42 | NULL                | table0        | mv              | 22479    | 676           | WAITING_TXN |      | NULL     | 86400   |
+-------+---------------+---------------------+---------------------+---------------+-----------------+----------+---------------+-------------+------+----------+---------+
~~~

Where JobId is 22478, cancel the Job and execute the command:

~~~SQL
cancel alter table rollup from db0.table0 (22478);
~~~

## Best Practices

### Precise de-duplication

Users can use the `bitmap_union(to_bitmap(col))` expression to create a materialized view on the duplicate table to achieve the exact bitmap-based count distinct that was only supported by the aggregated table.

For example, the user has a table recording advertising operations such as the date of a click, which ad was clicked, through which channel it was clicked, and who clicked.

~~~SQL
CREATE TABLE advertiser_view_record(
    TIME date,
    advertiser varchar(10),
    channel varchar(10),
    user_id int
) distributed BY hash(TIME)
properties("replication_num" = "1");
~~~

Query the advertisement UV using the following query statement:

~~~SQL
SELECT advertiser, channel, count(distinct user_id)
FROM advertiser_view_record
GROUP BY advertiser, channel;
~~~

In this case, you can create a materialized view and pre-calculate it using bitmap_union:

~~~SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, bitmap_union(to_bitmap(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
~~~

After the materialized view is created, `count(distinct user_id)` in the query statement is automatically rewritten to `bitmap_union_count (to_bitmap(user_id))` to hit the materialized view.

### Approximate de-duplication

The user can use the expression `hll_union(hll_hash(col))` to create a materialized view on the detailed table to achieve approximate de-duplication precomputation.

Create a  materialized view as follows:

~~~SQL
CREATE MATERIALIZED VIEW advertiser_uv AS
SELECT advertiser, channel, hll_union(hll_hash(user_id))
FROM advertiser_view_record
GROUP BY advertiser, channel;
~~~

### Match different prefix indexes

The base table tableA has three columns (k1, k2, k3), where k1, k2 are sorted keys. If the query condition contains `where k1=1 and k2=2`, it can be accelerated by shortkey indexing. However, if the condition contains `k3=3`,  it can’t be accelerated by shortkey indexing. In this case, you can create a materialized view with k3 as the first column:

~~~SQL
CREATE MATERIALIZED VIEW mv_1 AS
SELECT k3, k2, k1
FROM tableA
ORDER BY k3;
~~~

Now the query will read data directly from the newly created mv_1 materialized view. The materialized view has a prefix index on k3, which makes the query more efficient.

## Note

1. The aggregation function of materialized views only supports a single column as an argument, for example: `sum(a+b)` is not supported.
2. If the condition column of a delete statement does not exist in the materialized view, the delete operation cannot be performed. If you must delete the data, you need to delete the materialized view first .
3. Too many materialized views on a single table affects import efficiency. When importing data, the materialized view and the base table data are updated simultaneously. Therefore,  if there are more than 10 materialized views on a table, the import speed may be slow (it’s like importing 10 tables at the same time).
4. Same column with different aggregation functions can not appear in a materialized view at the same time, for example: `select sum(a), min(a) from table` is not supported.
5. The creation statement of materialized views currently does not support `JOIN`, `WHERE`, or the `HAVING` clause of GROUP BY`.
6. You cannot create more than one materialized view at the same time. You can only  create a new materialized view  after the previous creation  is complete
