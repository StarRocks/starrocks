# Materialized View

## Terminology

1. Duplicate model: The data model stores detailed data in StarRocks. The model can be specified during table build, and the data in the metric columns will not be aggregated.
2. Base table: The table created by `CREATE TABLE` in StarRocks.
3. Materialized Views table: Pre-calculated data set that contains results of a query. Abbreviated as MV.

## Use Scenarios

In a practical business scenario, there are usually two coexisting use cases: 1) aggregation analysis of fixed dimensions and 2) analysis of arbitrary dimensions of the original detailed data.

For example, consider an E-commerce business, the order data contains the following dimensions:

* `item_id`
* `sold_time`
* `customer_id`
* `price`

The two mentioned use cases are both valid since the users need to:

1. get the sales number of a certain item on a certain day, so we need to aggregate price on the `item_id` and `sold_time` dimensions;
2. analyze the transaction details of a certain item by a certain person on a certain day.

In the existing StarRocks data model, if you create only one table with aggregation model, for example a table with `item_id`, `sold_time`, `customer_id`, `sum(price)`, you are not able to analyze detailed data since the aggregation drops some information of the data. If only a duplicate model is built, you can run queries on all the dimension, but the queries will not be accelerated because of lack of Rollup support. If you build a table with the aggregation model and a table with the duplicate model at the same time, you can get both the performance and can run queries on all the dimensions, but the two tables are not related to each other, so you need to choose the analysis table manually. It doesn’t offer good flexibility and usability.

## How to use

Queries that use aggregation functions such as `sum` and `count` can be executed more efficiently in tables that already contain aggregated data. You would want the improved efficiency when querying large amounts of data. The data in the table is materialized in a storage node and can be kept consistent with the base table in incremental updates. After a user creates a MVs table, the query optimizer will select and query the most efficient MVs table instead of using the base table. Since the data in MVs tables is typically much smaller than the base table, queries are more efficient.

### **Build a materialized view**

~~~sql
CREATE MATERIALIZED VIEW materialized_view_name
AS SELECT id, SUM(clicks) AS sum_clicks
FROM  database_name.base_table
GROUP BY id ORDER BY id’
~~~

The creation of a materialized view is currently an asynchronous operation. The command for materialized view creation returns immediately, but the creation may still be running. You can use the `DESC "base_table_name" ALL` command to see the current materialized view of the base table. You can use the `SHOW ALTER TABLE MATERIALIZED VIEW FROM "database_name"` command to check the status of the current and the historical materialized views.

* **Restrictions**

  * The `partition column` in the base table must be one of  the `group by` columns of the created materialized view
  * Currently, only synchronous materialized views are supported. No multi-table joins.
  * Aggregation is not supported for key columns, but only for value columns.The type of aggregation operator cannot be changed.
  * The materialized view must contain at least one key column.
  * Expression calculation is not supported.
  * Queries a specified materialized view is not supported.

### **DROP MATERIALIZED VIEW**

`DROP MATERIALIZED VIEW [IF EXISTS] [db_name]. <mv_name>`

### **View materialized views**

* View all materialized views under this database

`SHOW MATERIALIZED VIEW [IN|FROM db_name]`

* View the table structure of the specified materialized view

`DESC table_name all`
( `DESC/DESCRIBE mv_name` is no longer supported)

* View the processing progress of a materialized view

`SHOW ALTER MATERIALIZED VIEW FROM db_name`

* Cancel the materialized view being created

`CANCEL ALTER MATERIALIZED VIEW FROM db_name.table_name`

* Confirm which materialized views were hit by the query

Users do not need to know the existence of the materialized view when running the queries, which means the name of the materialized view does not have to be explicitly specified. The query optimizer can automatically determine whether the query can be routed to the appropriate materialized view. Whether the query plan is rewritten or not, it can be seen with `explain SQL`, which can be executed on the MySQL client:

~~~SQL
Explain SQL:
 
0:OlapScanNode
TABLE: lineorder4
PREAGGREGATION: ON
partitions=1/1
RollUptable: lineorder4
tabletRatio=1/1
tabletList=15184
cardinality=119994608
avgRowSize=26.375498
numNodes=1
tuple ids: 0
~~~

The `RollUptable` field indicates which materialized view was hit. If the `PREAGGREGATION` field is `On`, the query will be faster. If the `PREAGGREGATION` field is `Off`, the reason will be shown. For example:

~~~ TEXT
PREAGGREGATION: OFF. Reason: The aggregate operator does not match.
~~~

This indicates the physical view cannot be used in the StarRocks storage engine because the aggregation function of the query does not match the one defined in the physical view, and requires on-site aggregation.

### **Importing Data**

Incremental imports to the base table are applied to all associated MVs tables. Data cannot be queried until the import to the base table and all MVs tables are all complete. StarRocks ensures that the data is consistent between the base and MVs tables. There is no data difference between querying the base table and the MVs table.

## Notes

### **Materialized view function support**

The materialized view must be an aggregation of a single table. Only the following aggregation functions are supported currently.

* COUNT
* MAX
* MIN
* SUM
* PERCENTILE_APPROX
* HLL_UNION
* Performs HLL aggregation on duplicate data and uses HLL functions to analyze querieddata. (This is mainly used for quick and non-exact de-duplication calculations. To use `HLL_UNION aggregation` for duplicate data, you need to call the `hll_hash` function first to transform the original data.)

~~~SQL
create materialized view dt_uv as
    select dt, page_id, HLL_UNION(hll_hash(user_id))
    from user_view
    group by dt, page_id;
select ndv(user_id) from user_view; #The query can hit the materialized view
~~~

* `HLL_UNION` aggregation operator is not supported for `DECIMAL`/`BITMAP`/`HLL`/`PERCENTILE` type columns currently.

* BITMAP_UNION

* Users can perform BITMAP aggregation on duplicate data and use the BITMAP function to analyze the data. It is mainly used to quickly calculate the exact de-duplication of `count(distinct)`. To use `BITMAP_UNION` aggregation on the duplicate data, you need to call the `to_bitmap function` first to convert the original data.

~~~SQL
create materialized view dt_uv as
    select dt, page_id, bitmap_union(to_bitmap(user_id))
    from user_view
    group by dt, page_id;
select count(distinct user_id) from user_view; #Query can hit the materialized view
~~~

* Currently, only `TINYINT`/`SMALLINT`/`INT`/`BITINT` types are supported, and the stored content must be a positive integer (including 0).

### **Intelligent routing of materialized views**

There is no need to explicitly specify a MV’s name during the query, StarRocks can intelligently route to the best MV based on the query SQL. The rules for MV selection are as follows.

1. Select the MV that contains all query columns
2. Select the most matching MV by the column defined in the query’s sorting and filtering condition.
3. Select the most matching MV by the column defined in the query’s joining condition.
4. Select the MV with the smallest number of rows
5. Select the MV with the smallest number of columns

### **Other restrictions**

1. The RollUp table model must accommodate the type of the base table (Use aggregate model for aggregate tables and duplicate model for duplicate tables.).
2. The `Delete` operation is not allowed if a key in the `where` condition does not exist in a RollUp table. In this case, users can delete the materialized view first, then perform the `delete` operation, and finally re-add the materialized view.
3. If the materialized view contains columns of the `REPLACE` aggregation, it must contain all key columns.
