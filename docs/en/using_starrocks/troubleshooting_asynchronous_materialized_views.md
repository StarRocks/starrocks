---
displayed_sidebar: "English"
---

# Troubleshooting asynchronous materialized views

This topic describes how to examine your asynchronous materialized views and solve the problems you have encountered while working with them.

> **CAUTION**
>
> Some of the features shown below are only supported from StarRocks v3.1 onwards.

## Examine an asynchronous materialized view

To get a whole picture of asynchronous materialized views that you are working with, you can first check their working state, refresh history, and resource consumption.

### Check the working state of an asynchronous materialized view

You can check the working state of an asynchronous materialized view using [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md). Among all the information returned, you can focus on the following fields:

- `is_active`: Whether the state of the materialized view is active. Only an active materialized view can be used for query acceleration and rewrite.
- `last_refresh_state`: The state of the last refresh, including PENDING, RUNNING, FAILED, and SUCCESS.
- `last_refresh_error_message`: The reason why the last refresh failed (if the materialized view state is not active).
- `rows`: The number of data rows in the materialized view. Please note that this value can be different from the actual row count of the materialized view because the updates can be deferred.

For detailed information on other fields returned, see [SHOW MATERIALIZED VIEWS - Returns](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md#returns).

Example:

```Plain
MySQL > SHOW MATERIALIZED VIEWS LIKE 'mv_pred_2'\G
***************************[ 1. row ]***************************
id                                   | 112517
database_name                        | ssb_1g
name                                 | mv_pred_2
refresh_type                         | ASYNC
is_active                            | true
inactive_reason                      | <null>
partition_type                       | UNPARTITIONED
task_id                              | 457930
task_name                            | mv-112517
last_refresh_start_time              | 2023-08-04 16:46:50
last_refresh_finished_time           | 2023-08-04 16:46:54
last_refresh_duration                | 3.996
last_refresh_state                   | SUCCESS
last_refresh_force_refresh           | false
last_refresh_start_partition         |
last_refresh_end_partition           |
last_refresh_base_refresh_partitions | {}
last_refresh_mv_refresh_partitions   |
last_refresh_error_code              | 0
last_refresh_error_message           |
rows                                 | 0
text                                 | CREATE MATERIALIZED VIEW `mv_pred_2` (`lo_quantity`, `lo_revenue`, `sum`)
DISTRIBUTED BY HASH(`lo_quantity`, `lo_revenue`) BUCKETS 2
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"storage_medium" = "HDD"
)
AS SELECT `lineorder`.`lo_quantity`, `lineorder`.`lo_revenue`, sum(`lineorder`.`lo_tax`) AS `sum`
FROM `ssb_1g`.`lineorder`
WHERE `lineorder`.`lo_linenumber` = 1
GROUP BY 1, 2;

1 row in set
Time: 0.003s
```

### View the refresh history of an asynchronous materialized view

You can view the refresh history of an asynchronous materialized view by querying the table `task_runs` in the database `information_schema`. Among all the information returned, you can focus on the following fields:

- `CREATE_TIME` and `FINISH_TIME`: The start and end time of the refresh task.
- `STATE`: The state of the refresh task, including PENDING, RUNNING, FAILED, and SUCCESS.
- `ERROR_MESSAGE`: The reason why the refresh task failed.

Example:

```Plain
MySQL > SELECT * FROM information_schema.task_runs WHERE task_name ='mv-112517' \G
***************************[ 1. row ]***************************
QUERY_ID      | 7434cee5-32a3-11ee-b73a-8e20563011de
TASK_NAME     | mv-112517
CREATE_TIME   | 2023-08-04 16:46:50
FINISH_TIME   | 2023-08-04 16:46:54
STATE         | SUCCESS
DATABASE      | ssb_1g
EXPIRE_TIME   | 2023-08-05 16:46:50
ERROR_CODE    | 0
ERROR_MESSAGE | <null>
PROGRESS      | 100%
EXTRA_MESSAGE | {"forceRefresh":false,"mvPartitionsToRefresh":[],"refBasePartitionsToRefreshMap":{},"basePartitionsToRefreshMap":{}}
PROPERTIES    | {"FORCE":"false"}
***************************[ 2. row ]***************************
QUERY_ID      | 72dd2f16-32a3-11ee-b73a-8e20563011de
TASK_NAME     | mv-112517
CREATE_TIME   | 2023-08-04 16:46:48
FINISH_TIME   | 2023-08-04 16:46:53
STATE         | SUCCESS
DATABASE      | ssb_1g
EXPIRE_TIME   | 2023-08-05 16:46:48
ERROR_CODE    | 0
ERROR_MESSAGE | <null>
PROGRESS      | 100%
EXTRA_MESSAGE | {"forceRefresh":true,"mvPartitionsToRefresh":["mv_pred_2"],"refBasePartitionsToRefreshMap":{},"basePartitionsToRefreshMap":{"lineorder":["lineorder"]}}
PROPERTIES    | {"FORCE":"true"}
```

### Monitor the resource consumption of an asynchronous materialized view

You can monitor and analyze the resource consumed by an asynchronous materialized view during and after the refresh.

#### Monitor resource consumption during refresh

During a refresh task, you can monitor its real-time resource consumption using [SHOW PROC '/current_queries'](../sql-reference/sql-statements/Administration/SHOW_PROC.md).

Among all the information returned, you can focus on the following fields:

- `ScanBytes`: The size of data that is scanned.
- `ScanRows`: The number of data rows that are scanned.
- `MemoryUsage`: The size of memory that is used.
- `CPUTime`: CPU time cost.
- `ExecTime`: The execution time of the query.

Example:

```Plain
MySQL > SHOW PROC '/current_queries'\G
***************************[ 1. row ]***************************
StartTime     | 2023-08-04 17:01:30
QueryId       | 806eed7d-32a5-11ee-b73a-8e20563011de
ConnectionId  | 0
Database      | ssb_1g
User          | root
ScanBytes     | 70.981 MB
ScanRows      | 6001215 rows
MemoryUsage   | 73.748 MB
DiskSpillSize | 0.000
CPUTime       | 2.515 s
ExecTime      | 2.583 s
```

#### Analyze resource consumption after refresh

After a refresh task, you can analyze its resource consumption via query profiles.

While an asynchronous materialized view refreshes itself, an INSERT OVERWRITE statement is executed. You can check the corresponding query profile to analyze the time and resources consumed by the refresh task.

Among all the information returned, you can focus on the following metrics:

- `Total`: Total time consumed by the query.
- `QueryCpuCost`: Total CPU time cost of the query. CPU time costs are aggregated for concurrent processes. As a result, the value of this metric may be greater than the actual execution time of the query.
- `QueryMemCost`: Total memory cost of the query.
- Other metrics for individual operators, such as join operators and aggregate operators.

For detailed information on how to check the query profile and understand other metrics, see [Analyze query profile](../administration/query_profile.md).

### Verify whether queries are rewritten by an asynchronous materialized view

You can check whether a query can be rewritten with an asynchronous materialized view from its query plan using [EXPLAIN](../sql-reference/sql-statements/Administration/EXPLAIN.md).

If the metric `SCAN` in the query plan shows the name of the corresponding materialized view, the query has been rewritten by the materialized view.

Example 1:

```Plain
MySQL > SHOW CREATE TABLE mv_agg\G
***************************[ 1. row ]***************************
Materialized View        | mv_agg
Create Materialized View | CREATE MATERIALIZED VIEW `mv_agg` (`c_custkey`)
DISTRIBUTED BY RANDOM
REFRESH ASYNC
PROPERTIES (
"replication_num" = "3",
"replicated_storage" = "true",
"storage_medium" = "HDD"
)
AS SELECT `customer`.`c_custkey`
FROM `ssb_1g`.`customer`
GROUP BY `customer`.`c_custkey`;

MySQL > EXPLAIN LOGICAL SELECT `customer`.`c_custkey`
                      -> FROM `ssb_1g`.`customer`
                      -> GROUP BY `customer`.`c_custkey`;
+-----------------------------------------------------------------------------------+
| Explain String                                                                    |
+-----------------------------------------------------------------------------------+
| - Output => [1:c_custkey]                                                         |
|     - SCAN [mv_agg] => [1:c_custkey]                                              |
|             Estimates: {row: 30000, cpu: ?, memory: ?, network: ?, cost: 15000.0} |
|             partitionRatio: 1/1, tabletRatio: 12/12                               |
|             1:c_custkey := 10:c_custkey                                           |
+-----------------------------------------------------------------------------------+
```

If you disable the query rewrite feature, StarRocks adopts the regular query plan.

Example 2:

```Plain
MySQL > SET enable_materialized_view_rewrite = false;
MySQL > EXPLAIN LOGICAL SELECT `customer`.`c_custkey`
                      -> FROM `ssb_1g`.`customer`
                      -> GROUP BY `customer`.`c_custkey`;
+---------------------------------------------------------------------------------------+
| Explain String                                                                        |
+---------------------------------------------------------------------------------------+
| - Output => [1:c_custkey]                                                             |
|     - AGGREGATE(GLOBAL) [1:c_custkey]                                                 |
|             Estimates: {row: 15000, cpu: ?, memory: ?, network: ?, cost: 120000.0}    |
|         - SCAN [mv_bitmap] => [1:c_custkey]                                           |
|                 Estimates: {row: 60000, cpu: ?, memory: ?, network: ?, cost: 30000.0} |
|                 partitionRatio: 1/1, tabletRatio: 12/12                               |
+---------------------------------------------------------------------------------------+
```

## Diagnose and solve problems

Here we list some common problems you might encounter while working with an asynchronous materialized view, and the corresponding solutions.

### Failed to create an asynchronous materialized view

If you failed to create an asynchronous materialized view, that is, the CREATE MATERIALIZED VIEW statement cannot be executed, you can look into the following aspects:

- **Check whether you have mistakenly used the  SQL  statement for synchronous materialized views.**
  
  StarRocks provides two different materialized views: the synchronous materialized view and the asynchronous materialized view.

  The basic SQL statement used to create a synchronous materialized view is as follows:

  ```SQL
  CREATE MATERIALIZED VIEW <mv_name> 
  AS <query>
  ```

  However, the SQL statement used to create an asynchronous materialized view contains more parameters:

  ```SQL
  CREATE MATERIALIZED VIEW <mv_name> 
  REFRESH ASYNC -- The refresh strategy of the asynchronous materialized view.
  DISTRIBUTED BY HASH(<column>) -- The data distribution strategy of the asynchronous materialized view.
  AS <query>
  ```

  In addition to the SQL statement, the main difference between the two materialized views is that asynchronous materialized views support all query syntax that StarRocks provides, but synchronous materialized views only support limited choices of aggregate functions.

- **Check whether you have specified a correct  `Partition By`  column.**

  When creating an asynchronous materialized view, you can specify a partitioning strategy, which allows you to refresh the materialized view on a finer granularity level.

  Currently, StarRocks only supports range partitioning, and only support referencing a single column from the SELECT expression in the query statement that is used to build the materialized view. You can use the date_trunc() function to truncate the column to change the granularity level of the partitioning strategy. Please note that any other expressions are not supported.

- **Check whether you have the necessary privileges to create the  materialized view****.**

  When creating an asynchronous materialized view, you need the SELECT privileges of all objects (tables, views, materialized views) that are queried. When UDFs are used in the query, you also need the USAGE privileges of the functions.

### Materialized view fails to refresh

If the materialized view fails to refresh, that is, the state of the refresh task is not SUCCESS, you can look into the following aspects:

- **Check whether you have adopted an inappropriate refresh strategy.**

  By default, the materialized view refreshes immediately after it is created. However, in v2.5 and early versions, materialized view that adopts the MANUAL refresh strategy does not refresh after being created. You must refresh it manually using REFRESH MATERIALIZED VIEW.

- **Check whether the refresh task exceeds the memory limit.**

  Usually, when an asynchronous materialized view involves large-scale aggregation or join calculations that exhaust the memory resources. To solve this problem, you can:

  - Specify a partitioning strategy for the materialized view to refresh one partition each time.
  - Enable the Spill to Disk feature for the refresh task. From v3.1 onwards, StarRocks supports spilling the intermediate results to disks when refreshing a materialized view. Execute the following statement to enable Spill to Disk:

    ```SQL
    SET enable_spill = true;
    ```

- **Check whether the refresh task exceeds the timeout duration.**

  A large-scale materialized view can fail to refresh because the refresh task exceeds the timeout duration. To solve this problem, you can:

  - Specify a partitioning strategy for the materialized view to refresh one partition each time.
  - Set a longer timeout duration.

From v3.0 onwards, you can define the following properties (session variables) while creating the materialized view or adding them using ALTER MATERIALIZED VIEW.

Example:

```SQL
-- Define the properties when creating the materialized view
CREATE MATERIALIZED VIEW mv1 
REFRESH ASYNC
PROPERTIES ( 'session.enable_spill'='true' )
AS <query>;

-- Add the properties.
ALTER MATERIALIZED VIEW mv2 
    SET ('session.enable_spill' = 'true');
```

### Materialized view state is not active

If the materialized view cannot rewrite queries or refresh, and the state of the materialized view `is_active` is `false`, it could be a result of the schema change on the base tables. To solve this problem, you can manually set the materialized view state to active by executing the following statement:

```SQL
ALTER MATERIALIZED VIEW mv1 ACTIVE;
```

If setting the materialized view state to active does not take effect, you need to drop the materialized view and create it again.

### Materialized view refresh task uses excessive resources

If you find that the refresh tasks are using excessive system resources, you can look into the following aspects:

- **Check whether you have created an oversized  materialized view****.**

  If you have joined too many tables that cause a significant amount of calculation, the refresh task will occupy many resources. To solve this problem, you need to assess the size of the materialized view and re-plan it.

- **Check whether you have set an unnecessarily frequent refresh interval.**

  If you adopt the fixed-interval refresh strategy, you can set a lower refresh frequency to solve the problem. If the refresh tasks are triggered by data changes in the base tables, loading data too frequently can also cause this problem. To solve this problem, you need to define a proper refresh strategy for your materialized view.

- **Check whether the  materialized view  is partitioned.**

  An unpartitioned materialized view can be costly to refresh because StarRocks refreshes the whole materialized view each time. To solve this problem, you need to specify a partitioning strategy for the materialized view to refresh one partition each time.

To stop a refresh task that occupies too many resources, you can:

- Set the materialized view state to inactive so that all of its refresh tasks are stopped:

  ```SQL
  ALTER MATERIALIZED VIEW mv1 INACTIVE;
  ```

- Terminate the running refresh task by using SHOW PROCESSLIST and KILL:

  ```SQL
  -- Get the ConnectionId of the running refresh task.
  SHOW PROCESSLIST;
  -- Terminate the running refresh task.
  KILL QUERY <ConnectionId>;
  ```

### Materialized view fails to rewrite queries

If your materialized view fails to rewrite relevant queries, you can look into the following aspects:

- **Check whether the  materialized view  and the  query  match.**

  - StarRocks matches the materialized view and the query with a structure-based matching technique rather than text-based matching. Therefore, it's not guaranteed that a query can be rewritten just because it appears similar to that of a materialized view.
  - Materialized views can only rewrite SPJG (Select/Projection/Join/Aggregation) type of queries. Queries involving window functions, nested aggregation, or join plus aggregation are not supported.
  - Materialized views cannot rewrite queries that involve complex Join predicates in Outer Joins. For example, in cases like `A LEFT JOIN B ON A.dt > '2023-01-01' AND A.id = B.id`, we recommend you specify the predicate from the `JOIN ON` clause in a `WHERE` clause.

  For more information on the limitations of the materialized view query rewrite, see [Query rewrite with materialized views - Limitations](./query_rewrite_with_materialized_views.md#limitations).

- **Check whether the  materialized view  state is active.**

  StarRocks checks the status of the materialized view before rewriting queries. Queries can be rewritten only when the materialized view state is active. To solve this problem, you can manually set the materialized view state to active by executing the following statement:

  ```SQL
  ALTER MATERIALIZED VIEW mv1 ACTIVE;
  ```

- **Check whether the  materialized view  meets the data  consistency  requirements.**

  StarRocks checks the consistency of data in the materialized view and in the base table data. By default, queries can be rewritten only when the data in the materialized view is up-to-date. To solve this problem, you can:

  - Add `PROPERTIES('query_rewrite_consistency'='LOOSE')` to the materialized view to disable consistency checks.
  - Add `PROPERTIES('mv_rewrite_staleness_second'='5')` to tolerate a certain degree of data inconsistency. Queries can be rewritten if the last refresh is before this time interval, regardless of whether the data in the base tables changes.

- **Check whether the  query  statement of the  materialized view  lacks output columns.**

  To rewrite range and point queries, you must specify the columns used as the filtering predicates in the SELECT expression of the materialized view's query statement. You need to check the SELECT statement of the materialized view to ensure it includes columns referenced in the WHERE and ORDER BY clauses of the query.

Example 1: The materialized view `mv1` uses nested aggregation. Thus, it cannot be used to rewrite queries.

```SQL
CREATE MATERIALIZED VIEW mv1 REFRESH ASYNC AS
select count(distinct cnt) 
from (
    select c_city, count(*) cnt 
    from customer 
    group by c_city
) t;
```

Example 2: The materialized view `mv2` uses join plus aggregation. Thus, it cannot be used to rewrite queries. To solve this problem, you can create a materialized view with aggregation, and then a nested materialized view with join based on the previous one.

```SQL
CREATE MATERIALIZED VIEW mv2 REFRESH ASYNC AS
select *
from (
    select lo_orderkey, lo_custkey, p_partkey, p_name
    from lineorder
    join part on lo_partkey = p_partkey
) lo
join (
    select c_custkey
    from customer
    group by c_custkey
) cust
on lo.lo_custkey = cust.c_custkey;
```

Example 3: The materialized view `mv3` cannot rewrite queries in the pattern of `SELECT c_city, sum(tax) FROM tbl WHERE dt='2023-01-01' AND c_city = 'xxx'` because the column that the predicate referenced is not in the SELECT expression.

```SQL
CREATE MATERIALIZED VIEW mv3 REFRESH ASYNC AS
SELECT c_city, sum(tax) FROM tbl GROUP BY c_city;
```

To solve this problem, you can create the materialized view as follows:

```SQL
CREATE MATERIALIZED VIEW mv3 REFRESH ASYNC AS
SELECT dt, c_city, sum(tax) FROM tbl GROUP BY dt, c_city;
```
