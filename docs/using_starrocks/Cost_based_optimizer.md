# Cost Based Optimizer

StarRocks 1.16.0 introduces cost-based optimizer (CBO). This feature is enabled by default in StarRocks 1.19 and later. CBO can select the optimal execution plan based on cost, significantly improving the performance of complex queries.

## What is CBO

The CBO optimizer uses the Cascades framework, estimates costs by using a variety of statistical information, and adds the Transformation and Implementation rules. All these enable the CBO to select the optimal execution plan with the lowest cost among tens of thousands of execution plans in  the search space.

The CBO optimizer uses a variety of statistics that StarRocks collects periodically, such as the number of rows, average size of columns, cardinality information, amount of NULL-valued data, and MAX/MIN value. These statistics are stored in `_statistics_.table_statistic_v1`.

StarRocks supports full or sampled statistics collection in a manual or periodic way, which can help the CBO optimizer refine cost estimation and choose the optimal execution plan.

| **Collection Type**           | **Collection Method**                  | **Description**                                              | **Advantages and Disadvantages**                             |
| ----------------------------- | -------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Full statistics collection    | Manual collection  Periodic collection | Gather statistics for the full table.                        | Advantages: Full statistics collection generates more accurate statistics that can help CBO more accurately evaluate execution plans.  Disadvantages: Full statistics collection consumes a lot of system resources and is relatively slow. |
| Sampled statistics collection | Manual collection  Periodic collection | Gather statistics of N rows of data from each partition of the table. | Advantages: Sampled statistics collection consumes less system resources and is relatively fast.  Disadvantages: Sampled statistics collection may cause errors, which may affect the accuracy of the CBO's choice of the optimal execution plan. |

> - Manual collection: Manually perform a task to gather statistics.
> - Periodic collection: Periodically perform a task to gather statistics. The collection interval is one day by default, and StarRocks checks for data updates every two hours by default. If data is updated and the collection interval has elapsed (one day by default), StarRocks collects the statistics again. If no data is updated or the collection interval has not elapsed, StarRocks does not collect the statistics again.

## Statistics Collection

When CBO is enabled, it consumes statistics collected by the Statistics Collector in a sampled and collected fashion. The Statistics Collector checks for data updates every two hours. If data is updated and the time from the previous collection has exceeded one day, Statistics Collector collects the statistics again by sampling 200,000 rows. You can also adjust the type of statistics collection (full or sampled) and the collection method (manual or periodic) according to your business needs.

### Collect full statistics

Choose a collection method (manual or periodic) and run the relevant commands. For descriptions of relevant parameters, see Parameters.

- If you need to perform manual collection, you can run the following command.

    ```SQL
    ANALYZE FULL TABLE tbl_name(columnA, columnB, columnC...);
    ```

- If you need to perform periodic collection, you can run the following commands to configure the collection interval and check interval.

    > If multiple periodic collection tasks gather statistics from the same object, the CBO runs only the latest periodic collection task (task with the largest ID).

    ```SQL
    -- Periodically collect full statistics from all databases.
    CREATE ANALYZE FULL ALL
    PROPERTIES(
        "update_interval_sec" = "43200",
        "collect_interval_sec" = "3600"
    );

    -- Periodically collect full statistics from all tables in a specified database.
    CREATE ANALYZE FULL DATABASE db_name
    PROPERTIES(
        "update_interval_sec" = "43200",
        "collect_interval_sec" = "3600"
    );

    -- Periodically collect full statistics from the specified tables and columns.
    CREATE ANALYZE FULL TABLE tbl_name(columnA, columnB, columnC...) 
    PROPERTIES(
        "update_interval_sec" = "43200",
        "collect_interval_sec" = "3600"
    );
    ```

    Example:

    ```SQL
    -- Periodically collect full statistics from all tables under the tpch database with default collection interval and default check interval.
    CREATE ANALYZE FULL DATABASE tpch;
    ```

### Collect sampled statistics

Choose a collection method (manual or periodic) and run the relevant commands. For descriptions of relevant parameters, see Parameters.

- you need to perform manual collection, you can run the following command to configure the number of rows to be sampled.

    ```SQL
    ANALYZE TABLE tbl_name(columnA, columnB, columnC...)
    PROPERTIES(
        "sample_collect_rows" = "100000"
    );
    ```

- If you need to perform periodic collection, you can run the following command to configure the number of rows to be sampled, collection interval, and check interval.

    > If multiple periodic collection tasks gather statistics from the same object, the CBO runs only the latest periodic collection task (task with the largest ID).

    ```SQL
    -- Periodically collect sampled statistics from all databases.
    CREATE ANALYZE ALL
    PROPERTIES(
        "sample_collect_rows" = "100000",
        "update_interval_sec" = "43200",
        "collect_interval_sec" = "3600"
    );

    -- Periodically collect sampled statistics from all tables in a specified database.
    CREATE ANALYZE DATABASE db_name
    PROPERTIES(
        "sample_collect_rows" = "100000",
        "update_interval_sec" = "43200",
        "collect_interval_sec" = "3600"
    );

    -- Periodically collect sampled statistics from the specified tables and columns.
    CREATE ANALYZE TABLE tbl_name(columnA, columnB, columnC...)
    PROPERTIES(
        "sample_collect_rows" = "100000",
        "update_interval_sec" = "43200",
        "collect_interval_sec" = "3600"
    );
    ```

    Example:

    ```SQL
    -- Periodically collect sampled statistics at intervals of 43,200 seconds (12 hours), with a default check interval.
    CREATE ANALYZE ALL PROPERTIES("update_interval_sec" = "43200");

    -- Periodically collect sampled statistics for column v1 in the table test, with the default collection interval and check interval.
    CREATE ANALYZE TABLE test(v1);
    ```

### View or delete collection tasks

- Run the following command to view all collection tasks.

    ```SQL
    SHOW ANALYZE;
    ```

- Run the following command to delete the specified collection task.

    > `ID` is the collection task ID, which can be obtained by running the `SHOW ANALYZE` command.

    ```SQL
    DROP ANALYZE <ID>;
    ```

### Parameters

- `sample_collect_rows`: The number of rows to be sampled during sampled statistics collection.

- `update_interval_sec`: The collection interval for periodic collection. Default value: 86400 seconds (one day). Unit: seconds.

- `collect_interval_sec`: The interval for checking data updates in a periodic task. The default value is 7,200 seconds (two hours). During a periodic task, StarRocks will check whether the table is updated at the interval specified by this parameter. If data is updated and the time from the previous collection has exceeded `update_interval_sec`, StarRocks collects the statistics again. If no  data is updated and the time from the last collection has not exceeded `update_interval_sec`, StarRocks does not collect the statistics again.

## FE Configuration Items

You can check or modify the default configuration for statistics collection in the FE configuration file **fe.conf**.

```bash
# Whether to collect statistics.
enable_statistic_collect = true;

# The interval for checking data updates in a periodic task. The default value is 7,200 seconds (two hours). 
statistic_collect_interval_sec = 7200;

# The collection interval for periodic collection. The default value is 86,400 seconds (one day).
statistic_update_interval_sec = 86400;

# The number of rows to be sampled during sampled statistics collection. The default value is 200,000 rows.
statistic_sample_collect_rows = 200000;
```
