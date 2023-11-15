# Gather statistics for CBO

This topic describes the basic concept of StarRocks CBO and how to collect statistics for the CBO. StarRocks 2.4 introduces histograms to gather accurate data distribution statistics.

## What is CBO

Cost-based optimizer (CBO) is critical to query optimization. After an SQL query arrives at StarRocks, it is parsed into a logical execution plan. The CBO rewrites and transforms the logical plan into multiple physical execution plans. The CBO then estimates the execution cost of each operator in the plan (such as CPU, memory, network, and I/O) and chooses a query path with the lowest cost as the final physical plan.

StarRocks CBO was launched in StarRocks 1.16.0 and is enabled by default from 1.19 onwards. Developed upon the Cascades framework, StarRocks CBO estimates cost based on various statistical information. It is capable of choosing an execution plan with the lowest cost among tens of thousands of execution plans, significantly improving the efficiency and performance of complex queries.

Statistics are important for the CBO. They determine whether cost estimation is accurate and useful. The following sections detail the types of statistical information, the collection policy, and how to collect statistics and view statistical information.

## Types of statistical information

StarRocks collects a variety of statistics as the input for cost estimation.

### Basic statistics

By default, StarRocks periodically collects the following basic statistics of tables and columns:

- row_count: total number of rows in a table

- data_size: data size of a column

- ndv: column cardinality, which is the number of distinct values in a column

- null_count: the amount of data with NULL values in a column

- min: minimum value in a column

- max: maximum value in a column

Basic statistics are stored in the `_statistics_.table_statistic_v1` table. You can view this table in the `_statistics_` database of your StarRocks cluster.

### Histogram

StarRocks 2.4 introduces histograms to complement basic statistics. Histogram is considered an effective way of data representation. For tables with skewed data, histograms can accurately reflect data distribution.

StarRocks uses equi-height histograms, which are constructed on several buckets. Each bucket contains an equal amount of data. For data values that are frequently queried and have a major impact on selectivity, StarRocks allocates separate buckets for them. More buckets mean more accurate estimation, but may also cause a slight increase in memory usage. You can adjust the number of buckets and most common values (MCVs) for a histogram collection task.

**Histograms are applicable to columns with highly skewed data and frequent queries.If your table data is uniformly distributed, you do not need to create histograms. Histograms can be created only on columns of numeric, DATE, DATETIME, or string types.**

Currently, StarRocks supports only manual collection of histograms. Histograms are stored in the `_statistics_.histogram_statistics` table.

## Collection types and methods

Data size and data distribution constantly change in a table. Statistics must be updated regularly to represent that data change. Before creating a statistics collection task, you must choose a collection type and method that best suit your business requirements.

StarRocks supports full and sampled collection, both can be performed automatically and manually. By default, StarRocks automatically collects full statistics of a table. It checks for any data updates every 5 minutes. If data change is detected, data collection will be automatically triggered. If you do not want to use automatic full collection, you can set the FE configuration item `enable_collect_full_statistic` to `false` and customize a collection task.

| **Collection type** | **Collection method** | **Description**                                              | **Advantage and disadvantage**                               |
| ------------------- | --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Full collection     | Automatic/manual      | Scans the full table to gather statistics.Statistics are collected partition. If a partition has no data change, data will not be collected from this partition, reducing resource consumption.Full statistics are stored in the `_statistics_.column_statistics` table. | Advantage: The statistics are accurate, which helps the CBO make accurate estimation.Disadvantage: It consumes system resources and is slow. |
| Sampled collection  | Automatic/manual      | Evenly extracts `N` rows of data from each partition of a table.Statistics are collected by table. Basic statistics of each column are stored as one record. Cardinality information (ndv) of a column is estimated based on the sampled data, which is not accurate.Sampled statistics are stored in the `_statistics_.table_statistic_v1` table. | Advantage: It consumes less system resources and is fast.Disadvantage: The statistics are not complete, which may affect the accuracy of cost estimation. |

## Collect statistics

StarRocks offers flexible statistics collection methods. You can choose automatic, manual, or custom collection, whichever suits your business scenarios.

### Automatic full collection

For basic statistics, StarRocks automatically collects full statistics of a table by default, without requiring manual operations. For tables on which no statistics have been collected, StarRocks automatically collects statistics within the scheduling period. For tables on which statistics have been collected, StarRocks updates the total number of rows and modified rows in the tables, and persists this information regularly for judging whether to trigger automatic collection.

Conditions that trigger automatic collection:

- Table data has changed since previous statistics collection.

- The health of table statistics is below the specified threshold (`statistic_auto_collect_ratio`).

> Formula for calculating statistics health: 1 - Number of added rows since the previous statistics collection/Total number of rows in the smallest partition

- Partition data has been modified. Partitions whose data is not modified will not be collected again.

Automatic full collection is enabled by default and run by the system using the default settings.

The following table describes the default settings. If you need to modify them, run the **ADMIN SET CONFIG** command.

| **FE** **configuration item**         | **Type** | **Default value** | **Description**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| enable_statistic_collect              | BOOLEAN  | TRUE              | Whether to collect statistics. This switch is turned on by default. |
| enable_collect_full_statistic         | BOOLEAN  | TRUE              | Whether to enable automatic full collection. This switch is turned on by default. |
| statistic_collect_interval_sec        | LONG     | 300               | The interval for checking data updates during automatic collection. Unit: seconds. |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | The threshold for determining  whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered. |
| statistic_max_full_collect_data_size | INT      | 107374182400      | The size of the largest partition for automatic collection to collect data. Unit: Byte. If a partition exceeds this value, full collection is discarded and sampled collection is performed instead. |
| statistic_collect_max_row_count_per_query | INT  | 5000000000        | The maximum number of rows to query for a single analyze task. An analyze task will be split into multiple queries if this value is exceeded. |
| statistic_auto_analyze_start_time | STRING      | 00:00:00   | The start time of automatic collection. Value range: `00:00:00` - `23:59:59`. |
| statistic_auto_analyze_end_time | STRING      | 23:59:59  | The end time of automatic collection. Value range: `00:00:00` - `23:59:59`. |

You can rely on automatic jobs for a majority of statistics collection, but if you have specific statistics requirements, you can manually create a task by executing the ANALYZE TABLE statement or customize an automatic task by executing the CREATE ANALYZE  statement.

### Manual collection

You can use ANALYZE TABLE to create a manual collection task. By default, manual collection is a synchronous operation. You can also set it to an asynchronous operation. In asynchronous mode, after you run ANALYZE TABLE, the system immediately returns whether this statement is successful. However, the collection task will be running in the background and you do not have to wait for the result. You can check the status of the task by running SHOW ANALYZE STATUS. Asynchronous collection is suitable for tables with large data volume, whereas synchronous collection is suitable for tables with small data volume. **Manual collection tasks are run only once after creation. You do not need to delete manual collection tasks.**

#### Manually collect basic statistics

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
PROPERTIES (property [,property]);
```

Parameter description:

- Collection type
  - FULL: indicates full collection.
  - SAMPLE: indicates sampled collection.
  - If no collection type is specified, full collection is used by default.

- `col_name`: columns from which to collect statistics. Separate multiple columns with commas (`,`). If this parameter is not specified, the entire table is collected.

- [WITH SYNC | ASYNC MODE]: whether to run the manual collection task in synchronous or asynchronous mode. Synchronous collection is used by default if you do not specify this parameter.

- `PROPERTIES`: custom parameters. If `PROPERTIES` is not specified, the default settings in the `fe.conf` file are used. The properties that are actually used can be viewed via the `Properties` column in the output of SHOW ANALYZE STATUS.

| **PROPERTIES**                | **Type** | **Default value** | **Description**                                              |
| ----------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows | INT      | 200000            | The minimum number of rows to collect for sampled collection.If the parameter value exceeds the actual number of rows in your table, full collection is performed. |

Examples

Manual full collection

```SQL
-- Manually collect full stats of a table using default settings.
ANALYZE TABLE tbl_name;

-- Manually collect full stats of a table using default settings.
ANALYZE FULL TABLE tbl_name;

-- Manually collect stats of specified columns in a table using default settings.
ANALYZE TABLE tbl_name(c1, c2, c3);
```

Manual sampled collection

```SQL
-- Manually collect partial stats of a table using default settings.
ANALYZE SAMPLE TABLE tbl_name;

-- Manually collect stats of specified columns in a table, with the number of rows to collect specified.
ANALYZE SAMPLE TABLE tbl_name (v1, v2, v3) PROPERTIES(
    "statistic_sample_collect_rows" = "1000000"
);
```

#### Manually collect histograms

```SQL
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON col_name [, col_name]
[WITH SYNC | ASYNC MODE]
[WITH N BUCKETS]
PROPERTIES (property [,property]);
```

Parameter description:

- `col_name`: columns from which to collect statistics. Separate multiple columns with commas (`,`). If this parameter is not specified, the entire table is collected. This parameter is required for histograms.

- [WITH SYNC | ASYNC MODE]: whether to run the manual collection task in synchronous or asynchronous mode. Synchronous collection is used by default if you not specify this parameter.

- `WITH N BUCKETS`: `N` is the number of buckets for histogram collection. If not specified, the default value in `fe.conf` is used.

- PROPERTIES: custom parameters. If `PROPERTIES` is not specified, the default settings in `fe.conf` are used.

| **PROPERTIES**                 | **Type** | **Default value** | **Description**                                              |
| ------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_sample_collect_rows  | INT      | 200000            | The minimum number of rows to collect. If the parameter value exceeds the actual number of rows in your table, full collection is performed. |
| histogram_buckets_size         | LONG     | 64                | The default bucket number for a histogram.                   |
| histogram_mcv_size             | INT      | 100               | The number of most common values (MCV) for a histogram.      |
| histogram_sample_ratio         | FLOAT    | 0.1               | The sampling ratio for a histogram.                          |
| histogram_max_sample_row_count | LONG     | 10000000          | The maximum number of rows to collect for a histogram.       |

The number of rows to collect for a histogram is controlled by multiple parameters. It is the larger value between `statistic_sample_collect_rows` and table row count * `histogram_sample_ratio`. The number cannot exceed the value specified by `histogram_max_sample_row_count`. If the value is exceeded, `histogram_max_sample_row_count` takes precedence.

The properties that are actually used can be viewed via the `Properties` column in the output of SHOW ANALYZE STATUS.

Examples

```SQL
-- Manually collect histograms on v1 using the default settings.
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1;

-- Manually collect histograms on v1 and v2, with 32 buckets, 32 MCVs, and 50% sampling ratio.
ANALYZE TABLE tbl_name UPDATE HISTOGRAM ON v1,v2 WITH 32 BUCKETS 
PROPERTIES(
   "histogram_mcv_size" = "32",
   "histogram_sample_ratio" = "0.5"
);
```

### Custom collection

#### Customize an automatic collection task

You can use the CREATE ANALYZE statement to customize an automatic collection task.

Before creating a custom automatic collection task, you must disable automatic full collection (`enable_collect_full_statistic = false`). Otherwise, custom tasks cannot take effect.

```SQL
-- Automatically collect stats of all databases.
CREATE ANALYZE [FULL|SAMPLE] ALL PROPERTIES (property [,property]);

-- Automatically collect stats of all tables in a database.
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
PROPERTIES (property [,property]);

-- Automatically collect stats of specified columns in a table.
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
PROPERTIES (property [,property]);
```

Parameter description:

- Collection type
  - FULL: indicates full collection.
  - SAMPLE: indicates sampled collection.
  - If no collection type is specified, full collection is used by default.

- `col_name`: columns from which to collect statistics. Separate multiple columns with commas (`,`). If this parameter is not specified, the entire table is collected.

- `PROPERTIES`: custom parameters. If `PROPERTIES` is not specified, the default settings in `fe.conf` are used.

| **PROPERTIES**                        | **Type** | **Default value** | **Description**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | The threshold for determining  whether the statistics for automatic collection are healthy. If the statistics health is below this threshold, automatic collection is triggered. |
| statistics_max_full_collect_data_size | INT      | 100               | The size of the largest partition for automatic collection to collect data. Unit: GB.If a partition exceeds this value, full collection is discarded and sampled collection is performed instead. |
| statistic_sample_collect_rows         | INT      | 200000            | The minimum number of rows to collect.If the parameter value exceeds the actual number of rows in your table, full collection is performed. |
| statistic_exclude_pattern             | String   | null              | The name of the database or table that needs to be excluded in the job. You can specify the database and table that do not collect statistics in the job. Note that this is a regular expression pattern, and the match content is `database.table`. |

Examples

Automatic full collection

```SQL
-- Automatically collect full stats of all databases.
CREATE ANALYZE ALL;

-- Automatically collect full stats of a database.
CREATE ANALYZE DATABASE db_name;

-- Automatically collect full stats of all tables in a database.
CREATE ANALYZE FULL DATABASE db_name;

-- Automatically collect full stats of specified columns in a table.
CREATE ANALYZE TABLE tbl_name(c1, c2, c3); 

-- Automatically collect stats of all databases, excluding specified database 'db_name'.
CREATE ANALYZE ALL PROPERTIES (
   "statistic_exclude_pattern" = "db_name\."
);
```

Automatic sampled collection

```SQL
-- Automatically collect stats of all tables in a database with default settings.
CREATE ANALYZE SAMPLE DATABASE db_name;

-- Automatically collect stats of all tables in a database, excluding specified table 'db_name.tbl_name'.
CREATE ANALYZE SAMPLE DATABASE db_name PROPERTIES (
   "statistic_exclude_pattern" = "db_name\.tbl_name"
);

-- Automatically collect stats of specified columns in a table, with statistics health and the number of rows to collect specified.
CREATE ANALYZE SAMPLE TABLE tbl_name(c1, c2, c3) PROPERTIES (
   "statistic_auto_collect_ratio" = "0.5",
   "statistic_sample_collect_rows" = "1000000"
);

```

#### View custom collection tasks

```SQL
SHOW ANALYZE JOB [WHERE predicate]
```

You can filter results by using the WHERE clause. The statement returns the following columns.

| **Column**   | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | The ID of the collection task.                               |
| Database     | The database name.                                           |
| Table        | The table name.                                              |
| Columns      | The column names.                                            |
| Type         | The type of statistics, including `FULL` and `SAMPLE`.       |
| Schedule     | The type of scheduling. The type is `SCHEDULE` for an automatic task. |
| Properties   | Custom parameters.                                           |
| Status       | The task status, including PENDING, RUNNING, SUCCESS, and FAILED. |
| LastWorkTime | The time of the last collection.                             |
| Reason       | The reason why the task failed. NULL is returned if task execution was successful. |

Examples

```SQL
-- View all the custom collection tasks.
SHOW ANALYZE JOB

-- View custom collection tasks of database `test`.
SHOW ANALYZE JOB where `database` = 'test';
```

#### Delete a custom collection task

```SQL
DROP ANALYZE <ID>;
```

The task ID can be obtained by using the SHOW ANALYZE JOB statement.

Examples

```SQL
DROP ANALYZE 266030;
```

## View status of collection tasks

You can view the status of all the current tasks by running the SHOW ANALYZE STATUS statement. This statement cannot be used to view the status of custom collection tasks. To view the status of custom collection tasks, use SHOW ANALYZE JOB.

```SQL
SHOW ANALYZE STATUS [WHERE predicate];
```

You can use `LILE or WHERE` to filter the information to return.

This statement returns the following columns.

| **List name** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| Id            | The ID of the collection task.                               |
| Database      | The database name.                                           |
| Table         | The table name.                                              |
| Columns       | The column names.                                            |
| Type          | The type of statistics, including FULL, SAMPLE, and HISTOGRAM. |
| Schedule      | The type of scheduling. `ONCE` means manual and `SCHEDULE` means automatic. |
| Status        | The status of the task.                                      |
| StartTime     | The time when the task starts executing.                     |
| EndTime       | The time when the task execution ends.                       |
| Properties    | Custom parameters.                                           |
| Reason        | The reason why the task failed. NULL is returned if the execution was successful. |

## View statistics

### View metadata of basic statistics

```SQL
SHOW STATS META [WHERE];
```

This statement returns the following columns.

| **Column** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | The database name.                                           |
| Table      | The table name.                                              |
| Columns    | The column names.                                            |
| Type       | The type of statistics. `FULL` means full collection and `SAMPLE` means sampled collection. |
| UpdateTime | The latest statistics update time for the current table.     |
| Properties | Custom parameters.                                           |
| Healthy    | The health of statistical information.                       |

### View metadata of histograms

```SQL
SHOW HISTOGRAM META [WHERE];
```

This statement returns the following columns.

| **Column** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Database   | The database name.                                           |
| Table      | The table name.                                              |
| Column     | The columns.                                                 |
| Type       | Type of statistics. The value is `HISTOGRAM` for histograms. |
| UpdateTime | The latest statistics update time for the current table.     |
| Properties | Custom parameters.                                           |

## Delete statistics

You can delete statistical information you do not need. When you delete statistics, both the data and metadata of the statistics are deleted, as well as the statistics in expired cache. Note that if an automatic collection task is ongoing, previously deleted statistics may be collected again. You can use `SHOW ANALYZE STATUS` to view the history of collection tasks.

### Delete basic statistics

```SQL
DROP STATS tbl_name
```

### Delete histograms

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name];
```

## Cancel a collection task

You can use the KILL ANALYZE statement to cancel a **running** collection task, including manual and custom tasks.

```SQL
KILL ANALYZE <ID>;
```

The task ID for a manual collection task can be obtained from SHOW ANALYZE STATUS. The task ID for a custom collection task can be obtained from SHOW ANALYZE SHOW ANALYZE JOB.

## FE configuration items

| **FE** **configuration item**        | **Type** | **Default value** | **Description**                                              |
| ------------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| enable_statistic_collect             | BOOLEAN  | TRUE              | Whether to collect statistics. This parameter is turned on by default. |
| enable_collect_full_statistic        | BOOLEAN  | TRUE              | Whether to enable automatic full statistics collection. This parameter is turned on by default. |
| statistic_auto_collect_ratio         | FLOAT    | 0.8               | The threshold for determining  whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered. |
| statistic_max_full_collect_data_size | LONG     | 107374182400      | The size of the largest partition for automatic collection to collect data. Unit: Byte. If a partition exceeds this value, full collection is discarded and sampled collection is performed instead. |
| statistic_collect_max_row_count_per_query | INT | 5000000000        | The maximum number of rows to query for a single analyze task. An analyze task will be split into multiple queries if this value is exceeded. |
| statistic_collect_interval_sec       | LONG     | 300               | The interval for checking data updates during automatic collection. Unit: seconds. |
| statistic_sample_collect_rows        | LONG     | 200000            | The minimum number of rows to collect for sampled collection. If the parameter value exceeds the actual number of rows in your table, full collection is performed. |
| statistic_collect_concurrency        | INT      | 3                 | The maximum number of manual collection tasks that can run in parallel. The value defaults to 3, which means you can run a maximum of three manual collections tasks in parallel. If the value is exceeded, incoming tasks will be in the PENDING state, waiting to be scheduled. |
| histogram_buckets_size               | LONG     | 64                | The default bucket number for a histogram.                   |
| histogram_mcv_size                   | LONG     | 100               | The number of most common values (MCV) for a histogram.      |
| histogram_sample_ratio               | FLOAT    | 0.1               | The sampling ratio for a histogram.                          |
| histogram_max_sample_row_count       | LONG     | 10000000          | The maximum number of rows to collect for a histogram.       |
| statistic_manager_sleep_time_sec     | LONG     | 60                | The interval at which metadata is scheduled. Unit: seconds. The system performs the following operations based on this interval:Create tables for storing statistics.Delete statistics that have been deleted.Delete expired statistics. |
| statistic_analyze_status_keep_second | LONG     | 259200            | The duration to retain the history of collection tasks. The default value is 3 days. Unit: seconds. |

## References

- To query FE configuration items, run [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md).

- To modify FE configuration items, run [ADMIN SET CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md).
