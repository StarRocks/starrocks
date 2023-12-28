---
displayed_sidebar: "English"
---

# Gather statistics for CBO

This topic describes the basic concept of StarRocks cost-based optimizer (CBO) and how to collect statistics for the CBO to select an optimal query plan. StarRocks 2.4 introduces histograms to gather accurate data distribution statistics.

Since v3.2.0, StarRocks supports collecting statistics from Hive, Iceberg, and Hudi tables, reducing the dependency on other metastore systems. The syntax is similar to collecting StarRocks internal tables.

## What is CBO

CBO is critical to query optimization. After an SQL query arrives at StarRocks, it is parsed into a logical execution plan. The CBO rewrites and transforms the logical plan into multiple physical execution plans. The CBO then estimates the execution cost of each operator in the plan (such as CPU, memory, network, and I/O) and chooses a query path with the lowest cost as the final physical plan.

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

Full statistics are stored in the `column_statistics` of the `_statistics_` database. You can view this table to query table statistics. Following is an example of querying statistics data from this table.

```sql
SELECT * FROM _statistics_.column_statistics\G
*************************** 1. row ***************************
      table_id: 10174
  partition_id: 10170
   column_name: is_minor
         db_id: 10169
    table_name: zj_test.source_wiki_edit
partition_name: p06
     row_count: 2
     data_size: 2
           ndv: NULL
    null_count: 0
           max: 1
           min: 0
```

### Histogram

StarRocks 2.4 introduces histograms to complement basic statistics. Histogram is considered an effective way of data representation. For tables with skewed data, histograms can accurately reflect data distribution.

StarRocks uses equi-height histograms, which are constructed on several buckets. Each bucket contains an equal amount of data. For data values that are frequently queried and have a major impact on selectivity, StarRocks allocates separate buckets for them. More buckets mean more accurate estimation, but may also cause a slight increase in memory usage. You can adjust the number of buckets and most common values (MCVs) for a histogram collection task.

**Histograms are applicable to columns with highly skewed data and frequent queries.If your table data is uniformly distributed, you do not need to create histograms. Histograms can be created only on columns of numeric, DATE, DATETIME, or string types.**

Currently, StarRocks supports only manual collection of histograms. Histograms are stored in the `histogram_statistics` table of the `_statistics_` database. Following is an example of querying statistics data from this table:

```sql
SELECT * FROM _statistics_.histogram_statistics\G
*************************** 1. row ***************************
   table_id: 10174
column_name: added
      db_id: 10169
 table_name: zj_test.source_wiki_edit
    buckets: NULL
        mcv: [["23","1"],["5","1"]]
update_time: 2023-12-01 15:17:10.274000
```

## Collection types and methods

Data size and data distribution constantly change in a table. Statistics must be updated regularly to represent that data change. Before creating a statistics collection task, you must choose a collection type and method that best suit your business requirements.

StarRocks supports full and sampled collection, both can be performed automatically and manually. By default, StarRocks automatically collects full statistics of a table. It checks for any data updates every 5 minutes. If data change is detected, data collection will be automatically triggered. If you do not want to use automatic full collection, you can set the FE configuration item `enable_collect_full_statistic` to `false` and customize a collection task.

| **Collection type** | **Collection method** | **Description**                                              | **Advantage and disadvantage**                               |
| ------------------- | --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Full collection     | Automatic/manual      | Scans the full table to gather statistics.Statistics are collected partition. If a partition has no data change, data will not be collected from this partition, reducing resource consumption.Full statistics are stored in the `_statistics_.column_statistics` table. | Advantage: The statistics are accurate, which helps the CBO make accurate estimation.Disadvantage: It consumes system resources and is slow. From 2.5 onwards, StarRocks allows you to specify an automatic collection period, which reduces resource consumption. |
| Sampled collection  | Automatic/manual      | Evenly extracts `N` rows of data from each partition of a table.Statistics are collected by table. Basic statistics of each column are stored as one record. Cardinality information (ndv) of a column is estimated based on the sampled data, which is not accurate. Sampled statistics are stored in the `_statistics_.table_statistic_v1` table. | Advantage: It consumes less system resources and is fast.Disadvantage: The statistics are not complete, which may affect the accuracy of cost estimation. |

## Collect statistics

StarRocks offers flexible statistics collection methods. You can choose automatic, manual, or custom collection, whichever suits your business scenarios.

### Automatic collection

For basic statistics, StarRocks automatically collects full statistics of a table by default, without requiring manual operations. For tables on which no statistics have been collected, StarRocks automatically collects statistics within the scheduling period. For tables on which statistics have been collected, StarRocks updates the total number of rows and modified rows in the tables, and persists this information regularly for judging whether to trigger automatic collection.

From 2.4.5 onwards, StarRocks allows you to specify a collection period for automatic full collection, which prevents cluster performance jitter caused by automatic full collection. This period is specified by FE parameters `statistic_auto_analyze_start_time` and `statistic_auto_analyze_end_time`.

Conditions that will trigger automatic collection:

- Table data has changed since previous statistics collection.

- The collection time falls within the range of the configured collection period. (The default collection period is all day.)

- The update time of the previous collecting job is earlier than the latest update time of partitions.

- The health of table statistics is below the specified threshold (`statistic_auto_collect_ratio`).

> Formula for calculating statistics health:
>
> If the number of partitions with data updated is less than 10, the formula is `1 - (Number of updated rows since previous collection/Total number of rows)`.
> If the number of partitions with data updated is greater than or equal to 10, the formula is `1 - MIN(Number of updated rows since previous collection/Total number of rows, Number of updated partitions since previous collection/Total number of partitions)`.

In addition, StarRocks allows you to configure collection policies based on table size and table update frequency:

- For tables with small data volume, **statistics are collected in real time with no restrictions, even though the table data is frequently updated. The `statistic_auto_collect_small_table_size` parameter can be used to determine whether a table is a small table or a large table. You can also use `statistic_auto_collect_small_table_interval` to configure collection intervals for small tables.

- For tables with large data volume, the following restrictions apply:

  - The default collection interval is not less than 12 hours, which can be configured using `statistic_auto_collect_large_table_interval`.

  - When the collection interval is met and the statistics health is lower than the threshold for automatic sampled collection (`statistic_auto_collect_sample_threshold`), sampled collection is triggered.

  - When the collection interval is met and the statistics health is higher than the threshold for automatic sampled collection (`statistic_auto_collect_sample_threshold`) and lower than the automatic collection threshold (`statistic_auto_collect_ratio`), full collection is triggered.

  - When the size of the largest partition to collect data (`statistic_max_full_collect_data_size`) is greater than 100 GB, sampled collection is triggered.

  - Only statistics of partitions whose update time is later than the time of the previous collection task are collected. Statistics of partitions with no data change are not collected.

:::tip

After the data of a table is changed, manually triggering a sampled collection task for this table will make the update time of the sampled collection task later than the data update time, which will not trigger automatic full collection for this table in this scheduling period.
:::

Automatic full collection is enabled by default and run by the system using the default settings.

The following table describes the default settings. If you need to modify them, run the **ADMIN SET CONFIG** command.

| **FE** **configuration item**         | **Type** | **Default value** | **Description**                                              |
| ------------------------------------- | -------- | ----------------- | ------------------------------------------------------------ |
| enable_statistic_collect              | BOOLEAN  | TRUE              | Whether to collect statistics. This switch is turned on by default. |
| enable_collect_full_statistic         | BOOLEAN  | TRUE              | Whether to enable automatic full collection. This switch is turned on by default. |
| statistic_collect_interval_sec        | LONG     | 300               | The interval for checking data updates during automatic collection. Unit: seconds. |
| statistic_auto_analyze_start_time | STRING      | 00:00:00   | The start time of automatic collection. Value range: `00:00:00` - `23:59:59`. |
| statistic_auto_analyze_end_time | STRING      | 23:59:59  | The end time of automatic collection. Value range: `00:00:00` - `23:59:59`. |
| statistic_auto_collect_small_table_size     | LONG    | 5368709120   | The threshold for determining whether a table is a small table for automatic full collection. A table whose size is greater than this value is considered a large table, whereas a table whose size is less than or equal to this value is considered a small table. Unit: Byte. Default value: 5368709120 (5 GB).                         |
| statistic_auto_collect_small_table_interval | LONG    | 0         | The interval for automatically collecting full statistics of small tables. Unit: seconds.                              |
| statistic_auto_collect_large_table_interval | LONG    | 43200        | The interval for automatically collecting full statistics of large tables. Unit: seconds. Default value: 43200 (12 hours).                               |
| statistic_auto_collect_ratio          | FLOAT    | 0.8               | The threshold for determining  whether the statistics for automatic collection are healthy. If statistics health is below this threshold, automatic collection is triggered. |
| statistic_auto_collect_sample_threshold  | DOUBLE	| 0.3   | The statistics health threshold for triggering automatic sampled collection. If the health value of statistics is lower than this threshold, automatic sampled collection is triggered. |
| statistic_max_full_collect_data_size | LONG      | 107374182400      | The size of the largest partition for automatic collection to collect data. Unit: Byte. Default value: 107374182400 (100 GB). If a partition exceeds this value, full collection is discarded and sampled collection is performed instead. |
| statistic_full_collect_buffer | LONG | 20971520 | The maximum buffer size taken by automatic collection tasks. Unit: Byte. Default value: 20971520 (20 MB). |
| statistic_collect_max_row_count_per_query | INT  | 5000000000        | The maximum number of rows to query for a single analyze task. An analyze task will be split into multiple queries if this value is exceeded. |
| statistic_collect_too_many_version_sleep | LONG | 600000 | The sleep time of automatic collection tasks if the table on which the collection task runs has too many data versions. Unit: ms. Default value: 600000 (10 minutes).  |

You can rely on automatic jobs for a majority of statistics collection, but if you have specific requirements, you can manually create a task by executing the ANALYZE TABLE statement or customize an automatic task by executing the CREATE ANALYZE  statement.

### Manual collection

You can use ANALYZE TABLE to create a manual collection task. By default, manual collection is a synchronous operation. You can also set it to an asynchronous operation. In asynchronous mode, after you run ANALYZE TABLE, the system immediately returns whether this statement is successful. However, the collection task will be running in the background and you do not have to wait for the result. You can check the status of the task by running SHOW ANALYZE STATUS. Asynchronous collection is suitable for tables with large data volume, whereas synchronous collection is suitable for tables with small data volume. **Manual collection tasks are run only once after creation. You do not need to delete manual collection tasks.**

#### Manually collect basic statistics

```SQL
ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES (property [,property])]
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
[PROPERTIES (property [,property])]
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
CREATE ANALYZE [FULL|SAMPLE] ALL [PROPERTIES (property [,property])]

-- Automatically collect stats of all tables in a database.
CREATE ANALYZE [FULL|SAMPLE] DATABASE db_name
[PROPERTIES (property [,property])]

-- Automatically collect stats of specified columns in a table.
CREATE ANALYZE [FULL|SAMPLE] TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
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
| statistic_auto_collect_interval       | LONG   |  0      | The interval for automatic collection. Unit: seconds. By default, StarRocks chooses `statistic_auto_collect_small_table_interval` or `statistic_auto_collect_large_table_interval` as the collection interval based on the table size. If you specified the `statistic_auto_collect_interval` property when you create an analyze job, this setting takes precedence over `statistic_auto_collect_small_table_interval` and `statistic_auto_collect_large_table_interval`. |

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
   "statistic_exclude_pattern" = "db_name.tbl_name"
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
DROP ANALYZE <ID>
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
SHOW STATS META [WHERE predicate]
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
SHOW HISTOGRAM META [WHERE predicate]
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
ANALYZE TABLE tbl_name DROP HISTOGRAM ON col_name [, col_name]
```

## Cancel a collection task

You can use the KILL ANALYZE statement to cancel a **running** collection task, including manual and custom tasks.

```SQL
KILL ANALYZE <ID>
```

The task ID for a manual collection task can be obtained from SHOW ANALYZE STATUS. The task ID for a custom collection task can be obtained from SHOW ANALYZE SHOW ANALYZE JOB.

## Other FE configuration items

| **FE configuration item**        | **Type** | **Default value** | **Description**                                              |
| ------------------------------------ | -------- | ----------------- | ------------------------------------------------------------ |
| statistic_collect_concurrency        | INT      | 3                 | The maximum number of manual collection tasks that can run in parallel. The value defaults to 3, which means you can run a maximum of three manual collections tasks in parallel. If the value is exceeded, incoming tasks will be in the PENDING state, waiting to be scheduled. |
| statistic_manager_sleep_time_sec     | LONG     | 60                | The interval at which metadata is scheduled. Unit: seconds. The system performs the following operations based on this interval:Create tables for storing statistics.Delete statistics that have been deleted.Delete expired statistics. |
| statistic_analyze_status_keep_second | LONG     | 259200            | The duration to retain the history of collection tasks. Unit: seconds. Default value: 259200 (3 days). |

## Session variable

`statistic_collect_parallel`: Used to adjust the parallelism of statistics collection tasks that can run on BEs. Default value: 1. You can increase this value to speed up collection tasks.

## Collect statistics of Hive/Iceberg/Hudi tables

Since v3.2.0, StarRocks supports collecting statistics of Hive, Iceberg, and Hudi tables. The syntax is similar to collecting StarRocks internal tables. **However, only manual and automatic full collection are supported. Sampled collection and histogram collection are not supported.** The collected statistics are stored in the `external_column_statistics` table of the `_statistics_` in the `default_catalog`. They are not stored in Hive Metastore and cannot be shared by other search engines. You can query data from the `default_catalog._statistics_.external_column_statistics` table to verify whether statistics are collected for a Hive/Iceberg/Hudi table.

Following is an example of querying statistics data from `external_column_statistics`.

```sql
SELECT * FROM _statistics_.external_column_statistics\G
*************************** 1. row ***************************
    table_uuid: hive_catalog.tn_test.ex_hive_tbl.1673596430
partition_name: 
   column_name: k1
  catalog_name: hive_catalog
       db_name: tn_test
    table_name: ex_hive_tbl
     row_count: 3
     data_size: 12
           ndv: NULL
    null_count: 0
           max: 3
           min: 2
   update_time: 2023-12-01 14:57:27.137000
```

### Limits

The following limits apply when you collect statistics for Hive, Iceberg, Hudi tables:

1. You can collect statistics of only Hive, Iceberg, and Hudi tables.
2. Only full collection is supported. Sampled collection and histogram collection are not supported.
3. For the system to automatically collect full statistics, you must create an Analyze job, which is different from collecting statistics of StarRocks internal tables where the system does this in the background by default.
4. For automatic collection tasks, you can only collect statistics of a specific table. You cannot collect statistics of all tables in a database or statistics of all databases in an external catalog.
5. For automatic collection tasks, StarRocks can detect whether data in Hive and Iceberg tables are updated and if so, collect statistics of only partitions whose data is updated. StarRocks cannot perceive whether data in Hudi tables are updated and can only perform periodic full collection.

The following examples happen in a database under the Hive external catalog. If you want to collect statistics of a Hive table from the `default_catalog`, reference the table in the `[catalog_name.][database_name.]<table_name>` format.

### Manual collection

You can create an Analyze job on demand and the job runs immediately after you create it.

#### Create a manual collection task

Syntax:

```sql
ANALYZE [FULL] TABLE tbl_name (col_name [,col_name])
[WITH SYNC | ASYNC MODE]
[PROPERTIES(property [,property])]
```

Example:

```sql
ANALYZE TABLE ex_hive_tbl(k1);
+----------------------------------+---------+----------+----------+
| Table                            | Op      | Msg_type | Msg_text |
+----------------------------------+---------+----------+----------+
| hive_catalog.tn_test.ex_hive_tbl | analyze | status   | OK       |
+----------------------------------+---------+----------+----------+
```

#### View the task status

Syntax:

```sql
SHOW ANALYZE STATUS [LIKE | WHERE predicate]
```

Example:

```sql
SHOW ANALYZE STATUS where `table` = 'ex_hive_tbl';
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| Id    | Database             | Table       | Columns | Type | Schedule | Status  | StartTime           | EndTime             | Properties | Reason |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
| 16400 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:31:42 | 2023-12-04 16:31:42 | {}         |        |
| 16465 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:35 | 2023-12-04 16:37:35 | {}         |        |
| 16467 | hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | ONCE     | SUCCESS | 2023-12-04 16:37:46 | 2023-12-04 16:37:46 | {}         |        |
+-------+----------------------+-------------+---------+------+----------+---------+---------------------+---------------------+------------+--------+
```

#### View metadata of statistics

Syntax:

```sql
SHOW STATS META [WHERE predicate]
```

Example:

```sql
SHOW STATS META where `table` = 'ex_hive_tbl';
+----------------------+-------------+---------+------+---------------------+------------+---------+
| Database             | Table       | Columns | Type | UpdateTime          | Properties | Healthy |
+----------------------+-------------+---------+------+---------------------+------------+---------+
| hive_catalog.tn_test | ex_hive_tbl | k1      | FULL | 2023-12-04 16:37:46 | {}         |         |
+----------------------+-------------+---------+------+---------------------+------------+---------+
```

#### Cancel a collection task

Cancels a running collection task.

Syntax:

```sql
KILL ANALYZE <ID>
```

You can view the task ID in the output of SHOW ANALYZE STATUS.

### Automatic collection

For the system to automatically collect statistics of tables in an external data source, you can create an Analyze job. StarRocks automatically checks whether to run the task at the default check interval of 5 minutes. For Hive and Iceberg tables, StarRocks runs a collection task only when data in the tables are updated.

However, data changes in Hudi tables cannot be perceived and StarRocks periodically collects statistics based on the check interval and collection interval you specified. You can specify the following properties when you create an Analyze job:

- statistic_collect_interval_sec

  The interval for checking data updates during automatic collection. Unit: seconds. Default: 5 minutes.

- statistic_auto_collect_small_table_rows (v3.2 and later)

  Threshold to determine whether a table in an external data source (Hive, Iceberg, Hudi) is a small table during automatic collection. Default: 10000000.

- statistic_auto_collect_small_table_interval

  The interval for collecting statistics of small tables. Unit: seconds. Default: 0.

- statistic_auto_collect_large_table_interval

  The interval for collecting statistics of large tables. Unit: seconds. Default: 43200 (12 hours).

Automatic collection threads checks data updates at the interval specified by `statistic_collect_interval_sec`. If the number of rows in a table is less than `statistic_auto_collect_small_table_rows`, it collects statistics of such tables based on `statistic_auto_collect_small_table_interval`.

If the number of rows in a table exceeds `statistic_auto_collect_small_table_rows`, it collects statistics of such tables based on `statistic_auto_collect_large_table_interval`. It updates statistics of large tables only when `Last table update time + Collection interval > Current time`. This prevents frequent analyze tasks for large tables.

#### Create an automatic collection task

Syntax:

```sql
CREATE ANALYZE TABLE tbl_name (col_name [,col_name])
[PROPERTIES (property [,property])]
```

Example:

```sql
CREATE ANALYZE TABLE ex_hive_tbl (k1)
PROPERTIES ("statistic_auto_collect_interval" = "5");

Query OK, 0 rows affected (0.01 sec)
```

#### View the status of an automatic collection task

Same as manual collection.

#### View metadata of statistics

Same as manual collection.

#### View automatic collection tasks

Syntax:

```sql
SHOW ANALYZE JOB [WHERE predicate]
```

Example:

```sql
SHOW ANALYZE JOB WHERE `id` = '17204';

Empty set (0.00 sec)
```

#### Cancel a collection task

Same as manual collection.

#### Delete statistics

```sql
DROP STATS tbl_name
```

## References

- To query FE configuration items, run [ADMIN SHOW CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SHOW_CONFIG.md).

- To modify FE configuration items, run [ADMIN SET CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md).
