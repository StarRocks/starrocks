# StarRocks version 2.1

## 2.1.13

Release date: September 6, 2022

### Improvements

- Added a BE configuration item `enable_check_string_lengths` to check the length of loaded data. This mechanism helps prevent compaction failures caused by VARCHAR data size out of range. [#10380](https://github.com/StarRocks/starrocks/issues/10380)
- Optimized the query performance when a query contains more than 1000 OR operators. [#9332](https://github.com/StarRocks/starrocks/pull/9332)

### Bug Fixes

The following bugs are fixed:

- An error may occur and BEs may crash when you query ARRAY columns (calculated by using the REPLACE_IF_NOT_NULL function) from a table using the Aggregate table. [#10144](https://github.com/StarRocks/starrocks/issues/10144)
- The query result is incorrect if more than one IFNULL() function is nested in the query. [#5028](https://github.com/StarRocks/starrocks/issues/5028) [#10486](https://github.com/StarRocks/starrocks/pull/10486)
- After a dynamic partition is truncated, the number of tablets in the partition changes from the value configured by dynamic partitioning to the default value. [#10435](https://github.com/StarRocks/starrocks/issues/10435)
- If the Kafka cluster is stopped when you use Routine Load to load data into StarRocks, deadlocks may occur, affecting query performance. [#8947](https://github.com/StarRocks/starrocks/issues/8947)
- An error occurs when a query contains both subqueries and ORDER BY clauses. [#10066](https://github.com/StarRocks/starrocks/pull/10066)

## 2.1.12

Release date: August 9, 2022

### Improvements

Added two parameters, `bdbje_cleaner_threads` and `bdbje_replay_cost_percent`, to speed up metadata cleanup in BDB JE. [#8371](https://github.com/StarRocks/starrocks/pull/8371)

### Bug Fixes

The following bugs are fixed:

- Some queries are forwarded to the Leader FE, causing the `/api/query_detail` action to return incorrect execution information about SQL statements such as SHOW FRONTENDS. [#9185](https://github.com/StarRocks/starrocks/issues/9185)
- After a BE is terminated, the current process is not completely terminated, resulting in a failed restart of the BE. [#9175](https://github.com/StarRocks/starrocks/pull/9267)
- When multiple Broker Load jobs are created to load the same HDFS data file, if one job encounters exceptions, the other jobs may not be able to properly read data and consequently fail. [#9506](https://github.com/StarRocks/starrocks/issues/9506)
- The related variables are not reset when the schema of a table changes, resulting in an error (`no delete vector found tablet`) when querying the table. [#9192](https://github.com/StarRocks/starrocks/issues/9192)

## 2.1.11

Release date: July 9, 2022

### Bug Fixes

The following bugs are fixed:

- Data loading into a table of the Primary Key table is suspended in the event of frequent data loads into that table.[#7763](https://github.com/StarRocks/starrocks/issues/7763)
- Aggregate expressions are processed in an incorrect sequence during low-cardinality optimization, causing the `count distinct` function to return unexpected results. [#7659](https://github.com/StarRocks/starrocks/issues/7659)
- No results are returned for the LIMIT clause, because the pruning rule in the clause cannot be properly processed. [#7894](https://github.com/StarRocks/starrocks/pull/7894)
- If the global dictionary for low-cardinality optimization is applied on columns that are defined as join conditions for a query, the query returns unexpected results. [#8302](https://github.com/StarRocks/starrocks/issues/8302)

## 2.1.10

Release date: June 24, 2022

### Bug Fixes

The following bugs are fixed:

- Switching Leader FE node repetitively may cause all load jobs hang and fail. [#7350](https://github.com/StarRocks/starrocks/issues/7350)
- Field of DECIMAL(18,2) type is shown as DECIMAL64(18,2) when checking the table schema with `DESC` SQL. [#7309](https://github.com/StarRocks/starrocks/pull/7309)
- BE crashes when the memory usage estimation of MemTable exceeds 4GB, because, during a data skew in load, some fields may occupy a large amount of memory resources. [#7161](https://github.com/StarRocks/starrocks/issues/7161)
- A large number of small segment files are created due to the overflow in the calculation of the max_rows_per_segment when there are many input rows in a compaction. [#5610](https://github.com/StarRocks/starrocks/issues/5610)

## 2.1.8

Release date: June 9, 2022

### Improvements

- The concurrency control mechanism used for internal processing workloads such as schema changes is optimized to reduce the pressure on frontend (FE) metadata. As such, load jobs are less likely to pile up and slow down if these load jobs are concurrently run to load a large amount of data. [#6560](https://github.com/StarRocks/starrocks/pull/6560) [#6804](https://github.com/StarRocks/starrocks/pull/6804)
- The performance of StarRocks in loading data at a high frequency is improved. [#6532](https://github.com/StarRocks/starrocks/pull/6532) [#6533](https://github.com/StarRocks/starrocks/pull/6533)

### Bug Fixes

The following bugs are fixed:

- ALTER operation logs do not record all information about LOAD statements. Therefore, after you perform an ALTER operation on a routine load job, the metadata of the job is lost after checkpoints are created. [#6936](https://github.com/StarRocks/starrocks/issues/6936)
- A deadlock may occur if you stop a routine load job. [#6450](https://github.com/StarRocks/starrocks/issues/6450)
- By default, a backend (BE) uses the default UTC+8 time zone for a load job. If your server uses the UTC time zone, 8 hours are added to the timestamps in the DateTime column of the table that is loaded by using a Spark load job. [#6592](https://github.com/StarRocks/starrocks/issues/6592)
- The GET_JSON_STRING function cannot process non-JSON strings. If you extract a JSON value from a JSON object or array, the function returns NULL. The function has been optimized to return an equivalent JSON-formatted STRING value for a JSON object or array. [#6426](https://github.com/StarRocks/starrocks/issues/6426)
- If the data volume is large, a schema change may fail due to excessive memory consumption. Optimizations have been made to allow you to specify memory consumption limits at all stages of a schema change. [#6705](https://github.com/StarRocks/starrocks/pull/6705)
- If the number of duplicate values in a column of a table that is being compacted exceeds 0x40000000, the compaction is suspended. [#6513](https://github.com/StarRocks/starrocks/issues/6513)
- After an FE restarts, it encounters high I/O and abnormally increasing disk usage due to a few issues in BDB JE v7.3.8 and shows no sign of restoring to normal. The FE is restored to normal after it rolls back to BDB JE v7.3.7. [#6634](https://github.com/StarRocks/starrocks/issues/6634)

## 2.1.7

Release date: May 26, 2022

### Improvements

For window functions in which the frame is set to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, if the partition involved in a calculation is large, StarRocks caches all data of the partition before it performs the calculation. In this situation, a large number of memory resources are consumed. StarRocks has been optimized not to cache all data of the partition in this situation. [5829](https://github.com/StarRocks/starrocks/issues/5829)

### Bug Fixes

The following bugs are fixed:

- When data is loaded into a table that uses the Primary Key table, data processing errors may occur if the creation time of each data version stored in the system does not monotonically increase due to reasons such as backward-moved system time and related unknown bugs. Such data processing errors cause backends (BEs) to stop. [#6046](https://github.com/StarRocks/starrocks/issues/6046)
- Some graphical user interface (GUI) tools automatically configure the set_sql_limit variable. As a result, the SQL statement ORDER BY LIMIT is ignored, and consequently an incorrect number of rows are returned for queries. [#5966](https://github.com/StarRocks/starrocks/issues/5966)
- If the DROP SCHEMA statement is executed on a database, the database is forcibly deleted and cannot be restored. [#6201](https://github.com/StarRocks/starrocks/issues/6201)
- When JSON-formatted data is loaded, BEs stop if the data contains JSON format errors. For example, key-value pairs are not separated by commas (,). [#6098](https://github.com/StarRocks/starrocks/issues/6098)
- When a large amount of data is being loaded in a highly concurrent manner, tasks that are run to write data to disks are piled up on BEs. In this situation, the BEs may stop. [#3877](https://github.com/StarRocks/starrocks/issues/3877)
- StarRocks estimates the amount of memory that is required before it performs a schema change on a table. If the table contains a large number of STRING fields, the memory estimation result may be inaccurate. In this situation, if the estimated amount of memory that is required exceeds the maximum memory that is allowed for a single schema change operation, schema change operations that are supposed to be properly run encounter errors. [#6322](https://github.com/StarRocks/starrocks/issues/6322)
- After a schema change is performed on a table that uses the Primary Key table, a "duplicate key xxx" error may occur when data is loaded into that table. [#5878](https://github.com/StarRocks/starrocks/issues/5878)
- If low-cardinality optimization is performed during Shuffle Join operations, partitioning errors may occur. [#4890](https://github.com/StarRocks/starrocks/issues/4890)
- If a colocation group (CG) contains a large number of tables and data is frequently loaded into the tables, the CG may not be able to stay in the stable state. In this case, the JOIN statement does not support Colocate Join operations. StarRocks has been optimized to wait for a little longer during data loading. This way, the integrity of the tablet replicas to which data is loaded can be maximized.

## 2.1.6

Release date: May 10, 2022

### Bug Fixes

The following bugs are fixed:

- When you run queries after you perform multiple DELETE operations, you may obtain incorrect query results if optimization on low-cardinality columns is performed for the queries. [#5712](https://github.com/StarRocks/starrocks/issues/5712)
- If tablets are migrated in specific data ingestion phases, data continues to be written to the original disk on which the tablets are stored. As a result, data is lost, and queries cannot be run properly. [#5160](https://github.com/StarRocks/starrocks/issues/5160)
- If you covert values between the DECIMAL and STRING data types, the return values may be in an unexpected precision. [#5608](https://github.com/StarRocks/starrocks/issues/5608)
- If you multiply a DECIMAL value by a BIGINT value, an arithmetic overflow may occur. A few adjustments and optimizations are made to fix this bug. [#4211](https://github.com/StarRocks/starrocks/pull/4211)

## 2.1.5

Release date: April 27, 2022

### Bug Fixes

The following bugs are fixed:

- The calculation result is not correct when decimal multiplication overflows. After the bug is fixed, NULL is returned when decimal multiplication overflows.
- When statistics have a considerable deviation from the actual statistics, the priority of Collocate Join may be lower than Broadcast Join. As a result, the query planner may not choose Colocate Join as the more appropriate Join strategy. [#4817](https://github.com/StarRocks/starrocks/pull/4817)
- Query fails because the plan for complex expressions is wrong when there are more than 4 tables to join.
- BEs may stop working under Shuffle Join when the shuffle column is a low-cardinality column. [#4890](https://github.com/StarRocks/starrocks/issues/4890)
- BEs may stop working when the SPLIT function uses a NULL parameter. [#4092](https://github.com/StarRocks/starrocks/issues/4092)

## 2.1.4

Release date: April 8, 2022

### New Features

- The `UUID_NUMERIC` function is supported, which returns a LARGEINT value. Compared with `UUID` function, the performance of `UUID_NUMERIC` function can be improved by nearly 2 orders of magnitude.

### Bug Fixes

The following bugs are fixed:

- After deleting columns, adding new partitions, and cloning tablets,  the columns' unique ids in old and new tablets may not be the same, which may cause BE to stop working because the system uses a shared tablet schema. [#4514](https://github.com/StarRocks/starrocks/issues/4514)
- When data is loading to a StarRocks external table, if the configured FE of the target StarRocks cluster is not a Leader, it will cause the FE to stop working. [#4573](https://github.com/StarRocks/starrocks/issues/4573)
- The results of `CAST` function are different in StarRocks version 1.19 and 2.1. [#4701](https://github.com/StarRocks/starrocks/pull/4701)
- Query results may be incorrect, when a Duplicate Key table performs schema change and creates materialized view at the same time. [#4839](https://github.com/StarRocks/starrocks/issues/4839)

## 2.1.3

Release date: March 19, 2022

### Bug Fixes

The following bugs are fixed:

- The problem of possible data loss due to BE failure (solved by using Batch publish version). [#3140](https://github.com/StarRocks/starrocks/issues/3140)
- Some queries may cause memory limit exceeded errors due to inappropriate execution plans.
- The checksum between replicas may be inconsistent in different compaction processes. [#3438](https://github.com/StarRocks/starrocks/issues/3438)
- Query may fail in some situation when JSON reorder projection is not processed correctly. [#4056](https://github.com/StarRocks/starrocks/pull/4056)

## 2.1.2

Release date: March 14, 2022

### Bug Fixes

The following bugs are fixed:

- In a rolling upgrade from version 1.19 to 2.1, BE nodes stop working because of unmatched chunk sizes beween two versions. [#3834](https://github.com/StarRocks/starrocks/issues/3834)
- Loading tasks may fail while StarRocks is updating from version 2.0 to 2.1. [#3828](https://github.com/StarRocks/starrocks/issues/3828)
- Query fails when there is no appropriate execution plan for single-tablet table joins. [#3854](https://github.com/StarRocks/starrocks/issues/3854)
- A deadlock problem may occur when an FE node collects information to build a global dictionary for low-cardinality optimization. [#3839](https://github.com/StarRocks/starrocks/issues/3839)
- Query fails when BE nodes are in suspended animation due to deadlock.
- BI tools cannot connect to StarRocks when the SHOW VARIABLES command fails.[#3708](https://github.com/StarRocks/starrocks/issues/3708)

## 2.1.0

Release date: February 24, 2022

### New Features

- [Preview] StarRocks now supports Iceberg external tables.
- [Preview] The pipeline engine is now available. It is a new execution engine designed for multicore scheduling. The query parallelism can be adaptively adjusted without the need to set the parallel_fragment_exec_instance_num parameter. This also improves performance in high concurrency scenarios.
- The CTAS (CREATE TABLE AS SELECT) statement is supported, making ETL and table creation easier.
- SQL fingerprint is supported. SQL fingerprint is generated in audit.log, which facilitates the location of slow queries.
- The ANY_VALUE, ARRAY_REMOVE and SHA2 functions are supported.

### Improvements

- Compaction is optimized. A flat table can contain up to 10,000 columns.
- The performance of first-time scan and page cache is optimized. Random I/O is reduced to improve first-time scan performance. The improvement is more noticeable if first-time scan occurs on SATA disks. StarRocks' page cache can store original data, which eliminates the need for bitshuffle encoding and unnecessary decoding. This improves the cache hit rate and query efficiency.
- Schema change is supported in the Primary Key table. You can add, delete, and modify bitmap indexes by using `Alter table`.
- [Preview] The size of a string can be up to 1 MB.
- JSON load performance is optimized. You can load more than 100 MB JSON data in a single file.
- Bitmap index performance is optimized.
- The performance of StarRocks Hive external tables is optimized. Data in the CSV format can be read.
- DEFAULT CURRENT_TIMESTAMP is supported in the `create table` statement.
- StarRocks supports the loading of CSV files with multiple delimiters.

### Bug Fixes

The following bugs are fixed:

- Auto __op mapping does not take effect if jsonpaths is specified in the command used for loading JSON data.
- BE nodes fail because the source data changes during data loading using Broker Load.
- Some SQL statements report errors after materialized views are created.
- The routine load does not work due to quoted jsonpaths.
- Query concurrency decreases sharply when the number of columns to query exceeds 200.

### Behavior Change

The API for disabling a Colocation Group is changed from `DELETE /api/colocate/group_stable` to `POST /api/colocate/group_unstable`.

### Others

flink-connector-starrocks is now available for Flink to read StarRocks data in batches. This improves data read efficiency compared to the JDBC connector.
