# StarRocks version 2.2

## 2.2.8

Release date: October 17, 2022

### Bug Fixes

The following bugs are fixed:

- BEs may crash if an expression encounters an error in the initial stage. [#11395](https://github.com/StarRocks/starrocks/pull/11395)

- BEs may crash if invalid JSON data is loaded. [#10804](https://github.com/StarRocks/starrocks/issues/10804)

- Parallel writing encounters an error when the pipeline engine is enabled. [#11451](https://github.com/StarRocks/starrocks/issues/11451)

- BEs crash when the ORDER BY NULL LIMIT clause is used. [#11648](https://github.com/StarRocks/starrocks/issues/11648)

- BEs crash if the column type defined in the external table is different from the column type in the source Parquet file. [#11839](https://github.com/StarRocks/starrocks/issues/11839)

## 2.2.7

Release date: September 23, 2022

### Bug Fixes

The following bugs are fixed:

- Data may be lost when users load JSON data into StarRocks. [#11054](https://github.com/StarRocks/starrocks/issues/11054)

- The output from SHOW FULL TABLES is incorrect. [#11126](https://github.com/StarRocks/starrocks/issues/11126)

- In previous versions, to access data in a view, users must have permissions on both the base tables and the view. In the current version, users are only required to have permissions on the view. [#11290](https://github.com/StarRocks/starrocks/pull/11290)

- The result from a complex query that is nested with EXISTS or IN is incorrect. [#11415](https://github.com/StarRocks/starrocks/pull/11415)

- REFRESH EXTERNAL TABLE fails if the schema of the corresponding Hive table is changed. [#11406](https://github.com/StarRocks/starrocks/pull/11406)

- An error may occur when a non-leader FE replays the bitmap index creation operation. [#11261](

## 2.2.6

Release date: September 14, 2022

### Bug Fixes

The following bugs are fixed:

- The result of `order by... limit...offset` is incorrect when the subquery contains LIMIT. [#9698](https://github.com/StarRocks/starrocks/issues/9698)

- The BE crashes if partial update is performed on a table with large data volume. [#9809](https://github.com/StarRocks/starrocks/issues/9809)

- Compaction causes BEs to crash if the size of BITMAP data to compact exceeds 2 GB. [#11159](https://github.com/StarRocks/starrocks/pull/11159)

- The like() and regexp() functions do not work if the pattern length exceeds 16 KB. [#10364](https://github.com/StarRocks/starrocks/issues/10364)

### Behavior Change

The format used to represent JSON values in an array in the output is modified. Escape characters are no longer used in the returned JSON values. [#10790](https://github.com/StarRocks/starrocks/issues/10790)

## 2.2.5

Release date: August 18, 2022

### Improvements

- Improved the system performance when the pipeline engine is enabled. [#9580](https://github.com/StarRocks/starrocks/pull/9580)

- Improved the accuracy of memory statistics for index metadata. [#9837](https://github.com/StarRocks/starrocks/pull/9837)

### Bug Fixes

The following bugs are fixed:

- BEs may be stuck in querying Kafka partition offsets (`get_partition_offset`) during Routine Load. [#9937](https://github.com/StarRocks/starrocks/pull/9937)

- An error occurs when multiple Broker Load threads attempt to load the same HDFS file. [#9507](https://github.com/StarRocks/starrocks/pull/9507)

## 2.2.4

Release date: August 3, 2022

### Improvements

- Supports synchronizing schema changes on Hive table to the corresponding external table. [#9010](https://github.com/StarRocks/starrocks/pull/9010)

- Supports loading ARRAY data in Parquet files via Broker Load. [#9131](https://github.com/StarRocks/starrocks/pull/9131)

### Bug Fixes

The following bugs are fixed:

- Broker Load cannot handle Kerberos logins with multiple keytab files. [#8820](https://github.com/StarRocks/starrocks/pull/8820) [#8837](https://github.com/StarRocks/starrocks/pull/8837)

- Supervisor may fail to restart services if **stop_be.sh** exits immediately after it is executed. [#9175](https://github.com/StarRocks/starrocks/pull/9175)

- Incorrect Join Reorder precedence causes error "Column cannot be resolved". [#9063](https://github.com/StarRocks/starrocks/pull/9063) [#9487](https://github.com/StarRocks/starrocks/pull/9487)

## 2.2.3

Release date: July 24, 2022

### Bug Fixes

The following bugs are fixed:

- An error occurs when users delete a resource group. [#8036](https://github.com/StarRocks/starrocks/pull/8036)

- Thrift server exits when the number of threads is insufficient. [#7974](https://github.com/StarRocks/starrocks/pull/7974)

- In some scenarios, join reorder in CBO returns no results. [#7099](https://github.com/StarRocks/starrocks/pull/7099) [#7831](https://github.com/StarRocks/starrocks/pull/7831) [#6866](https://github.com/StarRocks/starrocks/pull/6866)

## 2.2.2

Release date: June 29, 2022

### Improvements

- UDFs can be used across databases. [#6865](https://github.com/StarRocks/starrocks/pull/6865) [#7211](https://github.com/StarRocks/starrocks/pull/7211)

- Optimized concurrency control for internal processing such as schema change. This reduces pressure on FE metadata management. In addition, the possibility that load jobs may pile up or slow down is reduced in scenarios where huge volume of data needs to be loaded at high concurrency. [#6838](https://github.com/StarRocks/starrocks/pull/6838)

### Bug Fixes

The following bugs are fixed:

- The number of replicas (`replication_num`) created by using CTAS is incorrect. [#7036](https://github.com/StarRocks/starrocks/pull/7036)

- Metadata may be lost after ALTER ROUTINE LOAD is performed. [#7068](https://github.com/StarRocks/starrocks/pull/7068)

- Runtime filters fail to be pushed down. [#7206](https://github.com/StarRocks/starrocks/pull/7206) [#7258](https://github.com/StarRocks/starrocks/pull/7258)

- Pipeline issues that may cause memory leaks.  [#7295](https://github.com/StarRocks/starrocks/pull/7295)

- Deadlock may occur when a Routine Load job is aborted. [#6849](https://github.com/StarRocks/starrocks/pull/6849)

- Some profile statistics information is inaccurate. [#7074](https://github.com/StarRocks/starrocks/pull/7074) [#6789](https://github.com/StarRocks/starrocks/pull/6789)

- The get_json_string function incorrectly processes JSON arrays. [#7671](https://github.com/StarRocks/starrocks/pull/7671)

## 2.2.1

Release date: June 2, 2022

### Improvements

- Optimized the data loading performance and reduced long tail latency by reconstructing part of the hotspot code and reducing lock granularity. [#6641](https://github.com/StarRocks/starrocks/pull/6641)

- Added the CPU and memory usage information of the machines on which BEs are deployed for each query to the FE audit log. [#6208](https://github.com/StarRocks/starrocks/pull/6208) [#6209](https://github.com/StarRocks/starrocks/pull/6209)

- Supported JSON data types in the tables that use the Primary Key model and tables that use the Unique Key model. [#6544](https://github.com/StarRocks/starrocks/pull/6544)

- Reduced FEs load by reducing lock granularity and deduplicating BE report requests. Optimized the report performance when a large number of BEs are deployed, and solved the issue of Routine Load tasks getting stuck in a large cluster. [#6293](https://github.com/StarRocks/starrocks/pull/6293)

### Bug Fixes

The following bugs are fixed:

- An error occurs when StarRocks parses the escape characters specified in the `SHOW FULL TABLES FROM DatabaseName` statement. [#6559](https://github.com/StarRocks/starrocks/issues/6559)

- FE disk space usage rises sharply (Fix this bug by rolling back the BDBJE version). [#6708](https://github.com/StarRocks/starrocks/pull/6708)

- BEs become faulty because relevant fields cannot be found in the data returned after columnar scanning is enabled (`enable_docvalue_scan=true`). [#6600](https://github.com/StarRocks/starrocks/pull/6600)

## 2.2.0

Release date: May 22, 2022

### New Features

- [Preview] The resource group management feature is released. This feature allows StarRocks to isolate and efficiently use CPU and memory resources when StarRocks processes both complex queries and simple queries from different tenants in the same cluster.

- [Preview] A Java-based user-defined function (UDF) framework is implemented. This framework supports UDFs that are compiled in compliance with the syntax of Java to extend the capabilities of StarRocks.

- [Preview] The primary key model supports updates only to specific columns when data is loaded to the primary key model in real-time data update scenarios such as order updates and multi-stream joins.

- [Preview] JSON data types and JSON functions are supported.

- External tables can be used to query data from Apache Hudi. This further improves users' data lake analytics experience with StarRocks. For more information, see [External tables](../using_starrocks/External_table.md).

- The following functions are added:
  - ARRAY functions: [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md), array_sort, array_distinct, array_join, reverse, array_slice, array_concat, array_difference, arrays_overlap, and array_intersect
  - BITMAP functions: bitmap_max and bitmap_min
  - Other functions: retention and square

### Improvements

- The parser and analyzer of the cost-based optimizer (CBO) are restructured, the code structure is optimized, and syntaxes such as INSERT with Common Table Expression (CTE) are supported. These improvements are made to increase the performance of complex queries, such as queries that involve the reuse of CTEs.

- The performance of queries on Apache Hive™ external tables that are stored in cloud object storage services such as AWS Simple Storage Service (S3), Alibaba Cloud Object Storage Service (OSS), and Tencent Cloud Object Storage (COS) is optimized. After the optimization, the performance of object storage-based queries is comparable to that of HDFS-based queries. Additionally, late materialization of ORC files is supported, and queries on small files are accelerated. For more information, see [Apache Hive™ external table](../using_starrocks/External_table.md).

- When queries from Apache Hive™ are run by using external tables, StarRocks automatically performs incremental updates to the cached metadata by consuming Hive metastore events such as data changes and partition changes. StarRocks also supports queries on data of the DECIMAL and ARRAY types from Apache Hive™. For more information, see [Apache Hive™ external table](../using_starrocks/External_table.md).

- The UNION ALL operator is optimized to run 2 to 25 times faster than before.

- A pipeline engine that supports adaptive parallelism and provides optimized profiles is released to improve the performance of simple queries in high concurrency scenarios.

- Multiple characters can be combined and used as a single row delimiter for CSV files that are to be imported.

### Bug Fixes

- Deadlocks occur if data is loaded or changes are committed into tables that are based on the primary key model. [#4998](https://github.com/StarRocks/starrocks/pull/4998)

- Frontends (FEs), including FEs that run Oracle Berkeley DB Java Edition (BDB JE), are unstable. [#4428](https://github.com/StarRocks/starrocks/pull/4428), [#4666](https://github.com/StarRocks/starrocks/pull/4666), [#2](https://github.com/StarRocks/bdb-je/pull/2)

- The result that is returned by the SUM function encounters an arithmetic overflow if the function is invoked on a large amount of data. [#3944](https://github.com/StarRocks/starrocks/pull/3944)

- The precision of the results that are returned by the ROUND and TRUNCATE functions is unsatisfactory. [#4256](https://github.com/StarRocks/starrocks/pull/4256)

- A few bugs are detected by Synthesized Query Lancer (SQLancer). For more information, see [SQLancer-related issues](https://github.com/StarRocks/starrocks/issues?q=is:issue++label:sqlancer++milestone:2.2).

### Others

Flink-connector-starrocks supports Apache Flink® v1.14.

### Upgrade notes

- If you use a StarRocks version later than 2.0.4 or a StarRocks version 2.1.x later than 2.1.6, see [Upgrade notes for StarRocks](https://forum.starrocks.com/t/topic/2228).

- To roll back to the previous version that was used before the upgrade, add the `ignore_unknown_log_id` parameter to the **fe.conf** file of each FE and set the parameter to `true`. The parameter is required because new types of logs are added in StarRocks v2.2.0. If you do not add the parameter, you cannot roll back to the previous version. We recommend that you set the `ignore_unknown_log_id` parameter to `false` in the **fe.conf** file of each FE after checkpoints are created. Then, restart the FEs to restore the FEs to the previous configurations.
