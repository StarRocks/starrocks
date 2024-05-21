---
displayed_sidebar: "English"
---

# StarRocks version 2.5

## 2.5.21

Release date: May 15, 2024

### Improvements

- Optimized the usage of database locks for materialized view refresh to prevent deadlock. [#42801](https://github.com/StarRocks/starrocks/pull/42801)
- Both `s3a://` and `s3://` can be used to access data in AWS S3. [#42460](https://github.com/StarRocks/starrocks/pull/42460)

### Bug Fixes

Fixed the following issues:

- Schema change may cause issues in prefix index sorting, leading to incorrect results for queries based on prefix indexes. [#44941](https://github.com/StarRocks/starrocks/pull/44941)
- After a Routine Load task is paused due to Kafka cluster abnormalities, the background still attempts to connect to this abnormal Kafka cluster, which prevents other Routine Load tasks  in this StarRocks cluster from consuming normal Kafka messages. [#45029](https://github.com/StarRocks/starrocks/pull/45029)
- When querying views in `information_schema`, the database lock is held for an unexpectedly long time, which prolongs the overall query time. [#45392](https://github.com/StarRocks/starrocks/pull/45392)
- Enabling Query Cache may cause BEs to crash if the SQL query contains a HAVING clause. This issue can be resolved by disabling Query Cache using `set enable_query_cache=false`. [#43823](https://github.com/StarRocks/starrocks/pull/43823)
- When Query Cache is enabled, some queries may return an error message `All slotIds should be remapped`. [#42861](https://github.com/StarRocks/starrocks/pull/42861)

## 2.5.20

Release date: March 22, 2024

### Improvements

- `replace_if_not_null` supports BITMAP columns in an Aggregate table. Users can specify `replace_if_not_null` as the aggregate function for BITMAP columns in an Aggregate table. [#42104](https://github.com/StarRocks/starrocks/pull/42104)
- G1 Garbage Collector is used for JDK 9 and later by default. [#41374](https://github.com/StarRocks/starrocks/pull/41374)

### Parameter Changes

- The default value of the BE parameter `update_compaction_size_threshold` is changed from 256 MB to 64 MB to accelerate compaction. [#42776](https://github.com/StarRocks/starrocks/pull/42776)

### Bug Fixes

Fixed the following issues:

- Synchronizing data using StarRocks external tables encounters the error "commit and publish txn failed". The synchronization succeeds after a retry but the same copy of data is loaded twice.  [#25165](https://github.com/StarRocks/starrocks/pull/25165)
- RPC transmit resources are temporarily unavailable due to GC issues. [#41636](https://github.com/StarRocks/starrocks/pull/41636)
- array_agg() in v2.5 processes NULLs in a different way than it does in v2.3. As a result, the query result is incorrect after an upgrade from v2.3 to v2.5. [#42639](https://github.com/StarRocks/starrocks/pull/42639)
- The Sink Operator in a query unexpectedly exits, which causes BEs to crash. [#38662](https://github.com/StarRocks/starrocks/pull/38662)
- Executing the DELETE command on an Aggregate table results in a race for accessing tablet metadata, which causes BEs to crash. [#42174](https://github.com/StarRocks/starrocks/pull/42174)
- The MemTracker encounters the Use-After-Free issue during UDF calling, which causes BEs to crash. [#41710](https://github.com/StarRocks/starrocks/pull/41710)
- The unnest() function does not support aliases. [#42138](https://github.com/StarRocks/starrocks/pull/42138)

## 2.5.19

Release date: February 8, 2024

### New features

- Added Bitmap value processing functions: serialize, deserialize, and serializeToString. [#40162](https://github.com/StarRocks/starrocks/pull/40162/files)

### Improvements

- Supports automatic activation of inactive materialized views when refreshing these materialized views. [#38521](https://github.com/StarRocks/starrocks/pull/38521)
- Optimized BE log printing to prevent too many irrelevant logs. [#22820](https://github.com/StarRocks/starrocks/pull/22820) [#36187](https://github.com/StarRocks/starrocks/pull/36187)
- Supports using [Hive UDFs](https://docs.starrocks.io/docs/integrations/hive_bitmap_udf/) to process and load Bitmap data into StarRocks and export Bitmap data from StarRocks to Hive. [#40165](https://github.com/StarRocks/starrocks/pull/40165) [#40168](https://github.com/StarRocks/starrocks/pull/40168)
- Added date formats `yyyy-MM-ddTHH:mm` and `yyyy-MM-dd HH:mm` to support TIMESTAMP partition fields in Apache Iceberg tables. [#39986](https://github.com/StarRocks/starrocks/pull/39986)

### Bug Fixes

Fixed the following issues:

- Running a Spark Load job that has no PROPERTIES specified causes null pointer exceptions (NPEs). [#38765](https://github.com/StarRocks/starrocks/pull/38765)
- INSERT INTO SELECT occasionally encounters the error "timeout by txn manager". [#36688](https://github.com/StarRocks/starrocks/pull/36688)
- The memory consumption of PageCache exceeds the threshold specified by the BE dynamic parameter `storage_page_cache_limit` in certain circumstances. [#37740](https://github.com/StarRocks/starrocks/pull/37740)
- After a table is dropped and then re-created with the same table name, refreshing asynchronous materialized views created on that table fails. [#38008](https://github.com/StarRocks/starrocks/pull/38008) [#38982](https://github.com/StarRocks/starrocks/pull/38982)
- Writing data to S3 buckets using SELECT INTO occasionally encounters the error "The tablet write operation update metadata take a long time". [#38443](https://github.com/StarRocks/starrocks/pull/38443)
- Some operations during data loading may encounter "reached timeout". [#36746](https://github.com/StarRocks/starrocks/pull/36746)
- The DECIMAL type returned by SHOW CREATE TABLE is inconsistent with that specified in CREATE TABLE. [#39297](https://github.com/StarRocks/starrocks/pull/39297)
- If partition columns in external tables contain null values, queries against those tables will cause BEs to crash. [#38888](https://github.com/StarRocks/starrocks/pull/38888)
- When deleting data from a Duplicate Key table, if the condition in the WHERE clause of the DELETE statement has a leading space, the deleted data can still be queried using SELECT. [#39797](https://github.com/StarRocks/starrocks/pull/39797)
- Loading `array<string>` data from ORC files into StarRocks (`array<json>`) may cause BEs to crash. [#39233](https://github.com/StarRocks/starrocks/pull/39233)
- Querying Hive catalogs may be stuck and even expire. [#39863](https://github.com/StarRocks/starrocks/pull/39863)
- Partitions cannot be dynamically created if hour-level partitions are specified in the PARTITION BY clause. [#40256](https://github.com/StarRocks/starrocks/pull/40256)
- The error message "failed to call frontend service" is returned during loading from Apache Flink. [#40710](https://github.com/StarRocks/starrocks/pull/40710)

## 2.5.18

Release date: Jan 10, 2024

### New Features

- Users can set or modify session variables when they [CREATE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW/#parameters) or [ALTER](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW/) asynchronous materialized views. [#37401](https://github.com/StarRocks/starrocks/pull/37401)

### Improvements

- When using JDK, the default GC algorithm is G1. [#37498](https://github.com/StarRocks/starrocks/pull/37498)
- The result returned by the [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) statement now includes the timestamps of consumption messages from each partition. [#36222](https://github.com/StarRocks/starrocks/pull/36222)

### Behavior Change

- Added the session variable `enable_materialized_view_for_insert`, which controls whether materialized views rewrite the queries in INSERT INTO SELECT statements. The default value is `false`. [#37505](https://github.com/StarRocks/starrocks/pull/37505)
- Added the session variable `enable_strict_order_by`. When this variable is set to the default value `TRUE`, an error is reported for such a query pattern: Duplicate alias is used in different expressions of the query and this alias is also a sorting field in ORDER BY, for example, `select distinct t1.* from tbl1 t1 order by t1.k1;`. The logic is the same as that in v2.3 and earlier. When this variable is set to `FALSE`, a loose deduplication mechanism is used, which processes such queries as valid SQL queries. [#37910](https://github.com/StarRocks/starrocks/pull/37910)

### Parameter Change

- Added session variables `transaction_read_only` and `tx_read_only` to specify the transaction access mode, which are compatible with MySQL versions 5.7.20 and above. [#37249](https://github.com/StarRocks/starrocks/pull/37249)
- Added the FE configuration item `routine_load_unstable_threshold_second`. [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- Added the FE configuration item `http_worker_threads_num`, which specifies the number of threads for HTTP server to deal with HTTP requests. The default value is `0`. If the value for this parameter is set to a negative value or 0, the actual thread number is twice the number of CPU cores. [#37530](https://github.com/StarRocks/starrocks/pull/37530)
- Added the BE configuration item `pindex_major_compaction_limit_per_disk` to configure the maximum concurrency of compaction on a disk. This addresses the issue of uneven I/O across disks due to compaction. This issue can cause excessively high I/O for certain disks. The default value is `1`. [#37695](https://github.com/StarRocks/starrocks/pull/37695)

### Bug Fixes

Fixed the following issues:

- Using NaN (Not a Number) columns as ORDER BY columns may cause BEs to crash. [#30759](https://github.com/StarRocks/starrocks/pull/30759)
- Failure to update primary key indexes may cause the error "get_applied_rowsets failed". [#27488](https://github.com/StarRocks/starrocks/pull/27488)
- Hive metadata in [Hive catalogs](https://docs.starrocks.io/docs/2.5/data_source/catalog/hive_catalog/) is not automatically refreshed when new fields are added to Hive tables. [#37668](https://github.com/StarRocks/starrocks/pull/37668)
- When `SELECT ... FROM ... INTO OUTFILE` is executed to export data into CSV files, the error "Unmatched number of columns" is reported if the FROM clause contains multiple constants. [#38045](https://github.com/StarRocks/starrocks/pull/38045)
- In some cases, `bitmap_to_string` may return incorrect result due to data type overflow. [#37405](https://github.com/StarRocks/starrocks/pull/37405)

## 2.5.17

Release date: December 19, 2023

### New Features

- Added a new metric `max_tablet_rowset_num` for setting the maximum allowed number of rowsets. This metric helps detect possible compaction issues and thus reduces the occurrences of the error "too many versions". [#36539](https://github.com/StarRocks/starrocks/pull/36539)
- Added the [subdivide_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/) function. [#35817](https://github.com/StarRocks/starrocks/pull/35817)

### Improvements

- The result returned by the [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) statement provides a new field `OtherMsg`, which shows information about the last failed task. [#35806](https://github.com/StarRocks/starrocks/pull/35806)
- The default retention period of trash files is changed to 1 day from the original 3 days. [#37113](https://github.com/StarRocks/starrocks/pull/37113)
- Optimized the performance of persistent index update when compaction is performed on all rowsets of a Primary Key table, which reduces disk read I/O. [#36819](https://github.com/StarRocks/starrocks/pull/36819)
- Optimized the logic used to compute compaction scores for Primary Key tables, thereby aligning the compaction scores for Primary Key tables within a more consistent range with the other three table types. [#36534](https://github.com/StarRocks/starrocks/pull/36534)
- Queries on MySQL external tables and the external tables within JDBC catalogs support including keywords in the WHERE clause. [#35917](https://github.com/StarRocks/starrocks/pull/35917)
- Added the bitmap_from_binary function to Spark Load to support loading Binary data. [#36050](https://github.com/StarRocks/starrocks/pull/36050)
- The bRPC expiration time is shortened from 1 hour to the duration specified by the session variable [`query_timeout`](https://docs.starrocks.io/zh/docs/3.2/reference/System_variable/#query_timeout).  This prevents query failures caused by RPC request expiration. [#36778](https://github.com/StarRocks/starrocks/pull/36778)

### Parameter Change

- Added a BE configuration item `enable_stream_load_verbose_log` is added. The default value is `false`. With this parameter set to `true`, StarRocks can record the HTTP requests and responses for Stream Load jobs, making troubleshooting easier. [#36113](https://github.com/StarRocks/starrocks/pull/36113)
- Added a BE static parameter `update_compaction_per_tablet_min_interval_seconds` becomes mutable. [#36819](https://github.com/StarRocks/starrocks/pull/36819)

### Bug Fixes

Fixed the following issues:

- Queries fail during hash joins, causing BEs to crash. [#32219](https://github.com/StarRocks/starrocks/pull/32219)
- The FE performance plunges after the FE configuration item `enable_collect_query_detail_info` is set to `true`. [#35945](https://github.com/StarRocks/starrocks/pull/35945)
- Errors may be thrown if large amounts of data are loaded into a Primary Key table with persistent index enabled. [#34352](https://github.com/StarRocks/starrocks/pull/34352)
- The starrocks_be process may exit unexpectedly when `./agentctl.sh stop be` is used to stop a BE. [#35108](https://github.com/StarRocks/starrocks/pull/35108)
- The [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) function occasionally causes the BEs to crash. [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- Deadlocks may occur when users refresh materialized views. [#35736](https://github.com/StarRocks/starrocks/pull/35736)
- In some scenarios, dynamic partitioning may encounter an error, which causes FE start failures. [#36846](https://github.com/StarRocks/starrocks/pull/36846)

## 2.5.16

Release date: December 1, 2023

### Bug Fixes

Fixed the following issues:

- Global Runtime Filter may cause BEs to crash in certain scenarios. [#35776](https://github.com/StarRocks/starrocks/pull/35776)

## 2.5.15

Release date: November 29, 2023

### Improvements

- Added slow request logs to track slow requests. [#33908](https://github.com/StarRocks/starrocks/pull/33908)
- Optimized the performance of using Spark Load to read Parquet and ORC files when there are a large number of files. [#34787](https://github.com/StarRocks/starrocks/pull/34787)
- Optimized the performance of some Bitmap-related operations, including:
  - Optimized nested loop joins. [#340804](https://github.com/StarRocks/starrocks/pull/34804) [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - Optimized the `bitmap_xor` function. [#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - Supports Copy on Write to optimize Bitmap performance and reduce memory consumption. [#34047](https://github.com/StarRocks/starrocks/pull/34047)

### Parameter Change

- The FE dynamic parameter `enable_new_publish_mechanism` is changed to a static parameter. You must restart the FE after you modify the parameter settings. [#35338](https://github.com/StarRocks/starrocks/pull/35338)

### Bug Fixes

- If a filtering condition is specified in a Broker Load job, BEs may crash during the data loading in certain circumstances. [#29832](https://github.com/StarRocks/starrocks/pull/29832)
- Failures in replaying replica operations may cause FEs to crash. [#32295](https://github.com/StarRocks/starrocks/pull/32295)
- Setting the FE parameter `recover_with_empty_tablet` to `true` may cause FEs to crash. [#33071](https://github.com/StarRocks/starrocks/pull/33071)
- The error "get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction" is returned for queries. [#33246](https://github.com/StarRocks/starrocks/pull/33246)
- A query that contains a window function may cause BEs to crash. [#33671](https://github.com/StarRocks/starrocks/pull/33671)
- Running `show proc '/statistic'` may cause a deadlock. [#34237](https://github.com/StarRocks/starrocks/pull/34237/files)
- Errors may be thrown if large amounts of data are loaded into a Primary Key table with persistent index enabled. [#34566](https://github.com/StarRocks/starrocks/pull/34566)
- After StarRocks is upgraded from v2.4 or earlier to a later version, compaction scores may rise unexpectedly. [#34618](https://github.com/StarRocks/starrocks/pull/34618)
- If `INFORMATION_SCHEMA` is queried by using the database driver MariaDB ODBC, the `CATALOG_NAME` column returned in the `schemata` view holds only `null` values. [#34627](https://github.com/StarRocks/starrocks/pull/34627)
- If schema changes are being executed while a Stream Load job is in the **PREPARED** state, a portion of the source data to be loaded by the job is lost. [#34381](https://github.com/StarRocks/starrocks/pull/34381)
- Including two or more slashes (`/`) at the end of the HDFS storage path causes the backup and restore of the data from HDFS to fail. [#34601](https://github.com/StarRocks/starrocks/pull/34601)
- Running a loading task or a query may cause the FEs to hang. [#34569](https://github.com/StarRocks/starrocks/pull/34569)

## 2.5.14

Release date: November 14, 2023

### Improvements

- The `COLUMNS` table in the system database `INFORMATION_SCHEMA` can display ARRAY, MAP, and STRUCT columns. [#33431](https://github.com/StarRocks/starrocks/pull/33431)

### Parameter change

#### System variables

- Added a session variable `cbo_decimal_cast_string_strict`, which controls how the CBO converts data from the DECIMAL type to the STRING type. If this variable is set to `true`, the logic built in v2.5.x and later versions prevails and the system implements strict conversion (namely, the system truncates the generated string and fills 0s based on the scale length). If this variable is set to `false`, the logic built in versions earlier than v2.5.x prevails and the system processes all valid digits to generate a string. The default value is `true`. [#34208](https://github.com/StarRocks/starrocks/pull/34208)
- Added a session variable `cbo_eq_base_type`, which specifies the data type used for data comparison between DECIMAL-type data and STRING-type data. The default value is `VARCHAR`, and DECIMAL is also a valid value. [#34208](https://github.com/StarRocks/starrocks/pull/34208)

### Bug Fixes

Fixed the following issues:

- The error `java.lang.IllegalStateException: null` is reported if the ON condition is nested with a subquery. [#30876](https://github.com/StarRocks/starrocks/pull/30876)
- The result of COUNT(*) is inconsistent among replicas if COUNT(*) is run immediately after `INSERT INTO SELECT ... LIMIT` is successfully executed. [#24435](https://github.com/StarRocks/starrocks/pull/24435)
- BE may crash for specific data types if the target data type specified in the cast() function is the same as the original data type. [#31465](https://github.com/StarRocks/starrocks/pull/31465)
- An error is reported if specific path formats are used during data loading via Broker Load: `msg:Fail to parse columnsFromPath, expected: [rec_dt]`. [#32721](https://github.com/StarRocks/starrocks/issues/32721)
- During an upgrade to 3.x, if some column types are also upgraded (for example, Decimal is upgraded to Decimal v3), BEs crash when Compaction is performed on tables with specific characteristics. [#31626](https://github.com/StarRocks/starrocks/pull/31626)
- When data is loaded by using Flink Connector, the load job is suspended unexpectedly if there are highly concurrent load jobs and both the number of HTTP and Scan threads have reached their upper limits. [#32251](https://github.com/StarRocks/starrocks/pull/32251)
- BEs crash when libcurl is invoked. [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- Adding BITMAP columns to a Primary Key table fails with the following error: `Analyze columnDef error: No aggregate function specified for 'userid'`. [#31763](https://github.com/StarRocks/starrocks/pull/31763)
- Long-time, frequent data loading into a Primary Key table with persistent index enabled may cause BEs to crash. [#33220](https://github.com/StarRocks/starrocks/pull/33220)
- The query result is incorrect when Query Cache is enabled. [#32778](https://github.com/StarRocks/starrocks/pull/32778)
- Specifying a nullable Sort Key when creating a Primary Key table causes compaction to fail. [#29225](https://github.com/StarRocks/starrocks/pull/29225)
- The error "StarRocks planner use long time 10000 ms in logical phase" occasionally occurs for complex Join queries. [#34177](https://github.com/StarRocks/starrocks/pull/34177)

## 2.5.13

Release date: September 28, 2023

### Improvements

- Window functions COVAR_SAMP, COVAR_POP, CORR, VARIANCE, VAR_SAMP, STD, and STDDEV_SAMP now support the ORDER BY clause and Window clause. [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- An error instead of NULL is returned if a decimal overflow occurs during queries on the DECIMAL type data. [#30419](https://github.com/StarRocks/starrocks/pull/30419)
- Executing SQL commands with invalid comments now returns results consistent with MySQL. [#30210](https://github.com/StarRocks/starrocks/pull/30210)
- Rowsets corresponding to tablets that have been deleted are cleaned up, reducing the memory usage during BE startup. [#30625](https://github.com/StarRocks/starrocks/pull/30625)

### Bug Fixes

Fixed the following issues:

- An error "Set cancelled by MemoryScratchSinkOperator" occurs when users read data from StarRocks using the Spark Connector or Flink Connector. [#30702](https://github.com/StarRocks/starrocks/pull/30702) [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- An error "java.lang.IllegalStateException: null" occurs during queries with an ORDER BY clause that includes aggregate functions. [#30108](https://github.com/StarRocks/starrocks/pull/30108)
- FEs fail to restart when there are inactive materialized views. [#30015](https://github.com/StarRocks/starrocks/pull/30015)
- Performing INSERT OVERWRITE operations on duplicate partitions corrupts the metadata, leading to FE restart failures. [#27545](https://github.com/StarRocks/starrocks/pull/27545)
- An error "java.lang.NullPointerException: null" occurs when users modify columns that do not exist in a Primary Key table. [#30366](https://github.com/StarRocks/starrocks/pull/30366)
- An error "get TableMeta failed from TNetworkAddress" occurs when users load data into a partitioned StarRocks external table. [#30124](https://github.com/StarRocks/starrocks/pull/30124)
- In certain scenarios, an error occurs when users load data via CloudCanal. [#30799](https://github.com/StarRocks/starrocks/pull/30799)
- An error "current running txns on db xxx is 200, larger than limit 200" occurs when users load data via the Flink Connector or perform DELETE and INSERT operations. [#18393](https://github.com/StarRocks/starrocks/pull/18393)
- Asynchronous materialized views which use HAVING clauses that include aggregate functions cannot rewrite queries properly. [#29976](https://github.com/StarRocks/starrocks/pull/29976)

## 2.5.12

Release date: September 4, 2023

### Improvements

- Comments in an SQL are retained in the Audit Log. [#29747](https://github.com/StarRocks/starrocks/pull/29747)
- Added CPU and memory statistics of INSERT INTO SELECT to the Audit Log. [#29901](https://github.com/StarRocks/starrocks/pull/29901)

### Bug Fixes

Fixed the following issues:

- When Broker Load is used to load data, the NOT NULL attribute of some fields may cause BEs to crash or cause the "msg:mismatched row count" error. [#29832](https://github.com/StarRocks/starrocks/pull/29832)

- Queries against ORC-formatted files fail because the bugfix ORC-1304 ([apache/orc#1299](https://github.com/apache/orc/pull/1299)) from Apache ORC is not merged. [#29804](https://github.com/StarRocks/starrocks/pull/29804)

- Restoring Primary Key tables causes metadata inconsistency after BEs are restarted. [#30135](https://github.com/StarRocks/starrocks/pull/30135)

## 2.5.11

Release date: August 28, 2023

### Improvements

- Supports implicit conversions for all compound predicates and for all expressions in the WHERE clause. You can enable or disable implicit conversions by using the [session variable](https://docs.starrocks.io/en-us/3.1/reference/System_variable) `enable_strict_type`. The default value is `false`. [#21870](https://github.com/StarRocks/starrocks/pull/21870)
- Optimized the prompt returned if users do not specify `hive.metastore.uri` when they create an Iceberg Catalog. The error prompt is more accurate. [#16543](https://github.com/StarRocks/starrocks/issues/16543)
- Added more prompts in the error message `xxx too many versions xxx`. [#28397](https://github.com/StarRocks/starrocks/pull/28397)
- Dynamic partitioning further supports the partitioning unit to be `year`. [#28386](https://github.com/StarRocks/starrocks/pull/28386)

### Bug Fixes

Fixed the following issues:

- When data is loaded into tables with multiple replicas, a large number of invalid log records are written if some partitions of the tables are empty. [#28824](https://github.com/StarRocks/starrocks/issues/28824)
- The DELETE operation fails if the field in the WHERE condition is a BITMAP or HLL field. [#28592](https://github.com/StarRocks/starrocks/pull/28592)
- Manually refreshing an asynchronous materialized view via a synchronous call (SYNC MODE) results in multiple INSERT OVERWRITE records in the `information_schema.task_runs` table. [#28060](https://github.com/StarRocks/starrocks/pull/28060)
- If CLONE operations are triggered on tablets in an ERROR state, disk usage increases. [#28488](https://github.com/StarRocks/starrocks/pull/28488)
- When Join Reorder is enabled, the query result is incorrect if the column to query is a constant. [#29239](https://github.com/StarRocks/starrocks/pull/29239)
- During tablet migration between SSDs and HDDs, if the FE sends excessive migration tasks to BEs, BEs will encounter OOM issues. [#29055](https://github.com/StarRocks/starrocks/pull/29055)
- The security vulnerability in `/apache_hdfs_broker/lib/log4j-1.2.17.jar`. [#28866](https://github.com/StarRocks/starrocks/pull/28866)
- During data queries through Hive Catalog, if a partitioning column and an OR operator are used in the WHERE clause, the query result is incorrect. [#28876](https://github.com/StarRocks/starrocks/pull/28876)
- The error "java.util.ConcurrentModificationException: null" occasionally occurs during data queries. [#29296](https://github.com/StarRocks/starrocks/pull/29296)
- FEs cannot be restarted if the base table of an asynchronous materialized view is dropped. [#29318](https://github.com/StarRocks/starrocks/pull/29318)
- For an asynchronous materialized view that is created across databases, the Leader FE occasionally encounters a deadlock when data is being written into base tables of this materialized view. [#29432](https://github.com/StarRocks/starrocks/pull/29432)

## 2.5.10

Release date: August 7, 2023

### New features

- Supports aggregate functions [COVAR_SAMP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_samp/), [COVAR_POP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_pop/), and [CORR](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/corr/).
- Supports the following [window functions](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/): COVAR_SAMP, COVAR_POP, CORR, VARIANCE, VAR_SAMP, STD, and STDDEV_SAMP.

### Improvements

- Optimized the scheduling logic of TabletChecker to prevent the checker from repeatedly scheduling tablets that are not repaired. [#27648](https://github.com/StarRocks/starrocks/pull/27648)
- When Schema Change and Routine Load occur simultaneously, Routine Load jobs may fail if Schema Change completes first. The error message reported in this situation is optimized. [#28425](https://github.com/StarRocks/starrocks/pull/28425)
- Users are prohibited from defining NOT NULL columns when they create external tables (If NOT NULL columns are defined, errors will occur after an upgrade and the table must be created again). External catalogs are recommended starting from v2.3.0 to replace external tables. [#25485](https://github.com/StarRocks/starrocks/pull/25441)
- Added an error message when Broker Load retries encounter an error. This facilitates troubleshooting and debugging during data loading. [#21982](https://github.com/StarRocks/starrocks/pull/21982)
- Supports large-scale data writes when a load job involves both UPSERT and DELETE operations. [#17264](https://github.com/StarRocks/starrocks/pull/17264)
- Optimized query rewrite using materialized views. [#27934](https://github.com/StarRocks/starrocks/pull/27934) [#25542](https://github.com/StarRocks/starrocks/pull/25542) [#22300](https://github.com/StarRocks/starrocks/pull/22300) [#27557](https://github.com/StarRocks/starrocks/pull/27557) [#22300](https://github.com/StarRocks/starrocks/pull/22300) [#26957](https://github.com/StarRocks/starrocks/pull/26957) [#27728](https://github.com/StarRocks/starrocks/pull/27728) [#27900](https://github.com/StarRocks/starrocks/pull/27900)

### Bug Fixes

Fixed the following issues:

- When CAST is used to convert a string into an array, the result may be incorrect if the input includes constants. [#19793](https://github.com/StarRocks/starrocks/pull/19793)
- SHOW TABLET returns incorrect results if it contains ORDER BY and LIMIT. [#23375](https://github.com/StarRocks/starrocks/pull/23375)
- Outer join and Anti join rewrite errors for materialized views. [#28028](https://github.com/StarRocks/starrocks/pull/28028)
- Incorrect table-level scan statistics in FE cause inaccurate metrics for table queries and loading. [#27779](https://github.com/StarRocks/starrocks/pull/27779)
- `An exception occurred when using the current long link to access metastore. msg: Failed to get next notification based on last event id: 707602` is reported in FE logs if event listener is configured on the HMS to incrementally update Hive metadata. [#21056](https://github.com/StarRocks/starrocks/pull/21056)
- The query result is not stable if the sort key is modified for a partitioned table. [#27850](https://github.com/StarRocks/starrocks/pull/27850)
- Data loaded using Spark Load may be distributed to the wrong buckets if the bucketing column is a DATE, DATETIME, or DECIMAL column. [#27005](https://github.com/StarRocks/starrocks/pull/27005)
- The regex_replace function may cause BEs to crash in some scenarios. [#27117](https://github.com/StarRocks/starrocks/pull/27117)
- BE crashes if the input of the sub_bitmap function is not a BITMAP value. [#27982](https://github.com/StarRocks/starrocks/pull/27982)
- "Unknown error" is returned for a query when Join Reorder is enabled. [#27472](https://github.com/StarRocks/starrocks/pull/27472)
- Inaccurate estimation of average row size causes Primary Key partial updates to occupy excessively large memory. [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- Some INSERT jobs return `[42000][1064] Dict Decode failed, Dict can't take cover all key :0` if low-cardinality optimization is enabled. [#26463](https://github.com/StarRocks/starrocks/pull/26463)
- If users specify `"hadoop.security.authentication" = "simple"` in their Broker Load jobs created to load data from HDFS, the job fails. [#27774](https://github.com/StarRocks/starrocks/pull/27774)
- Modifying the refresh mode of materialized views causes inconsistent metadata between the leader FE and follower FE. [#28082](https://github.com/StarRocks/starrocks/pull/28082) [#28097](https://github.com/StarRocks/starrocks/pull/28097)
- Passwords are not hidden when SHOW CREATE CATALOG and SHOW RESOURCES are used to query specific information. [#28059](https://github.com/StarRocks/starrocks/pull/28059)
- FE memory leak caused by blocked LabelCleaner threads. [#28311](https://github.com/StarRocks/starrocks/pull/28311)

## 2.5.9

Release date: July 19, 2023

### New features

- Queries that contain a different type of join than the materialized view can be rewritten. [#25099](https://github.com/StarRocks/starrocks/pull/25099)

### Improvements

- StarRocks external tables whose destination cluster is the current StarRocks cluster cannot be created. [#25441](https://github.com/StarRocks/starrocks/pull/25441)
- If the queried fields are not included in the output columns of a materialized view but are included in the predicate of the materialized view, the query can still be rewritten. [#23028](https://github.com/StarRocks/starrocks/issues/23028)
- Added a new field `table_id` to the table `tables_config` in the database `Information_schema`. You can join `tables_config` with `be_tablets` on the column `table_id` to query the names of the database and table to which a tablet belongs. [#24061](https://github.com/StarRocks/starrocks/pull/24061)

### Bug Fixes

Fixed the following issues:

- Count Distinct result is incorrect for Duplicate Key tables. [#24222](https://github.com/StarRocks/starrocks/pull/24222)
- BEs may crash if the Join key is a large BINARY column. [#25084](https://github.com/StarRocks/starrocks/pull/25084)
- The INSERT operation hangs if the length of CHAR data in a STRUCT to be inserted exceeds the maximum CHAR length defined in the STRUCT column. [#25942](https://github.com/StarRocks/starrocks/pull/25942)
- The result of coalesce() is incorrect. [#26250](https://github.com/StarRocks/starrocks/pull/26250)
- The version number for a tablet is inconsistent between the BE and FE after data is restored. [#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- Partitions cannot be automatically created for recovered tables. [#26813](https://github.com/StarRocks/starrocks/pull/26813)

## 2.5.8

Release date: June 30, 2023

### Improvements

- Optimized the error message reported when partitions are added to a non-partitioned table. [#25266](https://github.com/StarRocks/starrocks/pull/25266)
- Optimized the [auto tablet distribution policy](https://docs.starrocks.io/docs/2.5/table_design/Data_distribution/#determine-the-number-of-tablets) for tables. [#24543](https://github.com/StarRocks/starrocks/pull/24543)
- Optimized the default comments in the CREATE TABLE statement. [#24803](https://github.com/StarRocks/starrocks/pull/24803)
- Optimized the manual refreshing of asynchronous materialized views. Supports using the REFRESH MATERIALIZED VIEW WITH SYNC MODE syntax to synchronously invoke materialized view refresh tasks. [#25910](https://github.com/StarRocks/starrocks/pull/25910)

### Bug Fixes

Fixed the following issues:

- The COUNT result of an asynchronous materialized view may be inaccurate if the materialized view is built on Union results. [#24460](https://github.com/StarRocks/starrocks/issues/24460)
- "Unknown error" is reported when users attempt to forcibly reset the root password. [#25492](https://github.com/StarRocks/starrocks/pull/25492)
- Inaccurate error message is displayed when INSERT OVERWRITE is executed on a cluster with less than three alive BEs. [#25314](https://github.com/StarRocks/starrocks/pull/25314)

## 2.5.7

Release date: June 14, 2023

### New features

- Inactive materialized views can be manually activated using `ALTER MATERIALIZED VIEW <mv_name> ACTIVE`. You can use this SQL command to activate materialized views whose base tables were dropped and then recreated. For more information, see [ALTER MATERIALIZED VIEW](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/ALTER_MATERIALIZED_VIEW/). [#24001](https://github.com/StarRocks/starrocks/pull/24001)
- StarRocks can automatically set an appropriate number of tablets when you create a table or add a partition, eliminating the need for manual operations. For more information, see [Determine the number of tablets](https://docs.starrocks.io/docs/2.5/table_design/Data_distribution/#determine-the-number-of-tablets). [#10614](https://github.com/StarRocks/starrocks/pull/10614)

### Improvements

- Optimized the I/O concurrency of Scan nodes used in external table queries, which reduces memory usage and improves the stability of data loading from external tables. [#23617](https://github.com/StarRocks/starrocks/pull/23617) [#23624](https://github.com/StarRocks/starrocks/pull/23624) [#23626](https://github.com/StarRocks/starrocks/pull/23626)
- Optimized the error message for Broker Load jobs. The error message contains retry information and the name of erroneous files. [#18038](https://github.com/StarRocks/starrocks/pull/18038) [#21982](https://github.com/StarRocks/starrocks/pull/21982)
- Optimized the error message returned when CREATE TABLE times out and added parameter tuning tips. [#24510](https://github.com/StarRocks/starrocks/pull/24510)
- Optimized the error message returned when ALTER TABLE fails because the table status is not Normal. [#24381](https://github.com/StarRocks/starrocks/pull/24381)
- Ignores full-width spaces in the CREATE TABLE statement. [#23885](https://github.com/StarRocks/starrocks/pull/23885)
- Optimized the Broker access timeout to increase the success rate of Broker Load jobs. [#22699](https://github.com/StarRocks/starrocks/pull/22699)
- For Primary Key tables, the `VersionCount` field returned by SHOW TABLET contains Rowsets that are in the Pending state. [#23847](https://github.com/StarRocks/starrocks/pull/23847)
- Optimized the Persistent Index policy. [#22140](https://github.com/StarRocks/starrocks/pull/22140)

### Bug Fixes

Fixed the following issues:

- When users load Parquet data into StarRocks, DATETIME values overflow during type conversion, causing data errors. [#22356](https://github.com/StarRocks/starrocks/pull/22356)
- Bucket information is lost after Dynamic Partitioning is disabled. [#22595](https://github.com/StarRocks/starrocks/pull/22595)
- Using unsupported properties in the CREATE TABLE statement causes null pointer exceptions (NPEs). [#23859](https://github.com/StarRocks/starrocks/pull/23859)
- Table permission filtering in `information_schema` becomes ineffective. As a result, users can view tables they do not have permission to. [#23804](https://github.com/StarRocks/starrocks/pull/23804)
- Information returned by SHOW TABLE STATUS is incomplete. [#24279](https://github.com/StarRocks/starrocks/issues/24279)
- A schema change sometimes may be hung if data loading occurs simultaneously with the schema change. [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- RocksDB WAL flush blocks the brpc worker from processing bthreads, which interrupts high-frequency data loading into Primary Key tables. [#22489](https://github.com/StarRocks/starrocks/pull/22489)
- TIME-type columns that are not supported in StarRocks can be successfully created. [#23474](https://github.com/StarRocks/starrocks/pull/23474)
- Materialized view Union rewrite fails. [#22922](https://github.com/StarRocks/starrocks/pull/22922)

## 2.5.6

Release date: May 19, 2023

### Improvements

- Optimized the error message reported when INSERT INTO ... SELECT expires due to a small `thrift_server_max_worker_thread` value. [#21964](https://github.com/StarRocks/starrocks/pull/21964)
- Tables created using CTAS have three replicas by default, which is consistent with the default replica number for common tables. [#22854](https://github.com/StarRocks/starrocks/pull/22854)

### Bug Fixes

- Truncating partitions fails because the TRUNCATE operation is case-sensitive to partition names. [#21809](https://github.com/StarRocks/starrocks/pull/21809)
- Decommissioning BE fails due to the failure in creating temporary partitions for materialized views. [#22745](https://github.com/StarRocks/starrocks/pull/22745)
- Dynamic FE parameters that require an ARRAY value cannot be set to an empty array. [#22225](https://github.com/StarRocks/starrocks/pull/22225)
- Materialized views with the `partition_refresh_number` property specified may fail to completely refresh. [#21619](https://github.com/StarRocks/starrocks/pull/21619)
- SHOW CREATE TABLE masks cloud credential information, which causes incorrect credential information in memory. [#21311](https://github.com/StarRocks/starrocks/pull/21311)
- Predicates cannot take effect on some ORC files that are queried via external tables. [#21901](https://github.com/StarRocks/starrocks/pull/21901)
- The min-max filter cannot properly handle lower- and upper-case letters in column names. [#22626](https://github.com/StarRocks/starrocks/pull/22626)
- Late materialization causes errors in querying complex data types (STRUCT or MAP).  [#22862](https://github.com/StarRocks/starrocks/pull/22862)
- The issue that occurs when restoring a Primary Key table. [#23384](https://github.com/StarRocks/starrocks/pull/23384)

## 2.5.5

Release date: April 28, 2023

### New features

Added a metric to monitor the tablet status of Primary Key tables:

- Added the FE metric `err_state_metric`.
- Added the `ErrorStateTabletNum` column to the output of `SHOW PROC '/statistic/'` to display the number of **err_state** tablets.
- Added the `ErrorStateTablets` column to the output of `SHOW PROC '/statistic/<db_id>/'` to display the IDs of **err_state** tablets.

For more information, see [SHOW PROC](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SHOW_PROC/).

### Improvements

- Optimized the disk balancing speed when multiple BEs are added. [# 19418](https://github.com/StarRocks/starrocks/pull/19418)
- Optimized the inference of `storage_medium`. When BEs use both SSD and HDD as storage devices, if the property `storage_cooldown_time` is specified, StarRocks sets `storage_medium` to `SSD`. Otherwise, StarRocks sets `storage_medium` to `HDD`. [#18649](https://github.com/StarRocks/starrocks/pull/18649)
- Optimized the performance of Unique Key tables by forbidding the collection of statistics from value columns. [#19563](https://github.com/StarRocks/starrocks/pull/19563)

### Bug Fixes

- For Colocation tables, the replica status can be manually specified as `bad` by using statements like `ADMIN SET REPLICA STATUS PROPERTIES ("tablet_id" = "10003", "backend_id" = "10001", "status" = "bad");`. If the number of BEs is less than or equal to the number of replicas, the corrupted replica cannot be repaired. [# 17876](https://github.com/StarRocks/starrocks/issues/17876)
- After a BE is started, its process exists but the BE port cannot be enabled. [# 19347](https://github.com/StarRocks/starrocks/pull/19347)
- Wrong results are returned for aggregate queries whose subquery is nested with a window function. [# 19725](https://github.com/StarRocks/starrocks/issues/19725)
- `auto_refresh_partitions_limit` does not take effect when the materialized view (MV) is refreshed for the first time. As a result, all the partitions are refreshed. [# 19759](https://github.com/StarRocks/starrocks/issues/19759)
- An error occurs when querying a CSV Hive external table whose array data is nested with complex data such as MAP and STRUCT. [# 20233](https://github.com/StarRocks/starrocks/pull/20233)
- Queries that use Spark connector time out. [# 20264](https://github.com/StarRocks/starrocks/pull/20264)
- If one replica of a two-replica table is corrupted, the table cannot recover. [# 20681](https://github.com/StarRocks/starrocks/pull/20681)
- Query failure caused by MV query rewrite failure. [# 19549](https://github.com/StarRocks/starrocks/issues/19549)
- The metric interface expires due to database lock. [# 20790](https://github.com/StarRocks/starrocks/pull/20790)
- Wrong results are returned for Broadcast Join. [# 20952](https://github.com/StarRocks/starrocks/issues/20952)
- NPE is returned when an unsupported data type is used in CREATE TABLE. [# 20999](https://github.com/StarRocks/starrocks/issues/20999)
- The issue caused by using window_funnel() with the Query Cache feature. [# 21474](https://github.com/StarRocks/starrocks/issues/21474)
- Optimization plan selection takes an unexpectedly long time after the CTE is rewritten. [# 16515](https://github.com/StarRocks/starrocks/pull/16515)

## 2.5.4

Release date: April 4, 2023

### Improvements

- Optimized the performance of rewriting queries on materialized views during query planning. The amount of time taken for query planning is reduced by about 70%. [#19579](https://github.com/StarRocks/starrocks/pull/19579)
- Optimized the type inference logic. If a query like `SELECT sum(CASE WHEN XXX);` contains a constant `0`, such as `SELECT sum(CASE WHEN k1 = 1 THEN v1 ELSE 0 END) FROM test;`, pre-aggregation is automatically enabled to accelerate the query. [#19474](https://github.com/StarRocks/starrocks/pull/19474)
- Supports using `SHOW CREATE VIEW` to view the creation statement of a materialized view. [#19999](https://github.com/StarRocks/starrocks/pull/19999)
- Supports transmitting packets that are 2 GB or larger in size for a single bRPC request between BE nodes. [#20283](https://github.com/StarRocks/starrocks/pull/20283) [#20230](https://github.com/StarRocks/starrocks/pull/20230)
- Supports using [SHOW CREATE CATALOG](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_CREATE_CATALOG/) to query the creation statement of an external catalog.

### Bug Fixes

The following bugs are fixed:

- After queries on materialized views are rewritten, the global dictionary for low-cardinality optimization does not take effect. [#19615](https://github.com/StarRocks/starrocks/pull/19615)
- If a query on materialized views fails to be rewritten, the query fails. [#19774](https://github.com/StarRocks/starrocks/pull/19774)
- If a materialized view is created based on a Primary Key or Unique Key table, queries on that materialized view cannot be rewritten. [#19600](https://github.com/StarRocks/starrocks/pull/19600)
- The column names of materialized views are case-sensitive. However, when you create a table, the table is successfully created without an error message even if column names are incorrect in the `PROPERTIES` of the table creation statement, and moreover the rewriting of queries on materialized views created on that table fails. [#19780](https://github.com/StarRocks/starrocks/pull/19780)
- After a query on materialized views is rewritten, the query plan ma contain partition column-based, invalid predicates, which affect query performance. [#19784](https://github.com/StarRocks/starrocks/pull/19784)
- When data is loaded into a newly created partition, queries on materialized views may fail to be rewritten. [#20323](https://github.com/StarRocks/starrocks/pull/20323)
- Configuring `"storage_medium" = "SSD"` at the creation of materialized views causes the refresh of the materialized views to fail. [#19539](https://github.com/StarRocks/starrocks/pull/19539) [#19626](https://github.com/StarRocks/starrocks/pull/19626)
- Concurrent compaction may happen on Primary Key tables. [#19692](https://github.com/StarRocks/starrocks/pull/19692)
- Compaction does not occur promptly after a large number of DELETE operations. [#19623](https://github.com/StarRocks/starrocks/pull/19623)
- If the expression of a statement contains multiple low-cardinality columns, the expression may fail to be properly rewritten. As a result, the global dictionary for low-cardinality optimization does not take effect. [#20161](https://github.com/StarRocks/starrocks/pull/20161)

## 2.5.3

Release date: March 10, 2023

### Improvements

- Optimized query rewrite for materialized views (MVs).
  - Supports rewriting queries with Outer Join and Cross Join. [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - Optimized the data scan logic for MVs, further accelerating the rewritten queries. [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - Enhanced rewrite capabilities for single-table aggregate queries.  [#18629](https://github.com/StarRocks/starrocks/pull/18629)
  - Enhanced rewrite capabilities in View Delta scenarios, which is when the queried tables are a subset of the MV's base tables. [#18800](https://github.com/StarRocks/starrocks/pull/18800)
- Optimized the performance and memory usage when the window function RANK() is used as a filter or a sort key. [#17553](https://github.com/StarRocks/starrocks/issues/17553)

### Bug Fixes

The following bugs are fixed:

- Errors caused by null literals `[]` in ARRAY data. [#18563](https://github.com/StarRocks/starrocks/pull/18563)
- Misuse of the low-cardinality optimization dictionary in some complex query scenarios. The dictionary mapping check is now added before applying the dictionary.  [#17318](https://github.com/StarRocks/starrocks/pull/17318)
- In a single BE environment, Local Shuffle causes GROUP BY to produce duplicate results. [#17845](https://github.com/StarRocks/starrocks/pull/17845)
- Misuses of partition-related PROPERTIES for a non-partitioned MV may cause the MV refresh to fail. The partition PROPERTIES check is now performed when users create an MV. [#18741](https://github.com/StarRocks/starrocks/pull/18741)
- Errors in parsing Parquet Repetition columns. [#17626](https://github.com/StarRocks/starrocks/pull/17626) [#17788](https://github.com/StarRocks/starrocks/pull/17788) [#18051](https://github.com/StarRocks/starrocks/pull/18051)
- The obtained column's nullable information is incorrect. Solution: When CTAS is used to create a Primary Key table, only the primary key columns are non-nullable; non-primary key columns are nullable. [#16431](https://github.com/StarRocks/starrocks/pull/16431)
- Some issues caused by deleting data from Primary Key tables.  [#18768](https://github.com/StarRocks/starrocks/pull/18768)

## 2.5.2

Release date: February 21, 2023

### New Features

- Supports using the Instance Profile and Assumed Role-based credential methods to access AWS S3 and AWS Glue. [#15958](https://github.com/StarRocks/starrocks/pull/15958)
- Supports the following bit functions: bit_shift_left, bit_shift_right, and bit_shift_right_logical. [#14151](https://github.com/StarRocks/starrocks/pull/14151)

### Improvements

- Optimized the memory release logic, which significantly reduces peak memory usage when a query contains a large number of aggregate queries. [#16913](https://github.com/StarRocks/starrocks/pull/16913)
- Reduced the memory usage of sorting. The memory consumption is halved when a query involves window functions or sorting. [#16937](https://github.com/StarRocks/starrocks/pull/16937) [#17362](https://github.com/StarRocks/starrocks/pull/17362) [#17408](https://github.com/StarRocks/starrocks/pull/17408)

### Bug Fixes

The following bugs are fixed:

- Apache Hive external tables that contain MAP and ARRAY data cannot be refreshed. [#17548](https://github.com/StarRocks/starrocks/pull/17548)
- Superset cannot identify column types of materialized views. [#17686](https://github.com/StarRocks/starrocks/pull/17686)
- BI connectivity fails because SET GLOBAL/SESSION TRANSACTION cannot be parsed. [#17295](https://github.com/StarRocks/starrocks/pull/17295)
- The bucket number of dynamic partitioned tables in a Colocate Group cannot be modified and an error message is returned. [#17418](https://github.com/StarRocks/starrocks/pull/17418/)
- Potential issues caused by a failure in the Prepare stage. [#17323](https://github.com/StarRocks/starrocks/pull/17323)

### Behavior Change

- Changed the default value of `enable_experimental_mv` from `false` to `true`, which means asynchronous materialized view is enabled by default.
- Added CHARACTER to the reserved keyword list. [#17488](https://github.com/StarRocks/starrocks/pull/17488)

## 2.5.1

Release date: February 5, 2023

### Improvements

- Asynchronous materialized views created based on external catalogs support query rewrite.  [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#15791](https://github.com/StarRocks/starrocks/issues/15791)
- Allows users to specify a collection period for automatic CBO statistics collection, which prevents cluster performance jitter caused by automatic full collection. [#14996](https://github.com/StarRocks/starrocks/pull/14996)
- Added Thrift server queue. Requests that cannot be processed immediately during INSERT INTO SELECT can be pending in the Thrift server queue, preventing requests from being rejected. [#14571](https://github.com/StarRocks/starrocks/pull/14571)
- Deprecated the FE parameter `default_storage_medium`. If `storage_medium` is not explicitly specified when users create a table, the system automatically infers the storage medium of the table based on BE disk type. For more information, see description of `storage_medium` in [CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_VIEW/). [#14394](https://github.com/StarRocks/starrocks/pull/14394)

### Bug Fixes

The following bugs are fixed:

- Null pointer exception (NPE) caused by SET PASSWORD. [#15247](https://github.com/StarRocks/starrocks/pull/15247)
- JSON data with empty keys cannot be parsed. [#16852](https://github.com/StarRocks/starrocks/pull/16852)
- Data of invalid types can be successfully converted into ARRAY data. [#16866](https://github.com/StarRocks/starrocks/pull/16866)
- Nested Loop Join cannot be interrupted when an exception occurs.  [#16875](https://github.com/StarRocks/starrocks/pull/16875)

### Behavior Change

- Deprecated the FE parameter `default_storage_medium`. The storage medium of a table is automatically inferred by the system. [#14394](https://github.com/StarRocks/starrocks/pull/14394)

## 2.5.0

Release date: January 22, 2023

### New Features

- Supports querying Merge On Read tables using [Hudi catalogs](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/) and [Hudi external tables](https://docs.starrocks.io/docs/data_source/External_table#deprecated-hudi-external-table). [#6780](https://github.com/StarRocks/starrocks/pull/6780)
- Supports querying STRUCT and MAP data using [Hive catalogs](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/), Hudi catalogs, and [Iceberg catalogs](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/). [#10677](https://github.com/StarRocks/starrocks/issues/10677)
- Provides [Data Cache](https://docs.starrocks.io/docs/data_source/data_cache/) to improve access performance of hot data stored in external storage systems, such as HDFS. [#11597](https://github.com/StarRocks/starrocks/pull/11579)
- Supports creating [Delta Lake catalogs](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/), which allow direct queries on data from Delta Lake. [#11972](https://github.com/StarRocks/starrocks/issues/11972)
- Hive, Hudi, and Iceberg catalogs are compatible with AWS Glue. [#12249](https://github.com/StarRocks/starrocks/issues/12249)
- Supports creating [file external tables](https://docs.starrocks.io/docs/data_source/file_external_table/), which allow direct queries on Parquet and ORC files from HDFS and object stores. [#13064](https://github.com/StarRocks/starrocks/pull/13064)
- Supports creating materialized views based on Hive, Hudi, Iceberg catalogs, and materialized views. For more information, see [Materialized view](https://docs.starrocks.io/docs/using_starrocks/Materialized_view/). [#11116](https://github.com/StarRocks/starrocks/issues/11116) [#11873](https://github.com/StarRocks/starrocks/pull/11873)
- Supports conditional updates for tables that use the Primary Key table. For more information, see [Change data through loading](https://docs.starrocks.io/docs/loading/Load_to_Primary_Key_tables/). [#12159](https://github.com/StarRocks/starrocks/pull/12159)
- Supports [Query Cache](https://docs.starrocks.io/docs/using_starrocks/query_cache/), which stores intermediate computation results of queries, improving the QPS and reduces the average latency of highly-concurrent, simple queries. [#9194](https://github.com/StarRocks/starrocks/pull/9194)
- Supports specifying the priority of Broker Load jobs. For more information, see [BROKER LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/) [#11029](https://github.com/StarRocks/starrocks/pull/11029)
- Supports specifying the number of replicas for data loading for StarRocks native tables. For more information, see [CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/). [#11253](https://github.com/StarRocks/starrocks/pull/11253)
- Supports [query queues](https://docs.starrocks.io/docs/administration/query_queues/). [#12594](https://github.com/StarRocks/starrocks/pull/12594)
- Supports isolating compute resources occupied by data loading, thereby limiting the resource consumption of data loading tasks. For more information, see [Resource group](https://docs.starrocks.io/docs/administration/resource_group/). [#12606](https://github.com/StarRocks/starrocks/pull/12606)
- Supports specifying the following data compression algorithms for StarRocks native tables: LZ4, Zstd, Snappy, and Zlib. For more information, see [Data compression](https://docs.starrocks.io/docs/table_design/data_compression/). [#10097](https://github.com/StarRocks/starrocks/pull/10097) [#12020](https://github.com/StarRocks/starrocks/pull/12020)
- Supports [user-defined variables](https://docs.starrocks.io/docs/reference/user_defined_variables/). [#10011](https://github.com/StarRocks/starrocks/pull/10011)
- Supports [lambda expression](https://docs.starrocks.io/docs/sql-reference/sql-functions/Lambda_expression/) and the following higher-order functions: [array_map](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_map/), [array_sum](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_sum/), and [array_sortby](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_sortby/). [#9461](https://github.com/StarRocks/starrocks/pull/9461) [#9806](https://github.com/StarRocks/starrocks/pull/9806) [#10323](https://github.com/StarRocks/starrocks/pull/10323) [#14034](https://github.com/StarRocks/starrocks/pull/14034)
- Provides the QUALIFY clause that filters the results of [window functions](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/). [#13239](https://github.com/StarRocks/starrocks/pull/13239)
- Supports using the result returned by the uuid() and uuid_numeric() functions as the default value of a column when you create a table. For more information, see [CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/). [#11155](https://github.com/StarRocks/starrocks/pull/11155)
- Supports the following functions: [map_size](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_size/), [map_keys](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_keys/), [map_values](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_values/), [max_by](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/max_by/), [sub_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/sub_bitmap/), [bitmap_to_base64](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_base64/), [host_name](https://docs.starrocks.io/docs/sql-reference/sql-functions/utility-functions/host_name/), and [date_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/date_slice/). [#11299](https://github.com/StarRocks/starrocks/pull/11299) [#11323](https://github.com/StarRocks/starrocks/pull/11323) [#12243](https://github.com/StarRocks/starrocks/pull/12243) [#11776](https://github.com/StarRocks/starrocks/pull/11776) [#12634](https://github.com/StarRocks/starrocks/pull/12634) [#14225](https://github.com/StarRocks/starrocks/pull/14225)

### Improvements

- Optimized the metadata access performance when you query external data using [Hive catalogs](https://docs.starrocks.io/docs/data_source/catalog/hive_catalog/), [Hudi catalogs](https://docs.starrocks.io/docs/data_source/catalog/hudi_catalog/), and [Iceberg catalogs](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/). [#11349](https://github.com/StarRocks/starrocks/issues/11349)
- Supports querying ARRAY data using [Elasticsearch external tables](https://docs.starrocks.io/docs/data_source/External_table#deprecated-elasticsearch-external-table). [#9693](https://github.com/StarRocks/starrocks/pull/9693)
- Optimized the following aspects of materialized views:
  - Asynchronous materialized views support automatic and transparent query rewrite based on the SPJG-type materialized views. For more information, see [Materialized view](https://docs.starrocks.io/docs/using_starrocks/Materialized_view#rewrite-and-accelerate-queries-with-the-asynchronous-materialized-view). [#13193](https://github.com/StarRocks/starrocks/issues/13193)
  - Asynchronous materialized views support multiple async refresh mechanisms. For more information, see [Materialized view](https://docs.starrocks.io/docs/using_starrocks/Materialized_view#manually-refresh-an-asynchronous-materialized-view). [#12712](https://github.com/StarRocks/starrocks/pull/12712) [#13171](https://github.com/StarRocks/starrocks/pull/13171) [#13229](https://github.com/StarRocks/starrocks/pull/13229) [#12926](https://github.com/StarRocks/starrocks/pull/12926)
  - The efficiency of refreshing materialized views is improved. [#13167](https://github.com/StarRocks/starrocks/issues/13167)
- Optimized the following aspects of data loading:
  - Optimized loading performance in multi-replica scenarios by supporting the "single leader replication" mode. Data loading gains a one-fold performance lift. For more information about "single leader replication", see `replicated_storage` in [CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/CREATE_TABLE/). [#10138](https://github.com/StarRocks/starrocks/pull/10138)
  - Broker Load and Spark Load no longer need to depend on brokers for data loading when only one HDFS cluster or one Kerberos user is configured. However, if you have multiple HDFS clusters or multiple Kerberos users, you still need to deploy a broker. For more information, see [Load data from HDFS or cloud storage](https://docs.starrocks.io/docs/loading/BrokerLoad/) and [Bulk load using Apache Spark™](https://docs.starrocks.io/docs/loading/SparkLoad/). [#9049](https://github.com/starrocks/starrocks/pull/9049) [#9228](https://github.com/StarRocks/starrocks/pull/9228)
  - Optimized the performance of Broker Load when a large number of small ORC files are loaded. [#11380](https://github.com/StarRocks/starrocks/pull/11380)
  - Reduced the memory usage when you load data into Primary Key tables.
- Optimized the `information_schema` database and the `tables` and `columns` tables within. Adds a new table `table_config`. For more information, see [Information Schema](https://docs.starrocks.io/docs/reference/overview-pages/information_schema/). [#10033](https://github.com/StarRocks/starrocks/pull/10033)
- Optimized data backup and restore:
  - Supports backing up and restoring data from multiple tables in a database at a time. For more information, see [Backup and restore data](https://docs.starrocks.io/docs/administration/Backup_and_restore/). [#11619](https://github.com/StarRocks/starrocks/issues/11619)
  - Supports backing up and restoring data from Primary Key tables. For more information, see Backup and restore. [#11885](https://github.com/StarRocks/starrocks/pull/11885)
- Optimized the following functions:
  - Added an optional parameter for the [time_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/time_slice/) function, which is used to determine whether the beginning or end of the time interval is returned. [#11216](https://github.com/StarRocks/starrocks/pull/11216)
  - Added a new mode `INCREASE` for the [window_funnel](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/window_funnel/) function to avoid computing duplicate timestamps. [#10134](https://github.com/StarRocks/starrocks/pull/10134)
  - Supports specifying multiple arguments in the [unnest](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/unnest/) function. [#12484](https://github.com/StarRocks/starrocks/pull/12484)
  - lead() and lag() functions support querying HLL and BITMAP data. For more information, see [Window function](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/). [#12108](https://github.com/StarRocks/starrocks/pull/12108)
  - The following ARRAY functions support querying JSON data: [array_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/), [array_sort](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_sort/), [array_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_concat/), [array_slice](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_slice/), and [reverse](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/reverse/). [#13155](https://github.com/StarRocks/starrocks/pull/13155)
  - Optimized the use of some functions. The `current_date`, `current_timestamp`, `current_time`, `localtimestamp`, and `localtime` functions can be executed without using `()`, for example, you can directly run `select current_date;`. [# 14319](https://github.com/StarRocks/starrocks/pull/14319)
- Removed some redundant information from FE logs. [# 15374](https://github.com/StarRocks/starrocks/pull/15374)

### Bug Fixes

The following bugs are fixed:

- The append_trailing_char_if_absent() function may return an incorrect result when the first argument is empty. [#13762](https://github.com/StarRocks/starrocks/pull/13762)
- After a table is restored using the RECOVER statement, the table does not exist. [#13921](https://github.com/StarRocks/starrocks/pull/13921)
- The result returned by the SHOW CREATE MATERIALIZED VIEW statement does not contain the database and catalog specified in the query statement when the materialized view was created. [#12833](https://github.com/StarRocks/starrocks/pull/12833)
- Schema change jobs in the `waiting_stable` state cannot be canceled. [#12530](https://github.com/StarRocks/starrocks/pull/12530)
- Running the `SHOW PROC '/statistic';` command on a Leader FE and non-Leader FE returns different results. [#12491](https://github.com/StarRocks/starrocks/issues/12491)
- The position of the ORDER BY clause is incorrect in the result returned by SHOW CREATE TABLE. [# 13809](https://github.com/StarRocks/starrocks/pull/13809)
- When users use Hive Catalog to query Hive data, if the execution plan generated by FE does not contain partition IDs, BEs fail to query Hive partition data. [# 15486](https://github.com/StarRocks/starrocks/pull/15486).

### Behavior Change

- Changed the default value of the `AWS_EC2_METADATA_DISABLED` parameter to `False`, which means that the metadata of Amazon EC2 is obtained to access AWS resources.
- Renamed session variable `is_report_success` to `enable_profile`, which can be queried using the SHOW VARIABLES statement.
- Added four reserved keywords: `CURRENT_DATE`, `CURRENT_TIME`, `LOCALTIME`, and `LOCALTIMESTAMP`. [# 14319](https://github.com/StarRocks/starrocks/pull/14319)
- The maximum length of table and database names can be up to 1023 characters. [# 14929](https://github.com/StarRocks/starrocks/pull/14929) [# 15020](https://github.com/StarRocks/starrocks/pull/15020)
- BE configuration items `enable_event_based_compaction_framework` and `enable_size_tiered_compaction_strategy` are set to `true` by default, which significantly reduces compaction overheads when there are a large number of tablets or a single tablet has large data volume.

### Upgrade Notes

- You can upgrade your cluster to 2.5.0 from 2.0.x, 2.1.x, 2.2.x, 2.3.x, or 2.4.x. However, if you need to perform a rollback, we recommend that you roll back only to 2.4.x.
