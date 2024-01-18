---
displayed_sidebar: "English"
---

# StarRocks version 3.0

## 3.0.9

Release date: January 2, 2024

### New features

- Added the [percentile_disc](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/percentile_disc/) function. [#36352](https://github.com/StarRocks/starrocks/pull/36352)
- Added a new metric `max_tablet_rowset_num` for setting the maximum allowed number of rowsets. This metric helps detect possible compaction issues and thus reduces the occurrences of the error "too many versions". [#36539](https://github.com/StarRocks/starrocks/pull/36539)

### Improvements

- A new value option `GROUP_CONCAT_LEGACY` is added to the session variable [sql_mode](https://docs.starrocks.io/docs/reference/System_variable#sql_mode) to provide compatibility with the implementation logic of the [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) function in versions earlier than v2.5. [#36150](https://github.com/StarRocks/starrocks/pull/36150)
- When using JDK, the default GC algorithm is G1. [#37386](https://github.com/StarRocks/starrocks/pull/37386)
- The `be_tablets` view in the `information_schema` database provides a new field `INDEX_DISK`, which records the disk usage (measured in bytes) of persistent indexes [#35615](https://github.com/StarRocks/starrocks/pull/35615)
- Queries on MySQL external tables and the external tables within JDBC catalogs support including keywords in the WHERE clause. [#35917](https://github.com/StarRocks/starrocks/pull/35917)
- Supports updates onto the specified partitions of an automatically partitioned table. If the specified partitions do not exist, an error is returned. [#34777](https://github.com/StarRocks/starrocks/pull/34777)
- The Primary Key table size returned by the [SHOW DATA](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_DATA/) statement includes the sizes of **.cols** files (these are files related to partial column updates and generated columns) and persistent index files. [#34898](https://github.com/StarRocks/starrocks/pull/34898)
- Optimized the performance of persistent index update when compaction is performed on all rowsets of a Primary Key table, which reduces disk read I/O. [#36819](https://github.com/StarRocks/starrocks/pull/36819)
- When the string on the right side of the LIKE operator within the WHERE clause does not include `%` or `_`, the LIKE operator is converted into the `=` operator. [#37515](https://github.com/StarRocks/starrocks/pull/37515)
- Optimized the logic used to compute compaction scores for Primary Key tables, thereby aligning the compaction scores for Primary Key tables within a more consistent range with the other three table types. [#36534](https://github.com/StarRocks/starrocks/pull/36534)
- The result returned by the [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) statement now includes the timestamps of consumption messages from each partition. [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- Optimized the performance of some Bitmap-related operations, including:
  - Optimized nested loop joins. [#340804](https://github.com/StarRocks/starrocks/pull/34804) [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - Optimized the `bitmap_xor` function. [#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - Supports Copy on Write to optimize Bitmap performance and reduce memory consumption. [#34047](https://github.com/StarRocks/starrocks/pull/34047)

### Behavior Change

- Added the session variable `enable_materialized_view_for_insert`, which controls whether materialized views rewrite the queries in INSERT INTO SELECT statements. The default value is `false`. [#37505](https://github.com/StarRocks/starrocks/pull/37505)
- Changed the FE configuration item `enable_new_publish_mechanism` to a static parameter from a dynamic one. You must restart the FE after you modify the parameter settings. [#35338](https://github.com/StarRocks/starrocks/pull/35338)
- Changed the default retention period of trash files to 1 day from the original 3 days. [#37113](https://github.com/StarRocks/starrocks/pull/37113)

### Parameter Change

#### Session variables

- Added session variable `cbo_decimal_cast_string_strict`, which controls how the CBO converts data from the DECIMAL type to the STRING type. If this variable is set to `true`, the logic built in v2.5.x and later versions prevails and the system implements strict conversion (namely, the system truncates the generated string and fills 0s based on the scale length). If this variable is set to `false`, the logic built in versions earlier than v2.5.x prevails and the system processes all valid digits to generate a string. The default value is `true`. [#34208](https://github.com/StarRocks/starrocks/pull/34208)
- Added session variables `transaction_read_only` and `tx_read_only` to specify the transaction access mode, which are compatible with MySQL versions 5.7.20 and above. [#37249](https://github.com/StarRocks/starrocks/pull/37249)

#### FE Parameters

- Added the FE configuration item `routine_load_unstable_threshold_second`. [#36222](https://github.com/StarRocks/starrocks/pull/36222)
- Added the FE configuration item `http_worker_threads_num`, which specifies the number of threads for HTTP server to deal with HTTP requests. The default value is `0`. If the value for this parameter is set to a negative value or 0, the actual thread number is twice the number of CPU cores. [#37530](https://github.com/StarRocks/starrocks/pull/37530)
- Added the FE configuration item `default_mv_refresh_immediate`, which specifies whether to immediately refresh the materialized view after the materialized view is created. The default value is `true`. [#37093](https://github.com/StarRocks/starrocks/pull/37093)

#### BE Parameters

- Added the BE configuration item `enable_stream_load_verbose_log`. The default value is `false`. With this parameter set to `true`, StarRocks can record the HTTP requests and responses for Stream Load jobs, making troubleshooting easier. [#36113](https://github.com/StarRocks/starrocks/pull/36113)
- Added the BE configuration item `pindex_major_compaction_limit_per_disk` to configure the maximum concurrency of compaction on a disk. This addresses the issue of uneven I/O across disks due to compaction. This issue can cause excessively high I/O for certain disks. The default value is `1`. [#37694](https://github.com/StarRocks/starrocks/pull/37694)
- Added BE configuration items to specify the timeout duration for connecting to object storage:
  - `object_storage_connect_timeout_ms`: Timeout duration to establish socket connections with object storage. The default value is `-1`, which means to use the default timeout duration of the SDK configurations.
  - `object_storage_request_timeout_ms`: Timeout duration to establish HTTP connections with object storage. The default value is `-1`, which means to use the default timeout duration of the SDK configurations.

### Bug Fixes

Fixed the following issues:

- In some cases, BEs may crash when a Catalog is used to read ORC external tables. [#27971](https://github.com/StarRocks/starrocks/pull/27971)
- The BEs crash if users create persistent indexes in the event of data corruption. [#30841](https://github.com/StarRocks/starrocks/pull/30841)
- BEs occasionally crash after a Bitmap index is added. [#26463](https://github.com/StarRocks/starrocks/pull/26463)
- Failures in replaying replica operations may cause FEs to crash. [#32295](https://github.com/StarRocks/starrocks/pull/32295)
- Setting the FE parameter `recover_with_empty_tablet` to `true` may cause FEs to crash. [#33071](https://github.com/StarRocks/starrocks/pull/33071)
- Queries fail during hash joins, causing BEs to crash. [#32219](https://github.com/StarRocks/starrocks/pull/32219)
- In a StarRocks shared-nothing cluster, queries against Iceberg or Hive tables may cause BEs to crash. [#34682](https://github.com/StarRocks/starrocks/pull/34682)
- The error "get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction" is returned for queries. [#33246](https://github.com/StarRocks/starrocks/pull/33246)
- Running `show proc '/statistic'` may cause a deadlock. [#34237](https://github.com/StarRocks/starrocks/pull/34237/files)
- The FE performance plunges after the FE configuration item `enable_collect_query_detail_info` is set to `true`. [#35945](https://github.com/StarRocks/starrocks/pull/35945)
- Errors may be thrown if large amounts of data are loaded into a Primary Key table with persistent index enabled. [#34352](https://github.com/StarRocks/starrocks/pull/34352)
- After StarRocks is upgraded from v2.4 or earlier to a later version, compaction scores may rise unexpectedly. [#34618](https://github.com/StarRocks/starrocks/pull/34618)
- If `INFORMATION_SCHEMA` is queried by using the database driver MariaDB ODBC, the `CATALOG_NAME` column returned in the `schemata` view holds only `null` values. [#34627](https://github.com/StarRocks/starrocks/pull/34627)
- FEs crash due to the abnormal data loaded and cannot restart. [#34590](https://github.com/StarRocks/starrocks/pull/34590)
- If schema changes are being executed while a Stream Load job is in the **PREPARD** state, a portion of the source data to be loaded by the job is lost. [#34381](https://github.com/StarRocks/starrocks/pull/34381)
- Including two or more slashes (`/`) at the end of the HDFS storage path causes the backup and restore of the data from HDFS to fail. [#34601](https://github.com/StarRocks/starrocks/pull/34601)
- The `partition_live_number` property added by using the ALTER TABLE statement does not take effect. [#34842](https://github.com/StarRocks/starrocks/pull/34842)
- The [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) function occasionally causes the BEs to crash. [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- Deadlocks may occur when users refresh materialized views. [#35736](https://github.com/StarRocks/starrocks/pull/35736)
- Global Runtime Filter may cause BEs to crash in certain scenarios. [#35776](https://github.com/StarRocks/starrocks/pull/35776)
- In some cases, `bitmap_to_string` may return incorrect result due to data type overflow. [#37405](https://github.com/StarRocks/starrocks/pull/37405)

## 3.0.8

Release date: November 17, 2023

### Improvements

- The `COLUMNS` view in the system database `INFORMATION_SCHEMA` can display ARRAY, MAP, and STRUCT columns. [#33431](https://github.com/StarRocks/starrocks/pull/33431)

### Bug Fixes

Fixed the following issues:

- When `show proc '/current_queries';` is being executed and meanwhile a query begins to be executed, BEs may crash. [#34316](https://github.com/StarRocks/starrocks/pull/34316)
- When data is continuously loaded into a Primary Key table with a sort key specified at a high frequency, compaction failures may occur. [#26486](https://github.com/StarRocks/starrocks/pull/26486)
- If a filtering condition is specified in a Broker Load job, BEs may crash during the data loading in certain circumstances. [#29832](https://github.com/StarRocks/starrocks/pull/29832)
- An unknown error is reported when SHOW GRANTS is executed. [#30100](https://github.com/StarRocks/starrocks/pull/30100)
- BE may crash for specific data types if the target data type specified in the `cast()` function is the same as the original data type. [#31465](https://github.com/StarRocks/starrocks/pull/31465)
- `DATA_TYPE` and `COLUMN_TYPE` for BINARY or VARBINARY data types are displayed as `unknown` in the `information_schema.columns` view. [#32678](https://github.com/StarRocks/starrocks/pull/32678)
- Long-time, frequent data loading into a Primary Key table with persistent index enabled may cause BEs to crash. [#33220](https://github.com/StarRocks/starrocks/pull/33220)
- The query result is incorrect when Query Cache is enabled. [#32778](https://github.com/StarRocks/starrocks/pull/32778)
- After a cluster is restarted, the data in a restored table may be inconsistent with the data in that table before being backed up. [#33567](https://github.com/StarRocks/starrocks/pull/33567)
- If RESTORE is executed and meanwhile Compaction takes place, it may cause BEs to crash. [#32902](https://github.com/StarRocks/starrocks/pull/32902)

## 3.0.7

Release date: October 18, 2023

### Improvements

- Window functions COVAR_SAMP, COVAR_POP, CORR, VARIANCE, VAR_SAMP, STD, and STDDEV_SAMP now support the ORDER BY clause and Window clause. [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- The Publish phase of a load job that writes data into a Primary Key table is changed from asynchronous mode to synchronous mode. As such, the data loaded can be queried immediately after the load job finishes. [#27055](https://github.com/StarRocks/starrocks/pull/27055)
- An error instead of NULL is returned if a decimal overflow occurs during queries on the DECIMAL type data. [#30419](https://github.com/StarRocks/starrocks/pull/30419)
- Executing SQL commands with invalid comments now returns results consistent with MySQL. [#30210](https://github.com/StarRocks/starrocks/pull/30210)
- For a StarRocks table that uses RANGE partitioning with only one partitioning column or expression partitioning, SQL predicates containing partition column expressions can also be used for partition pruning. [#30421](https://github.com/StarRocks/starrocks/pull/30421)

### Bug Fixes

Fixed the following issues:

- Concurrently creating and deleting databases and tables can, in certain cases, result in the table not being found and further leads to the failure of data loading into that table. [#28985](https://github.com/StarRocks/starrocks/pull/28985)
- Using UDFs may lead to memory leaks in certain cases. [#29467](https://github.com/StarRocks/starrocks/pull/29467) [#29465](https://github.com/StarRocks/starrocks/pull/29465)
- If the ORDER BY clause contains aggregate functions, an error "java.lang.IllegalStateException: null" is returned. [#30108](https://github.com/StarRocks/starrocks/pull/30108)
- If users run queries against data stored in Tencent COS by using their Hive catalog which consists of multiple levels, the query results will be incorrect. [#30363](https://github.com/StarRocks/starrocks/pull/30363)
- If some subcfields of the STRUCT in ARRAY&lt;STRUCT&gt; type data are missing, the data length is incorrect when default values are filled in the missing subcfields during queries, which causes BEs to crash.
- The version of Berkeley DB Java Edition is upgraded to avoid security vulnerabilities.[#30029](https://github.com/StarRocks/starrocks/pull/30029)
- If users load data into a Primary Key table on which truncate operations and queries are concurrently performed, an error "java.lang.NullPointerException" is thrown in certain cases. [#30573](https://github.com/StarRocks/starrocks/pull/30573)
- If the Schema Change execution time is too long, it may fail because the tablet of the specified version is garbage-collected. [#31376](https://github.com/StarRocks/starrocks/pull/31376)
- If users use CloudCanal to load data into table columns that are set to `NOT NULL` but have no default value specified, an error "Unsupported dataFormat value is : \N" is thrown. [#30799](https://github.com/StarRocks/starrocks/pull/30799)
- In StarRocks shared-data clusters, the information of table keys is not recorded in `information_schema.COLUMNS`. As a result, DELETE operations cannot be performed when data is loaded by using Flink Connector. [#31458](https://github.com/StarRocks/starrocks/pull/31458)
- During the upgrade, if the types of certain columns are also upgraded (for example, from Decimal type to Decimal v3 type), compaction on certain tables with specific characteristics may cause BEs to crash. [#31626](https://github.com/StarRocks/starrocks/pull/31626)
- When data is loaded by using Flink Connector, the load job is suspended unexpectedly if there are highly concurrent load jobs and both the number of HTTP threads and the number of Scan threads have reached their upper limits. [#32251](https://github.com/StarRocks/starrocks/pull/32251)
- BEs crash when libcurl is invoked. [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- An error occurs when a column of BITMAP type is added to a Primary Key table. [#31763](https://github.com/StarRocks/starrocks/pull/31763)

## 3.0.6

Release date: September 12, 2023

### Behavior Change

- When using the [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) function, you must use the SEPARATOR keyword to declare the separator.

### New Features

- The aggregate function [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) supports the DISTINCT keyword and the ORDER BY clause. [#28778](https://github.com/StarRocks/starrocks/pull/28778)
- Data in partitions can be automatically cooled down over time. (This feature is not supported for [list partitioning](https://docs.starrocks.io/docs/table_design/list_partitioning/).) [#29335](https://github.com/StarRocks/starrocks/pull/29335) [#29393](https://github.com/StarRocks/starrocks/pull/29393)

### Improvements

- Supports implicit conversions for all compound predicates and for all expressions in the WHERE clause. You can enable or disable implicit conversions by using the [session variable](https://docs.starrocks.io/docs/reference/System_variable/) `enable_strict_type`. The default value of this session variable is `false`. [#21870](https://github.com/StarRocks/starrocks/pull/21870)
- Unifies the logic between FEs and BEs in converting strings to integers. [#29969](https://github.com/StarRocks/starrocks/pull/29969)

### Bug Fixes

- If `enable_orc_late_materialization` is set to `true`, an unexpected result is returned when a Hive catalog is used to query STRUCT-type data in ORC files. [#27971](https://github.com/StarRocks/starrocks/pull/27971)
- During data queries through Hive Catalog, if a partitioning column and an OR operator are specified in the WHERE clause, the query result is incorrect. [#28876](https://github.com/StarRocks/starrocks/pull/28876)
- The values returned by the RESTful API action `show_data` for cloud-native tables are incorrect. [#29473](https://github.com/StarRocks/starrocks/pull/29473)
- If the [shared-data cluster](https://docs.starrocks.io/docs/deployment/shared_data/azure/) stores data in Azure Blob Storage and a table is created, the FE fails to start after the cluster is rolled back to version 3.0. [#29433](https://github.com/StarRocks/starrocks/pull/29433)
- A user has no permission when querying a table in the Iceberg catalog even if the user is granted permission on that table. [#29173](https://github.com/StarRocks/starrocks/pull/29173)
- The `Default` field values returned by the [SHOW FULL COLUMNS](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SHOW_FULL_COLUMNS/) statement for columns of the [BITMAP](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-types/BITMAP/) or [HLL](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-types/HLL/) data type are incorrect. [#29510](https://github.com/StarRocks/starrocks/pull/29510)
- Modifying the FE dynamic parameter `max_broker_load_job_concurrency` using the `ADMIN SET FRONTEND CONFIG` command does not take effect.
- The FE may fail to start when a materialized view is being refreshed while its refresh strategy is being modified. [#29964](https://github.com/StarRocks/starrocks/pull/29964) [#29720](https://github.com/StarRocks/starrocks/pull/29720)
- The error `unknown error` is returned when `select count(distinct(int+double)) from table_name` is executed. [#29691](https://github.com/StarRocks/starrocks/pull/29691)
- After a Primary Key table is restored, metadata errors occur and cause metadata inconsistencies occur if a BE is restarted. [#30135](https://github.com/StarRocks/starrocks/pull/30135)

## 3.0.5

Release date: August 16, 2023

### New Features

- Supports aggregate functions [COVAR_SAMP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_samp/), [COVAR_POP](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/covar_pop/), and [CORR](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/corr/).
- Supports the following [window functions](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function/): COVAR_SAMP, COVAR_POP, CORR, VARIANCE, VAR_SAMP, STD, and STDDEV_SAMP.

### Improvements

- Added more prompts in the error message `xxx too many versions xxx`. [#28397](https://github.com/StarRocks/starrocks/pull/28397)
- Dynamic partitioning further supports the partitioning unit to be year. [#28386](https://github.com/StarRocks/starrocks/pull/28386)
- The partitioning field is case-insensitive when expression partitioning is used at table creation and [INSERT OVERWRITE is used to overwrite data in a specific partition](https://docs.starrocks.io/docs/table_design/expression_partitioning#load-data-into-partitions). [#28309](https://github.com/StarRocks/starrocks/pull/28309)

### Bug Fixes

Fixed the following issues:

- Incorrect table-level scan statistics in FE cause inaccurate metrics for table queries and loading. [#27779](https://github.com/StarRocks/starrocks/pull/27779)
- The query result is not stable if the sort key is modified for a partitioned table. [#27850](https://github.com/StarRocks/starrocks/pull/27850)
- The version number for a tablet is inconsistent between the BE and FE after data is restored. [#26518](https://github.com/StarRocks/starrocks/pull/26518/files)
- If the bucket number is not specified when users create a Colocation table, the number will be inferred as 0, which causes failures in adding new partitions. [#27086](https://github.com/StarRocks/starrocks/pull/27086)
- When the SELECT result set of INSERT INTO SELECT is empty, the load job status returned by SHOW LOAD is `CANCELED`. [#26913](https://github.com/StarRocks/starrocks/pull/26913)
- BEs may crash when the input values of the sub_bitmap function are not of the BITMAP type. [#27982](https://github.com/StarRocks/starrocks/pull/27982)
- BEs may crash when the AUTO_INCREMENT column is being updated. [#27199](https://github.com/StarRocks/starrocks/pull/27199)
- Outer join and Anti join rewrite errors for materialized views. [#28028](https://github.com/StarRocks/starrocks/pull/28028)
- Inaccurate estimation of average row size causes Primary Key partial updates to occupy excessively large memory. [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- Activating an inactive materialized view may cause a FE to crash. [#27959](https://github.com/StarRocks/starrocks/pull/27959)
- Queries can not be rewritten to materialized views created based on external tables in a Hudi catalog. [#28023](https://github.com/StarRocks/starrocks/pull/28023)
- The data of a Hive table can still be queried even after the table is dropped and the metadata cache is manually updated. [#28223](https://github.com/StarRocks/starrocks/pull/28223)
- Manually refreshing an asynchronous materialized view via a synchronous call results in multiple INSERT OVERWRITE records in the `information_schema.task_runs` table. [#28060](https://github.com/StarRocks/starrocks/pull/28060)
- FE memory leak caused by blocked LabelCleaner threads. [#28311](https://github.com/StarRocks/starrocks/pull/28311)

## 3.0.4

Release date: July 18, 2023

### New Feature

Queries can be rewritten even when the queries contain a different type of join than the materialized view. [#25099](https://github.com/StarRocks/starrocks/pull/25099)

### Improvements

- Optimized the manual refreshing of asynchronous materialized views. Supports using the REFRESH MATERIALIZED VIEW WITH SYNC MODE syntax to synchronously invoke materialized view refresh tasks. [#25910](https://github.com/StarRocks/starrocks/pull/25910)
- If the queried fields are not included in the output columns of a materialized view but are included in the predicate of the materialized view, the query can still be rewritten to benefit from the materialized view. [#23028](https://github.com/StarRocks/starrocks/issues/23028)
- [When the SQL dialect (`sql_dialect`) is set to `trino`](https://docs.starrocks.io/docs/reference/System_variable/), table aliases are not case-sensitive. [#26094](https://github.com/StarRocks/starrocks/pull/26094) [#25282](https://github.com/StarRocks/starrocks/pull/25282)
- Added a new field `table_id` to the table `Information_schema.tables_config`. You can join the table `tables_config` with the table `be_tablets` on the column `table_id` in the database `Information_schema` to query the names of the database and table to which a tablet belongs. [#24061](https://github.com/StarRocks/starrocks/pull/24061)

### Bug Fixes

Fixed the following issues:

- If a query that contains the sum aggregate function is rewritten to directly obtain query results from a single-table materialized view, the values in sum() field may be incorrect due to type inference issues. [#25512](https://github.com/StarRocks/starrocks/pull/25512)
- An error occurs when SHOW PROC is used to view information about tablets in a StarRocks shared-data cluster.
- The INSERT operation hangs when the length of CHAR data in a STRUCT to be inserted exceeds the maximum length. [#25942](https://github.com/StarRocks/starrocks/pull/25942)
- Some data rows queried fail to be returned for INSERT INTO SELECT with FULL JOIN. [#26603](https://github.com/StarRocks/starrocks/pull/26603)
- An error `ERROR xxx: Unknown table property xxx` occurs when the ALTER TABLE statement is used to modify the table's property `default.storage_medium`. [#25870](https://github.com/StarRocks/starrocks/issues/25870)
- An error occurs when Broker Load is used to load empty files. [#26212](https://github.com/StarRocks/starrocks/pull/26212)
- Decommissioning a BE sometimes hangs. [#26509](https://github.com/StarRocks/starrocks/pull/26509)

## 3.0.3

Release date: June 28, 2023

### Improvements

- Metadata synchronization of StarRocks external tables has been changed to occur during data loading. [#24739](https://github.com/StarRocks/starrocks/pull/24739)
- Users can specify partitions when they run INSERT OVERWRITE on tables whose partitions are automatically created. For more information, see [Automatic partitioning](https://docs.starrocks.io/en-us/3.0/table_design/dynamic_partitioning). [#25005](https://github.com/StarRocks/starrocks/pull/25005)
- Optimized the error message reported when partitions are added to a non-partitioned table. [#25266](https://github.com/StarRocks/starrocks/pull/25266)

### Bug Fixes

Fixed the following issues:

- The min/max filter gets the wrong Parquet field when the Parquet file contains complex data types. [#23976](https://github.com/StarRocks/starrocks/pull/23976)
- Load tasks are still queuing even when the related database or table has been dropped. [#24801](https://github.com/StarRocks/starrocks/pull/24801)
- There is a low probability that an FE restart may cause BEs to crash. [#25037](https://github.com/StarRocks/starrocks/pull/25037)
- Load and query jobs occasionally freeze when the variable `enable_profile` is set to `true`. [#25060](https://github.com/StarRocks/starrocks/pull/25060)
- Inaccurate error message is displayed when INSERT OVERWRITE is executed on a cluster with less than three alive BEs. [#25314](https://github.com/StarRocks/starrocks/pull/25314)

## 3.0.2

Release date: June 13, 2023

### Improvements

- Predicates in a UNION query can be pushed down after the query is rewritten by an asynchronous materialized view. [#23312](https://github.com/StarRocks/starrocks/pull/23312)
- Optimized the [auto tablet distribution policy](https://docs.starrocks.io/docs/3.0/table_design/Data_distribution#set-the-number-of-buckets) for tables. [#24543](https://github.com/StarRocks/starrocks/pull/24543)
- Removed the dependency of NetworkTime on system clocks, which fixes incorrect NetworkTime caused by inconsistent system clocks across servers. [#24858](https://github.com/StarRocks/starrocks/pull/24858)

### Bug Fixes

Fixed the following issues:

- A schema change sometimes may be hung if data loading occurs simultaneously with the schema change. [#23456](https://github.com/StarRocks/starrocks/pull/23456)
- Queries encounter an error when the session variable `pipeline_profile_level` is set to `0`. [#23873](https://github.com/StarRocks/starrocks/pull/23873)
- CREATE TABLE encounters an error when `cloud_native_storage_type` is set to `S3`.
- LDAP authentication succeeds even when no password is used. [#24862](https://github.com/StarRocks/starrocks/pull/24862)
- CANCEL LOAD fails if the table involved in the load job does not exist. [#24922](https://github.com/StarRocks/starrocks/pull/24922)

### Upgrade Notes

If your system has a database named `starrocks`, change it to another name using ALTER DATABASE RENAME before the upgrade. This is because `starrocks` is the name of a default system database that stores privilege information.

## 3.0.1

Release date: June 1, 2023

### New Features

- [Preview] Supports spilling intermediate computation results of large operators to disks to reduce the memory consumption of large operators. For more information, see [Spill to disk](https://docs.starrocks.io/docs/administration/spill_to_disk/).
- [Routine Load](https://docs.starrocks.io/docs/loading/RoutineLoad#load-avro-format-data) supports loading Avro data.
- Supports [Microsoft Azure Storage](https://docs.starrocks.io/docs/integrations/authenticate_to_azure_storage/) (including Azure Blob Storage and Azure Data Lake Storage).

### Improvements

- Shared-data clusters support using StarRocks external tables to synchronize data with another StarRocks cluster.
- Added `load_tracking_logs` to [Information Schema](https://docs.starrocks.io/docs/reference/information_schema/load_tracking_logs/) to record recent loading errors.
- Ignores special characters in CREATE TABLE statements. [#23885](https://github.com/StarRocks/starrocks/pull/23885)

### Bug Fixes

Fixed the following issues:

- Information returned by SHOW CREATE TABLE is incorrect for Primary Key tables. [#24237](https://github.com/StarRocks/starrocks/issues/24237)
- BEs may crash during a Routine Load job. [#20677](https://github.com/StarRocks/starrocks/issues/20677)
- Null pointer exception (NPE) occurs if you specify unsupported properties when creating a partitioned table. [#21374](https://github.com/StarRocks/starrocks/issues/21374)
- Information returned by SHOW TABLE STATUS is incomplete. [#24279](https://github.com/StarRocks/starrocks/issues/24279)

### Upgrade Notes

If your system has a database named `starrocks`, change it to another name using ALTER DATABASE RENAME before the upgrade. This is because `starrocks` is the name of a default system database that stores privilege information.

## 3.0.0

Release date: April 28, 2023

### New Features

#### System architecture

- **Decouple storage and compute.** StarRocks now supports data persistence into S3-compatible object storage, enhancing resource isolation, reducing storage costs, and making compute resources more scalable. Local disks are used as hot data cache for boosting query performance. The query performance of the new shared-data architecture is comparable to the classic architecture (shared-nothing) when local disk cache is hit. For more information, see [Deploy and use shared-data StarRocks](https://docs.starrocks.io/docs/deployment/shared_data/s3/).

#### Storage engine and data ingestion

- The [AUTO_INCREMENT](https://docs.starrocks.io/docs/sql-reference/sql-statements/auto_increment/) attribute is supported to provide globally unique IDs, which simplifies data management.
- [Automatic partitioning and partitioning expressions](https://docs.starrocks.io/docs/3.0/table_design/automatic_partitioning/) are supported, which makes partition creation easier to use and more flexible.
- Primary Key tables support more complete [UPDATE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/UPDATE/) and [DELETE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/DELETE#primary-key-tables) syntax, including the use of CTEs and references to multiple tables.
- Added Load Profile for Broker Load and INSERT INTO jobs. You can view the details of a load job by querying the load profile. The usage is the same as [Analyze query profile](https://docs.starrocks.io/docs/administration/query_profile/).

#### Data Lake Analytics

- [Preview] Supports Presto/Trino compatible dialect. Presto/Trino's SQL can be automatically rewritten into StarRocks' SQL pattern. For more information, see [the system variable](https://docs.starrocks.io/docs/reference/System_variable/) `sql_dialect`.
- [Preview] Supports [JDBC catalogs](https://docs.starrocks.io/docs/data_source/catalog/jdbc_catalog/).
- Supports using [SET CATALOG](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-definition/SET_CATALOG/) to manually switch between catalogs in the current session.

#### Privileges and security

- Provides a new privilege system with full RBAC functionalities, supporting role inheritance and default roles. For more information, see [Overview of privileges](https://docs.starrocks.io/docs/administration/privilege_overview/).
- Provides more privilege management objects and more fine-grained privileges. For more information, see [Privileges supported by StarRocks](https://docs.starrocks.io/docs/administration/privilege_item/).

#### Query engine

<!-- - [Preview] Supports operator **spilling** for large queries, which can use disk space to ensure stable running of queries in case of insufficient memory. -->
- Allows more queries on joined tables to benefit from the [query cache](https://docs.starrocks.io/docs/using_starrocks/query_cache/). For example, the query cache now supports Broadcast Join and Bucket Shuffle Join.
- Supports [Global UDFs](https://docs.starrocks.io/docs/sql-reference/sql-functions/JAVA_UDF/).
- Dynamic adaptive parallelism: StarRocks can automatically adjust the `pipeline_dop` parameter for query concurrency.

#### SQL reference

- Added the following privilege-related SQL statements: [SET DEFAULT ROLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE/), [SET ROLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SET_ROLE/), [SHOW ROLES](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SHOW_ROLES/), and [SHOW USERS](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/SHOW_USERS/).
- Added the following semi-structured data analysis functions: [map_apply](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_apply/), [map_from_arrays](https://docs.starrocks.io/docs/sql-reference/sql-functions/map-functions/map_from_arrays/).
- [array_agg](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_agg/) supports ORDER BY.
- Window functions [lead](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lead) and [lag](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lag) support IGNORE NULLS.
- Added string functions [replace](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/replace/), [hex_decode_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/hex_decode_binary/), and [hex_decode_string()](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/hex_decode_string/).
- Added encryption functions [base64_decode_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/crytographic-functions/base64_decode_binary/) and [base64_decode_string](https://docs.starrocks.io/docs/sql-reference/sql-functions/crytographic-functions/base64_decode_string/).
- Added math functions [sinh](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/sinh/), [cosh](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/cosh/), and [tanh](https://docs.starrocks.io/docs/sql-reference/sql-functions/math-functions/tanh/).
- Added utility function [current_role](https://docs.starrocks.io/docs/sql-reference/sql-functions/utility-functions/current_role/).

### Improvements

#### Deployment

- Updated Docker image and the related [Docker deployment document](https://docs.starrocks.io/docs/quick_start/deploy_with_docker/) for version 3.0. [#20623](https://github.com/StarRocks/starrocks/pull/20623) [#21021](https://github.com/StarRocks/starrocks/pull/21021)

#### Storage engine and data ingestion

- Supports more CSV parameters for data ingestion, including SKIP_HEADER, TRIM_SPACE, ENCLOSE, and ESCAPE. See [STREAM LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/), [BROKER LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/BROKER_LOAD/), and [ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD/).
- The primary key and sort key are decoupled in [Primary Key tables](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/). The sort key can be separately specified in `ORDER BY` when you create a table.
- Optimized the memory usage of data ingestion into Primary Key tables in scenarios such as large-volume ingestion, partial updates, and persistent primary indexes.
- Supports creating asynchronous INSERT tasks. For more information, see [INSERT](https://docs.starrocks.io/docs/loading/InsertInto#load-data-asynchronously-using-insert) and [SUBMIT TASK](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SUBMIT_TASK/). [#20609](https://github.com/StarRocks/starrocks/issues/20609)

#### Materialized view

- Optimized the rewriting capabilities of [materialized views](https://docs.starrocks.io/docs/using_starrocks/Materialized_view/), including:
  - Supports rewrite of View Delta Join, Outer Join, and Cross Join.
  - Optimized SQL rewrite of Union with partition.
- Improved materialized view building capabilities: supporting CTE, select *, and Union.
- Optimized the information returned by [SHOW MATERIALIZED VIEWS](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW/).
- Supports adding MV partitions in batches, which improves the efficiency of partition addition during materialized view building. [#21167](https://github.com/StarRocks/starrocks/pull/21167)

#### Query engine

- All operators are supported in the pipeline engine. Non-pipeline code will be removed in later versions.
- Improved [Big Query Positioning](https://docs.starrocks.io/docs/administration/monitor_manage_big_queries/) and added big query log. [SHOW PROCESSLIST](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SHOW_PROCESSLIST/) supports viewing CPU and memory information.
- Optimized Outer Join Reorder.
- Optimized error messages in the SQL parsing stage, providing more accurate error positioning and clearer error messages.

#### Data Lake Analytics

- Optimized metadata statistics collection.
- Supports using [SHOW CREATE TABLE](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE/) to view the creation statements of the tables that are managed by an external catalog and are stored in Apache Hive™, Apache Iceberg, Apache Hudi, or Delta Lake.

### Bug Fixes

- Some URLs in the license header of StarRocks' source file cannot be accessed. [#2224](https://github.com/StarRocks/starrocks/issues/2224)
- An unknown error is returned during SELECT queries. [#19731](https://github.com/StarRocks/starrocks/issues/19731)
- Supports SHOW/SET CHARACTER. [#17480](https://github.com/StarRocks/starrocks/issues/17480)
- When the loaded data exceeds the field length supported by StarRocks, the error message returned is not correct. [#14](https://github.com/StarRocks/DataX/issues/14)
- Supports `show full fields from 'table'`. [#17233](https://github.com/StarRocks/starrocks/issues/17233)
- Partition pruning causes MV rewrites to fail. [#14641](https://github.com/StarRocks/starrocks/issues/14641)
- MV rewrite fails when the CREATE MATERIALIZED VIEW statement contains `count(distinct)` and `count(distinct)` is applied to the DISTRIBUTED BY column. [#16558](https://github.com/StarRocks/starrocks/issues/16558)
- FEs fail to start when a VARCHAR column is used as the partitioning column of a materialized view. [#19366](https://github.com/StarRocks/starrocks/issues/19366)
- Window functions [LEAD](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lead) and [LAG](https://docs.starrocks.io/docs/sql-reference/sql-functions/Window_function#lag) incorrectly handle IGNORE NULLS. [#21001](https://github.com/StarRocks/starrocks/pull/21001)
- Adding temporary partitions conflicts with automatic partition creation. [#21222](https://github.com/StarRocks/starrocks/issues/21222)

### Behavior Change

- The new role-based access control (RBAC) system supports the previous privileges and roles. However, the syntax of related statements such as [GRANT](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/) and [REVOKE](https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/REVOKE/) is changed.
- Renamed SHOW MATERIALIZED VIEW as [SHOW MATERIALIZED VIEWS](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW/).
- Added the following [Reserved keywords](https://docs.starrocks.io/docs/sql-reference/sql-statements/keywords/): AUTO_INCREMENT, CURRENT_ROLE, DEFERRED, ENCLOSE, ESCAPE, IMMEDIATE, PRIVILEGES, SKIP_HEADER, TRIM_SPACE, VARBINARY.

### Upgrade Notes

You can upgrade from v2.5 to v3.0 or downgrade from v3.0 to v2.5.

> In theory, an upgrade from a version earlier than v2.5 is also supported. To ensure system availability, we recommend that you first upgrade your cluster to v2.5 and then to v3.0.

Take note of the following points when you perform a downgrade from v3.0 to v2.5.

#### BDBJE

StarRocks upgrades the BDB library in v3.0. However, BDBJE cannot be rolled back. You must use BDB library of v3.0 after a downgrade. Perform the following steps:

1. After you replace the FE package with a v2.5 package, copy `fe/lib/starrocks-bdb-je-18.3.13.jar` of v3.0 to the `fe/lib` directory of v2.5.

2. Delete `fe/lib/je-7.*.jar`.

#### Privilege system

The new RBAC privilege system is used by default after you upgrade to v3.0. You can only downgrade to v2.5.

After a downgrade, run [ALTER SYSTEM CREATE IMAGE](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/ALTER_SYSTEM/) to create a new image and wait for the new image to be synchronized to all follower FEs. If you do not run this command, some of the downgrade operations may fail. This command is supported from 2.5.3 and later.

For details about the differences between the privilege system of v2.5 and v3.0, see "Upgrade notes" in [Privileges supported by StarRocks](https://docs.starrocks.io/docs/administration/privilege_item/).
