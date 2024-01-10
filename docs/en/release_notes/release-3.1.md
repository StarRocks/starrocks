---
displayed_sidebar: "English"
---

# StarRocks version 3.1

## 3.1.6

Release date: December 18, 2023

### New Features

- Added the [now(p)](https://docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/now/) function to return the current date and time with the specified fractional seconds precision (accurate to the microsecond). If `p` is not specified, this function returns only date and time accurate to the second. [#36676](https://github.com/StarRocks/starrocks/pull/36676)
- Added a new metric `max_tablet_rowset_num` for setting the maximum allowed number of rowsets. This metric helps detect possible compaction issues and thus reduces the occurrences of the error "too many versions". [#36539](https://github.com/StarRocks/starrocks/pull/36539)
- Supports obtaining heap profiles by using a command line tool, making troubleshooting easier.[#35322](https://github.com/StarRocks/starrocks/pull/35322)
- Supports creating asynchronous materialized views with common table expressions (CTEs). [#36142](https://github.com/StarRocks/starrocks/pull/36142)
- Added the following bitmap functions: [subdivide_bitmap](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/subdivide_bitmap/), [bitmap_from_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_from_binary/), and [bitmap_to_binary](https://docs.starrocks.io/docs/sql-reference/sql-functions/bitmap-functions/bitmap_to_binary/). [#35817](https://github.com/StarRocks/starrocks/pull/35817) [#35621](https://github.com/StarRocks/starrocks/pull/35621)
- Optimized the logic used to compute compaction scores for Primary Key tables, thereby aligning the compaction scores for Primary Key tables within a more consistent range with the other three table types. [#36534](https://github.com/StarRocks/starrocks/pull/36534)

### Parameter Change

- The default retention period of trash files is changed to 1 day from the original 3 days. [#37113](https://github.com/StarRocks/starrocks/pull/37113)
- A new BE configuration item `enable_stream_load_verbose_log` is added. The default value is `false`. With this parameter set to `true`, StarRocks can record the HTTP requests and responses for Stream Load jobs, making troubleshooting easier. [#36113](https://github.com/StarRocks/starrocks/pull/36113)
- A new BE configuration item `enable_lazy_delta_column_compaction` is added. The default value is `true`, indicating that StarRocks does not perform frequent compaction operations on delta columns. [#36654](https://github.com/StarRocks/starrocks/pull/36654)
- A new FE configuration item `enable_mv_automatic_active_check` is added to control whether the system automatically checks and re-activates the asynchronous materialized views that are set inactive because their base tables (views) had undergone Schema Change or had been dropped and re-created. The default value is `true`. [#36463](https://github.com/StarRocks/starrocks/pull/36463)

### Improvements

- A new value option `GROUP_CONCAT_LEGACY` is added to the session variable [sql_mode](https://docs.starrocks.io/docs/reference/System_variable/#sql_mode) to provide compatibility with the implementation logic of the [group_concat](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/group_concat/) function in versions earlier than v2.5. [#36150](https://github.com/StarRocks/starrocks/pull/36150)
- The Primary Key table size returned by the [SHOW DATA](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_DATA/) statement includes the sizes of **.cols** files (these are files related to partial column updates and generated columns) and persistent index files. [#34898](https://github.com/StarRocks/starrocks/pull/34898)
- Queries on MySQL external tables and the external tables within JDBC catalogs support including keywords in the WHERE clause. [#35917](https://github.com/StarRocks/starrocks/pull/35917)
- Plugin loading failures will no longer cause an error or cause an FE start failure. Instead, the FE can properly start, and the error status of the plug-in can be queried using [SHOW PLUGINS](https://docs.starrocks.io/docs/sql-reference/sql-statements/Administration/SHOW_PLUGINS/). [#36566](https://github.com/StarRocks/starrocks/pull/36566)
- Dynamic partitioning supports random distribution. [#35513](https://github.com/StarRocks/starrocks/pull/35513)
- The result returned by the [SHOW ROUTINE LOAD](https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/SHOW_ROUTINE_LOAD/) statement provides a new field `OtherMsg`, which shows information about the last failed task. [#35806](https://github.com/StarRocks/starrocks/pull/35806)
- The authentication information `aws.s3.access_key` and `aws.s3.access_secret` for AWS S3 in Broker Load jobs are hidden in audit logs. [#36571](https://github.com/StarRocks/starrocks/pull/36571)
- The `be_tablets` view in the `information_schema` database provides a new field `INDEX_DISK`, which records the disk usage (measured in bytes) of persistent indexes [#35615](https://github.com/StarRocks/starrocks/pull/35615)

### Bug Fixes

Fixed the following issues:

- The BEs crash if users create persistent indexes in the event of data corruption. [#30841](https://github.com/StarRocks/starrocks/pull/30841)
- If users create an asynchronous materialized view that contains nested queries, the error "resolve partition column failed" is reported. [#26078](https://github.com/StarRocks/starrocks/issues/26078)
- If users create an asynchronous materialized view on a base table whose data is corrupted, the error "Unexpected exception: null" is reported. [#30038](https://github.com/StarRocks/starrocks/pull/30038)
- If users run a query that contains a window function, the SQL error "[1064] [42000]: Row count of const column reach limit: 4294967296" is reported. [#33561](https://github.com/StarRocks/starrocks/pull/33561)
- The FE performance plunges after the FE configuration item `enable_collect_query_detail_info` is set to `true`. [#35945](https://github.com/StarRocks/starrocks/pull/35945)
- In the StarRocks shared-data mode, the error "Reduce your request rate" may be reported when users attempt to delete files from object storage. [#35566](https://github.com/StarRocks/starrocks/pull/35566)
- Deadlocks may occur when users refresh materialized views. [#35736](https://github.com/StarRocks/starrocks/pull/35736)
- After the DISTINCT window operator pushdown feature is enabled, errors are reported if SELECT DISTINCT operations are performed on the complex expressions of the columns computed by window functions. [#36357](https://github.com/StarRocks/starrocks/pull/36357)
- The BEs crash if the source data file is in ORC format and contains nested arrays. [#36127](https://github.com/StarRocks/starrocks/pull/36127)
- Some S3-compatible object storage returns duplicate files, causing the BEs to crash. [#36103](https://github.com/StarRocks/starrocks/pull/36103)
- The [array_distinct](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_distinct/) function occasionally causes the BEs to crash. [#36377](https://github.com/StarRocks/starrocks/pull/36377)
- Global Runtime Filter may cause BEs to crash in certain scenarios. [#35776](https://github.com/StarRocks/starrocks/pull/35776)

## 3.1.5

Release date: November 28, 2023

### New features

- The CN nodes of a StarRocks shared-data cluster now support data export. [#34018](https://github.com/StarRocks/starrocks/pull/34018)

### Improvements

- The [`COLUMNS`](../reference/information_schema/columns.md) view in the system database `INFORMATION_SCHEMA` can display ARRAY, MAP, and STRUCT columns. [#33431](https://github.com/StarRocks/starrocks/pull/33431)
- Supports queries against Parquet, ORC, and CSV formatted files that are compressed by using LZO and stored in [Hive](../data_source/catalog/hive_catalog.md). [#30923](https://github.com/StarRocks/starrocks/pull/30923)  [#30721](https://github.com/StarRocks/starrocks/pull/30721)
- Supports updates onto the specified partitions of an automatically partitioned table. If the specified partitions do not exist, an error is returned. [#34777](https://github.com/StarRocks/starrocks/pull/34777)
- Supports automatic refresh of materialized views when Swap, Drop, or Schema Change operations are performed on the tables and views (including the other tables and materialized views associated with these views) on which these materialized views are created. [#32829](https://github.com/StarRocks/starrocks/pull/32829)
- Optimized the performance of some Bitmap-related operations, including:
  - Optimized nested loop joins. [#340804](https://github.com/StarRocks/starrocks/pull/34804)  [#35003](https://github.com/StarRocks/starrocks/pull/35003)
  - Optimized the `bitmap_xor` function. [#34069](https://github.com/StarRocks/starrocks/pull/34069)
  - Supports Copy on Write to optimize Bitmap performance and reduce memory consumption. [#34047](https://github.com/StarRocks/starrocks/pull/34047)

### Bug Fixes

Fixed the following issues:

- If a filtering condition is specified in a Broker Load job, BEs may crash during the data loading in certain circumstances. [#29832](https://github.com/StarRocks/starrocks/pull/29832)
- An unknown error is reported when SHOW GRANTS is executed. [#30100](https://github.com/StarRocks/starrocks/pull/30100)
- When data is loaded into a table that uses expression-based automatic partitioning, the error "Error: The row create partition failed since Runtime error: failed to analyse partition value" may be thrown. [#33513](https://github.com/StarRocks/starrocks/pull/33513)
- The error "get_applied_rowsets failed, tablet updates is in error state: tablet:18849 actual row size changed after compaction" is returned for queries. [#33246](https://github.com/StarRocks/starrocks/pull/33246)
- In a StarRocks shared-nothing cluster, queries against Iceberg or Hive tables may cause BEs to crash. [#34682](https://github.com/StarRocks/starrocks/pull/34682)
- In a StarRocks shared-nothing cluster, if multiple partitions are automatically created during data loading, the data loaded may occasionally be written to unmatched partitions. [#34731](https://github.com/StarRocks/starrocks/pull/34731)
- Long-time, frequent data loading into a Primary Key table with persistent index enabled may cause BEs to crash. [#33220](https://github.com/StarRocks/starrocks/pull/33220)
- The error "Exception: java.lang.IllegalStateException: null" is returned for queries. [#33535](https://github.com/StarRocks/starrocks/pull/33535)
- When `show proc '/current_queries';` is being executed and meanwhile a query begins to be executed, BEs may crash. [#34316](https://github.com/StarRocks/starrocks/pull/34316)
- Errors may be thrown if large amounts of data are loaded into a Primary Key table with persistent index enabled. [#34352](https://github.com/StarRocks/starrocks/pull/34352)
- After StarRocks is upgraded from v2.4 or earlier to a later version, compaction scores may rise unexpectedly. [#34618](https://github.com/StarRocks/starrocks/pull/34618)
- If `INFORMATION_SCHEMA` is queried by using the database driver MariaDB ODBC, the `CATALOG_NAME` column returned in the `schemata` view holds only `null` values. [#34627](https://github.com/StarRocks/starrocks/pull/34627)
- FEs crash due to the abnormal data loaded and cannot restart. [#34590](https://github.com/StarRocks/starrocks/pull/34590)
- If schema changes are being executed while a Stream Load job is in the **PREPARD** state, a portion of the source data to be loaded by the job is lost. [#34381](https://github.com/StarRocks/starrocks/pull/34381)
- Including two or more slashes (`/`) at the end of the HDFS storage path causes the backup and restore of the data from HDFS to fail. [#34601](https://github.com/StarRocks/starrocks/pull/34601)
- Setting the session variable `enable_load_profile` to `true` makes Stream Load jobs prone to fail. [#34544](https://github.com/StarRocks/starrocks/pull/34544)
- Performing partial updates in column mode onto a Primary Key table causes some tablets of the table to show data inconsistencies between their replicas. [#34555](https://github.com/StarRocks/starrocks/pull/34555)
- The `partition_live_number` property added by using the ALTER TABLE statement does not take effect. [#34842](https://github.com/StarRocks/starrocks/pull/34842)
- FEs fail to start and report the error "failed to load journal type 118". [#34590](https://github.com/StarRocks/starrocks/pull/34590)
- Setting the FE parameter `recover_with_empty_tablet` to `true` may cause FEs to crash. [#33071](https://github.com/StarRocks/starrocks/pull/33071)
- Failures in replaying replica operations may cause FEs to crash. [#32295](https://github.com/StarRocks/starrocks/pull/32295)

### Parameter Change

#### FE/BE Parameters

- Added an FE configuration item [`enable_statistics_collect_profile`](../administration/FE_configuration.md#enable_statistics_collect_profile), which controls whether to generate profiles for statistics queries. The default value is `false`. [#33815](https://github.com/StarRocks/starrocks/pull/33815)
- The FE configuration item [`mysql_server_version`](../administration/FE_configuration.md#mysql_server_version) is now mutable. The new setting can take effect for the current session without requiring an FE restart. [#34033](https://github.com/StarRocks/starrocks/pull/34033)
- Added a BE/CN configuration item [`update_compaction_ratio_threshold`](../administration/BE_configuration.md#update_compaction_ratio_threshold), which controls the maximum proportion of data that a compaction can merge for a Primary Key table in a StarRocks shared-data cluster. The default value is `0.5`. We recommend shrinking this value if a single tablet becomes excessively large. For a StarRocks shared-nothing cluster, the proportion of data that a compaction can merge for a Primary Key table is still automatically adjusted. [#35129](https://github.com/StarRocks/starrocks/pull/35129)

#### System Variables

- Added a session variable `cbo_decimal_cast_string_strict`, which controls how the CBO converts data from the DECIMAL type to the STRING type. If this variable is set to `true`, the logic built in v2.5.x and later versions prevails and the system implements strict conversion (namely, the system truncates the generated string and fills 0s based on the scale length). If this variable is set to `false`, the logic built in versions earlier than v2.5.x prevails and the system processes all valid digits to generate a string. The default value is `true`. [#34208](https://github.com/StarRocks/starrocks/pull/34208)
- Added a session variable `cbo_eq_base_type`, which specifies the data type used for data comparison between DECIMAL-type data and STRING-type data. The default value is `VARCHAR`, and `DECIMAL` is also a valid value. [#34208](https://github.com/StarRocks/starrocks/pull/34208)
- Added a session variable `big_query_profile_second_threshold`. When the session variable [`enable_profile`](../reference/System_variable.md#enable_profile) is set to `false` and the amount of time taken by a query exceeds the threshold specified by the `big_query_profile_second_threshold` variable, a profile is generated for that query. [#33825](https://github.com/StarRocks/starrocks/pull/33825)

## 3.1.4

Release date: November 2, 2023

### New Features

- Supports sort keys for Primary Key tables created in shared-data StarRocks clusters.
- Supports using the str2date function to specify partition expressions for asynchronous materialized views. This helps facilitate incremental updates and query rewrites of asynchronous materialized views created on tables that reside in external catalogs and use the STRING-type data as their partitioning expressions. [#29923](https://github.com/StarRocks/starrocks/pull/29923) [#31964](https://github.com/StarRocks/starrocks/pull/31964)
- Added a new session variable `enable_query_tablet_affinity`, which controls whether to direct multiple queries against the same tablet to a fixed replica. This session variable is set to `false` by default. [#33049](https://github.com/StarRocks/starrocks/pull/33049)
- Added the utility function `is_role_in_session`, which is used to check whether the specified roles are activated in the current session. It supports checking nested roles granted to a user. [#32984](https://github.com/StarRocks/starrocks/pull/32984)
- Supports setting resource group-level query queue, which is controlled by the global variable `enable_group_level_query_queue` (default value: `false`). When the global-level or resource group-level resource consumption reaches a predefined threshold, new queries are placed in queue, and will be run when both the global-level resource consumption and the resource group-level resource consumption fall below their thresholds.
  - Users can set `concurrency_limit` for each resource group to limit the maximum number of concurrent queries allowed per BE.
  - Users can set `max_cpu_cores` for each resource group to limit the maximum CPU consumption allowed per BE.
- Added two parameters, `plan_cpu_cost_range` and `plan_mem_cost_range`, for resource group classifiers.
  - `plan_cpu_cost_range`: the CPU consumption range estimated by the system. The default value `NULL` indicates no limit is imposed.
  - `plan_mem_cost_range`: the memory consumption range estimated by the system. The default value `NULL` indicates no limit is imposed.

### Improvements

- Window functions COVAR_SAMP, COVAR_POP, CORR, VARIANCE, VAR_SAMP, STD, and STDDEV_SAMP now support the ORDER BY clause and Window clause. [#30786](https://github.com/StarRocks/starrocks/pull/30786)
- An error instead of NULL is returned if a decimal overflow occurs during queries on the DECIMAL type data. [#30419](https://github.com/StarRocks/starrocks/pull/30419)
- The number of concurrent queries allowed in a query queue is now managed by the leader FE. Each follower FE notifies of the leader FE when a query starts and finishes. If the number of concurrent queries reaches the global-level or resource group-level `concurrency_limit`, new queries are rejected or placed in queue.

### Bug Fixes

Fixed the following issues:

- Spark or Flink may report data read errors due to inaccurate memory usage statistics. [#30702](https://github.com/StarRocks/starrocks/pull/30702)  [#30751](https://github.com/StarRocks/starrocks/pull/30751)
- Memory usage statistics for Metadata Cache are inaccurate. [#31978](https://github.com/StarRocks/starrocks/pull/31978)
- BEs crash when libcurl is invoked. [#31667](https://github.com/StarRocks/starrocks/pull/31667)
- When StarRocks materialized views created on Hive views are refreshed, an error "java.lang.ClassCastException: com.starrocks.catalog.HiveView cannot be cast to com.starrocks.catalog.HiveMetaStoreTable" is returned. [#31004](https://github.com/StarRocks/starrocks/pull/31004)
- If the ORDER BY clause contains aggregate functions, an error "java.lang.IllegalStateException: null" is returned. [#30108](https://github.com/StarRocks/starrocks/pull/30108)
- In shared-data StarRocks clusters, the information of table keys is not recorded in `information_schema.COLUMNS`. As a result, DELETE operations cannot be performed when data is loaded by using Flink Connector. [#31458](https://github.com/StarRocks/starrocks/pull/31458)
- When data is loaded by using Flink Connector, the load job is suspended unexpectedly if there are highly concurrent load jobs and both the number of HTTP threads and the number of Scan threads have reached their upper limits. [#32251](https://github.com/StarRocks/starrocks/pull/32251)
- When a field of only a few bytes is added, executing SELECT COUNT(*) before the data change finishes returns an error that reads "error: invalid field name". [#33243](https://github.com/StarRocks/starrocks/pull/33243)
- Query results are incorrect after the query cache is enabled. [#32781](https://github.com/StarRocks/starrocks/pull/32781)
- Queries fail during hash joins, causing BEs to crash. [#32219](https://github.com/StarRocks/starrocks/pull/32219)
- `DATA_TYPE` and `COLUMN_TYPE` for BINARY or VARBINARY data types are displayed as `unknown` in the `information_schema.columns` view. [#32678](https://github.com/StarRocks/starrocks/pull/32678)

### Behavior Change

- From v3.1.4 onwards, persistent indexing is enabled by default for Primary Key tables created in new StarRocks clusters (this does not apply to existing StarRocks clusters whose versions are upgraded to v3.1.4 from an earlier version). [#33374](https://github.com/StarRocks/starrocks/pull/33374)
- A new FE parameter `enable_sync_publish` which is set to `true` by default is added. When this parameter is set to `true`, the Publish phase of a data load into a Primary Key table returns the execution result only after the Apply task finishes. As such, the data loaded can be queried immediately after the load job returns a success message. However, setting this parameter to `true` may cause data loads into Primary Key tables to take a longer time. (Before this parameter is added, the Apply task is asynchronous with the Publish phase.) [#27055](https://github.com/StarRocks/starrocks/pull/27055)

## 3.1.3

Release date: September 25, 2023

### New Features

- Primary Key tables created in shared-data StarRocks clusters support index persistence onto local disks in the same way as they do in shared-nothing StarRocks clusters.
- The aggregate function [group_concat](../sql-reference/sql-functions/string-functions/group_concat.md) supports the DISTINCT keyword and the ORDER BY clause. [#28778](https://github.com/StarRocks/starrocks/pull/28778)
- [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md), [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md), [Kafka Connector](../loading/Kafka-connector-starrocks.md), [Flink Connector](../loading/Flink-connector-starrocks.md), and [Spark Connector](../loading/Spark-connector-starrocks.md) support partial updates in column mode on a Primary Key table. [#28288](https://github.com/StarRocks/starrocks/pull/28288)
- Data in partitions can be automatically cooled down over time. (This feature is not supported for [list partitioning](../table_design/list_partitioning.md).) [#29335](https://github.com/StarRocks/starrocks/pull/29335) [#29393](https://github.com/StarRocks/starrocks/pull/29393)

### Improvements

Executing SQL commands with invalid comments now returns results consistent with MySQL. [#30210](https://github.com/StarRocks/starrocks/pull/30210)

### Bug Fixes

Fixed the following issues:

- If the [BITMAP](../sql-reference/sql-statements/data-types/BITMAP.md) or [HLL](../sql-reference/sql-statements/data-types/HLL.md) data type is specified in the WHERE clause of a [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md) statement to be executed, the statement cannot be properly executed. [#28592](https://github.com/StarRocks/starrocks/pull/28592)
- After a follower FE is restarted, CpuCores statistics are not up-to-date, resulting in query performance degradation. [#28472](https://github.com/StarRocks/starrocks/pull/28472) [#30434](https://github.com/StarRocks/starrocks/pull/30434)
- The execution cost of the [to_bitmap()](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) function is incorrectly calculated. As a result, an inappropriate execution plan is selected for the function after materialized views are rewritten. [#29961](https://github.com/StarRocks/starrocks/pull/29961)
- In certain use cases of the shared-data architecture, after a follower FE is restarted, queries submitted to the follower FE return an error that reads "Backend node not found. Check if any backend node is down". [#28615](https://github.com/StarRocks/starrocks/pull/28615)
- If data is continuously loaded into a table that is being altered by using the [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) statement, an error "Tablet is in error state" may be thrown. [#29364](https://github.com/StarRocks/starrocks/pull/29364)
- Modifying the FE dynamic parameter `max_broker_load_job_concurrency` using the `ADMIN SET FRONTEND CONFIG` command does not take effect. [#29964](https://github.com/StarRocks/starrocks/pull/29964) [#29720](https://github.com/StarRocks/starrocks/pull/29720)
- BEs crash if the time unit in the [date_diff()](../sql-reference/sql-functions/date-time-functions/date_diff.md) function is a constant but the dates are not constants. [#29937](https://github.com/StarRocks/starrocks/issues/29937)
- In the shared-data architecture, automatic partitioning does not take effect after asynchronous load is enabled. [#29986](https://github.com/StarRocks/starrocks/issues/29986)
- If users create a Primary Key table by using the [CREATE TABLE LIKE](../sql-reference/sql-statements/data-definition/CREATE_TABLE_LIKE.md) statement, an error `Unexpected exception: Unknown properties: {persistent_index_type=LOCAL}` is thrown. [#30255](https://github.com/StarRocks/starrocks/pull/30255)
- Restoring Primary Key tables causes metadata inconsistency after BEs are restarted. [#30135](https://github.com/StarRocks/starrocks/pull/30135)
- If users load data into a Primary Key table on which truncate operations and queries are concurrently performed, an error "java.lang.NullPointerException" is thrown in certain cases. [#30573](https://github.com/StarRocks/starrocks/pull/30573)
- If predicate expressions are specified in materialized view creation statements, the refresh results of those materialized views are incorrect. [#29904](https://github.com/StarRocks/starrocks/pull/29904)
- After users upgrade their StarRocks cluster to v3.1.2, the storage volume properties of the tables created before the upgrade are reset to `null`. [#30647](https://github.com/StarRocks/starrocks/pull/30647)
- If checkpointing and restoration are concurrently performed on tablet metadata, some tablet replicas will be lost and cannot be retrieved. [#30603](https://github.com/StarRocks/starrocks/pull/30603)
- If users use CloudCanal to load data into table columns that are set to `NOT NULL` but have no default value specified, an error "Unsupported dataFormat value is : \N" is thrown. [#30799](https://github.com/StarRocks/starrocks/pull/30799)

### Behavior Change

- When using the [group_concat](../sql-reference/sql-functions/string-functions/group_concat.md) function, users must use the SEPARATOR keyword to declare the separator.
- The default value of the session variable [`group_concat_max_len`](../reference/System_variable.md#group_concat_max_len) which controls the default maximum length of the string returned by the [group_concat](../sql-reference/sql-functions/string-functions/group_concat.md) function is changed from unlimited to `1024`.

## 3.1.2

Release date: August 25, 2023

### Bug Fixes

Fixed the following issues:

- If a user specifies which database is to be connected by default and the user only has permissions on tables in the database but does not have permissions on the database, an error stating that the user does not have permissions on the database is thrown. [#29767](https://github.com/StarRocks/starrocks/pull/29767)
- The values returned by the RESTful API action `show_data` for cloud-native tables are incorrect. [#29473](https://github.com/StarRocks/starrocks/pull/29473)
- BEs crash if queries are canceled while the [array_agg()](../sql-reference/sql-functions/array-functions/array_agg.md) function is being run. [#29400](https://github.com/StarRocks/starrocks/issues/29400)
- The `Default` field values returned by the [SHOW FULL COLUMNS](../sql-reference/sql-statements/Administration/SHOW_FULL_COLUMNS.md) statement for columns of the [BITMAP](../sql-reference/sql-statements/data-types/BITMAP.md) or [HLL](../sql-reference/sql-statements/data-types/HLL.md) data type are incorrect. [#29510](https://github.com/StarRocks/starrocks/pull/29510)
- If the [array_map()](../sql-reference/sql-functions/array-functions/array_map.md) function in queries involves multiple tables, the queries fail due to pushdown strategy issues. [#29504](https://github.com/StarRocks/starrocks/pull/29504)
- Queries against ORC-formatted files fail because the bugfix ORC-1304 ([apache/orc#1299](https://github.com/apache/orc/pull/1299)) from Apache ORC is not merged. [#29804](https://github.com/StarRocks/starrocks/pull/29804)

### Behavior Change

For a newly deployed StarRocks v3.1 cluster, you must have the USAGE privilege on the destination external catalog if you want to run [SET CATALOG](../sql-reference/sql-statements/data-definition/SET_CATALOG.md) to switch to that catalog. You can use [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant the required privileges.

For a v3.1 cluster upgraded from an earlier version, you can run SET CATALOG with inherited privilege.

## 3.1.1

Release date: August 18, 2023

### New Features

- Supports Azure Blob Storage for [shared-data clusters](../deployment/shared_data/s3.md).
- Supports List partitioning for [shared-data clusters](../deployment/shared_data/s3.md).
- Supports aggregate functions [COVAR_SAMP](../sql-reference/sql-functions/aggregate-functions/covar_samp.md), [COVAR_POP](../sql-reference/sql-functions/aggregate-functions/covar_pop.md), and [CORR](../sql-reference/sql-functions/aggregate-functions/corr.md).
- Supports the following [window functions](../sql-reference/sql-functions/Window_function.md): COVAR_SAMP, COVAR_POP, CORR, VARIANCE, VAR_SAMP, STD, and STDDEV_SAMP.

### Improvements

Supports implicit conversions for all compound predicates and for all expressions in the WHERE clause. You can enable or disable implicit conversions by using the [session variable](../reference/System_variable.md) `enable_strict_type`. The default value of this session variable is `false`.

### Bug Fixes

Fixed the following issues:

- When data is loaded into tables that have multiple replicas, a large number of invalid log records are written if some partitions of the tables are empty.  [#28824](https://github.com/StarRocks/starrocks/issues/28824)
- Inaccurate estimation of average row size causes partial updates in column mode on Primary Key tables to occupy excessively large memory.  [#27485](https://github.com/StarRocks/starrocks/pull/27485)
- If clone operations are triggered on tablets in an ERROR state, disk usage increases. [#28488](https://github.com/StarRocks/starrocks/pull/28488)
- Compaction causes cold data to be written to the local cache. [#28831](https://github.com/StarRocks/starrocks/pull/28831)

## 3.1.0

Release date: August 7, 2023

### New Features

#### Shared-data cluster

- Added support for Primary Key tables, on which persistent indexes cannot be enabled.
- Supports the [AUTO_INCREMENT](../sql-reference/sql-statements/auto_increment.md) column attribute, which enables a globally unique ID for each data row and thus simplifies data management.
- Supports [automatically creating partitions during loading and using partitioning expressions to define partitioning rules](../table_design/expression_partitioning.md), thereby making partition creation easier to use and more flexible.
- Supports [abstraction of storage volumes](../deployment/shared_data/s3.md#use-your-shared-data-starrocks-cluster), in which users can configure storage location and authentication information, in shared-data StarRocks clusters. Users can directly reference an existing storage volume when creating a database or table, making authentication configuration easier.

#### Data Lake analytics

- Supports accessing views created on tables within [Hive catalogs](../data_source/catalog/hive_catalog.md).
- Supports accessing Parquet-formatted Iceberg v2 tables.
- Supports [sinking data to Parquet-formatted Iceberg tables](../data_source/catalog/iceberg_catalog.md#sink-data-to-an-iceberg-table).
- [Preview] Supports accessing data stored in Elasticsearch by using [Elasticsearch catalogs](../data_source/catalog/elasticsearch_catalog.md). This simplifies the creation of Elasticsearch external tables.
- [Preview] Supports performing analytics on streaming data stored in Apache Paimon by using [Paimon catalogs](../data_source/catalog/paimon_catalog.md).

#### Storage engine, data ingestion, and query

- Upgraded automatic partitioning to [expression partitioning](../table_design/expression_partitioning.md). Users only need to use a simple partition expression (either a time function expression or a column expression) to specify a partitioning method at table creation, and StarRocks will automatically create partitions based on the data characteristics and the rule defined in the partition expression during data loading. This method of partition creation is suitable for most scenarios and is more flexible and user-friendly.
- Supports [list partitioning](../table_design/list_partitioning.md). Data is partitioned based on a list of values predefined for a particular column, which can accelerate queries and manage clearly categorized data more efficiently.
- Added a new table named `loads` to the `Information_schema` database. Users can query the results of [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) and [Insert](../sql-reference/sql-statements/data-manipulation/INSERT.md) jobs from the `loads` table.
- Supports logging the unqualified data rows that are filtered out by [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md), [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md), and [Spark Load](../sql-reference/sql-statements/data-manipulation/SPARK_LOAD.md) jobs. Users can use the `log_rejected_record_num` parameter in their load job to specify the maximum number of data rows that can be logged.
- Supports [random bucketing](../table_design/Data_distribution.md#how-to-choose-the-bucketing-columns). With this feature, users do not need to configure bucketing columns at table creation, and StarRocks will randomly distribute the data loaded into it to buckets. Using this feature together with the capability of automatically setting the number of buckets (`BUCKETS`) that StarRocks has provided since v2.5.7, users no longer need to consider bucket configurations, and table creation statements are greatly simplified. In big data and high performance-demanding scenarios, however, we recommend that users continue using hash bucketing, because this way they can use bucket pruning to accelerate queries.
- Supports using the table function FILES() in [INSERT INTO](../loading/InsertInto.md) to directly load the data of Parquet- or ORC-formatted data files stored in AWS S3. The FILES() function can automatically infer the table schema, which relieves the need to create external catalogs or file external tables before data loading and therefore greatly simplifies the data loading process.
- Supports [generated columns](../sql-reference/sql-statements/generated_columns.md). With the generated column feature, StarRocks can automatically generate and store the values of column expressions and automatically rewrite queries to improve query performance.
- Supports loading data from Spark to StarRocks by using [Spark connector](../loading/Spark-connector-starrocks.md). Compared to [Spark Load](../loading/SparkLoad.md), the Spark connector provides more comprehensive capabilities. Users can define a Spark job to perform ETL operations on the data, and the Spark connector serves as the sink in the Spark job.
- Supports loading data into columns of the [MAP](../sql-reference/sql-statements/data-types/Map.md) and [STRUCT](../sql-reference/sql-statements/data-types/STRUCT.md) data types, and supports nesting Fast Decimal values in ARRAY, MAP, and STRUCT.

#### SQL reference

- Added the following storage volume-related statements: [CREATE STORAGE VOLUME](../sql-reference/sql-statements/Administration/CREATE_STORAGE_VOLUME.md), [ALTER STORAGE VOLUME](../sql-reference/sql-statements/Administration/ALTER_STORAGE_VOLUME.md), [DROP STORAGE VOLUME](../sql-reference/sql-statements/Administration/DROP_STORAGE_VOLUME.md), [SET DEFAULT STORAGE VOLUME](../sql-reference/sql-statements/Administration/SET_DEFAULT_STORAGE_VOLUME.md), [DESC STORAGE VOLUME](../sql-reference/sql-statements/Administration/DESC_STORAGE_VOLUME.md), [SHOW STORAGE VOLUMES](../sql-reference/sql-statements/Administration/SHOW_STORAGE_VOLUMES.md).

- Supports altering table comments using [ALTER TABLE](../sql-reference/sql-statements/data-definition/ALTER_TABLE.md). [#21035](https://github.com/StarRocks/starrocks/pull/21035)

- Added the following functions:

  - Struct functions: [struct (row)](../sql-reference/sql-functions/struct-functions/row.md), [named_struct](../sql-reference/sql-functions/struct-functions/named_struct.md)
  - Map functions: [str_to_map](../sql-reference/sql-functions/string-functions/str_to_map.md), [map_concat](../sql-reference/sql-functions/map-functions/map_concat.md), [map_from_arrays](../sql-reference/sql-functions/map-functions/map_from_arrays.md), [element_at](../sql-reference/sql-functions/map-functions/element_at.md), [distinct_map_keys](../sql-reference/sql-functions/map-functions/distinct_map_keys.md), [cardinality](../sql-reference/sql-functions/map-functions/cardinality.md)
  - Higher-order Map functions: [map_filter](../sql-reference/sql-functions/map-functions/map_filter.md), [map_apply](../sql-reference/sql-functions/map-functions/map_apply.md), [transform_keys](../sql-reference/sql-functions/map-functions/transform_keys.md), [transform_values](../sql-reference/sql-functions/map-functions/transform_values.md)
  - Array functions: [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md) supports `ORDER BY`, [array_generate](../sql-reference/sql-functions/array-functions/array_generate.md), [element_at](../sql-reference/sql-functions/array-functions/element_at.md), [cardinality](../sql-reference/sql-functions/array-functions/cardinality.md)
  - Higher-order Array functions: [all_match](../sql-reference/sql-functions/array-functions/all_match.md), [any_match](../sql-reference/sql-functions/array-functions/any_match.md)
  - Aggregate functions: [min_by](../sql-reference/sql-functions/aggregate-functions/min_by.md), [percentile_disc](../sql-reference/sql-functions/aggregate-functions/percentile_disc.md)
  - Table functions: [FILES](../sql-reference/sql-functions/table-functions/files.md), [generate_series](../sql-reference/sql-functions/table-functions/generate_series.md)
  - Date functions: [next_day](../sql-reference/sql-functions/date-time-functions/next_day.md), [previous_day](../sql-reference/sql-functions/date-time-functions/previous_day.md), [last_day](../sql-reference/sql-functions/date-time-functions/last_day.md), [makedate](../sql-reference/sql-functions/date-time-functions/makedate.md), [date_diff](../sql-reference/sql-functions/date-time-functions/date_diff.md)
  - Bitmap functions：[bitmap_subset_limit](../sql-reference/sql-functions/bitmap-functions/bitmap_subset_limit.md), [bitmap_subset_in_range](../sql-reference/sql-functions/bitmap-functions/bitmap_subset_in_range.md)
  - Math functions: [cosine_similarity](../sql-reference/sql-functions/math-functions/cos_similarity.md), [cosine_similarity_norm](../sql-reference/sql-functions/math-functions/cos_similarity_norm.md)

#### Privileges and security

Added [privilege items](../administration/privilege_item.md#storage-volume) related to storage volumes and [privilege items](../administration/privilege_item.md#catalog) related to external catalogs, and supports using [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md) to grant and revoke these privileges.

### Improvements

#### Shared-data cluster

Optimized the data cache in shared-data StarRocks clusters. The optimized data cache allows for specifying the range of hot data. It can also prevent queries against cold data from occupying the local disk cache, thereby ensuring the performance of queries against hot data.

#### Materialized view

- Optimized the creation of an asynchronous materialized view:
  - Supports random bucketing. If users do not specify bucketing columns, StarRocks adopts random bucketing by default.
  - Supports using `ORDER BY` to specify a sort key.
  - Supports specifying attributes such as `colocate_group`, `storage_medium`, and `storage_cooldown_time`.
  - Supports using session variables. Users can configure these variables by using the `properties("session.<variable_name>" = "<value>")` syntax to flexibly adjust view refreshing strategies.
  - Enables the spill feature for all asynchronous materialized views and implements a query timeout duration of 1 hour by default.
  - Supports creating materialized views based on views. This makes materialized views easier to use in data modeling scenarios, because users can flexibly use views and materialized views based on their varying needs to implement layered modeling.
- Optimized query rewrite with asynchronous materialized views:
  - Supports Stale Rewrite, which allows materialized views that are not refreshed within a specified time interval to be used for query rewrite regardless of whether the base tables of the materialized views are updated. Users can specify the time interval by using the `mv_rewrite_staleness_second` property at materialized view creation.
  - Supports rewriting View Delta Join queries against materialized views that are created on Hive catalog tables (a primary key and a foreign key must be defined).
  - Optimized the mechanism for rewriting queries that contain union operations, and supports rewriting queries that contain joins or functions such as COUNT DISTINCT and time_slice.
- Optimized the refreshing of asynchronous materialized views:
  - Optimized the mechanism for refreshing materialized views that are created on Hive catalog tables. StarRocks now can perceive partition-level data changes, and refreshes only the partitions with data changes during each automatic refresh.
  - Supports using the `REFRESH MATERIALIZED VIEW WITH SYNC MODE` syntax to synchronously invoke materialized view refresh tasks.
- Enhanced the use of asynchronous materialized views:
  - Supports using `ALTER MATERIALIZED VIEW {ACTIVE | INACTIVE}` to enable or disable a materialized view. Materialized views that are disabled (in the `INACTIVE` state) cannot be refreshed or used for query rewrite, but can be directly queried.
  - Supports using `ALTER MATERIALIZED VIEW SWAP WITH` to swap two materialized views. Users can create a new materialized view and then perform an atomic swap with an existing materialized view to implement schema changes on the existing materialized view.
- Optimized synchronous materialized views:
  - Supports direct queries against synchronous materialized views using SQL hints `[_SYNC_MV_]`, allowing for walking around issues that some queries cannot be properly rewritten in rare circumstances.
  - Supports more expressions, such as `CASE-WHEN`, `CAST`, and mathematical operations, which make materialized views suitable for more business scenarios.

#### Data Lake analytics

- Optimized metadata caching and access for Iceberg to improve Iceberg data query performance.
- Optimized the data cache to further improve data lake analytics performance.

#### Storage engine, data ingestion, and query

- Announced the general availability of the [spill](../administration/spill_to_disk.md) feature, which supports spilling the intermediate computation results of some blocking operators to disk. With the spill feature enabled, when a query contains aggregate, sort, or join operators, StarRocks can cache the intermediate computation results of the operators to disk to reduce memory consumption, thereby minimizing query failures caused by memory limits.
- Supports pruning on cardinality-preserving joins. If users maintain a large number of tables which are organized in the star schema (for example, SSB) or the snowflake schema (for example, TCP-H) but they query only a small number of these tables, this feature helps prune unnecessary tables to improve the performance of joins.
- Supports partial updates in column mode. Users can enable the column mode when they perform partial updates on Primary Key tables by using the [UPDATE](../sql-reference/sql-statements/data-manipulation/UPDATE.md) statement. The column mode is suitable for updating a small number of columns but a large number of rows, and can improve the updating performance by up to 10 times.
- Optimized the collection of statistics for the CBO. This reduces the impact of statistics collection on data ingestion and increases statistics collection performance.
- Optimized the merge algorithm to increase the overall performance by up to 2 times in permutation scenarios.
- Optimized the query logic to reduce dependency on database locks.
- Dynamic partitioning further supports the partitioning unit to be year. [#28386](https://github.com/StarRocks/starrocks/pull/28386)

#### SQL reference

- Conditional functions case, coalesce, if, ifnull, and nullif support the ARRAY, MAP, STRUCT, and JSON data types.
- The following Array functions support nested types MAP, STRUCT, and ARRAY:
  - array_agg
  - array_contains, array_contains_all, array_contains_any
  - array_slice, array_concat
  - array_length, array_append, array_remove, array_position
  - reverse, array_distinct, array_intersect, arrays_overlap
  - array_sortby
- The following Array functions support the Fast Decimal data type:
  - array_agg
  - array_append, array_remove, array_position, array_contains
  - array_length
  - array_max, array_min, array_sum, array_avg
  - arrays_overlap, array_difference
  - array_slice, array_distinct, array_sort, reverse, array_intersect, array_concat
  - array_sortby, array_contains_all, array_contains_any

### Bug Fixes

Fixed the following issues:

- Requests to reconnect to Kafka for Routine Load jobs cannot be properly processed. [#23477](https://github.com/StarRocks/starrocks/issues/23477)
- For SQL queries that involve multiple tables and contain a `WHERE` clause, if these SQL queries have the same semantics but the order of the tables in each SQL query is different, some of these SQL queries may fail to be rewritten to benefit from the related materialized views. [#22875](https://github.com/StarRocks/starrocks/issues/22875)
- Duplicate records are returned for queries that contain a `GROUP BY` clause. [#19640](https://github.com/StarRocks/starrocks/issues/19640)
- Invoking the lead() or lag() function may cause BE crashes. [#22945](https://github.com/StarRocks/starrocks/issues/22945)
- Rewriting partial partition queries based on materialized views that are created on external catalog tables fail. [#19011](https://github.com/StarRocks/starrocks/issues/19011)
- SQL statements that contain both a backward slash (`\`) and a semicolon (`;`) cannot be properly parsed. [#16552](https://github.com/StarRocks/starrocks/issues/16552)
- A table cannot be truncated if a materialized view created on the table is removed. [#19802](https://github.com/StarRocks/starrocks/issues/19802)

### Behavior Change

- The `storage_cache_ttl` parameter is deleted from the table creation syntax used for shared-data StarRocks clusters. Now the data in the local cache is evicted based on the LRU algorithm.
- The BE configuration items `disable_storage_page_cache` and `alter_tablet_worker_count` and the FE configuration item `lake_compaction_max_tasks` are changed from immutable parameters to mutable parameters.
- The default value of the BE configuration item `block_cache_checksum_enable` is changed from `true` to `false`.
- The default value of the BE configuration item `enable_new_load_on_memory_limit_exceeded` is changed from `false` to `true`.
- The default value of the FE configuration item `max_running_txn_num_per_db` is changed from `100` to `1000`.
- The default value of the FE configuration item `http_max_header_size` is changed from `8192` to `32768`.
- The default value of the FE configuration item `tablet_create_timeout_second` is changed from `1` to `10`.
- The default value of the FE configuration item `max_routine_load_task_num_per_be` is changed from `5` to `16`, and error information will be returned if a large number of Routine Load tasks are created.
- The FE configuration item `quorom_publish_wait_time_ms` is renamed as `quorum_publish_wait_time_ms`, and the FE configuration item `async_load_task_pool_size` is renamed as `max_broker_load_job_concurrency`.
- The BE configuration item `routine_load_thread_pool_size` is deprecated. Now the routine load thread pool size per BE node is controlled only by the FE configuration item `max_routine_load_task_num_per_be`.
- The BE configuration item `txn_commit_rpc_timeout_ms` and the system variable `tx_visible_wait_timeout` are deprecated.
- The FE configuration items `max_broker_concurrency` and `load_parallel_instance_num` are deprecated.
- The FE configuration item `max_routine_load_job_num` is deprecated. Now StarRocks dynamically infers the maximum number of Routine Load tasks supported by each individual BE node based on the `max_routine_load_task_num_per_be` parameter and provides suggestions on task failures.
- The CN configuration item `thrift_port` is renamed as `be_port`.
- Two new Routine Load job properties, `task_consume_second` and `task_timeout_second`, are added to control the maximum amount of time to consume data and the timeout duration for individual load tasks within a Routine Load job, making job adjustment more flexible. If users do not specify these two properties in their Routine Load job, the FE configuration items `routine_load_task_consume_second` and `routine_load_task_timeout_second` prevail.
- The session variable `enable_resource_group` is deprecated because the [Resource Group](../administration/resource_group.md) feature is enabled by default since v3.1.0.
- Two new reserved keywords, COMPACTION and TEXT, are added.
