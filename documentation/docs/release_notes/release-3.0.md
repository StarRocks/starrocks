---
displayed_sidebar: "English"
---

# StarRocks version 3.0

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

- When using the [group_concat](../sql-reference/sql-functions/string-functions/group_concat.md) function, you must use the SEPARATOR keyword to declare the separator.

### New Features

- The aggregate function [group_concat](../sql-reference/sql-functions/string-functions/group_concat.md) supports the DISTINCT keyword and the ORDER BY clause. [#28778](https://github.com/StarRocks/starrocks/pull/28778)
- Data in partitions can be automatically cooled down over time. (This feature is not supported for [list partitioning](../table_design/list_partitioning.md).) [#29335](https://github.com/StarRocks/starrocks/pull/29335) [#29393](https://github.com/StarRocks/starrocks/pull/29393)

### Improvements

- Supports implicit conversions for all compound predicates and for all expressions in the WHERE clause. You can enable or disable implicit conversions by using the [session variable](../sql-reference/System_variable.md) `enable_strict_type`. The default value of this session variable is `false`. [#21870](https://github.com/StarRocks/starrocks/pull/21870)
- Unifies the logic between FEs and BEs in converting strings to integers. [#29969](https://github.com/StarRocks/starrocks/pull/29969)

### Bug Fixes

- If `enable_orc_late_materialization` is set to `true`, an unexpected result is returned when a Hive catalog is used to query STRUCT-type data in ORC files. [#27971](https://github.com/StarRocks/starrocks/pull/27971)
- During data queries through Hive Catalog, if a partitioning column and an OR operator are specified in the WHERE clause, the query result is incorrect. [#28876](https://github.com/StarRocks/starrocks/pull/28876)
- The values returned by the RESTful API action `show_data` for cloud-native tables are incorrect. [#29473](https://github.com/StarRocks/starrocks/pull/29473)
- If the [shared-data cluster](../deployment/shared_data/azure.md) stores data in Azure Blob Storage and a table is created, the FE fails to start after the cluster is rolled back to version 3.0. [#29433](https://github.com/StarRocks/starrocks/pull/29433)
- A user has no permission when querying a table in the Iceberg catalog even if the user is granted permission on that table. [#29173](https://github.com/StarRocks/starrocks/pull/29173)
- The `Default` field values returned by the [SHOW FULL COLUMNS](../sql-reference/sql-statements/Administration/SHOW_FULL_COLUMNS.md) statement for columns of the [BITMAP](../sql-reference/sql-statements/data-types/BITMAP.md) or [HLL](../sql-reference/sql-statements/data-types/HLL.md) data type are incorrect. [#29510](https://github.com/StarRocks/starrocks/pull/29510)
- Modifying the FE dynamic parameter `max_broker_load_job_concurrency` using the `ADMIN SET FRONTEND CONFIG` command does not take effect.
- The FE may fail to start when a materialized view is being refreshed while its refresh strategy is being modified. [#29964](https://github.com/StarRocks/starrocks/pull/29964) [#29720](https://github.com/StarRocks/starrocks/pull/29720)
- The error `unknown error` is returned when `select count(distinct(int+double)) from table_name` is executed. [#29691](https://github.com/StarRocks/starrocks/pull/29691)
- After a Primary Key table is restored, metadata errors occur and cause metadata inconsistencies occur if a BE is restarted. [#30135](https://github.com/StarRocks/starrocks/pull/30135)

## 3.0.5

Release date: August 16, 2023

### New Features

- Supports aggregate functions [COVAR_SAMP](../sql-reference/sql-functions/aggregate-functions/covar_samp.md), [COVAR_POP](../sql-reference/sql-functions/aggregate-functions/covar_pop.md), and [CORR](../sql-reference/sql-functions/aggregate-functions/corr.md).
- Supports the following [window functions](../sql-reference/sql-functions/Window_function.md): COVAR_SAMP, COVAR_POP, CORR, VARIANCE, VAR_SAMP, STD, and STDDEV_SAMP.

### Improvements

- Added more prompts in the error message `xxx too many versions xxx`. [#28397](https://github.com/StarRocks/starrocks/pull/28397)
- Dynamic partitioning further supports the partitioning unit to be year. [#28386](https://github.com/StarRocks/starrocks/pull/28386)
- The partitioning field is case-insensitive when expression partitioning is used at table creation and [INSERT OVERWRITE is used to overwrite data in a specific partition](../table_design/expression_partitioning.md#load-data-into-partitions). [#28309](https://github.com/StarRocks/starrocks/pull/28309)

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
- [When the SQL dialect (`sql_dialect`) is set to `trino`](../sql-reference/System_variable.md), table aliases are not case-sensitive. [#26094](https://github.com/StarRocks/starrocks/pull/26094) [#25282](https://github.com/StarRocks/starrocks/pull/25282)
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
- Optimized the [auto tablet distribution policy](../table_design/Data_distribution.md#determine-the-number-of-buckets) for tables. [#24543](https://github.com/StarRocks/starrocks/pull/24543)
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

- [Preview] Supports spilling intermediate computation results of large operators to disks to reduce the memory consumption of large operators. For more information, see [Spill to disk](../administration/spill_to_disk.md).
- [Routine Load](../loading/RoutineLoad.md#load-avro-format-data) supports loading Avro data.
- Supports [Microsoft Azure Storage](../integrations/authenticate_to_azure_storage.md) (including Azure Blob Storage and Azure Data Lake Storage).

### Improvements

- Shared-data clusters support using StarRocks external tables to synchronize data with another StarRocks cluster.
- Added `load_tracking_logs` to [Information Schema](../administration/information_schema.md#load_tracking_logs) to record recent loading errors.
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

- **Decouple storage and compute.** StarRocks now supports data persistence into S3-compatible object storage, enhancing resource isolation, reducing storage costs, and making compute resources more scalable. Local disks are used as hot data cache for boosting query performance. The query performance of the new shared-data architecture is comparable to the classic architecture (shared-nothing) when local disk cache is hit. For more information, see [Deploy and use shared-data StarRocks](../deployment/shared_data/s3.md).

#### Storage engine and data ingestion

- The [AUTO_INCREMENT](../sql-reference/sql-statements/auto_increment.md) attribute is supported to provide globally unique IDs, which simplifies data management.
- [Automatic partitioning and partitioning expressions](../table_design/dynamic_partitioning.md) are supported, which makes partition creation easier to use and more flexible.
- Primary Key tables support more complete [UPDATE](../sql-reference/sql-statements/data-manipulation/UPDATE.md) and [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md#primary-key-tables) syntax, including the use of CTEs and references to multiple tables.
- Added Load Profile for Broker Load and INSERT INTO jobs. You can view the details of a load job by querying the load profile. The usage is the same as [Analyze query profile](../administration/query_profile.md).

#### Data Lake Analytics

- [Preview] Supports Presto/Trino compatible dialect. Presto/Trino's SQL can be automatically rewritten into StarRocks' SQL pattern. For more information, see [the system variable](../sql-reference/System_variable.md) `sql_dialect`.
- [Preview] Supports [JDBC catalogs](../data_source/catalog/jdbc_catalog.md).
- Supports using [SET CATALOG](../sql-reference/sql-statements/data-definition/SET_CATALOG.md) to manually switch between catalogs in the current session.

#### Privileges and security

- Provides a new privilege system with full RBAC functionalities, supporting role inheritance and default roles. For more information, see [Overview of privileges](../administration/privilege_overview.md).
- Provides more privilege management objects and more fine-grained privileges. For more information, see [Privileges supported by StarRocks](../administration/privilege_item.md).

#### Query engine

<!-- - [Preview] Supports operator **spilling** for large queries, which can use disk space to ensure stable running of queries in case of insufficient memory. -->
- Allows more queries on joined tables to benefit from the [query cache](../using_starrocks/query_cache.md). For example, the query cache now supports Broadcast Join and Bucket Shuffle Join.
- Supports [Global UDFs](../sql-reference/sql-functions/JAVA_UDF.md).
- Dynamic adaptive parallelism: StarRocks can automatically adjust the `pipeline_dop` parameter for query concurrency.

#### SQL reference

- Added the following privilege-related SQL statements: [SET DEFAULT ROLE](../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md), [SET ROLE](../sql-reference/sql-statements/account-management/SET_ROLE.md), [SHOW ROLES](../sql-reference/sql-statements/account-management/SHOW_ROLES.md), and [SHOW USERS](../sql-reference/sql-statements/account-management/SHOW_USERS.md).
- Added the following semi-structured data analysis functions: [map_apply](../sql-reference/sql-functions/map-functions/map_apply.md), [map_from_arrays](../sql-reference/sql-functions/map-functions/map_from_arrays.md).
- [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md) supports ORDER BY.
- Window functions [lead](../sql-reference/sql-functions/Window_function.md#lead) and [lag](../sql-reference/sql-functions/Window_function.md#lag) support IGNORE NULLS.
- Added string functions [replace](../sql-reference/sql-functions/string-functions/replace.md), [hex_decode_binary](../sql-reference/sql-functions/string-functions/hex_decode_binary.md), and [hex_decode_string()](../sql-reference/sql-functions/string-functions/hex_decode_string.md).
- Added encryption functions [base64_decode_binary](../sql-reference/sql-functions/crytographic-functions/base64_decode_binary.md) and [base64_decode_string](../sql-reference/sql-functions/crytographic-functions/base64_decode_string.md).
- Added math functions [sinh](../sql-reference/sql-functions/math-functions/sinh.md), [cosh](../sql-reference/sql-functions/math-functions/cosh.md), and [tanh](../sql-reference/sql-functions/math-functions/tanh.md).
- Added utility function [current_role](../sql-reference/sql-functions/utility-functions/current_role.md).

### Improvements

#### Deployment

- Updated Docker image and the related [Docker deployment document](../quick_start/deploy_with_docker.md) for version 3.0. [#20623](https://github.com/StarRocks/starrocks/pull/20623) [#21021](https://github.com/StarRocks/starrocks/pull/21021)

#### Storage engine and data ingestion

- Supports more CSV parameters for data ingestion, including SKIP_HEADER, TRIM_SPACE, ENCLOSE, and ESCAPE. See [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md), [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md), and [ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE_ROUTINE_LOAD.md).
- The primary key and sort key are decoupled in [Primary Key tables](../table_design/table_types/primary_key_table.md). The sort key can be separately specified in `ORDER BY` when you create a table.
- Optimized the memory usage of data ingestion into Primary Key tables in scenarios such as large-volume ingestion, partial updates, and persistent primary indexes.
- Supports creating asynchronous INSERT tasks. For more information, see [INSERT](../loading/InsertInto.md#load-data-asynchronously-using-insert) and [SUBMIT TASK](../sql-reference/sql-statements/data-manipulation/SUBMIT_TASK.md). [#20609](https://github.com/StarRocks/starrocks/issues/20609)

#### Materialized view

- Optimized the rewriting capabilities of [materialized views](../using_starrocks/Materialized_view.md), including:
  - Supports rewrite of View Delta Join, Outer Join, and Cross Join.
  - Optimized SQL rewrite of Union with partition.
- Improved materialized view building capabilities: supporting CTE, select *, and Union.
- Optimized the information returned by [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md).
- Supports adding MV partitions in batches, which improves the efficiency of partition addition during materialized view building. [#21167](https://github.com/StarRocks/starrocks/pull/21167)

#### Query engine

- All operators are supported in the pipeline engine. Non-pipeline code will be removed in later versions.
- Improved [Big Query Positioning](../administration/monitor_manage_big_queries.md) and added big query log. [SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW_PROCESSLIST.md) supports viewing CPU and memory information.
- Optimized Outer Join Reorder.
- Optimized error messages in the SQL parsing stage, providing more accurate error positioning and clearer error messages.

#### Data Lake Analytics

- Optimized metadata statistics collection.
- Supports using [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md) to view the creation statements of the tables that are managed by an external catalog and are stored in Apache Hive™, Apache Iceberg, Apache Hudi, or Delta Lake.

### Bug Fixes

- Some URLs in the license header of StarRocks' source file cannot be accessed. [#2224](https://github.com/StarRocks/starrocks/issues/2224)
- An unknown error is returned during SELECT queries. [#19731](https://github.com/StarRocks/starrocks/issues/19731)
- Supports SHOW/SET CHARACTER. [#17480](https://github.com/StarRocks/starrocks/issues/17480)
- When the loaded data exceeds the field length supported by StarRocks, the error message returned is not correct. [#14](https://github.com/StarRocks/DataX/issues/14)
- Supports `show full fields from 'table'`. [#17233](https://github.com/StarRocks/starrocks/issues/17233)
- Partition pruning causes MV rewrites to fail. [#14641](https://github.com/StarRocks/starrocks/issues/14641)
- MV rewrite fails when the CREATE MATERIALIZED VIEW statement contains `count(distinct)` and `count(distinct)` is applied to the DISTRIBUTED BY column. [#16558](https://github.com/StarRocks/starrocks/issues/16558)
- FEs fail to start when a VARCHAR column is used as the partitioning column of a materialized view. [#19366](https://github.com/StarRocks/starrocks/issues/19366)
- Window functions [LEAD](../sql-reference/sql-functions/Window_function.md#lead) and [LAG](../sql-reference/sql-functions/Window_function.md#lag) incorrectly handle IGNORE NULLS. [#21001](https://github.com/StarRocks/starrocks/pull/21001)
- Adding temporary partitions conflicts with automatic partition creation. [#21222](https://github.com/StarRocks/starrocks/issues/21222)

### Behavior Change

- The new role-based access control (RBAC) system supports the previous privileges and roles. However, the syntax of related statements such as [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md) is changed.
- Renamed SHOW MATERIALIZED VIEW as [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW_MATERIALIZED_VIEW.md).
- Added the following [Reserved keywords](../sql-reference/sql-statements/keywords.md): AUTO_INCREMENT, CURRENT_ROLE, DEFERRED, ENCLOSE, ESCAPE, IMMEDIATE, PRIVILEGES, SKIP_HEADER, TRIM_SPACE, VARBINARY.

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

After a downgrade, run [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER_SYSTEM.md) to create a new image and wait for the new image to be synchronized to all follower FEs. If you do not run this command, some of the downgrade operations may fail. This command is supported from 2.5.3 and later.

For details about the differences between the privilege system of v2.5 and v3.0, see "Upgrade notes" in [Privileges supported by StarRocks](../administration/privilege_item.md).
