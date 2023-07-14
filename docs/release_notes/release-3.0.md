# StarRocks version 3.0

## 3.0.3

Release date: June 28, 2023

### Improvements

- Metadata synchronization of StarRocks external tables has been changed to occur during data loading. [#24739](https://github.com/StarRocks/starrocks/pull/24739)
- Users can specify partitions when they run INSERT OVERWRITE on tables whose partitions are automatically created. For more information, see [Automatic partitioning](../table_design/automatic_partitioning.md). [#25005](https://github.com/StarRocks/starrocks/pull/25005)
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
- Optimized the [auto tablet distribution policy](../table_design/Data_distribution.md#determine-the-number-of-tablets) for tables. [#24543](https://github.com/StarRocks/starrocks/pull/24543)
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
- Added `load_tracking_logs` to [Information Schema](../administration/information_schema.md#loadtrackinglogs) to record recent loading errors.
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

- **Decouple storage and compute.** StarRocks now supports data persistence into S3-compatible object storage, enhancing resource isolation, reducing storage costs, and making compute resources more scalable. Local disks are used as hot data cache for boosting query performance. The query performance of the new shared-data architecture is comparable to the classic architecture (shared-nothing) when local disk cache is hit. For more information, see [Deploy and use shared-data StarRocks](../deployment/deploy_shared_data.md).

#### Storage engine and data ingestion

- The [AUTO_INCREMENT](../sql-reference/sql-statements/auto_increment.md) attribute is supported to provide globally unique IDs, which simplifies data management.
- [Automatic partitioning and partitioning expressions](../table_design/automatic_partitioning.md) are supported, which makes partition creation easier to use and more flexible.
- Primary Key tables support more complete [UPDATE](../sql-reference/sql-statements/data-manipulation/UPDATE.md) and [DELETE](../sql-reference/sql-statements/data-manipulation/DELETE.md#primary-key-tables) syntax, including the use of CTEs and references to multiple tables.
- Added Load Profile for Broker Load and INSERT INTO jobs. You can view the details of a load job by querying the load profile. The usage is the same as [Analyze query profile](../administration/query_profile.md).

#### Data Lake Analytics

- [Preview] Supports Presto/Trino compatible dialect. Presto/Trino's SQL can be automatically rewritten into StarRocks' SQL pattern. For more information, see [the system variable](../reference/System_variable.md) `sql_dialect`.
- [Preview] Supports [JDBC catalogs](../data_source/catalog/jdbc_catalog.md).
- Supports using [SET CATALOG](../sql-reference/sql-statements/data-definition/SET%20CATALOG.md) to manually switch between catalogs in the current session.

#### Privileges and security

- Provides a new privilege system with full RBAC functionalities, supporting role inheritance and default roles. For more information, see [Overview of privileges](../administration/privilege_overview.md).
- Provides more privilege management objects and more fine-grained privileges. For more information, see [Privileges supported by StarRocks](../administration/privilege_item.md).

#### Query engine

<!-- - [Preview] Supports operator **spilling** for large queries, which can use disk space to ensure stable running of queries in case of insufficient memory. -->
- Allows more queries on joined tables to benefit from the [query cache](../using_starrocks/query_cache.md). For example, the query cache now supports Broadcast Join and Bucket Shuffle Join.
- Supports [Global UDFs](../sql-reference/sql-functions/JAVA_UDF.md).
- Dynamic adaptive parallelism: StarRocks can automatically adjust the `pipeline_dop` parameter for query concurrency.

#### SQL reference

- Added the following privilege-related SQL statements: [SET DEFAULT ROLE](../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md), [SET ROLE](../sql-reference/sql-statements/account-management/SET%20ROLE.md), [SHOW ROLES](../sql-reference/sql-statements/account-management/SHOW%20ROLES.md), and [SHOW USERS](../sql-reference/sql-statements/account-management/SHOW%20USERS.md).
- Added the following semi-structured data analysis functions: [map_apply](../sql-reference/sql-functions/map-functions/map_apply.md), [map_from_arrays](../sql-reference/sql-functions/map-functions/map_from_arrays.md), [map_filter](../sql-reference/sql-functions/map-functions/map_filter.md), [transform_keys](../sql-reference/sql-functions/map-functions/transform_keys.md), and [transform_values](../sql-reference/sql-functions/map-functions/transform_values.md).
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

- Supports more CSV parameters for data ingestion, including SKIP_HEADER, TRIM_SPACE, ENCLOSE, and ESCAPE. See [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md), [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md), and [ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE%20ROUTINE%20LOAD.md).
- The primary key and sort key are decoupled in [Primary Key tables](../table_design/table_types/primary_key_table.md). The sort key can be separately specified in `ORDER BY` when you create a table.
- Optimized the memory usage of data ingestion into Primary Key tables in scenarios such as large-volume ingestion, partial updates, and persistent primary indexes.
- Supports creating asynchronous INSERT tasks. For more information, see [INSERT](../loading/InsertInto.md#load-data-asynchronously-using-insert) and [SUBMIT TASK](../sql-reference/sql-statements/data-manipulation/SUBMIT%20TASK.md). [#20609](https://github.com/StarRocks/starrocks/issues/20609)

#### Materialized view

- Optimized the rewriting capabilities of [materialized views](../using_starrocks/Materialized_view.md), including:
  - Supports rewrite of View Delta Join, Outer Join, and Cross Join.
  - Optimized SQL rewrite of Union with partition.
- Improved materialized view building capabilities: supporting CTE, select *, and Union.
- Optimized the information returned by [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md).
- Supports adding MV partitions in batches, which improves the efficiency of partition addition during materialized view building. [#21167](https://github.com/StarRocks/starrocks/pull/21167)

#### Query engine

- All operators are supported in the pipeline engine. Non-pipeline code will be removed in later versions.
- Improved [Big Query Positioning](../administration/monitor_manage_big_queries.md) and added big query log. [SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW%20PROCESSLIST.md) supports viewing CPU and memory information.
- Optimized Outer Join Reorder.
- Optimized error messages in the SQL parsing stage, providing more accurate error positioning and clearer error messages.

#### Data Lake Analytics

- Optimized metadata statistics collection.
- Supports using [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20TABLE.md) to view the creation statements of the tables that are managed by an external catalog and are stored in Apache Hive™, Apache Iceberg, Apache Hudi, or Delta Lake.

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
- Renamed SHOW MATERIALIZED VIEW as [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md).
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

After a downgrade, run [ALTER SYSTEM CREATE IMAGE](../sql-reference/sql-statements/Administration/ALTER%20SYSTEM.md) to create a new image and wait for the new image to be synchronized to all follower FEs. If you do not run this command, some of the downgrade operations may fail. This command is supported from 2.5.3 and later.

For details about the differences between the privilege system of v2.5 and v3.0, see "Upgrade notes" in [Privileges supported by StarRocks](../administration/privilege_item.md).
