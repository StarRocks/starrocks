# StarRocks version 3.0

## 3.0.0-rc02

Release date: April 13, 2023

### Improvements

- Updated Docker image and the related [Docker deployment document](../quick_start/deploy_with_docker.md) for version 3.0. ([#20623](https://github.com/StarRocks/starrocks/pull/20623) [#21021](https://github.com/StarRocks/starrocks/pull/21021))
- Supports creating asynchronous INSERT tasks. For more information, see [INSERT](../loading/InsertInto.md#load-data-asynchronously-using-insert) and [SUBMIT TASK](../sql-reference/sql-statements/data-manipulation/SUBMIT%20TASK.md). ([#20609](https://github.com/StarRocks/starrocks/issues/20609))
- Supports adding MV partitions in batches, which improves the efficiency of partition addition during materialized view building. ([#21167](https://github.com/StarRocks/starrocks/pull/21167))

### Bug Fixes

Fixed the following issues:

- FEs fail to start when a VARCHAR column is used as the partitioning column of a materialized view. ([#19366](https://github.com/StarRocks/starrocks/issues/19366))
- Window functions [LEAD](../sql-reference/sql-functions/Window_function.md#lead) and [LAG](../sql-reference/sql-functions/Window_function.md#lag) incorrectly handle IGNORE NULLS. ([#21001](https://github.com/StarRocks/starrocks/pull/21001))
- Adding temporary partitions conflicts with automatic partition creation. ([#21222](https://github.com/StarRocks/starrocks/issues/21222))

## 3.0.0-rc01

Release date: March 31, 2023

### New Features

#### System architecture

- **Decouple storage and compute.** StarRocks now supports data persistence into S3-compatible object storage, enhancing resource isolation, reducing storage costs, and making compute resources more scalable. Local disks are used as hot data cache for boosting query performance. The query performance of the new shared-data architecture is comparable to the classic architecture (shared-nothing) when local cache is hit. For more information, see [Deploy and use shared-data StarRocks](../administration/deploy_shared_data.md).

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
- Allows more queries on joined tables to benefit from the [query cache](../using_starrocks/query_cache.md). For example, the query cache now supports aggregate queries on multiple tables that are joined by using  bucket shuffle joins and broadcast joins.
- Supports [Global UDFs](../sql-reference/sql-functions/JAVA_UDF.md).
- Dynamic adaptive parallelism: StarRocks can automatically adjust the `pipeline_dop` parameter for query concurrency.

#### SQL functions

- Added [map_apply](../sql-reference/sql-functions/map-functions/map_apply.md), [map_from_arrays](../sql-reference/sql-functions/map-functions/map_from_arrays.md), [map_filter](../sql-reference/sql-functions/map-functions/map_filter.md), [transform_keys](../sql-reference/sql-functions/map-functions/transform_keys.md), and [transform_values](../sql-reference/sql-functions/map-functions/transform_values.md) for semi-structured data analysis.
- [array_agg](../sql-reference/sql-functions/array-functions/array_agg.md) supports ORDER BY.
- Added the string function [replace](../sql-reference/sql-functions/string-functions/replace.md).

### Improvements

#### Storage engine and data ingestion

- Supports more CSV parameters for data ingestion, including SKIP_HEADER, TRIM_SPACE, ENCLOSE, and ESCAPE. See [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md), [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER%20LOAD.md), and [ROUTINE LOAD](../sql-reference/sql-statements/data-manipulation/CREATE%20ROUTINE%20LOAD.md).
- The primary key and sort key are decoupled in [Primary Key tables](../table_design/table_types/primary_key_table.md). The sort key can be separately specified in `ORDER BY`.
- Optimized the memory usage of data ingestion into Primary Key tables in scenarios such as large-volume ingestion, partial updates, and persistent primary indexes.

#### Materialized view

- Optimized the rewriting capabilities of [materialized views](../using_starrocks/Materialized_view.md), including:
  - Supports rewrite of View Delta Join, Outer Join, and Cross Join.
  - Optimized SQL rewrite of Union with partition.
- Improved materialized view building capabilities: supporting CTE, select *, and Union.
- Optimized the information returned by [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md).

#### Query engine

- All operators are supported in the pipeline engine. Non-pipeline code will be removed in later versions.
- Improved [Big Query Positioning](../administration/monitor_manage_big_queries.md) and added big query log. [SHOW PROCESSLIST](../sql-reference/sql-statements/Administration/SHOW%20PROCESSLIST.md) supports viewing CPU and memory information.
- Optimized Outer Join Reorder.
- Optimized error messages in the SQL parsing stage, providing more accurate error positioning and clearer error messages.

#### Data Lake Analytics

- Optimized metadata statistics collection.
- Supports using [SHOW CREATE TABLE](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20TABLE.md) to query the schema information of an external table and using [SHOW CREATE CATALOG](../sql-reference/sql-statements/data-manipulation/SHOW%20CREATE%20CATALOG.md) to query the creation statement of an external catalog.

### Bug Fixes

- Some URLs in the license header of StarRocks' source file cannot be accessed. #[2224](https://github.com/StarRocks/starrocks/issues/2224)
- An unknown error is returned during SELECT queries.  #[19731](https://github.com/StarRocks/starrocks/issues/19731)
- Supports SHOW/SET CHARACTER. #[17480](https://github.com/StarRocks/starrocks/issues/17480)
- When the loaded data exceeds the field length supported by StarRocks, the error message returned is not correct. #[14](https://github.com/StarRocks/DataX/issues/14)
- Supports `show full fields from 'table'`. #[17233](https://github.com/StarRocks/starrocks/issues/17233)
- Partition pruning causes MV rewrites to fail. #[14641](https://github.com/StarRocks/starrocks/issues/14641)
- MV rewrite fails when the CREATE MATERIALIZED VIEW statement contains `count(distinct)` and `count(distinct)` is applied to the DISTRIBUTED BY column. #[16558](https://github.com/StarRocks/starrocks/issues/16558)

### Behavior Change

- The new role-based access control (RBAC) system supports the previous privileges and roles. However, the syntax of related statements such as [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md) is changed.
- Renamed SHOW MATERIALIZED VIEW as [SHOW MATERIALIZED VIEWS](../sql-reference/sql-statements/data-manipulation/SHOW%20MATERIALIZED%20VIEW.md).

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
