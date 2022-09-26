# StarRocks version 2.4

## 2.4.0 RC

Release date: September 17th, 2022

### New Features

- Supports creating a materialized view based on multiple base tables to accelerate queries with JOIN operations. For more information, see [Materialized View](../using_starrocks/Materialized_view.md).

- Supports overwriting data via the INSERT OVERWRITE statement. For more information, see [Load data using INSERT](../loading/InsertInto.md).

- [Preview] Provides stateless Compute Nodes (CN) that can be horizontally scaled. You can use StarRocks Operator to deploy CN into your Kubernetes (K8s) cluster to achieve automatic horizontal scaling. For more information, see [Deploy and manage CN on Kubernetes with StarRocks Operator](../administration/k8s_operator_cn.md).

- Outer Join supports non-equi joins in which join items are related by comparison operators including `<`, `<=`, `>`, `>=`, and `<>`. For more information, see [SELECT](../sql-reference/sql-statements/data-manipulation/SELECT.md).

- Supports creating Iceberg catalogs and Hudi catalogs, which allow direct queries on data from Apache Iceberg and Apache Hudi. For more information, see [Iceberg catalog](../using_starrocks/catalog/iceberg_catalog.md) and [Hudi catalog](../using_starrocks/catalog/hudi_catalog.md).

- Supports querying ARRAY-type columns from Apache Hive™ tables in CSV format. For more information, see [External table](../using_starrocks/External_table.md).

- Supports viewing the schema of external data via the DESC statement. For more information, see [DESC](../sql-reference/sql-statements/Utility/DESCRIBE.md).

- Supports granting a specific role or IMPERSONATE permission to a user via the GRANT statement and revoking them via the REVOKE statement, and supports executing an SQL statement with IMPERSONATE permission via the EXECUTE AS statement. For more information, see [GRANT](../sql-reference/sql-statements/account-management/GRANT.md), [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md), and [EXECUTE AS](../sql-reference/sql-statements/account-management/EXECUTE%20AS.md).

- flink-connector-starrocks supports Primary Key model partial update. For more information, see [Load data by using flink-connector-starrocks](../loading/Flink-connector-starrocks.md).

- Provides the following new functions:

  - array_contains_all: checks whether a specific array is a subset of another. For more information, see [array_contains_all](../sql-reference/sql-functions/array-functions/array_contains_all.md).
  - percentile_cont: calculates the percentile value with linear interpolation. For more information, see [percentile_cont](../sql-reference/sql-functions/aggregate-functions/percentile_cont.md).

### Improvements

- The Primary Key model supports flushing VARCHAR-type primary key indexes to disks. From version 2.4.0, the Primary Key model supports the same data types for primary key indexes regardless of whether persistent primary key index is turned on or not.

- Optimized the query performance on external tables.

  - Supports late materialization during queries on external tables in Parquet format to optimize the query performance on data lakes with small-scale filtering involved.
  - Small I/O operations can be merged to reduce the delay for querying data lakes, thereby improving the query performance on external tables.

- Optimized the performance of window functions.

- Optimized the performance of Cross Join by supporting predicate pushdown.

- Histograms are added to CBO statistics. Full statistics collection is further optimized. For more information, see [Gather CBO statistics](../using_starrocks/Cost_based_optimizer.md).

- Adaptive multi-threading is enabled for tablet scanning to reduce the dependency of scanning performance on the tablet number. As a result, you can set the number of buckets more easily. For more information, see [Determine the number of buckets](../table_design/Data_distribution.md#how-to-determine-the-number-of-buckets).

- Supports querying compressed TXT files in Apache Hive.

- Adjusted the mechanisms of default PageCache size calculation and memory consistency check to avoid OOM issues during multi-instance deployments.

- Functions:

  - You can use COUNT DISTINCT over multiple columns to calculate the number of distinct column combinations. For more information, see [count](../sql-reference/sql-functions/aggregate-functions/count.md).
  - Window functions min() and max() support sliding windows. For more information, see [Window functions](../using_starrocks/Window_function.md).
  - Optimized the performance of the window_funnel function. For more information, see [window_funnel](../sql-reference/sql-functions/aggregate-functions/window_funnel.md).

### Bug Fixes

The following bugs are fixed:

- DECIMAL data types returned by DESC are different from those specified in the CREATE TABLE statement. [#7309](https://github.com/StarRocks/starrocks/pull/7309)

- FE metadata management issues that affect the stability of FEs. [#6685](https://github.com/StarRocks/starrocks/pull/6685) [#9445](https://github.com/StarRocks/starrocks/pull/9445) [#7974](https://github.com/StarRocks/starrocks/pull/7974) [#7455](https://github.com/StarRocks/starrocks/pull/7455)

- Data load-related issues:

  - Broke Load fails when ARRAY columns are specified. [#9158](https://github.com/StarRocks/starrocks/pull/9158)
  - Replicas are inconsistent after data is loaded to a non-Duplicate Key table via Broker Load. [#8714](https://github.com/StarRocks/starrocks/pull/8714)
  - Executing ALTER ROUTINE LOAD raises NPE. [#7804](https://github.com/StarRocks/starrocks/pull/7804)

- Data Lake analytics-related issues:

  - Queries on Parquet data in Hive external tables fail. [#7413](https://github.com/StarRocks/starrocks/pull/7413) [#7482](https://github.com/StarRocks/starrocks/pull/7482) [#7624](https://github.com/StarRocks/starrocks/pull/7624)
  - Incorrect results are returned for queries with the `limit` clause on Elasticsearch external tables. [#9226](https://github.com/StarRocks/starrocks/pull/9226)

- An unknown error occurs during queries on an Apache Iceberg table with a complex data type. [#11298](https://github.com/StarRocks/starrocks/pull/11298)

- Metadata is inconsistent between the Leader FE and Follower FE nodes. [#11215](https://github.com/StarRocks/starrocks/pull/11215)

- BE crashes when the size of BITMAP data exceeds 2 GB. [#11178](https://github.com/StarRocks/starrocks/pull/11178)

### Behavior Change

Page Cache is enabled by default. The default cache size is 20% of the system memory.

### Others

- Announcing the general availability of Resource Group.
- Announcing the general availability of the JSON data type and its related functions.
