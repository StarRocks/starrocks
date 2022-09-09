# 2.4.0 RC01

Release date: September 9th, 2022

## New Features

- Supports creating a materialized view based on multiple base tables to accelerate queries with JOIN operations.

- Supports overwriting data via the INSERT OVERWRITE statement.

- [Preview] Provides stateless Compute Nodes (CN) that can be horizontally scaled. You can use StarRocks Operator to deploy CN into your Kubernetes (K8s) cluster to achieve automatic horizontal scaling.

- Outer Join supports non-equi joins in which join items are related by comparison operators including `<`, `<=`, `>`, `>=`, and `<>`.

- Supports creating Iceberg catalogs and Hudi catalogs, which allow direct queries on data from Apache Iceberg and Apache Hudi.

- Supports querying ARRAY-type columns from Apache Hive™ tables in CSV format.

- Supports viewing the schema of external data via the DESC statement.

- Supports granting a specific role or IMPERSONATE permission to a user via the GRANT statement and revoking them via the REVOKE statement, and supports executing an SQL statement with IMPERSONATE permission via the EXECUTE AS statement.

- Supports FDQN access: now you can use domain name or the combination of hostname and port as the unique identification of a BE or an FE node. This prevents access failures caused by changing IP addresses.

- Provides the following new functions:

  - array_contains_all: checks whether a specific array is a subset of another.
  - percentile_cont: calculates the percentile value with linear interpolation.

## Improvements

- The Primary Key model supports flushing VARCHAR-type primary key indexes to disks.

  From version 2.4.0, the Primary Key model supports the same data types for primary key indexes regardless of whether persistent primary key index is turned on or not.

- Optimized the query performance on external tables.

  - Supports late materialization during queries on external tables in Parquet format to optimize the query performance on data lakes with small-scale filtering involved.
  - Small I/O operations can be merged to reduce the delay for querying data lakes, thereby improving the query performance on external tables.

- Optimized the performance of window functions.

- Optimized the performance of Cross Join by supporting predicate pushdown.

- Histograms are added to CBO statistics. Full statistics collection is further optimized.

- Adaptive multi-threading is enabled for tablet scanning to reduce the dependency of scanning performance on the tablet number.

- Functions:

  - You can use COUNT DISTINCT over multiple columns to calculate the number of distinct column combinations.
  - Window functions min() and max() support sliding windows.
  - Optimized the performance of the window_funnel function.

## Bug Fixes

The following bugs are fixed:

- DECIMAL data types returned by DESC are different from those specified in the CREATE TABLE statement. [#7309](https://github.com/StarRocks/starrocks/pull/7309)

- FE metadata management issues that affect the stability of FE. [#6685](https://github.com/StarRocks/starrocks/pull/6685) [#9445](https://github.com/StarRocks/starrocks/pull/9445) [#7974](https://github.com/StarRocks/starrocks/pull/7974) [#7455](https://github.com/StarRocks/starrocks/pull/7455)

- Data load-related issues:

  - Broke Load fails when ARRAY-type column is set. [#9158](https://github.com/StarRocks/starrocks/pull/9158)
  - Replicas are inconsistent after data is loaded to a non-Duplicate Key table via Broker Load. [#8714](https://github.com/StarRocks/starrocks/pull/8714)
  - Executing ALTER ROUTINE LOAD raises NPE. [#7804](https://github.com/StarRocks/starrocks/pull/7804)

- Data Lake analytic-related issues:

  - Queries on Parquet-format in Hive external tables fail. [#7413](https://github.com/StarRocks/starrocks/pull/7413) [#7482](https://github.com/StarRocks/starrocks/pull/7482) [#7624](https://github.com/StarRocks/starrocks/pull/7624)
  - Incorrect results are returned to queries with `limit` clause on Elasticsearch external table. [#9226](https://github.com/StarRocks/starrocks/pull/9226)

## Behavior Change

Page Cache is enabled by default. The default cache size is 20% of the system memory.

## Others

- Announcing the general availability of the Resource Group.
- Announcing the general availability of the JSON data type.
