---
displayed_sidebar: docs
---

# StarRocks version 3.5

## v3.5.1

Release Date: July 1, 2025

### New Features

- [Experimental] Starting from v3.5.1, StarRocks introduces a high-performance data transfer channel based on the Apache Arrow Flight SQL protocol, comprehensively optimizing the data import channel and significantly improving transfer efficiency. This solution establishes a fully columnar data transfer pipeline from the StarRocks columnar execution engine to the client, eliminating the frequent row-column conversions and serialization overhead typically seen in traditional JDBC and ODBC interfaces, and achieving true zero-copy, low-latency, and high-throughput data transfer capabilities. [#57956](https://github.com/StarRocks/starrocks/pull/57956)
- Java Scalar UDFs (user-defined functions) now support ARRAY and MAP types as input parameters. [#55356](https://github.com/StarRocks/starrocks/pull/55356)
- **Cross-node data cache sharing**: Enables nodes to share cached external table data of data lakes across compute nodes via the network. If a local cache miss occurs, the system first attempts to fetch data from the caches of other nodes within the same cluster. Only if all caches miss will it re-fetch data from remote storage. This feature effectively reduces performance jitter caused by cache invalidation during elastic scaling and ensures stable query performance. A new FE configuration parameter `enable_trace_historical_node` controls this behavior (Default: `false`). [#57083](https://github.com/StarRocks/starrocks/pull/57083)
- **Storage Volume adds native support for Google Cloud Storage (GCS)**: You can now use GCS as a backend storage volume and manage and access GCS resources through the native SDK. [#58815](https://github.com/StarRocks/starrocks/pull/58815)

### Improvements

- Optimized error messages when creating Hive external tables fails. [#60076](https://github.com/StarRocks/starrocks/pull/60076)
- Optimized `count(1)` query performance using the `file_record_count` in Iceberg metadata. [#60022](https://github.com/StarRocks/starrocks/pull/60022)
- Refined the Compaction scheduling logic to avoid delayed scheduling when all subtasks succeed. [#59998](https://github.com/StarRocks/starrocks/pull/59998)
- Added `JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED"` to BE and CN after upgrading to JDK 17. [#59947](https://github.com/StarRocks/starrocks/pull/59947)
- Supports modifying the `kafka_broker_list` property via the ALTER ROUTINE LOAD command when Kafka Broker endpoints change. [#59787](https://github.com/StarRocks/starrocks/pull/59787)
- Supports reducing build dependencies of the Docker base image through parameters. [#59772](https://github.com/StarRocks/starrocks/pull/59772)
- Supports accessing Azure using Managed Identity authentication. [#59657](https://github.com/StarRocks/starrocks/pull/59657)
- Improved error messages when querying external data via `Files()` function with duplicate path column names. [#59597](https://github.com/StarRocks/starrocks/pull/59597)
- Optimized LIMIT pushdown logic. [#59265](https://github.com/StarRocks/starrocks/pull/59265)

### Bug Fixes

Fixed the following issues:

- Partition pruning issue when queries include Max and Min aggregations and empty partitions. [#60162](https://github.com/StarRocks/starrocks/pull/60162)
- Incorrect query results when rewriting queries with materialized views due to missing NULL partitions. [#60087](https://github.com/StarRocks/starrocks/pull/60087)
- Refresh errors on Iceberg external tables when using partition expressions based on `str2date`. [#60089](https://github.com/StarRocks/starrocks/pull/60089)
- Incorrect partition range when creating temporary partitions using the START END syntax. [#60014](https://github.com/StarRocks/starrocks/pull/60014)
- Incorrect display of Routine Load metrics on non-leader FE nodes. [#59985](https://github.com/StarRocks/starrocks/pull/59985)
- BE/CN crashes when executing queries containing `COUNT(*)` window functions. [#60003](https://github.com/StarRocks/starrocks/pull/60003)
- Stream Load failures when the target table name contains Chinese characters. [#59722](https://github.com/StarRocks/starrocks/pull/59722)
- Overall loading failures to triple-replica tables when loading to a secondary replica fails. [#59762](https://github.com/StarRocks/starrocks/pull/59762)
- Missing parameters in SHOW CREATE VIEW output. [#59714](https://github.com/StarRocks/starrocks/pull/59714)

### Behavior Changes

- Some FE metrics include the `is_leader` label. [#59883](https://github.com/StarRocks/starrocks/pull/59883)

## v3.5.0

Release Date: June 13, 2025

### Upgrade Notes

- JDK 17 or later is required from StarRocks v3.5.0 onwards.
  - To upgrade a cluster from v3.4 or earlier, you must upgrade the version of JDK that StarRocks depends, and remove the options that are incompatible with JDK 17 in the configuration item `JAVA_OPTS` in the FE configuration file **fe.conf**, for example, options that involve CMS and GC. The default value of `JAVA_OPTS` in the v3.5 configuration file is recommended.
  - For clusters using external catalogs, you need to add `--add-opens=java.base/java.util=ALL-UNNAMED` to the `JAVA_OPTS` configuration item in the BE configuration file **be.conf**.
  - In addition, as of v3.5.0, StarRocks no longer provides JVM configurations for specific JDK versions. All versions of JDK use `JAVA_OPTS`.

### Shared-data Enhancement

- Shared-data clusters support generated columns. [#53526](https://github.com/StarRocks/starrocks/pull/53526)
- Cloud-native Primary Key tables in shared-data clusters support rebuilding specific indexes. The performance of the indexes is also optimized. [#53971](https://github.com/StarRocks/starrocks/pull/53971) [#54178](https://github.com/StarRocks/starrocks/pull/54178)
- Optimized the execution logic of large-scale data loading operations to avoid generating too many small files in Rowset due to memory limitations. During the import, the system will merge the temporary data blocks to reduce the generation of small files, which improves the query performance after the import and also reduces the subsequent Compaction operations to improve the system resource utilization. [#53954](https://github.com/StarRocks/starrocks/issues/53954) 

### Data Lake Analytics

- **[Beta]** Supports creating Iceberg views in the Iceberg Catalog with Hive Metastore integration. And supports adding or modifying the dialect of the Iceberg view using the ALTER VIEW statement for better syntax compatibility with external systems. [#56120](https://github.com/StarRocks/starrocks/pull/56120)
- Supports nested namespace for [Iceberg REST Catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/#rest). [#58016](https://github.com/StarRocks/starrocks/pull/58016)
- Supports using `IcebergAwsClientFactory` to create AWS clients in [Iceberg REST Catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/#rest) to offer vended credentials. [#58296](https://github.com/StarRocks/starrocks/pull/58296)
- Parquet Reader supports filtering data with Bloom Filter. [#56445](https://github.com/StarRocks/starrocks/pull/56445)
- Supports automatically creating global dictionaries for low-cardinality columns in Parquet-formatted Hive/Iceberg tables during queries. [#55167](https://github.com/StarRocks/starrocks/pull/55167) 

### Performance Improvement and Query Optimization

- Statistics optimization:
  - Supports Table Sample. Improved statistics accuracy and query performance by sampling data blocks in physical files. [#52787](https://github.com/StarRocks/starrocks/issues/52787)
  - Supports [recording the predicate columns in queries](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#predicate-column) for targeted statistics collection. [#53204](https://github.com/StarRocks/starrocks/issues/53204)
  - Supports partition-level cardinality estimation. The system reuses the system-defined view `_statistics_.column_statistics` to record the NDV of each partition. [#51513](https://github.com/StarRocks/starrocks/pull/51513)
  - Supports [multi-column Joint NDV collection](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#multi-column-joint-statistics) to optimize the query plan generated by CBO in the scenario where columns correlate with each other.  [#56481](https://github.com/StarRocks/starrocks/pull/56481) [#56715](https://github.com/StarRocks/starrocks/pull/56715) [#56766](https://github.com/StarRocks/starrocks/pull/56766) [#56836](https://github.com/StarRocks/starrocks/pull/56836)
  - Supports using histograms to estimate the Join node cardinality and in_predicate selectivity, thus improving the estimation accuracy in data skew. [#57874](https://github.com/StarRocks/starrocks/pull/57874) [#57639](https://github.com/StarRocks/starrocks/pull/57639)
  - Optimized [Query Feedback](https://docs.starrocks.io/docs/using_starrocks/query_feedback/). Queries with the identical structure but different parameter values will be categorized as the same type and share the same tuning guide for plan execution optimization. [#58306](https://github.com/StarRocks/starrocks/pull/58306)
- Supports Runtime Bitset Filter as an alternative for optimization to Bloom Filter in specific scenarios. [#57157](https://github.com/StarRocks/starrocks/pull/57157)
- Supports pushing down Join Runtime Filter to the storage layer. [#55124](https://github.com/StarRocks/starrocks/pull/55124)
- Supports Pipeline Event Scheduler. [#54259](https://github.com/StarRocks/starrocks/pull/54259)

### Partition Management

- Supports using ALTER TABLE to [merge expression partitions based on time functions](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/#merge-expression-partitions) for optimized storage efficiency and query performance. [#56840](https://github.com/StarRocks/starrocks/pull/56840)
- Supports partition Time-to-live (TTL) for List-partitioned tables and materialized views. And supports the property `partition_retention_condition` in tables and materialized views to allow users to set data retention strategies for list partitions, thus achieving more flexible partition deletion strategies. [#53117](https://github.com/StarRocks/starrocks/issues/53117)
- Supports using ALTER TABLE to [delete partitions specified by common partition expressions](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE/#drop-partitions), allowing users to flexibly delete partitions in batches. [#53118](https://github.com/StarRocks/starrocks/pull/53118)

### Cluster Management

- Upgraded FE compile target from Java 11 to Java 17 for better system stability and performance. [#53617](https://github.com/StarRocks/starrocks/pull/53617)  [#57030](https://github.com/StarRocks/starrocks/pull/57030)

### Security and Authentication

- Supports secure [connections encrypted by SSL](https://docs.starrocks.io/zh/docs/administration/user_privs/ssl_authentication/) based on the MySQL protocol. [#54877](https://github.com/StarRocks/starrocks/pull/54877)
- Enhanced authentication using external systems:
  - Supports creating StarRocks users with [OAuth 2.0](https://docs.starrocks.io/docs/administration/user_privs/authentication/oauth2_authentication/) and [JSON Web Token (JWT)](https://docs.starrocks.io/docs/administration/user_privs/authentication/jwt_authentication/).
  - Supports [Security Integration](https://docs.starrocks.io/docs/administration/user_privs/authentication/security_integration/) to simplify the authentication process with external systems. Security Integration supports LDAP, OAuth 2.0, and JWT. [#55846](https://github.com/StarRocks/starrocks/pull/55846)
- Supports [Group Provider](https://docs.starrocks.io/docs/administration/user_privs/group_provider/) to obtain the user group information from external authentication services. The group information can then be used in authentication and authorization. Group Provider supports acquiring group information from LDAP, operating systems, or files. Users can query the user group they belong to using the function `current_group()`. [#56670](https://github.com/StarRocks/starrocks/pull/56670) 

### Materialized Views

- Supports creating materialized views with multiple partition columns to allow users to partition the data with a more flexible strategy. [#52576](https://github.com/StarRocks/starrocks/issues/52576)
- Supports setting `query_rewrite_consistency` to `force_mv` to force the system to use the materialized view for query rewrite, thus keeping performance stability at the cost of data timeliness to a certain extent. [#53819](https://github.com/StarRocks/starrocks/pull/53819)

### Loading and Unloading

- Supports pausing Routine Load jobs on JSON parse errors by setting the property `pause_on_json_parse_error` to `true`. [#56062](https://github.com/StarRocks/starrocks/pull/56062)
- **[Beta]** Supports [transactions with multiple SQL statements](https://docs.starrocks.io/docs/loading/SQL_transaction/) (currently, only INSERT is supported). Users can start, apply, or undo a transaction to guarantee the ACID (atomicity, consistency, isolation, and durability) properties of multiple loading operations. [#53978](https://github.com/StarRocks/starrocks/issues/53978)

### Functions

- Introduced the system variable `lower_upper_support_utf8` on the session and global level, enhancing the support for UTF-8 strings (especially non-ASCII characters) in case conversion functions such as `upper()` and `lower()`. [#56192](https://github.com/StarRocks/starrocks/pull/56192)
- Added new functions:
  - [`field()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/field/) [#5533](https://github.com/StarRocks/starrocks/pull/55331)
  - [`ds_theta_count_distinct()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/ds_theta_count_distinct/) [#56960](https://github.com/StarRocks/starrocks/pull/56960)
  - [`array_flatten()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/array-functions/array_flatten/) [#50080](https://github.com/StarRocks/starrocks/pull/50080)
  - [`inet_aton()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/string-functions/inet_aton/) [#51883](https://github.com/StarRocks/starrocks/pull/51883)
  - [`percentile_approx_weight()`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/percentile_approx_weight/) [#57410](https://github.com/StarRocks/starrocks/pull/57410)
