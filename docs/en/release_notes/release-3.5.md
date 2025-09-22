---
displayed_sidebar: docs
---

# StarRocks version 3.5

:::warning

After upgrading StarRocks to v3.5, DO NOT downgrade it directly to v3.4.0 ～ v3.4.4, otherwise it will cause metadata incompatibility. You must downgrade the cluster to v3.4.5 or later to prevent the issue.

:::

## 3.5.6

Release date: September 22, 2025

### Improvements

- A decommissioned BE will be forcibly dropped when all its tablets are in the recycle bin, to avoid the decommission being blocked by those tablets. [#62781](https://github.com/StarRocks/starrocks/pull/62781)
- Vacuum metrics will be updated when Vacuum succeeds. [#62540](https://github.com/StarRocks/starrocks/pull/62540)
- Added thread pool metrics to the fragment instance execution state report, including active threads, queue count, and running threads. [#63067](https://github.com/StarRocks/starrocks/pull/63067)
- Supports S3 path-style access in shared-data clusters to improve compatibility with MinIO and other S3-compatible storage systems. You can enable this feature by setting `aws.s3.enable_path_style_access` to `true` when creating a storage volume. [#62591 ](https://github.com/StarRocks/starrocks/pull/62591)
- Supports resetting the starting point of the AUTO_INCREMENT value via `ALTER TABLE`` <table_name>`` AUTO_INCREMENT`` = 10000;`. [#62767 ](https://github.com/StarRocks/starrocks/pull/62767)
- Supports using Distinguished Name (DN) in Group Provider for group matching, improving the user group solution for LDAP/Microsoft Active Directory environments.  [#62711](https://github.com/StarRocks/starrocks/pull/62711)
- Supports Azure Workload Identity authentication for Azure Data Lake Storage Gen2.  [#62754](https://github.com/StarRocks/starrocks/pull/62754)
- Added transaction error messages to the `information_schema.``loads` view to aid failure diagnosis. [#61364](https://github.com/StarRocks/starrocks/pull/61364)
- Supports reusing common expressions for complex CASE WHEN expressions in Scan predicates to reduce repetitive computation. [#62779](https://github.com/StarRocks/starrocks/pull/62779)
- Uses the REFRESH (instead of ALTER) privilege on the materialized view to execute REFRESH statements. [#62636](https://github.com/StarRocks/starrocks/pull/62636)
- Disabled low-cardinality optimization on Lake tables by default to avoid potential issues. [#62586](https://github.com/StarRocks/starrocks/pull/62586)
- Enabled tablet balancing between workers by default in shared-data clusters. [#62661](https://github.com/StarRocks/starrocks/pull/62661)
- Supports reusing expressions in outer-join WHERE predicates to reduce repetitive computation. [#62139](https://github.com/StarRocks/starrocks/pull/62139)
- Added Clone metrics in FE. [#62421](https://github.com/StarRocks/starrocks/pull/62421)
- Added Clone metrics in BE. [#62479](https://github.com/StarRocks/starrocks/pull/62479)
- Added an FE configuration item `enable_statistic_cache_refresh_after_write` to disable statistics-cache lazy refresh by default. [#62518](https://github.com/StarRocks/starrocks/pull/62518)
- Masked credential information in SUBMIT TASK for better security. [#62311](https://github.com/StarRocks/starrocks/pull/62311)
- `json_extract` in the Trino dialect returns a JSON type. [#59718](https://github.com/StarRocks/starrocks/pull/59718)
- Supports ARRAY type in `null_or_empty`. [#62207](https://github.com/StarRocks/starrocks/pull/62207)
- Adjusted the size limit for the Iceberg manifest cache. [#61966](https://github.com/StarRocks/starrocks/pull/61966)
- Added a remote file-cache limit for Hive. [#62288](https://github.com/StarRocks/starrocks/pull/62288)

### Bug Fixes

The following issues have been fixed:

- Secondary replicas hang indefinitely due to negative timeout values, which cause incorrect timestamp comparisons. [#62805](https://github.com/StarRocks/starrocks/pull/62805)
- PublishTask may be blocked when TransactionState is REPLICATION. [#61664](https://github.com/StarRocks/starrocks/pull/61664)
- Incorrect repair mechanism for Hive tables that have been dropped and recreated during materialized view refresh. [#63072](https://github.com/StarRocks/starrocks/pull/63072)
- Incorrect execution plans were generated after the materialized view aggregation push‑down rewrite. [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- ANALYZE PROFILE failures caused by PlanTuningGuide producing unrecognized strings (null explainString) in the query profiles. [#63024](https://github.com/StarRocks/starrocks/pull/63024)
- Inappropriate return type of  `hour_from_unixtime` and incorrect rewrite rule of `CAST`. [#63006  ](https://github.com/StarRocks/starrocks/pull/63006)
- NPE in Iceberg manifest cache under data races. [#63043  ](https://github.com/StarRocks/starrocks/pull/63043)
- Shared-data clusters lack support for colocation in materialized views. [#62941 ](https://github.com/StarRocks/starrocks/pull/62941) 
- Iceberg table Scan Exception during Scan Range deployment.[ #62994 ](https://github.com/StarRocks/starrocks/pull/62994) 
- Incorrect execution plans were generated for view-based rewrite. [#62918](https://github.com/StarRocks/starrocks/pull/62918)
- Errors and disrupted tasks due to Compute Nodes are not gracefully shut down on exit. [#62916 ](https://github.com/StarRocks/starrocks/pull/62916) 
- NPE when Stream Load execution status updates. [#62921](https://github.com/StarRocks/starrocks/pull/62921)
- An issue with statistics when the column name and the name in the PARTITION BY clause differ in case. [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- Wrong results are returned when the `LEAST` function is used as a predicate. [#62826](https://github.com/StarRocks/starrocks/pull/62826)  
- Invalid ProjectOperator above the table-pruning frontier CTEConsumer. [#62914](https://github.com/StarRocks/starrocks/pull/62914)  
- Redundant replica handling after Clone. [#62542](https://github.com/StarRocks/starrocks/pull/62542)  
- Failed to collect Stream Load profiles. [#62802](https://github.com/StarRocks/starrocks/pull/62802)  
- Ineffective disk rebalancing caused by improper BE selection. [#62776](https://github.com/StarRocks/starrocks/pull/62776)  
- A potential NPE crash in LocalTabletsChannel when a missing `tablet_id` leads to a null delta writer.  [#62861](https://github.com/StarRocks/starrocks/pull/62861)
- KILL ANALYZE does not take effect. [ #62842](https://github.com/StarRocks/starrocks/pull/62842)
- SQL syntax errors in histogram stats when MCV values contain single quotes. [#62853](https://github.com/StarRocks/starrocks/pull/62853)
- Incorrect output format of metrics for Prometheus. [#62742](https://github.com/StarRocks/starrocks/pull/62742)
- NPE when querying `information_schema.analyze_status` after the database is dropped. [#62796](https://github.com/StarRocks/starrocks/pull/62796)
- CVE-2025-58056. [#62801](https://github.com/StarRocks/starrocks/pull/62801)
- When SHOW CREATE ROUTINE LOAD is executed, wrong results are returned because the database is considered null if not specified. [#62745](https://github.com/StarRocks/starrocks/pull/62745)
- Data loss caused by incorrectly skipping CSV headers in `files()`. [#62719](https://github.com/StarRocks/starrocks/pull/62719)
- NPE when replaying batch-transaction upserts. [#62715](https://github.com/StarRocks/starrocks/pull/62715)
- Publish being incorrectly reported as successful during graceful shutdown in shared-nothing clusters. [#62417](https://github.com/StarRocks/starrocks/pull/62417)
- Crash in asynchronous delta writer due to a null pointer. [#62626](https://github.com/StarRocks/starrocks/pull/62626)
- Materialized view refresh is skipped because the materialized view version map is not cleared after a failed restore job. [#62634](https://github.com/StarRocks/starrocks/pull/62634)
- Issues caused by case-sensitive partition column validation in the materialized view analyzer. [#62598](https://github.com/StarRocks/starrocks/pull/62598)
- Duplicate IDs for statements with syntax errors. [#62258](https://github.com/StarRocks/starrocks/pull/62258)
- StatisticsExecutor status is overridden due to redundant state assignment in CancelableAnalyzeTask. [#62538](https://github.com/StarRocks/starrocks/pull/62538)
- Incorrect error messages produced by statistics collection. [#62533](https://github.com/StarRocks/starrocks/pull/62533)
- Premature throttling caused by insufficient default maximum connections for external users. [#62523](https://github.com/StarRocks/starrocks/pull/62523)
- A potential NPE in materialized view backup and restore operations. [#62514](https://github.com/StarRocks/starrocks/pull/62514)
- Incorrect `http_workers_num` metric. [#62457](https://github.com/StarRocks/starrocks/pull/62457)
- The runtime filter fails to locate the corresponding execution group during construction. [#62465](https://github.com/StarRocks/starrocks/pull/62465)
- Tedious results on Scan Node caused by simplifying CASE WHEN with complex functions. [#62505](https://github.com/StarRocks/starrocks/pull/62505)
- `gmtime` is not thread-safe. [#60483](https://github.com/StarRocks/starrocks/pull/60483)
- An issue with getting Hive partitions with escaped strings. [#59032](https://github.com/StarRocks/starrocks/pull/59032)

## 3.5.5

Release date: September 5, 2025

### Improvements

- Added a new system variable `enable_drop_table_check_mv_dependency` (default: `false`). When set to `true`, if the object to be dropped is referenced by a downstream materialized view, the system prevents the execution of `DROP TABLE` / `DROP VIEW` / `DROP MATERIALIZED VIEW`. The error message lists the dependent materialized views and suggests checking the `sys.object_dependencies` view for details. [#61584](https://github.com/StarRocks/starrocks/pull/61584)
- Logs now include the Linux distribution and CPU architecture of the build, to facilitate issue reproduction and troubleshooting. Log format: `... build <hash> distro <id> arch <arch>`. [#62017](https://github.com/StarRocks/starrocks/pull/62017)
- Persisted per-Tablet index and incremental column group file sizes are now cached, replacing on-demand directory scans. This accelerates Tablet status reporting in BE and reduces latency under high I/O scenarios. [#61901](https://github.com/StarRocks/starrocks/pull/61901)
- Downgraded several high-frequency INFO logs in FE and BE to VLOG, and aggregated task submission logs, significantly reducing redundant storage-related logs and log volume under heavy load. [#62121](https://github.com/StarRocks/starrocks/pull/62121)
- Improved query performance for External Catalog metadata through `information_schema` by pushing table filters before calling `getTable`, avoiding per-table RPCs. [#62404](https://github.com/StarRocks/starrocks/pull/62404)

### Bug Fixes

The following issues have been fixed:

- NullPointerException when fetching partition-level column statistics during the Plan stage due to missing data. [#61935](https://github.com/StarRocks/starrocks/pull/61935)
- Fixed Parquet write issues with non-empty NULL arrays, and corrected `SPLIT(NULL, …)` behavior to consistently return NULL, preventing data corruption and runtime errors. [#61999](https://github.com/StarRocks/starrocks/pull/61999)
- Failure when creating materialized views using `CASE WHEN` expressions due to incompatible VARCHAR type returns (fixed by ensuring consistency before and after refresh, and introducing a new FE configuration `transform_type_prefer_string_for_varchar` to prefer STRING and avoid length mismatch). [#61996](https://github.com/StarRocks/starrocks/pull/61996)
- Statistics for nested CTEs could not be computed outside of memo when `enable_rbo_table_prune` was `false`. [#62070](https://github.com/StarRocks/starrocks/pull/62070)
- In Audit Logs, inaccurate Scan Rows results for INSERT INTO SELECT statements. [#61381](https://github.com/StarRocks/starrocks/pull/61381)
- ExceptionInInitializerError/NullPointerException during initialization caused FE startup failure when Query Queue v2 was enabled. [#62161](https://github.com/StarRocks/starrocks/pull/62161)
- BE crash when `LakePersistentIndex` initialization failed and `_memtable` cleanup was triggered. [#62279](https://github.com/StarRocks/starrocks/pull/62279)
- Permission issues during materialized view refresh due to creator roles not being activated (fixed by adding FE configuration `mv_use_creator_based_authorization`. When set to `false`, materialized views are refreshed as root, for compatibility with LDAP-authenticated clusters). [#62396](https://github.com/StarRocks/starrocks/pull/62396)
- Materialized view refresh failures caused by case-sensitive List partition table names (fixed by enforcing case-insensitive uniqueness checks on partition names, aligning with OLAP table semantics). [#62389](https://github.com/StarRocks/starrocks/pull/62389)

## 3.5.4

Release Date: August 22, 2025

### Improvements

- Added logs to clarify the reason that tablets cannot be repaired.  [#61959](https://github.com/StarRocks/starrocks/pull/61959)
- Optimized DROP PARTITION information in logs.  [#61787](https://github.com/StarRocks/starrocks/pull/61787)
- Assigned a large but configurable row count to tables with unknown stats for statistical estimation.  [#61332](https://github.com/StarRocks/starrocks/pull/61332)
- Added balance statistic according to label location.  [#61905](https://github.com/StarRocks/starrocks/pull/61905)
- Added colocate group balance statistics to improve cluster monitoring. [#61736](https://github.com/StarRocks/starrocks/pull/61736)
- Skipped the Publish waiting phase when the number of healthy replicas exceeds the default replica count. [#61820](https://github.com/StarRocks/starrocks/pull/61820)
- Included the tablet information collection time in the tablet report. [#61643](https://github.com/StarRocks/starrocks/pull/61643)
- Supports writing Starlet files with tags. [ #61605](https://github.com/StarRocks/starrocks/pull/61605)
- Supports viewing cluster balance statistics via SHOW PROC.  [#61578](https://github.com/StarRocks/starrocks/pull/61578)
- Bumped librdkafka to 2.11.0 to support Kafka 4.0 and removed deprecated configurations.  [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- Added `prepared_timeout` configuration to Stream Load Transaction Interface.  [#61539](https://github.com/StarRocks/starrocks/pull/61539)
- Upgraded StarOS to v3.5‑rc3.  [#61685](https://github.com/StarRocks/starrocks/pull/61685)

### Bug Fixes

The following issues have been fixed:

- Incorrect Dict version of random distribution tables. [#61933](https://github.com/StarRocks/starrocks/pull/61933)
- Incorrect query context in context conditions. [#61929](https://github.com/StarRocks/starrocks/pull/61929)
- Publish failures caused by synchronous Publish for shadow tablets during ALTER operations. [#61887](https://github.com/StarRocks/starrocks/pull/61887)
- CVE‑2025‑55163 issue.  [#62041](https://github.com/StarRocks/starrocks/pull/62041)
- Memory leak in real-time data ingestion from Apache Kafka.  [#61698](https://github.com/StarRocks/starrocks/pull/61698)
- Incorrect count of rebuild files in the lake persistent index. [#61859](https://github.com/StarRocks/starrocks/pull/61859)
- Statistics collection on generated expression columns causes cross-database query errors. [#61829](https://github.com/StarRocks/starrocks/pull/61829)
- Query Cache misaligns in shared-nothing clusters, causing inconsistent results. [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- High memory usage in CatalogRecycleBin due to retaining deleted partition information.[#61582](https://github.com/StarRocks/starrocks/pull/61582)
- SQL Server JDBC connections fail when the timeout exceeds 65,535 milliseconds. [#61719](https://github.com/StarRocks/starrocks/pull/61719)
- Security Integration fails to encrypt passwords, exposing sensitive information. [#60666](https://github.com/StarRocks/starrocks/pull/60666)
- `MIN()` and `MAX()` functions on Iceberg partition columns return NULL unexpectedly.  [#61858](https://github.com/StarRocks/starrocks/pull/61858)
- Other predicates of Join containing non‑push‑down subfields were incorrectly rewritten.  [#61868](https://github.com/StarRocks/starrocks/pull/61868)
- QueryContext cancellation can lead to a use‑after‑free situation.  [#61897](https://github.com/StarRocks/starrocks/pull/61897)
- CBO’s table pruning overlooks other predicates.  [#61881](https://github.com/StarRocks/starrocks/pull/61881)
- Partial Updates in `COLUMN_UPSERT_MODE` may overwrite auto-increment columns with zero.  [#61341](https://github.com/StarRocks/starrocks/pull/61341)
- JDBC TIME type conversion uses an incorrect timezone offset that leads to wrong time values. [#61783](https://github.com/StarRocks/starrocks/pull/61783)
- `max_filter_ratio` was not being serialized in Routine Load jobs. [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- Precision loss in the `now(precision)` function in Stream Load. [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- Cancelling a query may result in a “query id not found” error. [#61667](https://github.com/StarRocks/starrocks/pull/61667)
- LDAP authentication may miss PartialResultException, causing incomplete query results.[ #60667](https://github.com/StarRocks/starrocks/pull/60667)
- Paimon Timestamp timezone conversion issue when the query condition contains DATETIME.[ #60473](https://github.com/StarRocks/starrocks/pull/60473)

## 3.5.3

Release Date: August 11, 2025

### Improvements

- Lake Compaction adds Segment write time statistics. [#60891](https://github.com/StarRocks/starrocks/pull/60891)
- Disable inline mode for Data Cache writes to avoid performance degradation. [#60530](https://github.com/StarRocks/starrocks/pull/60530)
- Iceberg metadata scan supports shared file I/O. [#61012](https://github.com/StarRocks/starrocks/pull/61012)
- Support termination of all PENDING ANALYZE tasks. [#61118](https://github.com/StarRocks/starrocks/pull/61118)
- Force reuse when there are too many CTE nodes to avoid excessive optimization time. [#60983](https://github.com/StarRocks/starrocks/pull/60983)
- Added `BALANCE` type to cluster balance results. [#61081](https://github.com/StarRocks/starrocks/pull/61081)
- Optimized materialized view rewrite for external tables. [#61037](https://github.com/StarRocks/starrocks/pull/61037)
- Default value of system variable `enable_materialized_view_agg_pushdown_rewrite` is changed to `true`, enabling aggregation pushdown for materialized view queries by default. [#60976](https://github.com/StarRocks/starrocks/pull/60976)
- Optimized partition statistics lock competition. [#61041](https://github.com/StarRocks/starrocks/pull/61041)

### Bug Fixes

The following issues have been fixed:

- Inconsistent Chunk column size after column pruning. [#61271](https://github.com/StarRocks/starrocks/pull/61271)
- Synchronous execution of partition statistics loading may cause deadlocks. [#61300](https://github.com/StarRocks/starrocks/pull/61300)
- Crash when `array_map` processes constant array columns. [#61309](https://github.com/StarRocks/starrocks/pull/61309)
- Setting an auto-increment column to NULL results in the system mistakenly rejecting valid data within the same Chunk. [#61255](https://github.com/StarRocks/starrocks/pull/61255)
- The actual number of JDBC connections may exceed the `jdbc_connection_pool_size` limit. [#61038](https://github.com/StarRocks/starrocks/pull/61038)
- FQDN mode did not use IP addresses as cache map keys. [#61203](https://github.com/StarRocks/starrocks/pull/61203)
- Array column cloning error during array comparison. [#61036](https://github.com/StarRocks/starrocks/pull/61036)
- Deploying serialized thread pool blockage led to query performance degradation. [#61150](https://github.com/StarRocks/starrocks/pull/61150)
- OK hbResponse not synchronized after heartbeat retry counter reset. [#61249](https://github.com/StarRocks/starrocks/pull/61249)
- Incorrect result for the `hour_from_unixtime` function. [#61206](https://github.com/StarRocks/starrocks/pull/61206)
- Conflicts between ALTER TABLE jobs and partition creation. [#60890](https://github.com/StarRocks/starrocks/pull/60890)
- Cache does not take effect after upgrading from v3.3 to v3.4 or later. [#60973](https://github.com/StarRocks/starrocks/pull/60973)
- Vector index metric `hit_count` is not set. [#61102](https://github.com/StarRocks/starrocks/pull/61102)
- Stream Load transactions fail to find the coordinator node. [#60154](https://github.com/StarRocks/starrocks/pull/60154)
- BE crashes when loading OOM partitions. [#60778](https://github.com/StarRocks/starrocks/pull/60778)
- INSERT OVERWRITE failed on manually created partitions. [#60750](https://github.com/StarRocks/starrocks/pull/60750)
- Partition creation failed when partition names matched case-insensitively but had different values. [#60909](https://github.com/StarRocks/starrocks/pull/60909)
- The system does not support PostgreSQL UUID type. [#61021](https://github.com/StarRocks/starrocks/pull/61021)
- Case sensitivity issue with column names when loading Parquet data via `FILES()`. [#61059](https://github.com/StarRocks/starrocks/pull/61059)

## 3.5.2

Release Date: July 18, 2025

### Improvements

- Collected NDV (number of distinct values) statistics for ARRAY columns to improve query plan accuracy. [#60623](https://github.com/StarRocks/starrocks/pull/60623)
- Disabled replica balancing for Colocate tables and tablet scheduling in Shared-data clusters to reduce unnecessary log output. [#60737](https://github.com/StarRocks/starrocks/pull/60737)
- Optimized Catalog access workflow: FE now delays accessing external data sources asynchronously at startup to prevent hanging due to external service unavailability. [#60614](https://github.com/StarRocks/starrocks/pull/60614)
- Added session variable `enable_predicate_expr_reuse` to control predicate pushdown. [#60603](https://github.com/StarRocks/starrocks/pull/60603)
- Supports a retry mechanism when fetching Kafka partition information fails. [#60513](https://github.com/StarRocks/starrocks/pull/60513)
- Removed the restriction requiring exact mapping of partition columns between materialized views and base tables. [#60565](https://github.com/StarRocks/starrocks/pull/60565)
- Supports building Runtime In-Filters to enhance aggregation performance by filtering data during aggregation. [#59288](https://github.com/StarRocks/starrocks/pull/59288)

### Bug Fixes

Fixed the following issues:

- COUNT DISTINCT queries crash due to low-cardinality optimization for multiple columns. [#60664](https://github.com/StarRocks/starrocks/pull/60664)
- Incorrect matching of global UDFs when multiple functions share the same name. [#60550](https://github.com/StarRocks/starrocks/pull/60550)
- Null pointer exception (NPE) issue during Stream Load import. [#60755](https://github.com/StarRocks/starrocks/pull/60755)
- Null pointer exception (NPE) issue when starting FE during a recovery from a cluster snapshot. [#60604](https://github.com/StarRocks/starrocks/pull/60604)
- BE crash caused by column mode mismatch when processing short-circuit queries with out-of-order values. [#60466](https://github.com/StarRocks/starrocks/pull/60466)
- Session variables set via PROPERTIES in SUBMIT TASK statements did not take effect. [#60584](https://github.com/StarRocks/starrocks/pull/60584)
- Incorrect results for `SELECT min/max` queries under specific conditions. [#60601](https://github.com/StarRocks/starrocks/pull/60601)
- Incorrect bucket pruning when the left side of a predicate is a function, leading to incorrect query results. [#60467](https://github.com/StarRocks/starrocks/pull/60467)
- Crash for queries against a non-existent `query_id` via Arrow Flight SQL. [#60497](https://github.com/StarRocks/starrocks/pull/60497)

### Behavior Changes

- The default value of `lake_compaction_allow_partial_success` is set to `true`. Compaction operations can now be marked as successful even if partially completed, preventing blockage of subsequent compaction tasks. [#60643](https://github.com/StarRocks/starrocks/pull/60643)

## 3.5.1

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

## 3.5.0

Release Date: June 13, 2025

### Upgrade Notes

- JDK 17 or later is required from StarRocks v3.5.0 onwards.
  - To upgrade a cluster from v3.4 or earlier, you must upgrade the version of JDK that StarRocks depends, and remove the options that are incompatible with JDK 17 in the configuration item `JAVA_OPTS` in the FE configuration file **fe.conf**, for example, options that involve CMS and GC. The default value of `JAVA_OPTS` in the v3.5 configuration file is recommended.
  - For clusters using external catalogs, you need to add `--add-opens=java.base/java.util=ALL-UNNAMED` to the `JAVA_OPTS` configuration item in the BE configuration file **be.conf**.
  - For clusters using Java UDFs, you need to add `--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED` to the `JAVA_OPTS` configuration item in the BE configuration file **be.conf**.
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
