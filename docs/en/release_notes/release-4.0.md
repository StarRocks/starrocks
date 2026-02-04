---
displayed_sidebar: docs
---

# StarRocks version 4.0

:::warning

**Downgrade Notes**

- After upgrading StarRocks to v4.0, DO NOT downgrade it directly to v3.5.0 & v3.5.1, otherwise it will cause metadata incompatibility and FE crash. You must downgrade the cluster to v3.5.2 or later to prevent these issues.
- Before downgrading clusters from v4.0.2 to v4.0.1, v4.0.0, and v3.5.2~v3.5.10, execute the following statement:

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=false;
  ```

  After upgrading the cluster back to v4.0.2 and later, execute the following statement:

  ```SQL
  SET GLOBAL enable_rewrite_simple_agg_to_meta_scan=true;
  ```

:::

## 4.0.5

Release Date: February 3, 2026

### Improvements

- Bumped Paimon version to 1.3.1. [#67098](https://github.com/StarRocks/starrocks/pull/67098)
- Restored missing optimizations in DP statistics estimation to reduce redundant calculations. [#67852](https://github.com/StarRocks/starrocks/pull/67852)
- Improved pruning in DP Join reorder to skip expensive candidate plans earlier. [#67828](https://github.com/StarRocks/starrocks/pull/67828)
- Optimized JoinReorderDP partition enumeration to reduce object allocation and added an atom count cap (&le; 62). [#67643](https://github.com/StarRocks/starrocks/pull/67643)
- Optimized DP join reorder pruning and added checks to BitSet to reduce stream operation overhead. [#67644](https://github.com/StarRocks/starrocks/pull/67644)
- Skipped predicate column statistics collection during DP statistics estimation to reduce CPU overhead. [#67663](https://github.com/StarRocks/starrocks/pull/67663)
- Optimized correlated Join row count estimation to avoid repeatedly building `Statistics` objects. [#67773](https://github.com/StarRocks/starrocks/pull/67773)
- Reduced memory allocations in `Statistics.getUsedColumns`. [#67786](https://github.com/StarRocks/starrocks/pull/67786)
- Avoided redundant `Statistics` map copies when only row counts are updated. [#67777](https://github.com/StarRocks/starrocks/pull/67777)
- Skipped aggregate pushdown logic when no aggregation exists in the query to reduce overhead. [#67603](https://github.com/StarRocks/starrocks/pull/67603)
- Improved COUNT DISTINCT over windows, added support for fused multi-distinct aggregations, and optimized CTE generation. [#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Supports `map_agg` function in the Trino dialect. [#66673](https://github.com/StarRocks/starrocks/pull/66673)
- Supports batching retrieval of LakeTablet location information during physical planning to reduce RPC calls in shared-data clusters. [#67325](https://github.com/StarRocks/starrocks/pull/67325)
- Added a thread pool for Publish Version transactions to shared-nothing clusters to improve concurrency. [#67797](https://github.com/StarRocks/starrocks/pull/67797)
- Optimized LocalMetastore locking granularity by replacing database-level locks with table-level locks. [#67658](https://github.com/StarRocks/starrocks/pull/67658)
- Refactored MergeCommitTask lifecycle management and added support for task cancellation. [#67425](https://github.com/StarRocks/starrocks/pull/67425)
- Supports intervals for automated cluster snapshots. [#67525](https://github.com/StarRocks/starrocks/pull/67525)
- Automatically cleaned up unused `mem_pool` entries in MemTrackerManager. [#67347](https://github.com/StarRocks/starrocks/pull/67347)
- Ignored `information_schema` queries during warehouse idle checks. [#67958](https://github.com/StarRocks/starrocks/pull/67958)
- Supports dynamically enabling global shuffle for Iceberg table sinks based on data distribution. [#67442](https://github.com/StarRocks/starrocks/pull/67442)
- Added Profile metrics for connector sink modules. [#67761](https://github.com/StarRocks/starrocks/pull/67761)
- Improved the collection and display of load spill metrics in Profiles, distinguishing between local and remote I/O. [#67527](https://github.com/StarRocks/starrocks/pull/67527)
- Changed Async-Profiler log level to Error to avoid repeating warning logs. [#67297](https://github.com/StarRocks/starrocks/pull/67297)
- Notified Starlet during BE shutdown to report SHUTDOWN status to StarMgr. [#67461](https://github.com/StarRocks/starrocks/pull/67461)

### Bug Fixes

The following issues have been fixed:

- Lacking support for legal simple paths containing hyphens (`-`). [#67988](https://github.com/StarRocks/starrocks/pull/67988)
- Runtime error when aggregate pushdown occurred on grouping keys involving JSON types. [#68142](https://github.com/StarRocks/starrocks/pull/68142)
- Issue where JSON path rewrite rules incorrectly pruned partition columns referenced in partition predicates. [#67986](https://github.com/StarRocks/starrocks/pull/67986)
- Type mismatch issue when rewriting simple aggregation using statistics. [#67829](https://github.com/StarRocks/starrocks/pull/67829)
- Potential heap-buffer-overflow in partition Joins. [#67435](https://github.com/StarRocks/starrocks/pull/67435)
- Duplicate `slot_ids` introduced when pushing down heavy expressions. [#67477](https://github.com/StarRocks/starrocks/pull/67477)
- Division-by-zero error in ExecutionDAG fragment connection for lacking precondition checks. [#67918](https://github.com/StarRocks/starrocks/pull/67918)
- Potential issues caused by fragment parallel prepare for single BE. [#67798](https://github.com/StarRocks/starrocks/pull/67798)
- Operator terminates incorrectly for lacking `set_finished` method for RawValuesSourceOperator. [#67609](https://github.com/StarRocks/starrocks/pull/67609)
- BE crash caused by unsupported DECIMAL256 type (precision > 38) in column aggregators. [#68134](https://github.com/StarRocks/starrocks/pull/68134)
- Shared-data clusters lack support for Fast Schema Evolution v2 over DELETE operations by carrying `schema_key` in requests. [#67456](https://github.com/StarRocks/starrocks/pull/67456)
- Shared-data clusters lack support for Fast Schema Evolution v2 over synchronous materialized views and traditional schema changes. [#67443](https://github.com/StarRocks/starrocks/pull/67443)
- Vacuum might accidentally delete files when file bundling is disabled during FE downgrade. [#67849](https://github.com/StarRocks/starrocks/pull/67849)
- Incorrect graceful exit handling in MySQLReadListener. [#67917](https://github.com/StarRocks/starrocks/pull/67917)

## 4.0.4

Release Date: January 16, 2026

### Improvements

- Supports Parallel Prepare for Operators and Drivers, and single-node batch fragment deployment to improve query scheduling performance. [#63956](https://github.com/StarRocks/starrocks/pull/63956)
- Optimized `deltaRows` calculation with lazy evaluation for large partition tables. [#66381](https://github.com/StarRocks/starrocks/pull/66381)
- Optimized Flat JSON processing with sequential iteration and improved path derivation. [#66941](https://github.com/StarRocks/starrocks/pull/66941) [#66850](https://github.com/StarRocks/starrocks/pull/66850)
- Supports releasing Spill Operator memory earlier to reduce memory usage in group execution. [#66669](https://github.com/StarRocks/starrocks/pull/66669)
- Optimized the logic to reduce string comparison overhead. [#66570](https://github.com/StarRocks/starrocks/pull/66570)
- Improved skew detection in `GroupByCountDistinctDataSkewEliminateRule` and `SkewJoinOptimizeRule` to support histogram and NULL-based strategies. [#66640](https://github.com/StarRocks/starrocks/pull/66640) [#67100](https://github.com/StarRocks/starrocks/pull/67100)
- Enhanced Column ownership management in Chunk using Move semantics to reduce Copy-On-Write overhead. [#66805](https://github.com/StarRocks/starrocks/pull/66805)
- For shared-data clusters, added FE `TableSchemaService` and updated `MetaScanNode` to support Fast Schema Evolution v2 schema retrieval. [#66142](https://github.com/StarRocks/starrocks/pull/66142) [#66970](https://github.com/StarRocks/starrocks/pull/66970)
- Supports multi-warehouse Backend resource statistics and parallelism (DOP) calculation for better resource isolation. [#66632](https://github.com/StarRocks/starrocks/pull/66632)
- Supports configuring Iceberg split size via StarRocks session variable `connector_huge_file_size`. [#67044](https://github.com/StarRocks/starrocks/pull/67044)
- Supports label-formatted statistics in `QueryDumpDeserializer`. [#66656](https://github.com/StarRocks/starrocks/pull/66656)
- Added an FE configuration `lake_enable_fullvacuum` (Default: `false`) to allow disabling Full Vacuum in shared-data clusters. [#63859](https://github.com/StarRocks/starrocks/pull/63859)
- Upgraded lz4 dependency to v1.10.0. [#67045](https://github.com/StarRocks/starrocks/pull/67045)
- Added fallback logic for sample-type cardinality estimation when row count is 0. [#65599](https://github.com/StarRocks/starrocks/pull/65599)
- Validated Strict Weak Ordering property for lambda comparator in `array_sort`. [#66951](https://github.com/StarRocks/starrocks/pull/66951)
- Optimized error messages when fetching external table metadata (Delta/Hive/Hudi/Iceberg) fails, showing root causes. [#66916](https://github.com/StarRocks/starrocks/pull/66916)
- Supports dumping pipeline status on query timeout and cancelling with `TIMEOUT` state in FE. [#66540](https://github.com/StarRocks/starrocks/pull/66540)
- Displays matched rule index in SQL blacklist error messages. [#66618](https://github.com/StarRocks/starrocks/pull/66618)
- Added labels to column statistics in `EXPLAIN` output. [#65899](https://github.com/StarRocks/starrocks/pull/65899)
- Filtered out "cancel fragment" logs for normal query completions (for example, LIMIT reached). [#66506](https://github.com/StarRocks/starrocks/pull/66506)
- Reduced Backend heartbeat failure logs when the warehouse is suspended. [#66733](https://github.com/StarRocks/starrocks/pull/66733)
- Supports `IF EXISTS` in the `ALTER STORAGE VOLUME` syntax. [#66691](https://github.com/StarRocks/starrocks/pull/66691)

### Bug Fix

The following issues have been fixed:

- Incorrect `DISTINCT` and `GROUP BY` results under Low Cardinality optimization due to missing `withLocalShuffle`. [#66768](https://github.com/StarRocks/starrocks/pull/66768)
- Rewrite error for JSON v2 functions with Lambda expressions. [#66550](https://github.com/StarRocks/starrocks/pull/66550)
- Incorrect application of Partition Join in Null-aware Left Anti Join within correlated subqueries. [#67038](https://github.com/StarRocks/starrocks/pull/67038)
- Incorrect row count calculation in the Meta Scan rewrite rule. [#66852](https://github.com/StarRocks/starrocks/pull/66852)
- Nullable property mismatched in Union Node when rewriting Meta Scan by statistics. [#67051](https://github.com/StarRocks/starrocks/pull/67051)
- BE crash caused by optimization logic for Ranking window functions when `PARTITION BY` and `ORDER BY` are missing. [#67094](https://github.com/StarRocks/starrocks/pull/67094)
- Potential wrong results in Group Execution Join with window functions. [#66441](https://github.com/StarRocks/starrocks/pull/66441)
- Incorrect results from `PartitionColumnMinMaxRewriteRule` under specific filter conditions. [#66356](https://github.com/StarRocks/starrocks/pull/66356)
- Incorrect Nullable property deduction in Union operations after aggregation. [#65429](https://github.com/StarRocks/starrocks/pull/65429)
- Crash in `percentile_approx_weighted` when handling compression parameters. [#64838](https://github.com/StarRocks/starrocks/pull/64838)
- Crash when spilling with large string encoding. [#61495](https://github.com/StarRocks/starrocks/pull/61495)
- Crash triggered by multiple calls to `set_collector` when pushing down local TopN. [#66199](https://github.com/StarRocks/starrocks/pull/66199)
- Dependency deduction error in LowCardinality rewrite logic. [#66795](https://github.com/StarRocks/starrocks/pull/66795)
- Rowset ID leak when rowset commit fails. [#66301](https://github.com/StarRocks/starrocks/pull/66301)
- Metacache lock contention. [#66637](https://github.com/StarRocks/starrocks/pull/66637)
- Ingestion failure when column-mode partial update is used with conditional update. [#66139](https://github.com/StarRocks/starrocks/pull/66139)
- Concurrent import failure caused by Tablet deletion during the ALTER operation. [#65396](https://github.com/StarRocks/starrocks/pull/65396)
- Tablet metadata load error due to RocksDB iteration timeout. [#65146](https://github.com/StarRocks/starrocks/pull/65146)
- Compression settings were not applied during table creation and Schema Change in shared-data clusters. [#65673](https://github.com/StarRocks/starrocks/pull/65673)
- Delete Vector CRC32 compatibility issue during upgrade. [#65442](https://github.com/StarRocks/starrocks/pull/65442)
- Status check logic error in file cleanup after clone task failure. [#65709](https://github.com/StarRocks/starrocks/pull/65709)
- Abnormal statistics collection logic after `INSERT OVERWRITE`. [#65327](https://github.com/StarRocks/starrocks/pull/65327) [#65298](https://github.com/StarRocks/starrocks/pull/65298) [#65225](https://github.com/StarRocks/starrocks/pull/65225)
- Foreign Key constraints were lost after FE restart. [#66474](https://github.com/StarRocks/starrocks/pull/66474)
- Metadata retrieval error after Warehouse deletion. [#66436](https://github.com/StarRocks/starrocks/pull/66436)
- Inaccurate Audit Log scan statistics under high selectivity filters. [#66280](https://github.com/StarRocks/starrocks/pull/66280)
- Incorrect query error rate metrics calculation logic. [#65891](https://github.com/StarRocks/starrocks/pull/65891)
- Potential MySQL connection leaks when tasks exit. [#66829](https://github.com/StarRocks/starrocks/pull/66829)
- BE status was not updated immediately on the SIGSEGV crash. [#66212](https://github.com/StarRocks/starrocks/pull/66212)
- NPE during LDAP user login. [#65843](https://github.com/StarRocks/starrocks/pull/65843)
- Inaccurate error log when switching users in HTTP SQL requests. [#65371](https://github.com/StarRocks/starrocks/pull/65371)
- HTTP context leaks during TCP connection reuse. [#65203](https://github.com/StarRocks/starrocks/pull/65203)
- Missing QueryDetail in Profile logs for queries forwarded from Follower. [#64395](https://github.com/StarRocks/starrocks/pull/64395)
- Missing Prepare/Execute details in Audit logs. [#65448](https://github.com/StarRocks/starrocks/pull/65448)
- Crash caused by HyperLogLog memory allocation failures. [#66747](https://github.com/StarRocks/starrocks/pull/66747)
- Issue with the `trim` function memory reservation. [#66477](https://github.com/StarRocks/starrocks/pull/66477) [#66428](https://github.com/StarRocks/starrocks/pull/66428)
- CVE-2025-66566 and CVE-2025-12183. [#66453](https://github.com/StarRocks/starrocks/pull/66453) [#66362](https://github.com/StarRocks/starrocks/pull/66362) [#67053](https://github.com/StarRocks/starrocks/pull/67053) 
- Race condition in Exec Group driver submission. [#66099](https://github.com/StarRocks/starrocks/pull/66099)
- Use-after-free risk in Pipeline countdown. [#65940](https://github.com/StarRocks/starrocks/pull/65940)
- `MemoryScratchSinkOperator` hangs when the queue closes. [#66041](https://github.com/StarRocks/starrocks/pull/66041)
- Filesystem cache key collision issue. [#65823](https://github.com/StarRocks/starrocks/pull/65823)
- Wrong subtask count in `SHOW PROC '/compactions'`. [#67209](https://github.com/StarRocks/starrocks/pull/67209)
- A unified JSON format is not returned in the Query Profile API. [#67077](https://github.com/StarRocks/starrocks/pull/67077)
- Improper `getTable` exception handling that affects the materialized view check. [#67224](https://github.com/StarRocks/starrocks/pull/67224)
- Inconsistent output of the `Extra` column from the `DESC` statement for native and cloud-native tables. [#67238](https://github.com/StarRocks/starrocks/pull/67238)
- Race condition in single-node deployments. [#67215](https://github.com/StarRocks/starrocks/pull/67215)
- Log leakage from third-party libraries. [#67129](https://github.com/StarRocks/starrocks/pull/67129)
- Incorrect REST Catalog authentication logic that causes authentication failures. [#66861](https://github.com/StarRocks/starrocks/pull/66861)

## 4.0.3

Release Date: December 25, 2025

### Improvements

- Supports `ORDER BY` clauses for STRUCT data types [#66035](https://github.com/StarRocks/starrocks/pull/66035)
- Supports creating Iceberg views with properties and displaying properties in the output of `SHOW CREATE VIEW`. [#65938](https://github.com/StarRocks/starrocks/pull/65938)
- Supports altering Iceberg table partition specs using `ALTER TABLE ADD/DROP PARTITION COLUMN`. [#65922](https://github.com/StarRocks/starrocks/pull/65922)
- Supports `COUNT/SUM/AVG(DISTINCT)` aggregation over framed windows (for example, `ORDER BY`/`PARTITION BY`) with optimization options. [#65815](https://github.com/StarRocks/starrocks/pull/65815)
- Optimized CSV parsing performance by using `memchr` for single-character delimiters. [#63715](https://github.com/StarRocks/starrocks/pull/63715)
- Added an optimizer rule to push down Partial TopN to the Pre-Aggregation phase to reduce network overhead. [#61497](https://github.com/StarRocks/starrocks/pull/61497)
- Enhanced Data Cache monitoring
  - Added new metrics for memory/disk quota and usage. [#66168](https://github.com/StarRocks/starrocks/pull/66168)
  - Added Page Cache statistics to the `api/datacache/stat` HTTP endpoint. [#66240](https://github.com/StarRocks/starrocks/pull/66240)
  - Added hit rate statistics for native tables. [#66198](https://github.com/StarRocks/starrocks/pull/66198)
- Optimized Sort and Aggregation operators to support rapid memory release in OOM scenarios. [#66157](https://github.com/StarRocks/starrocks/pull/66157)
- Added `TableSchemaService` in FE for shared-data clusters to allow CNs to fetch specific schemas on demand. [#66142](https://github.com/StarRocks/starrocks/pull/66142)
- Optimized Fast Schema Evolution to retain history schemas until all dependent ingestion jobs are finished. [#65799](https://github.com/StarRocks/starrocks/pull/65799)
- Enhanced `filterPartitionsByTTL` to properly handle NULL partition values to prevent all partitions from being filtered. [#65923](https://github.com/StarRocks/starrocks/pull/65923)
- Optimized `FusedMultiDistinctState` to clear the associated MemPool upon reset. [#66073](https://github.com/StarRocks/starrocks/pull/66073)
- Made `ICEBERG_CATALOG_SECURITY` property check case-insensitive in Iceberg REST Catalog. [#66028](https://github.com/StarRocks/starrocks/pull/66028)
- Added HTTP endpoint `GET /service_id` to retrieve StarOS Service ID in shared-data clusters. [#65816](https://github.com/StarRocks/starrocks/pull/65816)
- Replaced deprecated `metadata.broker.list` with `bootstrap.servers` in Kafka consumer configurations. [#65437](https://github.com/StarRocks/starrocks/pull/65437)
- Added FE configuration `lake_enable_fullvacuum` (Default: false) to allow disabling the Full Vacuum Daemon. [#66685](https://github.com/StarRocks/starrocks/pull/66685)
- Updated lz4 library to v1.10.0. [#67080](https://github.com/StarRocks/starrocks/pull/67080)

### Bug Fixes

The following issues have been fixed:

- `latest_cached_tablet_metadata` could cause versions to be incorrectly skipped during batch Publish. [#66558](https://github.com/StarRocks/starrocks/pull/66558)
- Potential issues caused by `ClusterSnapshot` relative checks in `CatalogRecycleBin` when running in shared-nothing clusters. [#66501](https://github.com/StarRocks/starrocks/pull/66501)
- BE crash when writing complex data types (ARRAY/MAP/STRUCT) to Iceberg tables during Spill operations. [#66209](https://github.com/StarRocks/starrocks/pull/66209)
- Potential hang in Connector Chunk Sink when the writer's initialization or initial write fails. [#65951](https://github.com/StarRocks/starrocks/pull/65951)
- Connector Chunk Sink bug where `PartitionChunkWriter` initialization failure caused a null pointer dereference during close. [#66097](https://github.com/StarRocks/starrocks/pull/66097)
- Setting a non-existent system variable would silently succeed instead of reporting an error. [#66022](https://github.com/StarRocks/starrocks/pull/66022)
- Bundle metadata parsing failure when Data Cache is corrupted. [#66021](https://github.com/StarRocks/starrocks/pull/66021)
- MetaScan returned NULL instead of 0 for count columns when the result is empty. [#66010](https://github.com/StarRocks/starrocks/pull/66010)
- `SHOW VERBOSE RESOURCE GROUP ALL` displays NULL instead of `default_mem_pool` for resource groups created in earlier versions. [#65982](https://github.com/StarRocks/starrocks/pull/65982)
- A `RuntimeException` during query execution after disabling the `flat_json` table configuration. [#65921](https://github.com/StarRocks/starrocks/pull/65921)
- Type mismatch issue in shared-data clusters caused by rewriting `min`/`max` statistics to MetaScan after Schema Change. [#65911](https://github.com/StarRocks/starrocks/pull/65911)
- BE crash caused by ranking window optimization when `PARTITION BY` and `ORDER BY` are missing. [#67093](https://github.com/StarRocks/starrocks/pull/67093)
- Incorrect `can_use_bf` check when merging runtime filters, which could lead to wrong results or crashes. [#67062](https://github.com/StarRocks/starrocks/pull/67062)
- Pushing down runtime bitset filters into nested OR predicates causes incorrect results. [#67061](https://github.com/StarRocks/starrocks/pull/67061)
- Potential data race and data loss issues caused by write or flush operations after the DeltaWriter has finished. [#66966](https://github.com/StarRocks/starrocks/pull/66966)
- Execution error caused by mismatched nullable properties when rewriting simple aggregation to MetaScan. [#67068](https://github.com/StarRocks/starrocks/pull/67068)
- Incorrect row count calculation in the MetaScan rewrite rule. [#66967](https://github.com/StarRocks/starrocks/pull/66967)
- Versions might be incorrectly skipped during batch Publish due to inconsistent cached tablet metadata. [#66575](https://github.com/StarRocks/starrocks/pull/66575)
- Improper error handling for memory allocation failures in HyperLogLog operations. [#66827](https://github.com/StarRocks/starrocks/pull/66827)

## 4.0.2

Release Date: December 4, 2025

### New Features

- Introduced a new resource group attribute, `mem_pool`, allowing multiple resource groups to share the same memory pool and enforce a joint memory limit for the pool. This feature is backward compatible. `default_mem_pool` is used if `mem_pool` is not specified. [#64112](https://github.com/StarRocks/starrocks/pull/64112)

### Improvements

- Reduced remote storage access during Vacuum after File Bundling is enabled. [#65793](https://github.com/StarRocks/starrocks/pull/65793)
- The File Bundling feature caches the latest tablet metadata. [#65640](https://github.com/StarRocks/starrocks/pull/65640)
- Improved safety and stability for long-string scenarios. [#65433](https://github.com/StarRocks/starrocks/pull/65433) [#65148](https://github.com/StarRocks/starrocks/pull/65148)  
- Optimized the `SplitTopNAggregateRule` logic to avoid performance regression. [#65478](https://github.com/StarRocks/starrocks/pull/65478)  
- Applied the Iceberg/DeltaLake table statistics collection strategy to other external data sources to avoid collecting statistics when the table is a single table. [#65430](https://github.com/StarRocks/starrocks/pull/65430)  
- Added Page Cache metrics to the Data Cache HTTP API `api/datacache/app_stat`. [#65341](https://github.com/StarRocks/starrocks/pull/65341)  
- Supports ORC file splitting to enable parallel scanning of a single large ORC file. [#65188](https://github.com/StarRocks/starrocks/pull/65188)  
- Added selectivity estimation for IF predicates in the optimizer. [#64962](https://github.com/StarRocks/starrocks/pull/64962)  
- Supports constant evaluation of `hour`, `minute`, and `second` for `DATE` and `DATETIME` types in the FE. [#64953](https://github.com/StarRocks/starrocks/pull/64953)  
- Enabled rewrite of simple aggregation to MetaScan by default. [#64698](https://github.com/StarRocks/starrocks/pull/64698)  
- Improved multiple-replica assignment handling in shared-data clusters for enhanced reliability. [#64245](https://github.com/StarRocks/starrocks/pull/64245)  
- Exposes cache hit ratio in audit logs and metrics. [#63964](https://github.com/StarRocks/starrocks/pull/63964)  
- Estimates per-bucket distinct counts for histograms using HyperLogLog or sampling to provide more accurate NDV for predicates and joins. [#58516](https://github.com/StarRocks/starrocks/pull/58516)  
- Supports FULL OUTER JOIN USING with SQL-standard semantics. [#65122](https://github.com/StarRocks/starrocks/pull/65122)  
- Prints memory information when Optimizer times out for diagnostics. [#65206](https://github.com/StarRocks/starrocks/pull/65206)

### Bug Fixes

The following issues have been fixed:

- DECIMAL56 `mod`-related issue. [#65795](https://github.com/StarRocks/starrocks/pull/65795)  
- Issue related to Iceberg scan range handling. [#65658](https://github.com/StarRocks/starrocks/pull/65658)  
- MetaScan rewrite issues on temporary partitions and random buckets. [#65617](https://github.com/StarRocks/starrocks/pull/65617)  
- `JsonPathRewriteRule` uses the wrong table after transparent materialized view rewrite. [#65597](https://github.com/StarRocks/starrocks/pull/65597)  
- Materialized view refresh failures when `partition_retention_condition` referenced generated columns. [#65575](https://github.com/StarRocks/starrocks/pull/65575)
- Iceberg min/max value typing issue. [#65551](https://github.com/StarRocks/starrocks/pull/65551)  
- Issue with queries against `information_schema.tables` and `views` across different databases when `enable_evaluate_schema_scan_rule` is set to `true`. [#65533](https://github.com/StarRocks/starrocks/pull/65533)  
- Integer overflow in JSON array comparison. [#64981](https://github.com/StarRocks/starrocks/pull/64981)  
- MySQL Reader does not support SSL. [#65291](https://github.com/StarRocks/starrocks/pull/65291)  
- ARM build issue caused by SVE build incompatibility. [#65268](https://github.com/StarRocks/starrocks/pull/65268)  
- Queries based on bucket-aware execution may get stuck for bucketed Iceberg tables. [#65261](https://github.com/StarRocks/starrocks/pull/65261)  
- Robust error propagation and memory safety issues for the lack of memory limit checks in OLAP table scan. [#65131](https://github.com/StarRocks/starrocks/pull/65131)

### Behavior Changes

- When a materialized view is inactivated, the system recursively inactivates its dependent materialized views. [#65317](https://github.com/StarRocks/starrocks/pull/65317)  
- Uses the original materialized view query SQL (including comments/formatting) when generating SHOW CREATE output. [#64318](https://github.com/StarRocks/starrocks/pull/64318)

## 4.0.1

Release Date: November 17, 2025

### Improvements

- Optimized TaskRun session variable handling to process known variables only. [#64150](https://github.com/StarRocks/starrocks/pull/64150)
- Supports collecting statistics of Iceberg and Delta Lake tables from metadata by default. [#64140](https://github.com/StarRocks/starrocks/pull/64140)
- Supports collecting statistics of Iceberg tables with bucket and truncate partition transform. [#64122](https://github.com/StarRocks/starrocks/pull/64122)
- Supports inspecting FE `/proc` profile for debugging. [#63954](https://github.com/StarRocks/starrocks/pull/63954)
- Enhanced OAuth2 and JWT authentication support for Iceberg REST catalogs. [#63882](https://github.com/StarRocks/starrocks/pull/63882)
- Improved bundle tablet metadata validation and recovery handling. [#63949](https://github.com/StarRocks/starrocks/pull/63949)
- Improved scan-range memory estimation logic. [#64158](https://github.com/StarRocks/starrocks/pull/64158)

### Bug Fixes

The following issues have been fixed:

- Transaction logs were deleted when publishing bundle tablets. [#64030](https://github.com/StarRocks/starrocks/pull/64030)
- The join algorithm cannot guarantee the sort property because, after joining, the sort property is not reset. [#64086](https://github.com/StarRocks/starrocks/pull/64086)
- Issues related to transparent materialized view rewrite. [#63962](https://github.com/StarRocks/starrocks/pull/63962)

### Behavior Changes

- Added the property `enable_iceberg_table_cache` to Iceberg Catalogs to optionally disable Iceberg table cache and allow it always to read the latest data. [#64082](https://github.com/StarRocks/starrocks/pull/64082)
- Ensured `INSERT ... SELECT` reads the freshest metadata by refreshing external tables before planning. [#64026](https://github.com/StarRocks/starrocks/pull/64026)
- Increased lock table slots to 256 and added `rid` to slow-lock logs. [#63945](https://github.com/StarRocks/starrocks/pull/63945)
- Temporarily disabled `shared_scan` due to incompatibility with event-based scheduling. [#63543](https://github.com/StarRocks/starrocks/pull/63543)
- Changed the default Hive Catalog cache TTL to 24 hours and removed unused parameters. [#63459](https://github.com/StarRocks/starrocks/pull/63459)
- Automatically determine the Partial Update mode based on the session variable and the number of inserted columns. [#62091](https://github.com/StarRocks/starrocks/pull/62091)

## 4.0.0

Release date: October 17, 2025

### Data Lake Analytics

- Unified Page Cache and Data Cache for BE metadata, and adopted an adaptive strategy for scaling. [#61640](https://github.com/StarRocks/starrocks/issues/61640)
- Optimized metadata file parsing for Iceberg statistics to avoid repetitive parsing. [#59955](https://github.com/StarRocks/starrocks/pull/59955)
- Optimized COUNT/MIN/MAX queries against Iceberg metadata by efficiently skipping over data file scans, significantly improving aggregation query performance on large partitioned tables and reducing resource consumption. [#60385](https://github.com/StarRocks/starrocks/pull/60385)
- Supports compaction for Iceberg tables via procedure `rewrite_data_files`. 
- Supports Iceberg tables with hidden partitions, including creating, writing, and reading the tables. [#58914](https://github.com/StarRocks/starrocks/issues/58914)
- Supports setting sort keys when creating Iceberg tables.
- Optimizes sink performance for Iceberg tables.
  - Iceberg Sink supports spilling large operators, global shuffle, and local sorting to optimize memory usage and address small file issues. [#61963](https://github.com/StarRocks/starrocks/pull/61963)
  - Iceberg Sink optimizes local sorting based on Spill Partition Writer to improve write efficiency. [#62096](https://github.com/StarRocks/starrocks/pull/62096)
  - Iceberg Sink supports global shuffle for partitions to further reduce small files. [#62123](https://github.com/StarRocks/starrocks/pull/62123)
- Enhanced bucket-aware execution for Iceberg tables to improve concurrency and distribution capabilities of bucketed tables. [#61756](https://github.com/StarRocks/starrocks/pull/61756)
- Supports the TIME data type in the Paimon catalog. [#58292](https://github.com/StarRocks/starrocks/pull/58292)
- Upgraded Iceberg version to 1.10.0. [#63667](https://github.com/StarRocks/starrocks/pull/63667)

### Security and Authentication

- In scenarios where JWT authentication and the Iceberg REST Catalog are used, StarRocks supports the passthrough of user login information to Iceberg via the REST Session Catalog for subsequent data access authentication. [#59611](https://github.com/StarRocks/starrocks/pull/59611) [#58850](https://github.com/StarRocks/starrocks/pull/58850)
- Supports vended credentials for the Iceberg catalog.
- Supports granting StarRocks internal roles to external groups obtained via Group Provider. [#63385](https://github.com/StarRocks/starrocks/pull/63385) [#63258](https://github.com/StarRocks/starrocks/pull/63258)
- Added REFRESH privilege to external tables to control the permission to refresh them. [#63385](https://github.com/StarRocks/starrocks/pull/62636)

<!--
- Supports HTTPS via configuring certificates on the StarRocks FE side, enhancing system access security to meet encrypted transmission requirements on the cloud or intranet. [#56394](https://github.com/StarRocks/starrocks/pull/56394)
- Supports HTTPS communication between BE nodes to ensure the encryption and integrity of data transmission, preventing internal data leakage and Man-in-the-Middle attacks.[#53695](https://github.com/StarRocks/starrocks/pull/53695)
-->

### Storage Optimization and Cluster Management

- Introduced  the File Bundling optimization for the cloud-native table in shared-data clusters to automatically bundle the data files generated by loading, Compaction, or Publish operations, thereby reducing the API cost caused by high-frequency access to the external storage system. File Bundling is enabled by default for tables created in v4.0 or later. [#58316](https://github.com/StarRocks/starrocks/issues/58316)
- Supports Multi-Table Write-Write Transaction to allow users to control the atomic submission of INSERT, UPDATE, and DELETE operations. The transaction supports Stream Load and INSERT INTO interfaces, effectively guaranteeing cross-table consistency in ETL and real-time write scenarios. [#61362](https://github.com/StarRocks/starrocks/issues/61362)
- Supports Kafka 4.0 for Routine Load.
- Supports full-text inverted indexes on Primary Key tables in shared-nothing clusters.
- Supports modifying aggregate keys of Aggregate tables. [#62253](https://github.com/StarRocks/starrocks/issues/62253)
- Supports enabling case-insensitive processing on names of catalogs, databases, tables, views, and materialized views. [#61136](https://github.com/StarRocks/starrocks/pull/61136)
- Supports blacklisting Compute Nodes in shared-data clusters. [#60830](https://github.com/StarRocks/starrocks/pull/60830)
- Supports global connection ID. [#57256](https://github.com/StarRocks/starrocks/pull/57276)
- Added the `recyclebin_catalogs` metadata view to Information Schema to display recoverable deleted metadata. [#51007](https://github.com/StarRocks/starrocks/pull/51007)

### Query and Performance Improvement

- Supports DECIMAL256 data type, expanding the upper limit of precision from 38 to 76 bits. Its 256-bit storage provides better adaptability to high-precision financial and scientific computing scenarios, effectively mitigating DECIMAL128's precision overflow problem in very large aggregations and high-order operations. [#59645](https://github.com/StarRocks/starrocks/issues/59645)
- Improved the performance for basic operators.[#61691](https://github.com/StarRocks/starrocks/issues/61691) [#61632](https://github.com/StarRocks/starrocks/pull/61632) [#62585](https://github.com/StarRocks/starrocks/pull/62585) [#61405](https://github.com/StarRocks/starrocks/pull/61405)  [#61429](https://github.com/StarRocks/starrocks/pull/61429)
- Optimized the performance of the JOIN and AGG operators. [#61691](https://github.com/StarRocks/starrocks/issues/61691)
- [Preview] Introduced SQL Plan Manager to allow users to bind a query plan to a query, thereby preventing the query plan from changing due to system state changes (mainly data updates and statistics updates), thus stabilizing query performance. [#56310](https://github.com/StarRocks/starrocks/issues/56310)
- Introduced Partition-wise Spillable Aggregate/Distinct operators to replace the original Spill implementation based on sorted aggregation, significantly improving aggregation performance and reducing read/write overhead in complex and high-cardinality GROUP BY scenarios. [#60216](https://github.com/StarRocks/starrocks/pull/60216)
- Flat JSON V2:
  - Supports configuring Flat JSON on the table level. [#57379](https://github.com/StarRocks/starrocks/pull/57379)
  - Enhance JSON columnar storage by retaining the V1 mechanism while adding page- and segment-level indexes (ZoneMaps, Bloom filters), predicate pushdown with late materialization, dictionary encoding, and integration of a low-cardinality global dictionary to significantly boost execution efficiency. [#60953](https://github.com/StarRocks/starrocks/issues/60953)
- Supports an adaptive ZoneMap index creation strategy for the STRING data type. [#61960](https://github.com/StarRocks/starrocks/issues/61960)
- Enhanced query observability:
  - Optimized EXPLAIN ANALYZE output to display the execution metrics by group and by operator for better readability. [#63326](https://github.com/StarRocks/starrocks/pull/63326)
  - `QueryDetailActionV2` and `QueryProfileActionV2` now support JSON format, enhancing cross-FE query capabilities. [#63235](https://github.com/StarRocks/starrocks/pull/63235)
  - Supports retrieving Query Profile information across all FEs. [#61345](https://github.com/StarRocks/starrocks/pull/61345)
  - SHOW PROCESSLIST statements display Catalog, Query ID, and other information. [#62552](https://github.com/StarRocks/starrocks/pull/62552)
  - Enhanced query queue and process monitoring, supporting display of Running/Pending statuses.[#62261](https://github.com/StarRocks/starrocks/pull/62261)
- Materialized view rewrites consider the distribution and sort keys of the original table, improving the selection of optimal materialized views. [#62830](https://github.com/StarRocks/starrocks/pull/62830)

### Functions and SQL Syntax

- Added the following functions:
  - `bitmap_hash64` [#56913](https://github.com/StarRocks/starrocks/pull/56913)
  - `bool_or` [#57414 ](https://github.com/StarRocks/starrocks/pull/57414)
  - `strpos` [#57278](https://github.com/StarRocks/starrocks/pull/57287)
  - `to_datetime` and `to_datetime_ntz` [#60637](https://github.com/StarRocks/starrocks/pull/60637)
  - `regexp_count` [#57182](https://github.com/StarRocks/starrocks/pull/57182)
  - `tokenize` [#58965](https://github.com/StarRocks/starrocks/pull/58965)
  - `format_bytes` [#61535](https://github.com/StarRocks/starrocks/pull/61535)
  - `encode_sort_key` [#61781](https://github.com/StarRocks/starrocks/pull/61781)
  - `column_size` and `column_compressed_size`  [#62481](https://github.com/StarRocks/starrocks/pull/62481)
- Provides the following syntactic extensions:
  - Supports IF NOT EXISTS keywords in CREATE ANALYZE FULL TABLE. [#59789](https://github.com/StarRocks/starrocks/pull/59789)
  - Supports EXCLUDE clauses in SELECT.  [#57411](https://github.com/StarRocks/starrocks/pull/57411/files)
  - Supports FILTER clauses in aggregate functions, improving readability and execution efficiency of conditional aggregations. [#58937](https://github.com/StarRocks/starrocks/pull/58937)

### Behavior Changes

- Adjust the logic of the materialized view parameter `auto_partition_refresh_number` to limit the number of partitions to refresh regardless of auto refresh or manual refresh. [#62301](https://github.com/StarRocks/starrocks/pull/62301)
- Flat JSON is enabled by default. [#62097](https://github.com/StarRocks/starrocks/pull/62097)
- The default value of the system variable `enable_materialized_view_agg_pushdown_rewrite` is set to `true`, indicating that aggregation pushdown for materialized view query rewrite is enabled by default. [#60976](https://github.com/StarRocks/starrocks/pull/60976)
- Changed the type of some columns in `information_schema.materialized_views` to better align with the corresponding data. [#60054](https://github.com/StarRocks/starrocks/pull/60054)
- The `split_part` function returns NULL when the delimiter is not matched. [#56967](https://github.com/StarRocks/starrocks/pull/56967)
- Use STRING to replace fixed-length CHAR in CTAS/CREATE MATERIALIZED VIEW to avoid deducing the wrong column length, which may cause materialized view refresh failures. [#63114](https://github.com/StarRocks/starrocks/pull/63114) [#62476](https://github.com/StarRocks/starrocks/pull/62476)
- Data Cache-related configurations are simplified. [#61640](https://github.com/StarRocks/starrocks/issues/61640)
  - `datacache_mem_size` and `datacache_disk_size` are now effective.
  - `storage_page_cache_limit`, `block_cache_mem_size`, `block_cache_disk_size` are deprecated.
- Added new catalog properties (`remote_file_cache_memory_ratio` for Hive, and `iceberg_data_file_cache_memory_usage_ratio` and `iceberg_delete_file_cache_memory_usage_ratio` for Iceberg) to limit the memory resources used for Hive and Iceberg metadata cache, and set the default values to `0.1` (10%). Adjust the metadata cache TTL to 24 hours. [#63459](https://github.com/StarRocks/starrocks/pull/63459) [#63373](https://github.com/StarRocks/starrocks/pull/63373) [#61966](https://github.com/StarRocks/starrocks/pull/61966) [#62288](https://github.com/StarRocks/starrocks/pull/62288)
- SHOW DATA DISTRIBUTION now will not merge the statistics of all materialized indexes with the same bucket sequence number. It only shows data distribution at the materialized index level. [#59656](https://github.com/StarRocks/starrocks/pull/59656)
- The default bucket size for automatic bucket tables is changed from 4GB to 1GB to improve performance and resource utilization. [#63168](https://github.com/StarRocks/starrocks/pull/63168)
-  The system determines the Partial Update mode based on the corresponding session variable and the number of columns in the INSERT statement. [#62091](https://github.com/StarRocks/starrocks/pull/62091)
- Optimized the `fe_tablet_schedules` view in the Information Schema. [#62073](https://github.com/StarRocks/starrocks/pull/62073) [#59813](https://github.com/StarRocks/starrocks/pull/59813)
  - Renamed the `TABLET_STATUS` column to `SCHEDULE_REASON`, the `CLONE_SRC` column to `SRC_BE_ID`, and the `CLONE_DEST` column to `DEST_BE_ID`.
  - The data types of the `CREATE_TIME`, `SCHEDULE_TIME` and `FINISH_TIME` columns have been changed from `DOUBLE` to `DATETIME`.
- The `is_leader` label has been added to some FE metrics. [#63004](https://github.com/StarRocks/starrocks/pull/63004)
- Shared-data clusters using Microsoft Azure Blob Storage and Data Lake Storage Gen 2 as object storage will experience Data Cache failure after being upgraded to v4.0. The system will automatically reload the cache.
