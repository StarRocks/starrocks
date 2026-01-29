---
displayed_sidebar: docs
---

# StarRocks version 3.4

## 3.4.10

Release Date: January 12, 2026

### Improvements

- Supports pushing down GROUP BY expressions to scan operators and rewriting through materialized views, further improving query performance [#66546](https://github.com/StarRocks/starrocks/pull/66546)
- Added a configuration switch for the Hudi library internal metadata table, allowing users to disable it when encountering performance issues. [#67581](https://github.com/StarRocks/starrocks/pull/67581)

### Bug Fixes

The following issues have been fixed:

- CVE-2025-12183 and CVE-2025-66566. [#66373](https://github.com/StarRocks/starrocks/pull/66373) [#66480](https://github.com/StarRocks/starrocks/pull/66480)
- In multi-statement submission scenarios, the SQL/statement information recorded in the Profile may be incorrect, leading to unreliable troubleshooting and performance analysis data. [#67119](https://github.com/StarRocks/starrocks/pull/67119)
- Java UDF/UDAF parameter conversion may take an abnormal path when the input column is "all NULL and nullable", causing Java heap memory to balloon abnormally and potentially triggering OOM. [#67105](https://github.com/StarRocks/starrocks/pull/67105)
- When there is no `PARTITION BY`/`GROUP BY` and the window function is a ranking type (`row_number`/`rank`/`dense_rank`), the optimizer may generate an invalid execution plan (TOP-N with empty ORDER BY + MERGING-EXCHANGE), causing BE to crash. [#67085](https://github.com/StarRocks/starrocks/pull/67085)
- After operations like resizing, deserialization, or filtering on `Object`/JSON columns, the internal pointer cache may still point to old addresses (dangling pointers), returning `nullptr` when reading object values and causing segmentation faults/data corruption. [#66990](https://github.com/StarRocks/starrocks/pull/66990)
- `trim()` may trigger vector out-of-bounds operations (for example, underflow on empty slices) with specific Unicode whitespace characters/boundary inputs, causing BE internal errors or crashes. [#66484](https://github.com/StarRocks/starrocks/pull/66484)
- `trim()` has improper buffer reservation length calculation, potentially causing insufficient reservation and frequent expansions, triggering exception paths, and leading to BE internal errors in severe cases. [#66489](https://github.com/StarRocks/starrocks/pull/66489)
- After table value function rewrite (from `bitmap_to_array` to `unnest_bitmap`), the projection column type may be incorrectly inferred (bitmap mistaken as `ARRAY``<``BIGINT``>`), leading to type inconsistency risks in subsequent plans/execution. [#66986](https://github.com/StarRocks/starrocks/pull/66986)
- When BE triggers crash handling on fatal signals (for example, SIGSEGV), the heartbeat service may still briefly return success, causing FE to mistakenly believe the BE is alive (until heartbeat timeout), potentially scheduling queries to the crashed node during this period. [#66250](https://github.com/StarRocks/starrocks/pull/66250)
- Race condition exists between initial submission and dynamic driver addition (`submit_next_driver`) in Execution Groups, potentially causing "driver already blocked yet added to schedule" assertion failures and BE crashes. [#66111](https://github.com/StarRocks/starrocks/pull/66111)
- When pushing down DISTINCT + LIMIT predicates, the global LIMIT may be incorrectly applied before the Exchange node, causing data to be truncated prematurely and result sets to miss some rows. [#66129](https://github.com/StarRocks/starrocks/pull/66129)
- When using ExecutionGroup (group execution) mode, if JOIN is followed by window functions, data may be out of order or duplicated, leading to incorrect results. [#66458](https://github.com/StarRocks/starrocks/pull/66458)
- In audit logs and query statistics, statistics like scan row count may be missing or inaccurate in some high-selectivity filtering scenarios, causing monitoring and troubleshooting information to be distorted. [#66422](https://github.com/StarRocks/starrocks/pull/66422)
- When CASE-WHEN nesting is deep and each layer has many branches, the expression tree node count may explode exponentially, causing FE OOM. [#66379](https://github.com/StarRocks/starrocks/pull/66379)
- The `percentile_approx_weighted` function may access the wrong parameter position when obtaining the compression factor from const parameters, causing BE crashes. [#65217](https://github.com/StarRocks/starrocks/pull/65217)
- During BE startup, when loading tablet metadata, if RocksDB iteration times out, it may discard already-loaded tablets and retry from the beginning; in cross-disk migration scenarios, this may lead to version loss. [#65445](https://github.com/StarRocks/starrocks/pull/65445) [#65427](https://github.com/StarRocks/starrocks/pull/65427)
- Stream Load may fail during transaction commit due to invalid tablet references (for example, deleted during ALTER). [#65986](https://github.com/StarRocks/starrocks/pull/65986)
- When rowset COMMIT or Compaction COMMIT fails on Primary Key tables, the rowset ID is not released, causing files to not be garbage-collected and disk space to leak. [#66336](https://github.com/StarRocks/starrocks/pull/66336)
- DELETE statements during partition pruning may attempt materialized view rewrite preparation, potentially blocking or failing DELETE due to table lock order or deadlock issues. [#65818](https://github.com/StarRocks/starrocks/pull/65818) [#65820](https://github.com/StarRocks/starrocks/pull/65820)
- When the same table is referenced multiple times in a query, scan nodes may concurrently allocate partition IDs, potentially causing ID conflicts and partition mapping confusion, leading to incorrect query results. [#65608](https://github.com/StarRocks/starrocks/pull/65608)
- When column-mode partial update is used together with conditional update, loading may fail with "invalid rssid" errors. [#66217](https://github.com/StarRocks/starrocks/pull/66217)
- When concurrent transactions create temporary partitions with the same partition values but different transaction IDs, "Duplicate values" errors may cause automatic partition creation to fail. [#66203](https://github.com/StarRocks/starrocks/pull/66203) [#66398](https://github.com/StarRocks/starrocks/pull/66398)
- Clone task checks the wrong status variable during cleanup after `_finish_clone_primary` failure, potentially preventing cleanup logic from executing correctly. [#65765](https://github.com/StarRocks/starrocks/pull/65765)
- When DROP and CLONE tasks execute concurrently on the same tablet, the only replica may be deleted by DROP, causing query failures. [#66271](https://github.com/StarRocks/starrocks/pull/66271)
- In Spilling scenarios, very large string encoding may cause BE crashes due to buffer reservation errors or type overflows. [#65373](https://github.com/StarRocks/starrocks/pull/65373)
- When using CACHE SELECT functionality, if column iterators are not properly sought or the schema is incorrectly reordered, assertion failures or data confusion may occur, causing BE crashes. [#66276](https://github.com/StarRocks/starrocks/pull/66276)
- External table (file format schema detection) sampling scans may experience range index out-of-bounds, causing BE crashes or reading incorrect data. [#65931](https://github.com/StarRocks/starrocks/pull/65931)
- When materialized views are based on VIEW + JOIN scenarios and the view name matches a base table name (but in different databases), partition expression parsing may fail, causing materialized view creation errors. [#66315](https://github.com/StarRocks/starrocks/pull/66315)
- When materialized views refresh on multi-level partitioned base tables, only parent partition metadata (ID/Version) is checked, not sub-partition changes, causing materialized view not to refresh after sub-partition data updates. [#66108](https://github.com/StarRocks/starrocks/pull/66108)
- After the Iceberg table snapshot expiration, the partition `last_updated_at` may be null, causing materialized views depending on that table to fail to track partition changes correctly and skip refreshes. [#66044](https://github.com/StarRocks/starrocks/pull/66044)
- When queries contain the same table multiple times with different partition predicates, materialized view compensation (MVCompensation) may confuse partition information, leading to incorrect rewrites. [#66416](https://github.com/StarRocks/starrocks/pull/66416)
- Text-based materialized view rewrite after AST cache hits does not refresh metadata, potentially using outdated tablet information and causing query failures or data inconsistencies. [#66583](https://github.com/StarRocks/starrocks/pull/66583)
- Low cardinality optimization has bugs in disabled column propagation logic, potentially causing incorrect column disabling and wrong query results. [#66771](https://github.com/StarRocks/starrocks/pull/66771)
- PRIMARY KEY tables with low cardinality optimization enabled may crash or produce incorrect data due to incompatible global dictionary collection logic. [#66739](https://github.com/StarRocks/starrocks/pull/66739)
- Nested CTEs with partial inlining and partial reuse scenarios may have overly strict optimizer checks, rejecting valid plans. [#66703](https://github.com/StarRocks/starrocks/pull/66703)
- After merging UNION to constants (VALUES), output column nullability may be incorrectly set, causing downstream operators to crash or produce incorrect results. [#65454](https://github.com/StarRocks/starrocks/pull/65454)
- Partition column min/max rewrite optimization may generate invalid TOP-N in scenarios without PARTITION BY/ORDER BY, causing BE crashes or incorrect results. [#66498](https://github.com/StarRocks/starrocks/pull/66498)
- Non-deterministic functions (e.g., `now()`) are incorrectly pushed down to lower operators, potentially causing results to be inconsistent across different operators/shards. [#66391](https://github.com/StarRocks/starrocks/pull/66391)
- Foreign key constraints are lost after FE restart because `MaterializedView. onCreate()` does not trigger constraint rebuilding and registration. [#66615](https://github.com/StarRocks/starrocks/pull/66615)
- When materialized views contain `colocate_with` property, metadata is not written to Edit Log, causing follower FE to be unaware of colocate relationships and query performance degradation. [#65840](https://github.com/StarRocks/starrocks/pull/65840) [#65405](https://github.com/StarRocks/starrocks/pull/65405)
- After a warehouse is deleted, `SHOW LOAD` or `information_schema.loads` queries may fail; original sessions cannot execute any SQL (including switching warehouses). [#66464](https://github.com/StarRocks/starrocks/pull/66464)
- If the tablet statistics report is untimely, table cardinality estimation may incorrectly use sample-type statistics in some edge cases, with cardinality of 1, causing execution plans to deviate significantly. [#65655](https://github.com/StarRocks/starrocks/pull/65655)
- Tablet statistics reporting timing issues may cause partition row counts to be 0, rendering table cardinality estimation completely ineffective. [#65266](https://github.com/StarRocks/starrocks/pull/65266)
- FE's locking order on TransactionState during `createPartition` is opposite to Gson serialization, potentially causing deadlocks. [#65792](https://github.com/StarRocks/starrocks/pull/65792)
- DELETE VECTOR CRC32 may fail validation in ABA upgrade/downgrade scenarios due to version incompatibilities, causing query errors. [#65436](https://github.com/StarRocks/starrocks/pull/65436) [#65421](https://github.com/StarRocks/starrocks/pull/65421) [#65475](https://github.com/StarRocks/starrocks/pull/65475) [#65483](https://github.com/StarRocks/starrocks/pull/65483)
- `map_agg` aggregate function may trigger crashes on specific inputs. [#67460](https://github.com/StarRocks/starrocks/pull/67460)
- When `flat_path` is empty, calling `substr(1)` triggers `std::out_of_range` exception, causing BE crashes. [#65386](https://github.com/StarRocks/starrocks/pull/65386)
- In shared-data clusters, compression configurations do not take effect correctly during table creation and schema changes. [#65778](https://github.com/StarRocks/starrocks/pull/65778)
- When adding columns with default values, concurrent INSERTs may fail due to invalid column references. [#66107](https://github.com/StarRocks/starrocks/pull/66107) [#65968](https://github.com/StarRocks/starrocks/pull/65968)
- Segment iterator selection order is inconsistent during scan initialization between shared-data and shared-nothing scenarios, potentially causing inconsistent scan behavior. [#65782](https://github.com/StarRocks/starrocks/pull/65782) [#61171](https://github.com/StarRocks/starrocks/pull/61171)
- merge_condition is not supported when merge commit is enabled, causing partial update scenarios to fail. [#65278](https://github.com/StarRocks/starrocks/pull/65278)
- Image journal ID retrieval logic is incorrect, potentially causing cluster snapshot functionality abnormalities. [#65989](https://github.com/StarRocks/starrocks/pull/65989)
- LDAP users trigger NPE in TaskRun scenarios due to null `ConnectContext. get()`, causing task failures. [#65877](https://github.com/StarRocks/starrocks/pull/65877)
- When ANALYZE statements execute on follower FE, RPC timeout still uses `query_timeout` instead of `statistic_collect_query_timeout`, potentially causing timeouts too early or too late. [#66785](https://github.com/StarRocks/starrocks/pull/66785)
- `MemoryScratchSinkOperator` cannot properly finish pending_finish after RecordBatchQueue shutdown, causing tasks to hang. [#66095](https://github.com/StarRocks/starrocks/pull/66095)
- Query error rate metric calculations use the wrong variables, potentially producing negative values or inaccuracies. [#65901](https://github.com/StarRocks/starrocks/pull/65901)
- Load profile counters may be updated repeatedly, causing inflated statistics. [#65352](https://github.com/StarRocks/starrocks/pull/65352)
- In multi-task deployment (deploy more tasks) scenarios, the Profile collection thread context is not switched correctly, causing some metrics to be lost. [#65733](https://github.com/StarRocks/starrocks/pull/65733)
- Local/Lake TabletsChannel lifecycle management has circular lock waiting risks:  deadlocks may occur on specific close/deregister paths, affecting import/write task availability. [#66820](https://github.com/StarRocks/starrocks/pull/66820)
- Filesystem instance cache (filesystem cache) causes query performance to drop significantly and cannot recover after capacity is set to 0 (due to cache key and instance mismatch). [#65979](https://github.com/StarRocks/starrocks/pull/65979)

## 3.4.9

Release Date: November 24, 2025

### Behavior Changes

- Changed the return type of `json_extract` in the Trino dialect from STRING to JSON. This may cause incompatibility in CAST, UNNEST, and type check logic. [#59718](https://github.com/StarRocks/starrocks/pull/59718)
- The metric that reports “connections per user” under `/metrics` now requires admin authentication. Without authentication, only total connection counts are exposed, preventing information leakage of all usernames via metrics. [#64635](https://github.com/StarRocks/starrocks/pull/64635)
- Removed the deprecated system variable `analyze_mv`. Materialized view refresh no longer automatically triggers ANALYZE jobs, avoiding large numbers of background statistics tasks. This changes expectations for users relying on legacy behavior. [#64863](https://github.com/StarRocks/starrocks/pull/64863)
- Changed the overflow detection logic of casting from LARGEINT to DECIMAL128 on x86. `INT128_MIN * 1` is no longer considered an overflow to ensure consistent casting semantics for extreme values. [#63559](https://github.com/StarRocks/starrocks/pull/63559)
- Added a configurable table-level lock timeout to `finishTransaction`. If a table lock cannot be acquired within the timeout, finishing the transaction will fail for this round and be retried later, rather than blocking indefinitely. The final result is unchanged, but the lock behavior is more explicit. [#63981](https://github.com/StarRocks/starrocks/pull/63981)

### Bug Fixes

The following issues have been fixed:

- During BE start, if loading tablet metadata from RocksDB times out, RocksDB may restart loading from the beginning and accidentally pick up stale tablet entries, risking data version loss. [#65146](https://github.com/StarRocks/starrocks/pull/65146)
- Data corruption issues related to CRC32C checksum for delete-vectors of Lake Primary Key tables. [#65006](https://github.com/StarRocks/starrocks/pull/65006) [#65354](https://github.com/StarRocks/starrocks/pull/65354) [#65442](https://github.com/StarRocks/starrocks/pull/65442) [#65354](https://github.com/StarRocks/starrocks/pull/65354)
- When the internal `flat_path` string is empty because the JSON hyper extraction path is `$` or all paths are skipped, calling `substr` will throw an exception and cause BE crash. [#65260](https://github.com/StarRocks/starrocks/pull/65260)
- When spilling large strings to disk, insufficient length checks, using 32‑bit attachment sizes, and issues in the BlockReader could cause crashes. [#65373](https://github.com/StarRocks/starrocks/pull/65373)
- When multiple HTTP requests reuse the same TCP connection, if a non‑ExecuteSQL request arrives after an ExecuteSQL request, the `HttpConnectContext` cannot be unregistered at channel close, causing HTTP context leaks. [#65203](https://github.com/StarRocks/starrocks/pull/65203)
- Primitive value loss issue under certain circumstances when JSON data is being flattened. [#64939](https://github.com/StarRocks/starrocks/pull/64939) [#64703](https://github.com/StarRocks/starrocks/pull/64703)
- Crash in `ChunkAccumulator` when chunks are appended with incompatible JSON schemas. [#64894](https://github.com/StarRocks/starrocks/pull/64894)
- In `AsyncFlushOutputStream`, asynchronous I/O tasks may attempt to access a destroyed `MemTracker`, resulting in use‑after‑free crashes. [#64735](https://github.com/StarRocks/starrocks/pull/64735) 
- Concurrent Compaction tasks against the same Lake Primary Key table lack integrity checks, which could leave metadata in an inconsistent state after a failed publish.  [#65005](https://github.com/StarRocks/starrocks/pull/65005)
- When spilling Hash Joins, if the build side’s `set_finishing` task failed, it only recorded the status in the spiller, allowing the Probe side to continue, and eventually causing a crash or an indefinite loop. [#65027](https://github.com/StarRocks/starrocks/pull/65027)
- During tablet migration, if the only newest replica is marked as DECOMMISSION, the version of the target replica is outdated and stuck at VERSION_INCOMPLETE. [#62942](https://github.com/StarRocks/starrocks/pull/62942)
- Use-after-free issue because the relevant Block Group is not released when `PartitionedSpillerWriter` is removing partitions. [#63903](https://github.com/StarRocks/starrocks/pull/63903) [#63825](https://github.com/StarRocks/starrocks/pull/63825)
- BE crash caused by MorselQueue's failure to get splits. [#62753](https://github.com/StarRocks/starrocks/pull/62753)
- In shared-data clusters, Sorted-by-key Scans with multiple I/O tasks could produce wrong results in sort-based aggregations. [#63849](https://github.com/StarRocks/starrocks/pull/63849)
- On ARM, reading Parquet columns for certain Hive external tables could crash in LZ4 conversion when copying NULL bitmaps because the destination null buffer pointer was stale due to out-of-order execution. [#63294](https://github.com/StarRocks/starrocks/pull/63294)

## 3.4.8

Release Date: September 30, 2025

### Behavior Changes

- By setting the default value of `enable_lake_tablet_internal_parallel` to `true`, Parallel Scan for Cloud-native tables in shared-data clusters is enabled by default to increase per‑query internal parallelism. It may raise peak resource usage. [#62159](https://github.com/StarRocks/starrocks/pull/62159)

### Bug Fixes

The following issues have been fixed:

- Delta Lake partition column names were forcibly converted to lowercase, causing a mismatch with the actual column names. [#62953](https://github.com/StarRocks/starrocks/pull/62953)
- The Iceberg manifest cache eviction race could trigger a NullPointerException (NPE). [#](https://github.com/StarRocks/starrocks/pull/63052)[#63043](https://github.com/StarRocks/starrocks/pull/63043)
- Uncaught generic exceptions during the Iceberg scan phase interrupted scan range submission and produced no metrics. [#62994](https://github.com/StarRocks/starrocks/pull/62994)
- Complex multi-layer projected views used in materialized view rewrite produced invalid plans or missing column statistics. [#62918](https://github.com/StarRocks/starrocks/pull/62918) [#62198](https://github.com/StarRocks/starrocks/pull/62198)
- Case mismatch of partition columns in the Hive table-based materialized view was incorrectly rejected. [#62598](https://github.com/StarRocks/starrocks/pull/62598) 
- Materialized view refresh used only the creator’s default role, causing an insufficient privilege issue. [#62396](https://github.com/StarRocks/starrocks/pull/62396) 
- Case-insensitive conflicts in partition names of list-partitioned materialized views led to duplicate name errors. [#62389](https://github.com/StarRocks/starrocks/pull/62389)
- Residual version mapping after failed materialized view restores caused subsequent incremental refresh to be skipped, returning empty results. [#62634](https://github.com/StarRocks/starrocks/pull/62634)
- Abnormal partitions after materialized view restores caused FE restart NullPointerException. [#62563](https://github.com/StarRocks/starrocks/pull/62563)  
- Non-global aggregation queries incorrectly applied the aggregation pushdown rewrite, producing invalid plans. [#63060](https://github.com/StarRocks/starrocks/pull/63060)
- The tablet deletion state was only updated in memory and not persisted, so GC still treated it as running and skipped reclamation. [#63623](https://github.com/StarRocks/starrocks/pull/63623)
- Concurrent query and drop tablet led to early delvec cleanup and "no delete vector found" errors. [#63291](https://github.com/StarRocks/starrocks/pull/63291) 
- An issue with base and cumulative compaction for the Primary Key index sharing the same `max_rss_rowid`. [#63277](https://github.com/StarRocks/starrocks/pull/63277) 
- Possible BE crash when LakePersistentIndex destructor runs after a failed initialization. [#62279](https://github.com/StarRocks/starrocks/pull/62279) 
- Graceful shutdown of Publish thread pool silently discarded queued tasks without marking failures, creating version holes and a false "all succeeded" impression. [#62417](https://github.com/StarRocks/starrocks/pull/62417) 
- The newly cloned replica on a newly added BE during rebalance was immediately judged redundant and removed, preventing data migration to the new node. [#62542](https://github.com/StarRocks/starrocks/pull/62542) 
- Missing lock when reading the tablet's maximum version caused inconsistent replication transaction decisions. [#62238](https://github.com/StarRocks/starrocks/pull/62238)
- A combination of `date_trunc` equality and raw column range predicate was reduced to a point interval, returning empty result sets (for example, `date_trunc('month', dt)='2025-09-01' AND dt>'2025-09-23'`). [#63464](https://github.com/StarRocks/starrocks/pull/63464) 
- Pushdown of non-deterministic predicates (random/time functions) produced inconsistent results. [#63495](https://github.com/StarRocks/starrocks/pull/63495) 
- Missing consumer node after CTE reuse decision produced incomplete execution plans. [#62784](https://github.com/StarRocks/starrocks/pull/62784) 
- Type mismatch crashes when table functions and low-cardinality dictionary encoding coexist. [#62466](https://github.com/StarRocks/starrocks/pull/62466) [#62292](https://github.com/StarRocks/starrocks/pull/62292) 
- Oversized CSV split into parallel fragments caused every fragment to skip header rows, leading to data loss. [#62719](https://github.com/StarRocks/starrocks/pull/62719) 
- `SHOW CREATE ROUTINE LOAD` without explicit DB returned job from another database with the same name. [#62745](https://github.com/StarRocks/starrocks/pull/62745) 
- NullPointerException when `sameLabelJobs` became null during concurrent load job cleanup. [#63042](https://github.com/StarRocks/starrocks/pull/63042) 
- BE decommission blocked even when all tablets were already in the recycle bin. [#62781](https://github.com/StarRocks/starrocks/pull/62781) 
- `OPTIMIZE TABLE` task stuck in PENDING after thread pool rejection. [#62300](https://github.com/StarRocks/starrocks/pull/62300) 
- Dirty tablet metadata cleanup used GTID arguments in the wrong order. [62275](https://github.com/StarRocks/starrocks/pull/62275) 

## 3.4.7

Release Date: September 1, 2025

### Bug Fixes

The following issues have been fixed:

- Routine Load jobs did not serialize `max_filter_ratio`. [#61755](https://github.com/StarRocks/starrocks/pull/61755)
- In Stream Load, the `now(precision)` function lost the precision parameter. [#61721](https://github.com/StarRocks/starrocks/pull/61721)
- In Audit Log, the Scan Rows result for `INSERT INTO SELECT` statements was inaccurate. [#61381](https://github.com/StarRocks/starrocks/pull/61381)
- After upgrading the cluster to v3.4.5, the `fslib read iops` metric increased compared to before the upgrade. [#61724](https://github.com/StarRocks/starrocks/pull/61724)
- Queries against SQLServer using JDBC Catalog often got stuck. [#61719](https://github.com/StarRocks/starrocks/pull/61719)

## 3.4.6

Release Date: August 7, 2025

### Improvements

- When exporting data to Parquet files using `INSERT INTO FILES`, you can now specify the Parquet version via the [`parquet.version`](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files.md#parquetversion) property to improve compatibility with other tools when reading the exported files. [#60843](https://github.com/StarRocks/starrocks/pull/60843)

### Bug Fixes

The following issues have been fixed:

- Loading jobs failed due to overly coarse lock granularity in `TableMetricsManager`. [#58911](https://github.com/StarRocks/starrocks/pull/58911)
- Case sensitivity issue in column names when loading Parquet data via `FILES()`. [#61059](https://github.com/StarRocks/starrocks/pull/61059)
- Cache did not take effect after upgrading a shared-data cluster from v3.3 to v3.4 or later. [#60973](https://github.com/StarRocks/starrocks/pull/60973)
- A division-by-zero error occurred when the partition ID was null, causing a BE crash. [#60842](https://github.com/StarRocks/starrocks/pull/60842)
- Broker Load jobs failed during BE scaling. [#60224](https://github.com/StarRocks/starrocks/pull/60224)

### Behavior Changes

- The `keyword` column in the `information_schema.keywords` view has been renamed to `word` to align with the MySQL definition. [#60863](https://github.com/StarRocks/starrocks/pull/60863)

## 3.4.5

Release Date: July 10, 2025

### Improvements

- Enhanced observability of loading job execution: Unified the runtime information of loading tasks into the `information_schema.loads` view. Users can view the execution details of all INSERT, Broker Load, Stream Load, and Routine Load subtasks in this view. Additional fields have been added to help users better understand the status of loading tasks and the association with parent jobs (PIPES, Routine Load Jobs).
- Support modifying `kafka_broker_list` via the `ALTER ROUTINE LOAD` statement.

### Bug Fixes

The following issues have been fixed:

- Under high-frequency loading scenarios, Compaction could be delayed. [#59998](https://github.com/StarRocks/starrocks/pull/59998)
- Querying Iceberg external tables via Unified Catalog would throw an error: `not support getting unified metadata table factory`. [#59412](https://github.com/StarRocks/starrocks/pull/59412)
- When using `DESC FILES()` to view CSV files in remote storage, incorrect results were returned because the system mistakenly inferred `xinf` as the FLOAT type. [#59574](https://github.com/StarRocks/starrocks/pull/59574)
- `INSERT INTO` could cause BE to crash when encountering empty partitions. [#59553](https://github.com/StarRocks/starrocks/pull/59553)
- When StarRocks reads Equality Delete files in Iceberg, it could still access deleted data if the data had already been removed from the Iceberg table. [#59709](https://github.com/StarRocks/starrocks/pull/59709)
- Query failures caused by renaming columns. [#59178](https://github.com/StarRocks/starrocks/pull/59178)

### Behavior Changes

- The default value of the BE configuration item `skip_pk_preload` has been changed from `false` to `true`. As a result, the system will skip preloading Primary Key Indexes for Primary Key tables to reduce the likelihood of `Reached Timeout` errors. This change may increase query latency for operations that require loading Primary Key Indexes.

## 3.4.4

Release Date: June 10, 2025

### Improvements

- Storage Volume now supports ADLS2 using Managed Identity as the credential. [#58454](https://github.com/StarRocks/starrocks/pull/58454)
- For [partitions based on complex time function expressions](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/#partitioning-based-on-a-complex-time-function-expression-since-v34), partition pruning works well for partitions based on most DATETIME-related functions
- Supports loading Avro data files from Azure using the `FILES` function. [#58131](https://github.com/StarRocks/starrocks/pull/58131)
- When Routine Load encounters invalid JSON data, the consumed partition and offset information is logged in the error log to facilitate troubleshooting. [#55772](https://github.com/StarRocks/starrocks/pull/55772)

### Bug Fixes

The following issues have been fixed:

- Concurrent queries accessing the same partition in a partitioned table caused Hive Metastore to hang. [#58089](https://github.com/StarRocks/starrocks/pull/58089)
- Abnormal termination of `INSERT` tasks caused the job to remain in the `QUEUEING` state. [#58603](https://github.com/StarRocks/starrocks/pull/58603)
- After upgrading the cluster from v3.4.0 to v3.4.2, a large number of tablet replicas encounter exceptions. [#58518](https://github.com/StarRocks/starrocks/pull/58518)
- FE OOM caused by incorrect `UNION` execution plans. [#59040](https://github.com/StarRocks/starrocks/pull/59040)
- Invalid database IDs during partition recycling could cause FE startup to fail. [#59666](https://github.com/StarRocks/starrocks/pull/59666)
- After a failed FE CheckPoint operation, the process could not exit properly, resulting in blocking. [#58602](https://github.com/StarRocks/starrocks/pull/58602)

## 3.4.3

Release Date: April 30, 2025

### Improvements

- Routine Load and Stream Load support the use of Lambda expressions in the `columns` parameter for complex column data extraction. `array_filter`/`map_filter` can be used to filter and extract ARRAY/MAP data. Complex filtering and extraction of JSON data can be achieved by combining the `cast` function to convert JSON array/JSON object to ARRAY and MAP types. For example, `COLUMNS (js, col=array_filter(i -> json_query(i, '$.type')=='t1', cast(js as Array<JSON>))[1])` can extract the first JSON object from the JSON array `js` where `type` is `t1`. [#58149](https://github.com/StarRocks/starrocks/pull/58149)
- Supports converting JSON objects to MAP type using the `cast` function, combined with `map_filter` to extract items from the JSON object that meet specific conditions. For example, `map_filter((k, v) -> json_query(v, '$.type') == 't1', cast(js AS MAP<String, JSON>))` can extract the JSON object from `js` where `type` is `t1`. [#58045](https://github.com/StarRocks/starrocks/pull/58045)
- LIMIT is now supported when querying the `information_schema.task_runs` view. [#57404](https://github.com/StarRocks/starrocks/pull/57404)

### Bug Fixes

The following issues have been fixed:

- Queries against ORC format Hive tables are returned with an error `OrcChunkReader::lazy_seek_to failed. reason = bad read in RleDecoderV2: :readByte`. [#57454](https://github.com/StarRocks/starrocks/pull/57454)
- RuntimeFilter from the upper layer could not be pushed down when querying Iceberg tables that contain Equality Delete files. [#57651](https://github.com/StarRocks/starrocks/pull/57651)
- Enabling the spill-to-disk pre-aggregation strategy causes queries to fail. [#58022](https://github.com/StarRocks/starrocks/pull/58022)
- Queries are returned with an error `ConstantRef-cmp-ConstantRef not supported here, null != 111 should be eliminated earlier`. [#57735](https://github.com/StarRocks/starrocks/pull/57735)
- Query timeout with the `query_queue_pending_timeout_second` parameter while the Query Queue feature is not enabled. [#57719](https://github.com/StarRocks/starrocks/pull/57719)

## 3.4.2

Release Date: April 10, 2025

### Improvements

- FE supports graceful shutdown to improve system availability. When exiting FE via `./stop_fe.sh -g`, FE will first return a 500 status code to the front-end Load Balancer via the `/api/health` API to indicate that it is preparing to shut down, allowing the Load Balancer to switch to other available FE nodes. Meanwhile, FE will continue to run ongoing queries until they finish or timeout (default timeout: 60 seconds). [#56823](https://github.com/StarRocks/starrocks/pull/56823)

### Bug Fixes

The following issues have been fixed:

- Partition pruning might not work if the partition column is a generated column. [#54543](https://github.com/StarRocks/starrocks/pull/54543)
- Incorrect parameter handling in the `concat` function could cause a BE crash during query execution. [#57522](https://github.com/StarRocks/starrocks/pull/57522)
- The `ssl_enable` property did not take effect when using Broker Load to load data. [#57229](https://github.com/StarRocks/starrocks/pull/57229)
- When NULL values exist, querying subfields of STRUCT-type columns could cause a BE crash. [#56496](https://github.com/StarRocks/starrocks/pull/56496)
- When modifying the bucket distribution of a table with the statement `ALTER TABLE {table} PARTITIONS (p1, p1) DISTRIBUTED BY ...`, specifying duplicate partition names could result in failure to delete internally generated temporary partitions. [#57005](https://github.com/StarRocks/starrocks/pull/57005)
- In a shared-data cluster, running `SHOW PROC '/current_queries'` resulted in the error "Error 1064 (HY000): Sending collect query statistics request fails". [#56597](https://github.com/StarRocks/starrocks/pull/56597)
- Running `INSERT OVERWRITE` loading tasks in parallel caused the error "ConcurrentModificationException: null", resulting in loading failure. [#56557](https://github.com/StarRocks/starrocks/pull/56557)
- After upgrading from v2.5.21 to v3.1.17, running multiple Broker Load tasks concurrently could cause exceptions. [#56512](https://github.com/StarRocks/starrocks/pull/56512)

### Behavior Changes

- The default value of the BE configuration item `avro_ignore_union_type_tag` has been changed to `true`, enabling the direct parsing of `["NULL", "STRING"]` as STRING type data, which better aligns with typical user requirements. [#57553](https://github.com/StarRocks/starrocks/pull/57553)
- The default value of the session variable `big_query_profile_threshold` has been changed from 0 to 30 (seconds). [#57177](https://github.com/StarRocks/starrocks/pull/57177)
- A new FE configuration item `enable_mv_refresh_collect_profile` has been added to control whether to collect Profile information during materialized view refresh. The default value is `false` (previously, the system collected Profile by default). [#56971](https://github.com/StarRocks/starrocks/pull/56971)

## 3.4.1 (Yanked)

Release Date: March 12, 2025

:::tip

This version has been taken offline due to metadata loss issues in **shared-data clusters**.

- **Problem**: When there are committed compaction transactions that are not yet been published during a shift of Leader FE node in a shared-data cluster, metadata loss may occur after the shift.

- **Impact scope**: This problem only affects shared-data clusters. Shared-nothing clusters are unaffected.

- **Temporary workaround**: When the Publish task is returned with an error, you can execute `SHOW PROC 'compactions'` to check if there are any partitions that have two compaction transactions with empty `FinishTime`. You can execute `ALTER TABLE DROP PARTITION FORCE` to drop the partitions to avoid Publish tasks getting hang.

:::

### New Features and Enhancements

- Data lake analytics supports Deletion Vector in Delta Lake.
- Supports secure views. By creating a secure view, you can prevent users without the SELECT privilege on the referenced base tables from querying the view (even if they have the SELECT privilege on the view).
- Supports for Sketch HLL ([`ds_hll_count_distinct`](https://docs.starrocks.io/docs/sql-reference/sql-functions/aggregate-functions/ds_hll_count_distinct/)). Compared to `approx_count_distinct`, this function provides higher-precision approximate deduplication.
- Shared-data clusters support automatic snapshot creation for cluster recovery.
- Storage Volume in the shared-data clusters supports Azure Data Lake Storage Gen2.
- Supports SSL authentication for connections to StarRocks via the MySQL protocol, ensuring that data transmitted between the client and the StarRocks cluster cannot be read by unauthorized users.

### Bug Fixes

The following issues have been fixed:

- An issue where OLAP views affected the materialized view processing logic. [#52989](https://github.com/StarRocks/starrocks/pull/52989)
- Write transactions would fail if one replica was not found, regardless of how many replicas had successfully committed. (After the fix, the transaction succeeds as long as the majority replicas succeed. [#55212](https://github.com/StarRocks/starrocks/pull/55212)
- Stream Load fails when a node with an Alive status of false was scheduled. [#55371](https://github.com/StarRocks/starrocks/pull/55371)
- Files in cluster snapshots were mistakenly deleted. [#56338](https://github.com/StarRocks/starrocks/pull/56338)

### Behavior Changes

- Graceful shutdown is now enabled by default (previously it was disabled). The default value of the related BE/CN parameter `loop_count_wait_fragments_finish` has been changed to `2`, meaning that the system will wait up to 20 seconds for running queries to complete. [#56002](https://github.com/StarRocks/starrocks/pull/56002)

## 3.4.0

Release date: January 24, 2025

### Data Lake Analytics

- Optimized Iceberg V2 query performance and lowered memory usage by reducing repeated reads of delete-files.
- Supports column mapping for Delta Lake tables, allowing queries against data after Delta Schema Evolution. For more information, see [Delta Lake catalog - Feature support](https://docs.starrocks.io/docs/data_source/catalog/deltalake_catalog/#feature-support).
- Data Cache related improvements:
  - Introduces a Segmented LRU (SLRU) Cache eviction strategy, which significantly defends against cache pollution from occasional large queries, improves cache hit rate, and reduces fluctuations in query performance. In simulated test cases with large queries, SLRU-based query performance can be improved by 70% or even higher. For more information, see [Data Cache - Cache replacement policies](https://docs.starrocks.io/docs/data_source/data_cache/#cache-replacement-policies).
  - Unified the Data Cache instance used in both shared-data architecture and data lake query scenarios to simplify the configuration and improve resource utilization. For more information, see [Data Cache](https://docs.starrocks.io/docs/using_starrocks/caching/block_cache/).
  - Provides an adaptive I/O strategy optimization for Data Cache, which flexibly routes some query requests to remote storage based on the cache disk's load and performance, thereby enhancing overall access throughput.
  - Supports persistence of data in Data Cache in Data Lake query scenarios. The previously cached data can be reused after BE restarts to reduce query performance fluctuations.
- Supports automatic collection of external table statistics through automatic ANALYZE tasks triggered by queries. It can provide more accurate NDV information compared to metadata files, thereby optimizing the query plan and improving query performance. For more information, see [Query-triggered collection](https://docs.starrocks.io/docs/using_starrocks/Cost_based_optimizer/#query-triggered-collection).
- Provides Time Travel query capability for Iceberg, allowing data to be read from a specified BRANCH or TAG by specifying TIMESTAMP or VERSION.
- Supports asynchronous delivery of query fragments for data lake queries. It avoids the restriction that FE must obtain all files to be queried before BE can execute a query, thus allowing FE to fetch query files and BE to execute queries in parallel, and reducing the overall latency of data lake queries involving a large number of files that are not in the cache. Meanwhile, it reduces the memory load on FE due to caching the file list and improves query stability. (Currently, the optimization for Hudi and Delta Lake is implemented, while the optimization for Iceberg is still under development.)

### Performance Improvement and Query Optimization

- [Experimental] Offers a preliminary Query Feedback feature for automatic optimization of slow queries. The system will collect the execution details of slow queries, automatically analyze its query plan for potential opportunities for optimization, and generate a tailored optimization guide for the query. If CBO generates the same bad plan for subsequent identical queries, the system will locally optimize this query plan based on the guide. For more information, see [Query Feedback](https://docs.starrocks.io/docs/using_starrocks/query_feedback/).
- [Experimental] Supports Python UDFs, offering more convenient function customization compared to Java UDFs. For more information, see [Python UDF](https://docs.starrocks.io/docs/sql-reference/sql-functions/Python_UDF/).
- Enables the pushdown of multi-column OR predicates, allowing queries with multi-column OR conditions (for example, `a = xxx OR b = yyy`) to utilize certain column indexes, thus reducing data read volume and improving query performance.
- Optimized TPC-DS query performance by roughly 20% under the TPC-DS 1TB Iceberg dataset. Optimization methods include table pruning and aggregated column pruning using primary and foreign keys, and aggregation pushdown.

### Shared-data Enhancements

- Supports Query Cache, aligning the shared-nothing architecture.
- Supports synchronous materialized views, aligning the shared-nothing architecture.

### Storage Engine

- Unified all partitioning methods into the expression partitioning and supported multi-level partitioning, where each level can be any expression. For more information, see [Expression Partitioning](https://docs.starrocks.io/docs/table_design/data_distribution/expression_partitioning/).
- [Preview] Supports all native aggregate functions in Aggregate tables. By introducing a generic aggregate function state storage framework, all native aggregate functions supported by StarRocks can be used to define an Aggregate table.
- Supports vector indexes, enabling fast approximate nearest neighbor searches (ANNS) of large-scale, high-dimensional vectors, which are commonly required in deep learning and machine learning scenarios. Currently, StarRocks supports two types of vector indexes: IVFPQ and HNSW.

### Loading

- INSERT OVERWRITE now supports a new semantic - Dynamic Overwrite. When this semantic is enabled, the ingested data will either create new partitions or overwrite existing partitions that correspond to the new data records. Partitions not involved will not be truncated or deleted. This semantic is especially useful when users want to recover data in specific partitions without specifying the partition names. For more information, see [Dynamic Overwrite](https://docs.starrocks.io/docs/loading/InsertInto/#dynamic-overwrite).
- Optimized the data ingestion with INSERT from FILES to replace Broker Load as the preferred loading method:
  - FILES now supports listing files in remote storage, and providing basic statistics of the files. For more information, see [FILES - list_files_only](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#list_files_only).
  - INSERT now supports matching columns by name, which is especially useful when users load data from numerous columns with identical names. (The default behavior matches columns by their position.) For more information, see [Match column by name](https://docs.starrocks.io/docs/loading/InsertInto/#match-column-by-name).
  - INSERT supports specifying PROPERTIES, aligning with other loading methods. Users can specify `strict_mode`, `max_filter_ratio`, and `timeout` for INSERT operations to control and behavior and quality of the data ingestion. For more information, see [INSERT - PROPERTIES](https://docs.starrocks.io/docs/sql-reference/sql-statements/loading_unloading/INSERT/#properties).
  - INSERT from FILES supports pushing down the target table schema check to the Scan stage of FILES to infer a more accurate source data schema. For more information, see see [Push down target table schema check](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#push-down-target-table-schema-check).
  - FILES supports unionizing files with different schema. The schema of Parquet and ORC files are unionized based on the column names, and that of CSV files are unionized based on the position (order) of the columns. When there are mismatched columns, users can choose to fill the columns with NULL or return an error by specifying the property `fill_mismatch_column_with`. For more information, see [Union files with different schema](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#union-files-with-different-schema).
  - FILES supports inferring the STRUCT type data from Parquet files. (In earlier versions, STRUCT data is inferred as STRING type.) For more information, see [Infer STRUCT type from Parquet](https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/#infer-struct-type-from-parquet).
- Supports merging multiple concurrent Stream Load requests into a single transaction and committing data in a batch, thus improving the throughput of real-time data ingestion. It is designed for high concurrency, small-batch (from KB to tens of MB) real-time loading scenarios. It can reduce the excessive data versions caused by frequent loading operations, resource consumption during Compaction, and IOPS and I/O latency brought by excessive small files.

### Others

- Optimized the graceful exit process of BE and CN by accurately displaying the status of BE or CN nodes during a graceful exit as `SHUTDOWN`.
- Optimized log printing to avoid excessive disk space being occupied.
- Shared-nothing clusters now support backing up and restoring more objects: logical view, external catalog metadata, and partitions created with expression partitioning and list partitioning strategies.
- [Preview] Supports CheckPoint on Follower FE to avoid excessive memory on Leader FE during CheckPoint, thereby improving the stability of Leader FE.

### Behavior Changes

- Because the Data Cache instance used in both shared-data architecture and data lake query scenarios is now unified, there will be the following behavior changes after the upgrade to v3.4.0:

  - BE configuration item `datacache_disk_path` is now deprecated. The data will be cached under the directory `${storage_root_path}/datacache`. If you want to allocate a dedicated disk for data cache, you can manually point the directory to the directory mentioned above using a symlink.
  - Cached data in the shared-data cluster will be automatically migrated to `${storage_root_path}/datacache` and can be re-used after the upgrade.
  - The behavior changes of `datacache_disk_size`:

    - When `datacache_disk_size` is `0` (Default), the automatic adjustment of cache capacity is enabled (consistent with the behavior before the upgrade).
    - When `datacache_disk_size` is set to a value greater than `0`, the system will pick a larger value between `datacache_disk_size` and  `starlet_star_cache_disk_size_percent` as the cache capacity.

- From v3.4.0 onwards, `insert_timeout` applies to operations involved INSERT (for example, UPDATE, DELETE, CTAS, materialized view refresh, statistics collection, and PIPE), replacing `query_timeout`.
- From v3.4.0 onwards, the default value of `mysql_server_version` is changed to `8.0.33`.

### Downgrade Notes

- Clusters can be downgraded from v3.4.0 only to v3.3.9 and later.
