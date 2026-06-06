---
displayed_sidebar: docs
description: "StarRocks 4.1 release notes: multi-tenant range-based tablet auto-splitting, large-capacity tablet support (100 GB target), Fast Schema Evolution V2 for..."
---

# StarRocks version 4.1

:::danger

**Container Image Issue (v4.1.0)**

Due to an unstable load order issue in the v4.1.0 container image, BE processes may fail to start reliably in container environments. **Container environment users should NOT upgrade to v4.1.0.** Please wait for v4.1.1, which includes the fix ([#71825](https://github.com/StarRocks/starrocks/pull/71825)).

:::

:::warning

**Downgrade Notes**

- After upgrading StarRocks to v4.1, DO NOT downgrade to any v4.0 version below v4.0.6.

  Due to internal changes in data layout introduced in v4.1 (related to tablet splitting and distribution mechanisms), clusters upgraded to v4.1 may generate metadata and storage structures that are not fully compatible with earlier versions. As a result, downgrade from v4.1 is only supported to v4.0.6 or later. Downgrading to versions prior to v4.0.6 is not supported. This limitation is due to backward compatibility constraints in how earlier versions interpret tablet layout and distribution metadata.

:::

## 4.1.1

Release Date: May 29, 2026

### Behavior Changes

- The Hive connector now uses a native C++ Avro scanner instead of the JNI Avro scanner by default. [#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- Query rewrite over INCREMENTAL/AUTO materialized views is now disabled, and FORCE refresh and partition refresh are rejected for INCREMENTAL/AUTO materialized views. [#72890](https://github.com/StarRocks/starrocks/pull/72890) [#72336](https://github.com/StarRocks/starrocks/pull/72336) [#71355](https://github.com/StarRocks/starrocks/pull/71355)

### Improvements

- Java UDF/UDAF/UDTF now support more types: STRUCT arguments and return values for UDAF/UDTF, nested ARRAY/MAP types, DATE/DATETIME, DECIMAL, and varargs. [#72911](https://github.com/StarRocks/starrocks/pull/72911) [#72283](https://github.com/StarRocks/starrocks/pull/72283) [#72337](https://github.com/StarRocks/starrocks/pull/72337) [#72208](https://github.com/StarRocks/starrocks/pull/72208) [#68596](https://github.com/StarRocks/starrocks/pull/68596)
- Scalar UDFs now support STRUCT arguments. [#72620](https://github.com/StarRocks/starrocks/pull/72620)
- Python UDFs now support nested ARRAY/MAP types. [#72210](https://github.com/StarRocks/starrocks/pull/72210)
- UDAFs are now loaded and initialized once and reused across queries, reducing per-query overhead. [#72038](https://github.com/StarRocks/starrocks/pull/72038)
- Replaced the JNI Avro scanner with a native C++ scanner for the Hive connector, with direct binary decoding and support for `avro.schema.literal` and `avro.schema.url`. [#73237](https://github.com/StarRocks/starrocks/pull/73237) [#73283](https://github.com/StarRocks/starrocks/pull/73283) [#73257](https://github.com/StarRocks/starrocks/pull/73257) [#73569](https://github.com/StarRocks/starrocks/pull/73569)
- Supports the Trino `WITH` clause in CTAS statements. [#71960](https://github.com/StarRocks/starrocks/pull/71960)
- Completed Iceberg `timestamptz` partition transform support on the sink path. [#73397](https://github.com/StarRocks/starrocks/pull/73397)
- Enabled TopN runtime filter pushdown for Iceberg table aggregation. [#72332](https://github.com/StarRocks/starrocks/pull/72332)
- Supports Iceberg datetime min/max optimization. [#71870](https://github.com/StarRocks/starrocks/pull/71870)
- Allows HDFS HA configuration passthrough in Catalog and BE to support accessing multiple HDFS clusters. [#71521](https://github.com/StarRocks/starrocks/pull/71521)
- Added a partition scan number limit for external table queries. [#68480](https://github.com/StarRocks/starrocks/pull/68480)
- Fails fast for unsupported Iceberg V3 features. [#70242](https://github.com/StarRocks/starrocks/pull/70242)
- Supports `csv.enclose` and `csv.escape` for CSV exports via INSERT INTO FILES. [#71589](https://github.com/StarRocks/starrocks/pull/71589)
- Added the `enable_push_down_schema` INSERT property for full schema push-down to `files()`. [#70978](https://github.com/StarRocks/starrocks/pull/70978)
- Routine Load jobs are now paused on non-retryable errors (for example, primary key size exceeded). [#71161](https://github.com/StarRocks/starrocks/pull/71161)
- Supports join reorder for complex expressions from two children. [#71615](https://github.com/StarRocks/starrocks/pull/71615)
- Improved CBO statistics estimation, including MCV/null-fraction propagation for `date_trunc`, `array_map`, CASE WHEN, IS NULL, UNION, and constants. [#72233](https://github.com/StarRocks/starrocks/pull/72233) [#70372](https://github.com/StarRocks/starrocks/pull/70372) [#70221](https://github.com/StarRocks/starrocks/pull/70221) [#70865](https://github.com/StarRocks/starrocks/pull/70865) [#70989](https://github.com/StarRocks/starrocks/pull/70989) [#71000](https://github.com/StarRocks/starrocks/pull/71000)
- Improved skew join detection: skew is only detected when all join keys are skewed, and a `force_group_by_skew_eliminate_when_skewed` switch was added to force the skew rule. [#72753](https://github.com/StarRocks/starrocks/pull/72753) [#71382](https://github.com/StarRocks/starrocks/pull/71382)
- Supports constant folding for `regexp_replace` in the FE. [#70804](https://github.com/StarRocks/starrocks/pull/70804)
- Optimized MIN/MAX on date partition columns with constant partition values. [#69880](https://github.com/StarRocks/starrocks/pull/69880)
- Introduced the `SCHEDULE` keyword as a synonym for `ASYNC` in materialized view refresh. [#72329](https://github.com/StarRocks/starrocks/pull/72329)
- Supports tablet creation retry for Lake tables in shared-data mode. [#71068](https://github.com/StarRocks/starrocks/pull/71068)
- Supports conditional update for Lake column-mode partial update. [#71961](https://github.com/StarRocks/starrocks/pull/71961)
- Parallelized partial-update publish, persistent index initialization, and SSTable opening to improve ingestion throughput. [#71652](https://github.com/StarRocks/starrocks/pull/71652) [#71217](https://github.com/StarRocks/starrocks/pull/71217) [#72112](https://github.com/StarRocks/starrocks/pull/72112) [#71145](https://github.com/StarRocks/starrocks/pull/71145) [#72986](https://github.com/StarRocks/starrocks/pull/72986)
- Supports DCG file synchronization during shared-nothing to shared-data replication. [#69339](https://github.com/StarRocks/starrocks/pull/69339)
- Supports schema evolution for widening VARCHAR length on both key and non-key columns. [#70747](https://github.com/StarRocks/starrocks/pull/70747)
- Added the `snapshot_meta.json` marker for cluster snapshot integrity checks. [#71209](https://github.com/StarRocks/starrocks/pull/71209)
- Supports LDAP direct bind authentication via a DN pattern. [#71559](https://github.com/StarRocks/starrocks/pull/71559)
- Added the `get_query_dump_from_query_id` meta function for easier query troubleshooting. [#72875](https://github.com/StarRocks/starrocks/pull/72875)
- Supports auditing queried relations in the audit log. [#71596](https://github.com/StarRocks/starrocks/pull/71596)
- Added session variables for MySQL binary result encoding. [#71415](https://github.com/StarRocks/starrocks/pull/71415)
- Added metrics for better observability, including `tablet_num` for shared-data clusters, `MemtableIOSpeed`, `staros_shard_count`, and Iceberg metadata-table query metrics. [#71444](https://github.com/StarRocks/starrocks/pull/71444) [#69842](https://github.com/StarRocks/starrocks/pull/69842) [#73096](https://github.com/StarRocks/starrocks/pull/73096) [#70825](https://github.com/StarRocks/starrocks/pull/70825)
- Added the FE configuration `deploy_serialization_min_thread_pool_size`. [#72274](https://github.com/StarRocks/starrocks/pull/72274)
- Added the `tablet_reshard_enable_tablet_merge` configuration to disable MergeTabletJob creation. [#70906](https://github.com/StarRocks/starrocks/pull/70906)
- Eliminated HTTP-server accept thundering-herd via `SO_REUSEPORT`. [#72956](https://github.com/StarRocks/starrocks/pull/72956)

### Security

- [CVE] Upgraded Netty to 4.1.133.Final. [#72905](https://github.com/StarRocks/starrocks/pull/72905)
- [CVE-2026-42198] [CVE-2026-5598] Bumped pgjdbc to 42.7.11 (client-side DoS via unbounded SCRAM PBKDF2 iteration count) and BouncyCastle to 1.84 (FrodoKEM private-key leakage). [#72797](https://github.com/StarRocks/starrocks/pull/72797)
- [CVE-2026-32280] [CVE-2026-32282] Built pprof with go1.25.9 to eliminate Golang CVEs. [#71944](https://github.com/StarRocks/starrocks/pull/71944) [#73545](https://github.com/StarRocks/starrocks/pull/73545)
- Upgraded jetty-http to 9.4.58.v20250814. [#71762](https://github.com/StarRocks/starrocks/pull/71762)
- Cleaned up Broker dependency CVEs and removed `wildfly-openssl`. [#72184](https://github.com/StarRocks/starrocks/pull/72184) [#71908](https://github.com/StarRocks/starrocks/pull/71908)
- Redacted credentials in INSERT INTO FILES error messages. [#71245](https://github.com/StarRocks/starrocks/pull/71245)

### Bug Fixes

The following issues have been fixed:

- CN segfault on startup caused by `hash_util` static initialization order. [#71825](https://github.com/StarRocks/starrocks/pull/71825)
- CN crash when scanning an empty tablet with physical split enabled. [#70281](https://github.com/StarRocks/starrocks/pull/70281)
- BE crash when querying `information_schema.warehouse_queries`. [#72019](https://github.com/StarRocks/starrocks/pull/72019)
- SIGFPE in Lake compaction when rowset `num_rows` is zero. [#71742](https://github.com/StarRocks/starrocks/pull/71742)
- Division-by-zero in ExecutionDAG fragment connection. [#67918](https://github.com/StarRocks/starrocks/pull/67918)
- Graceful-exit crash in SinkBuffer. [#73202](https://github.com/StarRocks/starrocks/pull/73202)
- Spillable hash join probe crash. [#72397](https://github.com/StarRocks/starrocks/pull/72397)
- Stack-buffer-overflow when formatting into a temporary `std::string`. [#72728](https://github.com/StarRocks/starrocks/pull/72728)
- Crash in `reverse(DecimalV3)`. [#71834](https://github.com/StarRocks/starrocks/pull/71834)
- Use-after-free in `LoadChannel::get_load_replica_status` caused by temporary `shared_ptr` destruction. [#71843](https://github.com/StarRocks/starrocks/pull/71843)
- Use-after-free in `ThreadPool::do_submit` when thread creation fails. [#71276](https://github.com/StarRocks/starrocks/pull/71276)
- Hive partition descriptor use-after-free across fragment teardown. [#73176](https://github.com/StarRocks/starrocks/pull/73176)
- An information schema sink use-after-free. [#71513](https://github.com/StarRocks/starrocks/pull/71513)
- An FE file descriptor leak by reusing HttpClient instances. [#73239](https://github.com/StarRocks/starrocks/pull/73239)
- JNI local-reference leak in `JDBCScanner::_init_jdbc_scanner`. [#72913](https://github.com/StarRocks/starrocks/pull/72913)
- Memory leak when caching the MV plan context. [#72300](https://github.com/StarRocks/starrocks/pull/72300)
- Unexpected memory overuse in local exchange. [#72262](https://github.com/StarRocks/starrocks/pull/72262)
- Race on `response->tablet_metas` in Lake `publish_version`. [#73274](https://github.com/StarRocks/starrocks/pull/73274)
- Concurrent `SegmentFlushTask` race in `DeltaWriter::commit()`. [#73371](https://github.com/StarRocks/starrocks/pull/73371)
- `RuntimeProfile` min/max race during serialization. [#72904](https://github.com/StarRocks/starrocks/pull/72904)
- Race condition in `PipelineTimerTask` during query context destruction. [#73082](https://github.com/StarRocks/starrocks/pull/73082)
- Race condition in `_all_global_rf_ready_or_timeout`. [#70920](https://github.com/StarRocks/starrocks/pull/70920)
- Shared `NullColumn` issue in `map_apply` and `array_length`. [#71258](https://github.com/StarRocks/starrocks/pull/71258)
- Batch-publish deadlock caused by a partition version gap. [#71483](https://github.com/StarRocks/starrocks/pull/71483)
- Deadlock when warming up the LRU cache for rowset metadata in shared-nothing mode. [#71459](https://github.com/StarRocks/starrocks/pull/71459)
- `Locker` rollback is not exception-safe and unlock order is incorrect. [#72789](https://github.com/StarRocks/starrocks/pull/72789)
- Lock contention with DDL and StarOS RPCs caused by several DB locks on read-only and metadata paths. [#73067](https://github.com/StarRocks/starrocks/pull/73067) [#72475](https://github.com/StarRocks/starrocks/pull/72475) [#72108](https://github.com/StarRocks/starrocks/pull/72108) [#72218](https://github.com/StarRocks/starrocks/pull/72218) [#72178](https://github.com/StarRocks/starrocks/pull/72178)
- Incorrect shuffle distribution due to a missing project node. [#71075](https://github.com/StarRocks/starrocks/pull/71075)
- An AGG TopN runtime filter `exprOrder` mismatch causing crashes and wrong results. [#71479](https://github.com/StarRocks/starrocks/pull/71479)
- Wrong results from dict-merge GROUP BY. [#70866](https://github.com/StarRocks/starrocks/pull/70866)
- Query cache conflicts with local shuffle aggregation. [#73194](https://github.com/StarRocks/starrocks/pull/73194)
- Inconsistent global dictionary generation in flat JSON. [#72953](https://github.com/StarRocks/starrocks/pull/72953)
- Flat JSON merge empty inconsistency. [#72973](https://github.com/StarRocks/starrocks/pull/72973)
- A type mismatch in map literals when explicit key/value types are declared. [#71316](https://github.com/StarRocks/starrocks/pull/71316)
- COALESCE children is not casted to a common type in the JOIN USING transformer. [#72338](https://github.com/StarRocks/starrocks/pull/72338)
- VARCHAR length is not preserved after reduce-cast with global variables. [#70269](https://github.com/StarRocks/starrocks/pull/70269)
- VARBINARY is incorrectly encoded inside nested types in MySQL result sets. [#71346](https://github.com/StarRocks/starrocks/pull/71346)
- Check-having-clause issue when disabling aggregation spill on small LIMIT. [#72705](https://github.com/StarRocks/starrocks/pull/72705)
- Quotes are not stripped before date parsing, and PostgreSQL date/time bug. [#48517](https://github.com/StarRocks/starrocks/pull/48517) [#71016](https://github.com/StarRocks/starrocks/pull/71016)
- Data loss after a tablet split by skipping data-file deletion for range distribution tablets. [#71135](https://github.com/StarRocks/starrocks/pull/71135)
- Data file shared-flag loss that caused vacuum to delete files still referenced by sibling split tablets. [#71585](https://github.com/StarRocks/starrocks/pull/71585)
- Tablet merge correctness for the split→compaction→merge sequence. [#72350](https://github.com/StarRocks/starrocks/pull/72350)
- Cross-published txn log num_rows/data_size inflation during tablet splits. [#71144](https://github.com/StarRocks/starrocks/pull/71144)
- Delvec orphan entries caused by write-before-compaction in the same publish batch. [#71001](https://github.com/StarRocks/starrocks/pull/71001)
- "no queryable replica" on follower FE by syncing the StarMgr journal replay. [#71263](https://github.com/StarRocks/starrocks/pull/71263)
- `merge_condition` is not preserved when applying a normal rowset commit. [#72542](https://github.com/StarRocks/starrocks/pull/72542)
- Iceberg DELETE conflict detection using an incorrect snapshot ID and filter. [#73354](https://github.com/StarRocks/starrocks/pull/73354)
- An NPE on invalid Iceberg transform arguments. [#71917](https://github.com/StarRocks/starrocks/pull/71917)
- Iceberg min/max optimization being skipped due to extra columns injected by the planner. [#71863](https://github.com/StarRocks/starrocks/pull/71863)
- Aggregate-join-pushdown MV rewrite on Iceberg base tables. [#71856](https://github.com/StarRocks/starrocks/pull/71856)
- Missing Hive partition directory before INSERT OVERWRITE commit. [#71810](https://github.com/StarRocks/starrocks/pull/71810)
- AWS assume-role not being applied for the JNI scanner. [#71422](https://github.com/StarRocks/starrocks/pull/71422)
- Avro complex-type decoding for pruned children and nested nullable schemas. [#73474](https://github.com/StarRocks/starrocks/pull/73474)
- File/column/row context is missing in Parquet Broker Load errors. [#73236](https://github.com/StarRocks/starrocks/pull/73236)
- Lacking support for Arrow dictionary values in the Parquet scanner. [#71855](https://github.com/StarRocks/starrocks/pull/71855)
- The Primary Key for Paimon tables is not shown in SHOW CREATE and DESC returns. [#70535](https://github.com/StarRocks/starrocks/pull/70535)
- PostgreSQL/Oracle JDBC type compatibility and JDBC URL construction with trailing slashes. [#70626](https://github.com/StarRocks/starrocks/pull/70626) [#70992](https://github.com/StarRocks/starrocks/pull/70992)
- Materialized view refresh issue with SQL Server tables in a JDBC catalog. [#72962](https://github.com/StarRocks/starrocks/pull/72962)
- Lazy-materialization slot nullability issue for materialized views over outer joins. [#72621](https://github.com/StarRocks/starrocks/pull/72621)
- Rejected AUTO and INCREMENTAL materialized view partition refresh. [#71355](https://github.com/StarRocks/starrocks/pull/71355)
- The materialized view scheduler is not stopped after a materialized view becomes inactive. [#71265](https://github.com/StarRocks/starrocks/pull/71265)
- Lacking support for `SHOW GRANTS FOR CURRENT_USER()` for MySQL client compatibility. [#71959](https://github.com/StarRocks/starrocks/pull/71959)
- SHOW statements are not allowed inside an explicit transaction. [#72954](https://github.com/StarRocks/starrocks/pull/72954)
- Arrow Flight returning the column name `r` for an empty result set. [#71534](https://github.com/StarRocks/starrocks/pull/71534)
- Lacking JNI exception-handling checks in Java UDF code. [#71734](https://github.com/StarRocks/starrocks/pull/71734)
- `ai_query` function registration issue. [#72103](https://github.com/StarRocks/starrocks/pull/72103)
- Stream Load profile collection issue when `enable_load_profile` is used. [#71952](https://github.com/StarRocks/starrocks/pull/71952)
- Profile START_TIME/END_TIME is not displayed with the session timezone. [#71429](https://github.com/StarRocks/starrocks/pull/71429)
- `star_mgr_meta_sync_interval_sec` is not runtime mutable. [#71675](https://github.com/StarRocks/starrocks/pull/71675)
- `information_schema.tables` not escaping special characters in equality predicates. [#71273](https://github.com/StarRocks/starrocks/pull/71273)

## 4.1.0

Release Date: April 13, 2026

### Shared-data Architecture

- **New Multi-Tenant Data Management**

  Shared-data clusters now support range-based data distribution and automatic splitting and merging of tablets. Tablets can be automatically split when they become oversized or hotspots, without requiring schema changes, SQL modifications, or data re-ingestion. This feature can significantly improve usability, directly addressing data skew and hotspot issues in multi-tenant workloads. [#65199](https://github.com/StarRocks/starrocks/pull/65199) [#66342](https://github.com/StarRocks/starrocks/pull/66342) [#67056](https://github.com/StarRocks/starrocks/pull/67056) [#67386](https://github.com/StarRocks/starrocks/pull/67386) [#68342](https://github.com/StarRocks/starrocks/pull/68342) [#68569](https://github.com/StarRocks/starrocks/pull/68569) [#66743](https://github.com/StarRocks/starrocks/pull/66743) [#67441](https://github.com/StarRocks/starrocks/pull/67441) [#68497](https://github.com/StarRocks/starrocks/pull/68497) [#68591](https://github.com/StarRocks/starrocks/pull/68591) [#66672](https://github.com/StarRocks/starrocks/pull/66672) [#69155](https://github.com/StarRocks/starrocks/pull/69155)

- **Large-Capacity Tablet Support (Phase 1)**

  Enables shared-data clusters to host significantly more data per tablet, with a long-term target of 100 GB per tablet. Phase 1 introduces intra-tablet parallelism across the entire ingestion, Primary Key update, and Compaction pipeline, so a single Lake tablet no longer becomes a single-threaded bottleneck as it grows. Improvements include parallel Compaction within a single tablet (with segment-level splitting), parallel MemTable finalization, flush, and merge for Lake loading (including the load-spill path), tablet-internal parallel publish and parallel condition updates for Primary Key tables, and range-split / parallel / size-tiered Compaction for the cloud-native Primary Key index with remote-storage mapper file support. Together, these changes substantially reduce ingestion memory overhead, compaction amplification, and FE metadata pressure for large-tablet workloads. [#66424](https://github.com/StarRocks/starrocks/pull/66424) [#66522](https://github.com/StarRocks/starrocks/pull/66522) [#66778](https://github.com/StarRocks/starrocks/pull/66778) [#66586](https://github.com/StarRocks/starrocks/pull/66586) [#67432](https://github.com/StarRocks/starrocks/pull/67432) [#67478](https://github.com/StarRocks/starrocks/pull/67478) [#67554](https://github.com/StarRocks/starrocks/pull/67554) [#66796](https://github.com/StarRocks/starrocks/pull/66796) [#67392](https://github.com/StarRocks/starrocks/pull/67392) [#67878](https://github.com/StarRocks/starrocks/pull/67878) [#65908](https://github.com/StarRocks/starrocks/pull/65908) [#68677](https://github.com/StarRocks/starrocks/pull/68677) [#68123](https://github.com/StarRocks/starrocks/pull/68123) [#69865](https://github.com/StarRocks/starrocks/pull/69865)

- **Fast Schema Evolution V2**

  Shared-data clusters now support Fast Schema Evolution V2, which enables second-level DDL execution for schema operations, and further extends the support to materialized views. [#65726](https://github.com/StarRocks/starrocks/pull/65726) [#66774](https://github.com/StarRocks/starrocks/pull/66774) [#67915](https://github.com/StarRocks/starrocks/pull/67915)

- **[Beta] Inverted Index on shared-data**

  Enables built-in inverted indexes for shared-data clusters to accelerate text filtering and full-text search workloads. [#66541](https://github.com/StarRocks/starrocks/pull/66541)

- **Cache Observability**

  Query-level cache hit ratio is now exposed in audit logs and the monitoring system for better cache transparency and latency diagnosis. Additional Data Cache metrics include memory and disk quota usage, and page cache statistics. [#63964](https://github.com/StarRocks/starrocks/pull/63964)

- Added segment metadata filter for Lake tables to skip irrelevant segments based on sort key range during scans, reducing I/O for range-predicate queries. [#68124](https://github.com/StarRocks/starrocks/pull/68124)
- Supports fast cancel for Lake DeltaWriter, reducing latency for cancelled ingestion jobs in shared-data clusters. [#68877](https://github.com/StarRocks/starrocks/pull/68877)
- Added support for interval-based scheduling for automated cluster snapshots. [#67525](https://github.com/StarRocks/starrocks/pull/67525)
- Supports pipeline execution for MemTable flush and merge, improving ingestion throughput for cloud-native tables in shared-data clusters. [#67878](https://github.com/StarRocks/starrocks/pull/67878)
- Supports `dry_run` mode for repairing cloud-native tables, allowing users to preview repair actions before execution. [#68494](https://github.com/StarRocks/starrocks/pull/68494)
- Added a thread pool for publish transactions in shared-nothing clusters, improving publish throughput. [#67797](https://github.com/StarRocks/starrocks/pull/67797)
- Supports dynamically modifying the `datacache.enable` property for cloud-native tables. [#69011](https://github.com/StarRocks/starrocks/pull/69011)

### Data Lake Analytics

- **Iceberg DELETE Support**

  Supports writing position delete files for Iceberg tables, enabling DELETE operations on Iceberg tables directly from StarRocks. The support covers the full pipeline of Plan, Sink, Commit, and Audit. [#67259](https://github.com/StarRocks/starrocks/pull/67259) [#67277](https://github.com/StarRocks/starrocks/pull/67277) [#67421](https://github.com/StarRocks/starrocks/pull/67421) [#67567](https://github.com/StarRocks/starrocks/pull/67567)

- **TRUNCATE for Hive and Iceberg Tables**

  Supports TRUNCATE TABLE on external Hive and Iceberg tables. [#64768](https://github.com/StarRocks/starrocks/pull/64768) [#65016](https://github.com/StarRocks/starrocks/pull/65016)

- **Incremental materialized view on Iceberg**

  Extends the support for incremental materialized view refresh to Iceberg append-only tables, enabling query acceleration without full table refresh. [#65469](https://github.com/StarRocks/starrocks/pull/65469) [#62699](https://github.com/StarRocks/starrocks/pull/62699)

- **VARIANT Type for Semi-Structured Data in Iceberg**

  Supports the VARIANT data type in Iceberg Catalog for flexible, schema-on-read storage and querying of semi-structured data. Supports read, write, type casting, and Parquet integration. [#63639](https://github.com/StarRocks/starrocks/pull/63639) [#66539](https://github.com/StarRocks/starrocks/pull/66539)

- **Iceberg v3 Support**

  Added support for Iceberg v3 default value feature and row lineage. [#69525](https://github.com/StarRocks/starrocks/pull/69525) [#69633](https://github.com/StarRocks/starrocks/pull/69633)

- **Iceberg Table Maintenance Procedures**

  Added support for `rewrite_manifests` procedure and extended `expire_snapshots` and `remove_orphan_files` procedures with additional arguments for finer-grained table maintenance. [#68817](https://github.com/StarRocks/starrocks/pull/68817) [#68898](https://github.com/StarRocks/starrocks/pull/68898)

- **Iceberg `$properties` Metadata Table**

  Added support for querying Iceberg table properties via the `$properties` metadata table. [#68504](https://github.com/StarRocks/starrocks/pull/68504)

- Supports reading file path and row position metadata columns from Iceberg tables. [#67003](https://github.com/StarRocks/starrocks/pull/67003)
- Supports reading `_row_id` from Iceberg v3 tables, and supports global late materialization for Iceberg v3. [#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- Supports creating Iceberg views with custom properties, and displays properties in SHOW CREATE VIEW output. [#65938](https://github.com/StarRocks/starrocks/pull/65938)
- Supports querying Paimon tables with a specific branch, tag, version, or timestamp. [#63316](https://github.com/StarRocks/starrocks/pull/63316)
- Supports complex types (ARRAY, MAP, STRUCT) for Paimon tables. [#66784](https://github.com/StarRocks/starrocks/pull/66784)
- Supports Paimon views. [#56058](https://github.com/StarRocks/starrocks/pull/56058)
- Supports TRUNCATE for Paimon tables. [#67559](https://github.com/StarRocks/starrocks/pull/67559)
- Supports Partition Transforms with parentheses syntax when creating Iceberg tables. [#68945](https://github.com/StarRocks/starrocks/pull/68945)
- Supports ALTER TABLE REPLACE PARTITION COLUMN for Iceberg tables. [#70508](https://github.com/StarRocks/starrocks/pull/70508)
- Supports Iceberg global shuffle based on Transform Partition for improved data organization. [#70009](https://github.com/StarRocks/starrocks/pull/70009)
- Supports dynamically enabling global shuffle for Iceberg table sink. [#67442](https://github.com/StarRocks/starrocks/pull/67442)
- Introduced a Commit queue for Iceberg table sink to avoid concurrent Commit conflicts. [#68084](https://github.com/StarRocks/starrocks/pull/68084)
- Added host-level sorting for Iceberg table sink to improve data organization and reading performance. [#68121](https://github.com/StarRocks/starrocks/pull/68121)
- Enabled additional optimizations in ETL execution mode by default, improving performance for INSERT INTO SELECT, CREATE TABLE AS SELECT, and similar batch operations without explicit configuration. [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- Added commit audit information for INSERT and DELETE operations on Iceberg tables. [#69198](https://github.com/StarRocks/starrocks/pull/69198)
- Supports enabling or disabling view endpoint operations in Iceberg REST Catalog. [#66083](https://github.com/StarRocks/starrocks/pull/66083)
- Optimized cache lookup efficiency in CachingIcebergCatalog. [#66388](https://github.com/StarRocks/starrocks/pull/66388)
- Supports EXPLAIN on various Iceberg catalog types. [#66563](https://github.com/StarRocks/starrocks/pull/66563)
- Supports partition projection for tables in AWS Glue Catalog tables. [#67601](https://github.com/StarRocks/starrocks/pull/67601)
- Added resource share type support for AWS Glue `GetDatabases` API. [#69056](https://github.com/StarRocks/starrocks/pull/69056)
- Supports Azure ABFS/WASB path mapping with endpoint injection (`azblob`/`adls2`). [#67847](https://github.com/StarRocks/starrocks/pull/67847)
- Added a database metadata cache for JDBC catalog to reduce remote RPC overhead and impact of external system failures. [#68256](https://github.com/StarRocks/starrocks/pull/68256)
- Added `schema_resolver` property for JDBC catalog to support custom schema resolution. [#68682](https://github.com/StarRocks/starrocks/pull/68682)
- Supports column comments for PostgreSQL tables in `information_schema`. [#70520](https://github.com/StarRocks/starrocks/pull/70520)
- Improved Oracle and PostgreSQL JDBC type mapping. [#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### Query Engine

- **Recursive CTE**

  Supports Recursive Common Table Expressions (CTEs) for hierarchical traversals, graph queries, and iterative SQL computations. [#65932](https://github.com/StarRocks/starrocks/pull/65932)

- Improved Skew Join v2 rewrite with statistics-based skew detection, histogram support, and NULL-skew awareness. [#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- Improved COUNT DISTINCT over windows and added support for fused multi-distinct aggregations. [#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Supports explicit skew hint for window functions, with automatic optimization of window functions with skewed partition keys by splitting into UNION. [#68739](https://github.com/StarRocks/starrocks/pull/68739) [#67944](https://github.com/StarRocks/starrocks/pull/67944)
- Supports materialization hints for CTEs. [#70802](https://github.com/StarRocks/starrocks/pull/70802)
- Enabled Global Lazy Materialization by default, improving query performance by deferring column reads until needed. [#70412](https://github.com/StarRocks/starrocks/pull/70412)
- Supports EXPLAIN and EXPLAIN ANALYZE for INSERT statements in Trino Parser. [#70174](https://github.com/StarRocks/starrocks/pull/70174)
- Supports EXPLAIN for query queue visibility. [#69933](https://github.com/StarRocks/starrocks/pull/69933)

### Functions and SQL Syntax

- Added the following functions:
  - `array_top_n`: Returns the top N elements from an array ranked by value. [#63376](https://github.com/StarRocks/starrocks/pull/63376)
  - `arrays_zip`: Combines multiple arrays element-wise into an array of structs. [#65556](https://github.com/StarRocks/starrocks/pull/65556)
  - `json_pretty`: Formats a JSON string with indentation. [#66695](https://github.com/StarRocks/starrocks/pull/66695)
  - `json_set`: Sets a value at a specified path within a JSON string. [#66193](https://github.com/StarRocks/starrocks/pull/66193)
  - `initcap`: Converts the first letter of each word to uppercase. [#66837](https://github.com/StarRocks/starrocks/pull/66837)
  - `sum_map`: Sums MAP values across rows with the same key. [#67482](https://github.com/StarRocks/starrocks/pull/67482)
  - `current_timezone`: Returns the current session timezone. [#63653](https://github.com/StarRocks/starrocks/pull/63653)
  - `current_warehouse`: Returns the name of the current warehouse. [#66401](https://github.com/StarRocks/starrocks/pull/66401)
  - `sec_to_time`: Converts the number of seconds to a TIME value. [#62797](https://github.com/StarRocks/starrocks/pull/62797)
  - `ai_query`: Calls an external AI model from SQL for inference workloads. [#61583](https://github.com/StarRocks/starrocks/pull/61583)
  - `min_n` / `max_n`: Aggregate functions that return the top N minimum/maximum values. [#63807](https://github.com/StarRocks/starrocks/pull/63807)
  - `regexp_position`: Returns the position of a regular expression match in a string. [#67252](https://github.com/StarRocks/starrocks/pull/67252)
  - `is_json_scalar`: Returns whether a JSON value is a scalar. [#66050](https://github.com/StarRocks/starrocks/pull/66050)
  - `get_json_scalar`: Extracts a scalar value from a JSON string. [#68815](https://github.com/StarRocks/starrocks/pull/68815)
  - `raise_error`: Raises a user-defined error in SQL expressions. [#69661](https://github.com/StarRocks/starrocks/pull/69661)
  - `uuid_v7`: Generates time-ordered UUID v7 values. [#67694](https://github.com/StarRocks/starrocks/pull/67694)
  - `STRING_AGG`: Syntactic sugar for GROUP_CONCAT. [#64704](https://github.com/StarRocks/starrocks/pull/64704)
- Provides the following function or syntactic extensions:
  - Supports a lambda comparator in `array_sort` for custom sort ordering. [#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - Supports USING clause for FULL OUTER JOIN with SQL-standard semantics. [#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - Supports DISTINCT aggregation over framed window functions with ORDER BY/PARTITION BY. [#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - Supports ARRAY type in `lead`/`lag`/`first_value`/`last_value` window functions. [#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - Supports VARBINARY for count distinct-like aggregate functions. [#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - Supports `MULTIPLY`/`DIVIDE` for interval operations. [#68407](https://github.com/StarRocks/starrocks/pull/68407)
  - Supports date and string type casting in IN expressions. [#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - Supports WITH LABEL syntax for BEGIN/START TRANSACTION. [#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - Supports WHERE/ORDER/LIMIT clauses in SHOW statements. [#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - Supports `ALTER TASK` statements for task management. [#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - Supports SQL UDF creation via `CREATE FUNCTION ... AS <sql_body>`. [#67558](https://github.com/StarRocks/starrocks/pull/67558)
  - Supports loading UDFs from S3. [#64541](https://github.com/StarRocks/starrocks/pull/64541)
  - Supports named parameters in Scala functions. [#66344](https://github.com/StarRocks/starrocks/pull/66344)
  - Supports multiple compression formats (GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2) for CSV file exports. [#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - Supports `STRUCT_CAST_BY_NAME` SQL mode for name-based struct field matching. [#69845](https://github.com/StarRocks/starrocks/pull/69845)
  - Supports `last_query_id()` in `ANALYZE PROFILE` for easy query profile analysis. [#64557](https://github.com/StarRocks/starrocks/pull/64557)

### Management & Observability

- Supports `warehouses`, `cpu_weight_percent`, and `exclusive_cpu_weight` attributes for resource groups to improve multi-warehouse CPU resource isolation. [#66947](https://github.com/StarRocks/starrocks/pull/66947)
- Introduces the `information_schema.fe_threads` system view to inspect the FE thread state. [#65431](https://github.com/StarRocks/starrocks/pull/65431)
- Supports SQL Digest Blacklist to block specific query patterns at the cluster level. [#66499](https://github.com/StarRocks/starrocks/pull/66499)
- Supports Arrow Flight Data Retrieval from nodes that are otherwise inaccessible due to network topology constraints. [#66348](https://github.com/StarRocks/starrocks/pull/66348)
- Introduces the REFRESH CONNECTIONS command to propagate global variable changes to existing connections without reconnecting. [#64964](https://github.com/StarRocks/starrocks/pull/64964)
- Added built-in UI functions to analyze query profiles and view formatted SQL, making query tuning more accessible. [#63867](https://github.com/StarRocks/starrocks/pull/63867)
- Implements `ClusterSummaryActionV2` API endpoint to provide a structured cluster overview. [#68836](https://github.com/StarRocks/starrocks/pull/68836)
- Added a global read-only system variable `@@run_mode` to query the current cluster run mode (shared-data or shared-nothing). [#69247](https://github.com/StarRocks/starrocks/pull/69247)
- Enabled `query_queue_v2` by default for improved query queue management. [#67462](https://github.com/StarRocks/starrocks/pull/67462)
- Supports user-level default warehouse for Stream Load and Merge Commit operations. [#68106](https://github.com/StarRocks/starrocks/pull/68106) [#68616](https://github.com/StarRocks/starrocks/pull/68616)
- Added `skip_black_list` session variable to bypass backend blacklist verification when needed. [#67467](https://github.com/StarRocks/starrocks/pull/67467)
- Added `enable_table_metrics_collect` option for the metrics API. [#68691](https://github.com/StarRocks/starrocks/pull/68691)
- Added impersonate user support for query detail HTTP API. [#68674](https://github.com/StarRocks/starrocks/pull/68674)
- Added `table_query_timeout` as a table-level property. [#67547](https://github.com/StarRocks/starrocks/pull/67547)
- Added FE profile logging with configurable latency threshold. [#69396](https://github.com/StarRocks/starrocks/pull/69396)
- Supports adding FE observer nodes. [#67778](https://github.com/StarRocks/starrocks/pull/67778)
- Supports Merge Commit information in `information_schema.loads` for better load job visibility. [#67879](https://github.com/StarRocks/starrocks/pull/67879)
- Supports showing tablet status in cloud-native tables for better troubleshooting. [#69616](https://github.com/StarRocks/starrocks/pull/69616)
- Added per-catalog-type query metrics for external catalog observability. [#70533](https://github.com/StarRocks/starrocks/pull/70533)
- Added Debian (.deb) packaging support for FE and BE. [#68821](https://github.com/StarRocks/starrocks/pull/68821)

### Security

- [CVE-2026-33870] [CVE-2026-33871] Replaced AWS bundle and bumped Netty to 4.1.132.Final. [#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] Upgraded Hadoop to v3.4.2. [#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] Upgraded `spark-core_2.12` to 3.5.7. [#70862](https://github.com/StarRocks/starrocks/pull/70862)

### Bug Fixes

The following issues have been fixed:

- Fixed data loss after tablet split by skipping data file deletion for range distribution tablets. [#71135](https://github.com/StarRocks/starrocks/pull/71135)
- Fixed a memory leak in `DefaultValueColumnIterator` for complex types. [#71142](https://github.com/StarRocks/starrocks/pull/71142)
- Fixed a memory leak caused by `shared_ptr` cycle between `BatchUnit` and `FetchTaskContext`. [#71126](https://github.com/StarRocks/starrocks/pull/71126)
- Fixed use-after-free in parallel segment/rowset loading on error path. [#71083](https://github.com/StarRocks/starrocks/pull/71083)
- Fixed potential hash table data loss in aggregation spill `set_finishing`. [#70851](https://github.com/StarRocks/starrocks/pull/70851)
- Fixed double-free crash in SystemMetrics due to concurrent getline access. [#71040](https://github.com/StarRocks/starrocks/pull/71040)
- Fixed crash in SpillMemTableSink when eager merge consumes all blocks. [#69046](https://github.com/StarRocks/starrocks/pull/69046)
- Fixed NPE in `visitDictionaryGetExpr` when dictionary backing table is dropped. [#71109](https://github.com/StarRocks/starrocks/pull/71109)
- Fixed NPE when analyzing generated columns in Stream Load/Broker Load if a referenced column is missing. [#71116](https://github.com/StarRocks/starrocks/pull/71116)
- Fixed NPE when auto-created partition is dropped by TTL cleaner. [#68257](https://github.com/StarRocks/starrocks/pull/68257)
- Fixed NPE in `IcebergCatalog.getPartitionLastUpdatedTime` when snapshot is expired. [#68925](https://github.com/StarRocks/starrocks/pull/68925)
- Fixed incorrect predicate rewrite for outer join with constant-side column reference. [#67072](https://github.com/StarRocks/starrocks/pull/67072)
- Fixed PK tablet rowset meta loss caused by GC race during disk re-migration (A→B→A). [#70727](https://github.com/StarRocks/starrocks/pull/70727)
- Fixed DB read lock leak in SharedDataStorageVolumeMgr. [#70987](https://github.com/StarRocks/starrocks/pull/70987)
- Fixed error query results after modify CHAR column length in shared-data. [#68808](https://github.com/StarRocks/starrocks/pull/68808)
- Fixed MV refresh bug in the case of multiple tables. [#61763](https://github.com/StarRocks/starrocks/pull/61763)
- Fixed incorrect MV recycle time if force refreshed. [#68673](https://github.com/StarRocks/starrocks/pull/68673)
- Fixed all-null value handling bug in sync MV. [#69136](https://github.com/StarRocks/starrocks/pull/69136)
- Fixed duplicate column id error when querying MV after fast schema change ADD COLUMN. [#71072](https://github.com/StarRocks/starrocks/pull/71072)
- Fixed IVM refresh recording incomplete PCT partition metadata. [#71092](https://github.com/StarRocks/starrocks/pull/71092)
- Fixed low-cardinality rewrite NPE caused by shared DecodeInfo. [#68799](https://github.com/StarRocks/starrocks/pull/68799)
- Fixed low-cardinality join predicate type mismatch. [#68568](https://github.com/StarRocks/starrocks/pull/68568)
- Fixed Segfault in Parquet Page Index Filter when `null_counts` empty. [#68463](https://github.com/StarRocks/starrocks/pull/68463)
- Fixed JSON flatten array and object conflict on identical paths. [#68804](https://github.com/StarRocks/starrocks/pull/68804)
- Fixed Iceberg cache weigher inaccuracies. [#69058](https://github.com/StarRocks/starrocks/pull/69058)
- Fixed Iceberg table cache memory limit. [#67769](https://github.com/StarRocks/starrocks/pull/67769)
- Fixed Iceberg delete column nullability issue. [#68649](https://github.com/StarRocks/starrocks/pull/68649)
- Fixed Azure ABFS/WASB FileSystem cache key to include container. [#68901](https://github.com/StarRocks/starrocks/pull/68901)
- Fixed deadlock when the HMS connection pool is full. [#68033](https://github.com/StarRocks/starrocks/pull/68033)
- Fixed incorrect length for VARCHAR field type in Paimon Catalog. [#68383](https://github.com/StarRocks/starrocks/pull/68383)
- Fixed Paimon catalog refresh crash with ClassCastException on ObjectTable. [#70224](https://github.com/StarRocks/starrocks/pull/70224)
- Fixed PaimonView resolving table references against default_catalog instead of the Paimon catalog. [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- Fixed FULL OUTER JOIN USING with constant subqueries. [#69028](https://github.com/StarRocks/starrocks/pull/69028)
- Fixed join on clause bug with CTE scope. [#68809](https://github.com/StarRocks/starrocks/pull/68809)
- Fixed missing partition predicate in short-circuit point lookup. [#71124](https://github.com/StarRocks/starrocks/pull/71124)
- Fixed ConnectContext memory leaks by using bindScope() pattern. [#68215](https://github.com/StarRocks/starrocks/pull/68215)
- Fixed memory leak in `CatalogRecycleBin.asyncDeleteForTables` for shared-nothing clusters. [#68275](https://github.com/StarRocks/starrocks/pull/68275)
- Fixed Thrift accept thread from exiting when it encounters any exception. [#68644](https://github.com/StarRocks/starrocks/pull/68644)
- Fixed UDF resolution in routine load column mappings. [#68201](https://github.com/StarRocks/starrocks/pull/68201)
- Fixed `DROP FUNCTION IF EXISTS` ignoring `ifExists` flag. [#69216](https://github.com/StarRocks/starrocks/pull/69216)
- Fixed scan result error when dict page is too large. [#68258](https://github.com/StarRocks/starrocks/pull/68258)
- Fixed range partition overlap. [#68255](https://github.com/StarRocks/starrocks/pull/68255)
- Fixed query queue allocation time and pending timeout. [#65802](https://github.com/StarRocks/starrocks/pull/65802)
- Fixed `array_map` crash when processing null literal array. [#70629](https://github.com/StarRocks/starrocks/pull/70629)
- Fixed stack overflow for `to_base64`. [#70623](https://github.com/StarRocks/starrocks/pull/70623)
- Fixed optimizer timeout issue. [#70605](https://github.com/StarRocks/starrocks/pull/70605)
- Fixed case-insensitive username normalization for LDAP authentication. [#67966](https://github.com/StarRocks/starrocks/pull/67966)
- Mitigated SSRF risk for API `proc_file`. [#68997](https://github.com/StarRocks/starrocks/pull/68997)
- Masked user auth strings in audit and SQL redaction. [#70360](https://github.com/StarRocks/starrocks/pull/70360)

### Behavior Changes

- ETL execution mode optimizations are now enabled by default. This benefits INSERT INTO SELECT, CREATE TABLE AS SELECT, and similar batch workloads without explicit configuration changes. [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- The third argument of `lag`/`lead` window functions now supports column references in addition to constant values. [#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING now follows SQL-standard semantics: the USING column appears once in the output instead of twice. [#65122](https://github.com/StarRocks/starrocks/pull/65122)
- Global Lazy Materialization is now enabled by default. [#70412](https://github.com/StarRocks/starrocks/pull/70412)
- `query_queue_v2` is now enabled by default. [#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL transactions are gated behind the session variable `enable_sql_transaction` by default. [#63535](https://github.com/StarRocks/starrocks/pull/63535)
