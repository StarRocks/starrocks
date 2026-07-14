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

## 4.1.3

Release Date: July 14, 2026

### Behavior Changes

- `CTAS` now preserves explicitly declared `VARCHAR(N)` column lengths instead of widening them to `VARCHAR(MAX)`. Existing tables are unaffected; new tables created with `CTAS` will enforce the declared length on subsequent writes. [#73498](https://github.com/StarRocks/starrocks/pull/73498)
- Querying `sys.fe_memory_usage` or `sys.fe_locks` without the `OPERATE ON SYSTEM` privilege now returns a clear access-denied error instead of a misleading node-lookup failure. [#73567](https://github.com/StarRocks/starrocks/pull/73567)
- `FILES()` and broker/stream load no longer apply a session timezone shift to `INT64` Parquet timestamps written with `isAdjustedToUTC=false`; those timestamps are now treated as wall-clock values and loaded as-is. [#73674](https://github.com/StarRocks/starrocks/pull/73674)
- Successfully committed multi-table transaction stream load jobs are now correctly shown as `VISIBLE` in `information_schema.loads` and `SHOW STREAM LOAD` instead of remaining stuck at `PREPARING`. [#74386](https://github.com/StarRocks/starrocks/pull/74386)
- Connector incremental scan range scheduling now consistently reuses the deployed fragment's driver layout, preventing scan ranges from being incorrectly assigned to non-existent drivers. [#74674](https://github.com/StarRocks/starrocks/pull/74674)
- `LIKE` constant-folding now matches MySQL 8 backslash-escape semantics, correcting cases where patterns such as `'a\\\\b'` previously returned opposite results. [#74814](https://github.com/StarRocks/starrocks/pull/74814)
- Routine Load now supports the `property.kafka_partition_discovery` property, which allows partition auto-discovery to continue even when `kafka_partitions` and `kafka_offsets` are specified to seed exact starting offsets. [#74729](https://github.com/StarRocks/starrocks/pull/74729)
- Non-group-by aggregates are now pushed down through `UNION ALL` branches before merging, reducing network transfer and memory usage for queries that aggregate over a union. [#73930](https://github.com/StarRocks/starrocks/pull/73930)
- IVM maintenance queries are now re-derived from the current view definition at each refresh instead of using the frozen query text stored at `CREATE` time; existing MVs automatically benefit from rewriter bug fixes without needing to be recreated. [#74881](https://github.com/StarRocks/starrocks/pull/74881)
- Sample-based tablet pre-split now spreads pre-split shards across all compute nodes (`SPREAD` placement) instead of packing them onto the source tablet's worker (`PACK` placement), improving load parallelism. [#75514](https://github.com/StarRocks/starrocks/pull/75514)
- `ALTER TABLE ... MODIFY COLUMN` that changes only the column comment now takes the lightweight metadata-only path instead of spawning a full schema-change job, and this now works on Primary Key columns as well. [#75325](https://github.com/StarRocks/starrocks/pull/75325)
- `FLOOR` and `CEIL` are now treated as non-reserved keywords and can be used as column names without quoting. [#75241](https://github.com/StarRocks/starrocks/pull/75241)
- `SHOW FUNCTIONS` output now always includes the `isolation` property (`shared` or `isolated`) in the Properties column for UDFs and UDAFs. [#75255](https://github.com/StarRocks/starrocks/pull/75255)
- The default value of `lake_vacuum_min_batch_delete_size` is raised from 100 to 200, improving vacuum throughput on object storage by batching more stale-file deletions per `DeleteObjects` request. [#74304](https://github.com/StarRocks/starrocks/pull/74304)
- Iceberg REST catalog tables with vended credentials are now cached and their credentials are refreshed in the background, eliminating per-`getTable()` `GetDataAccess` calls that caused AWS Lake Formation rate limiting. [#75431](https://github.com/StarRocks/starrocks/pull/75431)
- IVM `bitmap_union`, `hll_union`, and `percentile_union` aggregate states are now stored once in the materialized view instead of twice (visible column + hidden `__AGG_STATE_` column), halving storage for those sketch types. [#75760](https://github.com/StarRocks/starrocks/pull/75760)
- Incremental materialized views now support `bitmap_agg`, `hll_union`, `percentile_union`, and `bitmap_union` aggregate functions, enabling exact distinct-count and sketch-based aggregations to be maintained incrementally. [#75587](https://github.com/StarRocks/starrocks/pull/75587) [#75610](https://github.com/StarRocks/starrocks/pull/75610)
- Sample-based tablet pre-split tablet count is now rounded up to the nearest multiple of the active compute-node count for even distribution, and bounded below by a minimum tablet size to avoid excessive fragmentation on small loads. [#75360](https://github.com/StarRocks/starrocks/pull/75360) [#75584](https://github.com/StarRocks/starrocks/pull/75584)

### Improvements

- The `ngram_search` function now accepts a non-constant needle argument. [#74675](https://github.com/StarRocks/starrocks/pull/74675)
- Added an HTTP authentication framework controlled by the `enable_http_auth` FE configuration, gating authentication and RBAC enforcement on all external HTTP endpoints. [#73822](https://github.com/StarRocks/starrocks/pull/73822)
- Added refresh and placement observability columns (`refresh_warehouse`, `refresh_resource_group`, `refresh_mode`, `refresh_type`, `last_refresh_details`) to `information_schema.materialized_views`. [#74342](https://github.com/StarRocks/starrocks/pull/74342)
- Added opt-in lazy refresh of the external statistics cache on journal replay, controlled by a new FE config, to prevent a slow or stuck external metastore from stalling FE journal replay or startup. [#74371](https://github.com/StarRocks/starrocks/pull/74371)
- `VARCHAR` length increase is now allowed on range-distribution (shared-data) sort-key columns via fast schema evolution without a data rewrite. [#74698](https://github.com/StarRocks/starrocks/pull/74698)
- Added a stack-trace dump when a shared-data transaction log write exceeds a configurable threshold, making slow `put_txn_log` / `put_combined_txn_log` calls easier to diagnose. [#74704](https://github.com/StarRocks/starrocks/pull/74704)
- Tablet pre-split meta-tier footer readers now support `DATE`, `DATETIME`, `DECIMAL`, `VARCHAR`, and ORC `TIMESTAMP` sort keys, reducing the number of loads that must fall back to data-tier sampling. [#74710](https://github.com/StarRocks/starrocks/pull/74710) [#74739](https://github.com/StarRocks/starrocks/pull/74739) [#74792](https://github.com/StarRocks/starrocks/pull/74792) [#74902](https://github.com/StarRocks/starrocks/pull/74902) [#74955](https://github.com/StarRocks/starrocks/pull/74955) [#75186](https://github.com/StarRocks/starrocks/pull/75186) [#75209](https://github.com/StarRocks/starrocks/pull/75209) [#75427](https://github.com/StarRocks/starrocks/pull/75427) [#75697](https://github.com/StarRocks/starrocks/pull/75697)
- Sample-based tablet pre-split now applies to `INSERT INTO ... SELECT ... FROM <OLAP table>` loads, and also to column-list `INSERT` statements that include all sort-key columns. [#74828](https://github.com/StarRocks/starrocks/pull/74828) [#75345](https://github.com/StarRocks/starrocks/pull/75345)
- Added Adler-32 checksum protection for shared-data tablet metadata and transaction log files, enabling silent corruption to be detected on read. [#74924](https://github.com/StarRocks/starrocks/pull/74924)
- Added the `txn_max_committed_pending_publish_ms` FE metric per database, reporting the age of the oldest committed-but-not-yet-published transaction to help detect stalled version publishing. [#75025](https://github.com/StarRocks/starrocks/pull/75025)
- Tablet split/merge is now triggered in real time from the publish-version response, reducing the lag between a load completing and automatic split/merge being initiated. [#75010](https://github.com/StarRocks/starrocks/pull/75010)
- Optimized condition-update compare phase for lake primary-key tables by routing no-SST condition-merge tasks to the `pk_index_execution` thread pool. [#74572](https://github.com/StarRocks/starrocks/pull/74572)
- Scoped lake schema-change and rollup job locks to the table level instead of the whole database, reducing lock contention on concurrent operations for other tables in the same database. [#75087](https://github.com/StarRocks/starrocks/pull/75087)
- Narrowed several database-level write locks to table-scoped intensive write locks in shared-nothing mode, reducing lock contention during BE report callbacks and cooldown operations. [#74521](https://github.com/StarRocks/starrocks/pull/74521) [#74523](https://github.com/StarRocks/starrocks/pull/74523)
- Avro Routine Load now supports native `MAP` and `STRUCT` target columns. [#74901](https://github.com/StarRocks/starrocks/pull/74901)
- Range-colocate tablet stability gating now waits for StarOS placement convergence before marking a group stable, ensuring colocate joins achieve host-local execution. [#75290](https://github.com/StarRocks/starrocks/pull/75290) [#75656](https://github.com/StarRocks/starrocks/pull/75656) [#75883](https://github.com/StarRocks/starrocks/pull/75883)
- Improved CBO statistics for external tables: the optimizer now estimates row counts from Iceberg manifests without full file enumeration, corrects Hive/Hudi row-count underestimation for Parquet/ORC compression, adds async row-count statistics for JDBC connectors, and provides NDV estimation fallback for Iceberg and external connectors when Puffin stats are unavailable. [#75280](https://github.com/StarRocks/starrocks/pull/75280) [#75082](https://github.com/StarRocks/starrocks/pull/75082) [#75083](https://github.com/StarRocks/starrocks/pull/75083) [#75092](https://github.com/StarRocks/starrocks/pull/75092) [#75097](https://github.com/StarRocks/starrocks/pull/75097) [#75382](https://github.com/StarRocks/starrocks/pull/75382) [#75474](https://github.com/StarRocks/starrocks/pull/75474)
- Iceberg manifest column statistics are now cached selectively for clustered columns only, reducing FE heap consumption for wide tables with many data files. [#75395](https://github.com/StarRocks/starrocks/pull/75395)
- External table statistics collection now supports persistent predicate-column tracking across FE restarts and HA failovers, enabling auto-ANALYZE to target the correct columns. [#75653](https://github.com/StarRocks/starrocks/pull/75653)
- Added structured `[ExternalStats]` log lines covering the full lifecycle of external-table statistics collection from scheduling through execution. [#75335](https://github.com/StarRocks/starrocks/pull/75335) [#75529](https://github.com/StarRocks/starrocks/pull/75529)
- `SHOW ANALYZE STATUS` now includes partition, column, and snapshot metadata in the Properties column for external table statistics jobs. [#75630](https://github.com/StarRocks/starrocks/pull/75630)
- The statistics source (`TABLE_METADATA`, `ANALYZE`, or `NONE`) for each external table is now exposed in the query runtime profile. [#75253](https://github.com/StarRocks/starrocks/pull/75253)
- Added support for partition filter requirement and partition count limit for Iceberg and Delta Lake external tables (previously only available for Hive, Hudi, and Paimon). [#75790](https://github.com/StarRocks/starrocks/pull/75790)
- Supports sub-1% sampling ratios in `TABLE SAMPLE` and histogram `ANALYZE`, fixing failures on large tables where the computed ratio would truncate to zero. [#74551](https://github.com/StarRocks/starrocks/pull/74551)
- Added the `jemalloc_conf` BE configuration item, making jemalloc runtime options visible via `information_schema.be_configs`. [#75344](https://github.com/StarRocks/starrocks/pull/75344)
- Added the `compaction_chunk_reset_memory_tracker_threshold_percent` BE configuration to reduce memory usage during Primary Key compaction in shared-nothing mode by releasing retained chunk capacity. [#75091](https://github.com/StarRocks/starrocks/pull/75091)
- Upgraded staros to v4.1.1, including persistent `datacache.enable` across restarts, per-worker-group shard warmup timeout override, and improved S3 retry jitter. [#75204](https://github.com/StarRocks/starrocks/pull/75204)
- Optimized SQL credential redaction on the audit hot path by skipping the regex scan when no credential marker is present in the SQL string. [#74812](https://github.com/StarRocks/starrocks/pull/74812)
- Expression-driven on-demand lazy column loading for the Parquet scanner reduces unnecessary I/O on multi-branch `OR` queries. [#74886](https://github.com/StarRocks/starrocks/pull/74886)
- `ds_hll_count_distinct` / `DataSketchesHll` now produces stable cardinality estimates by using the composite estimator instead of the order-dependent HIP estimator. [#75053](https://github.com/StarRocks/starrocks/pull/75053)

### Security

- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] Upgraded Netty to 4.1.135.Final to fix SNI handler heap exhaustion (DoS), IPv6 subnet filter bypass, and DNS cache poisoning. [#74668](https://github.com/StarRocks/starrocks/pull/74668)
- [CVE-2026-54512] [CVE-2026-54513] Upgraded `jackson-databind` to 2.21.4 to fix two deserialization vulnerabilities. [#75373](https://github.com/StarRocks/starrocks/pull/75373)
- [GHSA-2r2c-cx56-8933] [GHSA-47qp-hqvx-6r3f] Excluded `org.jline:jline-remote-telnet` from Hadoop transitive dependencies to remediate unauthenticated Telnet-server DoS vulnerabilities. [#75066](https://github.com/StarRocks/starrocks/pull/75066)
- [CVE-2026-39822] Updated pprof prebuilt to fix a vulnerability in the pprof binary. [#76248](https://github.com/StarRocks/starrocks/pull/76248) [#74669](https://github.com/StarRocks/starrocks/pull/74669)
- Fixed SQL injection in `information_schema.task_runs` where a single quote in a predicate value could escape the literal boundary. [#75520](https://github.com/StarRocks/starrocks/pull/75520)
- `tencent.cos.access_key`, `tencent.cos.secret_key`, and `iceberg.catalog.jdbc.password` are now masked in `SHOW CREATE CATALOG` output. [#74696](https://github.com/StarRocks/starrocks/pull/74696)
- Fixed an out-of-bounds read in `url_decode` when the input ends with a truncated percent-escape sequence. [#75139](https://github.com/StarRocks/starrocks/pull/75139)
- Fixed `HyperLogLog::deserialize` accepting out-of-range `SPARSE` register indices, which could corrupt heap memory and crash the BE on malformed input. [#75521](https://github.com/StarRocks/starrocks/pull/75521)
- Fixed `bar()` rejecting negative width values, which previously allowed unbounded string growth and BE memory exhaustion. [#75143](https://github.com/StarRocks/starrocks/pull/75143)

### Bug Fixes

The following issues have been fixed:

- `add_files` populated Iceberg file bounds with Parquet physical encoding bytes instead of logical typed values, causing incorrect file-level min/max pruning (for example, on `DECIMAL` columns). [#69207](https://github.com/StarRocks/starrocks/pull/69207)
- `ApplyTuningGuideRule` threw `UnsupportedOperationException` when traversing plan nodes whose input lists were built as immutable `List.of(...)`. [#70785](https://github.com/StarRocks/starrocks/pull/70785)
- `INSERT OVERWRITE` two-phase re-plan could produce stale lambda-argument column reference IDs from the first planning session, causing `expr_type does not match slot_type` errors. [#73273](https://github.com/StarRocks/starrocks/pull/73273)
- Partial updates on tables with GIN (inverted) indexes caused queries to hang indefinitely or fail when the GIN-indexed column was omitted from the update. [#73773](https://github.com/StarRocks/starrocks/pull/73773)
- Lake PCU (partial column update) crashed or silently corrupted data when a schema drift occurred between the rowset schema and the tablet schema. [#74005](https://github.com/StarRocks/starrocks/pull/74005)
- A combined `ALTER TABLE` on an external Iceberg table with multiple schema clauses incorrectly re-executed all previously queued actions on every clause dispatch. [#74036](https://github.com/StarRocks/starrocks/pull/74036)
- `PartitionedSpillerWriter` crashed with `SIGSEGV` when the `num_rows` snapshot exceeded the actual chunk row count during partition-flush and resource-group-cancellation interleaving. [#74081](https://github.com/StarRocks/starrocks/pull/74081)
- Unexpected BE process restarts. [#74424](https://github.com/StarRocks/starrocks/pull/74424)
- Parquet temporary dict-code columns leaked to upper layers when a struct VARCHAR subfield fill was skipped due to row-range filtering, causing type mismatches. [#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `SELECT ... INTO OUTFILE` recorded `ReturnRows=0` in the audit log instead of the actual exported row count. [#74467](https://github.com/StarRocks/starrocks/pull/74467)
- `TabletChecker.doCheck()` threw `IllegalMonitorStateException` in `blockingAddTabletCtxToScheduler` due to a lock type mismatch, causing entire checker rounds to abort silently. [#74596](https://github.com/StarRocks/starrocks/pull/74596)
- `information_schema.COLUMNS` always returned `NULL` for `DATETIME_PRECISION`, breaking MySQL-protocol clients that derive column size from that field. [#74623](https://github.com/StarRocks/starrocks/pull/74623)
- MV refresh failed with `Duplicate key` when the query joined two tables with the same unqualified name across different databases or catalogs. [#74730](https://github.com/StarRocks/starrocks/pull/74730)
- Spillable hash join probe crashed under certain conditions. [#74978](https://github.com/StarRocks/starrocks/pull/74978) [#75140](https://github.com/StarRocks/starrocks/pull/75140)
- Iceberg `truncate` and `bucket` transform functions crashed the BE with `SIGFPE` when the width or bucket count argument was zero. [#74998](https://github.com/StarRocks/starrocks/pull/74998)
- `mod()` and `pmod()` crashed the BE with `SIGFPE` when the dividend was `TYPE_MIN` and the divisor was `-1`. [#74980](https://github.com/StarRocks/starrocks/pull/74980)
- `histogram()` crashed the BE with `SIGFPE` when `bucket_num` was zero or negative. [#75041](https://github.com/StarRocks/starrocks/pull/75041)
- `encode_fingerprint_sha256` crashed with `SIGSEGV` when all input rows were `NULL`. [#75042](https://github.com/StarRocks/starrocks/pull/75042)
- `LIKE` patterns containing the single-char wildcard `_` returned incorrect results when evaluated via a GIN inverted index. [#75551](https://github.com/StarRocks/starrocks/pull/75551)
- AND-only `MATCH` queries against a GIN inverted index returned a spurious error when the target segment was empty. [#75161](https://github.com/StarRocks/starrocks/pull/75161)
- CLucene `match_all` queries returned incorrect results; resolved by upgrading the CLucene dependency. [#75180](https://github.com/StarRocks/starrocks/pull/75180)
- Vector index rewrite registered a synthetic distance column directly on the shared table schema, causing `Multiple entries with same key` errors for unrelated concurrent queries on the same table. [#74785](https://github.com/StarRocks/starrocks/pull/74785)
- Join reorder pruning could prune columns still referenced by scan predicates, causing statistics estimation to throw `missing statistic of col`. [#74791](https://github.com/StarRocks/starrocks/pull/74791)
- `avg(DISTINCT x)` was incorrectly rewritten via a sum/count materialized view, silently dropping the `DISTINCT` and returning wrong results when duplicates existed. [#75071](https://github.com/StarRocks/starrocks/pull/75071)
- `ALTER TABLE ... MODIFY COLUMN ... AFTER <nonexistent_col>` threw an internal `NullPointerException` instead of a clean semantic error. [#75073](https://github.com/StarRocks/starrocks/pull/75073)
- `SHOW CREATE ROUTINE LOAD` emitted a spurious leading comma before the first load-description clause for jobs without a `COLUMNS TERMINATED BY` clause. [#75522](https://github.com/StarRocks/starrocks/pull/75522)
- `SECURITY INVOKER` views with CTEs could fail privilege checks with an NPE when the CTE name was mistaken for a real table reference. [#74813](https://github.com/StarRocks/starrocks/pull/74813)
- `ReduceCastRule` aborted query planning with a `SemanticException` when a date/datetime boundary literal shifting would overflow the representable range (for example, `<= '9999-12-31'`). [#75036](https://github.com/StarRocks/starrocks/pull/75036)
- `SplitJoinORToUnionRule` emitted duplicate rows when a join condition used null-safe-equal (`<=>`) disjuncts. [#75038](https://github.com/StarRocks/starrocks/pull/75038)
- `Tracers` shared across parallel metadata-preparation threads for multi-table external queries caused `IllegalStateException` under `enable_profile=true`. [#74746](https://github.com/StarRocks/starrocks/pull/74746)
- Partition consumer errors in `ChunksPartitioner` were silently discarded, allowing a partitioned TopN to return partial or wrong results without surfacing an error. [#74693](https://github.com/StarRocks/starrocks/pull/74693)
- BE vacuum tasks continued running as zombies after the FE caller's timeout elapsed, exhausting the `RELEASE_SNAPSHOT` thread pool and collapsing vacuum throughput. [#74694](https://github.com/StarRocks/starrocks/pull/74694)
- An autovacuum race could momentarily compute a `minActiveTxnId` one greater than an in-flight transaction, causing the BE to delete still-needed combined transaction logs and permanently wedging publish. [#74906](https://github.com/StarRocks/starrocks/pull/74906)
- Queries were incorrectly marked as canceled after finishing successfully due to a race between FE EOS-cancel and BE stage-2 deploy. [#75009](https://github.com/StarRocks/starrocks/pull/75009)
- BE crashed with `SIGSEGV` in `AggTopNRuntimeFilterUpdaterImpl` when the aggregate TopN runtime filter build key was a `ConstColumn`. [#74809](https://github.com/StarRocks/starrocks/pull/74809) [#74941](https://github.com/StarRocks/starrocks/pull/74941)
- `array_map` / `transform` silently dropped `NULL` rows and returned wrong row counts when all non-null arrays were empty. [#75141](https://github.com/StarRocks/starrocks/pull/75141)
- `LARGEINT` / `DECIMAL128` literals above 2^64 were silently truncated to 64 bits in JIT-compiled expressions. [#75137](https://github.com/StarRocks/starrocks/pull/75137)
- UTF-8 string functions (`split`, `split_part`, `str_to_map`) read beyond the end of the string when the last character had a truncated or invalid multi-byte lead byte with an empty delimiter. [#75068](https://github.com/StarRocks/starrocks/pull/75068)
- `parse_json()` silently returned `NULL` for malformed JSON even in `ALLOW_THROW_EXCEPTION` SQL mode instead of failing the query. [#74976](https://github.com/StarRocks/starrocks/pull/74976)
- Strict-mode numeric narrowing casts incorrectly raised overflow errors on `NULL` rows whose slot data was undefined. [#74903](https://github.com/StarRocks/starrocks/pull/74903)
- Pre-1970 Parquet `INT64` timestamps with a non-zero sub-second part were decoded to garbage values due to negative truncating-division remainder. [#75207](https://github.com/StarRocks/starrocks/pull/75207)
- Pre-1970 ORC `TIMESTAMP` values had their sub-second component dropped on load. [#75432](https://github.com/StarRocks/starrocks/pull/75432)
- ORC stripe min/max timestamp statistics were decoded incorrectly for pre-1970 and sub-second bounds, causing data files to be incorrectly pruned. [#75543](https://github.com/StarRocks/starrocks/pull/75543)
- Nested `INT96` Parquet timestamps inside `ARRAY`, `MAP`, or `STRUCT` columns lost one session-timezone offset on load. [#74868](https://github.com/StarRocks/starrocks/pull/74868)
- Parquet `UINT_32` values were sign-extended instead of zero-extended when loaded into a `BIGINT` column, silently storing negative values for high-bit unsigned integers. [#75002](https://github.com/StarRocks/starrocks/pull/75002)
- `HiveDataSource` destructor caused a heap-use-after-free by destroying `_pool` (and its `Expr` nodes) before `_scanner_ctx` (which holds predicates referencing those nodes). [#74818](https://github.com/StarRocks/starrocks/pull/74818)
- Reading gzip-compressed JSON Hive external tables with OpenX SerDe failed with `UTF8_ERROR` when a multi-byte UTF-8 character straddled an 8 MB decompression buffer boundary. [#74827](https://github.com/StarRocks/starrocks/pull/74827)
- ADLS2 `ListPaths` crashed with `SIGSEGV` on non-HNS accounts due to missing JSON fields that the client accessed unconditionally. [#75166](https://github.com/StarRocks/starrocks/pull/75166)
- `unnest` crashed or returned wrong results when multiple `UNNEST` operators shared the same input array column and consumed different subfields. [#75012](https://github.com/StarRocks/starrocks/pull/75012) [#75445](https://github.com/StarRocks/starrocks/pull/75445) [#76002](https://github.com/StarRocks/starrocks/pull/76002)
- `query_mem_limit` was not enforced during `unnest` execution, allowing `unnest` over large arrays to OOM-kill the BE instead of failing the query. [#75179](https://github.com/StarRocks/starrocks/pull/75179)
- `TopN` with `RANK` boundary dropped one row when the rank limit fell exactly on a chunk boundary. [#75045](https://github.com/StarRocks/starrocks/pull/75045)
- Column pruning after `PushDownDistinctAggregateRule` could generate an empty analytic (window) operator, causing planning or execution errors. [#74810](https://github.com/StarRocks/starrocks/pull/74810)
- `EliminateSortColumnWithEqualityPredicateRule` set the row limit only on the scan operator without setting a global limit, causing `COUNT(*)` over a limited sub-query to return more rows than expected under concurrency. [#74983](https://github.com/StarRocks/starrocks/pull/74983)
- Lake primary-key persistent index rebuild used wrong segment iterator positions in segment-range mode, causing incorrect key-range filter application. [#74887](https://github.com/StarRocks/starrocks/pull/74887) [#75206](https://github.com/StarRocks/starrocks/pull/75206)
- `DROP PERSISTENT INDEX` modified `rebuildPindexVersion` without a table lock; `RestoreJob` post-restore mutated MV base-table info under only a DB READ lock; `FinalizeCreateTableAction` passed a DB-level lock across iterator creation. [#74968](https://github.com/StarRocks/starrocks/pull/74968)
- `dumpImage` could strand the global meta lock indefinitely if acquiring a per-database lock threw mid-loop. [#75488](https://github.com/StarRocks/starrocks/pull/75488)
- Multi-statement stream load leaked one `TxnStateCallbackFactory` entry per transaction, growing without bound and eventually exhausting FE heap. [#75188](https://github.com/StarRocks/starrocks/pull/75188)
- `information_schema.task_runs` row count for histogram statistics could overflow `primary_key_limit_size` (128 bytes) when catalog, database, table, or partition names were long. [#75735](https://github.com/StarRocks/starrocks/pull/75735)
- BE JVM metrics emitted invalid Prometheus `# TYPE` lines (label sets inside the metric name), causing Prometheus to abort the entire scrape. [#75240](https://github.com/StarRocks/starrocks/pull/75240)
- `SHOW PARTITIONS` and `information_schema.partitions_meta` reported all physical partitions' bucket counts as the table-level default instead of each partition's actual bucket count on shared-data tables. [#75734](https://github.com/StarRocks/starrocks/pull/75734)
- `SHOW PROC '.../index_schema/<id>'` returned the base table schema for all rollup indexes on shared-data (`CLOUD_NATIVE`) tables. [#76069](https://github.com/StarRocks/starrocks/pull/76069)
- `ALTER TABLE ... MODIFY COLUMN` no-op clauses were incorrectly routed to the lightweight comment path, causing `MODIFY COLUMN COMMENT can not be combined with other alter operations` errors in batch `ALTER TABLE` statements. [#75736](https://github.com/StarRocks/starrocks/pull/75736)
- `isCommentOnlyModification` could misidentify key/aggregation columns as comment-only changes due to incorrect `isKey` / `aggregationType` normalization. [#75545](https://github.com/StarRocks/starrocks/pull/75545)
- `ALTER VIEW` could commit a cyclic view definition that caused subsequent `SELECT` to throw `StackOverflowError`. [#75033](https://github.com/StarRocks/starrocks/pull/75033)
- `OrderedPartitionExchanger` caused a heap-use-after-free when a downstream consumer mutated the previous chunk while `accept()` still held a pointer to it. [#75279](https://github.com/StarRocks/starrocks/pull/75279)
- NLJoin crashed when the build-side's slot descriptor was non-nullable but the runtime state was nullable. [#75343](https://github.com/StarRocks/starrocks/pull/75343) [#75788](https://github.com/StarRocks/starrocks/pull/75788)
- `CAST(json/variant AS struct)` crashed the BE at fragment prepare when a struct field name was not a parseable JSON path. [#75355](https://github.com/StarRocks/starrocks/pull/75355)
- Dict-decode for nested dictionary expressions could produce incompatible dictionary translations between producer and consumer fragments, causing `Dict Decode failed` errors at runtime. [#75246](https://github.com/StarRocks/starrocks/pull/75246)
- Schema change crashed with a null-pointer dereference when `get_rowset_by_version` returned `nullptr` and the `gtid` comparison was placed before the null check. [#74855](https://github.com/StarRocks/starrocks/pull/74855)
- Shared-data cluster snapshots became unrestorable after a tablet split/merge because the snapshot manager did not consider reshard jobs when deciding whether to reap parent tablet metadata. [#75638](https://github.com/StarRocks/starrocks/pull/75638)
- File-bundling vacuum incorrectly flagged zero-row bundled segments of sibling tablets as non-shared, causing their bundle file to be deleted while other tablets still referenced it. [#75689](https://github.com/StarRocks/starrocks/pull/75689)
- Shared-data table compaction publish dropped rollup/synchronous-MV indexes that became visible after the compaction transaction began, leaving the bundle file without those indexes. [#76105](https://github.com/StarRocks/starrocks/pull/76105)
- Shared-data persistent-index compaction incorrectly deleted pass-through-reused SSTable files when a compaction was dropped during tablet reshard. [#75726](https://github.com/StarRocks/starrocks/pull/75726)
- `NOT NULL`-to-nullable flat-JSON column schema evolution caused a `CHECK` crash in the compaction read path. [#75680](https://github.com/StarRocks/starrocks/pull/75680)
- `count_combine` over a nullable column crashed the BE with `SIGSEGV` in the streaming pre-aggregation pass-through path. [#75298](https://github.com/StarRocks/starrocks/pull/75298)
- Java UDFs failed to load on JDK 21+ due to a reflective `DirectByteBuffer` constructor lookup that was removed in JDK 21. [#75666](https://github.com/StarRocks/starrocks/pull/75666)
- `CTAS` into a Unified catalog (Hive metastore) always failed because the parser did not support the `ENGINE` clause in `CREATE TABLE AS SELECT`. [#75771](https://github.com/StarRocks/starrocks/pull/75771)
- `JoinTuningGuide` feedback-driven join rebuild lost `predicateCommonOperators`, causing `InputDependenciesChecker` validation failures on plans with common sub-expression reuse. [#75773](https://github.com/StarRocks/starrocks/pull/75773)
- Query cache normalization crashed with `Preconditions.checkState` on tables with sub-partitions where empty sub-partitions were pruned before the version list was built. [#75789](https://github.com/StarRocks/starrocks/pull/75789)
- `replayFromJson` silently skipped session variables stored under a legacy alias, causing query dump replay to fall back to default values. [#75813](https://github.com/StarRocks/starrocks/pull/75813)
- Iceberg `_row_id` virtual column returned incorrect values for data files with more than one Parquet row group due to double-counting of the row-group start offset. [#75758](https://github.com/StarRocks/starrocks/pull/75758)
- Iceberg DELETE/UPDATE planner could not locate the target scan node because it matched by synthetic table ID instead of physical table identity, losing the base snapshot ID and conflict-detection filter. [#76013](https://github.com/StarRocks/starrocks/pull/76013)
- `FragmentContext::set_final_status` crashed with `SIGSEGV` when a `cancel_plan_fragment` RPC arrived before `PipelineExecutorSet::start()` was called. [#75030](https://github.com/StarRocks/starrocks/pull/75030)
- `QueryContext` could be reclaimed while `FragmentExecutor` was still tearing down a fragment, causing heap-use-after-free in `ResGuard::reset()`. [#74978](https://github.com/StarRocks/starrocks/pull/74978)
- `StringSearch::_pattern` was uninitialized, allowing a default-constructed `search()` to dereference an uninitialized pointer. [#75614](https://github.com/StarRocks/starrocks/pull/75614)
- `DATETIME` microseconds were rendered using the JVM default locale's digit set, causing non-ASCII digits on locales such as Arabic or Persian that broke boundary value parsing in tablet pre-split. [#75001](https://github.com/StarRocks/starrocks/pull/75001)
- Partition row counts could be written as zero into `_statistics_.column_statistics` after `INSERT OVERWRITE` before tablet stats were refreshed, causing the optimizer to collapse partition cardinality estimates. [#74801](https://github.com/StarRocks/starrocks/pull/74801)
- `enable_statistic_collect_on_first_load` table-level override could not enable first-load statistics collection when the global config disabled it. [#74794](https://github.com/StarRocks/starrocks/pull/74794)
- `PushDownNonGroupedAggregateBelowUnion` produced nullable outputs with non-nullable declared types when a union branch had no input rows, causing BE `CHECK` failures. [#76101](https://github.com/StarRocks/starrocks/pull/76101)


## 4.1.2

Release Date: June 18, 2026

### Behavior Changes

- Connecting to a database that the user has no privilege on now returns a proper MySQL error packet instead of closing the connection with ERROR 2013. [#70072](https://github.com/StarRocks/starrocks/pull/70072)
- `SHOW FUNCTIONS` now masks UDF file and object-file paths as `***` for users who can see a function through function-level privileges but do not hold the create-function scope privilege. [#73425](https://github.com/StarRocks/starrocks/pull/73425)
- Ranger row filter and column masking policies are now correctly applied when querying Hive views from external catalogs. [#73265](https://github.com/StarRocks/starrocks/pull/73265)
- `ALTER TABLE ... ADD COLUMN ... DEFAULT current_timestamp` now correctly preserves the `current_timestamp` generator expression. `DESCRIBE` and `information_schema` now reflect the expression rather than the backfill-time literal. [#73455](https://github.com/StarRocks/starrocks/pull/73455)
- `information_schema.loads` load-time filtering no longer shifts filter bounds on clusters whose session timezone differs from UTC+8. Load times are now exchanged as UTC epoch milliseconds across the FE–BE boundary. [#73365](https://github.com/StarRocks/starrocks/pull/73365)
- The `connector_max_split_size` session variable now correctly applies to Paimon scan split calculation instead of always using the default value. [#71756](https://github.com/StarRocks/starrocks/pull/71756)
- `pipeline_enable_large_column_checker` is now enabled by default. [#72798](https://github.com/StarRocks/starrocks/pull/72798)
- Hive partition statistics are no longer refreshed automatically per key on a timer. Partition stats are now only refreshed during explicit `refreshTable()` calls, reducing HMS load on large partitioned tables. [#73563](https://github.com/StarRocks/starrocks/pull/73563)
- When an Iceberg or external-catalog base table undergoes schema drift (column type change, column drop, or table drop), the dependent materialized view is now marked inactive at the next refresh instead of silently producing NULL rows or opaque errors. [#73770](https://github.com/StarRocks/starrocks/pull/73770)
- The Iceberg connector now partially pushes down the convertible side of `AND` compound predicates instead of discarding the entire predicate when only one side is convertible, improving partition pruning and data skipping. [#70293](https://github.com/StarRocks/starrocks/pull/70293)
- Explicit-transaction `COMMIT` now correctly waits up to `query_timeout` seconds (not milliseconds) for the database write lock, preventing spurious lock-timeout failures under brief concurrent write activity. [#73549](https://github.com/StarRocks/starrocks/pull/73549)
- IVM refresh now surfaces strict-load filter errors to the caller instead of silently dropping filtered rows. [#73938](https://github.com/StarRocks/starrocks/pull/73938)
- `count_combine(nullable_col)` now correctly excludes NULL rows, matching `COUNT(col)` semantics. Incremental MVs backed by `COUNT(<nullable column>)` were previously materializing inflated counts. [#74029](https://github.com/StarRocks/starrocks/pull/74029)
- `SHOW ALTER TABLE COLUMN` now also displays asynchronous metadata-only alter jobs triggered by `ALTER TABLE ... SET (...)` for properties such as `file_bundling` and `enable_persistent_index` on cloud-native (shared-data) tables. [#74198](https://github.com/StarRocks/starrocks/pull/74198)
- Creating an incremental MV with a `HAVING` clause that references an aggregate function now fails at `CREATE` time with a clear error instead of producing an internal plan error on first refresh. [#74054](https://github.com/StarRocks/starrocks/pull/74054)
- IVM now supports `MIN`/`MAX(DECIMAL)` aggregate functions in incremental materialized views. [#73969](https://github.com/StarRocks/starrocks/pull/73969)
- IVM adaptive refresh now correctly bounds the delta window when the first delta trait already exceeds `mv_max_rows_per_refresh`, preventing the entire backlog from being refreshed in one task run. [#74464](https://github.com/StarRocks/starrocks/pull/74464)
- GROUP-BY-only incremental MVs (for example, `SELECT k FROM t GROUP BY k`) now correctly encode `__ROW_ID__` as VARCHAR, fixing a crash on the second refresh. [#74030](https://github.com/StarRocks/starrocks/pull/74030)

### Improvements

- Supports Paimon views, including `CREATE`/`REPLACE`/`DROP`, `SHOW`/`DESC`, and querying Paimon views from external catalogs. Table references inside Paimon views are now resolved against the Paimon catalog instead of `default_catalog`. [#56058](https://github.com/StarRocks/starrocks/pull/56058) [#70217](https://github.com/StarRocks/starrocks/pull/70217)
- Supports an explicit `schema` parameter in `FILES()` for stable schema control when reading files with schema drift or complex nested types. [#72033](https://github.com/StarRocks/starrocks/pull/72033)
- `get_query_profile()` now retrieves query profile information across all FE nodes, not only the connected FE. [#71123](https://github.com/StarRocks/starrocks/pull/71123)
- Added the `query_id()` built-in function, which returns the UUID of the currently executing query. [#73621](https://github.com/StarRocks/starrocks/pull/73621)
- `CREATE`/`ALTER STORAGE VOLUME` in shared-data mode now validates storage location accessibility (credentials and endpoints) before persisting metadata, failing early on misconfiguration. [#70053](https://github.com/StarRocks/starrocks/pull/70053)
- Added `WebIdentity` token provider support for AWS S3 credentials in BE, matching the existing FE support for `AWS_S3_USE_WEB_IDENTITY_TOKEN_FILE`. [#69966](https://github.com/StarRocks/starrocks/pull/69966)
- Added the `ADMIN SKIP COMMITTED TRANSACTION` command to unblock stuck `COMMITTED` transactions on shared-data tables when publish is permanently blocked by missing `txnlog`, lost segments, or slow remote I/O. [#73553](https://github.com/StarRocks/starrocks/pull/73553)
- `information_schema.tables_config` now pushes down `table_name` predicates to the FE, greatly reducing overhead for single-table lookups. [#73210](https://github.com/StarRocks/starrocks/pull/73210)
- Added missing MySQL 8 columns to `information_schema` tables to improve compatibility with BI tools and JDBC drivers that inspect MySQL 8 schemas during connection introspection. [#73370](https://github.com/StarRocks/starrocks/pull/73370)
- Added the `enable_pipeline_event_scheduler` BE configuration as a cluster-wide kill switch that overrides the per-session variable when set to `false`. [#73264](https://github.com/StarRocks/starrocks/pull/73264)
- Added opt-in wide-string column isolation for statistics collection to reduce per-query memory peaks when collecting statistics on tables with multiple wide string columns. [#73258](https://github.com/StarRocks/starrocks/pull/73258)
- Slow-lock logging now supports per-event rate limiting and configurable stack-capture controls to prevent JVM safepoint stalls under high lock contention. [#73647](https://github.com/StarRocks/starrocks/pull/73647)
- MV refresh log entries now include the database name in the prefix, making log lines distinguishable in multi-tenant deployments where the same MV name exists in multiple schemas. [#73521](https://github.com/StarRocks/starrocks/pull/73521)
- `enable_profile_log` FE configuration is now mutable and can be toggled at runtime via `ADMIN SET FRONTEND CONFIG` without an FE restart. [#73894](https://github.com/StarRocks/starrocks/pull/73894)
- Added the `enable_print_load_profile_to_log` FE configuration (default `false`) to write load profiles (stream load, routine load, broker load, and merge-commit load) to `fe.profile.log`, preserving them even when the in-memory store is evicted by query-profile bursts. [#74150](https://github.com/StarRocks/starrocks/pull/74150)
- `SHOW ROUTINE LOAD` now correctly renders column mappings in `JobProperties` instead of Java object references. [#74199](https://github.com/StarRocks/starrocks/pull/74199)
- `CachingIcebergCatalog` now uses table-level locking instead of catalog-level locking, reducing refresh serialization lag on catalogs with many concurrently active tables. [#73079](https://github.com/StarRocks/starrocks/pull/73079)
- Meta scan (background statistics collection) now handles `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, and `REORDER COLUMN` schema changes gracefully instead of failing with a not-found error on post-change segment files. [#72901](https://github.com/StarRocks/starrocks/pull/72901)
- Sample-based tablet pre-split now covers multi-partition range-distribution tables and Broker Load, enabling first-load parallelism without an existing data-tier baseline. [#73101](https://github.com/StarRocks/starrocks/pull/73101) [#73912](https://github.com/StarRocks/starrocks/pull/73912) [#74048](https://github.com/StarRocks/starrocks/pull/74048)
- MySQL result serialization no longer uses per-row virtual dispatch; a typed column writer is built once per chunk, reducing serialization overhead for wide or large result sets. [#66316](https://github.com/StarRocks/starrocks/pull/66316)
- `DATETIME`/`DATE`-to-string casts now write directly to the output buffer, eliminating per-row heap allocations. [#73801](https://github.com/StarRocks/starrocks/pull/73801)
- Query statistics merge path replaced a `SpinLock` with a lock-free parallel map, reducing CPU usage on large clusters when workers send intermediate or final statistics. [#73796](https://github.com/StarRocks/starrocks/pull/73796)
- Aggregation hash-map and hash-set prefetch is now gated on L2-cache residency, avoiding a 4–9% regression when the bucket array fits in L2. The prefetch distance is now configurable. [#73943](https://github.com/StarRocks/starrocks/pull/73943)
- Pipelined per-segment `.lcrm` reads for light-compaction publish on shared-data Primary Key tables reduce sequential object-storage round-trips. [#73992](https://github.com/StarRocks/starrocks/pull/73992)
- Cold PK-index rebuild scan in shared-data mode is now parallelized across segments, reducing rebuild time when segment reads are remote-I/O bound. [#74249](https://github.com/StarRocks/starrocks/pull/74249)
- Internal queries (statistics collection, task runs, MV refreshes) are now visible in `SHOW PROC '/current_queries'` and can be killed with `KILL QUERY`. [#74488](https://github.com/StarRocks/starrocks/pull/74488)
- Added lake vacuum batch-size and retry-count bvar metrics for monitoring S3 throttling and tuning `lake_vacuum_min_batch_delete_size`. [#74112](https://github.com/StarRocks/starrocks/pull/74112)
- Added `CatalogRecycleBin` size gauge metrics to surface recycle-bin growth before it pressures FE heap. [#74440](https://github.com/StarRocks/starrocks/pull/74440)
- `LIST`-partitioned tables now open all partitions in `OlapTableSink` instead of applying the latest-N heuristic designed for range-partition tables, reducing incremental-open RPC overhead. [#74099](https://github.com/StarRocks/starrocks/pull/74099)
- Supports loading `LARGE_LIST` and `FIXED_SIZE_LIST` Arrow types into JSON columns via `FILES()` or Broker Load. [#73714](https://github.com/StarRocks/starrocks/pull/73714) [#73718](https://github.com/StarRocks/starrocks/pull/73718)
- Supports combined transaction log and file bundling for merge-commit (`FRONTEND_STREAMING`) loads on shared-data tables, aligning them with other load types. [#74460](https://github.com/StarRocks/starrocks/pull/74460)
- Added the mutable FE configuration `slow_publish_partition_log_threshold_ms` (default 3000 ms) to control the lake publish phase-breakdown warning threshold without an FE restart. [#74043](https://github.com/StarRocks/starrocks/pull/74043)

### Security

- [CVE-2026-43869] Bumped `libthrift` to 0.23.0 to address improper certificate host validation. [#73243](https://github.com/StarRocks/starrocks/pull/73243)
- [CVE-2026-41293] Bumped Apache Tomcat to 9.0.118 to address HTTP/2 request header validation. [#73797](https://github.com/StarRocks/starrocks/pull/73797)
- [CVE-2026-45416] [CVE-2026-44249] [CVE-2026-45673] Bumped Netty to 4.1.135.Final to address SNI handler heap exhaustion (DoS), IPv6 subnet filter bypass, and DNS cache poisoning. [#74668](https://github.com/StarRocks/starrocks/pull/74668)
- Upgraded pprof prebuilt binaries to Go 1.25.11 to include Go standard library security fixes. [#73545](https://github.com/StarRocks/starrocks/pull/73545) [#74669](https://github.com/StarRocks/starrocks/pull/74669)

### Bug Fixes

The following issues have been fixed:

- `parse_url()` returned a wrong host when the URL contained `:` outside a `host:port` pattern. [#63542](https://github.com/StarRocks/starrocks/pull/63542)
- Dictionary-translated expressions incorrectly assumed `f(null) = null` for expressions where this does not hold (for example, `IF(col = '1', NULL, 'ok')`). [#69376](https://github.com/StarRocks/starrocks/pull/69376)
- Transaction stream load used the default RPC timeout instead of the user-specified timeout, causing premature timeouts. [#67584](https://github.com/StarRocks/starrocks/pull/67584)
- Iceberg equality delete files with NULL values in identity columns failed to delete matching rows because `NULL = NULL` evaluates to UNKNOWN in the join predicate. [#67321](https://github.com/StarRocks/starrocks/pull/67321)
- Error messages for tables with `INJECTED` partition-projection columns are now more descriptive, showing which columns are causing the issue. [#68052](https://github.com/StarRocks/starrocks/pull/68052)
- Queries on insert-only ACID Hive tables returned more rows than expected because insert-overwrite operations were not recognized. [#71460](https://github.com/StarRocks/starrocks/pull/71460)
- Disk cache overflowed its configured capacity when Iceberg metadata entries were pinned during concurrent reads. [#71651](https://github.com/StarRocks/starrocks/pull/71651)
- Paimon primary key columns were incorrectly marked as non-nullable when querying external catalogs. [#71660](https://github.com/StarRocks/starrocks/pull/71660)
- Optimizer timed out when `MultiDistinctByMultiFuncRewriter` applied the same rule repeatedly on queries with multiple `ARRAY_AGG(DISTINCT <const>)` inputs. [#70605](https://github.com/StarRocks/starrocks/pull/70605)
- Oracle JDBC date predicates pushed down without the `DATE`/`TIMESTAMP` keyword caused NLS format errors. [#71412](https://github.com/StarRocks/starrocks/pull/71412)
- Partition TopN could lose required output columns from its child operator. [#72848](https://github.com/StarRocks/starrocks/pull/72848)
- Unpartitioned MVs could not be created on Iceberg tables with partition evolution. [#72285](https://github.com/StarRocks/starrocks/pull/72285)
- Compaction task stats were overwritten and lost for parallel subtasks in `information_schema.be_cloud_native_compactions`. [#72331](https://github.com/StarRocks/starrocks/pull/72331)
- `SHOW CREATE MATERIALIZED VIEW` for sync MVs failed with "Table is not found". [#73396](https://github.com/StarRocks/starrocks/pull/73396)
- Lake publish multi-statement transactions deadlocked during schema change when statement `.log` files landed at the 4-segment path. [#73423](https://github.com/StarRocks/starrocks/pull/73423)
- Sort merge provider errors were not propagated to the fragment context, causing silent query failures. [#73337](https://github.com/StarRocks/starrocks/pull/73337)
- `ConnectorTableId` overflowed from `int` to negative values on long-running follower FEs, causing Iceberg and Hive queries to fail with misleading "Invalid table type" errors. [#73344](https://github.com/StarRocks/starrocks/pull/73344)
- `ALTER TABLE` with an empty optimize clause (no distribution or partition spec) was incorrectly parsed and could corrupt the table's default distribution on FE replay. [#73352](https://github.com/StarRocks/starrocks/pull/73352)
- FE startup failed during ADLS2 shared-data disaster recovery because `AZURE_PATH_KEY` was not recognized as a valid `StorageVolumeMgr` parameter. [#73509](https://github.com/StarRocks/starrocks/pull/73509)
- Avro complex-type decoding failed when the optimizer pruned part of a nested type to `UNKNOWN_TYPE`, or when nullable array, map, or struct schemas were used. [#73474](https://github.com/StarRocks/starrocks/pull/73474)
- COW column mutation optimization caused a crash in `map_apply` and similar functions due to two `NullableColumn`s sharing the same `NullColumn` object. [#73480](https://github.com/StarRocks/starrocks/pull/73480)
- Iceberg tables with a custom `LocationProvider` failed on `SELECT` queries with `ClassNotFoundException` because the provider was eagerly instantiated on FE. [#73482](https://github.com/StarRocks/starrocks/pull/73482)
- JDBC `getTable()` performed an extra `getTableComment()` round-trip on every cache miss, extending the intensive planning-phase lock hold and blocking concurrent DDL. [#73488](https://github.com/StarRocks/starrocks/pull/73488)
- Nested MV refresh threw `NullPointerException` when a nested MV returned `FULL` or `UNKNOWN` timeliness. [#73644](https://github.com/StarRocks/starrocks/pull/73644)
- FE worker blocked indefinitely sending query results to a slow MySQL client. The result-send path now enforces a write timeout. [#73646](https://github.com/StarRocks/starrocks/pull/73646)
- PK `.del` files were not transcoded when replicating Primary Key tables from a V1-encoded (shared-nothing) to a V2-encoded (shared-data) cluster, or between two shared-data clusters. [#73649](https://github.com/StarRocks/starrocks/pull/73649) [#73958](https://github.com/StarRocks/starrocks/pull/73958)
- Duplicate replicas accumulated in `TabletInvertedIndex` during `VERSION_INCOMPLETE` recovery because the stale replica reference was not removed before adding the live one. [#73661](https://github.com/StarRocks/starrocks/pull/73661)
- Shared-data lake replication file-copy crashed the CN because `REPLICATE_SNAPSHOT` tasks and per-file copy sub-tasks shared the same thread pool. [#73666](https://github.com/StarRocks/starrocks/pull/73666)
- `RuntimeProfileParser` threw `NumberFormatException` when unit counters were formatted with a `.000` decimal suffix by the BE. [#73683](https://github.com/StarRocks/starrocks/pull/73683)
- Physical rowid encoding for shared segments in shared-data PK tablet split was incorrect, causing wrong `rss_rowid` entries. [#73686](https://github.com/StarRocks/starrocks/pull/73686)
- Mixed range-colocate × hash-distributed `JOIN` queries returned `Unknown error` instead of valid results. [#73702](https://github.com/StarRocks/starrocks/pull/73702)
- `TimeUtils.longToTimeString` used a fixed UTC+8 formatter; output now respects the session `time_zone`. [#73619](https://github.com/StarRocks/starrocks/pull/73619)
- Decimal-typed columns lost scale when all values were `NULL` and the column passed through the nullable unary function path, corrupting downstream result types. [#73789](https://github.com/StarRocks/starrocks/pull/73789)
- JSON partial-append for nested types caused an ASAN crash. [#73715](https://github.com/StarRocks/starrocks/pull/73715)
- Privilege cache for the `public` role was not invalidated on `GRANT`/`REVOKE`, leaving stale privileges in effect until expiration. [#73717](https://github.com/StarRocks/starrocks/pull/73717)
- `FlatJson` crashed when sub-writers had no appends. [#73730](https://github.com/StarRocks/starrocks/pull/73730)
- Aggregate MV rewrite was incorrectly applied when the MV itself had a `HAVING` predicate, potentially returning incomplete results. [#73610](https://github.com/StarRocks/starrocks/pull/73610)
- Data race on the spill writer's `auto_flush` flag when entering parallel merge mode caused unintended segment flushes on ARM. [#73616](https://github.com/StarRocks/starrocks/pull/73616)
- Routine load scheduler held a per-job write lock while making blocking BE RPCs to fetch Kafka or Pulsar partition metadata, causing lock-hold times of up to 33.6 s. [#73591](https://github.com/StarRocks/starrocks/pull/73591)
- Colocate tablets on dead BEs were incorrectly reported as `HEALTHY` when `tablet_sched_disable_colocate_balance` was enabled. [#73550](https://github.com/StarRocks/starrocks/pull/73550)
- `ADMIN SHOW REPLICA STATUS` desynchronized the MySQL result stream when a MISSING (phantom) replica row was present, causing client hangs or disconnects. [#74393](https://github.com/StarRocks/starrocks/pull/74393)
- Per-partition coordinator claim was not re-recorded on every sender's `open` RPC in shared-data mode, causing some senders to be missed in the coordinator election and leaving `combined_txn_log` files unwritten. [#73962](https://github.com/StarRocks/starrocks/pull/73962)
- `_statistics_.pipe_file_list` internal table was not re-created after the `_statistics_` database or table was dropped. [#73970](https://github.com/StarRocks/starrocks/pull/73970)
- Task runs force-killed by `TaskCleaner` were not archived, causing them to disappear from `information_schema.task_runs` with no trace. [#74146](https://github.com/StarRocks/starrocks/pull/74146)
- `RENAME TABLE` and `SWAP TABLE`/`SWAP MATERIALIZED VIEW` held only an intensive table lock instead of a database write lock, allowing concurrent readers to observe a torn intermediate name-to-table mapping. [#74100](https://github.com/StarRocks/starrocks/pull/74100)
- PK index compaction output sstables were opened without tablet metadata, causing permanent `metadata is null when loading delvec` failures. [#74037](https://github.com/StarRocks/starrocks/pull/74037)
- A partial-update `INSERT` inside an explicit transaction targeting a table already modified in the same transaction silently corrupted data at `COMMIT`. [#74344](https://github.com/StarRocks/starrocks/pull/74344)
- `ALTER TABLE` operations incompatible with range distribution (schema change, sort-key change) are now rejected with actionable errors instead of silently corrupting metadata. [#74020](https://github.com/StarRocks/starrocks/pull/74020)
- Aggregate functions with type-mismatched children in the optimizer caused incorrect query results. [#74159](https://github.com/StarRocks/starrocks/pull/74159)
- `ALTER ROUTINE LOAD` with a reserved-keyword table name wrote an unparseable `origStmt`, causing column mappings to be lost after FE restart. [#74188](https://github.com/StarRocks/starrocks/pull/74188)
- IVM `state_union` compatibility check did not recurse into nested types (for example, `ARRAY<VARCHAR>`), causing `CREATE MATERIALIZED VIEW` to fail for `ARRAY_AGG` IMVs. [#73627](https://github.com/StarRocks/starrocks/pull/73627)
- Parquet temporary dictionary-code columns leaked to upper layers when a scan range was fully filtered out, causing downstream type mismatches. [#74452](https://github.com/StarRocks/starrocks/pull/74452)
- `CASE WHEN` with mixed float and integer `WHEN` and result types produced invalid JIT IR, causing incorrect results or crashes. [#74382](https://github.com/StarRocks/starrocks/pull/74382)
- JIT compilation failure caused use-after-free of `LLVMContext`, resulting in SIGSEGV. [#74396](https://github.com/StarRocks/starrocks/pull/74396)
- Background statistics tasks overwrote the session `WAREHOUSE` setting, affecting subsequent user queries on the same connection context. [#74385](https://github.com/StarRocks/starrocks/pull/74385)
- `CatalogRecycleBin` stopped erasing entries when no cluster snapshot had ever completed successfully, causing unbounded FE memory growth under heavy `INSERT OVERWRITE` workloads. [#74379](https://github.com/StarRocks/starrocks/pull/74379)
- Non-PK replica version holes were not detected by the FE; queries were permanently routed to holed replicas with a frozen `max_version`. [#74408](https://github.com/StarRocks/starrocks/pull/74408)
- Data race on `MaterializedIndexMeta.updateSchemaBackendId` (a `HashSet` mutated under a shared read lock) could cause lost entries or set corruption. [#74412](https://github.com/StarRocks/starrocks/pull/74412)
- Vacuum watermark was not reported correctly when retain-boundary metadata had already been vacuumed, stalling the `file_bundling` switch-version cleanup. [#74429](https://github.com/StarRocks/starrocks/pull/74429)
- Lake vacuum retry used deterministic exponential backoff; decorrelated jitter is now added to spread retries across CNs under S3 throttling. [#74108](https://github.com/StarRocks/starrocks/pull/74108)
- Memory accounting in `OlapTableSink` was inflated because RPC requests allocated from the query memory pool were released in the process context. [#73807](https://github.com/StarRocks/starrocks/pull/73807)
- Race condition in `TabletSinkSender::_send_chunk_by_node` when automatic partition creation triggered `_incremental_open_node_channel` concurrently. [#73820](https://github.com/StarRocks/starrocks/pull/73820)
- UDAF context created by a backport of upstream changes caused a memory leak via `unique_ptr::release`. [#74025](https://github.com/StarRocks/starrocks/pull/74025)
- Potential out-of-bounds in partitioned join probe caused by inaccurate memory accounting in `append_selective`. [#74315](https://github.com/StarRocks/starrocks/pull/74315)
- `azure_adls2_oauth2_client_endpoint` config field had a typo in its name. [#74581](https://github.com/StarRocks/starrocks/pull/74581)
- `StarMgrMetaSyncer` incorrectly reaped range-colocate PACK shard groups as orphans, permanently deleting active shards in shared-data mode. [#74117](https://github.com/StarRocks/starrocks/pull/74117)
- Sort-key arity for colocate tablet split was resolved from the base schema instead of the materialized schema for PRIMARY KEY tables and tables without an explicit `ORDER BY`, causing split jobs to complete without reducing tablet size. [#74409](https://github.com/StarRocks/starrocks/pull/74409)
- Auto-merge daemon merged pre-split tablets back together when partition data was below the merge threshold, undoing the parallelism benefit of sample-based pre-split. [#74583](https://github.com/StarRocks/starrocks/pull/74583)
- db-level UDFs were missing on follower FEs after `RESTORE ... AS <new_db>` because the function's `FunctionName.db` still pointed at the source database. [#74313](https://github.com/StarRocks/starrocks/pull/74313)
- Wrong CN group was assigned for immutable-partition tablet locations in shared-data `DISTRIBUTED BY RANDOM` CTAS/INSERT when the warehouse had multiple CN groups. [#74316](https://github.com/StarRocks/starrocks/pull/74316)
- `NullPointerException` in `StatisticsCalcUtils` when a partition was dropped concurrently during statistics estimation. [#73711](https://github.com/StarRocks/starrocks/pull/73711)
- `InformationSchemaDataSource` and `FrontendServiceImpl` metadata RPC handlers held full database READ locks, blocking DDL on unrelated tables. [#73936](https://github.com/StarRocks/starrocks/pull/73936) [#73913](https://github.com/StarRocks/starrocks/pull/73913)
- Pipeline operators that flipped their finished state without notifying shared-context observers could stall peer drivers under the event scheduler. [#74055](https://github.com/StarRocks/starrocks/pull/74055) [#74056](https://github.com/StarRocks/starrocks/pull/74056)
- Non-root compound predicates in predicate pushdown yielded `NotPushDown` instead of a scan-level EOF, causing `OlapScanNode` to emit no rows when an impossible nested AND branch was present under UNION. [#74218](https://github.com/StarRocks/starrocks/pull/74218)
- `BackendLoadStatistic.init` performed an expensive per-replica scan on BEs with a single storage medium; the check is now O(1) for homogeneous-disk BEs. [#73555](https://github.com/StarRocks/starrocks/pull/73555)
- Thread-name setting race on data-dir load threads caused noisy `failed to set thread name` warnings on every BE startup. [#73862](https://github.com/StarRocks/starrocks/pull/73862)
- Task manager wrote an illegal `RUNNING→RUNNING` edit log, causing task runs to appear stuck in the running map indefinitely. [#73882](https://github.com/StarRocks/starrocks/pull/73882)
- PK multi-statement batch transactions did not accumulate `num_rows`, `data_size`, and `num_dels` across composite rowsets, causing incorrect row-count statistics on shared-data Primary Key tables. [#74059](https://github.com/StarRocks/starrocks/pull/74059)
- Lake load spill cleanup now uses txn-id-based vacuum-driven reclaim, preventing orphaned spill files after BE crash or OOM. [#73064](https://github.com/StarRocks/starrocks/pull/73064)
- PostgreSQL JDBC time values starting from year `0000` caused incorrect type mapping results. [#70842](https://github.com/StarRocks/starrocks/pull/70842)
- Null-check was missing before reading `gtid` from a rowset during schema change, causing an NPE crash. [#74855](https://github.com/StarRocks/starrocks/pull/74855)



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
- Supports SQL UDF creation via `CREATE FUNCTION ... AS <sql_body>`. [#67558](https://github.com/StarRocks/starrocks/pull/67558)
- Supports loading UDFs from S3. [#64541](https://github.com/StarRocks/starrocks/pull/64541)
- Added the `uuid_v7` function, which generates time-ordered UUID v7 values. [#67694](https://github.com/StarRocks/starrocks/pull/67694)
- Added per-catalog-type query metrics for external catalog observability. [#70533](https://github.com/StarRocks/starrocks/pull/70533)
- Supports an explicit skew hint for window functions, automatically optimizing window functions with skewed partition keys by splitting into UNION. [#68739](https://github.com/StarRocks/starrocks/pull/68739)

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
- Use-after-free in parallel segment/rowset loading on the error path. [#71083](https://github.com/StarRocks/starrocks/pull/71083)
- Potential hash table data loss in aggregation spill `set_finishing`. [#70851](https://github.com/StarRocks/starrocks/pull/70851)
- PK tablet rowset meta loss caused by a GC race during disk re-migration (A→B→A). [#70727](https://github.com/StarRocks/starrocks/pull/70727)
- DB read lock leak in `SharedDataStorageVolumeMgr`. [#70987](https://github.com/StarRocks/starrocks/pull/70987)
- IVM refresh recording incomplete PCT partition metadata. [#71092](https://github.com/StarRocks/starrocks/pull/71092)
- NPE when analyzing generated columns in Stream Load/Broker Load if a referenced column is missing. [#71116](https://github.com/StarRocks/starrocks/pull/71116)
- Missing partition predicate in short-circuit point lookup. [#71124](https://github.com/StarRocks/starrocks/pull/71124)

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

- Supports reading file path and row position metadata columns from Iceberg tables. [#67003](https://github.com/StarRocks/starrocks/pull/67003)
- Supports reading `_row_id` from Iceberg v3 tables, and supports global late materialization for Iceberg v3. [#62318](https://github.com/StarRocks/starrocks/pull/62318) [#64133](https://github.com/StarRocks/starrocks/pull/64133)
- Supports creating Iceberg views with custom properties, and displays properties in SHOW CREATE VIEW output. [#65938](https://github.com/StarRocks/starrocks/pull/65938)
- Supports querying Paimon tables with a specific branch, tag, version, or timestamp. [#63316](https://github.com/StarRocks/starrocks/pull/63316)
- Supports complex types (ARRAY, MAP, STRUCT) for Paimon tables. [#66784](https://github.com/StarRocks/starrocks/pull/66784)
- Supports Partition Transforms with parentheses syntax when creating Iceberg tables. [#68945](https://github.com/StarRocks/starrocks/pull/68945)
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
- Supports column comments for PostgreSQL tables in `information_schema`. [#70520](https://github.com/StarRocks/starrocks/pull/70520)
- Improved Oracle and PostgreSQL JDBC type mapping. [#70315](https://github.com/StarRocks/starrocks/pull/70315) [#70566](https://github.com/StarRocks/starrocks/pull/70566)

### Query Engine

- **Recursive CTE**

  Supports Recursive Common Table Expressions (CTEs) for hierarchical traversals, graph queries, and iterative SQL computations. [#65932](https://github.com/StarRocks/starrocks/pull/65932)

- Improved Skew Join v2 rewrite with statistics-based skew detection, histogram support, and NULL-skew awareness. [#68680](https://github.com/StarRocks/starrocks/pull/68680) [#68886](https://github.com/StarRocks/starrocks/pull/68886)
- Improved COUNT DISTINCT over windows and added support for fused multi-distinct aggregations. [#67453](https://github.com/StarRocks/starrocks/pull/67453)
- Supports explicit skew hint for window functions, with automatic optimization of window functions with skewed partition keys by splitting into UNION. [#67944](https://github.com/StarRocks/starrocks/pull/67944)
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
  - `raise_error`: Raises a user-defined error in SQL expressions. [#69661](https://github.com/StarRocks/starrocks/pull/69661)
- Provides the following function or syntactic extensions:
  - Supports a lambda comparator in `array_sort` for custom sort ordering. [#66607](https://github.com/StarRocks/starrocks/pull/66607)
  - Supports USING clause for FULL OUTER JOIN with SQL-standard semantics. [#65122](https://github.com/StarRocks/starrocks/pull/65122)
  - Supports DISTINCT aggregation over framed window functions with ORDER BY/PARTITION BY. [#65815](https://github.com/StarRocks/starrocks/pull/65815) [#65030](https://github.com/StarRocks/starrocks/pull/65030) [#67453](https://github.com/StarRocks/starrocks/pull/67453)
  - Supports ARRAY type in `lead`/`lag`/`first_value`/`last_value` window functions. [#63547](https://github.com/StarRocks/starrocks/pull/63547)
  - Supports VARBINARY for count distinct-like aggregate functions. [#68442](https://github.com/StarRocks/starrocks/pull/68442)
  - Supports date and string type casting in IN expressions. [#61746](https://github.com/StarRocks/starrocks/pull/61746)
  - Supports WITH LABEL syntax for BEGIN/START TRANSACTION. [#68320](https://github.com/StarRocks/starrocks/pull/68320)
  - Supports WHERE/ORDER/LIMIT clauses in SHOW statements. [#68834](https://github.com/StarRocks/starrocks/pull/68834)
  - Supports `ALTER TASK` statements for task management. [#68675](https://github.com/StarRocks/starrocks/pull/68675)
  - Supports multiple compression formats (GZIP/SNAPPY/ZSTD/LZ4/DEFLATE/ZLIB/BZIP2) for CSV file exports. [#68054](https://github.com/StarRocks/starrocks/pull/68054)
  - Supports `STRUCT_CAST_BY_NAME` SQL mode for name-based struct field matching. [#69845](https://github.com/StarRocks/starrocks/pull/69845)

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
- Supports adding FE observer nodes. [#67778](https://github.com/StarRocks/starrocks/pull/67778)
- Supports Merge Commit information in `information_schema.loads` for better load job visibility. [#67879](https://github.com/StarRocks/starrocks/pull/67879)
- Supports showing tablet status in cloud-native tables for better troubleshooting. [#69616](https://github.com/StarRocks/starrocks/pull/69616)

### Security

- [CVE-2026-33870] [CVE-2026-33871] Replaced AWS bundle and bumped Netty to 4.1.132.Final. [#71017](https://github.com/StarRocks/starrocks/pull/71017)
- [CVE-2025-27821] Upgraded Hadoop to v3.4.2. [#68529](https://github.com/StarRocks/starrocks/pull/68529)
- [CVE-2025-54920] Upgraded `spark-core_2.12` to 3.5.7. [#70862](https://github.com/StarRocks/starrocks/pull/70862)

### Bug Fixes

The following issues have been fixed:

- Fixed data loss after tablet split by skipping data file deletion for range distribution tablets. [#71135](https://github.com/StarRocks/starrocks/pull/71135)
- Fixed a memory leak in `DefaultValueColumnIterator` for complex types. [#71142](https://github.com/StarRocks/starrocks/pull/71142)
- Fixed a memory leak caused by `shared_ptr` cycle between `BatchUnit` and `FetchTaskContext`. [#71126](https://github.com/StarRocks/starrocks/pull/71126)
- Fixed double-free crash in SystemMetrics due to concurrent getline access. [#71040](https://github.com/StarRocks/starrocks/pull/71040)
- Fixed crash in SpillMemTableSink when eager merge consumes all blocks. [#69046](https://github.com/StarRocks/starrocks/pull/69046)
- Fixed NPE when auto-created partition is dropped by TTL cleaner. [#68257](https://github.com/StarRocks/starrocks/pull/68257)
- Fixed NPE in `IcebergCatalog.getPartitionLastUpdatedTime` when snapshot is expired. [#68925](https://github.com/StarRocks/starrocks/pull/68925)
- Fixed incorrect predicate rewrite for outer join with constant-side column reference. [#67072](https://github.com/StarRocks/starrocks/pull/67072)
- Fixed error query results after modify CHAR column length in shared-data. [#68808](https://github.com/StarRocks/starrocks/pull/68808)
- Fixed MV refresh bug in the case of multiple tables. [#61763](https://github.com/StarRocks/starrocks/pull/61763)
- Fixed incorrect MV recycle time if force refreshed. [#68673](https://github.com/StarRocks/starrocks/pull/68673)
- Fixed all-null value handling bug in sync MV. [#69136](https://github.com/StarRocks/starrocks/pull/69136)
- Fixed duplicate column id error when querying MV after fast schema change ADD COLUMN. [#71072](https://github.com/StarRocks/starrocks/pull/71072)
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
- Fixed FULL OUTER JOIN USING with constant subqueries. [#69028](https://github.com/StarRocks/starrocks/pull/69028)
- Fixed join on clause bug with CTE scope. [#68809](https://github.com/StarRocks/starrocks/pull/68809)
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
- Fixed case-insensitive username normalization for LDAP authentication. [#67966](https://github.com/StarRocks/starrocks/pull/67966)
- Mitigated SSRF risk for API `proc_file`. [#68997](https://github.com/StarRocks/starrocks/pull/68997)
- Masked user auth strings in audit and SQL redaction. [#70360](https://github.com/StarRocks/starrocks/pull/70360)

### Behavior Changes

- ETL execution mode optimizations are now enabled by default. This benefits INSERT INTO SELECT, CREATE TABLE AS SELECT, and similar batch workloads without explicit configuration changes. [#66841](https://github.com/StarRocks/starrocks/pull/66841)
- The third argument of `lag`/`lead` window functions now supports column references in addition to constant values. [#60209](https://github.com/StarRocks/starrocks/pull/60209)
- FULL OUTER JOIN USING now follows SQL-standard semantics: the USING column appears once in the output instead of twice. [#65122](https://github.com/StarRocks/starrocks/pull/65122)
- `query_queue_v2` is now enabled by default. [#67462](https://github.com/StarRocks/starrocks/pull/67462)
- SQL transactions are gated behind the session variable `enable_sql_transaction` by default. [#63535](https://github.com/StarRocks/starrocks/pull/63535)
