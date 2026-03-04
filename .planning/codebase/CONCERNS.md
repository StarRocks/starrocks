# Codebase Concerns

**Analysis Date:** 2026-03-04

## Tech Debt

**GlobalStateMgr God Object (FE):**
- Issue: `GlobalStateMgr` is a ~2900-line god class managing every piece of cluster state. Called directly via static `GlobalStateMgr.getCurrentState()` 3336+ times across the codebase.
- Files: `fe/fe-core/src/main/java/com/starrocks/server/GlobalStateMgr.java`
- Impact: Untestable without a full cluster, all FE code tightly coupled to one singleton, prevents modular testing
- Fix approach: Decompose responsibilities into injected managers; the class itself notes this division exists (`getWarehouseMgr`, `getMetadataMgr`) but callers bypass it via the static accessor

**AstBuilder Monolith (FE):**
- Issue: `AstBuilder.java` is 9909 lines — a single visitor class covering all SQL syntax. Any grammar addition grows this file further.
- Files: `fe/fe-core/src/main/java/com/starrocks/sql/parser/AstBuilder.java`
- Impact: Merge conflicts, cognitive load, hard to unit test individual visitors
- Fix approach: Split by statement category (DDL, DML, Analytics, etc.) using delegation pattern

**SessionVariable Monolith (FE):**
- Issue: `SessionVariable.java` is 6303 lines with 400+ individual session variables, heavily annotated and serialized in one class.
- Files: `fe/fe-core/src/main/java/com/starrocks/qe/SessionVariable.java`
- Impact: Difficult to add new variables without conflicts; the class contains 2 `@Deprecated` variables that remain active
- Fix approach: Group into feature-specific sub-objects; deprecated fields `QUERY_DEBUG_OPTIONS` and `CBO_DEBUG_ALIVE_BACKEND_NUMBER` can be removed

**Deprecated Config Parameters (FE):**
- Issue: 11 config fields in `Config.java` marked `@Deprecated` but still present and potentially referenced. Examples include configs replaced by new MV workgroup defaults.
- Files: `fe/fe-core/src/main/java/com/starrocks/common/Config.java` (lines 106, 683, 687, 1134, 1613, 1896, 2831, 2838, 2857, 3755, 3759)
- Impact: Misleading configuration surface; users may set deprecated configs expecting effect
- Fix approach: Remove deprecated configs after verifying no callers; add migration notices to docs

**Duplicated AsyncDeltaWriter (BE):**
- Issue: Two nearly identical `AsyncDeltaWriter` classes exist for local and lake tablet storage.
- Files: `be/src/storage/async_delta_writer.h`, `be/src/storage/lake/async_delta_writer.h`
- Impact: Bug fixes must be applied twice; classes diverge over time
- Fix approach: Unify behind a common interface; `TODO` in `be/src/storage/async_delta_writer.h:45` acknowledges this

**TaskRunHistory Thread Safety (FE):**
- Issue: `TaskRunHistory` is explicitly noted as not thread safe but used in concurrent scheduler contexts.
- Files: `fe/fe-core/src/main/java/com/starrocks/scheduler/history/TaskRunHistory.java:123`
- Impact: Potential data corruption in MV refresh history under concurrent task runs
- Fix approach: Add `ConcurrentLinkedDeque` or per-entry locking

**TaskRun Session Pollution (FE):**
- Issue: FIXME comment documents that session variable setting during task runs pollutes the global session context.
- Files: `fe/fe-core/src/main/java/com/starrocks/scheduler/TaskRun.java:375`
- Impact: MV refresh tasks may leak variables to other sessions
- Fix approach: Use session scoping or snapshot the session before modification

## Known Bugs

**TabletUpdatesPB Lost on Meta Copy (BE):**
- Symptoms: When generating a tablet meta copy (e.g., during clone), `TabletUpdatesPB` (primary key update state) is silently dropped from the copy.
- Files: `be/src/storage/tablet.cpp:1611`, `be/src/storage/tablet.cpp:1622`
- Trigger: `generate_tablet_meta_copy()` and `generate_tablet_meta_copy_unlocked()` calls during tablet clone or schema change
- Workaround: None noted; primary key tables may experience incomplete meta during clone

**TabletSchemaMap Potential Leak (BE):**
- Symptoms: If `tablet_schema_map` of a `TabletSchema` is null during `emplace`, a potential memory leak occurs.
- Files: `be/src/storage/tablet_schema_map.cpp:151`
- Trigger: Race condition when a weak pointer has expired before insertion completes
- Workaround: None; self-acknowledged as `TODO: (BUG)`

**ZoneMap Row Count Incorrect (BE):**
- Symptoms: Zone map detail records segment rows, not zone rows, making zone-level row count statistics incorrect.
- Files: `be/src/storage/zone_map_detail.h:48`
- Trigger: Any query relying on zone map statistics for cardinality estimation
- Workaround: None; noted as `FIXME`

**MV Refresh Not Triggered in New Publish Mechanism (FE):**
- Symptoms: After switching to new publish mechanism, MV refresh from publish version daemon is broken.
- Files: `fe/fe-core/src/main/java/com/starrocks/transaction/PublishVersionDaemon.java:306`
- Trigger: Committing transactions to tables with dependent MVs under new publish path
- Workaround: None noted; `FIXME(murphy)` outstanding

**BinaryColumn FIXME: Build Slice Side Effect (BE):**
- Symptoms: `BinaryColumn::get_data()` calls `build_slice()` which modifies column memory data, making it unsafe to call on const columns in multi-threaded contexts.
- Files: `be/src/column/column_helper.h:87`
- Trigger: Reading binary columns concurrently
- Workaround: Acknowledged FIXME; callers must be careful

**Column `slice` Assumption (BE):**
- Symptoms: Many derived `Column` implementations assume `|to|` equals `size()` in the `slice()` method. Passing other values produces incorrect results.
- Files: `be/src/column/column.h:367`
- Trigger: Calling `slice()` with `to != size()` on derived column types
- Workaround: Only call with `to == size()`

**Persistent Index NotSupported Stub (BE):**
- Symptoms: A method in `PersistentIndex` returns `Status::NotSupported("TODO")` — calling it yields an error.
- Files: `be/src/storage/persistent_index.cpp:3587`
- Trigger: Any code path that calls the unimplemented method

**Pipe File Piece FIXME (FE):**
- Symptoms: `FilePipePiece` has an unimplemented method marked `FIXME: implement it`.
- Files: `fe/fe-core/src/main/java/com/starrocks/load/pipe/FilePipePiece.java:59`
- Impact: File pipe features relying on this method silently fail or throw

## Security Considerations

**LDAP SSL Insecure Default (FE):**
- Risk: `authentication_ldap_simple_ssl_conn_allow_insecure` defaults to `true`, allowing LDAP authentication over unencrypted connections out of the box.
- Files: `fe/fe-core/src/main/java/com/starrocks/common/Config.java:2059`, `fe/fe-core/src/main/java/com/starrocks/authentication/SimpleLDAPSecurityIntegration.java`
- Current mitigation: Config option exists to disable, but default is permissive
- Recommendations: Change default to `false`; document security implications prominently

**SSL Force Secure Transport Disabled (FE):**
- Risk: `ssl_force_secure_transport` defaults to `false`, meaning plaintext MySQL protocol connections are accepted even when SSL is configured.
- Files: `fe/fe-core/src/main/java/com/starrocks/common/Config.java:3467`
- Current mitigation: Config can be enabled by operator
- Recommendations: Promote awareness in deployment guides; consider `true` default for new deployments

**Missing Privilege Checks in FrontendService (FE):**
- Risk: Multiple `TODO: check privilege` comments in `FrontendServiceImpl.java` indicate endpoints accessible without full authorization.
- Files: `fe/fe-core/src/main/java/com/starrocks/service/FrontendServiceImpl.java:583`, `fe/fe-core/src/main/java/com/starrocks/service/FrontendServiceImpl.java:621`
- Current mitigation: Backend APIs are typically internal (Thrift), not directly user-facing
- Recommendations: Audit all `TODO: check privilege` sites; add authorization guards

**Missing Privilege Checks for SHOW Commands (FE):**
- Risk: Several `information_schema` SHOW endpoints have `TODO(yiming): support showing user privilege info` comments — currently returning data without privilege filtering.
- Files: `fe/fe-core/src/main/java/com/starrocks/service/FrontendServiceImpl.java:742`, `752`, `792`
- Current mitigation: None noted

**Encryption Not Supported in Shared-Nothing Mode (BE):**
- Risk: Segment rewrite and rowset update state operations in non-cloud-native (shared-nothing) mode do not encrypt data even if encryption is configured.
- Files: `be/src/storage/rowset_update_state.cpp:789`, `be/src/storage/rowset/segment_rewriter.cpp:59`
- Current mitigation: Only affects shared-nothing deployments; shared-data (lake) mode has encryption
- Recommendations: Implement encryption support in shared-nothing path or document the gap clearly

**Broker Filesystem No Encryption (BE):**
- Risk: `BrokerFileSystem` and `MemoryFileSystem` return `Status::NotSupported` for encrypted writes/reads, silently refusing encryption.
- Files: `be/src/fs/fs_broker.cpp:269`, `be/src/fs/fs_memory.cpp:421`
- Current mitigation: Returns error rather than silently proceeding
- Recommendations: Document which filesystems support encryption

## Performance Bottlenecks

**Persistent Index I/O: One-by-One Operations (BE):**
- Problem: Persistent index reads and writes keys one at a time instead of batching, causing excessive I/O.
- Files: `be/src/storage/persistent_index.cpp:1431-1499`
- Cause: Current implementation iterates map entries individually; TODOs note batching as the fix
- Improvement path: Implement bulk read/write buffer approach; estimate ~10x I/O reduction

**Persistent Index Iterator Workaround (BE):**
- Problem: Iterator entries are erased and re-inserted to simulate in-place modification, doubling map operations.
- Files: `be/src/storage/persistent_index.cpp:1261`, `1325`, `1348`, `1396`, `4393`, `4446`, `4521`, `4552`
- Cause: phmap iterator doesn't support direct value modification
- Improvement path: Dive into phmap internals (`ctrl_` + `slot_`) for direct modification, or use a different map

**Column Hash: Element-by-Element for Arrays/Maps (BE):**
- Problem: Array and map column hashing processes elements one-by-one rather than vectorizing across the batch.
- Files: `be/src/column/column_hash/column_hash.cpp:333`
- Cause: Complex element access patterns prevent straightforward SIMD
- Improvement path: Build intermediate flat buffer then apply SIMD hash

**Fragment Deployment Serialization Bottleneck (FE):**
- Problem: Fragment instance serialization during query deployment is single-threaded.
- Files: `fe/fe-core/src/main/java/com/starrocks/qe/scheduler/Deployer.java:435`
- Cause: Sequential serialization loop; `Todo: consider parallel serialize if this become bottleneck`
- Improvement path: Parallelize serialization with a thread pool

**Client Cache Global Lock (BE):**
- Problem: `ClientCache` uses a single global lock for all operations; multiple TODOs note this as a scaling concern.
- Files: `be/src/runtime/client_cache.h:66-113`
- Cause: Single `_lock` protecting entire client pool
- Improvement path: Per-address client sub-caches; lock-free data structures

**Sort: Unnecessary Null Processing (BE):**
- Problem: Sort algorithms process null datums alongside valid data instead of separating them first.
- Files: `be/src/exec/sorting/sort_helper.h:77`, `143`
- Cause: Sort operates on full columns including nulls; TODOs explicitly note overhead
- Improvement path: Separate null/non-null before sort, merge after

**Cascade Merge: Whole-Chunk Copies (BE):**
- Problem: Cascade merge copies entire chunks when only order-by columns need to be compared and moved.
- Files: `be/src/exec/sorting/merge_cascade.cpp:205`, `225`
- Cause: No projection-before-copy optimization
- Improvement path: Copy only sort-key columns during cascade; project full row at finalization

**ES Scroll Parser: Row-at-a-Time (BE):**
- Problem: Elasticsearch scroll parser fills chunk row-by-row instead of column-by-column.
- Files: `be/src/exec/es/es_scroll_parser.cpp:191`
- Cause: Original row-oriented design; column fill is more cache-friendly
- Improvement path: Accumulate column arrays then batch-write to chunk

**Binary/Fixed-Length Column SIMD Gaps (BE):**
- Problem: Several copy operations in `BinaryColumn` and `FixedLengthColumn` lack SIMD acceleration.
- Files: `be/src/column/binary_column.cpp:184`, `be/src/column/fixed_length_column_base.cpp:112`
- Cause: Manual implementation not yet vectorized
- Improvement path: Apply `memcpy`/AVX2 bulk copy patterns already used elsewhere

## Fragile Areas

**Incremental Materialized Views (IVM) Infrastructure (FE/BE):**
- Files: `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/MVMaintenanceJob.java`, `MVMaintenanceTask.java`, `TxnBasedEpochCoordinator.java`, `IMTCreator.java`
- Why fragile: 20+ TODO/FIXME comments across the IVM subsystem; stop/pause/resume not implemented (throw `UnsupportedException`); plan serialization missing; multi-threading not implemented in `MVJobExecutor`; atomic epoch/transaction commit not implemented
- Safe modification: IVM appears to be in active development; assume any new feature here is incomplete; test heavily
- Test coverage: Limited; many paths throw `UnsupportedException` making testing trivial but incomplete

**Partition Traits for External Catalogs (FE):**
- Files: `fe/fe-core/src/main/java/com/starrocks/connector/partitiontraits/KuduPartitionTraits.java:38`, `DeltaLakePartitionTraits.java:40`, `HudiPartitionTraits.java:44`
- Why fragile: `getUpdatedPartitionNames()` returns `null` for Kudu and returns empty set for Hudi/Delta Lake, meaning MV partition-change tracking silently fails for these external table types
- Safe modification: Never assume external table partition change detection works for Kudu, Delta Lake, or Hudi without testing
- Test coverage: Likely untested since the methods are stubs

**Streaming/IVM State Tables (BE):**
- Files: `be/src/exec/stream/state/mem_state_table.cpp:171`, `be/src/exec/stream/aggregate/`
- Why fragile: Multiple `TODO` comments about required nullable support, column handling, and batch flush; streaming aggregate state is in-memory only with no spill support
- Safe modification: Avoid writing data to streaming state tables in production until flush/persist is confirmed working

**Arrow Flight SQL Service (FE):**
- Files: `fe/fe-core/src/main/java/com/starrocks/service/arrow/flight/sql/ArrowFlightSqlServiceImpl.java`
- Why fragile: `ConnectContext` is not designed for concurrent multi-statement execution (TODO at line 834); prepared statements do not support parameters (line 165); only single-endpoint responses (line 899)
- Safe modification: Treat Arrow Flight SQL as single-connection-at-a-time; parameterized queries via prepared statements are unsupported

**Binlog File Reader I/O (BE):**
- Files: `be/src/storage/binlog_file_reader.cpp:216-373`
- Why fragile: 6 TODO comments about buffer reuse, header hints, and sequential read optimization; current implementation does redundant I/O reads on every page
- Safe modification: Performance degrades linearly with binlog volume; benchmark before relying on binlog for high-throughput CDC

**Pipeline COW Migration (BE):**
- Files: `be/src/column/chunk.h:412-451`
- Why fragile: Copy-on-write (COW) migration is in progress; 4 `get_column_raw_ptr_*` methods use `const_cast` to return mutable pointers, which is unsafe if COW is partially enforced
- Safe modification: Do not add new callers of `get_column_raw_ptr_*`; use `get_column` with explicit mutability

## Scaling Limits

**FE Single-Leader Architecture:**
- Current capacity: 1 active leader FE; all DDL, metadata writes, and transaction coordination go through leader
- Limit: FE leader becomes CPU and memory bottleneck at very high DDL/transaction rates; `GlobalStateMgr` holds all metadata in-heap
- Scaling path: Horizontal scaling of FE requires BDB-JE journal replication; no sharding of metadata store

**Tablet Scheduler Single-Thread (FE):**
- Current capacity: `TabletScheduler` dynamically assigns clone slots but slot count is heuristic-based, not stats-driven
- Limit: `TODO(cmy): update the slot num based on statistic` — current static slot allocation may over- or under-provision
- Scaling path: Implement dynamic slot allocation based on BE load metrics (line 1956)

**MV Job Executor Single-Threaded (FE):**
- Current capacity: `MVJobExecutor` runs single-threaded
- Limit: `TODO(murphy) extend executor to multi-threading` — concurrent MV maintenance jobs cannot parallelize
- Scaling path: Introduce thread pool for MVJobExecutor

## Dependencies at Risk

**Vendored HiveMetaStoreClient (FE):**
- Risk: A full copy of `HiveMetaStoreClient.java` (2579 lines) and related Hive classes are vendored in-tree under `org.apache.hadoop.hive.*` with local modifications.
- Files: `fe/fe-core/src/main/java/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java`
- Impact: Security patches and upstream bug fixes in Apache Hive must be manually ported; divergence accumulates over time
- Migration plan: Use upstream Hive metastore client with extension points instead of fork

**Vendored Iceberg Classes (FE):**
- Risk: Core Iceberg scanning classes are copied and modified in-tree (`StarRocksIcebergTableScan`, `ManifestGroup`, `DeleteFileIndex`, `ManifestReader`).
- Files: `fe/fe-core/src/main/java/org/apache/iceberg/`
- Impact: Cannot upgrade Iceberg version without resolving forks; Iceberg's deletion vector and encryption features require re-porting
- Migration plan: Contribute upstream or use official extension APIs

## Missing Critical Features

**IVM Pause/Resume/Stop:**
- Problem: `MVMaintenanceJob` throws `UnsupportedException` for pause, resume, and stop operations on running incremental materialized views.
- Files: `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/MVMaintenanceJob.java:135`, `148`, `152`, `191`
- Blocks: Operators cannot gracefully control IVM jobs; any failure leaves jobs in an uncontrollable state

**Kudu/Delta Lake/Hudi Partition Change Tracking:**
- Problem: MV partition change detection returns `null` (Kudu) or empty set (Delta Lake, Hudi), disabling incremental MV refresh for these table types.
- Files: `fe/fe-core/src/main/java/com/starrocks/connector/partitiontraits/` (all three traits)
- Blocks: Partition-level MV refresh (`PCT refresh`) for Kudu, Delta Lake, and Hudi catalogs

**Elasticsearch Metadata Caching:**
- Problem: `ElasticsearchMetadata` has no metadata cache, fetching schema on every query.
- Files: `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/ElasticsearchMetadata.java:34`
- Blocks: High-frequency ES queries experience repeated schema fetch latency

**Persistent Index Full Snapshot Heuristic (BE):**
- Problem: The heuristic for deciding when to build a new snapshot vs. appending WAL is commented as potentially needing improvement.
- Files: `be/src/storage/persistent_index.cpp:4188`
- Blocks: WAL may grow unbounded without proper snapshot triggering

**IMT (Incremental Maintenance Table) Partition Support:**
- Problem: IMT tables cannot be partitioned; noted as `TODO(murphy) support partition the IMT`.
- Files: `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/IMTCreator.java:221`
- Blocks: Large incremental MV refreshes cannot use partition pruning

## Test Coverage Gaps

**IVM/Streaming MV Code Paths:**
- What's not tested: IVM stop/pause/resume (throw immediately), `MVMaintenanceTask` coordinator, `TxnBasedEpochCoordinator` epoch management, `IMTCreator` partition support
- Files: `fe/fe-core/src/main/java/com/starrocks/scheduler/mv/` (multiple files)
- Risk: Silent regressions in MV maintenance scheduling; behavior is undefined for error paths
- Priority: High

**External Catalog Partition Traits:**
- What's not tested: `KuduPartitionTraits.getUpdatedPartitionNames()` returns null; `DeltaLakePartitionTraits` returns null; `HudiPartitionTraits` returns empty set — these are functional stubs
- Files: `fe/fe-core/src/main/java/com/starrocks/connector/partitiontraits/`
- Risk: MV refresh on external tables silently refreshes nothing or crashes with NPE
- Priority: High

**Persistent Index Unimplemented Methods (BE):**
- What's not tested: `persistent_index.cpp:3587` returns `Status::NotSupported("TODO")` — the method is callable but always fails
- Files: `be/src/storage/persistent_index.cpp`
- Risk: Code paths reaching the stub produce unexpected errors in production
- Priority: Medium

**Stream State Table Correctness (BE):**
- What's not tested: Nullable column handling in `MemStateTable`, multi-column aggregate support, async flush
- Files: `be/src/exec/stream/state/`, `be/src/exec/stream/aggregate/`
- Risk: Streaming MV aggregations may produce incorrect results for nullable inputs
- Priority: High

**Tablet Clone Meta Completeness (BE):**
- What's not tested: `TabletUpdatesPB` survival across `generate_tablet_meta_copy()` — explicitly marked `FIXME`
- Files: `be/src/storage/tablet.cpp:1611`, `1622`
- Risk: Primary key tablet clones may lose update state silently
- Priority: High

---

*Concerns audit: 2026-03-04*
