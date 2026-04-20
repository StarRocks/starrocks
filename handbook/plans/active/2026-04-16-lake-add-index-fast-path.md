# Lake ADD/DROP INDEX Fast Path

- Status: active — all 5 SQL e2e green (interleaved + metrics included) on real shared-data cluster
- Owner: Schema Change
- Last Updated: 2026-04-21 (interleaved e2e + IDG metrics)

## Summary

Build out the lake-only fast path for `ALTER TABLE ... ADD INDEX` and
`DROP INDEX` so that adding/dropping a bloom-filter / n-gram bloom / bitmap /
GIN index does not rewrite segment data. New indexes land as standalone
`.idx` files (Index Delta Group, IDG) parallel to the existing Delta Column
Group `.cols` files for generated columns. DROP INDEX is a logical
tombstone with physical cleanup deferred to compaction.

## Acceptance Criteria

- Adding a BITMAP/NGRAMBF/GIN index on a lake table writes per-segment
  `.idx` files; segment data is untouched.
- Queries over post-alter snapshots prefer the IDG-backed index; older
  snapshots still see the pre-alter footer index (or no index).
- DROP INDEX is metadata-only; physical `.idx` reclamation happens when
  compaction next rebuilds the segment.
- Compaction in flight while ADD INDEX runs is detected and falls back to
  the existing apply-with-conflict branch.
- All BE module-boundary, gen-config-fwd, gensrc schema-compat, and
  render_be_agents harness checks pass.

## Branch / Commit Trail

Branch: `claude/optimize-tablet-schema-change-L6gM8`

Commits in chronological order:
1. Foundation (proto / thrift / config / pool / SegmentTaskRunner skeleton).
2. MetaFileBuilder apply_add_index / apply_drop_index + DROP flow + vacuum.
3. IndexFileWriter + SegmentReadOptions.idg_loader wiring.
4. IndexFileReader + compaction conflict check.
5. AddIndexSchemaChange (BITMAP) + do_process_add_index_only full impl.
6. ColumnReader bitmap iterator prefers IDG (bitmap read-side).
7. Owner-wrapped bitmap iterator (leak fix) + docs (EN/ZH) + BE UTs
   (index_file_writer_reader_test, index_delta_group_loader_test).
8. AddIndexSchemaChange extended to BLOOM_FILTER / NGRAMBF builder.
9. NGRAMBF read path (ColumnReader::bloom_filter prefers IDG) +
   BloomFilterOptions parsing from TabletIndexPB.index_properties +
   compaction post-publish IDG cleanup + FE classifier hook in
   SchemaChangeHandler + FE UT for SchemaChangeIndexFastPathClassifier.
10. FE LakeTableIndexFastPathJobBase + LakeTableAddIndexJob +
    LakeTableDropIndexJob (abstract-base + 2 concrete subclasses);
    SchemaChangeHandler dispatch via tryBuildLakeAddIndexJob /
    tryBuildLakeDropIndexJob; FE UT for Job construction + copyForPersist.
11. TSP integration fixups needed to get the PR to a green build on
    origin/upstream main (11 consecutive build attempts before SUCCESS):
    - metrics: METRICS_DEFINE_THREAD_POOL(lake_schema_change) was missing
    - BitmapIndexIterator needed defaulted move ctor (user-declared dtor
      suppressed it, std::move fell back to deleted copy)
    - ASSIGN_OR_RETURN(bool /*first_load*/, ...) doesn't expand to valid
      Java/C++ (comment stripped, left `bool  = ...`); bind to a named var
    - add_index_schema_change.h needed segment.pb.h for ColumnIndexMetaPB
    - upstream main removed Column::raw_data(); replaced 4 call sites with
      RawDataVisitor
    - enable_transparent_data_encryption lives in config_rowset_fwd.h
    - down_cast<const NullableColumn&>(*mutable_ptr_deref) needs non-const
      target to agree on constness; drop the const qualifier
    - NonPrimaryKeyTxnLogApplier has no _builder field; construct a local
      MetaFileBuilder for ADD/DROP INDEX ops
    - FE API drift: getMaterializedIndices -> getAllMaterializedIndices,
      getBaseIndex -> getLatestBaseIndex, getColumnByUniqueId(ColumnId) ->
      getColumn(ColumnId), Column.isBloomFilterColumn/setIsBloomFilterColumn
      don't exist (drop per-column flips; bloom-filter columns are
      table-level via bfColumns), new ColumnId(String) private
    - Utils.publishVersion takes TxnInfoPB not bare long txn-id; wrap it
      like LakeTableAlterMetaJobBase
    - getInfo() must emit 13-14 columns aligned to SchemaChangeProcDir
      TITLE_NAMES (JobId/TableName/CreateTime/FinishTime/IndexName/IndexId/
      OriginIndexId/SchemaVersion/TransactionId/State/Msg/Progress/Timeout/
      Warehouse). Previous 11-column output caused "Malformed packet" on
      SHOW ALTER TABLE COLUMN.

## Current Status (2026-04-20)

Build 1163 (commit 8be915e / b322137) landed green on main: BE + FE both
compile against current origin main and all checkstyle rules pass.

TSP cluster `hujie-lake-idx-e2e3` (1 FE + 3 CN, shared-data, 24h) has
been applied using build 1163 for next-session e2e validation.

### e2e debugging arc (2026-04-18 → 2026-04-20)

Five real-cluster issues surfaced in sequence once SQL e2e started
running. Each required reading CN `~/be/log/cn.INFO` (via
`sshpass -p sr@test ssh sr@<cn_ip>`) and FE `~/fe/log/fe.log`:

1. FE: `getInfo()` emitted 11 columns instead of the 13/14 declared by
   `SchemaChangeProcDir.TITLE_NAMES`; `SHOW ALTER TABLE COLUMN` returned
   a malformed packet. Fixed in commit `b322137026`.
2. FE: `dispatchAllTasks()` passed `null` to
   `AlterReplicaTask.alterLakeTablet` whose lake ctor does
   `Preconditions.checkNotNull(baseTabletReadSchema, …)`; the NPE
   aborted the loop, `batchTask` had 0 tasks, runRunningJob saw 0/0 as
   trivially complete, and the job fast-forwarded to FINISHED_REWRITING
   then looped on a 404 because no AlterReplicaTask ever left FE.
   Compute the schema with `SchemaInfo.fromMaterializedIndex(...)` per
   LakeTableSchemaChangeJob's pattern. Commit `f3e7c67baa`.
3. BE: `build_idg_for_segment` read `fs->new_random_access_file(file_info)`
   without bundle_file_offset / encryption — bundled rowsets read from
   byte 0 and surfaced "Bad page: checksum mismatch". Set
   `seg_fileinfo.bundle_file_offset` from
   `rowset_meta.bundle_file_offsets(seg_idx)` and switch to
   `new_random_access_file_with_bundling(opts, file_info)` with
   `segment->encryption_info()` propagated. Commit `2275e3278d`.
4. FE: `dispatchAllTasks()` called `AgentTaskExecutor.submit(batchTask)`
   without the preceding `AgentTaskQueue.addBatchTask(batchTask)`;
   when CNs reported back, `LeaderImpl.finishTask()` couldn't find the
   task in the queue ("cannot find task. type: ALTER, backendId: X,
   signature: Y") and the status update was silently dropped.
   `batchTask.isFinished()` never flipped to true so the job stayed in
   RUNNING indefinitely even after BE's
   `ADD INDEX fast path commit: tablet=... segment_entries=1` succeeded.
   Commit `ee4157ff2e`.
5. TEST: `show index from t1` output carries a qualified table name
   whose DB prefix embeds the `${uuid0}` hash, so -r / -v runs never
   matched. Drop the statement; selects are enough to demonstrate
   index visibility and correctness.

All four SQL e2e cases now pass both record and validate modes against
TSP cluster `hujie-lake-idx-e2e7` running build 1180 (commit
`ee4157ff2e`).

    test_lake_add_bitmap_index      record 22.9s  validate 88.9s  OK
    test_lake_drop_index_lifecycle  record 59.2s  validate 179.2s OK
    test_lake_add_index_fallback    record 0.7s   validate 0.9s   OK
    test_lake_add_index_pk_table    record 27.9s  validate 87.8s  OK

One pre-existing cluster quirk the e2e files accommodate: lake INSERT
publish sometimes takes 10+ minutes under low activity, so any test
that mixes INSERT and a follow-up ALTER watershed waits forever. The
committed T files stick to a single ALTER per case to stay within the
600s per-case timeout.

## Decision Log

- 2026-04-16: One `.idx` file per ADD INDEX alter per segment (mirrors DCG
  one-`.cols`-per-alter convention). Multiple alters create multiple IDG
  entries; readers pick highest visible version.
- 2026-04-16: DROP INDEX uses tombstones inside `IndexDeltaGroupEntryPB.dropped_keys`
  rather than physical deletion. Once an entry's keys are fully tombstoned
  it moves to `orphan_files` for vacuum.
- 2026-04-16: Dedicated `_thread_pool_lake_schema_change` rather than reusing
  `_thread_pool_alter_tablet`. Capacity auto-derived as
  `alter_tablet_worker_count * lake_schema_change_per_tablet_parallelism`
  to keep outer alter and inner segment work physically isolated and
  deadlock-free. Single user-facing config.
- 2026-04-16: Per-tablet parallelism config name carries `per_tablet`
  explicitly so the contrast with `alter_tablet_worker_count` is obvious;
  comment makes clear it currently only affects the ADD INDEX path.
- 2026-04-16: Segment-level parallelism scoped to ADD INDEX only.
  DirectSchemaChange / SortedSchemaChange remain single-threaded.
- 2026-04-16: PK tables permitted on the fast path for non-PK columns
  (PK columns are routed through persistent index, so column-level bloom /
  bitmap there would be redundant).

## Remaining Work

### BE
- GIN: InvertedWriter outputs to a per-column directory; IDG entry's
  `index_file` should point at the directory name; reader-side bridge
  needed. Shape differs from bitmap/bloom (file layout is a directory, not
  a single .idx file), so this is a larger follow-up. Intentionally out
  of scope per current direction.

### FE Job classes (LANDED in commit 10)
- `LakeTableIndexFastPathJobBase` (~350 lines) covers the full AlterJobV2
  lifecycle (runPendingJob → runWaitingTxnJob → runRunningJob →
  runFinishedRewritingJob + cancelImpl / getInfo / replay / publishVersion).
- `LakeTableAddIndexJob` (~130 lines) and `LakeTableDropIndexJob` (~150
  lines) plug in `populateAlterRequest` (fast-path flag) and
  `applyCatalogMutation` (add/remove `Index` on the table, flip
  `is_bf_column` on NGRAMBF target columns).
- SchemaChangeHandler dispatch: `tryBuildLakeAddIndexJob` /
  `tryBuildLakeDropIndexJob` helpers run under the classifier gate; any
  unexpected build failure falls through to the regular schema-change
  path so we fail safe.

### Owner-wrapper leak (FIXED in commit 7)

### FE
- LakeTableAddIndexJob: clones the AlterJobV2 lifecycle from
  LakeTableSchemaChangeJob (1248 lines) but emits AlterReplicaTask with
  `setOnlyAddIndex(indexes)` and skips shadow-index creation since no
  data rewrite happens. Approx 400 lines.
- LakeTableDropIndexJob: same shape but for DROP INDEX, calls
  `setOnlyDropIndex(drops)`. Approx 300 lines.
- SchemaChangeHandler dispatch: at the top of the alter-handling switch,
  consult `SchemaChangeIndexFastPathClassifier.shouldUseAddIndexFastPath`
  / `shouldUseDropIndexFastPath` and route to the new Job classes when
  true. Existing classifier file already in tree.

### Tests (all green as of 2026-04-20)
- **BE UTs** (8 cases, `starrocks_test` target, ASAN docker):
  - `IndexFileWriterReaderTest`: RoundTripMultipleEntries, BadMagicIsCorruption,
    TooSmallIsCorruption, FinalizeTwiceFails.
  - `IndexDeltaGroupLoaderTest`: EmptyMetadataReturnsEmpty, VersionVisibilityFilter,
    TombstonedKeysStripped, FullyTombstonedEntryOmitted.
  - Run via `./run-be-ut.sh --build-target starrocks_test --enable-shared-data
    --gtest_filter "IndexFileWriterReaderTest*:IndexDeltaGroupLoaderTest*"`
    inside the dev-env-ubuntu docker with `--ulimit nofile=131072`.
- **FE UTs** (17 cases, `fe-core` module):
  - `SchemaChangeIndexFastPathClassifierTest` (12 cases — full decision matrix
    for shouldUseAddIndexFastPath / shouldUseDropIndexFastPath / isSupportedIndexType).
  - `LakeTableIndexFastPathJobTest` (5 cases — copyForPersist,
    LakeTableAddIndexJob ctor, LakeTableDropIndexJob ctor, replay state, etc.).
  - Run via `mvn test -pl fe-core -am -Dtest="SchemaChangeIndexFastPathClassifierTest,
    LakeTableIndexFastPathJobTest" -Dsurefire.failIfNoSpecifiedTests=false`
    inside the dev-env-ubuntu docker.
- **SQL e2e** (5 cases, `test/sql/test_schema_change/`):
  - `test_lake_add_bitmap_index`: BITMAP ADD + index-backed queries.
  - `test_lake_drop_index_lifecycle`: ADD then DROP (tombstone-only), verify
    queries still correct.
  - `test_lake_add_index_fallback`: GIN ADD captures
    `enable_experimental_gin=false` FE error (flag off by default).
  - `test_lake_add_index_pk_table`: PK table + non-PK column BITMAP index.
  - `test_lake_add_index_interleaved`: two sequential ADD INDEX alters stack
    IDG entries on the same base rowset, then DROP idx_v1 validates tombstone
    semantics + remaining idx_v2 visibility. Exercises the Slice-based feed
    path for string columns (BinaryColumn / LargeBinaryColumn).
  - Both record (`-r`) and validate (`-v`) pass against a real shared-data
    cluster. Run from `$SSH_HOST:/home/disk4/hujie/claude/add-index/starrocks/test`
    with `sr.conf` pointing at a TSP-built cluster; see
    `handbook/domains/sql-integration.md` (if present) for invocation details.

### BE metrics
- `engine_requests_total{type="lake_add_index", status="total"/"failed"}`
  counters track fast-path ADD INDEX invocations (including soft fallbacks
  to regular schema change).
- `engine_requests_total{type="lake_drop_index", status="total"}` counts
  DROP INDEX fast-path invocations.
- `lake_idg_files_written_total` counter increments once per `.idx` file
  successfully written by `AddIndexSchemaChange::build_idg_for_segment`.
- All four are registered in `global_metrics_registry.cpp` and scraped via
  `/metrics` on every CN. Observed values after the 5 e2e runs on
  hujie-lake-idx-e2e8: `lake_add_index{total}=28, failed=0`,
  `lake_drop_index{total}=12`, `lake_idg_files_written_total=28` per CN.

### Out of scope for this PR (follow-ups)
- GIN real-fallback e2e: requires `enable_experimental_gin=true` cluster config
  and a GIN reader-side bridge (per plan, reader bridge itself is deferred).
  Current `test_lake_add_index_fallback` covers only the "GIN flag off" path.
- INSERT-between-alters e2e: the interleaved e2e tests multi-ADD IDG stacking
  on a single base rowset. INSERT interleaved with ALTER remains flaky on
  fresh TSP clusters (lake publish daemon occasionally takes 10+ minutes to
  transition a COMMITTED insert to VISIBLE, which stalls the next ALTER's
  watershed wait). Not a fast-path code issue.
- IDG-specific latency histograms (`lake_add_index_duration`): counters only
  for now; add histograms when grafana dashboards land.

### Docs (landed)
- `docs/en/administration/management/BE_parameters/stats_storage.md:135` and
  `docs/zh/.../stats_storage.md:126`: `lake_schema_change_per_tablet_parallelism`
  documented with its `alter_tablet_worker_count * …` derived-capacity note.
- Thread-pool health metrics (`lake_schema_change_*`) are exposed but deliberately
  not listed in `metrics.md`; this follows the existing precedent for
  `alter_tablet_*` / `update_schema_*` etc., which are pool-internals surfaced
  only via `/metrics` scrape.

## Risks (current state)

- Lake publish-version daemon on freshly-provisioned TSP clusters occasionally
  takes 10+ minutes to reach `FinishTime` on plain inserts. Unrelated to the
  fast-path code (fast-path jobs themselves publish in ~10s in e2e), but does
  bound the e2e scenarios that interleave INSERT with a follow-up ALTER.
