# Lake ADD/DROP INDEX Fast Path

- Status: active
- Owner: Schema Change
- Last Updated: 2026-04-20

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

### First e2e failure: BE publish 404

`ALTER TABLE t1 ADD INDEX idx_v1(v1) USING BITMAP` reaches
`FINISHED_REWRITING`, then `publishVersion()` loops on:

    Fail to publish version for tablets [10344]: starlet err [StatusCode=404]
    Get object s3://.../db{DB}/{TABLE}/{PARTITION}/log/{tablet_hex}_{txn_hex}.log
    error: The specified key does not exist.

The file BE is supposed to write at `txn_log_location(new_tablet_id,
watershedTxnId)` from `do_process_add_index_only` never shows up, yet
the AlterReplicaTask reports FINISHED (so BE's process_alter_tablet
returned OK). Three hypotheses:

- (a) do_process_add_index_only silently fell back to
  do_process_alter_tablet (line 666 or 688) which writes an
  OpSchemaChange log — but path should still match.
- (b) AddIndexSchemaChange::run returned OK but op_add_index was empty;
  put_txn_log wrote a near-empty log that publish reads but fails to
  apply (though the visible error is 404, not parse error).
- (c) Agent task layer swallowed a real BE error and reported success.

### Next session entry points

1. SSH into a CN via `sr / sr@test` (see memory
   reference_tsp_cluster_ssh.md): `sshpass -p 'sr@test' ssh sr@<cn_ip>
   'tail -500 /home/sr/starrocks/cn/log/be.INFO'` and grep for
   `ADD INDEX fast path` / `do_process_add_index_only` markers to learn
   whether the fast path actually ran and whether `put_txn_log` was
   called.
2. Also check `/home/sr/starrocks/cn/log/be.WARNING` for silent S3 put
   errors.
3. If BE never entered the fast path: the thrift-level `only_add_index`
   flag isn't reaching BE, or the `TTabletType::TABLET_TYPE_LAKE` branch
   is short-circuiting somewhere earlier.
4. Re-run the 4 SQL e2e (`-r` mode) against
   `hujie-lake-idx-e2e3` (FE ip printed by `tsp cluster status`),
   updating `test/conf/sr.conf` on `$SSH_HOST:/home/disk4/hujie/claude/
   add-index/starrocks/test/conf/sr.conf`.

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

### Tests
- BE UTs: index_file_writer/reader round-trip, IDG loader version filter
  + tombstone, AddIndexSchemaChange end-to-end on a synthetic segment,
  compaction-vs-add-index conflict fail-fast.
- FE UTs: classifier decision matrix, Job state-machine happy paths.
- SQL e2e under `test/sql/test_schema_change/T/`:
  `test_lake_add_bitmap_index`, `test_lake_drop_index_lifecycle`,
  `test_lake_add_index_fallback`, `test_lake_add_index_pk_table`.

### Docs
- `docs/en/administration/management/BE_configuration.md` and
  `docs/zh/`: document `lake_schema_change_per_tablet_parallelism`.
- `docs/en/administration/management/monitoring/metrics.md`: add IDG
  metrics once implemented (`lake_add_index_duration`,
  `lake_index_delta_group_files`, etc.).

## Risks

- All code below the proto/thrift layer is unreviewed by a build run in
  the implementation environment; first BE compile will likely surface
  small issues (header includes, namespace collisions, signature drift).
  These are easy fixes.
- FE Job classes are not yet in tree; until LakeTableAddIndexJob lands
  the BE plumbing is reachable only via direct `TAlterTabletReqV2`
  injection (test-only).
