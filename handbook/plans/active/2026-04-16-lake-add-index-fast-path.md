# Lake ADD/DROP INDEX Fast Path

- Status: active
- Owner: Schema Change
- Last Updated: 2026-04-16

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

### FE Job classes
- LakeTableAddIndexJob / LakeTableDropIndexJob: end-to-end FE Job that
  emits AlterReplicaTask with `setOnlyAddIndex(...)` /
  `setOnlyDropIndex(...)`. Needs ~400 lines duplicating AlterJobV2
  lifecycle (pending → waiting-txn → running → finished + persist /
  replay). Skipping shadow-index creation since the fast path mutates
  base tablets in place.
- SchemaChangeHandler dispatch: currently only logs the classifier
  decision and falls through to the regular path (commit 9). Once the
  Job classes exist, replace the log with `return new
  LakeTableAddIndexJob(...)` / `return new LakeTableDropIndexJob(...)`.

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
