# Shared-Data: Concurrent ADD PARTITION During Metadata-Only ALTER

- Status: active
- Owner: meegoo
- Last Updated: 2026-06-11

## Summary

Allow partition creation (manual `ALTER TABLE ... ADD PARTITION`, automatic creation
during load, and the dynamic-partition scheduler) to proceed concurrently with two
families of shared-data ALTER operations that are metadata-only and provably safe:

1. **Path 1 — Fast Schema Evolution V2 add/drop column** (synchronous, FE-catalog-only;
   transient `OlapTableState.UPDATING_META`).
2. **Path 2 — Lake ADD/DROP INDEX fast path** (`LakeTableIndexFastPathJobBase` and its
   subclasses `LakeTableAddIndexJob`, `LakeTableDropIndexJob`, including the
   bloom-filter-columns property variants built on them).

Previously all three partition-creation paths rejected the DDL or **cancelled** the
running alter job whenever the table state was non-`NORMAL` — even for these safe jobs.
The headline regression: a load that auto-created an expression partition would kill a
user's running lake ADD/DROP INDEX job.

Out of scope: `LakeTableSchemaChangeJob` and `SchemaChangeJobV2` (full rewrite with shadow
indexes — genuinely unsafe), the `LakeTableAlterMetaJobBase` family (candidate follow-up),
rollup jobs, and DROP PARTITION concurrency.

## Why Paths 1 and 2 Are Safe

- **Path 1 (FSE V2):** the whole mutation runs inside one table WRITE-lock critical section
  with one edit-log entry; `UPDATING_META` is observable only by lock-free readers, and any
  partition-creation path serializes behind the same WRITE lock (by which time the state is
  `NORMAL` again). `checkIfMetaChange` only compares `schemaHash`, which FSE V2 does not
  change.
- **Path 2 (lake index fast path):** the owned tablet set is snapshotted once at
  `runPendingJob` (`partitionToTablets`) and every later phase iterates only that snapshot;
  no table-level shadow meta is registered before FINISHED; the catalog flip at FINISHED is
  an idempotent, table-level-only change; cancel is FE-only cleanup. A partition created
  after the snapshot is simply outside the job's scope. `checkIfMetaChange` does not compare
  `TableIndexes`/`bfColumns`, so a job finishing inside an ADD PARTITION copy/commit window
  neither aborts the ADD PARTITION nor needs to.

## Design (as implemented)

1. **Capability method** — `AlterJobV2.allowConcurrentPartitionCreation()`, default `false`,
   overridden to `true` in `LakeTableIndexFastPathJobBase` only. Behavioral, not serialized
   state — replayed/restored jobs keep the override via their concrete class.
2. **Shared guard helper** — `AlterJobMgr.unfinishedAlterJobsAllowConcurrentPartitionCreation(tableId)`
   gathers the unfinished jobs from the schema-change and rollup handlers and returns
   `!jobs.isEmpty() && jobs.stream().allMatch(AlterJobV2::allowConcurrentPartitionCreation)`.
   An empty set with a non-`NORMAL` state is treated as NOT tolerable (anomaly conservatism).
3. **Config** — `enable_concurrent_add_partition_during_alter`, `@ConfField(mutable = true)`,
   default `true`; the kill switch that restores legacy exclusive behavior at every guard.
4. **Guards relaxed:**
   - **G1** `AlterJobExecutor.visitAlterTableStatement` — for an ADD-PARTITION-only statement,
     tolerate `UPDATING_META` (Path 1) and `SCHEMA_CHANGE` + all-safe-jobs (Path 2) instead of
     the unconditional throw.
   - **G2** `FrontendServiceImpl.cancelConflictingAlterJobs` — skip cancelling the schema-change
     job when the unfinished jobs tolerate concurrent partition creation.
   - **G3** `DynamicPartitionScheduler` — do not set `skipAddPartition` for the tolerable states.
   - **Authoritative check** `LocalMetastore.addPartitions` — `checkTableForAddPartitions` skips
     the `CatalogUtils.checkTableState` guard for the tolerable states at both the pre-lock entry
     and the under-lock re-check. Consistency is still guaranteed by `checkIfMetaChange` under the
     table WRITE lock.

> **Correction vs. the original design note:** the note assumed `LocalMetastore.addPartitions`
> had no table-state assertion. It does — `checkTableState` runs at both the pre-lock entry and
> the under-lock "check again" — so relaxing only G1/G2/G3 is insufficient. All three partition
> paths converge on `addPartitions`, which is why the `checkTableForAddPartitions` relaxation is
> the linchpin; G1/G2/G3 remain individually necessary upstream. An FE e2e test caught this.

Only ADD PARTITION is relaxed. DROP PARTITION, full-rewrite schema change, rollup, `OPTIMIZE`,
and all non-partition DDL keep the legacy behavior; `enable_concurrent_add_partition_during_alter=false`
restores legacy behavior everywhere.

## Test Plan

- `ConcurrentAddPartitionDuringAlterTest` — pure-unit coverage of the `AlterJobMgr` helper across
  empty / all-safe / mixed / unsafe-rollup / both-handlers combinations.
- `LakeTableIndexFastPathJobTest#testAllowConcurrentPartitionCreation` — capability values
  (lake add/drop index → `true`; full-rewrite jobs → `false`).
- `ConcurrentAddPartitionDuringAlterE2ETest` — shared-data cluster e2e of the G1 `UPDATING_META`
  branch (ADD PARTITION succeeds; ADD COLUMN still rejected; config=false still rejected). The
  `SCHEMA_CHANGE` + real-lake-index-job branch is validated on a deployed shared-data cluster
  (an FE UT would race the alter-scheduler daemon).
- Docs: config entry in `docs/{en,zh}/.../FE_parameters/stats_storage.md`; FAQ narrowed in
  `docs/{en,zh,ja}/faq/Others.md`.

## Acceptance Criteria

- With a lake ADD/DROP INDEX fast-path job in PENDING/WAITING_TXN/RUNNING/FINISHED_REWRITING:
  manual `ALTER TABLE ... ADD PARTITION` succeeds, automatic partition creation during load
  succeeds without cancelling the job, and the job subsequently finishes and applies its catalog
  mutation.
- A partition created mid-job is fully usable (load + query) after the job FINISHED and after it is
  CANCELLED.
- FSE V2 add/drop column racing `ADD PARTITION` during `UPDATING_META`: neither statement fails.
- All non-safe alter jobs (full schema change, rollup, `OPTIMIZE`) and all non-`ADD PARTITION` DDL
  keep the legacy rejection / cancellation behavior; `enable_concurrent_add_partition_during_alter=false`
  restores legacy behavior everywhere.
- FE unit tests pass and `mvn checkstyle:check` is clean. Shared-data cluster A/B validation shows the
  ADD INDEX job FINISHES (feature on) vs is CANCELLED by a concurrent load (feature off).

## Decision Log

- 2026-06-10: Scope fixed to shared-data + ADD PARTITION only; Path 1 (FSE V2 sync column changes) and
  Path 2 (lake index fast path) in scope; `LakeTableAlterMetaJobBase` deferred as follow-up; DROP
  PARTITION explicitly out.
- 2026-06-10: Chose a per-job capability method (`allowConcurrentPartitionCreation`) over state-enum
  changes — `SCHEMA_CHANGE` cannot distinguish safe from unsafe jobs, and new enum values would leak
  into persisted state and external tooling.
- 2026-06-10: Empty unfinished-job list with non-NORMAL state treated as NOT tolerable (conservative
  anomaly handling). Config default true with a mutable kill switch.
- 2026-06-11: Found via e2e test that `LocalMetastore.addPartitions` enforces `checkTableState` at
  both the pre-lock entry and the under-lock re-check (the original note assumed it did not). Added
  `checkTableForAddPartitions` to relax it for the tolerable states — the linchpin all three guards
  converge on.
