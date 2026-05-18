# CDC on Bookmark — design

**Date**: 2026-05-09
**Author**: luca.li@celerdata.com (via brainstorming session)

**Scope**: Re-anchor the previously planned CDC PR (originally consuming `com.starrocks.mvcc.*` types) onto the new `com.starrocks.lake.bookmark.*` API, after MVCC was refactored into bookmarks. PR is internal-only — the `CHANGES` SQL clause is callable by IVM and dev/test, not announced as a user feature.

**Source commit**: `1aab0ff329479bec46ff10454a64d4fe4ee12cc5` (in worktree `/home/disk3/lipengfei/worktree/pitq_and_cdc_prepapre`).

**Reference docs**:
- Bookmark API: `docs/superpowers/specs/bookmark_spec.md` and `fe/fe-core/src/main/java/com/starrocks/lake/bookmark/`.
- Original CDC plan: `/home/disk3/lipengfei/worktree/pitq_and_cdc_prepapre/docs/superpowers/plans/2026-04-22-cdc-pr.md`.
- Original split design: `/home/disk3/lipengfei/worktree/pitq_and_cdc_prepapre/docs/superpowers/specs/2026-04-22-split-mvcc-pitq-cdc-design.md` §6, §7.

---

## 1. Decisions

| # | Decision | Rationale |
|---|---|---|
| 1 | **Approach: direct type replacement** (not adapter, not flattened tuples). | Smallest conceptual gap from the original plan. Bookmark types sit at the right level of abstraction; rule code reads only `delta.getChanges().keySet()` and partition ids, so it sees no change. |
| 2 | **`CHANGES` SQL stays in PR, internal-only**. | Lets IVM consume CDC via SQL; lets dev/test exercise the path. Not announced as a user feature. |
| 3 | **Grammar verbatim**: `CHANGES FROM { VERSION <n> \| TIMESTAMP <t> } TO …`. | Reuses the existing `periodType` rule. `VERSION <n>` reinterprets as `bookmarkId` today; semantics may extend to a true commit version later without a grammar change. |
| 4 | **`TO` endpoint mandatory**, open-ended HEAD disallowed. | Bookmarks are explicitly created; no implicit "now" exists. Forcing both endpoints removes side-effect-y SELECT semantics and matches IVM's two-bookmark flow. |
| 5 | **Mutual exclusion with `FOR VERSION AS OF` enforced at grammar**, not analyzer. | Constraint is structural; ANTLR's `(queryPeriod \| changePeriod)?` rejects at parse time. Trade-off: parser error message is generic; acceptable for an internal feature. |
| 6 | **Caller holds bookmark references**; analyzer/planner does not acquire. | Matches the bookmark spec's lifecycle (`Mv(mvId)` for IVM, `Custom(name)` for tests). Avoids journaling SELECT-time `EditLog` writes. |
| 7 | **Row-id digest dropped** from PR (FE metadata column, operator field, helper signature; BE side audited at writing-plans). | Out of scope for CDC-on-bookmark; non-PK tables match by all columns. Reduces surface area. |
| 8 | **Design docs do not ship** in PR. | Internal-only feature; no announcement; docs stay on local branch. |

---

## 2. What changes vs the original CDC plan

The original CDC PR consumed four MVCC types — `TableVersionManager`, `TableVersionSnapshot`, `TableVersionDelta`, `PhysicalPartitionVersionEntry`. After the bookmark refactor, all four are gone from the codebase. This spec describes how to thread the equivalent calls through the bookmark API.

### 2.1 Type-mapping at three seams

**Seam 1 — `RelationTransformer.isChangesQuery` branch:**

| Original (MVCC) | Replacement (bookmark) |
|---|---|
| `tvm.getSnapshot(dbId, tid, snapshotId)` | `BookmarkManager.findBookmarkById(dbId, tid, bookmarkId).orElseThrow(...)` |
| `tvm.findByTimestamp(dbId, tid, ms)` | `BookmarkManager.findByTimestamp(dbId, tid, ms).orElseThrow(...)` |
| `tvm.getLatestSnapshot(...)` | deleted; open-ended HEAD now rejected (decision 4) |
| `tvm.computeDelta(base, head, table)` | `BookmarkChange.computeChanges(base, head)` |
| `delta.hasChainBroken()` | `!delta.isTrackable()` |
| `buildChainBrokenMessage(delta)` | `buildNonTrackableMessage(delta)` — walks `getChanges()`, names partitions by `ChangeType` (`INDEX_REPLACED` / `TABLET_RESHARD` / `DROPPED`) |
| `baseSnapshot.getSnapshotId() > headSnapshot.getSnapshotId()` | `base.getBookmarkId() > head.getBookmarkId()` |

**Seam 2 — `LogicalChangesScanOperator` field types:**

```
TableVersionDelta delta              → BookmarkChange delta
TableVersionSnapshot baseSnapshot    → Bookmark base
TableVersionSnapshot headSnapshot    → Bookmark head
List<ColumnRefOperator> logicalRowIdColumnRefs   → REMOVED (decision 7)
```

`delta.getChanges().keySet()` keeps its meaning (logical-partition ids); `setSelectedPartitionId` and `ChangesPartitionPruneRule` see no change.

**Seam 3 — `CDCPlanHelper.buildCDCPlanTree` signature:**

```
(OlapTable, TableVersionSnapshot base, TableVersionSnapshot head,
 List<Column> requiredColumns, boolean statsOnly)
                ↓
(OlapTable, Bookmark base, Bookmark head,
 List<Column> requiredColumns, boolean statsOnly)
```

Body: `delta = BookmarkChange.computeChanges(base, head)`; downstream is unchanged except for the row-id removal.

### 2.2 Unchanged from the original plan

Thrift `TChangesScanNode` (pending row-id audit), BE `commit_version` / `parent_version_chain` proto fields, BE `changes_connector` (pending row-id audit), `PhysicalChangesScanOperator`, `ChangesScanNode`, the three optimizer rules (`ChangesScanImplementationRule`, `ChangesPartitionPruneRule`, `ChangesDistributionPruneRule`), `OptChangesPartitionPruner`, `PushDownPredicateScanRule` CDC clause, the CDC-gate hunks in `RewriteSimpleAggToMetaScanRule` / `AddDecodeNodeForDictStringRule` / `MvRewritePreprocessor` / `BaseMaterializedViewRewriteRule`, the `ChangePeriod` AST shape (with a Javadoc update on `PeriodType.VERSION`), and the `cdc_net_changes` session variable.

---

## 3. Grammar

Two files, four hunks.

### `fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocks.g4`

New rule `changePeriod` (placed after `queryPeriod`; `TO` mandatory per decision 4):

```antlr
changePeriod
    : CHANGES (STATS)? FROM periodType start=expression TO periodType end=expression
    ;
```

Modified `relationPrimary` — `queryPeriod?` becomes `(queryPeriod | changePeriod)?` per decision 5:

```antlr
relationPrimary
    : qualifiedName (queryPeriod | changePeriod)? partitionNames? tabletList? replicaList? sampleClause? (
        AS? alias=identifier)? bracketHint? (BEFORE ts=string)?                          #tableAtom
    | '(' VALUES rowConstructor (',' rowConstructor)* ')'
        (AS? alias=identifier columnAliases?)?                                          #inlineTable
    ;
```

`CHANGES` added to the `nonReserved` keyword list (alphabetic position, between `CHAIN` and `CHARSET`).

### `fe-grammar/src/main/antlr/com/starrocks/grammar/StarRocksLex.g4`

`CHANGES: 'CHANGES';` lex token, between `CHAIN` and `CHAR`.

### Behavior

Grammar-accepted forms:

```sql
SELECT * FROM t CHANGES FROM VERSION 100 TO VERSION 200;
SELECT * FROM t CHANGES FROM TIMESTAMP '2026-05-01' TO TIMESTAMP '2026-05-02';
SELECT * FROM t CHANGES STATS FROM VERSION 100 TO VERSION 200;
SELECT * FROM t FOR VERSION AS OF 100;             -- PITQ; analyzer fires only when PITQ lands
```

Grammar-rejected (parse error):

```sql
SELECT * FROM t FOR VERSION AS OF 100 CHANGES FROM VERSION 3 TO VERSION 5;  -- mutex
SELECT * FROM t CHANGES FROM VERSION 100;                                    -- missing TO
```

Analyzer-rejected (syntactically valid, semantically forbidden):

```sql
SELECT * FROM t CHANGES FROM VERSION 200 TO VERSION 100;     -- baseId > headId
SELECT * FROM t CHANGES FROM VERSION 99999 TO VERSION 100;   -- bookmark not found
SELECT * FROM external_t CHANGES FROM ...;                   -- non-cloud-native
SELECT * FROM pk_table CHANGES FROM ...;                     -- stage-1 PK guard
```

---

## 4. Per-commit decomposition

Seven commits (the original plan's C8 docs commit is dropped per decision 8).

### C1 — Proto + thrift

Files: `gensrc/proto/lake_types.proto`, `gensrc/thrift/PlanNodes.thrift`.
Content: `commit_version` on `RowsetMetadata`, `parent_version_chain` on `TabletMetadata`, `TChangesScanNode` in `PlanNodes.thrift`. Same as original plan, modulo a row-id audit (drop digest fields if present in `TChangesScanNode`; do not reuse ordinals).

### C2 — BE storage layer

Files: `be/src/common/config.h` (+`tablet_metadata_parent_chain_depth`), `be/src/common/config_lake_fwd.h`, `be/src/storage/lake/transactions.cpp` (`commit_version` stamping, `parent_version_chain` build), `be/src/storage/lake/txn_log_applier.cpp`. Same as original, modulo row-id audit on rowset-metadata fields.

### C3 — BE connector + BE tests

Files: `be/src/connector/changes_connector.{h,cpp}`, registration in `be/src/connector/connector.{h,cpp}`, `be/src/exec/exec_factory.cpp`, `be/src/exec/exec_node.cpp`, `be/src/connector/CMakeLists.txt`, `be/test/connector/changes_connector_test.cpp`, `be/test/connector/changes_connector_split_test.cpp`, `be/test/CMakeLists.txt`. Same as original, modulo row-id audit.
**Plus**: improved error path when `parent_version_chain` is broken — emit `CHANGES_BASE_RECLAIMED: bookmark <id> base version was vacuumed; caller did not hold a reference for query duration` instead of a generic IO error (the BE-side leg of the caller-drops-reference mitigation; see §9).

### C4 — FE AST + grammar

New file: `fe/fe-core/src/main/java/com/starrocks/sql/ast/ChangePeriod.java`.
Modified: `fe/fe-core/src/main/java/com/starrocks/sql/ast/TableRelation.java` (+`changePeriod` field, getters, `isChangesQuery`, `isChangesStatsQuery`), `AstBuilder.java` CDC slice (`visitChangePeriod` + `import ChangePeriod`), `AstVisitor.java` CDC slice (`visit ChangePeriod`), `StarRocks.g4` (three hunks per §3), `StarRocksLex.g4` (`CHANGES` token).

`ChangePeriod.PeriodType.VERSION` Javadoc rewrites to:

```java
/**
 * Identifies the period endpoint by id. The id is interpreted as a
 * bookmark id (com.starrocks.lake.bookmark) today; semantics may extend
 * to a global commit version later without changing this enum.
 */
VERSION,
```

### C5 — FE analyzer

`fe/fe-core/src/main/java/com/starrocks/sql/analyzer/QueryAnalyzer.java`:
- CDC metadata column name constants: `CDC_CHANGE_TYPE_COLUMN_NAME`, `CDC_ROW_VERSION_COLUMN_NAME`. **No** `CDC_LOGICAL_ROW_ID_DIGEST_COLUMN_NAME` (decision 7).
- `isChangesQuery` / `isChangesStatsQuery` branches.
- Cloud-native guard.
- `getCdcMetadataColumns()` returns 2 columns.
- `getCdcStatsColumns()` returns the stats schema; any row-id column among the 22 is dropped during cherry-pick (decision 7).
- **No** cross-guard with `FOR VERSION AS OF` (decision 5; grammar enforces).
- No `import com.starrocks.mvcc.*`.

`fe/fe-core/src/main/java/com/starrocks/qe/SessionVariable.java`: `CDC_NET_CHANGES`, `cdcNetChanges` field, getter/setter.

### C6 — FE planner + operators + RelationTransformer CDC branch

New files:
- `fe/fe-core/src/main/java/com/starrocks/cdc/CDCPlanHelper.java` — signature `(OlapTable, Bookmark base, Bookmark head, List<Column> requiredColumns, boolean statsOnly)`. Body computes `BookmarkChange.computeChanges(base, head)`; no `logicalRowIdColumnRefs` build.
- `fe/fe-core/src/main/java/com/starrocks/cdc/CDCPlanResult.java`.
- `fe/fe-core/src/main/java/com/starrocks/planner/ChangesScanNode.java`.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/logical/LogicalChangesScanOperator.java` — fields `Bookmark base, Bookmark head, BookmarkChange delta, boolean statsOnly`. No `logicalRowIdColumnRefs`. `Builder` mirrors.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/operator/physical/PhysicalChangesScanOperator.java` — same field shape.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/implementation/ChangesScanImplementationRule.java`.

Shared-file modifications (CDC slice only; IVM hunks stay local):
- `OperatorType.java` — `LOGICAL_CHANGES_SCAN`, `PHYSICAL_CHANGES_SCAN`.
- `OperatorVisitor.java` — `visitLogicalChangesScan`, `visitPhysicalChangesScan`.
- `OptExpressionVisitor.java` — `visit Changes`.
- `OptimizerContext.java`, `OptimizerFactory.java`, `QueryOptimizer.java`, `RequiredPropertyDeriver.java` — CDC ctx fields and hunks.
- `RuleSet.java` — `ChangesScanImplementationRule` registration.
- `RuleType.java` — `CHANGES_*` entries.
- `PlanFragmentBuilder.java` — `ChangesScanNode` dispatch; per-partition `(baseVersion, headVersion)` extracted from `delta.getChanges()`: `DataChanged` → `(basePartition.visibleVersion, headPartition.visibleVersion)`; `PartitionAdded` → `(0, headPartition.visibleVersion)` (the partition did not exist at base, so all rowsets ≤ head are new).
- `RelationTransformer.java` — `isChangesQuery` branch rewritten with the bookmark API.

`isChangesQuery` branch sequence:

1. Resolve base via `findBookmarkById(dbId, tid, n)` for `VERSION <n>` or `findByTimestamp(dbId, tid, ms)` for `TIMESTAMP <t>`. Empty → SemanticException naming the FROM endpoint and table.
2. Resolve head likewise (TO is mandatory).
3. Validate `base.getBookmarkId() <= head.getBookmarkId()`.
4. **Fence-check** (the analyzer-time leg of the caller-drops-reference mitigation; see §9): for each `(lpId, ppId)` in `delta.getChanges()` after step 5, verify `BookmarkManager.getPhysicalPartitionFenceVersion(...)` returns ≤ `base.partitionsMeta.get(lpId).get(ppId).visibleVersion`. Empty or larger → SemanticException.
5. `BookmarkChange delta = BookmarkChange.computeChanges(base, head)`.
6. `if (!delta.isTrackable())` → SemanticException listing partitions by `ChangeType`.
7. PK-table guard.
8. Build `LogicalChangesScanOperator(table, …, delta, base, head, statsOnly, limit)`.
9. `setSelectedPartitionId(new ArrayList<>(delta.getChanges().keySet()))`.

Note: step ordering above places `computeChanges` (5) before fence-check (4) since fence-check needs the partition list. Implementation order: (1) → (2) → (3) → (5) → (4) → (6) → (7) → (8) → (9).

### C7 — Optimizer rules + FE UTs + SQL regression

New files:
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/ChangesPartitionPruneRule.java`.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/transformation/ChangesDistributionPruneRule.java`.
- `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rewrite/OptChangesPartitionPruner.java`.
- `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/ChangesPartitionPruneRuleTest.java` — cherry-pick; one fixture-level type swap.
- `fe/fe-core/src/test/java/com/starrocks/sql/optimizer/rule/transformation/PushDownPredicateChangesScanTest.java` — cherry-pick; one fixture-level type swap.
- **NEW** `fe/fe-core/src/test/java/com/starrocks/cdc/ChangesBookmarkResolutionTest.java` — covers `findBookmarkById` happy path, `findByTimestamp` happy path, bookmark-not-found, `baseId > headId`, `!isTrackable()` per change type, fence-check failure.
- **NEW** `fe/fe-core/src/test/java/com/starrocks/cdc/ChangesEndToEndTest.java` (Risk 7) — FE Java integration. Builds bookmarks via `BookmarkManager.create()` in test setup, runs CHANGES queries through the FE mock-BE harness, verifies plan shape and column projection. Covers the FE half of `data_read`, `stats`, `partition_prune`, `tablet_parallel`, and parts of `syntax`.
- SQL regression: `test/sql/test_cdc/{T,R}/test_changes_syntax`, `test/sql/test_cdc/{T,R}/test_changes_pk_guard`. The other 4 regression dirs from the original plan are dropped (need a bookmark-creation SQL surface that doesn't exist yet).

Shared-file modifications (CDC slice only):
- `PushDownPredicateScanRule.java` — Changes-specific pushdown.
- `RuleSet.java` — `ChangesPartitionPruneRule`, `ChangesDistributionPruneRule` registrations.
- `RewriteSimpleAggToMetaScanRule.java`, `AddDecodeNodeForDictStringRule.java`, `MvRewritePreprocessor.java`, `BaseMaterializedViewRewriteRule.java` — CDC gating hunks.

---

## 5. End-to-end data flow

For `SELECT * FROM t CHANGES FROM VERSION 100 TO VERSION 200`:

1. **Parse** — `(queryPeriod | changePeriod)?` matches `changePeriod`; `TableRelation.changePeriod = ChangePeriod(start=100/VERSION, end=200/VERSION, isStats=false)`.
2. **Analyze** — cloud-native guard → PK-table guard → inject 2 metadata columns (`__change_type__`, `__row_version__`).
3. **Transform** — resolve base/head via bookmark API → order check → `BookmarkChange.computeChanges` → `isTrackable` check → fence-check → build `LogicalChangesScanOperator(table, …, delta, base, head, statsOnly, limit)` → `setSelectedPartitionId(delta.getChanges().keySet())`.
4. **Optimize** — `ChangesPartitionPruneRule` → `ChangesDistributionPruneRule` → `PushDownPredicateScanRule` (CDC arm) → `ChangesScanImplementationRule`. CDC-gate skips MV-rewrite, dict-decode, agg-meta-scan rewrite.
5. **Build fragment** — `PlanFragmentBuilder` → `ChangesScanNode` → thrift `TChangesScanNode` with per-partition `(baseVersion, headVersion)` derived from `delta`'s `DataChanged` and `PartitionAdded` entries.
6. **BE execute** — `changes_connector` walks `parent_version_chain` per tablet; emits `(change_type, row_version, row)` triples. On `parent_version_chain` break, returns `CHANGES_BASE_RECLAIMED`.

---

## 6. Error taxonomy

All FE failures surface as `SemanticException` at analyze / transform time; the BE failure is at scan time.

| Trigger | Stage | Message |
|---|---|---|
| Non-cloud-native table | analyzer | `CHANGES is only supported on cloud-native tables` |
| Primary-key table | analyzer | `CHANGES on primary-key table is not supported yet` |
| `findBookmarkById(...).isEmpty()` | transformer | `CHANGES bookmark <id> not found for table <db.t>` (FROM / TO endpoint named) |
| `findByTimestamp(...).isEmpty()` | transformer | `no bookmark exists at or before <timestamp> for table <db.t>` |
| `base.getBookmarkId() > head.getBookmarkId()` | transformer | `CHANGES FROM endpoint must not be later than TO endpoint` |
| Fence-check fails on `(lpId, ppId)` | transformer | `CHANGES base bookmark <id> is not protected by any reference; caller must hold a reference for query duration` |
| `!delta.isTrackable()` | transformer | `CHANGES delta has non-trackable changes: physicalPartition <id>: INDEX_REPLACED, …` |
| `parent_version_chain` broken at scan | BE connector | `CHANGES_BASE_RECLAIMED: bookmark <id> base version was vacuumed; caller did not hold a reference for query duration` |

---

## 7. Caller contract

The caller — IVM, dev/test harness — owns the lifecycle:

- Caller holds a `Reference` on both `base` and `head` bookmarks for the entire query lifetime.
- Reference is the only mechanism preventing vacuum from reclaiming the data the connector reads. There is no retention buffer beyond the version fence.
- IVM uses `ReferenceHolder.Mv(mvId)`. Dev/test uses `ReferenceHolder.Custom(name)`.
- This PR adds zero query-lifecycle reference machinery beyond the analyzer-time fence-check (§9). Acquire / release stays caller's responsibility.

The contract is enforced as best-effort at analyze time (fence-check) and as an actionable error at BE scan time (`CHANGES_BASE_RECLAIMED`). Documented in `CDCPlanHelper` Javadoc and the analyzer fence-check error message.

---

## 8. Test plan

| Layer | Tests |
|---|---|
| BE UT (C3) | `changes_connector_test`, `changes_connector_split_test` (cherry-pick; row-id audit may drop digest cases). |
| FE rule UT (C7) | `ChangesPartitionPruneRuleTest`, `PushDownPredicateChangesScanTest` (cherry-pick; field-type swap at fixtures). |
| FE bookmark resolution UT (C7, new) | `ChangesBookmarkResolutionTest` — analyzer + transformer paths via the bookmark API. |
| FE end-to-end UT (C7, new) | `ChangesEndToEndTest` — bookmarks via `BookmarkManager.create()` in setup, plans through the FE mock-BE harness. |
| SQL regression (C7) | `test_changes_syntax`, `test_changes_pk_guard`. |
| Coverage gap | `data_read`, `stats`, `partition_prune`, `tablet_parallel` SQL regression cases deferred until a bookmark-creation SQL surface exists or IVM PR exercises the equivalent paths end-to-end. |

---

## 9. Risks & mitigations

| Risk | Mitigation |
|---|---|
| BE row-id surface not yet audited | Audit during writing-plans phase: read `gensrc/thrift/PlanNodes.thrift` `TChangesScanNode`, `be/src/connector/changes_connector.{h,cpp}`, `be/src/storage/lake/transactions.cpp` rowset metadata. Touchpoint list produced before plan tasks are written. Proto/thrift safe rule: drop digest fields if present, do not reuse ordinals. |
| BookmarkId from a foreign table | `findBookmarkById(dbId, tid, n)` is per-table by construction; foreign IDs return empty → SemanticException with table-qualified message. |
| Caller drops reference mid-query | Two-leg defence. (a) Analyzer-time fence-check via `getPhysicalPartitionFenceVersion`: catch most caller bugs at query analyze. (b) BE actionable error `CHANGES_BASE_RECLAIMED` when `parent_version_chain` breaks at scan: race-time failures still report a diagnosable cause. |
| Cross-PR with IVM | Zero coordination. `CDCPlanHelper.buildCDCPlanTree` is the contract surface. |
| Cross-PR with PITQ | Zero. Grammar mutex passively holds the door; when PITQ lands, no CDC code change. |
| Cherry-pick conflicts | C1–C5 + C7 mostly clean. C6 is the rewrite hotspot — `--reject` + hand-resolve. |
| Coverage gap from dropped SQL regression | `ChangesEndToEndTest` covers the FE half of all 4 dropped cases. End-to-end with real BE happens via the IVM PR. |

---

## 10. Migration mechanics

- Source: `1aab0ff329479bec46ff10454a64d4fe4ee12cc5` in `/home/disk3/lipengfei/worktree/pitq_and_cdc_prepapre`.
- Target: existing `cdc` worktree at `/home/disk3/lipengfei/worktree/cdc`, off bookmark-merged main.
- Per-commit strategy:
  - **C1, C2, C3 (BE)**: `git show 1aab0ff3294 -- <files> | git apply` for clean cherry-picks. C1/C3 add a row-id-audit step. C3 adds the `CHANGES_BASE_RECLAIMED` error path.
  - **C4 (grammar/AST)**: cherry-pick `ChangePeriod.java`, `TableRelation.java`, `StarRocksLex.g4` `CHANGES` token, `AstBuilder` / `AstVisitor` CDC slices. Hand-edit `StarRocks.g4`: `changePeriod` rule with `TO` mandatory, `relationPrimary` mutex, `nonReserved` keyword. Update `ChangePeriod.PeriodType.VERSION` Javadoc.
  - **C5 (analyzer)**: hand-edit `QueryAnalyzer.java` — drop `CDC_LOGICAL_ROW_ID_DIGEST_COLUMN_NAME`, drop the cross-guard hunks, drop MVCC imports, leave 2-column metadata injection. Cherry-pick `SessionVariable.java` CDC hunk.
  - **C6 (planner/operator/transformer)**: rewrite `LogicalChangesScanOperator` field types and `Builder`; rewrite `CDCPlanHelper.buildCDCPlanTree` to take Bookmarks and drop row-id; rewrite `RelationTransformer.isChangesQuery` branch using the bookmark API + fence-check. Cherry-pick `PhysicalChangesScanOperator`, `ChangesScanNode`, `ChangesScanImplementationRule`, plus 10 shared-file CDC slices (none reference MVCC types).
  - **C7 (rules + tests)**: cherry-pick rule files + `ChangesPartitionPruneRuleTest` + `PushDownPredicateChangesScanTest` (one fixture type swap each); cherry-pick `test_changes_syntax` and `test_changes_pk_guard` SQL regression dirs. Write `ChangesBookmarkResolutionTest` and `ChangesEndToEndTest` from scratch.
- Per-commit verify: `./build.sh --be` after C1–C3; `./build.sh --fe` after C4–C7; `./run-fe-ut.sh --test 'com.starrocks.sql.optimizer.rule.transformation.Changes*'` and the new test classes after C7.
- Pre-push: full FE+BE build, leakage grep:
  ```
  git grep -nE 'import com\.starrocks\.(ivm|mvcc)|\bIvm[A-Z]|\bIVM_|TableVersionManager|TableVersionSnapshot|TableVersionDelta|PhysicalPartitionVersionEntry'
  ```
  must return empty. `mvn checkstyle:check`.

---

## 11. Success criteria

- All seven commits compile (FE + BE).
- All four FE UT classes (2 cherry-picked + 2 new) plus the 2 BE UT classes pass.
- 2 SQL regression cases pass.
- Leakage grep returns empty (no MVCC, no IVM imports).
- Caller contract is documented in `CDCPlanHelper` Javadoc and the analyzer fence-check error message.
