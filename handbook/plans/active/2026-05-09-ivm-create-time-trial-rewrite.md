# IVM CREATE-time Trial Rewrite

- Status: design (not yet approved)
- Owner: IVM team / @Youngwb
- Last Updated: 2026-05-09

## Summary

`refresh_mode = INCREMENTAL` materialized views currently fail in two distinct ways:

1. **At CREATE time**: `IVMAnalyzer` rejects via the explicit checks (table-type whitelist, join-op whitelist, `isDistinct()` check, the `IVM_SUPPORTED_AGG_FUNCTIONS` whitelist introduced in #73014). The user gets a clear, actionable error.
2. **At REFRESH time**: the AST passes IVMAnalyzer but the optimizer (`IvmRewriter` and the delta/version rules) or the BE planner (thrift serialization, BE-side state union) can still fail. The user only finds out the materialized view is broken when the first refresh runs — sometimes hours after CREATE.

The whitelist closes today's known holes, but it is hand-maintained and can drift from what the rewriter actually supports as the codebase evolves. We want a complementary mechanism that catches **"passes whitelist but rewriter cannot actually produce a runnable plan"** at CREATE time, by attempting a trial compilation of the IVM refresh plan with mock context and surfacing any failure as a `CREATE MATERIALIZED VIEW` error.

This document is the design proposal; implementation is gated on review.

## Goals

- Surface, at CREATE MV time, every failure mode that today only manifests at refresh-time **plan compilation** (logical-plan rewrite, optimizer rules, fragment generation, thrift serialization).
- Keep the existing whitelist as the **first** gate (fast, deterministic, human-readable). Trial rewrite is the **second** gate (defensive, drift-detecting).
- No behavioral change for materialized views that already created successfully.
- Failure messages must clearly attribute the error to the trial pipeline (so users do not confuse them with refresh-time failures).

## Non-Goals

- **Catching runtime data-correctness bugs** (e.g. `AVG(DECIMAL)` silently returning 0 after incremental refresh). Those manifest only when the BE state-union operator runs on real data — trial compilation does not run BE code. The whitelist is and remains the only line of defense for these.
- **Replacing the whitelist**. The whitelist documents intent ("this combination is supported"); trial rewrite verifies that the implementation lives up to that intent.
- **Replacing or modifying `IvmRewriter`**. Trial rewrite reuses the existing pipeline; it does not introduce a parallel one.

## Background

### What the current bugs catch tells us

From the IVM agg cleanup batch (PRs #73000, #73003, #73004, #73014):

| Bug | Manifests at | Caught by whitelist? | Caught by trial rewrite? |
|---|---|---|---|
| #1 `AVG(DECIMAL)` returns 0 | runtime (BE state union) | ✅ rejected by whitelist | ❌ runtime-only |
| #2 `COUNT(DISTINCT)` accumulates | runtime (silent wrong data) | ✅ rejected (`isDistinct`) | ❌ runtime-only |
| #6 `MIN/MAX(VARCHAR)` `IllegalArgumentException` | refresh-time (compile) | ✅ rejected by whitelist | ✅ would be caught |
| #7 `ARRAY_AGG` `PseudoType not exposed` | refresh-time (thrift serialization) | ✅ rejected by whitelist | ✅ caught only if trial runs through fragment build |
| #9 `HLL_UNION` no matching function | CREATE-time (combinator lookup) | ✅ rejected by whitelist | ✅ already caught at CREATE |
| #10 `GROUP_CONCAT` no matching function | CREATE-time | ✅ rejected by whitelist | ✅ already caught at CREATE |

The **delta** between whitelist and trial rewrite is the "drift" scenario:

- Someone adds a new logical operator without a corresponding `IvmDelta*Rule` → whitelist still passes (operator is unrelated to agg list) → IvmRewriter convergence fails at refresh.
- Someone changes a combinator's `intermediateType` in a way that breaks `IvmOpUtils.buildStateUnionScalarOperator` → whitelist still passes (function is on list) → refresh throws.
- Someone makes a combinator polymorphic in a way that adds a new `PseudoType` exposure → whitelist still passes → refresh fails at thrift serialization.

Trial rewrite catches all three of those.

### What the current code expects at refresh time

`MVIVMRefreshProcessor.prepareRefreshPlan` does (in order):

1. Set `enable_ivm_refresh = true` and `tvr_target_mv_id = <real mv id>` on the `ConnectContext` (now scoped via try/finally per #73004).
2. Generate INSERT AST from the rewritten task definition.
3. `Analyzer.analyze(insertStmt)`.
4. `buildInsertPlan(insertStmt)` — assigns `TvrVersionRange` to each `TableRelation`.
5. `StatementPlanner.plan(insertStmt, ctx)` — runs the optimizer (with `IvmRewriter` enabled by gate 1) and produces an `ExecPlan` (logical → physical → fragments).

Trial rewrite needs to reproduce **steps 2-5** at CREATE time, with **mock** values for the parts that don't exist yet (the target MV row schema and the TvrVersionRange).

## Design Options

### Option A: Optimizer-only trial

Run only `Analyzer.analyze` + `StatementPlanner.plan` up through `QueryOptimizer.optimize` (which invokes `IvmRewriter`). Stop before fragment generation.

- ✅ Catches IvmRewriter convergence failures, IvmDeltaAggregateRule errors, IvmOpUtils type-equality precondition failures.
- ✅ Cheapest — no fragment build, no thrift serialization.
- ❌ Misses thrift serialization errors (e.g. ARRAY_AGG `PseudoType`).
- ❌ Misses BE-stage validation (column type mismatch with target MV's stored schema).

### Option B: Full plan compilation

Run the entire `StatementPlanner.plan` through fragment build and thrift serialization.

- ✅ Catches everything that fails at refresh-time compilation.
- ❌ More expensive (estimated +200–800ms on CREATE for a moderate join+agg MV).
- ❌ Requires mocking enough of the target MV that fragment generation can pick output columns correctly.

### Option C: Hybrid

Run Option A unconditionally; run the additional fragment-build step opt-in via a session variable or a config flag. This lets us roll out trial rewrite incrementally, observing the latency cost on real workloads before committing to Option B everywhere.

- ✅ Catches the high-frequency cases (IvmRewriter / IvmOpUtils errors) by default.
- ✅ Configurable defense-in-depth for thrift serialization / fragment build.
- ❌ Two paths to maintain (Option A's cheap path and Option B's full path).

### Recommended: **Option B**

Rationale:

- The cost of CREATE MV is one-time and amortized over the lifetime of the materialized view. A few hundred extra milliseconds is acceptable.
- Splitting cheap vs full path adds maintenance burden for a hypothetical performance concern.
- Fragment-build / thrift-serialization is exactly where #7 (ARRAY_AGG `PseudoType`) lives — losing that coverage materially weakens the value of the trial.

If concrete CREATE latency complaints emerge after rollout, downgrade to Option C.

## Mocking Strategy

Trial rewrite needs three things that don't exist yet at CREATE time.

### 1. Target MV (`MaterializedView` instance)

Why needed: `IvmRewriter.loadTargetMv(context)` reads `tvrTargetMvId` from the session, looks up the MV in `GlobalStateMgr`, and `IvmDeltaAggregateRule.transform` reads `mv.getColumn(__ROW_ID__)` and the `__AGG_STATE_*` columns to project against.

Three approaches:

- **Approach M1 (mock-and-register)**: Build a `MaterializedView` in memory matching what the real CREATE will produce (column list comes from IVMAnalyzer's rewritten AST), register it under a synthetic id in `GlobalStateMgr`, set `tvrTargetMvId` to that id, run the trial, then unregister. Simple call sites; cleanup is lockable but fiddly.

- **Approach M2 (refactor entry point)**: Add `IvmRewriter.rewriteWithExplicitTargetMv(tree, taskContext, scheduler, requiredColumns, MaterializedView mv)` that takes the MV directly instead of looking it up via session var. Trial rewrite uses this entry point with an in-memory MV; production refresh continues to use the existing `rewrite(...)` entry point (which internally calls the new method with `loadTargetMv(...)` of the session var). No catalog mutation.

- **Approach M3 (provider injection)**: Add a `TargetMvProvider` interface; production uses a session-backed implementation, trial uses an in-memory implementation. Cleanest but most invasive.

**Recommended: M2.** Minimal API change, no global-state mutation, trial path is a thin wrapper.

### 2. `TvrVersionRange` per base table

Why needed: `IvmDeltaIcebergScanRule.check` requires `scan.getTvrVersionRange() instanceof TvrTableDelta`. Other delta rules likely do the same.

Use a synthetic `TvrTableDelta.of(TvrVersion.of(0L), TvrVersion.of(1L))` for each base table. The actual snapshot ids do not matter — the trial never executes scans, only compiles plans.

Question: does `getTvrTableDeltaTrait().get().isAppendOnly()` always return `true` for synthetic ranges? Currently `LogicalIcebergScanOperator.getTvrTableDeltaTrait` hard-codes `isAppendOnly=true` (TODO comment in the code), so synthetic ranges are append-only — which is also what we need. If `isAppendOnly` ever becomes data-derived, trial rewrite must construct a synthetic range that the rule accepts.

### 3. Session variables

`enable_ivm_refresh = true`, `tvr_target_mv_id = <synthetic id or null>`. Already correctly scoped in `MVIVMRefreshProcessor.prepareRefreshPlan` (per #73004); trial rewrite must do the same — ideally **reusing** the same try/finally helper to keep the lifecycle consistent.

## Implementation Sketch

### Files

- New: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IvmTrialRewriter.java` (~200 lines).
- Modified: `fe/fe-core/src/main/java/com/starrocks/sql/analyzer/mv/IVMAnalyzer.java` — invoke `IvmTrialRewriter.runTrial(...)` at the end of `rewrite(...)` for `INCREMENTAL` mode.
- Modified: `fe/fe-core/src/main/java/com/starrocks/sql/optimizer/rule/ivm/IvmRewriter.java` — add `rewriteWithExplicitTargetMv(...)` entry point per Approach M2.
- New tests: `fe/fe-core/src/test/java/com/starrocks/sql/analyzer/mv/IvmTrialRewriterTest.java` covering accept paths and known reject patterns.

### `IvmTrialRewriter.runTrial(...)` outline

```java
public final class IvmTrialRewriter {
    /**
     * Compile a trial IVM refresh plan and return any error encountered. The returned
     * exception is re-thrown by IVMAnalyzer.rewrite so the user sees a CREATE-time error
     * rather than a refresh-time crash.
     */
    public static void runTrial(ConnectContext ctx,
                                CreateMaterializedViewStatement stmt,
                                QueryStatement rewrittenQuery) {
        // 1) Build a synthetic target MV from the rewritten query's output schema.
        MaterializedView mockMv = buildMockTargetMv(stmt, rewrittenQuery);

        // 2) Build a fake INSERT statement targeting the mock MV.
        InsertStmt trialInsert = buildTrialInsertStmt(mockMv, rewrittenQuery);

        // 3) Stamp synthetic TvrTableDelta on every base TableRelation.
        AnalyzerUtils.collectAllTableRelation(rewrittenQuery)
            .values()
            .forEach(rel -> rel.setTvrVersionRange(syntheticDelta(rel.getTable())));

        // 4) Scope IVM session vars (matching MVIVMRefreshProcessor.prepareRefreshPlan).
        SessionVarScope scope = SessionVarScope.beginIvm(ctx, mockMv.getMvId());
        try {
            Analyzer.analyze(trialInsert, ctx);
            // Use the explicit-target-MV entry point so we don't depend on real catalog state.
            ExecPlan trialPlan = StatementPlanner.planForIvmTrial(trialInsert, ctx, mockMv);
            // The plan is built end-to-end (incl. fragment generation + thrift serialization),
            // so any failure manifests as an exception caught here.
        } catch (Exception e) {
            throw new SemanticException(
                "IVM cannot incrementally maintain this materialized view: %s. " +
                "(Detected by CREATE-time trial compilation.)", e.getMessage(), e);
        } finally {
            scope.close();
        }
    }
}
```

### IVMAnalyzer integration

```java
public Optional<IVMAnalyzeResult> rewrite(MaterializedView.RefreshMode refreshMode) {
    ...
    boolean isRetractable = rewriteImpl(queryRelation);
    ...
    if (refreshMode.isIncremental()) {
        IvmTrialRewriter.runTrial(connectContext, statement, queryStatement);
    }
    return Optional.of(result);
}
```

For `AUTO` mode: skip trial — the AUTO path already falls back to PCT on any IVM failure, so a noisy CREATE-time failure would be more aggressive than the runtime behavior.

## Open Questions

1. **Where exactly does the trial run inside IVMAnalyzer.rewrite?** Right before the final `Optional.of(result)`, or earlier so partial AST mutations are still recoverable on trial failure? IVMAnalyzer already mutates `queryStatement` during `rewriteImpl`; trial running on the mutated AST is correct (that's exactly what refresh sees), but IVMAnalyzer's existing `rewrite` method already documents an "AST-mutation leak" in its catch block — trial rewrite should be careful not to leave the AST half-mutated if it throws.

2. **Should the trial run for `AUTO` mode?** Probably not — `AUTO` is designed to fall back to PCT; surfacing IVM-specific errors at CREATE for AUTO would be a behavior change. Confirm with the team.

3. **Is `StatementPlanner.planForIvmTrial(...)` a real entry point we want to expose?** Or should the trial reuse `StatementPlanner.plan(...)` directly with a `ConnectContext` already wired up correctly via Approach M2?

4. **Latency budget.** Run a benchmark on a representative set of MV definitions (single-table agg, 2-table join, 3-table join, union all) before committing to Option B. If any case exceeds ~1s, fall back to Option C (cheap-path-by-default).

5. **Mock TvrVersionRange for non-Iceberg base tables**. Currently IVM only supports Iceberg, but the mock factory `syntheticDelta(table)` should be table-type aware so adding Hudi or OLAP support later doesn't break trial rewrite.

6. **Failure messages**. The wrapped `SemanticException` should clearly say "trial compilation failed" so users don't conflate it with a real refresh-time error from a different MV. Consider including the trial plan digest as debug info.

## Phased Rollout

1. **Phase 1** (this doc → first impl PR): Implement `IvmTrialRewriter.runTrial` per Approach M2. Wire into `IVMAnalyzer.rewrite` for INCREMENTAL only. Add unit tests for the known reject patterns (#6, #7, #9, #10) — note that some of these are now whitelist-rejected before trial would run; the trial test must construct the bypass-whitelist scenario synthetically (or extend the whitelist temporarily for those tests).

2. **Phase 2** (after a release cycle of telemetry): Confirm latency is acceptable. If not, downgrade to Option C and gate the fragment-build phase behind `enable_ivm_create_time_full_trial=true` (default true; tunable down for a slow-CREATE workload).

3. **Phase 3**: When the whitelist widens (e.g. PR #7 enables `MIN/MAX(VARCHAR)`), trial rewrite should also pass for those combinations. Each enabling PR must verify the trial path works end-to-end before flipping the whitelist entry.

## Acceptance Criteria

- IVMAnalyzer invokes `IvmTrialRewriter.runTrial(...)` for INCREMENTAL mode.
- A test exists for each of the four refresh-time compilation failures (#6, #7, #9, #10) that asserts CREATE fails (with a clear "trial compilation failed" message) when the whitelist temporarily lets that combination through.
- All 100+ existing IVM tests in `fe-core` pass unchanged.
- A latency benchmark (run on the 4 representative MV shapes above) shows trial rewrite adds < 1s to CREATE per MV in the worst case.

## Decision Log

- 2026-05-09: Open this design doc as the prerequisite for the trial-rewrite PR. Recommend Option B + Approach M2; will revisit after team review.
