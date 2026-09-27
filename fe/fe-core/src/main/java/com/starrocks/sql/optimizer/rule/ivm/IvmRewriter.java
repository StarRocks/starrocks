// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.JsonSyntaxException;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.load.Load;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Map;

/**
 * Entry point for the unified IVM (Incremental View Maintenance) plan rewriting.
 *
 * <p>Wraps the MV query plan with a {@link LogicalDeltaOperator} marker, then iteratively
 * applies delta and version rewrite rules to push the marker down through the plan tree.
 * If any unresolved markers remain after rewriting, throws a {@link SemanticException}
 * — the partially-rewritten plan would still carry {@code TvrVersionRange} on the scan,
 * so silently dropping the markers and running it would produce delta-only state without
 * the state_union join the rules are responsible for, i.e. wrong data.</p>
 *
 * <p>This rewriter is table-type agnostic. Concrete delta/version resolution for specific
 * table types (Iceberg, OLAP, etc.) is handled by the individual scan rules registered
 * in {@link RuleSet#IVM_DELTA_REWRITE_RULES}.</p>
 *
 * <p>Version info for Iceberg tables is already set on scan operators by
 * {@code MVIVMBasedRefreshProcessor.buildInsertPlan()} via {@code RelationTransformer},
 * so no additional version binding is needed here.</p>
 */
public class IvmRewriter {
    private IvmRewriter() {
    }

    /**
     * Main entry point for IVM rewriting.
     *
     * <p>Flow:
     * <ol>
     *   <li>Wrap the plan root with {@link LogicalDeltaOperator} and apply delta/version rules</li>
     *   <li>Convergence check: if unresolved markers remain, throw — the rewrite cannot
     *       continue safely (see class javadoc)</li>
     *   <li>For PK target MVs, append __op column for UPSERT/DELETE semantics</li>
     * </ol>
     */
    public static void rewrite(OptExpression tree, TaskContext rootTaskContext, TaskScheduler scheduler,
                               ColumnRefSet requiredColumns) {
        OptimizerContext optimizerContext = rootTaskContext.getOptimizerContext();

        // Gate: only run when IVM refresh is enabled. The IVM session variables are
        // scoped by MVIVMRefreshProcessor.prepareRefreshPlan via try/finally, so the
        // hybrid IVM→PCT fallback (which reuses the same ConnectContext for the PCT
        // retry) sees the variable cleared and skips IvmRewriter entirely.
        if (!optimizerContext.getSessionVariable().isEnableIVMRefresh()) {
            return;
        }

        OptExpression originalPlan = tree.inputAt(0);
        ColumnRefSet requiredColumnsBefore = requiredColumns.clone();
        ColumnRefSet taskRequiredColumnsBefore = rootTaskContext.getRequiredColumns().clone();
        String originalPlanDigest = IvmRuleUtils.structureDigest(originalPlan);
        boolean committed = false;
        Throwable failure = null;

        try {
            // Phase 1: Rewrite a cloned trial plan so fallback cannot corrupt the original plan.
            ColumnRefOperator actionColumn = optimizerContext.getColumnRefFactory()
                    .create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
            OptExpression trialPlan = MvUtils.cloneExpression(originalPlan);
            tree.setChild(0, OptExpression.create(
                    new LogicalDeltaOperator(true, actionColumn), trialPlan));
            deriveLogicalProperty(tree);
            bindStateColumnsForAggregate(optimizerContext, trialPlan);
            scheduler.rewriteIterative(tree, rootTaskContext, RuleSet.IVM_DELTA_REWRITE_RULES);

            // Phase 2: Convergence check — every Delta/Version marker must have been
            // resolved. Continuing with the partially-rewritten plan would silently
            // produce wrong data; see class javadoc.
            if (IvmRuleUtils.containsLogicalDelta(tree.inputAt(0))
                    || IvmRuleUtils.containsLogicalVersion(tree.inputAt(0))) {
                throw new SemanticException(
                        "IVM rewrite failed to fully resolve incremental markers; "
                                + "remaining operators cannot be processed incrementally");
            }

            // Phase 3: For PK target MVs, append __op column for UPSERT/DELETE semantics.
            OptExpression rewrittenRoot = tree.inputAt(0);
            deriveLogicalProperty(rewrittenRoot);
            if (isPrimaryKeyTargetMv(optimizerContext)) {
                rewrittenRoot = appendPkLoadOpColumn(rewrittenRoot, rootTaskContext, requiredColumns, actionColumn);
            }
            tree.setChild(0, rewrittenRoot);
            deriveLogicalProperty(tree);
            committed = true;
        } catch (RuntimeException | Error t) {
            failure = t;
            throw t;
        } finally {
            if (!committed) {
                tree.setChild(0, originalPlan);
                restoreColumnRefSet(requiredColumns, requiredColumnsBefore);
                restoreColumnRefSet(rootTaskContext.getRequiredColumns(), taskRequiredColumnsBefore);
                deriveLogicalProperty(tree);
            }
            checkOriginalPlanNotMutated(originalPlanDigest, originalPlan, failure);
        }
    }

    private static void restoreColumnRefSet(ColumnRefSet target, ColumnRefSet source) {
        target.clear();
        target.union(source);
    }

    private static void checkOriginalPlanNotMutated(String digestBefore, OptExpression originalPlan, Throwable failure) {
        String digestAfter = IvmRuleUtils.structureDigest(originalPlan);
        if (digestBefore.equals(digestAfter)) {
            return;
        }
        IllegalStateException leak = new IllegalStateException("IVM rewrite leaked mutation into originalPlan");
        if (failure != null) {
            failure.addSuppressed(leak);
        } else {
            throw leak;
        }
    }

    private static boolean isPrimaryKeyTargetMv(OptimizerContext optimizerContext) {
        // At refresh the optimizer's statement is not the InsertStmt, so resolve the target MV from the tvr
        // context (loadTargetMv), falling back to the InsertStmt target for the direct-insert plan.
        MaterializedView targetMv = loadTargetMv(optimizerContext);
        if (targetMv == null && optimizerContext.getStatement() instanceof InsertStmt insertStmt
                && insertStmt.getTargetTable() instanceof MaterializedView insertTarget) {
            targetMv = insertTarget;
        }
        return targetMv != null && targetMv.getKeysType() == KeysType.PRIMARY_KEYS;
    }

    private static OptExpression appendPkLoadOpColumn(OptExpression root, TaskContext rootTaskContext,
                                                      ColumnRefSet requiredColumns,
                                                      ColumnRefOperator actionColumn) {
        OptimizerContext optimizerContext = rootTaskContext.getOptimizerContext();
        List<ColumnRefOperator> rootOutputColumns = root.getOutputColumns()
                .getColumnRefOperators(optimizerContext.getColumnRefFactory());
        // __op is pre-placed as a fixed trailing output before optimize (InsertPlanner); bind it to
        // __ACTION__ (same domain: INSERT_ACTION=UPSERT, DELETE_ACTION=DELETE) so it rides the sink by
        // position. Fall back to creating one for a direct insert that did not pre-place it.
        List<ColumnRefOperator> ivmOutputColumns = optimizerContext.getTvrOptContext().getIvmInsertOutputColumns();
        ColumnRefOperator loadOpColumn = ivmOutputColumns == null ? null : ivmOutputColumns.stream()
                .filter(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()))
                .findFirst().orElse(null);
        if (loadOpColumn != null && rootOutputColumns.contains(loadOpColumn)) {
            return root;
        }
        if (loadOpColumn == null) {
            loadOpColumn = optimizerContext.getColumnRefFactory()
                    .create(Load.LOAD_OP_COLUMN, IntegerType.TINYINT, false);
        }

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator outputColumn : rootOutputColumns) {
            if (!outputColumn.equals(actionColumn)) {
                projectMap.put(outputColumn, outputColumn);
            }
        }
        projectMap.put(loadOpColumn, actionColumn);

        requiredColumns.union(loadOpColumn);
        rootTaskContext.getRequiredColumns().union(loadOpColumn);

        return OptExpression.create(new LogicalProjectOperator(projectMap), root);
    }

    /**
     * Binds each aggregate output to the MV column that stores its state, keyed by ColumnRef id
     * (consumed by {@link IvmDeltaAggregateRule}); no-op for non-aggregate plans.
     *
     * <p>A state column is recognized structurally: an INSERT output whose expression resolves,
     * through pass-through projections, to a bare aggregation output ref. That matches the hidden
     * __AGG_STATE_ columns and collapsed union columns (whose visible output IS the state), and
     * excludes finalized outputs like CASE(sum_state_merge(...)), group keys and __ROW_ID__.
     * Keying by ref id instead of zipping prefix-filtered schema positions against id-sorted
     * aggregates keeps the pairing correct for any column layout.</p>
     */
    private static void bindStateColumnsForAggregate(OptimizerContext optimizerContext, OptExpression plan) {
        List<ColumnRefOperator> outputColumns =
                optimizerContext.getTvrOptContext().getIvmInsertOutputColumns();
        if (outputColumns == null) {
            return;
        }
        // __op is a pre-placed sink control column appended last (InsertPlanner), not an MV schema column;
        // exclude it so the positional binding against the MV schema stays aligned.
        if (!outputColumns.isEmpty()
                && Load.LOAD_OP_COLUMN.equalsIgnoreCase(outputColumns.get(outputColumns.size() - 1).getName())) {
            outputColumns = outputColumns.subList(0, outputColumns.size() - 1);
        }
        Map<ColumnRefOperator, ScalarOperator> translations = Maps.newHashMap();
        LogicalAggregationOperator aggOperator = null;
        OptExpression node = plan;
        while (node != null) {
            Operator op = node.getOp();
            if (op.getProjection() != null) {
                translations.putAll(op.getProjection().getColumnRefMap());
            }
            if (op instanceof LogicalProjectOperator) {
                translations.putAll(((LogicalProjectOperator) op).getColumnRefMap());
            } else if (op instanceof LogicalAggregationOperator) {
                aggOperator = (LogicalAggregationOperator) op;
                break;
            } else if (!(op instanceof LogicalFilterOperator) && !(op instanceof LogicalTopNOperator)) {
                break;
            }
            node = node.getInputs().isEmpty() ? null : node.inputAt(0);
        }
        if (aggOperator == null) {
            return;
        }
        MaterializedView mv = loadTargetMv(optimizerContext);
        if (mv == null) {
            return;
        }
        List<Column> schema = mv.getFullSchema();
        Preconditions.checkState(outputColumns.size() == schema.size(),
                "IVM insert output count %s must match MV '%s' schema size %s",
                outputColumns.size(), mv.getName(), schema.size());
        Map<Integer, String> stateColumnByAggRefId = Maps.newHashMap();
        for (int i = 0; i < outputColumns.size(); i++) {
            ScalarOperator resolved = outputColumns.get(i);
            for (int hop = 0; hop < 8 && resolved instanceof ColumnRefOperator
                    && translations.containsKey(resolved); hop++) {
                ScalarOperator next = translations.get(resolved);
                if (next == resolved) {
                    break;
                }
                resolved = next;
            }
            if (resolved instanceof ColumnRefOperator
                    && aggOperator.getAggregations().containsKey((ColumnRefOperator) resolved)) {
                stateColumnByAggRefId.put(((ColumnRefOperator) resolved).getId(), schema.get(i).getName());
            }
        }
        Preconditions.checkState(stateColumnByAggRefId.size() == aggOperator.getAggregations().size(),
                "every aggregate of MV '%s' must bind to exactly one state column, bound %s of %s",
                mv.getName(), stateColumnByAggRefId.size(), aggOperator.getAggregations().size());
        optimizerContext.getTvrOptContext().setIvmStateColumnNameByAggRefId(stateColumnByAggRefId);
    }

    static MaterializedView loadTargetMv(OptimizerContext optimizerContext) {
        // IvmTrialRewriter injects an unregistered mock MV here for CREATE-time trial.
        MaterializedView override = optimizerContext.getQueryMaterializationContext().getOverrideTargetMv();
        if (override != null) {
            return override;
        }
        String strMvId = optimizerContext.getSessionVariable().getTvrTargetMvId();
        if (Strings.isNullOrEmpty(strMvId)) {
            return null;
        }
        MvId mvId;
        try {
            mvId = GsonUtils.GSON.fromJson(strMvId, MvId.class);
        } catch (JsonSyntaxException e) {
            return null;
        }
        if (mvId == null) {
            return null;
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
        if (db == null) {
            return null;
        }
        if (!(db.getTable(mvId.getId()) instanceof MaterializedView mv)) {
            return null;
        }
        return mv;
    }

    static void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }
        if (root.getLogicalProperty() == null) {
            ExpressionContext context = new ExpressionContext(root);
            context.deriveLogicalProperty();
            root.setLogicalProperty(context.getRootProperty());
        }
    }

}
