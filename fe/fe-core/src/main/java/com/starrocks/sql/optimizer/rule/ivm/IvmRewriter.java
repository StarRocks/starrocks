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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.JsonSyntaxException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.load.Load;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
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
        StatementBase statement = optimizerContext.getStatement();
        if (!(statement instanceof InsertStmt insertStmt)) {
            return false;
        }
        if (!(insertStmt.getTargetTable() instanceof MaterializedView targetMv)) {
            return false;
        }
        return targetMv.getKeysType() == KeysType.PRIMARY_KEYS;
    }

    private static OptExpression appendPkLoadOpColumn(OptExpression root, TaskContext rootTaskContext,
                                                      ColumnRefSet requiredColumns,
                                                      ColumnRefOperator actionColumn) {
        List<ColumnRefOperator> rootOutputColumns = root.getOutputColumns()
                .getColumnRefOperators(rootTaskContext.getOptimizerContext().getColumnRefFactory());
        boolean hasLoadOpColumn = rootOutputColumns.stream()
                .anyMatch(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()));
        if (hasLoadOpColumn) {
            return root;
        }

        ColumnRefOperator loadOpColumn = rootTaskContext.getOptimizerContext().getColumnRefFactory()
                .create(Load.LOAD_OP_COLUMN, IntegerType.TINYINT, false);
        // __op shares __ACTION__'s domain (0 = UPSERT, 1 = DELETE) — direct alias.
        ScalarOperator loadOpExpr = actionColumn;

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator outputColumn : rootOutputColumns) {
            if (!outputColumn.equals(actionColumn)) {
                projectMap.put(outputColumn, outputColumn);
            }
        }
        projectMap.put(loadOpColumn, loadOpExpr);

        requiredColumns.union(loadOpColumn);
        rootTaskContext.getRequiredColumns().union(loadOpColumn);

        OptExpression projectExpr = OptExpression.create(new LogicalProjectOperator(projectMap), root);
        // TopN orders DELETEs before UPSERTs for same-PK batches. Skip it when __ACTION__
        // is provably constant (e.g. append-only Iceberg) — there are no DELETEs to order.
        if (isActionColumnConstant(root, actionColumn)) {
            return projectExpr;
        }
        List<Ordering> orderings = List.of(new Ordering(loadOpColumn, false, false));
        LogicalTopNOperator.Builder topNBuilder = LogicalTopNOperator.builder()
                .withOperator(
                        new LogicalTopNOperator(orderings, Operator.DEFAULT_LIMIT, Operator.DEFAULT_OFFSET,
                                SortPhase.PARTIAL))
                .setOrderByElements(orderings)
                .setPerPipeline(true);
        LogicalTopNOperator topN = topNBuilder.build();
        return OptExpression.create(topN, projectExpr);
    }

    /**
     * Walks down the plan tracing {@code target} through aliasing projections (including
     * projections attached to non-Project operators like Filter). Returns true if the trace
     * lands on a {@link ConstantOperator}; bails at multi-input operators (join/union/set op)
     * or any non-constant, non-alias expression.
     */
    private static boolean isActionColumnConstant(OptExpression root, ColumnRefOperator actionColumn) {
        OptExpression current = root;
        ColumnRefOperator target = actionColumn;
        for (int i = 0; current != null && i < 64; i++) {
            Map<ColumnRefOperator, ScalarOperator> colMap = extractColumnRefMap(current.getOp());
            if (colMap != null && colMap.containsKey(target)) {
                ScalarOperator expr = colMap.get(target);
                if (expr instanceof ConstantOperator) {
                    return true;
                }
                if (!(expr instanceof ColumnRefOperator)) {
                    return false;
                }
                target = (ColumnRefOperator) expr;
            }
            if (current.getInputs().size() != 1) {
                return false;
            }
            current = current.inputAt(0);
        }
        return false;
    }

    private static Map<ColumnRefOperator, ScalarOperator> extractColumnRefMap(Operator op) {
        if (op instanceof LogicalProjectOperator) {
            return ((LogicalProjectOperator) op).getColumnRefMap();
        }
        if (op.getProjection() != null) {
            return op.getProjection().getColumnRefMap();
        }
        return null;
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
