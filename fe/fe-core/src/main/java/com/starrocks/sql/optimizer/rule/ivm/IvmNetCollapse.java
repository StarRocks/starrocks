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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.load.Load;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Retractable (delete/update) net-collapse for cloud-native PRIMARY KEY IVM. Enterprise-only: it depends on
 * the delete-CDC delta the community CHANGES scan does not emit, so it lives outside {@link IvmRewriter} to
 * keep that shared file aligned with the community/append-only path (a plain {@code __ACTION__ -> __op}
 * projection).
 *
 * <p>{@link IvmRewriter#appendPkLoadOpColumn} calls {@link #applyIfRetractable} once; a non-retractable
 * (append-only / no row id) plan returns {@code null} and falls back to that shared projection unchanged.
 */
final class IvmNetCollapse {
    private IvmNetCollapse() {
    }

    /**
     * Returns a net-collapsed plan (each {@code __ROW_ID__} emits at most one op) when the plan is retractable,
     * else {@code null} to defer to the shared append-only handling in {@link IvmRewriter#appendPkLoadOpColumn}.
     */
    static OptExpression applyIfRetractable(OptExpression root, TaskContext rootTaskContext,
                                            ColumnRefSet requiredColumns, ColumnRefOperator actionColumn) {
        ColumnRefFactory factory = rootTaskContext.getOptimizerContext().getColumnRefFactory();
        List<ColumnRefOperator> rootOutputColumns = root.getOutputColumns().getColumnRefOperators(factory);
        ColumnRefOperator rowIdColumn = findRowIdColumn(root, rootOutputColumns);
        // Provably constant __ACTION__ (append-only) or no row id to group on: no DELETEs / no same-key pairs,
        // so let the shared path map __ACTION__ -> __op without collapsing.
        if (rowIdColumn == null || isActionColumnConstant(root, actionColumn)) {
            return null;
        }
        return appendPkNetCollapse(root, factory, requiredColumns, rootTaskContext, actionColumn, rowIdColumn,
                rootOutputColumns);
    }

    // The analyzer labels the row-id output __ROW_ID__, but the refresh path re-derives the query
    // through SQL serialization, which drops that alias and renames the column after its row-id encode
    // expression. Match the expression as a fallback so net-collapse still keys on the row id at refresh.
    private static ColumnRefOperator findRowIdColumn(OptExpression root, List<ColumnRefOperator> rootOutputColumns) {
        for (ColumnRefOperator col : rootOutputColumns) {
            if (IvmOpUtils.COLUMN_ROW_ID.equalsIgnoreCase(col.getName())) {
                return col;
            }
        }
        for (ColumnRefOperator col : rootOutputColumns) {
            if (tracesToRowIdEncode(root, col)) {
                return col;
            }
        }
        return null;
    }

    private static boolean tracesToRowIdEncode(OptExpression root, ColumnRefOperator target) {
        OptExpression current = root;
        ColumnRefOperator col = target;
        for (int i = 0; current != null && i < 64; i++) {
            Map<ColumnRefOperator, ScalarOperator> colMap = extractColumnRefMap(current.getOp());
            if (colMap != null && colMap.containsKey(col)) {
                ScalarOperator expr = colMap.get(col);
                if (isRowIdEncodeExpr(expr)) {
                    return true;
                }
                if (!(expr instanceof ColumnRefOperator)) {
                    return false;
                }
                col = (ColumnRefOperator) expr;
            }
            if (current.getInputs().size() != 1) {
                return false;
            }
            current = current.inputAt(0);
        }
        return false;
    }

    private static boolean isRowIdEncodeExpr(ScalarOperator expr) {
        if (!(expr instanceof CallOperator call)) {
            return false;
        }
        if (FunctionSet.FROM_BINARY.equalsIgnoreCase(call.getFnName()) && !call.getChildren().isEmpty()) {
            return isRowIdEncodeExpr(call.getChild(0));
        }
        return IvmOpUtils.ENCODE_ROW_ID_FUNCTION_MAP.containsValue(call.getFnName().toLowerCase());
    }

    private static OptExpression appendPkNetCollapse(OptExpression root, ColumnRefFactory factory,
                                                     ColumnRefSet requiredColumns, TaskContext rootTaskContext,
                                                     ColumnRefOperator actionColumn, ColumnRefOperator rowIdColumn,
                                                     List<ColumnRefOperator> rootOutputColumns) {
        OptExpression collapseInput = appendJoinTupleNetCancellation(root, factory, actionColumn, rowIdColumn,
                rootOutputColumns);
        // ROW_NUMBER() OVER (PARTITION BY __ROW_ID__ ORDER BY __ACTION__ ASC): rn=1 is the UPSERT (new) row,
        // or the DELETE (old) row when a group was emptied. A window keeps the value columns as-is, so the
        // insert sink's pre-bound column refs survive; a GROUP BY would replace them with new refs that then
        // get pruned.
        Function rowNumberFn = ExprUtils.getBuiltinFunction(
                FunctionSet.ROW_NUMBER, new Type[0], Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Preconditions.checkArgument(rowNumberFn != null, "IVM net-collapse: row_number function not found");
        CallOperator rowNumberCall =
                new CallOperator(FunctionSet.ROW_NUMBER, IntegerType.BIGINT, List.of(), rowNumberFn);
        ColumnRefOperator rnColumn = factory.create("rn", IntegerType.BIGINT, false);

        Ordering actionOrdering = new Ordering(actionColumn, true, true);
        List<Ordering> enforceSortColumns = List.of(new Ordering(rowIdColumn, true, true), actionOrdering);
        Map<ColumnRefOperator, CallOperator> windowCall = Maps.newHashMap();
        windowCall.put(rnColumn, rowNumberCall);
        OptExpression windowExpr = OptExpression.create(
                new LogicalWindowOperator.Builder()
                        .setWindowCall(windowCall)
                        .setPartitionExpressions(List.of(rowIdColumn))
                        .setOrderByElements(List.of(actionOrdering))
                        .setAnalyticWindow(AnalyticWindow.DEFAULT_ROWS_WINDOW)
                        .setEnforceSortColumns(enforceSortColumns)
                        .build(),
                collapseInput);

        OptExpression filterExpr = OptExpression.create(
                new LogicalFilterOperator(
                        new BinaryPredicateOperator(BinaryType.EQ, rnColumn, ConstantOperator.createBigint(1))),
                windowExpr);

        // __op is pre-placed as a fixed trailing output before optimize (InsertPlanner); bind it here so it
        // rides the sink by position. Fall back to creating one if it was not pre-placed.
        List<ColumnRefOperator> ivmOutputColumns =
                rootTaskContext.getOptimizerContext().getTvrOptContext().getIvmInsertOutputColumns();
        ColumnRefOperator loadOpColumn = ivmOutputColumns == null ? null : ivmOutputColumns.stream()
                .filter(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName()))
                .findFirst().orElse(null);
        if (loadOpColumn == null) {
            loadOpColumn = factory.create(Load.LOAD_OP_COLUMN, IntegerType.TINYINT, false);
        }
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        for (ColumnRefOperator outputColumn : rootOutputColumns) {
            if (!outputColumn.equals(actionColumn)) {
                projectMap.put(outputColumn, outputColumn);
            }
        }
        // __op shares __ACTION__'s domain (INSERT_ACTION = UPSERT, DELETE_ACTION = DELETE) — direct alias.
        projectMap.put(loadOpColumn, actionColumn);
        requiredColumns.union(loadOpColumn);
        rootTaskContext.getRequiredColumns().union(loadOpColumn);
        return OptExpression.create(new LogicalProjectOperator(projectMap), filterExpr);
    }

    /**
     * A join delta emits {@code Δ(A)⋈B_from ∪ A_to⋈Δ(B)}: when both sides change the same key in one
     * interval, the branches overlap in an INSERT/DELETE pair of one full tuple plus a stale intermediate
     * INSERT, and the pick above ties on {@code __ACTION__} without seeing value columns. Cancel
     * equal-tuple pairs first: a signed count over each tuple's peer group nets to zero exactly for the
     * overlap pairs. The peer-group frame keeps the partition key {@code __ROW_ID__}, so this window
     * shares the pick window's shuffle instead of re-hashing by every column.
     */
    private static OptExpression appendJoinTupleNetCancellation(OptExpression root, ColumnRefFactory factory,
                                                                ColumnRefOperator actionColumn,
                                                                ColumnRefOperator rowIdColumn,
                                                                List<ColumnRefOperator> rootOutputColumns) {
        // A join delta is the only shape that emits several rows of one __ROW_ID__ with differing values.
        // An aggregate collapses each group to one row (row id = group key), so it never has that
        // multiplicity even when its recompute joins the affected groups back to the base
        // (IvmDeltaRetractableAggregateRule) -- and its output carries non-orderable state columns the
        // peer-group ORDER BY below cannot sort. Leave every non-join and every aggregate plan untouched.
        if (!containsJoin(root) || containsAggregate(root)) {
            return root;
        }
        ColumnRefOperator sgnColumn = factory.create("sgn", IntegerType.BIGINT, false);
        Map<ColumnRefOperator, ScalarOperator> sgnProjectMap = Maps.newHashMap();
        for (ColumnRefOperator outputColumn : rootOutputColumns) {
            sgnProjectMap.put(outputColumn, outputColumn);
        }
        sgnProjectMap.put(sgnColumn, new CaseWhenOperator(IntegerType.BIGINT, null,
                ConstantOperator.createBigint(-1),
                List.of(new BinaryPredicateOperator(BinaryType.EQ, actionColumn,
                                ConstantOperator.createTinyInt(IvmRuleUtils.INSERT_ACTION)),
                        ConstantOperator.createBigint(1))));
        OptExpression sgnProject = OptExpression.create(new LogicalProjectOperator(sgnProjectMap), root);

        List<Ordering> tupleOrdering = new ArrayList<>();
        for (ColumnRefOperator outputColumn : rootOutputColumns) {
            if (!outputColumn.equals(actionColumn) && !outputColumn.equals(rowIdColumn)) {
                tupleOrdering.add(new Ordering(outputColumn, true, true));
            }
        }
        List<Ordering> enforceSortColumns = new ArrayList<>();
        enforceSortColumns.add(new Ordering(rowIdColumn, true, true));
        enforceSortColumns.addAll(tupleOrdering);

        Function sumFunction = ExprUtils.getBuiltinFunction(FunctionSet.SUM, new Type[] {IntegerType.BIGINT},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Preconditions.checkArgument(sumFunction != null, "IVM net-collapse: sum function not found");
        ColumnRefOperator netColumn = factory.create("net", IntegerType.BIGINT, true);
        Map<ColumnRefOperator, CallOperator> netCall = Maps.newHashMap();
        netCall.put(netColumn,
                new CallOperator(FunctionSet.SUM, IntegerType.BIGINT, List.of(sgnColumn), sumFunction));
        // RANGE CURRENT ROW..CURRENT ROW: the frame is exactly the peer group — the rows tying on every
        // value column.
        AnalyticWindow peerGroupWindow = new AnalyticWindow(AnalyticWindow.Type.RANGE,
                new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.CURRENT_ROW, null),
                new AnalyticWindowBoundary(AnalyticWindowBoundary.BoundaryType.CURRENT_ROW, null));
        OptExpression netWindow = OptExpression.create(
                new LogicalWindowOperator.Builder()
                        .setWindowCall(netCall)
                        .setPartitionExpressions(List.of(rowIdColumn))
                        .setOrderByElements(tupleOrdering)
                        .setAnalyticWindow(peerGroupWindow)
                        .setEnforceSortColumns(enforceSortColumns)
                        .build(),
                sgnProject);

        OptExpression netFilter = OptExpression.create(
                new LogicalFilterOperator(
                        new BinaryPredicateOperator(BinaryType.NE, netColumn, ConstantOperator.createBigint(0))),
                netWindow);

        // Emit the action the net sign implies. A group whose net is negative is a delete even if it still
        // carries a stray INSERT -- a same-tuple update on the surviving side paired with the other side's
        // delete leaves one INSERT and two DELETEs under one peer group (net = -1). Re-map __ACTION__ from
        // the net so the pick below emits a DELETE for it instead of tying on __ACTION__ and resurrecting the
        // row via the INSERT.
        Map<ColumnRefOperator, ScalarOperator> actionProjectMap = Maps.newHashMap();
        for (ColumnRefOperator outputColumn : rootOutputColumns) {
            if (!outputColumn.equals(actionColumn)) {
                actionProjectMap.put(outputColumn, outputColumn);
            }
        }
        actionProjectMap.put(actionColumn, new CaseWhenOperator(actionColumn.getType(), null,
                ConstantOperator.createTinyInt(IvmRuleUtils.DELETE_ACTION),
                List.of(new BinaryPredicateOperator(BinaryType.GT, netColumn, ConstantOperator.createBigint(0)),
                        ConstantOperator.createTinyInt(IvmRuleUtils.INSERT_ACTION))));
        return OptExpression.create(new LogicalProjectOperator(actionProjectMap), netFilter);
    }

    private static boolean containsJoin(OptExpression expr) {
        if (expr.getOp() instanceof LogicalJoinOperator) {
            return true;
        }
        for (OptExpression input : expr.getInputs()) {
            if (containsJoin(input)) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsAggregate(OptExpression expr) {
        if (expr.getOp() instanceof LogicalAggregationOperator) {
            return true;
        }
        for (OptExpression input : expr.getInputs()) {
            if (containsAggregate(input)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Traces {@code actionColumn} down aliasing projections (including projections attached to non-Project
     * operators like Filter). Returns true if the trace lands on a {@link ConstantOperator} — an append-only
     * scan whose {@code __ACTION__} is a constant UPSERT, so there are no DELETEs and net-collapse is skipped.
     * Bails at multi-input operators or any non-constant, non-alias expression.
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
}
