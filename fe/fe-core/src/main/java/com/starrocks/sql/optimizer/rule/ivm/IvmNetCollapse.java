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
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;

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
                root);

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
