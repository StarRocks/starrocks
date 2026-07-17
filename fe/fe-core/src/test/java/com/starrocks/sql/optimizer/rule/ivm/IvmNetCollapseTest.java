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

import com.google.common.collect.Maps;
import com.starrocks.load.Load;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmRuleUtils;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link IvmNetCollapse#applyIfRetractable}: a retractable plan (row id present, action not a
 * constant) collapses to one op per {@code __ROW_ID__}; an append-only plan (constant action or no row id)
 * returns null so the shared append-only path in {@link IvmRewriter} keeps the plain {@code __op} alias.
 */
public class IvmNetCollapseTest {

    private static ColumnRefOperator rowIdCol(ColumnRefFactory factory) {
        return factory.create(IvmOpUtils.COLUMN_ROW_ID, StringType.STRING, false);
    }

    private static ColumnRefOperator actionCol(ColumnRefFactory factory) {
        return factory.create(IvmRuleUtils.ACTION_COLUMN_NAME, IvmRuleUtils.ACTION_COLUMN_TYPE, false);
    }

    private static TaskContext newTaskContext(OptimizerContext context) {
        return new TaskContext(context, new PhysicalPropertySet(), new ColumnRefSet(), Double.MAX_VALUE);
    }

    private static void deriveLogicalProperty(OptExpression expression) {
        for (OptExpression child : expression.getInputs()) {
            deriveLogicalProperty(child);
        }
        ExpressionContext ctx = new ExpressionContext(expression);
        ctx.deriveLogicalProperty();
        expression.setLogicalProperty(ctx.getRootProperty());
    }

    @Test
    public void testNetCollapseForRetractablePlan() {
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator rowId = rowIdCol(factory);
        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator action = actionCol(factory);

        // A leaf whose output carries __ROW_ID__ and a non-constant __ACTION__ (a plain scan column).
        OptExpression root = OptExpression.create(
                new LogicalValuesOperator(List.of(rowId, idRef, action), Collections.emptyList()));
        deriveLogicalProperty(root);

        OptExpression result = IvmNetCollapse.applyIfRetractable(
                root, newTaskContext(context), new ColumnRefSet(), action);

        Assertions.assertNotNull(result, "row id + non-constant action must net-collapse");
        Assertions.assertTrue(result.getOp() instanceof LogicalProjectOperator, "top is the __op project");
        LogicalProjectOperator project = (LogicalProjectOperator) result.getOp();
        Assertions.assertTrue(project.getColumnRefMap().keySet().stream()
                        .anyMatch(col -> Load.LOAD_OP_COLUMN.equalsIgnoreCase(col.getName())),
                "collapsed project must produce __op");
        Assertions.assertTrue(project.getColumnRefMap().keySet().stream()
                        .noneMatch(col -> IvmRuleUtils.ACTION_COLUMN_NAME.equalsIgnoreCase(col.getName())),
                "__ACTION__ must be replaced by __op");

        OptExpression filter = result.inputAt(0);
        Assertions.assertTrue(filter.getOp() instanceof LogicalFilterOperator, "rn = 1 filter");
        OptExpression window = filter.inputAt(0);
        Assertions.assertTrue(window.getOp() instanceof LogicalWindowOperator, "row_number window");
        Assertions.assertEquals(List.of(rowId),
                ((LogicalWindowOperator) window.getOp()).getPartitionExpressions(),
                "window must partition by __ROW_ID__");
    }

    @Test
    public void testJoinDeltaGetsTupleNetCancellation() {
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator rowId = rowIdCol(factory);
        ColumnRefOperator valRef = factory.create("v", IntegerType.INT, false);
        ColumnRefOperator action = actionCol(factory);

        // A join in the plan means the delta branches can emit several rows per __ROW_ID__ with differing
        // values, so the collapse must cancel equal-tuple pairs before the per-row-id pick.
        OptExpression left = OptExpression.create(
                new LogicalValuesOperator(List.of(rowId, valRef), Collections.emptyList()));
        OptExpression right = OptExpression.create(
                new LogicalValuesOperator(List.of(action), Collections.emptyList()));
        OptExpression join = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, null), left, right);
        Map<ColumnRefOperator, ScalarOperator> rootMap = Maps.newHashMap();
        rootMap.put(rowId, rowId);
        rootMap.put(valRef, valRef);
        rootMap.put(action, action);
        OptExpression root = OptExpression.create(new LogicalProjectOperator(rootMap), join);
        deriveLogicalProperty(root);

        OptExpression result = IvmNetCollapse.applyIfRetractable(
                root, newTaskContext(context), new ColumnRefSet(), action);

        Assertions.assertNotNull(result);
        OptExpression pickFilter = result.inputAt(0);
        Assertions.assertTrue(pickFilter.getOp() instanceof LogicalFilterOperator, "rn = 1 filter");
        OptExpression pickWindow = pickFilter.inputAt(0);
        Assertions.assertTrue(pickWindow.getOp() instanceof LogicalWindowOperator, "row_number window");
        OptExpression actionRemap = pickWindow.inputAt(0);
        Assertions.assertTrue(actionRemap.getOp() instanceof LogicalProjectOperator, "net-sign action re-map");
        Assertions.assertTrue(((LogicalProjectOperator) actionRemap.getOp()).getColumnRefMap().get(action)
                        instanceof com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator,
                "__ACTION__ is re-mapped from the net sign so a negative group emits a delete");
        OptExpression netFilter = actionRemap.inputAt(0);
        Assertions.assertTrue(netFilter.getOp() instanceof LogicalFilterOperator, "net != 0 filter");
        OptExpression netWindow = netFilter.inputAt(0);
        Assertions.assertTrue(netWindow.getOp() instanceof LogicalWindowOperator, "peer-group SUM window");
        LogicalWindowOperator sumWindow = (LogicalWindowOperator) netWindow.getOp();
        Assertions.assertEquals(List.of(rowId), sumWindow.getPartitionExpressions(),
                "cancellation partitions by __ROW_ID__ only, sharing the pick window's shuffle");
        Assertions.assertEquals(List.of(valRef),
                sumWindow.getOrderByElements().stream().map(Ordering::getColumnRef).toList(),
                "peers tie on the value columns");
        Assertions.assertEquals(AnalyticWindow.Type.RANGE, sumWindow.getAnalyticWindow().getType());
        Assertions.assertEquals(AnalyticWindowBoundary.BoundaryType.CURRENT_ROW,
                sumWindow.getAnalyticWindow().getLeftBoundary().getBoundaryType());
        Assertions.assertEquals(AnalyticWindowBoundary.BoundaryType.CURRENT_ROW,
                sumWindow.getAnalyticWindow().getRightBoundary().getBoundaryType());
        OptExpression sgnProject = netWindow.inputAt(0);
        Assertions.assertTrue(sgnProject.getOp() instanceof LogicalProjectOperator, "sgn projection");
        Assertions.assertTrue(((LogicalProjectOperator) sgnProject.getOp()).getColumnRefMap().keySet().stream()
                        .anyMatch(col -> "sgn".equals(col.getName())),
                "signed action feeds the peer-group SUM");
        Assertions.assertSame(root, sgnProject.inputAt(0), "cancellation sits directly on the delta root");
    }

    @Test
    public void testAggregateOverJoinSkipsTupleCancellation() {
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator rowId = rowIdCol(factory);
        ColumnRefOperator gKey = factory.create("g", IntegerType.INT, false);
        ColumnRefOperator action = actionCol(factory);

        // An aggregate MV whose recompute joins the affected groups back to the base (the semi-join in
        // IvmDeltaRetractableAggregateRule) contains a join, but the aggregate collapses each group to one
        // row id, so the tuple cancellation must not apply -- the pick window is fed the plan directly.
        OptExpression left = OptExpression.create(
                new LogicalValuesOperator(List.of(rowId, gKey), Collections.emptyList()));
        OptExpression right = OptExpression.create(
                new LogicalValuesOperator(List.of(action), Collections.emptyList()));
        OptExpression join = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN, null), left, right);
        OptExpression agg = OptExpression.create(
                new LogicalAggregationOperator(AggType.GLOBAL, List.of(gKey), Maps.newHashMap()), join);
        Map<ColumnRefOperator, ScalarOperator> rootMap = Maps.newHashMap();
        rootMap.put(rowId, rowId);
        rootMap.put(gKey, gKey);
        rootMap.put(action, action);
        OptExpression root = OptExpression.create(new LogicalProjectOperator(rootMap), agg);
        deriveLogicalProperty(root);

        OptExpression result = IvmNetCollapse.applyIfRetractable(
                root, newTaskContext(context), new ColumnRefSet(), action);

        Assertions.assertNotNull(result);
        OptExpression pickWindow = result.inputAt(0).inputAt(0);
        Assertions.assertTrue(pickWindow.getOp() instanceof LogicalWindowOperator, "row_number window");
        Assertions.assertSame(root, pickWindow.inputAt(0),
                "an aggregate plan feeds the pick window directly, with no tuple-cancellation layer");
    }

    @Test
    public void testNetCollapseSkippedForConstantAction() {
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator rowId = rowIdCol(factory);
        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator action = actionCol(factory);

        // __ACTION__ resolves to a constant UPSERT (append-only): a projection maps it to a ConstantOperator.
        OptExpression leaf = OptExpression.create(
                new LogicalValuesOperator(List.of(rowId, idRef), Collections.emptyList()));
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.put(rowId, rowId);
        projectMap.put(idRef, idRef);
        projectMap.put(action, ConstantOperator.createTinyInt((byte) 0));
        OptExpression root = OptExpression.create(new LogicalProjectOperator(projectMap), leaf);
        deriveLogicalProperty(root);

        OptExpression result = IvmNetCollapse.applyIfRetractable(
                root, newTaskContext(context), new ColumnRefSet(), action);

        Assertions.assertNull(result, "constant __ACTION__ (append-only) must defer to the shared path");
    }

    @Test
    public void testNetCollapseSkippedWithoutRowId() {
        ColumnRefFactory factory = new ColumnRefFactory();
        OptimizerContext context = OptimizerFactory.mockContext(factory);
        ColumnRefOperator idRef = factory.create("id", IntegerType.INT, false);
        ColumnRefOperator action = actionCol(factory);

        // No __ROW_ID__ on the output: nothing to key deletes on, so net-collapse is skipped.
        OptExpression root = OptExpression.create(
                new LogicalValuesOperator(List.of(idRef, action), Collections.emptyList()));
        deriveLogicalProperty(root);

        OptExpression result = IvmNetCollapse.applyIfRetractable(
                root, newTaskContext(context), new ColumnRefSet(), action);

        Assertions.assertNull(result, "no __ROW_ID__ must defer to the shared path");
    }
}
