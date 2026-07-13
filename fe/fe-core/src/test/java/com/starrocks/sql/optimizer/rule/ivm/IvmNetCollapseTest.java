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
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
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
