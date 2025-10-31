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

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.wildfly.common.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PhysicalPlanColumnOrderUtilTest {
    private List<String> expectedColumnNames;
    private List<ColumnRefOperator> expectedColumnRefOperators;
    private PhysicalPlanColumnOrderUtil fixer;

    @BeforeEach
    public void setUp() {
        expectedColumnNames = Arrays.asList("col1", "col2", "col3");
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(0, Type.INT, expectedColumnNames.get(0), false);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(1, Type.INT, expectedColumnNames.get(1), false);
        ColumnRefOperator columnRefOperator3 = new ColumnRefOperator(2, Type.INT, expectedColumnNames.get(2), false);
        expectedColumnRefOperators = Arrays.asList(columnRefOperator1, columnRefOperator2, columnRefOperator3);
        fixer = new PhysicalPlanColumnOrderUtil(expectedColumnRefOperators);
    }

    @Test
    public void testNeedsOrderFix_WhenOrderMatches_ReturnsFalse() {
        // Prepare
        List<ColumnRefOperator> currentOrder = Arrays.asList(
                createColumnRefOperator("col1"),
                createColumnRefOperator("col2"),
                createColumnRefOperator("col3")
        );

        // Execution
        boolean result = fixer.needsOrderFix(currentOrder);

        // Verification
        Assert.assertFalse(result);
    }

    @Test
    public void testNeedsOrderFix_WhenOrderDifferent_ReturnsTrue() {
        // Prepare
        List<ColumnRefOperator> currentOrder = Arrays.asList(
                createColumnRefOperator("col2"),
                createColumnRefOperator("col1"),
                createColumnRefOperator("col3")
        );

        // Execution
        boolean result = fixer.needsOrderFix(currentOrder);

        // Verification
        Assert.assertTrue(result);
    }

    @Test
    public void testNeedsOrderFix_WhenDifferentSize_ReturnsFalse() {
        // Prepare
        List<ColumnRefOperator> currentOrder = Arrays.asList(
                createColumnRefOperator("col1"),
                createColumnRefOperator("col2")
        );

        // Execution
        boolean result = fixer.needsOrderFix(currentOrder);

        // Verification
        Assert.assertFalse(result);
    }

    @Test
    public void testIsAlreadyInOrder_WhenInOrder_ReturnsTrue() {
        // Prepare
        List<Integer> sortList = Arrays.asList(0, 1, 2);

        // Execution
        boolean result = fixer.isAlreadyInOrder(sortList);

        // Verification
        Assert.assertTrue(result);
    }

    @Test
    public void testIsAlreadyInOrder_WhenNotInOrder_ReturnsFalse() {
        // Prepare
        List<Integer> sortList = Arrays.asList(1, 0, 2);

        // Execution
        boolean result = fixer.isAlreadyInOrder(sortList);

        // Verification
        Assert.assertFalse(result);
    }

    @Test
    public void testFixPhysicalValuesOperator_WhenOrderNeedsFix() {
        // Prepare
        List<ColumnRefOperator> columnRefs = Arrays.asList(
                createColumnRefOperator("col2"),
                createColumnRefOperator("col1"),
                createColumnRefOperator("col3")
        );

        List<List<ScalarOperator>> rows = Arrays.asList(
                Arrays.asList(createScalarOperator("value2"), createScalarOperator("value1"), createScalarOperator("value3")),
                Arrays.asList(createScalarOperator("value2_2"), createScalarOperator("value1_2"), createScalarOperator("value3_2"))
        );

        PhysicalValuesOperator valuesOperator = Mockito.mock(PhysicalValuesOperator.class);
        Mockito.when(valuesOperator.getColumnRefSet()).thenReturn(columnRefs);
        Mockito.when(valuesOperator.getRows()).thenReturn(rows);
        Mockito.when(valuesOperator.getLimit()).thenReturn(-1L);
        Mockito.when(valuesOperator.getPredicate()).thenReturn(null);
        Mockito.when(valuesOperator.getProjection()).thenReturn(null);

        OptExpression optExpr = Mockito.mock(OptExpression.class);
        Mockito.when(optExpr.getOp()).thenReturn(valuesOperator);

        ExecPlan context = Mockito.mock(ExecPlan.class);
        Mockito.when(context.getPhysicalPlan()).thenReturn(null);

        // Execution
        fixer.fixPhysicalValuesOperator(optExpr, valuesOperator, context);

        // Verification - Check whether the operator has been replaced
        Mockito.verify(optExpr).setOp(Mockito.any(PhysicalValuesOperator.class));
    }

    @Test
    public void testVisit_WithPhysicalValuesOperator() {
        // Prepare
        PhysicalValuesOperator valuesOperator = Mockito.mock(PhysicalValuesOperator.class);
        Mockito.when(valuesOperator.getColumnRefSet()).thenReturn(Arrays.asList(
                createColumnRefOperator("col1"), createColumnRefOperator("col2"), createColumnRefOperator("col3")
        ));
        Mockito.when(valuesOperator.getRows()).thenReturn(Collections.emptyList());

        OptExpression optExpr = Mockito.mock(OptExpression.class);
        Mockito.when(optExpr.getOp()).thenReturn(valuesOperator);
        Mockito.when(optExpr.getInputs()).thenReturn(Collections.emptyList());

        ExecPlan context = Mockito.mock(ExecPlan.class);
        Mockito.when(context.getPhysicalPlan()).thenReturn(null);

        // Execution
        fixer.fixPhysicalPlan(optExpr, context);

        // Verification - Ensure that the method is executed normally without any abnormalities
        Mockito.verify(optExpr).getInputs();
    }

    @Test
    public void testFixPhysicalPlan() {
        // Prepare
        OptExpression optExpr = Mockito.mock(OptExpression.class);
        Mockito.when(optExpr.getInputs()).thenReturn(Collections.emptyList());
        Mockito.when(optExpr.getOp()).thenReturn(Mockito.mock(Operator.class));

        ExecPlan context = Mockito.mock(ExecPlan.class);

        // Execution
        fixer.fixPhysicalPlan(optExpr, context);

        // Verification - Make sure the visit method is called
        Mockito.verify(optExpr).getInputs();
    }

    // Auxiliary methods
    private ColumnRefOperator createColumnRefOperator(String name) {
        ColumnRefOperator colRef = Mockito.mock(ColumnRefOperator.class);
        Mockito.when(colRef.getName()).thenReturn(name);
        return colRef;
    }

    private ScalarOperator createScalarOperator(String value) {
        ScalarOperator scalar = Mockito.mock(ScalarOperator.class);
        return scalar;
    }
}
