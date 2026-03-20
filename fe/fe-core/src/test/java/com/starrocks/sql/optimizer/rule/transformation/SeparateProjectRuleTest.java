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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SeparateProjectRuleTest {

    /**
     * Test that SeparateProjectRule filters out projection entries whose used
     * columns are not available in the Union's base output after column pruning.
     */
    @Test
    public void testSeparateProjectFiltersInvalidProjectionEntries() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, VarcharType.VARCHAR, "k2", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, DateType.DATETIME, "v2", true);
        ColumnRefOperator dateResult = new ColumnRefOperator(3, DateType.DATE, "date_result", true);
        CallOperator dateCall = new CallOperator("date", DateType.DATE, ImmutableList.of(col2));

        ColumnRefOperator child1Col1 = new ColumnRefOperator(4, VarcharType.VARCHAR, "c1_k2", true);
        ColumnRefOperator child2Col1 = new ColumnRefOperator(5, VarcharType.VARCHAR, "c2_k2", true);

        // Union's base output only has col1 (col2 was pruned)
        List<ColumnRefOperator> outputs = Lists.newArrayList(col1);
        List<List<ColumnRefOperator>> childOutputs = Lists.newArrayList(
                Lists.newArrayList(child1Col1),
                Lists.newArrayList(child2Col1)
        );

        // Embedded projection still references col2 via date(col2)
        Map<ColumnRefOperator, ScalarOperator> projectionMap = new HashMap<>();
        projectionMap.put(dateResult, dateCall);
        projectionMap.put(col1, col1);

        LogicalUnionOperator unionOp = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(outputs)
                .setChildOutputColumns(childOutputs)
                .isUnionAll(true)
                .setProjection(new Projection(projectionMap))
                .build();

        OptExpression child1 = new OptExpression(
                new LogicalValuesOperator(ImmutableList.of(child1Col1)));
        OptExpression child2 = new OptExpression(
                new LogicalValuesOperator(ImmutableList.of(child2Col1)));
        OptExpression unionExpr = OptExpression.create(unionOp, child1, child2);

        SeparateProjectRule rule = new SeparateProjectRule();
        OptExpression result = rule.rewriteImpl(unionExpr);

        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, result.getOp().getOpType());
        LogicalProjectOperator projectOp = (LogicalProjectOperator) result.getOp();

        Assertions.assertFalse(projectOp.getColumnRefMap().containsKey(dateResult),
                "date(col2) should be filtered out because col2 is not in Union output");
        Assertions.assertTrue(projectOp.getColumnRefMap().containsKey(col1),
                "col1 pass-through should be preserved");

        LogicalUnionOperator childUnion = (LogicalUnionOperator) result.inputAt(0).getOp();
        Assertions.assertNull(childUnion.getProjection(),
                "Union should have no embedded projection after separation");
    }

    /**
     * Test that when ALL projection entries reference unavailable columns,
     * SeparateProjectRule returns the original operator without creating
     * an empty Project.
     */
    @Test
    public void testSeparateProjectAllEntriesInvalid() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, VarcharType.VARCHAR, "k2", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, DateType.DATETIME, "v2", true);
        ColumnRefOperator dateResult = new ColumnRefOperator(3, DateType.DATE, "date_result", true);
        CallOperator dateCall = new CallOperator("date", DateType.DATE, ImmutableList.of(col2));

        ColumnRefOperator child1Col1 = new ColumnRefOperator(4, VarcharType.VARCHAR, "c1_k2", true);

        List<ColumnRefOperator> outputs = Lists.newArrayList(col1);
        List<List<ColumnRefOperator>> childOutputs = Lists.newArrayList();
        childOutputs.add(Lists.newArrayList(child1Col1));

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new HashMap<>();
        projectionMap.put(dateResult, dateCall);

        LogicalUnionOperator unionOp = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(outputs)
                .setChildOutputColumns(childOutputs)
                .isUnionAll(true)
                .setProjection(new Projection(projectionMap))
                .build();

        OptExpression child = new OptExpression(
                new LogicalValuesOperator(ImmutableList.of(child1Col1)));
        OptExpression unionExpr = OptExpression.create(unionOp, child);

        SeparateProjectRule rule = new SeparateProjectRule();
        OptExpression result = rule.rewriteImpl(unionExpr);

        Assertions.assertEquals(OperatorType.LOGICAL_UNION, result.getOp().getOpType());
    }

    /**
     * Test that SeparateProjectRule works correctly for non-SetOperator types
     * (original behavior unchanged).
     */
    @Test
    public void testSeparateProjectNonSetOperator() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "k1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "v1", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new HashMap<>();
        projectionMap.put(col1, col1);
        projectionMap.put(col2, new CallOperator("abs", IntegerType.INT, ImmutableList.of(col2)));

        LogicalValuesOperator valuesOp = new LogicalValuesOperator(ImmutableList.of(col1, col2));
        valuesOp.setProjection(new Projection(projectionMap));

        OptExpression valuesExpr = new OptExpression(valuesOp);

        SeparateProjectRule rule = new SeparateProjectRule();
        OptExpression result = rule.rewriteImpl(valuesExpr);

        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, result.getOp().getOpType());
        LogicalProjectOperator projectOp = (LogicalProjectOperator) result.getOp();
        Assertions.assertEquals(2, projectOp.getColumnRefMap().size());
    }
}
