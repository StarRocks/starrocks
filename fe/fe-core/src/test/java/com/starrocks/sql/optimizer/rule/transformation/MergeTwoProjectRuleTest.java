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

import com.google.common.collect.Maps;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeTwoProjectRuleTest {

    @Test
    public void testMergeAndLimit() {
        // Bottom project: {b -> a, c -> 5}
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ColumnRefOperator c = new ColumnRefOperator(3, IntegerType.INT, "c", true);

        Map<ColumnRefOperator, ScalarOperator> bottomMap = Maps.newHashMap();
        bottomMap.put(b, a);
        bottomMap.put(c, ConstantOperator.createInt(5));
        LogicalProjectOperator bottomProject = new LogicalProjectOperator(bottomMap, 20);

        // Top project: {x -> b, y -> c}
        ColumnRefOperator x = new ColumnRefOperator(4, IntegerType.INT, "x", true);
        ColumnRefOperator y = new ColumnRefOperator(5, IntegerType.INT, "y", true);
        Map<ColumnRefOperator, ScalarOperator> topMap = Maps.newHashMap();
        topMap.put(x, b);
        topMap.put(y, c);
        LogicalProjectOperator topProject = new LogicalProjectOperator(topMap, 10);

        OptExpression top = new OptExpression(topProject);
        top.getInputs().add(OptExpression.create(bottomProject));

        MergeTwoProjectRule rule = new MergeTwoProjectRule();
        List<OptExpression> out = rule.transform(top, OptimizerFactory.mockContext(new ColumnRefFactory()));

        // Validate: the result is a single Project with remapped expressions and min limit (10)
        assertEquals(1, out.size());
        assertEquals(OperatorType.LOGICAL_PROJECT, out.get(0).getOp().getOpType());
        LogicalProjectOperator result = (LogicalProjectOperator) out.get(0).getOp();
        assertEquals(10, result.getLimit());

        Map<ColumnRefOperator, ScalarOperator> resMap = result.getColumnRefMap();
        // x -> a (because b -> a in bottom)
        assertInstanceOf(ColumnRefOperator.class, resMap.get(x));
        assertEquals(a, resMap.get(x));
        // y -> 5 (because c -> 5 in bottom)
        assertInstanceOf(ConstantOperator.class, resMap.get(y));
        assertEquals(5, ((ConstantOperator) resMap.get(y)).getInt());

        // The child of the merged project should be the child of bottom project (no projects below)
        assertEquals(0, out.get(0).getInputs().size());
    }

    @Test
    public void testUnlimitedAndLimit() {
        // Bottom project has unlimited (-1), top has 7 -> result should be 7
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);

        Map<ColumnRefOperator, ScalarOperator> bottomMap = Maps.newHashMap();
        bottomMap.put(b, a);
        LogicalProjectOperator bottomProject = new LogicalProjectOperator(bottomMap, -1);

        ColumnRefOperator x = new ColumnRefOperator(3, IntegerType.INT, "x", true);
        Map<ColumnRefOperator, ScalarOperator> topMap = Maps.newHashMap();
        topMap.put(x, b);
        LogicalProjectOperator topProject = new LogicalProjectOperator(topMap, 7);

        OptExpression top = new OptExpression(topProject);
        top.getInputs().add(OptExpression.create(bottomProject));

        MergeTwoProjectRule rule = new MergeTwoProjectRule();
        List<OptExpression> out = rule.transform(top, OptimizerFactory.mockContext(new ColumnRefFactory()));

        LogicalProjectOperator result = (LogicalProjectOperator) out.get(0).getOp();
        assertEquals(7, result.getLimit());
    }

    @Test
    public void testPreserveAssertTrue() {
        // Bottom project contains an ASSERT_TRUE() call; it should be preserved in the result
        ColumnRefOperator a = new ColumnRefOperator(1, BooleanType.BOOLEAN, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, BooleanType.BOOLEAN, "b", true);

        Map<ColumnRefOperator, ScalarOperator> bottomMap = Maps.newHashMap();
        // Construct a CallOperator with fnName = FunctionSet.ASSERT_TRUE, return type BOOLEAN, and one arg
        CallOperator assertTrue = new CallOperator(
                com.starrocks.catalog.FunctionSet.ASSERT_TRUE,
                BooleanType.BOOLEAN,
                java.util.List.of(a)
        );
        bottomMap.put(b, assertTrue);
        LogicalProjectOperator bottomProject = new LogicalProjectOperator(bottomMap, -1);

        // Top project just references b -> should not remove the ASSERT_TRUE entry for b
        ColumnRefOperator x = new ColumnRefOperator(3, BooleanType.BOOLEAN, "x", true);
        Map<ColumnRefOperator, ScalarOperator> topMap = Maps.newHashMap();
        topMap.put(x, b);
        LogicalProjectOperator topProject = new LogicalProjectOperator(topMap, -1);

        OptExpression top = new OptExpression(topProject);
        top.getInputs().add(OptExpression.create(bottomProject));

        MergeTwoProjectRule rule = new MergeTwoProjectRule();
        List<OptExpression> out = rule.transform(top, OptimizerFactory.mockContext(new ColumnRefFactory()));

        LogicalProjectOperator result = (LogicalProjectOperator) out.get(0).getOp();
        Map<ColumnRefOperator, ScalarOperator> resMap = result.getColumnRefMap();

        // x should be rewritten to ASSERT_TRUE(a) as it references b which maps to ASSERT_TRUE(a)
        assertInstanceOf(CallOperator.class, resMap.get(x));
        assertEquals(com.starrocks.catalog.FunctionSet.ASSERT_TRUE, ((CallOperator) resMap.get(x)).getFnName());

        // And the original b -> ASSERT_TRUE(a) mapping from the lower project must be preserved per rule
        assertInstanceOf(CallOperator.class, resMap.get(b));
        assertEquals(com.starrocks.catalog.FunctionSet.ASSERT_TRUE, ((CallOperator) resMap.get(b)).getFnName());
    }

    @Test
    public void testFoldConstantComparisonIntroducedByMerge() {
        ColumnRefOperator source = new ColumnRefOperator(1, IntegerType.INT, "source", true);
        ColumnRefOperator joinedValue = new ColumnRefOperator(2, VarcharType.VARCHAR, "joined_value", true);
        ColumnRefOperator value = new ColumnRefOperator(3, IntegerType.INT, "value", true);
        ColumnRefOperator output = new ColumnRefOperator(4, BooleanType.BOOLEAN, "output", true);

        Map<ColumnRefOperator, ScalarOperator> bottomMap = Maps.newHashMap();
        bottomMap.put(joinedValue, ConstantOperator.createNull(VarcharType.VARCHAR));
        bottomMap.put(value, source);

        BinaryPredicateOperator joinedValuePredicate = new BinaryPredicateOperator(
                BinaryType.EQ, joinedValue, ConstantOperator.createVarchar("foo"));
        BinaryPredicateOperator valuePredicate = new BinaryPredicateOperator(
                BinaryType.NE, value, ConstantOperator.createInt(0));
        CompoundPredicateOperator topExpression = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, joinedValuePredicate, valuePredicate);

        LogicalProjectOperator bottomProject = new LogicalProjectOperator(bottomMap);
        LogicalProjectOperator topProject = new LogicalProjectOperator(Map.of(output, topExpression));
        OptExpression top = OptExpression.create(topProject, OptExpression.create(bottomProject));

        MergeTwoProjectRule rule = new MergeTwoProjectRule();
        List<OptExpression> result = rule.transform(top, OptimizerFactory.mockContext(new ColumnRefFactory()));

        LogicalProjectOperator mergedProject = (LogicalProjectOperator) result.get(0).getOp();
        CompoundPredicateOperator mergedExpression =
                assertInstanceOf(CompoundPredicateOperator.class, mergedProject.getColumnRefMap().get(output));
        ConstantOperator foldedComparison =
                assertInstanceOf(ConstantOperator.class, mergedExpression.getChild(0));
        assertEquals(BooleanType.BOOLEAN, foldedComparison.getType());
        assertTrue(foldedComparison.isNull());
        BinaryPredicateOperator rewrittenValuePredicate =
                assertInstanceOf(BinaryPredicateOperator.class, mergedExpression.getChild(1));
        assertEquals(source, rewrittenValuePredicate.getChild(0));
    }

    @Test
    public void testNormalizeConstantComparisonIntroducedByMerge() {
        ColumnRefOperator source = new ColumnRefOperator(1, IntegerType.INT, "source", true);
        ColumnRefOperator constantValue = new ColumnRefOperator(2, IntegerType.INT, "constant_value", true);
        ColumnRefOperator inputValue = new ColumnRefOperator(3, IntegerType.INT, "input_value", true);
        ColumnRefOperator output = new ColumnRefOperator(4, BooleanType.BOOLEAN, "output", true);

        LogicalProjectOperator bottomProject = new LogicalProjectOperator(Map.of(
                constantValue, ConstantOperator.createInt(1),
                inputValue, source));
        BinaryPredicateOperator topExpression =
                new BinaryPredicateOperator(BinaryType.EQ, constantValue, inputValue);
        LogicalProjectOperator topProject = new LogicalProjectOperator(Map.of(output, topExpression));
        OptExpression top = OptExpression.create(topProject, OptExpression.create(bottomProject));

        MergeTwoProjectRule rule = new MergeTwoProjectRule();
        List<OptExpression> result = rule.transform(top, OptimizerFactory.mockContext(new ColumnRefFactory()));

        LogicalProjectOperator mergedProject = (LogicalProjectOperator) result.get(0).getOp();
        BinaryPredicateOperator normalizedExpression =
                assertInstanceOf(BinaryPredicateOperator.class, mergedProject.getColumnRefMap().get(output));
        assertEquals(source, normalizedExpression.getChild(0));
        assertEquals(ConstantOperator.createInt(1), normalizedExpression.getChild(1));
    }

    @Test
    public void testSkipConstantFoldWithoutConstantReplacement() {
        ColumnRefOperator source = new ColumnRefOperator(1, IntegerType.INT, "source", true);
        ColumnRefOperator value = new ColumnRefOperator(2, IntegerType.INT, "value", true);
        ColumnRefOperator output = new ColumnRefOperator(3, BooleanType.BOOLEAN, "output", true);

        LogicalProjectOperator bottomProject = new LogicalProjectOperator(Map.of(value, source));
        BinaryPredicateOperator constantPredicate = new BinaryPredicateOperator(
                BinaryType.EQ, ConstantOperator.createInt(1), ConstantOperator.createInt(1));
        BinaryPredicateOperator valuePredicate = new BinaryPredicateOperator(
                BinaryType.NE, value, ConstantOperator.createInt(0));
        CompoundPredicateOperator topExpression = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, constantPredicate, valuePredicate);
        LogicalProjectOperator topProject = new LogicalProjectOperator(Map.of(output, topExpression));
        OptExpression top = OptExpression.create(topProject, OptExpression.create(bottomProject));

        MergeTwoProjectRule rule = new MergeTwoProjectRule();
        List<OptExpression> result = rule.transform(top, OptimizerFactory.mockContext(new ColumnRefFactory()));

        LogicalProjectOperator mergedProject = (LogicalProjectOperator) result.get(0).getOp();
        CompoundPredicateOperator mergedExpression =
                assertInstanceOf(CompoundPredicateOperator.class, mergedProject.getColumnRefMap().get(output));
        assertInstanceOf(BinaryPredicateOperator.class, mergedExpression.getChild(0));
    }
}
