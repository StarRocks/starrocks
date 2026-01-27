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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RewriteDuplicateAggregateFnRuleTest {

    /**
     * Test case 1: Semantic equivalence with AND predicates in different order
     * 
     * This is the core issue from the bug report:
     * ds_hll_count_distinct(IF(province > 5 AND id > 5, age, NULL), 21) AS d1
     * ds_hll_count_distinct(IF(id > 5 AND province > 5, age, NULL), 21) AS d2
     * 
     * Expected: d1 and d2 should be recognized as duplicates, only one aggregate is computed
     */
    @Test
    public void testSemanticEquivalenceWithAndPredicate() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "v1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "v2", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "v3", true);
        ColumnRefOperator col4 = new ColumnRefOperator(4, IntegerType.INT, "v4", true);
        
        CompoundPredicateOperator and1 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                createGreaterThan(col1, 5),
                createGreaterThan(col2, 5)
        );
        
        CompoundPredicateOperator and2 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                createGreaterThan(col2, 5),
                createGreaterThan(col1, 5)
        );
        
        CallOperator if1 = createIfOperator(and1, col3);
        CallOperator if2 = createIfOperator(and2, col3);
        
        CallOperator agg1 = createDsHllCountDistinct(if1);
        CallOperator agg2 = createDsHllCountDistinct(if2);
        
        assertTrue(agg1.equivalent(agg2), 
                "Aggregations with AND predicates in different order should be semantically equivalent");
        
        ColumnRefOperator aggCol1 = new ColumnRefOperator(10, IntegerType.BIGINT, "d1", true);
        ColumnRefOperator aggCol2 = new ColumnRefOperator(11, IntegerType.BIGINT, "d2", true);
        
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(aggCol1, agg1);
        aggMap.put(aggCol2, agg2);
        
        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(
                AggType.GLOBAL,
                Lists.newArrayList(col4),
                aggMap
        );
        
        RewriteDuplicateAggregateFnRule rule = new RewriteDuplicateAggregateFnRule();
        OptExpression aggExpr = new OptExpression(aggOp);
        
        // Check if the rule detects duplicates
        assertTrue(rule.check(aggExpr, OptimizerFactory.mockContext(new ColumnRefFactory())),
                "Rule should detect semantic duplicates");
    }

    /**
     * Test case 2: Semantic equivalence with OR predicates in different order
     */
    @Test
    public void testSemanticEquivalenceWithOrPredicate() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "v1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "v2", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "v3", true);
        ColumnRefOperator col4 = new ColumnRefOperator(4, IntegerType.INT, "v4", true);
        
        CompoundPredicateOperator or1 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR,
                createGreaterThan(col1, 5),
                createGreaterThan(col2, 5)
        );
        
        CompoundPredicateOperator or2 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR,
                createGreaterThan(col2, 5),
                createGreaterThan(col1, 5)
        );
        
        CallOperator if1 = createIfOperator(or1, col3);
        CallOperator if2 = createIfOperator(or2, col3);
        
        CallOperator agg1 = createDsHllCountDistinct(if1);
        CallOperator agg2 = createDsHllCountDistinct(if2);
        
        // Verify they are semantically equivalent
        assertTrue(agg1.equivalent(agg2),
                "Aggregations with OR predicates in different order should be semantically equivalent");
    }

    /**
     * Test case 3: Exact duplicates (backward compatibility)
     */
    @Test
    public void testExactDuplicateAggregations() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "v1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "v2", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "v3", true);
        ColumnRefOperator col4 = new ColumnRefOperator(4, IntegerType.INT, "v4", true);
        
        CompoundPredicateOperator and1 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                createGreaterThan(col1, 5),
                createGreaterThan(col2, 5)
        );
        
        CompoundPredicateOperator and2 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                createGreaterThan(col1, 5),
                createGreaterThan(col2, 5)
        );
        
        CallOperator if1 = createIfOperator(and1, col3);
        CallOperator if2 = createIfOperator(and2, col3);
        
        CallOperator agg1 = createDsHllCountDistinct(if1);
        CallOperator agg2 = createDsHllCountDistinct(if2);
        
        assertEquals(agg1, agg2, "Exact duplicate aggregations should be equal");
    }

    /**
     * Test case 4: Non-duplicate aggregations (different input columns)
     */
    @Test
    public void testNonDuplicateAggregations() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "v1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "v2", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "v3", true);
        ColumnRefOperator col4 = new ColumnRefOperator(4, IntegerType.INT, "v4", true);
        
        CompoundPredicateOperator and1 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                createGreaterThan(col1, 5),
                createGreaterThan(col2, 5)
        );
        
        CallOperator if1 = createIfOperator(and1, col3);
        CallOperator if2 = createIfOperator(and1, col4);  // Different input column
        
        CallOperator agg1 = createDsHllCountDistinct(if1);
        CallOperator agg2 = createDsHllCountDistinct(if2);
        
        // Verify they are NOT semantically equivalent
        assertTrue(!agg1.equivalent(agg2),
                "Aggregations with different input columns should not be semantically equivalent");
    }

    /**
     * Test case 5: Transform with semantic duplicates
     */
    @Test
    public void testTransformWithSemanticDuplicates() {
        ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "v1", true);
        ColumnRefOperator col2 = new ColumnRefOperator(2, IntegerType.INT, "v2", true);
        ColumnRefOperator col3 = new ColumnRefOperator(3, IntegerType.INT, "v3", true);
        ColumnRefOperator col4 = new ColumnRefOperator(4, IntegerType.INT, "v4", true);
        
        CompoundPredicateOperator and1 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                createGreaterThan(col1, 5),
                createGreaterThan(col2, 5)
        );
        
        CompoundPredicateOperator and2 = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND,
                createGreaterThan(col2, 5),
                createGreaterThan(col1, 5)
        );
        
        CallOperator if1 = createIfOperator(and1, col3);
        CallOperator if2 = createIfOperator(and2, col3);
        
        CallOperator agg1 = createDsHllCountDistinct(if1);
        CallOperator agg2 = createDsHllCountDistinct(if2);
        
        ColumnRefOperator aggCol1 = new ColumnRefOperator(10, IntegerType.BIGINT, "d1", true);
        ColumnRefOperator aggCol2 = new ColumnRefOperator(11, IntegerType.BIGINT, "d2", true);
        
        Map<ColumnRefOperator, CallOperator> aggMap = Maps.newHashMap();
        aggMap.put(aggCol1, agg1);
        aggMap.put(aggCol2, agg2);
        
        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(
                AggType.GLOBAL,
                Lists.newArrayList(col4),
                aggMap
        );
        
        OptExpression aggExpr = new OptExpression(aggOp);
        
        RewriteDuplicateAggregateFnRule rule = new RewriteDuplicateAggregateFnRule();
        List<OptExpression> result = rule.transform(aggExpr, OptimizerFactory.mockContext(new ColumnRefFactory()));
        
        assertEquals(1, result.size(), "Should return one transformed expression");
        
        OptExpression transformed = result.get(0);
        assertEquals(OperatorType.LOGICAL_PROJECT, transformed.getOp().getOpType(),
                "Top operator should be PROJECT");
        
        OptExpression aggChild = transformed.getInputs().get(0);
        assertEquals(OperatorType.LOGICAL_AGGR, aggChild.getOp().getOpType(),
                "Child operator should be AGGREGATE");
        
        LogicalAggregationOperator newAggOp = (LogicalAggregationOperator) aggChild.getOp();
        assertEquals(1, newAggOp.getAggregations().size(),
                "Should have only one aggregation after deduplication");
    }

    private ScalarOperator createGreaterThan(ColumnRefOperator col, int value) {
        return new BinaryPredicateOperator(BinaryType.GT, col, ConstantOperator.createInt(value));
    }

    private CallOperator createIfOperator(ScalarOperator condition, ColumnRefOperator value) {
        return new CallOperator("if", IntegerType.INT,
                Lists.newArrayList(condition, value, ConstantOperator.createNull(IntegerType.INT)));
    }

    private CallOperator createDsHllCountDistinct(CallOperator ifExpr) {
        return new CallOperator("ds_hll_count_distinct", IntegerType.BIGINT,
                Lists.newArrayList(ifExpr, ConstantOperator.createInt(21)));
    }
}
