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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Set;

/**
 * OuterJoinEliminationRule implements the outer-join elimination optimization.
 *
 * This rule eliminates unnecessary outer joins when the query meets specific conditions:
 * 1. The outer join must be LEFT or RIGHT join
 * 2. All columns referenced in the query outputs must come from the outer side of the join
 * 3. All aggregation functions must be duplicate-insensitive (like MIN, MAX, COUNT(DISTINCT), etc.)
 *
 * Example:
 *
 *   Original query:
 *     SELECT MIN(t1.c1), MAX(t1.c1)
 *     FROM t1
 *     LEFT JOIN t2 ON t1.id = t2.id;
 *
 *   Rewritten to:
 *     SELECT MIN(t1.c1), MAX(t1.c1)
 *     FROM t1;
 *
 * This optimization improves query performance by eliminating unnecessary joins
 * when they don't affect the final query result.
 * Cuts one full-table scan (and the hash-table build) → less CPU, memory, and network.
 * Measurable in real workloads: removing a large dimension table often gives a 1-2× speed-up.
 */
public class OuterJoinEliminationRule extends TransformationRule {
    public static final OuterJoinEliminationRule INSTANCE = new OuterJoinEliminationRule();

    private static final Set<String> DUPLICATE_INSENSITIVE_FUNCTIONS = ImmutableSet.of(
            FunctionSet.MIN, FunctionSet.MAX, FunctionSet.ANY_VALUE, FunctionSet.FIRST_VALUE, FunctionSet.LAST_VALUE,
            FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION_COUNT, FunctionSet.HLL_UNION, FunctionSet.HLL_UNION_AGG,
            FunctionSet.BOOL_OR);

    public static OuterJoinEliminationRule getInstance() {
        return INSTANCE;
    }

    public OuterJoinEliminationRule() {
        super(RuleType.TF_OUTER_JOIN_ELIMINATION, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN, OperatorType.PATTERN_LEAF,
                        OperatorType.PATTERN_LEAF)));
    }

    // checking function is suitable for elimination
    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = (LogicalAggregationOperator) input.getOp();
        LogicalJoinOperator joinOp = (LogicalJoinOperator) input.inputAt(0).getOp();
        JoinOperator joinType = joinOp.getJoinType();

        // rules only LEFT and RIGHT outer joins
        if (!joinType.isLeftOuterJoin() && !joinType.isRightOuterJoin()) {
            return false;
        }

        // if a post-join filter exist, transform cause wrong elimination
        if (joinOp.getPredicate() != null) {
            return false;
        }

        ScalarOperator joinCondition = joinOp.getOnPredicate();
        if (joinCondition != null && !hasOnlySimpleColumnEqualities(joinCondition)) {
            return false;
        }

        for (CallOperator aggFunc : aggOp.getAggregations().values()) {
            if (!isDuplicateInsensitive(aggFunc)) {
                return false;
            }
        }

        // outer side find section
        int outerSideIdx = joinType.isLeftOuterJoin() ? 0 : 1;
        OptExpression outerChild = input.inputAt(0).inputAt(outerSideIdx);
        ColumnRefSet outerColumns = outerChild.getOutputColumns();

        // check if all grouping keys come from the outer side
        for (ColumnRefOperator groupingKey : aggOp.getGroupingKeys()) {
            if (!outerColumns.contains(groupingKey.getId())) {
                return false;
            }
        }

        // check if all aggregation function arguments come from the outer side
        for (CallOperator aggFunc : aggOp.getAggregations().values()) {
            for (ScalarOperator argument : aggFunc.getChildren()) {
                ColumnRefSet usedColumns = argument.getUsedColumns();
                if (!outerColumns.containsAll(usedColumns)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOp = (LogicalAggregationOperator) input.getOp();
        LogicalJoinOperator joinOp = (LogicalJoinOperator) input.inputAt(0).getOp();
        JoinOperator joinType = joinOp.getJoinType();

        int outerSideIdx = joinType.isLeftOuterJoin() ? 0 : 1;
        OptExpression outerChild = input.inputAt(0).inputAt(outerSideIdx);

        return Lists.newArrayList(OptExpression.create(aggOp, outerChild));
    }

    /**
     * check function is duplicate-insensitive.
     */
    private boolean isDuplicateInsensitive(CallOperator aggFunc) {
        String fnName = aggFunc.getFnName().toLowerCase();

        if (DUPLICATE_INSENSITIVE_FUNCTIONS.contains(fnName)) {
            return true;
        }

        //count distinct also duplicate-insensitive
        if (FunctionSet.COUNT.equals(fnName) && aggFunc.isDistinct()) {
            return true;
        }

        return false;
    }

    /**
     * checking if the join condition consists only of simple equality predicates between columns
     * only direct column-to-column comparisons are allowed, e.g.:
     * o.customer_id = c.customer_id allowed
     */
    private boolean hasOnlySimpleColumnEqualities(ScalarOperator joinCondition) {
        if (joinCondition == null) {
            return true;
        }

        List<ScalarOperator> conditions = Utils.extractConjuncts(joinCondition);
        for (ScalarOperator condition : conditions) {
            if (!(condition instanceof BinaryPredicateOperator binary)) {
                return false;
            }

            if (!binary.getBinaryType().equals(BinaryType.EQ)) {
                return false;
            }

            if (!binary.getChild(0).isColumnRef() || !binary.getChild(1).isColumnRef()) {
                return false;
            }
        }
        return true;
    }
}