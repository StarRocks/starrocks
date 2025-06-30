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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;

/**
 * OuterJoinEliminationRule implements the outer-join elimination optimization.
 */
public class OuterJoinEliminationRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(OuterJoinEliminationRule.class);

    public static final OuterJoinEliminationRule INSTANCE = new OuterJoinEliminationRule();

    private static final Set<String> DUPLICATE_INSENSITIVE_FUNCTIONS = ImmutableSet.of(
            "min", "max", "any_value", "first_value", "last_value",
            "bitmap_union", "bitmap_union_count", "hll_union", "hll_union_agg",
            "bool_and", "bool_or", "bit_and", "bit_or", "arbitrary"
    );

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
        try {
            LogicalAggregationOperator aggOp = (LogicalAggregationOperator) input.getOp();
            LogicalJoinOperator joinOp = (LogicalJoinOperator) input.inputAt(0).getOp();
            JoinOperator joinType = joinOp.getJoinType();

            // rules only LEFT and RIGHT outer joins
            if (!joinType.isLeftOuterJoin() && !joinType.isRightOuterJoin()) {
                return false;
            }

            // if a post-join filter exist, transfrom cause wrong elimination
            if (joinOp.getPredicate() != null) {
                return false;
            }

            ScalarOperator joinCondition = joinOp.getOnPredicate();
            if (joinCondition != null && !isStrictForeignKeyEquality(joinCondition)) {
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
        } catch (Exception e) {
            LOG.warn("Failed to check outer join elimination applicability", e);
            return false;
        }
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        try {
            LogicalAggregationOperator aggOp = (LogicalAggregationOperator) input.getOp();
            LogicalJoinOperator joinOp = (LogicalJoinOperator) input.inputAt(0).getOp();
            JoinOperator joinType = joinOp.getJoinType();

            int outerSideIdx = joinType.isLeftOuterJoin() ? 0 : 1;
            OptExpression outerChild = input.inputAt(0).inputAt(outerSideIdx);

            OptExpression result = outerChild;

            LogicalAggregationOperator newAggOp = new LogicalAggregationOperator(
                    aggOp.getType(), aggOp.getGroupingKeys(), aggOp.getAggregations());

            return Lists.newArrayList(OptExpression.create(newAggOp, result));
        } catch (Exception e) {
            LOG.warn("Failed to transform outer join elimination", e);
            return Lists.newArrayList();
        }
    }

    /**
     * check function is duplicate-insensitive.
     */
    private boolean isDuplicateInsensitive(CallOperator aggFunc) {
        String fnName = aggFunc.getFnName().toLowerCase();

        if (DUPLICATE_INSENSITIVE_FUNCTIONS.contains(fnName)) {
            return true;
        }

        //count disticnt also duplicate-insensitive
        if ("count".equals(fnName) && aggFunc.isDistinct()) {
            return true;
        }

        return false;
    }

    /**
     * only strict foreign key equality is allowed.
     * for example
     * o.customer_id = c.customer_id allowed
     */
    private boolean isStrictForeignKeyEquality(ScalarOperator joinCondition) {
        if (joinCondition == null) {
            return true;
        }

        List<ScalarOperator> conditions = Utils.extractConjuncts(joinCondition);
        for (ScalarOperator condition : conditions) {
            if (!(condition instanceof BinaryPredicateOperator)) {
                return false;
            }

            BinaryPredicateOperator binary = (BinaryPredicateOperator) condition;

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