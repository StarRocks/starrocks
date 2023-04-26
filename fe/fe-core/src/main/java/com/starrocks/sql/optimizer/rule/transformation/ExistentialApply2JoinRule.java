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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

public class ExistentialApply2JoinRule extends TransformationRule {
    public ExistentialApply2JoinRule() {
        super(RuleType.TF_EXISTENTIAL_APPLY_TO_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return apply.isUseSemiAnti() && apply.isExistential()
                && !SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        // Exists/Not Exists
        ExistsPredicateOperator epo = (ExistsPredicateOperator) apply.getSubqueryOperator();

        // correlation subquery
        if (null != apply.getCorrelationConjuncts()) {
            return transformCorrelation(input, apply, epo);
        } else {
            return transformUnCorrelation(input, apply, epo, context);
        }
    }

    private List<OptExpression> transformUnCorrelation(OptExpression input, LogicalApplyOperator apply,
                                                       ExistsPredicateOperator epo, OptimizerContext context) {
        if (epo.isNotExists()) {
            return transformUnCorrelationNotExists(input, apply, context);
        } else {
            return transformUnCorrelationExists(input, apply);
        }
    }

    // Not Exists:
    //        Filter((count = 0) And Predicate)
    //             |
    //         Cross-Join
    //          /      \
    //       LEFT     AGGREGATE(count(1))
    private List<OptExpression> transformUnCorrelationNotExists(OptExpression input, LogicalApplyOperator apply,
                                                                OptimizerContext context) {
        // Aggregate
        Map<ColumnRefOperator, CallOperator> aggregateFunctionMap = Maps.newHashMap();

        CallOperator countOperator = SubqueryUtils.createCountRowsOperator();
        ColumnRefOperator countRef =
                context.getColumnRefFactory().create(countOperator, Type.BIGINT, countOperator.isNullable());
        aggregateFunctionMap.put(countRef, countOperator);

        OptExpression aggExpression = new OptExpression(
                new LogicalAggregationOperator(AggType.GLOBAL, Lists.newArrayList(), aggregateFunctionMap));

        aggExpression.getInputs().add(input.getInputs().get(1));

        // Cross-Join
        OptExpression joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null));
        joinExpression.getInputs().add(input.getInputs().get(0));
        joinExpression.getInputs().add(aggExpression);

        // Filter
        BinaryPredicateOperator countCheck = new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ,
                countRef, ConstantOperator.createBigint(0));
        OptExpression filterExpression =
                new OptExpression(new LogicalFilterOperator(Utils.compoundAnd(countCheck, apply.getPredicate())));
        filterExpression.getInputs().add(joinExpression);

        return Lists.newArrayList(filterExpression);
    }

    // Exists:
    //      Cross-Join
    //       /      \
    //    LEFT     Limit 1
    //                \
    //            Filter(UnCorrelation)
    private List<OptExpression> transformUnCorrelationExists(OptExpression input, LogicalApplyOperator apply) {
        OptExpression limitExpression = new OptExpression(LogicalLimitOperator.init(1));
        limitExpression.getInputs().add(input.getInputs().get(1));

        OptExpression joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null));
        joinExpression.getInputs().add(input.getInputs().get(0));
        joinExpression.getInputs().add(limitExpression);

        if (null != apply.getPredicate()) {
            OptExpression filterExpression = new OptExpression(new LogicalFilterOperator(apply.getPredicate()));
            filterExpression.getInputs().add(input.getInputs().get(1));

            limitExpression.getInputs().clear();
            limitExpression.getInputs().add(filterExpression);
        }

        return Lists.newArrayList(joinExpression);
    }

    private List<OptExpression> transformCorrelation(OptExpression input, LogicalApplyOperator apply,
                                                     ExistsPredicateOperator epo) {
        boolean hasEqPredicate = Utils.extractConjuncts(apply.getCorrelationConjuncts()).stream()
                .anyMatch(d -> OperatorType.BINARY.equals(d.getOpType()) && BinaryPredicateOperator.BinaryType.EQ
                        .equals(((BinaryPredicateOperator) d).getBinaryType()));

        if (hasEqPredicate) {
            return transformCorrelationWithEQ(input, apply, epo);
        } else {
            return transformCorrelationWithOther();
        }
    }

    // EQ conjuncts:
    // Exists to SEMI-JOIN
    // Not Exists to ANTI-JOIN
    private List<OptExpression> transformCorrelationWithEQ(OptExpression input, LogicalApplyOperator apply,
                                                           ExistsPredicateOperator epo) {
        OptExpression joinExpression;
        if (epo.isNotExists()) {
            joinExpression = new OptExpression(
                    new LogicalJoinOperator(JoinOperator.LEFT_ANTI_JOIN,
                            Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate())));
        } else {
            joinExpression = new OptExpression(
                    new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN,
                            Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate())));
        }

        joinExpression.getInputs().addAll(input.getInputs());
        return Lists.newArrayList(joinExpression);
    }

    // @todo: right plan
    // Exists:
    //      Aggregate(Group by UniqueKey)
    //          |
    //   Filter(Correlation And Predicate)
    //          |
    //      Cross-Join
    //       /      \
    //  UniqueKey    \
    //    /           \
    //  LEFT          RIGHT
    //
    // Not Exists:
    //   Aggregate(Group by UniqueKey)
    //          |
    //   Filter(Not (Correlation And Predicate))
    //          |
    //      Cross-Join
    //       /      \
    //  UniqueKey    \
    //    /           \
    //  LEFT          RIGHT
    private List<OptExpression> transformCorrelationWithOther() {
        throw new SemanticException("Not support exists correlation subquery with Non-EQ predicate");
    }

}
