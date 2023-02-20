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
import com.google.common.collect.Sets;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CorrelatedPredicateRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Push down ApplyNode and AggregationNode as a whole
 * Pattern:
 * ApplyNode
 * /      \
 * LEFT     Aggregation(scalar aggregation)
 * \
 * Filter
 * \
 * ....
 * Before:
 * ApplyNode
 * /      \
 * LEFT     Aggregate(scalar aggregation)
 * \
 * Filter
 * \
 * ....
 * After:
 * ApplyNode
 * /      \
 * LEFT      Filter(correlation)
 * \
 * Aggregate(vector aggregation, group by correlation columns)
 * \
 * Project(correlation columns: expression)[optional]
 * \
 * Filter(un-correlation)[optional]
 * \
 * ....
 * Requirements:
 * 1. All predicate is Binary.EQ in correlation filter
 */
public class PushDownApplyAggFilterRule extends TransformationRule {
    public PushDownApplyAggFilterRule() {
        super(RuleType.TF_PUSH_DOWN_APPLY_AGG, Pattern.create(OperatorType.LOGICAL_APPLY).addChildren(
                Pattern.create(OperatorType.PATTERN_LEAF),
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(
                        Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        // must be correlation subquery
        if (!SubqueryUtils.containsCorrelationSubquery(input)) {
            return false;
        }

        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getInputs().get(1).getOp();

        // Don't support
        // 1. More grouping column and not Exists subquery
        // 2. Distinct aggregation(same with group by xxx)
        return aggregate.getGroupingKeys().isEmpty() || ((LogicalApplyOperator) input.getOp()).isExistential();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        LogicalAggregationOperator aggregate = (LogicalAggregationOperator) input.getInputs().get(1).getOp();

        LogicalFilterOperator filter = (LogicalFilterOperator) input.getInputs().get(1).getInputs().get(0).getOp();
        OptExpression filterOptExpression = input.getInputs().get(1).getInputs().get(0);

        List<ScalarOperator> predicates = Utils.extractConjuncts(filter.getPredicate());
        List<ScalarOperator> correlationPredicate = Lists.newArrayList();
        List<ScalarOperator> unCorrelationPredicate = Lists.newArrayList();

        for (ScalarOperator scalarOperator : predicates) {
            if (Utils.containAnyColumnRefs(apply.getCorrelationColumnRefs(), scalarOperator)) {
                correlationPredicate.add(scalarOperator);
            } else {
                unCorrelationPredicate.add(scalarOperator);
            }
        }

        if (correlationPredicate.isEmpty()) {
            // If the correlation predicate doesn't appear in here,
            // it should not be a situation that we currently support.
            throw new SemanticException(SubqueryUtils.NOT_FOUND_CORRELATED_PREDICATE);
        }

        // @Todo: Can be support contain one EQ predicate
        // check correlation filter
        if (!SubqueryUtils.checkAllIsBinaryEQ(correlationPredicate)) {
            throw new SemanticException(SubqueryUtils.EXIST_NON_EQ_PREDICATE);
        }

        // extract group columns
        CorrelatedPredicateRewriter rewriter = new CorrelatedPredicateRewriter(
                apply.getCorrelationColumnRefs(), context);

        ScalarOperator newPredicate = SubqueryUtils.rewritePredicateAndExtractColumnRefs(
                Utils.compoundAnd(correlationPredicate), rewriter);

        Map<ColumnRefOperator, ScalarOperator> innerRefMap = rewriter.getColumnRefToExprMap();
        // create new trees

        // vector aggregate
        Set<ColumnRefOperator> newGroupingKeys = Sets.newHashSet(innerRefMap.keySet());
        newGroupingKeys.addAll(aggregate.getGroupingKeys());
        OptExpression vectorAggregationOptExpression = new OptExpression(
                new LogicalAggregationOperator(AggType.GLOBAL, new ArrayList<>(newGroupingKeys),
                        aggregate.getAggregations()));

        // correlation filter. correlation filter -> agg
        OptExpression correlationFilterOptExpression =
                new OptExpression(new LogicalFilterOperator(newPredicate));
        correlationFilterOptExpression.getInputs().add(vectorAggregationOptExpression);

        OptExpression childOptExpression = vectorAggregationOptExpression;

        // exists expression, add project node. agg -> project
        if (SubqueryUtils.existNonColumnRef(innerRefMap.values())) {
            Map<ColumnRefOperator, ScalarOperator> projectMap = SubqueryUtils.generateChildOutColumns(
                    filterOptExpression, innerRefMap, context);
            OptExpression projectOptExpression = new OptExpression(new LogicalProjectOperator(projectMap));
            childOptExpression.getInputs().add(projectOptExpression);
            childOptExpression = projectOptExpression;
        }

        // add un-correlation filter, agg -> project -> un-correlation filter
        if (!unCorrelationPredicate.isEmpty()) {
            OptExpression unCorrelationFilterOptExpression =
                    new OptExpression(new LogicalFilterOperator(Utils.compoundAnd(unCorrelationPredicate)));

            childOptExpression.getInputs().add(unCorrelationFilterOptExpression);
            childOptExpression = unCorrelationFilterOptExpression;
        }

        // add un-correlation filter, agg -> project -> un-correlation filter -> inputs
        childOptExpression.getInputs().addAll(filterOptExpression.getInputs());

        // apply node
        OptExpression newApplyOptExpression = new OptExpression(
                LogicalApplyOperator.builder().withOperator(apply).setNeedCheckMaxRows(false).build());

        newApplyOptExpression.getInputs().add(input.getInputs().get(0));
        newApplyOptExpression.getInputs().add(correlationFilterOptExpression);

        return Lists.newArrayList(newApplyOptExpression);
    }

}
