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

// Push down ApplyNode and AggregationNode as a whole
// Pattern:
//      ApplyNode
//      /      \
//  LEFT     Aggregation(scalar aggregation)
//               \
//               Filter
//                 \
//                 ....
//
// Before:
//      ApplyNode
//      /      \
//  LEFT     Aggregate(scalar aggregation)
//               \
//               Filter
//                 \
//                 ....
//
// After:
//      ApplyNode
//      /      \
//  LEFT      Filter(correlation)
//               \
//            Aggregate(vector aggregation, group by correlation columns)
//                 \
//              Project(correlation columns: expression)[optional]
//                   \
//               Filter(un-correlation)[optional]
//                     \
//                      ....
//
// Requirements:
// 1. All predicate is Binary.EQ in correlation filter
//
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

        // check correlation filter
        if (!SubqueryUtils.checkAllIsBinaryEQ(correlationPredicate)) {
            return transformNonEQCorrelation(input, apply, aggregate, filterOptExpression.inputAt(0),
                    correlationPredicate, unCorrelationPredicate, context);
        }

        // extract group columns
        CorrelatedPredicateRewriter rewriter = new CorrelatedPredicateRewriter(
                apply.getCorrelationColumnRefs(), context);

        ScalarOperator newPredicate = rewriter.rewrite(Utils.compoundAnd(correlationPredicate));
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

    /*
     * The correlation predicate is not a conjunction of equalities, so the inner rows can't be grouped by a join key.
     * Group them by the distinct outer correlation columns instead, see NonEqCorrelationApplyRewriter:
     *
     *   SELECT t0.v1, (SELECT count(*) FROM t1 WHERE t1.v4 = t0.v1 AND t1.v5 < t0.v2) FROM t0
     * =>
     *   with cte as (select * from t0)
     *   select cte.*, ifnull(r.cnt, 0)
     *   from cte left outer join (
     *       select d.v1, d.v2, count(*) cnt
     *       from (select distinct v1, v2 from cte) d inner join t1 on t1.v4 = d.v1 and t1.v5 < d.v2
     *       group by d.v1, d.v2) r
     *     on cte.v1 <=> r.v1 and cte.v2 <=> r.v2
     */
    private List<OptExpression> transformNonEQCorrelation(OptExpression input, LogicalApplyOperator apply,
                                                          LogicalAggregationOperator aggregate, OptExpression inner,
                                                          List<ScalarOperator> correlationPredicate,
                                                          List<ScalarOperator> unCorrelationPredicate,
                                                          OptimizerContext context) {
        if (!apply.isScalar() || !aggregate.getGroupingKeys().isEmpty() || aggregate.getPredicate() != null
                || aggregate.getProjection() != null || apply.getCorrelationConjuncts() != null
                || apply.getPredicate() != null || apply.getSubqueryOperator() == null
                || SubqueryUtils.containsColumnRef(apply.getCorrelationColumnRefs(), inner)) {
            throw new SemanticException(SubqueryUtils.EXIST_NON_EQ_PREDICATE);
        }

        // the scalar aggregation returns a row even when no inner row qualifies, but the outer join returns NULL
        ScalarOperator applyOutput =
                ScalarApplyNormalizeCountRule.normalizeCount(apply.getSubqueryOperator(), aggregate.getAggregations());

        OptExpression result = new NonEqCorrelationApplyRewriter(input, context).rewrite(inner,
                Utils.compoundAnd(correlationPredicate), Utils.compoundAnd(unCorrelationPredicate),
                aggregate.getAggregations(), applyOutput, Maps.newHashMap());
        return Lists.newArrayList(result);
    }
}
