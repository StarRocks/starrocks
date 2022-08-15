// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;
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
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Arrays;
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
        if (!Utils.containsCorrelationSubquery(input.getGroupExpression())) {
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
            // it's should not be a situation that we currently support.
            throw new SemanticException("Not support none correlation predicate correlation subquery");
        }

        // @Todo: Can be support contain one EQ predicate
        // check correlation filter
        if (!SubqueryUtils.checkAllIsBinaryEQ(correlationPredicate, apply.getCorrelationColumnRefs())) {
            throw new SemanticException("Not support Non-EQ correlation predicate correlation subquery");
        }

        // extract group columns
        Pair<List<ScalarOperator>, Map<ColumnRefOperator, ScalarOperator>> pair =
                SubqueryUtils.rewritePredicateAndExtractColumnRefs(correlationPredicate,
                        apply.getCorrelationColumnRefs(), context);

        // create new trees

        // vector aggregate
        Set<ColumnRefOperator> newGroupingKeys = Sets.newHashSet(pair.second.keySet());
        newGroupingKeys.addAll(aggregate.getGroupingKeys());
        OptExpression vectorAggregationOptExpression = new OptExpression(
                new LogicalAggregationOperator(AggType.GLOBAL, new ArrayList<>(newGroupingKeys),
                        aggregate.getAggregations()));

        // correlation filter
        OptExpression correlationFilterOptExpression =
                new OptExpression(new LogicalFilterOperator(Utils.compoundAnd(pair.first)));
        correlationFilterOptExpression.getInputs().add(vectorAggregationOptExpression);

        OptExpression childOptExpression = vectorAggregationOptExpression;

        // project
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        pair.second.entrySet().stream().filter(d -> !(d.getValue().isColumnRef()))
                .forEach(d -> projectMap.put(d.getKey(), d.getValue()));

        if (!projectMap.isEmpty()) {
            // project add all output
            Arrays.stream(filterOptExpression.getOutputColumns().getColumnIds())
                    .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));

            OptExpression projectOptExpression = new OptExpression(new LogicalProjectOperator(projectMap));

            childOptExpression.getInputs().add(projectOptExpression);
            childOptExpression = projectOptExpression;
        }

        // un-correlation filter
        if (!unCorrelationPredicate.isEmpty()) {
            OptExpression unCorrelationFilterOptExpression =
                    new OptExpression(new LogicalFilterOperator(Utils.compoundAnd(unCorrelationPredicate)));

            childOptExpression.getInputs().add(unCorrelationFilterOptExpression);
            childOptExpression = unCorrelationFilterOptExpression;
        }

        childOptExpression.getInputs().addAll(filterOptExpression.getInputs());

        // apply node
        OptExpression newApplyOptExpression = new OptExpression(
                LogicalApplyOperator.builder().withOperator(apply).setNeedCheckMaxRows(false).build());

        newApplyOptExpression.getInputs().add(input.getInputs().get(0));
        newApplyOptExpression.getInputs().add(correlationFilterOptExpression);

        return Lists.newArrayList(newApplyOptExpression);
    }

}
