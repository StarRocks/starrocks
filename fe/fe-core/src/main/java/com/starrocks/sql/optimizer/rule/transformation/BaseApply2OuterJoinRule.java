// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.common.Pair;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseApply2OuterJoinRule extends TransformationRule {
    protected BaseApply2OuterJoinRule(RuleType type,
                                      Pattern pattern) {
        super(type, pattern);
    }

    // Exists:
    //      Project [..., join-key is not null]
    //         |
    //     Left-Outer-Join (left table always output all rows)
    //     /       \
    //  LEFT      AGG(GROUP BY join-key)
    //               \
    //            Project(correlation columns: expression)[optional]
    //                 \
    //                 Filter(UnCorrelation)
    //
    // Not exists:
    //      Project [..., join-key is null]
    //         |
    //     Left-Outer-Join
    //     /       \
    //  LEFT      AGG(GROUP BY join-key)
    //               \
    //             Project(correlation columns: expression)[optional]
    //                  \
    //                  Filter(UnCorrelation)
    public List<OptExpression> transform2OuterJoin(OptimizerContext context, OptExpression input,
                                                   LogicalApplyOperator apply,
                                                   List<ScalarOperator> correlationPredicates,
                                                   List<ColumnRefOperator> correlationColumnRefs,
                                                   boolean isNot) {
        // check correlation filter
        if (!SubqueryUtils.checkAllIsBinaryEQ(correlationPredicates, correlationColumnRefs)) {
            // @Todo:
            // 1. require least a EQ predicate
            // 2. for other binary predicate rewrite rule
            //  a. outer-key < inner key -> outer-key < aggregate MAX(key)
            //  b. outer-key > inner key -> outer-key > aggregate MIN(key)
            throw new SemanticException("Not support Non-EQ correlation predicate correlation subquery");
        }

        // extract join-key
        Pair<List<ScalarOperator>, Map<ColumnRefOperator, ScalarOperator>> pair =
                SubqueryUtils.rewritePredicateAndExtractColumnRefs(correlationPredicates,
                        correlationColumnRefs, context);

        // rootOptExpression
        OptExpression rootOptExpression;

        // aggregate
        LogicalAggregationOperator aggregate =
                new LogicalAggregationOperator(AggType.GLOBAL, new ArrayList<>(pair.second.keySet()), Maps.newHashMap());

        OptExpression aggregateOptExpression = OptExpression.create(aggregate);
        rootOptExpression = aggregateOptExpression;

        // aggregate project
        if (pair.second.entrySet().stream().anyMatch(d -> !(d.getValue().isColumnRef()))) {
            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
            pair.second.entrySet().stream().filter(d -> !(d.getValue().isColumnRef()))
                    .forEach(d -> projectMap.put(d.getKey(), d.getValue()));

            // project add all output
            Arrays.stream(input.getInputs().get(1).getOutputColumns().getColumnIds())
                    .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));

            OptExpression projectOptExpression = OptExpression.create(new LogicalProjectOperator(projectMap));
            rootOptExpression.getInputs().add(projectOptExpression);
            rootOptExpression = projectOptExpression;
        }

        // filter(UnCorrelation)
        if (null != apply.getPredicate()) {
            OptExpression filterOptExpression =
                    OptExpression.create(new LogicalFilterOperator(apply.getPredicate()), input.getInputs().get(1));

            rootOptExpression.getInputs().add(filterOptExpression);
            rootOptExpression = filterOptExpression;
        }

        rootOptExpression.getInputs().add(input.getInputs().get(1));

        // left outer join
        LogicalJoinOperator joinOperator =
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, Utils.compoundAnd(pair.first));
        OptExpression joinOptExpression =
                OptExpression.create(joinOperator, input.getInputs().get(0), aggregateOptExpression);

        // join project
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds())
                .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));
        Arrays.stream(input.getInputs().get(1).getOutputColumns().getColumnIds())
                .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));

        ScalarOperator nullPredicate = Utils.compoundAnd(
                pair.second.keySet().stream().map(d -> new IsNullPredicateOperator(!isNot, d))
                        .collect(Collectors.toList()));
        projectMap.put(apply.getOutput(), nullPredicate);
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
        OptExpression projectOptExpression = OptExpression.create(projectOperator, joinOptExpression);

        return Lists.newArrayList(projectOptExpression);
    }

}
