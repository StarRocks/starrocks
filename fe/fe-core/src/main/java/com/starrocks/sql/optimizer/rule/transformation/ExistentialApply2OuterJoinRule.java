// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExistentialApply2OuterJoinRule extends BaseApply2OuterJoinRule {
    public ExistentialApply2OuterJoinRule() {
        super(RuleType.TF_EXISTENTIAL_APPLY_TO_OUTER_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return !apply.isUseSemiAnti() && apply.isExistential()
                && !Utils.containsCorrelationSubquery(input.getGroupExpression());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        // Exists/Not Exists
        ExistsPredicateOperator epo = (ExistsPredicateOperator) apply.getSubqueryOperator();

        // correlation subquery
        if (null != apply.getCorrelationConjuncts()) {
            return transformCorrelation(input, apply, epo, context);
        } else {
            return transformUnCorrelation(input, apply, epo, context);
        }
    }

    private List<OptExpression> transformCorrelation(OptExpression input, LogicalApplyOperator apply,
                                                     ExistsPredicateOperator epo, OptimizerContext context) {
        boolean hasEqPredicate = Utils.extractConjuncts(apply.getCorrelationConjuncts()).stream()
                .anyMatch(d -> OperatorType.BINARY.equals(d.getOpType()) && BinaryPredicateOperator.BinaryType.EQ
                        .equals(((BinaryPredicateOperator) d).getBinaryType()));

        if (hasEqPredicate) {
            return transformCorrelationWithEQ(input, apply, epo, context);
        } else {
            return transformCorrelationWithOther();
        }
    }

    // @Todo: right plan
    // Exists:
    //       PROJECT(Apply-OutputRef, COUNT(Correlation And Predicate) > 0)
    //          |
    //      Aggregate(Group by UniqueKey, COUNT(Correlation And Predicate))
    //          |
    //      Cross-Join
    //       /      \
    //  UniqueKey    \
    //    /           \
    //  LEFT          RIGHT
    //
    // Not Exists:
    //       PROJECT(Apply-OutputRef, COUNT(Correlation And Predicate) = 0)
    //          |
    //      Aggregate(Group by UniqueKey, COUNT(Correlation And Predicate))
    //          |
    //      Cross-Join
    //       /      \
    //  UniqueKey    \
    //    /           \
    //  LEFT          RIGHT
    private List<OptExpression> transformCorrelationWithOther() {
        throw new SemanticException("Not support exists correlation subquery with Non-EQ predicate");
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
    // Not Exists:
    //      Project [..., join-key is null]
    //         |
    //     Left-Outer-Join
    //     /       \
    //  LEFT      AGG(GROUP BY join-key)
    //               \
    //             Project(correlation columns: expression)[optional]
    //                  \
    //                  Filter(UnCorrelation)
    private List<OptExpression> transformCorrelationWithEQ(OptExpression input, LogicalApplyOperator apply,
                                                           ExistsPredicateOperator epo, OptimizerContext context) {
        if (null == apply.getCorrelationConjuncts()) {
            // If the correlation predicate doesn't appear in here,
            // it's should not be a situation that we currently support.
            throw new SemanticException("Not support none correlation predicate correlation subquery");
        }

        List<ScalarOperator> correlationPredicates = Utils.extractConjuncts(apply.getCorrelationConjuncts());
        return transform2OuterJoin(context, input, apply, correlationPredicates, apply.getCorrelationColumnRefs(),
                epo.isNotExists());
    }

    // Exists:
    //      Project [..., COUNT(1) > 0]
    //         |
    //     Cross-Join
    //     /       \
    //  LEFT      AGG(COUNT(1))
    //               \
    //              Limit 1
    //                 \
    //               Filter(UnCorrelation)
    //
    // Not Exists:
    //      Project [..., COUNT(1) = 0]
    //         |
    //     Cross-Join
    //     /       \
    //  LEFT      AGG(COUNT(1))
    //               \
    //              Limit 1
    //                 \
    //               Filter(UnCorrelation)
    private List<OptExpression> transformUnCorrelation(OptExpression input, LogicalApplyOperator apply,
                                                       ExistsPredicateOperator epo, OptimizerContext context) {
        boolean isNotExists = epo.isNotExists();
        // limit
        OptExpression limitExpression = OptExpression.create(new LogicalLimitOperator(1), input.getInputs().get(1));

        // agg count(1)
        CallOperator countOp = SubqueryUtils.createCountRowsOperator();
        ColumnRefOperator count =
                context.getColumnRefFactory().create("COUNT(1)", countOp.getType(), countOp.isNullable());
        Map<ColumnRefOperator, CallOperator> aggregates = Maps.newHashMap();
        aggregates.put(count, countOp);
        OptExpression aggregateExpression =
                OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL, Lists.newArrayList(), aggregates),
                        limitExpression);

        // cross join
        OptExpression joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null));
        joinExpression.getInputs().add(input.getInputs().get(0));
        joinExpression.getInputs().add(aggregateExpression);

        // project
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds())
                .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));
        Arrays.stream(input.getInputs().get(1).getOutputColumns().getColumnIds())
                .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));

        if (isNotExists) {
            projectMap.put(apply.getOutput(), new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, count,
                    ConstantOperator.createBigint(0)));
        } else {
            projectMap.put(apply.getOutput(), new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.GT, count,
                    ConstantOperator.createBigint(0)));
        }
        OptExpression projectExpression = OptExpression.create(new LogicalProjectOperator(projectMap), joinExpression);

        // filter
        if (null != apply.getPredicate()) {
            OptExpression filterExpression = new OptExpression(new LogicalFilterOperator(apply.getPredicate()));
            filterExpression.getInputs().add(input.getInputs().get(1));

            limitExpression.getInputs().clear();
            limitExpression.getInputs().add(filterExpression);
        }

        return Lists.newArrayList(projectExpression);
    }

}
