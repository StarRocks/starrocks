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
import com.starrocks.analysis.BinaryType;
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
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CorrelatedPredicateRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ExistentialApply2OuterJoinRule extends TransformationRule {
    public ExistentialApply2OuterJoinRule() {
        super(RuleType.TF_EXISTENTIAL_APPLY_TO_OUTER_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return !apply.isUseSemiAnti() && apply.isExistential()
                && !SubqueryUtils.containsCorrelationSubquery(input);
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
                .anyMatch(d -> OperatorType.BINARY.equals(d.getOpType()) && BinaryType.EQ
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
            // it should not be a situation that we currently support.
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
        OptExpression limitExpression = OptExpression.create(LogicalLimitOperator.init(1), input.getInputs().get(1));

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
            projectMap.put(apply.getOutput(), new BinaryPredicateOperator(BinaryType.EQ, count,
                    ConstantOperator.createBigint(0)));
        } else {
            projectMap.put(apply.getOutput(), new BinaryPredicateOperator(BinaryType.GT, count,
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
        if (!SubqueryUtils.checkAllIsBinaryEQ(correlationPredicates)) {
            // @Todo:
            // 1. require least a EQ predicate
            // 2. for other binary predicate rewrite rule
            //  a. outer-key < inner key -> outer-key < aggregate MAX(key)
            //  b. outer-key > inner key -> outer-key > aggregate MIN(key)
            throw new SemanticException(SubqueryUtils.EXIST_NON_EQ_PREDICATE);
        }

        // extract join-key
        CorrelatedPredicateRewriter rewriter = new CorrelatedPredicateRewriter(
                correlationColumnRefs, context);
        ScalarOperator newPredicate = SubqueryUtils.rewritePredicateAndExtractColumnRefs(
                Utils.compoundAnd(correlationPredicates), rewriter);

        Map<ColumnRefOperator, ScalarOperator> innerRefMap = rewriter.getColumnRefToExprMap();

        // rootOptExpression
        OptExpression rootOptExpression;

        // aggregate, need add a countRows to indicate the further join match process result.
        Map<ColumnRefOperator, CallOperator> aggregates = Maps.newHashMap();
        CallOperator countRowsCallOp = SubqueryUtils.createCountRowsOperator();
        ColumnRefOperator countRowsCol = context.getColumnRefFactory()
                .create("countRows", countRowsCallOp.getType(), countRowsCallOp.isNullable());
        aggregates.put(countRowsCol, countRowsCallOp);
        LogicalAggregationOperator aggregate =
                new LogicalAggregationOperator(AggType.GLOBAL, Lists.newArrayList(innerRefMap.keySet()),
                        aggregates);

        OptExpression aggregateOptExpression = OptExpression.create(aggregate);
        rootOptExpression = aggregateOptExpression;

        // aggregate project, agg -> project
        if (SubqueryUtils.existNonColumnRef(innerRefMap.values())) {
            // exists expression, need put it in project node
            OptExpression rightChild = input.getInputs().get(1);
            Map<ColumnRefOperator, ScalarOperator> projectMap = SubqueryUtils.generateChildOutColumns(
                    rightChild, innerRefMap, context);
            OptExpression projectOptExpression = OptExpression.create(new LogicalProjectOperator(projectMap));
            rootOptExpression.getInputs().add(projectOptExpression);
            rootOptExpression = projectOptExpression;
        }

        // filter(UnCorrelation) agg -> project -> un-correlation filter
        if (null != apply.getPredicate()) {
            OptExpression filterOptExpression =
                    OptExpression.create(new LogicalFilterOperator(apply.getPredicate()), input.getInputs().get(1));

            rootOptExpression.getInputs().add(filterOptExpression);
            rootOptExpression = filterOptExpression;
        }

        rootOptExpression.getInputs().add(input.getInputs().get(1));

        // left outer join
        LogicalJoinOperator joinOperator =
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, newPredicate);
        OptExpression joinOptExpression =
                OptExpression.create(joinOperator, input.getInputs().get(0), aggregateOptExpression);

        // join project
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        Arrays.stream(input.getInputs().get(0).getOutputColumns().getColumnIds())
                .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));
        Arrays.stream(input.getInputs().get(1).getOutputColumns().getColumnIds())
                .mapToObj(context.getColumnRefFactory()::getColumnRef).forEach(d -> projectMap.put(d, d));

        ScalarOperator nullPredicate = new IsNullPredicateOperator(!isNot, countRowsCol);
        projectMap.put(apply.getOutput(), nullPredicate);
        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);
        OptExpression projectOptExpression = OptExpression.create(projectOperator, joinOptExpression);

        return Lists.newArrayList(projectOptExpression);
    }

}
