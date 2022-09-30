// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.SubqueryUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QuantifiedApply2JoinRule extends TransformationRule {
    public QuantifiedApply2JoinRule() {
        super(RuleType.TF_QUANTIFIED_APPLY_TO_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return apply.isUseSemiAnti() && apply.isQuantified()
                && !SubqueryUtils.containsCorrelationSubquery(input);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        // IN/NOT IN
        InPredicateOperator ipo = (InPredicateOperator) apply.getSubqueryOperator();
        BinaryPredicateOperator bpo =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, ipo.getChildren());

        if (ipo.getChildren().stream().anyMatch(ScalarOperator::isConstant)) {
            throw new SemanticException(SubqueryUtils.CONST_QUANTIFIED_COMPARISON);
        }

        // IN to SEMI-JOIN
        // NOT IN to ANTI-JOIN or NULL_AWARE_LEFT_ANTI_JOIN
        OptExpression joinExpression;
        if (ipo.isNotIn()) {
            //@TODO: if will can filter null, use left-anti-join
            joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN,
                    Utils.compoundAnd(bpo, Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate()))));
        } else {
            joinExpression = new OptExpression(new LogicalJoinOperator(JoinOperator.LEFT_SEMI_JOIN,
                    Utils.compoundAnd(bpo, Utils.compoundAnd(apply.getCorrelationConjuncts(), apply.getPredicate()))));
        }

        joinExpression.getInputs().addAll(input.getInputs());

        Map<ColumnRefOperator, ScalarOperator> outputColumns = input.getOutputColumns().getStream().mapToObj(
                id -> context.getColumnRefFactory().getColumnRef(id)
        ).collect(Collectors.toMap(Function.identity(), Function.identity()));
        return Lists.newArrayList(
                OptExpression.create(new LogicalProjectOperator(outputColumns), Lists.newArrayList(joinExpression)));
    }
}
