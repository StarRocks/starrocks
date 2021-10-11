// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class QuantifiedApply2OuterJoinRule extends BaseApply2OuterJoinRule {
    public QuantifiedApply2OuterJoinRule() {
        super(RuleType.TF_QUANTIFIED_APPLY_TO_OUTER_JOIN,
                Pattern.create(OperatorType.LOGICAL_APPLY, OperatorType.PATTERN_LEAF, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();
        return !apply.isUseSemiAnti() && apply.isQuantified()
                && !Utils.containsCorrelationSubquery(input.getGroupExpression());
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalApplyOperator apply = (LogicalApplyOperator) input.getOp();

        // IN/NOT IN
        InPredicateOperator ipo = (InPredicateOperator) apply.getSubqueryOperator();
        List<ScalarOperator> correlationPredicates = Utils.extractConjuncts(apply.getCorrelationConjuncts());

        // put in predicate
        BinaryPredicateOperator bpo =
                new BinaryPredicateOperator(BinaryPredicateOperator.BinaryType.EQ, ipo.getChildren());
        correlationPredicates.add(bpo);

        List<ColumnRefOperator> correlationColumnRefs = Utils.extractColumnRef(ipo.getChild(0));
        correlationColumnRefs.addAll(apply.getCorrelationColumnRefs());

        return transform2OuterJoin(context, input, apply, correlationPredicates, correlationColumnRefs, ipo.isNotIn());
    }
}
