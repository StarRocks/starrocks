// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;

/**
 * The processing flow of the subquery is as follows:
 * 1. Transform `Expr` expression to `ScalarOperator` expression, while subquery in simply wrapped in `SubqueryOperator`
 * 2. Parse `SubqueryOperator` to apply operator, and save the `SubqueryOperator -> ColumnRefOperator` in ExpressionMapping
 * 3. rewrite `ScalarOperator` created by step1 with the `ExpressionMapping` created by step2
 * <p>
 * This rule works in step3, simply replace `SubqueryOperator` with `ColumnRefOperator`
 */
public class ReplaceSubqueryRewriteRule extends TopDownScalarOperatorRewriteRule {

    private final ExpressionMapping expressionMapping;

    public ReplaceSubqueryRewriteRule(ExpressionMapping expressionMapping) {
        this.expressionMapping = expressionMapping;
    }

    @Override
    public ScalarOperator visitSubqueryOperator(SubqueryOperator operator, ScalarOperatorRewriteContext context) {
        Preconditions.checkState(expressionMapping.hasScalarOperator(operator));
        return expressionMapping.get(operator);
    }

    @Override
    public ScalarOperator visitInPredicate(InPredicateOperator predicate, ScalarOperatorRewriteContext context) {
        if (!(predicate.getChild(1) instanceof SubqueryOperator)) {
            return predicate;
        }
        Preconditions.checkState(expressionMapping.hasScalarOperator(predicate));
        return expressionMapping.get(predicate);
    }

    @Override
    public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate,
                                               ScalarOperatorRewriteContext context) {
        Preconditions.checkState(expressionMapping.hasScalarOperator(predicate));
        return expressionMapping.get(predicate);
    }
}
