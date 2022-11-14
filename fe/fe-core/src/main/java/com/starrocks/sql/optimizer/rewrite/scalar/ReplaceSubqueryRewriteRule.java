// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;

import java.util.Arrays;
import java.util.Map;

/**
 * The processing flow of the subquery is as follows:
 * 1. Transform `Expr` expression to `ScalarOperator` expression, while subquery in simply wrapped in `SubqueryOperator`
 * 2. Parse `SubqueryOperator` to apply operator, and save the `SubqueryOperator -> ColumnRefOperator` in ExpressionMapping
 * 3. rewrite `ScalarOperator` created by step1 with the `ExpressionMapping` created by step2
 * <p>
 * This rule works in step3, simply replace `SubqueryOperator` with `ColumnRefOperator`
 */
public class ReplaceSubqueryRewriteRule extends TopDownScalarOperatorRewriteRule {

    private final Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders;
    private OptExprBuilder builder;

    public ReplaceSubqueryRewriteRule(Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders,
                                      OptExprBuilder builder) {
        this.subqueryPlaceholders = subqueryPlaceholders;
        this.builder = builder;
    }

    public OptExprBuilder getBuilder() {
        return builder;
    }

    @Override
    public ScalarOperator visit(ScalarOperator scalarOperator, ScalarOperatorRewriteContext context) {
        if (subqueryPlaceholders == null) {
            return scalarOperator;
        }
        if (subqueryPlaceholders.containsKey(scalarOperator)) {
            SubqueryOperator subqueryOperator = subqueryPlaceholders.get(scalarOperator);
            LogicalApplyOperator applyOperator = subqueryOperator.getApplyOperator();
            builder = new OptExprBuilder(applyOperator, Arrays.asList(builder, subqueryOperator.getRootBuilder()),
                    builder.getExpressionMapping());
        }
        return scalarOperator;
    }
}
