// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

public class BaseScalarOperatorRewriteRule extends ScalarOperatorVisitor<ScalarOperator, ScalarOperatorRewriteContext>
        implements ScalarOperatorRewriteRule {
    @Override
    public boolean isBottomUp() {
        return false;
    }

    @Override
    public boolean isTopDown() {
        return false;
    }

    @Override
    public ScalarOperator apply(ScalarOperator root, ScalarOperatorRewriteContext context) {
        return root.accept(this, context);
    }

    @Override
    public ScalarOperator visit(ScalarOperator scalarOperator, ScalarOperatorRewriteContext context) {
        return scalarOperator;
    }
}
