// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;

public interface ScalarOperatorRewriteRule {
    boolean isBottomUp();

    boolean isTopDown();

    boolean isOnlyOnce();

    ScalarOperator apply(ScalarOperator root, ScalarOperatorRewriteContext context);
}
