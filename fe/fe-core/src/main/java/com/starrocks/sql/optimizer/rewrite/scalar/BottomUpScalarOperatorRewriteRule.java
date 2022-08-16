// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.scalar;

public class BottomUpScalarOperatorRewriteRule extends BaseScalarOperatorRewriteRule {
    @Override
    public boolean isBottomUp() {
        return true;
    }
}
