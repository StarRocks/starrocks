// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;

public interface PhysicalOperatorTreeRewriteRule {
    OptExpression rewrite(OptExpression root, ColumnRefFactory factory);
}
