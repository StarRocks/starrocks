// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rewrite.physical;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.task.TaskContext;

public interface PhysicalOperatorTreeRewriteRule {
    OptExpression rewrite(OptExpression root, TaskContext taskContext);
}
