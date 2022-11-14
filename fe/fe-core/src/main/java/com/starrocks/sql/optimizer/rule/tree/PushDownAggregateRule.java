// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.tree.pdagg.PushDownAggregateRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

public class PushDownAggregateRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        PushDownAggregateRewriter rewriter = new PushDownAggregateRewriter(taskContext);
        return rewriter.rewrite(root);
    }
}
