// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.tree.pdagg.PushDownDistinctAggregateRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

public class PushDownDistinctAggregateRule implements TreeRewriteRule {
    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        PushDownDistinctAggregateRewriter rewriter = new PushDownDistinctAggregateRewriter(taskContext);
        return rewriter.rewrite(root);
    }
}
