// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.tree.pdagg.PushDownAggregateRewriter;
import com.starrocks.sql.optimizer.rule.tree.pdaggb4mv.PushDownAggregateBeforeMVRewriter;
import com.starrocks.sql.optimizer.task.TaskContext;

public class PushDownAggregateBeforeMVRule implements TreeRewriteRule {


    private final PushDownAggregateBeforeMVRewriter rewriter;

    public PushDownAggregateBeforeMVRule(TaskContext taskContext) {
        this.rewriter = new PushDownAggregateBeforeMVRewriter(taskContext);
    }

    public PushDownAggregateBeforeMVRewriter getRewriter() {
        return rewriter;
    }

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        return rewriter.rewrite(root);
    }
}
