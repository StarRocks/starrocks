// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

public class BottomUpRewriteTask extends RewriteTask {
    private boolean isBack;

    protected BottomUpRewriteTask(TaskContext context, Group group, List<Rule> ruleSet, boolean onlyOnce) {
        super(context, group, ruleSet, onlyOnce);
        this.isBack = false;
    }

    @Override
    protected RewriteTask copy() {
        BottomUpRewriteTask task = new BottomUpRewriteTask(context, group, candidateRules, onlyOnce);
        task.isBack = this.isBack;
        return task;
    }

    private BottomUpRewriteTask back() {
        BottomUpRewriteTask task = new BottomUpRewriteTask(context, group, candidateRules, onlyOnce);
        task.isBack = true;
        return task;
    }

    @Override
    public void execute() {
        if (isBack) {
            rewrite();
        } else {
            pushTask(back());
            for (Group childGroup : group.getFirstLogicalExpression().getInputs()) {
                pushTask(new BottomUpRewriteTask(context, childGroup, candidateRules, onlyOnce));
            }
        }
    }

}
