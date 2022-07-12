// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

/**
 * TopDownRewriteTask performs a top-down rewrite logic operator pass.
 * <p>
 * This class modify from CMU noisepage project TopDownRewrite class
 */
public class TopDownRewriteTask extends RewriteTask {
    protected TopDownRewriteTask(TaskContext context, Group group, List<Rule> ruleSet, boolean onlyOnce) {
        super(context, group, ruleSet, onlyOnce);
    }

    @Override
    protected RewriteTask copy() {
        return new TopDownRewriteTask(context, group, candidateRules, onlyOnce);
    }

    @Override
    public void execute() {
        if (rewrite()) {
            for (Group childGroup : group.getFirstLogicalExpression().getInputs()) {
                pushTask(new TopDownRewriteTask(context, childGroup, candidateRules, onlyOnce));
            }
        }
    }
}
