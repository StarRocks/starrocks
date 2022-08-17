// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

/**
 * Base class for Cascades search task.
 */
public abstract class OptimizerTask {
    protected TaskContext context;

    OptimizerTask(TaskContext context) {
        this.context = context;
    }

    public abstract void execute();

    public void pushTask(OptimizerTask task) {
        context.getOptimizerContext().getTaskScheduler().pushTask(task);
    }

    /**
     * Filter invalid rules by group expression and the rule pattern
     *
     * @param groupExpression the groupExpression will be applied rule
     * @param candidateRules  all candidate rules need to be checked
     * @param validRules      the result valid rules
     */
    void filterInValidRules(GroupExpression groupExpression,
                            List<Rule> candidateRules,
                            List<Rule> validRules) {
        for (Rule rule : candidateRules) {
            if (groupExpression.hasRuleExplored(rule)) {
                continue;
            }

            if (!rule.getPattern().matchWithoutChild(groupExpression)) {
                continue;
            }
            validRules.add(rule);
        }
    }
}
