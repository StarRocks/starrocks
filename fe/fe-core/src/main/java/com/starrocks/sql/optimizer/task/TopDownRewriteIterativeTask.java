// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.task;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;

import java.util.List;

/**
 * TopDownIterativeTask will rewrite every operator iteration,
 * and iterate for new operators until there is no matching pattern
 */
public class TopDownRewriteIterativeTask extends TopDownRewriteTask {
    public TopDownRewriteIterativeTask(TaskContext context, Group group, List<Rule> ruleSet) {
        super(context, group, ruleSet);
    }

    public TopDownRewriteIterativeTask(TaskContext context, Group group, Rule rule) {
        this(context, group, Lists.newArrayList(rule));
    }

    public TopDownRewriteIterativeTask(TaskContext context, Group group, RuleSetType ruleSetType) {
        this(context, group, context.getOptimizerContext().getRuleSet().
                getRewriteRulesByType(ruleSetType));
    }

    @Override
    public void execute() {
        super.doExecute(false);
    }
}
