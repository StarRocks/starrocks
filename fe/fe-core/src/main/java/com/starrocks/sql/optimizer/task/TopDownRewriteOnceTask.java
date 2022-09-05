// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.task;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;

import java.util.List;

/**
 * TopDownRewriteOnceTask will only be rewritten once for each operator,
 * and will not repeat iterations for newly generated operators, and will continue to traverse the children.
 */
public class TopDownRewriteOnceTask extends TopDownRewriteTask {
    public TopDownRewriteOnceTask(TaskContext context, Group group, List<Rule> candidateRules) {
        super(context, group, candidateRules);
    }

    public TopDownRewriteOnceTask(TaskContext context, Group group, Rule rule) {
        this(context, group, Lists.newArrayList(rule));
    }

    public TopDownRewriteOnceTask(TaskContext context, Group group, RuleSetType ruleSetType) {
        super(context, group, context.getOptimizerContext().getRuleSet().
                getRewriteRulesByType(ruleSetType));
    }

    @Override
    public void execute() {
        super.doExecute(true);
    }
}
