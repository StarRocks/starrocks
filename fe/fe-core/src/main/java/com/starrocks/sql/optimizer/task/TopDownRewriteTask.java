// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.task;

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

/**
 * TopDownRewriteTask performs a top-down rewrite logic operator pass.
 * <p>
 * This class modify from CMU noisepage project TopDownRewrite class
 */
public abstract class TopDownRewriteTask extends OptimizerTask {
    protected final Group group;
    protected final List<Rule> candidateRules;

    public TopDownRewriteTask(TaskContext context, Group group, List<Rule> ruleSet) {
        super(context);
        this.group = group;
        this.candidateRules = ruleSet;
    }
}
