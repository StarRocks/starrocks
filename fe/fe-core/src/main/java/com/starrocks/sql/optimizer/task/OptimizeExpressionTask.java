// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Comparator;
import java.util.List;

/**
 * If isExplore is true:
 * OptimizeExpressionTask explores a GroupExpression by constructing all logical
 * transformations and applying those rules, and to drive group's statistics.
 * <p>
 * If isExplore is false:
 * OptimizeExpressionTask optimizes a GroupExpression by constructing all logical
 * and physical transformations and applying those rules, and to drive group's statistics.
 * <p>
 * The rules are sorted by promise and applied in that order
 * so a physical transformation rule is applied before a logical transformation rule.
 */
public class OptimizeExpressionTask extends OptimizerTask {
    private final GroupExpression groupExpression;
    private final boolean isExplore;

    OptimizeExpressionTask(TaskContext context, GroupExpression expression) {
        super(context);
        this.groupExpression = expression;
        this.isExplore = false;
    }

    OptimizeExpressionTask(TaskContext context, GroupExpression expression, boolean isExplore) {
        super(context);
        this.groupExpression = expression;
        this.isExplore = isExplore;
    }

    private List<Rule> getValidRules() {
        List<Rule> validRules = Lists.newArrayListWithCapacity(RuleType.NUM_RULES.id());
        List<Rule> logicalRules = context.getOptimizerContext().getRuleSet().getTransformRules();
        filterInValidRules(groupExpression, logicalRules, validRules);

        if (!isExplore) {
            List<Rule> physicalRules = context.getOptimizerContext().getRuleSet().getImplementRules();
            filterInValidRules(groupExpression, physicalRules, validRules);
        }

        validRules.sort(Comparator.comparingInt(Rule::promise));
        return validRules;
    }

    @Override
    public String toString() {
        return "OptimizeExpressionTask" + (this.isExplore ? "[explore]" : "")
                + " for groupExpression " + groupExpression;
    }

    @Override
    public void execute() {
        List<Rule> rules = getValidRules();

        for (Rule rule : rules) {
            pushTask(new ApplyRuleTask(context, groupExpression, rule, isExplore));
        }

        pushTask(new DeriveStatsTask(context, groupExpression));
        for (int i = groupExpression.arity() - 1; i >= 0; i--) {
            pushTask(new ExploreGroupTask(context, groupExpression.getInputs().get(i)));
        }
    }
}
