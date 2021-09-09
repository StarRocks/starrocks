// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Binder;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/**
 * TopDownRewriteTask performs a top-down rewrite logic operator pass.
 * <p>
 * This class modify from CMU noisepage project TopDownRewrite class
 */
public class TopDownRewriteTask extends OptimizerTask {
    private final Group group;
    private final RuleSetType ruleSetType;

    public TopDownRewriteTask(TaskContext context, Group group, RuleSetType ruleSetType) {
        super(context);
        this.group = group;
        this.ruleSetType = ruleSetType;
    }

    @Override
    public void execute() {
        List<Rule> validRules = Lists.newArrayListWithCapacity(RuleType.NUM_RULES.id());
        List<Rule> candidateRules = context.getOptimizerContext().getRuleSet().
                getRewriteRulesByType(ruleSetType);

        Preconditions.checkState(group.isValidInitState());

        GroupExpression curGroupExpression = group.getLogicalExpressions().get(0);
        filterInValidRules(curGroupExpression, candidateRules, validRules);

        for (Rule rule : validRules) {
            // Apply rule and get all new OptExpressions
            Pattern pattern = rule.getPattern();
            Binder binder = new Binder(pattern, curGroupExpression);
            OptExpression extractExpr = binder.next();
            List<OptExpression> newExpressions = Lists.newArrayList();
            while (extractExpr != null) {
                if (!rule.check(extractExpr, context.getOptimizerContext())) {
                    extractExpr = binder.next();
                    continue;
                }
                newExpressions.addAll(rule.transform(extractExpr, context.getOptimizerContext()));
                Preconditions.checkState(newExpressions.size() <= 1,
                        "Rewrite rule should provide at most 1 expression");
                if (!newExpressions.isEmpty()) {
                    context.getOptimizerContext().getMemo().replaceRewriteExpression(
                            group, newExpressions.get(0));
                    // This group has been merged
                    if (group.getLogicalExpressions().isEmpty()) {
                        return;
                    }
                    pushTask(new TopDownRewriteTask(context, group, ruleSetType));
                    return;
                }
                extractExpr = binder.next();
            }
        }

        for (Group childGroup : curGroupExpression.getInputs()) {
            pushTask(new TopDownRewriteTask(context, childGroup, ruleSetType));
        }
    }
}
