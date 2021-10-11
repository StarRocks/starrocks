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
 * TopDownRewriteOnceTask will only be rewritten once for each operator,
 * and will not repeat iterations for newly generated operators, and will continue to traverse the children.
 */
public class TopDownRewriteOnceTask extends TopDownRewriteTask {
    public TopDownRewriteOnceTask(TaskContext context, Group group, List<Rule> ruleSet) {
        super(context, group, ruleSet);
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
        List<Rule> validRules = Lists.newArrayListWithCapacity(RuleType.NUM_RULES.id());
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
                        continue;
                    }

                    curGroupExpression = group.getFirstLogicalExpression();
                }
                extractExpr = binder.next();
            }
        }

        for (Group childGroup : group.getFirstLogicalExpression().getInputs()) {
            pushTask(new TopDownRewriteOnceTask(context, childGroup, candidateRules));
        }
    }
}
