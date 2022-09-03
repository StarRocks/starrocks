// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerTraceInfo;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Binder;
import com.starrocks.sql.optimizer.rule.Rule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * ApplyRuleTask firstly applies a rule, then
 * <p>
 * If the rule is transformation rule and isExplore is true:
 * We need to explore (apply logical rules)
 * <p>
 * If the rule is transformation rule and isExplore is false:
 * We need to optimize (apply logical & physical rules)
 * <p>
 * If the rule is implementation rule:
 * We directly enforce and cost the physical expression
 */

public class ApplyRuleTask extends OptimizerTask {
    private static final Logger LOG = LogManager.getLogger(Optimizer.class);
    private final GroupExpression groupExpression;
    private final Rule rule;
    private final boolean isExplore;

    ApplyRuleTask(TaskContext context, GroupExpression groupExpression, Rule rule, boolean isExplore) {
        super(context);
        this.groupExpression = groupExpression;
        this.rule = rule;
        this.isExplore = isExplore;
    }

    @Override
    public String toString() {
        return "ApplyRuleTask" + (this.isExplore ? "[explore]" : "") + " for groupExpression " + groupExpression +
                "\n rule " + rule;
    }

    @Override
    public void execute() {
        if (groupExpression.hasRuleExplored(rule) ||
                groupExpression.isUnused()) {
            return;
        }
        SessionVariable sessionVariable = context.getOptimizerContext().getSessionVariable();
        // Apply rule and get all new OptExpressions
        Pattern pattern = rule.getPattern();
        Binder binder = new Binder(pattern, groupExpression);
        OptExpression extractExpr = binder.next();
        List<OptExpression> newExpressions = Lists.newArrayList();
        while (extractExpr != null) {
            if (!rule.check(extractExpr, context.getOptimizerContext())) {
                extractExpr = binder.next();
                continue;
            }
            List<OptExpression> targetExpressions = rule.transform(extractExpr, context.getOptimizerContext());
            newExpressions.addAll(targetExpressions);

            OptimizerTraceInfo traceInfo = context.getOptimizerContext().getTraceInfo();
            OptimizerTraceUtil.logApplyRule(sessionVariable, traceInfo, rule, extractExpr, targetExpressions);

            extractExpr = binder.next();
        }

        for (OptExpression expression : newExpressions) {
            // Insert new OptExpression to memo
            Pair<Boolean, GroupExpression> result = context.getOptimizerContext().getMemo().
                    copyIn(groupExpression.getGroup(), expression);

            // The group has been merged
            if (groupExpression.hasEmptyRootGroup()) {
                return;
            }

            GroupExpression newGroupExpression = result.second;
            if (newGroupExpression.getOp().isLogical()) {
                // For logic newGroupExpression, optimize it
                pushTask(new OptimizeExpressionTask(context, newGroupExpression, isExplore));
            } else {
                // For physical newGroupExpression, enforce and cost it,
                // Optimize its inputs if needed
                pushTask(new EnforceAndCostTask(context, newGroupExpression));
            }
        }

        groupExpression.setRuleExplored(rule);
    }
}
