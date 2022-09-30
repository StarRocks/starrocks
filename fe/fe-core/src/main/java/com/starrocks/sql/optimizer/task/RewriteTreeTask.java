// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Preconditions;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerTraceInfo;
import com.starrocks.sql.optimizer.OptimizerTraceUtil;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

/*
 *
 * Rewrite whole tree by TopDown way
 * Rules will be applied to each node from the TopDown, and will repeat
 * push same task to rewrite whole tree when the task isn't only once and
 * tree was changed, until the tree is no changed.
 *
 */
public class RewriteTreeTask extends OptimizerTask {
    private final OptExpression planTree;
    private final boolean onlyOnce;
    private final List<Rule> rules;
    private long change = 0;

    public RewriteTreeTask(TaskContext context, OptExpression root, List<Rule> rules, boolean onlyOnce) {
        super(context);
        this.planTree = root;
        this.rules = rules;
        this.onlyOnce = onlyOnce;
        Preconditions.checkState(planTree.getOp().getOpType() == OperatorType.LOGICAL);
    }

    public OptExpression getResult() {
        return planTree.getInputs().get(0);
    }

    @Override
    public void execute() {
        // first node must be RewriteAnchorNode
        rewrite(planTree, 0, planTree.getInputs().get(0));

        if (change > 0 && !onlyOnce) {
            pushTask(new RewriteTreeTask(context, planTree, rules, onlyOnce));
        }
    }

    private void rewrite(OptExpression parent, int childIndex, OptExpression root) {
        SessionVariable sessionVariable = context.getOptimizerContext().getSessionVariable();

        for (Rule rule : rules) {
            if (!match(rule.getPattern(), root) || !rule.check(root, context.getOptimizerContext())) {
                continue;
            }

            List<OptExpression> result = rule.transform(root, context.getOptimizerContext());
            Preconditions.checkState(result.size() <= 1, "Rewrite rule should provide at most 1 expression");

            OptimizerTraceInfo traceInfo = context.getOptimizerContext().getTraceInfo();
            OptimizerTraceUtil.logApplyRule(sessionVariable, traceInfo, rule, root, result);

            if (result.isEmpty()) {
                continue;
            }

            parent.getInputs().set(childIndex, result.get(0));
            root = result.get(0);
            change++;
            deriveLogicalProperty(root);
        }

        // prune cte column depend on prune right child first
        for (int i = root.getInputs().size() - 1; i >= 0; i--) {
            rewrite(root, i, root.getInputs().get(i));
        }
    }

    private boolean match(Pattern pattern, OptExpression root) {
        if (!pattern.matchWithoutChild(root)) {
            return false;
        }

        int patternIndex = 0;
        int childIndex = 0;

        while (patternIndex < pattern.children().size() && childIndex < root.getInputs().size()) {
            OptExpression child = root.getInputs().get(childIndex);
            Pattern childPattern = pattern.childAt(patternIndex);

            if (!match(childPattern, child)) {
                return false;
            }

            if (!(childPattern.isPatternMultiLeaf() && (root.getInputs().size() - childIndex) >
                    (pattern.children().size() - patternIndex))) {
                patternIndex++;
            }

            childIndex++;
        }
        return true;
    }

    private void deriveLogicalProperty(OptExpression root) {
        for (OptExpression child : root.getInputs()) {
            deriveLogicalProperty(child);
        }

        if (root.getLogicalProperty() == null) {
            ExpressionContext context = new ExpressionContext(root);
            context.deriveLogicalProperty();
            root.setLogicalProperty(context.getRootProperty());
        }
    }
}
