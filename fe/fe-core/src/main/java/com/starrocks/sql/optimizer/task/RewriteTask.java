// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.task;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Rule;

import java.util.List;

public class RewriteTask extends OptimizerTask {
    private final OptExpression planTree;
    private final boolean onlyOnce;
    private final List<Rule> rules;
    private long change = 0;

    public RewriteTask(TaskContext context, OptExpression root, List<Rule> rules, boolean onlyOnce) {
        super(context);
        this.planTree = root;
        this.rules = rules;
        this.onlyOnce = onlyOnce;
    }

    public OptExpression getResult() {
        return planTree.getInputs().get(0);
    }

    @Override
    public void execute() {
        // first node must be RewriteAnchorNode
        rewrite(planTree, 0, planTree.getInputs().get(0));

        if (change > 0 && !onlyOnce) {
            pushTask(new RewriteTask(context, planTree, rules, onlyOnce));
        }
    }

    private void rewrite(OptExpression parent, int childIndex, OptExpression root) {
        for (Rule rule : rules) {
            if (!match(rule.getPattern(), root) || !rule.check(root, context.getOptimizerContext())) {
                continue;
            }

            List<OptExpression> result = rule.transform(root, context.getOptimizerContext());
            Preconditions.checkState(result.size() <= 1, "Rewrite rule should provide at most 1 expression");

            if (result.isEmpty()) {
                continue;
            }

            parent.getInputs().set(childIndex, result.get(0));
            root = result.get(0);
            change++;
            deriveLogicalProperty(root);
        }

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
