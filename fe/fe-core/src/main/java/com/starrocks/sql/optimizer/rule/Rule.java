// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;

import java.util.List;

/**
 * A rule is a description of how to
 * transform an expression to a logically equivalent expression.
 * A new expression is generated
 * when a rule is applied to a given expression.
 * <p>
 * Each rule is defined as a pair of pattern and substitute.
 * A pattern defines the structure of the logical expression
 * that can be applied to the rule.
 * A substitute defines the structure of the result after applying the rule.
 * <p>
 * Pattern and substitute are represented as expression trees.
 * <p>
 * Transformation rules and implementation rules are two common types of rules.
 * Transformation rules: logical -> logical
 * Implementation rules: logical -> physical
 * <p>
 * An implementation rule has a higher promise than transformation rule,
 * indicating implementation rules are always scheduled earlier
 */
public abstract class Rule {
    private final RuleType type;
    private final Pattern pattern;

    protected Rule(RuleType type, Pattern pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    public RuleType type() {
        return type;
    }

    public Pattern getPattern() {
        return pattern;
    }

    public int promise() {
        return 1;
    }

    public boolean check(final OptExpression input, OptimizerContext context) {
        return true;
    }

    /**
     * If this transform don't change the input OptExpression, should return the empty list
     */
    public abstract List<OptExpression> transform(OptExpression input, OptimizerContext context);

    @Override
    public String toString() {
        return type.name() + " " + type.id();
    }
}
