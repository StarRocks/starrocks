// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.implementation;

import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

/**
 * Implementation rules: logical -> physical
 */
public abstract class ImplementationRule extends Rule {

    protected ImplementationRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    // Implementation rules have higher promise than transformation rule.
    public int promise() {
        return 2;
    }
}
