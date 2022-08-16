// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.Rule;
import com.starrocks.sql.optimizer.rule.RuleType;

/**
 * Transformation rules: logical -> logical
 */
public abstract class TransformationRule extends Rule {
    protected TransformationRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }
}
