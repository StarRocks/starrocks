// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Join
 *
 */
public class OnlyJoinRule extends BaseMaterializedViewRewriteRule {
    private static final OnlyJoinRule INSTANCE = new OnlyJoinRule();

    public OnlyJoinRule() {
        super(RuleType.TF_MV_ONLY_JOIN_RULE, Pattern.create(OperatorType.PATTERN_MULTIJOIN));
    }

    public static OnlyJoinRule getInstance() {
        return INSTANCE;
    }
}