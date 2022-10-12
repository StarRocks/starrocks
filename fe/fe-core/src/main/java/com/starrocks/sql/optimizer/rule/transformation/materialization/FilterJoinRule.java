// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Filter - Join
 *
 */
public class FilterJoinRule extends BaseMaterializedViewRewriteRule {
    private static FilterJoinRule INSTANCE = new FilterJoinRule();
    public FilterJoinRule() {
        super(RuleType.TF_MV_FILTER_JOIN_RULE, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTIJOIN)));
    }

    public static FilterJoinRule getInstance() {
        return INSTANCE;
    }
}