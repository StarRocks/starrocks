// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Aggregate-Join
 *
 */
public class AggregateJoinRule extends BaseMaterializedViewRewriteRule {
    private static AggregateJoinRule INSTANCE = new AggregateJoinRule();

    public AggregateJoinRule() {
        super(RuleType.TF_MV_AGGREGATE_JOIN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTIJOIN)), true);
    }

    public static AggregateJoinRule getInstance() {
        return INSTANCE;
    }
}