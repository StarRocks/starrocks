// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

/*
 *
 * Here is the rule for pattern Projection - Filter - Join
 *
 */
public class ProjectionFilterJoinRule extends BaseMaterializedViewRewriteRule {
    private static ProjectionFilterJoinRule INSTANCE = new ProjectionFilterJoinRule();

    public ProjectionFilterJoinRule() {
        super(RuleType.TF_MV_PROJECT_FILTER_JOIN_RULE, Pattern.create(OperatorType.LOGICAL_PROJECT)
                .addChildren(Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_MULTIJOIN)));
    }

    public static ProjectionFilterJoinRule getInstance() {
        return INSTANCE;
    }
}
