// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

/*
 *
 * Here is the rule for pattern Aggregate-Join
 *
 */
public class AggregateJoinRule extends BaseMaterializedViewRewriteRule {
    private static AggregateJoinRule INSTANCE = new AggregateJoinRule();

    public AggregateJoinRule() {
        super(RuleType.TF_MV_AGGREGATE_JOIN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_MULTIJOIN)));
    }

    public static AggregateJoinRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!MvUtils.isLogicalSPJG(input)) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    public MaterializedViewRewriter getMaterializedViewRewrite(MvRewriteContext mvContext) {
        return new AggregatedMaterializedViewRewriter(mvContext);
    }
}