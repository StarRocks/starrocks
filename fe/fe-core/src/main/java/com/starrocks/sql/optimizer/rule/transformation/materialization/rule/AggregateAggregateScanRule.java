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

/**
 * Materialized View Rewrite Rule for pattern:
 * - Aggregate
 *  - Aggregate
 *      - Scan
 */
public class AggregateAggregateScanRule extends SingleTableRewriteBaseRule {
    private static final AggregateAggregateScanRule INSTANCE = new AggregateAggregateScanRule();

    public AggregateAggregateScanRule() {
        super(RuleType.TF_MV_AGGREGATE_AGGREGATE_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(OperatorType.PATTERN_SCAN))));
    }

    public static AggregateAggregateScanRule getInstance() {
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