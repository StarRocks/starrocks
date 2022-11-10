// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;

public abstract class AggregateRewriteBaseRule extends BaseMaterializedViewRewriteRule {
    public AggregateRewriteBaseRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        boolean ret = Utils.isLogicalSPJG(input);
        if (!ret) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    MaterializedViewRewriter getMaterializedViewRewrite(MaterializationContext mvContext) {
        return new AggregatedMaterializedViewRewriter(mvContext);
    }
}
