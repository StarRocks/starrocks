// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializationContext;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;

import java.util.List;

/*
 * SPJG materialized view rewrite rule, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 */
public abstract class BaseMaterializedViewRewriteRule extends TransformationRule {

    public BaseMaterializedViewRewriteRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (context.getCandidateMvs().isEmpty()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression queryExpression, OptimizerContext context) {
        List<OptExpression> results = Lists.newArrayList();
        for (MaterializationContext mvContext : context.getCandidateMvs()) {
            mvContext.setQueryExpression(queryExpression);
            mvContext.setQueryRefFactory(context.getColumnRefFactory());
            MaterializedViewRewriter rewriter = getMaterializedViewRewrite(mvContext);
            List<OptExpression> rewritten = rewriter.rewrite();
            if (rewritten != null) {
                results.addAll(rewritten);
            }
        }
        return results;
    }

    MaterializedViewRewriter getMaterializedViewRewrite(MaterializationContext mvContext) {
        return new MaterializedViewRewriter(mvContext);
    }
}
