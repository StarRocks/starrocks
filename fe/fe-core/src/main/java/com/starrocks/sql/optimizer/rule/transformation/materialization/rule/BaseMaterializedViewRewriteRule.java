// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.ColumnPruner;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;

import java.util.List;

public abstract class BaseMaterializedViewRewriteRule extends TransformationRule {

    protected BaseMaterializedViewRewriteRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        return !context.getCandidateMvs().isEmpty();
    }

    @Override
    public List<OptExpression> transform(OptExpression queryExpression, OptimizerContext context) {
        List<OptExpression> results = Lists.newArrayList();
        for (MaterializationContext mvContext : context.getCandidateMvs()) {
            mvContext.setQueryExpression(queryExpression);
            mvContext.setOptimizerContext(context);
            MaterializedViewRewriter rewriter = getMaterializedViewRewrite(mvContext);
            List<OptExpression> rewrittens = rewriter.rewrite();
            if (rewrittens != null && !rewrittens.isEmpty()) {
                for (OptExpression rewritten : rewrittens) {
                    ColumnPruner columnPruner = new ColumnPruner();
                    OptExpression exprAfterPrune = columnPruner.pruneColumns(rewritten);
                    if (exprAfterPrune != null) {
                        results.add(exprAfterPrune);
                    }
                }
            }
        }

        return results;
    }

    public MaterializedViewRewriter getMaterializedViewRewrite(MaterializationContext mvContext) {
        return new MaterializedViewRewriter(mvContext);
    }
}
