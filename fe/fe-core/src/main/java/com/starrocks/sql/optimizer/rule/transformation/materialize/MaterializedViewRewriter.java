// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialize;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.EquivalenceClasses;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

/*
 * SPJG materialized view rewriter, based on
 * 《Optimizing Queries Using Materialized Views: A Practical, Scalable Solution》
 *
 *
 */
public class MaterializedViewRewriter {
    private final Pair<ScalarOperator, ScalarOperator> queryPredicatesPair;
    private final EquivalenceClasses queryEc;
    private final LogicalProjectOperator queryProjection;
    private final OptExpression query;
    private final MaterializedView mv;
    private final OptimizerContext optimizerContext;

    public MaterializedViewRewriter(Pair<ScalarOperator, ScalarOperator> queryPredicatesPair,
                                    EquivalenceClasses queryEc, LogicalProjectOperator queryProjection,
                                    OptExpression query, MaterializedView mv, OptimizerContext optimizerContext) {
        this.queryPredicatesPair = queryPredicatesPair;
        this.queryEc = queryEc;
        this.queryProjection = queryProjection;
        this.query = query;
        this.mv = mv;
        this.optimizerContext = optimizerContext;
    }

    public OptExpression rewriteQuery() {
        return null;
    }
}
