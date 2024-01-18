// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.MaterializedView;
<<<<<<< HEAD
import com.starrocks.catalog.MvPlanContext;
=======
import com.starrocks.catalog.MaterializedView.MvRewriteContext;
>>>>>>> branch-2.5-mrs
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;

public class MaterializedViewOptimizer {
<<<<<<< HEAD
    public MvPlanContext optimize(MaterializedView mv,
                                  ConnectContext connectContext,
                                  OptimizerConfig optimizerConfig) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans =
                MvUtils.getRuleOptimizedLogicalPlan(mv, mvSql, columnRefFactory, connectContext, optimizerConfig);
=======
    public MvRewriteContext optimize(MaterializedView mv,
                                     ConnectContext connectContext) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans =
                MvUtils.getRuleOptimizedLogicalPlan(mvSql, columnRefFactory, connectContext);
>>>>>>> branch-2.5-mrs
        if (plans == null) {
            return null;
        }
        OptExpression mvPlan = plans.first;
        if (!MvUtils.isValidMVPlan(mvPlan)) {
<<<<<<< HEAD
            return new MvPlanContext();
        }
        MvPlanContext mvRewriteContext =
                new MvPlanContext(mvPlan, plans.second.getOutputColumn(), columnRefFactory);
=======
            return new MvRewriteContext();
        }
        MvRewriteContext mvRewriteContext = new MvRewriteContext(mvPlan, plans.second.getOutputColumn(), columnRefFactory);
>>>>>>> branch-2.5-mrs
        return mvRewriteContext;
    }
}
