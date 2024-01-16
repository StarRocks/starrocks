// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.MaterializedView;
<<<<<<< HEAD
import com.starrocks.catalog.MaterializedView.MvRewriteContext;
=======
import com.starrocks.catalog.MvPlanContext;
>>>>>>> branch-2.5
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;

public class MaterializedViewOptimizer {
<<<<<<< HEAD
    public MvRewriteContext optimize(MaterializedView mv,
                                     ConnectContext connectContext) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans =
                MvUtils.getRuleOptimizedLogicalPlan(mvSql, columnRefFactory, connectContext);
=======
    public MvPlanContext optimize(MaterializedView mv,
                                  ConnectContext connectContext,
                                  OptimizerConfig optimizerConfig) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans =
                MvUtils.getRuleOptimizedLogicalPlan(mv, mvSql, columnRefFactory, connectContext, optimizerConfig);
>>>>>>> branch-2.5
        if (plans == null) {
            return new MvPlanContext(false, "No query plan for it");
        }
        OptExpression mvPlan = plans.first;
        if (!MvUtils.isValidMVPlan(mvPlan)) {
<<<<<<< HEAD
<<<<<<< HEAD
            return new MvRewriteContext();
        }
        MvRewriteContext mvRewriteContext = new MvRewriteContext(mvPlan, plans.second.getOutputColumn(), columnRefFactory);
=======
            return new MvPlanContext();
        }
        MvPlanContext mvRewriteContext =
                new MvPlanContext(mvPlan, plans.second.getOutputColumn(), columnRefFactory);
>>>>>>> branch-2.5
        return mvRewriteContext;
=======
            return new MvPlanContext(false, MvUtils.getInvalidReason(mvPlan));
        }
        return new MvPlanContext(mvPlan, plans.second.getOutputColumn(), columnRefFactory);
>>>>>>> branch-2.5
    }
}
