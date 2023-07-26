// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedView.MVRewriteContextCache;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;

public class MaterializedViewOptimizer {
    public MVRewriteContextCache optimize(MaterializedView mv,
                                          ConnectContext connectContext,
                                          OptimizerConfig optimizerConfig) {
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans =
                MvUtils.getRuleOptimizedLogicalPlan(mv, mvSql, columnRefFactory, connectContext, optimizerConfig);
        if (plans == null) {
            return null;
        }
        OptExpression mvPlan = plans.first;
        if (!MvUtils.isValidMVPlan(mvPlan)) {
            return new MVRewriteContextCache();
        }
        MVRewriteContextCache mvRewriteContext =
                new MVRewriteContextCache(mvPlan, plans.second.getOutputColumn(), columnRefFactory);
        return mvRewriteContext;
    }
}
