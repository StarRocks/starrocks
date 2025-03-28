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
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;

public class MaterializedViewOptimizer {
    public MvPlanContext optimize(MaterializedView mv,
                                  ConnectContext connectContext) {
        return optimize(mv, connectContext, true, true);
    }

    public MvPlanContext optimize(MaterializedView mv,
                                  ConnectContext connectContext,
                                  boolean inlineView,
                                  boolean isCheckNonDeterministicFunction) {
        // bind scope force to ensure thread local is set correctly by using connectContext
        try (var guard = connectContext.bindScope()) {
            return optimizeImpl(mv, connectContext, inlineView, isCheckNonDeterministicFunction);
        }
    }

    private MvPlanContext optimizeImpl(MaterializedView mv,
                                       ConnectContext connectContext,
                                       boolean inlineView,
                                       boolean isCheckNonDeterministicFunction) {
        // optimize the sql by rule and disable rule based materialized view rewrite
        OptimizerOptions optimizerOptions = OptimizerOptions.newRuleBaseOpt();
        // Disable partition prune for mv's plan so no needs  to compensate pruned predicates anymore.
        // Only needs to compensate mv's ref-base-table's partition predicates when mv's freshness cannot be satisfied.
        optimizerOptions.disableRule(RuleType.GP_PARTITION_PRUNE);
        optimizerOptions.disableRule(RuleType.GP_ALL_MV_REWRITE);
        // INTERSECT_REWRITE is used for INTERSECT related plan optimize, which can not be SPJG;
        // And INTERSECT_REWRITE should be based on PARTITION_PRUNE rule set.
        // So exclude it
        optimizerOptions.disableRule(RuleType.GP_INTERSECT_REWRITE);
        optimizerOptions.disableRule(RuleType.TF_REWRITE_GROUP_BY_COUNT_DISTINCT);
        optimizerOptions.disableRule(RuleType.TF_PRUNE_EMPTY_SCAN);
        optimizerOptions.disableRule(RuleType.TF_MV_TEXT_MATCH_REWRITE_RULE);
        optimizerOptions.disableRule(RuleType.TF_MV_TRANSPARENT_REWRITE_RULE);
        optimizerOptions.disableRule(RuleType.TF_ELIMINATE_AGG);
        optimizerOptions.disableRule(RuleType.TF_PULL_UP_PREDICATE_SCAN);
        // For sync mv, no rewrite query by original sync mv rule to avoid useless rewrite.
        if (mv.getRefreshScheme().isSync()) {
            optimizerOptions.disableRule(RuleType.TF_MATERIALIZED_VIEW);
        }
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        String mvSql = mv.getViewDefineSql();
        // parse mv's defined query
        StatementBase stmt = MvUtils.parse(mv, mvSql, connectContext);
        if (stmt == null) {
            return new MvPlanContext(false, "MV Plan parse failed");
        }
        // check whether mv's defined query contains non-deterministic functions
        final Pair<Boolean, String> containsNonDeterministicFunctions = AnalyzerUtils.containsNonDeterministicFunction(stmt);
        final boolean containsNDFunctions = containsNonDeterministicFunctions != null && containsNonDeterministicFunctions.first;
        if (isCheckNonDeterministicFunction && containsNDFunctions) {
            final String invalidPlanReason = String.format("MV contains non-deterministic functions(%s)",
                    containsNonDeterministicFunctions.second);
            return new MvPlanContext(false, invalidPlanReason);
        }
        // push down mode
        int originAggPushDownMode = connectContext.getSessionVariable().getCboPushDownAggregateMode();
        if (originAggPushDownMode != -1) {
            connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
        }
        // disable function fold constants
        final boolean originDisableFunctionFoldConstants = connectContext.getSessionVariable().isDisableFunctionFoldConstants();
        if (!isCheckNonDeterministicFunction) {
            // In some cases(eg, transparent mv), we can generate a plan for mv even
            // if it contains non-deterministic functions, but we need to disable function fold constants because
            // non-deterministic functions can be evaluated for each query.
            connectContext.getSessionVariable().setDisableFunctionFoldConstants(true);
        }

        try {
            // get optimized plan of mv's defined query
            Pair<OptExpression, LogicalPlan> plans =
                    MvUtils.getRuleOptimizedLogicalPlan(stmt, columnRefFactory, connectContext, optimizerOptions,
                            inlineView);
            if (plans == null) {
                return new MvPlanContext(false, "No query plan for it");
            }
            OptExpression mvPlan = plans.first;
            boolean isValidPlan = MvUtils.isValidMVPlan(mvPlan);
            // not set it invalid plan if text match rewrite is on because text match rewrite can support all query pattern.
            String invalidPlanReason = "";
            if (!isValidPlan) {
                invalidPlanReason = MvUtils.getInvalidReason(mvPlan, inlineView);
            }
            return new MvPlanContext(mvPlan, plans.second.getOutputColumn(), columnRefFactory, isValidPlan,
                    containsNDFunctions, invalidPlanReason);
        } finally {
            connectContext.getSessionVariable().setCboPushDownAggregateMode(originAggPushDownMode);
            connectContext.getSessionVariable().setDisableFunctionFoldConstants(originDisableFunctionFoldConstants);
        }
    }
}
