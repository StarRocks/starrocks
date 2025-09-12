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
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;

public class MaterializedViewOptimizer {
    public MvPlanContext optimize(MaterializedView mv,
                                  ConnectContext connectContext) {
        return optimize(mv, connectContext, true);
    }

    public MvPlanContext optimize(MaterializedView mv,
                                  ConnectContext connectContext,
                                  boolean inlineView) {
        // optimize the sql by rule and disable rule based materialized view rewrite
        OptimizerConfig optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED);
        // Disable partition prune for mv's plan so no needs  to compensate pruned predicates anymore.
        // Only needs to compensate mv's ref-base-table's partition predicates when mv's freshness cannot be satisfied.
        optimizerConfig.disableRuleSet(RuleSetType.PARTITION_PRUNE);
        optimizerConfig.disableRuleSet(RuleSetType.ALL_MV_REWRITE);
        // INTERSECT_REWRITE is used for INTERSECT related plan optimize, which can not be SPJG;
        // And INTERSECT_REWRITE should be based on PARTITION_PRUNE rule set.
        // So exclude it
        optimizerConfig.disableRuleSet(RuleSetType.INTERSECT_REWRITE);
        optimizerConfig.disableRule(RuleType.TF_REWRITE_GROUP_BY_COUNT_DISTINCT);
        optimizerConfig.disableRule(RuleType.TF_PRUNE_EMPTY_SCAN);
        optimizerConfig.disableRule(RuleType.TF_MV_TEXT_MATCH_REWRITE_RULE);
        optimizerConfig.disableRule(RuleType.TF_MV_TRANSPARENT_REWRITE_RULE);
        optimizerConfig.disableRule(RuleType.TF_ELIMINATE_AGG);
        optimizerConfig.disableRule(RuleType.TF_PULL_UP_PREDICATE_SCAN);
        // For sync mv, no rewrite query by original sync mv rule to avoid useless rewrite.
        if (mv.getRefreshScheme().isSync()) {
            optimizerConfig.disableRule(RuleType.TF_MATERIALIZED_VIEW);
        }
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        String mvSql = mv.getViewDefineSql();
        // parse mv's defined query
        StatementBase stmt = MvUtils.parse(mv, mvSql, connectContext);
        if (stmt == null) {
            return new MvPlanContext(false, "MV Plan parse failed");
        }
        // check whether mv's defined query contains non-deterministic functions
        Pair<Boolean, String> containsNonDeterministicFunctions = AnalyzerUtils.containsNonDeterministicFunction(stmt);
        if (containsNonDeterministicFunctions != null && containsNonDeterministicFunctions.first) {
            String invalidPlanReason = String.format("MV contains non-deterministic functions(%s)",
                    containsNonDeterministicFunctions.second);
            return new MvPlanContext(false, invalidPlanReason);
        }
        // get optimized plan of mv's defined query
        MVTransformerContext mvTransformerContext = new MVTransformerContext(connectContext, inlineView);
        Pair<OptExpression, LogicalPlan> plans =
                MvUtils.getRuleOptimizedLogicalPlan(stmt, columnRefFactory, 
                    connectContext, optimizerConfig, mvTransformerContext);
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
        return new MvPlanContext(mvPlan, plans.second.getOutputColumn(), columnRefFactory, isValidPlan, invalidPlanReason);
    }
}
