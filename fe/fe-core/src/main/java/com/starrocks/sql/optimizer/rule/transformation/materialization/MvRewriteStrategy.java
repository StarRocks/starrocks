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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerOptions;
import com.starrocks.sql.optimizer.rule.RuleType;

public class MvRewriteStrategy {
    public static final MvRewriteStrategy DEFAULT = new MvRewriteStrategy();

    /**
     * Materialized view rewrite strategy, when multi stages is enabled, we will rewrite query plan in multi stages.
     * - Default: rewrite the query after required rules are executed in rule based rewrite stage.
     * - MultiStage: rewrite the query in early stage(before some rules which may break plans' structure) and later stage
     * (required rules are executed) in rule based rewrite stage.
     * Attention: MultiStage may increase rewrite optimizer time, so it's disabled by default.
     */
    public enum MVStrategy {
        DEFAULT(0),
        MULTI_STAGES(1);
        private int ordinal;
        MVStrategy(int ordinal) {
            this.ordinal = ordinal;
        }
        public int getOrdinal() {
            return this.ordinal;
        }
        public boolean isMultiStages() {
            return this == MULTI_STAGES;
        }
    }

    public enum MVRewriteStage {
        PHASE0(0),
        PHASE1(1),
        PHASE2(2);
        private int ordinal;
        MVRewriteStage(int ordinal) {
            this.ordinal = ordinal;
        }
        public int getOrdinal() {
            return this.ordinal;
        }
    }

    // general config
    public boolean enableMaterializedViewRewrite = false;
    // Whether enable force rewrite for query plans with join operator by rule based mv rewrite
    public boolean enableForceRBORewrite = false;
    public MVStrategy mvStrategy = MVStrategy.DEFAULT;

    public boolean enableViewBasedRewrite = false;
    public boolean enableSingleTableRewrite = false;
    public boolean enableMultiTableRewrite = false;

    static class MvStrategyArbitrator {
        private final OptimizerOptions optimizerOptions;
        private final OptimizerContext optimizerContext;
        private final SessionVariable sessionVariable;

        public MvStrategyArbitrator(OptimizerContext optimizerContext,
                                    ConnectContext connectContext) {
            this.optimizerContext = optimizerContext;
            this.optimizerOptions = optimizerContext.getOptimizerOptions();
            // from connectContext rather than optimizerContext
            this.sessionVariable = connectContext.getSessionVariable();
        }

        private boolean isEnableMaterializedViewRewrite() {
            if (sessionVariable.isDisableMaterializedViewRewrite()) {
                return false;
            }
            // if disable isEnableMaterializedViewRewrite, return false.
            if (!sessionVariable.isEnableMaterializedViewRewrite()) {
                return false;
            }
            // if mv candidates are empty, return false.
            if (optimizerContext.getCandidateMvs() == null ||
                    optimizerContext.getCandidateMvs().isEmpty()) {
                return false;
            }
            if (optimizerOptions.isRuleDisable(RuleType.GP_SINGLE_TABLE_MV_REWRITE) &&
                    optimizerOptions.isRuleDisable(RuleType.GP_MULTI_TABLE_MV_REWRITE)) {
                return false;
            }
            return true;
        }

        private boolean isEnableRBOViewBasedRewrite() {
            return optimizerContext.getQueryMaterializationContext() != null
                    && optimizerContext.getQueryMaterializationContext().getQueryOptPlanWithView() != null
                    && sessionVariable.isEnableViewBasedMvRewrite()
                    && !sessionVariable.isEnableCBOViewBasedMvRewrite();
        }

        private boolean isEnableRBOSingleTableRewrite(OptExpression queryPlan) {
            // if disable single mv rewrite, return false.
            if (optimizerOptions.isRuleDisable(RuleType.GP_SINGLE_TABLE_MV_REWRITE)) {
                return false;
            }
            // If query only has one table use single table rewrite, view delta only rewrites multi-tables query.
            if (!sessionVariable.isEnableMaterializedViewSingleTableViewDeltaRewrite() &&
                    MvUtils.getAllTables(queryPlan).size() <= 1) {
                return true;
            }
            // If view delta is enabled and there are multi-table mvs, return false.
            // if mv has multi table sources, we will process it in memo to support view delta join rewrite
            if (sessionVariable.isEnableMaterializedViewViewDeltaRewrite() &&
                    optimizerContext.getCandidateMvs().stream().anyMatch(MaterializationContext::hasMultiTables)) {
                return false;
            }
            return true;
        }

        private boolean isEnableCBOMultiTableRewrite(OptExpression queryPlan) {
            if (!sessionVariable.isEnableMaterializedViewSingleTableViewDeltaRewrite() &&
                    MvUtils.getAllTables(queryPlan).size() <= 1) {
                return false;
            }
            return true;
        }
    }

    /**
     * Prepare mv rewrite strategy used in optimizor's rbo/cbo phase.
     * @param optimizerContext: optimizer context
     * @param connectContext: connect context
     * @param queryPlan: query plan to rewrite or not
     */
    public static MvRewriteStrategy prepareRewriteStrategy(OptimizerContext optimizerContext,
                                                           ConnectContext connectContext,
                                                           OptExpression queryPlan) {
        MvRewriteStrategy strategy = new MvRewriteStrategy();
        Preconditions.checkState(strategy != null, "MvRewriteStrategy is null");
        MvStrategyArbitrator arbitrator = new MvStrategyArbitrator(optimizerContext, connectContext);
        strategy.enableMaterializedViewRewrite = arbitrator.isEnableMaterializedViewRewrite();
        // only rewrite when enableMaterializedViewRewrite is enabled
        if (!strategy.enableMaterializedViewRewrite) {
            return DEFAULT;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();

        // only enable multi-stages when force rewrite is enabled
        if (sessionVariable.isEnableMaterializedViewMultiStagesRewrite() ||
                sessionVariable.isEnableMaterializedViewForceRewrite()) {
            strategy.mvStrategy = MVStrategy.MULTI_STAGES;
        }
        strategy.enableForceRBORewrite = sessionVariable.isEnableForceRuleBasedMvRewrite();

        // rbo strategies
        strategy.enableViewBasedRewrite = arbitrator.isEnableRBOViewBasedRewrite();
        strategy.enableSingleTableRewrite = arbitrator.isEnableRBOSingleTableRewrite(queryPlan);

        // cbo strategies
        strategy.enableMultiTableRewrite = arbitrator.isEnableCBOMultiTableRewrite(queryPlan);
        return strategy;
    }

    @Override
    public String toString() {
        return "MvRewriteStrategy{" +
                "enableMaterializedViewRewrite=" + enableMaterializedViewRewrite +
                ", enableForceRBORewrite=" + enableForceRBORewrite +
                ", enableViewBasedRewrite=" + enableViewBasedRewrite +
                ", enableSingleTableRewrite=" + enableSingleTableRewrite +
                ", enableMultiTableRewrite=" + enableMultiTableRewrite +
                ", mvStrategy=" + mvStrategy +
                '}';
    }
}
