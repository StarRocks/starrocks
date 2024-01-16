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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.rule.RuleSetType;
import com.starrocks.sql.optimizer.rule.RuleType;

public class MvPlanContextBuilder {
    public static MvPlanContext getPlanContext(MaterializedView mv) {
        // build mv query logical plan
        MaterializedViewOptimizer mvOptimizer = new MaterializedViewOptimizer();
        // optimize the sql by rule and disable rule based materialized view rewrite
        OptimizerConfig optimizerConfig = new OptimizerConfig(OptimizerConfig.OptimizerAlgorithm.RULE_BASED);
        optimizerConfig.disableRuleSet(RuleSetType.PARTITION_PRUNE);
        optimizerConfig.disableRuleSet(RuleSetType.SINGLE_TABLE_MV_REWRITE);
        optimizerConfig.disableRule(RuleType.TF_REWRITE_GROUP_BY_COUNT_DISTINCT);
        optimizerConfig.setMVRewritePlan(true);
        ConnectContext connectContext = ConnectContext.get();
        // If the caller is not from query (eg. background schema change thread), set thread local info to avoid
        // NPE in the planning.
        if (connectContext == null) {
            connectContext = new ConnectContext();
            connectContext.setThreadLocalInfo();
        }
        return mvOptimizer.optimize(mv, connectContext, optimizerConfig);
    }
}
