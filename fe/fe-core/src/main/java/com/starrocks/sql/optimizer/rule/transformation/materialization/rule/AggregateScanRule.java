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


package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;

/**
 * Materialized View Rewrite Rule for pattern:
 * - Aggregate
 * - Scan
 */
public class AggregateScanRule extends SingleTableRewriteBaseRule {
    private static final AggregateScanRule INSTANCE = new AggregateScanRule();

    public AggregateScanRule() {
        super(RuleType.TF_MV_AGGREGATE_SCAN_RULE, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.PATTERN_SCAN)));
    }

    public static AggregateScanRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!MvUtils.isLogicalSPJG(input)) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    public MaterializedViewRewriter getMaterializedViewRewrite(MvRewriteContext mvContext) {
        return new AggregatedMaterializedViewRewriter(mvContext);
    }
}