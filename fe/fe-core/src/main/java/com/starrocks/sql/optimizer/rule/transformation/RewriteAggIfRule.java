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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/**
 * Rewrite rule for aggregate functions with _if suffix (e.g., min_by_if, max_by_if, array_agg_if).
 * This rule uses CTE to optimize agg_if functions by splitting them into filter + base aggregate.
 */
public class RewriteAggIfRule extends TransformationRule {

    public RewriteAggIfRule() {
        super(RuleType.TF_REWRITE_AGG_IF,
                Pattern.create(com.starrocks.sql.optimizer.operator.OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(
                        com.starrocks.sql.optimizer.operator.OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        
        // Check if there are any agg_if functions
        return agg.getAggregations().values().stream()
                .anyMatch(this::isAggIf);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        AggIfCTERewriter rewriter = new AggIfCTERewriter();
        return rewriter.transformImpl(input, context);
    }

    private boolean isAggIf(CallOperator callOperator) {
        if (!callOperator.isAggregate()) {
            return false;
        }
        String fnName = callOperator.getFnName();
        return fnName.equalsIgnoreCase(FunctionSet.ARRAY_AGG + FunctionSet.AGG_STATE_IF_SUFFIX) ||
                fnName.equalsIgnoreCase(FunctionSet.ARRAY_AGG_DISTINCT + FunctionSet.AGG_STATE_IF_SUFFIX);
    }
}

