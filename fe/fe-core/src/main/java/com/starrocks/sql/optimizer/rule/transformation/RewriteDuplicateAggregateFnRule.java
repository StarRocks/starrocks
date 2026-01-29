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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Map;

// Rewrite sql:
// select bitmap_union_count(x), count(distinct x) from table having count(distinct x)
// reduce one calculation count(distinct)
public class RewriteDuplicateAggregateFnRule extends TransformationRule {
    public RewriteDuplicateAggregateFnRule() {
        super(RuleType.TF_REWRITE_DUPLICATE_AGGREGATE_FN,
                Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregation = (LogicalAggregationOperator) input.getOp();
        List<CallOperator> aggregations = Lists.newArrayList(aggregation.getAggregations().values());
        
        // Check for duplicates using semantic equivalence comparison
        for (int i = 0; i < aggregations.size(); i++) {
            for (int j = i + 1; j < aggregations.size(); j++) {
                if (isSemanticallySameAggregation(aggregations.get(i), aggregations.get(j))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if two aggregate functions are semantically the same.
     * This includes exact equality and semantic equivalence (e.g., different argument order in AND/OR).
     */
    private boolean isSemanticallySameAggregation(CallOperator agg1, CallOperator agg2) {
        if (agg1.equals(agg2)) {
            return true;
        }

        if (!agg1.getFnName().equalsIgnoreCase(agg2.getFnName())) {
            return false;
        }
        
        if (agg1.isDistinct() != agg2.isDistinct()) {
            return false;
        }
        
        List<ScalarOperator> args1 = agg1.getArguments();
        List<ScalarOperator> args2 = agg2.getArguments();
        
        if (args1.size() != args2.size()) {
            return false;
        }
        
        // Check if all arguments are semantically equivalent
        for (int i = 0; i < args1.size(); i++) {
            if (!args1.get(i).equivalent(args2.get(i))) {
                return false;
            }
        }
        
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregation = (LogicalAggregationOperator) input.getOp();

        Map<CallOperator, ColumnRefOperator> revertAggMap = Maps.newHashMap();

        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        aggregation.getGroupingKeys().forEach(g -> projectMap.put(g, g));

        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregation.getAggregations().entrySet()) {
            // Find if there's a semantically equivalent aggregation already processed
            CallOperator foundEquivalent = null;
            for (CallOperator processed : revertAggMap.keySet()) {
                if (isSemanticallySameAggregation(entry.getValue(), processed)) {
                    foundEquivalent = processed;
                    break;
                }
            }
            
            if (foundEquivalent != null) {
                projectMap.put(entry.getKey(), revertAggMap.get(foundEquivalent));
            } else {
                projectMap.put(entry.getKey(), entry.getKey());
                revertAggMap.put(entry.getValue(), entry.getKey());
            }
        }

        LogicalProjectOperator projectOperator = new LogicalProjectOperator(projectMap);

        Map<ColumnRefOperator, CallOperator> newAggMap = Maps.newHashMap();
        revertAggMap.forEach((key, value) -> newAggMap.put(value, key));

        LogicalAggregationOperator newAggregation =
                new LogicalAggregationOperator(aggregation.getType(), aggregation.getGroupingKeys(), newAggMap);

        return Lists.newArrayList(OptExpression.create(projectOperator,
                OptExpression.create(newAggregation, input.getInputs())));
    }
}
