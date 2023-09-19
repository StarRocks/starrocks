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

import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Set;

// optimization for select min/max/count distinct dt from table where predicates
// and predicates only has partition columns.

public class PartitionColumnValueOnlyOnScanRule extends TransformationRule {
    public PartitionColumnValueOnlyOnScanRule() {
        // agg -> project -> scan[checked in `check`]
        super(RuleType.TF_REWRITE_PARTITION_COLUMN_ONLY_AGG,
                Pattern.create(OperatorType.LOGICAL_AGGR).addChildren(Pattern.create(OperatorType.LOGICAL_PROJECT)));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) input.getOp();
        Operator operator = input.getInputs().get(0).getInputs().get(0).getOp();
        if (!(operator instanceof LogicalScanOperator)) {
            return false;
        }
        LogicalScanOperator scanOperator = (LogicalScanOperator) operator;

        // we can only apply this rule to the queries met all the following conditions:
        // 1. only contain MIN/MAX/COUNT DISTINCT agg functions
        // 2. scan operator only output partition columns.(right now we only support 1 column)
        if (!singlePartitionColumnInScanOperator(scanOperator)) {
            return false;
        }

        boolean allValid = aggregationOperator.getAggregations().values().stream().allMatch(aggregator -> {
            AggregateFunction aggregateFunction = (AggregateFunction) aggregator.getFunction();
            String functionName = aggregateFunction.functionName();

            // min/max/count distinct(a)
            if (!(functionName.equals(FunctionSet.MAX) || functionName.equals(FunctionSet.MIN) ||
                    (functionName.equals(FunctionSet.COUNT) && aggregator.isDistinct()))) {
                return false;
            }

            return true;
        });
        return allValid;
    }

    private static boolean singlePartitionColumnInScanOperator(LogicalScanOperator scanOperator) {
        int pc = 0;
        Set<String> partitionColumns = scanOperator.getPartitionColumns();
        for (ColumnRefOperator c : scanOperator.getOutputColumns()) {
            if (!partitionColumns.contains(c.getName())) {
                // materialized column could be taken durding prune stage.
                if (!scanOperator.getScanOptimzeOption().getCanUseAnyColumn()) {
                    return false;
                }
            } else {
                pc += 1;
            }
        }
        return pc == 1;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getInputs().get(0).getInputs().get(0).getOp();
        scanOperator.getScanOptimzeOption().setUsePartitionColumnValueOnly(true);
        return Collections.emptyList();
    }
}
