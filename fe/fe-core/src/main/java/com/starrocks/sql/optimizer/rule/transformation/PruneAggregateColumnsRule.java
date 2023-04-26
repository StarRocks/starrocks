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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PruneAggregateColumnsRule extends TransformationRule {
    public PruneAggregateColumnsRule() {
        super(RuleType.TF_PRUNE_AGG_COLUMNS, Pattern.create(OperatorType.LOGICAL_AGGR).
                addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggOperator = (LogicalAggregationOperator) input.getOp();

        ColumnRefSet requiredInputColumns = new ColumnRefSet(aggOperator.getGroupingKeys());

        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        // Agg required input provide the having used columns
        if (aggOperator.getPredicate() != null) {
            requiredInputColumns.union(aggOperator.getPredicate().getUsedColumns());
            // For SQL: SELECT 8 from t0 group by v1 having avg(v2) < 63;
            // We need `requiredOutputColumns` early union having used columns, in order to
            // don't prune avg(v2)
            requiredOutputColumns.union(aggOperator.getPredicate().getUsedColumns());
        }

        Map<ColumnRefOperator, CallOperator> newAggregations = Maps.newHashMap();

        Map<ColumnRefOperator, CallOperator> aggregations =
                aggOperator.getAggregations();
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregations.entrySet()) {
            if (requiredOutputColumns.contains(kv.getKey())) {
                fillRequiredInputColumnsAndNewAggregations(kv, requiredInputColumns, newAggregations);
            }
        }

        // For SQL: SELECT 1 FROM (SELECT COUNT(2) FROM testData) t;
        // The requiredInputColumns will be empty, but for get the right result in execute engine
        // 1. we should at least have one requiredInputColumn
        // 2. we should at least have one aggregate function
        if (requiredInputColumns.isEmpty()) {
            Preconditions.checkState(!aggregations.isEmpty());
            Optional<Map.Entry<ColumnRefOperator, CallOperator>> optKv = aggregations.entrySet().stream().findFirst();
            Preconditions.checkState(optKv.isPresent());
            Map.Entry<ColumnRefOperator, CallOperator> kv = optKv.get();
            fillRequiredInputColumnsAndNewAggregations(kv, requiredInputColumns, newAggregations);
        }

        // Change the requiredOutputColumns in context
        requiredOutputColumns.union(requiredInputColumns);

        if (newAggregations.keySet().equals(aggregations.keySet())) {
            return Collections.emptyList();
        }
        LogicalAggregationOperator newAggOperator = new LogicalAggregationOperator.Builder().withOperator(aggOperator)
                .setType(AggType.GLOBAL)
                .setAggregations(newAggregations)
                .build();

        return Lists.newArrayList(OptExpression.create(newAggOperator, input.getInputs()));
    }

    private void fillRequiredInputColumnsAndNewAggregations(Map.Entry<ColumnRefOperator, CallOperator> kv,
                                                            ColumnRefSet requiredInputColumns,
                                                            Map<ColumnRefOperator, CallOperator> newAggregations) {
        if (!kv.getValue().isCountStar()) {
            // Agg required input provide the output aggregate columns
            requiredInputColumns.union(kv.getValue().getUsedColumns());
        }
        newAggregations.put(kv.getKey(), kv.getValue());
    }
}
