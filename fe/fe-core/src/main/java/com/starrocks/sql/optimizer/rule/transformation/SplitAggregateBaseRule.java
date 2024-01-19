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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class SplitAggregateBaseRule extends TransformationRule {
    protected SplitAggregateBaseRule(RuleType ruleType) {
        super(ruleType, Pattern.create(OperatorType.LOGICAL_AGGR, OperatorType.PATTERN_LEAF));
    }

    protected Map<ColumnRefOperator, CallOperator> createNormalAgg(AggType aggType,
                                                                   Map<ColumnRefOperator, CallOperator> aggregationMap) {
        Map<ColumnRefOperator, CallOperator> newAggregationMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationMap.entrySet()) {
            ColumnRefOperator column = entry.getKey();
            CallOperator aggregation = entry.getValue();

            CallOperator callOperator;
            Type intermediateType = getIntermediateType(aggregation);
            // For merge agg function, we need to replace the agg input args to the update agg function result
            if (aggType.isGlobal()) {
                List<ScalarOperator> arguments =
                        Lists.newArrayList(new ColumnRefOperator(column.getId(), intermediateType, column.getName(),
                                column.isNullable()));
                appendConstantColumns(arguments, aggregation);
                callOperator = new CallOperator(
                        aggregation.getFnName(),
                        aggregation.getType(),
                        arguments,
                        aggregation.getFunction());
            } else {
                callOperator = new CallOperator(aggregation.getFnName(), intermediateType,
                        aggregation.getChildren(), aggregation.getFunction(),
                        aggregation.isDistinct(), aggregation.isRemovedDistinct());
            }

            newAggregationMap.put(
                    new ColumnRefOperator(column.getId(), column.getType(), column.getName(), column.isNullable()),
                    callOperator);
        }

        return newAggregationMap;
    }

    // For Multi args aggregation functions, if they have const args,
    // We should also pass the const args to merge phase aggregator for performance.
    // For example, for intersect_count(user_id, dt, '20191111'),
    // We should pass '20191111' to update and merge phase aggregator in BE both.
    protected static void appendConstantColumns(List<ScalarOperator> arguments, CallOperator aggregation) {
        if (aggregation.getChildren().size() > 1) {
            aggregation.getChildren().stream().filter(ScalarOperator::isConstantRef).forEach(arguments::add);
        }
    }

    protected Optional<List<ColumnRefOperator>> extractCommonDistinctCols(Collection<CallOperator> aggCallOperators) {
        List<ColumnRefOperator> distinctChildren = Lists.newArrayList();
        for (CallOperator callOperator : aggCallOperators) {
            if (callOperator.isDistinct()) {
                if (distinctChildren.isEmpty()) {
                    distinctChildren = callOperator.getDistinctChildren().stream()
                            .filter(e -> e.isColumnRef())
                            .map(e -> (ColumnRefOperator) e)
                            .collect(Collectors.toList());
                } else {
                    List<ColumnRefOperator> nextDistinctChildren = callOperator.getDistinctChildren().stream()
                            .filter(e -> e.isColumnRef())
                            .map(e -> (ColumnRefOperator) e)
                            .collect(Collectors.toList());
                    if (!Objects.equal(distinctChildren, nextDistinctChildren)) {
                        return Optional.empty();
                    }
                }
            }
        }
        return Optional.of(distinctChildren);
    }

    private Type getIntermediateType(CallOperator aggregation) {
        AggregateFunction af = (AggregateFunction) aggregation.getFunction();
        Preconditions.checkState(af != null);
        return af.getIntermediateType() == null ? af.getReturnType() : af.getIntermediateType();
    }
}
