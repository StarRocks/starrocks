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

package com.starrocks.sql.optimizer.rule.ivm;

import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IvmDeltaFilterRule extends TransformationRule {
    public IvmDeltaFilterRule() {
        super(RuleType.TF_IVM_DELTA_FILTER,
                Pattern.create(OperatorType.LOGICAL_DELTA)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_FILTER, OperatorType.PATTERN_LEAF)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalDeltaOperator delta = (LogicalDeltaOperator) input.getOp();
        LogicalFilterOperator filter = (LogicalFilterOperator) input.inputAt(0).getOp();

        LogicalFilterOperator newFilter = filter;
        ColumnRefOperator actionColumn = delta.getActionColumn();
        if (actionColumn != null) {
            Projection filterProjection = filter.getProjection();
            Map<ColumnRefOperator, ScalarOperator> projectionMap;
            Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;
            if (filterProjection != null) {
                projectionMap = Maps.newHashMap(filterProjection.getColumnRefMap());
                commonSubOperatorMap = filterProjection.getCommonSubOperatorMap();
            } else {
                projectionMap = input.inputAt(0).getOutputColumns().getColumnRefOperators(context.getColumnRefFactory())
                        .stream()
                        .collect(Collectors.toMap(Function.identity(), Function.identity(),
                                (left, right) -> left, Maps::newHashMap));
                commonSubOperatorMap = Maps.newHashMap();
            }
            projectionMap.put(actionColumn, actionColumn);
            Projection newProjection = new Projection(projectionMap, commonSubOperatorMap);
            newFilter = new LogicalFilterOperator.Builder()
                    .withOperator(filter)
                    .setProjection(newProjection)
                    .build();
        }

        OptExpression child = input.inputAt(0).inputAt(0);
        OptExpression rewritten = OptExpression.create(newFilter, OptExpression.create(delta, child));
        return List.of(rewritten);
    }
}
