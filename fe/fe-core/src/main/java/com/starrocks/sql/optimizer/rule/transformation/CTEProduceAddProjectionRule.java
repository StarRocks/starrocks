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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

public class CTEProduceAddProjectionRule extends TransformationRule {

    public CTEProduceAddProjectionRule() {
        super(RuleType.TF_CTE_ADD_PROJECTION,
                Pattern.create(OperatorType.LOGICAL_CTE_PRODUCE, OperatorType.LOGICAL_REPEAT));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        final OptExpression repeatOpt = input.getInputs().get(0);
        final LogicalRepeatOperator repeat = (LogicalRepeatOperator) repeatOpt.getOp();
        if (repeat.getProjection() == null) {
            final RowOutputInfo rowOutputInfo = repeatOpt.getInputs().get(0).getRowOutputInfo();
            final ImmutableMap.Builder<ColumnRefOperator, ScalarOperator> builder = ImmutableMap.builder();

            for (ColumnOutputInfo columnOutputInfo : rowOutputInfo.getColumnOutputInfo()) {
                final ColumnRefOperator columnRef = columnOutputInfo.getColumnRef();
                builder.put(columnRef, columnRef);
            }

            // add grouping id to projection
            for (ColumnRefOperator columnRefOperator : repeat.getOutputGrouping()) {
                builder.put(columnRefOperator, columnRefOperator);
            }
            repeat.setProjection(new Projection(builder.build()));
            return Lists.newArrayList(input);
        } else {
            return Collections.emptyList();
        }
    }
}
