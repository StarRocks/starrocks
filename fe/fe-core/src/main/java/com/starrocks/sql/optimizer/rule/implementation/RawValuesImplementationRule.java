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

package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRawValuesOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class RawValuesImplementationRule extends ImplementationRule {
    public RawValuesImplementationRule() {
        super(RuleType.IMP_RAW_VALUES, Pattern.create(OperatorType.LOGICAL_RAW_VALUES));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalRawValuesOperator logicalOp = (LogicalRawValuesOperator) input.getOp();

        PhysicalRawValuesOperator physicalOp = new PhysicalRawValuesOperator(
                logicalOp.getColumnRefSet(),
                logicalOp.getConstantType(),
                logicalOp.getRawConstantList(),
                logicalOp.getConstantCount()
        );

        return Lists.newArrayList(OptExpression.create(physicalOp, input.getInputs()));
    }
}
