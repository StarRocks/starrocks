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

package com.starrocks.sql.optimizer.rule.implementation.stream;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class StreamScanImplementationRule extends StreamImplementationRule {

    private static final StreamScanImplementationRule INSTANCE =
            new StreamScanImplementationRule(RuleType.IMP_BINLOG_SCAN);

    public static StreamScanImplementationRule getInstance() {
        return INSTANCE;
    }

    private StreamScanImplementationRule(RuleType type) {
        super(type, Pattern.create(OperatorType.LOGICAL_BINLOG_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator logical = (LogicalScanOperator) input.getOp();
        PhysicalStreamScanOperator physical = new PhysicalStreamScanOperator(
                logical.getTable(),
                logical.getColRefToColumnMetaMap(),
                logical.getPredicate(),
                logical.getProjection());
        return Lists.newArrayList(OptExpression.create(physical, input.getInputs()));
    }
}
