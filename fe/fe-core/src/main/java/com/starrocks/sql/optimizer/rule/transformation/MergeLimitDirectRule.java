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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Set;

public class MergeLimitDirectRule extends TransformationRule {
    private static final Set<OperatorType> SUPPORT_OPERATOR = ImmutableSet.<OperatorType>builder()
            .add(OperatorType.LOGICAL_OLAP_SCAN)
            .add(OperatorType.LOGICAL_VIEW_SCAN)
            .add(OperatorType.LOGICAL_HIVE_SCAN)
            .add(OperatorType.LOGICAL_ICEBERG_SCAN)
            .add(OperatorType.LOGICAL_HUDI_SCAN)
            .add(OperatorType.LOGICAL_DELTALAKE_SCAN)
            .add(OperatorType.LOGICAL_FILE_SCAN)
            .add(OperatorType.LOGICAL_PAIMON_SCAN)
            .add(OperatorType.LOGICAL_ODPS_SCAN)
            .add(OperatorType.LOGICAL_KUDU_SCAN)
            .add(OperatorType.LOGICAL_SCHEMA_SCAN)
            .add(OperatorType.LOGICAL_MYSQL_SCAN)
            .add(OperatorType.LOGICAL_ES_SCAN)
            .add(OperatorType.LOGICAL_JDBC_SCAN)
            .add(OperatorType.LOGICAL_ICEBERG_METADATA_SCAN)
            .add(OperatorType.LOGICAL_AGGR)
            .add(OperatorType.LOGICAL_WINDOW)
            .add(OperatorType.LOGICAL_INTERSECT)
            .add(OperatorType.LOGICAL_EXCEPT)
            .add(OperatorType.LOGICAL_VALUES)
            .add(OperatorType.LOGICAL_FILTER)
            .add(OperatorType.LOGICAL_TABLE_FUNCTION)
            .add(OperatorType.LOGICAL_TABLE_FUNCTION_TABLE_SCAN)
            .add(OperatorType.LOGICAL_CTE_CONSUME)
            .build();

    public MergeLimitDirectRule() {
        super(RuleType.TF_MERGE_LIMIT_DIRECT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(MultiOpPattern.of(SUPPORT_OPERATOR)
                        .addChildren(Pattern.create(OperatorType.PATTERN_MULTI_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        return limit.isLocal();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalLimitOperator limit = (LogicalLimitOperator) input.getOp();
        Preconditions.checkState(!limit.hasOffset());
        LogicalOperator op = (LogicalOperator) input.getInputs().get(0).getOp();
        op.setLimit(limit.getLimit());

        return Lists.newArrayList(OptExpression.create(op, input.getInputs().get(0).getInputs()));
    }
}
