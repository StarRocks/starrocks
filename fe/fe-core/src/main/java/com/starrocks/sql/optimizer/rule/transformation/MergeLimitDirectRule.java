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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

public class MergeLimitDirectRule extends TransformationRule {
    public static final MergeLimitDirectRule AGGREGATE = new MergeLimitDirectRule(OperatorType.LOGICAL_AGGR);
    public static final MergeLimitDirectRule OLAP_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_OLAP_SCAN);
    public static final MergeLimitDirectRule VIEW_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_VIEW_SCAN);
    public static final MergeLimitDirectRule HIVE_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final MergeLimitDirectRule ICEBERG_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    public static final MergeLimitDirectRule HUDI_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_HUDI_SCAN);
    public static final MergeLimitDirectRule DELTALAKE_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_DELTALAKE_SCAN);
    public static final MergeLimitDirectRule FILE_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_FILE_SCAN);
    public static final MergeLimitDirectRule PAIMON_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_PAIMON_SCAN);
    public static final MergeLimitDirectRule ODPS_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_ODPS_SCAN);
    public static final MergeLimitDirectRule SCHEMA_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_SCHEMA_SCAN);
    public static final MergeLimitDirectRule MYSQL_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_MYSQL_SCAN);
    public static final MergeLimitDirectRule ES_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_ES_SCAN);
    public static final MergeLimitDirectRule JDBC_SCAN = new MergeLimitDirectRule(OperatorType.LOGICAL_JDBC_SCAN);
    public static final MergeLimitDirectRule WINDOW = new MergeLimitDirectRule(OperatorType.LOGICAL_WINDOW);
    public static final MergeLimitDirectRule INTERSECT = new MergeLimitDirectRule(OperatorType.LOGICAL_INTERSECT);
    public static final MergeLimitDirectRule EXCEPT = new MergeLimitDirectRule(OperatorType.LOGICAL_EXCEPT);
    public static final MergeLimitDirectRule VALUES = new MergeLimitDirectRule(OperatorType.LOGICAL_VALUES);
    public static final MergeLimitDirectRule FILTER = new MergeLimitDirectRule(OperatorType.LOGICAL_FILTER);
    public static final MergeLimitDirectRule TABLE_FUNCTION =
            new MergeLimitDirectRule(OperatorType.LOGICAL_TABLE_FUNCTION);
    public static final MergeLimitDirectRule TABLE_FUNCTION_TABLE_SCAN =
            new MergeLimitDirectRule(OperatorType.LOGICAL_TABLE_FUNCTION_TABLE_SCAN);

    private MergeLimitDirectRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_MERGE_LIMIT_DIRECT, Pattern.create(OperatorType.LOGICAL_LIMIT)
                .addChildren(Pattern.create(logicalOperatorType, OperatorType.PATTERN_MULTI_LEAF)));
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
