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

import com.clearspring.analytics.util.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ExternalScanPartitionPruneRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(ExternalScanPartitionPruneRule.class);

    public static final ExternalScanPartitionPruneRule HIVE_SCAN =
            new ExternalScanPartitionPruneRule(OperatorType.LOGICAL_HIVE_SCAN);
    public static final ExternalScanPartitionPruneRule HUDI_SCAN =
            new ExternalScanPartitionPruneRule(OperatorType.LOGICAL_HUDI_SCAN);
    public static final ExternalScanPartitionPruneRule ICEBERG_SCAN =
            new ExternalScanPartitionPruneRule(OperatorType.LOGICAL_ICEBERG_SCAN);
    public static final ExternalScanPartitionPruneRule DELTALAKE_SCAN =
            new ExternalScanPartitionPruneRule(OperatorType.LOGICAL_DELTALAKE_SCAN);
    public static final ExternalScanPartitionPruneRule FILE_SCAN =
            new ExternalScanPartitionPruneRule(OperatorType.LOGICAL_FILE_SCAN);
    public static final ExternalScanPartitionPruneRule ES_SCAN =
            new ExternalScanPartitionPruneRule(OperatorType.LOGICAL_ES_SCAN);
    public static final ExternalScanPartitionPruneRule PAIMON_SCAN =
            new ExternalScanPartitionPruneRule(OperatorType.LOGICAL_PAIMON_SCAN);

    public ExternalScanPartitionPruneRule(OperatorType logicalOperatorType) {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(logicalOperatorType));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator operator = (LogicalScanOperator) input.getOp();
        OptExternalPartitionPruner.prunePartitions(context, operator);
        return Lists.newArrayList();
    }
}
