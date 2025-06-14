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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_PARTITION_PRUNED;

public class ExternalScanPartitionPruneRule extends TransformationRule {

    private static final ImmutableSet<OperatorType> SUPPORT = ImmutableSet.of(
            OperatorType.LOGICAL_HIVE_SCAN,
            OperatorType.LOGICAL_HUDI_SCAN,
            OperatorType.LOGICAL_ICEBERG_SCAN,
            OperatorType.LOGICAL_DELTALAKE_SCAN,
            OperatorType.LOGICAL_FILE_SCAN,
            OperatorType.LOGICAL_ES_SCAN,
            OperatorType.LOGICAL_PAIMON_SCAN,
            OperatorType.LOGICAL_ODPS_SCAN,
            OperatorType.LOGICAL_KUDU_SCAN
    );

    public ExternalScanPartitionPruneRule() {
        super(RuleType.TF_PARTITION_PRUNE, MultiOpPattern.of(SUPPORT));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator operator = (LogicalScanOperator) input.getOp();
        if (operator.isOpRuleBitSet(OP_PARTITION_PRUNED)) {
            return Collections.emptyList();
        }
        OptExternalPartitionPruner.prunePartitions(context, operator);
        operator.setOpRuleBit(OP_PARTITION_PRUNED);
        return Lists.newArrayList();
    }
}
