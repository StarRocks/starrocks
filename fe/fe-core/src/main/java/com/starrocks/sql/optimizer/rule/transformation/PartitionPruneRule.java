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

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;

/**
 * This class does:
 * 1. Prune the Olap table partition ids, Dependency predicates push down scan node
 * 2. Prune predicate if the data of partitions meets the predicate, to avoid execute predicate.
 * <p>
 * Note:
 * Partition value range always be Left-Closed-Right-Open interval
 * <p>
 * Attention:
 * 1. Only support single partition column
 * 2. Only support prune BinaryType predicate
 * <p>
 * e.g.
 * select partitions:
 * PARTITION p3 VALUES [2020-04-01, 2020-07-01)
 * PARTITION p4 VALUES [2020-07-01, 2020-12-01)
 * <p>
 * predicate:
 * d = 2020-02-02 AND d > 2020-08-01, None prune
 * d >= 2020-04-01 AND d > 2020-09-01, All Prune
 * d >= 2020-04-01 AND d < 2020-09-01, "d >= 2020-04-01" prune, "d < 2020-09-01" not prune
 * d IN (2020-05-01, 2020-06-01), None prune
 */
public class PartitionPruneRule extends TransformationRule {

    public PartitionPruneRule() {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        Operator op = input.getOp();
        // if the partition id is already selected, no need to prune again
        if (Utils.isOpAppliedRule(op, Operator.OP_PARTITION_PRUNE_BIT)) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator logicalOlapScanOperator = (LogicalOlapScanOperator) input.getOp();
        LogicalOlapScanOperator prunedOlapScanOperator = null;
        if (logicalOlapScanOperator.getSelectedPartitionId() == null) {
            prunedOlapScanOperator = OptOlapPartitionPruner.prunePartitions(logicalOlapScanOperator);
        } else {
            // do merge pruned partitions with new pruned partitions
            prunedOlapScanOperator = OptOlapPartitionPruner.mergePartitionPrune(logicalOlapScanOperator);
        }
        Utils.setOpAppliedRule(prunedOlapScanOperator, Operator.OP_PARTITION_PRUNE_BIT);
        return Lists.newArrayList(OptExpression.create(prunedOlapScanOperator, input.getInputs()));
    }
}
