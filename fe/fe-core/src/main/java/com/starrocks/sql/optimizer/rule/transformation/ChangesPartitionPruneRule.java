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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalChangesScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rewrite.OptOlapPartitionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

import static com.starrocks.sql.optimizer.operator.OpRuleBit.OP_PARTITION_PRUNED;

/**
 * Prunes the logical partitions of a cloud-native CHANGES scan by its partition-column predicates,
 * reusing the OLAP scan's partition-pruning algorithm over the bookmark-scoped table. That scoped
 * table exposes only the delta's partitions, so the surviving set is the predicate-selected
 * partitions intersected with the delta partitions.
 */
public class ChangesPartitionPruneRule extends TransformationRule {

    public ChangesPartitionPruneRule() {
        super(RuleType.TF_CHANGES_PARTITION_PRUNE, Pattern.create(OperatorType.LOGICAL_CHANGES_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalChangesScanOperator scan = (LogicalChangesScanOperator) input.getOp();
        if (scan.isOpRuleBitSet(OP_PARTITION_PRUNED)) {
            return Collections.emptyList();
        }
        LogicalChangesScanOperator newScan = OptOlapPartitionPruner.pruneChangesScanPartitions(scan);
        newScan.setOpRuleBit(OP_PARTITION_PRUNED);
        return Lists.newArrayList(OptExpression.create(newScan, input.getInputs()));
    }
}
