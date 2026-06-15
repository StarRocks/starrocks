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
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.Collections;
import java.util.List;

/**
 * Prunes the tablets of a cloud-native CHANGES scan by its distribution-column predicates, within
 * the partitions chosen by ChangesPartitionPruneRule, reusing the OLAP scan's tablet-pruning
 * algorithm over the base index of each selected partition. Only hash and range distribution can
 * prune; a random distribution returns every tablet.
 */
public class ChangesDistributionPruneRule extends TransformationRule {

    public ChangesDistributionPruneRule() {
        super(RuleType.TF_CHANGES_DISTRIBUTION_PRUNE, Pattern.create(OperatorType.LOGICAL_CHANGES_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalChangesScanOperator scan = (LogicalChangesScanOperator) input.getOp();
        LogicalChangesScanOperator newScan = OptDistributionPruner.pruneChangesScanTablets(scan);
        if (newScan.getSelectedTabletId().equals(scan.getSelectedTabletId())) {
            // No change: obey the optimizer rule contract and avoid an endless rewrite loop.
            return Collections.emptyList();
        }
        return Lists.newArrayList(OptExpression.create(newScan, input.getInputs()));
    }
}
