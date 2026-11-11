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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rewrite.OptDistributionPruner;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.transformer.TabletClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class need to run after PartitionPruneRule
 * This class actually Prune the Olap table tablet ids.
 * <p>
 * Dependency predicate push down scan node
 */
public class DistributionPruneRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(DistributionPruneRule.class);

    public DistributionPruneRule() {
        super(RuleType.TF_DISTRIBUTION_PRUNE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) input.getOp();

        Preconditions.checkState(olapScanOperator.getHintsTabletIds() != null);
        List<Long> result;
        if (!olapScanOperator.getHintsTabletIds().isEmpty()) {
            result = namedTabletsOfSelectedPartitions(olapScanOperator);
        } else {
            result = OptDistributionPruner.pruneTabletIds(olapScanOperator,
                    olapScanOperator.getSelectedPartitionId());
        }

        if (result.equals(olapScanOperator.getSelectedTabletId())) {
            return Collections.emptyList();
        }

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        return Lists.newArrayList(OptExpression.create(
                builder.withOperator(olapScanOperator).setSelectedTabletId(result).build(),
                input.getInputs()));
    }

    // A TABLET clause names tablets of the partitions the scan reads, and partition pruning may still drop the
    // partition of one, because the WHERE clause rules it out or because it holds no data. Such a tablet holds nothing
    // the scan reads, so it is left out; once every named tablet is, the scan reads nothing.
    private static List<Long> namedTabletsOfSelectedPartitions(LogicalOlapScanOperator olapScanOperator) {
        OlapTable table = (OlapTable) olapScanOperator.getTable();
        List<Long> selectedPartitionIds = olapScanOperator.getSelectedPartitionId();
        List<Partition> selectedPartitions = Lists.newArrayListWithCapacity(selectedPartitionIds.size());
        for (Long partitionId : selectedPartitionIds) {
            selectedPartitions.add(table.getPartition(partitionId));
        }
        List<Long> namedTabletIds = olapScanOperator.getHintsTabletIds();
        Set<Long> selectedTabletIds = TabletClause.partitionsHolding(selectedPartitions,
                olapScanOperator.getSelectedIndexMetaId(), new HashSet<>(namedTabletIds)).keySet();
        List<Long> result = Lists.newArrayList();
        for (Long tabletId : namedTabletIds) {
            if (selectedTabletIds.contains(tabletId)) {
                result.add(tabletId);
            }
        }
        return result;
    }
}
