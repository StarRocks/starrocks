// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.DistributionPruner;
import com.starrocks.planner.HashDistributionPruner;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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

        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();

        List<Long> result = Lists.newArrayList();
        for (Long partitionId : olapScanOperator.getSelectedPartitionId()) {
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedIndex table = partition.getIndex(olapScanOperator.getSelectedIndexId());
            Collection<Long> tabletIds = distributionPrune(table, partition.getDistributionInfo(), olapScanOperator);
            result.addAll(tabletIds);
        }

        // prune hint tablet
        Preconditions.checkState(olapScanOperator.getHintsTabletIds() != null);
        if (!olapScanOperator.getHintsTabletIds().isEmpty()) {
            result.retainAll(olapScanOperator.getHintsTabletIds());
        }

        if (result.equals(olapScanOperator.getSelectedTabletId())) {
            return Collections.emptyList();
        }

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        return Lists.newArrayList(OptExpression.create(
                builder.withOperator(olapScanOperator).setSelectedTabletId(result).build(),
                input.getInputs()));
    }

    private Collection<Long> distributionPrune(MaterializedIndex index, DistributionInfo distributionInfo,
                                               LogicalOlapScanOperator operator) {
        try {
            DistributionPruner distributionPruner;
            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                distributionPruner = new HashDistributionPruner(index.getTabletIdsInOrder(),
                        info.getDistributionColumns(),
                        operator.getColumnFilters(),
                        info.getBucketNum());
                return distributionPruner.prune();
            }
        } catch (AnalysisException e) {
            LOG.warn("distribution prune failed. ", e);
        }

        return index.getTabletIdsInOrder();
    }
}
