// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class actually Prune the Olap table partition ids.
 * <p>
 * Dependency predicate push down scan node
 */
public class PartitionPruneRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(PartitionPruneRule.class);

    public PartitionPruneRule() {
        super(RuleType.TF_PARTITION_PRUNE, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) input.getOp();
        if (olapScanOperator.getSelectedPartitionId() != null) {
            return Collections.emptyList();
        }

        OlapTable table = (OlapTable) olapScanOperator.getTable();

        PartitionInfo partitionInfo = table.getPartitionInfo();
        List<Long> selectedPartitionIds = null;

        if (partitionInfo.getType() == PartitionType.RANGE) {
            selectedPartitionIds =
                    partitionPrune(table, (RangePartitionInfo) partitionInfo, olapScanOperator);
        }

        if (selectedPartitionIds == null) {
            selectedPartitionIds =
                    table.getPartitions().stream().filter(Partition::hasData).map(Partition::getId).collect(
                            Collectors.toList());
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> table.getPartition(id).hasData()).collect(Collectors.toList());
        }

        return Lists.newArrayList(OptExpression.create(new LogicalOlapScanOperator(
                olapScanOperator.getTable(),
                olapScanOperator.getOutputColumns(),
                olapScanOperator.getColRefToColumnMetaMap(),
                olapScanOperator.getColumnMetaToColRefMap(),
                olapScanOperator.getDistributionSpec(),
                olapScanOperator.getLimit(),
                olapScanOperator.getPredicate(),
                olapScanOperator.getSelectedIndexId(),
                selectedPartitionIds,
                olapScanOperator.getPartitionNames(),
                olapScanOperator.getSelectedTabletId(),
                olapScanOperator.getHintsTabletIds()), input.getInputs()));
    }

    private List<Long> partitionPrune(OlapTable olapTable, RangePartitionInfo partitionInfo,
                                            LogicalOlapScanOperator operator) {
        Map<Long, Range<PartitionKey>> keyRangeById;
        if (operator.getPartitionNames() != null) {
            keyRangeById = Maps.newHashMap();
            for (String partName : operator.getPartitionNames().getPartitionNames()) {
                Partition part = olapTable.getPartition(partName, operator.getPartitionNames().isTemp());
                if (part == null) {
                    continue;
                }
                keyRangeById.put(part.getId(), partitionInfo.getRange(part.getId()));
            }
        } else {
            keyRangeById = partitionInfo.getIdToRange(false);
        }
        PartitionPruner partitionPruner = new RangePartitionPruner(keyRangeById,
                partitionInfo.getPartitionColumns(), operator.getColumnFilters());
        try {
            return partitionPruner.prune();
        } catch (AnalysisException e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return null;
    }
}
