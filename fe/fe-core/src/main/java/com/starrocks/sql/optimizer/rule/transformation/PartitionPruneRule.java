// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Predicates;
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
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rewrite.PartitionColPredicateEvaluator;
import com.starrocks.sql.optimizer.rewrite.PartitionColPredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
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

        boolean disableTest =
                ConnectContext.get() == null || !ConnectContext.get().getSessionVariable().isEnableTestMode();
        Predicate<? super Partition> hasData = disableTest ? Partition::hasData : Predicates.alwaysTrue();
        if (selectedPartitionIds == null) {
            selectedPartitionIds =
                    table.getPartitions().stream().filter(hasData).map(Partition::getId).collect(
                            Collectors.toList());
            // some test cases need to perceive partitions pruned, so we can not filter empty partitions.
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> hasData.test(table.getPartition(id))).collect(Collectors.toList());
        }

        if (isNeedFurtherPrune(selectedPartitionIds, olapScanOperator, partitionInfo)) {
            PartitionColPredicateExtractor extractor = new PartitionColPredicateExtractor(
                    (RangePartitionInfo) partitionInfo, olapScanOperator.getColumnMetaToColRefMap());
            PartitionColPredicateEvaluator evaluator = new PartitionColPredicateEvaluator((RangePartitionInfo) partitionInfo,
                    selectedPartitionIds);
            selectedPartitionIds = evaluator.prunePartitions(extractor, olapScanOperator.getPredicate());
        }

        LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();

        return Lists.newArrayList(OptExpression.create(
                builder.withOperator(olapScanOperator).setSelectedPartitionId(selectedPartitionIds).build(),
                input.getInputs()));
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

    private boolean isNeedFurtherPrune(List<Long> candidatePartitions, LogicalOlapScanOperator olapScanOperator,
                                       PartitionInfo partitionInfo) {
        boolean probeResult = true;
        if (candidatePartitions.isEmpty()) {
            probeResult = false;
        } else if (partitionInfo.getType() != PartitionType.RANGE) {
            probeResult = false;
        } else if (((RangePartitionInfo) partitionInfo).getPartitionColumns().size() > 1) {
            probeResult = false;
        } else if (olapScanOperator.getPredicate() == null) {
            probeResult = false;
        }

        return probeResult;
    }

}
