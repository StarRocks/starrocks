// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
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
            predicatePrune(olapScanOperator, (RangePartitionInfo) partitionInfo, selectedPartitionIds);
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

    // Prune predicate if the data of partitions is meets the predicate, can be avoid execute predicate
    //
    // Note:
    //  Partition value range always be Left-Closed-Right-Open interval
    //
    // Attention:
    //  1. Only support single partition column
    //  2. Only support prune BinaryType predicate
    //
    // e.g.
    // select partitions:
    //  PARTITION p3 VALUES [2020-04-01, 2020-07-01)
    //  PARTITION p4 VALUES [2020-07-01, 2020-12-01)
    //
    // predicate:
    //  d = 2020-02-02 AND d > 2020-08-01, None prune
    //  d >= 2020-04-01 AND d > 2020-09-01, All Prune
    //  d >= 2020-04-01 AND d < 2020-09-01, "d >= 2020-04-01" prune, "d < 2020-09-01" not prune
    //  d IN (2020-05-01, 2020-06-01), None prune
    private void predicatePrune(LogicalOlapScanOperator operator, RangePartitionInfo partitionInfo,
                                Collection<Long> partitionIds) {
        if (partitionInfo.getPartitionColumns().size() != 1 || partitionIds.isEmpty()) {
            return;
        }

        List<ScalarOperator> allPredicate = Utils.extractConjuncts(operator.getPredicate());
        List<ScalarOperator> removePredicate = Lists.newArrayList();
        Map<String, PartitionColumnFilter> predicateRangeMap = Maps.newHashMap();

        String columnName = partitionInfo.getPartitionColumns().get(0).getName();
        Column column = operator.getTable().getColumn(columnName);

        List<Range<PartitionKey>> partitionRanges =
                partitionIds.stream().map(partitionInfo::getRange).collect(Collectors.toList());

        PartitionKey minRange =
                Collections.min(partitionRanges.stream().map(Range::lowerEndpoint).collect(Collectors.toList()));
        PartitionKey maxRange =
                Collections.max(partitionRanges.stream().map(Range::upperEndpoint).collect(Collectors.toList()));

        for (ScalarOperator predicate : allPredicate) {
            if (!Utils.containColumnRef(predicate, columnName)) {
                continue;
            }

            predicateRangeMap.clear();
            ColumnFilterConverter.convertColumnFilter(predicate, predicateRangeMap);

            if (predicateRangeMap.isEmpty()) {
                continue;
            }

            PartitionColumnFilter pcf = predicateRangeMap.get(columnName);

            // In predicate don't support predicate prune
            if (null != pcf.getInPredicateLiterals()) {
                continue;
            }

            // None bound predicate can't prune
            if (null == pcf.lowerBound && null == pcf.upperBound) {
                continue;
            }

            boolean lowerBind = true;
            boolean upperBind = true;
            if (null != pcf.lowerBound) {
                lowerBind = false;
                PartitionKey min = new PartitionKey();
                min.pushColumn(pcf.lowerBound, column.getPrimitiveType());
                int cmp = minRange.compareTo(min);
                if (cmp > 0 || (0 == cmp && pcf.lowerBoundInclusive)) {
                    lowerBind = true;
                }
            }

            if (null != pcf.upperBound) {
                upperBind = false;
                PartitionKey max = new PartitionKey();
                max.pushColumn(pcf.upperBound, column.getPrimitiveType());
                int cmp = maxRange.compareTo(max);
                if (cmp < 0 || (0 == cmp && !pcf.upperBoundInclusive)) {
                    upperBind = true;
                }
            }

            if (lowerBind && upperBind) {
                removePredicate.add(predicate);
            }
        }

        if (removePredicate.isEmpty()) {
            return;
        }

        allPredicate.removeAll(removePredicate);
        operator.setPredicate(Utils.compoundAnd(allPredicate));
    }
}
