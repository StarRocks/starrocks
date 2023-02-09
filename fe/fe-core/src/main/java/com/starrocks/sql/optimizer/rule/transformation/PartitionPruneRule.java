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
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.PartitionColPredicateEvaluator;
import com.starrocks.sql.optimizer.rewrite.PartitionColPredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
            selectedPartitionIds = rangePartitionPrune(table, (RangePartitionInfo) partitionInfo, olapScanOperator);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            selectedPartitionIds = listPartitionPrune(table, (ListPartitionInfo) partitionInfo, olapScanOperator);
        }

        if (selectedPartitionIds == null) {
            selectedPartitionIds =
                    table.getPartitions().stream().filter(Partition::hasData).map(Partition::getId).collect(
                            Collectors.toList());
            // some test cases need to perceive partitions pruned, so we can not filter empty partitions.
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> table.getPartition(id).hasData()).collect(Collectors.toList());
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

    private void putValueMapItem(TreeMap<LiteralExpr, Set<Long>> partitionValueToIds,
                                 Long partitionId,
                                 LiteralExpr value) {
        Set<Long> partitionIdSet = partitionValueToIds.get(value);
        if (partitionIdSet == null) {
            partitionIdSet = new HashSet<>();
        }
        partitionIdSet.add(partitionId);
        partitionValueToIds.put(value, partitionIdSet);
    }

    private List<Long> listPartitionPrune(OlapTable olapTable, ListPartitionInfo listPartitionInfo,
                                          LogicalOlapScanOperator operator) {

        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = new HashMap<>();
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = new HashMap<>();

        // Currently queries either specify a temporary partition, or do not. There is no situation
        // where two partitions are checked at the same time
        boolean isTemporaryPartitionPrune = false;
        // single item list partition has only one column mapper
        Map<Long, List<LiteralExpr>> literalExprValuesMap = listPartitionInfo.getLiteralExprValues();
        List<Long> partitionIds = Lists.newArrayList();
        if (operator.getPartitionNames() != null) {
            for (String partName : operator.getPartitionNames().getPartitionNames()) {
                boolean isTemp = operator.getPartitionNames().isTemp();
                if (isTemp) {
                    isTemporaryPartitionPrune = true;
                }
                Partition part = olapTable.getPartition(partName, isTemp);
                if (part == null) {
                    continue;
                }
                partitionIds.add(part.getId());
            }
        } else {
            partitionIds = listPartitionInfo.getPartitionIds(false);
        }

        if (literalExprValuesMap != null && literalExprValuesMap.size() > 0) {
            TreeMap<LiteralExpr, Set<Long>> partitionValueToIds = new TreeMap<>();
            for (Map.Entry<Long, List<LiteralExpr>> entry : literalExprValuesMap.entrySet()) {
                Long partitionId = entry.getKey();
                if (!partitionIds.contains(partitionId)) {
                    continue;
                }
                List<LiteralExpr> values = entry.getValue();
                values.forEach(value ->
                        putValueMapItem(partitionValueToIds, partitionId, value));
            }
            // single item list partition has only one column
            Column column = listPartitionInfo.getPartitionColumns().get(0);
            ColumnRefOperator columnRefOperator = operator.getColumnReference(column);
            columnToPartitionValuesMap.put(columnRefOperator, partitionValueToIds);
            columnToNullPartitions.put(columnRefOperator, new HashSet<>());
        }

        // multiItem list partition mapper
        Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues = listPartitionInfo.getMultiLiteralExprValues();
        if (multiLiteralExprValues != null && multiLiteralExprValues.size() > 0) {
            List<Column> columnList = listPartitionInfo.getPartitionColumns();
            for (int i = 0; i < columnList.size(); i++) {
                TreeMap<LiteralExpr, Set<Long>> partitionValueToIds = new TreeMap<>();
                for (Map.Entry<Long, List<List<LiteralExpr>>> entry : multiLiteralExprValues.entrySet()) {
                    Long partitionId = entry.getKey();
                    if (!partitionIds.contains(partitionId)) {
                        continue;
                    }
                    List<List<LiteralExpr>> multiValues = entry.getValue();
                    for (List<LiteralExpr> values : multiValues) {
                        LiteralExpr value = values.get(i);
                        putValueMapItem(partitionValueToIds, partitionId, value);
                    }
                }
                Column column = columnList.get(i);
                ColumnRefOperator columnRefOperator = operator.getColumnReference(column);
                columnToPartitionValuesMap.put(columnRefOperator, partitionValueToIds);
                columnToNullPartitions.put(columnRefOperator, new HashSet<>());
            }
        }

        List<ScalarOperator> scalarOperatorList = Utils.extractConjuncts(operator.getPredicate());
        PartitionPruner partitionPruner = new ListPartitionPruner(columnToPartitionValuesMap,
                columnToNullPartitions, scalarOperatorList);
        try {
            List<Long> prune = partitionPruner.prune();
            if (prune == null && isTemporaryPartitionPrune)  {
                return partitionIds;
            } else {
                return prune;
            }
        } catch (AnalysisException e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return null;
    }

    private List<Long> rangePartitionPrune(OlapTable olapTable, RangePartitionInfo partitionInfo,
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
