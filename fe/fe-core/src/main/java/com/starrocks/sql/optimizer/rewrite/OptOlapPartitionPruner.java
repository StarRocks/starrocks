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


package com.starrocks.sql.optimizer.rewrite;

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
import com.starrocks.common.Pair;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
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

public class OptOlapPartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptOlapPartitionPruner.class);

    public static LogicalOlapScanOperator prunePartitions(LogicalOlapScanOperator olapScanOperator) {
        OlapTable table = (OlapTable) olapScanOperator.getTable();
        PartitionInfo partitionInfo = table.getPartitionInfo();

        List<Long> selectedPartitionIds = null;
        if (partitionInfo.getType() == PartitionType.RANGE) {
            selectedPartitionIds =
                    rangePartitionPrune(table, (RangePartitionInfo) partitionInfo, olapScanOperator);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            selectedPartitionIds =
                    listPartitionPrune((ListPartitionInfo) partitionInfo, olapScanOperator);
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

        final Pair<ScalarOperator, List<ScalarOperator>> prunePartitionPredicate =
                prunePartitionPredicates(olapScanOperator, selectedPartitionIds);

        final LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(olapScanOperator)
                .setSelectedPartitionId(selectedPartitionIds);

        if (prunePartitionPredicate != null) {
            builder.setPredicate(Utils.compoundAnd(prunePartitionPredicate.first))
                    .setPrunedPartitionPredicates(prunePartitionPredicate.second);
        }
        return builder.build();
    }

    private static Pair<ScalarOperator, List<ScalarOperator>> prunePartitionPredicates(
            LogicalOlapScanOperator logicalOlapScanOperator, List<Long> selectedPartitionIds) {
        List<ScalarOperator> scanPredicates = Utils.extractConjuncts(logicalOlapScanOperator.getPredicate());

        OlapTable table = (OlapTable) logicalOlapScanOperator.getTable();
        PartitionInfo tablePartitionInfo = table.getPartitionInfo();
        if (tablePartitionInfo.getType() != PartitionType.RANGE) {
            return null;
        }

        RangePartitionInfo partitionInfo = (RangePartitionInfo) tablePartitionInfo;
        if (partitionInfo.getPartitionColumns().size() != 1 || selectedPartitionIds.isEmpty()) {
            return null;
        }
        List<ScalarOperator> prunedPartitionPredicates = Lists.newArrayList();
        Map<String, PartitionColumnFilter> predicateRangeMap = Maps.newHashMap();

        String columnName = partitionInfo.getPartitionColumns().get(0).getName();
        Column column = logicalOlapScanOperator.getTable().getColumn(columnName);

        List<Range<PartitionKey>> partitionRanges =
                selectedPartitionIds.stream().map(partitionInfo::getRange).collect(Collectors.toList());

        PartitionKey minRange =
                Collections.min(partitionRanges.stream().map(Range::lowerEndpoint).collect(Collectors.toList()));
        PartitionKey maxRange =
                Collections.max(partitionRanges.stream().map(Range::upperEndpoint).collect(Collectors.toList()));

        for (ScalarOperator predicate : scanPredicates) {
            if (!Utils.containColumnRef(predicate, columnName)) {
                continue;
            }

            predicateRangeMap.clear();
            ColumnFilterConverter.convertColumnFilter(predicate, predicateRangeMap, table);

            if (predicateRangeMap.isEmpty()) {
                continue;
            }

            PartitionColumnFilter pcf = predicateRangeMap.get(columnName);

            // In predicate don't support predicate prune
            if (null != pcf.getInPredicateLiterals()) {
                continue;
            }

            // None/Null bound predicate can't prune
            if ((null == pcf.lowerBound || pcf.lowerBound.isConstantNull()) &&
                    (null == pcf.upperBound || pcf.upperBound.isConstantNull())) {
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
                prunedPartitionPredicates.add(predicate);
            }
        }

        if (prunedPartitionPredicates.isEmpty()) {
            return null;
        }

        scanPredicates.removeAll(prunedPartitionPredicates);
        return Pair.create(Utils.compoundAnd(scanPredicates), prunedPartitionPredicates);
    }

    private static void putValueMapItem(TreeMap<LiteralExpr, Set<Long>> partitionValueToIds,
                                 Long partitionId,
                                 LiteralExpr value) {
        Set<Long> partitionIdSet = partitionValueToIds.get(value);
        if (partitionIdSet == null) {
            partitionIdSet = new HashSet<>();
        }
        partitionIdSet.add(partitionId);
        partitionValueToIds.put(value, partitionIdSet);
    }

    private static List<Long> listPartitionPrune(ListPartitionInfo listPartitionInfo,
                                          LogicalOlapScanOperator olapScanOperator) {

        Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = new HashMap<>();
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = new HashMap<>();

        // single item list partition has only one column mapper
        Map<Long, List<LiteralExpr>> literalExprValuesMap = listPartitionInfo.getLiteralExprValues();
        if (literalExprValuesMap != null && literalExprValuesMap.size() > 0) {
            TreeMap<LiteralExpr, Set<Long>> partitionValueToIds = new TreeMap<>();
            literalExprValuesMap.forEach((partitionId, values) ->
                    values.forEach(value ->
                            putValueMapItem(partitionValueToIds, partitionId, value)));
            // single item list partition has only one column
            Column column = listPartitionInfo.getPartitionColumns().get(0);
            ColumnRefOperator columnRefOperator = olapScanOperator.getColumnReference(column);
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
                    List<List<LiteralExpr>> multiValues = entry.getValue();
                    for (List<LiteralExpr> values : multiValues) {
                        LiteralExpr value = values.get(i);
                        putValueMapItem(partitionValueToIds, partitionId, value);
                    }
                }
                Column column = columnList.get(i);
                ColumnRefOperator columnRefOperator = olapScanOperator.getColumnReference(column);
                columnToPartitionValuesMap.put(columnRefOperator, partitionValueToIds);
                columnToNullPartitions.put(columnRefOperator, new HashSet<>());
            }
        }

        List<ScalarOperator> scalarOperatorList = Utils.extractConjuncts(olapScanOperator.getPredicate());
        PartitionPruner partitionPruner = new ListPartitionPruner(columnToPartitionValuesMap,
                columnToNullPartitions, scalarOperatorList);
        try {
            return partitionPruner.prune();
        } catch (AnalysisException e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return null;
    }

    private static List<Long> rangePartitionPrune(OlapTable olapTable, RangePartitionInfo partitionInfo,
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

    private static boolean isNeedFurtherPrune(List<Long> candidatePartitions, LogicalOlapScanOperator olapScanOperator,
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
