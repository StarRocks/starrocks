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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.FunctionSet;
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
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class OptOlapPartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptOlapPartitionPruner.class);

    public static LogicalOlapScanOperator prunePartitions(LogicalOlapScanOperator logicalOlapScanOperator) {
        List<Long> selectedPartitionIds = null;
        OlapTable table = (OlapTable) logicalOlapScanOperator.getTable();

        PartitionInfo partitionInfo = table.getPartitionInfo();

        if (partitionInfo.isRangePartition()) {
            selectedPartitionIds = rangePartitionPrune(table, (RangePartitionInfo) partitionInfo, logicalOlapScanOperator);
        } else if (partitionInfo.getType() == PartitionType.LIST) {
            selectedPartitionIds = listPartitionPrune(table, (ListPartitionInfo) partitionInfo, logicalOlapScanOperator);
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

        if (isNeedFurtherPrune(selectedPartitionIds, logicalOlapScanOperator, partitionInfo)) {
            PartitionColPredicateExtractor extractor = new PartitionColPredicateExtractor(
                    (RangePartitionInfo) partitionInfo, logicalOlapScanOperator.getColumnMetaToColRefMap());
            PartitionColPredicateEvaluator evaluator = new PartitionColPredicateEvaluator((RangePartitionInfo) partitionInfo,
                    selectedPartitionIds);
            selectedPartitionIds = evaluator.prunePartitions(extractor, logicalOlapScanOperator.getPredicate());
        }

        final Pair<ScalarOperator, List<ScalarOperator>> prunePartitionPredicate =
                prunePartitionPredicates(logicalOlapScanOperator, selectedPartitionIds);

        final LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(logicalOlapScanOperator)
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
        if (!tablePartitionInfo.isRangePartition()) {
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

        // we convert range to [minRange, maxRange]
        PartitionKey minRange =
                Collections.min(partitionRanges.stream().map(range -> {
                    PartitionKey lower = range.lowerEndpoint();
                    if (range.contains(lower)) {
                        return lower;
                    } else {
                        return lower.successor();
                    }
                }).collect(Collectors.toList()));
        PartitionKey maxRange =
                Collections.max(partitionRanges.stream().map(range -> {
                    PartitionKey upper = range.upperEndpoint();
                    if (range.contains(upper)) {
                        return upper;
                    } else {
                        return upper.predecessor();
                    }
                }).collect(Collectors.toList()));

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
            LiteralExpr lowerBound = pcf.getLowerBound();
            LiteralExpr upperBound = pcf.getUpperBound();
            if ((null == lowerBound || lowerBound.isConstantNull()) &&
                    (null == upperBound || upperBound.isConstantNull())) {
                continue;
            }

            boolean lowerBind = true;
            boolean upperBind = true;
            if (null != lowerBound) {
                lowerBind = false;
                PartitionKey min = new PartitionKey();
                // TODO: take care `isConvertToDate` for olap table
                min.pushColumn(pcf.getLowerBound(), column.getPrimitiveType());
                int cmp = minRange.compareTo(min);
                if (cmp > 0 || (0 == cmp && pcf.lowerBoundInclusive)) {
                    lowerBind = true;
                }
            }

            if (null != upperBound) {
                upperBind = false;
                PartitionKey max = new PartitionKey();
                // TODO: take care `isConvertToDate` for olap table
                max.pushColumn(upperBound, column.getPrimitiveType());
                int cmp = maxRange.compareTo(max);
                if (cmp < 0 || (0 == cmp && pcf.upperBoundInclusive)) {
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

        if (column.isAllowNull() && containsNullValue(column, minRange)
                && !checkFilterNullValue(scanPredicates, logicalOlapScanOperator.getPredicate().clone())) {
            return null;
        }

        return Pair.create(Utils.compoundAnd(scanPredicates), prunedPartitionPredicates);
    }

    private static void putValueMapItem(ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds,
                                        Long partitionId,
                                        LiteralExpr value) {
        Set<Long> partitionIdSet = partitionValueToIds.get(value);
        if (partitionIdSet == null) {
            partitionIdSet = new HashSet<>();
        }
        partitionIdSet.add(partitionId);
        partitionValueToIds.put(value, partitionIdSet);
    }

    private static List<Long> listPartitionPrune(OlapTable olapTable, ListPartitionInfo listPartitionInfo,
                                                 LogicalOlapScanOperator operator) {

        Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap =
                Maps.newConcurrentMap();
        Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = new HashMap<>();

        // Currently queries either specify a temporary partition, or do not. There is no situation
        // where two partitions are checked at the same time
        boolean isTemporaryPartitionPrune = false;
        List<Long> specifyPartitionIds = null;
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
            specifyPartitionIds = partitionIds;
        } else {
            partitionIds = listPartitionInfo.getPartitionIds(false);
        }

        if (literalExprValuesMap != null && literalExprValuesMap.size() > 0) {
            ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds = new ConcurrentSkipListMap<>();
            for (Map.Entry<Long, List<LiteralExpr>> entry : literalExprValuesMap.entrySet()) {
                Long partitionId = entry.getKey();
                if (!partitionIds.contains(partitionId)) {
                    continue;
                }
                List<LiteralExpr> values = entry.getValue();
                if (values == null || values.isEmpty()) {
                    continue;
                }
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
                ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds = new ConcurrentSkipListMap<>();
                for (Map.Entry<Long, List<List<LiteralExpr>>> entry : multiLiteralExprValues.entrySet()) {
                    Long partitionId = entry.getKey();
                    if (!partitionIds.contains(partitionId)) {
                        continue;
                    }
                    List<List<LiteralExpr>> multiValues = entry.getValue();
                    if (multiValues == null || multiValues.isEmpty()) {
                        continue;
                    }
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
                columnToNullPartitions, scalarOperatorList, specifyPartitionIds);
        try {
            List<Long> prune = partitionPruner.prune();
            if (prune == null && isTemporaryPartitionPrune) {
                return partitionIds;
            } else {
                return prune;
            }
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
        } catch (Exception e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return null;
    }

    private static boolean isNeedFurtherPrune(List<Long> candidatePartitions, LogicalOlapScanOperator olapScanOperator,
                                              PartitionInfo partitionInfo) {
        if (candidatePartitions.isEmpty()
                || olapScanOperator.getPredicate() == null) {
            return false;
        }

        // only support RANGE and EXPR_RANGE
        // EXPR_RANGE_V2 type like partition by RANGE(cast(substring(col, 3)) as int)) is unsupported
        if (partitionInfo.getType() == PartitionType.RANGE) {
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            return rangePartitionInfo.getPartitionColumns().size() == 1
                    && !rangePartitionInfo.getIdToRange(true).containsKey(candidatePartitions.get(0));
        } else if (partitionInfo.getType() == PartitionType.EXPR_RANGE) {
            ExpressionRangePartitionInfo exprPartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
            List<Expr> partitionExpr = exprPartitionInfo.getPartitionExprs();
            if (partitionExpr.size() == 1 && partitionExpr.get(0) instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr.get(0);
                String functionName = functionCallExpr.getFnName().getFunction();
                return (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)
                        || FunctionSet.TIME_SLICE.equalsIgnoreCase(functionName))
                        && !exprPartitionInfo.getIdToRange(true).containsKey(candidatePartitions.get(0));
            }
        }
        return false;
    }

    private static boolean containsNullValue(Column column, PartitionKey minRange) {
        PartitionKey nullValue = new PartitionKey();
        try {
            nullValue.pushColumn(LiteralExpr.createInfinity(column.getType(), false), column.getPrimitiveType());
            return minRange.compareTo(nullValue) <= 0;
        } catch (AnalysisException e) {
            return false;
        }
    }


    private static boolean checkFilterNullValue(List<ScalarOperator> scanPredicates, ScalarOperator predicate) {
        ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
        ScalarOperator newPredicate = Utils.compoundAnd(scanPredicates);
        boolean newPredicateFilterNulls = false;
        boolean predicateFilterNulls = false;

        BaseScalarOperatorShuttle shuttle = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
                return ConstantOperator.createNull(variable.getType());
            }
        };

        if (newPredicate != null) {
            newPredicate = newPredicate.accept(shuttle, null);

            ScalarOperator value = scalarRewriter.rewrite(newPredicate, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
            if ((value.isConstantRef() && ((ConstantOperator) value).isNull()) ||
                    value.equals(ConstantOperator.createBoolean(false))) {
                newPredicateFilterNulls = true;
            }
        }

        predicate = predicate.accept(shuttle, null);
        ScalarOperator value = scalarRewriter.rewrite(predicate, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        if ((value.isConstantRef() && ((ConstantOperator) value).isNull())
                || value.equals(ConstantOperator.createBoolean(false))) {
            predicateFilterNulls = true;
        }

        return newPredicateFilterNulls == predicateFilterNulls;
    }

}
