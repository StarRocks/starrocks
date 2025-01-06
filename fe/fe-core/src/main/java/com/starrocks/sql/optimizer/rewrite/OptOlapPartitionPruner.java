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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfoV2;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
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
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner.collectOlapTablePartitionValuesMap;

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
                    table.getVisiblePartitions().stream().filter(Partition::hasData).map(Partition::getId).collect(
                            Collectors.toList());
            // some test cases need to perceive partitions pruned, so we can not filter empty partitions.
        } else {
            selectedPartitionIds = selectedPartitionIds.stream()
                    .filter(id -> table.getPartition(id).hasData()).collect(Collectors.toList());
        }

        // Do further partition prune if needed
        if (isNeedFurtherPrune(selectedPartitionIds, logicalOlapScanOperator, partitionInfo)) {
            selectedPartitionIds = doFurtherPartitionPrune(table, logicalOlapScanOperator.getPredicate(),
                    logicalOlapScanOperator.getColumnMetaToColRefMap(), selectedPartitionIds);
        }

        try {
            checkScanPartitionLimit(selectedPartitionIds.size());
        } catch (StarRocksPlannerException e) {
            LOG.warn("{} queryId: {}", e.getMessage(), DebugUtil.printId(ConnectContext.get().getQueryId()));
            throw new StarRocksPlannerException(e.getMessage(), ErrorType.USER_ERROR);
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

    /**
     * If the selected partition ids are not null, merge the selected partition ids and do further partition pruning.
     * @param olapScanOperator the olap scan operator
     * @return the new olap scan operator with merged partition ids and predicates
     */
    public static LogicalOlapScanOperator mergePartitionPrune(LogicalOlapScanOperator olapScanOperator) {
        // already pruned partitions
        List<Long> selectedPartitionIds = olapScanOperator.getSelectedPartitionId();
        // new pruned partitions
        LogicalOlapScanOperator newOlapScanOperator = OptOlapPartitionPruner.prunePartitions(olapScanOperator);
        List<Long> newSelectedPartitionIds = newOlapScanOperator.getSelectedPartitionId();
        // merge selected partition ids
        List<Long> ansPartitionIds = null;
        if (newSelectedPartitionIds != null && selectedPartitionIds != null) {
            ansPartitionIds = Lists.newArrayList(selectedPartitionIds);
            // use hash set to accelerate the intersection operation
            ansPartitionIds.retainAll(new HashSet<>(newSelectedPartitionIds));
        } else {
            ansPartitionIds = (selectedPartitionIds == null) ? newSelectedPartitionIds : selectedPartitionIds;
        }

        try {
            checkScanPartitionLimit(ansPartitionIds.size());
        } catch (StarRocksPlannerException e) {
            LOG.warn("{} queryId: {}", e.getMessage(), DebugUtil.printId(ConnectContext.get().getQueryId()));
            throw new StarRocksPlannerException(e.getMessage(), ErrorType.USER_ERROR);
        }

        final LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        builder.withOperator(newOlapScanOperator)
                .setSelectedPartitionId(ansPartitionIds)
                // new predicate should cover the old one
                .setPredicate(newOlapScanOperator.getPredicate())
                // use the new pruned partition predicates
                .setPrunedPartitionPredicates(newOlapScanOperator.getPrunedPartitionPredicates());
        return builder.build();
    }

    private static void checkScanPartitionLimit(int selectedPartitionNum) throws StarRocksPlannerException {
        int scanOlapPartitionNumLimit = 0;
        try {
            scanOlapPartitionNumLimit = ConnectContext.get().getSessionVariable().getScanOlapPartitionNumLimit();
        } catch (Exception e) {
            LOG.warn("fail to get variable scan_olap_partition_num_limit, set default value 0, msg: {}", e.getMessage());
        }
        if (scanOlapPartitionNumLimit > 0 && selectedPartitionNum > scanOlapPartitionNumLimit) {
            String msg = "Exceeded the limit of number of olap table partitions to be scanned. " +
                         "Number of partitions allowed: " + scanOlapPartitionNumLimit +
                         ", number of partitions to be scanned: " + selectedPartitionNum +
                         ". Please adjust the SQL or change the limit by set variable scan_olap_partition_num_limit.";
            throw new StarRocksPlannerException(msg, ErrorType.USER_ERROR);
        }
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
        if (partitionInfo.getPartitionColumnsSize() != 1 || selectedPartitionIds.isEmpty()) {
            return null;
        }
        List<ScalarOperator> prunedPartitionPredicates = Lists.newArrayList();
        Map<String, PartitionColumnFilter> predicateRangeMap = Maps.newHashMap();

        String columnName = partitionInfo.getPartitionColumns(table.getIdToColumn()).get(0).getName();
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
                min.pushColumn(pcf.getLowerBound(), column.getPrimitiveType());
                int cmp = minRange.compareTo(min);
                if (cmp > 0 || (0 == cmp && pcf.lowerBoundInclusive)) {
                    lowerBind = true;
                }
            }

            if (null != upperBound) {
                upperBind = false;
                PartitionKey max = new PartitionKey();
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

        if (!table.getDistributionColumnNames().contains(columnName)) {
            scanPredicates.removeAll(prunedPartitionPredicates);
        }

        if (column.isAllowNull() && containsNullValue(minRange)
                && !checkFilterNullValue(scanPredicates, logicalOlapScanOperator.getPredicate().clone())) {
            return null;
        }

        return Pair.create(Utils.compoundAnd(scanPredicates), prunedPartitionPredicates);
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
        Set<Long> partitionIds = Sets.newHashSet();
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
            specifyPartitionIds = Lists.newArrayList(partitionIds);
        } else {
            partitionIds = Sets.newHashSet(listPartitionInfo.getPartitionIds(false));
        }
        Map<Column, ColumnRefOperator> columnRefMap = operator.getColumnMetaToColRefMap();

        // collect partition values map
        collectOlapTablePartitionValuesMap(olapTable, partitionIds, columnRefMap,
                columnToPartitionValuesMap, columnToNullPartitions, false);

        List<ScalarOperator> scalarOperatorList = Utils.extractConjuncts(operator.getPredicate());
        ListPartitionPruner partitionPruner = new ListPartitionPruner(columnToPartitionValuesMap,
                columnToNullPartitions, scalarOperatorList, specifyPartitionIds, listPartitionInfo);
        partitionPruner.prepareDeduceExtraConjuncts(operator);
        try {
            List<Long> prune = partitionPruner.prune();
            if (prune == null && isTemporaryPartitionPrune) {
                return Lists.newArrayList(partitionIds);
            } else {
                return prune;
            }
        } catch (AnalysisException e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return specifyPartitionIds;
    }

    private static List<Long> rangePartitionPrune(OlapTable olapTable, RangePartitionInfo partitionInfo,
                                                  LogicalOlapScanOperator operator) {
        Map<Long, Range<PartitionKey>> keyRangeById;
        if (operator.getPartitionNames() != null && operator.getPartitionNames().getPartitionNames() != null) {
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
                partitionInfo.getPartitionColumns(olapTable.getIdToColumn()),
                operator.getColumnFilters());
        try {
            return partitionPruner.prune();
        } catch (Exception e) {
            LOG.warn("PartitionPrune Failed. ", e);
        }
        return Lists.newArrayList(keyRangeById.keySet());
    }

    private static boolean isNeedFurtherPrune(List<Long> candidatePartitions,
                                              LogicalOlapScanOperator olapScanOperator,
                                             PartitionInfo partitionInfo) {
        return isNeedFurtherPrune((OlapTable) olapScanOperator.getTable(), candidatePartitions,
                olapScanOperator.getPredicate(), partitionInfo);
    }

    public static List<Long> doFurtherPartitionPrune(OlapTable table,
                                                     ScalarOperator predicate,
                                                     Map<Column, ColumnRefOperator> columnRefOperatorMap,
                                                     List<Long> selectedPartitionIds) {
        Preconditions.checkArgument(table.getPartitionInfo() instanceof RangePartitionInfo);
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        List<Range<PartitionKey>> candidateRanges = selectedPartitionIds.stream()
                .map(rangePartitionInfo::getRange)
                .collect(Collectors.toList());
        return doFurtherPartitionPrune(table, predicate, columnRefOperatorMap, selectedPartitionIds, candidateRanges);
    }

    /**
     * Use input candidateRanges to prune partitions rather than all table's partitions.
     */
    public static List<Long> doFurtherPartitionPrune(OlapTable table,
                                                     ScalarOperator predicate,
                                                     Map<Column, ColumnRefOperator> columnRefOperatorMap,
                                                     List<Long> selectedPartitionIds,
                                                     List<Range<PartitionKey>> candidateRanges) {
        List<Column> partitionColumns = table.getPartitionColumns();
        PartitionColPredicateExtractor extractor = new PartitionColPredicateExtractor(
                partitionColumns,
                columnRefOperatorMap);
        PartitionColPredicateEvaluator evaluator = new PartitionColPredicateEvaluator(
                partitionColumns, selectedPartitionIds, candidateRanges);
        return evaluator.prunePartitions(extractor, predicate);
    }

    public static boolean isNeedFurtherPrune(OlapTable olapTable,
                                             List<Long> candidatePartitions,
                                             ScalarOperator predicate,
                                             PartitionInfo partitionInfo) {
        if (candidatePartitions.isEmpty() || predicate == null) {
            return false;
        }
        if (partitionInfo == null || !(partitionInfo instanceof RangePartitionInfo)) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        return isNeedFurtherPrune(olapTable, candidatePartitions, predicate, rangePartitionInfo,
                rangePartitionInfo.getIdToRange(true));
    }

    /**
     * Use input idToRange to check whether to further prune partitions rather than all table's partitions.
     */
    public static boolean isNeedFurtherPrune(OlapTable olapTable,
                                             List<Long> candidatePartitions,
                                             ScalarOperator predicate,
                                             RangePartitionInfo rangePartitionInfo,
                                             Map<Long, Range<PartitionKey>> tmpIdToRange) {
        if (candidatePartitions.isEmpty() || predicate == null) {
            return false;
        }

        // only support RANGE and EXPR_RANGE
        // EXPR_RANGE_V2 type like partition by RANGE(cast(substring(col, 3)) as int)) is unsupported
        if (rangePartitionInfo instanceof ExpressionRangePartitionInfo) {
            ExpressionRangePartitionInfo exprPartitionInfo = (ExpressionRangePartitionInfo) rangePartitionInfo;
            List<Expr> partitionExpr = exprPartitionInfo.getPartitionExprs(olapTable.getIdToColumn());
            if (partitionExpr.size() == 1 && partitionExpr.get(0) instanceof FunctionCallExpr) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) partitionExpr.get(0);
                String functionName = functionCallExpr.getFnName().getFunction();
                return (FunctionSet.DATE_TRUNC.equalsIgnoreCase(functionName)
                        || FunctionSet.TIME_SLICE.equalsIgnoreCase(functionName))
                        && !tmpIdToRange.containsKey(candidatePartitions.get(0));
            } else if (partitionExpr.size() == 1 && partitionExpr.get(0) instanceof SlotRef) {
                return !tmpIdToRange.containsKey(candidatePartitions.get(0));
            }
        } else if (rangePartitionInfo instanceof ExpressionRangePartitionInfoV2) {
            return false;
        } else if (rangePartitionInfo instanceof RangePartitionInfo) {
            return rangePartitionInfo.getPartitionColumnsSize() == 1
                    && !tmpIdToRange.containsKey(candidatePartitions.get(0));
        }
        return false;
    }

    private static boolean containsNullValue(PartitionKey minRange) {
        PartitionKey nullValue = new PartitionKey();
        try {
            for (int i = 0; i < minRange.getKeys().size(); ++i) {
                LiteralExpr rangeKey = minRange.getKeys().get(i);
                PrimitiveType type = minRange.getTypes().get(i);
                nullValue.pushColumn(LiteralExpr.createInfinity(rangeKey.getType(), false), type);
            }

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
