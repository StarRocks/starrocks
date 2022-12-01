// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.util.List;
import java.util.Set;

public class MaterializedViewOptimizer {
    private List<ColumnRefOperator> outputExpressions;

    public OptExpression optimize(MaterializedView mv,
                                  ColumnRefFactory columnRefFactory,
                                  ConnectContext connectContext,
                                  Set<String> mvPartitionNamesToRefresh) {
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans = MvUtils.getRuleOptimizedLogicalPlan(mvSql, columnRefFactory, connectContext);
        if (plans == null) {
            return null;
        }
        outputExpressions = plans.second.getOutputColumn();
        OptExpression mvPlan = plans.first;
        if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !mvPartitionNamesToRefresh.isEmpty()) {
            boolean ret = updateScanWithPartitionRange(mv, mvPlan, mvPartitionNamesToRefresh);
            if (!ret) {
                return null;
            }
        }
        return mvPlan;
    }

    public List<ColumnRefOperator> getOutputExpressions() {
        return outputExpressions;
    }

    // try to get partitial partition predicate of partitioned mv.
    // for example, mv1 has two partition: p1:[2022-01-01, 2022-01-02), p2:[2022-01-02, 2022-01-03).
    // p1 is updated, p2 is outdated.
    // mv1's base partition table is t1, partition column is k1.
    // then this function will add predicate: k1 >= "2022-01-01" and k1 < "2022-01-02" to scan node of t1
    public boolean updateScanWithPartitionRange(MaterializedView mv,
                                                OptExpression mvPlan,
                                                Set<String> mvPartitionNamesToRefresh) {
        Pair<Table, Column> partitionTableAndColumns = mv.getPartitionTableAndColumn();
        if (partitionTableAndColumns == null) {
            return false;
        }
        if (!partitionTableAndColumns.first.isNativeTable()) {
            // for external table, we can not get modified partitions now.
            return false;
        }
        OlapTable partitionByTable = (OlapTable) partitionTableAndColumns.first;
        List<Range<PartitionKey>> latestBaseTableRanges =
                getLatestPartitionRangeForTable(partitionByTable, mv, mvPartitionNamesToRefresh);
        if (latestBaseTableRanges.isEmpty()) {
            // if do not have an uptodate partition, do not rewrite
            return false;
        }

        Column partitionColumn = partitionTableAndColumns.second;
        List<OptExpression> scanExprs = MvUtils.collectScanExprs(mvPlan);
        for (OptExpression scanExpr : scanExprs) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) scanExpr.getOp();
            Table scanTable = scanOperator.getTable();
            if ((scanTable.isLocalTable() && !scanTable.equals(partitionTableAndColumns.first))
                    || (!scanTable.isLocalTable()) && !scanTable.getTableIdentifier().equals(
                    partitionTableAndColumns.first.getTableIdentifier())) {
                continue;
            }
            ColumnRefOperator columnRef = scanOperator.getColumnReference(partitionColumn);
            List<ScalarOperator> partitionPredicates = convertRanges(columnRef, latestBaseTableRanges);
            ScalarOperator partialPartitionPredicate = Utils.compoundOr(partitionPredicates);
            ScalarOperator originalPredicate = scanOperator.getPredicate();
            ScalarOperator newPredicate = Utils.compoundAnd(originalPredicate, partialPartitionPredicate);
            scanOperator.setPredicate(newPredicate);
        }
        return true;
    }

    private List<Range<PartitionKey>> getLatestPartitionRangeForTable(OlapTable partitionByTable,
                                                                      MaterializedView mv,
                                                                      Set<String> mvPartitionNamesToRefresh) {
        Set<String> modifiedPartitionNames = mv.getUpdatedPartitionNamesOfTable(partitionByTable);
        List<Range<PartitionKey>> baseTableRanges = getLatestPartitionRange(partitionByTable, modifiedPartitionNames);
        List<Range<PartitionKey>> mvRanges = getLatestPartitionRange(mv, mvPartitionNamesToRefresh);
        List<Range<PartitionKey>> uptodateBaseTableRanges = Lists.newArrayList();
        for (Range<PartitionKey> range : baseTableRanges) {
            if (mvRanges.stream().anyMatch(mvRange -> mvRange.encloses(range))) {
                uptodateBaseTableRanges.add(range);
            }
        }
        return uptodateBaseTableRanges;
    }

    private List<Range<PartitionKey>> getLatestPartitionRange(OlapTable table, Set<String> modifiedPartitionNames) {
        List<Range<PartitionKey>> resultRanges = Lists.newArrayList();
        // partitions that will be excluded
        Set<Long> modifiedIds = Sets.newHashSet();
        for (Partition p : table.getPartitions()) {
            if (modifiedPartitionNames.contains(p.getName())) {
                modifiedIds.add(p.getId());
            }
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        List<Range<PartitionKey>> latestPartitionRanges = rangePartitionInfo.getRangeList(modifiedIds, false);
        if (latestPartitionRanges.isEmpty()) {
            return resultRanges;
        }

        for (int i = 0; i < latestPartitionRanges.size(); i++) {
            Range<PartitionKey> currentRange = latestPartitionRanges.get(i);
            boolean merged = false;
            for (int j = 0; j < resultRanges.size(); j++) {
                // 1 < r < 10, 10 <= r < 20 => 1 < r < 20
                Range<PartitionKey> resultRange = resultRanges.get(j);
                if (currentRange.isConnected(currentRange) && currentRange.gap(resultRange).isEmpty()) {
                    resultRanges.set(j, resultRange.span(currentRange));
                    merged = true;
                }
            }
            if (!merged) {
                resultRanges.add(currentRange);
            }
        }
        return resultRanges;
    }

    private List<ScalarOperator> convertRanges(ScalarOperator partitionScalar, List<Range<PartitionKey>> partitionRanges) {
        List<ScalarOperator> rangeParts = Lists.newArrayList();
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }
            // partition range must have lower bound and upper bound
            Preconditions.checkState(range.hasLowerBound() && range.hasUpperBound());
            LiteralExpr lowerExpr = range.lowerEndpoint().getKeys().get(0);
            if (lowerExpr.isMinValue() && range.upperEndpoint().isMaxValue()) {
                continue;
            } else if (lowerExpr.isMinValue()) {
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                BinaryPredicateOperator upperPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.LT, partitionScalar, upperBound);
                rangeParts.add(upperPredicate);
            } else if (range.upperEndpoint().isMaxValue()) {
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                BinaryPredicateOperator lowerPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.GE, partitionScalar, lowerBound);
                rangeParts.add(lowerPredicate);
            } else {
                // close, open range
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                BinaryPredicateOperator lowerPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.GE, partitionScalar, lowerBound);

                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                BinaryPredicateOperator upperPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.LT, partitionScalar, upperBound);

                CompoundPredicateOperator andPredicate = new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, lowerPredicate, upperPredicate);
                rangeParts.add(andPredicate);
            }
        }
        return rangeParts;
    }
}
