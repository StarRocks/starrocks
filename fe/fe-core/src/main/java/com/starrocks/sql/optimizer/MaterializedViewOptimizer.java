// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class MaterializedViewOptimizer {
    private List<ColumnRefOperator> outputExpressions;
    private Set<String> partitionNamesToRefresh;
    private List<Range<PartitionKey>> mvRanges;

    public OptExpression optimize(MaterializedView mv, ColumnRefFactory columnRefFactory, ConnectContext connectContext) {
        String mvSql = mv.getViewDefineSql();
        Pair<OptExpression, LogicalPlan> plans = MvUtils.getRuleOptimizedLogicalPlan(mvSql, columnRefFactory, connectContext);
        if (plans == null) {
            return null;
        }
        outputExpressions = plans.second.getOutputColumn();
        OptExpression mvPlan = plans.first;
        partitionNamesToRefresh = mv.getPartitionNamesToRefreshForMv();
        if (mv.getPartitionInfo() instanceof ExpressionRangePartitionInfo && !partitionNamesToRefresh.isEmpty()) {
            boolean ret = updatePartitionByScanPredicate(mv, mvPlan);
            if (!ret) {
                return null;
            }
        }
        return mvPlan;
    }

    public List<ColumnRefOperator> getOutputExpressions() {
        return outputExpressions;
    }

    // return partial partition predicate for scan mv expression
    public ScalarOperator getPartialPartitionPredicate(MaterializedView mv) {
        if (partitionNamesToRefresh == null || partitionNamesToRefresh.isEmpty()) {
            // all partitions are uptodate, do not add filter
            return null;
        }
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (!(partitionInfo instanceof ExpressionRangePartitionInfo)) {
            return null;
        }
        ExpressionRangePartitionInfo exprPartitionInfo = (ExpressionRangePartitionInfo) partitionInfo;
        Pair<Table, Column> partitionTableAndColumns = mv.getPartitionTableAndColumn();
        if (partitionTableAndColumns == null) {
            return null;
        }

        Column partitionColumn = partitionTableAndColumns.second;
        Expr partitionExpr = exprPartitionInfo.getPartitionExprs().get(0);
        List<SlotRef> partitionSlotRefs = Lists.newArrayList();
        partitionExpr.collect(SlotRef.class, partitionSlotRefs);
        Preconditions.checkState(partitionSlotRefs.size() == 1);
        ColumnRefFactory refFactory = new ColumnRefFactory();
        ColumnRefOperator partitionColumnRef = refFactory.create(
                partitionSlotRefs.get(0).getColumnName(), partitionColumn.getType(), partitionColumn.isAllowNull());
        ExpressionMapping expressionMapping =
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                        Lists.newArrayList());
        expressionMapping.put(partitionSlotRefs.get(0), partitionColumnRef);
        ScalarOperator partitionScalarForScan =
                SqlToScalarOperatorTranslator.translate(partitionExpr, expressionMapping, refFactory);
        List<ScalarOperator> partitionPredicatesForScan = convertRanges(partitionScalarForScan, mvRanges);
        return Utils.compoundOr(partitionPredicatesForScan);
    }

    // try to get partial partition predicate of partitioned mv.
    // for example, mv1 has two partition: p1:[2022-01-01, 2022-01-02), p2:[2022-01-02, 2022-01-03).
    // p1 is updated, p2 is outdated.
    // mv1's base partition table is t1, partition column is k1.
    // then this function will add predicate: k1 >= "2022-01-01" and k1 < "2022-01-02" to scan node of t1
    private boolean updatePartitionByScanPredicate(MaterializedView mv, OptExpression mvPlan) {
        // convert finalRanges into ScalarOperator
        Pair<Table, Column> partitionTableAndColumns = mv.getPartitionTableAndColumn();
        if (partitionTableAndColumns == null) {
            return false;
        }
        OlapTable partitionByTable = (OlapTable) partitionTableAndColumns.first;
        Set<String> modifiedPartitionNames = mv.getUpdatedPartitionNamesOfTable(partitionByTable);
        List<Range<PartitionKey>> baseTableRanges = getUptodatedPartitionRange(partitionByTable, modifiedPartitionNames);
        mvRanges = getUptodatedPartitionRange(mv, partitionNamesToRefresh);
        List<Range<PartitionKey>> finalRanges = Lists.newArrayList();
        for (Range<PartitionKey> range : baseTableRanges) {
            if (mvRanges.stream().anyMatch(mvRange -> mvRange.encloses(range))) {
                finalRanges.add(range);
            }
        }
        if (finalRanges.isEmpty()) {
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
            Optional<Column> columnOptional = scanOperator.getColumnMetaToColRefMap()
                    .keySet().stream().filter(col -> col.getName().equals(partitionColumn.getName())).findFirst();
            Preconditions.checkState(columnOptional.isPresent());
            ColumnRefOperator columnRef = scanOperator.getColumnReference(columnOptional.get());
            List<ScalarOperator> partitionPredicates = convertRanges(columnRef, finalRanges);
            ScalarOperator partialPartitionPredicate = Utils.compoundOr(partitionPredicates);
            ScalarOperator originalPredicate = scanOperator.getPredicate();
            ScalarOperator newPredicate = Utils.compoundAnd(originalPredicate, partialPartitionPredicate);
            scanOperator.setPredicate(newPredicate);
        }
        return true;
    }

    private List<Range<PartitionKey>> getUptodatedPartitionRange(OlapTable table, Set<String> modifiedPartitionNames) {
        List<Range<PartitionKey>> finalRanges = Lists.newArrayList();
        // partitions that will be excluded
        Set<Long> modifiedIds = Sets.newHashSet();
        for (Partition p : table.getPartitions()) {
            if (modifiedPartitionNames.contains(p.getName())) {
                modifiedIds.add(p.getId());
            }
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
        List<Range<PartitionKey>> uptodatePartitionRanges = rangePartitionInfo.getRangeList(modifiedIds, false);
        if (uptodatePartitionRanges.isEmpty()) {
            return finalRanges;
        }

        for (int i = 0; i < uptodatePartitionRanges.size(); i++) {
            Range<PartitionKey> currentRange = uptodatePartitionRanges.get(i);
            boolean merged = false;
            for (int j = 0; j < finalRanges.size(); j++) {
                // 1 < r < 10, 10 <= r < 20 => 1 < r < 20
                Range<PartitionKey> resultRange = finalRanges.get(j);
                if (currentRange.isConnected(currentRange) && currentRange.gap(resultRange).isEmpty()) {
                    finalRanges.set(j, resultRange.span(currentRange));
                    merged = true;
                }
            }
            if (!merged) {
                finalRanges.add(currentRange);
            }
        }
        return finalRanges;
    }

    private List<ScalarOperator> convertRanges(ScalarOperator partitionScalar, List<Range<PartitionKey>> partitionRanges) {
        List<ScalarOperator> rangeParts = Lists.newArrayList();
        for (Range<PartitionKey> range : partitionRanges) {
            if (range.isEmpty()) {
                continue;
            }
            if (range.hasLowerBound() && range.hasUpperBound()) {
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
            } else if (range.hasUpperBound()) {
                ConstantOperator upperBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.upperEndpoint().getKeys().get(0));
                BinaryPredicateOperator upperPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.LT, partitionScalar, upperBound);
                rangeParts.add(upperPredicate);
            } else if (range.hasLowerBound()) {
                ConstantOperator lowerBound =
                        (ConstantOperator) SqlToScalarOperatorTranslator.translate(range.lowerEndpoint().getKeys().get(0));
                BinaryPredicateOperator lowerPredicate = new BinaryPredicateOperator(
                        BinaryPredicateOperator.BinaryType.GE, partitionScalar, lowerBound);
                rangeParts.add(lowerPredicate);
            } else {
                Preconditions.checkState(false);
            }
        }
        return rangeParts;
    }
}
