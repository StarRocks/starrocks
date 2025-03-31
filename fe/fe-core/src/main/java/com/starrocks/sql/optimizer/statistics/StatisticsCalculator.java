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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.connector.statistics.StatisticsUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergMetadataScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalKuduScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionTableScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFileScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergMetadataScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalKuduScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOdpsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.operator.stream.LogicalBinlogScanOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.columns.PredicateColumnsMgr;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.statistics.ColumnStatistic.buildFrom;
import static com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient.PREDICATE_UNKNOWN_FILTER_COEFFICIENT;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;

/**
 * Estimate stats for the root group using a group expression's children's stats
 * The estimated column stats will store in {@link Group}
 */
public class StatisticsCalculator extends OperatorVisitor<Void, ExpressionContext> {
    private static final Logger LOG = LogManager.getLogger(StatisticsCalculator.class);

    private final ExpressionContext expressionContext;
    private final ColumnRefFactory columnRefFactory;
    private final OptimizerContext optimizerContext;

    public StatisticsCalculator(ExpressionContext expressionContext,
                                ColumnRefFactory columnRefFactory,
                                OptimizerContext optimizerContext) {
        this.expressionContext = expressionContext;
        this.columnRefFactory = columnRefFactory;
        this.optimizerContext = optimizerContext;
    }

    @VisibleForTesting
    public StatisticsCalculator() {
        this.expressionContext = null;
        this.columnRefFactory = null;
        this.optimizerContext = null;
    }

    public void estimatorStats() {
        expressionContext.getOp().accept(this, expressionContext);
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        ScalarOperator predicate = null;
        long limit = Operator.DEFAULT_LIMIT;

        if (node instanceof LogicalOperator) {
            LogicalOperator logical = (LogicalOperator) node;
            predicate = logical.getPredicate();
            limit = logical.getLimit();
        } else if (node instanceof PhysicalOperator) {
            PhysicalOperator physical = (PhysicalOperator) node;
            predicate = physical.getPredicate();
            limit = physical.getLimit();
        }

        PredicateColumnsMgr.getInstance().recordPredicateColumns(predicate, optimizerContext.getColumnRefFactory(),
                context.getOptExpression());

        predicate = removePartitionPredicate(predicate, node, optimizerContext);
        Statistics statistics = context.getStatistics();
        if (null != predicate && !predicate.isNotEvalEstimate()) {
            statistics = estimateStatistics(ImmutableList.of(predicate), statistics);
        }

        Statistics.Builder statisticsBuilder = Statistics.buildFrom(statistics);
        if (limit != Operator.DEFAULT_LIMIT && limit < statistics.getOutputRowCount()) {
            statisticsBuilder.setOutputRowCount(limit);
        }
        // CTE consumer has children but the children do not estimate the statistics, so here need to filter null
        if (context.getChildrenStatistics().stream().filter(Objects::nonNull)
                .anyMatch(Statistics::isTableRowCountMayInaccurate)) {
            statisticsBuilder.setTableRowCountMayInaccurate(true);
        }

        Projection projection = node.getProjection();
        if (projection != null) {
            Map<ColumnRefOperator, SubfieldOperator> subfieldColumns = Maps.newHashMap();
            Preconditions.checkState(projection.getCommonSubOperatorMap().isEmpty());
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : projection.getColumnRefMap().entrySet()) {
                if (entry.getValue() instanceof SubfieldOperator && (node instanceof LogicalScanOperator ||
                        node instanceof PhysicalScanOperator)) {
                    subfieldColumns.put(entry.getKey(), (SubfieldOperator) entry.getValue());
                } else {
                    statisticsBuilder.addColumnStatistic(entry.getKey(),
                            ExpressionStatisticCalculator.calculate(entry.getValue(), statisticsBuilder.build()));
                }
            }
            // for subfield operator, we get the statistics from statistics storage
            if (!subfieldColumns.isEmpty()) {
                addSubFiledStatistics(node, subfieldColumns, statisticsBuilder);
            }
        }
        context.setStatistics(statisticsBuilder.build());
        return null;
    }

    private void addSubFiledStatistics(Operator node, Map<ColumnRefOperator, SubfieldOperator> subfieldColumns,
                                       Statistics.Builder statisticsBuilder) {
        if (!(node instanceof LogicalScanOperator) && !(node instanceof PhysicalScanOperator)) {
            return;
        }

        Table table = node.isLogical() ? ((LogicalScanOperator) node).getTable() :
                ((PhysicalScanOperator) node).getTable();

        List<Pair<String, ColumnRefOperator>> columnRefPairs = subfieldColumns.entrySet().stream()
                .map(entry -> Pair.create(entry.getValue().getPath(), entry.getKey())).collect(Collectors.toList());
        List<String> columnNames = columnRefPairs.stream().map(p -> p.first).collect(Collectors.toList());

        List<ColumnStatistic> columnStatisticList;
        Map<String, Histogram> histogramStatistics;
        if (table.isNativeTableOrMaterializedView()) {
            columnStatisticList = GlobalStateMgr.getCurrentState().getStatisticStorage().getColumnStatistics(table,
                    columnNames);
            histogramStatistics = GlobalStateMgr.getCurrentState().getStatisticStorage().getHistogramStatistics(table,
                    columnNames);
        } else {
            columnStatisticList = GlobalStateMgr.getCurrentState().getStatisticStorage().getConnectorTableStatistics(table,
                    columnNames).stream().map(ConnectorTableColumnStats::getColumnStatistic).collect(Collectors.toList());
            histogramStatistics = GlobalStateMgr.getCurrentState().getStatisticStorage().
                    getConnectorHistogramStatistics(table, columnNames);
        }
        for (int i = 0; i < columnNames.size(); ++i) {
            String columnName = columnNames.get(i);
            ColumnRefOperator columnRef = columnRefPairs.get(i).second;
            if (histogramStatistics.containsKey(columnName)) {
                Histogram histogram = histogramStatistics.get(columnName);
                statisticsBuilder.addColumnStatistic(columnRef, ColumnStatistic.buildFrom(
                                columnStatisticList.get(i)).
                        setHistogram(histogram).build());
            } else {
                statisticsBuilder.addColumnStatistic(columnRef, columnStatisticList.get(i));
            }
        }
    }

    @Override
    public Void visitLogicalOlapScan(LogicalOlapScanOperator node, ExpressionContext context) {
        return computeOlapScanNode(node, context, node.getTable(), node.getSelectedPartitionId(),
                node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
        return computeOlapScanNode(node, context, node.getTable(), node.getSelectedPartitionId(),
                node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitLogicalBinlogScan(LogicalBinlogScanOperator node, ExpressionContext context) {
        return computeOlapScanNode(node, context, node.getTable(), Lists.newArrayList(),
                node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitLogicalViewScan(LogicalViewScanOperator node, ExpressionContext context) {
        Statistics.Builder builder = Statistics.builder();
        List<ColumnRefOperator> requiredColumnRefs = Lists.newArrayList(node.getColRefToColumnMetaMap().keySet());
        for (int i = 0; i < requiredColumnRefs.size(); ++i) {
            builder.addColumnStatistic(requiredColumnRefs.get(i), ColumnStatistic.unknown());
        }
        // set output row count to max to make optimizer skip this plan
        builder.setOutputRowCount(POSITIVE_INFINITY);
        builder.setTableRowCountMayInaccurate(true);
        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalTableFunctionTableScan(LogicalTableFunctionTableScanOperator node, ExpressionContext context) {
        return computeFileScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalStreamScan(PhysicalStreamScanOperator node, ExpressionContext context) {
        return computeOlapScanNode(node, context, node.getTable(), Lists.newArrayList(),
                node.getColRefToColumnMetaMap());
    }

    private Void computeOlapScanNode(Operator node, ExpressionContext context, Table table,
                                     Collection<Long> selectedPartitionIds,
                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Preconditions.checkState(context.arity() == 0);

        PredicateColumnsMgr.getInstance().recordScanColumns(colRefToColumnMetaMap, table, context.getOptExpression());
        PredicateColumnsMgr.getInstance()
                .recordPredicateColumns(node.getPredicate(), optimizerContext.getColumnRefFactory(),
                        context.getOptExpression());

        // 1. get table row count
        long tableRowCount = StatisticsCalcUtils.getTableRowCount(table, node, optimizerContext);
        // 2. get required columns statistics
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(table, colRefToColumnMetaMap, optimizerContext);
        if (tableRowCount <= 1) {
            builder.setTableRowCountMayInaccurate(true);
        }
        // 3. get multi-column combined statistics according to required columns
        builder = StatisticsCalcUtils.estimateMultiColumnCombinedStats(table, builder, colRefToColumnMetaMap);

        // 4. deal with column statistics for partition prune
        OlapTable olapTable = (OlapTable) table;
        adjustPartitionColsStatistic(selectedPartitionIds, olapTable, builder, colRefToColumnMetaMap);
        builder.setOutputRowCount(tableRowCount);

        // 5. deal with predicate cardinality for list partition table
        adjustPredicateCardinality(olapTable, selectedPartitionIds, node, colRefToColumnMetaMap, builder);

        // 6. deal with OlapScan which is generated by MV
        if (isRewrittenMvGE(node, table, context)) {
            adjustNestedMvStatistics(context.getGroupExpression().getGroup(), (MaterializedView) olapTable, builder);
            if (node.getProjection() != null) {
                builder.setShadowColumns(node.getProjection().getOutputColumns());
            }
        }
        context.setStatistics(builder.build());

        return visitOperator(node, context);
    }

    /**
     * For LIST partition table, we cannot use the table_rows * predicate_selectivity to estimate the output rows of
     * ScanOperator, since the predicate selectivity is calculated with the table-level NDV, which may not be able
     * to represent partition-level NDV.
     * So here we use SUM(partition_rows * partition predicate_selectivity)  to estimate output rows of ScanOperator
     */
    private void adjustPredicateCardinality(OlapTable table,
                                            Collection<Long> partitions,
                                            Operator node,
                                            Map<ColumnRefOperator, Column> columnMap,
                                            Statistics.Builder statistics) {
        if (optimizerContext != null &&
                !optimizerContext.getSessionVariable().isEnablePartitionLevelCardinalityEstimation()) {
            return;
        }
        if (node.getPredicate() == null) {
            return;
        }
        PartitionInfo partitionInfo = table.getPartitionInfo();
        if (!partitionInfo.isListPartition() || CollectionUtils.isEmpty(partitions)) {
            return;
        }
        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
        long totalPartitions = listPartitionInfo.getPartitionIds(false).size();
        double selectedRatio = 1.0 * partitions.size() / totalPartitions;
        if (selectedRatio > 0.3 || partitions.size() > 128) {
            // if select most partitions, usually the table-level statistics is good enough
            return;
        }

        ScalarOperator predicate = node.getPredicate();
        Map<Long, Statistics> partitionStatistics =
                StatisticsCalcUtils.getPartitionStatistics(node, table, columnMap, statistics);
        if (MapUtils.isEmpty(partitionStatistics)) {
            return;
        }
        long tableRows = 0;

        // This block is reentrant. This is the first cardinality estimation of the scan node.
        // we need to ensure that the state of each predicate is reset before each entry.
        Utils.extractConjuncts(predicate).forEach(op -> op.setNotEvalEstimate(false));
        predicate = removePartitionPredicate(predicate, node, optimizerContext);
        for (var entry : partitionStatistics.entrySet()) {
            Statistics partitionStat = estimateStatistics(ImmutableList.of(predicate), entry.getValue());
            long partitionSelectedRows = (long) partitionStat.getOutputRowCount();
            if (partitionStat.isTableRowCountMayInaccurate()) {
                return;
            }
            tableRows += partitionSelectedRows;
        }
        // For scan node, there is no need to evaluate the predicate later.
        Utils.extractConjuncts(predicate).forEach(op -> op.setNotEvalEstimate(true));
        // adjust output rows
        statistics.setOutputRowCount(tableRows);

        // adjust output column statistics if possible
        // The basic theory is, if selected partitions have similar NDV, we would assume that they are in the same
        // domain, so it's reasonable to use the average NDV to represent all of them.
        // And also,
        for (ColumnRefOperator column : columnMap.keySet()) {
            List<ColumnStatistic> partitionStats = partitionStatistics.values().stream()
                    .map(x -> x.getColumnStatistic(column))
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(partitionStats) || partitionStats.stream().anyMatch(Objects::isNull)) {
                return;
            }
            double avgDistinctCount = partitionStats.stream()
                    .mapToDouble(ColumnStatistic::getDistinctValuesCount)
                    .average().getAsDouble();
            boolean hasSimilarNDV = partitionStats.stream()
                    .allMatch(x -> withinDelta(x.getDistinctValuesCount(), avgDistinctCount, 0.1));
            if (hasSimilarNDV) {
                ColumnStatistic tableStats = statistics.getColumnStatistics(column);
                ColumnStatistic newStats = buildFrom(tableStats).setDistinctValuesCount(avgDistinctCount).build();
                statistics.addColumnStatistic(column, newStats);
            }
        }

        if (partitionStatistics.size() == 1) {
            Statistics partitionStats =  partitionStatistics.values().iterator().next();
            columnMap.keySet().forEach(column ->
                    statistics.addColumnStatistic(column, partitionStats.getColumnStatistic(column)));
        }
    }

    private boolean withinDelta(double a, double b, double ratio) {
        double delta = Math.abs(a - b);
        double actualRatio = Math.max(delta / a, delta / b);
        return actualRatio <= ratio;
    }

    private void adjustNestedMvStatistics(Group group, MaterializedView mv, Statistics.Builder builder) {
        for (Map.Entry<Long, List<Long>> entry : group.getRelatedMvs().entrySet()) {
            if (entry.getValue().contains(mv.getId())) {
                // mv is a nested mv based on entry.getKey
                Optional<Statistics> baseStatistics = group.getMvStatistics(entry.getKey());
                // for single table mv, nested mv's statistics will be more accurate
                if (baseStatistics.isPresent()
                        && mv.getBaseTableInfos() != null
                        && mv.getBaseTableInfos().size() == 1
                        && !builder.getTableRowCountMayInaccurate()) {
                    // update based mv output row count to nested mv
                    Statistics.Builder baseBuilder = Statistics.buildFrom(baseStatistics.get());
                    baseBuilder.setOutputRowCount(builder.getOutputRowCount());
                    group.setMvStatistics(entry.getKey(), baseBuilder.build());
                }
            }
        }
    }

    private boolean isRewrittenMvGE(Operator node, Table table, ExpressionContext context) {
        return table.isMaterializedView()
                && node instanceof LogicalOlapScanOperator
                && context.getGroupExpression() != null
                && context.getGroupExpression().hasAppliedMVRules();
    }

    @Override
    public Void visitLogicalIcebergScan(LogicalIcebergScanOperator node, ExpressionContext context) {
        return computeIcebergScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalIcebergScan(PhysicalIcebergScanOperator node, ExpressionContext context) {
        return computeIcebergScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitLogicalIcebergEqualityDeleteScan(LogicalIcebergEqualityDeleteScanOperator node, ExpressionContext context) {
        return computeIcebergEqualityDeleteScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalIcebergEqualityDeleteScan(PhysicalIcebergEqualityDeleteScanOperator node,
                                                       ExpressionContext context) {
        return computeIcebergEqualityDeleteScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    private Void computeIcebergEqualityDeleteScanNode(Operator node, ExpressionContext context, Table table,
                                                      Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        context.setStatistics(StatisticsUtils.buildDefaultStatistics(colRefToColumnMetaMap.keySet()));
        return visitOperator(node, context);
    }

    private Void computeIcebergScanNode(Operator node, ExpressionContext context, Table table,
                                        Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        if (context.getStatistics() == null) {
            String catalogName = table.getCatalogName();
            TableVersionRange version;
            IcebergMORParams icebergMORParams;
            if (node.isLogical()) {
                version = ((LogicalIcebergScanOperator) node).getTableVersionRange();
                icebergMORParams = ((LogicalIcebergScanOperator) node).getMORParam();
            } else {
                version = ((PhysicalIcebergScanOperator) node).getTableVersionRange();
                icebergMORParams = ((PhysicalIcebergScanOperator) node).getMORParams();
            }

            Statistics statistics;
            if (icebergMORParams == IcebergMORParams.EMPTY || icebergMORParams == IcebergMORParams.DATA_FILE_WITHOUT_EQ_DELETE) {
                statistics = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                        optimizerContext, catalogName, table, colRefToColumnMetaMap, null,
                        node.getPredicate(), node.getLimit(), version);
            } else {
                statistics = StatisticsUtils.buildDefaultStatistics(colRefToColumnMetaMap.keySet());
            }

            context.setStatistics(statistics);
            if (node.isLogical()) {
                boolean hasUnknownColumns = statistics.getColumnStatistics().values().stream()
                        .anyMatch(ColumnStatistic::isUnknown);
                ((LogicalIcebergScanOperator) node).setHasUnknownColumn(hasUnknownColumns);
            }
        }

        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalDeltaLakeScan(LogicalDeltaLakeScanOperator node, ExpressionContext context) {
        return computeDeltaLakeScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalDeltaLakeScan(PhysicalDeltaLakeScanOperator node, ExpressionContext context) {
        return computeDeltaLakeScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    private Void computeDeltaLakeScanNode(Operator node, ExpressionContext context, Table table,
                                          Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        if (context.getStatistics() == null) {
            String catalogName = table.getCatalogName();
            Statistics stats = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                    optimizerContext, catalogName, table, columnRefOperatorColumnMap, null,
                    node.getPredicate(), node.getLimit(), TableVersionRange.empty());
            context.setStatistics(stats);

            if (node.isLogical()) {
                boolean hasUnknownColumns = stats.getColumnStatistics().values().stream()
                        .anyMatch(ColumnStatistic::isUnknown);
                ((LogicalDeltaLakeScanOperator) node).setHasUnknownColumn(hasUnknownColumns);
            }
        }

        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalIcebergMetadataScan(LogicalIcebergMetadataScanOperator node, ExpressionContext context) {
        return computeMetadataScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalIcebergMetadataScan(PhysicalIcebergMetadataScanOperator node, ExpressionContext context) {
        return computeMetadataScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    private Void computeMetadataScanNode(Operator node, ExpressionContext context,
                                         Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columnRefOperatorColumnMap.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }

        builder.setOutputRowCount(1);
        context.setStatistics(builder.build());

        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalFileScan(LogicalFileScanOperator node, ExpressionContext context) {
        return computeFileScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalFileScan(PhysicalFileScanOperator node, ExpressionContext context) {
        return computeFileScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    private Void computeFileScanNode(Operator node, ExpressionContext context,
                                     Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        // Use default statistics for now.
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRefOperator : columnRefOperatorColumnMap.keySet()) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }
        // cause we don't know the real schema in file，just use the default Row Count now
        builder.setOutputRowCount(1);
        context.setStatistics(builder.build());

        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalPaimonScan(LogicalPaimonScanOperator node, ExpressionContext context) {
        return computePaimonScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalPaimonScan(PhysicalPaimonScanOperator node, ExpressionContext context) {
        return computePaimonScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    private Void computePaimonScanNode(Operator node, ExpressionContext context, Table table,
                                       Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        if (context.getStatistics() == null) {
            String catalogName = table.getCatalogName();
            Statistics stats = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                    optimizerContext, catalogName, table, columnRefOperatorColumnMap, null,
                    node.getPredicate(), node.getLimit(), TableVersionRange.empty());
            context.setStatistics(stats);
            if (node.isLogical()) {
                boolean hasUnknownColumns = stats.getColumnStatistics().values().stream()
                        .anyMatch(ColumnStatistic::isUnknown);
                ((LogicalPaimonScanOperator) node).setHasUnknownColumn(hasUnknownColumns);
            }
        }

        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalOdpsScan(LogicalOdpsScanOperator node, ExpressionContext context) {
        return computeOdpsScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalOdpsScan(PhysicalOdpsScanOperator node, ExpressionContext context) {
        return computeOdpsScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    private Void computeOdpsScanNode(Operator node, ExpressionContext context, Table table,
                                     Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        if (context.getStatistics() == null) {
            String catalogName = table.getCatalogName();
            Statistics stats = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                    optimizerContext, catalogName, table, columnRefOperatorColumnMap, null, node.getPredicate());
            context.setStatistics(stats);
        }
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalKuduScan(LogicalKuduScanOperator node, ExpressionContext context) {
        return computeKuduScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalKuduScan(PhysicalKuduScanOperator node, ExpressionContext context) {
        return computeKuduScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    private Void computeKuduScanNode(Operator node, ExpressionContext context, Table table,
                                     Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        if (context.getStatistics() == null) {
            String catalogName = ((KuduTable) table).getCatalogName();
            Statistics stats = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                    optimizerContext, catalogName, table, columnRefOperatorColumnMap, null,
                    node.getPredicate(), -1, TableVersionRange.empty());
            context.setStatistics(stats);
        }

        return visitOperator(node, context);
    }

    public Void visitLogicalHudiScan(LogicalHudiScanOperator node, ExpressionContext context) {
        return computeHMSTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalHudiScan(PhysicalHudiScanOperator node, ExpressionContext context) {
        return computeHMSTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitLogicalHiveScan(LogicalHiveScanOperator node, ExpressionContext context) {
        return computeHMSTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalHiveScan(PhysicalHiveScanOperator node, ExpressionContext context) {
        return computeHMSTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    public Void computeHMSTableScanNode(Operator node, ExpressionContext context, Table table,
                                        Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Preconditions.checkState(context.arity() == 0);

        ScanOperatorPredicates predicates;
        try {
            if (node.isLogical()) {
                predicates = ((LogicalScanOperator) node).getScanOperatorPredicates();
            } else {
                predicates = ((PhysicalScanOperator) node).getScanOperatorPredicates();
            }
            // If partition pruned, we should use the selected partition keys to estimate the statistics,
            // otherwise, we should use all partition keys to estimate the statistics.
            List<PartitionKey> partitionKeys = predicates.hasPrunedPartition() ? predicates.getSelectedPartitionKeys() :
                    PartitionUtil.getPartitionKeys(table);

            String catalogName = (table).getCatalogName();
            Statistics statistics = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                    optimizerContext, catalogName, table, colRefToColumnMetaMap, partitionKeys, null);
            context.setStatistics(statistics);

            if (node.isLogical()) {
                boolean hasUnknownColumns = statistics.getColumnStatistics().values().stream()
                        .anyMatch(ColumnStatistic::isUnknown);
                if (node instanceof LogicalHiveScanOperator) {
                    ((LogicalHiveScanOperator) node).setHasUnknownColumn(hasUnknownColumns);
                } else if (node instanceof LogicalHudiScanOperator) {
                    ((LogicalHudiScanOperator) node).setHasUnknownColumn(hasUnknownColumns);
                }
            }

            return visitOperator(node, context);
        } catch (AnalysisException e) {
            LOG.warn("compute hive table row count failed : " + e);
            throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
        }
    }

    private Void computeNormalExternalTableScanNode(Operator node, ExpressionContext context, Table table,
                                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                    int outputRowCount) {
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(table, colRefToColumnMetaMap, optimizerContext);
        builder.setOutputRowCount(outputRowCount);

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalMysqlScan(LogicalMysqlScanOperator node, ExpressionContext context) {
        return computeNormalExternalTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap(),
                StatisticsEstimateCoefficient.DEFAULT_MYSQL_OUTPUT_ROWS);
    }

    @Override
    public Void visitPhysicalMysqlScan(PhysicalMysqlScanOperator node, ExpressionContext context) {
        return computeNormalExternalTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap(),
                StatisticsEstimateCoefficient.DEFAULT_MYSQL_OUTPUT_ROWS);
    }

    @Override
    public Void visitLogicalEsScan(LogicalEsScanOperator node, ExpressionContext context) {
        return computeNormalExternalTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap(),
                StatisticsEstimateCoefficient.DEFAULT_ES_OUTPUT_ROWS);
    }

    @Override
    public Void visitPhysicalEsScan(PhysicalEsScanOperator node, ExpressionContext context) {
        return computeNormalExternalTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap(),
                StatisticsEstimateCoefficient.DEFAULT_ES_OUTPUT_ROWS);
    }

    @Override
    public Void visitLogicalSchemaScan(LogicalSchemaScanOperator node, ExpressionContext context) {
        Table table = node.getTable();
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(table,
                node.getColRefToColumnMetaMap(), optimizerContext);
        builder.setOutputRowCount(1);

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, ExpressionContext context) {
        Table table = node.getTable();
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(table,
                node.getColRefToColumnMetaMap(), optimizerContext);
        builder.setOutputRowCount(1);

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalMetaScan(LogicalMetaScanOperator node, ExpressionContext context) {
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(node.getTable(),
                node.getColRefToColumnMetaMap(), optimizerContext);
        builder.setOutputRowCount(node.getAggColumnIdToNames().size());

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalMetaScan(PhysicalMetaScanOperator node, ExpressionContext context) {
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(node.getTable(),
                node.getColRefToColumnMetaMap(), optimizerContext);
        builder.setOutputRowCount(node.getAggColumnIdToNames().size());

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalJDBCScan(LogicalJDBCScanOperator node, ExpressionContext context) {
        return computeNormalExternalTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap(),
                StatisticsEstimateCoefficient.DEFAULT_JDBC_OUTPUT_ROWS);
    }

    @Override
    public Void visitPhysicalJDBCScan(PhysicalJDBCScanOperator node, ExpressionContext context) {
        return computeNormalExternalTableScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap(),
                StatisticsEstimateCoefficient.DEFAULT_JDBC_OUTPUT_ROWS);
    }

    /**
     * At present, we only have table-level statistics. When partition prune occurs,
     * the statistics of the Partition column need to be adjusted to avoid subsequent estimation errors.
     * return new partition column statistics or else null
     */
    private void adjustPartitionColsStatistic(Collection<Long> selectedPartitionId, OlapTable olapTable,
                                              Statistics.Builder builder,
                                              Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        if (CollectionUtils.isEmpty(selectedPartitionId)) {
            return;
        }

        Map<String, ColumnRefOperator> colNameMap = Maps.newHashMap();
        colRefToColumnMetaMap.entrySet().stream().forEach(e -> colNameMap.put(e.getValue().getName(), e.getKey()));
        // It might contain null value, if some partition columns are not referenced in the scan
        List<ColumnRefOperator> partitionCols =
                olapTable.getPartitionColumnNames().stream()
                        .map(colNameMap::get)
                        .collect(Collectors.toList());
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo instanceof RangePartitionInfo) {
            if (partitionCols.size() != 1 || partitionCols.stream().anyMatch(Objects::isNull)) {
                return;
            }
            if (optimizerContext.getDumpInfo() != null) {
                optimizerContext.getDumpInfo().addTableStatistics(olapTable,
                        partitionCols.get(0).getName(),
                        builder.getColumnStatistics(partitionCols.get(0)));
            }
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
            List<Map.Entry<Long, Range<PartitionKey>>> rangeList = null;
            try {
                rangeList = rangePartitionInfo.getSortedRangeMap(new HashSet<>(selectedPartitionId));
            } catch (AnalysisException e) {
                LOG.warn("get sorted range partition failed, msg : " + e.getMessage());
            }
            if (CollectionUtils.isEmpty(rangeList)) {
                return;
            }
            Map.Entry<Long, Range<PartitionKey>> firstKey = rangeList.get(0);
            Map.Entry<Long, Range<PartitionKey>> lastKey = rangeList.get(rangeList.size() - 1);

            LiteralExpr minLiteral = firstKey.getValue().lowerEndpoint().getKeys().get(0);
            LiteralExpr maxLiteral = lastKey.getValue().upperEndpoint().getKeys().get(0);
            double min;
            double max;
            if (minLiteral instanceof DateLiteral) {
                DateLiteral minDateLiteral = (DateLiteral) minLiteral;
                DateLiteral maxDateLiteral;
                maxDateLiteral = maxLiteral instanceof MaxLiteral ? new DateLiteral(Type.DATE, true) :
                        (DateLiteral) maxLiteral;
                min = Utils.getLongFromDateTime(minDateLiteral.toLocalDateTime());
                max = Utils.getLongFromDateTime(maxDateLiteral.toLocalDateTime());
            } else {
                min = firstKey.getValue().lowerEndpoint().getKeys().get(0).getDoubleValue();
                max = lastKey.getValue().upperEndpoint().getKeys().get(0).getDoubleValue();
            }

            int selectedPartitionsSize = selectedPartitionId.size();
            int allNoEmptyPartitionsSize = (int) olapTable.getPartitions().stream().filter(Partition::hasData).count();
            double distinctValues =
                    builder.getColumnStatistics(partitionCols.get(0)).getDistinctValuesCount() * 1.0 * selectedPartitionsSize /
                            allNoEmptyPartitionsSize;
            ColumnStatistic columnStatistic = ColumnStatistic.buildFrom(builder.getColumnStatistics(partitionCols.get(0)))
                    .setMinValue(min).setMaxValue(max).setDistinctValuesCount(max(distinctValues, 1)).build();
            builder.addColumnStatistic(partitionCols.get(0), columnStatistic);
        } else if (partitionInfo instanceof ListPartitionInfo) {
            ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
            for (int i = 0; i < partitionCols.size(); i++) {
                ColumnRefOperator columnRef = partitionCols.get(i);
                // For multi-column list partition, pruning on any column should adjust the statistics
                if (columnRef == null) {
                    continue;
                }
                if (optimizerContext.getDumpInfo() != null) {
                    optimizerContext.getDumpInfo().addTableStatistics(olapTable,
                            partitionCols.get(i).getName(),
                            builder.getColumnStatistics(partitionCols.get(i)));
                }
                long ndv = extractDistinctPartitionValues(listPartitionInfo, selectedPartitionId, i);
                ColumnStatistic columnStatistic = ColumnStatistic.buildFrom(builder.getColumnStatistics(columnRef))
                        .setDistinctValuesCount(ndv).build();
                builder.addColumnStatistic(columnRef, columnStatistic);
            }
        }
    }

    @Override
    public Void visitLogicalProject(LogicalProjectOperator node, ExpressionContext context) {
        return computeProjectNode(context, node.getColumnRefMap());
    }

    @Override
    public Void visitPhysicalProject(PhysicalProjectOperator node, ExpressionContext context) {
        return computeProjectNode(context, node.getColumnRefMap());
    }

    private Void computeProjectNode(ExpressionContext context, Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        Preconditions.checkState(context.arity() == 1);

        Statistics.Builder builder = Statistics.builder();
        Statistics inputStatistics = context.getChildStatistics(0);
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

        Statistics.Builder allBuilder = Statistics.builder();
        allBuilder.setOutputRowCount(inputStatistics.getOutputRowCount());
        allBuilder.addColumnStatistics(inputStatistics.getColumnStatistics());

        for (ColumnRefOperator requiredColumnRefOperator : columnRefMap.keySet()) {
            ScalarOperator mapOperator = columnRefMap.get(requiredColumnRefOperator);
            if (mapOperator instanceof SubfieldOperator && context.getOptExpression() != null) {
                Operator child = context.getOptExpression().inputAt(0).getOp();
                if (child instanceof LogicalScanOperator || child instanceof PhysicalScanOperator) {
                    addSubFiledStatistics(child, ImmutableMap.of(requiredColumnRefOperator,
                            (SubfieldOperator) mapOperator), builder);
                    continue;
                }
            }
            ColumnStatistic outputStatistic =
                    ExpressionStatisticCalculator.calculate(mapOperator, allBuilder.build());
            builder.addColumnStatistic(requiredColumnRefOperator, outputStatistic);
            allBuilder.addColumnStatistic(requiredColumnRefOperator, outputStatistic);
        }

        context.setStatistics(builder.build());
        return visitOperator(context.getOp(), context);
    }

    @Override
    public Void visitLogicalAggregation(LogicalAggregationOperator node, ExpressionContext context) {
        return computeAggregateNode(node, context, node.getGroupingKeys(), node.getAggregations());
    }

    @Override
    public Void visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, ExpressionContext context) {
        return computeAggregateNode(node, context, node.getGroupBys(), node.getAggregations());
    }

    private Void computeAggregateNode(Operator node, ExpressionContext context, List<ColumnRefOperator> groupBys,
                                      Map<ColumnRefOperator, CallOperator> aggregations) {
        Preconditions.checkState(context.arity() == 1);
        Statistics.Builder builder = Statistics.builder();
        Statistics inputStatistics = context.getChildStatistics(0);

        //Update the statistics of the GroupBy column
        Map<ColumnRefOperator, ColumnStatistic> groupStatisticsMap = new HashMap<>();
        double rowCount = computeGroupByStatistics(groupBys, inputStatistics, groupStatisticsMap);
        PredicateColumnsMgr.getInstance()
                .recordGroupByColumns(aggregations, groupBys, optimizerContext.getColumnRefFactory(),
                        context.getOptExpression());

        //Update Node Statistics
        builder.addColumnStatistics(groupStatisticsMap);
        rowCount = min(inputStatistics.getOutputRowCount(), rowCount);
        builder.setOutputRowCount(rowCount);
        // use inputStatistics and aggregateNode cardinality to estimate aggregate call operator column statistics.
        // because of we need cardinality to estimate count function.
        double estimateCount = rowCount;
        aggregations.forEach((key, value) -> builder
                .addColumnStatistic(key,
                        ExpressionStatisticCalculator.calculate(value, inputStatistics, estimateCount)));

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    public static double computeGroupByStatistics(List<ColumnRefOperator> groupBys, Statistics inputStatistics,
                                                  Map<ColumnRefOperator, ColumnStatistic> groupStatisticsMap) {
        for (ColumnRefOperator groupByColumn : groupBys) {
            ColumnStatistic groupByColumnStatics = inputStatistics.getColumnStatistic(groupByColumn);
            ColumnStatistic.Builder statsBuilder = buildFrom(groupByColumnStatics);
            if (groupByColumnStatics.getNullsFraction() == 0) {
                statsBuilder.setNullsFraction(0);
            } else {
                statsBuilder.setNullsFraction(1 / (groupByColumnStatics.getDistinctValuesCount() + 1));
            }

            groupStatisticsMap.put(groupByColumn, statsBuilder.build());
        }

        // Use multi-column combined statistics for more accurate row count estimation
        double rowCount = 1;
        Set<ColumnRefOperator> columnsWithMultiColStats = new HashSet<>();
        // Try to get the largest subset with multi-column statistics
        Pair<Set<ColumnRefOperator>, MultiColumnCombinedStats> mcStats =
                inputStatistics.getLargestSubsetMCStats(groupStatisticsMap.keySet());

        if (mcStats != null) {
            Set<ColumnRefOperator> combinedMultiColumns = mcStats.first;
            long ndv = mcStats.second.getNdv();
            columnsWithMultiColStats.addAll(combinedMultiColumns);
            rowCount = ndv;

            // If all columns are covered by multi-column statistics, return the result directly
            if (columnsWithMultiColStats.size() == groupBys.size()) {
                return Math.min(Math.max(1, rowCount), inputStatistics.getOutputRowCount());
            }
        }

        // Process columns not covered by multi-column statistics
        boolean hasUnknownStats = false;
        List<ColumnRefOperator> columnsWithoutMultiColStats = new ArrayList<>();

        for (ColumnRefOperator column : groupBys) {
            if (columnsWithMultiColStats.contains(column)) {
                continue;
            }

            columnsWithoutMultiColStats.add(column);
            if (!hasUnknownStats && inputStatistics.getColumnStatistic(column).isUnknown()) {
                hasUnknownStats = true;
            }
        }

        // Choose different estimation strategies based on whether there are unknown statistics
        if (hasUnknownStats) {
            // Use default coefficients for estimation
            for (int i = 0; i < columnsWithoutMultiColStats.size(); i++) {
                if (i == 0) {
                    rowCount = inputStatistics.getOutputRowCount() *
                            StatisticsEstimateCoefficient.DEFAULT_GROUP_BY_CORRELATION_COEFFICIENT;
                } else {
                    rowCount *= StatisticsEstimateCoefficient.DEFAULT_GROUP_BY_EXPAND_COEFFICIENT;
                    if (rowCount > inputStatistics.getOutputRowCount()) {
                        rowCount = inputStatistics.getOutputRowCount();
                        break;
                    }
                }
            }
        } else {
            // Use column statistics for precise estimation
            for (int i = 0; i < columnsWithoutMultiColStats.size(); i++) {
                ColumnRefOperator column = columnsWithoutMultiColStats.get(i);
                ColumnStatistic columnStats = inputStatistics.getColumnStatistic(column);
                double cardinality = columnStats.getDistinctValuesCount() +
                        ((columnStats.getNullsFraction() == 0.0) ? 0 : 1);

                if (columnsWithMultiColStats.isEmpty() && i == 0) {
                    rowCount *= cardinality;
                } else {
                    // Adjust cardinality using correlation factor
                    double correlationFactor = Math.pow(
                            StatisticsEstimateCoefficient.UNKNOWN_GROUP_BY_CORRELATION_COEFFICIENT, i + 1D);
                    rowCount *= Math.max(1, cardinality * correlationFactor);

                    if (rowCount > inputStatistics.getOutputRowCount()) {
                        rowCount = inputStatistics.getOutputRowCount();
                        break;
                    }
                }
            }
        }

        return Math.min(Math.max(1, rowCount), inputStatistics.getOutputRowCount());
    }

    @Override
    public Void visitLogicalJoin(LogicalJoinOperator node, ExpressionContext context) {
        return computeJoinNode(context, node.getJoinType(), node.getOnPredicate());
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        return computeJoinNode(context, node.getJoinType(), node.getOnPredicate());
    }

    @Override
    public Void visitPhysicalMergeJoin(PhysicalMergeJoinOperator node, ExpressionContext context) {
        return computeJoinNode(context, node.getJoinType(), node.getOnPredicate());
    }

    @Override
    public Void visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator node, ExpressionContext context) {
        return computeJoinNode(context, node.getJoinType(), node.getOnPredicate());
    }

    private Void computeJoinNode(ExpressionContext context, JoinOperator joinType, ScalarOperator joinOnPredicate) {
        Preconditions.checkState(context.arity() == 2);

        List<ScalarOperator> allJoinPredicate = Utils.extractConjuncts(joinOnPredicate);
        Statistics leftStatistics = context.getChildStatistics(0);
        Statistics rightStatistics = context.getChildStatistics(1);
        // construct cross join statistics
        Statistics.Builder crossBuilder = Statistics.builder();
        crossBuilder.addColumnStatisticsFromOtherStatistic(leftStatistics, context.getChildOutputColumns(0), false);
        crossBuilder.addColumnStatisticsFromOtherStatistic(rightStatistics, context.getChildOutputColumns(1), false);
        double leftRowCount = leftStatistics.getOutputRowCount();
        double rightRowCount = rightStatistics.getOutputRowCount();
        double crossRowCount = StatisticUtils.multiplyRowCount(leftRowCount, rightRowCount);
        crossBuilder.setOutputRowCount(crossRowCount);

        List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(leftStatistics.getUsedColumns(),
                rightStatistics.getUsedColumns(), allJoinPredicate);

        PredicateColumnsMgr.getInstance().recordJoinPredicate(eqOnPredicates, optimizerContext.getColumnRefFactory(),
                context.getOptExpression());

        Statistics crossJoinStats = crossBuilder.build();
        double innerRowCount = -1;
        // For unknown column Statistics
        boolean hasUnknownColumnStatistics =
                eqOnPredicates.stream().map(PredicateOperator::getChildren).flatMap(Collection::stream)
                        .filter(ScalarOperator::isColumnRef)
                        .map(column -> crossJoinStats.getColumnStatistic((ColumnRefOperator) column))
                        .anyMatch(ColumnStatistic::isUnknown);
        if (hasUnknownColumnStatistics) {
            // To avoid anti-join estimation of 0 rows, the rows of inner join
            // need to take a table with a small number of rows
            if (joinType.isAntiJoin()) {
                innerRowCount = Math.max(0, Math.min(leftRowCount, rightRowCount));
            } else {
                // if the large table is ten million times larger than the small table, we'd better multiply
                // a filter coefficient.
                double scaleFactor = Math.max(leftRowCount, rightRowCount) / Math.min(leftRowCount, rightRowCount);
                if (scaleFactor >= Math.pow(10, 7)) {
                    innerRowCount = Math.max(1, Math.max(leftRowCount, rightRowCount)
                            * pow(PREDICATE_UNKNOWN_FILTER_COEFFICIENT, eqOnPredicates.size()));
                } else {
                    innerRowCount = Math.max(1, Math.max(leftRowCount, rightRowCount));
                }

            }
        }

        Statistics innerJoinStats;
        if (innerRowCount == -1) {
            innerJoinStats = estimateInnerJoinStatistics(crossJoinStats, eqOnPredicates);

            OptExpression optExpression = context.getOptExpression();
            SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();

            if (optExpression != null && sessionVariable.isEnableUKFKOpt()) {
                UKFKConstraintsCollector.collectColumnConstraints(optExpression);
                UKFKConstraints constraints = optExpression.getConstraints();
                Statistics ukfkJoinStat = buildStatisticsForUKFKJoin(joinType, constraints,
                        leftRowCount, rightRowCount, crossBuilder);
                if (ukfkJoinStat.getOutputRowCount() < innerJoinStats.getOutputRowCount()) {
                    innerJoinStats = ukfkJoinStat;
                }
            }
            innerRowCount = innerJoinStats.getOutputRowCount();
        } else {
            innerJoinStats = Statistics.buildFrom(crossJoinStats).setOutputRowCount(innerRowCount).build();
        }

        Statistics.Builder joinStatsBuilder;
        switch (joinType) {
            case CROSS_JOIN:
                joinStatsBuilder = Statistics.buildFrom(crossJoinStats);
                break;
            case INNER_JOIN:
                if (eqOnPredicates.isEmpty()) {
                    joinStatsBuilder = Statistics.buildFrom(crossJoinStats);
                    break;
                }
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                break;
            case LEFT_OUTER_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(innerRowCount, leftRowCount));
                computeNullFractionForOuterJoin(leftRowCount, innerRowCount, rightStatistics, joinStatsBuilder);
                break;
            case LEFT_SEMI_JOIN:
                joinStatsBuilder = Statistics.buildFrom(StatisticsEstimateUtils.adjustStatisticsByRowCount(
                        innerJoinStats, Math.min(leftRowCount, innerRowCount)));
                break;
            case RIGHT_SEMI_JOIN:
                joinStatsBuilder = Statistics.buildFrom(StatisticsEstimateUtils.adjustStatisticsByRowCount(
                        innerJoinStats, Math.min(rightRowCount, innerRowCount)));
                break;
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(
                        leftRowCount * StatisticsEstimateCoefficient.DEFAULT_ANTI_JOIN_SELECTIVITY_COEFFICIENT,
                        leftRowCount - innerRowCount));
                break;
            case RIGHT_OUTER_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(innerRowCount, rightRowCount));
                computeNullFractionForOuterJoin(rightRowCount, innerRowCount, leftStatistics, joinStatsBuilder);
                break;
            case RIGHT_ANTI_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(
                        rightRowCount * StatisticsEstimateCoefficient.DEFAULT_ANTI_JOIN_SELECTIVITY_COEFFICIENT,
                        rightRowCount - innerRowCount));
                break;
            case FULL_OUTER_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(1, leftRowCount + rightRowCount - innerRowCount));
                computeNullFractionForOuterJoin(leftRowCount + rightRowCount, innerRowCount, leftStatistics,
                        joinStatsBuilder);
                computeNullFractionForOuterJoin(leftRowCount + rightRowCount, innerRowCount, rightStatistics,
                        joinStatsBuilder);
                break;
            default:
                throw new StarRocksPlannerException("Not support join type : " + joinType,
                        ErrorType.INTERNAL_ERROR);
        }
        Statistics joinStats = joinStatsBuilder.build();

        allJoinPredicate.removeAll(eqOnPredicates);
        Statistics estimateStatistics = estimateStatistics(allJoinPredicate, joinStats);
        if (joinType.isLeftOuterJoin()) {
            estimateStatistics = Statistics.buildFrom(estimateStatistics)
                    .setOutputRowCount(Math.max(estimateStatistics.getOutputRowCount(), leftRowCount))
                    .build();
        } else if (joinType.isRightOuterJoin()) {
            estimateStatistics = Statistics.buildFrom(estimateStatistics)
                    .setOutputRowCount(Math.max(estimateStatistics.getOutputRowCount(), rightRowCount))
                    .build();
        } else if (joinType.isFullOuterJoin()) {
            estimateStatistics = Statistics.buildFrom(estimateStatistics)
                    .setOutputRowCount(Math.max(estimateStatistics.getOutputRowCount(), joinStats.getOutputRowCount()))
                    .build();
        }

        context.setStatistics(estimateStatistics);
        return visitOperator(context.getOp(), context);
    }

    private Statistics buildStatisticsForUKFKJoin(JoinOperator joinType, UKFKConstraints constraints,
                                                  double leftRowCount, double rightRowCount,
                                                  Statistics.Builder builder) {
        UKFKConstraints.JoinProperty joinProperty = constraints.getJoinProperty();
        if (joinProperty == null) {
            return builder.build();
        }
        if (joinType.isInnerJoin()) {
            if (joinProperty.isLeftUK) {
                builder.setOutputRowCount(rightRowCount);
            } else {
                builder.setOutputRowCount(leftRowCount);
            }
        } else if (joinType.isLeftOuterJoin()) {
            if (!joinProperty.isLeftUK) {
                builder.setOutputRowCount(leftRowCount);
            }
        } else if (joinType.isRightOuterJoin()) {
            if (joinProperty.isLeftUK) {
                builder.setOutputRowCount(rightRowCount);
            }
        } else if (joinType.isLeftSemiJoin()) {
            if (joinProperty.isLeftUK) {
                builder.setOutputRowCount(leftRowCount);
            }
        } else if (joinType.isRightSemiJoin()) {
            if (!joinProperty.isLeftUK) {
                builder.setOutputRowCount(rightRowCount);
            }
        }

        return builder.build();
    }

    private void computeNullFractionForOuterJoin(double outerTableRowCount, double innerJoinRowCount,
                                                 Statistics statistics, Statistics.Builder builder) {
        if (outerTableRowCount > innerJoinRowCount) {
            double nullRowCount = outerTableRowCount - innerJoinRowCount;
            for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : statistics.getColumnStatistics().entrySet()) {
                ColumnStatistic columnStatistic = entry.getValue();
                double columnNullCount = columnStatistic.getNullsFraction() * innerJoinRowCount;
                double newNullFraction = (columnNullCount + nullRowCount) / outerTableRowCount;
                builder.addColumnStatistic(entry.getKey(),
                        buildFrom(columnStatistic).setNullsFraction(newNullFraction).build());
            }
        }
    }

    @Override
    public Void visitLogicalUnion(LogicalUnionOperator node, ExpressionContext context) {
        return computeUnionNode(node, context, node.getOutputColumnRefOp(), node.getChildOutputColumns());
    }

    @Override
    public Void visitPhysicalUnion(PhysicalUnionOperator node, ExpressionContext context) {
        return computeUnionNode(node, context, node.getOutputColumnRefOp(), node.getChildOutputColumns());
    }

    private Void computeUnionNode(Operator node, ExpressionContext context, List<ColumnRefOperator> outputColumnRef,
                                  List<List<ColumnRefOperator>> childOutputColumns) {
        Statistics.Builder builder = Statistics.builder();
        if (context.arity() < 1) {
            context.setStatistics(builder.build());
            return visitOperator(node, context);
        }
        List<ColumnStatistic> estimateColumnStatistics = childOutputColumns.get(0).stream().map(columnRefOperator ->
                context.getChildStatistics(0).getColumnStatistic(columnRefOperator)).collect(Collectors.toList());

        boolean isFromIcebergEqualityDeleteRewrite;
        if (node.isLogical()) {
            isFromIcebergEqualityDeleteRewrite = ((LogicalUnionOperator) node).isFromIcebergEqualityDeleteRewrite();
        } else {
            isFromIcebergEqualityDeleteRewrite = ((PhysicalUnionOperator) node).isFromIcebergEqualityDeleteRewrite();
        }

        for (int outputIdx = 0; outputIdx < outputColumnRef.size(); ++outputIdx) {
            double estimateRowCount = context.getChildrenStatistics().get(0).getOutputRowCount();
            for (int childIdx = 1; childIdx < context.arity(); ++childIdx) {
                ColumnRefOperator childOutputColumn = childOutputColumns.get(childIdx).get(outputIdx);
                Statistics childStatistics = context.getChildStatistics(childIdx);
                ColumnStatistic estimateColumnStatistic;
                if (!isFromIcebergEqualityDeleteRewrite) {
                    estimateColumnStatistic = StatisticsEstimateUtils.unionColumnStatistic(
                            estimateColumnStatistics.get(outputIdx), estimateRowCount,
                            childStatistics.getColumnStatistic(childOutputColumn), childStatistics.getOutputRowCount());
                } else {
                    estimateColumnStatistic = estimateColumnStatistics.get(outputIdx);
                }

                // set new estimate column statistic
                estimateColumnStatistics.set(outputIdx, estimateColumnStatistic);
                estimateRowCount += childStatistics.getOutputRowCount();
            }
            builder.addColumnStatistic(outputColumnRef.get(outputIdx), estimateColumnStatistics.get(outputIdx));
            builder.setOutputRowCount(estimateRowCount);
        }

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalExcept(LogicalExceptOperator node, ExpressionContext context) {
        return computeExceptNode(node, context, node.getOutputColumnRefOp());
    }

    @Override
    public Void visitPhysicalExcept(PhysicalExceptOperator node, ExpressionContext context) {
        return computeExceptNode(node, context, node.getOutputColumnRefOp());
    }

    private Void computeExceptNode(Operator node, ExpressionContext context, List<ColumnRefOperator> outputColumnRef) {
        Statistics.Builder builder = Statistics.builder();

        builder.setOutputRowCount(context.getChildStatistics(0).getOutputRowCount());

        for (ColumnRefOperator columnRefOperator : outputColumnRef) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalIntersect(LogicalIntersectOperator node, ExpressionContext context) {
        return computeIntersectNode(node, context, node.getOutputColumnRefOp(), node.getChildOutputColumns());
    }

    @Override
    public Void visitPhysicalIntersect(PhysicalIntersectOperator node, ExpressionContext context) {
        return computeIntersectNode(node, context, node.getOutputColumnRefOp(), node.getChildOutputColumns());
    }

    private Void computeIntersectNode(Operator node, ExpressionContext context,
                                      List<ColumnRefOperator> outputColumnRef,
                                      List<List<ColumnRefOperator>> childOutputColumns) {
        Statistics.Builder builder = Statistics.builder();

        int minOutputIndex = 0;
        double minOutputRowCount = context.getChildStatistics(0).getOutputRowCount();
        for (int i = 1; i < context.arity(); ++i) {
            double childOutputRowCount = context.getChildStatistics(i).getOutputRowCount();
            if (childOutputRowCount < minOutputRowCount) {
                minOutputIndex = i;
                minOutputRowCount = childOutputRowCount;
            }
        }
        // use child column statistics which has min OutputRowCount
        Statistics minOutputChildStats = context.getChildStatistics(minOutputIndex);
        for (int i = 0; i < outputColumnRef.size(); ++i) {
            ColumnRefOperator columnRefOperator = childOutputColumns.get(minOutputIndex).get(i);
            builder.addColumnStatistic(outputColumnRef.get(i), minOutputChildStats.getColumnStatistics().
                    get(columnRefOperator));
        }
        // compute the children maximum output row count with distinct value
        double childMaxDistinctOutput = Double.MAX_VALUE;
        for (int childIndex = 0; childIndex < context.arity(); ++childIndex) {
            double childDistinctOutput = 1.0;
            for (ColumnRefOperator columnRefOperator : childOutputColumns.get(childIndex)) {
                childDistinctOutput *= context.getChildStatistics(childIndex).getColumnStatistics().
                        get(columnRefOperator).getDistinctValuesCount();
            }
            if (childDistinctOutput < childMaxDistinctOutput) {
                childMaxDistinctOutput = childDistinctOutput;
            }
        }
        double outputRowCount = Math.min(minOutputChildStats.getOutputRowCount(), childMaxDistinctOutput);
        builder.setOutputRowCount(outputRowCount);

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalValues(LogicalValuesOperator node, ExpressionContext context) {
        return computeValuesNode(context, node.getColumnRefSet(), node.getRows());
    }

    @Override
    public Void visitPhysicalValues(PhysicalValuesOperator node, ExpressionContext context) {
        return computeValuesNode(context, node.getColumnRefSet(), node.getRows());
    }

    private Void computeValuesNode(ExpressionContext context, List<ColumnRefOperator> columnRefSet,
                                   List<List<ScalarOperator>> rows) {
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRef : columnRefSet) {
            builder.addColumnStatistic(columnRef, ColumnStatistic.unknown());
        }

        builder.setOutputRowCount(rows.size());

        context.setStatistics(builder.build());
        return visitOperator(context.getOp(), context);
    }

    @Override
    public Void visitLogicalRepeat(LogicalRepeatOperator node, ExpressionContext context) {
        return computeRepeatNode(context, node.getOutputGrouping(), node.getGroupingIds(), node.getRepeatColumnRef());
    }

    @Override
    public Void visitPhysicalRepeat(PhysicalRepeatOperator node, ExpressionContext context) {
        return computeRepeatNode(context, node.getOutputGrouping(), node.getGroupingIds(), node.getRepeatColumnRef());
    }

    private Void computeRepeatNode(ExpressionContext context, List<ColumnRefOperator> outputGrouping,
                                   List<List<Long>> groupingIds, List<List<ColumnRefOperator>> repeatColumnRef) {
        Preconditions.checkState(context.arity() == 1);
        Preconditions.checkState(outputGrouping.size() == groupingIds.size());
        Statistics.Builder builder = Statistics.builder();
        for (int index = 0; index < outputGrouping.size(); ++index) {
            // calculate the column statistics for grouping
            List<Long> groupingId = groupingIds.get(index);
            builder.addColumnStatistic(outputGrouping.get(index),
                    new ColumnStatistic(Collections.min(groupingId), Collections.max(groupingId), 0, 8,
                            groupingId.size()));
        }

        Statistics inputStatistics = context.getChildStatistics(0);
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(inputStatistics.getOutputRowCount() * repeatColumnRef.size());

        context.setStatistics(builder.build());
        return visitOperator(context.getOp(), context);
    }

    @Override
    public Void visitLogicalTableFunction(LogicalTableFunctionOperator node, ExpressionContext context) {
        return computeTableFunctionNode(context, node.getOutputColRefs(), node.getFn().functionName(),
                node.getFnParamColumnProject().stream().map(x -> x.first).collect(Collectors.toList()));
    }

    @Override
    public Void visitPhysicalTableFunction(PhysicalTableFunctionOperator node, ExpressionContext context) {
        return computeTableFunctionNode(context, node.getOutputColRefs(), node.getFn().functionName(),
                node.getFnParamColumnRefs());
    }

    private Void computeTableFunctionNode(ExpressionContext context, List<ColumnRefOperator> outputColumns,
                                          String funcName, List<ColumnRefOperator> fnParamColumnRefs) {
        Statistics.Builder builder = Statistics.builder();

        Statistics inputStatistics = context.getChildStatistics(0);
        double rowCount = inputStatistics.getOutputRowCount();
        Map<ColumnRefOperator, ColumnStatistic> columnStats = inputStatistics.getColumnStatistics();

        for (ColumnRefOperator col : outputColumns) {
            if (columnStats.containsKey(col)) {
                builder.addColumnStatistic(col, columnStats.get(col));
            } else {
                builder.addColumnStatistic(col, ColumnStatistic.unknown());
            }
        }

        if (funcName.equalsIgnoreCase("unnest")) {
            double maxRows = rowCount;
            for (ColumnRefOperator column : fnParamColumnRefs) {
                ColumnStatistic columnStatistic = inputStatistics.getColumnStatistic(column);
                if (columnStatistic != null) {
                    double collectionSize = columnStatistic.getCollectionSize();
                    if (collectionSize >= 1 && collectionSize * rowCount >= maxRows) {
                        maxRows = collectionSize * rowCount;
                    }
                }
            }
            rowCount = maxRows;
        }

        builder.setOutputRowCount(rowCount);

        context.setStatistics(builder.build());
        return visitOperator(context.getOp(), context);
    }

    public Statistics estimateInnerJoinStatistics(Statistics statistics, List<BinaryPredicateOperator> eqOnPredicates) {
        if (eqOnPredicates.isEmpty()) {
            return statistics;
        }
        if (ConnectContext.get().getSessionVariable().isUseCorrelatedJoinEstimate()) {
            return estimatedInnerJoinStatisticsAssumeCorrelated(statistics, eqOnPredicates);
        } else {
            return Statistics.buildFrom(statistics)
                    .setOutputRowCount(estimateInnerRowCountMiddleGround(statistics, eqOnPredicates)).build();
        }
    }

    // The implementation here refers to Presto
    // Join equality clauses are usually correlated. Therefore we shouldn't treat each join equality
    // clause separately because stats estimates would be way off. Instead we choose so called
    // "driving predicate" which mostly reduces join output rows cardinality and apply UNKNOWN_FILTER_COEFFICIENT
    // for other (auxiliary) predicates.
    private Statistics estimatedInnerJoinStatisticsAssumeCorrelated(Statistics statistics,
                                                                    List<BinaryPredicateOperator> eqOnPredicates) {
        Queue<BinaryPredicateOperator> remainingEqOnPredicates = new LinkedList<>(eqOnPredicates);
        BinaryPredicateOperator drivingPredicate = remainingEqOnPredicates.poll();
        Statistics result = statistics;
        for (int i = 0; i < eqOnPredicates.size(); ++i) {
            Statistics estimateStatistics =
                    estimateByEqOnPredicates(statistics, drivingPredicate, remainingEqOnPredicates);
            if (estimateStatistics.getOutputRowCount() < result.getOutputRowCount()) {
                result = estimateStatistics;
            }
            remainingEqOnPredicates.add(drivingPredicate);
            drivingPredicate = remainingEqOnPredicates.poll();
        }
        return result;
    }

    private double getPredicateSelectivity(PredicateOperator predicateOperator, Statistics statistics) {
        Statistics estimatedStatistics = estimateStatistics(Lists.newArrayList(predicateOperator), statistics);
        return estimatedStatistics.getOutputRowCount() / statistics.getOutputRowCount();
    }

    //  This estimate join row count method refers to ORCA.
    //  use a damping method to moderately decrease the impact of subsequent predicates to account for correlated columns.
    //  This damping only occurs on sorted predicates of the same table, otherwise we assume independence.
    //  complex predicate(such as t1.a + t2.b = t3.c) also assume independence.
    //  For example, given AND predicates (t1.a = t2.a AND t1.b = t2.b AND t2.b = t3.a) with the given selectivity(Represented
    //  as S for simple):
    //  t1.a = t2.a has selectivity(S1) 0.3
    //  t1.b = t2.b has selectivity(S2) 0.5
    //  t2.b = t3.a has selectivity(S3) 0.1
    //  S1 and S2 would use the sqrt algorithm, and S3 is independent. Additionally,
    //  S2 has a larger selectivity so it comes first.
    //  The cumulative selectivity would be as follows:
    //     S = ( S2 * sqrt(S1) ) * S3
    //   0.03 = 0.5 * sqrt(0.3) * 0.1
    //  Note: This will underestimate the cardinality of highly correlated columns and overestimate the
    //  cardinality of highly independent columns, but seems to be a good middle ground in the absence
    //  of correlated column statistics
    private double estimateInnerRowCountMiddleGround(Statistics statistics,
                                                     List<BinaryPredicateOperator> eqOnPredicates) {
        Map<Pair<Integer, Integer>, List<Pair<BinaryPredicateOperator, Double>>> tablePairToPredicateWithSelectivity =
                Maps.newHashMap();
        List<Double> complexEqOnPredicatesSelectivity = Lists.newArrayList();
        double cumulativeSelectivity = 1.0;
        computeJoinOnPredicateSelectivityMap(tablePairToPredicateWithSelectivity, complexEqOnPredicatesSelectivity,
                eqOnPredicates, statistics);

        for (Map.Entry<Pair<Integer, Integer>, List<Pair<BinaryPredicateOperator, Double>>> entry :
                tablePairToPredicateWithSelectivity.entrySet()) {
            entry.getValue().sort((o1, o2) -> ((int) (o2.second - o1.second)));
            for (int index = 0; index < entry.getValue().size(); ++index) {
                double selectivity = entry.getValue().get(index).second;
                double sqrtNum = pow(2, index);
                cumulativeSelectivity = cumulativeSelectivity * pow(selectivity, 1 / sqrtNum);
            }
        }
        for (double complexSelectivity : complexEqOnPredicatesSelectivity) {
            cumulativeSelectivity *= complexSelectivity;
        }
        return cumulativeSelectivity * statistics.getOutputRowCount();
    }

    private void computeJoinOnPredicateSelectivityMap(
            Map<Pair<Integer, Integer>, List<Pair<BinaryPredicateOperator, Double>>> tablePairToPredicateWithSelectivity,
            List<Double> complexEqOnPredicatesSelectivity, List<BinaryPredicateOperator> eqOnPredicates,
            Statistics statistics) {
        for (BinaryPredicateOperator predicateOperator : eqOnPredicates) {
            // calculate the selectivity of the predicate
            double selectivity = getPredicateSelectivity(predicateOperator, statistics);

            ColumnRefSet leftChildColumns = predicateOperator.getChild(0).getUsedColumns();
            ColumnRefSet rightChildColumns = predicateOperator.getChild(1).getUsedColumns();
            Set<Integer> leftChildRelationIds =
                    leftChildColumns.getStream().map(columnRefFactory::getRelationId).collect(Collectors.toSet());
            Set<Integer> rightChildRelationIds =
                    rightChildColumns.getStream().map(columnRefFactory::getRelationId)
                            .collect(Collectors.toSet());

            // Check that the predicate is complex, such as t1.a + t2.b = t3.c is complex predicate
            if (leftChildRelationIds.size() == 1 && rightChildRelationIds.size() == 1) {
                int leftChildRelationId = Lists.newArrayList(leftChildRelationIds).get(0);
                int rightChildRelationId = Lists.newArrayList(rightChildRelationIds).get(0);
                Pair<Integer, Integer> relationIdPair = new Pair<>(leftChildRelationId, rightChildRelationId);
                if (!tablePairToPredicateWithSelectivity.containsKey(relationIdPair)) {
                    tablePairToPredicateWithSelectivity.put(relationIdPair, Lists.newArrayList());
                }
                tablePairToPredicateWithSelectivity.get(relationIdPair).add(new Pair<>(predicateOperator, selectivity));
            } else {
                // this equal on predicate is complex
                complexEqOnPredicatesSelectivity.add(getPredicateSelectivity(predicateOperator, statistics));
            }
        }
    }

    public Statistics estimateByEqOnPredicates(Statistics statistics, BinaryPredicateOperator divingPredicate,
                                               Collection<BinaryPredicateOperator> remainingEqOnPredicate) {
        Statistics estimateStatistics = estimateStatistics(ImmutableList.of(divingPredicate), statistics);
        for (BinaryPredicateOperator ignored : remainingEqOnPredicate) {
            estimateStatistics = estimateByAuxiliaryPredicates(estimateStatistics);
        }
        return estimateStatistics;
    }

    public Statistics estimateByAuxiliaryPredicates(Statistics estimateStatistics) {
        double rowCount = estimateStatistics.getOutputRowCount() *
                StatisticsEstimateCoefficient.UNKNOWN_AUXILIARY_FILTER_COEFFICIENT;
        return Statistics.buildFrom(estimateStatistics).setOutputRowCount(rowCount).build();
    }

    @Override
    public Void visitLogicalTopN(LogicalTopNOperator node, ExpressionContext context) {
        return computeTopNNode(context, node);
    }

    @Override
    public Void visitPhysicalTopN(PhysicalTopNOperator node, ExpressionContext context) {
        return computeTopNNode(context, node);
    }

    private Void computeTopNNode(ExpressionContext context, Operator node) {
        Preconditions.checkState(context.arity() == 1);

        long partitionLimit = 0;
        List<ColumnRefOperator> partitions = Collections.emptyList();
        if (node instanceof LogicalTopNOperator) {
            partitionLimit = ((LogicalTopNOperator) node).getPartitionLimit();
            partitions = ((LogicalTopNOperator) node).getPartitionByColumns();
        } else if (node instanceof PhysicalTopNOperator) {
            partitionLimit = ((PhysicalTopNOperator) node).getPartitionLimit();
            partitions = ((PhysicalTopNOperator) node).getPartitionByColumns();
        }

        Statistics.Builder builder = Statistics.builder();
        Statistics inputStatistics = context.getChildStatistics(0);
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

        Map<ColumnRefOperator, CallOperator> preAggFnCall = null;
        if (node instanceof PhysicalTopNOperator) {
            PhysicalTopNOperator physicalTopNOperator = (PhysicalTopNOperator) node;
            preAggFnCall = physicalTopNOperator.getPreAggCall();
        } else {
            LogicalTopNOperator logicalTopNOperator = (LogicalTopNOperator) node;
            preAggFnCall = logicalTopNOperator.getPartitionPreAggCall();
        }
        if (preAggFnCall != null) {
            preAggFnCall.forEach((key, value) -> builder
                    .addColumnStatistic(key,
                            ExpressionStatisticCalculator.calculate(value, inputStatistics,
                                    inputStatistics.getOutputRowCount())));
        }

        if (partitionLimit > 0 && !partitions.isEmpty()
                && partitions.stream().map(inputStatistics::getColumnStatistic).noneMatch(ColumnStatistic::isUnknown)) {
            double partitionNums = partitions.stream()
                    .map(inputStatistics::getColumnStatistic)
                    .map(ColumnStatistic::getDistinctValuesCount).reduce((a, b) -> a * b).orElse(1D);
            double rowCount = Math.min(inputStatistics.getOutputRowCount(), partitionLimit * partitionNums);
            builder.setOutputRowCount(rowCount);
        }

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalAssertOneRow(LogicalAssertOneRowOperator node, ExpressionContext context) {
        return computeAssertOneRowNode(context);
    }

    @Override
    public Void visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, ExpressionContext context) {
        return computeAssertOneRowNode(context);
    }

    private Void computeAssertOneRowNode(ExpressionContext context) {
        Statistics inputStatistics = context.getChildStatistics(0);

        Statistics.Builder builder = Statistics.builder();
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(1);

        context.setStatistics(builder.build());
        return visitOperator(context.getOp(), context);
    }

    @Override
    public Void visitLogicalFilter(LogicalFilterOperator node, ExpressionContext context) {
        return computeFilterNode(node, context);
    }

    @Override
    public Void visitPhysicalFilter(PhysicalFilterOperator node, ExpressionContext context) {
        return computeFilterNode(node, context);
    }

    private Void computeFilterNode(Operator node, ExpressionContext context) {
        Statistics inputStatistics = context.getChildStatistics(0);

        Statistics.Builder builder = Statistics.builder();
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalAnalytic(LogicalWindowOperator node, ExpressionContext context) {
        PredicateColumnsMgr.getInstance().recordWindowPartitionBy(node.getPartitionExpressions(),
                optimizerContext.getColumnRefFactory(), context.getOptExpression());
        return computeAnalyticNode(context, node.getWindowCall());
    }

    @Override
    public Void visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
        PredicateColumnsMgr.getInstance().recordWindowPartitionBy(node.getPartitionExpressions(),
                optimizerContext.getColumnRefFactory(), context.getOptExpression());
        return computeAnalyticNode(context, node.getAnalyticCall());
    }

    private Void computeAnalyticNode(ExpressionContext context, Map<ColumnRefOperator, CallOperator> analyticCall) {
        Preconditions.checkState(context.arity() == 1);

        Statistics.Builder builder = Statistics.builder();
        Statistics inputStatistics = context.getChildStatistics(0);
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());

        analyticCall.forEach((key, value) -> builder
                .addColumnStatistic(key, ExpressionStatisticCalculator.calculate(value, inputStatistics)));

        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

        context.setStatistics(builder.build());
        return visitOperator(context.getOp(), context);
    }

    public Statistics estimateStatistics(List<ScalarOperator> predicateList, Statistics statistics) {
        if (predicateList.isEmpty()) {
            return statistics;
        }

        Statistics result = statistics;
        for (ScalarOperator predicate : predicateList) {
            result = PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics);
        }

        // avoid sample statistics filter all data, save one rows least
        if (statistics.getOutputRowCount() > 0 && result.getOutputRowCount() == 0) {
            return Statistics.buildFrom(result).setOutputRowCount(1).build();
        }
        return result;
    }

    @Override
    public Void visitLogicalLimit(LogicalLimitOperator node, ExpressionContext context) {
        Statistics inputStatistics = context.getChildStatistics(0);

        Statistics.Builder builder = Statistics.builder();
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(node.getLimit());

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalLimit(PhysicalLimitOperator node, ExpressionContext context) {
        Statistics inputStatistics = context.getChildStatistics(0);

        Statistics.Builder builder = Statistics.builder();
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(node.getLimit());

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalCTEAnchor(LogicalCTEAnchorOperator node, ExpressionContext context) {
        context.setStatistics(context.getChildStatistics(1));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, ExpressionContext context) {
        context.setStatistics(context.getChildStatistics(1));
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalCTEConsume(LogicalCTEConsumeOperator node, ExpressionContext context) {
        return computeCTEConsume(node, context, node.getCteId(), node.getCteOutputColumnRefMap());
    }

    @Override
    public Void visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, ExpressionContext context) {
        return computeCTEConsume(node, context, node.getCteId(), node.getCteOutputColumnRefMap());
    }

    private Void computeCTEConsume(Operator node, ExpressionContext context, int cteId,
                                   Map<ColumnRefOperator, ColumnRefOperator> columnRefMap) {

        if (!context.getChildrenStatistics().isEmpty() && context.getChildStatistics(0) != null) {
            //  use the statistics of children first
            context.setStatistics(context.getChildStatistics(0));
            Projection projection = node.getProjection();
            if (projection != null) {
                Statistics.Builder statisticsBuilder = Statistics.buildFrom(context.getStatistics());
                for (ColumnRefOperator columnRefOperator : projection.getColumnRefMap().keySet()) {
                    ScalarOperator mapOperator = projection.getColumnRefMap().get(columnRefOperator);
                    statisticsBuilder.addColumnStatistic(columnRefOperator,
                            ExpressionStatisticCalculator.calculate(mapOperator, context.getStatistics()));
                }
                context.setStatistics(statisticsBuilder.build());
            }
            return null;
        }

        // None children, may force CTE, use the statistics of producer
        Optional<Statistics> produceStatisticsOp = optimizerContext.getCteContext().getCTEStatistics(cteId);
        Preconditions.checkState(produceStatisticsOp.isPresent(),
                "cannot obtain cte statistics for %s", node);
        Statistics produceStatistics = produceStatisticsOp.get();
        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator ref : columnRefMap.keySet()) {
            ColumnRefOperator produceRef = columnRefMap.get(ref);
            ColumnStatistic statistic = produceStatistics.getColumnStatistic(produceRef);
            builder.addColumnStatistic(ref, statistic);
        }

        builder.setOutputRowCount(produceStatistics.getOutputRowCount());
        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalCTEProduce(LogicalCTEProduceOperator node, ExpressionContext context) {
        Statistics statistics = context.getChildStatistics(0);
        context.setStatistics(statistics);
        optimizerContext.getCteContext().addCTEStatistics(node.getCteId(), context.getChildStatistics(0));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
        Statistics statistics = context.getChildStatistics(0);
        context.setStatistics(statistics);
        optimizerContext.getCteContext().addCTEStatistics(node.getCteId(), context.getChildStatistics(0));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        context.setStatistics(context.getChildStatistics(0));
        return visitOperator(node, context);
    }

    // avoid use partition cols filter rows twice
    @VisibleForTesting
    public ScalarOperator removePartitionPredicate(ScalarOperator predicate, Operator operator,
                                                   OptimizerContext optimizerContext) {
        boolean isTableTypeSupported = operator instanceof LogicalIcebergScanOperator ||
                isOlapScanListPartitionTable(operator);
        if (isTableTypeSupported && !optimizerContext.isObtainedFromInternalStatistics()) {
            LogicalScanOperator scanOperator = operator.cast();
            List<String> partitionColNames = scanOperator.getTable().getPartitionColumnNames();
            partitionColNames.addAll(ListPartitionPruner.deduceGenerateColumns(scanOperator));

            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            List<ScalarOperator> newPredicates = Lists.newArrayList();
            for (ScalarOperator scalarOperator : conjuncts) {
                if (ListPartitionPruner.canPruneWithConjunct(scalarOperator) &&
                        isPartitionCol(scalarOperator.getChild(0), partitionColNames)) {
                    // drop this predicate
                } else {
                    newPredicates.add(scalarOperator);
                }
            }
            return newPredicates.isEmpty() ? ConstantOperator.TRUE : Utils.compoundAnd(newPredicates);
        }
        return predicate;
    }

    // NOTE: Why list partition ?
    // The list partition only have one unique value for each partition, but range partition doesn't.
    // So only the partition-predicate of list partition can be removed without affect the cardinality estimation
    private boolean isOlapScanListPartitionTable(Operator operator) {
        if (!(operator instanceof LogicalOlapScanOperator)) {
            return false;
        }
        LogicalOlapScanOperator scan = operator.cast();
        OlapTable table = (OlapTable) scan.getTable();
        return table.getPartitionInfo().isListPartition();
    }

    private boolean isPartitionCol(ScalarOperator scalarOperator, Collection<String> partitionColumns) {
        if (scalarOperator.isColumnRef()) {
            String colName = ((ColumnRefOperator) scalarOperator).getName();
            return partitionColumns.contains(StringUtils.lowerCase(colName));
        } else if (scalarOperator instanceof CastOperator && scalarOperator.getChild(0).isColumnRef()) {
            String colName = ((ColumnRefOperator) scalarOperator.getChild(0)).getName();
            return partitionColumns.contains(StringUtils.lowerCase(colName));
        }
        return false;
    }

    private long extractDistinctPartitionValues(ListPartitionInfo listPartitionInfo, Collection<Long> selectedPartitionId,
                                                int partitionColIdx) {
        Set<String> distinctValues = Sets.newHashSet();
        for (long partitionId : selectedPartitionId) {
            if (listPartitionInfo.getIdToMultiValues().containsKey(partitionId)) {
                List<List<String>> values = listPartitionInfo.getIdToMultiValues().get(partitionId);
                values.forEach(v -> distinctValues.add(v.get(partitionColIdx)));
            } else if (listPartitionInfo.getIdToValues().containsKey(partitionId)) {
                distinctValues.addAll(listPartitionInfo.getIdToValues().get(partitionId));
            }

        }
        return Math.max(1, distinctValues.size());
    }
}
