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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
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
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMergeJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
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
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.stream.LogicalBinlogScanOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;
import com.starrocks.statistic.StatisticUtils;
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
import static java.lang.Math.max;
import static java.lang.Math.min;

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

        Statistics statistics = context.getStatistics();
        if (null != predicate) {
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
            Preconditions.checkState(projection.getCommonSubOperatorMap().isEmpty());
            for (ColumnRefOperator columnRefOperator : projection.getColumnRefMap().keySet()) {
                ScalarOperator mapOperator = projection.getColumnRefMap().get(columnRefOperator);
                statisticsBuilder.addColumnStatistic(columnRefOperator,
                        ExpressionStatisticCalculator.calculate(mapOperator, statisticsBuilder.build()));
            }

        }
        context.setStatistics(statisticsBuilder.build());
        return null;
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
        return computeOlapScanNode(node, context, node.getTable(), new ArrayList<>(),
                node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitLogicalTableFunctionTableScan(LogicalTableFunctionTableScanOperator node, ExpressionContext context) {
        return computeFileScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalStreamScan(PhysicalStreamScanOperator node, ExpressionContext context) {
        return computeOlapScanNode(node, context, node.getTable(), new ArrayList<>(),
                node.getColRefToColumnMetaMap());
    }

    private Void computeOlapScanNode(Operator node, ExpressionContext context, Table table,
                                     Collection<Long> selectedPartitionIds,
                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Preconditions.checkState(context.arity() == 0);
        // 1. get table row count
        long tableRowCount = StatisticsCalcUtils.getTableRowCount(table, node, optimizerContext);
        // 2. get required columns statistics
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(table, colRefToColumnMetaMap, optimizerContext);
        if (tableRowCount <= 1) {
            builder.setTableRowCountMayInaccurate(true);
        }
        // 3. deal with column statistics for partition prune
        OlapTable olapTable = (OlapTable) table;
        ColumnStatistic partitionStatistic = adjustPartitionStatistic(selectedPartitionIds, olapTable);
        if (partitionStatistic != null) {
            String partitionColumnName = olapTable.getPartitionColumnNames().get(0);
            Optional<Map.Entry<ColumnRefOperator, Column>> partitionColumnEntry =
                    colRefToColumnMetaMap.entrySet().stream().
                            filter(column -> column.getValue().getName().equalsIgnoreCase(partitionColumnName))
                            .findAny();
            // partition prune maybe because partition has none data
            partitionColumnEntry.ifPresent(entry -> builder.addColumnStatistic(entry.getKey(), partitionStatistic));
        }

        builder.setOutputRowCount(tableRowCount);
        // 4. estimate cardinality
        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalIcebergScan(LogicalIcebergScanOperator node, ExpressionContext context) {
        return computeIcebergScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalIcebergScan(PhysicalIcebergScanOperator node, ExpressionContext context) {
        return computeIcebergScanNode(node, context, node.getTable(), node.getColRefToColumnMetaMap());
    }

    private Void computeIcebergScanNode(Operator node, ExpressionContext context, Table table,
                                        Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        if (context.getStatistics() == null) {
            String catalogName = ((IcebergTable) table).getCatalogName();
            Statistics stats = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                    optimizerContext, catalogName, table, colRefToColumnMetaMap, null, node.getPredicate());
            context.setStatistics(stats);
        }

        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalDeltaLakeScan(LogicalDeltaLakeScanOperator node, ExpressionContext context) {
        return computeDeltaLakeScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    @Override
    public Void visitPhysicalDeltaLakeScan(PhysicalDeltaLakeScanOperator node, ExpressionContext context) {
        return computeDeltaLakeScanNode(node, context, node.getColRefToColumnMetaMap());
    }

    private Void computeDeltaLakeScanNode(Operator node, ExpressionContext context,
                                          Map<ColumnRefOperator, Column> columnRefOperatorColumnMap) {
        // Use default statistics for now.
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
            String catalogName = ((PaimonTable) table).getCatalogName();
            Statistics stats = GlobalStateMgr.getCurrentState().getMetadataMgr().getTableStatistics(
                    optimizerContext, catalogName, table, columnRefOperatorColumnMap, null, node.getPredicate());
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
            List<PartitionKey> partitionKeys = predicates.getSelectedPartitionKeys();

            String catalogName = ((HiveMetaStoreTable) table).getCatalogName();
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
    private ColumnStatistic adjustPartitionStatistic(Collection<Long> selectedPartitionId, OlapTable olapTable) {
        int selectedPartitionsSize = selectedPartitionId.size();
        int allNoEmptyPartitionsSize = (int) olapTable.getPartitions().stream().filter(Partition::hasData).count();
        if (selectedPartitionsSize != allNoEmptyPartitionsSize) {
            if (olapTable.getPartitionColumnNames().size() != 1) {
                return null;
            }
            String partitionColumn = olapTable.getPartitionColumnNames().get(0);
            ColumnStatistic partitionColumnStatistic =
                    GlobalStateMgr.getCurrentStatisticStorage().getColumnStatistic(olapTable, partitionColumn);

            if (optimizerContext.getDumpInfo() != null) {
                optimizerContext.getDumpInfo().addTableStatistics(olapTable, partitionColumn, partitionColumnStatistic);
            }

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Map.Entry<Long, Range<PartitionKey>>> rangeList;
                try {
                    rangeList = rangePartitionInfo.getSortedRangeMap(new HashSet<>(selectedPartitionId));
                } catch (AnalysisException e) {
                    LOG.warn("get sorted range partition failed, msg : " + e.getMessage());
                    return null;
                }
                if (rangeList.isEmpty()) {
                    return null;
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
                    try {
                        maxDateLiteral = maxLiteral instanceof MaxLiteral ? new DateLiteral(Type.DATE, true) :
                                (DateLiteral) maxLiteral;
                    } catch (AnalysisException e) {
                        LOG.warn("get max date literal failed, msg : " + e.getMessage());
                        return null;
                    }
                    min = Utils.getLongFromDateTime(minDateLiteral.toLocalDateTime());
                    max = Utils.getLongFromDateTime(maxDateLiteral.toLocalDateTime());
                } else {
                    min = firstKey.getValue().lowerEndpoint().getKeys().get(0).getDoubleValue();
                    max = lastKey.getValue().upperEndpoint().getKeys().get(0).getDoubleValue();
                }
                double distinctValues =
                        partitionColumnStatistic.getDistinctValuesCount() * 1.0 * selectedPartitionsSize /
                                allNoEmptyPartitionsSize;
                return buildFrom(partitionColumnStatistic).
                        setMinValue(min).setMaxValue(max).setDistinctValuesCount(max(distinctValues, 1)).build();
            }
        }
        return null;
    }

    @Override
    public Void visitLogicalProject(LogicalProjectOperator node, ExpressionContext context) {
        return computeProjectNode(context, node.getColumnRefMap());
    }

    @Override
    public Void visitPhysicalProject(PhysicalProjectOperator node, ExpressionContext context) {
        Preconditions.checkState(node.getCommonSubOperatorMap().isEmpty());
        return computeProjectNode(context, node.getColumnRefMap());
    }

    private Void computeProjectNode(ExpressionContext context, Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        Preconditions.checkState(context.arity() == 1);

        Statistics.Builder builder = Statistics.builder();
        Statistics inputStatistics = context.getChildStatistics(0);
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

        Statistics.Builder allBuilder = Statistics.builder();
        allBuilder.addColumnStatistics(inputStatistics.getColumnStatistics());

        for (ColumnRefOperator requiredColumnRefOperator : columnRefMap.keySet()) {
            ScalarOperator mapOperator = columnRefMap.get(requiredColumnRefOperator);
            ColumnStatistic outputStatistic = ExpressionStatisticCalculator.calculate(mapOperator, allBuilder.build());
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

        //Update the number of output rows of AggregateNode
        double rowCount = 1;
        if (groupStatisticsMap.values().stream().anyMatch(ColumnStatistic::isUnknown)) {
            // estimate with default column statistics
            for (int groupByIndex = 0; groupByIndex < groupBys.size(); ++groupByIndex) {
                if (groupByIndex == 0) {
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
            for (int groupByIndex = 0; groupByIndex < groupBys.size(); ++groupByIndex) {
                ColumnRefOperator groupByColumn = groupBys.get(groupByIndex);
                ColumnStatistic groupByColumnStatics = inputStatistics.getColumnStatistic(groupByColumn);
                double cardinality = groupByColumnStatics.getDistinctValuesCount() +
                        ((groupByColumnStatics.getNullsFraction() == 0.0) ? 0 : 1);
                if (groupByIndex == 0) {
                    rowCount *= cardinality;
                } else {
                    rowCount *= Math.max(1, cardinality * Math.pow(
                            StatisticsEstimateCoefficient.UNKNOWN_GROUP_BY_CORRELATION_COEFFICIENT, groupByIndex + 1D));
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
        crossBuilder.addColumnStatisticsFromOtherStatistic(leftStatistics, context.getChildOutputColumns(0));
        crossBuilder.addColumnStatisticsFromOtherStatistic(rightStatistics, context.getChildOutputColumns(1));
        double leftRowCount = leftStatistics.getOutputRowCount();
        double rightRowCount = rightStatistics.getOutputRowCount();
        double crossRowCount = StatisticUtils.multiplyRowCount(leftRowCount, rightRowCount);
        crossBuilder.setOutputRowCount(crossRowCount);

        List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(leftStatistics.getUsedColumns(),
                rightStatistics.getUsedColumns(), allJoinPredicate);

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
                innerRowCount = Math.max(1, Math.max(leftRowCount, rightRowCount));
            }
        }

        Statistics innerJoinStats;
        if (innerRowCount == -1) {
            innerJoinStats = estimateInnerJoinStatistics(crossJoinStats, eqOnPredicates);
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
        context.setStatistics(estimateStatistics);
        return visitOperator(context.getOp(), context);
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

        for (int outputIdx = 0; outputIdx < outputColumnRef.size(); ++outputIdx) {
            double estimateRowCount = context.getChildrenStatistics().get(0).getOutputRowCount();
            for (int childIdx = 1; childIdx < context.arity(); ++childIdx) {
                ColumnRefOperator childOutputColumn = childOutputColumns.get(childIdx).get(outputIdx);
                Statistics childStatistics = context.getChildStatistics(childIdx);
                ColumnStatistic estimateColumnStatistic = StatisticsEstimateUtils.unionColumnStatistic(
                        estimateColumnStatistics.get(outputIdx), estimateRowCount,
                        childStatistics.getColumnStatistic(childOutputColumn), childStatistics.getOutputRowCount());
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
        return computeTableFunctionNode(context, node.getOutputColRefs());
    }

    @Override
    public Void visitPhysicalTableFunction(PhysicalTableFunctionOperator node, ExpressionContext context) {
        return computeTableFunctionNode(context, node.getOutputColRefs());
    }

    private Void computeTableFunctionNode(ExpressionContext context, List<ColumnRefOperator> outputColumns) {
        Statistics.Builder builder = Statistics.builder();

        for (ColumnRefOperator col : outputColumns) {
            builder.addColumnStatistic(col, ColumnStatistic.unknown());
        }

        Statistics inputStatistics = context.getChildStatistics(0);
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

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
    //  For example, given AND predicates (t1.a = t2.a AND t1.b = t2.b AND t2.b = t3.a) with the given selectivity(Represented as S for simple):
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
                double sqrtNum = Math.pow(2, index);
                cumulativeSelectivity = cumulativeSelectivity * Math.pow(selectivity, 1 / sqrtNum);
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

        Statistics.Builder builder = Statistics.builder();
        Statistics inputStatistics = context.getChildStatistics(0);
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

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
        return computeAnalyticNode(context, node.getWindowCall());
    }

    @Override
    public Void visitPhysicalAnalytic(PhysicalWindowOperator node, ExpressionContext context) {
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
        Optional<Statistics> produceStatisticsOp = optimizerContext.getCteContext().getCTEStatistics(cteId);

        // The statistics of producer and children are equal theoretically, but statistics of children
        // plan maybe more accurate in actually
        if (!produceStatisticsOp.isPresent() && (context.getChildrenStatistics().isEmpty() ||
                context.getChildrenStatistics().stream().anyMatch(Objects::isNull))) {
            Preconditions.checkState(false, "Impossible cte statistics");
        }

        if (!context.getChildrenStatistics().isEmpty() && context.getChildStatistics(0) != null) {
            //  use the statistics of children first
            context.setStatistics(context.getChildStatistics(0));
            Projection projection = node.getProjection();
            if (projection != null) {
                Statistics.Builder statisticsBuilder = Statistics.buildFrom(context.getStatistics());
                Preconditions.checkState(projection.getCommonSubOperatorMap().isEmpty());
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
        Preconditions.checkState(produceStatisticsOp.isPresent());
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
}
