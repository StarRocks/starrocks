// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.external.iceberg.cost.IcebergTableStatisticCalculator;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.OptExpression;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
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
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
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
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

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

    private Void computeOlapScanNode(Operator node, ExpressionContext context, Table table,
                                     Collection<Long> selectedPartitionIds,
                                     Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Preconditions.checkState(context.arity() == 0);
        // 1. get table row count
        long tableRowCount = getTableRowCount(table, node);
        // 2. get required columns statistics
        Statistics.Builder builder = estimateScanColumns(table, colRefToColumnMetaMap);
        if (tableRowCount <= 1) {
            builder.setTableRowCountMayInaccurate(true);
        }
        // 3. deal with column statistics for partition prune
        OlapTable olapTable = (OlapTable) table;
        ColumnStatistic partitionStatistic = adjustPartitionStatistic(selectedPartitionIds, olapTable);
        if (partitionStatistic != null) {
            String partitionColumnName = Lists.newArrayList(olapTable.getPartitionColumnNames()).get(0);
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
        Statistics stats = IcebergTableStatisticCalculator.getTableStatistics(
                // TODO: pass predicate to get table statistics
                new ArrayList<>(),
                ((IcebergTable) table).getIcebergTable(), colRefToColumnMetaMap);
        context.setStatistics(stats);
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

        // 1. get table row count
        long tableRowCount = getTableRowCount(table, node);
        // 2. get required columns statistics
        Statistics.Builder builder = estimateHMSTableScanColumns(table, tableRowCount, colRefToColumnMetaMap);
        builder.setOutputRowCount(tableRowCount);

        // 3. estimate cardinality
        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    private Statistics.Builder estimateHMSTableScanColumns(Table table, long tableRowCount,
                                                           Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        HiveMetaStoreTable tableWithStats = (HiveMetaStoreTable) table;
        Statistics.Builder builder = Statistics.builder();

        List<ColumnRefOperator> requiredColumns = new ArrayList<>(colRefToColumnMetaMap.keySet());

        List<ColumnStatistic> columnStatisticList = null;
        try {
            if (optimizerContext.getSessionVariable().enableHiveColumnStats()) {
                Map<String, HiveColumnStats> hiveColumnStatisticMap =
                        tableWithStats.getTableLevelColumnStats(requiredColumns.stream().
                                map(ColumnRefOperator::getName).collect(Collectors.toList()));
                List<HiveColumnStats> hiveColumnStatisticList = requiredColumns.stream().map(requireColumn ->
                        computeHiveColumnStatistics(requireColumn, hiveColumnStatisticMap.get(requireColumn.getName())))
                        .collect(Collectors.toList());
                columnStatisticList = hiveColumnStatisticList.stream().map(hiveColumnStats -> {
                    return hiveColumnStats.isUnknown() ? ColumnStatistic.unknown() :
                            new ColumnStatistic(hiveColumnStats.getMinValue(), hiveColumnStats.getMaxValue(),
                                    hiveColumnStats.getNumNulls() * 1.0 / Math.max(tableRowCount, 1),
                                    hiveColumnStats.getAvgSize(), hiveColumnStats.getNumDistinctValues());
                }).collect(Collectors.toList());
            } else {
                LOG.warn("Session variable " + SessionVariable.ENABLE_HIVE_COLUMN_STATS + " is false");
            }
        } catch (Exception e) {
            LOG.warn("Failed to {} get table column. error : {}", table.getName(), e);
        } finally {
            if (columnStatisticList == null || columnStatisticList.isEmpty()) {
                columnStatisticList = Collections.nCopies(requiredColumns.size(), ColumnStatistic.unknown());
            }
        }

        Preconditions.checkState(requiredColumns.size() == columnStatisticList.size());
        for (int i = 0; i < requiredColumns.size(); ++i) {
            builder.addColumnStatistic(requiredColumns.get(i), columnStatisticList.get(i));
            optimizerContext.getDumpInfo()
                    .addTableStatistics(table, requiredColumns.get(i).getName(), columnStatisticList.get(i));
        }

        return builder;
    }

    // Hive column statistics may be -1 in avgSize, numNulls and distinct values, default values need to be reassigned
    private HiveColumnStats computeHiveColumnStatistics(ColumnRefOperator column, HiveColumnStats hiveColumnStats) {
        double avgSize =
                hiveColumnStats.getAvgSize() != -1 ? hiveColumnStats.getAvgSize() : column.getType().getTypeSize();
        long numNulls = hiveColumnStats.getNumNulls() != -1 ? hiveColumnStats.getNumNulls() : 0;
        long distinctValues = hiveColumnStats.getNumDistinctValues() != -1 ? hiveColumnStats.getNumDistinctValues() : 1;

        hiveColumnStats.setAvgSize(avgSize);
        hiveColumnStats.setNumNulls(numNulls);
        hiveColumnStats.setNumDistinctValues(distinctValues);
        return hiveColumnStats;
    }

    private Statistics.Builder estimateScanColumns(Table table, Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        Statistics.Builder builder = Statistics.builder();
        List<ColumnRefOperator> requiredColumns = new ArrayList<>(colRefToColumnMetaMap.keySet());
        List<ColumnStatistic> columnStatisticList = Catalog.getCurrentStatisticStorage().getColumnStatistics(table,
                requiredColumns.stream().map(ColumnRefOperator::getName).collect(Collectors.toList()));
        Preconditions.checkState(requiredColumns.size() == columnStatisticList.size());
        for (int i = 0; i < requiredColumns.size(); ++i) {
            builder.addColumnStatistic(requiredColumns.get(i), columnStatisticList.get(i));
            optimizerContext.getDumpInfo()
                    .addTableStatistics(table, requiredColumns.get(i).getName(), columnStatisticList.get(i));
        }

        return builder;
    }

    private Void computeNormalExternalTableScanNode(Operator node, ExpressionContext context, Table table,
                                                    Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                    int outputRowCount) {
        Statistics.Builder builder = estimateScanColumns(table, colRefToColumnMetaMap);
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
        Statistics.Builder builder = estimateScanColumns(table, node.getColRefToColumnMetaMap());
        builder.setOutputRowCount(1);

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, ExpressionContext context) {
        Table table = node.getTable();
        Statistics.Builder builder = estimateScanColumns(table, node.getColRefToColumnMetaMap());
        builder.setOutputRowCount(1);

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalMetaScan(LogicalMetaScanOperator node, ExpressionContext context) {
        Statistics.Builder builder = estimateScanColumns(node.getTable(), node.getColRefToColumnMetaMap());
        builder.setOutputRowCount(node.getAggColumnIdToNames().size());

        context.setStatistics(builder.build());
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalMetaScan(PhysicalMetaScanOperator node, ExpressionContext context) {
        Statistics.Builder builder = estimateScanColumns(node.getTable(), node.getColRefToColumnMetaMap());
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
        int allPartitionsSize = olapTable.getPartitions().size();
        if (selectedPartitionsSize != allPartitionsSize) {
            if (olapTable.getPartitionColumnNames().size() != 1) {
                return null;
            }
            String partitionColumn = Lists.newArrayList(olapTable.getPartitionColumnNames()).get(0);
            ColumnStatistic partitionColumnStatistic =
                    Catalog.getCurrentStatisticStorage().getColumnStatistic(olapTable, partitionColumn);
            optimizerContext.getDumpInfo().addTableStatistics(olapTable, partitionColumn, partitionColumnStatistic);

            PartitionInfo partitionInfo = olapTable.getPartitionInfo();
            if (partitionInfo instanceof RangePartitionInfo) {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                List<Map.Entry<Long, Range<PartitionKey>>> rangeList;
                try {
                    rangeList = rangePartitionInfo.getSortedRangeMap(new HashSet<>(selectedPartitionId));
                } catch (AnalysisException e) {
                    Log.warn("get sorted range partition failed, msg : " + e.getMessage());
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
                    DateLiteral maxDateLiteral = (DateLiteral) maxLiteral;
                    min = Utils.getLongFromDateTime(minDateLiteral.toLocalDateTime());
                    max = Utils.getLongFromDateTime(maxDateLiteral.toLocalDateTime());
                } else {
                    min = firstKey.getValue().lowerEndpoint().getKeys().get(0).getDoubleValue();
                    max = lastKey.getValue().upperEndpoint().getKeys().get(0).getDoubleValue();
                }
                double distinctValues =
                        partitionColumnStatistic.getDistinctValuesCount() * 1.0 * selectedPartitionsSize /
                                allPartitionsSize;
                return buildFrom(partitionColumnStatistic).
                        setMinValue(min).setMaxValue(max).setDistinctValuesCount(max(distinctValues, 1)).build();
            }
        }
        return null;
    }

    private long getTableRowCount(Table table, Operator node) {
        if (Table.TableType.OLAP == table.getType()) {
            OlapTable olapTable = (OlapTable) table;
            List<Partition> selectedPartitions;
            if (node.isLogical()) {
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) node;
                selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                        olapTable::getPartition).collect(Collectors.toList());
            } else {
                PhysicalOlapScanOperator olapScanOperator = (PhysicalOlapScanOperator) node;
                selectedPartitions = olapScanOperator.getSelectedPartitionId().stream().map(
                        olapTable::getPartition).collect(Collectors.toList());
            }
            long rowCount = 0;
            for (Partition partition : selectedPartitions) {
                rowCount += partition.getBaseIndex().getRowCount();
                optimizerContext.getDumpInfo()
                        .addPartitionRowCount(table, partition.getName(), partition.getBaseIndex().getRowCount());
            }
            // Currently, after FE just start, the row count of table is always 0.
            // Explicitly set table row count to 1 to make our cost estimate work.
            return Math.max(rowCount, 1);
        } else if (Table.TableType.HIVE == table.getType() || Table.TableType.HUDI == table.getType()) {
            try {
                ScanOperatorPredicates predicates = null;
                if (node.isLogical()) {
                    predicates = ((LogicalScanOperator) node).getScanOperatorPredicates();
                } else {
                    predicates = ((PhysicalScanOperator) node).getScanOperatorPredicates();
                }
                return Math.max(computeHMSTableTableRowCount(table,
                        predicates.getSelectedPartitionIds(),
                        predicates.getIdToPartitionKey()), 1);
            } catch (DdlException | AnalysisException e) {
                LOG.warn("compute hive table row count failed : " + e);
                throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
            }
        }
        return 1;
    }

    /**
     * 1. compute based on table stats and partition file total bytes to be scanned
     * 2. get from partition row num stats if table stats is missing
     * 3. use totalBytes / schema size to compute if partition stats is missing
     */
    private long computeHMSTableTableRowCount(Table table, Collection<Long> selectedPartitionIds,
                                              Map<Long, PartitionKey> idToPartitionKey) throws DdlException {
        HiveMetaStoreTable tableWithStats = (HiveMetaStoreTable) table;

        long numRows = -1;
        HiveTableStats tableStats = null;
        // 1. get row count from table stats
        try {
            tableStats = tableWithStats.getTableStats();
        } catch (DdlException e) {
            LOG.warn("Table {} gets stats failed", table.getName(), e);
            throw e;
        }
        if (tableStats != null) {
            numRows = tableStats.getNumRows();
        }
        if (numRows >= 0) {
            return numRows;
        }
        // 2. get row count from partition stats
        List<PartitionKey> partitions = Lists.newArrayList();
        for (long partitionId : selectedPartitionIds) {
            partitions.add(idToPartitionKey.get(partitionId));
        }
        numRows = tableWithStats.getPartitionStatsRowCount(partitions);
        LOG.debug("Get cardinality from partition stats: {}", numRows);
        if (numRows >= 0) {
            return numRows;
        }
        // 3. estimated row count for the given number of file bytes
        long totalBytes = 0;
        if (selectedPartitionIds.isEmpty()) {
            return 0;
        }

        List<HivePartition> hivePartitions = tableWithStats.getPartitions(partitions);
        for (HivePartition hivePartition : hivePartitions) {
            for (HdfsFileDesc fileDesc : hivePartition.getFiles()) {
                totalBytes += fileDesc.getLength();
            }
        }
        numRows = totalBytes /
                table.getBaseSchema().stream().mapToInt(column -> column.getType().getTypeSize()).sum();
        return numRows;
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

    private Void computeJoinNode(ExpressionContext context, JoinOperator joinType, ScalarOperator joinOnPredicate) {
        Preconditions.checkState(context.arity() == 2);

        Statistics leftStatistics = context.getChildStatistics(0);
        Statistics rightStatistics = context.getChildStatistics(1);
        // construct cross join statistics
        Statistics.Builder crossBuilder = Statistics.builder();
        crossBuilder.addColumnStatistics(leftStatistics.getOutputColumnsStatistics(context.getChildOutputColumns(0)));
        crossBuilder.addColumnStatistics(rightStatistics.getOutputColumnsStatistics(context.getChildOutputColumns(1)));
        double leftRowCount = leftStatistics.getOutputRowCount();
        double rightRowCount = rightStatistics.getOutputRowCount();
        double crossRowCount = leftRowCount * rightRowCount;
        crossBuilder.setOutputRowCount(crossRowCount);

        List<BinaryPredicateOperator> eqOnPredicates = JoinPredicateUtils.getEqConj(leftStatistics.getUsedColumns(),
                rightStatistics.getUsedColumns(),
                Utils.extractConjuncts(joinOnPredicate));

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
            case RIGHT_SEMI_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                break;
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(0, leftRowCount - innerRowCount));
                break;
            case RIGHT_OUTER_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(innerRowCount, rightRowCount));
                computeNullFractionForOuterJoin(rightRowCount, innerRowCount, leftStatistics, joinStatsBuilder);
                break;
            case RIGHT_ANTI_JOIN:
                joinStatsBuilder = Statistics.buildFrom(innerJoinStats);
                joinStatsBuilder.setOutputRowCount(max(0, rightRowCount - innerRowCount));
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

        List<ScalarOperator> notEqJoin = Utils.extractConjuncts(joinOnPredicate);
        notEqJoin.removeAll(eqOnPredicates);

        Statistics estimateStatistics = estimateStatistics(notEqJoin, joinStats);
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
        return computeIntersectNode(node, context, node.getOutputColumnRefOp());
    }

    @Override
    public Void visitPhysicalIntersect(PhysicalIntersectOperator node, ExpressionContext context) {
        return computeIntersectNode(node, context, node.getOutputColumnRefOp());
    }

    private Void computeIntersectNode(Operator node, ExpressionContext context,
                                      List<ColumnRefOperator> outputColumnRef) {
        Statistics.Builder builder = Statistics.builder();

        double outputRowCount = context.getChildStatistics(0).getOutputRowCount();
        for (int childIdx = 0; childIdx < context.arity(); ++childIdx) {
            Statistics inputStatistics = context.getChildStatistics(childIdx);
            if (inputStatistics.getOutputRowCount() < outputRowCount) {
                outputRowCount = inputStatistics.getOutputRowCount();
            }
        }
        builder.setOutputRowCount(outputRowCount);

        for (ColumnRefOperator columnRefOperator : outputColumnRef) {
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
        }

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
        ColumnRefSet columnRefSet = new ColumnRefSet();
        columnRefSet.union(node.getFnResultColumnRefSet());
        columnRefSet.union(node.getOuterColumnRefSet());
        return computeTableFunctionNode(context, columnRefSet);
    }

    @Override
    public Void visitPhysicalTableFunction(PhysicalTableFunctionOperator node, ExpressionContext context) {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        columnRefSet.union(node.getFnResultColumnRefSet());
        columnRefSet.union(node.getOuterColumnRefSet());
        return computeTableFunctionNode(context, columnRefSet);
    }

    private Void computeTableFunctionNode(ExpressionContext context, ColumnRefSet outputColumns) {
        Statistics.Builder builder = Statistics.builder();

        for (int columnId : outputColumns.getColumnIds()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.getColumnRef(columnId);
            builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
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
        OptExpression produce = optimizerContext.getCteContext().getCTEProduce(cteId);
        Statistics produceStatistics = produce.getGroupExpression().getGroup().getStatistics();
        if (null == produceStatistics) {
            produceStatistics = produce.getStatistics();
        }

        Preconditions.checkNotNull(produce.getStatistics());

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
        context.setStatistics(context.getChildStatistics(0));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, ExpressionContext context) {
        context.setStatistics(context.getChildStatistics(0));
        return visitOperator(node, context);
    }

    @Override
    public Void visitPhysicalNoCTE(PhysicalNoCTEOperator node, ExpressionContext context) {
        context.setStatistics(context.getChildStatistics(0));
        return visitOperator(node, context);
    }
}
