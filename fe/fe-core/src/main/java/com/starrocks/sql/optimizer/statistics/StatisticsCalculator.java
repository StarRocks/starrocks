// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HdfsFileDesc;
import com.starrocks.external.hive.HiveColumnStats;
import com.starrocks.external.hive.HivePartition;
import com.starrocks.external.hive.HiveTableStats;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.transformation.JoinPredicateUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mortbay.log.Log;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * Estimate stats for the root group using a group expression's children's stats
 * The estimated column stats will store in {@link Group}
 */
public class StatisticsCalculator extends OperatorVisitor<Void, ExpressionContext> {
    private static final Logger LOG = LogManager.getLogger(StatisticsCalculator.class);

    private final ExpressionContext expressionContext;
    private final ColumnRefSet requiredCols;
    private final ColumnRefFactory columnRefFactory;
    private final DumpInfo dumpInfo;

    public StatisticsCalculator(ExpressionContext expressionContext,
                                ColumnRefSet requiredCols,
                                ColumnRefFactory columnRefFactory,
                                DumpInfo dumpInfo) {
        this.expressionContext = expressionContext;
        this.requiredCols = requiredCols;
        this.columnRefFactory = columnRefFactory;
        this.dumpInfo = dumpInfo;
    }

    public void estimatorStats() {
        expressionContext.getOp().accept(this, expressionContext);
    }

    @Override
    public Void visitOperator(Operator node, ExpressionContext context) {
        return null;
    }

    public Void visitOperator(Operator node, ExpressionContext context, Statistics.Builder builder) {
        ScalarOperator predicate = null;
        long limit = -1;

        if (node instanceof LogicalOperator) {
            LogicalOperator logical = (LogicalOperator) node;
            predicate = logical.getPredicate();
            limit = logical.getLimit();
        } else if (node instanceof PhysicalOperator) {
            PhysicalOperator physical = (PhysicalOperator) node;
            predicate = physical.getPredicate();
            limit = physical.getLimit();
        }

        Statistics statistics = builder.build();
        if (null != predicate) {
            statistics = estimateStatistics(ImmutableList.of(predicate), statistics);
        }

        if (limit != -1 && limit < statistics.getOutputRowCount()) {
            statistics = new Statistics(limit, statistics.getColumnStatistics());
        }

        context.setStatistics(statistics);
        return visitOperator(node, context);
    }

    @Override
    public Void visitLogicalOlapScan(LogicalOlapScanOperator node, ExpressionContext context) {
        return computeOlapScanNode(node, context, node.getOlapTable(), node.getSelectedPartitionId(),
                node.getColumnToIds());
    }

    @Override
    public Void visitPhysicalOlapScan(PhysicalOlapScanOperator node, ExpressionContext context) {
        return computeOlapScanNode(node, context, node.getTable(), node.getSelectedPartitionId(),
                node.getColumnToIds());
    }

    private Void computeOlapScanNode(Operator node, ExpressionContext context, Table table,
                                     Collection<Long> selectedPartitionIds,
                                     Map<Column, Integer> columnToIds) {
        Preconditions.checkState(context.arity() == 0);
        // 1. get table row count
        long tableRowCount = getTableRowCount(table, node);
        // 2. get required columns statistics
        Statistics.Builder builder = estimateScanColumns(table);
        // 3. deal with column statistics for partition prune
        OlapTable olapTable = (OlapTable) table;
        ColumnStatistic partitionStatistic = adjustPartitionStatistic(selectedPartitionIds, olapTable);
        if (partitionStatistic != null) {
            String partitionColumnName = Lists.newArrayList(olapTable.getPartitionColumnNames()).get(0);
            Optional<Map.Entry<Column, Integer>> partitionColumnEntry = columnToIds.entrySet().stream().
                    filter(column -> column.getKey().getName().equalsIgnoreCase(partitionColumnName)).findAny();
            Preconditions.checkState(partitionColumnEntry.isPresent());
            builder.addColumnStatistic(columnRefFactory.getColumnRef(partitionColumnEntry.get().getValue()),
                    partitionStatistic);
        }

        builder.setOutputRowCount(tableRowCount);
        // 4. estimate cardinality
        return visitOperator(node, context, builder);
    }

    @Override
    public Void visitLogicalHiveScan(LogicalHiveScanOperator node, ExpressionContext context) {
        return computeHiveScanNode(node, context, node.getTable());
    }

    @Override
    public Void visitPhysicalHiveScan(PhysicalHiveScanOperator node, ExpressionContext context) {
        return computeHiveScanNode(node, context, node.getTable());
    }

    public Void computeHiveScanNode(Operator node, ExpressionContext context, Table table) {
        Preconditions.checkState(context.arity() == 0);

        // 1. get table row count
        long tableRowCount = getTableRowCount(table, node);
        // 2. get required columns statistics
        Statistics.Builder builder = estimateHiveScanColumns((HiveTable) table, tableRowCount);

        builder.setOutputRowCount(tableRowCount);
        // 3. estimate cardinality
        return visitOperator(node, context, builder);
    }

    private Statistics.Builder estimateHiveScanColumns(HiveTable table, long tableRowCount) {
        Statistics.Builder builder = Statistics.builder();
        List<ColumnRefOperator> requiredColumns = new ArrayList<>();
        for (int columnId : requiredCols.getColumnIds()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.getColumnRef(columnId);
            requiredColumns.add(columnRefOperator);
        }
        List<ColumnStatistic> columnStatisticList;
        try {
            Map<String, HiveColumnStats> hiveColumnStatisticMap =
                    table.getTableLevelColumnStats(requiredColumns.stream().
                            map(ColumnRefOperator::getName).collect(Collectors.toList()));
            List<HiveColumnStats> hiveColumnStatisticList = requiredColumns.stream().map(requireColumn ->
                    computeHiveColumnStatistics(requireColumn, hiveColumnStatisticMap.get(requireColumn.getName())))
                    .collect(Collectors.toList());
            columnStatisticList = hiveColumnStatisticList.stream().map(hiveColumnStats ->
                    new ColumnStatistic(hiveColumnStats.getMinValue(), hiveColumnStats.getMaxValue(),
                            hiveColumnStats.getNumNulls() * 1.0 / Math.max(tableRowCount, 1),
                            hiveColumnStats.getAvgSize(), hiveColumnStats.getNumDistinctValues()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("hive table {} get column failed. error : {}", table.getName(), e);
            columnStatisticList = Collections.nCopies(requiredColumns.size(), ColumnStatistic.unknown());
        }
        Preconditions.checkState(requiredColumns.size() == columnStatisticList.size());
        for (int i = 0; i < requiredColumns.size(); ++i) {
            builder.addColumnStatistic(requiredColumns.get(i), columnStatisticList.get(i));
            dumpInfo.addTableStatistics(table, requiredColumns.get(i).getName(), columnStatisticList.get(i));
        }

        return builder;
    }

    // Hive column statistics may be -1 in avgSize, numNulls and distinct values, default values need to be reassigned
    private HiveColumnStats computeHiveColumnStatistics(ColumnRefOperator column, HiveColumnStats hiveColumnStats) {
        double avgSize =
                hiveColumnStats.getAvgSize() != -1 ? hiveColumnStats.getAvgSize() : column.getType().getSlotSize();
        long numNulls = hiveColumnStats.getNumNulls() != -1 ? hiveColumnStats.getNumNulls() : 0;
        long distinctValues = hiveColumnStats.getNumDistinctValues() != -1 ? hiveColumnStats.getNumDistinctValues() : 1;

        hiveColumnStats.setAvgSize(avgSize);
        hiveColumnStats.setNumNulls(numNulls);
        hiveColumnStats.setNumDistinctValues(distinctValues);
        return hiveColumnStats;
    }

    private Statistics.Builder estimateScanColumns(Table table) {
        Statistics.Builder builder = Statistics.builder();
        List<ColumnRefOperator> requiredColumns = new ArrayList<>();
        for (int columnId : requiredCols.getColumnIds()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.getColumnRef(columnId);
            requiredColumns.add(columnRefOperator);
        }
        List<ColumnStatistic> columnStatisticList = Catalog.getCurrentStatisticStorage().getColumnStatistics(table,
                requiredColumns.stream().map(ColumnRefOperator::getName).collect(Collectors.toList()));
        Preconditions.checkState(requiredColumns.size() == columnStatisticList.size());
        for (int i = 0; i < requiredColumns.size(); ++i) {
            builder.addColumnStatistic(requiredColumns.get(i), columnStatisticList.get(i));
            dumpInfo.addTableStatistics(table, requiredColumns.get(i).getName(), columnStatisticList.get(i));
        }

        return builder;
    }

    @Override
    public Void visitLogicalMysqlScan(LogicalMysqlScanOperator node, ExpressionContext context) {
        return computeMysqlScanNode(node, context, node.getTable());
    }

    @Override
    public Void visitPhysicalMysqlScan(PhysicalMysqlScanOperator node, ExpressionContext context) {
        return computeMysqlScanNode(node, context, node.getTable());
    }

    private Void computeMysqlScanNode(Operator node, ExpressionContext context, Table table) {
        Statistics.Builder builder = estimateScanColumns(table);
        builder.setOutputRowCount(1);
        return visitOperator(node, context, builder);
    }

    @Override
    public Void visitLogicalEsScan(LogicalEsScanOperator node, ExpressionContext context) {
        return computeEsScanNode(node, context, node.getTable());
    }

    @Override
    public Void visitPhysicalEsScan(PhysicalEsScanOperator node, ExpressionContext context) {
        return computeEsScanNode(node, context, node.getTable());
    }

    private Void computeEsScanNode(Operator node, ExpressionContext context, Table table) {
        Statistics.Builder builder = estimateScanColumns(table);
        builder.setOutputRowCount(1);
        return visitOperator(node, context, builder);
    }

    @Override
    public Void visitLogicalSchemaScan(LogicalSchemaScanOperator node, ExpressionContext context) {
        Table table = node.getTable();
        Statistics.Builder builder = estimateScanColumns(table);
        builder.setOutputRowCount(1);
        return visitOperator(node, context, builder);
    }

    @Override
    public Void visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, ExpressionContext context) {
        Table table = node.getTable();
        Statistics.Builder builder = estimateScanColumns(table);
        builder.setOutputRowCount(1);
        return visitOperator(node, context, builder);
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
            dumpInfo.addTableStatistics(olapTable, partitionColumn, partitionColumnStatistic);

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
                return ColumnStatistic.buildFrom(partitionColumnStatistic).
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
                dumpInfo.addPartitionRowCount(table, partition.getName(), partition.getBaseIndex().getRowCount());
            }
            // Currently, after FE just start, the row count of table is always 0.
            // Explicitly set table row count to 1 to make our cost estimate work.
            return Math.max(rowCount, 1);
        } else if (Table.TableType.HIVE == table.getType()) {
            try {
                if (node.isLogical()) {
                    LogicalHiveScanOperator scanOperator = (LogicalHiveScanOperator) node;
                    return Math.max(computeHiveTableRowCount((HiveTable) table, scanOperator.getSelectedPartitionIds(),
                            scanOperator.getIdToPartitionKey()), 1);
                } else {
                    PhysicalHiveScanOperator scanOperator = (PhysicalHiveScanOperator) node;
                    return Math.max(computeHiveTableRowCount((HiveTable) table, scanOperator.getSelectedPartitionIds(),
                            scanOperator.getIdToPartitionKey()), 1);
                }
            } catch (DdlException e) {
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
    private long computeHiveTableRowCount(HiveTable hiveTable, Collection<Long> selectedPartitionIds,
                                          Map<Long, PartitionKey> idToPartitionKey) throws DdlException {
        long numRows = -1;
        HiveTableStats tableStats = null;
        // 1. get row count from table stats
        try {
            tableStats = hiveTable.getTableStats();
        } catch (DdlException e) {
            LOG.warn("table {} gets stats failed", hiveTable.getName(), e);
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
        numRows = hiveTable.getPartitionStatsRowCount(partitions);
        LOG.debug("get cardinality from partition stats: {}", numRows);
        if (numRows >= 0) {
            return numRows;
        }
        // 3. estimated row count for the given number of file bytes
        long totalBytes = 0;
        if (selectedPartitionIds.isEmpty()) {
            return 0;
        }
        for (long partitionId : selectedPartitionIds) {
            PartitionKey partitionKey = idToPartitionKey.get(partitionId);
            HivePartition partition = hiveTable.getPartition(partitionKey);
            for (HdfsFileDesc fileDesc : partition.getFiles()) {
                totalBytes += fileDesc.getLength();
            }
        }
        numRows = totalBytes /
                hiveTable.getBaseSchema().stream().mapToInt(column -> column.getType().getSlotSize()).sum();
        return numRows;
    }

    @Override
    public Void visitLogicalProject(LogicalProjectOperator node, ExpressionContext context) {
        return computeProjectNode(context, node.getColumnRefMap(), node.getCommonSubOperatorMap());
    }

    @Override
    public Void visitPhysicalProject(PhysicalProjectOperator node, ExpressionContext context) {
        return computeProjectNode(context, node.getColumnRefMap(), node.getCommonSubOperatorMap());
    }

    private Void computeProjectNode(ExpressionContext context, Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                                    Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        Preconditions.checkState(context.arity() == 1);

        Statistics.Builder builder = Statistics.builder();
        Statistics inputStatistics = context.getChildStatistics(0);
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

        // if required columns is nothing, such as select count(*) from lineitems, project node use child statistics
        if (requiredCols.cardinality() == 0) {
            for (Map.Entry<ColumnRefOperator, ColumnStatistic> entry : inputStatistics.getColumnStatistics()
                    .entrySet()) {
                builder.addColumnStatistic(entry.getKey(), entry.getValue());
            }
            context.setStatistics(builder.build());
            return visitOperator(context.getOp(), context);
        }

        for (int columnId : requiredCols.getColumnIds()) {
            ColumnRefOperator requiredColumnRefOperator = columnRefFactory.getColumnRef(columnId);
            // derive stats from child
            // use clone here because it will be rewrite later
            ScalarOperator mapOperator = columnRefMap.get(requiredColumnRefOperator).clone();

            ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(commonSubOperatorMap, true);
            mapOperator = mapOperator.accept(rewriter, null);
            builder.addColumnStatistic(requiredColumnRefOperator,
                    ExpressionStatisticCalculator.calculate(mapOperator, inputStatistics));
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

        Map<ColumnRefOperator, ColumnStatistic> groupStatisticsMap = groupBys.stream().collect(
                Collectors.toMap(Function.identity(), inputStatistics::getColumnStatistic));
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
                    }
                }
            }
        } else {
            for (int groupByIndex = 0; groupByIndex < groupBys.size(); ++groupByIndex) {
                ColumnRefOperator groupByColumnRef = groupBys.get(groupByIndex);
                ColumnStatistic columnStatistic = inputStatistics.getColumnStatistic(groupByColumnRef);
                if (groupByIndex == 0) {
                    rowCount *= columnStatistic.getDistinctValuesCount();
                } else {
                    rowCount *= columnStatistic.getDistinctValuesCount() *
                            Math.pow(StatisticsEstimateCoefficient.UNKNOWN_GROUP_BY_CORRELATION_COEFFICIENT,
                                    groupByIndex + 1);
                    if (rowCount > inputStatistics.getOutputRowCount()) {
                        rowCount = inputStatistics.getOutputRowCount();
                    }
                }
            }
        }
        builder.addColumnStatistics(groupStatisticsMap);
        rowCount = min(inputStatistics.getOutputRowCount(), rowCount);
        builder.setOutputRowCount(rowCount);

        aggregations.forEach((key, value) -> builder
                .addColumnStatistic(key, ExpressionStatisticCalculator.calculate(value, inputStatistics)));

        return visitOperator(node, context, builder);
    }

    @Override
    public Void visitLogicalJoin(LogicalJoinOperator node, ExpressionContext context) {
        return computeJoinNode(context, node.getOnPredicate(), node.getPredicate(), node.getJoinType(), node.getLimit(),
                node.getPruneOutputColumns());
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoinOperator node, ExpressionContext context) {
        return computeJoinNode(context, node.getJoinPredicate(), node.getPredicate(), node.getJoinType(),
                node.getLimit(),
                node.getPruneOutputColumns());
    }

    private Void computeJoinNode(ExpressionContext context, ScalarOperator joinOnPredicate, ScalarOperator predicate,
                                 JoinOperator joinType, long limit, List<ColumnRefOperator> outputColumns) {
        Preconditions.checkState(context.arity() == 2);

        Statistics.Builder builder = Statistics.builder();
        Statistics leftStatistics = context.getChildStatistics(0);
        Statistics rightStatistics = context.getChildStatistics(1);
        double leftRowCount = leftStatistics.getOutputRowCount();
        double rightRowCount = rightStatistics.getOutputRowCount();

        builder.addColumnStatistics(leftStatistics.getColumnStatistics());
        builder.addColumnStatistics(rightStatistics.getColumnStatistics());
        List<BinaryPredicateOperator> eqOnPredicates = JoinPredicateUtils.getEqConj(leftStatistics.getUsedColumns(),
                rightStatistics.getUsedColumns(),
                Utils.extractConjuncts(joinOnPredicate));

        double crossRowCount = leftRowCount * rightRowCount;
        builder.setOutputRowCount(crossRowCount);
        // TODO(ywb): now join node statistics only care obout the row count, but the column statistics will also change
        //  after join operation
        Statistics statistics = builder.build();
        double innerRowCount = -1;
        // For no column Statistics
        for (BinaryPredicateOperator binaryPredicateOperator : eqOnPredicates) {
            ScalarOperator leftOperator = binaryPredicateOperator.getChild(0);
            ScalarOperator rightOperator = binaryPredicateOperator.getChild(1);
            if (leftOperator.isColumnRef() && rightOperator.isColumnRef()) {
                ColumnStatistic leftColumn = statistics.getColumnStatistic((ColumnRefOperator) leftOperator);
                ColumnStatistic rightColumn = statistics.getColumnStatistic((ColumnRefOperator) rightOperator);
                if (leftColumn.isUnknown() || rightColumn.isUnknown()) {
                    // To avoid anti-join estimation of 0 rows, the rows of inner join
                    // need to take a table with a small number of rows
                    if (joinType.isAntiJoin()) {
                        innerRowCount = Math.max(0, Math.min(leftRowCount, rightRowCount));
                    } else {
                        innerRowCount = Math.max(1, Math.max(leftRowCount, rightRowCount));
                    }
                    break;
                }
            }
        }
        if (innerRowCount == -1) {
            innerRowCount = estimateInnerRowCount(builder.build(), eqOnPredicates);
        }

        switch (joinType) {
            case CROSS_JOIN:
                builder.setOutputRowCount(crossRowCount);
                break;
            case INNER_JOIN:
                if (eqOnPredicates.isEmpty()) {
                    builder.setOutputRowCount(crossRowCount);
                    break;
                }
                builder.setOutputRowCount(innerRowCount);
                break;
            case LEFT_OUTER_JOIN:
                builder.setOutputRowCount(max(innerRowCount, leftRowCount));
                break;
            case LEFT_SEMI_JOIN:
            case RIGHT_SEMI_JOIN:
                builder.setOutputRowCount(innerRowCount);
                break;
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                builder.setOutputRowCount(max(0, leftRowCount - innerRowCount));
                break;
            case RIGHT_OUTER_JOIN:
                builder.setOutputRowCount(max(innerRowCount, rightRowCount));
                break;
            case RIGHT_ANTI_JOIN:
                builder.setOutputRowCount(max(0, rightRowCount - innerRowCount));
                break;
            case FULL_OUTER_JOIN:
                builder.setOutputRowCount(leftRowCount + rightRowCount - innerRowCount);
                break;
            case MERGE_JOIN:
                // TODO
                break;
        }
        List<ScalarOperator> notEqJoin = Utils.extractConjuncts(joinOnPredicate);
        notEqJoin.removeAll(eqOnPredicates);
        notEqJoin.add(predicate);

        Statistics estimateStatistics;
        estimateStatistics = estimateStatistics(notEqJoin, builder.build());

        if (limit != -1 && limit < estimateStatistics.getOutputRowCount()) {
            estimateStatistics = new Statistics(limit, estimateStatistics.getColumnStatistics());
        }

        if (outputColumns == null) {
            context.setStatistics(estimateStatistics);
            return visitOperator(context.getOp(), context);
        }
        if (outputColumns.isEmpty()) {
            if (!context.getRootProperty().getOutputColumns().isEmpty()) {
                outputColumns.add(Utils.findSmallestColumnRef(context.getRootProperty().getOutputColumns().getStream().
                        mapToObj(columnRefFactory::getColumnRef).collect(Collectors.toList())));
            }
        }
        // use outputColumns to prune column statistics
        Map<ColumnRefOperator, ColumnStatistic> outputColumnStatisticMap = estimateStatistics.getColumnStatistics().
                entrySet().stream().filter(entry -> outputColumns.contains(entry.getKey())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Statistics.Builder joinBuilder = Statistics.builder();
        joinBuilder.setOutputRowCount(estimateStatistics.getOutputRowCount());
        joinBuilder.addColumnStatistics(outputColumnStatisticMap);

        context.setStatistics(joinBuilder.build());
        return visitOperator(context.getOp(), context);
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
            return visitOperator(node, context, builder);
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

        return visitOperator(node, context, builder);
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

        return visitOperator(node, context, builder);
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

        return visitOperator(node, context, builder);
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
        return computeRepeatNode(context, node.getOutputGrouping());
    }

    @Override
    public Void visitPhysicalRepeat(PhysicalRepeatOperator node, ExpressionContext context) {
        return computeRepeatNode(context, node.getOutputGrouping());
    }

    private Void computeRepeatNode(ExpressionContext context, List<ColumnRefOperator> outputGrouping) {
        Preconditions.checkState(context.arity() == 1);

        Statistics.Builder builder = Statistics.builder();
        for (ColumnRefOperator columnRef : outputGrouping) {
            builder.addColumnStatistic(columnRef, ColumnStatistic.unknown());
        }

        Statistics inputStatistics = context.getChildStatistics(0);
        builder.addColumnStatistics(inputStatistics.getColumnStatistics());
        builder.setOutputRowCount(inputStatistics.getOutputRowCount());

        context.setStatistics(builder.build());
        return visitOperator(context.getOp(), context);
    }

    @Override
    public Void visitLogicalTableFunction(LogicalTableFunctionOperator node, ExpressionContext context) {
        return computeTableFunctionNode(context, node.getOutputColumns(null));
    }

    @Override
    public Void visitPhysicalTableFunction(PhysicalTableFunctionOperator node, ExpressionContext context) {
        return computeTableFunctionNode(context, node.getOutputColumns());
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

    public double estimateInnerRowCount(Statistics statistics, List<BinaryPredicateOperator> eqOnPredicates) {
        if (eqOnPredicates.isEmpty()) {
            return statistics.getOutputRowCount();
        }
        // Join equality clauses are usually correlated. Therefore we shouldn't treat each join equality
        // clause separately because stats estimates would be way off. Instead we choose so called
        // "driving predicate" which mostly reduces join output rows cardinality and apply UNKNOWN_FILTER_COEFFICIENT
        // for other (auxiliary) predicates.
        Queue<BinaryPredicateOperator> remainingEqOnPredicates = new LinkedList<>(eqOnPredicates);
        BinaryPredicateOperator drivingPredicate = remainingEqOnPredicates.poll();
        double result = statistics.getOutputRowCount();
        for (int i = 0; i < eqOnPredicates.size(); ++i) {
            Statistics estimateStatistics =
                    estimateByEqOnPredicates(statistics, drivingPredicate, remainingEqOnPredicates);
            if (estimateStatistics.getOutputRowCount() < result) {
                result = estimateStatistics.getOutputRowCount();
            }
            remainingEqOnPredicates.add(drivingPredicate);
            drivingPredicate = remainingEqOnPredicates.poll();
        }
        return result;
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
        return visitOperator(node, context, builder);
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
        return visitOperator(node, context, builder);
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
        for (ScalarOperator predicate : predicateList) {
            statistics = PredicateStatisticsCalculator.statisticsCalculate(predicate, statistics);
        }
        return statistics;
    }
}
