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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.statistics.ConnectorTableColumnStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.MockDumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalKuduScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class StatisticsCalculatorTest {
    private static ConnectContext connectContext;
    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory;
    private static StarRocksAssert starRocksAssert;
    @TempDir
    public static File temp;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = OptimizerFactory.mockContext(connectContext, columnRefFactory);

        starRocksAssert = new StarRocksAssert(connectContext);
        ConnectorPlanTestBase.mockAllCatalogs(connectContext, newFolder(temp, "junit").toURI().toString());

        String dbName = "statistics_test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);
        FeConstants.runningUnitTest = false;
    }

    @BeforeEach
    public void before() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_all_type` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "PARTITION BY RANGE (id_date)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"), \n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")  \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type_day_partition` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "PARTITION BY RANGE (id_date)\n" +
                "(\n" +
                "partition p1 values [('2020-04-23'), ('2020-04-24')),\n" +
                "partition p2 values [('2020-04-24'), ('2020-04-25')),\n" +
                "partition p3 values [('2020-04-25'), ('2020-04-26')) \n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @AfterEach
    public void after() throws Exception {
        starRocksAssert.dropTable("test_all_type");
        starRocksAssert.dropTable("test_all_type_day_partition");
    }

    @Test
    public void testLogicalAggregationRowCount() throws Exception {
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);

        List<ColumnRefOperator> groupByColumns = Lists.newArrayList(v1);
        Map<ColumnRefOperator, CallOperator> aggCall = new HashMap<>();

        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 100, 0, 10, 50)));

        Group childGroup = new Group(0);
        childGroup.setStatistics(builder.build());

        LogicalAggregationOperator aggNode = new LogicalAggregationOperator(AggType.GLOBAL, groupByColumns, aggCall);
        GroupExpression groupExpression = new GroupExpression(aggNode, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(50, expressionContext.getStatistics().getOutputRowCount(), 0.001);

        groupByColumns = Lists.newArrayList(v1, v2);
        aggNode = new LogicalAggregationOperator(AggType.GLOBAL, groupByColumns, aggCall);
        groupExpression = new GroupExpression(aggNode, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(
                50 * 50 * Math.pow(StatisticsEstimateCoefficient.UNKNOWN_GROUP_BY_CORRELATION_COEFFICIENT, 2),
                expressionContext.getStatistics().getOutputRowCount(), 0.001);
    }

    @Test
    public void testLogicalUnion() throws Exception {
        // child 1 output column
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);
        // child 2 output column
        ColumnRefOperator v3 = columnRefFactory.create("v3", IntegerType.INT, true);
        ColumnRefOperator v4 = columnRefFactory.create("v4", IntegerType.INT, true);
        // union node output column
        ColumnRefOperator v5 = columnRefFactory.create("v3", IntegerType.INT, true);
        ColumnRefOperator v6 = columnRefFactory.create("v4", IntegerType.INT, true);
        // child 1 statistics
        Statistics.Builder childBuilder1 = Statistics.builder();
        childBuilder1.setOutputRowCount(10000);
        childBuilder1.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        childBuilder1.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 50, 0, 10, 50)));
        Group childGroup1 = new Group(0);
        childGroup1.setStatistics(childBuilder1.build());
        // child 2 statistics
        Statistics.Builder childBuilder2 = Statistics.builder();
        childBuilder2.setOutputRowCount(20000);
        childBuilder2.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(100, 200, 0, 10, 50)));
        childBuilder2.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(0, 100, 0, 10, 100)));
        Group childGroup2 = new Group(1);
        childGroup2.setStatistics(childBuilder2.build());
        // construct group expression
        LogicalUnionOperator unionOperator = new LogicalUnionOperator(Lists.newArrayList(v5, v6),
                Lists.newArrayList(Lists.newArrayList(v1, v2), Lists.newArrayList(v3, v4)), true);
        GroupExpression groupExpression =
                new GroupExpression(unionOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();

        ColumnStatistic columnStatisticV5 = expressionContext.getStatistics().getColumnStatistic(v5);
        ColumnStatistic columnStatisticV6 = expressionContext.getStatistics().getColumnStatistic(v6);
        Assertions.assertEquals(30000, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(new StatisticRangeValues(0, 200, 99), StatisticRangeValues.from(columnStatisticV5));
        Assertions.assertEquals(new StatisticRangeValues(0, 100, 100), StatisticRangeValues.from(columnStatisticV6));
    }

    @Test
    public void testLogicalOlapTableScan() throws Exception {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test").getTable("test_all_type");
        Collection<Partition> partitions = table.getPartitions();
        List<Long> partitionIds =
                partitions.stream().mapToLong(partition -> partition.getId()).boxed().collect(Collectors.toList());
        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000);
        }

        List<Column> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
            Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
            Column column = columns.get(i);
            ColumnRefOperator ref = new ColumnRefOperator(i, column.getType(), column.getName(), true);
            refToColumn.put(ref, column);
            columnToRef.put(column, ref);

            LogicalOlapScanOperator olapScanOperator = new LogicalOlapScanOperator(table,
                    refToColumn, columnToRef,
                    null, -1, null,
                    ((OlapTable) table).getBaseIndexMetaId(),
                    partitionIds,
                    null,
                    false,
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    false);

            GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
            groupExpression.setGroup(new Group(0));
            ExpressionContext expressionContext = new ExpressionContext(groupExpression);
            Statistics.Builder builder = Statistics.builder();
            olapScanOperator.getOutputColumns().forEach(col ->
                    builder.addColumnStatistic(col,
                            new ColumnStatistic(-100, 100, 0.0, 5.0, 10))
            );
            expressionContext.setStatistics(builder.build());
            StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                    columnRefFactory, optimizerContext);
            statisticsCalculator.estimatorStats();
            Assertions.assertEquals(1000 * partitions.size(), expressionContext.getStatistics().getOutputRowCount(), 0.001);
            Assertions.assertEquals(ref.getType().getTypeSize() * 1000 * partitions.size(),
                    expressionContext.getStatistics().getComputeSize(), 0.001);
        }
    }

    /**
     * Operator level test for both call sites that ask the statistics storage for a subfield path: the projection of a
     * scan operator, and a LogicalProject on top of a scan. A query reaches the first one, see
     * {@link #testQueryWithSubfieldOfArrayElement()}, but not the second one, because the project is merged into the
     * scan projection before statistics are estimated. That branch is therefore covered by a hand built operator tree.
     */
    @Test
    public void testSkipStatisticsForSubfieldOfArrayElement() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test")
                .getTable("test_all_type");
        List<Long> partitionIds = table.getPartitions().stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());

        Type structType = new StructType(
                new ArrayList<>(List.of(new StructField("bucket_id", IntegerType.INT))));
        Type arrayType = new ArrayType(structType);
        Column arrayColumn = new Column("bucket_array", arrayType, true);
        ColumnRefOperator arrayRef = columnRefFactory.create("bucket_array", arrayType, true);
        ColumnRefOperator outputRef = columnRefFactory.create("bucket_id", IntegerType.INT, true);
        CollectionElementOperator arrayElement = new CollectionElementOperator(
                structType, arrayRef, ConstantOperator.createInt(1), false);
        SubfieldOperator subfield = new SubfieldOperator(
                arrayElement, IntegerType.INT, List.of("bucket_id"));

        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(table,
                ImmutableMap.of(arrayRef, arrayColumn),
                ImmutableMap.of(arrayColumn, arrayRef),
                null, -1, null,
                table.getBaseIndexMetaId(),
                partitionIds,
                null,
                false,
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                false);
        scanOperator.setProjection(new Projection(ImmutableMap.of(outputRef, subfield)));

        List<String> requestedColumns = new ArrayList<>();
        StatisticStorage originalStorage = globalStateMgr.getStatisticStorage();
        globalStateMgr.setStatisticStorage(new EmptyStatisticStorage() {
            @Override
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                requestedColumns.addAll(columns);
                return super.getColumnStatistics(table, columns);
            }
        });
        try {
            GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
            groupExpression.setGroup(new Group(0));
            ExpressionContext expressionContext = new ExpressionContext(groupExpression);

            new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

            // the array column itself has no collectable statistics, so only the subfield path could be requested,
            // and a path rooted at the array element must not be
            Assertions.assertFalse(requestedColumns.stream().anyMatch(column -> column.contains(",")),
                    requestedColumns.toString());
            Assertions.assertTrue(expressionContext.getStatistics().getColumnStatistic(outputRef).isUnknown());

            requestedColumns.clear();
            scanOperator.setProjection(null);
            OptExpression scanExpression = OptExpression.create(scanOperator);
            scanExpression.setStatistics(Statistics.builder()
                    .setOutputRowCount(100)
                    .addColumnStatistic(arrayRef, ColumnStatistic.unknown())
                    .build());
            OptExpression projectExpression = OptExpression.create(
                    new LogicalProjectOperator(ImmutableMap.of(outputRef, subfield)), scanExpression);
            ExpressionContext projectContext = new ExpressionContext(projectExpression);

            new StatisticsCalculator(projectContext, columnRefFactory, optimizerContext).estimatorStats();

            Assertions.assertTrue(requestedColumns.isEmpty());
            Assertions.assertTrue(projectContext.getStatistics().getColumnStatistic(outputRef).isUnknown());
        } finally {
            globalStateMgr.setStatisticStorage(originalStorage);
        }
    }

    @Test
    public void testStatisticsForDirectSubfield() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test")
                .getTable("test_all_type");
        List<Long> partitionIds = table.getPartitions().stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());

        Type structType = new StructType(
                new ArrayList<>(List.of(new StructField("bucket_id", IntegerType.INT))));
        Column structColumn = new Column("bucket_struct", structType, true);
        ColumnRefOperator structRef = columnRefFactory.create("bucket_struct", structType, true);
        ColumnRefOperator outputRef = columnRefFactory.create("bucket_id", IntegerType.INT, true);
        SubfieldOperator subfield = new SubfieldOperator(
                structRef, IntegerType.INT, List.of("bucket_id"));

        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(table,
                ImmutableMap.of(structRef, structColumn),
                ImmutableMap.of(structColumn, structRef),
                null, -1, null,
                table.getBaseIndexMetaId(),
                partitionIds,
                null,
                false,
                Lists.newArrayList(),
                Lists.newArrayList(),
                Lists.newArrayList(),
                false);
        scanOperator.setProjection(new Projection(ImmutableMap.of(outputRef, subfield)));

        ColumnStatistic expected = new ColumnStatistic(1, 10, 0, 8, 5);
        StatisticStorage originalStorage = globalStateMgr.getStatisticStorage();
        globalStateMgr.setStatisticStorage(new EmptyStatisticStorage() {
            @Override
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                if (columns.equals(List.of("bucket_struct.bucket_id"))) {
                    return List.of(expected);
                }
                return super.getColumnStatistics(table, columns);
            }
        });
        try {
            GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
            groupExpression.setGroup(new Group(0));
            ExpressionContext expressionContext = new ExpressionContext(groupExpression);

            new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

            ColumnStatistic actual = expressionContext.getStatistics().getColumnStatistic(outputRef);
            Assertions.assertFalse(actual.isUnknown());
            Assertions.assertEquals(expected.getMinValue(), actual.getMinValue());
            Assertions.assertEquals(expected.getMaxValue(), actual.getMaxValue());
            Assertions.assertEquals(expected.getDistinctValuesCount(), actual.getDistinctValuesCount());
        } finally {
            globalStateMgr.setStatisticStorage(originalStorage);
        }
    }

    /**
     * Query level regression test for the same bug. `ass[1].a` reaches the optimizer as a SubfieldOperator whose root
     * is an expression instead of a table column, so before the fix the scan asked the statistics storage for the path
     * `3: ass,1.a`, which no table column can ever match.
     * <p>
     * The statistics storage below reproduces what production then does with such a path: the loader behind
     * CachedStatisticStorage resolves every requested path with StatisticUtils#getQueryStatisticsColumnType, which
     * rejects an unknown root column, and CachedStatisticStorage falls back to unknown statistics for the whole
     * requested batch. Since one scan requests all of its subfield paths in a single batch, the unsupported path also
     * throws away the statistics of the valid `st1.s1` path selected by the same query. That is the user visible
     * impact: a repeated failing statistics lookup plus worse cost estimates for the other columns of the projection.
     */
    @Test
    public void testQueryWithSubfieldOfArrayElement() throws Exception {
        starRocksAssert.withTable("CREATE TABLE `test_subfield_statistics` (\n" +
                "  `v1` bigint NULL,\n" +
                "  `st1` struct<s1 int, s2 int> NULL,\n" +
                "  `ass` array<struct<a int, b int>> NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        ColumnStatistic structSubfieldStatistic = new ColumnStatistic(1, 10, 0, 4, 5);
        List<String> requestedColumns = new ArrayList<>();
        List<String> rejectedRequests = new ArrayList<>();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        StatisticStorage originalStorage = globalStateMgr.getStatisticStorage();
        // the test table has no data, keep the scan instead of letting PruneEmptyScanRule replace it with values
        boolean originalPruneEmptyOutputScan = FeConstants.enablePruneEmptyOutputScan;
        FeConstants.enablePruneEmptyOutputScan = false;
        globalStateMgr.setStatisticStorage(new EmptyStatisticStorage() {
            @Override
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                requestedColumns.addAll(columns);
                try {
                    columns.forEach(column -> StatisticUtils.getQueryStatisticsColumnType(table, column));
                } catch (SemanticException e) {
                    rejectedRequests.add(columns.toString());
                    return super.getColumnStatistics(table, columns);
                }
                return columns.stream()
                        .map(column -> "st1.s1".equals(column) ? structSubfieldStatistic : ColumnStatistic.unknown())
                        .collect(Collectors.toList());
            }
        });
        try {
            String plan = UtFrameUtils.getPlanAndFragment(connectContext,
                            "select ass[1].a, st1.s1, " +
                                    "cast(parse_json('{\"k\": 45}') as struct<k varchar>).k " +
                                    "from test_subfield_statistics")
                    .second.getExplainString(TExplainLevel.COSTS);

            Assertions.assertTrue(rejectedRequests.isEmpty(),
                    "unsupported statistics path requested: " + rejectedRequests);
            Assertions.assertTrue(requestedColumns.contains("st1.s1"),
                    "statistics of the supported subfield path were not requested: " + requestedColumns);
            Assertions.assertTrue(plan.contains("-->[1.0, 10.0, 0.0, 4.0, 5.0] ESTIMATE"), plan);
        } finally {
            FeConstants.enablePruneEmptyOutputScan = originalPruneEmptyOutputScan;
            globalStateMgr.setStatisticStorage(originalStorage);
            starRocksAssert.dropTable("test_subfield_statistics");
        }
    }

    @Test
    public void testLogicalIcebergTableScan() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table icebergTable = globalStateMgr.getMetadataMgr().getTable(connectContext, "iceberg0", "partitioned_db", "t1");
        List<Column> columns = icebergTable.getColumns();

        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        ColumnRefOperator partitionColumn = null;
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            ColumnRefOperator ref = new ColumnRefOperator(i, column.getType(), column.getName(), true);
            if (column.getName().equals("date")) {
                partitionColumn = ref;
            }
            refToColumn.put(ref, column);
            columnToRef.put(column, ref);
        }

        BinaryPredicateOperator predicateOperator = new BinaryPredicateOperator(BinaryType.LT,
                partitionColumn, ConstantOperator.createInt(50));
        LogicalIcebergScanOperator icebergScanOperator = new LogicalIcebergScanOperator(icebergTable, refToColumn,
                columnToRef, -1, predicateOperator, TvrTableSnapshot.empty());

        GroupExpression groupExpression = new GroupExpression(icebergScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatisticsFromInternalStatistics(Table table, Map<ColumnRefOperator,
                    Column> columns) {
                Statistics.Builder builder = Statistics.builder();
                icebergScanOperator.getOutputColumns().forEach(col ->
                        builder.addColumnStatistic(col,
                                new ColumnStatistic(0, 100, 0.0, 5.0, 100))
                );
                builder.setOutputRowCount(100);
                // like the real method: internal statistics are full-table ANALYZE statistics
                return builder.setStatsSource(Statistics.StatsSource.ANALYZE).build();
            }
        };
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        // ANALYZE statistics cover the whole table, so the partition predicate stays in the selectivity
        Assertions.assertEquals(50, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(50, expressionContext.getStatistics().
                getColumnStatistic(partitionColumn).getMaxValue(), 0.001);
    }

    @Test
    public void testRemoveIcebergPartitionPredicateFollowsScanStatsSource() {
        LogicalIcebergScanOperator scan = icebergScan("partitioned_db", "t1");
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.LT, icebergColumn(scan, "date"),
                ConstantOperator.createVarchar("2020-01-03"));
        StatisticsCalculator calculator = new StatisticsCalculator();

        // row count from the pruned files: the partition predicate is already applied
        Assertions.assertEquals(ConstantOperator.TRUE,
                calculator.removePartitionPredicate(predicate, scan, ICEBERG_TABLE_METADATA));
        // full-table ANALYZE statistics and default statistics keep it in the selectivity
        Statistics analyzed = Statistics.builder().setStatsSource(Statistics.StatsSource.ANALYZE).build();
        Assertions.assertEquals(predicate, calculator.removePartitionPredicate(predicate, scan, analyzed));
        Assertions.assertEquals(predicate,
                calculator.removePartitionPredicate(predicate, scan, Statistics.builder().build()));
        Assertions.assertEquals(predicate, calculator.removePartitionPredicate(predicate, scan, null));
    }

    @Test
    public void testRemovedIcebergPartitionPredicateKeepsColumnStatistics() {
        LogicalIcebergScanOperator scan = icebergScan("partitioned_db", "t1");
        ColumnRefOperator date = icebergColumn(scan, "date");
        ColumnRefOperator id = icebergColumn(scan, "id");
        ScalarOperator predicate = new InPredicateOperator(false, date, ConstantOperator.createVarchar("2020-01-01"),
                ConstantOperator.createVarchar("2020-01-02"));
        scan = new LogicalIcebergScanOperator(scan.getTable(), scan.getColRefToColumnMetaMap(),
                scan.getColumnMetaToColRefMap(), -1, predicate, TvrTableSnapshot.empty());
        LogicalIcebergScanOperator finalScan = scan;
        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns,
                                                 List<PartitionKey> partitionKeys, ScalarOperator predicate,
                                                 long limit, TvrVersionRange version) {
                // the row count of the files planned with the scan predicate
                Statistics.Builder builder = Statistics.builder().setOutputRowCount(100)
                        .setStatsSource(Statistics.StatsSource.TABLE_METADATA);
                finalScan.getOutputColumns().forEach(col -> builder.addColumnStatistic(col,
                        new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.0, 10.0, 50)));
                return builder.build();
            }
        };
        GroupExpression groupExpression = new GroupExpression(scan, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

        Statistics statistics = expressionContext.getStatistics();
        // the IN list is not applied to the pruned row count again, but still caps the NDV of its column
        Assertions.assertEquals(100, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(2, statistics.getColumnStatistic(date).getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(50, statistics.getColumnStatistic(id).getDistinctValuesCount(), 0.001);
    }

    @Test
    public void testRemoveIcebergPartitionPredicateOnlyForPrunedConjuncts() {
        StatisticsCalculator calculator = new StatisticsCalculator();

        // a string-to-temporal cast on the identity partition column is not part of the converted scan predicate
        LogicalIcebergScanOperator identity = icebergScan("partitioned_db", "t1");
        ScalarOperator castEq = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(DateType.DATE, icebergColumn(identity, "date")),
                ConstantOperator.createDate(LocalDateTime.of(2020, 1, 1, 0, 0)));
        Assertions.assertEquals(castEq, calculator.removePartitionPredicate(castEq, identity, ICEBERG_TABLE_METADATA));
        // a literal the converter cannot bind to the string column does not prune either
        ScalarOperator intLt = new BinaryPredicateOperator(BinaryType.LT, icebergColumn(identity, "date"),
                ConstantOperator.createInt(50));
        Assertions.assertEquals(intLt, calculator.removePartitionPredicate(intLt, identity, ICEBERG_TABLE_METADATA));

        // bucket / truncate / time transforms prune at most whole transformed partitions, a range on a bucket column
        // not at all
        List<String[]> transformed = List.of(new String[] {"t0_bucket", "ts"}, new String[] {"t0_day", "ts"},
                new String[] {"t0_truncate", "data"});
        for (String[] t : transformed) {
            LogicalIcebergScanOperator scan = icebergScan("partitioned_transforms_db", t[0]);
            ColumnRefOperator column = icebergColumn(scan, t[1]);
            ScalarOperator literal = column.getType().isStringType() ? ConstantOperator.createVarchar("abcde")
                    : ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0));
            ScalarOperator ge = new BinaryPredicateOperator(BinaryType.GE, column, literal);
            Assertions.assertEquals(ge, calculator.removePartitionPredicate(ge, scan, ICEBERG_TABLE_METADATA), t[0]);
        }

        // identity partitioned in the current spec only: files of the older month(ts) spec are not pruned exactly
        LogicalIcebergScanOperator evolved = icebergScan("partitioned_transforms_db", "t0_date_month_identity_evolution");
        ScalarOperator tsEq = new BinaryPredicateOperator(BinaryType.EQ, icebergColumn(evolved, "ts"),
                ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 1, 0, 0)));
        Assertions.assertEquals(tsEq, calculator.removePartitionPredicate(tsEq, evolved, ICEBERG_TABLE_METADATA));
    }

    private static final Statistics ICEBERG_TABLE_METADATA =
            Statistics.builder().setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();

    private LogicalIcebergScanOperator icebergScan(String db, String tableName) {
        Table icebergTable = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(connectContext, "iceberg0", db, tableName);
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        List<Column> columns = icebergTable.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnRefOperator ref = new ColumnRefOperator(i, columns.get(i).getType(), columns.get(i).getName(), true);
            refToColumn.put(ref, columns.get(i));
            columnToRef.put(columns.get(i), ref);
        }
        return new LogicalIcebergScanOperator(icebergTable, refToColumn, columnToRef, -1, null,
                TvrTableSnapshot.empty());
    }

    private static ColumnRefOperator icebergColumn(LogicalIcebergScanOperator scan, String name) {
        return scan.getColRefToColumnMetaMap().keySet().stream().filter(ref -> ref.getName().equals(name))
                .findFirst().orElseThrow();
    }

    @Test
    public void testLogicalIcebergTableScanCapturesAnalyzedStatsToDump() {
        // A query dump records the per-column ANALYZE row counts and the iceberg format version of an external table
        // planned from ANALYZE statistics, so replay can reproduce the scan row count and V3-only rewrites.
        Table icebergTable = connectContext.getGlobalStateMgr().getMetadataMgr()
                .getTable(connectContext, "iceberg0", "partitioned_db", "t1");
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        List<Column> columns = icebergTable.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            ColumnRefOperator ref = new ColumnRefOperator(i, columns.get(i).getType(), columns.get(i).getName(), true);
            refToColumn.put(ref, columns.get(i));
            columnToRef.put(columns.get(i), ref);
        }
        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatisticsFromInternalStatistics(Table table, Map<ColumnRefOperator,
                    Column> cols) {
                Statistics.Builder builder = Statistics.builder();
                cols.keySet().forEach(col -> builder.addColumnStatistic(col, new ColumnStatistic(0, 100, 0.0, 5.0, 100)));
                return builder.setOutputRowCount(100).build();
            }
        };
        String analyzed = columns.get(0).getName();
        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ConnectorTableColumnStats> getConnectorTableStatistics(Table table, List<String> cols) {
                return cols.stream().map(c -> c.equals(analyzed)
                        ? new ConnectorTableColumnStats(new ColumnStatistic(0, 100, 0.0, 5.0, 100), 3612L, "")
                        : ConnectorTableColumnStats.unknown()).collect(Collectors.toList());
            }
        };

        DumpInfo previous = connectContext.getDumpInfo();
        try {
            QueryDumpInfo dumpInfo = new QueryDumpInfo(connectContext);
            // the analyzer registers every table of a dumped query
            dumpInfo.addTable("partitioned_db", icebergTable);
            connectContext.setDumpInfo(dumpInfo);
            estimateIcebergScan(icebergTable, refToColumn, columnToRef);
            Assertions.assertEquals(Map.of("partitioned_db.t1", Map.of(analyzed, 3612L)),
                    dumpInfo.getExternalColumnRowCountMap());
            Assertions.assertEquals(1, dumpInfo.getExternalTableFormatVersionMap().size());

            // a dump info that does not keep these sections, and no dump at all, plan the same way
            connectContext.setDumpInfo(new MockDumpInfo());
            estimateIcebergScan(icebergTable, refToColumn, columnToRef);
            connectContext.setDumpInfo(null);
            estimateIcebergScan(icebergTable, refToColumn, columnToRef);
        } finally {
            connectContext.setDumpInfo(previous);
        }
    }

    private void estimateIcebergScan(Table icebergTable, Map<ColumnRefOperator, Column> refToColumn,
                                     Map<Column, ColumnRefOperator> columnToRef) {
        LogicalIcebergScanOperator scan = new LogicalIcebergScanOperator(icebergTable, refToColumn, columnToRef, -1,
                null, TvrTableSnapshot.empty());
        GroupExpression groupExpression = new GroupExpression(scan, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();
        Assertions.assertEquals(100, expressionContext.getStatistics().getOutputRowCount(), 0.001);
    }

    @Test
    public void testLogicalOlapTableEmptyPartition() {
        ColumnRefOperator idDate = columnRefFactory.create("id_date", DateType.DATE, true);
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table table = globalStateMgr.getLocalMetastore().getDb("statistics_test").getTable("test_all_type");

        List<Partition> partitions = new ArrayList<>(table.getPartitions());

        Partition partition1 = partitions.get(0);
        Partition partition2 = partitions.get(1);
        Partition partition3 = partitions.get(2);
        // mock one empty partition
        partition1.getDefaultPhysicalPartition().setVisibleVersion(Partition.PARTITION_INIT_VERSION, System.currentTimeMillis());
        partition1.getDefaultPhysicalPartition().setDataVersion(Partition.PARTITION_INIT_VERSION);
        partition2.getDefaultPhysicalPartition().setVisibleVersion(2, System.currentTimeMillis());
        partition2.getDefaultPhysicalPartition().setDataVersion(2);
        partition3.getDefaultPhysicalPartition().setVisibleVersion(2, System.currentTimeMillis());
        partition3.getDefaultPhysicalPartition().setDataVersion(2);
        List<Long> partitionIds = partitions.stream().filter(partition -> !(partition.getName().equalsIgnoreCase("p1"))).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());

        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                return List.of(new ColumnStatistic(0, Utils.getLongFromDateTime(
                        LocalDateTime.of(2014, 12, 01, 0, 0, 0)), 0, 0, 30));
            }
        };

        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate),
                        null, -1, null,
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);

        GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        ColumnStatistic columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        Assertions.assertEquals(30, columnStatistic.getDistinctValuesCount(), 0.001);

    }

    @Test
    public void testLogicalOlapTableScanPartitionPrune1() {
        FeConstants.runningUnitTest = true;
        ColumnRefOperator idDate = columnRefFactory.create("id_date", DateType.DATE, true);

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        Table table = globalStateMgr.getLocalMetastore().getDb("statistics_test").getTable("test_all_type");

        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                return List.of(new ColumnStatistic(0, Utils.getLongFromDateTime(
                        LocalDateTime.of(2014, 12, 01, 0, 0, 0)), 0, 0, 30));
            }

            @Mock
            public ColumnStatistic getColumnStatistic(Table table, String column) {
                return new ColumnStatistic(0, Utils.getLongFromDateTime(
                        LocalDateTime.of(2014, 12, 01, 0, 0, 0)), 0, 0, 30);
            }
        };

        Collection<Partition> partitions = ((OlapTable) table).getPartitions();
        // select partition p1
        List<Long> partitionIds = partitions.stream().filter(partition -> partition.getName().equalsIgnoreCase("p1")).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000);
        }

        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate),
                        null, -1,
                        new BinaryPredicateOperator(BinaryType.EQ,
                                idDate, ConstantOperator.createDate(LocalDateTime.of(2013, 12, 30, 0, 0, 0))),
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);

        GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        // partition column count distinct values is 30 in table level, after partition prune,
        // the column statistic distinct values is 10, so the estimate row count is 1000 * (1/10)
        Assertions.assertEquals(100, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        ColumnStatistic columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2013, 12, 30, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);

        // select partition p2, p3
        partitionIds.clear();
        partitionIds = partitions.stream().filter(partition -> !(partition.getName().equalsIgnoreCase("p1"))).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate),
                        null, -1, null, ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);
        olapScanOperator.setPredicate(new BinaryPredicateOperator(BinaryType.GE,
                idDate, ConstantOperator.createDate(LocalDateTime.of(2014, 5, 1, 0, 0, 0))));

        groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);

        Assertions.assertEquals(1281.4371, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2014, 5, 1, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2014, 12, 1, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(20, columnStatistic.getDistinctValuesCount(), 0.001);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testLogicalOlapTableScanPartitionPrune2() {
        FeConstants.runningUnitTest = true;
        ColumnRefOperator idDate = columnRefFactory.create("id_date", DateType.DATE, true);

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test")
                .getTable("test_all_type_day_partition");

        new MockUp<CachedStatisticStorage>() {
            @Mock
            public List<ColumnStatistic> getColumnStatistics(Table table, List<String> columns) {
                return List.of(new ColumnStatistic(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 23, 0, 0, 0)),
                        Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 25, 0, 0, 0)), 0, 0, 3));
            }

            @Mock
            public ColumnStatistic getColumnStatistic(Table table, String column) {
                return new ColumnStatistic(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 23, 0, 0, 0)),
                        Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 25, 0, 0, 0)), 0, 0, 3);
            }
        };

        Collection<Partition> partitions = table.getPartitions();
        // select partition p2
        List<Long> partitionIds = partitions.stream().filter(partition -> partition.getName().equalsIgnoreCase("p2")).
                mapToLong(partition -> partition.getId()).boxed().collect(Collectors.toList());
        for (Partition partition : partitions) {
            partition.getDefaultPhysicalPartition().getLatestBaseIndex().setRowCount(1000);
        }

        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate), null, -1, null,
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);

        GroupExpression groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();

        Assertions.assertEquals(1000, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        ColumnStatistic columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 24, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 25, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(1, columnStatistic.getDistinctValuesCount(), 0.001);

        // select partition p2, p3
        partitionIds.clear();
        partitionIds = partitions.stream().filter(partition -> !(partition.getName().equalsIgnoreCase("p1"))).
                mapToLong(Partition::getId).boxed().collect(Collectors.toList());
        olapScanOperator =
                new LogicalOlapScanOperator(table,
                        ImmutableMap.of(idDate, new Column("id_date", DateType.DATE, true)),
                        ImmutableMap.of(new Column("id_date", DateType.DATE, true), idDate), null, -1, null,
                        ((OlapTable) table).getBaseIndexMetaId(),
                        partitionIds,
                        null,
                        false,
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        Lists.newArrayList(),
                        false);
        olapScanOperator.setPredicate(new BinaryPredicateOperator(BinaryType.GE,
                idDate, ConstantOperator.createDate(LocalDateTime.of(2020, 04, 24, 0, 0, 0))));

        groupExpression = new GroupExpression(olapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        columnStatistic = expressionContext.getStatistics().getColumnStatistic(idDate);
        // has two partitions
        Assertions.assertEquals(2000, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 24, 0, 0, 0)),
                columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(Utils.getLongFromDateTime(LocalDateTime.of(2020, 4, 26, 0, 0, 0)),
                columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(2, columnStatistic.getDistinctValuesCount(), 0.001);
        FeConstants.runningUnitTest = false;
    }

    @Test
    public void testPredicateOnColumnPinnedToSingleValue() {
        // `d_year = 2000` narrows the range to one value but keeps its NDV; the same filter applied again, e.g. on a
        // CTE consumer, must keep every row instead of dividing by that NDV a second time
        Statistics input = Statistics.builder().setOutputRowCount(1000).build();
        ColumnStatistic pinned = new ColumnStatistic(2000, 2000, 0, 4, 202);
        Assertions.assertEquals(1000, BinaryPredicateStatisticCalculator.estimatePredicateRange(Optional.empty(),
                pinned, new StatisticRangeValues(2000, 2000, 1), input).getOutputRowCount(), 1e-6);
        // `col < 2000` / `col > 2000` are encoded as ranges including 2000 (with NaN NDV, as the LT / GT estimators
        // build them); they must not keep the pinned rows
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), pinned, new StatisticRangeValues(Double.NEGATIVE_INFINITY, 2000, Double.NaN), input)
                .getOutputRowCount(), 1e-6);
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), pinned, new StatisticRangeValues(2000, Double.POSITIVE_INFINITY, Double.NaN), input)
                .getOutputRowCount(), 1e-6);
        // outside the pinned value: nothing is kept (the row count is floored at 1)
        Assertions.assertTrue(BinaryPredicateStatisticCalculator.estimatePredicateRange(Optional.empty(),
                pinned, new StatisticRangeValues(2001, 2001, 1), input).getOutputRowCount() <= 1);
        // a string constant has no finite bounds, and a string column's min = max is only a placeholder
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), pinned, new StatisticRangeValues(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                        1), input).getOutputRowCount(), 1e-6);
        // a real range still keeps 1 / NDV of its rows for a single value
        ColumnStatistic spread = new ColumnStatistic(1900, 2100, 0, 4, 202);
        Assertions.assertEquals(1000.0 / 202, BinaryPredicateStatisticCalculator.estimatePredicateRange(
                Optional.empty(), spread, new StatisticRangeValues(2000, 2000, 1), input).getOutputRowCount(), 1e-6);
    }

    @Test
    public void testJoinEstimateWithMultiColumns() {
        // child 1 output column
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);
        ColumnRefOperator v5 = columnRefFactory.create("v5", IntegerType.INT, true);
        // child 2 output column
        ColumnRefOperator v3 = columnRefFactory.create("v3", IntegerType.INT, true);
        ColumnRefOperator v4 = columnRefFactory.create("v4", IntegerType.INT, true);
        ColumnRefOperator v6 = columnRefFactory.create("v6", IntegerType.INT, true);
        // child 1 statistics
        Statistics.Builder childBuilder1 = Statistics.builder();
        childBuilder1.setOutputRowCount(10000);
        childBuilder1.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        childBuilder1.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 50, 0, 10, 50)));
        childBuilder1.addColumnStatistics(ImmutableMap.of(v5, new ColumnStatistic(0, 50, 0, 10, 50)));
        Group childGroup1 = new Group(0);
        childGroup1.setStatistics(childBuilder1.build());
        childGroup1.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(v1, v2, v5))));
        // child 2 statistics
        Statistics.Builder childBuilder2 = Statistics.builder();
        childBuilder2.setOutputRowCount(20000);
        childBuilder2.addColumnStatistics(ImmutableMap.of(v3, new ColumnStatistic(100, 200, 0, 10, 50)));
        childBuilder2.addColumnStatistics(ImmutableMap.of(v4, new ColumnStatistic(0, 100, 0, 10, 100)));
        childBuilder2.addColumnStatistics(ImmutableMap.of(v6, new ColumnStatistic(0, 100, 0, 10, 100)));
        Group childGroup2 = new Group(1);
        childGroup2.setStatistics(childBuilder2.build());
        childGroup2.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(v3, v4, v6))));

        // record column id to relation id
        columnRefFactory.updateColumnToRelationIds(v1.getId(), 0);
        columnRefFactory.updateColumnToRelationIds(v2.getId(), 0);
        columnRefFactory.updateColumnToRelationIds(v3.getId(), 1);
        columnRefFactory.updateColumnToRelationIds(v4.getId(), 1);
        columnRefFactory.updateColumnToRelationIds(v5.getId(), 3);
        columnRefFactory.updateColumnToRelationIds(v6.getId(), 4);
        // on predicate : t0.v1 = t1.v3 and t0.v2 = t1.v4
        BinaryPredicateOperator eqOnPredicate1 =
                new BinaryPredicateOperator(BinaryType.EQ, v1, v3);
        BinaryPredicateOperator eqOnPredicate2 =
                new BinaryPredicateOperator(BinaryType.EQ, v2, v4);
        BinaryPredicateOperator eqOnPredicate3 =
                new BinaryPredicateOperator(BinaryType.EQ, v5, v6);
        // construct group expression
        LogicalJoinOperator joinOperator =
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, eqOnPredicate1, eqOnPredicate2));
        GroupExpression groupExpression =
                new GroupExpression(joinOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(400000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);
        // use correlated method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(true);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(1800000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);

        // on predicate : t0.v2 = t1.v4 and t0.v1 = t1.v3, the same predicates in the other order: middle ground
        // applies the larger selectivity first whatever the order, so the estimate must not change
        GroupExpression swappedExpression = new GroupExpression(
                new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.AND, eqOnPredicate2, eqOnPredicate1)),
                Lists.newArrayList(childGroup1, childGroup2));
        swappedExpression.setGroup(new Group(2));
        ExpressionContext swappedContext = new ExpressionContext(swappedExpression);
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        new StatisticsCalculator(swappedContext, columnRefFactory, optimizerContext).estimatorStats();
        Assertions.assertEquals(400000.0, swappedContext.getStatistics().getOutputRowCount(), 0.0001);

        // on predicate : t0.v1 = t1.v3 and t0.v2 = t2.v4
        columnRefFactory.updateColumnToRelationIds(v4.getId(), 2);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(40000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);
        columnRefFactory.updateColumnToRelationIds(v4.getId(), 1);

        // on predicate : t0.v1 = t1.v3 and t0.v2 = t2.v4 and t3.v5 = t4.v6
        // construct group expression
        joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, eqOnPredicate1, new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, eqOnPredicate2, eqOnPredicate3)));
        groupExpression = new GroupExpression(joinOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(4000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);

        // on predicate : t0.v1 = t1.v3 + t1.v4 and t0.v2 = t1.v3 + t1.v4
        BinaryPredicateOperator eqOnPredicateWithAdd1 =
                new BinaryPredicateOperator(BinaryType.EQ, v1,
                        new CallOperator("add", IntegerType.BIGINT, Lists.newArrayList(v3, v4)));
        BinaryPredicateOperator eqOnPredicateWithAdd2 =
                new BinaryPredicateOperator(BinaryType.EQ, v2,
                        new CallOperator("add", IntegerType.BIGINT, Lists.newArrayList(v3, v4)));
        joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.AND, eqOnPredicateWithAdd1, eqOnPredicateWithAdd2));
        groupExpression = new GroupExpression(joinOperator, Lists.newArrayList(childGroup1, childGroup2));
        groupExpression.setGroup(new Group(2));
        expressionContext = new ExpressionContext(groupExpression);
        statisticsCalculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        // use middle ground method to estimate
        ConnectContext.get().getSessionVariable().setUseCorrelatedJoinEstimate(false);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(200000.0, expressionContext.getStatistics().getOutputRowCount(), 0.0001);
    }

    @Test
    public void testJoinEstimateAppliesAllResidualPredicates() {
        ColumnRefOperator l1 = columnRefFactory.create("l1", IntegerType.INT, true);
        ColumnRefOperator l2 = columnRefFactory.create("l2", IntegerType.INT, true);
        ColumnRefOperator r1 = columnRefFactory.create("r1", IntegerType.INT, true);
        ColumnRefOperator r2 = columnRefFactory.create("r2", IntegerType.INT, true);
        ColumnRefOperator r3 = columnRefFactory.create("r3", IntegerType.INT, true);
        columnRefFactory.updateColumnToRelationIds(l1.getId(), 0);
        columnRefFactory.updateColumnToRelationIds(l2.getId(), 0);
        columnRefFactory.updateColumnToRelationIds(r1.getId(), 1);
        columnRefFactory.updateColumnToRelationIds(r2.getId(), 1);
        columnRefFactory.updateColumnToRelationIds(r3.getId(), 1);

        Statistics.Builder leftBuilder = Statistics.builder();
        leftBuilder.setOutputRowCount(10000);
        leftBuilder.addColumnStatistics(ImmutableMap.of(l1, new ColumnStatistic(0, 100, 0, 10, 50)));
        leftBuilder.addColumnStatistics(ImmutableMap.of(l2, new ColumnStatistic(0, 50, 0, 10, 50)));
        Group leftGroup = new Group(0);
        leftGroup.setStatistics(leftBuilder.build());
        leftGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(l1, l2))));

        Statistics.Builder rightBuilder = Statistics.builder();
        rightBuilder.setOutputRowCount(20000);
        rightBuilder.addColumnStatistics(ImmutableMap.of(r1, new ColumnStatistic(0, 100, 0, 10, 100)));
        rightBuilder.addColumnStatistics(ImmutableMap.of(r2, new ColumnStatistic(0, 50, 0, 10, 50)));
        rightBuilder.addColumnStatistics(ImmutableMap.of(r3, new ColumnStatistic(0, 50, 0, 10, 50)));
        Group rightGroup = new Group(1);
        rightGroup.setStatistics(rightBuilder.build());
        rightGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(r1, r2, r3))));

        BinaryPredicateOperator eq = new BinaryPredicateOperator(BinaryType.EQ, l1, r1);
        BinaryPredicateOperator ge = new BinaryPredicateOperator(BinaryType.GE, l2, r2);
        BinaryPredicateOperator le = new BinaryPredicateOperator(BinaryType.LE, l2, r3);

        // ON l1 = r1
        double eqRows = estimateJoin(JoinOperator.INNER_JOIN, eq, leftGroup, rightGroup).getOutputRowCount();
        Assertions.assertTrue(eqRows > 1);

        // ON l1 = r1 AND l2 >= r2: one column-to-column residual, unknown coefficient 0.5
        Statistics oneResidual =
                estimateJoin(JoinOperator.INNER_JOIN, Utils.compoundAnd(eq, ge), leftGroup, rightGroup);
        Assertions.assertEquals(eqRows * 0.5, oneResidual.getOutputRowCount(), 0.0001);

        // ON l1 = r1 AND l2 >= r2 AND l2 <= r3: both residuals must apply, 0.5 * 0.5
        Statistics twoResiduals =
                estimateJoin(JoinOperator.INNER_JOIN, Utils.compoundAnd(eq, ge, le), leftGroup, rightGroup);
        Assertions.assertEquals(eqRows * 0.25, twoResiduals.getOutputRowCount(), 0.0001);

        // ON l1 = r1 AND l2 <= 25 AND l2 >= r2: the constant range residual is not the last conjunct,
        // its column statistics narrowing must survive the following residual
        BinaryPredicateOperator constRange =
                new BinaryPredicateOperator(BinaryType.LE, l2, ConstantOperator.createInt(25));
        Statistics withConstRange =
                estimateJoin(JoinOperator.INNER_JOIN, Utils.compoundAnd(eq, constRange, ge), leftGroup, rightGroup);
        Assertions.assertEquals(25, withConstRange.getColumnStatistic(l2).getMaxValue(), 0.0001);
        Assertions.assertTrue(withConstRange.getOutputRowCount() < oneResidual.getOutputRowCount());
    }

    @Test
    public void testNotFoundColumnStatistics() {
        ColumnRefOperator v1 = columnRefFactory.create("v1", IntegerType.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", IntegerType.INT, true);

        ColumnRefOperator v3 = columnRefFactory.create("v3", IntegerType.INT, true);
        Statistics.Builder builder = Statistics.builder();
        builder.setOutputRowCount(10000);
        builder.addColumnStatistics(ImmutableMap.of(v1, new ColumnStatistic(0, 100, 0, 10, 50)));
        builder.addColumnStatistics(ImmutableMap.of(v2, new ColumnStatistic(0, 100, 0, 10, 50)));
        Statistics statistics = builder.build();
        Assertions.assertThrows(StarRocksPlannerException.class, () -> statistics.getColumnStatistic(v3));
    }

    public enum OuterJoin {
        LEFT_OUTER_JOIN,
        RIGHT_OUTER_JOIN
    }

    @ParameterizedTest
    @EnumSource(value = OuterJoin.class)
    public void testOuterJoinPreservesOuterSideNullFraction(OuterJoin outerJoin) {
        // GIVEN
        ColumnRefOperator outerJoinKey = columnRefFactory.create("nullable_col", IntegerType.INT, true);
        ColumnRefOperator outerOtherCol = columnRefFactory.create("other_col", IntegerType.INT, true);

        final double outerNullFraction = 0.755;
        Statistics.Builder outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(1000000);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerJoinKey, new ColumnStatistic(1, 100000, outerNullFraction, 8, 50000)));
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerOtherCol, new ColumnStatistic(0, 100, 0.1, 8, 50)));

        Group outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(
                new ColumnRefSet(Lists.newArrayList(outerJoinKey, outerOtherCol))));

        ColumnRefOperator innerKey = columnRefFactory.create("dim_id", IntegerType.INT, true);
        ColumnRefOperator innerVal = columnRefFactory.create("dim_val", IntegerType.INT, true);

        Statistics.Builder innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(200000);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, new ColumnStatistic(1, 200000, 0, 8, 200000)));
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerVal, new ColumnStatistic(0, 100, 0, 8, 50)));

        Group innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(innerKey, innerVal))));

        // LEFT JOIN: outer.nullable_col = inner.dim1_id
        // RIGHT JOIN: inner.dim1_id = outer.nullable_col
        JoinOperator joinType;
        if (outerJoin == OuterJoin.LEFT_OUTER_JOIN) {
            joinType = JoinOperator.LEFT_OUTER_JOIN;
        } else {
            joinType = JoinOperator.RIGHT_OUTER_JOIN;
        }

        BinaryPredicateOperator joinPred;
        GroupExpression groupExpr;
        if (joinType == JoinOperator.LEFT_OUTER_JOIN) {
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, outerJoinKey, innerKey);
        } else {
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, innerKey, outerJoinKey);
        }

        LogicalJoinOperator joinOp = new LogicalJoinOperator(joinType, joinPred);

        if (joinType == JoinOperator.LEFT_OUTER_JOIN) {
            groupExpr = new GroupExpression(joinOp, Lists.newArrayList(outerGroup, innerGroup));
        } else {
            groupExpr = new GroupExpression(joinOp, Lists.newArrayList(innerGroup, outerGroup));
        }

        Group joinGroup = new Group(2);
        groupExpr.setGroup(joinGroup);
        ExpressionContext exprCtx = new ExpressionContext(groupExpr);
        StatisticsCalculator calc = new StatisticsCalculator(exprCtx, columnRefFactory, optimizerContext);

        // WHEN
        // The outer side join key's null fraction should be preserved after LEFT JOIN
        calc.estimatorStats();
        Statistics joinStats = exprCtx.getStatistics();
        ColumnStatistic outerJoinKeyStatAfterJoin = joinStats.getColumnStatistic(outerJoinKey);

        // THEN
        Assertions.assertEquals(outerNullFraction, outerJoinKeyStatAfterJoin.getNullsFraction(), 0.001,
                "Outer join key null fraction should be preserved after " + outerJoin.name());
    }

    @ParameterizedTest
    @EnumSource(value = OuterJoin.class)
    public void testOuterJoinEqForNullDoesNotTreatNullKeysAsUnmatched(OuterJoin outerJoin) {
        // GIVEN
        final var outerJoinKey = columnRefFactory.create("outer_key", IntegerType.INT, true);
        final var outerOtherCol = columnRefFactory.create("outer_other_col", IntegerType.INT, true);

        final double outerNullFraction = 0.6;
        final var outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(100000);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerJoinKey, new ColumnStatistic(1, 1000, outerNullFraction, 8, 500)));
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerOtherCol, new ColumnStatistic(0, 100, 0.1, 8, 50)));

        final var outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(
                new ColumnRefSet(Lists.newArrayList(outerJoinKey, outerOtherCol))));

        final var innerKey = columnRefFactory.create("inner_id", IntegerType.INT, true);
        final var innerVal = columnRefFactory.create("inner_val", IntegerType.INT, true);

        final var innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(80000);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, new ColumnStatistic(1, 1000, 0.1, 8, 500)));
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerVal, new ColumnStatistic(0, 100, 0, 8, 50)));

        final var innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(innerKey, innerVal))));

        JoinOperator joinType;
        if (outerJoin == OuterJoin.LEFT_OUTER_JOIN) {
            joinType = JoinOperator.LEFT_OUTER_JOIN;
        } else {
            joinType = JoinOperator.RIGHT_OUTER_JOIN;
        }

        BinaryPredicateOperator joinPred;
        GroupExpression groupExpr;
        if (joinType == JoinOperator.LEFT_OUTER_JOIN) {
            joinPred = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, outerJoinKey, innerKey);
        } else {
            joinPred = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, innerKey, outerJoinKey);
        }

        final var joinOp = new LogicalJoinOperator(joinType, joinPred);

        if (joinType == JoinOperator.LEFT_OUTER_JOIN) {
            groupExpr = new GroupExpression(joinOp, Lists.newArrayList(outerGroup, innerGroup));
        } else {
            groupExpr = new GroupExpression(joinOp, Lists.newArrayList(innerGroup, outerGroup));
        }

        final var joinGroup = new Group(2);
        groupExpr.setGroup(joinGroup);
        final var exprCtx = new ExpressionContext(groupExpr);
        final var calc = new StatisticsCalculator(exprCtx, columnRefFactory, optimizerContext);

        // WHEN
        calc.estimatorStats();
        final var joinStats = exprCtx.getStatistics();
        final var innerValStatAfterJoin = joinStats.getColumnStatistic(innerVal);

        // THEN
        Assertions.assertTrue(innerValStatAfterJoin.getNullsFraction() < outerNullFraction);
    }

    @Test
    public void testLeftJoinInnerSideNullFractionReflectsResultNulls() {
        // GIVEN
        // LEFT JOIN where outer key has high null fraction and inner has UNKNOWN stats on one eq col,
        // triggering innerRowCount = outerRowCount. Inner-side null fraction should still reflect the high null fraction.
        final var outerKey = columnRefFactory.create("outer_key", IntegerType.BIGINT, true);
        final var outerCol = columnRefFactory.create("outer_col", IntegerType.BIGINT, true);
        final var innerKey = columnRefFactory.create("inner_key", IntegerType.BIGINT, true);
        final var innerCol = columnRefFactory.create("inner_col", IntegerType.BIGINT, true);
        final var innerVal = columnRefFactory.create("val", VarcharType.VARCHAR, true);

        final double outerKeyNullFraction = 0.95;
        final var outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(1000);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerKey, new ColumnStatistic(1, 1000, outerKeyNullFraction, 8, 500),
                outerCol, new ColumnStatistic(0, 100, 0, 8, 1000)));
        final var outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(outerKey, outerCol))));

        final var innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(500);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, new ColumnStatistic(1, 1000, 0, 8, 500),
                innerCol, ColumnStatistic.unknown(),
                innerVal, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0.001, 10, 100)));
        final var innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(innerKey, innerCol, innerVal))));

        final var joinPred = Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.EQ, outerCol, innerCol),
                new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey));
        final var joinOp = new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, joinPred);
        final var groupExpr = new GroupExpression(joinOp, Lists.newArrayList(outerGroup, innerGroup));
        groupExpr.setGroup(new Group(2));

        final var ctx = new ExpressionContext(groupExpr);

        // WHEN
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        final var valStat = ctx.getStatistics().getColumnStatistic(innerVal);

        // THEN
        Assertions.assertTrue(valStat.getNullsFraction() >= outerKeyNullFraction - 0.01);
    }

    /**
     * LEFT OUTER JOIN with left=1M, right=950K, actual null frac on join key=5%, but
     * estimated null frac=15%. The null fraction for inner-side columns should reflect only the row-count gap (~5%) since
     * selectivity estimation should be preferred.
     */
    @ParameterizedTest
    @EnumSource(value = OuterJoin.class)
    public void testOuterJoinKnownStatsUsesSelectivityNotNullKeyFraction(OuterJoin outerJoin) {
        // GIVEN
        final var outerKey = columnRefFactory.create("outer_key", IntegerType.BIGINT, true);
        final var innerKey = columnRefFactory.create("inner_key", IntegerType.BIGINT, true);
        final var innerVal = columnRefFactory.create("inner_val", IntegerType.BIGINT, true);

        // Outer side: 1M rows, join key has 15% null fraction (stale/overestimated)
        final double staleOuterKeyNullFraction = 0.15;
        final var outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(1_000_000);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerKey, new ColumnStatistic(1, 1_000_000, staleOuterKeyNullFraction, 8, 800_000)));
        final var outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(outerKey))));

        // Inner side: 950K rows, known stats on join key
        final var innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(950_000);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, new ColumnStatistic(1, 1_000_000, 0.0, 8, 800_000),
                innerVal, new ColumnStatistic(0, 100, 0.02, 8, 50)));
        final var innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(innerKey, innerVal))));

        JoinOperator joinType;
        BinaryPredicateOperator joinPred;
        GroupExpression groupExpr;
        if (outerJoin == OuterJoin.LEFT_OUTER_JOIN) {
            joinType = JoinOperator.LEFT_OUTER_JOIN;
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey);
            groupExpr = new GroupExpression(
                    new LogicalJoinOperator(joinType, joinPred), Lists.newArrayList(outerGroup, innerGroup));
        } else {
            joinType = JoinOperator.RIGHT_OUTER_JOIN;
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, innerKey, outerKey);
            groupExpr = new GroupExpression(
                    new LogicalJoinOperator(joinType, joinPred), Lists.newArrayList(innerGroup, outerGroup));
        }
        groupExpr.setGroup(new Group(2));

        final var ctx = new ExpressionContext(groupExpr);

        // WHEN
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        final var valStat = ctx.getStatistics().getColumnStatistic(innerVal);

        // THEN — null fraction should be modest (driven by selectivity, not by the 15% null fraction)
        Assertions.assertTrue(valStat.getNullsFraction() < staleOuterKeyNullFraction);
    }

    /**
     * Selectivity path: When outer has MORE rows than inner and all stats are known,
     * the inner-side null fraction should reflect the unmatched-row gap.
     */
    @Test
    public void testLeftJoinKnownStatsWithUnmatchedRows() {
        // GIVEN
        final var outerKey = columnRefFactory.create("outer_key", IntegerType.BIGINT, true);
        final var innerKey = columnRefFactory.create("inner_key", IntegerType.BIGINT, true);
        final var innerVal = columnRefFactory.create("inner_val", IntegerType.BIGINT, true);

        final var outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(1000);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerKey, new ColumnStatistic(1, 1000, 0, 8, 500)));
        final var outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(outerKey))));

        final var innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(200);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, new ColumnStatistic(1, 500, 0, 8, 200),
                innerVal, new ColumnStatistic(0, 100, 0.0, 8, 50)));
        final var innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(innerKey, innerVal))));

        final var joinPred = new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey);
        final var joinOp = new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, joinPred);
        final var groupExpr = new GroupExpression(joinOp, Lists.newArrayList(outerGroup, innerGroup));
        groupExpr.setGroup(new Group(2));

        final var ctx = new ExpressionContext(groupExpr);

        // WHEN
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        final var valStat = ctx.getStatistics().getColumnStatistic(innerVal);

        // THEN — inner-side null fraction should be positive (some outer rows unmatched)
        Assertions.assertTrue(valStat.getNullsFraction() > 0,
                "Inner-side column should have non-zero null fraction due to unmatched outer rows");
    }

    /**
     * Unknown stats fallback path: When inner-side join key has UNKNOWN stats,
     * innerRowCount defaults to max(left, right), making selectivity-based null count = 0.
     * The fallback should use the outer-side key null fraction instead.
     */
    @ParameterizedTest
    @EnumSource(value = OuterJoin.class)
    public void testOuterJoinUnknownStatsUsesNullKeyFallback(OuterJoin outerJoin) {
        // GIVEN
        final var outerKey = columnRefFactory.create("outer_key", IntegerType.BIGINT, true);
        final var outerCol = columnRefFactory.create("outer_col", IntegerType.BIGINT, true);
        final var innerKey = columnRefFactory.create("inner_key", IntegerType.BIGINT, true);
        final var innerVal = columnRefFactory.create("inner_val", IntegerType.BIGINT, true);

        // Outer side (preserved): 1000 rows, high null fraction on join key
        final double outerKeyNullFraction = 0.90;
        final var outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(1000);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerKey, new ColumnStatistic(1, 1000, outerKeyNullFraction, 8, 500),
                outerCol, new ColumnStatistic(0, 100, 0, 8, 100)));
        final var outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(
                new ColumnRefSet(Lists.newArrayList(outerKey, outerCol))));

        // Inner side: 500 rows, UNKNOWN stats on the join key
        final var innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(500);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, ColumnStatistic.unknown(),
                innerVal, new ColumnStatistic(0, 100, 0.001, 8, 50)));
        final var innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(
                new ColumnRefSet(Lists.newArrayList(innerKey, innerVal))));

        JoinOperator joinType;
        BinaryPredicateOperator joinPred;
        GroupExpression groupExpr;
        if (outerJoin == OuterJoin.LEFT_OUTER_JOIN) {
            joinType = JoinOperator.LEFT_OUTER_JOIN;
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey);
            groupExpr = new GroupExpression(
                    new LogicalJoinOperator(joinType, joinPred), Lists.newArrayList(outerGroup, innerGroup));
        } else {
            joinType = JoinOperator.RIGHT_OUTER_JOIN;
            joinPred = new BinaryPredicateOperator(BinaryType.EQ, innerKey, outerKey);
            groupExpr = new GroupExpression(
                    new LogicalJoinOperator(joinType, joinPred), Lists.newArrayList(innerGroup, outerGroup));
        }
        groupExpr.setGroup(new Group(2));

        final var ctx = new ExpressionContext(groupExpr);

        // WHEN
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        final var innerValStat = ctx.getStatistics().getColumnStatistic(innerVal);

        // THEN — inner-side null fraction should reflect the outer key's null fraction
        Assertions.assertTrue(innerValStat.getNullsFraction() >= outerKeyNullFraction - 0.01,
                "With unknown stats, inner-side null fraction (" + innerValStat.getNullsFraction()
                        + ") should reflect outer key null fraction (" + outerKeyNullFraction
                        + ") for " + outerJoin);
    }

    /**
     * Statistics borrowed for an unknown key only price the join: the key of an outer join's preserved side keeps its
     * unknown statistics in the output, because unmatched rows keep values outside the lender's domain. Here l.k is
     * all 100 (unknown) and r.k is all 1 (NDV 1); the row count equals the full-statistics estimate L * R / 1.
     */
    @Test
    public void testOuterJoinPreservedKeyDoesNotKeepBorrowedStatistics() {
        final var lk = columnRefFactory.create("lk", IntegerType.BIGINT, true);
        final var rk = columnRefFactory.create("rk", IntegerType.BIGINT, true);
        final var unknownGroup = new Group(0);
        unknownGroup.setStatistics(Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistics(ImmutableMap.of(lk, ColumnStatistic.unknown())).build());
        unknownGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(lk))));
        final var knownGroup = new Group(1);
        knownGroup.setStatistics(Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistics(ImmutableMap.of(rk, new ColumnStatistic(1, 1, 0, 8, 1))).build());
        knownGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(rk))));

        // l LEFT JOIN r and r RIGHT JOIN l: l.k is preserved and stays unknown
        for (boolean unknownOnLeft : new boolean[] {true, false}) {
            JoinOperator joinType = unknownOnLeft ? JoinOperator.LEFT_OUTER_JOIN : JoinOperator.RIGHT_OUTER_JOIN;
            var groupExpr = new GroupExpression(new LogicalJoinOperator(joinType,
                    new BinaryPredicateOperator(BinaryType.EQ, lk, rk)), unknownOnLeft ?
                    Lists.newArrayList(unknownGroup, knownGroup) : Lists.newArrayList(knownGroup, unknownGroup));
            groupExpr.setGroup(new Group(2));
            var ctx = new ExpressionContext(groupExpr);
            new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
            Assertions.assertEquals(1000000, ctx.getStatistics().getOutputRowCount(), 1, joinType.toString());
            Assertions.assertTrue(ctx.getStatistics().getColumnStatistic(lk).isUnknown(), joinType.toString());
        }

        // inner join: every output l.k matches an r.k, so the borrowed statistics stay
        var groupExpr = new GroupExpression(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, lk, rk)), Lists.newArrayList(unknownGroup, knownGroup));
        groupExpr.setGroup(new Group(2));
        var ctx = new ExpressionContext(groupExpr);
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        ColumnStatistic innerKey = ctx.getStatistics().getColumnStatistic(lk);
        Assertions.assertFalse(innerKey.isUnknown());
        Assertions.assertEquals(1, innerKey.getMaxValue(), 0);
        Assertions.assertEquals(1, innerKey.getDistinctValuesCount(), 0);
    }

    /**
     * An equi-join key unknown on one side only borrows the known side's statistics, so a many-to-many join is
     * estimated as L * R / ndv(known) instead of max(L, R). Keys unknown on both sides keep the old fallback.
     */
    @Test
    public void testInnerJoinKeyUnknownOnOneSideBorrowsKnownSide() {
        final var leftKey = columnRefFactory.create("left_key", IntegerType.BIGINT, true);
        final var rightKey = columnRefFactory.create("right_key", IntegerType.BIGINT, true);
        final var rightKey2 = columnRefFactory.create("right_key2", IntegerType.BIGINT, true);

        final var leftGroup = new Group(0);
        leftGroup.setStatistics(Statistics.builder().setOutputRowCount(10000)
                .addColumnStatistics(ImmutableMap.of(leftKey, ColumnStatistic.unknown())).build());
        leftGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(leftKey))));

        final var rightGroup = new Group(1);
        rightGroup.setStatistics(Statistics.builder().setOutputRowCount(5000)
                .addColumnStatistics(ImmutableMap.of(
                        rightKey, new ColumnStatistic(1, 100, 0, 8, 100),
                        rightKey2, ColumnStatistic.unknown())).build());
        rightGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(rightKey, rightKey2))));

        // one side known: 10000 * 5000 / 100
        var groupExpr = new GroupExpression(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey)), Lists.newArrayList(leftGroup, rightGroup));
        groupExpr.setGroup(new Group(2));
        var ctx = new ExpressionContext(groupExpr);
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        Assertions.assertEquals(500000, ctx.getStatistics().getOutputRowCount(), 1);
        Assertions.assertFalse(ctx.getStatistics().getColumnStatistic(leftKey).isUnknown());

        // both sides unknown: max(L, R)
        groupExpr = new GroupExpression(new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey2)), Lists.newArrayList(leftGroup, rightGroup));
        groupExpr.setGroup(new Group(2));
        ctx = new ExpressionContext(groupExpr);
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        Assertions.assertEquals(10000, ctx.getStatistics().getOutputRowCount(), 1);
    }

    private Statistics estimateJoin(JoinOperator joinType, ScalarOperator onPredicate, Group leftGroup,
                                    Group rightGroup) {
        GroupExpression groupExpr = new GroupExpression(new LogicalJoinOperator(joinType, onPredicate),
                Lists.newArrayList(leftGroup, rightGroup));
        groupExpr.setGroup(new Group(2));
        ExpressionContext ctx = new ExpressionContext(groupExpr);
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        return ctx.getStatistics();
    }

    private static Group joinChild(int id, double rowCount, Map<ColumnRefOperator, ColumnStatistic> columns) {
        Group group = new Group(id);
        group.setStatistics(Statistics.builder().setOutputRowCount(rowCount).addColumnStatistics(columns).build());
        group.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(columns.keySet()))));
        return group;
    }

    /**
     * The unknown side's row count may itself be a guess (e.g. a dimension filtered on an unknown column with the
     * default selectivity). Borrowing the known side's NDV must then not push the estimate below the unknown-stats
     * fallback max(L, R): 1e6 * 1e4 / 1e5 = 1e5 would be lower, so the estimate stays at 1e6.
     */
    @Test
    public void testBorrowedJoinEstimateNeverBelowUnknownFallback() {
        final var factKey = columnRefFactory.create("fact_key", IntegerType.BIGINT, true);
        final var dimKey = columnRefFactory.create("dim_key", IntegerType.BIGINT, true);
        Group fact = joinChild(0, 1_000_000, ImmutableMap.of(factKey, new ColumnStatistic(1, 100000, 0, 8, 100000)));
        Group dim = joinChild(1, 10_000, ImmutableMap.of(dimKey, ColumnStatistic.unknown()));

        Statistics stats = estimateJoin(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, factKey, dimKey), fact, dim);
        Assertions.assertEquals(1_000_000, stats.getOutputRowCount(), 1);
        // the column statistics are still the borrowed ones
        Assertions.assertFalse(stats.getColumnStatistic(dimKey).isUnknown());

        // dimension on the left side, same answer
        stats = estimateJoin(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, dimKey, factKey), dim, fact);
        Assertions.assertEquals(1_000_000, stats.getOutputRowCount(), 1);
    }

    /**
     * One unknown column equated to two known columns borrows the smaller NDV (the larger estimate) whatever the
     * predicate order. With NDV 100 borrowed, the driving predicate is the one against NDV 1000:
     * 10000 * 5000 / 1000 = 50000, damped by 0.9 for the second predicate = 45000.
     */
    @Test
    public void testBorrowFromSeveralKnownColumnsIsOrderIndependent() {
        final var leftKey = columnRefFactory.create("left_key", IntegerType.BIGINT, true);
        final var rightNdv100 = columnRefFactory.create("right_ndv100", IntegerType.BIGINT, true);
        final var rightNdv1000 = columnRefFactory.create("right_ndv1000", IntegerType.BIGINT, true);
        Group left = joinChild(0, 10000, ImmutableMap.of(leftKey, ColumnStatistic.unknown()));
        Group right = joinChild(1, 5000, ImmutableMap.of(
                rightNdv100, new ColumnStatistic(1, 100, 0, 8, 100),
                rightNdv1000, new ColumnStatistic(1, 1000, 0, 8, 1000)));

        BinaryPredicateOperator eq100 = new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightNdv100);
        BinaryPredicateOperator eq1000 = new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightNdv1000);
        Statistics first = estimateJoin(JoinOperator.INNER_JOIN,
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, eq100, eq1000), left, right);
        Statistics second = estimateJoin(JoinOperator.INNER_JOIN,
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, eq1000, eq100), left, right);
        Assertions.assertEquals(first.getOutputRowCount(), second.getOutputRowCount(), 1e-6);
        Assertions.assertEquals(first.getColumnStatistic(leftKey).getDistinctValuesCount(),
                second.getColumnStatistic(leftKey).getDistinctValuesCount(), 1e-6);
        Assertions.assertEquals(45000, first.getOutputRowCount(), 1);
        Assertions.assertEquals(100, first.getColumnStatistic(leftKey).getDistinctValuesCount(), 0);
    }

    /**
     * LEFT JOIN whose left (preserved) key is unknown: the inner part is L * R / ndv(known) = 500000, the output
     * keeps it since it is above L. Every left row is expected to match and the left key's null fraction is unknown,
     * so the right side's columns get no null fraction.
     */
    @Test
    public void testLeftJoinUnknownLeftKeyBorrowsKnownSide() {
        final var leftKey = columnRefFactory.create("left_key", IntegerType.BIGINT, true);
        final var rightKey = columnRefFactory.create("right_key", IntegerType.BIGINT, true);
        final var rightVal = columnRefFactory.create("right_val", IntegerType.BIGINT, true);
        Group left = joinChild(0, 10000, ImmutableMap.of(leftKey, ColumnStatistic.unknown()));
        Group right = joinChild(1, 5000, ImmutableMap.of(
                rightKey, new ColumnStatistic(1, 100, 0, 8, 100),
                rightVal, new ColumnStatistic(1, 5000, 0, 8, 5000)));

        Statistics stats = estimateJoin(JoinOperator.LEFT_OUTER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey), left, right);
        Assertions.assertEquals(500000, stats.getOutputRowCount(), 1);
        Assertions.assertEquals(0, stats.getColumnStatistic(rightVal).getNullsFraction(), 0);
    }

    /**
     * When the UK/FK estimate wins, it must still carry the statistics the unknown FK key borrowed, or the joins above
     * see the key as unknown again. fact.fk (unknown, 10000 rows) = dim.uk (NDV 100, 1000 rows): the NDV estimate is
     * 10000 * 1000 / 100 = 100000, the UK/FK estimate 10000 wins, and fact.fk keeps the borrowed NDV 100.
     */
    @Test
    public void testUKFKEstimateKeepsBorrowedKeyStatistics() {
        final var fk = columnRefFactory.create("fk", IntegerType.BIGINT, true);
        final var uk = columnRefFactory.create("uk", IntegerType.BIGINT, true);
        OptExpression fact = new OptExpression(new LogicalValuesOperator(List.of(fk)));
        fact.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(fk))));
        fact.setStatistics(Statistics.builder().setOutputRowCount(10000)
                .addColumnStatistics(ImmutableMap.of(fk, ColumnStatistic.unknown())).build());
        OptExpression dim = new OptExpression(new LogicalValuesOperator(List.of(uk)));
        dim.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(uk))));
        dim.setStatistics(Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistics(ImmutableMap.of(uk, new ColumnStatistic(1, 100, 0, 8, 100))).build());
        BinaryPredicateOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, fk, uk);
        OptExpression join = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, predicate), fact, dim);
        UKFKConstraints constraints = new UKFKConstraints();
        constraints.setJoinProperty(new UKFKConstraints.JoinProperty(predicate, null, null, uk, fk, false));
        join.setConstraints(constraints);
        new MockUp<UKFKConstraintsCollector>() {
            @Mock
            public void collectColumnConstraints(OptExpression root) {
            }
        };

        boolean ukfk = connectContext.getSessionVariable().isEnableUKFKOpt();
        connectContext.getSessionVariable().setEnableUKFKOpt(true);
        try {
            ExpressionContext ctx = new ExpressionContext(join);
            new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
            Assertions.assertEquals(10000, ctx.getStatistics().getOutputRowCount(), 1);
            ColumnStatistic fkStat = ctx.getStatistics().getColumnStatistic(fk);
            Assertions.assertFalse(fkStat.isUnknown());
            Assertions.assertEquals(100, fkStat.getDistinctValuesCount(), 0);
        } finally {
            connectContext.getSessionVariable().setEnableUKFKOpt(ukfk);
        }
    }

    /**
     * Two keys, one known on both sides (ndv 50) and one unknown on the left (borrows ndv 100 from the right): the
     * borrowed key now takes part in the estimate instead of the whole join falling back to max(L, R) = 10000. The
     * borrowed key drives: 10000 * 5000 / 100 = 500000, damped by 0.9 for the second key = 450000.
     */
    @Test
    public void testTwoKeysOneKnownOneUnknown() {
        final var leftUnknown = columnRefFactory.create("left_unknown", IntegerType.BIGINT, true);
        final var leftKnown = columnRefFactory.create("left_known", IntegerType.BIGINT, true);
        final var rightA = columnRefFactory.create("right_a", IntegerType.BIGINT, true);
        final var rightB = columnRefFactory.create("right_b", IntegerType.BIGINT, true);
        Group left = joinChild(0, 10000, ImmutableMap.of(
                leftUnknown, ColumnStatistic.unknown(),
                leftKnown, new ColumnStatistic(1, 50, 0, 8, 50)));
        Group right = joinChild(1, 5000, ImmutableMap.of(
                rightA, new ColumnStatistic(1, 100, 0, 8, 100),
                rightB, new ColumnStatistic(1, 50, 0, 8, 50)));

        Statistics stats = estimateJoin(JoinOperator.INNER_JOIN,
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                        new BinaryPredicateOperator(BinaryType.EQ, leftUnknown, rightA),
                        new BinaryPredicateOperator(BinaryType.EQ, leftKnown, rightB)), left, right);
        Assertions.assertEquals(450000, stats.getOutputRowCount(), 1);
        Assertions.assertEquals(100, stats.getColumnStatistic(leftUnknown).getDistinctValuesCount(), 0);
    }

    /**
     * Unknown stats fallback path: FULL OUTER JOIN where one side has UNKNOWN key stats.
     * Both sides' non-join columns should get elevated null fractions.
     */
    @Test
    public void testFullOuterJoinUnknownStatsUsesNullKeyFallback() {
        // GIVEN
        final var leftKey = columnRefFactory.create("left_key", IntegerType.BIGINT, true);
        final var leftVal = columnRefFactory.create("left_val", IntegerType.BIGINT, true);
        final var rightKey = columnRefFactory.create("right_key", IntegerType.BIGINT, true);
        final var rightVal = columnRefFactory.create("right_val", IntegerType.BIGINT, true);

        // Left: 1000 rows, 80% null fraction on key
        final double leftKeyNullFraction = 0.80;
        final var leftBuilder = Statistics.builder();
        leftBuilder.setOutputRowCount(1000);
        leftBuilder.addColumnStatistics(ImmutableMap.of(
                leftKey, new ColumnStatistic(1, 1000, leftKeyNullFraction, 8, 200),
                leftVal, new ColumnStatistic(0, 100, 0.01, 8, 50)));
        final var leftGroup = new Group(0);
        leftGroup.setStatistics(leftBuilder.build());
        leftGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(leftKey, leftVal))));

        // Right: 500 rows, UNKNOWN stats on key
        final var rightBuilder = Statistics.builder();
        rightBuilder.setOutputRowCount(500);
        rightBuilder.addColumnStatistics(ImmutableMap.of(
                rightKey, ColumnStatistic.unknown(),
                rightVal, new ColumnStatistic(0, 100, 0.02, 8, 30)));
        final var rightGroup = new Group(1);
        rightGroup.setStatistics(rightBuilder.build());
        rightGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(rightKey, rightVal))));

        final var joinPred = new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey);
        final var joinOp = new LogicalJoinOperator(JoinOperator.FULL_OUTER_JOIN, joinPred);
        final var groupExpr = new GroupExpression(joinOp, Lists.newArrayList(leftGroup, rightGroup));
        groupExpr.setGroup(new Group(2));

        final var ctx = new ExpressionContext(groupExpr);

        // WHEN
        new StatisticsCalculator(ctx, columnRefFactory, optimizerContext).estimatorStats();
        final var rightValStat = ctx.getStatistics().getColumnStatistic(rightVal);

        // THEN — right side is nullable from left's perspective, and left key has 80% nulls
        // With unknown stats fallback, the right-side null fraction should reflect the left key's null fraction
        Assertions.assertTrue(rightValStat.getNullsFraction() >= leftKeyNullFraction - 0.05);
    }

    private static File newFolder(File root, String... subDirs) throws IOException {
        String subFolder = String.join("/", subDirs);
        File result = new File(root, subFolder);
        if (!result.mkdirs()) {
            throw new IOException("Couldn't create folders " + root);
        }
        return result;
    }

    @Test
    public void testSkipPredicateColumnsCollectionScopeIsNestableAndRestores() {
        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

        try (var scope1 = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
            Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

            try (var scope2 = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
                Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
            }

            Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
        }

        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
    }

    @Test
    public void testSkipPredicateColumnsCollectionScopeDoesNotLeakAcrossThreads() throws Exception {
        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<Throwable> err = new AtomicReference<>();

        Thread t = new Thread(() -> {
            try (var scope = StatisticsCalculator.skipPredicateColumnsCollectionScope()) {
                entered.countDown();
                Assertions.assertTrue(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());
            } catch (Throwable e) {
                err.set(e);
            } finally {
                done.countDown();
            }
        });
        t.start();

        Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
        Assertions.assertFalse(StatisticsCalculator.isInSkipPredicateColumnsCollectionScope());

        Assertions.assertTrue(done.await(5, TimeUnit.SECONDS));
        if (err.get() != null) {
            throw new AssertionError(err.get());
        }
    }

    @Test
    public void testComputeEsScanNode(@Mocked EsTable esTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalEsScanOperator scanOperator =
                new LogicalEsScanOperator(esTable, refToColumn, columnToRef, -1, null, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return Statistics.builder().setOutputRowCount(999_000).build();
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(999_000D, expressionContext.getStatistics().getOutputRowCount(), 0.01);
    }

    @Test
    public void testComputeKuduScanNode(@Mocked KuduTable kuduTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalKuduScanOperator scanOperator =
                new LogicalKuduScanOperator(kuduTable, refToColumn, columnToRef, -1, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return Statistics.builder().setOutputRowCount(888_000).build();
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(888_000D, expressionContext.getStatistics().getOutputRowCount(), 0.01);
    }

    @Test
    public void testExternalScanPreservesConnectorForegroundSource(@Mocked KuduTable kuduTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalKuduScanOperator scanOperator =
                new LogicalKuduScanOperator(kuduTable, refToColumn, columnToRef, -1, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return Statistics.builder().setOutputRowCount(100)
                        .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA,
                expressionContext.getStatistics().getStatsSource());
    }

    @Test
    public void testExternalScanDefaultUnknown(@Mocked KuduTable kuduTable) {
        Map<ColumnRefOperator, Column> refToColumn = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        LogicalKuduScanOperator scanOperator =
                new LogicalKuduScanOperator(kuduTable, refToColumn, columnToRef, -1, null);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        // getTableStatistics returns null, triggering catch -> computeNormalExternalTableScanNode with UNKNOWN
        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatistics(OptimizerContext session, String catalogName, Table table,
                                                 Map<ColumnRefOperator, Column> columns, List<PartitionKey> partitionKeys,
                                                 ScalarOperator predicate) {
                return null;
            }
        };
        new MockUp<StatisticsCalcUtils>() {
            @Mock
            public Statistics.Builder estimateScanColumns(Table table,
                                                          Map<ColumnRefOperator, Column> colRefToColumnMetaMap,
                                                          OptimizerContext ctx) {
                return Statistics.builder();
            }
        };

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(Statistics.StatsSource.NONE,
                expressionContext.getStatistics().getStatsSource());
    }

    @Test
    public void testStatsSourceDefaults() {
        // Default Builder produces UNKNOWN
        Statistics stats = Statistics.builder().setOutputRowCount(100).build();
        Assertions.assertEquals(Statistics.StatsSource.NONE, stats.getStatsSource());

        // Explicit set works
        Statistics foreground = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, foreground.getStatsSource());

        Statistics background = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.ANALYZE).build();
        Assertions.assertEquals(Statistics.StatsSource.ANALYZE, background.getStatsSource());
    }

    @Test
    public void testStatsSourceBuildFromPreservesSource() {
        Statistics original = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.TABLE_METADATA).build();
        Statistics rebuilt = Statistics.buildFrom(original).setOutputRowCount(200).build();
        Assertions.assertEquals(Statistics.StatsSource.TABLE_METADATA, rebuilt.getStatsSource());
        Assertions.assertEquals(200, rebuilt.getOutputRowCount(), 0.001);
    }

    @Test
    public void testStatsSourceWithOutputRowCountPreservesSource() {
        Statistics original = Statistics.builder().setOutputRowCount(100)
                .setStatsSource(Statistics.StatsSource.ANALYZE).build();
        Statistics adjusted = original.withOutputRowCount(200);
        Assertions.assertEquals(Statistics.StatsSource.ANALYZE, adjusted.getStatsSource());
        Assertions.assertEquals(200, adjusted.getOutputRowCount(), 0.001);
    }

    @Test
    public void testOlapScanGetsBackground() {
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getDb("statistics_test")
                .getTable("test_all_type");
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = Maps.newHashMap();
        Map<Column, ColumnRefOperator> columnToRef = Maps.newHashMap();
        for (Column col : table.getBaseSchema()) {
            ColumnRefOperator ref = columnRefFactory.create(col.getName(), col.getType(), col.isAllowNull());
            colRefToColumnMetaMap.put(ref, col);
            columnToRef.put(col, ref);
        }
        List<Long> partitionIds = table.getPartitions().stream()
                .mapToLong(Partition::getId).boxed().collect(Collectors.toList());

        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(table, colRefToColumnMetaMap,
                columnToRef, null, -1, null, table.getBaseIndexMetaId(),
                partitionIds, null, false, Lists.newArrayList(),
                Lists.newArrayList(), Lists.newArrayList(), false);

        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        StatisticsCalculator calculator = new StatisticsCalculator(expressionContext,
                columnRefFactory, optimizerContext);
        calculator.estimatorStats();
        Assertions.assertEquals(Statistics.StatsSource.ANALYZE, expressionContext.getStatistics().getStatsSource());
    }

    @Test
    public void testUnpartitionedRowNumberStatistics() {
        ColumnRefOperator pk = columnRefFactory.create("pk", IntegerType.BIGINT, false);
        ColumnRefOperator rn = columnRefFactory.create("rn", IntegerType.BIGINT, false);
        CallOperator rowNumber = new CallOperator(FunctionSet.ROW_NUMBER, IntegerType.BIGINT, Lists.newArrayList());

        Statistics.Builder childStats = Statistics.builder();
        childStats.setOutputRowCount(1000);
        childStats.addColumnStatistic(pk, ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100).setDistinctValuesCount(100).setNullsFraction(0).build());
        Group childGroup = new Group(0);
        childGroup.setStatistics(childStats.build());

        LogicalWindowOperator windowOperator = new LogicalWindowOperator.Builder()
                .setWindowCall(ImmutableMap.of(rn, rowNumber))
                .build();
        GroupExpression groupExpression = new GroupExpression(windowOperator, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

        ColumnStatistic rowNumberStatistic = expressionContext.getStatistics().getColumnStatistic(rn);
        Assertions.assertFalse(rowNumberStatistic.isUnknown());
        Assertions.assertEquals(1, rowNumberStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(1000, rowNumberStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(1000, rowNumberStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, rowNumberStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testPartitionedRowNumberStatistics() {
        ColumnRefOperator pk = columnRefFactory.create("pk", IntegerType.BIGINT, false);
        ColumnRefOperator rn = columnRefFactory.create("rn", IntegerType.BIGINT, false);
        CallOperator rowNumber = new CallOperator(FunctionSet.ROW_NUMBER, IntegerType.BIGINT, Lists.newArrayList());

        Statistics.Builder childStats = Statistics.builder();
        childStats.setOutputRowCount(1000);
        childStats.addColumnStatistic(pk, ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100).setDistinctValuesCount(100).setNullsFraction(0).build());
        Group childGroup = new Group(0);
        childGroup.setStatistics(childStats.build());

        LogicalWindowOperator windowOperator = new LogicalWindowOperator.Builder()
                .setWindowCall(ImmutableMap.of(rn, rowNumber))
                .setPartitionExpressions(Lists.newArrayList(pk))
                .build();
        GroupExpression groupExpression = new GroupExpression(windowOperator, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

        ColumnStatistic rowNumberStatistic = expressionContext.getStatistics().getColumnStatistic(rn);
        Assertions.assertFalse(rowNumberStatistic.isUnknown());
        Assertions.assertEquals(1, rowNumberStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(10, rowNumberStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(10, rowNumberStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, rowNumberStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testPartitionedRowNumberStatisticsWithDuplicateKeys() {
        ColumnRefOperator pk = columnRefFactory.create("pk", IntegerType.BIGINT, false);
        ColumnRefOperator rn = columnRefFactory.create("rn", IntegerType.BIGINT, false);
        CallOperator rowNumber = new CallOperator(FunctionSet.ROW_NUMBER, IntegerType.BIGINT, Lists.newArrayList());

        Statistics.Builder childStats = Statistics.builder();
        childStats.setOutputRowCount(1000);
        childStats.addColumnStatistic(pk, ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100).setDistinctValuesCount(100).setNullsFraction(0).build());
        Group childGroup = new Group(0);
        childGroup.setStatistics(childStats.build());

        // Duplicate partition keys (PARTITION BY pk, pk) must not create extra partitions, so the estimate
        // should match PARTITION BY pk instead of multiplying the group count.
        LogicalWindowOperator windowOperator = new LogicalWindowOperator.Builder()
                .setWindowCall(ImmutableMap.of(rn, rowNumber))
                .setPartitionExpressions(Lists.newArrayList(pk, pk))
                .build();
        GroupExpression groupExpression = new GroupExpression(windowOperator, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

        ColumnStatistic rowNumberStatistic = expressionContext.getStatistics().getColumnStatistic(rn);
        Assertions.assertFalse(rowNumberStatistic.isUnknown());
        Assertions.assertEquals(1, rowNumberStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(10, rowNumberStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(10, rowNumberStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, rowNumberStatistic.getNullsFraction(), 0.001);
    }

    @Test
    public void testPartitionedRowNumberStatisticsWithUnknownPartitionStats() {
        ColumnRefOperator pk = columnRefFactory.create("pk", IntegerType.BIGINT, false);
        ColumnRefOperator rn = columnRefFactory.create("rn", IntegerType.BIGINT, false);
        CallOperator rowNumber = new CallOperator(FunctionSet.ROW_NUMBER, IntegerType.BIGINT, Lists.newArrayList());

        // No statistics are provided for the partition column pk, so its ColumnStatistic is unknown.
        Statistics.Builder childStats = Statistics.builder();
        childStats.setOutputRowCount(1000);
        childStats.addColumnStatistic(pk, ColumnStatistic.unknown());
        Group childGroup = new Group(0);
        childGroup.setStatistics(childStats.build());

        LogicalWindowOperator windowOperator = new LogicalWindowOperator.Builder()
                .setWindowCall(ImmutableMap.of(rn, rowNumber))
                .setPartitionExpressions(Lists.newArrayList(pk))
                .build();
        GroupExpression groupExpression = new GroupExpression(windowOperator, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

        // Without partition statistics, the partition size and row-number NDV cannot be estimated reliably.
        ColumnStatistic rowNumberStatistic = expressionContext.getStatistics().getColumnStatistic(rn);
        Assertions.assertTrue(rowNumberStatistic.isUnknown());
    }

    @Test
    public void testPartitionedRowNumberStatisticsWithComputedPartitionExpression() {
        ColumnRefOperator pk = columnRefFactory.create("pk", IntegerType.BIGINT, false);
        ColumnRefOperator rn = columnRefFactory.create("rn", IntegerType.BIGINT, false);
        CallOperator rowNumber = new CallOperator(FunctionSet.ROW_NUMBER, IntegerType.BIGINT, Lists.newArrayList());

        Statistics.Builder childStats = Statistics.builder();
        childStats.setOutputRowCount(1000);
        childStats.addColumnStatistic(pk, ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100).setDistinctValuesCount(100).setNullsFraction(0).build());
        Group childGroup = new Group(0);
        childGroup.setStatistics(childStats.build());

        LogicalWindowOperator windowOperator = new LogicalWindowOperator.Builder()
                .setWindowCall(ImmutableMap.of(rn, rowNumber))
                .setPartitionExpressions(Lists.newArrayList(new CastOperator(IntegerType.BIGINT, pk)))
                .build();
        GroupExpression groupExpression = new GroupExpression(windowOperator, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

        ColumnStatistic rowNumberStatistic = expressionContext.getStatistics().getColumnStatistic(rn);
        Assertions.assertTrue(rowNumberStatistic.isUnknown());
    }

    @Test
    public void testPartitionedRowNumberStatisticsWithCombinedStats() {
        ColumnRefOperator pk1 = columnRefFactory.create("pk1", IntegerType.BIGINT, false);
        ColumnRefOperator pk2 = columnRefFactory.create("pk2", IntegerType.BIGINT, false);
        ColumnRefOperator rn = columnRefFactory.create("rn", IntegerType.BIGINT, false);
        CallOperator rowNumber = new CallOperator(FunctionSet.ROW_NUMBER, IntegerType.BIGINT, Lists.newArrayList());

        Statistics.Builder childStats = Statistics.builder();
        childStats.setOutputRowCount(1000);
        childStats.addColumnStatistic(pk1, ColumnStatistic.unknown());
        childStats.addColumnStatistic(pk2, ColumnStatistic.unknown());
        childStats.addMultiColumnStatistics(
                ImmutableSet.of(pk1, pk2), new MultiColumnCombinedStats(100));
        Group childGroup = new Group(0);
        childGroup.setStatistics(childStats.build());

        LogicalWindowOperator windowOperator = new LogicalWindowOperator.Builder()
                .setWindowCall(ImmutableMap.of(rn, rowNumber))
                .setPartitionExpressions(Lists.newArrayList(pk1, pk2))
                .build();
        GroupExpression groupExpression = new GroupExpression(windowOperator, Lists.newArrayList(childGroup));
        groupExpression.setGroup(new Group(1));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);
        new StatisticsCalculator(expressionContext, columnRefFactory, optimizerContext).estimatorStats();

        ColumnStatistic rowNumberStatistic = expressionContext.getStatistics().getColumnStatistic(rn);
        Assertions.assertFalse(rowNumberStatistic.isUnknown());
        Assertions.assertEquals(1, rowNumberStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(10, rowNumberStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(10, rowNumberStatistic.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, rowNumberStatistic.getNullsFraction(), 0.001);
    }
}
