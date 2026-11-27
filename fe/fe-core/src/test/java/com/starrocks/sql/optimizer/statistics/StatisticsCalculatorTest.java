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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;
import com.starrocks.sql.optimizer.rule.transformation.DeriveRangeJoinPredicateRule;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

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
        ColumnRefOperator v1 = columnRefFactory.create("v1", Type.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", Type.INT, true);

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
        ColumnRefOperator v1 = columnRefFactory.create("v1", Type.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", Type.INT, true);
        // child 2 output column
        ColumnRefOperator v3 = columnRefFactory.create("v3", Type.INT, true);
        ColumnRefOperator v4 = columnRefFactory.create("v4", Type.INT, true);
        // union node output column
        ColumnRefOperator v5 = columnRefFactory.create("v3", Type.INT, true);
        ColumnRefOperator v6 = columnRefFactory.create("v4", Type.INT, true);
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
            partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(1000);
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
                        ((OlapTable) table).getBaseIndexId(),
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
                new ArrayList<>(List.of(new StructField("bucket_id", Type.INT))));
        Type arrayType = new ArrayType(structType);
        Column arrayColumn = new Column("bucket_array", arrayType, true);
        ColumnRefOperator arrayRef = columnRefFactory.create("bucket_array", arrayType, true);
        ColumnRefOperator outputRef = columnRefFactory.create("bucket_id", Type.INT, true);
        CollectionElementOperator arrayElement = new CollectionElementOperator(
                structType, arrayRef, ConstantOperator.createInt(1), false);
        SubfieldOperator subfield = new SubfieldOperator(
                arrayElement, Type.INT, List.of("bucket_id"));

        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(table,
                ImmutableMap.of(arrayRef, arrayColumn),
                ImmutableMap.of(arrayColumn, arrayRef),
                null, -1, null,
                table.getBaseIndexId(),
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
                new ArrayList<>(List.of(new StructField("bucket_id", Type.INT))));
        Column structColumn = new Column("bucket_struct", structType, true);
        ColumnRefOperator structRef = columnRefFactory.create("bucket_struct", structType, true);
        ColumnRefOperator outputRef = columnRefFactory.create("bucket_id", Type.INT, true);
        SubfieldOperator subfield = new SubfieldOperator(
                structRef, Type.INT, List.of("bucket_id"));

        LogicalOlapScanOperator scanOperator = new LogicalOlapScanOperator(table,
                ImmutableMap.of(structRef, structColumn),
                ImmutableMap.of(structColumn, structRef),
                null, -1, null,
                table.getBaseIndexId(),
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
                    columnToRef, -1, predicateOperator, TableVersionRange.empty());

        GroupExpression groupExpression = new GroupExpression(icebergScanOperator, Lists.newArrayList());
        groupExpression.setGroup(new Group(0));
        ExpressionContext expressionContext = new ExpressionContext(groupExpression);

        new MockUp<MetadataMgr>() {
            @Mock
            public Statistics getTableStatisticsFromInternalStatistics(Table table, Map<ColumnRefOperator,
                        Column> columns, boolean queryTriggerAnalyzeEnabled) {
                Statistics.Builder builder = Statistics.builder();
                icebergScanOperator.getOutputColumns().forEach(col ->
                            builder.addColumnStatistic(col,
                                        new ColumnStatistic(0, 100, 0.0, 5.0, 100))
                );
                builder.setOutputRowCount(100);
                return builder.build();
            }
        };
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(expressionContext,
                    columnRefFactory, optimizerContext);
        statisticsCalculator.estimatorStats();
        Assertions.assertEquals(50, expressionContext.getStatistics().getOutputRowCount(), 0.001);
        Assertions.assertEquals(50, expressionContext.getStatistics().
                    getColumnStatistic(partitionColumn).getMaxValue(), 0.001);
        Assertions.assertTrue(optimizerContext.isObtainedFromInternalStatistics());
        optimizerContext.setObtainedFromInternalStatistics(false);
    }

    @Test
    public void testLogicalOlapTableEmptyPartition() {
        ColumnRefOperator idDate = columnRefFactory.create("id_date", Type.DATE, true);
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
                                ImmutableMap.of(idDate, new Column("id_date", Type.DATE, true)),
                                ImmutableMap.of(new Column("id_date", Type.DATE, true), idDate),
                                null, -1, null,
                                ((OlapTable) table).getBaseIndexId(),
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
        ColumnRefOperator idDate = columnRefFactory.create("id_date", Type.DATE, true);

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
            partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(1000);
        }

        LogicalOlapScanOperator olapScanOperator =
                    new LogicalOlapScanOperator(table,
                                ImmutableMap.of(idDate, new Column("id_date", Type.DATE, true)),
                                ImmutableMap.of(new Column("id_date", Type.DATE, true), idDate),
                                null, -1,
                                new BinaryPredicateOperator(BinaryType.EQ,
                                            idDate, ConstantOperator.createDate(LocalDateTime.of(2013, 12, 30, 0, 0, 0))),
                                ((OlapTable) table).getBaseIndexId(),
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
                                ImmutableMap.of(idDate, new Column("id_date", Type.DATE, true)),
                                ImmutableMap.of(new Column("id_date", Type.DATE, true), idDate),
                                null, -1, null, ((OlapTable) table).getBaseIndexId(),
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
        ColumnRefOperator idDate = columnRefFactory.create("id_date", Type.DATE, true);

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
            partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(1000);
        }

        LogicalOlapScanOperator olapScanOperator =
                    new LogicalOlapScanOperator(table,
                                ImmutableMap.of(idDate, new Column("id_date", Type.DATE, true)),
                                ImmutableMap.of(new Column("id_date", Type.DATE, true), idDate), null, -1, null,
                                ((OlapTable) table).getBaseIndexId(),
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
                                ImmutableMap.of(idDate, new Column("id_date", Type.DATE, true)),
                                ImmutableMap.of(new Column("id_date", Type.DATE, true), idDate), null, -1, null,
                                ((OlapTable) table).getBaseIndexId(),
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
        ColumnRefOperator v1 = columnRefFactory.create("v1", Type.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", Type.INT, true);
        ColumnRefOperator v5 = columnRefFactory.create("v5", Type.INT, true);
        // child 2 output column
        ColumnRefOperator v3 = columnRefFactory.create("v3", Type.INT, true);
        ColumnRefOperator v4 = columnRefFactory.create("v4", Type.INT, true);
        ColumnRefOperator v6 = columnRefFactory.create("v6", Type.INT, true);
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
                                new CallOperator("add", Type.BIGINT, Lists.newArrayList(v3, v4)));
        BinaryPredicateOperator eqOnPredicateWithAdd2 =
                    new BinaryPredicateOperator(BinaryType.EQ, v2,
                                new CallOperator("add", Type.BIGINT, Lists.newArrayList(v3, v4)));
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
    public void testNotFoundColumnStatistics() {
        ColumnRefOperator v1 = columnRefFactory.create("v1", Type.INT, true);
        ColumnRefOperator v2 = columnRefFactory.create("v2", Type.INT, true);

        ColumnRefOperator v3 = columnRefFactory.create("v3", Type.INT, true);
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
        ColumnRefOperator outerJoinKey = columnRefFactory.create("nullable_col", Type.INT, true);
        ColumnRefOperator outerOtherCol = columnRefFactory.create("other_col", Type.INT, true);

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

        ColumnRefOperator innerKey = columnRefFactory.create("dim_id", Type.INT, true);
        ColumnRefOperator innerVal = columnRefFactory.create("dim_val", Type.INT, true);

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
    public void testOuterJoinKeepsPreservedKeyDomain(OuterJoin outerJoin) {
        // GIVEN
        // A big preserved side whose key spans a wide range, joined to a tiny side with 32 distinct keys,
        // like `member LEFT JOIN confirmation ON member.contact_id = confirmation.contact_id WHERE confirmation.x IS NULL`.
        ColumnRefOperator outerKey = columnRefFactory.create("contact_id", Type.BIGINT, true);
        Statistics.Builder outerBuilder = Statistics.builder();
        outerBuilder.setOutputRowCount(106_421_169);
        outerBuilder.addColumnStatistics(ImmutableMap.of(
                outerKey, new ColumnStatistic(0, 771_126_758, 0.007, 8, 106_421_169)));
        Group outerGroup = new Group(0);
        outerGroup.setStatistics(outerBuilder.build());
        outerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(outerKey))));

        ColumnRefOperator innerKey = columnRefFactory.create("employment_id", Type.BIGINT, true);
        Statistics.Builder innerBuilder = Statistics.builder();
        innerBuilder.setOutputRowCount(38);
        innerBuilder.addColumnStatistics(ImmutableMap.of(
                innerKey, new ColumnStatistic(2238, 747_550_773, 0, 8, 32)));
        Group innerGroup = new Group(1);
        innerGroup.setStatistics(innerBuilder.build());
        innerGroup.setLogicalProperty(new LogicalProperty(new ColumnRefSet(Lists.newArrayList(innerKey))));

        boolean left = outerJoin == OuterJoin.LEFT_OUTER_JOIN;
        LogicalJoinOperator joinOp = new LogicalJoinOperator(
                left ? JoinOperator.LEFT_OUTER_JOIN : JoinOperator.RIGHT_OUTER_JOIN,
                left ? new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey)
                        : new BinaryPredicateOperator(BinaryType.EQ, innerKey, outerKey));
        GroupExpression groupExpr = new GroupExpression(joinOp,
                left ? Lists.newArrayList(outerGroup, innerGroup) : Lists.newArrayList(innerGroup, outerGroup));
        groupExpr.setGroup(new Group(2));
        ExpressionContext exprCtx = new ExpressionContext(groupExpr);
        StatisticsCalculator calc = new StatisticsCalculator(exprCtx, columnRefFactory, optimizerContext);

        // WHEN
        calc.estimatorStats();
        ColumnStatistic outerKeyAfterJoin = exprCtx.getStatistics().getColumnStatistic(outerKey);

        // THEN
        // Every preserved row survives, so the preserved key keeps its own range and ndv instead of the
        // 32 values of the other side; a GROUP BY on it must not be estimated at a few dozen rows.
        Assertions.assertEquals(106_421_169, outerKeyAfterJoin.getDistinctValuesCount(), 1);
        Assertions.assertEquals(0, outerKeyAfterJoin.getMinValue(), 0.001);
        Assertions.assertEquals(771_126_758, outerKeyAfterJoin.getMaxValue(), 0.001);
        Assertions.assertEquals(0.007, outerKeyAfterJoin.getNullsFraction(), 0.001);
    }

    @ParameterizedTest
    @EnumSource(value = OuterJoin.class)
    public void testOuterJoinRepeatedMatchesKeepPreservedKeyDomain(OuterJoin outerJoin) {
        ColumnRefOperator outerKey = columnRefFactory.create("outer_key", Type.BIGINT, true);
        ColumnRefOperator innerKey = columnRefFactory.create("inner_key", Type.BIGINT, true);
        Group outer = joinChild(0, 1000, ImmutableMap.of(outerKey,
                new ColumnStatistic(0, 1000, 0.1, 8, 800)));
        Group inner = joinChild(1, 100_000, ImmutableMap.of(innerKey,
                new ColumnStatistic(100, 200, 0, 8, 100)));
        boolean left = outerJoin == OuterJoin.LEFT_OUTER_JOIN;
        Statistics joined = estimateJoin(left ? JoinOperator.LEFT_OUTER_JOIN : JoinOperator.RIGHT_OUTER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey),
                left ? outer : inner, left ? inner : outer);

        Assertions.assertTrue(joined.getOutputRowCount() > 1000);
        ColumnStatistic key = joined.getColumnStatistic(outerKey);
        Assertions.assertEquals(800, key.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, key.getMinValue(), 0.001);
        Assertions.assertEquals(1000, key.getMaxValue(), 0.001);
        // Preserve main's existing scaling when matches multiply the output rows.
        Assertions.assertEquals(100.0 / joined.getOutputRowCount(), key.getNullsFraction(), 0.000001);
    }

    @ParameterizedTest
    @EnumSource(value = JoinOperator.class, names = {"LEFT_OUTER_JOIN", "RIGHT_OUTER_JOIN", "FULL_OUTER_JOIN"})
    public void testOuterJoinPreservesStringBoundsForRangeDerivation(JoinOperator joinType) {
        ColumnRefOperator leftKey = columnRefFactory.create("left_key", Type.VARCHAR, true);
        ColumnRefOperator rightKey = columnRefFactory.create("right_key", Type.VARCHAR, true);
        ColumnStatistic leftStatistic = ColumnStatistic.builder().setMinString("a").setMaxString("z")
                .setDistinctValuesCount(1000).setNullsFraction(0.1).setAverageRowSize(8).build();
        ColumnStatistic rightStatistic = ColumnStatistic.builder().setMinString("m").setMaxString("t")
                .setDistinctValuesCount(100).setNullsFraction(0.2).setAverageRowSize(8).build();
        Group left = joinChild(0, 10_000, ImmutableMap.of(leftKey, leftStatistic));
        Group right = joinChild(1, 10_000, ImmutableMap.of(rightKey, rightStatistic));
        BinaryPredicateOperator equality = new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey);
        boolean correlatedEstimate = connectContext.getSessionVariable().isUseCorrelatedJoinEstimate();
        boolean deriveRange = connectContext.getSessionVariable().enableCboDeriveRangeJoinPredicate();
        try {
            connectContext.getSessionVariable().setUseCorrelatedJoinEstimate(true);
            connectContext.getSessionVariable().setCboDeriveRangeJoinPredicate(true);
            Statistics joined = estimateJoin(joinType, equality, left, right);
            // branch-4.0 lacks #73489, so a FULL OUTER JOIN keeps the older row-count and null-fraction estimate
            // (max(1, L + R - inner)); only the restored key bounds are checked for it.
            boolean checkRowCount = !joinType.isFullOuterJoin();
            // Repeated matches avoid input-statistic restoration by the nullable-side padding loop.
            if (checkRowCount) {
                Assertions.assertEquals(72_000, joined.getOutputRowCount(), 0.001);
            }
            for (ColumnRefOperator key : List.of(leftKey, rightKey)) {
                ColumnStatistic original = key == leftKey ? leftStatistic : rightStatistic;
                ColumnStatistic actual = joined.getColumnStatistic(key);
                boolean preserved = joinType.isFullOuterJoin() ||
                        (joinType.isLeftOuterJoin() ? key == leftKey : key == rightKey);
                Assertions.assertEquals(preserved ? original.getMinString() : null, actual.getMinString());
                Assertions.assertEquals(preserved ? original.getMaxString() : null, actual.getMaxString());
                if (checkRowCount) {
                    Assertions.assertEquals(preserved ? original.getNullsFraction() * 10_000 / 72_000 : 0,
                            actual.getNullsFraction(), 0.000001);
                }
            }
            Statistics innerJoined = estimateJoin(JoinOperator.INNER_JOIN, equality, left, right);
            for (ColumnRefOperator key : List.of(leftKey, rightKey)) {
                Assertions.assertNull(innerJoined.getColumnStatistic(key).getMinString());
                Assertions.assertNull(innerJoined.getColumnStatistic(key).getMaxString());
            }

            ColumnRefOperator preservedKey = joinType.isRightOuterJoin() ? rightKey : leftKey;
            ColumnStatistic original = preservedKey == leftKey ? leftStatistic : rightStatistic;
            OptExpression outer = OptExpression.create(new LogicalJoinOperator(joinType, equality),
                    OptExpression.create(new LogicalValuesOperator(List.of(leftKey))),
                    OptExpression.create(new LogicalValuesOperator(List.of(rightKey))));
            outer.setLogicalProperty(new LogicalProperty(new ColumnRefSet(List.of(leftKey, rightKey))));
            outer.setStatistics(joined);
            ColumnRefOperator lower = columnRefFactory.create("lower", Type.VARCHAR, true);
            ColumnRefOperator upper = columnRefFactory.create("upper", Type.VARCHAR, true);
            OptExpression bounds = OptExpression.create(new LogicalValuesOperator(List.of(lower, upper)));
            bounds.deriveLogicalPropertyItself();
            bounds.setStatistics(Statistics.builder().setOutputRowCount(1)
                    .addColumnStatistic(lower, ColumnStatistic.unknown())
                    .addColumnStatistic(upper, ColumnStatistic.unknown()).build());
            ScalarOperator range = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.GT, preservedKey, lower),
                    new BinaryPredicateOperator(BinaryType.LT, preservedKey, upper));
            OptExpression rangeJoin = OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, range),
                    outer, bounds);
            DeriveRangeJoinPredicateRule rule = new DeriveRangeJoinPredicateRule();
            Assertions.assertTrue(rule.check(rangeJoin, optimizerContext));
            List<OptExpression> rewritten = rule.transform(rangeJoin, optimizerContext);
            Assertions.assertEquals(1, rewritten.size());
            Assertions.assertEquals(Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.LE, lower,
                            new ConstantOperator(original.getMaxString(), Type.STRING)),
                    new BinaryPredicateOperator(BinaryType.GE, upper,
                            new ConstantOperator(original.getMinString(), Type.STRING))),
                    rewritten.get(0).inputAt(1).getOp().getPredicate());
        } finally {
            connectContext.getSessionVariable().setUseCorrelatedJoinEstimate(correlatedEstimate);
            connectContext.getSessionVariable().setCboDeriveRangeJoinPredicate(deriveRange);
        }
    }

    @Test
    public void testFullOuterJoinKeepsBothKeyDomainsWithRepeatedMatches() {
        ColumnRefOperator leftKey = columnRefFactory.create("left_key", Type.BIGINT, true);
        ColumnRefOperator rightKey = columnRefFactory.create("right_key", Type.BIGINT, true);
        Group left = joinChild(0, 10_000, ImmutableMap.of(leftKey,
                new ColumnStatistic(0, 1000, 0.1, 8, 1000)));
        Group right = joinChild(1, 10_000, ImmutableMap.of(rightKey,
                new ColumnStatistic(500, 1500, 0.2, 8, 1000)));
        Statistics joined = estimateJoin(JoinOperator.FULL_OUTER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey), left, right);

        // branch-4.0 lacks #73489, so the FULL OUTER JOIN row count and null fractions follow the older estimate
        // (max(1, L + R - inner)); only the restored key domains are checked here.
        ColumnStatistic leftStat = joined.getColumnStatistic(leftKey);
        ColumnStatistic rightStat = joined.getColumnStatistic(rightKey);
        Assertions.assertEquals(1000, leftStat.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(0, leftStat.getMinValue(), 0.001);
        Assertions.assertEquals(1000, leftStat.getMaxValue(), 0.001);
        Assertions.assertEquals(1000, rightStat.getDistinctValuesCount(), 0.001);
        Assertions.assertEquals(500, rightStat.getMinValue(), 0.001);
        Assertions.assertEquals(1500, rightStat.getMaxValue(), 0.001);
    }

    @ParameterizedTest
    @EnumSource(value = OuterJoin.class)
    public void testOuterJoinHistogramDoesNotDiscardUnmatchedKeyRange(OuterJoin outerJoin) {
        ColumnRefOperator outerKey = columnRefFactory.create("outer_key", Type.BIGINT, true);
        ColumnRefOperator innerKey = columnRefFactory.create("inner_key", Type.BIGINT, true);
        ColumnStatistic outerStatistic = uniformHistogramStatistic(0, 1000, 1000);
        ColumnStatistic innerStatistic = uniformHistogramStatistic(100, 200, 100);
        Group outer = joinChild(0, 1000, ImmutableMap.of(outerKey, outerStatistic));
        Group inner = joinChild(1, 100, ImmutableMap.of(innerKey, innerStatistic));
        boolean histogramEnabled = connectContext.getSessionVariable().isCboEnableHistogramJoinEstimation();
        boolean correlatedEstimate = connectContext.getSessionVariable().isUseCorrelatedJoinEstimate();
        try {
            connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(true);
            connectContext.getSessionVariable().setUseCorrelatedJoinEstimate(true);
            boolean left = outerJoin == OuterJoin.LEFT_OUTER_JOIN;
            Statistics joined = estimateJoin(left ? JoinOperator.LEFT_OUTER_JOIN : JoinOperator.RIGHT_OUTER_JOIN,
                    new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey),
                    left ? outer : inner, left ? inner : outer);
            ColumnStatistic preserved = joined.getColumnStatistic(outerKey);
            Assertions.assertNull(preserved.getHistogram());
            Assertions.assertEquals(0, preserved.getMinValue(), 0.001);
            Assertions.assertEquals(1000, preserved.getMaxValue(), 0.001);
            Assertions.assertEquals(1000, preserved.getDistinctValuesCount(), 0.001);
            Statistics filtered = PredicateStatisticsCalculator.statisticsCalculate(
                    new BinaryPredicateOperator(BinaryType.LT, outerKey, ConstantOperator.createBigint(100)), joined);
            Assertions.assertEquals(100, filtered.getOutputRowCount(), 0.001,
                    "Unmatched keys below the inner range still retain their selectivity");

            // Preserve the nullable side's existing statistics and null-padding behavior.
            ColumnStatistic nullable = joined.getColumnStatistic(innerKey);
            Assertions.assertSame(innerStatistic.getHistogram(), nullable.getHistogram());
            Assertions.assertEquals(0.9, nullable.getNullsFraction(), 0.001);

            // Inner joins continue to carry the histogram of the matching key intersection.
            Statistics innerJoined = estimateJoin(JoinOperator.INNER_JOIN,
                    new BinaryPredicateOperator(BinaryType.EQ, outerKey, innerKey), outer, inner);
            for (ColumnRefOperator key : List.of(outerKey, innerKey)) {
                Histogram histogram = innerJoined.getColumnStatistic(key).getHistogram();
                Assertions.assertNotNull(histogram);
                Assertions.assertEquals(100, histogram.getBuckets().get(0).getLower(), 0.001);
                Assertions.assertEquals(200, histogram.getBuckets().get(0).getUpper(), 0.001);
            }
        } finally {
            connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(histogramEnabled);
            connectContext.getSessionVariable().setUseCorrelatedJoinEstimate(correlatedEstimate);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFullOuterJoinRestoresHistogramsAfterBothNullPaddingPasses(boolean histogramEnabled) {
        ColumnRefOperator leftKey = columnRefFactory.create("left_key", Type.BIGINT, true);
        ColumnRefOperator rightKey = columnRefFactory.create("right_key", Type.BIGINT, true);
        Group left = joinChild(0, 1000, ImmutableMap.of(leftKey, uniformHistogramStatistic(0, 1000, 1000)));
        Group right = joinChild(1, 1000, ImmutableMap.of(rightKey, uniformHistogramStatistic(500, 1500, 1000)));
        boolean originalHistogramEnabled = connectContext.getSessionVariable().isCboEnableHistogramJoinEstimation();
        boolean correlatedEstimate = connectContext.getSessionVariable().isUseCorrelatedJoinEstimate();
        try {
            connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(histogramEnabled);
            connectContext.getSessionVariable().setUseCorrelatedJoinEstimate(true);
            Statistics joined = estimateJoin(JoinOperator.FULL_OUTER_JOIN,
                    new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey), left, right);
            ColumnStatistic leftStatistic = joined.getColumnStatistic(leftKey);
            ColumnStatistic rightStatistic = joined.getColumnStatistic(rightKey);
            Assertions.assertNull(leftStatistic.getHistogram());
            Assertions.assertNull(rightStatistic.getHistogram());
            Assertions.assertEquals(0, leftStatistic.getMinValue(), 0.001);
            Assertions.assertEquals(1000, leftStatistic.getMaxValue(), 0.001);
            Assertions.assertEquals(500, rightStatistic.getMinValue(), 0.001);
            Assertions.assertEquals(1500, rightStatistic.getMaxValue(), 0.001);
            if (histogramEnabled) {
                // Both sides have unmatched rows, so the second null-padding pass must preserve the first fix.
                Assertions.assertTrue(leftStatistic.getNullsFraction() > 0);
                Assertions.assertTrue(rightStatistic.getNullsFraction() > 0);
            }
            // The filtered row counts asserted on main depend on the FULL OUTER JOIN null fractions of #73489,
            // which branch-4.0 does not have.
        } finally {
            connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(originalHistogramEnabled);
            connectContext.getSessionVariable().setUseCorrelatedJoinEstimate(correlatedEstimate);
        }
    }

    private static ColumnStatistic uniformHistogramStatistic(double min, double max, long rows) {
        Histogram histogram = new Histogram(List.of(new Bucket(min, max, rows, 1L)), Map.of());
        return ColumnStatistic.builder().setMinValue(min).setMaxValue(max).setDistinctValuesCount(rows)
                .setNullsFraction(0).setAverageRowSize(8).setHistogram(histogram).build();
    }

    @Test
    public void testOuterJoinDomainRestorationRespectsDisabledHistograms() {
        ColumnRefOperator leftKey = columnRefFactory.create("left_key", Type.BIGINT, true);
        ColumnRefOperator rightKey = columnRefFactory.create("right_key", Type.BIGINT, true);
        Histogram histogram = new Histogram(List.of(new Bucket(0, 1000, 1000L, 1L)), Map.of());
        Group left = joinChild(0, 1000, ImmutableMap.of(leftKey,
                ColumnStatistic.builder().setMinValue(0).setMaxValue(1000).setDistinctValuesCount(1000)
                        .setHistogram(histogram).build()));
        Group right = joinChild(1, 10, ImmutableMap.of(rightKey,
                new ColumnStatistic(100, 110, 0, 8, 10)));
        boolean histogramEnabled = connectContext.getSessionVariable().isCboEnableHistogramJoinEstimation();
        try {
            connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(false);
            Statistics joined = estimateJoin(JoinOperator.LEFT_OUTER_JOIN,
                    new BinaryPredicateOperator(BinaryType.EQ, leftKey, rightKey), left, right);
            Assertions.assertNull(joined.getColumnStatistic(leftKey).getHistogram());
        } finally {
            connectContext.getSessionVariable().setCboEnableHistogramJoinEstimation(histogramEnabled);
        }
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
}
