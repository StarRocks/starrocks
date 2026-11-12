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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.EsTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
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
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalKuduScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
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

        // a string-to-temporal cast on the identity string partition column is not converted, but the connector
        // prunes manifests / files with it against the partition values, so the row count already reflects it
        LogicalIcebergScanOperator identity = icebergScan("partitioned_db", "t1");
        ScalarOperator castEq = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(DateType.DATETIME, icebergColumn(identity, "date")),
                ConstantOperator.createDatetime(LocalDateTime.of(2020, 1, 1, 0, 0)));
        Assertions.assertEquals(ConstantOperator.TRUE,
                calculator.removePartitionPredicate(castEq, identity, ICEBERG_TABLE_METADATA));
        // ... but not with statistics that do not come from the planned files
        Assertions.assertEquals(castEq, calculator.removePartitionPredicate(castEq, identity, null));
        // shapes the manifest range check cannot prune with (a DATE cast, !=) keep every manifest, so they stay
        ScalarOperator castDateEq = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(DateType.DATE, icebergColumn(identity, "date")),
                ConstantOperator.createDate(LocalDateTime.of(2020, 1, 1, 0, 0)));
        Assertions.assertEquals(castDateEq,
                calculator.removePartitionPredicate(castDateEq, identity, ICEBERG_TABLE_METADATA));
        ScalarOperator castNe = new BinaryPredicateOperator(BinaryType.NE,
                new CastOperator(DateType.DATETIME, icebergColumn(identity, "date")),
                ConstantOperator.createDatetime(LocalDateTime.of(2020, 1, 1, 0, 0)));
        Assertions.assertEquals(castNe, calculator.removePartitionPredicate(castNe, identity, ICEBERG_TABLE_METADATA));
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

        // after a partition transform evolution the file partition values are not read, so the cast residual keeps
        // every file and the cast stays
        new MockUp<IcebergTable>() {
            @Mock
            public boolean hasPartitionTransformedEvolution() {
                return true;
            }
        };
        Assertions.assertEquals(castEq, calculator.removePartitionPredicate(castEq, identity, ICEBERG_TABLE_METADATA));
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
}
