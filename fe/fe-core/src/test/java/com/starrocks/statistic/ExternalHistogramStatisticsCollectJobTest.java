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

package com.starrocks.statistic;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import mockit.Mock;
import mockit.MockUp;
import org.apache.velocity.VelocityContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ExternalHistogramStatisticsCollectJobTest extends HistogramStatisticsCollectJobTestBase {
    @Test
    public void testColumnNameSqlEscaping() {
        String columnName = "a\\b'c";
        Database db = new Database(1, "test");
        OlapTable table = new OlapTable();
        table.setId(2);
        table.setName("t0");
        ExternalHistogramStatisticsCollectJob job = new ExternalHistogramStatisticsCollectJob(
                "hive0", db, table, Lists.newArrayList(columnName), Lists.newArrayList(IntegerType.BIGINT),
                StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        VelocityContext context = Deencapsulation.invoke(job, "buildBaseContext", db, table, columnName);
        assertSqlLiteralRoundTrips(columnName, (String) context.get("columnNameStr"));
    }

    @Test
    public void testBatchInsertCombinesMultipleColumnTypes() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);

            fixture.collect();

            String expectedSql = """
                    INSERT INTO _statistics_.external_histogram_statistics(\
                    table_uuid, column_name, catalog_name, db_name, table_name, buckets, mcv, update_time) VALUES \
                    ('%s', 'v2', 'hive0', 'test', 't0_stats', '[["1","2","3","4"]]', '[["1","10"]]', NOW()), \
                    ('%s', 'v7', 'hive0', 'test', 't0_stats', '[["1","2","3","4"]]', '[["1","10"]]', NOW());
                    """.formatted(fixture.tableUuidHash(), fixture.tableUuidHash()).strip();
            Assertions.assertEquals(Lists.newArrayList(expectedSql), fixture.batchInsertSql());
        }
    }

    @Test
    public void testBatchInsertPreservesNullForEmptyBuckets() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);
            fixture.returnEmptyV2Histogram();

            fixture.collect();

            String expectedSql = """
                    INSERT INTO _statistics_.external_histogram_statistics(\
                    table_uuid, column_name, catalog_name, db_name, table_name, buckets, mcv, update_time) VALUES \
                    ('%s', 'v2', 'hive0', 'test', 't0_stats', NULL, '[["1","10"]]', NOW()), \
                    ('%s', 'v7', 'hive0', 'test', 't0_stats', \
                    '[["1","2","3","4"]]', '[["1","10"]]', NOW());
                    """.formatted(fixture.tableUuidHash(), fixture.tableUuidHash()).strip();
            Assertions.assertEquals(Lists.newArrayList(expectedSql), fixture.batchInsertSql());
        }
    }

    @Test
    public void testBatchInsertCalculatesMcvsAndHistogramsForMultipleColumnTypes() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);

            fixture.collect();

            String expectedV2McvSql = """
                    select cast(version as INT), cast(column_key as varchar), cast(column_value as varchar)
                    from (select 7 as version, `v2` as column_key, count(`v2`) as column_value
                        from `hive0`.`test`.`t0_stats`
                        where `v2` is not null
                        group by `v2`
                        order by column_value desc limit 100
                    ) t
                    """;
            String expectedV2HistogramSql = """
                    SELECT cast(7 as INT), 'v2',
                    histogram(`column_key`, cast(64 as int), cast(0.1 as double))
                    FROM (
                        SELECT `v2` as column_key
                        FROM `hive0`.`test`.`t0_stats`
                        where rand() <= 0.1 and `v2` is not null and `v2` not in (1)
                        ORDER BY `v2` LIMIT 10000000
                    ) t
                    """;
            String expectedV7McvSql = """
                    select cast(version as INT), cast(column_key as varchar), cast(column_value as varchar)
                    from (select 7 as version, `v7` as column_key, count(`v7`) as column_value
                        from `hive0`.`test`.`t0_stats`
                        where `v7` is not null
                        group by `v7`
                        order by column_value desc limit 100
                    ) t
                    """;
            String expectedV7HistogramSql = """
                    SELECT cast(7 as INT), 'v7',
                    concat('[["Infinity","Infinity",',
                        cast(greatest(0, count(`v7`) - 10) as varchar),
                        ',0]]')
                    FROM `hive0`.`test`.`t0_stats`
                    """;
            assertSqlStatements(
                    Lists.newArrayList(
                            expectedV2McvSql, expectedV2HistogramSql, expectedV7McvSql, expectedV7HistogramSql),
                    fixture.statisticsQueries());
        }
    }

    @Test
    public void testBatchInsertFlushesRowsAtBufferLimit() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(1);

            fixture.collect();

            String expectedV2Sql = """
                    INSERT INTO _statistics_.external_histogram_statistics(\
                    table_uuid, column_name, catalog_name, db_name, table_name, buckets, mcv, update_time) VALUES \
                    ('%s', 'v2', 'hive0', 'test', 't0_stats', '[["1","2","3","4"]]', '[["1","10"]]', NOW());
                    """.formatted(fixture.tableUuidHash()).strip();
            String expectedV7Sql = """
                    INSERT INTO _statistics_.external_histogram_statistics(\
                    table_uuid, column_name, catalog_name, db_name, table_name, buckets, mcv, update_time) VALUES \
                    ('%s', 'v7', 'hive0', 'test', 't0_stats', '[["1","2","3","4"]]', '[["1","10"]]', NOW());
                    """.formatted(fixture.tableUuidHash()).strip();
            Assertions.assertEquals(
                    Lists.newArrayList(expectedV2Sql, expectedV7Sql), fixture.batchInsertSql());
        }
    }

    @Test
    public void testBatchInsertCleansInsertedColumns() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);

            fixture.collect();

            Assertions.assertEquals(1, fixture.cleanupCallCount());
            Assertions.assertEquals(Lists.newArrayList("v2", "v7"), fixture.cleanedColumns());
        }
    }

    @Test
    public void testBatchInsertCleansInsertedColumnsAfterFailure() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);
            fixture.failOnSecondColumn();

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, fixture::collect);

            Assertions.assertEquals("mock second-column failure", exception.getMessage());
            Assertions.assertEquals(1, fixture.batchInsertSql().size());
            Assertions.assertEquals(1, fixture.cleanupCallCount());
            Assertions.assertEquals(Lists.newArrayList("v2"), fixture.cleanedColumns());
        }
    }

    @Test
    public void testBatchInsertFlushesCompletedRowsBeforeInvalidHistogramResult() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);
            fixture.returnNoSecondColumnHistogramResults();

            Exception exception = Assertions.assertThrows(Exception.class, fixture::collect);

            Assertions.assertEquals(
                    "Expected exactly one external histogram result for column v7, but got 0",
                    exception.getMessage());
            Assertions.assertEquals(1, fixture.batchInsertSql().size());
            Assertions.assertTrue(fixture.batchInsertSql().get(0).contains("'v2'"));
            Assertions.assertFalse(fixture.batchInsertSql().get(0).contains("'v7'"));
            Assertions.assertEquals(1, fixture.cleanupCallCount());
            Assertions.assertEquals(Lists.newArrayList("v2"), fixture.cleanedColumns());
        }
    }

    @Test
    public void testUsesLegacyInsertWhenBatchDisabled() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.disableBatch();

            fixture.collect();

            Assertions.assertTrue(fixture.batchInsertSql().isEmpty());
            Assertions.assertEquals(2, fixture.legacyInsertCount(), "one legacy INSERT per column");
        }
    }

    @Test
    public void testBatchInsertCreatesFreshStatementForRetry() throws Exception {
        try (ExternalHistogramBatchFixture fixture = new ExternalHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);

            fixture.collect();

            Assertions.assertEquals(1, fixture.batchInsertSql().size());
            Assertions.assertNotSame(fixture.firstBatchInsertStatement(), fixture.firstRetryBatchInsertStatement());
        }
    }

    private static class ExternalHistogramBatchFixture implements AutoCloseable {
        private static final String HISTOGRAM = "[[\"1\",\"2\",\"3\",\"4\"]]";

        private final ConnectContext context;
        private final OlapTable table;
        private final ExternalHistogramStatisticsCollectJob job;
        private final AtomicInteger cleanupCalls = new AtomicInteger();
        private final AtomicInteger failSecondColumn = new AtomicInteger();
        private final List<String> cleanedColumns = new ArrayList<>();
        private boolean emptyV2Histogram;
        private boolean noSecondColumnHistogramResults;
        private final List<String> statisticsQueries = new ArrayList<>();
        private final List<StatementBase> batchInsertStatements = new ArrayList<>();
        private final List<StatementBase> retryBatchInsertStatements = new ArrayList<>();
        private final List<String> batchInsertSql = new ArrayList<>();
        private final List<String> legacyInsertSql = new ArrayList<>();
        private final boolean originalEnableBatch = Config.enable_batch_insert_histogram_statistics;
        private final long originalBufferSize = Config.histogram_batch_insert_buffer_size;

        private ExternalHistogramBatchFixture(ConnectContext context) {
            this.context = context;
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "t0_stats");

            Map<String, String> properties = new HashMap<>();
            properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, "0.1");
            properties.put(StatsConstants.HISTOGRAM_BUCKET_NUM, "64");
            properties.put(StatsConstants.HISTOGRAM_MCV_SIZE, "100");
            job = new ExternalHistogramStatisticsCollectJob(
                    "hive0", db, table, Lists.newArrayList("v2", "v7"),
                    Lists.newArrayList(IntegerType.BIGINT, VarcharType.VARCHAR),
                    StatsConstants.AnalyzeType.HISTOGRAM, StatsConstants.ScheduleType.ONCE, properties);

            new MockUp<StatisticExecutor>() {
                @Mock
                public List<TStatisticData> executeStatisticDQL(ConnectContext ctx, String sql) {
                    statisticsQueries.add(sql);
                    if (failSecondColumn.get() != 0 && sql.contains("`v7`")) {
                        throw new RuntimeException("mock second-column failure");
                    }
                    if (noSecondColumnHistogramResults && sql.contains("'v7'")) {
                        return Lists.newArrayList();
                    }

                    TStatisticData data = new TStatisticData();
                    if (sql.toLowerCase().contains("group by")) {
                        data.columnName = "1";
                        data.histogram = "10";
                    } else if (emptyV2Histogram && sql.contains("'v2'")) {
                        data.histogram = null;
                    } else {
                        data.histogram = HISTOGRAM;
                    }
                    return Lists.newArrayList(data);
                }

                @Mock
                public boolean dropExternalHistogramRawColumns(
                        ConnectContext ctx, String tableUUID, List<String> columns) {
                    cleanupCalls.incrementAndGet();
                    cleanedColumns.clear();
                    cleanedColumns.addAll(columns);
                    return true;
                }
            };

            new MockUp<ExternalHistogramStatisticsCollectJob>() {
                @Mock
                public void collectStatisticSync(
                        Supplier<StatementBase> statementSupplier, ConnectContext ctx, AnalyzeStatus status) {
                    StatementBase statement = statementSupplier.get();
                    batchInsertStatements.add(statement);
                    retryBatchInsertStatements.add(statementSupplier.get());
                    batchInsertSql.add(statement.getOrigStmt().getOrigStmt());
                }

                @Mock
                public void collectStatisticSync(String sql, ConnectContext ctx, AnalyzeStatus status) {
                    legacyInsertSql.add(sql);
                }
            };
        }

        private void enableBatch(long bufferSize) {
            Config.enable_batch_insert_histogram_statistics = true;
            Config.histogram_batch_insert_buffer_size = bufferSize;
        }

        private void disableBatch() {
            Config.enable_batch_insert_histogram_statistics = false;
        }

        private void failOnSecondColumn() {
            failSecondColumn.set(1);
        }

        private void returnEmptyV2Histogram() {
            emptyV2Histogram = true;
        }

        private void returnNoSecondColumnHistogramResults() {
            noSecondColumnHistogramResults = true;
        }

        private void collect() throws Exception {
            job.collect(context, new NativeAnalyzeStatus());
        }

        private String tableUuidHash() {
            return StatisticUtils.hashTableUuidForPkStorage(table.getUUID());
        }

        private List<String> batchInsertSql() {
            return batchInsertSql;
        }

        private int cleanupCallCount() {
            return cleanupCalls.get();
        }

        private List<String> cleanedColumns() {
            return cleanedColumns;
        }

        private List<String> statisticsQueries() {
            return statisticsQueries;
        }

        private int legacyInsertCount() {
            return legacyInsertSql.size();
        }

        private StatementBase firstBatchInsertStatement() {
            return batchInsertStatements.get(0);
        }

        private StatementBase firstRetryBatchInsertStatement() {
            return retryBatchInsertStatements.get(0);
        }

        @Override
        public void close() {
            Config.enable_batch_insert_histogram_statistics = originalEnableBatch;
            Config.histogram_batch_insert_buffer_size = originalBufferSize;
        }
    }
}
