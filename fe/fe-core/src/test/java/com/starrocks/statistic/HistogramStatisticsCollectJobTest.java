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

public class HistogramStatisticsCollectJobTest extends HistogramStatisticsCollectJobTestBase {
    @Test
    public void testColumnNameSqlEscaping() {
        String columnName = "a\\b'c";
        Database db = new Database(1, "test");
        OlapTable table = new OlapTable();
        table.setId(2);
        table.setName("t0");
        HistogramStatisticsCollectJob job = new HistogramStatisticsCollectJob(
                db, table, Lists.newArrayList(columnName), Lists.newArrayList(IntegerType.BIGINT),
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap());

        VelocityContext context = HistogramStatisticsUtils.buildBaseContext(
                db, table, job.getCatalogName(), columnName);
        assertSqlLiteralRoundTrips(columnName, (String) context.get("columnNameStr"));
    }

    @Test
    public void testBatchInsertCombinesMultipleColumnTypesAndCompletesCollection() throws Exception {
        try (NativeHistogramBatchFixture fixture = new NativeHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);

            NativeAnalyzeStatus status = fixture.collect();

            Assertions.assertEquals(4, fixture.statisticsQueryCount(), "two queries per column: MCV and histogram");
            String expectedSql = """
                    INSERT INTO _statistics_.histogram_statistics(\
                    table_id, column_name, db_id, table_name, buckets, mcv, update_time) VALUES \
                    (%d, 'v2', %d, 'test.t0_stats', '[["1","2","3","4"]]', '[["1","100"]]', NOW()), \
                    (%d, 'v7', %d, 'test.t0_stats', '[["1","2","3","4"]]', '[["1","100"]]', NOW());
                    """.formatted(
                    fixture.table.getId(), fixture.db.getId(), fixture.table.getId(), fixture.db.getId()).strip();
            Assertions.assertEquals(Lists.newArrayList(expectedSql), fixture.batchInsertSql());
            Assertions.assertEquals(100, status.getProgress());
        }
    }

    @Test
    public void testBatchInsertPreservesNullForEmptyBuckets() throws Exception {
        try (NativeHistogramBatchFixture fixture = new NativeHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);
            fixture.returnEmptyV2Histogram();

            fixture.collect();

            String expectedSql = """
                    INSERT INTO _statistics_.histogram_statistics(\
                    table_id, column_name, db_id, table_name, buckets, mcv, update_time) VALUES \
                    (%d, 'v2', %d, 'test.t0_stats', NULL, '[["1","100"]]', NOW()), \
                    (%d, 'v7', %d, 'test.t0_stats', '[["1","2","3","4"]]', '[["1","100"]]', NOW());
                    """.formatted(
                    fixture.table.getId(), fixture.db.getId(), fixture.table.getId(), fixture.db.getId()).strip();
            Assertions.assertEquals(Lists.newArrayList(expectedSql), fixture.batchInsertSql());
        }
    }

    @Test
    public void testBatchInsertCalculatesMcvsAndHistogramsForMultipleColumnTypes() throws Exception {
        try (NativeHistogramBatchFixture fixture = new NativeHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);

            fixture.collect();

            String expectedV2McvSql = """
                    select cast(version as INT), cast(db_id as BIGINT), cast(table_id as BIGINT),
                    cast(column_key as varchar), cast(column_value as varchar) from (
                        SELECT 2 as version, %d as db_id, %d as table_id, `v2` as column_key,
                        count(`v2`) as column_value
                        FROM `test`.`t0_stats` SAMPLE('percent'='10')
                        WHERE `v2` is not null
                        GROUP BY `v2`
                        ORDER BY count(`v2`) desc limit 100
                    ) t
                    """.formatted(fixture.db.getId(), fixture.table.getId());
            String expectedV2HistogramSql = """
                    SELECT cast(2 as INT), cast(%d as BIGINT), cast(%d as BIGINT), 'v2',
                    histogram(`column_key`, cast(64 as int), cast(0.1 as double))
                    FROM (
                        SELECT `v2` as column_key
                        FROM `test`.`t0_stats` SAMPLE('percent'='10')
                        WHERE TRUE and `v2` is not null and `v2` not in (1)
                        ORDER BY `v2` LIMIT 10000000
                    ) t
                    """.formatted(fixture.db.getId(), fixture.table.getId());
            String expectedV7McvSql = """
                    select cast(version as INT), cast(db_id as BIGINT), cast(table_id as BIGINT),
                    cast(column_key as varchar), cast(column_value as varchar) from (
                        SELECT 2 as version, %d as db_id, %d as table_id, `v7` as column_key,
                        count(`v7`) as column_value
                        FROM `test`.`t0_stats` SAMPLE('percent'='10')
                        WHERE `v7` is not null
                        GROUP BY `v7`
                        ORDER BY count(`v7`) desc limit 100
                    ) t
                    """.formatted(fixture.db.getId(), fixture.table.getId());
            String expectedV7HistogramSql = """
                    SELECT cast(2 as INT), cast(%d as BIGINT), cast(%d as BIGINT), 'v7',
                    concat('[["Infinity","Infinity",',
                        cast(cast(greatest(0, count(`v7`) / cast(0.1 as double) - 100) as bigint) as varchar),
                        ',0]]')
                    FROM `test`.`t0_stats` SAMPLE('percent'='10')
                    """.formatted(fixture.db.getId(), fixture.table.getId());
            assertSqlStatements(
                    Lists.newArrayList(
                            expectedV2McvSql, expectedV2HistogramSql, expectedV7McvSql, expectedV7HistogramSql),
                    fixture.statisticsQueries());
        }
    }

    @Test
    public void testBatchInsertFlushesRowsAtBufferLimit() throws Exception {
        try (NativeHistogramBatchFixture fixture = new NativeHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(1);

            fixture.collect();

            String expectedV2Sql = """
                    INSERT INTO _statistics_.histogram_statistics(\
                    table_id, column_name, db_id, table_name, buckets, mcv, update_time) VALUES \
                    (%d, 'v2', %d, 'test.t0_stats', '[["1","2","3","4"]]', '[["1","100"]]', NOW());
                    """.formatted(fixture.table.getId(), fixture.db.getId()).strip();
            String expectedV7Sql = """
                    INSERT INTO _statistics_.histogram_statistics(\
                    table_id, column_name, db_id, table_name, buckets, mcv, update_time) VALUES \
                    (%d, 'v7', %d, 'test.t0_stats', '[["1","2","3","4"]]', '[["1","100"]]', NOW());
                    """.formatted(fixture.table.getId(), fixture.db.getId()).strip();
            Assertions.assertEquals(
                    Lists.newArrayList(expectedV2Sql, expectedV7Sql),
                    fixture.batchInsertSql());
        }
    }

    @Test
    public void testBatchInsertFlushesCompletedRowsBeforeCollectionFailure() throws Exception {
        try (NativeHistogramBatchFixture fixture = new NativeHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);
            fixture.failOnSecondColumn();

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, fixture::collect);

            Assertions.assertEquals("mock second-column failure", exception.getMessage());
            Assertions.assertEquals(1, fixture.batchInsertSql().size());
            Assertions.assertTrue(fixture.batchInsertSql().get(0).contains("'v2'"));
            Assertions.assertFalse(fixture.batchInsertSql().get(0).contains("'v7'"));
        }
    }

    @Test
    public void testCollectionFailureRemainsPrimaryWhenEmergencyFlushFails() throws Exception {
        try (NativeHistogramBatchFixture fixture = new NativeHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);
            fixture.failOnSecondColumn();
            fixture.failBatchInsert();

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, fixture::collect);

            Assertions.assertEquals("mock second-column failure", exception.getMessage());
            Assertions.assertEquals(1, fixture.batchInsertAttempts());
            Assertions.assertEquals(1, exception.getSuppressed().length);
            Assertions.assertEquals("mock batch insert failure", exception.getSuppressed()[0].getMessage());
        }
    }

    @Test
    public void testBatchInsertCreatesFreshStatementForRetry() throws Exception {
        try (NativeHistogramBatchFixture fixture = new NativeHistogramBatchFixture(connectContext)) {
            fixture.enableBatch(20L * 1024 * 1024);
            fixture.collect();

            Assertions.assertEquals(1, fixture.batchInsertSql().size());
            Assertions.assertNotSame(fixture.firstBatchInsertStatement(), fixture.firstRetryBatchInsertStatement());
        }
    }

    @Test
    public void testParseNdvModeNone() {
        // Given analyze properties carrying a recognised histogram_collect_bucket_ndv_mode
        // CASE WHEN the mode is "none" THEN NONE WHEN "sample" THEN SAMPLE WHEN "hll" THEN HLL
        //      ELSE warn and fall back to NONE END

        String ndvModeProperty = "none";
        StatsConstants.HistogramCollectBucketNdvMode expectedNdvMode = StatsConstants.HistogramCollectBucketNdvMode.NONE;

        StatsConstants.HistogramCollectBucketNdvMode actualNdvMode =
                HistogramCollectParams.parseBucketNdvMode(ndvModeProperty);

        Assertions.assertEquals(expectedNdvMode, actualNdvMode);
    }

    @Test
    public void testParseNdvModeSample() {
        // Given analyze properties carrying a recognised histogram_collect_bucket_ndv_mode
        // CASE WHEN the mode is "none" THEN NONE WHEN "sample" THEN SAMPLE WHEN "hll" THEN HLL
        //      ELSE warn and fall back to NONE END

        String ndvModeProperty = "sample";
        StatsConstants.HistogramCollectBucketNdvMode expectedNdvMode =
                StatsConstants.HistogramCollectBucketNdvMode.SAMPLE;

        StatsConstants.HistogramCollectBucketNdvMode actualNdvMode =
                HistogramCollectParams.parseBucketNdvMode(ndvModeProperty);

        Assertions.assertEquals(expectedNdvMode, actualNdvMode);
    }

    @Test
    public void testParseNdvModeHll() {
        // Given analyze properties carrying a recognised histogram_collect_bucket_ndv_mode
        // CASE WHEN the mode is "none" THEN NONE WHEN "sample" THEN SAMPLE WHEN "hll" THEN HLL
        //      ELSE warn and fall back to NONE END

        String ndvModeProperty = "hll";
        StatsConstants.HistogramCollectBucketNdvMode expectedNdvMode = StatsConstants.HistogramCollectBucketNdvMode.HLL;

        StatsConstants.HistogramCollectBucketNdvMode actualNdvMode =
                HistogramCollectParams.parseBucketNdvMode(ndvModeProperty);

        Assertions.assertEquals(expectedNdvMode, actualNdvMode);
    }

    @Test
    public void testParseNdvModeIgnoresCase() {
        // Given analyze properties whose histogram_collect_bucket_ndv_mode is a mode name in upper case
        // CASE WHEN the mode matches a known name ignoring case THEN that mode
        //      ELSE warn and fall back to NONE END

        String ndvModeProperty = "HLL";
        StatsConstants.HistogramCollectBucketNdvMode expectedNdvMode = StatsConstants.HistogramCollectBucketNdvMode.HLL;

        StatsConstants.HistogramCollectBucketNdvMode actualNdvMode =
                HistogramCollectParams.parseBucketNdvMode(ndvModeProperty);

        Assertions.assertEquals(expectedNdvMode, actualNdvMode);
    }

    @Test
    public void testParseNdvModeUnrecognised() {
        // Given analyze properties whose histogram_collect_bucket_ndv_mode names no known mode
        // CASE WHEN the mode matches a known name ignoring case THEN that mode
        //      ELSE warn and fall back to NONE, so an unusable property cannot fail the analyze job END

        String ndvModeProperty = "bogus";
        StatsConstants.HistogramCollectBucketNdvMode expectedNdvMode = StatsConstants.HistogramCollectBucketNdvMode.NONE;

        StatsConstants.HistogramCollectBucketNdvMode actualNdvMode =
                HistogramCollectParams.parseBucketNdvMode(ndvModeProperty);

        Assertions.assertEquals(expectedNdvMode, actualNdvMode);
    }

    @Test
    public void testParseNdvModeAbsent() {
        // Given analyze properties that carry no histogram_collect_bucket_ndv_mode at all, so the
        // raw property value is null
        // CASE WHEN the mode matches a known name ignoring case THEN that mode
        //      ELSE warn and fall back to NONE, so a missing property cannot fail the analyze job END

        String ndvModeProperty = null;
        StatsConstants.HistogramCollectBucketNdvMode expectedNdvMode = StatsConstants.HistogramCollectBucketNdvMode.NONE;

        StatsConstants.HistogramCollectBucketNdvMode actualNdvMode =
                HistogramCollectParams.parseBucketNdvMode(ndvModeProperty);

        Assertions.assertEquals(expectedNdvMode, actualNdvMode);
    }

    private static class NativeHistogramBatchFixture implements AutoCloseable {
        private static final String HISTOGRAM = "[[\"1\",\"2\",\"3\",\"4\"]]";

        private final ConnectContext context;
        private final Database db;
        private final OlapTable table;
        private final HistogramStatisticsCollectJob job;
        private final AtomicInteger statisticsQueryCalls = new AtomicInteger();
        private final AtomicInteger failSecondColumn = new AtomicInteger();
        private final AtomicInteger batchInsertAttempts = new AtomicInteger();
        private final List<String> statisticsQueries = new ArrayList<>();
        private boolean emptyV2Histogram;
        private boolean failBatchInsert;
        private final List<StatementBase> batchInsertStatements = new ArrayList<>();
        private final List<StatementBase> retryBatchInsertStatements = new ArrayList<>();
        private final List<String> capturedBatchInsertSql = new ArrayList<>();
        private final long originalBufferSize = Config.histogram_batch_insert_buffer_size;

        private NativeHistogramBatchFixture(ConnectContext context) {
            this.context = context;
            db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "t0_stats");

            Map<String, String> properties = new HashMap<>();
            properties.put(StatsConstants.HISTOGRAM_SAMPLE_RATIO, "0.1");
            properties.put(StatsConstants.HISTOGRAM_BUCKET_NUM, "64");
            properties.put(StatsConstants.HISTOGRAM_MCV_SIZE, "100");
            properties.put(StatsConstants.HISTOGRAM_COLLECT_BUCKET_NDV_MODE, "none");
            job = new HistogramStatisticsCollectJob(
                    db, table, Lists.newArrayList("v2", "v7"),
                    Lists.newArrayList(IntegerType.BIGINT, VarcharType.VARCHAR),
                    StatsConstants.ScheduleType.ONCE, properties);

            new MockUp<StatisticExecutor>() {
                @Mock
                public List<TStatisticData> executeStatisticDQL(ConnectContext ctx, String sql) {
                    statisticsQueryCalls.incrementAndGet();
                    statisticsQueries.add(sql);
                    if (failSecondColumn.get() != 0 && sql.contains("`v7`")) {
                        throw new RuntimeException("mock second-column failure");
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
            };

            new MockUp<HistogramStatisticsCollectJob>() {
                @Mock
                public void collectStatisticSync(Supplier<StatementBase> statementSupplier,
                                                 ConnectContext ctx, AnalyzeStatus status) {
                    batchInsertAttempts.incrementAndGet();
                    if (failBatchInsert) {
                        throw new RuntimeException("mock batch insert failure");
                    }
                    StatementBase statement = statementSupplier.get();
                    batchInsertStatements.add(statement);
                    retryBatchInsertStatements.add(statementSupplier.get());
                    capturedBatchInsertSql.add(statement.getOrigStmt().getOrigStmt());
                }
            };
        }

        private void enableBatch(long bufferSize) {
            Config.histogram_batch_insert_buffer_size = bufferSize;
        }

        private void returnEmptyV2Histogram() {
            emptyV2Histogram = true;
        }

        private void failOnSecondColumn() {
            failSecondColumn.set(1);
        }

        private void failBatchInsert() {
            failBatchInsert = true;
        }

        private NativeAnalyzeStatus collect() throws Exception {
            NativeAnalyzeStatus status = new NativeAnalyzeStatus();
            job.collect(context, status);
            return status;
        }

        private List<String> batchInsertSql() {
            return capturedBatchInsertSql;
        }

        private int statisticsQueryCount() {
            return statisticsQueryCalls.get();
        }

        private int batchInsertAttempts() {
            return batchInsertAttempts.get();
        }

        private List<String> statisticsQueries() {
            return statisticsQueries;
        }

        private StatementBase firstBatchInsertStatement() {
            return batchInsertStatements.get(0);
        }

        private StatementBase firstRetryBatchInsertStatement() {
            return retryBatchInsertStatements.get(0);
        }

        @Override
        public void close() {
            Config.histogram_batch_insert_buffer_size = originalBufferSize;
        }
    }
}
