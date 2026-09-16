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

package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.function.MetaFunctions;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.InMemoryStatisticStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.plan.ReplayFromDumpTestBase.getDumpInfoFromJson;
import static org.assertj.core.api.Assertions.assertThat;

public class QueryDumpSerializationReplayTest extends PlanTestBase {
    private static final String EVENT_TABLE = "test.query_dump_events";
    private static final String ACCOUNT_TABLE = "test.query_dump_accounts";

    private DumpInfo previousDumpInfo;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("""
                CREATE TABLE query_dump_events (
                    event_id BIGINT NOT NULL,
                    account_id BIGINT,
                    event_type VARCHAR(32),
                    tags ARRAY<VARCHAR(32)>,
                    event_time DATETIME,
                    score DOUBLE,
                    quality DOUBLE,
                    metadata VARCHAR(64)
                ) ENGINE=OLAP
                DUPLICATE KEY(event_id)
                DISTRIBUTED BY HASH(event_id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
                """);
        starRocksAssert.withTable("""
                CREATE TABLE query_dump_accounts (
                    account_id BIGINT NOT NULL,
                    region VARCHAR(32),
                    tier INT
                ) ENGINE=OLAP
                DUPLICATE KEY(account_id)
                DISTRIBUTED BY HASH(account_id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
                """);
    }

    @BeforeEach
    public void setUp() {
        previousDumpInfo = connectContext.getDumpInfo();
        connectContext.setDumpInfo(null);
    }

    @AfterEach
    public void tearDown() {
        assertThat(connectContext.isHTTPQueryDump()).isFalse();
        assertThat(connectContext.getCurrentCatalog()).isEqualTo("default_catalog");
        assertThat(connectContext.getDatabase()).isEqualTo("test");
        connectContext.setDumpInfo(previousDumpInfo);
    }

    @Test
    public void testGetQueryDumpRoundTrips() throws Exception {
        // GIVEN
        final var sql = ReplayFromDumpTestBase.getContentFromFile("dump/complex.sql");
        final var eventTable = (OlapTable) starRocksAssert.getTable("test", "query_dump_events");
        final var accountTable = (OlapTable) starRocksAssert.getTable("test", "query_dump_accounts");
        final var previousStorage = connectContext.getGlobalStateMgr().getStatisticStorage();

        final var eventTypeHistogram = createEventTypeHistogram();
        final var expectedEventStatistics = createEventStatistics(eventTypeHistogram);
        final var expectedAccountStatistics = createAccountStatistics();
        final var statisticStorage = new FullStatisticStorage();

        setTableStatistics(eventTable, 10_000);
        setTableStatistics(accountTable, 1_000);
        expectedEventStatistics.forEach((column, statistic) ->
                statisticStorage.addColumnStatistic(eventTable, column,
                        column.equals("event_type")
                                ? ColumnStatistic.buildFrom(statistic).setHistogram(null).build()
                                : statistic));
        expectedAccountStatistics.forEach((column, statistic) ->
                statisticStorage.addColumnStatistic(accountTable, column, statistic));
        statisticStorage.addHistogramStatistics(eventTable, "event_type", eventTypeHistogram);
        connectContext.getGlobalStateMgr().setStatisticStorage(statisticStorage);

        try {
            // WHEN
            // get_query_dump plans the query and the returned JSON is read back into the dump model.
            final var rawDump = MetaFunctions.getQueryDump(ConstantOperator.createVarchar(sql)).getVarchar();
            final var queryDumpInfo = getDumpInfoFromJson(rawDump);

            // THEN
            assertThat(queryDumpInfo.getOriginStmt()).isEqualTo(sql);
            assertThat(queryDumpInfo.getCreateTableStmtMap()).containsOnlyKeys(EVENT_TABLE, ACCOUNT_TABLE);

            assertTableStatistics(queryDumpInfo, EVENT_TABLE, expectedEventStatistics);
            assertTableStatistics(queryDumpInfo, ACCOUNT_TABLE, expectedAccountStatistics);
        } finally {
            connectContext.getGlobalStateMgr().setStatisticStorage(previousStorage);
        }
    }

    private static Map<String, ColumnStatistic> createEventStatistics(Histogram eventTypeHistogram) {
        Map<String, ColumnStatistic> statistics = new LinkedHashMap<>();
        statistics.put("event_id", statistic(1, 10_000, 0, 8, 10_000));
        statistics.put("account_id", statistic(1, 1_000, 0.01, 8, 1_000));
        statistics.put("event_type", ColumnStatistic.builder()
                .setMinValue(Double.NEGATIVE_INFINITY)
                .setMaxValue(Double.POSITIVE_INFINITY)
                .setNullsFraction(0.05)
                .setAverageRowSize(12)
                .setDistinctValuesCount(5)
                .setMinString("click")
                .setMaxString("view")
                .setHistogram(eventTypeHistogram)
                .build());
        statistics.put("tags", ColumnStatistic.builder()
                .setMinValue(Double.NEGATIVE_INFINITY)
                .setMaxValue(Double.POSITIVE_INFINITY)
                .setNullsFraction(0.1)
                .setAverageRowSize(48)
                .setDistinctValuesCount(400)
                .setCollectionSize(3.5)
                .build());
        statistics.put("event_time", statistic(1_704_067_200, 1_735_689_599, 0, 8, 365));
        statistics.put("score", statistic(0, 1_000, 0.02, 8, 500));
        statistics.put("quality", ColumnStatistic.builder()
                .setMinValue(Double.NaN)
                .setMaxValue(Double.NaN)
                .setNullsFraction(0.2)
                .setAverageRowSize(8)
                .setDistinctValuesCount(20)
                .build());
        statistics.put("metadata", ColumnStatistic.unknown());
        return statistics;
    }

    private static Map<String, ColumnStatistic> createAccountStatistics() {
        Map<String, ColumnStatistic> statistics = new LinkedHashMap<>();
        statistics.put("account_id", statistic(1, 1_000, 0, 8, 1_000));
        statistics.put("region", ColumnStatistic.builder()
                .setMinValue(Double.NEGATIVE_INFINITY)
                .setMaxValue(Double.POSITIVE_INFINITY)
                .setNullsFraction(0)
                .setAverageRowSize(10)
                .setDistinctValuesCount(8)
                .setMinString("africa")
                .setMaxString("south_america")
                .build());
        statistics.put("tier", statistic(1, 5, 0, 4, 5));
        return statistics;
    }

    private static Histogram createEventTypeHistogram() {
        Map<String, Long> mostCommonValues = new LinkedHashMap<>();
        mostCommonValues.put("purchase", 400L);
        mostCommonValues.put("refund", 120L);
        return new Histogram(List.of(
                new Bucket(1, 10, 100L, 5L),
                new Bucket(10, 20, 200L, 3L, 7L)), mostCommonValues);
    }

    private static ColumnStatistic statistic(double min, double max, double nullFraction,
                                             double averageRowSize, double distinctValues) {
        return ColumnStatistic.builder()
                .setMinValue(min)
                .setMaxValue(max)
                .setNullsFraction(nullFraction)
                .setAverageRowSize(averageRowSize)
                .setDistinctValuesCount(distinctValues)
                .build();
    }

    private static void assertTableStatistics(QueryDumpInfo dump, String tableName,
                                              Map<String, ColumnStatistic> expectedStatistics) {
        Map<String, ColumnStatistic> actualStatistics = dump.getTableStatisticsMap().get(tableName);
        assertThat(actualStatistics).isNotNull();
        assertThat(actualStatistics.keySet()).containsExactlyInAnyOrderElementsOf(expectedStatistics.keySet());
        expectedStatistics.forEach((column, expected) ->
                assertColumnStatistic(expected, actualStatistics.get(column)));
    }

    private static void assertColumnStatistic(ColumnStatistic expected, ColumnStatistic actual) {
        assertThat(actual).isNotNull();
        assertDouble(actual.getMinValue(), expected.getMinValue());
        assertDouble(actual.getMaxValue(), expected.getMaxValue());
        assertDouble(actual.getNullsFraction(), expected.getNullsFraction());
        assertDouble(actual.getAverageRowSize(), expected.getAverageRowSize());
        assertDouble(actual.getDistinctValuesCount(), expected.getDistinctValuesCount());
        assertDouble(actual.getCollectionSize(), expected.getCollectionSize());
        assertThat(actual.getType()).isEqualTo(expected.getType());
        assertThat(actual.getMinString()).isEqualTo(expected.getMinString());
        assertThat(actual.getMaxString()).isEqualTo(expected.getMaxString());
        assertHistogram(expected.getHistogram(), actual.getHistogram());
    }

    private static void assertDouble(double actual, double expected) {
        if (Double.isNaN(expected)) {
            assertThat(actual).isNaN();
        } else {
            assertThat(actual).isEqualTo(expected);
        }
    }

    private static void assertHistogram(Histogram expected, Histogram actual) {
        if (expected == null) {
            assertThat(actual).isNull();
            return;
        }

        assertThat(actual).isNotNull();
        assertThat(actual.getMCV()).containsExactlyEntriesOf(expected.getMCV());
        assertThat(actual.getBuckets()).hasSameSizeAs(expected.getBuckets());
        for (int i = 0; i < expected.getBuckets().size(); i++) {
            Bucket expectedBucket = expected.getBuckets().get(i);
            Bucket actualBucket = actual.getBuckets().get(i);
            assertThat(actualBucket.getLower()).isEqualTo(expectedBucket.getLower());
            assertThat(actualBucket.getUpper()).isEqualTo(expectedBucket.getUpper());
            assertThat(actualBucket.getCount()).isEqualTo(expectedBucket.getCount());
            assertThat(actualBucket.getUpperRepeats()).isEqualTo(expectedBucket.getUpperRepeats());
            assertThat(actualBucket.getDistinctCount()).isEqualTo(expectedBucket.getDistinctCount());
        }
    }

    private static class FullStatisticStorage extends InMemoryStatisticStorage {
        private final Map<Long, Map<String, Histogram>> histograms = new HashMap<>();

        @Override
        public void addHistogramStatistics(Table table, String column, Histogram histogram) {
            histograms.computeIfAbsent(table.getId(), ignored -> new HashMap<>()).put(column, histogram);
        }

        @Override
        public Map<String, Histogram> getHistogramStatistics(Table table, List<String> columns) {
            Map<String, Histogram> tableHistograms = histograms.getOrDefault(table.getId(), Map.of());
            Map<String, Histogram> result = new HashMap<>();
            for (String column : columns) {
                if (tableHistograms.containsKey(column)) {
                    result.put(column, tableHistograms.get(column));
                }
            }
            return result;
        }
    }
}
