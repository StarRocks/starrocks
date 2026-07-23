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

package com.starrocks.sql.optimizer.dump;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.HistogramUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for QueryDumpDeserializer's ability to parse both old and new label-formatted statistics.
 */
public class QueryDumpDeserializerTest {

    private static Stream<Arguments> basicStatisticsFormats() {
        return Stream.of(
                Arguments.of("old format", "[1.0, 100.0, 0.0, 8.0, 50.0] ESTIMATE",
                        1.0, 100.0, 0.0, 8.0, 50.0, ColumnStatistic.StatisticType.ESTIMATE),
                Arguments.of("new labeled format", "[MIN: 1.0, MAX: 100.0, NULLS: 0.0, ROS: 8.0, NDV: 50.0] ESTIMATE",
                        1.0, 100.0, 0.0, 8.0, 50.0, ColumnStatistic.StatisticType.ESTIMATE)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("basicStatisticsFormats")
    public void testParseBasicStatistics(String formatName, String format,
                                         double expectedMin, double expectedMax,
                                         double expectedNulls, double expectedAvgSize,
                                         double expectedNdv, ColumnStatistic.StatisticType expectedType) {
        ColumnStatistic stats = ColumnStatistic.buildFrom(format).build();

        assertThat(stats.getMinValue()).isEqualTo(expectedMin);
        assertThat(stats.getMaxValue()).isEqualTo(expectedMax);
        assertThat(stats.getNullsFraction()).isEqualTo(expectedNulls);
        assertThat(stats.getAverageRowSize()).isEqualTo(expectedAvgSize);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(expectedNdv);
        assertThat(stats.getType()).isEqualTo(expectedType);
    }

    private static Stream<Arguments> unknownTypeFormats() {
        return Stream.of(
                Arguments.of("old format with UNKNOWN type", "[-Infinity, Infinity, 0.0, 1.0, 1.0] UNKNOWN"),
                Arguments.of("new labeled format with UNKNOWN type",
                        "[MIN: -Infinity, MAX: Infinity, NULLS: 0.0, ROS: 1.0, NDV: 1.0] UNKNOWN"),
                Arguments.of("old format without type", "[-Infinity, Infinity, 0.0, 1.0, 1.0]"),
                Arguments.of("new labeled format without type",
                        "[MIN: -Infinity, MAX: Infinity, NULLS: 0.0, ROS: 1.0, NDV: 1.0]")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unknownTypeFormats")
    public void testParseUnknownTypeStatistics(String formatName, String format) {
        ColumnStatistic stats = ColumnStatistic.buildFrom(format).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(stats.getNullsFraction()).isEqualTo(0.0);
        assertThat(stats.getAverageRowSize()).isEqualTo(1.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.UNKNOWN);
    }

    private static Stream<Arguments> nullFractionFormats() {
        return Stream.of(
                Arguments.of("old format", "[0.0, 1000.0, 0.1, 16.0, 200.0] ESTIMATE"),
                Arguments.of("new labeled format",
                        "[MIN: 0.0, MAX: 1000.0, NULLS: 0.1, ROS: 16.0, NDV: 200.0] ESTIMATE")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nullFractionFormats")
    public void testParseStatisticsWithNullFraction(String formatName, String format) {
        ColumnStatistic stats = ColumnStatistic.buildFrom(format).build();

        assertThat(stats.getMinValue()).isEqualTo(0.0);
        assertThat(stats.getMaxValue()).isEqualTo(1000.0);
        assertThat(stats.getNullsFraction()).isEqualTo(0.1);
        assertThat(stats.getAverageRowSize()).isEqualTo(16.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(200.0);
    }

    private static Stream<Arguments> invalidRangeFormats() {
        return Stream.of(
                Arguments.of("old format", "[100.0, 1.0, 0.0, 8.0, 50.0] ESTIMATE"),
                Arguments.of("new labeled format", "[MIN: 100.0, MAX: 1.0, NULLS: 0.0, ROS: 8.0, NDV: 50.0] ESTIMATE")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRangeFormats")
    public void testParseStatisticsWithInvalidRange(String formatName, String format) {
        // When minValue > maxValue, they should be reset to -Infinity and +Infinity
        ColumnStatistic stats = ColumnStatistic.buildFrom(format).build();

        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    private static Stream<Arguments> negativeNdvFormats() {
        return Stream.of(
                Arguments.of("old format", "[1.0, 100.0, 0.0, 8.0, -5.0] ESTIMATE"),
                Arguments.of("new labeled format", "[MIN: 1.0, MAX: 100.0, NULLS: 0.0, ROS: 8.0, NDV: 0.0] ESTIMATE")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("negativeNdvFormats")
    public void testParseStatisticsWithNegativeDistinctValues(String formatName, String format) {
        // When distinctValues <= 0, it should be set to 1
        ColumnStatistic stats = ColumnStatistic.buildFrom(format).build();

        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0);
    }

    private static Stream<Arguments> scientificNotationFormats() {
        return Stream.of(
                Arguments.of("old format", "[1.0E-10, 1.0E10, 0.0, 8.0, 1.0E5] ESTIMATE"),
                Arguments.of("new labeled format",
                        "[MIN: 1.0E-10, MAX: 1.0E10, NULLS: 0.0, ROS: 8.0, NDV: 1.0E5] ESTIMATE")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("scientificNotationFormats")
    public void testParseStatisticsWithScientificNotation(String formatName, String format) {
        ColumnStatistic stats = ColumnStatistic.buildFrom(format).build();

        assertThat(stats.getMinValue()).isEqualTo(1.0E-10);
        assertThat(stats.getMaxValue()).isEqualTo(1.0E10);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(1.0E5);
    }

    @Test
    public void testBackwardCompatibilityBothFormatsProduceSameResult() {
        // Ensure both formats produce the same ColumnStatistic object
        String oldFormat = "[1.0, 100.0, 0.1, 8.0, 50.0] ESTIMATE";
        String newFormat = "[MIN: 1.0, MAX: 100.0, NULLS: 0.1, ROS: 8.0, NDV: 50.0] ESTIMATE";

        ColumnStatistic oldStats = ColumnStatistic.buildFrom(oldFormat).build();
        ColumnStatistic newStats = ColumnStatistic.buildFrom(newFormat).build();

        assertThat(oldStats.getMinValue()).isEqualTo(newStats.getMinValue());
        assertThat(oldStats.getMaxValue()).isEqualTo(newStats.getMaxValue());
        assertThat(oldStats.getNullsFraction()).isEqualTo(newStats.getNullsFraction());
        assertThat(oldStats.getAverageRowSize()).isEqualTo(newStats.getAverageRowSize());
        assertThat(oldStats.getDistinctValuesCount()).isEqualTo(newStats.getDistinctValuesCount());
        assertThat(oldStats.getType()).isEqualTo(newStats.getType());
    }

    private static Stream<Arguments> queryDumpFormats() {
        return Stream.of(
                Arguments.of("old format",
                        "\"id\": \"[1.0, 100.0, 0.0, 4.0, 100.0] ESTIMATE\""),
                Arguments.of("new labeled format",
                        "\"id\": \"[MIN: 1.0, MAX: 100.0, NULLS: 0.0, ROS: 4.0, NDV: 100.0] ESTIMATE\"")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("queryDumpFormats")
    public void testDeserializeQueryDumpWithStatistics(String formatName, String columnStatistics) {
        // Simulate a query dump JSON with statistics
        String queryDumpJson = "{"
                + "\"statement\": \"select * from t1\","
                + "\"table_meta\": {\"test.t1\": \"CREATE TABLE t1 (id INT)\"},"
                + "\"table_row_count\": {\"test.t1\": {\"t1\": 1000}},"
                + "\"column_statistics\": {"
                + "  \"test.t1\": {"
                + "    " + columnStatistics
                + "  }"
                + "},"
                + "\"be_number\": 3"
                + "}";

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpDeserializer())
                .create();

        QueryDumpInfo dumpInfo = gson.fromJson(queryDumpJson, QueryDumpInfo.class);

        assertThat(dumpInfo).isNotNull();
        assertThat(dumpInfo.getOriginStmt()).isEqualTo("select * from t1");
        assertThat(dumpInfo.getTableStatisticsMap()).containsKey("test.t1");

        ColumnStatistic stats = dumpInfo.getTableStatisticsMap().get("test.t1").get("id");
        assertThat(stats.getMinValue()).isEqualTo(1.0);
        assertThat(stats.getMaxValue()).isEqualTo(100.0);
        assertThat(stats.getNullsFraction()).isEqualTo(0.0);
        assertThat(stats.getAverageRowSize()).isEqualTo(4.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(100.0);
    }

    @Test
    public void testDeserializeMixedFormatStatistics() {
        // Simulate a query dump JSON with both old and new format statistics in different columns
        String queryDumpJson = "{"
                + "\"statement\": \"select * from t1\","
                + "\"table_meta\": {\"test.t1\": \"CREATE TABLE t1 (id INT, name VARCHAR)\"},"
                + "\"table_row_count\": {\"test.t1\": {\"t1\": 1000}},"
                + "\"column_statistics\": {"
                + "  \"test.t1\": {"
                + "    \"id\": \"[1.0, 100.0, 0.0, 4.0, 100.0] ESTIMATE\","
                + "    \"name\": \"[MIN: 0.0, MAX: 1000.0, NULLS: 0.1, ROS: 20.0, NDV: 500.0] ESTIMATE\""
                + "  }"
                + "},"
                + "\"be_number\": 3"
                + "}";

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpDeserializer())
                .create();

        QueryDumpInfo dumpInfo = gson.fromJson(queryDumpJson, QueryDumpInfo.class);

        assertThat(dumpInfo).isNotNull();

        // Verify old format column (id)
        ColumnStatistic idStats = dumpInfo.getTableStatisticsMap().get("test.t1").get("id");
        assertThat(idStats.getMinValue()).isEqualTo(1.0);
        assertThat(idStats.getMaxValue()).isEqualTo(100.0);
        assertThat(idStats.getDistinctValuesCount()).isEqualTo(100.0);

        // Verify new labeled format column (name)
        ColumnStatistic nameStats = dumpInfo.getTableStatisticsMap().get("test.t1").get("name");
        assertThat(nameStats.getMinValue()).isEqualTo(0.0);
        assertThat(nameStats.getMaxValue()).isEqualTo(1000.0);
        assertThat(nameStats.getNullsFraction()).isEqualTo(0.1);
        assertThat(nameStats.getDistinctValuesCount()).isEqualTo(500.0);
    }

    @Test
    public void testDeserializeStructuredObjectStatistics() {
        // New structured (name-keyed) format written by the current serializer.
        String queryDumpJson = "{"
                + "\"statement\": \"select * from t1\","
                + "\"table_meta\": {\"test.t1\": \"CREATE TABLE t1 (id INT)\"},"
                + "\"table_row_count\": {\"test.t1\": {\"t1\": 1000}},"
                + "\"column_statistics\": {"
                + "  \"test.t1\": {"
                + "    \"id\": {\"version\": 1, \"min\": \"1.0\", \"max\": \"100.0\", \"nullsFraction\": \"0.0\","
                + "             \"averageRowSize\": \"4.0\", \"distinctValuesCount\": \"100.0\","
                + "             \"collectionSize\": \"7.0\", \"type\": \"ESTIMATE\"}"
                + "  }"
                + "},"
                + "\"be_number\": 3"
                + "}";

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpDeserializer())
                .create();

        QueryDumpInfo dumpInfo = gson.fromJson(queryDumpJson, QueryDumpInfo.class);

        ColumnStatistic stats = dumpInfo.getTableStatisticsMap().get("test.t1").get("id");
        assertThat(stats.getMinValue()).isEqualTo(1.0);
        assertThat(stats.getMaxValue()).isEqualTo(100.0);
        assertThat(stats.getNullsFraction()).isEqualTo(0.0);
        assertThat(stats.getAverageRowSize()).isEqualTo(4.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(100.0);
        assertThat(stats.getCollectionSize()).isEqualTo(7.0);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.ESTIMATE);
    }

    @Test
    public void testDeserializeStructuredObjectWithInfinities() {
        // Infinities are stored as strings so the JSON stays valid; verify they parse back.
        String queryDumpJson = "{"
                + "\"statement\": \"select * from t1\","
                + "\"table_meta\": {\"test.t1\": \"CREATE TABLE t1 (id INT)\"},"
                + "\"table_row_count\": {\"test.t1\": {\"t1\": 1000}},"
                + "\"column_statistics\": {"
                + "  \"test.t1\": {"
                + "    \"id\": {\"version\": 1, \"min\": \"-Infinity\", \"max\": \"Infinity\", \"nullsFraction\": \"0.0\","
                + "             \"averageRowSize\": \"1.0\", \"distinctValuesCount\": \"1.0\", \"type\": \"UNKNOWN\"}"
                + "  }"
                + "},"
                + "\"be_number\": 3"
                + "}";

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpDeserializer())
                .create();

        QueryDumpInfo dumpInfo = gson.fromJson(queryDumpJson, QueryDumpInfo.class);

        ColumnStatistic stats = dumpInfo.getTableStatisticsMap().get("test.t1").get("id");
        assertThat(stats.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(stats.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(stats.getType()).isEqualTo(ColumnStatistic.StatisticType.UNKNOWN);
    }

    @Test
    public void testHistogramSerializeDeserializeRoundTrip() {
        // A bucket without distinctCount (4-element) and one with it (5-element), plus MCV entries.
        List<Bucket> buckets = Lists.newArrayList(
                new Bucket(1.0, 10.0, 100L, 5L),
                new Bucket(11.0, 20.0, 200L, 8L, 7L));
        Map<String, Long> mcv = ImmutableMap.of("apple", 30L, "banana", 20L);
        Histogram original = new Histogram(buckets, mcv);

        Histogram roundTrip = HistogramUtils.deserializeHistogram(HistogramUtils.serializeHistogram(original));

        assertThat(roundTrip.getBuckets()).hasSize(2);
        Bucket b0 = roundTrip.getBuckets().get(0);
        assertThat(b0.getLower()).isEqualTo(1.0);
        assertThat(b0.getUpper()).isEqualTo(10.0);
        assertThat(b0.getCount()).isEqualTo(100L);
        assertThat(b0.getUpperRepeats()).isEqualTo(5L);
        assertThat(b0.getDistinctCount()).isEmpty();
        Bucket b1 = roundTrip.getBuckets().get(1);
        assertThat(b1.getLower()).isEqualTo(11.0);
        assertThat(b1.getUpper()).isEqualTo(20.0);
        assertThat(b1.getCount()).isEqualTo(200L);
        assertThat(b1.getUpperRepeats()).isEqualTo(8L);
        assertThat(b1.getDistinctCount()).hasValue(7L);
        assertThat(roundTrip.getMCV()).hasSize(2).containsEntry("apple", 30L).containsEntry("banana", 20L);
    }

    @Test
    public void testDeserializeQueryDumpWithHistogram() {
        // Build a dump whose column_histogram section carries the serialized histogram, then verify the
        // deserializer merges it back onto the column statistic parsed from column_statistics.
        List<Bucket> buckets = Lists.newArrayList(
                new Bucket(1.0, 10.0, 100L, 5L),
                new Bucket(11.0, 20.0, 200L, 8L, 7L));
        Histogram original = new Histogram(buckets, ImmutableMap.of("5", 30L, "15", 20L));

        JsonObject dump = new JsonObject();
        dump.addProperty("statement", "select * from t1");
        JsonObject tableMeta = new JsonObject();
        tableMeta.addProperty("test.t1", "CREATE TABLE t1 (id INT)");
        dump.add("table_meta", tableMeta);
        JsonObject rowCount = new JsonObject();
        JsonObject partitionRowCount = new JsonObject();
        partitionRowCount.addProperty("t1", 1000L);
        rowCount.add("test.t1", partitionRowCount);
        dump.add("table_row_count", rowCount);
        JsonObject columnStatistics = new JsonObject();
        JsonObject t1ColumnStatistics = new JsonObject();
        t1ColumnStatistics.addProperty("id", "[1.0, 100.0, 0.0, 4.0, 100.0] ESTIMATE");
        columnStatistics.add("test.t1", t1ColumnStatistics);
        dump.add("column_statistics", columnStatistics);
        JsonObject columnHistogram = new JsonObject();
        JsonObject t1ColumnHistogram = new JsonObject();
        t1ColumnHistogram.addProperty("id", HistogramUtils.serializeHistogram(original));
        columnHistogram.add("test.t1", t1ColumnHistogram);
        dump.add("column_histogram", columnHistogram);
        dump.addProperty("be_number", 3);

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpDeserializer())
                .create();
        QueryDumpInfo dumpInfo = gson.fromJson(dump, QueryDumpInfo.class);

        ColumnStatistic stats = dumpInfo.getTableStatisticsMap().get("test.t1").get("id");
        // The base statistic parsed from column_statistics must be preserved.
        assertThat(stats.getMinValue()).isEqualTo(1.0);
        assertThat(stats.getMaxValue()).isEqualTo(100.0);
        assertThat(stats.getDistinctValuesCount()).isEqualTo(100.0);
        // The histogram must be merged back on with buckets and MCV intact.
        Histogram histogram = stats.getHistogram();
        assertThat(histogram).isNotNull();
        assertThat(histogram.getBuckets()).hasSize(2);
        assertThat(histogram.getBuckets().get(1).getDistinctCount()).hasValue(7L);
        assertThat(histogram.getMCV()).hasSize(2).containsEntry("5", 30L).containsEntry("15", 20L);
    }

    @Test
    public void testBuildFromStatisticStringWithCollectionSizeAndMcvPrefix() {
        // ColumnStatistic.toString() emits "COS: .. MCV: [..]" before the trailing type token when the
        // statistic carries a collection size and/or histogram. buildFrom must recover the trailing type
        // instead of throwing on StatisticType.valueOf over the whole tail (the pre-fix behavior).
        String withCosAndMcv = "[1.0, 100.0, 0.0, 8.0, 50.0] COS: 5.0 MCV: [[5:10][15:8]] ESTIMATE";
        ColumnStatistic parsed = ColumnStatistic.buildFrom(withCosAndMcv).build();
        assertThat(parsed.getType()).isEqualTo(ColumnStatistic.StatisticType.ESTIMATE);
        assertThat(parsed.getMinValue()).isEqualTo(1.0);
        assertThat(parsed.getMaxValue()).isEqualTo(100.0);
        assertThat(parsed.getDistinctValuesCount()).isEqualTo(50.0);

        // MCV-only tail (no collection size) must parse too.
        String withMcvOnly = "[1.0, 100.0, 0.0, 8.0, 50.0] MCV: [[5:10]] ESTIMATE";
        assertThat(ColumnStatistic.buildFrom(withMcvOnly).build().getType())
                .isEqualTo(ColumnStatistic.StatisticType.ESTIMATE);
    }
}
