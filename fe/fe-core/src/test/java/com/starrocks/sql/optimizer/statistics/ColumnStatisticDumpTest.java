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

import com.google.gson.JsonElement;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ColumnStatisticDumpTest {

    private static ColumnStatistic roundTrip(ColumnStatistic input) {
        ColumnStatisticDump dump = ColumnStatisticDump.from(input);
        JsonElement tree = GsonUtils.GSON.toJsonTree(dump);
        ColumnStatisticDump parsed = GsonUtils.GSON.fromJson(tree, ColumnStatisticDump.class);
        return parsed.toColumnStatistic();
    }

    @Test
    public void testBasicRoundTrip() {
        ColumnStatistic input = ColumnStatistic.builder()
                .setMinValue(1.0)
                .setMaxValue(100.0)
                .setNullsFraction(0.1)
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(50.0)
                .setType(ColumnStatistic.StatisticType.ESTIMATE)
                .build();

        ColumnStatistic result = roundTrip(input);

        assertThat(result.getMinValue()).isEqualTo(1.0);
        assertThat(result.getMaxValue()).isEqualTo(100.0);
        assertThat(result.getNullsFraction()).isEqualTo(0.1);
        assertThat(result.getAverageRowSize()).isEqualTo(8.0);
        assertThat(result.getDistinctValuesCount()).isEqualTo(50.0);
        assertThat(result.getType()).isEqualTo(ColumnStatistic.StatisticType.ESTIMATE);
    }

    @Test
    public void testUnknownWithInfinitiesRoundTrip() {
        // This is the case that used to break: infinities are not valid JSON numbers, so they must
        // be serialized as strings. Also verifies the shared Gson never sees a special float.
        ColumnStatistic input = ColumnStatistic.unknown();

        String json = GsonUtils.GSON.toJson(ColumnStatisticDump.from(input));
        assertThat(json).contains("-Infinity").contains("Infinity");

        ColumnStatistic result = roundTrip(input);
        assertThat(result.getMinValue()).isEqualTo(Double.NEGATIVE_INFINITY);
        assertThat(result.getMaxValue()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(result.isUnknown()).isTrue();
        assertThat(result.getType()).isEqualTo(ColumnStatistic.StatisticType.UNKNOWN);
    }

    @Test
    public void testNaNRoundTrip() {
        ColumnStatistic input = ColumnStatistic.builder()
                .setMinValue(Double.NaN)
                .setMaxValue(Double.NaN)
                .setNullsFraction(0.0)
                .setAverageRowSize(1.0)
                .setDistinctValuesCount(1.0)
                .build();

        ColumnStatistic result = roundTrip(input);
        assertThat(result.hasNaNValue()).isTrue();
    }

    @Test
    public void testCollectionSizePreserved() {
        ColumnStatistic input = ColumnStatistic.builder()
                .setMinValue(1.0)
                .setMaxValue(2.0)
                .setNullsFraction(0.0)
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(2.0)
                .setCollectionSize(7.0)
                .build();

        // Collection size used to be silently dropped by the text round-trip.
        ColumnStatistic result = roundTrip(input);
        assertThat(result.getCollectionSize()).isEqualTo(7.0);
    }

    @Test
    public void testDefaultCollectionSizePreserved() {
        ColumnStatistic input = ColumnStatistic.builder()
                .setMinValue(1.0)
                .setMaxValue(2.0)
                .setNullsFraction(0.0)
                .setAverageRowSize(4.0)
                .setDistinctValuesCount(2.0)
                .build();

        ColumnStatistic result = roundTrip(input);
        assertThat(result.getCollectionSize()).isEqualTo(ColumnStatistic.DEFAULT_COLLECTION_SIZE);
    }

    @Test
    public void testHistogramRoundTrip() {
        List<Bucket> buckets = List.of(
                new Bucket(1.0, 10.0, 100L, 5L),
                new Bucket(10.0, 20.0, 200L, 3L, 7L));
        Map<String, Long> mcv = new LinkedHashMap<>();
        mcv.put("a", 50L);
        mcv.put("b", 30L);
        Histogram histogram = new Histogram(buckets, mcv);

        ColumnStatistic input = ColumnStatistic.builder()
                .setMinValue(1.0)
                .setMaxValue(20.0)
                .setNullsFraction(0.0)
                .setAverageRowSize(8.0)
                .setDistinctValuesCount(9.0)
                .setHistogram(histogram)
                .build();

        // The histogram used to be dropped entirely on read; verify buckets + full MCV survive.
        ColumnStatistic result = roundTrip(input);
        Histogram resultHistogram = result.getHistogram();
        assertThat(resultHistogram).isNotNull();

        assertThat(resultHistogram.getMCV()).containsEntry("a", 50L).containsEntry("b", 30L);

        List<Bucket> resultBuckets = resultHistogram.getBuckets();
        assertThat(resultBuckets).hasSize(2);

        assertThat(resultBuckets.get(0).getLower()).isEqualTo(1.0);
        assertThat(resultBuckets.get(0).getUpper()).isEqualTo(10.0);
        assertThat(resultBuckets.get(0).getCount()).isEqualTo(100L);
        assertThat(resultBuckets.get(0).getUpperRepeats()).isEqualTo(5L);
        assertThat(resultBuckets.get(0).getDistinctCount()).isEmpty();

        assertThat(resultBuckets.get(1).getDistinctCount()).contains(7L);
    }

    @Test
    public void testVersionIsWritten() {
        ColumnStatisticDump dump = ColumnStatisticDump.from(ColumnStatistic.unknown());
        assertThat(dump.version).isEqualTo(ColumnStatisticDump.CURRENT_VERSION);
    }
}

