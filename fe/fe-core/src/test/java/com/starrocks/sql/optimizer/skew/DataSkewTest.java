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

package com.starrocks.sql.optimizer.skew;

import com.google.crypto.tink.subtle.Random;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.Bucket;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataSkewTest {

    private static ColumnRefOperator createNewTestColumn() {
        return new ColumnRefOperator(Random.randInt(), IntegerType.INT, UUID.randomUUID().toString(), true);
    }

    @Test
    void itShouldDetectNullSkew() {
        // GIVEN
        final var skewedCol = createNewTestColumn();
        final var skewedColStats = ColumnStatistic.builder()
                .setNullsFraction(0.9 /* heavily null skewed */) //
                .build();

        final var nonSkewedCol = createNewTestColumn();
        final var nonSkewedColStats = ColumnStatistic.builder()
                .setNullsFraction(0.1 /* not null skewed */) //
                .build();

        final var stats = new Statistics.Builder()
                .addColumnStatistic(skewedCol, skewedColStats) //
                .addColumnStatistic(nonSkewedCol, nonSkewedColStats) //
                .build();

        // WHEN / THEN
        assertTrue(DataSkew.isColumnSkewed(stats, skewedColStats));
        assertFalse(DataSkew.isColumnSkewed(stats, nonSkewedColStats));
    }

    @Test
    void itShouldReturnFalseWhenNoStatistics() {
        // GIVEN
        final var col = createNewTestColumn();
        final var colStats = ColumnStatistic.unknown();

        final var stats = new Statistics.Builder()
                .addColumnStatistic(col, colStats) //
                .build();

        // WHEN / THEN
        assertFalse(DataSkew.isColumnSkewed(stats, colStats));
    }

    @Test
    void itShouldRespectCustomThresholds() {
        // GIVEN
        final var skewedCol = createNewTestColumn();
        final var skewedColStats = ColumnStatistic.builder()
                .setNullsFraction(0.7) //
                .build();

        final var nonSkewedCol = createNewTestColumn();
        final var nonSkewedColStats = ColumnStatistic.builder()
                .setNullsFraction(0.6) //
                .build();

        final var stats = new Statistics.Builder()
                .addColumnStatistic(skewedCol, skewedColStats) //
                .addColumnStatistic(nonSkewedCol, nonSkewedColStats) //
                .build();

        // WHEN / THEN
        assertTrue(DataSkew.isColumnSkewed(stats, skewedColStats, new DataSkew.Thresholds(5, 0.7)));
        assertFalse(DataSkew.isColumnSkewed(stats, skewedColStats, new DataSkew.Thresholds(5, 0.71)));
        assertFalse(DataSkew.isColumnSkewed(stats, nonSkewedColStats, new DataSkew.Thresholds(5, 0.7)));
    }

    @Test
    void itShouldDetectHistogramSkew() {
        // GIVEN
        final var skewedHistogram = getSkewedHistogram();
        final var skewedCol = createNewTestColumn();
        final var skewedColStats = ColumnStatistic.builder()
                .setNullsFraction(0.01) //
                .setHistogram(skewedHistogram) //
                .build();

        final var nonSkewedHistogram = getNonSkewedHistogram();
        final var nonSkewedCol = createNewTestColumn();
        final var nonSkewedColStats = ColumnStatistic.builder()
                .setNullsFraction(0.02) //
                .setHistogram(nonSkewedHistogram) //
                .build();

        final var stats = new Statistics.Builder()
                .setOutputRowCount(100_000)
                .addColumnStatistic(skewedCol, skewedColStats) //
                .addColumnStatistic(nonSkewedCol, nonSkewedColStats) //
                .build();

        // WHEN / THEN
        assertTrue(DataSkew.isColumnSkewed(stats, skewedColStats));
        assertFalse(DataSkew.isColumnSkewed(stats, nonSkewedColStats));
    }

    private static Histogram getSkewedHistogram() {
        List<Bucket> skewedBuckets = List.of(
                new Bucket(6, 20, 1000L, 50L), // skew
                new Bucket(21, 100, 1200L, 10L),
                new Bucket(101, 1000, 1250L, 1L),
                new Bucket(1001, 10000, 1260L, 1L)
        );

        Map<String, Long> skewedMcv = Map.of(
                "1", 50000L,  // skew
                "2", 20000L,
                "3", 10000L,
                "4", 5000L,
                "5", 500L
        );

        return new Histogram(skewedBuckets, skewedMcv);
    }

    private static Histogram getNonSkewedHistogram() {
        Map<String, Long> uniformMcv = Map.of(
                "50", 10L,
                "150", 10L,
                "250", 10L,
                "350", 10L
        );

        List<Bucket> uniformBuckets = List.of(
                new Bucket(0, 99, 990L, 10L),
                new Bucket(100, 199, 1980L, 10L),
                new Bucket(200, 299, 2970L, 10L),
                new Bucket(300, 399, 3960L, 10L)
        );

        return new Histogram(uniformBuckets, uniformMcv);
    }
}
