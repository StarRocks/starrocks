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

import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class StatisticsEstimateUtilsTest {
    @BeforeAll
    public static void beforeAll() {
        UtFrameUtils.createDefaultCtx();
    }

    @Test
    void itShouldMergeMcvsFromBothChildrenForUnion() {
        // GIVEN
        final var leftHistogram = new Histogram(List.of(), Map.of("1", 5000L, "2", 3000L));
        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(leftHistogram)
                .build();

        final var rightHistogram = new Histogram(List.of(), Map.of("3", 4000L, "4", 2000L));
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(rightHistogram)
                .build();

        // WHEN
        final var result = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 10_000, rightColStats, 20_000);

        // THEN
        assertNotNull(result.getHistogram());
        final var mcv = result.getHistogram().getMCV();
        assertEquals(4, mcv.size());
        assertEquals(5000L, mcv.get("1"));
        assertEquals(3000L, mcv.get("2"));
        assertEquals(4000L, mcv.get("3"));
        assertEquals(2000L, mcv.get("4"));
    }

    @Test
    void itShouldSumMcvFrequenciesForOverlappingKeysForUnion() {
        // GIVEN
        final var leftHistogram = new Histogram(List.of(), Map.of("1", 5000L, "2", 3000L));
        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(leftHistogram)
                .build();

        final var rightHistogram = new Histogram(List.of(), Map.of("1", 7000L, "3", 2000L));
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(rightHistogram)
                .build();

        // WHEN
        final var result = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 10_000, rightColStats, 20_000);

        // THEN
        assertNotNull(result.getHistogram());
        final var mcv = result.getHistogram().getMCV();
        assertEquals(3, mcv.size());
        assertEquals(12000L, mcv.get("1")); // 5000 + 7000
        assertEquals(3000L, mcv.get("2"));
        assertEquals(2000L, mcv.get("3"));
    }

    @Test
    void itShouldPropagateMcvsWhenOnlyOneChildHasHistogramForUnion() {
        // GIVEN
        final var histogram = new Histogram(List.of(), Map.of("1", 5000L, "2", 3000L));
        final var statsWithHistogram = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(histogram)
                .build();

        final var statsWithoutHistogram = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .build();

        // WHEN
        var result = StatisticsEstimateUtils.unionColumnStatistic(
                statsWithHistogram, 10_000, statsWithoutHistogram, 20_000);

        // THEN
        assertNotNull(result.getHistogram());
        var mcv = result.getHistogram().getMCV();
        assertEquals(2, mcv.size());
        assertEquals(5000L, mcv.get("1"));
        assertEquals(3000L, mcv.get("2"));

        // WHEN
        result = StatisticsEstimateUtils.unionColumnStatistic(
                statsWithoutHistogram, 10_000, statsWithHistogram, 20_000);

        // THEN
        assertNotNull(result.getHistogram());
        mcv = result.getHistogram().getMCV();
        assertEquals(2, mcv.size());
        assertEquals(5000L, mcv.get("1"));
        assertEquals(3000L, mcv.get("2"));
    }

    @Test
    void itShouldReturnNoHistogramWhenNeitherChildHasMcvsForUnion() {
        // GIVEN
        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .build();

        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .build();

        // WHEN
        final var result = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 10_000, rightColStats, 20_000);

        // THEN
        assertNull(result.getHistogram());
    }

    @Test
    void itShouldReturnNoHistogramWhenBothChildrenHaveEmptyMcvsForUnion() {
        // GIVEN
        final var leftHistogram = new Histogram(
                List.of(new Bucket(0, 10, 100L, 10L)), Map.of());
        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(0).setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(leftHistogram)
                .build();

        final var rightHistogram = new Histogram(
                List.of(new Bucket(0, 20, 200L, 20L)), Map.of());
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(0).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(rightHistogram)
                .build();

        // WHEN
        final var result = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 10_000, rightColStats, 20_000);

        // THEN
        assertNull(result.getHistogram());
    }

    @Test
    void itShouldPropagateHistogramWhenNoBucketsEvenIfMcvsBelowThreshold() {
        // GIVEN
        final var leftHistogram = new Histogram(List.of(), Map.of("1", 1000L));
        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(leftHistogram)
                .build();

        final var rightHistogram = new Histogram(List.of(), Map.of("2", 1000L));
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(1)
                .setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(rightHistogram)
                .build();

        // WHEN
        final var result = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 5_000, rightColStats, 5_000);

        // THEN
        assertNotNull(result.getHistogram());
        final var mcv = result.getHistogram().getMCV();
        assertEquals(2, mcv.size());
        assertEquals(1000L, mcv.get("1"));
        assertEquals(1000L, mcv.get("2"));
    }

    @Test
    void itShouldPropagateHistogramWhenBucketsExistButMcvsAboveThreshold() {
        // GIVEN
        final var leftHistogram = new Histogram(
                List.of(new Bucket(0, 10, 100L, 10L)),
                Map.of("1", 4000L, "2", 3000L));
        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(0)
                .setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(leftHistogram)
                .build();

        final var rightHistogram = new Histogram(
                List.of(new Bucket(0, 20, 200L, 20L)),
                Map.of("1", 6000L, "3", 2000L));
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(0)
                .setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(rightHistogram)
                .build();

        // WHEN
        final var result = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 10_000, rightColStats, 10_000);

        // THEN
        assertNotNull(result.getHistogram());
        final var mcv = result.getHistogram().getMCV();
        assertEquals(3, mcv.size());
        assertEquals(10000L, mcv.get("1")); // 4000 + 6000
        assertEquals(3000L, mcv.get("2"));
        assertEquals(2000L, mcv.get("3"));
    }

    @Test
    void itShouldNotPropagateHistogramWhenBucketsExistAndMcvsBelowThreshold() {
        // GIVEN
        final var leftHistogram = new Histogram(
                List.of(new Bucket(0, 10, 100L, 10L)),
                Map.of("1", 500L));
        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(0).setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(leftHistogram)
                .build();

        final var rightHistogram = new Histogram(
                List.of(new Bucket(0, 20, 200L, 20L)),
                Map.of("2", 500L));
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(0).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(rightHistogram)
                .build();

        // WHEN
        final var result = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 5_000, rightColStats, 5_000);

        // THEN
        assertNull(result.getHistogram());
    }

    @Test
    void itShouldCarryNonMcvRowsInUnionBucket() {
        // Given a union of 10000 and 20000 rows with no nulls, whose merged MCVs hold 14000 rows,
        // over the merged range [1, 200]
        // CASE WHEN the union propagates a histogram THEN it carries the rows outside the MCVs in a
        // bucket over the merged range, so the union's total row count is no longer just its MCV rows END

        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(new Histogram(List.of(), Map.of("1", 5000L, "2", 3000L)))
                .build();
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(new Histogram(List.of(), Map.of("3", 4000L, "4", 2000L)))
                .build();
        final var expectedLowerBound = 1.0;
        final var expectedUpperBound = 200.0;
        final var expectedNonMcvRows = 16_000L;
        final var expectedUpperRepeats = 0L;
        final var expectedTotalRows = 30_000L;

        final var actualHistogram = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 10_000, rightColStats, 20_000).getHistogram();

        final var actualBuckets = actualHistogram.getBuckets();
        assertEquals(1, actualBuckets.size());
        assertEquals(expectedLowerBound, actualBuckets.get(0).getLower());
        assertEquals(expectedUpperBound, actualBuckets.get(0).getUpper());
        assertEquals(expectedNonMcvRows, actualBuckets.get(0).getCount());
        assertEquals(expectedUpperRepeats, actualBuckets.get(0).getUpperRepeats());
        assertEquals(expectedTotalRows, actualHistogram.getTotalRows());
    }

    @Test
    void itShouldExcludeNullsFromTheUnionBucket() {
        // Given a union of 10000 rows that are half null and 20000 rows with no nulls, so 25000 of
        // the 30000 union rows are non-null, whose merged MCVs hold 14000 rows
        // CASE WHEN the union propagates a histogram THEN the bucket counts only non-null rows
        // outside the MCVs, because an MCV key is a value and so never counts a null END

        final var leftColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(100)
                .setNullsFraction(0.5)
                .setAverageRowSize(4)
                .setDistinctValuesCount(50)
                .setHistogram(new Histogram(List.of(), Map.of("1", 5000L, "2", 3000L)))
                .build();
        final var rightColStats = ColumnStatistic.builder()
                .setMinValue(1).setMaxValue(200)
                .setNullsFraction(0.0)
                .setAverageRowSize(4)
                .setDistinctValuesCount(80)
                .setHistogram(new Histogram(List.of(), Map.of("3", 4000L, "4", 2000L)))
                .build();
        final var expectedNonMcvRows = 11_000L;
        final var expectedTotalRows = 25_000L;

        final var actualHistogram = StatisticsEstimateUtils.unionColumnStatistic(
                leftColStats, 10_000, rightColStats, 20_000).getHistogram();

        assertEquals(expectedNonMcvRows, actualHistogram.getBuckets().get(0).getCount());
        assertEquals(expectedTotalRows, actualHistogram.getTotalRows());
    }
}
