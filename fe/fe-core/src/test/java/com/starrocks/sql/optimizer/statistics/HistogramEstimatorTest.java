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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HistogramEstimatorTest {

    @ParameterizedTest
    @MethodSource("provideTestCases")
    public void testEstimateEqualToSelectivity(
            ColumnStatistic left, ColumnStatistic right, Double expectedSelectivity) {
        Double actualSelectivity = HistogramEstimator.estimateEqualToSelectivity(left, right);
        if (expectedSelectivity == null) {
            Assertions.assertNull(actualSelectivity);
        } else {
            Assertions.assertNotNull(actualSelectivity);
            assertEquals(expectedSelectivity, actualSelectivity, 0.01);
        }
    }

    private static Stream<Arguments> provideTestCases() {
        return Stream.of(
                // Normal case: overlapping histograms
                Arguments.of(
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}),
                        createColumnStatistic(new double[] {3, 7, 12}, new long[] {150, 250}),
                        0.81),
                Arguments.of(
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {1, 2}),
                        createColumnStatistic(new double[] {3, 7, 12}, new long[] {150, 250}),
                        0.83),
                Arguments.of(
                        createColumnStatistic(new double[] {3, 7, 12}, new long[] {150, 250}),
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}),
                        0.61),

                // Normal case: diverse bucket
                Arguments.of(
                        createColumnStatistic(new double[] {1, 100, 200, 300, 400}, new long[] {100, 200, 200, 400}),
                        createColumnStatistic(new double[] {1, 200, 400}, new long[] {150, 250}),
                        0.44),

                // Normal case: lots of buckets, but the range is same
                Arguments.of(
                        createColumnStatistic(createUniformedHistogram(100, 1024, 1 << 16)),
                        createColumnStatistic(createUniformedHistogram(100, 1024, 1 << 16)),
                        1.0),
                Arguments.of(
                        createColumnStatistic(createUniformedHistogram(100, 1024, 1 << 10)),
                        createColumnStatistic(createUniformedHistogram(100, 1024, 1 << 16)),
                        1.0),
                Arguments.of(
                        createColumnStatistic(createUniformedHistogram(100, 1024, 1 << 10)),
                        createColumnStatistic(createUniformedHistogram(800, 128, 1 << 16)),
                        1.0),
                Arguments.of(
                        createColumnStatistic(createUniformedHistogram(100, 1024, 1 << 10)),
                        createColumnStatistic(createUniformedHistogram(10, 10240, 1 << 16)),
                        1.0),

                // Completely overlapping histograms
                Arguments.of(
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}),
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}),
                        1.0),
                Arguments.of(
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {10, 20}),
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}),
                        1.0),

                // Non-overlapping histograms
                Arguments.of(
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}),
                        createColumnStatistic(new double[] {15, 20, 25}, new long[] {150, 250}),
                        0.0),

                // One empty histogram
                Arguments.of(
                        createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}),
                        createColumnStatistic(),
                        null),
                // Both empty histograms
                Arguments.of(createColumnStatistic(), createColumnStatistic(), null),
                // One null histogram
                Arguments.of(createColumnStatistic(new double[] {1, 5, 10}, new long[] {100, 200}), null, null));
    }

    private static Histogram createUniformedHistogram(int numBuckets, double bucketRange, long perBucketCount) {
        Histogram.Builder builder = new Histogram.Builder();
        double lower = 0.0;
        for (int i = 0; i < numBuckets; i++) {
            builder.addBucket(new Bucket(lower, lower + bucketRange, perBucketCount, 1L));
            lower += bucketRange;
        }
        return builder.build();
    }

    // create an empty column statistics
    private static ColumnStatistic createColumnStatistic() {
        return new ColumnStatistic(0, 0, 0, 0, 0, null, ColumnStatistic.StatisticType.ESTIMATE);
    }

    private static ColumnStatistic createColumnStatistic(Histogram hist) {
        return new ColumnStatistic(0, 0, 0, 0, 0, hist, ColumnStatistic.StatisticType.ESTIMATE);
    }

    private static ColumnStatistic createColumnStatistic(double[] bounds, long[] counts) {
        Histogram.Builder builder = new Histogram.Builder();
        for (int i = 0; i < counts.length; i++) {
            builder.addBucket(new Bucket(bounds[i], bounds[i + 1], counts[i], 0L));
        }
        Histogram histogram = builder.build();
        return new ColumnStatistic(0, 0, 0, 0, 0, histogram, ColumnStatistic.StatisticType.ESTIMATE);
    }

    private List<Integer> verifyBucketIndex(Histogram histogram, List<Bucket> buckets) {
        return buckets.stream().map(x -> histogram.getBuckets().indexOf(x)).collect(Collectors.toList());
    }

    @Test
    public void testGetOverlappedBuckets() {
        Histogram histogram = new Histogram(
                Lists.newArrayList(
                        new Bucket(0, 5, 100L, 0L),
                        new Bucket(5, 10, 200L, 0L),
                        new Bucket(10, 15, 300L, 0L),
                        new Bucket(15, 20, 400L, 0L)
                ),
                Maps.newHashMap()
        );

        // totally covered range
        assertEquals(List.of(0, 1, 2, 3), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(0, 100)));
        assertEquals(List.of(0, 1, 2, 3), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(2, 16)));
        assertEquals(List.of(0, 1, 2, 3), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(0, 15)));
        assertEquals(List.of(0, 1, 2, 3), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(5, 17)));

        // partially covered
        assertEquals(List.of(1, 2, 3), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(10, 17)));
        assertEquals(List.of(0, 1), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(1, 6)));
        assertEquals(List.of(0, 1), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(0, 5)));
        assertEquals(List.of(0, 1, 2), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(0, 10)));
        assertEquals(List.of(2, 3), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(15, 20)));

        // boundary overlapped
        assertEquals(List.of(0), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(-1, 0)));
        assertEquals(List.of(3), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(20, 21)));
        assertEquals(List.of(0, 1), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(5, 5)));

        // no overlap
        assertEquals(List.of(), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(30, 100)));
        assertEquals(List.of(), verifyBucketIndex(histogram, histogram.getOverlappedBuckets(-10, -1)));
    }
    
}
