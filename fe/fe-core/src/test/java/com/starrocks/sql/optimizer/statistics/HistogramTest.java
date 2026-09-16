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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HistogramTest {

    @Test
    public void testNoBucketsStayEmpty() {
        // Given a histogram constructed without any bucket
        // CASE WHEN no bucket is passed THEN getBuckets() stays empty, which is how a reader knows
        // there are no rows outside the MCVs ELSE the passed buckets are kept as-is END

        final Map<String, Long> mcv = Map.of("1", 100L, "2", 50L);

        final List<Bucket> actualBuckets = new Histogram(List.of(), mcv).getBuckets();

        Assertions.assertTrue(actualBuckets.isEmpty());
    }

    @Test
    public void testPassedBucketsAreKept() {
        // Given a histogram constructed with two collected buckets
        // CASE WHEN buckets are passed THEN getBuckets() returns exactly those buckets END

        final Bucket firstBucket = new Bucket(0, 10, 100L, 5L);
        final Bucket secondBucket = new Bucket(10, 20, 250L, 5L);
        final List<Bucket> expectedBuckets = List.of(firstBucket, secondBucket);

        final List<Bucket> actualBuckets = new Histogram(expectedBuckets, Map.of("1", 100L)).getBuckets();

        Assertions.assertEquals(expectedBuckets, actualBuckets);
    }

    @Test
    public void testTotalRowsWithoutBuckets() {
        // Given a histogram constructed without any bucket, whose MCVs hold 100 and 50 rows
        // CASE WHEN no bucket is passed THEN getTotalRows() reports the MCV rows only, because the
        // rows outside the MCVs are unknown ELSE the last bucket's cumulative count is added END

        final Map<String, Long> mcv = Map.of("1", 100L, "2", 50L);
        final long expectedTotalRows = 150L;

        final long actualTotalRows = new Histogram(List.of(), mcv).getTotalRows();

        Assertions.assertEquals(expectedTotalRows, actualTotalRows);
    }

    @Test
    public void testTotalRowsWithBuckets() {
        // Given a histogram whose last bucket carries a cumulative 250 non-MCV rows and whose MCVs
        // hold 100 rows
        // CASE WHEN buckets are passed THEN getTotalRows() adds the last cumulative count to the MCV
        // rows, since bucket counts exclude MCV rows END

        final List<Bucket> buckets = List.of(new Bucket(0, 10, 100L, 5L), new Bucket(10, 20, 250L, 5L));
        final Map<String, Long> mcv = Map.of("1", 100L);
        final long expectedTotalRows = 350L;

        final long actualTotalRows = new Histogram(buckets, mcv).getTotalRows();

        Assertions.assertEquals(expectedTotalRows, actualTotalRows);
    }

    @Test
    public void testTotalRowsWithoutBucketsOrMcv() {
        // Given a histogram constructed from null arguments
        // CASE WHEN neither buckets nor MCVs are passed THEN getTotalRows() floors at one row END

        final long expectedTotalRows = 1L;

        final long actualTotalRows = new Histogram(null, null).getTotalRows();

        Assertions.assertEquals(expectedTotalRows, actualTotalRows);
    }

    @Test
    public void testRowCountInBucketWithoutBuckets() {
        // Given a histogram constructed without any bucket
        // CASE WHEN a value is looked up against the placeholder THEN no row count is returned,
        // because a placeholder carrying no rows must not be read as an estimate END

        final Map<String, Long> mcv = Map.of("1", 100L);
        final double lookupValue = 42.0;
        final double distinctValuesCount = 10.0;

        final Optional<Long> actualRowCount =
                new Histogram(List.of(), mcv).getRowCountInBucket(lookupValue, distinctValuesCount, true);

        Assertions.assertTrue(actualRowCount.isEmpty());
    }

    @Test
    public void testRowCountInBucketWithBuckets() {
        // Given a histogram with one collected bucket over [0, 20) holding 200 non-MCV rows and 10
        // distinct values
        // CASE WHEN a value inside the bucket is looked up THEN the bucket's rows are spread over its
        // distinct values END

        final List<Bucket> buckets = List.of(new Bucket(0, 20, 200L, 0L));
        final double lookupValue = 5.0;
        final double distinctValuesCount = 10.0;
        final long expectedRowCount = 20L;

        final Optional<Long> actualRowCount =
                new Histogram(buckets, Map.of()).getRowCountInBucket(lookupValue, distinctValuesCount, false);

        Assertions.assertEquals(Optional.of(expectedRowCount), actualRowCount);
    }
}
