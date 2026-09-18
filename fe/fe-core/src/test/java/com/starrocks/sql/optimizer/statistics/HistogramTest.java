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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Test
    public void testSingleBucketCarriesNonMcvRows() {
        // Given 1000 rows over [100, 200] of which 300 sit in the MCVs
        // CASE WHEN a single-bucket histogram is built THEN the bucket spans the given bounds and
        // carries the rows left outside the MCVs END

        final Map<String, Long> mcv = Map.of("a", 100L, "b", 200L);
        final double lowerBound = 100.0;
        final double upperBound = 200.0;
        final double totalRows = 1000;
        final long expectedNonMcvRows = 700L;
        final long expectedUpperRepeats = 0L;

        final List<Bucket> actualBuckets =
                Histogram.ofSingleBucket(lowerBound, upperBound, totalRows, mcv).getBuckets();

        Assertions.assertEquals(1, actualBuckets.size());
        Assertions.assertEquals(lowerBound, actualBuckets.get(0).getLower());
        Assertions.assertEquals(upperBound, actualBuckets.get(0).getUpper());
        Assertions.assertEquals(expectedNonMcvRows, actualBuckets.get(0).getCount());
        Assertions.assertEquals(expectedUpperRepeats, actualBuckets.get(0).getUpperRepeats());
    }

    @Test
    public void testSingleBucketWithInfiniteBounds() {
        // Given 1000 rows of which 100 sit in the MCVs, over a column whose bounds are unknown
        // CASE WHEN the bounds are not finite THEN the bucket still carries the non-MCV rows, with
        // the inert bounds that statistics collection also writes, so the rows reach getTotalRows()
        // without the bucket answering per-value lookups END

        final Map<String, Long> mcv = Map.of("a", 100L);
        final double totalRows = 1000;
        final long expectedNonMcvRows = 900L;
        final long expectedTotalRows = 1000L;

        final Histogram actualHistogram = Histogram.ofSingleBucket(
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, totalRows, mcv);
        final List<Bucket> actualBuckets = actualHistogram.getBuckets();

        Assertions.assertEquals(1, actualBuckets.size());
        Assertions.assertEquals(Double.POSITIVE_INFINITY, actualBuckets.get(0).getLower());
        Assertions.assertEquals(Double.POSITIVE_INFINITY, actualBuckets.get(0).getUpper());
        Assertions.assertEquals(expectedNonMcvRows, actualBuckets.get(0).getCount());
        Assertions.assertEquals(expectedTotalRows, actualHistogram.getTotalRows());
        Assertions.assertTrue(actualHistogram.getRowCountInBucket(42.0, 10.0, false).isEmpty());
    }

    @Test
    public void testSingleBucketWithNoRows() {
        // Given a total that the MCVs already account for in full, over finite bounds
        // CASE WHEN there are no rows left to carry THEN no bucket is built, because a bucket holding
        // nothing describes neither rows nor position END

        final Map<String, Long> mcv = Map.of("a", 1000L);
        final double totalRows = 1000;
        final long expectedTotalRows = 1000L;

        final Histogram actualHistogram = Histogram.ofSingleBucket(1.0, 2.0, totalRows, mcv);

        Assertions.assertTrue(actualHistogram.getBuckets().isEmpty());
        Assertions.assertEquals(expectedTotalRows, actualHistogram.getTotalRows());
    }

    @Test
    public void testSingleBucketClampsNonMcvRowsAtZero() {
        // Given MCVs holding 1200 rows against a total of 1000, which the collection scales can
        // produce because MCV counts are not rescaled below their source
        // CASE WHEN the MCVs hold more rows than the total THEN the bucket count clamps at zero
        // rather than going negative END

        final Map<String, Long> mcv = Map.of("a", 800L, "b", 400L);
        final double totalRows = 1000;
        final long expectedTotalRows = 1200L;

        final Histogram actualHistogram = Histogram.ofSingleBucket(1.0, 2.0, totalRows, mcv);

        Assertions.assertTrue(actualHistogram.getBuckets().isEmpty());
        Assertions.assertEquals(expectedTotalRows, actualHistogram.getTotalRows());
    }

    @Test
    public void testMcvOnlyConstructor() {
        // Given MCVs holding 100 and 50 rows, which the caller has established are every row
        // CASE WHEN the MCV-only constructor is used THEN the histogram has no buckets and its total
        // row count is the MCV rows END

        final Map<String, Long> mcv = Map.of("1", 100L, "2", 50L);
        final long expectedTotalRows = 150L;

        final Histogram actualHistogram = new Histogram(mcv);

        Assertions.assertTrue(actualHistogram.getBuckets().isEmpty());
        Assertions.assertEquals(expectedTotalRows, actualHistogram.getTotalRows());
    }

    @Test
    public void testEmptyBucketListWarns() {
        // Given a histogram constructed with an empty bucket list
        // CASE WHEN buckets are missing THEN the constructor warns, so whoever wrote the call site
        // learns that the rows outside the MCVs have been lost END

        final Map<String, Long> mcv = Map.of("1", 100L);
        final int expectedWarnCount = 1;

        final int actualWarnCount = warnCountWhile(() -> new Histogram(List.of(), mcv));

        Assertions.assertEquals(expectedWarnCount, actualWarnCount);
    }

    @Test
    public void testMcvOnlyConstructorDoesNotWarn() {
        // Given a histogram constructed through the MCV-only constructor
        // CASE WHEN the caller has accounted for the absent buckets THEN nothing is warned, because
        // there is no missing bucket left to report END

        final Map<String, Long> mcv = Map.of("1", 100L);
        final int expectedWarnCount = 0;

        final int actualWarnCount = warnCountWhile(() -> new Histogram(mcv));

        Assertions.assertEquals(expectedWarnCount, actualWarnCount);
    }

    @Test
    public void testSingleBucketWithNoRowsDoesNotWarn() {
        // Given a total that the MCVs already account for in full
        // CASE WHEN ofSingleBucket finds no rows left to carry THEN it reaches the quiet door,
        // because it has just proved the very thing the warning would ask the caller to check END

        final Map<String, Long> mcv = Map.of("a", 1000L);
        final double totalRows = 1000;
        final int expectedWarnCount = 0;

        final int actualWarnCount = warnCountWhile(() -> Histogram.ofSingleBucket(1.0, 2.0, totalRows, mcv));

        Assertions.assertEquals(expectedWarnCount, actualWarnCount);
    }

    private static int warnCountWhile(Runnable action) {
        WarnCounterAppender appender = new WarnCounterAppender();
        org.apache.logging.log4j.core.Logger logger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Histogram.class);
        appender.start();
        logger.addAppender(appender);
        try {
            action.run();
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }
        return appender.getWarnCount();
    }

    private static class WarnCounterAppender extends AbstractAppender {
        private final AtomicInteger warnCount = new AtomicInteger();

        WarnCounterAppender() {
            super("histogram-warn-counter", null, null);
        }

        @Override
        public void append(LogEvent event) {
            if (event.getLevel() == Level.WARN) {
                warnCount.incrementAndGet();
            }
        }

        int getWarnCount() {
            return warnCount.get();
        }
    }
}
