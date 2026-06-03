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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.StarRocksException;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.DUMMY_CONTEXT;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintTuple;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.varcharColumn;

public class ParquetMetadataSamplerTest {

    private static SampleRequest singleColumnRequest() {
        return new SampleRequest(DUMMY_CONTEXT, List.of(bigintColumn("k")), 1024L, 0L);
    }

    private static RowGroupStatistics rowGroup(long min, long max, long rowCount) {
        return new RowGroupStatistics(bigintTuple(min), bigintTuple(max), rowCount, false);
    }

    private static RowGroupStatistics rowGroupTruncated(long min, long max, long rowCount) {
        return new RowGroupStatistics(bigintTuple(min), bigintTuple(max), rowCount, true);
    }

    private static RowGroupStatistics rowGroupMissingMin(long rowCount) {
        return new RowGroupStatistics(null, bigintTuple(0), rowCount, false);
    }

    private static RowGroupStatistics rowGroupAllNull(long rowCount) {
        Tuple nullTuple = new Tuple(List.of((Variant) new NullVariant(IntegerType.BIGINT)));
        return new RowGroupStatistics(nullTuple, nullTuple, rowCount, false);
    }

    /**
     * Stub provider that returns canned RowGroupStatistics.
     */
    private static final class FakeProvider implements RowGroupStatisticsProvider {
        private final List<RowGroupStatistics> statistics;

        FakeProvider(List<RowGroupStatistics> statistics) {
            this.statistics = statistics;
        }

        @Override
        public List<RowGroupStatistics> fetch(SampleRequest request) {
            return statistics;
        }
    }

    @Test
    public void testHappyPathFourTabletsOverEvenlyDistributedRowGroups() throws Exception {
        // 8 row groups, each 100 rows wide, non-overlapping ranges 0..99, 100..199, ...
        // total = 800 rows. tabletCount=4 → thresholds at 200, 400, 600 → land in
        // row groups [2, 4, 6] → mins = 200, 400, 600.
        List<RowGroupStatistics> statistics = new ArrayList<>();
        for (long i = 0; i < 8; i++) {
            statistics.add(rowGroup(i * 100, i * 100 + 99, 100));
        }
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        BoundaryPlannerResult result = sampler.tryPlan(singleColumnRequest(), 4);

        Assertions.assertEquals(4, result.getEffectiveTabletCount());
        Assertions.assertEquals(3, result.getBoundaries().size());
        Assertions.assertEquals(bigintTuple(200), result.getBoundaries().get(0));
        Assertions.assertEquals(bigintTuple(400), result.getBoundaries().get(1));
        Assertions.assertEquals(bigintTuple(600), result.getBoundaries().get(2));
    }

    @Test
    public void testHappyPathTwoTabletsSingleBoundaryAtMidpoint() throws Exception {
        // 4 row groups, 100 rows each, total 400. tabletCount=2 → threshold at 200 →
        // cumulative reaches 200 at end of rowGroup[1]; next rowGroup[2] starts at 200..299.
        List<RowGroupStatistics> statistics = List.of(
                rowGroup(0, 99, 100), rowGroup(100, 199, 100),
                rowGroup(200, 299, 100), rowGroup(300, 399, 100));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        BoundaryPlannerResult result = sampler.tryPlan(singleColumnRequest(), 2);

        Assertions.assertEquals(2, result.getEffectiveTabletCount());
        Assertions.assertEquals(bigintTuple(200), result.getBoundaries().get(0));
    }

    @Test
    public void testZeroRowGroupsAreIgnored() throws Exception {
        // Mixes 4 real row groups and 2 zero-row "phantom" entries — those are
        // skipped without triggering fallback.
        List<RowGroupStatistics> statistics = List.of(
                rowGroup(0, 99, 100),
                rowGroup(100, 199, 0),
                rowGroup(200, 299, 100),
                rowGroup(300, 399, 0),
                rowGroup(400, 499, 100),
                rowGroup(500, 599, 100));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        BoundaryPlannerResult result = sampler.tryPlan(singleColumnRequest(), 4);

        // Zero-row groups are filtered before quantile placement; usable = {0..99, 200..299, 400..499, 500..599}.
        Assertions.assertEquals(3, result.getBoundaries().size());
        Assertions.assertEquals(bigintTuple(200), result.getBoundaries().get(0));
        Assertions.assertEquals(bigintTuple(400), result.getBoundaries().get(1));
        Assertions.assertEquals(bigintTuple(500), result.getBoundaries().get(2));
    }

    @Test
    public void testEmptyStatisticsIsNoSplit() throws Exception {
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(
                new FakeProvider(Collections.emptyList()));

        BoundaryPlannerResult result = sampler.tryPlan(singleColumnRequest(), 4);

        Assertions.assertTrue(result.isNoSplit());
        Assertions.assertEquals(1, result.getEffectiveTabletCount());
    }

    @Test
    public void testAllZeroRowGroupsIsNoSplit() throws Exception {
        List<RowGroupStatistics> statistics = List.of(rowGroup(0, 99, 0), rowGroup(100, 199, 0));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        BoundaryPlannerResult result = sampler.tryPlan(singleColumnRequest(), 4);

        Assertions.assertTrue(result.isNoSplit());
    }

    @Test
    public void testSingleRowGroupWithRangeFallback() {
        // A single row group spans 0..99 with 100 rows. Meta tier can only emit row-group
        // minTuples and they all equal the global minimum → no useful cut. But the data
        // has range, so data tier (row sampling) might succeed → throw MetaTierUnavailable so
        // the coordinator retries.
        List<RowGroupStatistics> statistics = List.of(rowGroup(0, 99, 100));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
    }

    @Test
    public void testAllEqualRangeIsNoSplit() throws Exception {
        // Row groups with min == max (all-equal data) → genuinely no useful split, even
        // for data tier. Should return NO_SPLIT, not MetaTierUnavailable.
        List<RowGroupStatistics> statistics = List.of(rowGroup(42, 42, 50), rowGroup(42, 42, 50));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        BoundaryPlannerResult result = sampler.tryPlan(singleColumnRequest(), 4);

        Assertions.assertTrue(result.isNoSplit());
    }

    @Test
    public void testNonOverlappingPairsBelowThresholdAccepted() throws Exception {
        // 5 row groups, 2 of which overlap a neighbor — overlap fraction = 2/4 = 0.5.
        // With a relaxed threshold of 0.6, fallback should NOT trigger.
        List<RowGroupStatistics> statistics = List.of(
                rowGroup(0, 50, 100),
                rowGroup(40, 99, 100),   // overlaps rowGroup[0] (40 < 50)
                rowGroup(100, 199, 100),
                rowGroup(180, 250, 100), // overlaps rowGroup[2] (180 < 199)
                rowGroup(260, 359, 100));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(
                new FakeProvider(statistics), /*overlapThreshold=*/ 0.6);

        BoundaryPlannerResult result = sampler.tryPlan(singleColumnRequest(), 3);
        Assertions.assertFalse(result.isNoSplit());
    }

    @Test
    public void testCompositeSortKeyFallback() {
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(
                new FakeProvider(List.of(rowGroup(0, 99, 100))));
        SampleRequest request = new SampleRequest(
                DUMMY_CONTEXT, List.of(bigintColumn("k"), varcharColumn("g")), 1024L, 0L);

        MetaTierUnavailableException exception = Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(request, 4));
        Assertions.assertTrue(exception.getMessage().contains("composite sort key"),
                "expected composite-key error, got: " + exception.getMessage());
    }

    @Test
    public void testMissingMinFallback() {
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(
                new FakeProvider(List.of(rowGroup(0, 99, 100), rowGroupMissingMin(100))));

        MetaTierUnavailableException exception = Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertTrue(exception.getMessage().contains("missing min/max"));
    }

    @Test
    public void testTruncatedStatisticsFallback() {
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(
                new FakeProvider(List.of(rowGroup(0, 99, 100), rowGroupTruncated(100, 199, 100))));

        MetaTierUnavailableException exception = Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertTrue(exception.getMessage().contains("truncated"));
    }

    @Test
    public void testAllNullStatisticsFallback() {
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(
                new FakeProvider(List.of(rowGroup(0, 99, 100), rowGroupAllNull(100))));

        MetaTierUnavailableException exception = Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertTrue(exception.getMessage().contains("all-null"));
    }

    @Test
    public void testOverlapAboveThresholdFallback() {
        // 4 row groups all overlapping each other — overlap fraction = 3/3 = 1.0 > default 0.3.
        List<RowGroupStatistics> statistics = List.of(
                rowGroup(0, 200, 100),
                rowGroup(50, 250, 100),
                rowGroup(100, 300, 100),
                rowGroup(150, 350, 100));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        MetaTierUnavailableException exception = Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertTrue(exception.getMessage().contains("overlap"));
    }

    @Test
    public void testProviderExceptionPropagated() {
        RowGroupStatisticsProvider failing = request -> {
            throw new StarRocksException("connector unavailable");
        };
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(failing);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        // Not a MetaTierUnavailableException — generic sampling failure; coordinator
        // treats as "skip pre-split entirely", not "try data tier".
        Assertions.assertFalse(exception instanceof MetaTierUnavailableException);
        Assertions.assertEquals("connector unavailable", exception.getMessage());
    }

    @Test
    public void testRejectsRequestedTabletCountBelowTwo() {
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(
                new FakeProvider(List.of(rowGroup(0, 99, 100))));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 0));
    }

    @Test
    public void testRejectsNullProvider() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new ParquetMetadataSampler(null));
    }

    @Test
    public void testRejectsOutOfRangeOverlapThreshold() {
        FakeProvider provider = new FakeProvider(Collections.emptyList());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ParquetMetadataSampler(provider, -0.01));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ParquetMetadataSampler(provider, 1.01));
    }

    @Test
    public void testNullStatisticsListThrows() {
        // A null fetch result is a provider contract violation; treat as fatal, not NO_SPLIT.
        RowGroupStatisticsProvider nullReturning = request -> null;
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(nullReturning);

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertFalse(exception instanceof MetaTierUnavailableException);
        Assertions.assertTrue(exception.getMessage().contains("null"),
                "expected null in message, got: " + exception.getMessage());
    }

    @Test
    public void testNestedOverlapAboveThresholdFallback() {
        // A wide first group covers every following narrower group. Pairwise-only
        // overlap detection would count 1/4 = 0.25 and let this through; the
        // running max-seen detection correctly counts 4/4 = 1.0.
        List<RowGroupStatistics> statistics = List.of(
                rowGroup(0, 1000, 100),
                rowGroup(10, 20, 100),
                rowGroup(30, 40, 100),
                rowGroup(50, 60, 100),
                rowGroup(70, 80, 100));
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(statistics));

        MetaTierUnavailableException exception = Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertTrue(exception.getMessage().contains("overlap"));
    }

    @Test
    public void testInvalidMinMaxOrderingFallback() {
        // minTuple > maxTuple is treated as unreliable footer statistics — same class
        // as missing/truncated/all-null. Data tier (row sampling) can still try.
        RowGroupStatistics inverted = new RowGroupStatistics(bigintTuple(99), bigintTuple(0), 100, false);
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(List.of(inverted)));

        MetaTierUnavailableException exception = Assertions.assertThrows(MetaTierUnavailableException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertTrue(exception.getMessage().contains("minTuple > maxTuple"),
                "expected min>max error, got: " + exception.getMessage());
    }

    @Test
    public void testSchemaMismatchInRowGroupMinTupleThrows() {
        // Sort key column is BIGINT, but row-group MIN tuple uses VARCHAR — schema mismatch.
        Tuple varcharMin = new Tuple(List.of(Variant.of(com.starrocks.type.VarcharType.VARCHAR, "abc")));
        Tuple bigintMax = bigintTuple(99);
        RowGroupStatistics mismatched = new RowGroupStatistics(varcharMin, bigintMax, 100, false);
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(List.of(mismatched)));

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertFalse(exception instanceof MetaTierUnavailableException);
        Assertions.assertTrue(exception.getMessage().contains("minTuple"),
                "expected error to mention minTuple, got: " + exception.getMessage());
        Assertions.assertTrue(exception.getMessage().toLowerCase().contains("primitive type"),
                "expected primitive-type-mismatch error, got: " + exception.getMessage());
    }

    @Test
    public void testSchemaMismatchInRowGroupMaxTupleThrows() {
        // Sort key column is BIGINT, MIN tuple is BIGINT, but MAX tuple uses VARCHAR.
        // Exercises the second validateTupleAgainstSchema call inside filterUsableRowGroups.
        Tuple bigintMin = bigintTuple(0);
        Tuple varcharMax = new Tuple(List.of(Variant.of(com.starrocks.type.VarcharType.VARCHAR, "xyz")));
        RowGroupStatistics mismatched = new RowGroupStatistics(bigintMin, varcharMax, 100, false);
        ParquetMetadataSampler sampler = new ParquetMetadataSampler(new FakeProvider(List.of(mismatched)));

        StarRocksException exception = Assertions.assertThrows(StarRocksException.class,
                () -> sampler.tryPlan(singleColumnRequest(), 4));
        Assertions.assertFalse(exception instanceof MetaTierUnavailableException);
        Assertions.assertTrue(exception.getMessage().contains("maxTuple"),
                "expected error to mention maxTuple, got: " + exception.getMessage());
    }
}
