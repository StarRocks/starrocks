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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Config;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintColumn;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.bigintTuple;

public class AutoIncrementRowIdBoundariesTest {

    private static final Column ROW_ID_COLUMN = bigintColumn("__ROW_ID__");

    private int savedIdCacheSize;

    @BeforeEach
    public void setUp() {
        savedIdCacheSize = Config.auto_increment_cache_size;
    }

    @AfterEach
    public void tearDown() {
        Config.auto_increment_cache_size = savedIdCacheSize;
    }

    @Test
    public void testPristineSpanIsCutAtEqualRowShares() {
        // Cuts must be 1 + i * ceil(totalRows / tabletCount): the load writes ids from 1 upward, so a
        // regression here (an off-by-one origin, or cuts spaced by something other than the row share)
        // would leave the first or the last tablet holding every row.
        Config.auto_increment_cache_size = 25;

        DerivedBoundarySource.Result result = AutoIncrementRowIdBoundaries.plan(
                /*currentAutoIncrementId=*/ null, /*totalRows=*/ 1000, /*requestedTabletCount=*/ 4,
                /*activeComputeNodeCount=*/ 1, ROW_ID_COLUMN);

        List<Tuple> boundaries = boundariesOf(result);
        Assertions.assertEquals(List.of(bigintTuple(251), bigintTuple(501), bigintTuple(751)), boundaries);
        Assertions.assertEquals(4, result.boundaries().getEffectiveTabletCount());
        assertStrictlyIncreasing(boundaries);
        for (Tuple boundary : boundaries) {
            Assertions.assertEquals(1, boundary.getValues().size(), "row-id sort key is single-column");
            Assertions.assertEquals(IntegerType.BIGINT.getPrimitiveType(),
                    boundary.getValues().get(0).getType().getPrimitiveType());
        }
    }

    @Test
    public void testUsedCounterIsNotDerivable() {
        // Any allocation already made means a compute node may hold cached ids this planner cannot
        // bound. If this gate regressed, boundaries would be derived for a span whose real occupancy
        // is unknown.
        Config.auto_increment_cache_size = 25;

        DerivedBoundarySource.Result result = AutoIncrementRowIdBoundaries.plan(
                /*currentAutoIncrementId=*/ 1L, /*totalRows=*/ 1000, /*requestedTabletCount=*/ 4,
                /*activeComputeNodeCount=*/ 1, ROW_ID_COLUMN);

        assertSkipped(SkipReason.ROW_ID_SPACE_NOT_PRISTINE, result);
    }

    @Test
    public void testTabletCountIsClampedToTheGapHeadroom() {
        // gapMass = 3 nodes * 100000 cached ids = 300000, so a tablet needs 3000000 rows to keep the
        // unused-id gaps down to ~10% of its share. If the clamp regressed, small loads would be split
        // into tablets whose contents are dominated by gaps rather than rows.
        Config.auto_increment_cache_size = 100000;

        DerivedBoundarySource.Result rowsDwarfGaps = AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 1_000_000_000L, /*requestedTabletCount=*/ 9,
                /*activeComputeNodeCount=*/ 3, ROW_ID_COLUMN);
        Assertions.assertEquals(8, boundariesOf(rowsDwarfGaps).size(), "1e9 rows affords all 9 requested tablets");

        DerivedBoundarySource.Result clamped = AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 10_000_000L, /*requestedTabletCount=*/ 9,
                /*activeComputeNodeCount=*/ 3, ROW_ID_COLUMN);
        List<Tuple> clampedBoundaries = boundariesOf(clamped);
        Assertions.assertEquals(2, clampedBoundaries.size(), "1e7 rows affords only 3 tablets");
        assertStrictlyIncreasing(clampedBoundaries);

        DerivedBoundarySource.Result tooSmall = AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 1_000_000L, /*requestedTabletCount=*/ 9,
                /*activeComputeNodeCount=*/ 3, ROW_ID_COLUMN);
        assertSkipped(SkipReason.ROW_ID_SPAN_TOO_SMALL, tooSmall);
    }

    @Test
    public void testClampFollowsIdCacheSizeConfig() {
        // The gap size is a function of auto_increment_cache_size, so raising the config must clamp the
        // same estimate harder. A planner that hard-coded the default would keep 9 tablets here and
        // under-provision the headroom on a cluster configured for larger id blocks.
        Config.auto_increment_cache_size = 100000;
        DerivedBoundarySource.Result atDefault = AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 100_000_000L, /*requestedTabletCount=*/ 9,
                /*activeComputeNodeCount=*/ 3, ROW_ID_COLUMN);
        Assertions.assertEquals(8, boundariesOf(atDefault).size());

        Config.auto_increment_cache_size = 1_000_000;
        DerivedBoundarySource.Result atTenfold = AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 100_000_000L, /*requestedTabletCount=*/ 9,
                /*activeComputeNodeCount=*/ 3, ROW_ID_COLUMN);
        Assertions.assertEquals(2, boundariesOf(atTenfold).size(), "tenfold cache size affords only 3 tablets");
    }

    @Test
    public void testNonPositiveIdCacheSizeIsNotDerivable() {
        // The config is a divisor. A regression that dropped this guard would throw
        // ArithmeticException out of the planner instead of falling back to no pre-split.
        Config.auto_increment_cache_size = 0;
        assertSkipped(SkipReason.DERIVATION_FAILED, AutoIncrementRowIdBoundaries.plan(
                null, 1_000_000_000L, 9, 3, ROW_ID_COLUMN));

        Config.auto_increment_cache_size = -1;
        assertSkipped(SkipReason.DERIVATION_FAILED, AutoIncrementRowIdBoundaries.plan(
                null, 1_000_000_000L, 9, 3, ROW_ID_COLUMN));
    }

    @Test
    public void testGapMassWiderThanIntDoesNotWrap() {
        // 100000 * 100000 wraps to 1410065408 in int width, which would shrink the required headroom by
        // 7x and let this estimate through as 7 tablets instead of being rejected.
        Config.auto_increment_cache_size = 100_000;

        assertSkipped(SkipReason.ROW_ID_SPAN_TOO_SMALL, AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 100_000_000_000L, /*requestedTabletCount=*/ 9,
                /*activeComputeNodeCount=*/ 100_000, ROW_ID_COLUMN));
    }

    @Test
    public void testHeadroomOverflowSaturatesInsteadOfWrapping() {
        // Absurd inputs, kept only to pin the arithmetic: gapMass is 1.95e18, so multiplying in the
        // headroom overflows long and must clamp. Wrapping would produce ~1.05e18 instead and hand back
        // 3 tablets for an estimate that affords none.
        Config.auto_increment_cache_size = 1_300_000_000;

        assertSkipped(SkipReason.ROW_ID_SPAN_TOO_SMALL, AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 4_000_000_000_000_000_000L, /*requestedTabletCount=*/ 9,
                /*activeComputeNodeCount=*/ 1_500_000_000, ROW_ID_COLUMN));
    }

    @Test
    public void testMissingEstimateIsReportedSeparately() {
        // No estimate is a different operator story from an estimate that is too small, and the two
        // feed different skip metrics.
        Config.auto_increment_cache_size = 25;

        assertSkipped(SkipReason.ESTIMATE_UNAVAILABLE, AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 0, /*requestedTabletCount=*/ 4,
                /*activeComputeNodeCount=*/ 1, ROW_ID_COLUMN));
    }

    @Test
    public void testSingleTabletRequestProducesNoCuts() {
        // A caller that sized the load down to one tablet must get a skip, not a zero-cut result the
        // reshard job would then be built from.
        Config.auto_increment_cache_size = 25;

        assertSkipped(SkipReason.ROW_ID_SPAN_TOO_SMALL, AutoIncrementRowIdBoundaries.plan(
                null, /*totalRows=*/ 1000, /*requestedTabletCount=*/ 1,
                /*activeComputeNodeCount=*/ 1, ROW_ID_COLUMN));
    }

    @Test
    public void testStrideNeverFallsBelowOne() {
        // Exercised directly because the gap clamp keeps plan() from ever reaching a tablet count above
        // its row estimate (a tablet needs at least 10 rows per cached id). A stride of 0 would emit
        // duplicate cuts, which collapse into fewer tablets than the caller asked for.
        Assertions.assertEquals(1, AutoIncrementRowIdBoundaries.rowIdStride(3, 8));
        Assertions.assertEquals(1, AutoIncrementRowIdBoundaries.rowIdStride(1, 2));
        Assertions.assertEquals(250, AutoIncrementRowIdBoundaries.rowIdStride(1000, 4));
        Assertions.assertEquals(251, AutoIncrementRowIdBoundaries.rowIdStride(1001, 4));
    }

    private static List<Tuple> boundariesOf(DerivedBoundarySource.Result result) {
        Assertions.assertNull(result.skipReason(), "expected cuts, got skip");
        Assertions.assertNotNull(result.boundaries());
        return result.boundaries().getBoundaries();
    }

    private static void assertSkipped(SkipReason expected, DerivedBoundarySource.Result result) {
        Assertions.assertEquals(expected, result.skipReason());
        Assertions.assertNull(result.boundaries(), "a skipped plan must not carry boundaries");
    }

    private static void assertStrictlyIncreasing(List<Tuple> boundaries) {
        for (int i = 1; i < boundaries.size(); i++) {
            Assertions.assertTrue(boundaries.get(i - 1).compareTo(boundaries.get(i)) < 0,
                    "boundaries not strictly increasing: " + boundaries);
        }
    }
}
