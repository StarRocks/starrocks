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

package com.starrocks.alter;

import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.lake.LakeTablet;
import com.starrocks.planner.RangeColocateScanDispatch;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Boundary planning of a range rewrite of a table whose group has the ColocateRanges (-inf, 100), [100, 200) and
 * [200, +inf) on its single colocate column k1. The new sort key is (k1, k3, k2) unless a test says otherwise.
 */
public class RangeColocateRewriteLayoutTest {
    private static final List<Column> SORT_KEY = List.of(
            new Column("k1", IntegerType.INT), new Column("k3", IntegerType.INT), new Column("k2", IntegerType.INT));

    /** A tuple of INT values; a null element is a NULL. */
    private static Tuple tuple(Integer... values) {
        List<Variant> variants = new ArrayList<>(values.length);
        for (Integer value : values) {
            variants.add(value == null ? Variant.nullVariant(IntegerType.INT) : Variant.of(IntegerType.INT, value.toString()));
        }
        return new Tuple(variants);
    }

    private static List<ColocateRange> threeColocateRanges() {
        return List.of(
                new ColocateRange(Range.lt(tuple(100)), 1L),
                new ColocateRange(Range.gelt(tuple(100), tuple(200)), 2L),
                new ColocateRange(Range.ge(tuple(200)), 3L));
    }

    private static RangeColocateRewriteLayout layout() {
        return new RangeColocateRewriteLayout(threeColocateRanges(), SORT_KEY, 1);
    }

    /** Asserts that the tablets the boundaries describe are aligned with the three ColocateRanges. */
    private static void assertAligned(List<Tuple> boundaries, List<Column> sortKey) {
        MaterializedIndex index = new MaterializedIndex(1L, MaterializedIndex.IndexState.SHADOW);
        long tabletId = 100L;
        for (TabletRange range : LakeRangeRewriteSchemaChangeJob.buildTabletRanges(boundaries)) {
            index.addTablet(new LakeTablet(tabletId++, range), null, false);
        }
        assertTrue(RangeColocateScanDispatch.isTabletRangesAligned(index, sortKey, threeColocateRanges(), 1),
                "not aligned: " + boundaries);
    }

    @Test
    public void testColocateBoundariesAreTheExpandedLowerBounds() {
        List<Tuple> boundaries = layout().colocateBoundaries();

        assertEquals(List.of(tuple(100, null, null), tuple(200, null, null)), boundaries);
        assertAligned(boundaries, SORT_KEY);
    }

    @Test
    public void testExpandedBoundsCarryTheTypesOfTheNewSortKey() {
        List<Column> sortKey = List.of(new Column("k1", IntegerType.INT),
                new Column("s", TypeFactory.createVarcharType(20)));

        Tuple boundary = new RangeColocateRewriteLayout(threeColocateRanges(), sortKey, 1).colocateBoundaries().get(0);

        Variant suffix = boundary.getValues().get(1);
        assertInstanceOf(NullVariant.class, suffix);
        assertEquals(20, ((ScalarType) suffix.getType()).getLength());
    }

    @Test
    public void testSingleColocateRangeHasNoBound() {
        RangeColocateRewriteLayout layout =
                new RangeColocateRewriteLayout(List.of(new ColocateRange(Range.all(), 1L)), SORT_KEY, 1);

        assertTrue(layout.colocateBoundaries().isEmpty());
    }

    @Test
    public void testSamplesAreBucketedByTheColocateRangeOfTheirColocateColumns() {
        List<List<Tuple>> samplesByRange = layout().bucketByColocateRange(List.of(
                tuple(null, 5, 5), tuple(99, 9, 9), tuple(100, null, null), tuple(199, 9, 9), tuple(200, 1, 1)));

        // A NULL colocate column sorts below every bound, so it falls in the first ColocateRange.
        assertEquals(List.of(
                List.of(tuple(null, 5, 5), tuple(99, 9, 9)),
                List.of(tuple(100, null, null), tuple(199, 9, 9)),
                List.of(tuple(200, 1, 1))), samplesByRange);
    }

    @Test
    public void testTabletCountsFollowTheShareOfTheSample() {
        RangeColocateRewriteLayout layout = layout();
        List<List<Tuple>> samplesByRange = layout.bucketByColocateRange(List.of(
                tuple(10, 1, 1), tuple(20, 1, 1),
                tuple(110, 1, 1), tuple(120, 1, 1), tuple(130, 1, 1), tuple(140, 1, 1)));

        // 6 tablets shared 2:4:0, but the empty ColocateRange still gets one.
        assertArrayEquals(new int[] {2, 4, 1}, layout.tabletCountsBySampleShare(samplesByRange, 6));
    }

    @Test
    public void testEmptySampleGivesOneTabletPerColocateRange() throws Exception {
        RangeColocateRewriteLayout layout = layout();
        List<List<Tuple>> samplesByRange = layout.bucketByColocateRange(List.of());
        int[] tabletCounts = layout.tabletCountsBySampleShare(samplesByRange, 6);

        assertArrayEquals(new int[] {1, 1, 1}, tabletCounts);
        assertEquals(layout.colocateBoundaries(), layout.planBoundaries(samplesByRange, tabletCounts));
    }

    @Test
    public void testBaseTabletCountsPerColocateRange() {
        List<Tablet> baseTablets = List.of(
                new LakeTablet(1L, new TabletRange(Range.lt(tuple(50, 1, 1)))),
                new LakeTablet(2L, new TabletRange(Range.gelt(tuple(50, 1, 1), tuple(100, null, null)))),
                // Crosses the bound 200 while the group is being aligned: counts for [100, 200).
                new LakeTablet(3L, new TabletRange(Range.gelt(tuple(100, null, null), tuple(250, 1, 1)))),
                new LakeTablet(4L, new TabletRange(Range.ge(tuple(250, 1, 1)))));

        assertArrayEquals(new int[] {2, 1, 1}, layout().tabletCountsOfBaseTablets(baseTablets));
        assertArrayEquals(new int[] {1, 1, 1},
                layout().tabletCountsOfBaseTablets(List.of(new LakeTablet(1L, new TabletRange(Range.all())))));
    }

    /**
     * The boundaries are the ColocateRange bounds plus the row-quantile cuts of each ColocateRange's sample. A sample
     * equal to a ColocateRange bound is never a second boundary, and a canonical-shaped cut is dropped.
     */
    @Test
    public void testBoundariesUnionColocateBoundsAndPerRangeQuantileCuts() throws Exception {
        RangeColocateRewriteLayout layout = layout();
        List<List<Tuple>> samplesByRange = layout.bucketByColocateRange(List.of(
                tuple(10, 1, 1), tuple(20, 1, 1), tuple(30, 1, 1), tuple(40, 1, 1),
                tuple(100, null, null), tuple(120, 5, 5), tuple(150, null, null), tuple(150, 1, 1),
                tuple(170, 1, 1), tuple(180, 1, 1), tuple(190, 1, 1), tuple(199, 9, 9),
                tuple(200, 1, 1), tuple(250, 1, 1), tuple(300, 1, 1), tuple(350, 1, 1)));
        int[] tabletCounts = layout.tabletCountsBySampleShare(samplesByRange, 6);
        assertArrayEquals(new int[] {2, 3, 2}, tabletCounts);

        List<Tuple> boundaries = layout.planBoundaries(samplesByRange, tabletCounts);

        // [100, 200) asks for 3 tablets, but its first cut (150, NULL, NULL) has the shape of a ColocateRange bound.
        assertEquals(List.of(tuple(30, 1, 1), tuple(100, null, null), tuple(180, 1, 1), tuple(200, null, null),
                tuple(300, 1, 1)), boundaries);
        assertArrayEquals(new int[] {2, 2, 2}, layout.tabletCountsOf(boundaries));
        assertAligned(boundaries, SORT_KEY);
    }

    /** When the colocate columns are the whole sort key, every cut has the shape of a ColocateRange bound. */
    @Test
    public void testSortKeyOfColocateColumnsUsesNoSampledCut() throws Exception {
        List<Column> sortKey = List.of(new Column("k1", IntegerType.INT));
        RangeColocateRewriteLayout layout = new RangeColocateRewriteLayout(threeColocateRanges(), sortKey, 1);
        List<List<Tuple>> samplesByRange =
                layout.bucketByColocateRange(List.of(tuple(110), tuple(120), tuple(130), tuple(140)));

        assertFalse(layout.usesSampledCuts());
        assertEquals(List.of(tuple(100), tuple(200)), layout.planBoundaries(samplesByRange, new int[] {1, 4, 1}));
        assertTrue(layout().usesSampledCuts());
    }

    /**
     * Two NULLs of VARCHAR types of different lengths are not equal, but compare equal, and two boundaries that compare
     * equal would leave an empty tablet between them.
     */
    @Test
    public void testMergeDropsBoundariesThatCompareEqual() {
        Tuple nullOfVarchar10 = new Tuple(List.of(Variant.of(IntegerType.INT, "100"),
                Variant.nullVariant(TypeFactory.createVarcharType(10))));
        Tuple nullOfVarchar20 = new Tuple(List.of(Variant.of(IntegerType.INT, "100"),
                Variant.nullVariant(TypeFactory.createVarcharType(20))));
        assertNotEquals(nullOfVarchar10, nullOfVarchar20);
        assertEquals(0, nullOfVarchar10.compareTo(nullOfVarchar20));

        assertEquals(List.of(nullOfVarchar20),
                RangeColocateRewriteLayout.mergeBoundaries(List.of(nullOfVarchar20), List.of(nullOfVarchar10)));
        assertEquals(List.of(tuple(1), tuple(3), tuple(5)),
                RangeColocateRewriteLayout.mergeBoundaries(List.of(tuple(5), tuple(1)), List.of(tuple(3), tuple(5))));
    }
}
