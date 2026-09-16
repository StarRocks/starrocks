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

package com.starrocks.catalog;

import com.starrocks.common.Range;
import com.starrocks.lake.LakeTablet;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class ColocateRangeUtilsTest {

    private static final List<Column> SORT_KEY_COLUMNS = Arrays.asList(
            new Column("k1", IntegerType.INT),
            new Column("k2", VarcharType.VARCHAR),
            new Column("k3", IntegerType.BIGINT));

    private static Tuple makeTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    @Test
    public void testExpandAllRange() {
        Range<Tuple> result = ColocateRangeUtils.expandToFullSortKey(
                Range.all(), SORT_KEY_COLUMNS, 1);
        Assertions.assertTrue(result.isAll());
    }

    @Test
    public void testExpandAllRangeWithAllColocateColumns() {
        Range<Tuple> result = ColocateRangeUtils.expandToFullSortKey(
                Range.all(), SORT_KEY_COLUMNS, 3);
        Assertions.assertTrue(result.isAll());
    }

    // [100, 200) -> [(100, NULL, NULL), (200, NULL, NULL))
    @Test
    public void testExpandBoundedRange() {
        Range<Tuple> colocateRange = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> result = ColocateRangeUtils.expandToFullSortKey(
                colocateRange, SORT_KEY_COLUMNS, 1);

        Assertions.assertTrue(result.isLowerBoundIncluded());
        Assertions.assertFalse(result.isUpperBoundIncluded());

        Tuple lower = result.getLowerBound();
        Assertions.assertEquals(3, lower.getValues().size());
        Assertions.assertEquals("100", lower.getValues().get(0).getStringValue());
        Assertions.assertTrue(lower.getValues().get(1) instanceof NullVariant);
        Assertions.assertTrue(lower.getValues().get(2) instanceof NullVariant);

        Tuple upper = result.getUpperBound();
        Assertions.assertEquals(3, upper.getValues().size());
        Assertions.assertEquals("200", upper.getValues().get(0).getStringValue());
        Assertions.assertTrue(upper.getValues().get(1) instanceof NullVariant);
        Assertions.assertTrue(upper.getValues().get(2) instanceof NullVariant);
    }

    // (-inf, 200) -> (-inf, (200, NULL, NULL))
    @Test
    public void testExpandLowerUnbounded() {
        Range<Tuple> colocateRange = Range.lt(makeTuple(200));
        Range<Tuple> result = ColocateRangeUtils.expandToFullSortKey(
                colocateRange, SORT_KEY_COLUMNS, 1);

        Assertions.assertTrue(result.isMinimum());
        Assertions.assertNull(result.getLowerBound());
        Assertions.assertFalse(result.isUpperBoundIncluded());

        Tuple upper = result.getUpperBound();
        Assertions.assertEquals(3, upper.getValues().size());
        Assertions.assertTrue(upper.getValues().get(1) instanceof NullVariant);
    }

    // [100, +inf) -> [(100, NULL, NULL), +inf)
    @Test
    public void testExpandUpperUnbounded() {
        Range<Tuple> colocateRange = Range.ge(makeTuple(100));
        Range<Tuple> result = ColocateRangeUtils.expandToFullSortKey(
                colocateRange, SORT_KEY_COLUMNS, 1);

        Assertions.assertTrue(result.isMaximum());
        Assertions.assertNull(result.getUpperBound());
        Assertions.assertTrue(result.isLowerBoundIncluded());

        Tuple lower = result.getLowerBound();
        Assertions.assertEquals(3, lower.getValues().size());
        Assertions.assertTrue(lower.getValues().get(1) instanceof NullVariant);
    }

    @Test
    public void testExpandNoRemainingColumns() {
        List<Column> singleColumnSortKey = Arrays.asList(new Column("k1", IntegerType.INT));
        Range<Tuple> colocateRange = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> result = ColocateRangeUtils.expandToFullSortKey(
                colocateRange, singleColumnSortKey, 1);

        // No expansion, tuple size unchanged
        Assertions.assertEquals(1, result.getLowerBound().getValues().size());
        Assertions.assertEquals(1, result.getUpperBound().getValues().size());
    }

    // Invalid: exclusive lower bound should be rejected
    @Test
    public void testRejectExclusiveLowerBound() {
        Range<Tuple> colocateRange = Range.gt(makeTuple(100));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangeUtils.expandToFullSortKey(colocateRange, SORT_KEY_COLUMNS, 1));
    }

    // Invalid: inclusive upper bound should be rejected
    @Test
    public void testRejectInclusiveUpperBound() {
        Range<Tuple> colocateRange = Range.le(makeTuple(200));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangeUtils.expandToFullSortKey(colocateRange, SORT_KEY_COLUMNS, 1));
    }

    // Invalid: colocateColumnCount out of range
    @Test
    public void testRejectInvalidColocateColumnCount() {
        Range<Tuple> colocateRange = Range.gelt(makeTuple(100), makeTuple(200));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangeUtils.expandToFullSortKey(colocateRange, SORT_KEY_COLUMNS, -1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangeUtils.expandToFullSortKey(colocateRange, SORT_KEY_COLUMNS, 4));
    }

    // ---- extractColocatePrefix ----

    @Test
    public void testExtractPrefixFromAllRange() {
        // Range.all() has -inf lower bound: caller must fall back to first colocate range.
        Assertions.assertNull(ColocateRangeUtils.extractColocatePrefix(Range.all(), 1));
    }

    @Test
    public void testExtractPrefixFromLowerUnbounded() {
        // (-inf, (200, NULL, NULL)) -> still unbounded below.
        Range<Tuple> expanded = ColocateRangeUtils.expandToFullSortKey(
                Range.lt(makeTuple(200)), SORT_KEY_COLUMNS, 1);
        Assertions.assertNull(ColocateRangeUtils.extractColocatePrefix(expanded, 1));
    }

    @Test
    public void testExtractPrefixTruncatesFullSortKeyTuple() {
        // [(100, NULL, NULL), (200, NULL, NULL)) with 1 colocate column -> (100,)
        Range<Tuple> expanded = ColocateRangeUtils.expandToFullSortKey(
                Range.gelt(makeTuple(100), makeTuple(200)), SORT_KEY_COLUMNS, 1);
        Tuple prefix = ColocateRangeUtils.extractColocatePrefix(expanded, 1);
        Assertions.assertNotNull(prefix);
        Assertions.assertEquals(1, prefix.getValues().size());
        Assertions.assertEquals("100", prefix.getValues().get(0).getStringValue());
    }

    @Test
    public void testExtractPrefixReturnsLowerBoundWhenSizesMatch() {
        // Tablet whose range was created with colocateColumnCount == sort key count:
        // expansion is a no-op, lower bound itself IS the colocate prefix.
        List<Column> singleColumnSortKey = Arrays.asList(new Column("k1", IntegerType.INT));
        Range<Tuple> expanded = ColocateRangeUtils.expandToFullSortKey(
                Range.gelt(makeTuple(100), makeTuple(200)), singleColumnSortKey, 1);
        Tuple prefix = ColocateRangeUtils.extractColocatePrefix(expanded, 1);
        Assertions.assertSame(expanded.getLowerBound(), prefix);
    }

    @Test
    public void testExtractPrefixWithIntraColocateSplit() {
        // After P3 intra-colocate split, a tablet may be [(100, "a", 1), (100, "z", 9)).
        Tuple lower = new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, "100"),
                Variant.of(VarcharType.VARCHAR, "a"),
                Variant.of(IntegerType.BIGINT, "1")));
        Tuple upper = new Tuple(Arrays.asList(
                Variant.of(IntegerType.INT, "100"),
                Variant.of(VarcharType.VARCHAR, "z"),
                Variant.of(IntegerType.BIGINT, "9")));
        Range<Tuple> tabletRange = Range.gelt(lower, upper);
        Tuple prefix = ColocateRangeUtils.extractColocatePrefix(tabletRange, 1);
        Assertions.assertNotNull(prefix);
        Assertions.assertEquals(1, prefix.getValues().size());
        Assertions.assertEquals("100", prefix.getValues().get(0).getStringValue());
    }

    @Test
    public void testExtractPrefixRejectsZeroOrNegativeCount() {
        Range<Tuple> tabletRange = ColocateRangeUtils.expandToFullSortKey(
                Range.gelt(makeTuple(100), makeTuple(200)), SORT_KEY_COLUMNS, 1);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangeUtils.extractColocatePrefix(tabletRange, 0));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangeUtils.extractColocatePrefix(tabletRange, -1));
    }

    @Test
    public void testExtractPrefixRejectsLowerBoundShorterThanColocateCount() {
        Tuple lower = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "100")));
        Tuple upper = new Tuple(Arrays.asList(Variant.of(IntegerType.INT, "200")));
        Range<Tuple> tabletRange = Range.gelt(lower, upper);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangeUtils.extractColocatePrefix(tabletRange, 2));
    }

    @Test
    public void testLookupPackShardGroupIdMapsPrefixToOwningRange() {
        // Two colocate ranges split at prefix 100: [MIN, 100) -> 1001, [100, MAX) -> 1002.
        Tuple boundary = makeTuple(100);
        List<ColocateRange> ranges = Arrays.asList(
                new ColocateRange(Range.lt(boundary), 1001L),
                new ColocateRange(Range.ge(boundary), 1002L));
        // prefix 50 lands in the first range, prefix 150 in the second.
        Assertions.assertEquals(1001L,
                ColocateRangeUtils.lookupPackShardGroupId(Range.ge(makeTuple(50)), ranges, 1));
        Assertions.assertEquals(1002L,
                ColocateRangeUtils.lookupPackShardGroupId(Range.ge(makeTuple(150)), ranges, 1));
        // A boundary-aligned tablet at exactly 100 belongs to the second range ([100, MAX)).
        Assertions.assertEquals(1002L,
                ColocateRangeUtils.lookupPackShardGroupId(Range.ge(boundary), ranges, 1));
        // An unbounded-below tablet (-inf) maps to the first range.
        Assertions.assertEquals(1001L,
                ColocateRangeUtils.lookupPackShardGroupId(Range.all(), ranges, 1));
    }

    @Test
    public void testLookupPackShardGroupIdReturnsSentinelWhenUncovered() {
        Assertions.assertEquals(PhysicalPartition.INVALID_SHARD_GROUP_ID,
                ColocateRangeUtils.lookupPackShardGroupId(Range.all(), List.of(), 1));
    }

    // ---- Classifier ----

    // Three colocate ranges on the single-column prefix k1:
    //   R0 = (-inf, 200) -> 1000, R1 = [200, 300) -> 1001, R2 = [300, +inf) -> 1002
    private static final List<ColocateRange> THREE_RANGES = Arrays.asList(
            new ColocateRange(Range.lt(makeTuple(200)), 1000L),
            new ColocateRange(Range.gelt(makeTuple(200), makeTuple(300)), 1001L),
            new ColocateRange(Range.ge(makeTuple(300)), 1002L));

    private static Range<Tuple> expandedTabletRange(Range<Tuple> colocateShapedRange) {
        return ColocateRangeUtils.expandToFullSortKey(colocateShapedRange, SORT_KEY_COLUMNS, 1);
    }

    private static ColocateRangeUtils.Classifier threeRangeClassifier() {
        return ColocateRangeUtils.Classifier.of(THREE_RANGES, SORT_KEY_COLUMNS, 1);
    }

    @Test
    public void testClassifierIsNullWhenNotRangeColocate() {
        Assertions.assertNull(ColocateRangeUtils.Classifier.of(null, SORT_KEY_COLUMNS, 1));
    }

    @Test
    public void testClassifierIndexOfRange() {
        ColocateRangeUtils.Classifier classifier = threeRangeClassifier();

        // Fully inside the MIDDLE range: neither first nor last, so an implementation that always
        // answered 0 (or ranges.size()-1) cannot pass.
        Assertions.assertEquals(1, classifier.indexOf(
                expandedTabletRange(Range.gelt(makeTuple(200), makeTuple(250)))));
        // Unbounded below -> first range; unbounded above -> last range.
        Assertions.assertEquals(0, classifier.indexOf(expandedTabletRange(Range.lt(makeTuple(200)))));
        Assertions.assertEquals(2, classifier.indexOf(expandedTabletRange(Range.ge(makeTuple(300)))));
        // Spans R1 and R2: the prefix resolves to R1 but the range is not contained in it.
        Assertions.assertEquals(-1, classifier.indexOf(
                expandedTabletRange(Range.gelt(makeTuple(250), makeTuple(350)))));
        // A null range is classified, not dereferenced.
        Assertions.assertEquals(-1, classifier.indexOf((Range<Tuple>) null));
    }

    @Test
    public void testClassifierIndexOfTabletWithoutRange() {
        // A tablet carrying no TabletRange classifies as -1 rather than throwing, so a caller walking
        // an index does not have to spell out the null check.
        Assertions.assertEquals(-1, threeRangeClassifier().indexOf(new LakeTablet(1L)));
    }

    @Test
    public void testClassifierUncoveredPrefix() {
        // A partial range list that does not cover prefix 50.
        List<ColocateRange> partial = List.of(THREE_RANGES.get(1));
        ColocateRangeUtils.Classifier classifier =
                ColocateRangeUtils.Classifier.of(partial, SORT_KEY_COLUMNS, 1);
        Assertions.assertEquals(-1, classifier.indexOf(
                expandedTabletRange(Range.gelt(makeTuple(50), makeTuple(60)))));
    }

    @Test
    public void testClassifierRangeTooShortForPrefix() {
        // A mixed-version or faulty BE can publish a range whose lower tuple is shorter than the
        // colocate prefix, tripping extractColocatePrefix's precondition. indexOf must absorb that
        // into -1: its callers run past points of no return where an escaping exception cannot be
        // unwound, and the only sensible answer there is "uncontained" anyway.
        Assertions.assertEquals(-1, threeRangeClassifier().indexOf(Range.ge(new Tuple(List.of()))));
    }

    @Test
    public void testClassifierZeroColocateColumns() {
        // colocateColumnCount == 0 is the degenerate single-[MIN,MAX) shape. The prefix must stay null
        // (indexOf maps null to the first range); calling extractColocatePrefix unconditionally would
        // trip its colocateColumnCount > 0 precondition instead.
        List<ColocateRange> allRange = List.of(new ColocateRange(Range.all(), 1000L));
        ColocateRangeUtils.Classifier classifier =
                ColocateRangeUtils.Classifier.of(allRange, SORT_KEY_COLUMNS, 0);
        Assertions.assertEquals(0, classifier.indexOf(
                expandedTabletRange(Range.gelt(makeTuple(200), makeTuple(250)))));
    }

    /**
     * Classifier.indexOf(...) >= 0 must agree with the pre-existing isContainedInOwningColocateRange
     * for every valid non-null tablet range, so the two lookups cannot drift apart. A null range is
     * deliberately NOT in this table: the classifier returns -1 where isContainedInOwningColocateRange
     * dereferences, an intentional difference asserted in {@link #testClassifierIndexOfRange}.
     */
    @Test
    public void testClassifierAgreesWithIsContained() {
        ColocateRangeUtils.Classifier classifier = threeRangeClassifier();
        List<Range<Tuple>> cases = Arrays.asList(
                expandedTabletRange(Range.gelt(makeTuple(200), makeTuple(250))),  // inside R1
                expandedTabletRange(Range.lt(makeTuple(200))),                    // inside R0
                expandedTabletRange(Range.ge(makeTuple(300))),                    // inside R2
                expandedTabletRange(Range.gelt(makeTuple(250), makeTuple(350))),  // spans R1..R2
                expandedTabletRange(Range.gelt(makeTuple(100), makeTuple(400))),  // spans all three
                Range.all());
        for (Range<Tuple> tabletRange : cases) {
            Assertions.assertEquals(
                    ColocateRangeUtils.isContainedInOwningColocateRange(
                            tabletRange, THREE_RANGES, SORT_KEY_COLUMNS, 1),
                    classifier.indexOf(tabletRange) >= 0,
                    "lookups disagree for " + tabletRange);
        }
    }

}
