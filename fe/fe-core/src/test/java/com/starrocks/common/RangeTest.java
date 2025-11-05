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

package com.starrocks.common;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.starrocks.persist.gson.GsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RangeTest {

    // Test class that contains Range as a member variable
    public static class RangeContainer {
        @SerializedName(value = "id")
        private int id;

        @SerializedName(value = "name")
        private String name;

        @SerializedName(value = "valueRange")
        private Range<Integer> valueRange;

        @SerializedName(value = "nameRange")
        private Range<String> nameRange;

        public RangeContainer(int id, String name, Range<Integer> valueRange, Range<String> nameRange) {
            this.id = id;
            this.name = name;
            this.valueRange = valueRange;
            this.nameRange = nameRange;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Range<Integer> getValueRange() {
            return valueRange;
        }

        public Range<String> getNameRange() {
            return nameRange;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RangeContainer that = (RangeContainer) obj;
            return id == that.id &&
                    name.equals(that.name) &&
                    valueRange.equals(that.valueRange) &&
                    nameRange.equals(that.nameRange);
        }
    }

    // ==================== Factory Method Tests ====================

    @Test
    public void testOfMethodWithAllNull() {
        Range<Integer> range = Range.of(null, null, false, false);
        Assertions.assertNull(range.getLowerBound());
        Assertions.assertNull(range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertTrue(range.isAll());
    }

    @Test
    public void testOfMethodWithLowerBoundNull() {
        Range<Integer> rangeLT = Range.of(null, 10, false, false);
        Assertions.assertNull(rangeLT.getLowerBound());
        Assertions.assertEquals(10, rangeLT.getUpperBound());
        Assertions.assertFalse(rangeLT.isUpperBoundIncluded());

        Range<Integer> rangeLE = Range.of(null, 10, false, true);
        Assertions.assertNull(rangeLE.getLowerBound());
        Assertions.assertEquals(10, rangeLE.getUpperBound());
        Assertions.assertTrue(rangeLE.isUpperBoundIncluded());
    }

    @Test
    public void testOfMethodWithUpperBoundNull() {
        Range<Integer> rangeGT = Range.of(5, null, false, false);
        Assertions.assertEquals(5, rangeGT.getLowerBound());
        Assertions.assertNull(rangeGT.getUpperBound());
        Assertions.assertFalse(rangeGT.isLowerBoundIncluded());

        Range<Integer> rangeGE = Range.of(5, null, true, false);
        Assertions.assertEquals(5, rangeGE.getLowerBound());
        Assertions.assertNull(rangeGE.getUpperBound());
        Assertions.assertTrue(rangeGE.isLowerBoundIncluded());
    }

    @Test
    public void testOfMethodWithBothBounds() {
        Range<Integer> openRange = Range.of(1, 10, false, false);
        Assertions.assertEquals(1, openRange.getLowerBound());
        Assertions.assertEquals(10, openRange.getUpperBound());
        Assertions.assertFalse(openRange.isLowerBoundIncluded());
        Assertions.assertFalse(openRange.isUpperBoundIncluded());

        Range<Integer> closedRange = Range.of(1, 10, true, true);
        Assertions.assertEquals(1, closedRange.getLowerBound());
        Assertions.assertEquals(10, closedRange.getUpperBound());
        Assertions.assertTrue(closedRange.isLowerBoundIncluded());
        Assertions.assertTrue(closedRange.isUpperBoundIncluded());

        Range<Integer> closedOpen = Range.of(1, 10, true, false);
        Assertions.assertEquals(1, closedOpen.getLowerBound());
        Assertions.assertEquals(10, closedOpen.getUpperBound());
        Assertions.assertTrue(closedOpen.isLowerBoundIncluded());
        Assertions.assertFalse(closedOpen.isUpperBoundIncluded());

        Range<Integer> openClosed = Range.of(1, 10, false, true);
        Assertions.assertEquals(1, openClosed.getLowerBound());
        Assertions.assertEquals(10, openClosed.getUpperBound());
        Assertions.assertFalse(openClosed.isLowerBoundIncluded());
        Assertions.assertTrue(openClosed.isUpperBoundIncluded());
    }

    @Test
    public void testAllFactoryMethod() {
        Range<Integer> range = Range.all();
        Assertions.assertNull(range.getLowerBound());
        Assertions.assertNull(range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertTrue(range.isAll());
        Assertions.assertEquals("(-∞, +∞)", range.toString());
    }

    @Test
    public void testLTFactoryMethod() {
        Range<Integer> range = Range.lt(100);
        Assertions.assertNull(range.getLowerBound());
        Assertions.assertEquals(100, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("(-∞, 100)", range.toString());
    }

    @Test
    public void testLEFactoryMethod() {
        Range<Integer> range = Range.le(50);
        Assertions.assertNull(range.getLowerBound());
        Assertions.assertEquals(50, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertTrue(range.isUpperBoundIncluded());
        Assertions.assertEquals("(-∞, 50]", range.toString());
    }

    @Test
    public void testGTFactoryMethod() {
        Range<Integer> range = Range.gt(20);
        Assertions.assertEquals(20, range.getLowerBound());
        Assertions.assertNull(range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("(20, +∞)", range.toString());
    }

    @Test
    public void testGEFactoryMethod() {
        Range<Integer> range = Range.ge(30);
        Assertions.assertEquals(30, range.getLowerBound());
        Assertions.assertNull(range.getUpperBound());
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("[30, +∞)", range.toString());
    }

    @Test
    public void testGTLTFactoryMethod() {
        Range<Integer> range = Range.gtlt(10, 20);
        Assertions.assertEquals(10, range.getLowerBound());
        Assertions.assertEquals(20, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("(10, 20)", range.toString());
    }

    @Test
    public void testGELTFactoryMethod() {
        Range<Integer> range = Range.gelt(5, 15);
        Assertions.assertEquals(5, range.getLowerBound());
        Assertions.assertEquals(15, range.getUpperBound());
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("[5, 15)", range.toString());
    }

    @Test
    public void testGTLEFactoryMethod() {
        Range<Integer> range = Range.gtle(3, 13);
        Assertions.assertEquals(3, range.getLowerBound());
        Assertions.assertEquals(13, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertTrue(range.isUpperBoundIncluded());
        Assertions.assertEquals("(3, 13]", range.toString());
    }

    @Test
    public void testGELEFactoryMethod() {
        Range<Integer> range = Range.gele(7, 17);
        Assertions.assertEquals(7, range.getLowerBound());
        Assertions.assertEquals(17, range.getUpperBound());
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertTrue(range.isUpperBoundIncluded());
        Assertions.assertEquals("[7, 17]", range.toString());
    }

    @Test
    public void testFactoryMethodsWithStringType() {
        Range<String> ltRange = Range.lt("z");
        Assertions.assertTrue(ltRange.contains("a"));
        Assertions.assertFalse(ltRange.contains("z"));

        Range<String> geRange = Range.ge("m");
        Assertions.assertTrue(geRange.contains("m"));
        Assertions.assertTrue(geRange.contains("z"));
        Assertions.assertFalse(geRange.contains("a"));

        Range<String> geleRange = Range.gele("d", "w");
        Assertions.assertTrue(geleRange.contains("d"));
        Assertions.assertTrue(geleRange.contains("m"));
        Assertions.assertTrue(geleRange.contains("w"));
        Assertions.assertFalse(geleRange.contains("a"));
        Assertions.assertFalse(geleRange.contains("z"));
    }

    @Test
    public void testFactoryMethodEquivalentToOf() {
        // Verify that factory methods create equivalent ranges to Range.of
        Assertions.assertEquals(Range.of(null, null, false, false), Range.all());
        Assertions.assertEquals(Range.of(null, 10, false, false), Range.lt(10));
        Assertions.assertEquals(Range.of(null, 10, false, true), Range.le(10));
        Assertions.assertEquals(Range.of(5, null, false, false), Range.gt(5));
        Assertions.assertEquals(Range.of(5, null, true, false), Range.ge(5));
        Assertions.assertEquals(Range.of(1, 10, false, false), Range.gtlt(1, 10));
        Assertions.assertEquals(Range.of(1, 10, true, false), Range.gelt(1, 10));
        Assertions.assertEquals(Range.of(1, 10, false, true), Range.gtle(1, 10));
        Assertions.assertEquals(Range.of(1, 10, true, true), Range.gele(1, 10));
    }

    // ==================== AllRange Singleton Tests ====================

    @Test
    public void testAllRangeSingleton() {
        Range<Integer> instance1 = Range.of(null, null, false, false);
        Range<Integer> instance2 = Range.of(null, null, false, false);
        Range<String> instance3 = Range.of(null, null, false, false);

        Assertions.assertSame(instance1, instance2);
        Assertions.assertSame(instance1, instance3); // Same singleton regardless of type parameter
    }

    @Test
    public void testAllRangeProperties() {
        Range<Integer> allRange = Range.of(null, null, false, false);
        Assertions.assertTrue(allRange.isAll());
        Assertions.assertTrue(allRange.isMinimum());
        Assertions.assertTrue(allRange.isMaximum());
        Assertions.assertFalse(allRange.isIdentical());
    }

    // ==================== Individual Range Type Tests ====================

    @Test
    public void testLTRange() {
        Range<Integer> range = Range.of(null, 10, false, false);
        Assertions.assertNull(range.getLowerBound());
        Assertions.assertEquals(10, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("(-∞, 10)", range.toString());
    }

    @Test
    public void testLERange() {
        Range<Integer> range = Range.of(null, 10, false, true);
        Assertions.assertNull(range.getLowerBound());
        Assertions.assertEquals(10, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertTrue(range.isUpperBoundIncluded());
        Assertions.assertEquals("(-∞, 10]", range.toString());
    }

    @Test
    public void testGTRange() {
        Range<Integer> range = Range.of(5, null, false, false);
        Assertions.assertEquals(5, range.getLowerBound());
        Assertions.assertNull(range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("(5, +∞)", range.toString());
    }

    @Test
    public void testGERange() {
        Range<Integer> range = Range.of(5, null, true, false);
        Assertions.assertEquals(5, range.getLowerBound());
        Assertions.assertNull(range.getUpperBound());
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("[5, +∞)", range.toString());
    }

    @Test
    public void testGTLTRange() {
        Range<Integer> range = Range.of(1, 10, false, false);
        Assertions.assertEquals(1, range.getLowerBound());
        Assertions.assertEquals(10, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("(1, 10)", range.toString());
    }

    @Test
    public void testGELTRange() {
        Range<Integer> range = Range.of(1, 10, true, false);
        Assertions.assertEquals(1, range.getLowerBound());
        Assertions.assertEquals(10, range.getUpperBound());
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
        Assertions.assertEquals("[1, 10)", range.toString());
    }

    @Test
    public void testGTLERange() {
        Range<Integer> range = Range.of(1, 10, false, true);
        Assertions.assertEquals(1, range.getLowerBound());
        Assertions.assertEquals(10, range.getUpperBound());
        Assertions.assertFalse(range.isLowerBoundIncluded());
        Assertions.assertTrue(range.isUpperBoundIncluded());
        Assertions.assertEquals("(1, 10]", range.toString());
    }

    @Test
    public void testGELERange() {
        Range<Integer> range = Range.of(1, 10, true, true);
        Assertions.assertEquals(1, range.getLowerBound());
        Assertions.assertEquals(10, range.getUpperBound());
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertTrue(range.isUpperBoundIncluded());
        Assertions.assertEquals("[1, 10]", range.toString());
    }

    // ==================== Contains Element Tests ====================

    @Test
    public void testContainsElement() {
        Range<Integer> closedRange = Range.of(1, 10, true, true);
        Assertions.assertTrue(closedRange.contains(1));
        Assertions.assertTrue(closedRange.contains(5));
        Assertions.assertTrue(closedRange.contains(10));
        Assertions.assertFalse(closedRange.contains(0));
        Assertions.assertFalse(closedRange.contains(11));

        Range<Integer> openRange = Range.of(1, 10, false, false);
        Assertions.assertFalse(openRange.contains(1));
        Assertions.assertTrue(openRange.contains(5));
        Assertions.assertFalse(openRange.contains(10));
        Assertions.assertFalse(openRange.contains(0));
        Assertions.assertFalse(openRange.contains(11));
    }

    @Test
    public void testContainsElementWithInfiniteRange() {
        Range<Integer> allRange = Range.of(null, null, false, false);
        Assertions.assertTrue(allRange.contains(Integer.MIN_VALUE));
        Assertions.assertTrue(allRange.contains(0));
        Assertions.assertTrue(allRange.contains(Integer.MAX_VALUE));
    }

    // ==================== IsLessThan/IsGreaterThan Element Tests ====================

    @Test
    public void testIsLessThanElement() {
        Range<Integer> range = Range.of(1, 5, true, true);
        Assertions.assertTrue(range.isLessThan(10));
        Assertions.assertFalse(range.isLessThan(5));
        Assertions.assertFalse(range.isLessThan(3));

        Range<Integer> openRange = Range.of(1, 5, false, false);
        Assertions.assertTrue(openRange.isLessThan(5));
        Assertions.assertFalse(openRange.isLessThan(4));
    }

    @Test
    public void testIsGreaterThanElement() {
        Range<Integer> range = Range.of(5, 10, true, true);
        Assertions.assertTrue(range.isGreaterThan(3));
        Assertions.assertFalse(range.isGreaterThan(5));
        Assertions.assertFalse(range.isGreaterThan(7));

        Range<Integer> openRange = Range.of(5, 10, false, false);
        Assertions.assertTrue(openRange.isGreaterThan(5));
        Assertions.assertFalse(openRange.isGreaterThan(6));
    }

    // ==================== IsLessThan/IsGreaterThan Range Tests ====================

    @Test
    public void testIsLessThanRange() {
        Range<Integer> range1 = Range.of(1, 5, true, true);
        Range<Integer> range2 = Range.of(6, 10, true, true);
        Assertions.assertTrue(range1.isLessThan(range2));
        Assertions.assertFalse(range2.isLessThan(range1));

        Range<Integer> range3 = Range.of(5, 10, true, true);
        Assertions.assertFalse(range1.isLessThan(range3)); // They touch at 5
    }

    @Test
    public void testIsGreaterThanRange() {
        Range<Integer> range1 = Range.of(6, 10, true, true);
        Range<Integer> range2 = Range.of(1, 5, true, true);
        Assertions.assertTrue(range1.isGreaterThan(range2));
        Assertions.assertFalse(range2.isGreaterThan(range1));
    }

    @Test
    public void testRangeTouchingAtBoundary() {
        Range<Integer> range1 = Range.of(1, 5, true, false); // [1, 5)
        Range<Integer> range2 = Range.of(5, 10, true, false); // [5, 10)
        Assertions.assertTrue(range1.isLessThan(range2));
        Assertions.assertTrue(range2.isGreaterThan(range1));
        Assertions.assertFalse(range1.isOverlapping(range2));
    }

    // ==================== IsOverlapping Tests ====================

    @Test
    public void testIsOverlapping() {
        Range<Integer> range1 = Range.of(1, 10, true, true);
        Range<Integer> range2 = Range.of(5, 15, true, true);
        Assertions.assertTrue(range1.isOverlapping(range2));
        Assertions.assertTrue(range2.isOverlapping(range1));

        Range<Integer> range3 = Range.of(20, 30, true, true);
        Assertions.assertFalse(range1.isOverlapping(range3));
        Assertions.assertFalse(range3.isOverlapping(range1));
    }

    @Test
    public void testIsOverlappingWithContainedRange() {
        Range<Integer> outer = Range.of(1, 10, true, true);
        Range<Integer> inner = Range.of(3, 7, true, true);
        Assertions.assertTrue(outer.isOverlapping(inner));
        Assertions.assertTrue(inner.isOverlapping(outer));
    }

    // ==================== CompareTo Tests ====================

    @Test
    public void testCompareTo() {
        Range<Integer> range1 = Range.of(1, 5, true, true);
        Range<Integer> range2 = Range.of(6, 10, true, true);
        Range<Integer> range3 = Range.of(3, 8, true, true);

        Assertions.assertTrue(range1.compareTo(range2) < 0);
        Assertions.assertTrue(range2.compareTo(range1) > 0);
        Assertions.assertEquals(0, range1.compareTo(range3)); // Overlapping ranges
    }

    // ==================== Equals and HashCode Tests ====================

    @Test
    public void testEquals() {
        Range<Integer> range1 = Range.of(1, 10, true, true);
        Range<Integer> range2 = Range.of(1, 10, true, true);
        Range<Integer> range3 = Range.of(1, 11, true, true);
        Range<Integer> range4 = Range.of(1, 10, true, false); // Different inclusiveness

        Assertions.assertEquals(range1, range2);
        Assertions.assertNotEquals(range1, range3);
        Assertions.assertNotEquals(range1, range4);
        Assertions.assertNotEquals(range1, null);
        Assertions.assertNotEquals(range1, "not a range");
    }

    @Test
    public void testHashCode() {
        Range<Integer> range1 = Range.of(1, 10, true, true);
        Range<Integer> range2 = Range.of(1, 10, true, true);
        Range<Integer> range3 = Range.of(1, 11, true, true);

        Assertions.assertEquals(range1.hashCode(), range2.hashCode());
        // Not guaranteed to be different, but likely
        Assertions.assertNotEquals(range1.hashCode(), range3.hashCode());
    }

    @Test
    public void testEqualsSameInstance() {
        Range<Integer> range = Range.of(1, 10, true, true);
        Assertions.assertEquals(range, range);
    }

    // ==================== IsIdentical Tests ====================

    @Test
    public void testIsIdentical() {
        Range<Integer> pointRange = Range.of(5, 5, true, true);
        Assertions.assertTrue(pointRange.isIdentical());

        Range<Integer> normalRange = Range.of(1, 10, true, true);
        Assertions.assertFalse(normalRange.isIdentical());

        Range<Integer> openPoint = Range.of(5, 5, false, false);
        Assertions.assertFalse(openPoint.isIdentical()); // Not included
    }

    // ==================== Boundary Condition Tests ====================

    @Test
    public void testBoundaryExcluded() {
        Range<Integer> range = Range.of(1, 10, true, true);
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertTrue(range.isUpperBoundIncluded());
        Assertions.assertFalse(range.isLowerBoundExcluded());
        Assertions.assertFalse(range.isUpperBoundExcluded());
    }

    @Test
    public void testMinimumMaximum() {
        Range<Integer> ltRange = Range.of(null, 10, false, false);
        Assertions.assertTrue(ltRange.isMinimum());
        Assertions.assertFalse(ltRange.isMaximum());

        Range<Integer> gtRange = Range.of(5, null, false, false);
        Assertions.assertFalse(gtRange.isMinimum());
        Assertions.assertTrue(gtRange.isMaximum());

        Range<Integer> normalRange = Range.of(1, 10, true, true);
        Assertions.assertFalse(normalRange.isMinimum());
        Assertions.assertFalse(normalRange.isMaximum());
    }

    // ==================== Null Parameter Tests ====================

    @Test
    public void testNullParameterInContains() {
        Range<Integer> range = Range.of(1, 10, true, true);
        Assertions.assertThrows(NullPointerException.class, () -> range.contains(null));
    }

    @Test
    public void testNullParameterInIsLessThan() {
        Range<Integer> range = Range.of(1, 10, true, true);
        Assertions.assertThrows(NullPointerException.class, () -> range.isLessThan((Integer) null));
    }

    @Test
    public void testNullParameterInIsGreaterThan() {
        Range<Integer> range = Range.of(1, 10, true, true);
        Assertions.assertThrows(NullPointerException.class, () -> range.isGreaterThan((Integer) null));
    }

    @Test
    public void testNullParameterInRangeComparison() {
        Range<Integer> range = Range.of(1, 10, true, true);
        Assertions.assertThrows(NullPointerException.class, () -> range.isLessThan((Range<Integer>) null));
        Assertions.assertThrows(NullPointerException.class, () -> range.isGreaterThan((Range<Integer>) null));
        Assertions.assertThrows(NullPointerException.class, () -> range.isOverlapping(null));
    }

    @Test
    public void testValidRangeWithEqualBounds() {
        // Equal bounds should be allowed (point ranges)
        Range<Integer> pointOpen = Range.of(5, 5, false, false); // (5, 5) - empty range
        Assertions.assertNotNull(pointOpen);
        Assertions.assertFalse(pointOpen.contains(5));
        Assertions.assertFalse(pointOpen.isIdentical());

        Range<Integer> pointClosed = Range.of(5, 5, true, true); // [5, 5] - point range
        Assertions.assertNotNull(pointClosed);
        Assertions.assertTrue(pointClosed.contains(5));
        Assertions.assertTrue(pointClosed.isIdentical());

        Range<Integer> halfOpen1 = Range.of(5, 5, true, false); // [5, 5)
        Assertions.assertNotNull(halfOpen1);
        Assertions.assertFalse(halfOpen1.contains(5));

        Range<Integer> halfOpen2 = Range.of(5, 5, false, true); // (5, 5]
        Assertions.assertNotNull(halfOpen2);
        Assertions.assertFalse(halfOpen2.contains(5));
    }

    // ==================== ToString Tests ====================

    @Test
    public void testToString() {
        Assertions.assertEquals("(-∞, +∞)", Range.of(null, null, false, false).toString());
        Assertions.assertEquals("(-∞, 10)", Range.of(null, 10, false, false).toString());
        Assertions.assertEquals("(-∞, 10]", Range.of(null, 10, false, true).toString());
        Assertions.assertEquals("(5, +∞)", Range.of(5, null, false, false).toString());
        Assertions.assertEquals("[5, +∞)", Range.of(5, null, true, false).toString());
        Assertions.assertEquals("(1, 10)", Range.of(1, 10, false, false).toString());
        Assertions.assertEquals("[1, 10)", Range.of(1, 10, true, false).toString());
        Assertions.assertEquals("(1, 10]", Range.of(1, 10, false, true).toString());
        Assertions.assertEquals("[1, 10]", Range.of(1, 10, true, true).toString());
    }

    // ==================== String Range Tests ====================

    @Test
    public void testStringRange() {
        Range<String> range = Range.of("a", "z", true, true);
        Assertions.assertTrue(range.contains("m"));
        Assertions.assertTrue(range.contains("a"));
        Assertions.assertTrue(range.contains("z"));
        Assertions.assertFalse(range.contains("A"));
        Assertions.assertTrue(range.isLessThan("zz"));
        Assertions.assertTrue(range.isGreaterThan("A"));
    }

    // ==================== Gson Serialization Tests ====================
    // Note: All serialization tests use TypeToken to preserve generic type information.
    // Without TypeToken, JSON numbers would be deserialized as Double instead of Integer.

    @Test
    public void testAllRangeSerialization() {
        Range<Integer> original = Range.of(null, null, false, false);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertSame(original, deserialized); // Singleton should be preserved
    }

    @Test
    public void testLTRangeSerialization() {
        Range<Integer> original = Range.of(null, 100, false, false);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(100, deserialized.getUpperBound());
        Assertions.assertNull(deserialized.getLowerBound());
        Assertions.assertFalse(deserialized.isUpperBoundIncluded());
    }

    @Test
    public void testLERangeSerialization() {
        Range<Integer> original = Range.of(null, 50, false, true);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(50, deserialized.getUpperBound());
        Assertions.assertTrue(deserialized.isUpperBoundIncluded());
    }

    @Test
    public void testGTRangeSerialization() {
        Range<Integer> original = Range.of(20, null, false, false);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(20, deserialized.getLowerBound());
        Assertions.assertFalse(deserialized.isLowerBoundIncluded());
    }

    @Test
    public void testGERangeSerialization() {
        Range<Integer> original = Range.of(30, null, true, false);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(30, deserialized.getLowerBound());
        Assertions.assertTrue(deserialized.isLowerBoundIncluded());
    }

    @Test
    public void testGTLTRangeSerialization() {
        Range<Integer> original = Range.of(10, 20, false, false);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(10, deserialized.getLowerBound());
        Assertions.assertEquals(20, deserialized.getUpperBound());
        Assertions.assertFalse(deserialized.isLowerBoundIncluded());
        Assertions.assertFalse(deserialized.isUpperBoundIncluded());
    }

    @Test
    public void testGELTRangeSerialization() {
        Range<Integer> original = Range.of(5, 15, true, false);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(5, deserialized.getLowerBound());
        Assertions.assertEquals(15, deserialized.getUpperBound());
        Assertions.assertTrue(deserialized.isLowerBoundIncluded());
        Assertions.assertFalse(deserialized.isUpperBoundIncluded());
    }

    @Test
    public void testGTLERangeSerialization() {
        Range<Integer> original = Range.of(3, 13, false, true);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(3, deserialized.getLowerBound());
        Assertions.assertEquals(13, deserialized.getUpperBound());
        Assertions.assertFalse(deserialized.isLowerBoundIncluded());
        Assertions.assertTrue(deserialized.isUpperBoundIncluded());
    }

    @Test
    public void testGELERangeSerialization() {
        Range<Integer> original = Range.of(7, 17, true, true);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals(7, deserialized.getLowerBound());
        Assertions.assertEquals(17, deserialized.getUpperBound());
        Assertions.assertTrue(deserialized.isLowerBoundIncluded());
        Assertions.assertTrue(deserialized.isUpperBoundIncluded());
    }

    @Test
    public void testStringRangeSerialization() {
        Range<String> original = Range.of("apple", "orange", true, true);
        String json = GsonUtils.GSON.toJson(original);
        Range<String> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<String>>(){}.getType());

        Assertions.assertEquals(original, deserialized);
        Assertions.assertEquals("apple", deserialized.getLowerBound());
        Assertions.assertEquals("orange", deserialized.getUpperBound());
    }

    @Test
    public void testSerializedRangeOperations() {
        // Serialize and deserialize a range, then test operations
        Range<Integer> original = Range.of(10, 20, true, true);
        String json = GsonUtils.GSON.toJson(original);
        Range<Integer> deserialized = GsonUtils.GSON.fromJson(json,
                new TypeToken<Range<Integer>>(){}.getType());

        // Test operations on deserialized range
        Assertions.assertTrue(deserialized.contains(15));
        Assertions.assertFalse(deserialized.contains(25));
        Assertions.assertTrue(deserialized.isLessThan(30));
        Assertions.assertTrue(deserialized.isGreaterThan(5));

        Range<Integer> other = Range.of(25, 35, true, true);
        Assertions.assertFalse(deserialized.isOverlapping(other));
    }

    @Test
    public void testRangeContainerSerialization() {
        // Create a container with different types of ranges
        Range<Integer> valueRange = Range.of(1, 100, true, true);
        Range<String> nameRange = Range.of("Alice", "Zoe", true, false);
        RangeContainer original = new RangeContainer(1001, "TestContainer", valueRange, nameRange);

        // Serialize
        String json = GsonUtils.GSON.toJson(original);
        Assertions.assertNotNull(json);
        Assertions.assertTrue(json.contains("\"id\":1001"));
        Assertions.assertTrue(json.contains("\"name\":\"TestContainer\""));
        Assertions.assertTrue(json.contains("\"valueRange\""));
        Assertions.assertTrue(json.contains("\"nameRange\""));

        // Deserialize
        RangeContainer deserialized = GsonUtils.GSON.fromJson(json, RangeContainer.class);

        // Verify container properties
        Assertions.assertNotNull(deserialized);
        Assertions.assertEquals(1001, deserialized.getId());
        Assertions.assertEquals("TestContainer", deserialized.getName());

        // Verify value range
        Assertions.assertNotNull(deserialized.getValueRange());
        Assertions.assertEquals(valueRange, deserialized.getValueRange());
        Assertions.assertTrue(deserialized.getValueRange().contains(50));
        Assertions.assertFalse(deserialized.getValueRange().contains(150));

        // Verify name range
        Assertions.assertNotNull(deserialized.getNameRange());
        Assertions.assertEquals(nameRange, deserialized.getNameRange());
        Assertions.assertTrue(deserialized.getNameRange().contains("Bob"));
        Assertions.assertFalse(deserialized.getNameRange().contains("Adam"));

        // Verify complete equality
        Assertions.assertEquals(original, deserialized);
    }

    @Test
    public void testRangeContainerWithAllRange() {
        // Test container with AllRange singleton
        Range<Integer> allRange = Range.of(null, null, false, false);
        Range<String> stringRange = Range.of("M", null, false, false);
        RangeContainer original = new RangeContainer(2002, "AllRangeContainer", allRange, stringRange);

        // Serialize and deserialize
        String json = GsonUtils.GSON.toJson(original);
        RangeContainer deserialized = GsonUtils.GSON.fromJson(json, RangeContainer.class);

        // Verify AllRange singleton is preserved
        Assertions.assertNotNull(deserialized.getValueRange());
        Assertions.assertSame(Range.of(null, null, false, false), deserialized.getValueRange());

        // Verify string range
        Assertions.assertEquals("M", deserialized.getNameRange().getLowerBound());
        Assertions.assertTrue(deserialized.getNameRange().contains("Tom"));
        Assertions.assertFalse(deserialized.getNameRange().contains("Alice"));
    }

    @Test
    public void testRangeContainerWithVariousRangeTypes() {
        // Test with different range types
        Range<Integer> ltRange = Range.of(null, 100, false, false);
        Range<String> geRange = Range.of("G", null, true, false);
        RangeContainer container1 = new RangeContainer(3001, "Container1", ltRange, geRange);

        String json1 = GsonUtils.GSON.toJson(container1);
        RangeContainer deserialized1 = GsonUtils.GSON.fromJson(json1, RangeContainer.class);

        Assertions.assertEquals(container1, deserialized1);

        // Test with another combination
        Range<Integer> gtRange = Range.of(0, null, false, false);
        Range<String> leRange = Range.of(null, "Z", false, true);
        RangeContainer container2 = new RangeContainer(3002, "Container2", gtRange, leRange);

        String json2 = GsonUtils.GSON.toJson(container2);
        RangeContainer deserialized2 = GsonUtils.GSON.fromJson(json2, RangeContainer.class);

        Assertions.assertEquals(container2, deserialized2);
    }
}
