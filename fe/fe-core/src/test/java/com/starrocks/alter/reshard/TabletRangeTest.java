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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.thrift.TTabletRange;
import com.starrocks.thrift.TTuple;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeSerializer;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class TabletRangeTest {
    @Test
    public void testTabletRangeToStringAllRange() {
        // Test (-∞, +∞) - default constructor
        TabletRange tabletRange = new TabletRange();
        Assertions.assertEquals("(-∞, +∞)", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringGTRange() {
        // Test (lower, +∞) - greater than, lower excluded
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "0"))),
                null,
                false, false);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("([0], +∞)", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringGERange() {
        // Test [lower, +∞) - greater than or equal, lower included
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "100"))),
                null,
                true, false);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("[[100], +∞)", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringLTRange() {
        // Test (-∞, upper) - less than, upper excluded
        Range<Tuple> range = Range.of(
                null,
                new Tuple(List.of(Variant.of(IntegerType.INT, "50"))),
                false, false);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("(-∞, [50])", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringLERange() {
        // Test (-∞, upper] - less than or equal, upper included
        Range<Tuple> range = Range.of(
                null,
                new Tuple(List.of(Variant.of(IntegerType.INT, "50"))),
                false, true);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("(-∞, [50]]", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringGELTRange() {
        // Test [lower, upper) - half-open interval, lower included, upper excluded
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "10"))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "20"))),
                true, false);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("[[10], [20])", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringGTLERange() {
        // Test (lower, upper] - half-open interval, lower excluded, upper included
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "10"))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "20"))),
                false, true);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("([10], [20]]", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringGELERange() {
        // Test [lower, upper] - closed interval, both bounds included
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "10"))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "20"))),
                true, true);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("[[10], [20]]", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringGTLTRange() {
        // Test (lower, upper) - open interval, both bounds excluded
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "10"))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "20"))),
                false, false);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("([10], [20])", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringMultipleColumns() {
        // Test multiple columns in tuple
        Range<Tuple> range = Range.of(
                new Tuple(
                        List.of(Variant.of(IntegerType.INT, "0"),
                                Variant.of(VarcharType.VARCHAR, "abc"))),
                null,
                false, false);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("([0, abc], +∞)", tabletRange.toString());
    }

    @Test
    public void testTabletRangeToStringMultipleColumnsClosedInterval() {
        // Test multiple columns with closed interval
        Range<Tuple> range = Range.of(
                new Tuple(List.of(
                        Variant.of(IntegerType.INT, "1"),
                        Variant.of(VarcharType.VARCHAR, "start"))),
                new Tuple(List.of(
                        Variant.of(IntegerType.INT, "100"),
                        Variant.of(VarcharType.VARCHAR, "end"))),
                true, true);
        TabletRange tabletRange = new TabletRange(range);
        Assertions.assertEquals("[[1, start], [100, end]]", tabletRange.toString());
    }

    @Test
    public void testGetRange() {
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "10"))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "20"))),
                true, false);
        TabletRange tabletRange = new TabletRange(range);

        Range<Tuple> retrievedRange = tabletRange.getRange();
        Assertions.assertNotNull(retrievedRange);
        Assertions.assertEquals(range, retrievedRange);
        Assertions.assertTrue(retrievedRange.isLowerBoundIncluded());
        Assertions.assertFalse(retrievedRange.isUpperBoundIncluded());
    }

    @Test
    public void testGetRangeAllRange() {
        TabletRange tabletRange = new TabletRange();
        Range<Tuple> range = tabletRange.getRange();

        Assertions.assertNotNull(range);
        Assertions.assertTrue(range.isAll());
        Assertions.assertTrue(range.isMinimum());
        Assertions.assertTrue(range.isMaximum());
    }

    @Test
    public void testToThriftAllRange() {
        TabletRange tabletRange = new TabletRange();
        TTabletRange tRange = tabletRange.toThrift();

        Assertions.assertNotNull(tRange);
        Assertions.assertFalse(tRange.isSetLower_bound());
        Assertions.assertFalse(tRange.isSetUpper_bound());
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());
    }

    @Test
    public void testToThriftWithLowerBound() {
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "100"))),
                null,
                true, false);
        TabletRange tabletRange = new TabletRange(range);
        TTabletRange tRange = tabletRange.toThrift();

        Assertions.assertNotNull(tRange);
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertFalse(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());
    }

    @Test
    public void testToThriftWithUpperBound() {
        Range<Tuple> range = Range.of(
                null,
                new Tuple(List.of(Variant.of(IntegerType.INT, "200"))),
                false, true);
        TabletRange tabletRange = new TabletRange(range);
        TTabletRange tRange = tabletRange.toThrift();

        Assertions.assertNotNull(tRange);
        Assertions.assertFalse(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertTrue(tRange.isUpper_bound_included());
    }

    @Test
    public void testToThriftWithBothBounds() {
        Range<Tuple> range = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.INT, "10"))),
                new Tuple(List.of(Variant.of(IntegerType.INT, "20"))),
                true, false);
        TabletRange tabletRange = new TabletRange(range);
        TTabletRange tRange = tabletRange.toThrift();

        Assertions.assertNotNull(tRange);
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        // Verify the tuple values
        TTuple lowerTuple = tRange.getLower_bound();
        Assertions.assertNotNull(lowerTuple);
        Assertions.assertEquals(1, lowerTuple.getValuesSize());

        TTuple upperTuple = tRange.getUpper_bound();
        Assertions.assertNotNull(upperTuple);
        Assertions.assertEquals(1, upperTuple.getValuesSize());
    }

    @Test
    public void testFromThrift() {
        // Create TTabletRange with bounds
        TTabletRange tRange = new TTabletRange();
        tRange.setLower_bound_included(true);
        tRange.setUpper_bound_included(false);

        // Create lower bound
        TTuple lowerTuple = new TTuple();
        TVariant lowerVariant = new TVariant();
        lowerVariant.setType(TypeSerializer.toThrift(IntegerType.INT));
        lowerVariant.setValue("10");
        lowerTuple.setValues(List.of(lowerVariant));
        tRange.setLower_bound(lowerTuple);

        // Create upper bound
        TTuple upperTuple = new TTuple();
        TVariant upperVariant = new TVariant();
        upperVariant.setType(TypeSerializer.toThrift(IntegerType.INT));
        upperVariant.setValue("20");
        upperTuple.setValues(List.of(upperVariant));
        tRange.setUpper_bound(upperTuple);

        TabletRange tabletRange = TabletRange.fromThrift(tRange);

        Assertions.assertNotNull(tabletRange);
        Assertions.assertEquals("[[10], [20])", tabletRange.toString());

        Range<Tuple> range = tabletRange.getRange();
        Assertions.assertTrue(range.isLowerBoundIncluded());
        Assertions.assertFalse(range.isUpperBoundIncluded());
    }

    @Test
    public void testFromThriftClosedInterval() {
        TTabletRange tRange = new TTabletRange();
        tRange.setLower_bound_included(true);
        tRange.setUpper_bound_included(true);

        TTuple lowerTuple = new TTuple();
        TVariant lowerVariant = new TVariant();
        lowerVariant.setType(TypeSerializer.toThrift(IntegerType.BIGINT));
        lowerVariant.setValue("1000");
        lowerTuple.setValues(List.of(lowerVariant));
        tRange.setLower_bound(lowerTuple);

        TTuple upperTuple = new TTuple();
        TVariant upperVariant = new TVariant();
        upperVariant.setType(TypeSerializer.toThrift(IntegerType.BIGINT));
        upperVariant.setValue("2000");
        upperTuple.setValues(List.of(upperVariant));
        tRange.setUpper_bound(upperTuple);

        TabletRange tabletRange = TabletRange.fromThrift(tRange);

        Assertions.assertEquals("[[1000], [2000]]", tabletRange.toString());
        Assertions.assertTrue(tabletRange.getRange().isLowerBoundIncluded());
        Assertions.assertTrue(tabletRange.getRange().isUpperBoundIncluded());
    }

    @Test
    public void testFromThriftWithVarchar() {
        TTabletRange tRange = new TTabletRange();
        tRange.setLower_bound_included(false);
        tRange.setUpper_bound_included(true);

        TTuple lowerTuple = new TTuple();
        TVariant lowerVariant = new TVariant();
        lowerVariant.setType(TypeSerializer.toThrift(VarcharType.VARCHAR));
        lowerVariant.setValue("aaa");
        lowerTuple.setValues(List.of(lowerVariant));
        tRange.setLower_bound(lowerTuple);

        TTuple upperTuple = new TTuple();
        TVariant upperVariant = new TVariant();
        upperVariant.setType(TypeSerializer.toThrift(VarcharType.VARCHAR));
        upperVariant.setValue("zzz");
        upperTuple.setValues(List.of(upperVariant));
        tRange.setUpper_bound(upperTuple);

        TabletRange tabletRange = TabletRange.fromThrift(tRange);

        Assertions.assertEquals("([aaa], [zzz]]", tabletRange.toString());
    }

    @Test
    public void testRoundTripThriftConversion() {
        // Create original TabletRange
        Range<Tuple> originalRange = Range.of(
                new Tuple(List.of(
                        Variant.of(IntegerType.INT, "5"),
                        Variant.of(VarcharType.VARCHAR, "test"))),
                new Tuple(List.of(
                        Variant.of(IntegerType.INT, "50"),
                        Variant.of(VarcharType.VARCHAR, "test2"))),
                true, false);
        TabletRange original = new TabletRange(originalRange);

        // Convert to Thrift
        TTabletRange tRange = original.toThrift();

        // Convert back to TabletRange
        TabletRange converted = TabletRange.fromThrift(tRange);

        // Verify they are equivalent
        Assertions.assertEquals(original.toString(), converted.toString());
        Assertions.assertEquals(
                original.getRange().isLowerBoundIncluded(),
                converted.getRange().isLowerBoundIncluded());
        Assertions.assertEquals(
                original.getRange().isUpperBoundIncluded(),
                converted.getRange().isUpperBoundIncluded());
    }

    @Test
    public void testDifferentIntegerTypes() {
        // Test with TINYINT
        Range<Tuple> tinyintRange = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.TINYINT, "1"))),
                new Tuple(List.of(Variant.of(IntegerType.TINYINT, "127"))),
                true, true);
        TabletRange tinyintTabletRange = new TabletRange(tinyintRange);
        Assertions.assertEquals("[[1], [127]]", tinyintTabletRange.toString());

        // Test with SMALLINT
        Range<Tuple> smallintRange = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.SMALLINT, "100"))),
                new Tuple(List.of(Variant.of(IntegerType.SMALLINT, "1000"))),
                true, true);
        TabletRange smallintTabletRange = new TabletRange(smallintRange);
        Assertions.assertEquals("[[100], [1000]]", smallintTabletRange.toString());

        // Test with BIGINT
        Range<Tuple> bigintRange = Range.of(
                new Tuple(List.of(Variant.of(IntegerType.BIGINT, "9223372036854775000"))),
                new Tuple(List.of(Variant.of(IntegerType.BIGINT, "9223372036854775807"))),
                true, true);
        TabletRange bigintTabletRange = new TabletRange(bigintRange);
        Assertions.assertEquals("[[9223372036854775000], [9223372036854775807]]", bigintTabletRange.toString());
    }
}