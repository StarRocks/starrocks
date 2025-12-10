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
import com.starrocks.thrift.TTabletRange;
import com.starrocks.thrift.TTuple;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TabletRangeTest {

    @Test
    public void testAllRangeToThrift() {
        Range<Tuple> all = Range.all();
        TabletRange tabletRange = new TabletRange(all);

        TTabletRange tRange = tabletRange.toThrift();

        // For all range, no concrete bounds should be set.
        Assertions.assertFalse(tRange.isSetLower_bound());
        Assertions.assertFalse(tRange.isSetUpper_bound());
        // Inclusiveness flags should reflect the underlying Range semantics.
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());
    }

    @Test
    public void testClosedRangeToThrift() {
        // [ (1, "a"), (5, "z") ]
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "a")));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5),
                new StringVariant(VarcharType.VARCHAR, "z")));

        Range<Tuple> range = Range.gele(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertTrue(tRange.isUpper_bound_included());

        TTuple tLower = tRange.getLower_bound();
        TTuple tUpper = tRange.getUpper_bound();
        Assertions.assertEquals(2, tLower.getValues().size());
        Assertions.assertEquals(2, tUpper.getValues().size());

        TVariant lowerInt = tLower.getValues().get(0);
        TVariant lowerStr = tLower.getValues().get(1);
        TVariant upperInt = tUpper.getValues().get(0);
        TVariant upperStr = tUpper.getValues().get(1);

        Assertions.assertTrue(lowerInt.isSetLong_value());
        Assertions.assertEquals(1L, lowerInt.getLong_value());
        Assertions.assertTrue(lowerStr.isSetString_value());
        Assertions.assertEquals("a", lowerStr.getString_value());

        Assertions.assertTrue(upperInt.isSetLong_value());
        Assertions.assertEquals(5L, upperInt.getLong_value());
        Assertions.assertTrue(upperStr.isSetString_value());
        Assertions.assertEquals("z", upperStr.getString_value());
    }

    @Test
    public void testLowerOnlyRangeToThrift() {
        // [ (10), +inf )
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.BIGINT, 10L)));
        Range<Tuple> range = Range.ge(lower);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertFalse(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant tv = tRange.getLower_bound().getValues().get(0);
        Assertions.assertTrue(tv.isSetLong_value());
        Assertions.assertEquals(10L, tv.getLong_value());
    }

    @Test
    public void testUpperOnlyRangeToThrift() {
        // (-inf, 100 )
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.BIGINT, 100L)));
        Range<Tuple> range = Range.lt(upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertFalse(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant tv = tRange.getUpper_bound().getValues().get(0);
        Assertions.assertTrue(tv.isSetLong_value());
        Assertions.assertEquals(100L, tv.getLong_value());
    }

    @Test
    public void testHalfOpenRangeToThrift() {
        // [ (1),  (5) )
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1)));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5)));

        Range<Tuple> range = Range.gelt(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertTrue(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant lowerInt = tRange.getLower_bound().getValues().get(0);
        TVariant upperInt = tRange.getUpper_bound().getValues().get(0);
        Assertions.assertTrue(lowerInt.isSetLong_value());
        Assertions.assertEquals(1L, lowerInt.getLong_value());
        Assertions.assertTrue(upperInt.isSetLong_value());
        Assertions.assertEquals(5L, upperInt.getLong_value());
    }

    @Test
    public void testOpenRangeToThrift() {
        // ( (1),  (5) )
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1)));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5)));

        Range<Tuple> range = Range.gtlt(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());
        Assertions.assertFalse(tRange.isLower_bound_included());
        Assertions.assertFalse(tRange.isUpper_bound_included());

        TVariant lowerInt = tRange.getLower_bound().getValues().get(0);
        TVariant upperInt = tRange.getUpper_bound().getValues().get(0);
        Assertions.assertTrue(lowerInt.isSetLong_value());
        Assertions.assertEquals(1L, lowerInt.getLong_value());
        Assertions.assertTrue(upperInt.isSetLong_value());
        Assertions.assertEquals(5L, upperInt.getLong_value());
    }

    @Test
    public void testDateRangeToThrift() {
        // [ date '2024-01-01', date '2024-01-31' ]
        Tuple lower = new Tuple(Arrays.asList(
                new DateVariant(DateType.DATE, "2024-01-01")));
        Tuple upper = new Tuple(Arrays.asList(
                new DateVariant(DateType.DATE, "2024-01-31")));

        Range<Tuple> range = Range.gele(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TTabletRange tRange = tabletRange.toThrift();
        Assertions.assertTrue(tRange.isSetLower_bound());
        Assertions.assertTrue(tRange.isSetUpper_bound());

        TVariant lowerDate = tRange.getLower_bound().getValues().get(0);
        TVariant upperDate = tRange.getUpper_bound().getValues().get(0);

        // Dates should be encoded via string_value, not long_value.
        Assertions.assertFalse(lowerDate.isSetLong_value());
        Assertions.assertTrue(lowerDate.isSetString_value());
        Assertions.assertFalse(lowerDate.getString_value().isEmpty());

        Assertions.assertFalse(upperDate.isSetLong_value());
        Assertions.assertTrue(upperDate.isSetString_value());
        Assertions.assertFalse(upperDate.getString_value().isEmpty());
    }
}


