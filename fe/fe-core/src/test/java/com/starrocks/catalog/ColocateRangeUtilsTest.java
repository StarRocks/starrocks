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
}
