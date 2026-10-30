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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.NullVariant;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TrailingSortKeyRangeReprojectionTest {

    private static final Column NEW_COLUMN = new Column("k2", VarcharType.VARCHAR);

    private static Tuple makeTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    @Test
    public void testAppendToAllRangeReturnsAll() {
        Range<Tuple> result = TrailingSortKeyRangeReprojection.appendTrailing(Range.all(), List.of(NEW_COLUMN));
        Assertions.assertTrue(result.isAll());
    }

    // [100, 200) -> [(100, NULL), (200, NULL))
    @Test
    public void testAppendToBoundedRangePreservesPrefixAndInclusivity() {
        Range<Tuple> old = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> result = TrailingSortKeyRangeReprojection.appendTrailing(old, List.of(NEW_COLUMN));

        Assertions.assertTrue(result.isLowerBoundIncluded());
        Assertions.assertFalse(result.isUpperBoundIncluded());

        Tuple lower = result.getLowerBound();
        Assertions.assertEquals(2, lower.getValues().size());
        Assertions.assertEquals("100", lower.getValues().get(0).getStringValue());
        Assertions.assertTrue(lower.getValues().get(1) instanceof NullVariant);

        Tuple upper = result.getUpperBound();
        Assertions.assertEquals(2, upper.getValues().size());
        Assertions.assertEquals("200", upper.getValues().get(0).getStringValue());
        Assertions.assertTrue(upper.getValues().get(1) instanceof NullVariant);

        // original tuple must not be mutated
        Assertions.assertEquals(1, old.getLowerBound().getValues().size());
        Assertions.assertEquals(1, old.getUpperBound().getValues().size());
    }

    // (-inf, 200) -> (-inf, (200, NULL))
    @Test
    public void testAppendKeepsLowerUnboundedAndExtendsUpperOnly() {
        Range<Tuple> old = Range.lt(makeTuple(200));
        Range<Tuple> result = TrailingSortKeyRangeReprojection.appendTrailing(old, List.of(NEW_COLUMN));

        Assertions.assertTrue(result.isMinimum());
        Assertions.assertNull(result.getLowerBound());
        Assertions.assertFalse(result.isUpperBoundIncluded());

        Tuple upper = result.getUpperBound();
        Assertions.assertEquals(2, upper.getValues().size());
        Assertions.assertEquals("200", upper.getValues().get(0).getStringValue());
        Assertions.assertTrue(upper.getValues().get(1) instanceof NullVariant);
    }

    // [100, +inf) -> [(100, NULL), +inf)
    @Test
    public void testAppendKeepsUpperUnboundedAndExtendsLowerOnly() {
        Range<Tuple> old = Range.ge(makeTuple(100));
        Range<Tuple> result = TrailingSortKeyRangeReprojection.appendTrailing(old, List.of(NEW_COLUMN));

        Assertions.assertTrue(result.isMaximum());
        Assertions.assertNull(result.getUpperBound());
        Assertions.assertTrue(result.isLowerBoundIncluded());

        Tuple lower = result.getLowerBound();
        Assertions.assertEquals(2, lower.getValues().size());
        Assertions.assertEquals("100", lower.getValues().get(0).getStringValue());
        Assertions.assertTrue(lower.getValues().get(1) instanceof NullVariant);
    }

    // [100, 200) with two trailing keys -> [(100, NULL, NULL), (200, NULL, NULL))
    @Test
    public void testAppendMultipleTrailingColumns() {
        Column k3 = new Column("k3", IntegerType.INT);
        Range<Tuple> old = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> result = TrailingSortKeyRangeReprojection.appendTrailing(old, List.of(NEW_COLUMN, k3));

        Tuple lower = result.getLowerBound();
        Assertions.assertEquals(3, lower.getValues().size());
        Assertions.assertEquals("100", lower.getValues().get(0).getStringValue());
        Assertions.assertTrue(lower.getValues().get(1) instanceof NullVariant);
        Assertions.assertTrue(lower.getValues().get(2) instanceof NullVariant);
        Assertions.assertEquals(VarcharType.VARCHAR, lower.getValues().get(1).getType());
        Assertions.assertEquals(IntegerType.INT, lower.getValues().get(2).getType());

        Tuple upper = result.getUpperBound();
        Assertions.assertEquals(3, upper.getValues().size());
        Assertions.assertEquals("200", upper.getValues().get(0).getStringValue());
        Assertions.assertTrue(upper.getValues().get(1) instanceof NullVariant);
        Assertions.assertTrue(upper.getValues().get(2) instanceof NullVariant);
    }

    @Test
    public void testAppendedNullVariantHasNewColumnType() {
        Range<Tuple> old = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> result = TrailingSortKeyRangeReprojection.appendTrailing(old, List.of(NEW_COLUMN));

        Assertions.assertEquals(VarcharType.VARCHAR, result.getLowerBound().getValues().get(1).getType());
        Assertions.assertEquals(VarcharType.VARCHAR, result.getUpperBound().getValues().get(1).getType());
    }
}
