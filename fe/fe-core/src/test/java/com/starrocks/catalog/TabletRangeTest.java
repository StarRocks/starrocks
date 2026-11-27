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

import com.google.gson.Gson;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.common.io.DeepCopy;
import com.starrocks.persist.gson.DefaultGsonBuilderFactory;
import com.starrocks.proto.TabletRangePB;
import com.starrocks.proto.TuplePB;
import com.starrocks.proto.VariantPB;
import com.starrocks.proto.VariantTypePB;
import com.starrocks.thrift.TTabletRange;
import com.starrocks.thrift.TTuple;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

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
    public void testAllRangeToProto() {
        Range<Tuple> all = Range.all();
        TabletRange tabletRange = new TabletRange(all);

        TabletRangePB rangePB = tabletRange.toProto();
        Assertions.assertNull(rangePB.lowerBound);
        Assertions.assertNull(rangePB.upperBound);
        Assertions.assertFalse(rangePB.lowerBoundIncluded);
        Assertions.assertFalse(rangePB.upperBoundIncluded);
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

        // All Variant values are encoded via the `value` field.
        Assertions.assertTrue(lowerInt.isSetValue());
        Assertions.assertEquals("1", new String(lowerInt.getValue(), StandardCharsets.UTF_8));
        Assertions.assertTrue(lowerStr.isSetValue());
        Assertions.assertEquals("a", new String(lowerStr.getValue(), StandardCharsets.UTF_8));

        Assertions.assertTrue(upperInt.isSetValue());
        Assertions.assertEquals("5", new String(upperInt.getValue(), StandardCharsets.UTF_8));
        Assertions.assertTrue(upperStr.isSetValue());
        Assertions.assertEquals("z", new String(upperStr.getValue(), StandardCharsets.UTF_8));
    }

    @Test
    public void testClosedRangeToProto() {
        Tuple lower = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 1),
                new StringVariant(VarcharType.VARCHAR, "a")));
        Tuple upper = new Tuple(Arrays.asList(
                new IntVariant(IntegerType.INT, 5),
                new StringVariant(VarcharType.VARCHAR, "z")));

        Range<Tuple> range = Range.gele(lower, upper);
        TabletRange tabletRange = new TabletRange(range);

        TabletRangePB rangePB = tabletRange.toProto();
        Assertions.assertNotNull(rangePB.lowerBound);
        Assertions.assertNotNull(rangePB.upperBound);
        Assertions.assertTrue(rangePB.lowerBoundIncluded);
        Assertions.assertTrue(rangePB.upperBoundIncluded);

        TuplePB lowerPB = rangePB.lowerBound;
        TuplePB upperPB = rangePB.upperBound;
        Assertions.assertEquals(2, lowerPB.values.size());
        Assertions.assertEquals(2, upperPB.values.size());

        VariantPB lowerInt = lowerPB.values.get(0);
        VariantPB lowerStr = lowerPB.values.get(1);
        VariantPB upperInt = upperPB.values.get(0);
        VariantPB upperStr = upperPB.values.get(1);

        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, lowerInt.variantType);
        Assertions.assertEquals("1", new String(lowerInt.value, StandardCharsets.UTF_8));
        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, lowerStr.variantType);
        Assertions.assertEquals("a", new String(lowerStr.value, StandardCharsets.UTF_8));

        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, upperInt.variantType);
        Assertions.assertEquals("5", new String(upperInt.value, StandardCharsets.UTF_8));
        Assertions.assertEquals(VariantTypePB.NORMAL_VALUE, upperStr.variantType);
        Assertions.assertEquals("z", new String(upperStr.value, StandardCharsets.UTF_8));
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
        Assertions.assertTrue(tv.isSetValue());
        Assertions.assertEquals("10", new String(tv.getValue(), StandardCharsets.UTF_8));
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
        Assertions.assertTrue(tv.isSetValue());
        Assertions.assertEquals("100", new String(tv.getValue(), StandardCharsets.UTF_8));
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
        Assertions.assertTrue(lowerInt.isSetValue());
        Assertions.assertEquals("1", new String(lowerInt.getValue(), StandardCharsets.UTF_8));
        Assertions.assertTrue(upperInt.isSetValue());
        Assertions.assertEquals("5", new String(upperInt.getValue(), StandardCharsets.UTF_8));
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
        Assertions.assertTrue(lowerInt.isSetValue());
        Assertions.assertEquals("1", new String(lowerInt.getValue(), StandardCharsets.UTF_8));
        Assertions.assertTrue(upperInt.isSetValue());
        Assertions.assertEquals("5", new String(upperInt.getValue(), StandardCharsets.UTF_8));
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

        // Dates should be encoded via the `value` field.
        Assertions.assertTrue(lowerDate.isSetValue());
        Assertions.assertTrue(lowerDate.getValue().length > 0);

        Assertions.assertTrue(upperDate.isSetValue());
        Assertions.assertTrue(upperDate.getValue().length > 0);
    }

    // A TabletRange as the released writer persisted it, pinned verbatim.
    private static final String LEGACY_TABLET_RANGE =
            "{\"range\":{\"lowerBound\":{\"values\":[{\"clazz\":\"IntVariant\",\"value\":1,"
                    + "\"type\":{\"type\":\"INT\",\"len\":-1,\"precision\":0,\"scale\":0}},"
                    + "{\"clazz\":\"StringVariant\",\"value\":\"a\",\"type\":{\"type\":\"VARCHAR\",\"len\":-1,"
                    + "\"precision\":0,\"scale\":0}}]},\"upperBound\":{\"values\":[{\"clazz\":\"IntVariant\","
                    + "\"value\":5,\"type\":{\"type\":\"INT\",\"len\":-1,\"precision\":0,\"scale\":0}},"
                    + "{\"clazz\":\"StringVariant\",\"value\":\"z\",\"type\":{\"type\":\"VARCHAR\",\"len\":-1,"
                    + "\"precision\":0,\"scale\":0}}]},\"lowerBoundIncluded\":true}}";

    @Test
    public void testLegacyTupleInPersistedRanges() {
        boolean saved = Config.metadata_ignore_unknown_subtype;
        try {
            for (boolean ignoreUnknown : new boolean[] {false, true}) {
                Config.metadata_ignore_unknown_subtype = ignoreUnknown;
                Gson gson = new DefaultGsonBuilderFactory().create().create();
                TabletRange loaded = gson.fromJson(LEGACY_TABLET_RANGE, TabletRange.class);
                Tuple lower = loaded.getRange().getLowerBound();
                Tuple upper = loaded.getRange().getUpperBound();
                Assertions.assertEquals(2, lower.getValues().size());
                Assertions.assertEquals(2, upper.getValues().size());
                Assertions.assertInstanceOf(IntVariant.class, lower.getValues().get(0));
                Assertions.assertInstanceOf(StringVariant.class, lower.getValues().get(1));
                Assertions.assertEquals(VarcharType.VARCHAR.getPrimitiveType(),
                        lower.getValues().get(1).getType().getPrimitiveType());
                Assertions.assertEquals(1, lower.getValues().get(0).getLongValue());
                Assertions.assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), lower.getValues().get(1).getBytesValue());
                Assertions.assertEquals(5, upper.getValues().get(0).getLongValue());
                Assertions.assertArrayEquals("z".getBytes(StandardCharsets.UTF_8), upper.getValues().get(1).getBytesValue());

                String json = gson.toJson(loaded);
                Assertions.assertTrue(json.contains("\"clazz\":\"StringVariant\""), json);
                Assertions.assertTrue(json.contains("\"value\":\"a\""), json);
                Assertions.assertEquals(loaded.getRange(), gson.fromJson(json, TabletRange.class).getRange());
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = saved;
        }
    }

    @Test
    public void testInvalidUtf8TupleInPersistedRanges() {
        boolean saved = Config.metadata_ignore_unknown_subtype;
        try {
            byte[] lowerBytes = {'a', (byte) 0xFF};
            byte[] upperBytes = {'z', 0, (byte) 0xC3};
            TabletRange original = new TabletRange(Range.gelt(
                    new Tuple(Arrays.asList(new IntVariant(IntegerType.INT, 1),
                            Variant.of(VarcharType.VARCHAR, lowerBytes))),
                    new Tuple(Arrays.asList(new IntVariant(IntegerType.INT, 5),
                            Variant.of(VarcharType.VARCHAR, upperBytes)))));
            for (boolean ignoreUnknown : new boolean[] {false, true}) {
                Config.metadata_ignore_unknown_subtype = ignoreUnknown;
                Gson gson = new DefaultGsonBuilderFactory().create().create();
                String json = gson.toJson(original);
                Assertions.assertTrue(json.contains("\"clazz\":\"StringVariant\""), json);
                TabletRange loaded = gson.fromJson(json, TabletRange.class);
                List<Variant> lower = loaded.getRange().getLowerBound().getValues();
                List<Variant> upper = loaded.getRange().getUpperBound().getValues();
                Assertions.assertEquals(2, lower.size());
                Assertions.assertEquals(2, upper.size());
                Assertions.assertEquals(1, lower.get(0).getLongValue());
                Assertions.assertArrayEquals(lowerBytes, lower.get(1).getBytesValue());
                Assertions.assertEquals(5, upper.get(0).getLongValue());
                Assertions.assertArrayEquals(upperBytes, upper.get(1).getBytesValue());
                Assertions.assertEquals(original.getRange(), loaded.getRange());
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = saved;
        }
    }

    @Test
    public void testDeepCopyKeepsBytes() {
        byte[] lowerBytes = new byte[12];
        Arrays.fill(lowerBytes, 0, 10, (byte) 'p');
        lowerBytes[10] = (byte) 0xC3;
        lowerBytes[11] = (byte) 0xA9;
        byte[] upperBytes = Arrays.copyOf(lowerBytes, 13);
        upperBytes[12] = (byte) 0xFF;
        Tuple lower = new Tuple(Arrays.asList(new StringVariant(VarcharType.VARCHAR, lowerBytes)));
        Tuple upper = new Tuple(Arrays.asList(new StringVariant(VarcharType.VARCHAR, upperBytes)));
        TabletRange original = new TabletRange(Range.gelt(lower, upper));

        TabletRange copy = DeepCopy.copyWithGson(original, TabletRange.class);

        Assertions.assertNotNull(copy);
        Assertions.assertArrayEquals(lowerBytes, copy.getRange().getLowerBound().getValues().get(0).getBytesValue());
        Assertions.assertArrayEquals(upperBytes, copy.getRange().getUpperBound().getValues().get(0).getBytesValue());
    }
}
