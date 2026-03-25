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
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ColocateRangeTest {

    private static Tuple makeTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    @Test
    public void testBasicProperties() {
        Range<Tuple> range = Range.gelt(makeTuple(100), makeTuple(200));
        ColocateRange colocateRange = new ColocateRange(range, 1001L);

        Assertions.assertEquals(range, colocateRange.getRange());
        Assertions.assertEquals(1001L, colocateRange.getShardGroupId());
    }

    @Test
    public void testAllRange() {
        Range<Tuple> allRange = Range.all();
        ColocateRange colocateRange = new ColocateRange(allRange, 2001L);

        Assertions.assertTrue(colocateRange.getRange().isAll());
        Assertions.assertEquals(2001L, colocateRange.getShardGroupId());
    }

    @Test
    public void testEquals() {
        Range<Tuple> range1 = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> range2 = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> range3 = Range.gelt(makeTuple(200), makeTuple(300));

        ColocateRange cr1 = new ColocateRange(range1, 1001L);
        ColocateRange cr2 = new ColocateRange(range2, 1001L);
        ColocateRange cr3 = new ColocateRange(range3, 1001L);
        ColocateRange cr4 = new ColocateRange(range1, 1002L);

        Assertions.assertEquals(cr1, cr2);
        Assertions.assertNotEquals(cr1, cr3);
        Assertions.assertNotEquals(cr1, cr4);
        Assertions.assertNotEquals(cr1, null);
    }

    @Test
    public void testHashCode() {
        Range<Tuple> range1 = Range.gelt(makeTuple(100), makeTuple(200));
        Range<Tuple> range2 = Range.gelt(makeTuple(100), makeTuple(200));

        ColocateRange cr1 = new ColocateRange(range1, 1001L);
        ColocateRange cr2 = new ColocateRange(range2, 1001L);

        Assertions.assertEquals(cr1.hashCode(), cr2.hashCode());
    }

    @Test
    public void testToString() {
        Range<Tuple> range = Range.gelt(makeTuple(100), makeTuple(200));
        ColocateRange colocateRange = new ColocateRange(range, 1001L);

        String str = colocateRange.toString();
        Assertions.assertTrue(str.contains("1001"));
        Assertions.assertTrue(str.contains("shardGroup"));
    }

    @Test
    public void testGsonSerialization() {
        Range<Tuple> range = Range.gelt(makeTuple(100), makeTuple(200));
        ColocateRange original = new ColocateRange(range, 1001L);

        String json = GsonUtils.GSON.toJson(original);
        ColocateRange deserialized = GsonUtils.GSON.fromJson(json, ColocateRange.class);

        Assertions.assertEquals(original.getShardGroupId(), deserialized.getShardGroupId());
        Assertions.assertEquals(original.getRange(), deserialized.getRange());
    }

    @Test
    public void testGsonSerializationAllRange() {
        ColocateRange original = new ColocateRange(Range.all(), 2001L);

        String json = GsonUtils.GSON.toJson(original);
        ColocateRange deserialized = GsonUtils.GSON.fromJson(json, ColocateRange.class);

        Assertions.assertEquals(2001L, deserialized.getShardGroupId());
        Assertions.assertTrue(deserialized.getRange().isAll());
    }

    @Test
    public void testNullRangeThrows() {
        Assertions.assertThrows(NullPointerException.class, () -> new ColocateRange(null, 1001L));
    }
}
