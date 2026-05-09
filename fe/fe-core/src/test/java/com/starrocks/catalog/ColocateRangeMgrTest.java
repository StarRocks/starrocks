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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ColocateRangeMgrTest {

    private ColocateRangeMgr colocateRangeMgr;
    private static final long COLOCATE_GROUP_ID = 100L;

    private static Tuple makeTuple(int value) {
        return new Tuple(Arrays.asList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    @BeforeEach
    public void setUp() {
        colocateRangeMgr = new ColocateRangeMgr();
    }

    // ---- initColocateGroup ----

    @Test
    public void testInitColocateGroup() {
        colocateRangeMgr.initColocateGroup(COLOCATE_GROUP_ID, 1001L);

        List<ColocateRange> ranges = colocateRangeMgr.getColocateRanges(COLOCATE_GROUP_ID);
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertTrue(ranges.get(0).getRange().isAll());
        Assertions.assertEquals(1001L, ranges.get(0).getShardGroupId());
    }

    @Test
    public void testInitColocateGroupDuplicate() {
        colocateRangeMgr.initColocateGroup(COLOCATE_GROUP_ID, 1001L);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> colocateRangeMgr.initColocateGroup(COLOCATE_GROUP_ID, 1002L));
    }

    // ---- containsColocateGroup ----

    @Test
    public void testContainsColocateGroup() {
        Assertions.assertFalse(colocateRangeMgr.containsColocateGroup(COLOCATE_GROUP_ID));
        colocateRangeMgr.initColocateGroup(COLOCATE_GROUP_ID, 1001L);
        Assertions.assertTrue(colocateRangeMgr.containsColocateGroup(COLOCATE_GROUP_ID));
    }

    // ---- getColocateRanges ----

    @Test
    public void testGetColocateRangesNonExistent() {
        List<ColocateRange> ranges = colocateRangeMgr.getColocateRanges(999L);
        Assertions.assertTrue(ranges.isEmpty());
    }

    // ---- getColocateRange ----

    @Test
    public void testGetColocateRangeInAllRange() {
        colocateRangeMgr.initColocateGroup(COLOCATE_GROUP_ID, 1001L);

        ColocateRange found = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(500));
        Assertions.assertNotNull(found);
        Assertions.assertEquals(1001L, found.getShardGroupId());
    }

    @Test
    public void testGetColocateRangeNonExistentGroup() {
        ColocateRange found = colocateRangeMgr.getColocateRange(999L, makeTuple(100));
        Assertions.assertNull(found);
    }

    @Test
    public void testGetColocateRangeAfterSetColocateRanges() {
        // Set up 3 ranges via setColocateRanges: (-inf,100), [100,200), [200,+inf)
        List<ColocateRange> ranges = Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 1001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 1002L),
                new ColocateRange(Range.ge(makeTuple(200)), 1003L));
        colocateRangeMgr.setColocateRanges(COLOCATE_GROUP_ID, ranges);

        // Find in (-inf, 100) range
        ColocateRange found50 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(50));
        Assertions.assertNotNull(found50);
        Assertions.assertEquals(1001L, found50.getShardGroupId());

        // Find in [100, 200) range
        ColocateRange found150 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(150));
        Assertions.assertNotNull(found150);
        Assertions.assertEquals(1002L, found150.getShardGroupId());

        // Find at boundary 100 (should be in [100, 200))
        ColocateRange found100 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(100));
        Assertions.assertNotNull(found100);
        Assertions.assertEquals(1002L, found100.getShardGroupId());

        // Find in [200, +inf) range
        ColocateRange found500 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(500));
        Assertions.assertNotNull(found500);
        Assertions.assertEquals(1003L, found500.getShardGroupId());
    }

    // ---- removeColocateGroup ----

    @Test
    public void testRemoveColocateGroup() {
        colocateRangeMgr.initColocateGroup(COLOCATE_GROUP_ID, 1001L);
        Assertions.assertTrue(colocateRangeMgr.containsColocateGroup(COLOCATE_GROUP_ID));

        colocateRangeMgr.removeColocateGroup(COLOCATE_GROUP_ID);
        Assertions.assertFalse(colocateRangeMgr.containsColocateGroup(COLOCATE_GROUP_ID));
        Assertions.assertTrue(colocateRangeMgr.getColocateRanges(COLOCATE_GROUP_ID).isEmpty());
    }

    // ---- setColocateRanges (replay) ----

    @Test
    public void testSetColocateRanges() {
        List<ColocateRange> ranges = Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(200)), 1001L),
                new ColocateRange(Range.ge(makeTuple(200)), 1002L));

        colocateRangeMgr.setColocateRanges(COLOCATE_GROUP_ID, ranges);

        List<ColocateRange> retrieved = colocateRangeMgr.getColocateRanges(COLOCATE_GROUP_ID);
        Assertions.assertEquals(2, retrieved.size());
        Assertions.assertEquals(1001L, retrieved.get(0).getShardGroupId());
        Assertions.assertEquals(1002L, retrieved.get(1).getShardGroupId());
    }

    // ---- Gson Serialization ----

    @Test
    public void testGsonSerialization() {
        // Set up 3 ranges via setColocateRanges
        List<ColocateRange> ranges = Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 1001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 1003L),
                new ColocateRange(Range.ge(makeTuple(200)), 1002L));
        colocateRangeMgr.setColocateRanges(COLOCATE_GROUP_ID, ranges);

        // Serialize
        String json = GsonUtils.GSON.toJson(colocateRangeMgr);

        // Deserialize
        ColocateRangeMgr deserialized = GsonUtils.GSON.fromJson(json, ColocateRangeMgr.class);

        // Verify
        List<ColocateRange> deserializedRanges = deserialized.getColocateRanges(COLOCATE_GROUP_ID);
        Assertions.assertEquals(3, deserializedRanges.size());
        Assertions.assertEquals(1001L, deserializedRanges.get(0).getShardGroupId());
        Assertions.assertEquals(1003L, deserializedRanges.get(1).getShardGroupId());
        Assertions.assertEquals(1002L, deserializedRanges.get(2).getShardGroupId());

        // Verify getColocateRange works after deserialization
        ColocateRange found = deserialized.getColocateRange(COLOCATE_GROUP_ID, makeTuple(150));
        Assertions.assertNotNull(found);
        Assertions.assertEquals(1003L, found.getShardGroupId());
    }

    // ---- Binary search correctness ----

    @Test
    public void testGetColocateRangeBinarySearchManyRanges() {
        // Build 100 ranges: (-inf,10), [10,20), [20,30), ..., [990,+inf)
        List<ColocateRange> ranges = new ArrayList<>();
        ranges.add(new ColocateRange(Range.lt(makeTuple(10)), 1000L));
        for (int i = 10; i < 990; i += 10) {
            ranges.add(new ColocateRange(Range.gelt(makeTuple(i), makeTuple(i + 10)), 1000L + i));
        }
        ranges.add(new ColocateRange(Range.ge(makeTuple(990)), 1990L));
        colocateRangeMgr.setColocateRanges(COLOCATE_GROUP_ID, ranges);

        Assertions.assertEquals(100, colocateRangeMgr.getColocateRanges(COLOCATE_GROUP_ID).size());

        // Value 5 should be in first range (-inf, 10) -> shardGroup 1000
        ColocateRange found5 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(5));
        Assertions.assertNotNull(found5);
        Assertions.assertEquals(1000L, found5.getShardGroupId());

        // Value 10 should be in [10, 20) -> shardGroup 1010
        ColocateRange found10 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(10));
        Assertions.assertNotNull(found10);
        Assertions.assertEquals(1010L, found10.getShardGroupId());

        // Value 15 should be in [10, 20) -> shardGroup 1010
        ColocateRange found15 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(15));
        Assertions.assertNotNull(found15);
        Assertions.assertEquals(1010L, found15.getShardGroupId());

        // Value 555 should be in [550, 560) -> shardGroup 1550
        ColocateRange found555 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(555));
        Assertions.assertNotNull(found555);
        Assertions.assertEquals(1550L, found555.getShardGroupId());

        // Value 990 should be in [990, +inf) -> shardGroup 1990
        ColocateRange found990 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(990));
        Assertions.assertNotNull(found990);
        Assertions.assertEquals(1990L, found990.getShardGroupId());

        // Value 99999 should be in [990, +inf) -> shardGroup 1990
        ColocateRange found99999 = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(99999));
        Assertions.assertNotNull(found99999);
        Assertions.assertEquals(1990L, found99999.getShardGroupId());
    }

    // ---- getColocateRangeIndex ----

    @Test
    public void testGetColocateRangeIndexNonExistentGroup() {
        Assertions.assertEquals(-1, colocateRangeMgr.getColocateRangeIndex(999L, makeTuple(100)));
    }

    @Test
    public void testGetColocateRangeIndexAllRange() {
        colocateRangeMgr.initColocateGroup(COLOCATE_GROUP_ID, 1001L);
        // Any value falls in the single (-inf, +inf) range at index 0.
        Assertions.assertEquals(0, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(-9999)));
        Assertions.assertEquals(0, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(0)));
        Assertions.assertEquals(0, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(9999)));
        // Null value (tablet lower bound is -inf) maps to the first range.
        Assertions.assertEquals(0, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, null));
    }

    @Test
    public void testGetColocateRangeIndexThreeRanges() {
        // (-inf, 100), [100, 200), [200, +inf)
        List<ColocateRange> ranges = Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 1001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 1002L),
                new ColocateRange(Range.ge(makeTuple(200)), 1003L));
        colocateRangeMgr.setColocateRanges(COLOCATE_GROUP_ID, ranges);

        Assertions.assertEquals(0, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, null));
        Assertions.assertEquals(0, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(50)));
        Assertions.assertEquals(1, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(100)));
        Assertions.assertEquals(1, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(150)));
        Assertions.assertEquals(2, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(200)));
        Assertions.assertEquals(2, colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(99999)));
    }

    @Test
    public void testGetColocateRangeIndexMatchesLookup() {
        // The index must point to the same ColocateRange that getColocateRange returns.
        List<ColocateRange> ranges = Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(50)), 1001L),
                new ColocateRange(Range.gelt(makeTuple(50), makeTuple(150)), 1002L),
                new ColocateRange(Range.gelt(makeTuple(150), makeTuple(250)), 1003L),
                new ColocateRange(Range.ge(makeTuple(250)), 1004L));
        colocateRangeMgr.setColocateRanges(COLOCATE_GROUP_ID, ranges);

        for (int probe : new int[] {-1, 49, 50, 100, 149, 150, 249, 250, 9999}) {
            ColocateRange found = colocateRangeMgr.getColocateRange(COLOCATE_GROUP_ID, makeTuple(probe));
            int index = colocateRangeMgr.getColocateRangeIndex(COLOCATE_GROUP_ID, makeTuple(probe));
            Assertions.assertEquals(ranges.indexOf(found), index, "probe=" + probe);
        }
    }

    // ---- Multiple colocate groups ----

    @Test
    public void testMultipleColocateGroups() {
        long groupId1 = 100L;
        long groupId2 = 200L;

        colocateRangeMgr.initColocateGroup(groupId1, 1001L);
        colocateRangeMgr.initColocateGroup(groupId2, 2001L);

        // Group 1 has 1 range
        Assertions.assertEquals(1, colocateRangeMgr.getColocateRanges(groupId1).size());
        // Group 2 has 1 range
        Assertions.assertEquals(1, colocateRangeMgr.getColocateRanges(groupId2).size());

        // Find in group 1
        ColocateRange found1 = colocateRangeMgr.getColocateRange(groupId1, makeTuple(100));
        Assertions.assertEquals(1001L, found1.getShardGroupId());

        // Find in group 2
        ColocateRange found2 = colocateRangeMgr.getColocateRange(groupId2, makeTuple(100));
        Assertions.assertEquals(2001L, found2.getShardGroupId());
    }
}
