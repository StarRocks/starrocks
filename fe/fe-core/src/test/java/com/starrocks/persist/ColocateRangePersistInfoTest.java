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

package com.starrocks.persist;

import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Range;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

public class ColocateRangePersistInfoTest {

    private static final long COLOCATE_GROUP_ID = 100L;

    private static Tuple makeTuple(int value) {
        return new Tuple(Collections.singletonList(Variant.of(IntegerType.INT, String.valueOf(value))));
    }

    @Test
    public void testRoundTripSingleAllRange() {
        ColocateRangePersistInfo info = ColocateRangePersistInfo.create(COLOCATE_GROUP_ID,
                Collections.singletonList(new ColocateRange(Range.all(), 1001L)));

        String json = GsonUtils.GSON.toJson(info);
        ColocateRangePersistInfo restored = GsonUtils.GSON.fromJson(json, ColocateRangePersistInfo.class);

        Assertions.assertEquals(info, restored);
        Assertions.assertEquals(COLOCATE_GROUP_ID, restored.getColocateGroupId());
        Assertions.assertEquals(1, restored.getColocateRanges().size());
        Assertions.assertTrue(restored.getColocateRanges().get(0).getRange().isAll());
        Assertions.assertEquals(1001L, restored.getColocateRanges().get(0).getShardGroupId());
    }

    @Test
    public void testRoundTripMultipleRanges() {
        ColocateRangePersistInfo info = ColocateRangePersistInfo.create(COLOCATE_GROUP_ID, Arrays.asList(
                new ColocateRange(Range.lt(makeTuple(100)), 1001L),
                new ColocateRange(Range.gelt(makeTuple(100), makeTuple(200)), 1002L),
                new ColocateRange(Range.ge(makeTuple(200)), 1003L)));

        String json = GsonUtils.GSON.toJson(info);
        ColocateRangePersistInfo restored = GsonUtils.GSON.fromJson(json, ColocateRangePersistInfo.class);

        Assertions.assertEquals(info, restored);
        Assertions.assertEquals(3, restored.getColocateRanges().size());
        Assertions.assertEquals(1001L, restored.getColocateRanges().get(0).getShardGroupId());
        Assertions.assertEquals(1002L, restored.getColocateRanges().get(1).getShardGroupId());
        Assertions.assertEquals(1003L, restored.getColocateRanges().get(2).getShardGroupId());
    }

    @Test
    public void testCreateRejectsEmptyList() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangePersistInfo.create(COLOCATE_GROUP_ID, Collections.emptyList()));
    }

    @Test
    public void testCreateRejectsNullList() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ColocateRangePersistInfo.create(COLOCATE_GROUP_ID, null));
    }
}
