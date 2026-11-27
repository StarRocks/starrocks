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

import com.google.gson.Gson;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.IntVariant;
import com.starrocks.catalog.StringVariant;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.persist.gson.DefaultGsonBuilderFactory;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    // The persist info as the released writer wrote it, pinned verbatim.
    private static final String LEGACY_INFO =
            "{\"cg\":100,\"rs\":[{\"r\":{\"lowerBound\":{\"values\":[{\"clazz\":\"IntVariant\",\"value\":1,"
                    + "\"type\":{\"type\":\"INT\",\"len\":-1,\"precision\":0,\"scale\":0}},"
                    + "{\"clazz\":\"StringVariant\",\"value\":\"a\",\"type\":{\"type\":\"VARCHAR\",\"len\":-1,"
                    + "\"precision\":0,\"scale\":0}}]},\"upperBound\":{\"values\":[{\"clazz\":\"IntVariant\","
                    + "\"value\":5,\"type\":{\"type\":\"INT\",\"len\":-1,\"precision\":0,\"scale\":0}},"
                    + "{\"clazz\":\"StringVariant\",\"value\":\"z\",\"type\":{\"type\":\"VARCHAR\",\"len\":-1,"
                    + "\"precision\":0,\"scale\":0}}]},\"lowerBoundIncluded\":true},\"sg\":1001}]}";

    @Test
    public void testLegacyTupleInPersistedRanges() {
        boolean saved = Config.metadata_ignore_unknown_subtype;
        try {
            for (boolean ignoreUnknown : new boolean[] {false, true}) {
                Config.metadata_ignore_unknown_subtype = ignoreUnknown;
                Gson gson = new DefaultGsonBuilderFactory().create().create();
                ColocateRangePersistInfo loaded = gson.fromJson(LEGACY_INFO, ColocateRangePersistInfo.class);
                Assertions.assertEquals(1, loaded.getColocateRanges().size());
                Range<Tuple> range = loaded.getColocateRanges().get(0).getRange();
                List<Variant> lower = range.getLowerBound().getValues();
                List<Variant> upper = range.getUpperBound().getValues();
                Assertions.assertEquals(2, lower.size());
                Assertions.assertEquals(2, upper.size());
                Assertions.assertEquals(1, lower.get(0).getLongValue());
                Assertions.assertInstanceOf(StringVariant.class, lower.get(1));
                Assertions.assertEquals("VARCHAR", lower.get(1).getType().getPrimitiveType().name());
                Assertions.assertArrayEquals("a".getBytes(StandardCharsets.UTF_8), lower.get(1).getBytesValue());
                Assertions.assertEquals(5, upper.get(0).getLongValue());
                Assertions.assertArrayEquals("z".getBytes(StandardCharsets.UTF_8), upper.get(1).getBytesValue());

                String json = gson.toJson(loaded);
                Assertions.assertTrue(json.contains("\"clazz\":\"StringVariant\""), json);
                Assertions.assertTrue(json.contains("\"value\":\"a\""), json);
                Assertions.assertEquals(loaded, gson.fromJson(json, ColocateRangePersistInfo.class));
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
            ColocateRangePersistInfo original = ColocateRangePersistInfo.create(COLOCATE_GROUP_ID,
                    Collections.singletonList(new ColocateRange(Range.gelt(
                            new Tuple(Arrays.asList(new IntVariant(IntegerType.INT, 1),
                                    Variant.of(VarcharType.VARCHAR, lowerBytes))),
                            new Tuple(Arrays.asList(new IntVariant(IntegerType.INT, 5),
                                    Variant.of(VarcharType.VARCHAR, upperBytes)))), 1001L)));
            for (boolean ignoreUnknown : new boolean[] {false, true}) {
                Config.metadata_ignore_unknown_subtype = ignoreUnknown;
                Gson gson = new DefaultGsonBuilderFactory().create().create();
                String json = gson.toJson(original);
                Assertions.assertTrue(json.contains("\"clazz\":\"StringVariant\""), json);
                ColocateRangePersistInfo loaded = gson.fromJson(json, ColocateRangePersistInfo.class);
                Range<Tuple> range = loaded.getColocateRanges().get(0).getRange();
                List<Variant> lower = range.getLowerBound().getValues();
                List<Variant> upper = range.getUpperBound().getValues();
                Assertions.assertEquals(2, lower.size());
                Assertions.assertEquals(2, upper.size());
                Assertions.assertEquals(1, lower.get(0).getLongValue());
                Assertions.assertArrayEquals(lowerBytes, lower.get(1).getBytesValue());
                Assertions.assertEquals(5, upper.get(0).getLongValue());
                Assertions.assertArrayEquals(upperBytes, upper.get(1).getBytesValue());
                Assertions.assertEquals(original, loaded);
            }
        } finally {
            Config.metadata_ignore_unknown_subtype = saved;
        }
    }
}
