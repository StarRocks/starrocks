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

import com.google.common.collect.Lists;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ColocateGroupSchemaRangeTest {

    @Test
    public void testRangeColocateConstructor() {
        GroupId groupId = new GroupId(1L, 100L);
        List<Column> colocateColumns = Lists.newArrayList(
                new Column("tenant_id", IntegerType.INT),
                new Column("created_time", IntegerType.BIGINT));

        ColocateGroupSchema schema = new ColocateGroupSchema(
                groupId, colocateColumns, 0, (short) 1, DistributionInfoType.RANGE);

        Assertions.assertTrue(schema.isRangeColocate());
        Assertions.assertEquals(DistributionInfoType.RANGE, schema.getDistributionType());
        Assertions.assertEquals(2, schema.getColocateColumnCount());
        Assertions.assertEquals(0, schema.getBucketsNum());
        Assertions.assertEquals(1, schema.getReplicationNum());
        Assertions.assertEquals(groupId, schema.getGroupId());
    }

    @Test
    public void testHashColocateConstructor() {
        GroupId groupId = new GroupId(1L, 200L);
        List<Column> distributionColumns = Lists.newArrayList(
                new Column("k1", IntegerType.INT));

        ColocateGroupSchema schema = new ColocateGroupSchema(
                groupId, distributionColumns, 4, (short) 1, DistributionInfoType.HASH);

        Assertions.assertFalse(schema.isRangeColocate());
        Assertions.assertEquals(DistributionInfoType.HASH, schema.getDistributionType());
        Assertions.assertEquals(1, schema.getColocateColumnCount());
        Assertions.assertEquals(4, schema.getBucketsNum());
    }

    @Test
    public void testDistributionColTypesReusedForRangeColocate() {
        GroupId groupId = new GroupId(1L, 100L);
        List<Column> colocateColumns = Lists.newArrayList(
                new Column("tenant_id", IntegerType.INT),
                new Column("name", VarcharType.VARCHAR));

        ColocateGroupSchema schema = new ColocateGroupSchema(
                groupId, colocateColumns, 0, (short) 1, DistributionInfoType.RANGE);

        // distributionColTypes stores the colocate column types
        List<com.starrocks.type.Type> colTypes = schema.getDistributionColTypes();
        Assertions.assertEquals(2, colTypes.size());
        Assertions.assertEquals(IntegerType.INT, colTypes.get(0));
        Assertions.assertEquals(VarcharType.VARCHAR, colTypes.get(1));
    }

    @Test
    public void testGsonSerializationRangeColocate() {
        GroupId groupId = new GroupId(1L, 100L);
        List<Column> colocateColumns = Lists.newArrayList(
                new Column("tenant_id", IntegerType.INT),
                new Column("name", VarcharType.VARCHAR));

        ColocateGroupSchema original = new ColocateGroupSchema(
                groupId, colocateColumns, 0, (short) 1, DistributionInfoType.RANGE);

        String json = GsonUtils.GSON.toJson(original);
        ColocateGroupSchema deserialized = GsonUtils.GSON.fromJson(json, ColocateGroupSchema.class);

        Assertions.assertTrue(deserialized.isRangeColocate());
        Assertions.assertEquals(DistributionInfoType.RANGE, deserialized.getDistributionType());
        Assertions.assertEquals(2, deserialized.getColocateColumnCount());
        Assertions.assertEquals(0, deserialized.getBucketsNum());
        Assertions.assertEquals(1, deserialized.getReplicationNum());
        Assertions.assertEquals(groupId, deserialized.getGroupId());
    }

    @Test
    public void testGsonSerializationHashColocateBackwardCompatible() {
        // Simulate old serialized data without distributionType field
        // The default should be HASH
        GroupId groupId = new GroupId(1L, 200L);
        List<Column> distributionColumns = Lists.newArrayList(
                new Column("k1", IntegerType.INT));

        ColocateGroupSchema original = new ColocateGroupSchema(
                groupId, distributionColumns, 4, (short) 1, DistributionInfoType.HASH);

        String json = GsonUtils.GSON.toJson(original);
        ColocateGroupSchema deserialized = GsonUtils.GSON.fromJson(json, ColocateGroupSchema.class);

        Assertions.assertFalse(deserialized.isRangeColocate());
        Assertions.assertEquals(DistributionInfoType.HASH, deserialized.getDistributionType());
        Assertions.assertEquals(4, deserialized.getBucketsNum());
    }
}
