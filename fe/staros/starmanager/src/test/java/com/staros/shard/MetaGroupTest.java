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


package com.staros.shard;

import com.staros.proto.MetaGroupInfo;
import com.staros.proto.PlacementPolicy;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MetaGroupTest {
    @Test
    public void testMetaGroup() {
        String serviceId = "testMetaGroup";
        long metaGroupId = 123;
        List<Long> shardGroupIds = new ArrayList<>();
        shardGroupIds.add(1L);
        shardGroupIds.add(2L);
        shardGroupIds.add(3L);
        PlacementPolicy placementPolicy = PlacementPolicy.PACK;
        MetaGroup metaGroup = new MetaGroup(serviceId, metaGroupId, shardGroupIds, placementPolicy);

        Assert.assertEquals(metaGroup.getServiceId(), serviceId);
        Assert.assertEquals(metaGroup.getMetaGroupId(), metaGroupId);
        List<Long> shardGroupIds2 = metaGroup.getShardGroupIds();
        Assert.assertEquals(shardGroupIds.size(), shardGroupIds2.size());
        for (int i = 0; i < shardGroupIds.size(); ++i) {
            Assert.assertEquals(shardGroupIds.get(i), shardGroupIds2.get(i));
        }
        Assert.assertEquals(metaGroup.getPlacementPolicy(), placementPolicy);

        // test set shard group id list
        shardGroupIds.clear();
        MetaGroup metaGroup2 = new MetaGroup(serviceId, metaGroupId, shardGroupIds, placementPolicy);
        metaGroup2.setShardGroupIds(shardGroupIds2);
        List<Long> shardGroupIds3 = metaGroup.getShardGroupIds();
        Assert.assertEquals(shardGroupIds2.size(), shardGroupIds3.size());
        for (int i = 0; i < shardGroupIds2.size(); ++i) {
            Assert.assertEquals(shardGroupIds2.get(i), shardGroupIds3.get(i));
        }
    }

    @Test
    public void testSerialization() {
        String serviceId = "testMetaGroupSerialization";
        long metaGroupId = 123;
        List<Long> shardGroupIds = new ArrayList<>();
        shardGroupIds.add(1L);
        shardGroupIds.add(2L);
        shardGroupIds.add(3L);
        PlacementPolicy placementPolicy = PlacementPolicy.PACK;
        MetaGroup metaGroup1 = new MetaGroup(serviceId, metaGroupId, shardGroupIds, placementPolicy);

        // serialization
        MetaGroupInfo info = metaGroup1.toProtobuf();

        // deserialization
        MetaGroup metaGroup2 = MetaGroup.fromProtobuf(info);

        Assert.assertEquals(metaGroup1.getServiceId(), metaGroup2.getServiceId());
        Assert.assertEquals(metaGroup1.getMetaGroupId(), metaGroup2.getMetaGroupId());
        Assert.assertEquals(metaGroup1.getShardGroupIds(), metaGroup2.getShardGroupIds());
        Assert.assertEquals(metaGroup1.getPlacementPolicy(), metaGroup2.getPlacementPolicy());
    }
}
