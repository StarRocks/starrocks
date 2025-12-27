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

import com.google.common.collect.Lists;
import com.staros.proto.PlacementPolicy;
import com.staros.proto.ShardGroupInfo;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShardGroupTest {
    private String serviceId = "ShardGroupTest";

    @Test
    public void testShardGroup() {
        // Constructor- style 1
        long groupId = 999;
        ShardGroup shardGroup = new ShardGroup(serviceId, groupId);

        Assert.assertEquals(shardGroup.getServiceId(), serviceId);
        Assert.assertEquals(shardGroup.getGroupId(), groupId);
        Assert.assertTrue(shardGroup.getLabels().isEmpty());
        Assert.assertTrue(shardGroup.getProperties().isEmpty());
        Assert.assertEquals(shardGroup.getPlacementPolicy(), PlacementPolicy.NONE);
        Assert.assertFalse(shardGroup.isAnonymous());
        Assert.assertEquals(shardGroup.getMetaGroupId(), 0);

        Assert.assertNull(shardGroup.getProperty("abc"));
        Assert.assertNull(shardGroup.getLabel("label1"));

        long shardId1 = 888;
        long shardId2 = 777;
        long shardId3 = 666;
        Assert.assertEquals(shardGroup.addShardId(shardId1), true);
        Assert.assertEquals(shardGroup.addShardId(shardId2), true);
        Assert.assertEquals(shardGroup.addShardId(shardId3), true);
        Assert.assertEquals(shardGroup.addShardId(shardId1), false);

        Assert.assertEquals(shardGroup.removeShardId(shardId1), true);
        Assert.assertEquals(shardGroup.removeShardId(shardId1), false);

        List<Long> shardIds = shardGroup.getShardIds();
        Assert.assertEquals(shardIds.size(), 2);
        Assert.assertEquals(shardIds.contains((Long) shardId2), true);
        Assert.assertEquals(shardIds.contains((Long) shardId3), true);

        // Constructor - style 2
        long metaGroupId = 123;
        ShardGroup group2 = new ShardGroup(serviceId, groupId, PlacementPolicy.RANDOM, false /* anonymous */, metaGroupId);
        Assert.assertEquals(group2.getServiceId(), serviceId);
        Assert.assertEquals(group2.getGroupId(), groupId);
        Assert.assertEquals(group2.getMetaGroupId(), metaGroupId);
        Assert.assertTrue(group2.getLabels().isEmpty());
        Assert.assertTrue(group2.getProperties().isEmpty());
        Assert.assertEquals(group2.getPlacementPolicy(), PlacementPolicy.RANDOM);
        Assert.assertFalse(group2.isAnonymous());

        Map<String, String> props = new HashMap<>();
        props.put("p1", "v1");
        Map<String, String> labels = new HashMap<>();
        labels.put("l1", "t1");
        // Constructor - style 3
        ShardGroupInfo groupInfo = ShardGroupInfo.newBuilder().setServiceId(serviceId)
                .setGroupId(groupId)
                .setPolicy(PlacementPolicy.PACK)
                .setAnonymous(false)
                .setMetaGroupId(metaGroupId)
                .putAllProperties(props)
                .putAllLabels(labels)
                .build();
        ShardGroup group3 = new ShardGroup(groupInfo);
        Assert.assertEquals(group3.getServiceId(), serviceId);
        Assert.assertEquals(group3.getGroupId(), groupId);
        Assert.assertFalse(group3.isAnonymous());
        Assert.assertEquals(group3.getMetaGroupId(), metaGroupId);
        Assert.assertEquals(group3.getLabels(), labels);
        Assert.assertEquals(group3.getProperties(), props);
        Assert.assertEquals(group3.getPlacementPolicy(), PlacementPolicy.PACK);
        Assert.assertEquals(group3.getProperty("p1"), "v1");
        Assert.assertEquals(group3.getLabel("l1"), "t1");
    }

    @Test
    public void testSerialization() {
        long groupId = 12345;
        long metaGroupId = 54321;
        Map<String, String> props = new HashMap<>();
        props.put("p1", "v1");

        Map<String, String> labels = new HashMap<>();
        labels.put("l1", "t1");

        ShardGroup shardGroup1 = new ShardGroup(serviceId, groupId, PlacementPolicy.SPREAD, true, metaGroupId, props, labels);

        // serialization
        ShardGroupInfo info = shardGroup1.toProtobuf();

        // deserialization
        ShardGroup shardGroup2 = ShardGroup.fromProtobuf(info);

        Assert.assertEquals(shardGroup1.getServiceId(), shardGroup2.getServiceId());
        Assert.assertEquals(shardGroup1.getGroupId(), shardGroup2.getGroupId());
        Assert.assertEquals(shardGroup1.getPlacementPolicy(), shardGroup2.getPlacementPolicy());
        Assert.assertEquals(shardGroup1.isAnonymous(), shardGroup2.isAnonymous());
        Assert.assertEquals(shardGroup1.getMetaGroupId(), shardGroup2.getMetaGroupId());
        Assert.assertEquals(shardGroup1.getProperties(), shardGroup2.getProperties());
        Assert.assertEquals(shardGroup1.getLabels(), shardGroup2.getLabels());
    }

    @Test
    public void testShardGroupSetIndex() throws IllegalAccessException {
        long groupId = 9527;
        long indexThreshold = (long) FieldUtils.readStaticField(ShardGroup.class, "SET_INDEX_THRESHOLD", true);
        ShardGroup group = new ShardGroup(serviceId, groupId);
        long i = 0;
        for (; i < indexThreshold; ++i) {
            group.addShardId(i);
            Object idSets = FieldUtils.readField(group, "shardIdSet", true);
            Assert.assertNull(idSets);
            Assert.assertEquals(i + 1, group.getShardIds().size());
        }
        for (; i < indexThreshold * 2; ++i) {
            // two items, one added, the other is duplicated.
            List<Long> batchIds = Lists.newArrayList(i - indexThreshold, i);
            List<Long> added = group.batchAddShardId(batchIds);
            Set<Long> idSets = (Set<Long>) FieldUtils.readField(group, "shardIdSet", true);
            Assert.assertEquals(1L, added.size());
            Assert.assertEquals(i, (long) added.get(0));
            Assert.assertEquals(i + 1, group.getShardIds().size());

            // both are added. repeat addition returns false
            Assert.assertFalse(group.addShardId(i));
            Assert.assertFalse(group.addShardId(i - indexThreshold));

            // check idSet
            Assert.assertEquals(idSets.size(), group.getShardIds().size());
            Assert.assertEquals(new HashSet<>(group.getShardIds()), idSets);
        }

        List<Long> ids = group.getShardIds();
        for (long id : ids) {
            // id removed from idSet as well
            group.removeShardId(id);
            Set<Long> idSets = (Set<Long>) FieldUtils.readField(group, "shardIdSet", true);
            Assert.assertFalse(idSets.contains(id));
            Assert.assertEquals(new HashSet<>(group.getShardIds()), idSets);
        }
    }
}
