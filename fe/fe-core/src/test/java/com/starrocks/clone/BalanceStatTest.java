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

package com.starrocks.clone;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.clone.BalanceStat.BalanceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class BalanceStatTest {

    @Test
    public void testNormal() {
        {
            BalanceStat stat = BalanceStat.BALANCED_STAT;
            Assertions.assertTrue(stat.isBalanced());
            Assertions.assertNull(stat.getBalanceType());
            Assertions.assertEquals("{\"balanced\":true}", stat.toString());
        }

        {
            BalanceStat stat = BalanceStat.createClusterDiskBalanceStat(1L, 2L, 0.9, 0.1);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.INTER_NODE_DISK_USAGE, stat.getBalanceType());
            Assertions.assertEquals("inter-node disk usage", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxUsedPercent\":0.9,\"minUsedPercent\":0.1,\"maxBeId\":1,\"minBeId\":2," +
                            "\"type\":\"INTER_NODE_DISK_USAGE\",\"balanced\":false}",
                    stat.toString());
        }

        {
            BalanceStat stat = BalanceStat.createClusterTabletBalanceStat(1L, 2L, 9L, 1L);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.INTER_NODE_TABLET_DISTRIBUTION, stat.getBalanceType());
            Assertions.assertEquals("inter-node tablet distribution", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxTabletNum\":9,\"minTabletNum\":1,\"maxBeId\":1,\"minBeId\":2," +
                            "\"type\":\"INTER_NODE_TABLET_DISTRIBUTION\",\"balanced\":false}",
                    stat.toString());
        }

        {
            BalanceStat stat = BalanceStat.createBackendDiskBalanceStat(1L, "disk1", "disk2", 0.9, 0.1);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.INTRA_NODE_DISK_USAGE, stat.getBalanceType());
            Assertions.assertEquals("intra-node disk usage", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxUsedPercent\":0.9,\"minUsedPercent\":0.1,\"beId\":1,\"maxPath\":\"disk1\",\"minPath\":\"disk2\"," +
                            "\"type\":\"INTRA_NODE_DISK_USAGE\",\"balanced\":false}",
                    stat.toString());
        }

        {
            BalanceStat stat = BalanceStat.createBackendTabletBalanceStat(1L, "disk1", "disk2",  9L, 1L);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.INTRA_NODE_TABLET_DISTRIBUTION, stat.getBalanceType());
            Assertions.assertEquals("intra-node tablet distribution", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxTabletNum\":9,\"minTabletNum\":1,\"beId\":1,\"maxPath\":\"disk1\",\"minPath\":\"disk2\"," +
                            "\"type\":\"INTRA_NODE_TABLET_DISTRIBUTION\",\"balanced\":false}",
                    stat.toString());
        }

        {
            Set<Long> currentBes = Sets.newHashSet(1L, 2L);
            Set<Long> expectedBes = Sets.newHashSet(2L, 3L);
            BalanceStat stat = BalanceStat.createColocationGroupBalanceStat(1L, currentBes, expectedBes);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.COLOCATION_GROUP, stat.getBalanceType());
            Assertions.assertEquals("colocation group", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"tabletId\":1,\"currentBes\":[1,2],\"expectedBes\":[2,3],\"type\":\"COLOCATION_GROUP\"," +
                            "\"balanced\":false}",
                    stat.toString());
        }

        {
            Set<Long> currentBes = Sets.newHashSet(1L, 2L);
            Map<String, Collection<String>> expectedLocations = Maps.newHashMap();
            expectedLocations.put("rack", Arrays.asList("rack1", "rack2"));
            BalanceStat stat = BalanceStat.createLabelLocationBalanceStat(1L, currentBes, expectedLocations);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.LABEL_AWARE_LOCATION, stat.getBalanceType());
            Assertions.assertEquals("label-aware location", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"tabletId\":1,\"currentBes\":[1,2],\"expectedLocations\":{\"rack\":[\"rack1\",\"rack2\"]}," +
                            "\"type\":\"LABEL_AWARE_LOCATION\",\"balanced\":false}",
                    stat.toString());
        }
    }
}
