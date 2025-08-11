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

import com.starrocks.clone.BalanceStat.BalanceType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
            Assertions.assertEquals(BalanceType.CLUSTER_DISK, stat.getBalanceType());
            Assertions.assertEquals("inter-node disk usage", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxUsedPercent\":0.9,\"minUsedPercent\":0.1,\"maxBeId\":1,\"minBeId\":2,\"type\":\"CLUSTER_DISK\"," +
                            "\"balanced\":false}",
                    stat.toString());
        }

        {
            BalanceStat stat = BalanceStat.createClusterTabletBalanceStat(1L, 2L, 9L, 1L);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.CLUSTER_TABLET, stat.getBalanceType());
            Assertions.assertEquals("inter-node tablet distribution", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxTabletNum\":9,\"minTabletNum\":1,\"maxBeId\":1,\"minBeId\":2,\"type\":\"CLUSTER_TABLET\"," +
                            "\"balanced\":false}",
                    stat.toString());
        }

        {
            BalanceStat stat = BalanceStat.createBackendDiskBalanceStat(1L, "disk1", "disk2", 0.9, 0.1);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.BACKEND_DISK, stat.getBalanceType());
            Assertions.assertEquals("intra-node disk usage", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxUsedPercent\":0.9,\"minUsedPercent\":0.1,\"beId\":1,\"maxPath\":\"disk1\",\"minPath\":\"disk2\"," +
                            "\"type\":\"BACKEND_DISK\",\"balanced\":false}",
                    stat.toString());
        }

        {
            BalanceStat stat = BalanceStat.createBackendTabletBalanceStat(1L, "disk1", "disk2",  9L, 1L);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.BACKEND_TABLET, stat.getBalanceType());
            Assertions.assertEquals("intra-node tablet distribution", stat.getBalanceType().label());
            Assertions.assertEquals(
                    "{\"maxTabletNum\":9,\"minTabletNum\":1,\"beId\":1,\"maxPath\":\"disk1\",\"minPath\":\"disk2\"," +
                            "\"type\":\"BACKEND_TABLET\",\"balanced\":false}",
                    stat.toString());
        }
    }
}
