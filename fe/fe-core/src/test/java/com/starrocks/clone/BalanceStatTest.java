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
        Assertions.assertTrue(BalanceStat.BALANCED_STAT.isBalanced());

        {
            BalanceStat stat = BalanceStat.createClusterDiskBalanceStat(1L, 2L, 0.9, 0.1);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.CLUSTER_DISK, stat.getBalanceType());
            Assertions.assertEquals("cluster disk", stat.getBalanceType().label());
        }

        {
            BalanceStat stat = BalanceStat.createClusterTabletBalanceStat(1L, 2L, 9L, 1L);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.CLUSTER_TABLET, stat.getBalanceType());
            Assertions.assertEquals("cluster tablet", stat.getBalanceType().label());
        }

        {
            BalanceStat stat = BalanceStat.createBackendDiskBalanceStat(1L, "disk1", "disk2", 0.9, 0.1);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.BACKEND_DISK, stat.getBalanceType());
            Assertions.assertEquals("backend disk", stat.getBalanceType().label());
        }

        {
            BalanceStat stat = BalanceStat.createBackendTabletBalanceStat(1L, "disk1", "disk2",  9L, 1L);
            Assertions.assertFalse(stat.isBalanced());
            Assertions.assertEquals(BalanceType.BACKEND_TABLET, stat.getBalanceType());
            Assertions.assertEquals("backend tablet", stat.getBalanceType().label());
        }
    }
}
