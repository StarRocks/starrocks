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

package com.starrocks.connector.hive;

import org.junit.Assert;
import org.junit.Test;

public class HivePartitionStatsTest {
    @Test
    public void testFromCommonStats() {
        long rowNums = 5;
        long fileSize = 100;
        HivePartitionStats hivePartitionStats = HivePartitionStats.fromCommonStats(rowNums, fileSize);
        Assert.assertEquals(5, hivePartitionStats.getCommonStats().getRowNums());
        Assert.assertEquals(100, hivePartitionStats.getCommonStats().getTotalFileBytes());
        Assert.assertTrue(hivePartitionStats.getColumnStats().isEmpty());
    }

    @Test
    public void testMerge() {
        HivePartitionStats current = HivePartitionStats.empty();
        HivePartitionStats update = HivePartitionStats.empty();
        Assert.assertEquals(current, HivePartitionStats.merge(current, update));

        current = HivePartitionStats.fromCommonStats(5, 100);
        update = HivePartitionStats.empty();
        Assert.assertEquals(current, HivePartitionStats.merge(current, update));

        current = HivePartitionStats.fromCommonStats(0, 0);
        update = HivePartitionStats.fromCommonStats(5, 100);
        Assert.assertEquals(update, HivePartitionStats.merge(current, update));

        current = HivePartitionStats.fromCommonStats(5, 100);
        Assert.assertEquals(10, HivePartitionStats.merge(current, update).getCommonStats().getRowNums());
        Assert.assertEquals(200, HivePartitionStats.merge(current, update).getCommonStats().getTotalFileBytes());
    }

    @Test
    public void testReduce() {
        Assert.assertEquals(10, HivePartitionStats.reduce(5, 5, HivePartitionStats.ReduceOperator.ADD));
        Assert.assertEquals(0, HivePartitionStats.reduce(5, 5, HivePartitionStats.ReduceOperator.SUBTRACT));
        Assert.assertEquals(5, HivePartitionStats.reduce(5, 6, HivePartitionStats.ReduceOperator.MIN));
        Assert.assertEquals(6, HivePartitionStats.reduce(5, 6, HivePartitionStats.ReduceOperator.MAX));
    }
}
