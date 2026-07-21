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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PhysicalPartitionLastAccessTimeTest {

    @Test
    public void testUpdateIsMonotonic() {
        MaterializedIndex index = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        PhysicalPartition pp = new PhysicalPartition(10L, 1L, index);
        Assertions.assertEquals(0L, pp.getLastAccessTime());

        pp.updateLastAccessTime(1000L);
        Assertions.assertEquals(1000L, pp.getLastAccessTime());

        // Older time must NOT overwrite.
        pp.updateLastAccessTime(500L);
        Assertions.assertEquals(1000L, pp.getLastAccessTime());

        // Newer time updates.
        pp.updateLastAccessTime(2000L);
        Assertions.assertEquals(2000L, pp.getLastAccessTime());
    }

    @Test
    public void testPartitionAggregatesMaxOverSubPartitions() {
        MaterializedIndex index = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(100L, 101L, "p1", index, null);
        Assertions.assertEquals(0L, partition.getLastAccessTime());

        for (PhysicalPartition pp : partition.getSubPartitions()) {
            pp.updateLastAccessTime(5000L);
        }
        Assertions.assertEquals(5000L, partition.getLastAccessTime());
    }

    @Test
    public void testUpdateTimeIsMonotonicAndAggregates() {
        MaterializedIndex index = new MaterializedIndex(1L, MaterializedIndex.IndexState.NORMAL);
        PhysicalPartition pp = new PhysicalPartition(10L, 1L, index);
        Assertions.assertEquals(0L, pp.getLastUpdateTime());

        pp.updateLastUpdateTime(1000L);
        pp.updateLastUpdateTime(500L);   // older must NOT overwrite
        Assertions.assertEquals(1000L, pp.getLastUpdateTime());
        pp.updateLastUpdateTime(2000L);
        Assertions.assertEquals(2000L, pp.getLastUpdateTime());

        Partition partition = new Partition(100L, 101L, "p1", index, null);
        for (PhysicalPartition sub : partition.getSubPartitions()) {
            sub.updateLastUpdateTime(7000L);
        }
        Assertions.assertEquals(7000L, partition.getLastUpdateTime());
    }
}
