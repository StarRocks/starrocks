// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.lake;

import com.google.common.collect.Range;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RecyclePartitionInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RecycleLakeDeletedPartitionInfo}: a non-recoverable lake partition must be
 * compacted into a flat descriptor while preserving the identity/ids the delete path needs, and a
 * recoverable one must keep its full graph.
 */
public class RecycleLakeDeletedPartitionInfoTest {

    private static Partition buildPartition(long partitionId, long physicalPartitionId, long[] tabletIds,
                                            long shardGroupId, String name) {
        HashDistributionInfo distributionInfo = new HashDistributionInfo();
        Partition partition = new Partition(partitionId, name, distributionInfo);
        MaterializedIndex baseIndex = new MaterializedIndex(1000L, MaterializedIndex.IndexState.NORMAL, shardGroupId);
        for (long tabletId : tabletIds) {
            LakeTablet tablet = new LakeTablet(tabletId);
            baseIndex.addTablet(tablet, null, false);
        }
        PhysicalPartition physicalPartition = new PhysicalPartition(physicalPartitionId, partitionId, baseIndex);
        physicalPartition.setShardGroupId(shardGroupId);
        partition.addSubPartition(physicalPartition);
        return partition;
    }

    private static RecycleLakeRangePartitionInfo buildLakeInfo(Partition partition, boolean recoverable)
            throws com.starrocks.common.AnalysisException {
        Range<PartitionKey> range = Range.closedOpen(PartitionKey.ofDate(java.time.LocalDate.of(2020, 1, 1)),
                PartitionKey.ofDate(java.time.LocalDate.of(2020, 1, 2)));
        RecycleLakeRangePartitionInfo info = new RecycleLakeRangePartitionInfo(
                10L, 20L, partition, range, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.setRecoverable(recoverable);
        return info;
    }

    @Test
    public void testNonRecoverableLakePartitionIsCompacted() throws Exception {
        Partition partition = buildPartition(100L, 200L, new long[] {1L, 2L, 3L}, 5000L, "p1");
        RecyclePartitionInfo info = buildLakeInfo(partition, false);

        RecyclePartitionInfo compacted = RecycleLakeDeletedPartitionInfo.compactIfPossible(info);

        Assertions.assertInstanceOf(RecycleLakeDeletedPartitionInfo.class, compacted);
        RecycleLakeDeletedPartitionInfo descriptor = (RecycleLakeDeletedPartitionInfo) compacted;
        // Identity retained without the Partition graph.
        Assertions.assertNull(descriptor.getPartition());
        Assertions.assertEquals(100L, descriptor.getPartitionId());
        Assertions.assertEquals("p1", descriptor.getPartitionName());
        Assertions.assertEquals(10L, descriptor.getDbId());
        Assertions.assertEquals(20L, descriptor.getTableId());
        Assertions.assertFalse(descriptor.isRecoverable());
        // Flattened ids retained for the delete path.
        Assertions.assertEquals(java.util.List.of(1L, 2L, 3L), descriptor.collectTabletIds());
        Assertions.assertEquals(java.util.Set.of(5000L), descriptor.collectShardGroupIds());
        Assertions.assertEquals(1, descriptor.getPhysicalPartitionShardIds().size());
        Assertions.assertEquals(200L, descriptor.getPhysicalPartitionShardIds().get(0)[0]);
        Assertions.assertEquals(1, descriptor.getIndexTabletIds().size());
        Assertions.assertEquals(200L, descriptor.getIndexTabletIds().get(0)[0]);
        Assertions.assertEquals(1000L, descriptor.getIndexTabletIds().get(0)[1]);
    }

    @Test
    public void testRecoverableLakePartitionIsUnchanged() throws Exception {
        Partition partition = buildPartition(101L, 201L, new long[] {9L}, 6000L, "p2");
        RecyclePartitionInfo info = buildLakeInfo(partition, true);

        RecyclePartitionInfo result = RecycleLakeDeletedPartitionInfo.compactIfPossible(info);

        Assertions.assertSame(info, result);
        Assertions.assertNotNull(result.getPartition());
    }

    @Test
    public void testNonLakePartitionIsUnchanged() throws Exception {
        // A shared-nothing RecycleRangePartitionInfo is not a lake descriptor and must be left alone.
        Partition partition = buildPartition(102L, 202L, new long[] {7L}, 7000L, "p3");
        Range<PartitionKey> range = Range.closedOpen(PartitionKey.ofDate(java.time.LocalDate.of(2020, 1, 1)),
                PartitionKey.ofDate(java.time.LocalDate.of(2020, 1, 2)));
        com.starrocks.catalog.RecycleRangePartitionInfo info = new com.starrocks.catalog.RecycleRangePartitionInfo(
                10L, 20L, partition, range, DataProperty.DEFAULT_DATA_PROPERTY, (short) 1, null);
        info.setRecoverable(false);

        RecyclePartitionInfo result = RecycleLakeDeletedPartitionInfo.compactIfPossible(info);

        Assertions.assertSame(info, result);
    }

    @Test
    public void testEmptyPartitionCompactsWithNoShardId() throws Exception {
        Partition partition = buildPartition(103L, 203L, new long[] {}, 8000L, "p4");
        RecyclePartitionInfo info = buildLakeInfo(partition, false);

        RecycleLakeDeletedPartitionInfo descriptor =
                (RecycleLakeDeletedPartitionInfo) RecycleLakeDeletedPartitionInfo.compactIfPossible(info);

        Assertions.assertTrue(descriptor.collectTabletIds().isEmpty());
        // Empty sub-partition records shardId -1 so the delete path skips it cleanly.
        Assertions.assertEquals(-1L, descriptor.getPhysicalPartitionShardIds().get(0)[1]);
    }
}