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

package com.starrocks.connector.partitiontraits;

import com.starrocks.catalog.MaterializedView.BasePartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OlapPartitionTraitsTest {
    private static final long PARTITION_ID = 1L;
    private static final BasePartitionInfo RECORDED = new BasePartitionInfo(PARTITION_ID, 5L, 1000L);

    private static OlapTable table(boolean cloudNative) {
        OlapTable table = mock(OlapTable.class);
        when(table.isCloudNativeTableOrMaterializedView()).thenReturn(cloudNative);
        return table;
    }

    private static PhysicalPartition physicalPartition(long version, long versionTime) {
        PhysicalPartition physicalPartition = mock(PhysicalPartition.class);
        when(physicalPartition.getVisibleVersion()).thenReturn(version);
        when(physicalPartition.getVisibleVersionTime()).thenReturn(versionTime);
        return physicalPartition;
    }

    private static Partition partition(long id, PhysicalPartition latest, PhysicalPartition... subPartitions) {
        Partition partition = mock(Partition.class);
        when(partition.getId()).thenReturn(id);
        when(partition.getLatestPhysicalPartition()).thenReturn(latest);
        when(partition.getSubPartitions()).thenReturn(List.of(subPartitions));
        return partition;
    }

    @Test
    public void testCloudNativeSinglePhysicalPartitionIgnoresVersionTime() {
        PhysicalPartition only = physicalPartition(5L, 2000L);
        Assertions.assertFalse(
                OlapPartitionTraits.isBaseTableChanged(table(true), partition(PARTITION_ID, only, only), RECORDED));
    }

    @Test
    public void testCloudNativeSinglePhysicalPartitionDetectsNewVersion() {
        PhysicalPartition only = physicalPartition(6L, 1000L);
        Assertions.assertTrue(
                OlapPartitionTraits.isBaseTableChanged(table(true), partition(PARTITION_ID, only, only), RECORDED));
    }

    @Test
    public void testRecreatedPartitionIsChanged() {
        PhysicalPartition only = physicalPartition(5L, 1000L);
        Assertions.assertTrue(
                OlapPartitionTraits.isBaseTableChanged(table(true), partition(PARTITION_ID + 1, only, only), RECORDED));
    }

    @Test
    public void testCloudNativeMultiplePhysicalPartitionsStillCompareVersionTime() {
        PhysicalPartition sealed = physicalPartition(5L, 1000L);
        PhysicalPartition added = physicalPartition(5L, 2000L);
        Assertions.assertTrue(OlapPartitionTraits.isBaseTableChanged(
                table(true), partition(PARTITION_ID, added, sealed, added), RECORDED));
    }

    @Test
    public void testSharedNothingPartitionStillComparesVersionTime() {
        PhysicalPartition only = physicalPartition(5L, 2000L);
        Assertions.assertTrue(
                OlapPartitionTraits.isBaseTableChanged(table(false), partition(PARTITION_ID, only, only), RECORDED));
    }
}
