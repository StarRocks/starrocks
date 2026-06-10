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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class PCTTableSnapshotInfoTest {

    @Test
    void testUpdatePartitionInfos() {
        // Mock olap table
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        OlapTable baseTable = mock(OlapTable.class);
        when(baseTable.isNativeTableOrMaterializedView()).thenReturn(true);
        // Mock partition without sub-partitions
        Partition partition = mock(Partition.class);
        PhysicalPartition mockDefault = mock(PhysicalPartition.class);
        when(partition.getLatestPhysicalPartition()).thenReturn(mockDefault);
        when(partition.getSubPartitions()).thenReturn(Collections.emptyList());
        when(partition.getName()).thenReturn("p1");
        when(partition.getId()).thenReturn(1L);
        when(mockDefault.getVisibleVersion()).thenReturn(10L);
        when(mockDefault.getVisibleVersionTime()).thenReturn(1000L);

        when(baseTable.getPartition("p1")).thenReturn(partition);

        PCTTableSnapshotInfo pctTableSnapshotInfo = new PCTTableSnapshotInfo(baseTableInfo, baseTable);
        pctTableSnapshotInfo.updatePartitionInfos(List.of("p1"));
        Map<String, MaterializedView.BasePartitionInfo> refreshedPartitionInfos =
                pctTableSnapshotInfo.getRefreshedPartitionInfos();
        MaterializedView.BasePartitionInfo mvPartitionInfo = refreshedPartitionInfos.get("p1");

        Assertions.assertEquals(1L, mvPartitionInfo.getId());
        Assertions.assertEquals(10L, mvPartitionInfo.getVersion());
        Assertions.assertEquals(1000L, mvPartitionInfo.getLastRefreshTime());
    }

    @Test
    void testUpdatePartitionInfosWithSubPartitions() {
        // Mock olap table
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        OlapTable baseTable = mock(OlapTable.class);
        when(baseTable.isNativeTableOrMaterializedView()).thenReturn(true);

        Partition partition = spy(new Partition(1L, "p1", null));
        // Mock the default physical partition and two sub-partitions
        PhysicalPartition mockDefault = mock(PhysicalPartition.class);
        PhysicalPartition sub1 = mock(PhysicalPartition.class);
        PhysicalPartition sub2 = mock(PhysicalPartition.class);
        when(sub1.getVisibleVersionTime()).thenReturn(2000L);
        when(sub1.getVisibleVersion()).thenReturn(20L);
        when(sub2.getVisibleVersionTime()).thenReturn(3000L);
        when(sub2.getVisibleVersion()).thenReturn(30L);

        List<PhysicalPartition> subs = Arrays.asList(sub1, sub2);
        when(partition.getDefaultPhysicalPartition()).thenReturn(mockDefault);
        when(partition.getSubPartitions()).thenReturn(subs);
        when(partition.getName()).thenReturn("p1");
        when(partition.getId()).thenReturn(1L);

        when(baseTable.getPartition("p1")).thenReturn(partition);

        PCTTableSnapshotInfo pctTableSnapshotInfo = new PCTTableSnapshotInfo(baseTableInfo, baseTable);
        pctTableSnapshotInfo.updatePartitionInfos(List.of("p1"));
        Map<String, MaterializedView.BasePartitionInfo> refreshedPartitionInfos =
                pctTableSnapshotInfo.getRefreshedPartitionInfos();
        MaterializedView.BasePartitionInfo mvPartitionInfo = refreshedPartitionInfos.get("p1");

        Assertions.assertEquals(1L, mvPartitionInfo.getId());
        Assertions.assertEquals(30L, mvPartitionInfo.getVersion());
        Assertions.assertEquals(3000L, mvPartitionInfo.getLastRefreshTime());
    }
}