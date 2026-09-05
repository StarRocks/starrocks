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
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    private static PCellSortedSet listCells(String... values) {
        return PCellSortedSet.of(Arrays.stream(values)
                .map(v -> PCellWithName.of("p" + v, new PListCell(v)))
                .toList());
    }

    private static OlapTable listPartitionedTable(PCellSortedSet cells) {
        OlapTable table = mock(OlapTable.class);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isUnPartitioned()).thenReturn(false);
        when(partitionInfo.isListPartition()).thenReturn(true);
        when(table.isOlapOrCloudNativeTable()).thenReturn(true);
        when(table.getPartitionInfo()).thenReturn(partitionInfo);
        when(table.getListPartitionItems()).thenReturn(cells);
        return table;
    }

    /**
     * The drift check must compare the snapshot copy against the live table. Comparing the snapshot
     * with itself always reports "unchanged", which silently disables the retry loop that guards MV
     * refresh against base-table partitions changing mid-refresh.
     */
    @Test
    void testListPartitionDriftIsDetectedAgainstLiveTable() {
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        MaterializedView mv = mock(MaterializedView.class);
        OlapTable snapshot = listPartitionedTable(listCells("2026-01-01", "2026-01-02"));

        OlapTable live = listPartitionedTable(listCells("2026-01-01", "2026-01-02", "2026-01-03"));
        mockLiveTable(live);
        Assertions.assertTrue(new PCTTableSnapshotInfo(baseTableInfo, snapshot).hasBaseTableChanged(mv),
                "a partition added to the live table after the snapshot must be reported as changed");

        // Control: an unchanged live table must still report false, so the assertion above cannot pass
        // merely because hasBaseTableChanged swallows an exception and defaults to true.
        mockLiveTable(listPartitionedTable(listCells("2026-01-01", "2026-01-02")));
        Assertions.assertFalse(new PCTTableSnapshotInfo(baseTableInfo, snapshot).hasBaseTableChanged(mv),
                "an unchanged live table must not be reported as changed");
    }

    private static void mockLiveTable(OlapTable live) {
        new MockUp<MvUtils>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(BaseTableInfo baseTableInfo) {
                return Optional.of(live);
            }
        };
    }
}