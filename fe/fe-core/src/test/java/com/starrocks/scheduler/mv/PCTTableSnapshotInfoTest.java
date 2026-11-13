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

import com.google.common.collect.Range;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.MVPartitionCellBuilder;
import com.starrocks.mv.pct.BaseToMVPartitionMapping;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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

    @ParameterizedTest
    @CsvSource({"true, false", "true, true", "false, false", "false, true"})
    void testExternalPartitionDriftUsesSnapshotPin(boolean pinned, boolean foreignWindow) throws AnalysisException {
        Table snapshot = mock(Table.class);
        when(snapshot.getType()).thenReturn(Table.TableType.ICEBERG);
        Table live = mock(Table.class);
        when(live.getType()).thenReturn(Table.TableType.ICEBERG);
        MaterializedView mv = mock(MaterializedView.class);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.isRangePartition()).thenReturn(true);
        when(mv.getPartitionInfo()).thenReturn(partitionInfo);
        when(mv.getRefBaseTablePartitionColumns()).thenReturn(Map.of(snapshot, List.of(mock(Column.class))));
        when(mv.getRangePartitionFirstExpr()).thenReturn(Optional.of(mock(Expr.class)));
        MaterializedView.MvRefreshScheme scheme = new MaterializedView.MvRefreshScheme();
        when(mv.getRefreshScheme()).thenReturn(scheme);
        BaseTableInfo baseTableInfo = mock(BaseTableInfo.class);
        MaterializedView.AsyncRefreshContext shared = scheme.getAsyncRefreshContext();
        Map<BaseTableInfo, TvrVersionRange> foreign = foreignWindow
                ? Map.of(baseTableInfo, TvrTableSnapshot.of(99L)) : Map.of();
        shared.replaceTempBaseTableInfoTvrDeltaMap(foreignWindow ? "another-refresh" : null, foreign);
        shared.getBaseTableInfoTvrVersionRangeMap().put(baseTableInfo, TvrTableSnapshot.of(0L));
        Map<BaseTableInfo, TvrVersionRange> baseline = Map.copyOf(shared.getBaseTableInfoTvrVersionRangeMap());
        mockLiveTable(live);
        Map<Table, PCellSortedSet> cellsByTable = new HashMap<>(Map.of(snapshot, dayCells(1), live, dayCells(2)));
        new MockUp<MVPartitionCellBuilder>() {
            @Mock
            public BaseToMVPartitionMapping getPartitionKeyRange(Table table, Column column, Expr expression) {
                return BaseToMVPartitionMapping.of(cellsByTable.get(table));
            }
        };
        PCTTableSnapshotInfo info = new PCTTableSnapshotInfo(baseTableInfo, snapshot);
        if (pinned) {
            info.setPinnedRange(TvrTableSnapshot.of(42L));
        }

        Assertions.assertEquals(!pinned, info.hasBaseTableChanged(mv),
                "only this snapshot's pin may suppress live partition drift, regardless of another job's window");
        // Control against exceptions being swallowed and reported as drift.
        cellsByTable.put(live, dayCells(1));
        Assertions.assertFalse(info.hasBaseTableChanged(mv));
        Assertions.assertEquals(foreignWindow ? "another-refresh" : null, shared.getTempTvrOwnerStartTaskRunId());
        Assertions.assertEquals(foreign, shared.getTempBaseTableInfoTvrDeltaMap());
        Assertions.assertEquals(baseline, shared.getBaseTableInfoTvrVersionRangeMap());
    }

    private static PCellSortedSet dayCells(int days) throws AnalysisException {
        List<PCellWithName> cells = new ArrayList<>();
        for (int i = 0; i < days; i++) {
            LocalDate day = LocalDate.of(2026, 1, 1).plusDays(i);
            cells.add(PCellWithName.of("p" + i, new PRangeCell(
                    Range.closedOpen(PartitionKey.ofDate(day), PartitionKey.ofDate(day.plusDays(1))))));
        }
        return PCellSortedSet.of(cells);
    }

    private static void mockLiveTable(Table live) {
        new MockUp<MvUtils>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(BaseTableInfo baseTableInfo) {
                return Optional.of(live);
            }
        };
    }
}
