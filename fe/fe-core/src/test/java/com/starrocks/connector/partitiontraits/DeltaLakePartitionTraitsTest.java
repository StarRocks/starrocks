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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.LightWeightDeltaLakeTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeltaLakePartitionTraitsTest {
    private static final String CATALOG = "delta_catalog";
    private static final String DB = "delta_db";
    private static final String TBL = "tbl";
    private static final String TABLE_ID = "6f3a2c1e-0000-4000-8000-000000000001";
    private static final long COMMIT_TIME = 1_700_000_000_000L;

    private static DeltaLakeTable table(String deltaTableId, List<String> partitionColumnNames, long version) {
        Metadata metadata = Mockito.mock(Metadata.class);
        Mockito.when(metadata.getId()).thenReturn(deltaTableId);
        SnapshotImpl snapshot = Mockito.mock(SnapshotImpl.class);
        Mockito.when(snapshot.getMetadata()).thenReturn(metadata);
        Mockito.when(snapshot.getVersion()).thenReturn(version);
        Mockito.when(snapshot.getTimestamp(Mockito.any())).thenReturn(COMMIT_TIME);
        return new DeltaLakeTable(1L, CATALOG, DB, TBL,
                List.of(new Column("col1", IntegerType.INT), new Column("p", StringType.STRING)),
                partitionColumnNames, snapshot, Mockito.mock(Engine.class), new MetastoreTable(DB, TBL, "", 0L));
    }

    private static DeltaLakeTable table(long version) {
        return table(TABLE_ID, List.of(), version);
    }

    private static DeltaLakeTable partitionedTable(long version) {
        return table(TABLE_ID, List.of("p"), version);
    }

    private static BaseTableInfo baseTableInfo(DeltaLakeTable table) {
        return new BaseTableInfo(CATALOG, DB, TBL, table.getTableIdentifier());
    }

    private static MaterializedView.AsyncRefreshContext refreshedFrom(DeltaLakeTable table, String... partitionNames) {
        MaterializedView.AsyncRefreshContext context = new MaterializedView.AsyncRefreshContext();
        Map<String, PartitionInfo> infos =
                ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo(List.of(partitionNames));
        Map<String, MaterializedView.BasePartitionInfo> versionMap = context.getBaseTableRefreshInfo(baseTableInfo(table));
        infos.forEach((name, info) -> versionMap.put(name, MaterializedView.BasePartitionInfo.fromExternalTable(info)));
        return context;
    }

    private static Set<String> updatedPartitionNames(DeltaLakeTable table, BaseTableInfo baseTableInfo,
                                                     MaterializedView.AsyncRefreshContext context) {
        return ConnectorPartitionTraits.build(table).getUpdatedPartitionNames(List.of(baseTableInfo), context);
    }

    @Test
    public void mvRefreshedFromTheCurrentVersionIsFresh() {
        DeltaLakeTable table = table(7);
        assertEquals(Set.of(), updatedPartitionNames(table, baseTableInfo(table), refreshedFrom(table, TBL)));
    }

    @Test
    public void mvRefreshedFromAnOlderVersionIsStale() {
        DeltaLakeTable current = table(8);
        assertEquals(Set.of(TBL), updatedPartitionNames(current, baseTableInfo(current), refreshedFrom(table(7), TBL)));
    }

    @Test
    public void mvWithoutARecordedVersionIsStale() {
        DeltaLakeTable table = table(7);
        assertEquals(Set.of(TBL), updatedPartitionNames(table, baseTableInfo(table),
                new MaterializedView.AsyncRefreshContext()));
    }

    @Test
    public void checkDoesNotWriteTheRefreshState() {
        DeltaLakeTable table = table(7);
        MaterializedView.AsyncRefreshContext context = new MaterializedView.AsyncRefreshContext();
        updatedPartitionNames(table, baseTableInfo(table), context);
        assertTrue(context.getBaseTableInfoVisibleVersionMap().isEmpty());
    }

    @Test
    public void recreatedTableIsStale() {
        DeltaLakeTable dropped = table(7);
        DeltaLakeTable recreated = table("6f3a2c1e-0000-4000-8000-000000000002", List.of(), 7);
        assertEquals(Set.of(TBL), updatedPartitionNames(recreated, baseTableInfo(dropped), refreshedFrom(dropped, TBL)));
    }

    @Test
    public void partitionedTableIsNotTrackedAndAlwaysStale() {
        DeltaLakeTable table = partitionedTable(7);
        assertFalse(DeltaLakePartitionTraits.isTableVersionTracked(table));
        assertEquals(Set.of(TBL), updatedPartitionNames(table, baseTableInfo(table),
                new MaterializedView.AsyncRefreshContext()));
    }

    @Test
    public void tableWithoutASnapshotIsNotTrackedAndAlwaysStale() {
        DeltaLakeTable table = table(7);
        table.clearMetadata();
        assertFalse(DeltaLakePartitionTraits.isTableVersionTracked(table));
        assertEquals(Set.of(TBL), updatedPartitionNames(table, new BaseTableInfo(CATALOG, DB, TBL, TBL + ":" + TABLE_ID),
                new MaterializedView.AsyncRefreshContext()));
    }

    @Test
    public void lightWeightTableIsNotTrackedAndAlwaysStale() {
        DeltaLakeTable table = table(7);
        DeltaLakeTable lightWeight = new LightWeightDeltaLakeTable(table);
        assertFalse(DeltaLakePartitionTraits.isTableVersionTracked(lightWeight));
        assertEquals(Set.of(TBL), updatedPartitionNames(lightWeight, baseTableInfo(table), refreshedFrom(table, TBL)));
    }

    @Test
    public void tableIsItsOnlyPartitionCarryingTheVersion() {
        DeltaLakeTable table = table(7);
        assertTrue(DeltaLakePartitionTraits.isTableVersionTracked(table));
        Map<String, PartitionInfo> infos = ConnectorPartitionTraits.build(table).getPartitionNameWithPartitionInfo();
        assertEquals(Set.of(TBL), infos.keySet());
        assertEquals(7, infos.get(TBL).getVersion());
    }

    @Test
    public void statisticsSeeNoUpdateTime() {
        ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(table(7));
        assertNull(traits.getUpdatedPartitionNames(LocalDateTime.MIN, 60));
        assertNull(traits.getTableLastUpdateTime(60));
    }

    @Test
    public void refreshRecordsTheCommitTime() {
        MaterializedView.BasePartitionInfo refreshed = MaterializedView.BasePartitionInfo.fromExternalTable(
                ConnectorPartitionTraits.build(table(7)).getPartitions(List.of(TBL)).get(0));
        assertEquals(7, refreshed.getVersion());
        assertEquals(COMMIT_TIME, refreshed.getLastRefreshTime());
    }
}
