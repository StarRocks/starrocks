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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.type.IntegerType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.view.RefreshState;
import org.apache.iceberg.view.RefreshStateParser;
import org.apache.iceberg.view.SourceState;
import org.apache.iceberg.view.SourceTableState;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class IcebergMVStorageTest {

    @Test
    public void testStorageAndViewNames() {
        assertEquals("mv1__storage", IcebergMVStorage.storageTableName("mv1"));
        assertEquals("mv1", IcebergMVStorage.storageViewName("mv1"));
    }

    @Test
    public void testSerializeRefreshStateRoundTrip() {
        SourceTableState src = new SourceTableState(
                "orders", List.of("sales"), "ice_cat", "uuid-123", 42L, null);
        RefreshState state = new RefreshState(7, List.of(src), 1000L);

        String json = IcebergMVStorage.serializeRefreshState(state);
        RefreshState parsed = RefreshStateParser.fromJson(json);

        assertEquals(7, parsed.viewVersionId());
        assertEquals(1000L, parsed.refreshStartTimestampMs());
        assertEquals(1, parsed.sourceStates().size());
        SourceState parsedSource = parsed.sourceStates().get(0);
        assertInstanceOf(SourceTableState.class, parsedSource);
        SourceTableState parsedTable = (SourceTableState) parsedSource;
        assertEquals("orders", parsedTable.name());
        assertEquals(List.of("sales"), parsedTable.namespace());
        assertEquals("ice_cat", parsedTable.catalog());
        assertEquals("uuid-123", parsedTable.uuid());
        assertEquals(42L, parsedTable.snapshotId());
    }

    private static IcebergTable mockIcebergTable(String cat, String db, String name, String uuid, Long snapshotId) {
        IcebergTable t = mock(IcebergTable.class);
        when(t.getCatalogName()).thenReturn(cat);
        when(t.getCatalogDBName()).thenReturn(db);
        when(t.getCatalogTableName()).thenReturn(name);
        when(t.getUUID()).thenReturn(uuid);
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        if (snapshotId == null) {
            when(nativeTable.currentSnapshot()).thenReturn(null);
        } else {
            Snapshot snapshot = mock(Snapshot.class);
            when(snapshot.snapshotId()).thenReturn(snapshotId);
            when(nativeTable.currentSnapshot()).thenReturn(snapshot);
        }
        when(t.getNativeTable()).thenReturn(nativeTable);
        return t;
    }

    @Test
    public void testBuildRefreshStateFromIcebergBasesOnly() {
        BaseTableInfo icebergBase = mock(BaseTableInfo.class);
        BaseTableInfo olapBase = mock(BaseTableInfo.class);
        IcebergTable icebergTable = mockIcebergTable("ice_cat", "sales", "orders", "uuid-123", 42L);
        OlapTable olapTable = mock(OlapTable.class);

        try (MockedStatic<MvUtils> mvUtils = mockStatic(MvUtils.class)) {
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(icebergBase))
                    .thenReturn(Optional.of((Table) icebergTable));
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(olapBase))
                    .thenReturn(Optional.of((Table) olapTable));

            RefreshState state = IcebergMVStorage.buildRefreshState(List.of(icebergBase, olapBase), 9);

            assertEquals(9, state.viewVersionId());
            // Only the Iceberg base contributes a source state; the OLAP base is skipped.
            assertEquals(1, state.sourceStates().size());
            SourceTableState src = (SourceTableState) state.sourceStates().get(0);
            assertEquals("orders", src.name());
            assertEquals(List.of("sales"), src.namespace());
            assertEquals("ice_cat", src.catalog());
            assertEquals("uuid-123", src.uuid());
            assertEquals(42L, src.snapshotId());
        }
    }

    @Test
    public void testBuildRefreshStateHandlesMissingSnapshot() {
        BaseTableInfo icebergBase = mock(BaseTableInfo.class);
        IcebergTable icebergTable = mockIcebergTable("ice_cat", "sales", "empty_tbl", "uuid-999", null);

        try (MockedStatic<MvUtils> mvUtils = mockStatic(MvUtils.class)) {
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(icebergBase))
                    .thenReturn(Optional.of((Table) icebergTable));

            RefreshState state = IcebergMVStorage.buildRefreshState(List.of(icebergBase), 1);
            assertEquals(1, state.sourceStates().size());
            // No current snapshot -> sentinel -1 snapshot id (empty source).
            assertEquals(-1L, ((SourceTableState) state.sourceStates().get(0)).snapshotId());
        }
    }

    @Test
    public void testBuildRefreshStateSkipsUnresolvedBase() {
        BaseTableInfo missingBase = mock(BaseTableInfo.class);
        try (MockedStatic<MvUtils> mvUtils = mockStatic(MvUtils.class)) {
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(missingBase)).thenReturn(Optional.empty());

            RefreshState state = IcebergMVStorage.buildRefreshState(List.of(missingBase), 3);
            assertTrue(state.sourceStates().isEmpty());
        }
    }

    @Test
    public void testValidateAllIcebergBaseTablesPasses() {
        BaseTableInfo b1 = mock(BaseTableInfo.class);
        BaseTableInfo b2 = mock(BaseTableInfo.class);
        IcebergTable t1 = mock(IcebergTable.class);
        IcebergTable t2 = mock(IcebergTable.class);
        try (MockedStatic<MvUtils> mvUtils = mockStatic(MvUtils.class)) {
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(b1)).thenReturn(Optional.of((Table) t1));
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(b2)).thenReturn(Optional.of((Table) t2));
            assertDoesNotThrow(() -> IcebergMVStorage.validateAllIcebergBaseTables(List.of(b1, b2)));
        }
    }

    @Test
    public void testValidateAllIcebergBaseTablesRejectsNonIceberg() {
        BaseTableInfo icebergBase = mock(BaseTableInfo.class);
        BaseTableInfo olapBase = mock(BaseTableInfo.class);
        when(olapBase.getTableName()).thenReturn("t_olap");
        try (MockedStatic<MvUtils> mvUtils = mockStatic(MvUtils.class)) {
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(icebergBase))
                    .thenReturn(Optional.of((Table) mock(IcebergTable.class)));
            mvUtils.when(() -> MvUtils.getTableWithIdentifier(olapBase))
                    .thenReturn(Optional.of((Table) mock(OlapTable.class)));
            SemanticException e = assertThrows(SemanticException.class,
                    () -> IcebergMVStorage.validateAllIcebergBaseTables(List.of(icebergBase, olapBase)));
            assertTrue(e.getMessage().contains("t_olap"));
        }
    }

    @Test
    public void testValidateIcebergSchemaPasses() {
        List<Column> columns = List.of(new Column("id", IntegerType.INT));
        assertDoesNotThrow(() -> IcebergMVStorage.validateIcebergSchema(columns));
    }
}
