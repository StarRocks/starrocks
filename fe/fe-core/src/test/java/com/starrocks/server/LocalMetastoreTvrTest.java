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

package com.starrocks.server;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.KeysType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalMetastoreTvrTest {

    private LocalMetastore store;

    @BeforeEach
    public void setUp() {
        store = mock(LocalMetastore.class, CALLS_REAL_METHODS);
    }

    private OlapTable mockOlapTable(KeysType keysType, long... versions) {
        OlapTable table = mock(OlapTable.class);
        List<PhysicalPartition> partitions = new ArrayList<>();
        for (long v : versions) {
            PhysicalPartition p = mock(PhysicalPartition.class);
            when(p.getVisibleVersion()).thenReturn(v);
            partitions.add(p);
        }
        when(table.getAllPhysicalPartitions()).thenReturn(partitions);
        when(table.getKeysType()).thenReturn(keysType);
        return table;
    }

    // ---- getCurrentTvrSnapshot ----

    @Test
    public void testSnapshot_dupKeys_returnsSumVersion() {
        OlapTable table = mockOlapTable(KeysType.DUP_KEYS, 3L, 5L, 2L);
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", table);
        assertFalse(snap.isEmpty());
        assertEquals(10L, snap.getSnapshotId()); // 3 + 5 + 2
    }

    @Test
    public void testSnapshot_noPartitions_returnsEmpty() {
        OlapTable table = mockOlapTable(KeysType.DUP_KEYS);
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", table);
        assertTrue(snap.isEmpty());
    }

    @Test
    public void testSnapshot_nonOlapTable_returnsEmpty() {
        Table table = mock(Table.class);
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", table);
        assertTrue(snap.isEmpty());
    }

    @Test
    public void testSnapshot_primaryKeys_returnsSumVersion() {
        OlapTable table = mockOlapTable(KeysType.PRIMARY_KEYS, 10L, 8L);
        TvrTableSnapshot snap = store.getCurrentTvrSnapshot("db", table);
        assertFalse(snap.isEmpty());
        assertEquals(18L, snap.getSnapshotId()); // 10 + 8
    }

    // ---- listTableDeltaTraits ----

    @Test
    public void testDeltaTraits_dupKeys_retractable() {
        OlapTable table = mockOlapTable(KeysType.DUP_KEYS, 5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly()); // DUP_KEYS supports DELETE; conservative = retractable
    }

    @Test
    public void testDeltaTraits_primaryKeys_retractable() {
        OlapTable table = mockOlapTable(KeysType.PRIMARY_KEYS, 5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }

    @Test
    public void testDeltaTraits_uniqueKeys_retractable() {
        OlapTable table = mockOlapTable(KeysType.UNIQUE_KEYS, 5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }

    @Test
    public void testDeltaTraits_aggKeys_retractable() {
        OlapTable table = mockOlapTable(KeysType.AGG_KEYS, 5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertEquals(1, traits.size());
        assertFalse(traits.get(0).isAppendOnly());
    }

    @Test
    public void testDeltaTraits_fromEqualsTo_returnsEmpty() {
        OlapTable table = mockOlapTable(KeysType.DUP_KEYS, 5L);
        TvrTableSnapshot snap = TvrTableSnapshot.of(5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits("db", table, snap, snap);
        assertTrue(traits.isEmpty());
    }

    @Test
    public void testDeltaTraits_fromGreaterThanTo_throwsAncestryError() {
        OlapTable table = mockOlapTable(KeysType.DUP_KEYS, 3L);
        TvrTableSnapshot from = TvrTableSnapshot.of(7L);
        TvrTableSnapshot to = TvrTableSnapshot.of(3L);
        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> store.listTableDeltaTraits("db", table, from, to));
        assertTrue(ex.getMessage().contains("is not a parent ancestor"));
    }

    @Test
    public void testDeltaTraits_nonOlapTable_returnsEmpty() {
        Table table = mock(Table.class);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertTrue(traits.isEmpty());
    }

    @Test
    public void testDeltaTraits_emptyFrom_deltaStartsFromMin() {
        OlapTable table = mockOlapTable(KeysType.DUP_KEYS, 5L);
        List<TvrTableDeltaTrait> traits = store.listTableDeltaTraits(
                "db", table, TvrTableSnapshot.empty(), TvrTableSnapshot.of(5L));
        assertEquals(1, traits.size());
        assertTrue(traits.get(0).getTvrDelta().start().isEmpty());
        assertEquals(Optional.of(5L), traits.get(0).getTvrDelta().end());
    }
}
