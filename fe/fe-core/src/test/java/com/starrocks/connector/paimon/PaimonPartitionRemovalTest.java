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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaimonPartitionRemovalTest {
    private Catalog catalog;
    private Table nativeTable;
    private PaimonMetadata metadata;

    @BeforeEach
    public void setUp() throws Exception {
        catalog = mock(Catalog.class);
        nativeTable = mock(Table.class);
        when(nativeTable.partitionKeys()).thenReturn(List.of("dt"));
        when(nativeTable.rowType()).thenReturn(RowType.of(new org.apache.paimon.types.DataType[] {DataTypes.STRING()},
                new String[] {"dt"}));
        when(nativeTable.options()).thenReturn(Map.of());
        metadata = new PaimonMetadata("paimon", null, catalog, null);
    }

    private PaimonTable table(String db, String name) throws Exception {
        when(catalog.getTable(Identifier.create(db, name))).thenReturn(nativeTable);
        return new PaimonTable("paimon", db, name, List.of(), nativeTable);
    }

    private org.apache.paimon.partition.Partition partition(String value, long time) {
        return new org.apache.paimon.partition.Partition(Map.of("dt", value), 1L, 1L, 1L, time, true);
    }

    @Test
    public void testRemoveDroppedPartitionsAndRefreshVersions() throws Exception {
        PaimonTable table = table("db", "t");
        Identifier id = Identifier.create("db", "t");
        when(catalog.listPartitions(id))
                .thenReturn(List.of(partition("one", 100000L), partition("two", 200000L)));
        assertEquals(Set.of("dt=one", "dt=two"), Set.copyOf(metadata.listPartitionNames("db", "t", null)));
        long oldVersion = metadata.getPartitions(table, List.of("dt=two")).get(0).getModifiedTime();

        when(catalog.listPartitions(id))
                .thenReturn(List.of(partition("two", 300000L), partition("three", 400000L)));
        assertEquals(Set.of("dt=two", "dt=three"), Set.copyOf(metadata.listPartitionNames("db", "t", null)));
        assertTrue(metadata.getPartitions(table, List.of("dt=one")).isEmpty());
        assertNotEquals(oldVersion, metadata.getPartitions(table, List.of("dt=two")).get(0).getModifiedTime());

        when(catalog.listPartitions(id)).thenReturn(List.of());
        assertTrue(metadata.listPartitionNames("db", "t", null).isEmpty());
        assertTrue(metadata.getPartitions(table, List.of("dt=two", "dt=three")).isEmpty());
    }

    @Test
    public void testRemovalDoesNotAffectAnotherTable() throws Exception {
        table("db", "a");
        PaimonTable b = table("db", "b");
        when(catalog.listPartitions(Identifier.create("db", "a"))).thenReturn(List.of(partition("same", 100000L)));
        when(catalog.listPartitions(Identifier.create("db", "b"))).thenReturn(List.of(partition("same", 200000L)));
        metadata.listPartitionNames("db", "a", null);
        metadata.listPartitionNames("db", "b", null);
        long bVersion = metadata.getPartitions(b, List.of("dt=same")).get(0).getModifiedTime();
        when(catalog.listPartitions(Identifier.create("db", "a"))).thenReturn(List.of());
        assertTrue(metadata.listPartitionNames("db", "a", null).isEmpty());
        assertEquals(bVersion, metadata.getPartitions(b, List.of("dt=same")).get(0).getModifiedTime());
    }
}
