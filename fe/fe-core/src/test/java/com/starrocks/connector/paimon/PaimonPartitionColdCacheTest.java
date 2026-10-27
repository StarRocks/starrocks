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
import com.starrocks.connector.PartitionInfo;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PaimonPartitionColdCacheTest {
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
    public void testGetPartitionsWithoutListingFirst() throws Exception {
        PaimonTable table = table("db", "t");
        when(catalog.listPartitions(Identifier.create("db", "t")))
                .thenReturn(List.of(partition("one", 100000L), partition("two", 200000L)));

        List<PartitionInfo> partitions = metadata.getPartitions(table, List.of("dt=two", "dt=one"));
        assertEquals(2, partitions.size());
        assertEquals("dt=two", ((Partition) partitions.get(0)).getPartitionName());
        assertEquals("dt=one", ((Partition) partitions.get(1)).getPartitionName());
        assertNotEquals(partitions.get(0).getModifiedTime(), partitions.get(1).getModifiedTime());
    }

    @Test
    public void testMissingPartitionOnColdCache() throws Exception {
        PaimonTable table = table("db", "t");
        when(catalog.listPartitions(Identifier.create("db", "t"))).thenReturn(List.of());
        assertTrue(metadata.getPartitions(table, List.of("dt=missing")).isEmpty());
    }

    @Test
    public void testLoadNewPartitionOnWarmCache() throws Exception {
        PaimonTable table = table("db", "t");
        Identifier id = Identifier.create("db", "t");
        when(catalog.listPartitions(id)).thenReturn(List.of(partition("one", 100000L)));
        metadata.listPartitionNames("db", "t", null);
        when(catalog.listPartitions(id))
                .thenReturn(List.of(partition("one", 100000L), partition("two", 200000L)));
        List<PartitionInfo> partitions = metadata.getPartitions(table, List.of("dt=two", "dt=one"));
        assertEquals(2, partitions.size());
        assertEquals("dt=two", ((Partition) partitions.get(0)).getPartitionName());
        assertEquals("dt=one", ((Partition) partitions.get(1)).getPartitionName());
    }
}
