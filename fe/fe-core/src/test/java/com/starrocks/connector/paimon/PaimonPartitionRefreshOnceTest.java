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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PaimonPartitionRefreshOnceTest {
    private static final Identifier ID = Identifier.create("db", "t");

    private Catalog catalog;
    private Table nativeTable;
    private PaimonMetadata metadata;
    private PaimonTable table;

    @BeforeEach
    public void setUp() throws Exception {
        catalog = mock(Catalog.class);
        nativeTable = mock(Table.class);
        when(nativeTable.partitionKeys()).thenReturn(List.of("dt"));
        when(nativeTable.rowType()).thenReturn(RowType.of(new org.apache.paimon.types.DataType[] {DataTypes.STRING()},
                new String[] {"dt"}));
        when(nativeTable.options()).thenReturn(Map.of());
        when(catalog.getTable(ID)).thenReturn(nativeTable);
        metadata = new PaimonMetadata("paimon", null, catalog, null);
        table = new PaimonTable("paimon", "db", "t", List.of(), nativeTable);
    }

    private org.apache.paimon.partition.Partition partition(String value, long time) {
        return new org.apache.paimon.partition.Partition(Map.of("dt", value), 1L, 1L, 1L, time, true);
    }

    @Test
    public void testMultipleMissingPartitionsListOnlyOnce() throws Exception {
        when(catalog.listPartitions(ID)).thenReturn(List.of(partition("one", 100000L)));
        metadata.listPartitionNames("db", "t", null);
        verify(catalog, times(1)).listPartitions(ID);

        assertTrue(metadata.getPartitions(table, List.of("dt=gone1", "dt=gone2", "dt=gone3")).isEmpty());
        verify(catalog, times(2)).listPartitions(ID);
    }

    @Test
    public void testColdCacheListsOnlyOnce() throws Exception {
        when(catalog.listPartitions(ID)).thenReturn(List.of(partition("one", 100000L), partition("two", 200000L)));

        List<PartitionInfo> partitions = metadata.getPartitions(table, List.of("dt=two", "dt=missing", "dt=one"));
        assertEquals(2, partitions.size());
        assertEquals("dt=two", ((Partition) partitions.get(0)).getPartitionName());
        assertEquals("dt=one", ((Partition) partitions.get(1)).getPartitionName());
        verify(catalog, times(1)).listPartitions(ID);
    }

    @Test
    public void testWarmCacheHitDoesNotList() throws Exception {
        when(catalog.listPartitions(ID)).thenReturn(List.of(partition("one", 100000L), partition("two", 200000L)));
        metadata.listPartitionNames("db", "t", null);
        verify(catalog, times(1)).listPartitions(ID);

        assertEquals(2, metadata.getPartitions(table, List.of("dt=one", "dt=two")).size());
        verify(catalog, times(1)).listPartitions(ID);
    }

    @Test
    public void testLaterMissRefreshesEarlierCacheHit() throws Exception {
        when(catalog.listPartitions(ID)).thenReturn(List.of(partition("one", 100000L)));
        metadata.listPartitionNames("db", "t", null);
        long oldTime = metadata.getPartitions(table, List.of("dt=one")).get(0).getModifiedTime();
        when(catalog.listPartitions(ID)).thenReturn(List.of(partition("one", 300000L), partition("two", 200000L)));

        List<PartitionInfo> partitions = metadata.getPartitions(table, List.of("dt=one", "dt=two", "dt=missing"));
        assertEquals(2, partitions.size());
        assertEquals("dt=one", ((Partition) partitions.get(0)).getPartitionName());
        assertEquals(oldTime + 200000L, partitions.get(0).getModifiedTime());
        assertEquals("dt=two", ((Partition) partitions.get(1)).getPartitionName());
        assertEquals(oldTime + 100000L, partitions.get(1).getModifiedTime());
        verify(catalog, times(2)).listPartitions(ID);
    }

    @Test
    public void testEmptyRequestDoesNotLoadColdCache() throws Exception {
        assertTrue(metadata.getPartitions(table, List.of()).isEmpty());
        verify(catalog, never()).listPartitions(ID);
    }

    @Test
    public void testUnpartitionedTableDoesNotList() throws Exception {
        when(nativeTable.partitionKeys()).thenReturn(List.of());
        PaimonTable unpartitioned = new PaimonTable("paimon", "db", "t", List.of(), nativeTable);
        assertEquals(1, metadata.getPartitions(unpartitioned, List.of("t")).size());
        verify(catalog, never()).listPartitions(ID);
    }
}
