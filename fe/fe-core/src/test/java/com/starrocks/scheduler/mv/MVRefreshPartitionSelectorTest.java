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

import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.sql.common.PCellNone;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellWithName;
import com.starrocks.sql.common.PartitionNameSetMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;

public class MVRefreshPartitionSelectorTest {

    @BeforeAll
    public static void setUp() throws Exception {
    }

    private Map<Table, PCellSortedSet> mockPartitionSet(long rows, long bytes) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);

        Mockito.when(partition.getRowCount()).thenReturn(rows);
        Mockito.when(partition.getDataSize()).thenReturn(bytes);
        Mockito.when(table.getPartition(anyString())).thenReturn(partition);

        Map<Table, PCellSortedSet> map = new HashMap<>();
        PCellSortedSet set = PCellSortedSet.of();
        set.add(PCellWithName.of("p1", new PCellNone()));
        map.put(table, set);
        return map;
    }

    private Table mockExternalTable() {
        IcebergTable table = Mockito.mock(IcebergTable.class);
        Mockito.when(table.getUUID()).thenReturn(UUID.randomUUID().toString());
        return table;
    }

    @Test
    public void testFirstPartitionAlwaysAllowed() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        Assertions.assertTrue(selector.canAddPartition(mockPartitionSet(2000, 20000)));
    }

    @Test
    public void testCanAddWithinThreshold() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(500, 5000)); // First one always allowed

        Assertions.assertTrue(selector.canAddPartition(mockPartitionSet(400, 4000)));
    }

    @Test
    public void testExceedRowLimit() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(900, 5000));

        Assertions.assertFalse(selector.canAddPartition(mockPartitionSet(200, 1000))); // 900+200 > 1000
    }

    @Test
    public void testExceedByteLimit() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(800, 9000));

        Assertions.assertFalse(selector.canAddPartition(mockPartitionSet(100, 2000))); // 9000+2000 > 10000
    }

    @Test
    public void testExceedPartitionLimit() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));
        selector.addPartition(mockPartitionSet(10, 200));

        Assertions.assertFalse(selector.canAddPartition(mockPartitionSet(10, 200)));
    }

    @Test
    public void testAddPartitionAccumulatesUsage() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(300, 3000));
        selector.addPartition(mockPartitionSet(400, 4000));

        Assertions.assertFalse(selector.canAddPartition(mockPartitionSet(400, 4000)));
    }

    @Test
    public void testExternalTablePartitionsStatistics() throws Exception {
        Map<Table, PartitionNameSetMap> externalPartitionMap = new HashMap<>();
        PartitionNameSetMap partitionMap1 = PartitionNameSetMap.of();
        partitionMap1.put("p1", Set.of("dt=p1"));
        partitionMap1.put("p2", Set.of("dt=p2", "dt=p3"));
        externalPartitionMap.put(mockExternalTable(), partitionMap1);
        PartitionNameSetMap partitionMap2 = PartitionNameSetMap.of();
        partitionMap2.put("p1", Set.of("dt=p1"));
        partitionMap2.put("p2", Set.of("dt=p2"));
        externalPartitionMap.put(mockExternalTable(), partitionMap2);

        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000,
                10, externalPartitionMap);
        HashMap<Table, PCellSortedSet> toSelectedPartitionMap = new HashMap<>();
        toSelectedPartitionMap.put(mockExternalTable(),
                PCellSortedSet.of(Set.of(new PCellWithName("p1", new PCellNone()))));
        Assertions.assertTrue(selector.canAddPartition(toSelectedPartitionMap));
    }
}
