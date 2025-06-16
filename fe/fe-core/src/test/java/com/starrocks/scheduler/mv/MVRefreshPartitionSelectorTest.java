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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;

public class MVRefreshPartitionSelectorTest {

    private Map<Table, Set<String>> mockPartitionSet(long rows, long bytes) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);

        Mockito.when(partition.getRowCount()).thenReturn(rows);
        Mockito.when(partition.getDataSize()).thenReturn(bytes);
        Mockito.when(table.getPartition(anyString())).thenReturn(partition);

        Map<Table, Set<String>> map = new HashMap<>();
        map.put(table, new HashSet<>(Collections.singleton("p1")));
        return map;
    }

    @Test
    public void testFirstPartitionAlwaysAllowed() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        Assert.assertTrue(selector.canAddPartition(mockPartitionSet(2000, 20000)));
    }

    @Test
    public void testCanAddWithinThreshold() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(500, 5000)); // First one always allowed

        Assert.assertTrue(selector.canAddPartition(mockPartitionSet(400, 4000)));
    }

    @Test
    public void testExceedRowLimit() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(900, 5000));

        Assert.assertFalse(selector.canAddPartition(mockPartitionSet(200, 1000))); // 900+200 > 1000
    }

    @Test
    public void testExceedByteLimit() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(800, 9000));

        Assert.assertFalse(selector.canAddPartition(mockPartitionSet(100, 2000))); // 9000+2000 > 10000
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

        Assert.assertFalse(selector.canAddPartition(mockPartitionSet(10, 200)));
    }

    @Test
    public void testAddPartitionAccumulatesUsage() throws Exception {
        MVRefreshPartitionSelector selector = new MVRefreshPartitionSelector(1000, 10000, 10);
        selector.addPartition(mockPartitionSet(300, 3000));
        selector.addPartition(mockPartitionSet(400, 4000));

        Assert.assertFalse(selector.canAddPartition(mockPartitionSet(400, 4000)));
    }
}
