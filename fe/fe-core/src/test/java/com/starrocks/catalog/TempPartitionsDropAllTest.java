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

package com.starrocks.catalog;

import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TempPartitionsDropAllTest {

    @BeforeAll
    public static void setUp() {
        // Mock isCheckpointThread to return true so dropPartition skips tablet cleanup
        new MockUp<GlobalStateMgr>() {
            @Mock
            public boolean isCheckpointThread() {
                return true;
            }
        };
    }

    private Partition createMockPartition(long id, String name) {
        MaterializedIndex baseIndex = new MaterializedIndex(id * 10, MaterializedIndex.IndexState.NORMAL);
        return new Partition(id, id, name, baseIndex, null);
    }

    @Test
    public void testDropAllNormal() {
        TempPartitions tempPartitions = new TempPartitions();
        Partition p1 = createMockPartition(1L, "p1");
        Partition p2 = createMockPartition(2L, "p2");

        tempPartitions.addPartition(p1);
        tempPartitions.addPartition(p2);

        Assertions.assertFalse(tempPartitions.isEmpty());
        Assertions.assertEquals(2, tempPartitions.getAllPartitions().size());

        tempPartitions.dropAll();

        Assertions.assertTrue(tempPartitions.isEmpty());
        Assertions.assertEquals(0, tempPartitions.getAllPartitions().size());
    }

    @Test
    public void testDropAllWithOrphanEntries() {
        // Simulate the bug: two partitions with same name but different IDs.
        // nameToPartition will only have the last one, but idToPartition has both.
        TempPartitions tempPartitions = new TempPartitions();

        Partition p1 = createMockPartition(1L, "same_name");
        Partition p2 = createMockPartition(2L, "same_name");

        // Add first partition
        tempPartitions.addPartition(p1);
        // Add second partition with same name - overwrites nameToPartition but not idToPartition
        tempPartitions.addPartition(p2);

        // idToPartition has 2 entries, nameToPartition has 1
        Assertions.assertEquals(2, tempPartitions.getAllPartitions().size());
        Assertions.assertFalse(tempPartitions.isEmpty());

        // Before fix: dropAll() would only drop from nameToPartition (1 entry),
        // leaving orphan in idToPartition. After fix: idToPartition is also cleared.
        tempPartitions.dropAll();

        Assertions.assertTrue(tempPartitions.isEmpty());
        Assertions.assertEquals(0, tempPartitions.getAllPartitions().size());
        Assertions.assertNull(tempPartitions.getPartition(1L));
        Assertions.assertNull(tempPartitions.getPartition(2L));
        Assertions.assertNull(tempPartitions.getPartition("same_name"));
    }

    @Test
    public void testDropAllEmptyPartitions() {
        TempPartitions tempPartitions = new TempPartitions();

        Assertions.assertTrue(tempPartitions.isEmpty());
        // Should not throw
        tempPartitions.dropAll();
        Assertions.assertTrue(tempPartitions.isEmpty());
    }

    @Test
    public void testDropAllWithMultipleOrphans() {
        // Multiple orphan entries with different names that got overwritten
        TempPartitions tempPartitions = new TempPartitions();

        Partition p1 = createMockPartition(1L, "name_a");
        Partition p2 = createMockPartition(2L, "name_a");  // overwrites p1 in nameToPartition
        Partition p3 = createMockPartition(3L, "name_b");
        Partition p4 = createMockPartition(4L, "name_b");  // overwrites p3 in nameToPartition

        tempPartitions.addPartition(p1);
        tempPartitions.addPartition(p2);
        tempPartitions.addPartition(p3);
        tempPartitions.addPartition(p4);

        // idToPartition: {1, 2, 3, 4}, nameToPartition: {name_a->2, name_b->4}
        Assertions.assertEquals(4, tempPartitions.getAllPartitions().size());

        tempPartitions.dropAll();

        // After fix, all should be cleaned
        Assertions.assertTrue(tempPartitions.isEmpty());
        Assertions.assertEquals(0, tempPartitions.getAllPartitions().size());
    }
}
