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

import com.starrocks.thrift.TStorageMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;


/**
 * Unit tests for TabletInvertedIndex class
 */
public class TabletInvertedIndexTest {

    private TabletInvertedIndex tabletInvertedIndex;
    private TabletMeta tabletMeta;
    private Replica replica1;
    private Replica replica2;
    private Replica replica3;

    @BeforeEach
    public void setUp() {
        tabletInvertedIndex = new TabletInvertedIndex();
        
        // Create test tablet meta
        tabletMeta = new TabletMeta(1L, 2L, 3L, 4L, 1, TStorageMedium.HDD);
        
        // Create test replicas
        replica1 = new Replica(100L, 1000L, 1L, 123, 0L, 0L, 
                              Replica.ReplicaState.NORMAL, -1L, 1L);
        replica2 = new Replica(101L, 1001L, 1L, 123, 0L, 0L, 
                              Replica.ReplicaState.NORMAL, -1L, 1L);
        replica3 = new Replica(102L, 1002L, 1L, 123, 0L, 0L, 
                              Replica.ReplicaState.NORMAL, -1L, 1L);
    }

    @Test
    public void testGetReplicas_WithReplicas() {
        // Given: Add tablet and replicas
        long tabletId = 1000L;
        tabletInvertedIndex.addTablet(tabletId, tabletMeta);
        tabletInvertedIndex.addReplica(tabletId, replica1);
        tabletInvertedIndex.addReplica(tabletId, replica2);
        tabletInvertedIndex.addReplica(tabletId, replica3);

        // When: Get replicas for the tablet
        Map<Long, Replica> replicas = tabletInvertedIndex.getReplicas(tabletId);

        // Then: Verify the result
        Assertions.assertNotNull(replicas, "Replicas map should not be null");
        Assertions.assertEquals(3, replicas.size(), "Should have 3 replicas");
        
        // Verify each replica is present
        Assertions.assertTrue(replicas.containsKey(1000L), "Should contain replica on backend 1000");
        Assertions.assertTrue(replicas.containsKey(1001L), "Should contain replica on backend 1001");
        Assertions.assertTrue(replicas.containsKey(1002L), "Should contain replica on backend 1002");
        
        // Verify replica details
        Assertions.assertEquals(replica1, replicas.get(1000L), "Replica on backend 1000 should match");
        Assertions.assertEquals(replica2, replicas.get(1001L), "Replica on backend 1001 should match");
        Assertions.assertEquals(replica3, replicas.get(1002L), "Replica on backend 1002 should match");
    }
}
