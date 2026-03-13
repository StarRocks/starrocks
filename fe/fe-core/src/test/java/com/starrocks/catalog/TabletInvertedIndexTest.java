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

import com.starrocks.authorization.IdGenerator;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Unit tests for TabletInvertedIndex class
 */
public class TabletInvertedIndexTest {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndexTest.class);

    private TabletInvertedIndex tabletInvertedIndex;
    private TabletMeta tabletMeta;
    private Replica replica1;
    private Replica replica2;
    private Replica replica3;

    @BeforeEach
    public void setUp() {
        tabletInvertedIndex = new TabletInvertedIndex();

        // Create test tablet meta
        tabletMeta = new TabletMeta(1L, 2L, 3L, 4L, TStorageMedium.HDD);

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
        //Map<Long, Replica> replicas = tabletInvertedIndex.getReplicas(tabletId);
        List<Replica> replicas = tabletInvertedIndex.getReplicasByTabletId(tabletId);

        // Then: Verify the result
        Assertions.assertNotNull(replicas, "Replicas map should not be null");
        Assertions.assertEquals(3, replicas.size(), "Should have 3 replicas");

        Assertions.assertTrue(replicas.stream().anyMatch(replica -> replica.getBackendId() == 1000L),
                "Should contain replica on backend 1000");
        Assertions.assertTrue(replicas.stream().anyMatch(replica -> replica.getBackendId() == 1001L),
                "Should contain replica on backend 1001");
        Assertions.assertTrue(replicas.stream().anyMatch(replica -> replica.getBackendId() == 1002L),
                "Should contain replica on backend 1002");

        // Verify replica details
        Assertions.assertTrue(replicas.stream().anyMatch(replica -> replica.equals(replica1)),
                "Replica on backend 1000 should match");
        Assertions.assertTrue(replicas.stream().anyMatch(replica -> replica.equals(replica2)),
                "Replica on backend 1001 should match");
        Assertions.assertTrue(replicas.stream().anyMatch(replica -> replica.equals(replica3)),
                "Replica on backend 1002 should match");
    }

    @Test
    public void testDeleteTabletsBatch() {
        // Given: Add 3 tablets with replicas on different backends
        long tabletId1 = 2001L;
        long tabletId2 = 2002L;
        long tabletId3 = 2003L;

        TabletMeta meta1 = new TabletMeta(1L, 2L, 3L, 4L, TStorageMedium.HDD);
        TabletMeta meta2 = new TabletMeta(1L, 2L, 3L, 5L, TStorageMedium.HDD);
        TabletMeta meta3 = new TabletMeta(1L, 2L, 3L, 6L, TStorageMedium.HDD);

        tabletInvertedIndex.addTablet(tabletId1, meta1);
        tabletInvertedIndex.addTablet(tabletId2, meta2);
        tabletInvertedIndex.addTablet(tabletId3, meta3);

        tabletInvertedIndex.addReplica(tabletId1, replica1);
        tabletInvertedIndex.addReplica(tabletId2, replica2);
        tabletInvertedIndex.addReplica(tabletId3, replica3);

        // Verify tablets and replicas exist
        Assertions.assertNotNull(tabletInvertedIndex.getTabletMeta(tabletId1));
        Assertions.assertNotNull(tabletInvertedIndex.getTabletMeta(tabletId2));
        Assertions.assertNotNull(tabletInvertedIndex.getTabletMeta(tabletId3));

        // When: Batch delete tablets 1 and 3
        tabletInvertedIndex.deleteTablets(Arrays.asList(tabletId1, tabletId3));

        // Then: Tablets 1 and 3 should be gone, tablet 2 should remain
        Assertions.assertNull(tabletInvertedIndex.getTabletMeta(tabletId1));
        Assertions.assertNotNull(tabletInvertedIndex.getTabletMeta(tabletId2));
        Assertions.assertNull(tabletInvertedIndex.getTabletMeta(tabletId3));

        // Replicas should also be cleaned up
        Assertions.assertEquals(0, tabletInvertedIndex.getReplicasByTabletId(tabletId1).size());
        Assertions.assertEquals(1, tabletInvertedIndex.getReplicasByTabletId(tabletId2).size());
        Assertions.assertEquals(0, tabletInvertedIndex.getReplicasByTabletId(tabletId3).size());

        // Backend-to-tablet mapping should be updated
        List<Long> backend1000Tablets = tabletInvertedIndex.getTabletIdsByBackendId(1000L);
        Assertions.assertFalse(backend1000Tablets.contains(tabletId1));
        List<Long> backend1002Tablets = tabletInvertedIndex.getTabletIdsByBackendId(1002L);
        Assertions.assertFalse(backend1002Tablets.contains(tabletId3));
        // Backend 1001 still has tablet 2
        List<Long> backend1001Tablets = tabletInvertedIndex.getTabletIdsByBackendId(1001L);
        Assertions.assertTrue(backend1001Tablets.contains(tabletId2));
    }

    @Test
    public void testDeleteTabletsEmptyCollection() {
        // Given: Add a tablet
        long tabletId = 3001L;
        tabletInvertedIndex.addTablet(tabletId, tabletMeta);
        tabletInvertedIndex.addReplica(tabletId, replica1);

        // When: Batch delete with empty collection
        tabletInvertedIndex.deleteTablets(Collections.emptyList());

        // Then: Tablet should still exist
        Assertions.assertNotNull(tabletInvertedIndex.getTabletMeta(tabletId));
        Assertions.assertEquals(1, tabletInvertedIndex.getReplicasByTabletId(tabletId).size());
    }

    @Test
    public void testDeleteTabletsConsistentWithSingleDelete() {
        // Verify batch delete produces same result as sequential single deletes
        long tabletId1 = 4001L;
        long tabletId2 = 4002L;

        // Setup two identical indexes
        TabletInvertedIndex batchIndex = new TabletInvertedIndex();
        TabletInvertedIndex singleIndex = new TabletInvertedIndex();

        TabletMeta meta = new TabletMeta(1L, 2L, 3L, 4L, TStorageMedium.HDD);
        Replica r1 = new Replica(200L, 2000L, 1L, 123, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, 1L);
        Replica r2 = new Replica(201L, 2001L, 1L, 123, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, 1L);
        // Need separate replica instances for the second index
        Replica r1Copy = new Replica(200L, 2000L, 1L, 123, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, 1L);
        Replica r2Copy = new Replica(201L, 2001L, 1L, 123, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, 1L);

        for (TabletInvertedIndex idx : new TabletInvertedIndex[] {batchIndex, singleIndex}) {
            idx.addTablet(tabletId1, meta);
            idx.addTablet(tabletId2, meta);
        }
        batchIndex.addReplica(tabletId1, r1);
        batchIndex.addReplica(tabletId2, r2);
        singleIndex.addReplica(tabletId1, r1Copy);
        singleIndex.addReplica(tabletId2, r2Copy);

        // Batch delete
        batchIndex.deleteTablets(Arrays.asList(tabletId1, tabletId2));

        // Single deletes
        singleIndex.deleteTablet(tabletId1);
        singleIndex.deleteTablet(tabletId2);

        // Both should produce same result
        Assertions.assertNull(batchIndex.getTabletMeta(tabletId1));
        Assertions.assertNull(batchIndex.getTabletMeta(tabletId2));
        Assertions.assertNull(singleIndex.getTabletMeta(tabletId1));
        Assertions.assertNull(singleIndex.getTabletMeta(tabletId2));

        Assertions.assertEquals(
                batchIndex.getTabletIdsByBackendId(2000L),
                singleIndex.getTabletIdsByBackendId(2000L));
        Assertions.assertEquals(
                batchIndex.getTabletIdsByBackendId(2001L),
                singleIndex.getTabletIdsByBackendId(2001L));
    }

    @Test
    public void testTabletInvertedIndexPerf() {
        // 300k tablets, each tablet has 1 replica, has 24 disks
        tabletInvertedIndex = new TabletInvertedIndex();

        int diskNum = 24;
        Long[] diskHashes = new Long[diskNum];
        for (int i = 0; i < diskNum; ++i) {
            diskHashes[i] = ThreadLocalRandom.current().nextLong();
        }

        IdGenerator idGenerator = new IdGenerator();
        long dbId = idGenerator.getNextId();
        long tableId = idGenerator.getNextId();
        long partitionId = idGenerator.getNextId();
        long indexId = idGenerator.getNextId();
        long backendId = idGenerator.getNextId();
        int schemaHash = (int) idGenerator.getNextId();

        int num = 500000;
        // build test data, create `num` of tablets and distribute them into `diskNum` of disks
        for (int i = 0; i < num; ++i) {
            TabletMeta meta = new TabletMeta(dbId, tableId, partitionId, indexId, TStorageMedium.HDD);
            long tabletId = idGenerator.getNextId();
            tabletInvertedIndex.addTablet(tabletId, meta);

            int diskIndex = ThreadLocalRandom.current().nextInt(diskNum);
            long replicaId = idGenerator.getNextId();
            Replica replica = new Replica(replicaId, backendId, schemaHash, Replica.ReplicaState.NORMAL);
            replica.setPathHash(diskHashes[diskIndex]);
            tabletInvertedIndex.addReplica(tabletId, replica);
        }

        long repeats = 1;
        Map<Long, Long> result1 = new HashMap<>();
        {
            long start = System.currentTimeMillis();
            for (int i = 0; i < repeats; ++i) {
                for (int j = 0; j < diskNum; ++j) {
                    Map<Long, Long> pathHashToTabletNum = tabletInvertedIndex.getTabletNumByBackendIdGroupByPathHash(backendId);
                    result1.put(diskHashes[j], pathHashToTabletNum.get(diskHashes[j]));
                }
            }
            long end = System.currentTimeMillis();
            LOG.warn("[tabletNum={}, diskNum={}] getTabletNumByBackendIdAndPathHash() cost {} ms", num, diskNum,
                    end - start);
        }
        Map<Long, Long> result2 = new HashMap<>();
        {
            long start = System.currentTimeMillis();
            for (int i = 0; i < repeats; ++i) {
                result2 = tabletInvertedIndex.getTabletNumByBackendIdGroupByPathHash(backendId);
            }
            long end = System.currentTimeMillis();
            LOG.warn("[tabletNum={}, diskNum={}] getTabletNumByBackendIdGroupByPathHash() cost {} ms", num, diskNum,
                    end - start);
        }
        Assertions.assertEquals(result1, result2);
    }
}
