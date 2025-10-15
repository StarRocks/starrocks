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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Unit tests for TabletInvertedIndex class
 */
public class TabletInvertedIndexTest {
    private static final Logger LOG = LogManager.getLogger(TabletInvertedIndexTest.class);

    public static class IdGenerator {
        private static final int BATCH_ID_INTERVAL = 1000;
        private long nextId;

        public IdGenerator() {
            nextId = BATCH_ID_INTERVAL;
        }

        // performance is more quickly
        public synchronized long getNextId() {
            return nextId++;
        }
    }

    @Test
    public void testTabletInvertedIndexPerf() {
        // 300k tablets, each tablet has 1 replica, has 24 disks
        TabletInvertedIndex tabletInvertedIndex = new TabletInvertedIndex();

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
            TabletMeta meta = new TabletMeta(dbId, tableId, partitionId, indexId, schemaHash, TStorageMedium.HDD);
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
                    result1.put(diskHashes[j],
                            tabletInvertedIndex.getTabletNumByBackendIdAndPathHash(backendId, diskHashes[j]));
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
