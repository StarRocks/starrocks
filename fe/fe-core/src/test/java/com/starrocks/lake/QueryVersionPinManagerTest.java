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

package com.starrocks.lake;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryVersionPinManagerTest {

    private QueryVersionPinManager manager;

    @BeforeEach
    public void setUp() {
        manager = new QueryVersionPinManager();
    }

    @AfterEach
    public void tearDown() {
        manager.clear();
    }

    @Test
    public void testPinAndUnpinBasicLifecycle() {
        Map<Long, Long> partitionVersions = new HashMap<>();
        partitionVersions.put(100L, 5L);
        partitionVersions.put(200L, 10L);

        manager.pinVersions("query-1", partitionVersions);

        assertEquals(5L, manager.getMinPinnedVersion(100L));
        assertEquals(10L, manager.getMinPinnedVersion(200L));
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(999L));
        assertTrue(manager.hasPinnedVersions(100L));
        assertFalse(manager.hasPinnedVersions(999L));
        assertEquals(1, manager.getActivePinCount());

        manager.unpinVersions("query-1");

        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(200L));
        assertFalse(manager.hasPinnedVersions(100L));
        assertEquals(0, manager.getActivePinCount());
    }

    @Test
    public void testMultipleQueriesSamePartition() {
        Map<Long, Long> versions1 = new HashMap<>();
        versions1.put(100L, 5L);

        Map<Long, Long> versions2 = new HashMap<>();
        versions2.put(100L, 8L);

        Map<Long, Long> versions3 = new HashMap<>();
        versions3.put(100L, 3L);

        manager.pinVersions("query-1", versions1);
        manager.pinVersions("query-2", versions2);
        manager.pinVersions("query-3", versions3);

        assertEquals(3L, manager.getMinPinnedVersion(100L));
        assertEquals(3, manager.getActivePinCount());

        manager.unpinVersions("query-3");
        assertEquals(5L, manager.getMinPinnedVersion(100L));
        assertEquals(2, manager.getActivePinCount());

        manager.unpinVersions("query-1");
        assertEquals(8L, manager.getMinPinnedVersion(100L));

        manager.unpinVersions("query-2");
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
    }

    @Test
    public void testSameVersionMultipleQueries() {
        Map<Long, Long> versions1 = new HashMap<>();
        versions1.put(100L, 5L);

        Map<Long, Long> versions2 = new HashMap<>();
        versions2.put(100L, 5L);

        manager.pinVersions("query-1", versions1);
        manager.pinVersions("query-2", versions2);

        assertEquals(5L, manager.getMinPinnedVersion(100L));

        Map<Long, Integer> pinned = manager.getPinnedVersionsForPartition(100L);
        assertEquals(2, pinned.get(5L).intValue());

        manager.unpinVersions("query-1");
        assertEquals(5L, manager.getMinPinnedVersion(100L));
        pinned = manager.getPinnedVersionsForPartition(100L);
        assertEquals(1, pinned.get(5L).intValue());

        manager.unpinVersions("query-2");
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
    }

    @Test
    public void testIdempotentUnpin() {
        Map<Long, Long> versions = new HashMap<>();
        versions.put(100L, 5L);
        manager.pinVersions("query-1", versions);

        manager.unpinVersions("query-1");
        manager.unpinVersions("query-1");
        manager.unpinVersions("query-1");

        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
        assertEquals(0, manager.getActivePinCount());
    }

    @Test
    public void testUnpinNonexistentQuery() {
        manager.unpinVersions("nonexistent-query");
        assertEquals(0, manager.getActivePinCount());
    }

    @Test
    public void testPinEmptyOrNullVersions() {
        manager.pinVersions("query-1", new HashMap<>());
        assertEquals(0, manager.getActivePinCount());

        manager.pinVersions("query-2", null);
        assertEquals(0, manager.getActivePinCount());
    }

    @Test
    public void testCleanupStalePins() throws InterruptedException {
        Map<Long, Long> versions1 = new HashMap<>();
        versions1.put(100L, 5L);
        manager.pinVersions("query-old", versions1);

        Thread.sleep(50);

        Map<Long, Long> versions2 = new HashMap<>();
        versions2.put(200L, 10L);
        manager.pinVersions("query-new", versions2);

        int cleaned = manager.cleanupStalePins(30);
        assertEquals(1, cleaned);
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
        assertEquals(10L, manager.getMinPinnedVersion(200L));
        assertEquals(1, manager.getActivePinCount());
    }

    @Test
    public void testCleanupNoStalePins() {
        Map<Long, Long> versions = new HashMap<>();
        versions.put(100L, 5L);
        manager.pinVersions("query-1", versions);

        int cleaned = manager.cleanupStalePins(60_000);
        assertEquals(0, cleaned);
        assertEquals(5L, manager.getMinPinnedVersion(100L));
    }

    @Test
    public void testConcurrentPinUnpin() throws InterruptedException {
        int threadCount = 16;
        int queriesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < queriesPerThread; i++) {
                        String queryId = "thread-" + threadId + "-query-" + i;
                        Map<Long, Long> versions = new HashMap<>();
                        versions.put(100L, (long) (i + 1));
                        versions.put(200L, (long) (i + 1));
                        manager.pinVersions(queryId, versions);
                        manager.unpinVersions(queryId);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS));

        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(200L));
        assertEquals(0, manager.getActivePinCount());

        executor.shutdown();
    }

    @Test
    public void testGetPinnedVersionsForPartition() {
        Map<Long, Long> versions1 = new HashMap<>();
        versions1.put(100L, 5L);
        versions1.put(200L, 10L);

        Map<Long, Long> versions2 = new HashMap<>();
        versions2.put(100L, 5L);
        versions2.put(100L, 8L);

        manager.pinVersions("query-1", versions1);
        manager.pinVersions("query-2", versions2);

        Map<Long, Integer> pinned = manager.getPinnedVersionsForPartition(100L);
        assertTrue(pinned.containsKey(5L) || pinned.containsKey(8L));

        Map<Long, Integer> empty = manager.getPinnedVersionsForPartition(999L);
        assertTrue(empty.isEmpty());
    }

    @Test
    public void testClear() {
        Map<Long, Long> versions = new HashMap<>();
        versions.put(100L, 5L);
        versions.put(200L, 10L);
        manager.pinVersions("query-1", versions);

        assertEquals(2, manager.getPinnedVersionsForPartition(100L).size()
                + manager.getPinnedVersionsForPartition(200L).size());

        manager.clear();

        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(200L));
        assertEquals(0, manager.getActivePinCount());
    }

    @Test
    public void testMultiplePartitionsPerQuery() {
        Map<Long, Long> versions = new HashMap<>();
        versions.put(100L, 5L);
        versions.put(200L, 10L);
        versions.put(300L, 15L);

        manager.pinVersions("query-1", versions);

        assertEquals(5L, manager.getMinPinnedVersion(100L));
        assertEquals(10L, manager.getMinPinnedVersion(200L));
        assertEquals(15L, manager.getMinPinnedVersion(300L));
        assertEquals(1, manager.getActivePinCount());

        manager.unpinVersions("query-1");

        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(100L));
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(200L));
        assertEquals(Long.MAX_VALUE, manager.getMinPinnedVersion(300L));
    }
}
