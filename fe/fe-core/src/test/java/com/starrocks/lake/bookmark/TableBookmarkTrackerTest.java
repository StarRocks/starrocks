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

package com.starrocks.lake.bookmark;

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TableBookmarkTrackerTest extends BookmarkTestBase {

    /** Tracker subclass that parks at the test seam on a reusable semaphore pair. */
    static class TestTracker extends TableBookmarkTracker {
        final Semaphore arrival = new Semaphore(0);
        final Semaphore release = new Semaphore(0);
        volatile boolean armed = true;

        TestTracker(long dbId, long tableId) {
            super(dbId, tableId);
        }

        @Override
        protected void onCopyTableStateInBookmarkCreation() {
            if (!armed) {
                return;
            }
            arrival.release();
            try {
                release.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Manager subclass that hands out a TestTracker. */
    static class TestBookmarkManager extends BookmarkManager {
        private volatile TestTracker last;

        @Override
        protected TableBookmarkTracker createTracker(long dbId, long tableId) {
            TestTracker t = new TestTracker(dbId, tableId);
            last = t;
            return t;
        }

        TestTracker tracker() {
            return last;
        }
    }

    private static TestTracker waitForTracker(TestBookmarkManager mgr) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (mgr.tracker() == null) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("TestTracker was not created within 10s");
            }
            Thread.sleep(1);
        }
        return mgr.tracker();
    }

    @Test
    public void testCreatingBookmark() throws Exception {
        long tableId = createDefaultTable();
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        Partition p1 = table.getPartition("p1");
        long lp1 = p1.getId();
        long pp1 = p1.getSubPartitions().iterator().next().getId();
        TestBookmarkManager mgr = new TestBookmarkManager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("create_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("create_h2");

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            // Phase 1 — empty active map; only the in-flight bookmark covers p1.
            Future<Bookmark> f1 = pool.submit(() -> mgr.create(dbId, tableId, h1));
            TestTracker spy = waitForTracker(mgr);
            spy.arrival.acquire();

            assertNotNull(spy.peekCreating());
            assertEquals(0, mgr.activeBookmarkCount(dbId, tableId));
            assertEquals(1L, mgr.getPhysicalPartitionFenceVersion(dbId, tableId, lp1, pp1).get());

            spy.release.release();
            Bookmark b1 = f1.get(60, TimeUnit.SECONDS);
            assertNull(spy.peekCreating());
            assertEquals(1, mgr.activeBookmarkCount(dbId, tableId));
            assertTrue(mgr.findBookmarkById(dbId, tableId, b1.getBookmarkId()).isPresent());
            assertEquals(1L, mgr.getPhysicalPartitionFenceVersion(dbId, tableId, lp1, pp1).get());

            // Phase 2 — active b1 plus an in-flight bookmark that captures a freshly added
            // partition p3. b1 does not cover p3, so the fence for p3 must come from the
            // in-flight slot — exercises the partition-only-in-newer-bookmark path.
            addPartition(tableId, "p3", "2024-04-01");
            Partition p3 = table.getPartition("p3");
            long lp3 = p3.getId();
            long pp3 = p3.getSubPartitions().iterator().next().getId();
            Future<Bookmark> f2 = pool.submit(() -> mgr.create(dbId, tableId, h2));
            spy.arrival.acquire();

            long inFlightId = spy.peekCreating().getBookmarkId();
            assertTrue(inFlightId > b1.getBookmarkId());
            assertEquals(1, mgr.activeBookmarkCount(dbId, tableId));
            assertEquals(1L, mgr.getPhysicalPartitionFenceVersion(dbId, tableId, lp1, pp1).get());
            assertEquals(1L, mgr.getPhysicalPartitionFenceVersion(dbId, tableId, lp3, pp3).get());

            spy.release.release();
            Bookmark b2 = f2.get(60, TimeUnit.SECONDS);
            assertNull(spy.peekCreating());
            assertEquals(2, mgr.activeBookmarkCount(dbId, tableId));
            assertTrue(mgr.findBookmarkById(dbId, tableId, b1.getBookmarkId()).isPresent());
            assertTrue(mgr.findBookmarkById(dbId, tableId, b2.getBookmarkId()).isPresent());

            // Phase 3 — same holder + unchanged state → AlreadyAtLatest, slot still null after exception.
            Future<?> f3 = pool.submit(() -> {
                assertThrows(AlreadyAtLatestException.class,
                        () -> mgr.create(dbId, tableId, h2));
                return null;
            });
            spy.arrival.acquire();
            spy.release.release();
            f3.get(60, TimeUnit.SECONDS);
            assertNull(spy.peekCreating());
        } finally {
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testConcurrentCreate() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = GlobalStateMgr.getCurrentState().getBookmarkManager();

        int n = 16;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Bookmark>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                final int id = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    return mgr.create(dbId, tableId, BookmarkHolder.forEmptyInfo("cc_h_" + id));
                }));
            }
            start.countDown();

            Set<Long> bookmarkIds = new HashSet<>();
            for (Future<Bookmark> f : futures) {
                bookmarkIds.add(f.get(60, TimeUnit.SECONDS).getBookmarkId());
            }
            assertEquals(1, bookmarkIds.size(),
                    "concurrent creates should converge on a single bookmark");

            assertEquals(1, mgr.activeBookmarkCount(dbId, tableId));
            assertEquals(n, mgr.referenceCount(dbId, tableId, bookmarkIds.iterator().next()));
        } finally {
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
