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

import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        TestTracker(long dbId, long tableId, BookmarkMetrics metrics) {
            super(dbId, tableId, metrics);
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
            TestTracker t = new TestTracker(dbId, tableId, metrics());
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

    /**
     * create() must publish the in-flight bookmark, partition versions already captured, BEFORE
     * releasing the table read lock. The vacuum daemons rely on exactly this: version advances
     * take the table write lock, so a slot published under the read lock is ordered before any
     * visible version a vacuum round can read afterwards, and the round's fence read (placed
     * after its visible-version read) cannot miss the bookmark. If create() released the lock
     * first, a publish plus a whole vacuum round could run before the slot appears and reclaim
     * the versions the bookmark just captured.
     */
    @Test
    public void testCreatePublishesInFlightBookmarkBeforeTableUnlock() throws Exception {
        long tableId = createDefaultTable();
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        Partition p1 = table.getPartition("p1");
        long lp1 = p1.getId();
        long pp1 = p1.getSubPartitions().iterator().next().getId();
        long expectedVersion = p1.getSubPartitions().iterator().next().getVisibleVersion();

        TestBookmarkManager mgr = new TestBookmarkManager();
        AtomicReference<Thread> createThread = new AtomicReference<>();
        // null = create's unlock not observed; empty = the slot was missing (or had not captured
        // p1) at the moment create() released the table lock. Only the first unlock on the
        // create thread is recorded; background daemons unlocking the same table are ignored.
        AtomicReference<Optional<Long>> slotVersionAtUnlock = new AtomicReference<>();
        new MockUp<Locker>() {
            @Mock
            public void unLockTableWithIntensiveDbLock(Invocation invocation,
                                                       Long lockDbId, Long lockTableId, LockType lockType) {
                if (Thread.currentThread() == createThread.get()
                        && lockTableId != null && lockTableId == tableId && lockType == LockType.READ) {
                    TableBookmarkTracker tracker = mgr.tracker();
                    Bookmark inFlight = tracker == null ? null : tracker.peekCreating();
                    slotVersionAtUnlock.compareAndSet(null, inFlight == null
                            ? Optional.empty()
                            : inFlight.getPhysicalPartitionVersion(lp1, pp1));
                }
                invocation.proceed();
            }
        };

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<Bookmark> f = pool.submit(() -> {
                createThread.set(Thread.currentThread());
                return mgr.create(dbId, tableId, BookmarkHolder.forEmptyInfo("unlock_h1"));
            });
            TestTracker spy = waitForTracker(mgr);
            spy.arrival.acquire();
            // create() is parked just past its unlock; the sensor already recorded what the
            // in-flight slot looked like at that moment.
            assertEquals(Optional.of(expectedVersion), slotVersionAtUnlock.get());
            spy.release.release();
            f.get(60, TimeUnit.SECONDS);
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

    @Test
    public void testListAllBookmarks() {
        TableBookmarkTracker tracker = new TableBookmarkTracker(1L, 2L, new BookmarkMetrics());

        // Seed two bookmarks via replay path: avoids needing a real OlapTable.
        // Lower bookmarkId has 1 reference, higher bookmarkId has 2 references.
        Map<Long, Map<Long, PhysicalPartitionMeta>> emptyMeta = Collections.emptyMap();
        Bookmark b1 = new Bookmark(1L, 2L, 100L, 1_000L, emptyMeta);
        Bookmark b2 = new Bookmark(1L, 2L, 200L, 2_000L, emptyMeta);

        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("snap_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("snap_h2");
        BookmarkHolder h3 = BookmarkHolder.forEmptyInfo("snap_h3");

        tracker.replayLogEntry(BookmarkLogEntry.AddBookmark.of(b1, h1, 1_100L, -1L));

        Map<HolderId, Reference> b2Initial = new HashMap<>();
        b2Initial.put(h2.getHolderId(), new Reference(2_100L, h2.getHolderInfo(), -1L));
        b2Initial.put(h3.getHolderId(), new Reference(2_200L, h3.getHolderInfo(), -1L));
        tracker.replayLogEntry(new BookmarkLogEntry.AddBookmark(b2, b2Initial));

        List<Bookmark.View> views = tracker.listAllBookmarks();

        assertEquals(2, views.size());
        // Ascending bookmarkId order.
        assertTrue(views.get(0).getBookmark().getBookmarkId()
                < views.get(1).getBookmark().getBookmarkId());
        assertEquals(100L, views.get(0).getBookmark().getBookmarkId());
        assertEquals(200L, views.get(1).getBookmark().getBookmarkId());

        // dbId / tableId propagated from the tracker via the underlying Bookmark.
        for (Bookmark.View s : views) {
            assertEquals(1L, s.getBookmark().getDbId());
            assertEquals(2L, s.getBookmark().getTableId());
            assertNotNull(s.getReferences());
        }

        // Reference counts match what we seeded.
        assertEquals(1, views.get(0).getReferences().size());
        assertEquals(2, views.get(1).getReferences().size());

        // Holder ids and acquired-at timestamps survive the read.
        Reference.View r0 = views.get(0).getReferences().get(0);
        assertEquals(h1.getHolderId().getId(), r0.getHolderId());
        assertEquals(1_100L, r0.getAcquiredAtMs());

        Set<String> holderIds = new HashSet<>();
        Set<Long> acquiredAts = new HashSet<>();
        for (Reference.View r : views.get(1).getReferences()) {
            holderIds.add(r.getHolderId());
            acquiredAts.add(r.getAcquiredAtMs());
        }
        assertTrue(holderIds.contains(h2.getHolderId().getId()));
        assertTrue(holderIds.contains(h3.getHolderId().getId()));
        assertTrue(acquiredAts.contains(2_100L));
        assertTrue(acquiredAts.contains(2_200L));
    }

    @Test
    public void testFillStatsAges() {
        TableBookmarkTracker tracker = new TableBookmarkTracker(1L, 2L, new BookmarkMetrics());

        // No bookmarks yet — ages absent.
        BookmarkActiveStats.Builder emptyBuilder = BookmarkActiveStats.newBuilder();
        tracker.fillStats(emptyBuilder, System.currentTimeMillis(), -1L);
        BookmarkActiveStats empty = emptyBuilder.build();
        assertFalse(empty.maxBookmarkAgeMs().isPresent());
        assertFalse(empty.maxReferenceAgeMs().isPresent());

        // Seed two bookmarks via the replay path so the tracker doesn't need a real OlapTable.
        Map<Long, Map<Long, PhysicalPartitionMeta>> partsA = new HashMap<>();
        partsA.put(10L, Collections.singletonMap(11L, new PhysicalPartitionMeta(1L, 1L, 1L, 0L)));
        Map<Long, Map<Long, PhysicalPartitionMeta>> partsB = new HashMap<>();
        partsB.put(20L, Collections.singletonMap(21L, new PhysicalPartitionMeta(2L, 2L, 1L, 0L)));
        partsB.put(22L, Collections.singletonMap(23L, new PhysicalPartitionMeta(2L, 2L, 1L, 0L)));

        Bookmark b1 = new Bookmark(1L, 2L, 100L, 1_000L, partsA);
        Bookmark b2 = new Bookmark(1L, 2L, 200L, 2_000L, partsB);

        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("stats_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("stats_h2");
        BookmarkHolder h3 = BookmarkHolder.forEmptyInfo("stats_h3");

        tracker.replayLogEntry(BookmarkLogEntry.AddBookmark.of(b1, h1, 1_100L, -1L));
        Map<HolderId, Reference> b2Initial = new HashMap<>();
        b2Initial.put(h2.getHolderId(), new Reference(2_100L, h2.getHolderInfo(), -1L));
        b2Initial.put(h3.getHolderId(), new Reference(2_200L, h3.getHolderInfo(), -1L));
        tracker.replayLogEntry(new BookmarkLogEntry.AddBookmark(b2, b2Initial));

        BookmarkActiveStats.Builder builder = BookmarkActiveStats.newBuilder();
        tracker.fillStats(builder, System.currentTimeMillis(), -1L);
        BookmarkActiveStats stats = builder.build();
        // Ages are computed against System.currentTimeMillis(); the older
        // bookmark / reference yields the larger age.
        assertTrue(stats.maxBookmarkAgeMs().isPresent());
        assertTrue(stats.maxReferenceAgeMs().isPresent());
        assertTrue(stats.maxBookmarkAgeMs().getAsLong() > 0L);
        assertTrue(stats.maxReferenceAgeMs().getAsLong() > 0L);
    }

    /**
     * bookmark_renew can only build an EmptyInfo holder, so the sidecar has to come from the
     * reference being renewed -- otherwise renewing an MV's reference durably erases its MvId.
     */
    @Test
    public void testRenewKeepsTheHolderSidecar() throws Exception {
        long tableId = createDefaultTable();
        TableBookmarkTracker tracker = new TableBookmarkTracker(dbId, tableId, new BookmarkMetrics());
        BookmarkHolder mvHolder = BookmarkHolder.forMv(new MvId(dbId, 987654L));

        long bid = tracker.create(mvHolder, 600000L).getBookmarkId();
        tracker.renewReference(bid, BookmarkHolder.forEmptyInfo(mvHolder.getHolderId().getId()), 600000L);

        Reference stored = tracker.referencesByBookmark().get(bid).get(mvHolder.getHolderId());
        assertTrue(stored.getHolderInfo() instanceof HolderInfo.MvInfo,
                "renewal must not replace the MV sidecar with EmptyInfo");
    }

    /** A rejected live renewal must not change the reference metric. */
    @Test
    public void testRenewMissingReferenceDoesNotChangeMetric() throws Exception {
        long tableId = createDefaultTable();
        BookmarkMetrics metrics = new BookmarkMetrics();
        TableBookmarkTracker tracker = new TableBookmarkTracker(dbId, tableId, metrics);
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("trk_rn_count_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("trk_rn_count_h2");

        long bid = tracker.create(h1, 600000L).getBookmarkId();
        long afterCreate = metrics.bookmarkReferenceCount.sum();

        tracker.renewReference(bid, h1, 600000L);
        assertEquals(afterCreate, metrics.bookmarkReferenceCount.sum(), "the replace leg adds nothing");
        assertEquals(1L, tracker.referencesByBookmark().get(bid).get(h1.getHolderId()).getRenewCount());

        assertThrows(ReferenceNotFoundException.class, () -> tracker.renewReference(bid, h2, 600000L));
        assertEquals(afterCreate, metrics.bookmarkReferenceCount.sum());
    }

    @Test
    public void testFindAndReleaseExpiredReferences() throws Exception {
        long tableId = createDefaultTable();
        TableBookmarkTracker tracker = new TableBookmarkTracker(dbId, tableId, new BookmarkMetrics());

        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("trk_expired_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("trk_expired_h2");

        // h1 holds the bookmark with a 100ms TTL; h2 acquires it with no TTL.
        Bookmark b = tracker.create(h1, 100L);
        long bid = b.getBookmarkId();
        long acq = b.getBookmarkTimeMs();
        tracker.acquireReference(bid, h2);

        // Not yet expired: no candidate, nothing released.
        assertTrue(expiredBookmarkIds(tracker, acq + 50, -1L).isEmpty());
        assertEquals(0, tracker.releaseExpiredReferences(bid, acq + 50, -1L));
        assertEquals(2, tracker.referenceCount(bid));

        // Past h1's TTL: bookmark is a candidate; release drops only h1, keeps h2.
        assertEquals(Set.of(bid), expiredBookmarkIds(tracker, acq + 1_000L, -1L));
        assertEquals(1, tracker.releaseExpiredReferences(bid, acq + 1_000L, -1L));
        assertEquals(1, tracker.referenceCount(bid));

        // Releasing a bookmark that no longer has an expired reference is a no-op.
        assertEquals(0, tracker.releaseExpiredReferences(bid, acq + 1_000L, -1L));

        // Global ceiling forces h2 (no own TTL) to expire; bookmark reclaimed.
        assertEquals(Set.of(bid), expiredBookmarkIds(tracker, acq + 1_000L, 50L));
        assertEquals(1, tracker.releaseExpiredReferences(bid, acq + 1_000L, 50L));
        assertTrue(tracker.findByBookmarkId(bid).isEmpty());

        // Releasing a gone bookmark is a no-op (returns 0, no NPE).
        assertEquals(0, tracker.releaseExpiredReferences(bid, acq + 1_000L, -1L));
    }

    // Bookmark ids the tracker reports as holding an expired reference, read back
    // from the active-stats snapshot that fillStats now populates.
    private static Set<Long> expiredBookmarkIds(TableBookmarkTracker tracker, long nowMs, long maxTtlMs) {
        BookmarkActiveStats.Builder builder = BookmarkActiveStats.newBuilder();
        tracker.fillStats(builder, nowMs, maxTtlMs);
        return builder.build().bookmarksWithExpiredReferences()
                .getOrDefault(tracker.getDbId(), Map.of())
                .getOrDefault(tracker.getTableId(), Set.of());
    }
}
