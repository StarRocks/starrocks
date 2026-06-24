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

import com.codahale.metrics.MetricRegistry;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkManagerTest extends BookmarkTestBase {

    private BookmarkManager manager() {
        return GlobalStateMgr.getCurrentState().getBookmarkManager();
    }

    /* ---------- Lifecycle ---------- */

    @Test
    public void testCreate() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("create_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("create_h2");

        // 1. First create — bookmark added with one reference.
        Bookmark b1 = mgr.create(dbId, tableId, h1);
        assertNotNull(b1);
        assertTrue(mgr.findBookmarkById(dbId, tableId, b1.getBookmarkId()).isPresent());
        assertEquals(1, mgr.activeBookmarkCount(dbId, tableId));
        assertEquals(1, mgr.referenceCount(dbId, tableId, b1.getBookmarkId()));

        // 2. Same holder, unchanged state → AlreadyAtLatest carrying the first id.
        AlreadyAtLatestException atLatest = assertThrows(AlreadyAtLatestException.class,
                () -> mgr.create(dbId, tableId, h1));
        assertEquals(b1.getBookmarkId(), atLatest.getBookmarkId());

        // 3. Different holder, unchanged state → reuses bookmark, refCount becomes 2.
        Bookmark reused = mgr.create(dbId, tableId, h2);
        assertEquals(b1.getBookmarkId(), reused.getBookmarkId());
        assertEquals(2, mgr.referenceCount(dbId, tableId, b1.getBookmarkId()));

        // 4. ALTER ADD PARTITION, then create → new bookmarkId, two active bookmarks.
        addPartition(tableId, "p3", "2024-04-01");
        Bookmark b2 = mgr.create(dbId, tableId, h1);
        assertNotEquals(b1.getBookmarkId(), b2.getBookmarkId());
        assertTrue(b2.getBookmarkId() > b1.getBookmarkId());
        assertEquals(2, mgr.activeBookmarkCount(dbId, tableId));
        assertTrue(mgr.findBookmarkById(dbId, tableId, b1.getBookmarkId()).isPresent());
        assertTrue(mgr.findBookmarkById(dbId, tableId, b2.getBookmarkId()).isPresent());
    }

    @Test
    public void testAcquireRelease() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("ar_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("ar_h2");

        Bookmark b = mgr.create(dbId, tableId, h1);
        long bid = b.getBookmarkId();

        // acquireReference for second holder — refCount 1 → 2.
        Bookmark again = mgr.acquireReference(dbId, tableId, bid, h2);
        assertEquals(bid, again.getBookmarkId());
        assertEquals(2, mgr.referenceCount(dbId, tableId, bid));

        // Release one — bookmark still active, refCount = 1.
        mgr.releaseReference(dbId, tableId, bid, h2.getHolderId());
        assertEquals(1, mgr.referenceCount(dbId, tableId, bid));
        assertTrue(mgr.findBookmarkById(dbId, tableId, bid).isPresent());

        // Release last — tracker reclaimed.
        mgr.releaseReference(dbId, tableId, bid, h1.getHolderId());
        assertFalse(mgr.findBookmarkById(dbId, tableId, bid).isPresent());
    }

    @Test
    public void testAcquireFailure() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("af_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("af_h2");

        Bookmark b = mgr.create(dbId, tableId, h1);
        long bid = b.getBookmarkId();

        // Same holder twice on the same bookmark.
        assertThrows(AlreadyReferencedException.class,
                () -> mgr.acquireReference(dbId, tableId, bid, h1));

        // Unknown bookmarkId on a tracked table.
        assertThrows(BookmarkNotFoundException.class,
                () -> mgr.acquireReference(dbId, tableId, bid + 9999, h2));

        // No tracker for table.
        long otherTableId = createDefaultTable();
        assertThrows(BookmarkNotFoundException.class,
                () -> mgr.acquireReference(dbId, otherTableId, bid, h2));
    }

    @Test
    public void testReleaseFailure() throws Exception {
        long emptyTableId = createDefaultTable();
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("rf_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("rf_h2");

        // No tracker.
        assertThrows(BookmarkNotFoundException.class,
                () -> mgr.releaseReference(dbId, emptyTableId, 999L, h1.getHolderId()));

        Bookmark b = mgr.create(dbId, tableId, h1);
        long bid = b.getBookmarkId();

        // Wrong bookmarkId.
        assertThrows(BookmarkNotFoundException.class,
                () -> mgr.releaseReference(dbId, tableId, bid + 9999, h1.getHolderId()));

        // Holder never held this bookmark.
        assertThrows(ReferenceNotFoundException.class,
                () -> mgr.releaseReference(dbId, tableId, bid, h2.getHolderId()));
    }

    /* ---------- Lookups ---------- */

    @Test
    public void testLookup() throws Exception {
        long tableId = createDefaultTable();
        long noTrackerTableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("lk_h1");

        Bookmark b1 = mgr.create(dbId, tableId, h1);
        long t1 = b1.getBookmarkTimeMs();
        // Sleep so System.currentTimeMillis() advances and bookmarkTimeMs values are distinct.
        Thread.sleep(2);
        addPartition(tableId, "p3", "2024-04-01");
        Bookmark b2 = mgr.create(dbId, tableId, h1);
        long t2 = b2.getBookmarkTimeMs();
        // Sleep so System.currentTimeMillis() advances and bookmarkTimeMs values are distinct.
        Thread.sleep(2);
        addPartition(tableId, "p4", "2024-05-01");
        Bookmark b3 = mgr.create(dbId, tableId, h1);
        long t3 = b3.getBookmarkTimeMs();

        // findBookmarkById
        assertTrue(mgr.findBookmarkById(dbId, tableId, b2.getBookmarkId()).isPresent());
        assertFalse(mgr.findBookmarkById(dbId, tableId, b3.getBookmarkId() + 9999).isPresent());
        assertFalse(mgr.findBookmarkById(dbId, noTrackerTableId, b1.getBookmarkId()).isPresent());

        // findByTimestamp — before earliest, exact, between, after newest.
        assertFalse(mgr.findByTimestamp(dbId, tableId, t1 - 1).isPresent());
        assertEquals(b2.getBookmarkId(),
                mgr.findByTimestamp(dbId, tableId, t2).get().getBookmarkId());
        assertEquals(b2.getBookmarkId(),
                mgr.findByTimestamp(dbId, tableId, (t2 + t3) / 2).get().getBookmarkId());
        assertEquals(b3.getBookmarkId(),
                mgr.findByTimestamp(dbId, tableId, t3 + 1000).get().getBookmarkId());
    }

    @Test
    public void testGetPhysicalPartitionFenceVersion() throws Exception {
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("gf_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("gf_h2");

        // No tracker → empty for any partition pair.
        long emptyTableId = createDefaultTable();
        assertFalse(mgr.getPhysicalPartitionFenceVersion(dbId, emptyTableId, 1L, 2L).isPresent());

        long tableId = createDefaultTable();
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(tableId);
        Partition p1 = table.getPartition("p1");
        long lp1 = p1.getId();
        long pp1 = p1.getSubPartitions().iterator().next().getId();

        // Bookmark A captures p1@v=1.
        mgr.create(dbId, tableId, h1);

        // Bump p1 visibleVersion to 5 and add a new partition p3.
        p1.getSubPartitions().iterator().next().setVisibleVersion(5L, System.currentTimeMillis());
        addPartition(tableId, "p3", "2024-04-01");
        Partition p3 = table.getPartition("p3");
        long lp3 = p3.getId();
        long pp3 = p3.getSubPartitions().iterator().next().getId();

        // Bookmark B captures p1@v=5 plus p3@v=1.
        mgr.create(dbId, tableId, h2);

        // p1 in both bookmarks → take the oldest version (1 from A).
        assertEquals(1L, mgr.getPhysicalPartitionFenceVersion(dbId, tableId, lp1, pp1).get());

        // p3 only in B (added after A) — must still be protected, returning B's version (1).
        assertEquals(1L, mgr.getPhysicalPartitionFenceVersion(dbId, tableId, lp3, pp3).get());

        // Unknown partition pair → empty.
        assertFalse(mgr.getPhysicalPartitionFenceVersion(dbId, tableId, 999_999L, 999_999L).isPresent());
    }

    /* ---------- Holder lookups / bulk release ---------- */

    @Test
    public void testListBookmarkIdsByHolder() throws Exception {
        long tableId = createDefaultTable();
        long otherTableId = createDefaultTable();
        long noTrackerTableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("list_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("list_h2");

        // 1. No tracker → empty.
        assertTrue(mgr.listBookmarkIdsByHolder(dbId, noTrackerTableId, h1.getHolderId()).isEmpty());

        // 2. h1 holds one bookmark on `tableId`.
        Bookmark b1 = mgr.create(dbId, tableId, h1);
        assertEquals(List.of(b1.getBookmarkId()),
                mgr.listBookmarkIdsByHolder(dbId, tableId, h1.getHolderId()));

        // 3. h2 holds nothing on `tableId` even though the tracker exists.
        assertTrue(mgr.listBookmarkIdsByHolder(dbId, tableId, h2.getHolderId()).isEmpty());

        // 4. ALTER ADD PARTITION then h1 creates a second bookmark — ascending order.
        addPartition(tableId, "p3", "2024-04-01");
        Bookmark b2 = mgr.create(dbId, tableId, h1);
        assertEquals(List.of(b1.getBookmarkId(), b2.getBookmarkId()),
                mgr.listBookmarkIdsByHolder(dbId, tableId, h1.getHolderId()));

        // 5. h2 attaches to b2 only — list returns only the matched id, not all active ids.
        mgr.acquireReference(dbId, tableId, b2.getBookmarkId(), h2);
        assertEquals(List.of(b2.getBookmarkId()),
                mgr.listBookmarkIdsByHolder(dbId, tableId, h2.getHolderId()));

        // 6. Listing is scoped to (dbId, tableId): h1 on a different table is empty.
        assertTrue(mgr.listBookmarkIdsByHolder(dbId, otherTableId, h1.getHolderId()).isEmpty());
    }

    @Test
    public void testReleaseAllForHolder() throws Exception {
        long tableA = createDefaultTable();
        long tableB = createDefaultTable();
        long noTrackerTableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("rall_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("rall_h2");

        // h1 holds two bookmarks on tableA (one shared with h2) plus one on tableB.
        Bookmark bA1 = mgr.create(dbId, tableA, h1);
        mgr.acquireReference(dbId, tableA, bA1.getBookmarkId(), h2);
        addPartition(tableA, "p3", "2024-04-01");
        Bookmark bA2 = mgr.create(dbId, tableA, h1);
        Bookmark bB = mgr.create(dbId, tableB, h1);
        assertEquals(2, mgr.referenceCount(dbId, tableA, bA1.getBookmarkId()));
        assertEquals(1, mgr.referenceCount(dbId, tableA, bA2.getBookmarkId()));
        assertEquals(1, mgr.referenceCount(dbId, tableB, bB.getBookmarkId()));

        // 1. Release h1 on tableA only — leaves bA1 alive (h2 still holds it), drops bA2,
        // and does NOT touch tableB.
        mgr.releaseAllForHolder(dbId, tableA, h1.getHolderId());
        assertEquals(1, mgr.referenceCount(dbId, tableA, bA1.getBookmarkId()));
        assertTrue(mgr.findBookmarkById(dbId, tableA, bA1.getBookmarkId()).isPresent());
        assertFalse(mgr.findBookmarkById(dbId, tableA, bA2.getBookmarkId()).isPresent());
        assertTrue(mgr.listBookmarkIdsByHolder(dbId, tableA, h1.getHolderId()).isEmpty());
        assertEquals(1, mgr.referenceCount(dbId, tableB, bB.getBookmarkId()));

        // 2. Idempotent: second call on the same scope is a silent no-op and doesn't touch h2.
        mgr.releaseAllForHolder(dbId, tableA, h1.getHolderId());
        assertEquals(1, mgr.referenceCount(dbId, tableA, bA1.getBookmarkId()));

        // 3. Releasing h1 on tableB drops bB (its only reference).
        mgr.releaseAllForHolder(dbId, tableB, h1.getHolderId());
        assertFalse(mgr.findBookmarkById(dbId, tableB, bB.getBookmarkId()).isPresent());

        // 4. Releasing h2 reclaims the last reference on bA1.
        mgr.releaseAllForHolder(dbId, tableA, h2.getHolderId());
        assertFalse(mgr.findBookmarkById(dbId, tableA, bA1.getBookmarkId()).isPresent());

        // 5. No tracker for table → no-op.
        mgr.releaseAllForHolder(dbId, noTrackerTableId, h1.getHolderId());

        // 6. Holder owns nothing on the (already-empty) table → no-op.
        mgr.releaseAllForHolder(dbId, tableA, BookmarkHolder.forEmptyInfo("rall_unknown").getHolderId());
    }

    /* ---------- Replay ---------- */

    @Test
    public void testReplay() {
        BookmarkManager mgr = new BookmarkManager();
        long fakeDbId = 999_001L;
        long fakeTableId = 999_101L;
        long fakeBookmarkId = 999_201L;
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("rp_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("rp_h2");

        // 1. Null entry → ignored.
        mgr.replay(null);
        assertEquals(0, mgr.activeBookmarkCount(fakeDbId, fakeTableId));

        // 2. Unknown subtype → ignored.
        BookmarkLogEntry unknown = new BookmarkLogEntry(fakeDbId, fakeTableId) { };
        mgr.replay(unknown);
        assertEquals(0, mgr.activeBookmarkCount(fakeDbId, fakeTableId));

        // 3. AddBookmark with empty initialReferences → IllegalStateException.
        Map<Long, Map<Long, PhysicalPartitionMeta>> emptyParts = new HashMap<>();
        Bookmark fakeBookmark = new Bookmark(fakeDbId, fakeTableId, fakeBookmarkId, 1000L, emptyParts);
        BookmarkLogEntry.AddBookmark badAdd = new BookmarkLogEntry.AddBookmark(
                fakeBookmark, new HashMap<>());
        assertThrows(IllegalStateException.class, () -> mgr.replay(badAdd));

        // 4. Valid AddBookmark — creates tracker + bookmark.
        BookmarkLogEntry.AddBookmark goodAdd = BookmarkLogEntry.AddBookmark.of(fakeBookmark, h1, 1000L, -1L);
        mgr.replay(goodAdd);
        assertTrue(mgr.findBookmarkById(fakeDbId, fakeTableId, fakeBookmarkId).isPresent());
        assertEquals(1, mgr.referenceCount(fakeDbId, fakeTableId, fakeBookmarkId));

        // 5. Idempotent replay — refCount unchanged.
        mgr.replay(goodAdd);
        assertEquals(1, mgr.referenceCount(fakeDbId, fakeTableId, fakeBookmarkId));

        // 6. AcquireReference — adds a reference (refCount 1 → 2).
        BookmarkLogEntry.AcquireReference acq = BookmarkLogEntry.AcquireReference.of(
                fakeDbId, fakeTableId, fakeBookmarkId, h2, 2000L, -1L);
        mgr.replay(acq);
        assertEquals(2, mgr.referenceCount(fakeDbId, fakeTableId, fakeBookmarkId));

        // 7. ReleaseReference for non-last — bookmark still active.
        Reference ref1 = new Reference(1000L, HolderInfo.EmptyInfo.INSTANCE, -1L);
        BookmarkLogEntry.ReleaseReference rel1 = BookmarkLogEntry.ReleaseReference.of(
                fakeDbId, fakeTableId, fakeBookmarkId, h1.getHolderId(), ref1);
        mgr.replay(rel1);
        assertEquals(1, mgr.referenceCount(fakeDbId, fakeTableId, fakeBookmarkId));

        // 8. Final ReleaseReference — tracker reclaimed.
        Reference ref2 = new Reference(2000L, HolderInfo.EmptyInfo.INSTANCE, -1L);
        BookmarkLogEntry.ReleaseReference rel2 = BookmarkLogEntry.ReleaseReference.of(
                fakeDbId, fakeTableId, fakeBookmarkId, h2.getHolderId(), ref2);
        mgr.replay(rel2);
        assertFalse(mgr.findBookmarkById(fakeDbId, fakeTableId, fakeBookmarkId).isPresent());

        // 9. AcquireReference on now-released bookmark — silently ignored, no resurrection.
        mgr.replay(acq);
        assertFalse(mgr.findBookmarkById(fakeDbId, fakeTableId, fakeBookmarkId).isPresent());

        // 10. ReleaseReference on missing tracker — silently ignored, no NPE.
        mgr.replay(rel1);
    }

    /* ---------- Cluster-wide listing ---------- */

    @Test
    public void testListAllBookmarksClusterWide() {
        // Seed two trackers via the replay path: avoids needing real OlapTables.
        // (db=1, table=2) holds one bookmark, (db=1, table=3) holds one bookmark.
        BookmarkManager mgr = new BookmarkManager();
        Map<Long, Map<Long, PhysicalPartitionMeta>> emptyMeta = Collections.emptyMap();
        Bookmark b1 = new Bookmark(1L, 2L, 100L, 1_000L, emptyMeta);
        Bookmark b2 = new Bookmark(1L, 3L, 200L, 2_000L, emptyMeta);
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("snap_cluster_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("snap_cluster_h2");

        mgr.replay(BookmarkLogEntry.AddBookmark.of(b1, h1, 1_100L, -1L));
        mgr.replay(BookmarkLogEntry.AddBookmark.of(b2, h2, 2_100L, -1L));

        // No filter returns every tracker.
        List<Bookmark.View> views = mgr.listAllBookmarks(
                Optional.empty(), Optional.empty(), Optional.empty());
        assertEquals(2, views.size());
        Set<Long> tableIds = new HashSet<>();
        for (Bookmark.View s : views) {
            tableIds.add(s.getBookmark().getTableId());
        }
        assertEquals(Set.of(2L, 3L), tableIds);

        // Filter to a single tableId returns just that tracker's bookmarks.
        List<Bookmark.View> filtered = mgr.listAllBookmarks(
                Optional.empty(), Optional.of(2L), Optional.empty());
        assertEquals(1, filtered.size());
        assertEquals(2L, filtered.get(0).getBookmark().getTableId());

        // Filter by dbId.
        List<Bookmark.View> dbFiltered = mgr.listAllBookmarks(
                Optional.of(1L), Optional.empty(), Optional.empty());
        assertEquals(2, dbFiltered.size());

        // Filter by bookmarkId.
        List<Bookmark.View> bmFiltered = mgr.listAllBookmarks(
                Optional.empty(), Optional.empty(), Optional.of(100L));
        assertEquals(1, bmFiltered.size());
        assertEquals(100L, bmFiltered.get(0).getBookmark().getBookmarkId());
    }

    /* ---------- Active stats / metrics ---------- */

    @Test
    public void testGetActiveStats() {
        // Two trackers seeded via replay so the test doesn't need real OlapTables.
        // (db=1, table=2) holds bookmark@100 with one reference @ 1_100.
        // (db=1, table=3) holds bookmark@200 with two references @ 2_100 and 2_200.
        BookmarkManager mgr = new BookmarkManager();
        Map<Long, Map<Long, PhysicalPartitionMeta>> partsA = new HashMap<>();
        partsA.put(10L, Collections.singletonMap(11L, new PhysicalPartitionMeta(1L, 1L, 1L, 0L)));
        Map<Long, Map<Long, PhysicalPartitionMeta>> partsB = new HashMap<>();
        partsB.put(20L, Collections.singletonMap(21L, new PhysicalPartitionMeta(2L, 2L, 1L, 0L)));
        partsB.put(22L, Collections.singletonMap(23L, new PhysicalPartitionMeta(2L, 2L, 1L, 0L)));

        Bookmark bA = new Bookmark(1L, 2L, 100L, 1_000L, partsA);
        Bookmark bB = new Bookmark(1L, 3L, 200L, 2_000L, partsB);

        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("stats_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("stats_h2");
        BookmarkHolder h3 = BookmarkHolder.forEmptyInfo("stats_h3");

        mgr.replay(BookmarkLogEntry.AddBookmark.of(bA, h1, 1_100L, -1L));

        Map<HolderId, Reference> bInitial = new HashMap<>();
        bInitial.put(h2.getHolderId(), new Reference(2_100L, h2.getHolderInfo(), -1L));
        bInitial.put(h3.getHolderId(), new Reference(2_200L, h3.getHolderInfo(), -1L));
        mgr.replay(new BookmarkLogEntry.AddBookmark(bB, bInitial));

        BookmarkActiveStats stats = mgr.getActiveStats(System.currentTimeMillis(), -1L);
        assertTrue(stats.maxBookmarkAgeMs().isPresent());
        assertTrue(stats.maxReferenceAgeMs().isPresent());
        // Ages are computed against System.currentTimeMillis(); just assert
        // the older bookmark / reference produces the larger age value.
        assertTrue(stats.maxBookmarkAgeMs().getAsLong() > 0L);
        assertTrue(stats.maxReferenceAgeMs().getAsLong() > 0L);

        // An empty manager produces empty stats — no trackers walked.
        BookmarkActiveStats noTrackers = new BookmarkManager().getActiveStats(System.currentTimeMillis(), -1L);
        assertFalse(noTrackers.maxBookmarkAgeMs().isPresent());
        assertFalse(noTrackers.maxReferenceAgeMs().isPresent());
    }

    @Test
    public void testRegisterMetrics() {
        BookmarkManager mgr = new BookmarkManager();
        MetricRegistry coda = new MetricRegistry();
        // Should not throw, even when invoked twice on the same manager —
        // histograms are get-or-create on the registry, counters/gauges are
        // appended to the global MetricRepo.
        mgr.registerMetrics(coda);
        mgr.registerMetrics(coda);
    }

    @Test
    public void testReplayBumpsAllMetrics() {
        // Replay path bumps both cardinality and cumulative metrics — a
        // follower promoted to leader needs *_total to reflect the work it
        // applied via replay, otherwise its rate() would start from 0 and
        // hide everything that happened before the promotion.
        BookmarkManager mgr = new BookmarkManager();
        long fakeDbId = 998_001L;
        long fakeTableId = 998_101L;
        long fakeBookmarkIdA = 998_201L;
        long fakeBookmarkIdB = 998_301L;
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("repcc_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("repcc_h2");

        Map<Long, Map<Long, PhysicalPartitionMeta>> partsA = new HashMap<>();
        partsA.put(10L, Collections.singletonMap(11L, new PhysicalPartitionMeta(1L, 1L, 1L, 0L)));
        Map<Long, Map<Long, PhysicalPartitionMeta>> partsB = new HashMap<>();
        partsB.put(20L, Collections.singletonMap(21L, new PhysicalPartitionMeta(2L, 2L, 1L, 0L)));
        partsB.put(22L, Collections.singletonMap(23L, new PhysicalPartitionMeta(2L, 2L, 1L, 0L)));

        Bookmark bA = new Bookmark(fakeDbId, fakeTableId, fakeBookmarkIdA, 1_000L, partsA);
        Bookmark bB = new Bookmark(fakeDbId, fakeTableId, fakeBookmarkIdB, 2_000L, partsB);

        // AddBookmark for A — 1 bookmark, 1 ref, 1 logical / 1 physical partition.
        mgr.replay(BookmarkLogEntry.AddBookmark.of(bA, h1, 1_100L, -1L));
        assertEquals(1L, mgr.metrics().bookmarkCount.longValue());
        assertEquals(1L, mgr.metrics().bookmarkReferenceCount.longValue());
        assertEquals(1L, mgr.metrics().bookmarkLogicalPartitionCount.longValue());
        assertEquals(1L, mgr.metrics().bookmarkPhysicalPartitionCount.longValue());
        assertEquals(1L, mgr.metrics().bookmarkCreatedTotal.longValue());
        assertEquals(1L, mgr.metrics().bookmarkReferenceAddedTotal.longValue());

        // AddBookmark for B with 2 initial refs and 2 logical / 2 physical partitions.
        Map<HolderId, Reference> bInitial = new HashMap<>();
        bInitial.put(h1.getHolderId(), new Reference(2_100L, h1.getHolderInfo(), -1L));
        bInitial.put(h2.getHolderId(), new Reference(2_200L, h2.getHolderInfo(), -1L));
        mgr.replay(new BookmarkLogEntry.AddBookmark(bB, bInitial));
        assertEquals(2L, mgr.metrics().bookmarkCount.longValue());
        assertEquals(3L, mgr.metrics().bookmarkReferenceCount.longValue());
        assertEquals(3L, mgr.metrics().bookmarkLogicalPartitionCount.longValue());
        assertEquals(3L, mgr.metrics().bookmarkPhysicalPartitionCount.longValue());
        assertEquals(2L, mgr.metrics().bookmarkCreatedTotal.longValue());
        assertEquals(3L, mgr.metrics().bookmarkReferenceAddedTotal.longValue());

        // AcquireReference for h2 on A — bookmarkReferenceCount bumps to 4.
        mgr.replay(BookmarkLogEntry.AcquireReference.of(
                fakeDbId, fakeTableId, fakeBookmarkIdA, h2, 1_500L, -1L));
        assertEquals(4L, mgr.metrics().bookmarkReferenceCount.longValue());
        assertEquals(4L, mgr.metrics().bookmarkReferenceAddedTotal.longValue());

        // Idempotent AcquireReference (h2 already there) — no double-bump.
        mgr.replay(BookmarkLogEntry.AcquireReference.of(
                fakeDbId, fakeTableId, fakeBookmarkIdA, h2, 1_500L, -1L));
        assertEquals(4L, mgr.metrics().bookmarkReferenceCount.longValue());
        assertEquals(4L, mgr.metrics().bookmarkReferenceAddedTotal.longValue());

        // ReleaseReference removes the holder; bookmark stays because h1
        // still references it.
        Reference relRefAh2 = new Reference(1_500L, h2.getHolderInfo(), -1L);
        mgr.replay(BookmarkLogEntry.ReleaseReference.of(
                fakeDbId, fakeTableId, fakeBookmarkIdA, h2.getHolderId(), relRefAh2));
        assertEquals(3L, mgr.metrics().bookmarkReferenceCount.longValue());
        assertEquals(1L, mgr.metrics().bookmarkReferenceReleasedTotal.longValue());

        // Idempotent ReleaseReference for same holder — no double-decrement.
        mgr.replay(BookmarkLogEntry.ReleaseReference.of(
                fakeDbId, fakeTableId, fakeBookmarkIdA, h2.getHolderId(), relRefAh2));
        assertEquals(3L, mgr.metrics().bookmarkReferenceCount.longValue());
        assertEquals(1L, mgr.metrics().bookmarkReferenceReleasedTotal.longValue());

        // Final ReleaseReference on A — last reference, bookmark is reclaimed,
        // bookmarkCount and partition counts both drop.
        Reference relRefAh1 = new Reference(1_100L, h1.getHolderInfo(), -1L);
        mgr.replay(BookmarkLogEntry.ReleaseReference.of(
                fakeDbId, fakeTableId, fakeBookmarkIdA, h1.getHolderId(), relRefAh1));
        assertEquals(1L, mgr.metrics().bookmarkCount.longValue());
        assertEquals(2L, mgr.metrics().bookmarkReferenceCount.longValue());
        assertEquals(2L, mgr.metrics().bookmarkLogicalPartitionCount.longValue());
        assertEquals(2L, mgr.metrics().bookmarkPhysicalPartitionCount.longValue());
        assertEquals(1L, mgr.metrics().bookmarkRemovedTotal.longValue());
        assertEquals(2L, mgr.metrics().bookmarkReferenceReleasedTotal.longValue());
    }

    @Test
    public void testDaemonCycleRefreshesMaxAges() throws Exception {
        // Seed a tracker via replay so the test doesn't need real OlapTables,
        // then run one daemon cycle that the metric scrape relies on.
        BookmarkManager mgr = new BookmarkManager();
        Bookmark b = new Bookmark(1L, 2L, 100L, 1_000L, Collections.emptyMap());
        BookmarkHolder h = BookmarkHolder.forEmptyInfo("age_h1");
        mgr.replay(BookmarkLogEntry.AddBookmark.of(b, h, 1_100L, -1L));

        // Before the daemon runs the cached values are the AtomicLong default (0).
        assertEquals(0L, mgr.metrics().bookmarkMaxActiveAgeMs.get());
        assertEquals(0L, mgr.metrics().bookmarkReferenceMaxActiveAgeMs.get());

        mgr.runAfterLeaseValid();

        // Bookmark was created at epoch=1_000ms, reference acquired at
        // 1_100ms; both ages are well in the past, so the cached values
        // should now be strictly positive.
        assertTrue(mgr.metrics().bookmarkMaxActiveAgeMs.get() > 0L);
        assertTrue(mgr.metrics().bookmarkReferenceMaxActiveAgeMs.get() > 0L);
    }

    @Test
    public void testImageSeedsCardinality() throws Exception {
        long tableA = createDefaultTable();
        long tableB = createDefaultTable();
        BookmarkManager src = manager();
        BookmarkHolder hA = BookmarkHolder.forEmptyInfo("iseed_a");
        BookmarkHolder hB1 = BookmarkHolder.forEmptyInfo("iseed_b1");
        BookmarkHolder hB2 = BookmarkHolder.forEmptyInfo("iseed_b2");

        // tableA: one bookmark, 1 reference. tableB: one bookmark, 2 references.
        src.create(dbId, tableA, hA);
        src.create(dbId, tableB, hB1);
        src.create(dbId, tableB, hB2);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        src.save(image.getImageWriter());

        BookmarkManager loaded = new BookmarkManager();
        // Cardinality is at zero before load — the gson path has not fired apply().
        assertEquals(0L, loaded.metrics().bookmarkCount.longValue());
        assertEquals(0L, loaded.metrics().bookmarkReferenceCount.longValue());

        SRMetaBlockReader reader = image.getMetaBlockReader();
        try {
            loaded.load(reader);
        } finally {
            reader.close();
        }

        // After load, cardinality reflects the inherited state: 2 bookmarks
        // and 3 total references (1 + 2).
        assertEquals(2L, loaded.metrics().bookmarkCount.longValue());
        assertEquals(3L, loaded.metrics().bookmarkReferenceCount.longValue());
        // Cumulative counters stayed at zero — image load is not "work done",
        // and the gson path skips apply() entirely.
        assertEquals(0L, loaded.metrics().bookmarkCreatedTotal.longValue());
        assertEquals(0L, loaded.metrics().bookmarkReferenceAddedTotal.longValue());
    }

    /* ---------- Image ---------- */

    @Test
    public void testImage() throws Exception {
        long tableA = createDefaultTable();
        long tableB = createDefaultTable();
        BookmarkManager src = manager();
        BookmarkHolder hA1 = BookmarkHolder.forEmptyInfo("img_a1");
        BookmarkHolder hA2 = BookmarkHolder.forEmptyInfo("img_a2");
        BookmarkHolder hB1 = BookmarkHolder.forEmptyInfo("img_b1");
        BookmarkHolder hB2 = BookmarkHolder.forEmptyInfo("img_b2");

        Bookmark bA = src.create(dbId, tableA, hA1);
        src.create(dbId, tableA, hA2);
        Bookmark bB = src.create(dbId, tableB, hB1);
        src.create(dbId, tableB, hB2);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        src.save(image.getImageWriter());

        BookmarkManager loaded = new BookmarkManager();
        SRMetaBlockReader reader = image.getMetaBlockReader();
        try {
            loaded.load(reader);
        } finally {
            reader.close();
        }

        // Both bookmarks present with both holders each.
        assertTrue(loaded.findBookmarkById(dbId, tableA, bA.getBookmarkId()).isPresent());
        assertTrue(loaded.findBookmarkById(dbId, tableB, bB.getBookmarkId()).isPresent());
        assertEquals(2, loaded.referenceCount(dbId, tableA, bA.getBookmarkId()));
        assertEquals(2, loaded.referenceCount(dbId, tableB, bB.getBookmarkId()));

        // A fresh create() on the loaded manager works — verifies that trackers
        // restored from the image are usable for new bookmark creation.
        addPartition(tableA, "p3", "2024-04-01");
        Bookmark fresh = loaded.create(dbId, tableA, BookmarkHolder.forEmptyInfo("img_after_load"));
        assertNotEquals(bA.getBookmarkId(), fresh.getBookmarkId());
    }

    /* ---------- TTL sweep ---------- */

    // Drives one TTL sweep the way the daemon does: collect the bookmarks with an expired
    // reference at a fixed instant, then release them.
    private static void sweep(BookmarkManager mgr, long nowMs, long maxTtlMs) {
        mgr.sweepExpiredReferences(
                mgr.getActiveStats(nowMs, maxTtlMs).bookmarksWithExpiredReferences(), nowMs, maxTtlMs);
    }

    @Test
    public void testSweepReleasesExpiredAndReclaims() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h = BookmarkHolder.forEmptyInfo("sweep_h1");

        Bookmark b = mgr.create(dbId, tableId, h, 100L);
        long bid = b.getBookmarkId();
        long acq = b.getBookmarkTimeMs();

        // Before expiry: nothing happens.
        sweep(mgr, acq + 50, -1L);
        assertEquals(1, mgr.referenceCount(dbId, tableId, bid));

        // At/after expiry: reference released, bookmark + tracker reclaimed.
        sweep(mgr, acq + 100, -1L);
        assertFalse(mgr.findBookmarkById(dbId, tableId, bid).isPresent());
        assertEquals(0, mgr.activeBookmarkCount(dbId, tableId));
    }

    @Test
    public void testSweepIgnoresDisabledTtl() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h = BookmarkHolder.forEmptyInfo("sweep_disabled");

        Bookmark b = mgr.create(dbId, tableId, h);   // no-TTL overload -> -1
        long bid = b.getBookmarkId();

        sweep(mgr, b.getBookmarkTimeMs() + 1_000_000L, -1L);
        assertTrue(mgr.findBookmarkById(dbId, tableId, bid).isPresent());
    }

    @Test
    public void testSweepGlobalCeiling() throws Exception {
        BookmarkManager mgr = manager();

        // (a) Ceiling forces expiry of a no-TTL reference.
        long t1 = createDefaultTable();
        Bookmark b1 = mgr.create(dbId, t1, BookmarkHolder.forEmptyInfo("ceil_a"), -1L);
        sweep(mgr, b1.getBookmarkTimeMs() + 100, 50L);
        assertFalse(mgr.findBookmarkById(dbId, t1, b1.getBookmarkId()).isPresent());

        // (b) Explicit stricter than ceiling: effective = min(50, 100) = 50.
        long t2 = createDefaultTable();
        Bookmark b2 = mgr.create(dbId, t2, BookmarkHolder.forEmptyInfo("ceil_b"), 50L);
        sweep(mgr, b2.getBookmarkTimeMs() + 60, 100L);
        assertFalse(mgr.findBookmarkById(dbId, t2, b2.getBookmarkId()).isPresent());

        // (c) Ceiling stricter than explicit: effective = min(100, 50) = 50.
        long t3 = createDefaultTable();
        Bookmark b3 = mgr.create(dbId, t3, BookmarkHolder.forEmptyInfo("ceil_c"), 100L);
        sweep(mgr, b3.getBookmarkTimeMs() + 60, 50L);
        assertFalse(mgr.findBookmarkById(dbId, t3, b3.getBookmarkId()).isPresent());
    }

    @Test
    public void testSweepReleasesOnlyExpiredHolders() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h1 = BookmarkHolder.forEmptyInfo("mix_h1");
        BookmarkHolder h2 = BookmarkHolder.forEmptyInfo("mix_h2");

        Bookmark b = mgr.create(dbId, tableId, h1, 100L);   // h1 expires
        long bid = b.getBookmarkId();
        mgr.acquireReference(dbId, tableId, bid, h2);        // h2 no-TTL, never expires

        sweep(mgr, b.getBookmarkTimeMs() + 1_000L, -1L);

        // Only h1 dropped; the bookmark survives on h2.
        assertEquals(1, mgr.referenceCount(dbId, tableId, bid));
        assertTrue(mgr.findBookmarkById(dbId, tableId, bid).isPresent());
        assertTrue(mgr.listBookmarkIdsByHolder(dbId, tableId, h1.getHolderId()).isEmpty());
        assertTrue(mgr.listBookmarkIdsByHolder(dbId, tableId, h2.getHolderId()).contains(bid));
    }

    @Test
    public void testSweepRechecksLiveReference() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager mgr = manager();
        BookmarkHolder h = BookmarkHolder.forEmptyInfo("recheck_h");

        Bookmark b = mgr.create(dbId, tableId, h, 100L);
        long bid = b.getBookmarkId();

        // A sweep whose nowMs precedes effective expiry leaves the reference
        // intact — the same evaluation pass 2 makes against a re-acquired ref.
        sweep(mgr, b.getBookmarkTimeMs() + 50, -1L);
        assertEquals(1, mgr.referenceCount(dbId, tableId, bid));
        assertTrue(mgr.findBookmarkById(dbId, tableId, bid).isPresent());
    }

    @Test
    public void testTtlSurvivesImage() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager src = manager();
        BookmarkHolder h = BookmarkHolder.forEmptyInfo("img_ttl");

        Bookmark b = src.create(dbId, tableId, h, 7_000L);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        src.save(image.getImageWriter());
        BookmarkManager loaded = new BookmarkManager();
        SRMetaBlockReader reader = image.getMetaBlockReader();
        try {
            loaded.load(reader);
        } finally {
            reader.close();
        }

        List<Bookmark.View> views = loaded.listAllBookmarks(
                Optional.of(dbId), Optional.of(tableId), Optional.of(b.getBookmarkId()));
        assertEquals(1, views.size());
        assertEquals(1, views.get(0).getReferences().size());
        assertEquals(7_000L, views.get(0).getReferences().get(0).getTtlMs());
    }
}
