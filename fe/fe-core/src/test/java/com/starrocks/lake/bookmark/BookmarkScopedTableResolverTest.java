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

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.lake.bookmark.BookmarkChange.DataChanged;
import com.starrocks.lake.bookmark.BookmarkChange.PartitionAdded;
import com.starrocks.lake.bookmark.BookmarkChange.PhysicalPartitionChange;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BookmarkScopedTableResolverTest extends BookmarkTestBase {

    private OlapTable table(long id) {
        OlapTable t = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(dbId).getTable(id);
        // LocalMetastore.createTable does not stamp dbId on the OlapTable; production
        // callers (QueryAnalyzer) populate it before the resolver runs, so seed it here.
        t.maySetDatabaseId(dbId);
        return t;
    }

    private Bookmark registerBookmark(OlapTable t) throws Exception {
        return GlobalStateMgr.getCurrentState().getBookmarkManager()
                .create(dbId, t.getId(), BookmarkHolder.forEmptyInfo("test_" + t.getId()));
    }

    @Test
    public void testResolveByIdNotFound() throws Exception {
        OlapTable t = table(createDefaultTable());
        SemanticException ex = assertThrows(SemanticException.class,
                () -> BookmarkScopedTableResolver.resolveById(t, 9_999_999L));
        assertTrue(ex.getMessage().contains("bookmark 9999999 not found"));
    }

    @Test
    public void testResolveByTimestampMiss() throws Exception {
        OlapTable t = table(createDefaultTable());
        SemanticException ex = assertThrows(SemanticException.class,
                () -> BookmarkScopedTableResolver.resolveByTimestamp(t, 0L));
        assertTrue(ex.getMessage().contains("no bookmark for table"));
    }

    @Test
    public void testResolveByIdSuccess() throws Exception {
        OlapTable t = table(createDefaultTable());
        Bookmark b = registerBookmark(t);

        OlapTable scoped = BookmarkScopedTableResolver.resolveById(t, b.getBookmarkId());
        assertNotSame(t, scoped);
        assertEquals(t.getId(), scoped.getId());
        assertEquals(t.getPartitions().size(), scoped.getPartitions().size());

        for (Partition p : scoped.getPartitions()) {
            for (PhysicalPartition pp : p.getSubPartitions()) {
                long bookmarkedVersion =
                        b.getPhysicalPartitionVersion(p.getId(), pp.getId()).orElseThrow();
                assertEquals(bookmarkedVersion, pp.getVisibleVersion());
            }
        }
    }

    @Test
    public void testPartitionMissing() throws Exception {
        long tid = createDefaultTable();
        OlapTable t = table(tid);
        Bookmark b = registerBookmark(t);

        Partition dropped = t.getPartitions().iterator().next();
        String droppedName = dropped.getName();
        AlterTableStmt alter = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table " + t.getName() + " drop partition " + droppedName,
                connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alter);

        SemanticException ex = assertThrows(SemanticException.class,
                () -> BookmarkScopedTableResolver.resolveById(t, b.getBookmarkId()));
        assertTrue(ex.getMessage().contains("physical partition"), ex.getMessage());
        assertTrue(ex.getMessage().contains("no longer exists"), ex.getMessage());
    }

    @Test
    public void testFastSchemaChangeIsTrackable() throws Exception {
        OlapTable t = table(createDefaultTable());
        Bookmark b = registerBookmark(t);

        AlterTableStmt alter = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(
                "alter table " + t.getName() + " add column extra bigint",
                connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alter);

        OlapTable scoped = BookmarkScopedTableResolver.resolveById(t, b.getBookmarkId());
        assertEquals(t.getPartitions().size(), scoped.getPartitions().size());
    }

    @Test
    public void testIsolationFromLive() throws Exception {
        OlapTable t = table(createDefaultTable());
        Bookmark b = registerBookmark(t);

        OlapTable scoped = BookmarkScopedTableResolver.resolveById(t, b.getBookmarkId());

        Partition firstScoped = scoped.getPartitions().iterator().next();
        long firstScopedId = firstScoped.getId();
        long liveBefore = t.getPartition(firstScopedId).getSubPartitions()
                .iterator().next().getVisibleVersion();
        firstScoped.getSubPartitions().iterator().next().setVisibleVersion(9999L, 1L);
        long liveAfter = t.getPartition(firstScopedId).getSubPartitions()
                .iterator().next().getVisibleVersion();
        assertEquals(liveBefore, liveAfter);

        assertTrue(scoped.getTempPartitions().isEmpty());
    }

    @Test
    public void testIndexReplaced() throws Exception {
        OlapTable t = table(createDefaultTable());
        Bookmark synthetic = synthesizeMismatchedBookmark(t, /* shiftMetaId = */ true);
        long bookmarkId = synthetic.getBookmarkId();
        registerSyntheticBookmark(synthetic);

        SemanticException ex = assertThrows(SemanticException.class,
                () -> BookmarkScopedTableResolver.resolveById(t, bookmarkId));
        assertTrue(ex.getMessage().contains("rewrote its data"), ex.getMessage());
    }

    @Test
    public void testTabletReshard() throws Exception {
        OlapTable t = table(createDefaultTable());
        Bookmark synthetic = synthesizeMismatchedBookmark(t, /* shiftMetaId = */ false);
        long bookmarkId = synthetic.getBookmarkId();
        registerSyntheticBookmark(synthetic);

        SemanticException ex = assertThrows(SemanticException.class,
                () -> BookmarkScopedTableResolver.resolveById(t, bookmarkId));
        assertTrue(ex.getMessage().contains("redistributed"), ex.getMessage());
    }

    /**
     * Builds a Bookmark whose first physical partition mismatches the live
     * table on either {@code baseMaterializedIndexMetaId} (when {@code shiftMetaId}
     * is true) or {@code baseMaterializedIndexId} (false). The other partitions
     * and physical partitions match live.
     */
    private Bookmark synthesizeMismatchedBookmark(OlapTable live, boolean shiftMetaId) {
        long bookmarkId = GlobalStateMgr.getCurrentState().getNextId();
        long bookmarkTimeMs = System.currentTimeMillis();
        java.util.Map<Long, java.util.Map<Long, PhysicalPartitionMeta>> parts = new java.util.HashMap<>();
        boolean firstShifted = false;
        for (Partition p : live.getPartitions()) {
            java.util.Map<Long, PhysicalPartitionMeta> inner = new java.util.HashMap<>();
            for (PhysicalPartition pp : p.getSubPartitions()) {
                MaterializedIndex base = pp.getLatestBaseIndex();
                long metaId = base.getMetaId();
                long indexId = base.getId();
                if (!firstShifted) {
                    if (shiftMetaId) {
                        metaId = metaId + 1;
                    } else {
                        indexId = indexId + 1;
                    }
                    firstShifted = true;
                }
                inner.put(pp.getId(), new PhysicalPartitionMeta(
                        indexId, metaId,
                        pp.getVisibleVersion(), pp.getVisibleVersionTime()));
            }
            parts.put(p.getId(), inner);
        }
        return new Bookmark(dbId, live.getId(), bookmarkId, bookmarkTimeMs, parts);
    }

    private void registerSyntheticBookmark(Bookmark b) {
        GlobalStateMgr.getCurrentState().getBookmarkManager()
                .replay(BookmarkLogEntry.AddBookmark.of(
                        b, BookmarkHolder.forEmptyInfo("synthetic"), b.getBookmarkTimeMs()));
    }

    /**
     * Build a BookmarkChange whose entries cover every partition of {@code live}:
     * the first as ADDED, the rest as DATA_CHANGED. Each head meta carries a
     * fabricated visibleVersion / visibleVersionTimeMs distinct from live so the
     * tests can prove the resolver stamps versions from the diff, not from live.
     */
    private BookmarkChange buildDiffWithAddedAndDataChanged(OlapTable live) {
        Map<Long, List<PhysicalPartitionChange>> changes = new HashMap<>();
        long versionSeed = 1000L;
        long timeSeed = 10_000L;
        boolean first = true;
        for (Partition p : live.getPartitions()) {
            long logicalId = p.getId();
            PhysicalPartition pp = p.getDefaultPhysicalPartition();
            MaterializedIndex base = pp.getLatestBaseIndex();
            PhysicalPartitionMeta head = new PhysicalPartitionMeta(
                    base.getId(), base.getMetaId(), versionSeed++, timeSeed++);
            List<PhysicalPartitionChange> row = new ArrayList<>();
            if (first) {
                row.add(new PartitionAdded(logicalId, pp.getId(), head));
                first = false;
            } else {
                PhysicalPartitionMeta basePartitionMeta = new PhysicalPartitionMeta(
                        base.getId(), base.getMetaId(),
                        pp.getVisibleVersion(), pp.getVisibleVersionTime());
                row.add(new DataChanged(logicalId, pp.getId(), basePartitionMeta, head));
            }
            changes.put(logicalId, row);
        }
        return new BookmarkChange(changes);
    }

    /** Head meta of every trackable change (ADDED / DATA_CHANGED), in iteration order. */
    private List<PhysicalPartitionChange> trackableChanges(BookmarkChange diff) {
        List<PhysicalPartitionChange> result = new ArrayList<>();
        for (List<PhysicalPartitionChange> row : diff.getChanges().values()) {
            for (PhysicalPartitionChange c : row) {
                if (c instanceof PartitionAdded || c instanceof DataChanged) {
                    result.add(c);
                }
            }
        }
        return result;
    }

    private PhysicalPartitionMeta headMetaOf(PhysicalPartitionChange c) {
        if (c instanceof PartitionAdded) {
            return ((PartitionAdded) c).getHeadPartition();
        }
        return ((DataChanged) c).getHeadPartition();
    }

    @Test
    public void testResolveByBookmarkChangeRetainsTrackablePartitions() throws Exception {
        OlapTable live = table(createDefaultTable());
        BookmarkChange diff = buildDiffWithAddedAndDataChanged(live);

        OlapTable scoped = BookmarkScopedTableResolver.resolveByChange(live, diff);

        Set<Long> trackable = new HashSet<>();
        for (PhysicalPartitionChange c : trackableChanges(diff)) {
            trackable.add(c.getLogicalPartitionId());
        }
        Set<Long> scopedIds = new HashSet<>();
        for (Partition p : scoped.getPartitions()) {
            scopedIds.add(p.getId());
        }
        assertEquals(trackable, scopedIds);
    }

    @Test
    public void testResolveByBookmarkChangeStampsHeadVersion() throws Exception {
        OlapTable live = table(createDefaultTable());
        BookmarkChange diff = buildDiffWithAddedAndDataChanged(live);

        OlapTable scoped = BookmarkScopedTableResolver.resolveByChange(live, diff);

        for (PhysicalPartitionChange c : trackableChanges(diff)) {
            PhysicalPartitionMeta head = headMetaOf(c);
            PhysicalPartition pp = scoped.getPartition(c.getLogicalPartitionId())
                    .getDefaultPhysicalPartition();
            assertEquals(head.getVisibleVersion(), pp.getVisibleVersion());
            assertEquals(head.getVisibleVersionTimeMs(), pp.getVisibleVersionTime());
        }
    }

    @Test
    public void testResolveByBookmarkChangePreservesFullSchema() throws Exception {
        OlapTable live = table(createDefaultTable());
        BookmarkChange diff = buildDiffWithAddedAndDataChanged(live);

        OlapTable scoped = BookmarkScopedTableResolver.resolveByChange(live, diff);

        assertEquals(live.getFullSchema(), scoped.getFullSchema());
    }

    @Test
    public void testResolveByBookmarkChangeKeepsOnlyBaseIndex() throws Exception {
        OlapTable live = table(createDefaultTable());
        // Inject a non-base MaterializedIndexMeta to stand in for a rollup; the
        // resolver must prune it so the scoped table carries only the base.
        Iterator<MaterializedIndexMeta> baseIter = live.getIndexMetaIdToMeta().values().iterator();
        MaterializedIndexMeta baseMeta = baseIter.next();
        long fakeRollupMetaId = baseMeta.getIndexMetaId() + 100_000L;
        MaterializedIndexMeta fakeRollupMeta = new MaterializedIndexMeta(
                fakeRollupMetaId, baseMeta.getSchema(), baseMeta.getSchemaVersion(),
                baseMeta.getSchemaHash(), baseMeta.getShortKeyColumnCount(),
                baseMeta.getStorageType(), baseMeta.getKeysType(), null);
        live.getIndexMetaIdToMeta().put(fakeRollupMetaId, fakeRollupMeta);
        assertTrue(live.getIndexMetaIdToMeta().size() >= 2);

        BookmarkChange diff = buildDiffWithAddedAndDataChanged(live);
        OlapTable scoped = BookmarkScopedTableResolver.resolveByChange(live, diff);

        assertEquals(1, scoped.getIndexMetaIdToMeta().size());
        assertTrue(scoped.getIndexMetaIdToMeta().containsKey(live.getBaseIndexMetaId()));
    }
}
