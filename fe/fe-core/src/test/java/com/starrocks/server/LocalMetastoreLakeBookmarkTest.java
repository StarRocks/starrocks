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

package com.starrocks.server;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkTestBase;
import com.starrocks.lake.bookmark.HolderId;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link LocalMetastore}'s IVM-on-Lake bookmark glue:
 * {@code acquireTvrSnapshot}, {@code listTableDeltaTraits}, plus the bookmark
 * release on MV / DB drop. Builds on {@link BookmarkTestBase}'s shared
 * mini-cluster — pure dispatch-level coverage, end-to-end IVM refresh is
 * covered elsewhere.
 */
public class LocalMetastoreLakeBookmarkTest extends BookmarkTestBase {

    private static final AtomicInteger NAME_COUNTER = new AtomicInteger();

    private LocalMetastore localMetastore() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore();
    }

    private BookmarkManager bookmarkManager() {
        return GlobalStateMgr.getCurrentState().getBookmarkManager();
    }

    private Table getTableById(long tableId) {
        return localMetastore().getDb(dbId).getTable(tableId);
    }

    /* ---------- acquireTvrSnapshot ---------- */

    @Test
    public void testAcquireTvrSnapshot_lakeTableFirstAcquire_createsBookmark() throws Exception {
        long tableId = createDefaultTable();
        MvId mvId = new MvId(dbId, 9001L);
        BookmarkManager bm = bookmarkManager();

        TvrTableSnapshot snap = localMetastore().acquireTvrSnapshot(DB_NAME, getTableById(tableId), mvId);

        assertFalse(snap.isEmpty());
        long bookmarkId = snap.getSnapshotId();
        assertTrue(bm.findBookmarkById(dbId, tableId, bookmarkId).isPresent());
        assertEquals(List.of(bookmarkId),
                bm.listBookmarkIdsByHolder(dbId, tableId, HolderId.forMv(mvId)));
    }

    @Test
    public void testAcquireTvrSnapshot_unchangedState_reusesViaAlreadyAtLatest() throws Exception {
        long tableId = createDefaultTable();
        MvId mvId = new MvId(dbId, 9002L);
        LocalMetastore lm = localMetastore();
        Table table = getTableById(tableId);

        TvrTableSnapshot first = lm.acquireTvrSnapshot(DB_NAME, table, mvId);
        TvrTableSnapshot second = lm.acquireTvrSnapshot(DB_NAME, table, mvId);

        // Same partition meta + same holder → AlreadyAtLatest path reuses the id
        // rather than fanning out another reference.
        assertEquals(first.getSnapshotId(), second.getSnapshotId());
        assertEquals(1, bookmarkManager().referenceCount(dbId, tableId, first.getSnapshotId()));
    }

    @Test
    public void testAcquireTvrSnapshot_releasesOrphanFromFailedRefresh() throws Exception {
        long tableId = createDefaultTable();
        MvId mvId = new MvId(dbId, 9003L);
        BookmarkManager bm = bookmarkManager();
        BookmarkHolder holder = BookmarkHolder.forMv(mvId);

        // Stand in for a failed prior refresh: a bookmark this holder still pins
        // that is NOT recorded in any MV TVR map. The new acquire on an evolved
        // state must drop that orphan.
        long orphanId = bm.create(dbId, tableId, holder).getBookmarkId();
        addPartition(tableId, "p3", "2024-04-01");

        TvrTableSnapshot snap = localMetastore().acquireTvrSnapshot(DB_NAME, getTableById(tableId), mvId);

        long newId = snap.getSnapshotId();
        assertNotEquals(orphanId, newId);
        assertEquals(List.of(newId), bm.listBookmarkIdsByHolder(dbId, tableId, holder.getHolderId()));
    }

    @Test
    public void testAcquireTvrSnapshot_nullMvId_fallsBackWithoutPinning() throws Exception {
        long tableId = createDefaultTable();

        TvrTableSnapshot snap = localMetastore().acquireTvrSnapshot(DB_NAME, getTableById(tableId), null);

        // null mvId → behaves like getCurrentTvrSnapshot, which defaults to empty
        // for Lake on LocalMetastore; no bookmark created.
        assertTrue(snap.isEmpty());
        assertEquals(0, bookmarkManager().activeBookmarkCount(dbId, tableId));
    }

    /* ---------- listTableDeltaTraits ---------- */

    @Test
    public void testListTableDeltaTraits_dispatchesToAdapter() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager bm = bookmarkManager();
        BookmarkHolder holder = BookmarkHolder.forMv(new MvId(dbId, 9004L));

        Bookmark base = bm.create(dbId, tableId, holder);
        addPartition(tableId, "p3", "2024-04-01");
        Bookmark head = bm.create(dbId, tableId, holder);

        List<TvrTableDeltaTrait> traits = localMetastore().listTableDeltaTraits(DB_NAME, getTableById(tableId),
                TvrTableSnapshot.of(base.getBookmarkId()),
                TvrTableSnapshot.of(head.getBookmarkId()));

        // Only-ADDED partition delta → trackable → MONOTONIC via the adapter.
        assertEquals(1, traits.size());
        assertTrue(traits.get(0).isAppendOnly());
        assertEquals(base.getBookmarkId(), traits.get(0).getTvrDelta().start().orElseThrow());
        assertEquals(head.getBookmarkId(), traits.get(0).getTvrDelta().end().orElseThrow());
    }

    @Test
    public void testListTableDeltaTraits_fromEmptyTreatedAsFirstRefresh() throws Exception {
        long tableId = createDefaultTable();
        BookmarkManager bm = bookmarkManager();
        BookmarkHolder holder = BookmarkHolder.forMv(new MvId(dbId, 9005L));
        Bookmark head = bm.create(dbId, tableId, holder);

        List<TvrTableDeltaTrait> traits = localMetastore().listTableDeltaTraits(DB_NAME, getTableById(tableId),
                TvrTableSnapshot.empty(),
                TvrTableSnapshot.of(head.getBookmarkId()));

        // base = null path through the adapter; every partition is ADDED → MONOTONIC.
        assertEquals(1, traits.size());
        assertTrue(traits.get(0).isAppendOnly());
    }

    /* ---------- drop hooks ---------- */

    @Test
    public void testDropMaterializedView_releasesBookmarksHeldByMv() throws Exception {
        long baseTableId = createDefaultTable();
        String mvName = createMvOver(DB_NAME, baseTableId);
        MaterializedView mv = (MaterializedView) localMetastore().getDb(DB_NAME).getTable(mvName);
        BookmarkManager bm = bookmarkManager();

        bm.create(dbId, baseTableId, BookmarkHolder.forMv(mv.getMvId()));
        assertEquals(1, bm.activeBookmarkCount(dbId, baseTableId));

        dropMv(DB_NAME, mvName);

        assertEquals(0, bm.activeBookmarkCount(dbId, baseTableId));
    }

    /* ---------- helpers ---------- */

    private String createMvOver(String inDb, long baseTableId) throws Exception {
        OlapTable base = (OlapTable) localMetastore().getDb(inDb).getTable(baseTableId);
        String mvName = "mv_" + NAME_COUNTER.getAndIncrement();
        String ddl = "CREATE MATERIALIZED VIEW " + inDb + "." + mvName + "\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "REFRESH ASYNC\n"
                + "PROPERTIES (\"replication_num\" = \"1\")\n"
                + "AS SELECT k, dt FROM " + inDb + "." + base.getName() + ";";
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(ddl, connectContext);
        // StmtExecutor.execute needs a queryId on ConnectContext to build executionId.
        connectContext.setQueryId(UUIDUtil.genUUID());
        new StmtExecutor(connectContext, stmt).execute();
        return mvName;
    }

    private void dropMv(String inDb, String mvName) throws Exception {
        String ddl = "DROP MATERIALIZED VIEW " + inDb + "." + mvName + ";";
        DropMaterializedViewStmt stmt =
                (DropMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(ddl, connectContext);
        localMetastore().dropMaterializedView(stmt);
    }

}
