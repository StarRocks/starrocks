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

package com.starrocks.lake.ivm;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.lake.bookmark.AlreadyAtLatestException;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.BookmarkNotFoundException;
import com.starrocks.lake.bookmark.HolderId;
import com.starrocks.lake.bookmark.ReferenceNotFoundException;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;

/**
 * Bookmark-side glue for IVM-on-Lake: lifecycle operations on Lake bookmarks
 * keyed by an MV's {@link HolderId}. Called from {@code LocalMetastore}'s
 * ConnectorMetadata overrides and from {@code dropMaterializedView}.
 */
public final class MvBookmarkOps {

    private MvBookmarkOps() {
    }

    /**
     * Acquire a bookmark for {@code mvId} on a Lake {@code baseTable}: create
     * (or reuse via {@link AlreadyAtLatestException}), then drop every orphan
     * reference this holder still pins except the new id and the MV's
     * last-committed id. Orphans are reachable because
     * {@code TableBookmarkTracker.findLatestEquivalent} only dedups against
     * the latest active bookmark, so older equivalent state (e.g. ADD then
     * DROP between two refreshes) gets a fresh id.
     */
    public static TvrTableSnapshot acquire(long dbId, Table baseTable, MvId mvId) {
        long tableId = baseTable.getId();
        BookmarkManager bookmarkManager = GlobalStateMgr.getCurrentState().getBookmarkManager();
        BookmarkHolder holder = BookmarkHolder.forMv(mvId);
        HolderId holderId = holder.getHolderId();

        long newId;
        try {
            newId = bookmarkManager.create(dbId, tableId, holder).getBookmarkId();
        } catch (AlreadyAtLatestException e) {
            newId = e.getBookmarkId();
        } catch (LockTimeoutException e) {
            throw new StarRocksConnectorException("acquire bookmark timed out: " + e.getMessage());
        }

        Long vCommitted = resolveCommittedBookmarkId(mvId, baseTable);
        for (long bookmarkId : bookmarkManager.listBookmarkIdsByHolder(dbId, tableId, holderId)) {
            if (bookmarkId == newId || (vCommitted != null && bookmarkId == vCommitted)) {
                continue;
            }
            try {
                bookmarkManager.releaseReference(dbId, tableId, bookmarkId, holderId);
            } catch (BookmarkNotFoundException | ReferenceNotFoundException ignored) {
                // At-most-once: another caller released this reference between list and release.
            }
        }
        return TvrTableSnapshot.of(newId);
    }

    /**
     * Resolve {@code (from, to]} bookmarks and ask {@link BookmarkChangeTvrAdapter}
     * for the single-trait delta describing the range.
     */
    public static List<TvrTableDeltaTrait> computeDeltaTraits(long dbId, long tableId,
                                                              TvrTableSnapshot fromSnapshotExclusive,
                                                              TvrTableSnapshot toSnapshotInclusive) {
        BookmarkManager bookmarkManager = GlobalStateMgr.getCurrentState().getBookmarkManager();
        Bookmark base = fromSnapshotExclusive.isEmpty()
                ? null
                : bookmarkManager.findBookmarkById(dbId, tableId, fromSnapshotExclusive.getSnapshotId())
                .orElseThrow(() -> new StarRocksConnectorException(
                        "from-snapshot bookmark not found: db=" + dbId + ", table=" + tableId
                                + ", id=" + fromSnapshotExclusive.getSnapshotId()));
        Bookmark head = bookmarkManager.findBookmarkById(dbId, tableId, toSnapshotInclusive.getSnapshotId())
                .orElseThrow(() -> new StarRocksConnectorException(
                        "to-snapshot bookmark not found: db=" + dbId + ", table=" + tableId
                                + ", id=" + toSnapshotInclusive.getSnapshotId()));
        return BookmarkChangeTvrAdapter.toTvrTraits(base, head);
    }

    /** Release every bookmark {@code mv} pins on its internal-catalog base tables. */
    public static void releaseAll(MaterializedView mv) {
        BookmarkManager bookmarkManager = GlobalStateMgr.getCurrentState().getBookmarkManager();
        HolderId holderId = HolderId.forMv(mv.getMvId());
        for (BaseTableInfo info : mv.getBaseTableInfos()) {
            if (info.isInternalCatalog()) {
                bookmarkManager.releaseAllForHolder(info.getDbId(), info.getTableId(), holderId);
            }
        }
    }

    /** Last-committed bookmark id for {@code baseTable} in {@code mvId}'s TVR map, or null. */
    private static Long resolveCommittedBookmarkId(MvId mvId, Table baseTable) {
        Database mvDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
        if (mvDb == null) {
            return null;
        }
        Table mvTable = mvDb.getTable(mvId.getId());
        if (!(mvTable instanceof MaterializedView mv)) {
            return null;
        }
        for (Map.Entry<BaseTableInfo, TvrVersionRange> e : mv.getRefreshScheme().getAsyncRefreshContext()
                .getBaseTableInfoTvrVersionRangeMap().entrySet()) {
            if (e.getKey().matchTable(baseTable)) {
                return e.getValue().end().orElse(null);
            }
        }
        return null;
    }
}
