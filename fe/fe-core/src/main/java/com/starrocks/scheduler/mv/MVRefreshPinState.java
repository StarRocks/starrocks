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

package com.starrocks.scheduler.mv;

import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.lake.bookmark.BookmarkHolder;
import com.starrocks.lake.ivm.MvBookmarkOps;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;

/**
 * Where a refresh run plans against: the base-table snapshot it pins its scan to, and the bookmarks it reads base
 * tables at. A refresh keeps both on the materialized view, where its later batches and its commit find them. An
 * EXPLAIN / TRACE REFRESH plans the same way against a private copy, so it never changes what a refresh in flight
 * is about to commit.
 */
public abstract class MVRefreshPinState {

    public static MVRefreshPinState of(MaterializedView mv) {
        return new Persisted(mv);
    }

    public static MVRefreshPinState forExplain(MaterializedView mv) {
        return new Explain(mv);
    }

    /** Whether the run started as {@code startTaskRunId} froze the snapshot it scans. */
    public boolean isPinnedBy(String startTaskRunId) {
        String owner = getOwner();
        return owner != null && owner.equals(startTaskRunId);
    }

    /** The start task run id of the job that froze the snapshot, or null when none did. */
    public abstract String getOwner();

    /** The frozen version range of each base table; empty when nothing is frozen. */
    public abstract Map<BaseTableInfo, TvrVersionRange> getFrozenTvrMap();

    public abstract void freeze(String startTaskRunId, Map<BaseTableInfo, TvrVersionRange> frozen);

    public abstract void clear();

    /** The snapshot of {@code table} to read, held as a bookmark on tables that have them. */
    public abstract TvrVersionRange acquireSnapshot(BaseTableInfo baseTableInfo, Table table);

    /** Give back the bookmarks only this run holds. */
    public void release() {
    }

    private static final class Persisted extends MVRefreshPinState {
        private final MaterializedView mv;

        private Persisted(MaterializedView mv) {
            this.mv = mv;
        }

        private MaterializedView.AsyncRefreshContext refreshContext() {
            return mv.getRefreshScheme().getAsyncRefreshContext();
        }

        @Override
        public String getOwner() {
            return refreshContext().getTempTvrOwnerStartTaskRunId();
        }

        @Override
        public Map<BaseTableInfo, TvrVersionRange> getFrozenTvrMap() {
            return refreshContext().getTempBaseTableInfoTvrDeltaMap();
        }

        @Override
        public void freeze(String startTaskRunId, Map<BaseTableInfo, TvrVersionRange> frozen) {
            refreshContext().replaceTempBaseTableInfoTvrDeltaMap(startTaskRunId, frozen);
        }

        @Override
        public void clear() {
            refreshContext().clearTempBaseTableInfoTvrDeltaState();
        }

        @Override
        public TvrVersionRange acquireSnapshot(BaseTableInfo baseTableInfo, Table table) {
            return GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .acquireTvrSnapshot(baseTableInfo.getDbName(), table, mv.getMvId());
        }
    }

    private static final class Explain extends MVRefreshPinState {
        private final MaterializedView mv;
        private final BookmarkHolder holder;
        // table id -> db id of every table this run holds a bookmark on
        private final Map<Long, Long> bookmarkedTables = Maps.newHashMap();
        private String owner;
        private Map<BaseTableInfo, TvrVersionRange> frozen = Map.of();

        private Explain(MaterializedView mv) {
            this.mv = mv;
            this.holder = BookmarkHolder.forMvExplain(mv.getMvId(), UUIDUtil.genUUID().toString());
        }

        @Override
        public String getOwner() {
            return owner;
        }

        @Override
        public Map<BaseTableInfo, TvrVersionRange> getFrozenTvrMap() {
            return frozen;
        }

        @Override
        public void freeze(String startTaskRunId, Map<BaseTableInfo, TvrVersionRange> frozen) {
            this.owner = startTaskRunId;
            this.frozen = Map.copyOf(frozen);
        }

        @Override
        public void clear() {
            owner = null;
            frozen = Map.of();
        }

        @Override
        public TvrVersionRange acquireSnapshot(BaseTableInfo baseTableInfo, Table table) {
            if (!table.isCloudNativeTableOrMaterializedView()) {
                return GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .acquireTvrSnapshot(baseTableInfo.getDbName(), table, mv.getMvId());
            }
            bookmarkedTables.put(table.getId(), baseTableInfo.getDbId());
            return MvBookmarkOps.acquireForExplain(baseTableInfo.getDbId(), table, holder);
        }

        @Override
        public void release() {
            bookmarkedTables.forEach((tableId, dbId) ->
                    MvBookmarkOps.releaseForExplain(dbId, tableId, holder.getHolderId()));
            bookmarkedTables.clear();
        }
    }
}
