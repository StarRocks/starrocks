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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A superseded materialized index scheduled for removal by the {@link CatalogRecycleBin} (e.g. the old
 * index retired by a tablet split/merge, issue #75993). This record only carries the index's
 * coordinates -- the index object itself stays on its live {@link PhysicalPartition} for the whole
 * retention window and is only detached at erase time.
 *
 * <p>Keeping the old index installed is what protects its shards during retention: a split reuses the
 * parent's shard group for the child, so the group is never orphaned and the group-level reaper never
 * touches it; the shards are reclaimed by {@code StarMgrMetaSyncer.syncTableMetaInternal}, which lists
 * every shard in the group and drops the ones not referenced by an index still on the partition. While
 * the old index is installed, {@code getAllMaterializedIndices} enumerates it, its tablets are
 * subtracted, and its shards are kept. Reads/writes never pick it: every scan/load resolves the
 * partition via {@code getLatestIndex}/{@code getLatestMaterializedIndices}, which return only the new
 * child.
 *
 * <p>{@link #delete()} detaches the index from the partition (under the table write lock) and drops its
 * tablets from the {@code TabletInvertedIndex}. The next {@code StarMgrMetaSyncer} cycle then reaps the
 * now-unreferenced shards per-shard -- never a partition directory, which matters for a split where the
 * parent and child tablets share the same object-storage directory.
 */
public class RecycleMaterializedIndexInfo {
    private static final Logger LOG = LogManager.getLogger(RecycleMaterializedIndexInfo.class);

    @SerializedName(value = "dbId")
    private final long dbId;
    @SerializedName(value = "tableId")
    private final long tableId;
    @SerializedName(value = "physicalPartitionId")
    private final long physicalPartitionId;
    @SerializedName(value = "indexId")
    private final long indexId;

    public RecycleMaterializedIndexInfo(long dbId, long tableId, long physicalPartitionId, long indexId) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.physicalPartitionId = physicalPartitionId;
        this.indexId = indexId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public long getIndexId() {
        return indexId;
    }

    /**
     * Detach the index from its live partition and drop its tablets from the inverted index. Physical
     * shard/data reclamation then happens on the {@code StarMgrMetaSyncer} cycle, which deletes the
     * now-unreferenced shards per-shard (never a directory) and is still gated by cluster-snapshot
     * safety. Idempotent and null-safe: a re-run/replay, or a table/partition that has since been
     * dropped, is a no-op.
     */
    public void delete() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        MaterializedIndex removed = null;
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(dbId, Lists.newArrayList(tableId), LockType.WRITE);
        try {
            Table table = globalStateMgr.getLocalMetastore().getTable(dbId, tableId);
            if (!(table instanceof OlapTable)) {
                return;
            }
            PhysicalPartition physicalPartition = ((OlapTable) table).getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                return;
            }
            removed = physicalPartition.deleteMaterializedIndexByIndexId(indexId);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(dbId, Lists.newArrayList(tableId), LockType.WRITE);
        }

        if (removed == null) {
            return;
        }
        TabletInvertedIndex invertedIndex = globalStateMgr.getTabletInvertedIndex();
        for (Tablet tablet : removed.getTablets()) {
            invertedIndex.deleteTablet(tablet.getId());
        }
        LOG.info("Detached recycled materialized index {} from partition {} (table {}); {} tablets unregistered",
                indexId, physicalPartitionId, tableId, removed.getTablets().size());
    }
}
