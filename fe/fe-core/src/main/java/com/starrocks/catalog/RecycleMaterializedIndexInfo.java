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

import com.google.gson.annotations.SerializedName;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;

/**
 * A superseded materialized index parked in the {@link CatalogRecycleBin} (e.g. the old index retired
 * by a tablet split/merge, issue #75993). Modeling the retired object as an index -- rather than
 * wrapping it in a synthetic partition -- lets the recycle bin retain and reclaim it at its natural
 * granularity: never user-recoverable, and erased strictly at the tablet level.
 *
 * <p>{@link #delete()} only drops the index's tablets from the {@code TabletInvertedIndex} (the
 * index-level analog of {@code LocalMetastore#onErasePartition}); the physical shard reclamation is
 * left to {@code StarMgrMetaSyncer}, which reaps the now-orphaned shard group per-shard. It never
 * removes a partition directory -- which matters for a split, where the parent and child tablets share
 * the same object-storage directory and a directory-level delete would destroy the live child data.
 */
public class RecycleMaterializedIndexInfo {
    @SerializedName(value = "dbId")
    private final long dbId;
    @SerializedName(value = "tableId")
    private final long tableId;
    @SerializedName(value = "physicalPartitionId")
    private final long physicalPartitionId;
    @SerializedName(value = "index")
    private final MaterializedIndex index;

    public RecycleMaterializedIndexInfo(long dbId, long tableId, long physicalPartitionId, MaterializedIndex index) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.physicalPartitionId = physicalPartitionId;
        this.index = index;
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

    public MaterializedIndex getIndex() {
        return index;
    }

    public long getIndexId() {
        return index.getId();
    }

    public long getShardGroupId() {
        return index.getShardGroupId();
    }

    /**
     * Register the retained index's tablets in the inverted index so the FE keeps them addressable
     * while the index is parked. Mirrors {@code CatalogRecycleBin#addTabletToInvertedIndex} for
     * recycled partitions; used when the recycle bin is reloaded from the image.
     */
    public void addTabletToInvertedIndex() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartitionId, index.getId(),
                TStorageMedium.HDD, true);
        for (Tablet tablet : index.getTablets()) {
            invertedIndex.addTablet(tablet.getId(), tabletMeta);
        }
    }

    /**
     * Drop the index's tablets from the inverted index. Physical shard/data reclamation then happens
     * on the {@code StarMgrMetaSyncer} cycle, which deletes the orphaned shards per-shard (never a
     * directory) and is still gated by {@code isSafeToDelete()} / cluster-snapshot safety.
     */
    public void delete() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        for (Tablet tablet : index.getTablets()) {
            invertedIndex.deleteTablet(tablet.getId());
        }
    }
}
