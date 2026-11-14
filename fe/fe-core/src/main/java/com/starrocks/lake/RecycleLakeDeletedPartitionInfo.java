// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.lake;

import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.staros.client.StarClientException;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RecyclePartitionInfo;
import com.starrocks.catalog.RecyclePartitionInfoV2;
import com.starrocks.catalog.Tablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.cngroup.ComputeResource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Compact recycle-bin descriptor for a <b>non-recoverable</b> lake (shared-data) partition.
 *
 * <p>A non-recoverable lake partition (produced by {@code INSERT OVERWRITE} swap, force
 * {@code DROP PARTITION}, or table deletion) is never returned by {@code recoverPartition()}: that
 * method skips any entry with {@code isRecoverable() == false}, and {@code delete()} itself first
 * forces {@code recoverable = false}. Its full
 * {@code Partition -> PhysicalPartition -> MaterializedIndex -> List<LakeTablet>} object graph
 * therefore exists only to be walked once by {@code delete()} (and once by the image-time
 * inverted-index rebuild). Everything else about it — ranges, columns, distribution, replicas — is
 * dead weight.
 *
 * <p>This class keeps only the ids those paths actually consult, so the heavy graph can become
 * garbage immediately at {@code recyclePartition()} time:
 * <ul>
 *   <li>{@link #physicalPartitionShardIds} — per sub-partition {physicalPartitionId, shardId}, to
 *       locate and drop the storage directory (and to detect shared directories);</li>
 *   <li>{@link #indexTabletIds} — per {physicalPartitionId, indexId} the tablet ids, feeding
 *       {@code onErasePartition} and the image-time inverted-index rebuild;</li>
 *   <li>{@link #shardGroupIds} — feeds {@code deleteShardGroupMeta}.</li>
 * </ul>
 *
 * <p>Recoverable partitions keep today's full graph; {@code recover()} needs it.
 */
public class RecycleLakeDeletedPartitionInfo extends RecyclePartitionInfoV2 {
    /** {physicalPartitionId, indexId, tabletIds...} for each materialized index. */
    @SerializedName(value = "indexTabletIds")
    private final List<long[]> indexTabletIds;
    /** {physicalPartitionId, shardId} for each sub-partition; shardId {@code -1} means empty. */
    @SerializedName(value = "physicalPartitionShardIds")
    private final List<long[]> physicalPartitionShardIds;
    @SerializedName(value = "shardGroupIds")
    private final List<Long> shardGroupIds;

    public RecycleLakeDeletedPartitionInfo(long dbId, long tableId, long partitionId, String partitionName,
                                           DataProperty dataProperty, short replicationNum,
                                           DataCacheInfo dataCacheInfo,
                                           List<long[]> indexTabletIds, List<long[]> physicalPartitionShardIds,
                                           List<Long> shardGroupIds) {
        super(dbId, tableId, null, dataProperty, replicationNum, dataCacheInfo);
        setPartitionIdentity(partitionId, partitionName);
        this.indexTabletIds = indexTabletIds;
        this.physicalPartitionShardIds = physicalPartitionShardIds;
        this.shardGroupIds = shardGroupIds;
        // Non-recoverable by construction: only built for entries that can never be recovered.
        setRecoverable(false);
    }

    @Override
    public boolean delete() {
        try {
            ComputeResource computeResource =
                    GlobalStateMgr.getCurrentState().getWarehouseMgr().getBackgroundComputeResource(tableId);
            if (LakeTableHelper.removePartitionDirectory(computeResource, physicalPartitionShardIds,
                    isForceRemoveDirectory())) {
                GlobalStateMgr.getCurrentState().getLocalMetastore().onErasePartition(collectTabletIds());
                LakeTableHelper.deleteShardGroupMeta(Sets.newHashSet(shardGroupIds));
                return true;
            }
            return false;
        } catch (StarClientException e) {
            return false;
        }
    }

    public List<Long> collectTabletIds() {
        List<Long> tabletIds = new ArrayList<>();
        for (long[] group : indexTabletIds) {
            for (int i = 2; i < group.length; i++) {
                tabletIds.add(group[i]);
            }
        }
        return tabletIds;
    }

    public List<long[]> getIndexTabletIds() {
        return indexTabletIds;
    }

    public List<long[]> getPhysicalPartitionShardIds() {
        return physicalPartitionShardIds;
    }

    public List<Long> getShardGroupIds() {
        return shardGroupIds;
    }

    public Set<Long> collectShardGroupIds() {
        return Sets.newHashSet(shardGroupIds);
    }

    /**
     * If {@code info} describes a non-recoverable lake partition, walk its object graph once and
     * return a compact descriptor; otherwise return {@code info} unchanged.
     *
     * <p>Collapsing the graph here (rather than lazily in {@code delete()}) is the whole point: the
     * caller drops its own reference to the partition right after enqueueing, so the
     * {@code MaterializedIndex} / {@code LakeTablet} objects become collectable immediately instead
     * of surviving until the async directory removal finishes.
     */
    public static RecyclePartitionInfo compactIfPossible(RecyclePartitionInfo info) {
        if (info.isRecoverable() || info.getPartition() == null) {
            return info;
        }
        if (!(info instanceof RecycleLakeRangePartitionInfo)
                && !(info instanceof RecycleLakeListPartitionInfo)
                && !(info instanceof RecycleLakeUnPartitionInfo)) {
            return info;
        }
        Partition partition = info.getPartition();
        List<long[]> indexTabletIds = new ArrayList<>();
        List<long[]> physicalPartitionShardIds = new ArrayList<>();
        List<Long> shardGroupIds = new ArrayList<>();
        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
            long physicalPartitionId = physicalPartition.getId();
            long firstShardId = -1L;
            boolean sawIndex = false;
            for (MaterializedIndex index : physicalPartition.getAllMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                long indexId = index.getId();
                List<Tablet> tablets = index.getTablets();
                long[] group = new long[2 + tablets.size()];
                group[0] = physicalPartitionId;
                group[1] = indexId;
                for (int i = 0; i < tablets.size(); i++) {
                    group[2 + i] = tablets.get(i).getId();
                }
                indexTabletIds.add(group);
                if (index.getShardGroupId() >= 0) {
                    shardGroupIds.add(index.getShardGroupId());
                }
                if (!sawIndex && !tablets.isEmpty()) {
                    // Any tablet of any index in the sub-partition points at the same shard group's
                    // directory; the first one is enough to locate it (mirrors getAssociatedShardInfo).
                    firstShardId = ((LakeTablet) tablets.get(0)).getShardId();
                    sawIndex = true;
                }
            }
            physicalPartitionShardIds.add(new long[] {physicalPartitionId, firstShardId});
        }

        RecycleLakeDeletedPartitionInfo compact = new RecycleLakeDeletedPartitionInfo(
                info.getDbId(), info.getTableId(), info.getPartitionId(), info.getPartitionName(),
                info.getDataProperty(), info.getReplicationNum(), info.getDataCacheInfo(),
                indexTabletIds, physicalPartitionShardIds, shardGroupIds);
        compact.setRetentionPeriod(info.getRetentionPeriod());
        compact.setFromTableDeletion(info.isFromTableDeletion());
        return compact;
    }
}