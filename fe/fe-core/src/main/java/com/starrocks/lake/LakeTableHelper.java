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

package com.starrocks.lake;

import com.google.common.base.Preconditions;
import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.staros.proto.StatusCode;
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.LakeTableAlterJobV2Builder;
import com.starrocks.alter.LakeTableRollupBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.transaction.TransactionState;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class LakeTableHelper {
    private static final Logger LOG = LogManager.getLogger(LakeTableHelper.class);

    static boolean deleteTable(long dbId, OlapTable table, boolean replay) {
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());
        table.removeTableBinds(replay);
        // LakeTable's data cleaning operation is a time-consuming operation that may need to be repeated
        // many times before it succeeds, so we chose to let the recycle bin perform this work for two purposes:
        // 1. to avoid blocking the user's session for too long
        // 2. to utilize the recycle bin's delete retry capability to perform retries.
        GlobalStateMgr.getCurrentState().getRecycleBin().recycleTable(dbId, table, false);
        return true;
    }

    static boolean deleteTableFromRecycleBin(long dbId, OlapTable table, boolean replay) {
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());
        table.removeTableBinds(replay);
        if (replay) {
            table.removeTabletsFromInvertedIndex();
            return true;
        }
        LakeTableCleaner cleaner = new LakeTableCleaner(table);
        boolean succ = cleaner.cleanTable();
        if (succ) {
            table.removeTabletsFromInvertedIndex();
        }
        return succ;
    }

    static AlterJobV2Builder alterTable(OlapTable table) {
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());
        return new LakeTableAlterJobV2Builder(table);
    }

    static AlterJobV2Builder rollUp(OlapTable table) {
        Preconditions.checkState(table.isCloudNativeTableOrMaterializedView());
        return new LakeTableRollupBuilder(table);
    }

    static boolean removeShardRootDirectory(ShardInfo shardInfo) {
        DropTableRequest request = new DropTableRequest();
        final String path = shardInfo.getFilePath().getFullPath();
        request.tabletId = shardInfo.getShardId();
        request.path = path;
        ComputeNode node = Utils.chooseNode(shardInfo);
        if (node == null) {
            LOG.warn("Fail to remove {}: no alive node", path);
            return false;
        }
        TNetworkAddress address = new TNetworkAddress(node.getHost(), node.getBrpcPort());
        try {
            LakeService lakeService = BrpcProxy.getLakeService(address);
            StatusPB status = lakeService.dropTable(request).get().status;
            if (status != null && status.statusCode != 0) {
                LOG.warn("[{}]Fail to remove {}: {}", node.getHost(), path, StringUtils.join(status.errorMsgs, ","));
                return false;
            }
            LOG.info("Removed {} at node {}", path, node.getHost());
            return true;
        } catch (Exception e) {
            LOG.warn("Fail to remove {} on node {}: {}", path, node.getHost(), e.getMessage());
            return false;
        }
    }

    static Optional<ShardInfo> getAssociatedShardInfo(PhysicalPartition partition, long warehouseId) throws StarClientException {
        List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
        for (MaterializedIndex materializedIndex : allIndices) {
            List<Tablet> tablets = materializedIndex.getTablets();
            if (tablets.isEmpty()) {
                continue;
            }
            LakeTablet tablet = (LakeTablet) tablets.get(0);
            try {
                if (GlobalStateMgr.isCheckpointThread()) {
                    throw new RuntimeException("Cannot call getShardInfo in checkpoint thread");
                }
                WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                long workerGroupId = warehouseManager.selectWorkerGroupByWarehouseId(warehouseId)
                        .orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                ShardInfo shardInfo = GlobalStateMgr.getCurrentState().getStarOSAgent().getShardInfo(tablet.getShardId(),
                        workerGroupId);

                return Optional.of(shardInfo);
            } catch (StarClientException e) {
                if (e.getCode() != StatusCode.NOT_EXIST) {
                    throw e;
                }
                // Shard does not exist, ignore this shard
            }
        }
        return Optional.empty();
    }

    static boolean removePartitionDirectory(Partition partition, long warehouseId) throws StarClientException {
        boolean ret = true;
        for (PhysicalPartition subPartition : partition.getSubPartitions()) {
            ShardInfo shardInfo = getAssociatedShardInfo(subPartition, warehouseId).orElse(null);
            if (shardInfo == null) {
                LOG.info("Skipped remove directory of empty partition {}", subPartition.getId());
                continue;
            }
            if (isSharedDirectory(shardInfo.getFilePath().getFullPath(), subPartition.getId())) {
                LOG.info("Skipped remove possible directory shared by multiple partitions: {}",
                        shardInfo.getFilePath().getFullPath());
                continue;
            }
            if (!removeShardRootDirectory(shardInfo)) {
                ret = false;
            }
        }
        return ret;
    }

    /**
     * delete `partition`'s all shard group meta (shards meta included) from starmanager
     */
    static void deleteShardGroupMeta(Partition partition)  {
        // use Set to avoid duplicate shard group id
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        Collection<PhysicalPartition> subPartitions = partition.getSubPartitions();
        Set<Long> needRemoveShardGroupIdSet = new HashSet<>();
        for (PhysicalPartition subPartition : subPartitions) {
            for (MaterializedIndex index : subPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                needRemoveShardGroupIdSet.add(index.getShardGroupId());
            }
        }
        if (!needRemoveShardGroupIdSet.isEmpty()) {
            starOSAgent.deleteShardGroup(new ArrayList<>(needRemoveShardGroupIdSet));
            LOG.debug("Deleted shard group related to partition {}, group ids: {}", partition.getId(),
                    needRemoveShardGroupIdSet);
        }
    }

    public static boolean isSharedPartitionDirectory(PhysicalPartition physicalPartition, long warehouseId)
            throws StarClientException {
        ShardInfo shardInfo = getAssociatedShardInfo(physicalPartition, warehouseId).orElse(null);
        if (shardInfo == null) {
            return false;
        }
        return isSharedDirectory(shardInfo.getFilePath().getFullPath(), physicalPartition.getId());
    }

    /**
     * Check if a directory is shared by multiple partitions.
     * If {@code path} ends with {@code partitionId}, it is considered exclusive to the
     * partition, otherwise it is considered shared.
     * <p>
     * Reference: {@link OlapTable#getPartitionFilePathInfo(long)}
     *
     * @return true if the directory is a shared directory, false otherwise
     */
    public static boolean isSharedDirectory(String path, long physicalPartitionId) {
        return !path.endsWith(String.format("/%d", physicalPartitionId));
    }

    /**
     * For tables created in the old version of StarRocks cluster, the column unique id is generated on BE and
     * is not saved in FE catalog. For these tables, we want to be able to record their column unique id in the
     * catalog after the upgrade, and the column unique id recorded must be consistent with the one on BE.
     * For shared data mode, the algorithm to generate column unique id on BE is simple: take the subscript of
     * each column as their unique id, so here we just need to follow the same algorithm to calculate the unique
     * id of each column.
     *
     * @param table the table to restore column unique id
     * @return the max column unique id
     */
    public static int restoreColumnUniqueId(OlapTable table) {
        int maxId = 0;
        for (MaterializedIndexMeta indexMeta : table.getIndexIdToMeta().values()) {
            final int columnCount = indexMeta.getSchema().size();
            maxId = Math.max(maxId, columnCount - 1);
            for (int i = 0; i < columnCount; i++) {
                Column col = indexMeta.getSchema().get(i);
                Preconditions.checkState(col.getUniqueId() <= 0, col.getUniqueId());
                col.setUniqueId(i);
            }
        }
        return maxId;
    }

    public static boolean supportCombinedTxnLog(TransactionState.LoadJobSourceType sourceType) {
        return RunMode.isSharedDataMode() && Config.lake_use_combined_txn_log && isLoadingTransaction(sourceType);
    }

    private static boolean isLoadingTransaction(TransactionState.LoadJobSourceType sourceType) {
        return sourceType == TransactionState.LoadJobSourceType.BACKEND_STREAMING ||
                sourceType == TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK ||
                sourceType == TransactionState.LoadJobSourceType.INSERT_STREAMING ||
                sourceType == TransactionState.LoadJobSourceType.BATCH_LOAD_JOB;
    }
}
