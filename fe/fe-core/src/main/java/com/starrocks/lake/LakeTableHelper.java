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
import com.starrocks.alter.AlterJobV2Builder;
import com.starrocks.alter.LakeTableAlterJobV2Builder;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.proto.DropTableRequest;
import com.starrocks.proto.StatusPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

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

    static Optional<ShardInfo> getAssociatedShardInfo(PhysicalPartition partition) throws StarClientException {
        List<MaterializedIndex> allIndices = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
        for (MaterializedIndex materializedIndex : allIndices) {
            List<Tablet> tablets = materializedIndex.getTablets();
            if (tablets.isEmpty()) {
                continue;
            }
            LakeTablet tablet = (LakeTablet) tablets.get(0);
            return Optional.of(tablet.getShardInfo());
        }
        return Optional.empty();
    }

    static boolean removePartitionDirectory(Partition partition) throws StarClientException {
        boolean ret = true;
        for (PhysicalPartition subPartition : partition.getSubPartitions()) {
            ShardInfo shardInfo = getAssociatedShardInfo(subPartition).orElse(null);
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

    public static boolean isSharedPartitionDirectory(PhysicalPartition partition) throws StarClientException {
        ShardInfo shardInfo = getAssociatedShardInfo(partition).orElse(null);
        if (shardInfo == null) {
            return false;
        }
        return isSharedDirectory(shardInfo.getFilePath().getFullPath(), partition.getId());
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
    public static boolean isSharedDirectory(String path, long partitionId) {
        return !path.endsWith(String.format("/%d", partitionId));
    }
}
