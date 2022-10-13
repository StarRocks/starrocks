// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.lake.proto.DeleteTabletRequest;
import com.starrocks.lake.proto.DeleteTabletResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ShardDeleter extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(ShardDeleter.class);

    private Map<Long, Set<Long>> getAllPartitionId() {
        Map<Long, Set<Long>> partitionToShards = new HashMap<>();
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIdsIncludeRecycleBin();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }

            if (db.isInfoSchemaDb()) {
                continue;
            }

            db.readLock();

            try {
                for (Table table : GlobalStateMgr.getCurrentState().getTablesIncludeRecycleBin(db)) {
                    if (table.isLakeTable()) {
                        // for debug
                        LOG.info("table name is {}", table.getName());
                        OlapTable olapTbl = (OlapTable) table;
                        for (Partition partition : GlobalStateMgr.getCurrentState().getAllPartitionsIncludeRecycleBin(olapTbl)) {
                            Set<Long> shardId = new HashSet<>();
                            for (MaterializedIndex idx : partition
                                    .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                                for (Tablet tablet : idx.getTablets()) {
                                    shardId.add(tablet.getId());
                                }
                            }
                            partitionToShards.put(partition.getId(), shardId);
                        }
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        return partitionToShards;
    }

    private Set<Long> dropTabletAndDeleteShard(Set<Long> shardIds) {
        Map<Long, Set<Long>> shardIdsByBeMap = new HashMap<>();
        // group shards by be
        for (long shardId : shardIds) {
            try {
                long backendId = GlobalStateMgr.getCurrentState().getStarOSAgent().getPrimaryBackendIdByShard(shardId);
                shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
            } catch (UserException ignored1) {
                // ignore error
            }
        }

        Set<Long> deletedShards = Sets.newHashSet();
        for (Map.Entry<Long, Set<Long>> entry : shardIdsByBeMap.entrySet()) {
            long backendId = entry.getKey();
            Set<Long> shards = entry.getValue();

            // 1. drop tablet
            Backend backend = GlobalStateMgr.getCurrentState().getCurrentSystemInfo().getBackend(backendId);
            DeleteTabletRequest request = new DeleteTabletRequest();
            request.tabletIds = Lists.newArrayList(shards);

            try {
                LakeService lakeService = BrpcProxy.getLakeService(backend.getHost(), backend.getBrpcPort());
                DeleteTabletResponse response = lakeService.deleteTablet(request).get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    LOG.info("failedTablets is {}", response.failedTablets);
                    response.failedTablets.forEach(shards::remove);
                }
            } catch (Throwable e) {
                LOG.error(e);
                continue;
            }

            // 2. delete shard
            try {
                GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShards(shards);
            } catch (DdlException e) {
                LOG.warn("failed to delete shard from starMgr");
                continue;
            }
            // for debug
            LOG.info("succ delete shards {}", shards);

            deletedShards.addAll(shards);
        }

        return deletedShards;
    }


    private void deleteUnusedShards() {
        // 1.delete shard group
        // 1.1.get all partitions of fe
        Map<Long, Set<Long>> allPartitionToShards = getAllPartitionId();
        Set<Long> allPartitionId = allPartitionToShards.keySet();
        // 1.2.get all shard group from starMgr
        Set<Long> allShardGroupId = GlobalStateMgr.getCurrentState().getStarOSAgent().listShardGroup();
        // 1.3.compute diff
        Set<Long> diff = new HashSet<>(allShardGroupId);
        // need to remove default shard group----> 0
        diff.remove(0);
        diff.removeAll(allPartitionId);
        // 1.4.collect redundant shard groups
        List<Long> needDeleteShardGroup = new ArrayList<>();
        // for debug
        LOG.info("diff group is {}", diff);
        for (long partitionId : diff) {
            // for debug
            LOG.info("partitionId is {}", partitionId);
            Set<Long> shardIds = GlobalStateMgr.getCurrentState().getStarOSAgent().listShard(partitionId);
            // for debug
            LOG.info("we will delete shards {}", shardIds);
            if (shardIds.isEmpty()) {
                // for debug
                LOG.info("add partitionId {} into needDeleteShardGroup", partitionId);
                needDeleteShardGroup.add(partitionId);
            } else {
                Set<Long> deletedShardIds = dropTabletAndDeleteShard(shardIds);
                if (deletedShardIds == shardIds) {
                    // for debug
                    LOG.info("add partitionId {} into needDeleteShardGroup", partitionId);
                    needDeleteShardGroup.add(partitionId);
                }
            }
        }

        // 2. delete shards
        for (Map.Entry<Long, Set<Long>> entry : allPartitionToShards.entrySet()) {
            long partitionId = entry.getKey();
            Set<Long> shardIds = entry.getValue();
            Set<Long> allshardIds = GlobalStateMgr.getCurrentState().getStarOSAgent().listShard(partitionId);
            // for debug
            LOG.info("partition {}： shardIds is {}, allshardIds is {}", partitionId, shardIds, allshardIds);
            // collect empty shard group
            if (allshardIds.isEmpty()) {
                needDeleteShardGroup.add(partitionId);
            } else {
                diff = new HashSet<>(allshardIds);
                diff.removeAll(shardIds);
                dropTabletAndDeleteShard(diff);
            }
        }

        // delete shard group
        if (!needDeleteShardGroup.isEmpty()) {
            // for debug
            LOG.info("we will delete shard group {}", needDeleteShardGroup);
            GlobalStateMgr.getCurrentState().getStarOSAgent().deleteShardGroup(needDeleteShardGroup);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        deleteUnusedShards();
    }
}