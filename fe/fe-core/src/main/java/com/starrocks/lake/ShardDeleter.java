// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import autovalue.shaded.com.google.common.common.collect.Lists;
import autovalue.shaded.com.google.common.common.collect.Sets;
import com.staros.proto.ShardGroupInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ShardDeleter extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(ShardDeleter.class);

    private List<Long> getAllPartitionId() {
        List<Long> groupIds = new ArrayList<>();
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
                        OlapTable olapTbl = (OlapTable) table;
                        for (Partition partition : GlobalStateMgr.getCurrentState().getAllPartitionsIncludeRecycleBin(olapTbl)) {
                            groupIds.add(partition.getShardGroupId());
                        }
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        return groupIds;
    }

    public static void dropTabletAndDeleteShard(List<Long> shardIds, StarOSAgent starOSAgent) {
        Map<Long, Set<Long>> shardIdsByBeMap = new HashMap<>();
        // group shards by be
        for (long shardId : shardIds) {
            try {
                long backendId = starOSAgent.getPrimaryBackendIdByShard(shardId);
                shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
            } catch (UserException ignored1) {
                // ignore error
            }
        }

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
                starOSAgent.deleteShards(shards);
            } catch (DdlException e) {
                LOG.warn("failed to delete shard from starMgr");
                continue;
            }
        }
    }

    // delete redundant shard/shardGroup
    // collect all shardGroup from starMgr and do diff, the redundant groups expect 0 should be deleted from starMgr
    // for shard groups which exist in starMgr but not fe, if their create time , delete tShardGroupInfo::getGroupIdhem
    private void deleteUnusedShards() {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();

        // 1.1.get all partitions of fe
        // 1.2.get all shard group from starMgr
        // 1.3.compute diff
        List<Long> groupIdFe = getAllPartitionId();
        List<ShardGroupInfo> shardGroupInfos = starOSAgent.listShardGroup();
        shardGroupInfos = shardGroupInfos.stream()
                .filter(item -> item.getGroupId() != 0L)
                .collect(Collectors.toList());

        if (shardGroupInfos.isEmpty()) {
            return;
        }

        // for debug
        LOG.info("size of groupIdFe is {}, size of shardGroupInfos is {}",
                groupIdFe.size(), shardGroupInfos.size());
        LOG.info("groupIdFe is {}", groupIdFe);

        Map<Long, String> groupToCreateTimeMap =
                shardGroupInfos.stream().collect(Collectors.toMap(
                        obj -> obj.getGroupId(),
                        obj -> obj.getPropertiesMap().get("createTime"),
                        (key1, key2) -> key1
                ));

        List<Long> groupIdStaros = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());
        List<Long> diff = groupIdStaros.stream().filter(e -> {
            return !groupIdFe.contains(e);
        }).collect(Collectors.toList());

        // for debug
        LOG.info("diff.size is {}", diff.size());
        LOG.info("diff is {}", diff);

        // 1.4.collect redundant shard groups and delete
        List<Long> emptyShardGroup = new ArrayList<>();
        for (long groupId : diff) {
            if (System.currentTimeMillis() - Long.valueOf(groupToCreateTimeMap.get(groupId))
                    > Config.shard_group_clean_interval) {
                try {
                    List<Long> shardIds = starOSAgent.listShard(groupId);
                    if (shardIds.isEmpty()) {
                        emptyShardGroup.add(groupId);
                    } else {
                        dropTabletAndDeleteShard(shardIds, starOSAgent);
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        }

        // for debug
        LOG.info("Config.shard_group_clean_interval is {}", Config.shard_group_clean_interval);
        LOG.info("emptyShardGroup.size is {}", emptyShardGroup.size());

        if (!emptyShardGroup.isEmpty()) {
            starOSAgent.deleteShardGroup(emptyShardGroup);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        deleteUnusedShards();
    }
}