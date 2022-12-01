// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.lake;

import com.staros.proto.ShardGroupInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
        // for debug
        LOG.info("size of groupIdFe is {}, size of shardGroupInfos is {}",
                groupIdFe.size(),shardGroupInfos.size());
        List<Long> groupIdStaros = shardGroupInfos.stream().map(ShardGroupInfo::getGroupId).collect(Collectors.toList());

        if (shardGroupInfos.isEmpty()) {
            return;
        }

        Map<Long, String> groupToCreateTimeMap =
                shardGroupInfos.stream().collect(Collectors.toMap(
                        obj -> obj.getGroupId(),
                        obj -> obj.getLabelsMap().get("createTime"),
                        (key1, key2) -> key1
                ));

        List<Long> diff = groupIdStaros.stream().filter(e -> {
            return !groupIdFe.contains(e);
        }).collect(Collectors.toList());

        diff.remove(Long.valueOf(0));

        // 1.4.collect redundant shard groups
        List<Long> needDeleteShardGroups = new ArrayList<>();
        for (long groupId : diff) {
            if (Long.valueOf(groupToCreateTimeMap.get(groupId)) - System.currentTimeMillis()
                    > Config.shard_group_clean_interval) {
                needDeleteShardGroups.add(groupId);
            }
        }

        // delete shard group
        if (!needDeleteShardGroups.isEmpty()) {
            starOSAgent.deleteShardGroup(needDeleteShardGroups);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        deleteUnusedShards();
    }
}