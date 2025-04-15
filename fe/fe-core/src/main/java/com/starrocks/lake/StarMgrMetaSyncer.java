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

import autovalue.shaded.com.google.common.common.collect.Lists;
import autovalue.shaded.com.google.common.common.collect.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.staros.proto.ShardGroupInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StarMgrMetaSyncer extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(StarMgrMetaSyncer.class);

    public StarMgrMetaSyncer() {
        super("StarMgrMetaSyncer", Config.star_mgr_meta_sync_interval_sec * 1000L);
    }

    @VisibleForTesting
    Set<Long> getAllPartitionShardGroupId() {
        HashSet<Long> groupIds = new HashSet<>();
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIdsIncludeRecycleBin();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }
            if (db.isSystemDatabase()) {
                continue;
            }

            db.readLock();
            try {
                for (Table table : GlobalStateMgr.getCurrentState().getTablesIncludeRecycleBin(db)) {
                    if (table.isCloudNativeTableOrMaterializedView()) {
                        GlobalStateMgr.getCurrentState()
                                .getAllPartitionsIncludeRecycleBin((OlapTable) table)
                                .stream()
                                .map(Partition::getSubPartitions)
                                .flatMap(p -> p.stream().map(PhysicalPartition::getShardGroupId))
                                .forEach(groupIds::add);
                    }
                }
            } finally {
                db.readUnlock();
            }
        }
        return groupIds;
    }

    public static void dropTabletAndDeleteShard(List<Long> shardIds, StarOSAgent starOSAgent) {
        Preconditions.checkNotNull(starOSAgent);
        Map<Long, Set<Long>> shardIdsByBeMap = new HashMap<>();
        // group shards by be
        for (long shardId : shardIds) {
            try {
                long backendId = starOSAgent.getPrimaryComputeNodeIdByShard(shardId);
                shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
            } catch (UserException ignored1) {
                // ignore error
            }
        }

        for (Map.Entry<Long, Set<Long>> entry : shardIdsByBeMap.entrySet()) {
            long backendId = entry.getKey();
            Set<Long> shards = entry.getValue();

            // 1. drop tablet
            ComputeNode node = GlobalStateMgr.getCurrentState().getCurrentSystemInfo().getBackendOrComputeNode(backendId);
            if (node == null) {
                continue;
            }
            DeleteTabletRequest request = new DeleteTabletRequest();
            request.tabletIds = Lists.newArrayList(shards);

            boolean forceDelete = Config.meta_sync_force_delete_shard_meta;
            try {
                LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                DeleteTabletResponse response = lakeService.deleteTablet(request).get();
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    TStatusCode stCode = TStatusCode.findByValue(response.status.statusCode);
                    LOG.info("Fail to delete tablet. StatusCode: {}, failedTablets: {}", stCode, response.failedTablets);

                    // ignore INVALID_ARGUMENT error, treat it as success
                    if (stCode != TStatusCode.INVALID_ARGUMENT && !forceDelete) {
                        response.failedTablets.forEach(shards::remove);
                    }
                }
            } catch (Throwable e) {
                LOG.error(e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (!forceDelete) {
                    continue;
                }
            }

            // 2. delete shard
            try {
                if (!shards.isEmpty()) {
                    starOSAgent.deleteShards(shards);
                }
            } catch (DdlException e) {
                LOG.warn("failed to delete shard from starMgr");
                continue;
            }
        }
    }

    /**
     * Delete redundant shard & shard group.
     * 1. List shard groups from FE and from StarMgr
     * 2. Compare the list and get a list of shard groups that in StarMgr but not in FE
     * 3. shard groups with empty shards and older than threshold, will be permanently deleted.
     */
    private void deleteUnusedShardAndShardGroup() {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentStarOSAgent();

        // Take this timestamp as reference, all ShardGroups created after this timestamp will be safe for sure.
        long creationExpireTime = System.currentTimeMillis() - Config.shard_group_clean_threshold_sec * 1000L;

        Set<Long> groupIdFe = getAllPartitionShardGroupId();
        // TODO: use starclient pagination interface to minimize the memory consumption of holding all results in a single list
        List<ShardGroupInfo> shardGroupsInfo = starOSAgent.listShardGroup()
                .stream()
                .filter(x -> x.getGroupId() != 0L)
                .collect(Collectors.toList());
        LOG.debug("size of groupIdFe is {}, size of shardGroupsInfo is {}", groupIdFe.size(), shardGroupsInfo.size());

        if (shardGroupsInfo.isEmpty()) {
            return;
        }
        if (groupIdFe.size() > 100) {
            // Be a gentleman, avoid printing a long lists in log line.
            LOG.debug("first 100 elements in groupIdFe is {}",
                    groupIdFe.stream().limit(100).collect(Collectors.toList()));
        } else {
            LOG.debug("groupIdFe is {}", groupIdFe);
        }

        // Constructing a map with shardGroupId -> ShardGroupInfo with filtering out shardGroups that can be found in partitions
        Map<Long, ShardGroupInfo> diffGroupInfoMap = shardGroupsInfo.stream()
                .filter(x -> !groupIdFe.contains(x.getGroupId()))
                .collect(Collectors.toMap(ShardGroupInfo::getGroupId, val -> val, (key1, key2) -> key1));
        LOG.debug("diff.size is {}, diff: {}", diffGroupInfoMap.size(), diffGroupInfoMap.keySet());

        // 1.4.collect redundant shard groups and delete
        List<Long> emptyShardGroup = new ArrayList<>();
        for (Map.Entry<Long, ShardGroupInfo> entry : diffGroupInfoMap.entrySet()) {
            long shardGroupId = entry.getKey();
            long createTimeTs = Long.parseLong(entry.getValue().getPropertiesOrDefault("createTime", "0"));
            if (createTimeTs == 0) {
                LOG.debug("Can't parse createTime from shardGroup:{} properties, ignore it for now.", shardGroupId);
                continue;
            }

            if (createTimeTs < creationExpireTime) {
                try {
                    List<Long> shardIds = starOSAgent.listShard(shardGroupId);
                    if (shardIds.isEmpty()) {
                        emptyShardGroup.add(shardGroupId);
                    } else {
                        dropTabletAndDeleteShard(shardIds, starOSAgent);
                    }
                } catch (Exception e) {
                    continue;
                }
            }
        }
        LOG.debug("emptyShardGroup.size is {}", emptyShardGroup.size());
        if (!emptyShardGroup.isEmpty()) {
            starOSAgent.deleteShardGroup(emptyShardGroup);
        }
    }

    // get snapshot of star mgr workers and fe backend/compute node,
    // if worker not found in backend/compute node, remove it from star mgr
    public int deleteUnusedWorker() {
        int cnt = 0;
        try {
            List<String> workerAddresses = GlobalStateMgr.getCurrentStarOSAgent().listDefaultWorkerGroupIpPort();

            // filter backend
            List<Backend> backends = GlobalStateMgr.getCurrentSystemInfo().getBackends();
            for (Backend backend : backends) {
                if (backend.getStarletPort() != 0) {
                    String workerAddr = backend.getHost() + ":" + backend.getStarletPort();
                    workerAddresses.remove(workerAddr);
                }
            }

            // filter compute node
            List<ComputeNode> computeNodes = GlobalStateMgr.getCurrentSystemInfo().getComputeNodes();
            for (ComputeNode computeNode : computeNodes) {
                if (computeNode.getStarletPort() != 0) {
                    String workerAddr = computeNode.getHost() + ":" + computeNode.getStarletPort();
                    workerAddresses.remove(workerAddr);
                }
            }

            for (String unusedWorkerAddress : workerAddresses) {
                GlobalStateMgr.getCurrentStarOSAgent().removeWorker(unusedWorkerAddress);
                LOG.info("unused worker {} removed from star mgr", unusedWorkerAddress);
                cnt++;
            }
        } catch (Exception e) {
            LOG.warn("fail to delete unused worker, {}", e);
        }
        return cnt;
    }

    public void syncTableMetaAndColocationInfo() {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db == null) {
                continue;
            }
            if (db.isSystemDatabase()) {
                continue;
            }

            List<Table> tables = db.getTables();
            for (Table table : tables) {
                if (!table.isCloudNativeTableOrMaterializedView()) {
                    continue;
                }
                try {
                    syncTableMetaAndColocationInfoInternal(db, (OlapTable) table, true /* forceDeleteData */);
                } catch (Exception e) {
                    LOG.info("fail to sync table {} meta, {}", table.getName(), e.getMessage());
                }
            }
        }
    }

    // return true if starmgr shard meta changed
    private boolean syncTableMetaInternal(Database db, OlapTable table, boolean forceDeleteData) throws DdlException {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentStarOSAgent();
        HashMap<Long, Set<Long>> redundantGroupToShards = new HashMap<>();
        List<PhysicalPartition> physicalPartitions = new ArrayList<>();
        db.readLock();
        try {
            if (db.getTable(table.getId()) == null) {
                return false; // table might be dropped
            }
            GlobalStateMgr.getCurrentState()
                    .getAllPartitionsIncludeRecycleBin(table)
                    .stream()
                    .map(Partition::getSubPartitions)
                    .forEach(physicalPartitions::addAll);
        } finally {
            db.readUnlock();
        }

        for (PhysicalPartition physicalPartition : physicalPartitions) {
            db.readLock();
            try {
                if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                    return false; // table might be in schema change
                }
                // no need to check db/table/partition again, everything still works
                long groupId = physicalPartition.getShardGroupId();
                Set<Long> starmgrShardIdsSet = null;
                if (redundantGroupToShards.get(groupId) != null) {
                    starmgrShardIdsSet = redundantGroupToShards.get(groupId);
                } else {
                    List<Long> starmgrShardIds = starOSAgent.listShard(groupId);
                    starmgrShardIdsSet = new HashSet<>(starmgrShardIds);
                }
                for (MaterializedIndex materializedIndex :
                        physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        starmgrShardIdsSet.remove(tablet.getId());
                    }
                }
                // collect shard in starmgr but not in fe
                redundantGroupToShards.put(groupId, starmgrShardIdsSet);
            } finally {
                db.readUnlock();
            }
        }

        // try to delete data, if fail, still delete redundant shard meta in starmgr
        Set<Long> shardToDelete = new HashSet<>();
        for (Map.Entry<Long, Set<Long>> entry : redundantGroupToShards.entrySet()) {
            if (forceDeleteData) {
                try {
                    List<Long> shardIds = new ArrayList<>();
                    shardIds.addAll(entry.getValue());
                    dropTabletAndDeleteShard(shardIds, starOSAgent);
                } catch (Exception e) {
                    // ignore exception
                    LOG.info(e.getMessage());
                }
            }
            shardToDelete.addAll(entry.getValue());
        }

        // do final meta delete, regardless whether above tablet deleted or not
        if (!shardToDelete.isEmpty()) {
            starOSAgent.deleteShards(shardToDelete);
        }
        return !shardToDelete.isEmpty();
    }

    private void syncTableColocationInfo(Database db, OlapTable table) throws DdlException {
        // quick check
        if (!GlobalStateMgr.getCurrentColocateIndex().isLakeColocateTable(table.getId())) {
            return;
        }
        db.writeLock();
        try {
            // check db and table again
            if (GlobalStateMgr.getCurrentState().getDb(db.getId()) == null) {
                return;
            }
            if (db.getTable(table.getId()) == null) {
                return;
            }
            GlobalStateMgr.getCurrentColocateIndex().updateLakeTableColocationInfo(table, true /* isJoin */,
                        null /* expectGroupId */);
        } finally {
            db.writeUnlock();
        }
    }

    // delete all shards from this table that exist in starmgr but not in fe(mostly from schema change),
    // and update colocation info
    private void syncTableMetaAndColocationInfoInternal(Database db, OlapTable table, boolean forceDeleteData)
            throws DdlException {
        boolean changed = syncTableMetaInternal(db, table, forceDeleteData);
        // if meta is changed, need to sync colocation info
        if (changed) {
            syncTableColocationInfo(db, table);
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        deleteUnusedShardAndShardGroup();
        deleteUnusedWorker();
        syncTableMetaAndColocationInfo();
    }

    public void syncTableMeta(String dbName, String tableName, boolean forceDeleteData) throws DdlException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException(String.format("db %s does not exist.", dbName));
        }

        Table table = db.getTable(tableName);
        if (table == null) {
            throw new DdlException(String.format("table %s does not exist.", tableName));
        }
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new DdlException("only support cloud table or cloud mv.");
        }

        syncTableMetaAndColocationInfoInternal(db, (OlapTable) table, forceDeleteData);
    }
}
