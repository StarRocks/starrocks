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

<<<<<<< HEAD

package com.starrocks.lake;

import autovalue.shaded.com.google.common.common.collect.Lists;
import autovalue.shaded.com.google.common.common.collect.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
=======
package com.starrocks.lake;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.staros.proto.ShardGroupInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
<<<<<<< HEAD
=======
import com.starrocks.catalog.PhysicalPartition;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
<<<<<<< HEAD
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
=======
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
=======
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Warehouse;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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

    private List<Long> getAllPartitionShardGroupId() {
        List<Long> groupIds = new ArrayList<>();
<<<<<<< HEAD
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIdsIncludeRecycleBin();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(dbId);
=======
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIdsIncludeRecycleBin();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(dbId);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            if (db == null) {
                continue;
            }
            if (db.isSystemDatabase()) {
                continue;
            }

<<<<<<< HEAD
            db.readLock();
            try {
                for (Table table : GlobalStateMgr.getCurrentState().getTablesIncludeRecycleBin(db)) {
                    if (table.isCloudNativeTableOrMaterializedView()) {
                        GlobalStateMgr.getCurrentState()
                                .getAllPartitionsIncludeRecycleBin((OlapTable) table)
                                .stream()
                                .map(Partition::getShardGroupId)
                                .forEach(groupIds::add);
                    }
                }
            } finally {
                db.readUnlock();
=======
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTablesIncludeRecycleBin(db)) {
                    if (table.isCloudNativeTableOrMaterializedView()) {
                        GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .getAllPartitionsIncludeRecycleBin((OlapTable) table)
                                .stream()
                                .map(Partition::getSubPartitions)
                                .flatMap(p -> p.stream().map(PhysicalPartition::getShardGroupIds))
                                .forEach(groupIds::addAll);
                    }
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
                long backendId = starOSAgent.getPrimaryComputeNodeIdByShard(shardId);
                shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
            } catch (UserException ignored1) {
=======
                WarehouseManager manager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
                Warehouse warehouse = manager.getBackgroundWarehouse();
                long workerGroupId = manager.selectWorkerGroupByWarehouseId(warehouse.getId())
                        .orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                long backendId = starOSAgent.getPrimaryComputeNodeIdByShard(shardId, workerGroupId);
                shardIdsByBeMap.computeIfAbsent(backendId, k -> Sets.newHashSet()).add(shardId);
            } catch (StarRocksException ignored1) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                // ignore error
            }
        }

        for (Map.Entry<Long, Set<Long>> entry : shardIdsByBeMap.entrySet()) {
            long backendId = entry.getKey();
            Set<Long> shards = entry.getValue();

            // 1. drop tablet
<<<<<<< HEAD
            ComputeNode node = GlobalStateMgr.getCurrentState().getCurrentSystemInfo().getBackendOrComputeNode(backendId);
=======
            ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getBackendOrComputeNode(backendId);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
                LOG.error(e);
=======
                LOG.error(e.getMessage(), e);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentStarOSAgent();

        List<Long> groupIdFe = getAllPartitionShardGroupId();
        List<ShardGroupInfo> shardGroupsInfo = starOSAgent.listShardGroup()
                        .stream()
                        .filter(x -> x.getGroupId() != 0L)
                        .collect(Collectors.toList());
=======
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();

        List<Long> groupIdFe = getAllPartitionShardGroupId();
        List<ShardGroupInfo> shardGroupsInfo = starOSAgent.listShardGroup()
                .stream()
                .filter(x -> x.getGroupId() != 0L)
                .collect(Collectors.toList());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

        if (shardGroupsInfo.isEmpty()) {
            return;
        }

        LOG.debug("size of groupIdFe is {}, size of shardGroupsInfo is {}",
                groupIdFe.size(), shardGroupsInfo.size());
        LOG.debug("groupIdFe is {}", groupIdFe);

        Map<Long, String> groupToCreateTimeMap = shardGroupsInfo.stream().collect(Collectors.toMap(
                ShardGroupInfo::getGroupId,
                val -> val.getPropertiesMap().get("createTime"),
                (key1, key2) -> key1
        ));

        List<Long> diffList = shardGroupsInfo.stream()
                .map(ShardGroupInfo::getGroupId)
                .filter(x -> !groupIdFe.contains(x))
                .collect(Collectors.toList());
        LOG.debug("diff.size is {}, diff: {}", diffList.size(), diffList);

        // 1.4.collect redundant shard groups and delete
        long nowMs = System.currentTimeMillis();
        List<Long> emptyShardGroup = new ArrayList<>();
        for (long groupId : diffList) {
            if (Config.shard_group_clean_threshold_sec * 1000L + Long.parseLong(groupToCreateTimeMap.get(groupId)) < nowMs) {
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
<<<<<<< HEAD
            List<String> workerAddresses = GlobalStateMgr.getCurrentStarOSAgent().listDefaultWorkerGroupIpPort();

            // filter backend
            List<Backend> backends = GlobalStateMgr.getCurrentSystemInfo().getBackends();
            for (Backend backend : backends) {
                if (backend.getStarletPort() != 0) {
                    String workerAddr = backend.getHost() + ":" + backend.getStarletPort();
=======
            WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
            Warehouse warehouse = warehouseManager.getBackgroundWarehouse();
            long workerGroupId = warehouseManager.selectWorkerGroupByWarehouseId(warehouse.getId())
                    .orElse(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            List<String> workerAddresses = GlobalStateMgr.getCurrentState().getStarOSAgent().listWorkerGroupIpPort(workerGroupId);

            // filter backend
            List<Backend> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends();
            for (Backend backend : backends) {
                if (backend.getStarletPort() != 0) {
                    String workerAddr = NetUtils.getHostPortInAccessibleFormat(backend.getHost(),
                            backend.getStarletPort());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    workerAddresses.remove(workerAddr);
                }
            }

            // filter compute node
<<<<<<< HEAD
            ImmutableCollection<ComputeNode> computeNodes =
                    GlobalStateMgr.getCurrentSystemInfo().getComputeNodes(false /* needAlive */);
            for (ComputeNode computeNode : computeNodes) {
                if (computeNode.getStarletPort() != 0) {
                    String workerAddr = computeNode.getHost() + ":" + computeNode.getStarletPort();
=======
            List<ComputeNode> computeNodes = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodes();
            for (ComputeNode computeNode : computeNodes) {
                if (computeNode.getStarletPort() != 0) {
                    String workerAddr = NetUtils.getHostPortInAccessibleFormat(computeNode.getHost(),
                            computeNode.getStarletPort());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                    workerAddresses.remove(workerAddr);
                }
            }

            for (String unusedWorkerAddress : workerAddresses) {
<<<<<<< HEAD
                GlobalStateMgr.getCurrentStarOSAgent().removeWorker(unusedWorkerAddress);
=======
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(unusedWorkerAddress, workerGroupId);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                LOG.info("unused worker {} removed from star mgr", unusedWorkerAddress);
                cnt++;
            }
        } catch (Exception e) {
            LOG.warn("fail to delete unused worker, {}", e);
        }
        return cnt;
    }

    public void syncTableMetaAndColocationInfo() {
<<<<<<< HEAD
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
=======
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            if (db == null) {
                continue;
            }
            if (db.isSystemDatabase()) {
                continue;
            }

<<<<<<< HEAD
            List<Table> tables = db.getTables();
=======
            List<Table> tables = GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
    private boolean syncTableMetaInternal(Database db, OlapTable table, boolean forceDeleteData) throws DdlException {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentStarOSAgent();
        HashMap<Long, Set<Long>> redundantGroupToShards = new HashMap<>();
        List<Partition> partitions = new ArrayList<>();
        db.readLock();
        try {
            if (db.getTable(table.getId()) == null) {
                return false; // table might be dropped
            }
            GlobalStateMgr.getCurrentState()
                    .getAllPartitionsIncludeRecycleBin((OlapTable) table)
                    .stream()
                    .forEach(partitions::add);
        } finally {
            db.readUnlock();
        }

        for (Partition partition : partitions) {
            db.readLock();
            try {
                if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                    return false; // table might be in schema change
                }
                // no need to check db/table/partition again, everything still works
                long groupId = partition.getShardGroupId();
                List<Long> starmgrShardIds = starOSAgent.listShard(groupId);
                Set<Long> starmgrShardIdsSet = new HashSet<>(starmgrShardIds);
                for (MaterializedIndex materializedIndex :
                        partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    for (Tablet tablet : materializedIndex.getTablets()) {
                        starmgrShardIdsSet.remove(tablet.getId());
                    }
                }
                // collect shard in starmgr but not in fe
                redundantGroupToShards.put(groupId, starmgrShardIdsSet);
            } finally {
                db.readUnlock();
=======
    @VisibleForTesting
    public boolean syncTableMetaInternal(Database db, OlapTable table, boolean forceDeleteData) throws DdlException {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        HashMap<Long, Set<Long>> redundantGroupToShards = new HashMap<>();
        List<PhysicalPartition> physicalPartitions = new ArrayList<>();
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            if (GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), table.getId()) == null) {
                return false; // table might be dropped
            }
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getAllPartitionsIncludeRecycleBin(table)
                    .stream()
                    .map(Partition::getSubPartitions)
                    .forEach(physicalPartitions::addAll);
            table.setShardGroupChanged(false);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        for (PhysicalPartition physicalPartition : physicalPartitions) {
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                // schema change might replace the shards in the original shard group
                if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                    return false;
                }
                // automatic bucketing will create new shards in the original shard group
                if (table.isAutomaticBucketing()) {
                    return false;
                }
                // automatic bucketing will change physicalPartitions make shard group changed even after it's done
                if (table.hasShardGroupChanged()) {
                    return false;
                }

                // no need to check db/table/partition again, everything still works
                for (MaterializedIndex materializedIndex :
                        physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                    long groupId = materializedIndex.getShardGroupId();
                    Set<Long> starmgrShardIdsSet = null;
                    if (redundantGroupToShards.get(groupId) != null) {
                        starmgrShardIdsSet = redundantGroupToShards.get(groupId);
                    } else {
                        List<Long> starmgrShardIds = starOSAgent.listShard(groupId);
                        starmgrShardIdsSet = new HashSet<>(starmgrShardIds);
                    }

                    for (Tablet tablet : materializedIndex.getTablets()) {
                        starmgrShardIdsSet.remove(tablet.getId());
                    }
                    // collect shard in starmgr but not in fe
                    redundantGroupToShards.put(materializedIndex.getShardGroupId(), starmgrShardIdsSet);
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
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
=======
        if (!GlobalStateMgr.getCurrentState().getColocateTableIndex().isLakeColocateTable(table.getId())) {
            return;
        }
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.WRITE);
        try {
            // check db and table again
            if (GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(db.getId()) == null) {
                return;
            }
            if (GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), table.getId()) == null) {
                return;
            }
            GlobalStateMgr.getCurrentState().getColocateTableIndex().updateLakeTableColocationInfo(table, true /* isJoin */,
                    null /* expectGroupId */);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.WRITE);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
=======
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (db == null) {
            throw new DdlException(String.format("db %s does not exist.", dbName));
        }

<<<<<<< HEAD
        Table table = db.getTable(tableName);
=======
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (table == null) {
            throw new DdlException(String.format("table %s does not exist.", tableName));
        }
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new DdlException("only support cloud table or cloud mv.");
        }

        syncTableMetaAndColocationInfoInternal(db, (OlapTable) table, forceDeleteData);
    }
}
