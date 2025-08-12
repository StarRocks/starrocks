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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.DoubleCounterMetric;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.proto.DeleteTabletRequest;
import com.starrocks.proto.DeleteTabletResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class StarMgrMetaSyncer extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(StarMgrMetaSyncer.class);

    private static final LongCounterMetric SHARD_GROUP_DELETE_COUNTER = new LongCounterMetric(
            "starmgr_meta_sync_shard_group_deletion_total", Metric.MetricUnit.NOUNIT,
            "The number of shard groups deleted by StarMgrMetaSyncer");

    private static final LongCounterMetric SHARD_DELETE_COUNTER = new LongCounterMetric(
            "starmgr_meta_sync_shard_deletion_total", Metric.MetricUnit.NOUNIT,
            "The total number of shards deleted by StarMgrMetaSyncer");

    private static final DoubleCounterMetric META_SYNC_PROCESS_TIME_COST_TOTAL = new DoubleCounterMetric(
            "starmgr_meta_sync_process_time_total", Metric.MetricUnit.SECONDS,
            "The total number of seconds spent on meta sync by StarMgrMetaSyncer");

    // make sure the metrics are registered only once
    private static final AtomicBoolean IS_METRIC_REGISTERED = new AtomicBoolean(false);

    public StarMgrMetaSyncer() {
        super("StarMgrMetaSyncer", Config.star_mgr_meta_sync_interval_sec * 1000L);
    }

    @Override
    public synchronized void start() {
        super.start();
        if (IS_METRIC_REGISTERED.compareAndSet(false, true)) {
            // register metrics
            MetricRepo.addMetric(SHARD_GROUP_DELETE_COUNTER);
            MetricRepo.addMetric(SHARD_DELETE_COUNTER);
            MetricRepo.addMetric(META_SYNC_PROCESS_TIME_COST_TOTAL);
        }
    }

    @VisibleForTesting
    Set<Long> getAllPartitionShardGroupId() {
        HashSet<Long> groupIds = new HashSet<>();
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIdsIncludeRecycleBin();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }
            if (db.isSystemDatabase()) {
                continue;
            }

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
            }
        }
        return groupIds;
    }

    public static void dropTabletAndDeleteShard(ComputeResource computeResource,
                                                List<Long> shardIds, StarOSAgent starOSAgent,
                                                boolean isFileBundling) {
        Preconditions.checkNotNull(starOSAgent);
        Map<Long, Set<Long>> shardIdsByBeMap = new HashMap<>();
        long pickBackendId = -1;
        // group shards by be
        for (long shardId : shardIds) {
            try {
                if (isFileBundling) {
                    if (pickBackendId == -1) {
                        ComputeNode cn = LakeAggregator.chooseAggregatorNode(computeResource);
                        if (cn == null) {
                            throw new NoAliveBackendException("No available compute node found for the operation");
                        }
                        pickBackendId = cn.getId();
                    }
                } else {
                    pickBackendId = starOSAgent.getPrimaryComputeNodeIdByShard(shardId, computeResource.getWorkerGroupId());
                }
                shardIdsByBeMap.computeIfAbsent(pickBackendId, k -> Sets.newHashSet()).add(shardId);
            } catch (StarRocksException ignored1) {
                // ignore error
            }
        }

        Map<Long, Future<DeleteTabletResponse>> futureMap = new HashMap<>();

        for (Map.Entry<Long, Set<Long>> entry : shardIdsByBeMap.entrySet()) {
            long backendId = entry.getKey();
            Set<Long> shards = entry.getValue();

            // 1. drop tablet
            ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .getBackendOrComputeNode(backendId);
            if (node == null) {
                continue;
            }
            DeleteTabletRequest request = new DeleteTabletRequest();
            request.tabletIds = Lists.newArrayList(shards);

            try {
                LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                // Delete tablets in parallel
                futureMap.put(backendId, lakeService.deleteTablet(request));
            } catch (Throwable e) {
                LOG.info("Fail to send the deleteTablet request to host: {}. error: {}. Ignored for now.",
                        node.toString(), e.getMessage());
            }
        }

        Set<Long> shardToDelete = new HashSet<>();
        for (Map.Entry<Long, Future<DeleteTabletResponse>> entry : futureMap.entrySet()) {
            long nodeId = entry.getKey();
            Future<DeleteTabletResponse> future = entry.getValue();
            try {
                DeleteTabletResponse response = future.get();
                Set<Long> shards = shardIdsByBeMap.get(entry.getKey());
                if (response != null && response.failedTablets != null && !response.failedTablets.isEmpty()) {
                    String errorMsg = "";
                    if (response.status != null && response.status.errorMsgs != null &&
                            !response.status.errorMsgs.isEmpty()) {
                        errorMsg = response.status.errorMsgs.get(0);
                    }
                    TStatusCode stCode = TStatusCode.findByValue(response.status.statusCode);
                    LOG.info("Fail to delete tablet from node: {}. StatusCode: {}, Error: {}, failedTablets: {}",
                            nodeId, stCode, errorMsg, response.failedTablets);

                    // ignore INVALID_ARGUMENT error, treat it as success
                    if (stCode != TStatusCode.INVALID_ARGUMENT) {
                        // preserve the shards that failed to delete, don't delete them from starMgr
                        response.failedTablets.forEach(shards::remove);
                    }
                    shardToDelete.addAll(shards);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                LOG.info("Interrupted while waiting for delete tablet response from node:{}, error: {}", nodeId,
                        exception.getMessage());
            } catch (Exception e) {
                LOG.info("Failed to get delete tablet response from node:{}, error: {}", nodeId, e.getMessage());
            }
        }

        // Delete shards from starMgr
        if (!shardToDelete.isEmpty()) {
            try {
                starOSAgent.deleteShards(shardToDelete);
                SHARD_DELETE_COUNTER.increase((long) shardToDelete.size());
            } catch (DdlException e) {
                LOG.info("Failed to delete shard: {} from starMgr, error: {}", shardToDelete, e.getMessage());
            }
        }
    }

    private boolean isSafeToDelete(long shardGroupId, ShardGroupInfo shardInfo) {
        long dbId = 0;
        long tableId = 0;
        long partitionId = 0;
        long indexId = 0;
        try {
            dbId = Long.parseLong(shardInfo.getLabels().get("dbId"));
            tableId = Long.parseLong(shardInfo.getLabels().get("tableId"));
            partitionId =  Long.parseLong(shardInfo.getLabels().get("partitionId"));
            indexId = Long.parseLong(shardInfo.getLabels().get("indexId"));
        } catch (Exception e) {
            LOG.debug("shardGroup:{} labels is not valid, ignore it for now. {}", shardGroupId, e.getMessage());
        }

        if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isTableSafeToDeleteTablet(tableId)) {
            LOG.debug("table with id: {} can not be delete shard for now, because of automated cluster snapshot",
                      tableId);
            return false;
        }

        if (GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isMaterializedIndexInClusterSnapshotInfo(
                dbId, tableId, partitionId, indexId)) {
            LOG.debug("shard group {} can not be delete shard for now, because it exists in cluster snapshot info",
                      shardGroupId);
            return false;
        }

        if (GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isShardGroupIdInClusterSnapshotInfo(
                dbId, tableId, partitionId, shardGroupId)) {
            LOG.debug("shard group {} can not be delete shard for now, because it exists in cluster snapshot info",
                      shardGroupId);
            return false;
        }
        return true;
    }

    /**
     * Delete redundant shard & shard group.
     * 1. List shard groups from FE and from StarMgr
     * 2. Compare the list and get a list of shard groups that in StarMgr but not in FE
     * 3. shard groups with empty shards and older than threshold, will be permanently deleted.
     */
    private void deleteUnusedShardAndShardGroup() {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();

        // Take this timestamp as reference, all ShardGroups created after this timestamp will be safe for sure.
        long creationExpireTime = System.currentTimeMillis() - Config.shard_group_clean_threshold_sec * 1000L;

        Set<Long> groupIdFe = getAllPartitionShardGroupId();
        if (groupIdFe.size() > 100) {
            // Be a gentleman, avoid printing a long lists in log line.
            LOG.debug("There are {} elements in groupIdFe, the first 100 elements in groupIdFe is {}", groupIdFe.size(),
                    groupIdFe.stream().limit(100).collect(Collectors.toList()));
        } else {
            LOG.debug("groupIdFe is {}", groupIdFe);
        }

        long nextShardGroupId = 0;
        do {
            StarOSAgent.ListShardGroupResult result;
            try {
                result = starOSAgent.listShardGroup(nextShardGroupId);
                nextShardGroupId = result.nextShardGroupId();
            } catch (DdlException exception) {
                LOG.info("Fail to list shardgroup from starmgr: {}. Abort the clean up", exception.getMessage());
                break;
            }

            Map<Long, ShardGroupInfo> diffGroupInfoMap = new HashMap<>();
            result.shardGroupInfos().stream()
                    .filter(x -> x.getGroupId() != 0)
                    .filter(x -> !groupIdFe.contains(x.getGroupId()))
                    .forEach(x -> diffGroupInfoMap.put(x.getGroupId(), x));

            for (Map.Entry<Long, ShardGroupInfo> entry : diffGroupInfoMap.entrySet()) {
                long shardGroupId = entry.getKey();
                ShardGroupInfo shardGroupInfo = entry.getValue();
                if (!isSafeToDelete(shardGroupId, shardGroupInfo)) {
                    continue;
                }

                long createTimeTs = Long.parseLong(shardGroupInfo.getPropertiesOrDefault("createTime", "0"));
                if (createTimeTs == 0) {
                    LOG.debug("Can't parse createTime from shardGroup:{} properties, ignore it for now.", shardGroupId);
                    continue;
                }

                if (createTimeTs < creationExpireTime) {
                    ComputeResource computeResourceLocal = computeResource;
                    try {
                        long tableId = Long.parseLong(shardGroupInfo.getLabels().get("tableId"));
                        computeResourceLocal = GlobalStateMgr.getCurrentState().getWarehouseMgr()
                                .getBackgroundComputeResource(tableId);
                    } catch (Exception e) {
                        LOG.debug("can not get background compute resource, {}", e.getMessage());
                        // continue, default compute resource is already set
                    }
                    if (cleanOneGroup(computeResourceLocal, shardGroupId, starOSAgent)) {
                        // clear the empty shard group immediately
                        starOSAgent.deleteShardGroup(Collections.singletonList(shardGroupId));
                        SHARD_GROUP_DELETE_COUNTER.increase(1L);
                    }
                }
            }
        } while (nextShardGroupId != 0);
    }

    /**
     * Clean one shard group, delete all shards in this group from starMgr and FE.
     * If Config.meta_sync_force_delete_shard_meta is true, only delete shard meta from starMgr,
     * otherwise, delete shard meta and data from FE.
     *
     * @param computeResource the compute resource to use for deleting tablets
     * @param groupId         the shard group id to clean
     * @param starOSAgent     the StarOS agent to interact with StarMgr
     * @return true if the shard group is empty, false otherwise
     */
    @VisibleForTesting
    boolean cleanOneGroup(ComputeResource computeResource, long groupId, StarOSAgent starOSAgent) {
        try {
            List<Long> shardIds = starOSAgent.listShard(groupId);
            if (shardIds.isEmpty()) {
                return true;
            }
            // delete shard from star manager only, not considering tablet data on be/cn
            if (Config.meta_sync_force_delete_shard_meta) {
                forceDeleteShards(groupId, starOSAgent, shardIds);
            } else {
                // drop meta and data
                long start = System.currentTimeMillis();
                // Since the DB and table cannot be determined, the file bundle flag is uniformly set to true, 
                // allowing a single BE node to complete the tablet deletion. 
                // Here, even for tables without file bundle enabled, 
                // the tablet deletion can still be performed by a single node.
                dropTabletAndDeleteShard(computeResource, shardIds, starOSAgent, true);
                LOG.debug("delete shards from starMgr and FE, shard group: {}, cost: {} ms",
                        groupId, (System.currentTimeMillis() - start));
            }
        } catch (Exception e) {
            LOG.warn("delete shards from starMgr and FE failed, shard group: {}, {}", groupId, e.getMessage());
        }
        return false;
    }

    private static void forceDeleteShards(long groupId, StarOSAgent starOSAgent, List<Long> shardIds)
            throws DdlException {
        LOG.debug("delete shards from starMgr only, shard group: {}", groupId);
        // before deleting shardIds, let's record the root directory of this shard group first
        // root directory has the format like `s3://bucket/xx/db15570/15648/15944`
        String rootDirectory = null;
        long shardId = shardIds.get(0);
        try {
            // all shards have the same root directory
            ShardInfo shardInfo = starOSAgent.getShardInfo(shardId, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
            if (shardInfo != null) {
                rootDirectory = shardInfo.getFilePath().getFullPath();
            }
        } catch (Exception e) {
            LOG.warn("failed to get shard root directory from starMgr, shard id: {}, group id: {}, {}", shardId,
                    groupId, e.getMessage());
        }
        starOSAgent.deleteShards(new HashSet<>(shardIds));
        SHARD_DELETE_COUNTER.increase((long) shardIds.size());
        if (StringUtils.isNotEmpty(rootDirectory)) {
            LOG.info("shard group {} deleted from starMgr only, you may need to delete remote file path manually," +
                    " file path is: {}", groupId, rootDirectory);
        }
    }

    // get snapshot of star mgr workers and fe backend/compute node,
    // if worker not found in backend/compute node, remove it from star mgr
    public int deleteUnusedWorker() {
        int cnt = 0;
        try {
            final long workerGroupId = computeResource.getWorkerGroupId();
            List<String> workerAddresses = GlobalStateMgr.getCurrentState().getStarOSAgent().listWorkerGroupIpPort(workerGroupId);

            // filter backend
            List<Backend> backends = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends();
            for (Backend backend : backends) {
                if (backend.getStarletPort() != 0) {
                    String workerAddr = NetUtils.getHostPortInAccessibleFormat(backend.getHost(),
                            backend.getStarletPort());
                    workerAddresses.remove(workerAddr);
                }
            }

            // filter compute node
            List<ComputeNode> computeNodes = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getComputeNodes();
            for (ComputeNode computeNode : computeNodes) {
                if (computeNode.getStarletPort() != 0) {
                    String workerAddr = NetUtils.getHostPortInAccessibleFormat(computeNode.getHost(),
                            computeNode.getStarletPort());
                    workerAddresses.remove(workerAddr);
                }
            }

            for (String unusedWorkerAddress : workerAddresses) {
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(unusedWorkerAddress, workerGroupId);
                LOG.info("unused worker {} removed from star mgr", unusedWorkerAddress);
                cnt++;
            }
        } catch (Exception e) {
            LOG.warn("fail to delete unused worker, {}", e);
        }
        return cnt;
    }

    public void syncTableMetaAndColocationInfo() {
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }
            if (db.isSystemDatabase()) {
                continue;
            }

            List<Table> tables = GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
            for (Table table : tables) {
                if (!table.isCloudNativeTableOrMaterializedView()) {
                    continue;
                }
                if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isTableSafeToDeleteTablet(table.getId())) {
                    LOG.debug("table: {} can not be synced meta for now, because of automated cluster snapshot", table.getName());
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

                    if (GlobalStateMgr.getCurrentState()
                                      .getClusterSnapshotMgr().isMaterializedIndexInClusterSnapshotInfo(
                                            db.getId(), table.getId(), physicalPartition.getParentId(),
                                                physicalPartition.getId(), materializedIndex.getId())) {
                        continue;
                    }

                    if (GlobalStateMgr.getCurrentState()
                                      .getClusterSnapshotMgr().isShardGroupIdInClusterSnapshotInfo(
                                            db.getId(), table.getId(), physicalPartition.getParentId(),
                                                physicalPartition.getId(), materializedIndex.getShardGroupId())) {
                        continue;
                    }
                    // collect shard in starmgr but not in fe
                    redundantGroupToShards.put(materializedIndex.getShardGroupId(), starmgrShardIdsSet);
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }

        // try to delete data, if fail, still delete redundant shard meta in starmgr
        Set<Long> shardToDelete = new HashSet<>();
        for (Map.Entry<Long, Set<Long>> entry : redundantGroupToShards.entrySet()) {
            if (forceDeleteData) {
                try {
                    List<Long> shardIds = new ArrayList<>();
                    shardIds.addAll(entry.getValue());
                    dropTabletAndDeleteShard(computeResource, shardIds, starOSAgent, table.isFileBundling());
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
            SHARD_DELETE_COUNTER.increase((long) shardToDelete.size());
        }
        return !shardToDelete.isEmpty();
    }

    private void syncTableColocationInfo(Database db, OlapTable table) throws DdlException {
        // quick check
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
        long start = System.currentTimeMillis();
        acquireBackgroundComputeResource();
        deleteUnusedShardAndShardGroup();
        deleteUnusedWorker();
        syncTableMetaAndColocationInfo();
        long end = System.currentTimeMillis();
        META_SYNC_PROCESS_TIME_COST_TOTAL.increase((end - start) / 1000.0);
    }

    public void syncTableMeta(String dbName, String tableName, boolean forceDeleteData) throws DdlException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new DdlException(String.format("db %s does not exist.", dbName));
        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        if (table == null) {
            throw new DdlException(String.format("table %s does not exist.", tableName));
        }
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new DdlException("only support cloud table or cloud mv.");
        }
        if (!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isTableSafeToDeleteTablet(table.getId())) {
            throw new DdlException("table: " + table.getName() +
                                   " can not be synced meta for now, because of automated cluster snapshot");
        }

        syncTableMetaAndColocationInfoInternal(db, (OlapTable) table, forceDeleteData);
    }
}
