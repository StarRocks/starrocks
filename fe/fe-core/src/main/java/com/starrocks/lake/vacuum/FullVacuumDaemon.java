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
package com.starrocks.lake.vacuum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.proto.VacuumFullRequest;
import com.starrocks.proto.VacuumFullResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.starrocks.rpc.LakeService.TIMEOUT_VACUUM_FULL;

public class FullVacuumDaemon extends FrontendDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(FullVacuumDaemon.class);

    private static final long MILLISECONDS_PER_SECOND = 1000;

    private final Set<Long> vacuumingPartitions = Sets.newConcurrentHashSet();

    private final BlockingThreadPoolExecutorService executorService =
            BlockingThreadPoolExecutorService.newInstance(Config.lake_fullvacuum_parallel_partitions, 0, TIMEOUT_VACUUM_FULL,
                    TimeUnit.MILLISECONDS, "fullvacuum");

    public FullVacuumDaemon() {
        // Check every minute if we should run a full vacuum
        super("FullVacuumDaemon", 1000 * 60);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        List<Long> dbIds = GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds();
        for (Long dbId : dbIds) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (db == null) {
                continue;
            }

            List<Table> tables = new ArrayList<>();
            for (Table table : GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId())) {
                if (table.isCloudNativeTableOrMaterializedView()) {
                    tables.add(table);
                }
            }

            // Full vacuum cleans up two types of data (orphan data files and redundant db/table/partition)
            // 1. Cleanup Orphan Data Files: Data files not referenced by tablet metadata.
            for (Table table : tables) {
                vacuumTable(db, table);
            }
            //  2. Redundant db/table/partition: Dbs, tables, or partitions that have been deleted in FE but not in the object storage.

        }
    }

    public boolean shouldVacuum(PhysicalPartition partition) {
        long current = System.currentTimeMillis();
        if (partition.getLastFullVacuumTime() == 0) {
            // init LastFullVacuumTime
            partition.setLastFullVacuumTime(current);
            return false;
        }
        // prevent vacuum too frequent
        return current >= partition.getLastFullVacuumTime() + Config.lake_fullvacuum_partition_naptime_seconds * 1000;
    }

    private void vacuumTable(Database db, Table baseTable) {
        OlapTable table = (OlapTable) baseTable;
        List<PhysicalPartition> partitions;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(baseTable.getId()), LockType.READ);
        try {
            partitions = table.getPhysicalPartitions().stream().filter(this::shouldVacuum).toList();
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(baseTable.getId()), LockType.READ);
        }

        for (PhysicalPartition partition : partitions) {
            if (vacuumingPartitions.add(partition.getId())) {
                executorService.execute(() -> vacuumPartition(db, table, partition));
            }
        }
    }

    private void vacuumPartition(Database db, OlapTable table, PhysicalPartition partition) {
        try {
            vacuumPartitionImpl(db, table, partition);
        } finally {
            vacuumingPartitions.remove(partition.getId());
        }
    }

    private void vacuumPartitionImpl(Database db, OlapTable table, PhysicalPartition partition) {
        LOG.info("Running orphan file deletion task for table={}, partition={}.", table.getName(), partition.getId());
        List<Tablet> tablets = new ArrayList<>();
        long visibleVersion;
        long startTime = System.currentTimeMillis();
        long minActiveTxnId = computeMinActiveTxnId(db, table);

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)) {
                tablets.addAll(index.getTablets());
            }
            visibleVersion = partition.getVisibleVersion();
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }

        if (visibleVersion <= 1) {
            LOG.info("skipping full vacuum of partition={} because its visible version is {}", partition.getId(), visibleVersion);
            partition.setLastFullVacuumTime(startTime);
            return;
        }

        ClusterSnapshotMgr clusterSnapshotMgr = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Set<ComputeNode> involvedNodes = new HashSet<>();
        Map<ComputeNode, Tablet> nodeToTablet = new HashMap<>();

        for (Tablet tablet : tablets) {
            ComputeNode node = warehouseManager.getComputeNodeAssignedToTablet(WarehouseManager.DEFAULT_RESOURCE, tablet.getId());

            if (node == null) {
                LOG.error("Could not get CN for tablet={}, returning early.", tablet.getId());
                return;
            }
            involvedNodes.add(node);
            nodeToTablet.put(node, tablet); // save any one tablet of this node
        }

        if (involvedNodes.isEmpty()) {
            LOG.error("Could not find any CN node for full vacuum, returning early.");
            return;
        }

        // choose a node for full vacuum by random
        List<ComputeNode> involvedNodesList = involvedNodes.stream().collect(Collectors.toList());
        Random random = new Random();
        ComputeNode chosenNode = involvedNodesList.get(random.nextInt(involvedNodesList.size()));

        VacuumFullRequest vacuumFullRequest = new VacuumFullRequest();
        vacuumFullRequest.setPartitionId(partition.getId());
        vacuumFullRequest.setMinActiveTxnId(minActiveTxnId);

        long graceTimestamp = startTime / MILLISECONDS_PER_SECOND - Config.lake_fullvacuum_meta_expired_seconds;
        graceTimestamp = Math.min(graceTimestamp,
                                  Math.max(clusterSnapshotMgr.getSafeDeletionTimeMs() / MILLISECONDS_PER_SECOND, 1));
        vacuumFullRequest.setGraceTimestamp(graceTimestamp);

        List<Long> retainVersions = new ArrayList<>();
        retainVersions.addAll(clusterSnapshotMgr.getVacuumRetainVersions(
                              db.getId(), table.getId(), partition.getParentId(), partition.getId()));
        if (!retainVersions.contains(visibleVersion)) {
            retainVersions.add(visibleVersion); // current visibleVersion should be retained 
        }
        long minCheckVersion = 0;
        long maxCheckVersion = visibleVersion; // always should be inited by current visibleVersion
        vacuumFullRequest.setMinCheckVersion(minCheckVersion);
        vacuumFullRequest.setMaxCheckVersion(maxCheckVersion);
        vacuumFullRequest.setRetainVersions(retainVersions);
        vacuumFullRequest.setTabletId(nodeToTablet.get(chosenNode).getId());

        LOG.info(
                "Sending full vacuum request to cn={}: table={}, partition={}, max_check_version={}, " + "min_active_txn_id={}",
                chosenNode.getHost(), table.getName(), vacuumFullRequest.getPartitionId(), vacuumFullRequest.maxCheckVersion,
                vacuumFullRequest.minActiveTxnId);


        boolean hasError = false;
        long vacuumedFiles = 0;
        long vacuumedFileSize = 0;
        long vacuumedVersion = Long.MAX_VALUE;
        Future<VacuumFullResponse> responseFuture = null;
        try {
            LakeService service = BrpcProxy.getLakeService(chosenNode.getHost(), chosenNode.getBrpcPort());
            responseFuture = service.vacuumFull(vacuumFullRequest);
        } catch (RpcException e) {
            LOG.error("failed to send full vacuum request for partition {}.{}.{}", db.getFullName(), table.getName(),
                    partition.getId(), e);
            hasError = true;
        }

        try {
            if (responseFuture != null) {
                VacuumFullResponse response = responseFuture.get();
                if (response.status.statusCode != 0) {
                    hasError = true;
                    LOG.warn("Vacuumed {}.{}.{} with error: {}", db.getFullName(), table.getName(), partition.getId(),
                             response.status.errorMsgs != null && !response.status.errorMsgs.isEmpty() ?
                             response.status.errorMsgs.get(0) : "");
                } else {
                    vacuumedFiles += response.vacuumedFiles;
                    vacuumedFileSize += response.vacuumedFileSize;
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("thread interrupted");
            Thread.currentThread().interrupt();
            hasError = true;
        } catch (ExecutionException e) {
            LOG.error("failed to full vacuum {}.{}.{}: {}", db.getFullName(), table.getName(), partition.getId(),
                    e.getMessage());
            hasError = true;
        }

        partition.setLastFullVacuumTime(startTime);

        LOG.info("Full vacuumed {}.{}.{} hasError={} vacuumedFiles={} vacuumedFileSize={} " +
                        "visibleVersion={} minActiveTxnId={} vacuumVersion={} cost={}ms", db.getFullName(), table.getName(),
                partition.getId(), hasError, vacuumedFiles, vacuumedFileSize, visibleVersion, minActiveTxnId, vacuumedVersion,
                System.currentTimeMillis() - startTime);
    }

    @VisibleForTesting
    public static long computeMinActiveTxnId(Database db, Table table) {
        long a = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getMinActiveTxnIdOfDatabase(db.getId());
        Optional<Long> b = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getActiveTxnIdOfTable(table.getId());
        return Math.min(a, b.orElse(Long.MAX_VALUE));
    }
}
