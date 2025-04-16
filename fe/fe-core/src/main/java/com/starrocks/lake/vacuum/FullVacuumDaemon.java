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
import com.starrocks.lake.LakeTablet;
import com.starrocks.proto.VacuumFullRequest;
import com.starrocks.proto.VacuumFullResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.starrocks.rpc.LakeService.TIMEOUT_VACUUM_FULL;

public class FullVacuumDaemon extends FrontendDaemon implements Writable {
    private static final Logger LOG = LogManager.getLogger(FullVacuumDaemon.class);

    private final Set<Long> vacuumingPartitions = Sets.newConcurrentHashSet();

    private final Map<Long, AtomicInteger> cnIdToRunningFullVacuum = new ConcurrentHashMap<>();

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
            // TODO Implement

        }
    }

    public boolean shouldVacuum(PhysicalPartition partition) {
        long current = System.currentTimeMillis();
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

    private ComputeNode selectNodeWithMinVacuum(Collection<ComputeNode> involvedNodes) {
        ComputeNode chosenNode = null;
        int minRunning = Integer.MAX_VALUE;

        for (ComputeNode node : involvedNodes) {
            AtomicInteger currentlyRunningFullVacuum =
                    cnIdToRunningFullVacuum.computeIfAbsent(node.getId(), id -> new AtomicInteger(0));

            int currentRunning = currentlyRunningFullVacuum.get();
            if (currentRunning < minRunning) {
                minRunning = currentRunning;
                chosenNode = node;
            }
        }

        if (chosenNode == null || chosenNode.getHost() == null) {
            LOG.error("Could not find usable ComputeNode to send full vacuum. Problem: {}",
                    chosenNode == null ? "ComputeNode is null" : "Host is null");
            return null;
        } else {
            cnIdToRunningFullVacuum.get(chosenNode.getId()).incrementAndGet();
        }

        return chosenNode;
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
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
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

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = warehouseManager.getBackgroundWarehouse();
        List<Long> allTabletIds = new ArrayList<>(tablets.size());
        Set<ComputeNode> involvedNodes = new HashSet<>();

        for (Tablet tablet : tablets) {
            LakeTablet lakeTablet = (LakeTablet) tablet;
            ComputeNode node = warehouseManager.getComputeNodeAssignedToTablet(warehouse.getId(), lakeTablet);

            if (node == null) {
                LOG.error("Could not get CN for tablet={}, returning early.", tablet.getId());
                return;
            }
            allTabletIds.add(tablet.getId());
            involvedNodes.add(node);
        }
        VacuumFullRequest vacuumFullRequest = new VacuumFullRequest();
        vacuumFullRequest.setPartitionId(partition.getId());
        // All tablet ids must be part of the request, since the CN will delete any + all tablets unspecified by the request.
        vacuumFullRequest.setTabletIds(allTabletIds);
        // TODO(cbrennan) min check version is an optimization to save compute on deleting metadata, but at some point this
        //  should be something other than the default of 1.
        vacuumFullRequest.setMinCheckVersion(1L);
        vacuumFullRequest.setMaxCheckVersion(visibleVersion);
        vacuumFullRequest.setMinActiveTxnId(minActiveTxnId);

        ComputeNode chosenNode = selectNodeWithMinVacuum(involvedNodes);
        if (chosenNode == null) {
            return;
        }

        LOG.info(
                "Sending full vacuum request to cn={}: table={}, partition={}, tablet_ids={}, max_check_version={}, " +
                        "min_active_txn_id={}",
                chosenNode.getHost(), table.getName(), vacuumFullRequest.getPartitionId(), vacuumFullRequest.getTabletIds(),
                vacuumFullRequest.maxCheckVersion, vacuumFullRequest.minActiveTxnId);


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
            VacuumFullResponse response = responseFuture.get();
            if (response.status.statusCode != 0) {
                hasError = true;
                LOG.warn("Vacuumed {}.{}.{} with error: {}", db.getFullName(), table.getName(), partition.getId(),
                        response.status.errorMsgs.get(0));
            } else {
                vacuumedFiles += response.vacuumedFiles;
                vacuumedFileSize += response.vacuumedFileSize;
            }
        } catch (InterruptedException e) {
            LOG.warn("thread interrupted");
            Thread.currentThread().interrupt();
            hasError = true;
        } catch (ExecutionException e) {
            LOG.error("failed to full vacuum {}.{}.{}: {}", db.getFullName(), table.getName(), partition.getId(),
                    e.getMessage());
            hasError = true;
        } finally {
            cnIdToRunningFullVacuum.get(chosenNode.getId()).decrementAndGet();
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

    public void testVacuumPartitionImpl(Database db, OlapTable table, PhysicalPartition partition) {
        vacuumPartitionImpl(db, table, partition);
    }
}
