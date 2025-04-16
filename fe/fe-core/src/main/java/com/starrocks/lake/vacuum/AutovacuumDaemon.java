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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.proto.TabletInfoPB;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AutovacuumDaemon extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(AutovacuumDaemon.class);

    private static final long MILLISECONDS_PER_SECOND = 1000;
    private static final long SECONDS_PER_MINUTE = 60;
    private static final long MINUTES_PER_HOUR = 60;
    private static final long MILLISECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND;

    private final Set<Long> vacuumingPartitions = Sets.newConcurrentHashSet();
    private final BlockingThreadPoolExecutorService executorService = BlockingThreadPoolExecutorService.newInstance(
            Config.lake_autovacuum_parallel_partitions, 0, 1, TimeUnit.HOURS, "autovacuum");

    public AutovacuumDaemon() {
        super("autovacuum", 2000);
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

            for (Table table : tables) {
                vacuumTable(db, table);
            }
        }
    }

    public boolean shouldVacuum(PhysicalPartition partition) {
        long current = System.currentTimeMillis();
        long staleTime = current - Config.lake_autovacuum_stale_partition_threshold * MILLISECONDS_PER_HOUR;

        if (partition.getVisibleVersionTime() <= staleTime) {
            return false;
        }
        // empty parition
        if (partition.getVisibleVersion() <= 1) {
            return false;
        }
        // prevent vacuum too frequent
        if (current < partition.getLastVacuumTime() + Config.lake_autovacuum_partition_naptime_seconds * 1000) {
            return false;
        }
        if (Config.lake_autovacuum_detect_vaccumed_version) {
            long minRetainVersion = partition.getMinRetainVersion();
            if (minRetainVersion <= 0) {
                minRetainVersion = Math.max(1, partition.getVisibleVersion() - Config.lake_autovacuum_max_previous_versions);
            }
            // the file before minRetainVersion vacuum success
            if (partition.getLastSuccVacuumVersion() >= minRetainVersion) {
                return false;
            }
        }
        // TODO(zhangqiang)
        // add partition data size and storage size on S3 to decide vacuum or not
        return true;
    }

    private void vacuumTable(Database db, Table baseTable) {
        OlapTable table = (OlapTable) baseTable;
        List<PhysicalPartition> partitions;

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(baseTable.getId()), LockType.READ);
        try {
            partitions = table.getPhysicalPartitions().stream()
                    .filter(p -> shouldVacuum(p))
                    .collect(Collectors.toList());
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
        List<Tablet> tablets = new ArrayList<>();
        long visibleVersion;
        long minRetainVersion;
        long startTime = System.currentTimeMillis();
        long minActiveTxnId = computeMinActiveTxnId(db, table);
        Map<ComputeNode, List<TabletInfoPB>> nodeToTablets = new HashMap<>();

        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                tablets.addAll(index.getTablets());
            }
            visibleVersion = partition.getVisibleVersion();
            minRetainVersion = partition.getMinRetainVersion();
            if (minRetainVersion <= 0) {
                minRetainVersion = Math.max(1, visibleVersion - Config.lake_autovacuum_max_previous_versions);
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }

        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        Warehouse warehouse = warehouseManager.getBackgroundWarehouse();
        for (Tablet tablet : tablets) {
            LakeTablet lakeTablet = (LakeTablet) tablet;
            ComputeNode node = warehouseManager.getComputeNodeAssignedToTablet(warehouse.getId(), lakeTablet);

            if (node == null) {
                return;
            }
            TabletInfoPB tabletInfo = new TabletInfoPB();
            tabletInfo.setTabletId(tablet.getId());
            tabletInfo.setMinVersion(lakeTablet.getMinVersion());
            nodeToTablets.computeIfAbsent(node, k -> Lists.newArrayList()).add(tabletInfo);
        }

        boolean hasError = false;
        long vacuumedFiles = 0;
        long vacuumedFileSize = 0;
        long vacuumedVersion = Long.MAX_VALUE;
        boolean needDeleteTxnLog = true;
        List<Future<VacuumResponse>> responseFutures = Lists.newArrayListWithCapacity(nodeToTablets.size());
        for (Map.Entry<ComputeNode, List<TabletInfoPB>> entry : nodeToTablets.entrySet()) {
            ComputeNode node = entry.getKey();
            VacuumRequest vacuumRequest = new VacuumRequest();
            // vacuumRequest.tabletIds is deprecated, use tabletInfos instead.
            vacuumRequest.tabletInfos = entry.getValue();
            vacuumRequest.minRetainVersion = minRetainVersion;
            vacuumRequest.graceTimestamp =
                    startTime / MILLISECONDS_PER_SECOND - Config.lake_autovacuum_grace_period_minutes * 60;
            vacuumRequest.graceTimestamp = Math.min(vacuumRequest.graceTimestamp,
                    Math.max(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr()
                            .getSafeDeletionTimeMs() / MILLISECONDS_PER_SECOND, 1));
            vacuumRequest.minActiveTxnId = minActiveTxnId;
            vacuumRequest.partitionId = partition.getId();
            vacuumRequest.deleteTxnLog = needDeleteTxnLog;
            // Perform deletion of txn log on the first node only.
            needDeleteTxnLog = false;
            try {
                LakeService service = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                responseFutures.add(service.vacuum(vacuumRequest));
            } catch (RpcException e) {
                LOG.error("failed to send vacuum request for partition {}.{}.{}", db.getFullName(), table.getName(),
                        partition.getId(), e);
                hasError = true;
                break;
            }
        }

        for (Future<VacuumResponse> responseFuture : responseFutures) {
            try {
                VacuumResponse response = responseFuture.get();
                if (response.status.statusCode != 0) {
                    hasError = true;
                    LOG.warn("Vacuumed {}.{}.{} with error: {}", db.getFullName(), table.getName(), partition.getId(),
                            response.status.errorMsgs.get(0));
                } else {
                    vacuumedFiles += response.vacuumedFiles;
                    vacuumedFileSize += response.vacuumedFileSize;
                    vacuumedVersion = Math.min(vacuumedVersion, response.vacuumedVersion);

                    if (response.tabletInfos != null) {
                        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
                        for (TabletInfoPB tabletInfo : response.tabletInfos) {
                            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletInfo.tabletId);
                            if (tabletMeta != null) {
                                MaterializedIndex index = partition.getIndex(tabletMeta.getIndexId());
                                if (index != null) {
                                    Tablet tablet = index.getTablet(tabletInfo.tabletId);
                                    if (tablet != null) {
                                        LakeTablet lakeTablet = (LakeTablet) tablet;
                                        lakeTablet.setMinVersion(tabletInfo.minVersion);
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("thread interrupted");
                Thread.currentThread().interrupt();
                hasError = true;
            } catch (ExecutionException e) {
                LOG.error("failed to vacuum {}.{}.{}: {}", db.getFullName(), table.getName(), partition.getId(),
                        e.getMessage());
                hasError = true;
            }
        }

        partition.setLastVacuumTime(startTime);
        if (!hasError && vacuumedVersion > partition.getLastSuccVacuumVersion()) {
            // hasError is false means that the vacuum operation on all tablets was successful.
            // the vacuumedVersion isthe minimum success vacuum version among all tablets within the partition which
            // means that all the garbage files before the vacuumVersion have been deleted.
            partition.setLastSuccVacuumVersion(vacuumedVersion);
        }
        LOG.info("Vacuumed {}.{}.{} hasError={} vacuumedFiles={} vacuumedFileSize={} " +
                        "visibleVersion={} minRetainVersion={} minActiveTxnId={} vacuumVersion={} cost={}ms",
                db.getFullName(), table.getName(), partition.getId(), hasError, vacuumedFiles, vacuumedFileSize,
                visibleVersion, minRetainVersion, minActiveTxnId, vacuumedVersion, System.currentTimeMillis() - startTime);
    }

    private static long computeMinActiveTxnId(Database db, Table table) {
        long a = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getMinActiveTxnIdOfDatabase(db.getId());
        Optional<Long> b =
                GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getActiveTxnIdOfTable(table.getId());
        return Math.min(a, b.orElse(Long.MAX_VALUE));
    }

    public void testVacuumPartitionImpl(Database db, OlapTable table, PhysicalPartition partition) {
        vacuumPartitionImpl(db, table, partition);
    }
}
