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
import com.starrocks.lake.LakeAggregator;
import com.starrocks.lake.LakeTableHelper;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.snapshot.ClusterSnapshotMgr;
import com.starrocks.metric.MetricRepo;
import com.starrocks.proto.TabletInfoPB;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        super("auto-vacuum", 2000);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (FeConstants.runningUnitTest) {
            return;
        }

        // acquire background resource
        acquireBackgroundComputeResource();

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

        if (partition.getVisibleVersionTime() <= staleTime && partition.getMetadataSwitchVersion() == 0) {
            return false;
        }
        // empty partition
        if (partition.getVisibleVersion() <= 1) {
            return false;
        }
        if (vacuumImmediatelyPartition(partition)) {
            return true;
        }
        // prevent vacuum too frequent
        if (current < partition.getLastVacuumTime() + Config.lake_autovacuum_partition_naptime_seconds * 1000) {
            return false;
        }

        if (Config.lake_autovacuum_detect_vaccumed_version) {
            long minRetainVersion = partition.getMinRetainVersion();
            if (minRetainVersion <= 0) {
                minRetainVersion = Math.max(1, partition.getVisibleVersion() - Config.lake_autovacuum_max_previous_versions);
            } else {
                minRetainVersion = Math.min(minRetainVersion, 
                                        partition.getVisibleVersion() - Config.lake_autovacuum_max_previous_versions);
            }
            // Apply the same takeover clamp the request path applies, so scheduling is decided on the
            // floor that will actually be sent. Without it a partition whose lastSuccVacuumVersion has
            // already caught up with the lower unclamped floor is rejected here and never gets a round
            // carrying the higher one. The OrNull variant is deliberate: this runs both under the
            // collection's table read lock and lock-free from submitPendingCandidates, and a
            // schedulability decision does not warrant taking a lock -- the request path re-reads the
            // takeover under the table lock and remains authoritative.
            MaterializedIndex latestBaseIndex = partition.getLatestBaseIndexOrNull();
            if (latestBaseIndex != null && latestBaseIndex.getTakeoverVersion() > minRetainVersion) {
                minRetainVersion = latestBaseIndex.getTakeoverVersion();
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

        // Confirmed/lagged-watermark debounce, against a begin-transaction vs autovacuum race:
        // beginTransaction() draws an id (advancing peekNextTransactionId()) BEFORE registering it in
        // idToRunningTransactionState, so a probe landing in that gap can compute a minActiveTxnId one
        // greater than an in-flight txn. Acting on it would let the BE delete that txn's still-needed
        // combined log and permanently wedge publish on the partition. So we only act on a value confirmed
        // by the PREVIOUS round (non-decreasing) and sweep txn logs with that older, confirmed value.
        //
        // When the current value is NOT confirmed -- first observation (also after FE restart/failover,
        // since lastMinActiveTxnId is in-memory and resets to 0) or a regression -- we skip the ENTIRE
        // round, not merely the txn-log delete. Skipping only the delete would still let the round advance
        // lastSuccVacuumVersion; with lake_autovacuum_detect_vaccumed_version=true, shouldVacuum() then
        // stops scheduling the partition once lastSuccVacuumVersion >= minRetainVersion, so for a partition
        // that goes cold the confirming follow-up round (and its sweep) might never run, leaking txn logs.
        // A full skip leaves lastSuccVacuumVersion untouched, so the partition stays schedulable and the
        // next round runs with a confirmed watermark; the only cost is deferring this partition's
        // metadata/data vacuum by one (rare) cycle. lastVacuumTime is set so naptime is still respected.
        long lastMinActiveTxnId = partition.getLastMinActiveTxnId();
        if (lastMinActiveTxnId <= 0 || minActiveTxnId < lastMinActiveTxnId) {
            if (minActiveTxnId < lastMinActiveTxnId) {
                LOG.warn("minActiveTxnId regressed {} -> {} for {}.{}.{}; skipping this vacuum round "
                                + "(possible begin/vacuum race)",
                        lastMinActiveTxnId, minActiveTxnId, db.getFullName(), table.getName(), partition.getId());
            }
            partition.setLastMinActiveTxnId(minActiveTxnId);
            partition.setLastVacuumTime(startTime);
            return;
        }
        // Confirmed non-decreasing: sweep txn logs with the previous (older, lower) value.
        final long txnLogSweepWatermark = lastMinActiveTxnId;
        partition.setLastMinActiveTxnId(minActiveTxnId);

        long baseGenerationTakeover = 0;
        long preExtraFileSize = 0;
        // If shared file cleanup is enabled, vacuum runs on a single aggregator node.
        Map<ComputeNode, List<TabletInfoPB>> nodeToTablets = new HashMap<>();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        boolean fileBundling = table.isFileBundling();
        boolean rangeDistribution = table.isRangeDistribution();
        try {
            for (MaterializedIndex index : partition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                tablets.addAll(index.getTablets());
            }
            MaterializedIndex latestBaseIndex = partition.getLatestBaseIndex();
            if (latestBaseIndex != null) {
                baseGenerationTakeover = latestBaseIndex.getTakeoverVersion();
            }
            visibleVersion = partition.getVisibleVersion();
            minRetainVersion = partition.getMinRetainVersion();
            if (minRetainVersion <= 0) {
                minRetainVersion = Math.max(1, visibleVersion - Config.lake_autovacuum_max_previous_versions);
            } else {
                minRetainVersion = Math.min(minRetainVersion, visibleVersion - Config.lake_autovacuum_max_previous_versions);
            }

            preExtraFileSize = partition.getExtraFileSize();
            if (partition.getMetadataSwitchVersion() != 0) {
                // If metadataSwitchVersion is not 0, it means that for versions prior to this, the value of 
                // fileBundling should be the ​​opposite​​ of the current value.
                fileBundling = !fileBundling;
            }

        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }

        // Versions below the base generation's takeover do not exist for its tablets: a tablet
        // created by a tablet split/merge has no metadata below the reshard commit version. Asking
        // the backend to retain them is not merely vacuous -- the vacuum walk starts at the entry
        // version and cannot anchor, so it steps down one version at a time paying a remote
        // NotFound per step, records no resume cursor for an un-anchored walk, and contributes an
        // empty range that the partition-level intersection turns into no progress for the whole
        // partition, round after round. Clamp with the BASE generation only: raising the floor to a
        // non-base index's newer takeover would un-retain base versions that are still wanted. A
        // partition can also be PARTIALLY resharded (a split skips indexes whose tablets are all
        // under the target size), so a live non-base index may still hold metadata below this
        // clamp; that is safe because in-flight readers are grace-timestamp-protected on the
        // backend. A non-base index whose tablets start above this floor can still stall the
        // proposal -- that generic case needs per-tablet handling in the backend propose path and
        // is out of scope here.
        if (baseGenerationTakeover > minRetainVersion) {
            minRetainVersion = baseGenerationTakeover;
        }

        boolean enableSharedFileCleanup = fileBundling || rangeDistribution;
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();
        ComputeResource computeResource = warehouseManager.getBackgroundComputeResource(table.getId());

        // Resolve all tablet owners in a single batched RPC. The result serves both:
        // - enableSharedFileCleanup: collect candidate aggregator nodes (prefer a node
        //   that owns at least one tablet), then assign all tablets to the chosen one.
        // - non-shared: assign each tablet to its first alive owner CN.
        // This avoids N per-tablet getComputeNodeAssignedToTablet RPCs in either path.
        Map<Long, List<Long>> shardToNodeIds = null;
        if (!tablets.isEmpty()) {
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            List<Long> tabletIds = tablets.stream().map(Tablet::getId).collect(Collectors.toList());
            try {
                shardToNodeIds = starOSAgent.getAllNodeIdsByShards(
                        tabletIds, computeResource.getWorkerGroupId());
            } catch (Exception e) {
                LOG.warn("Failed to batch-resolve tablet owners for {} tablets, falling back",
                        tablets.size(), e);
            }
        }

        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();

        if (enableSharedFileCleanup) {
            // Collect candidate aggregator nodes from the batched result, then pick one.
            Set<ComputeNode> candidateAggregatorNodes = Sets.newHashSet();
            if (shardToNodeIds != null) {
                for (List<Long> nodeIds : shardToNodeIds.values()) {
                    if (nodeIds == null || nodeIds.isEmpty()) {
                        continue;
                    }
                    ComputeNode owner = clusterInfo.getBackendOrComputeNode(nodeIds.get(0));
                    if (owner != null) {
                        candidateAggregatorNodes.add(owner);
                    }
                }
            }
            ComputeNode pickNode = LakeAggregator.chooseAggregatorNode(computeResource, candidateAggregatorNodes);
            if (pickNode == null) {
                return;
            }
            for (Tablet tablet : tablets) {
                LakeTablet lakeTablet = (LakeTablet) tablet;
                TabletInfoPB tabletInfo = new TabletInfoPB();
                tabletInfo.setTabletId(tablet.getId());
                tabletInfo.setMinVersion(lakeTablet.getMinVersion());
                nodeToTablets.computeIfAbsent(pickNode, k -> Lists.newArrayList()).add(tabletInfo);
            }
        } else {
            for (Tablet tablet : tablets) {
                LakeTablet lakeTablet = (LakeTablet) tablet;
                // Try batched result first: find first alive owner for this tablet.
                ComputeNode pickNode = null;
                List<Long> nodeIds = (shardToNodeIds != null)
                        ? shardToNodeIds.get(lakeTablet.getId()) : null;
                if (nodeIds != null) {
                    for (long nodeId : nodeIds) {
                        if (clusterInfo.checkBackendAlive(nodeId)
                                || clusterInfo.checkComputeNodeAlive(nodeId)) {
                            pickNode = clusterInfo.getBackendOrComputeNode(nodeId);
                            break;
                        }
                    }
                }
                if (pickNode == null) {
                    // Batched result missing or no alive replica — fall back to per-tablet RPC.
                    pickNode = warehouseManager.getComputeNodeAssignedToTablet(
                            computeResource, lakeTablet.getId());
                }
                if (pickNode == null) {
                    return;
                }
                TabletInfoPB tabletInfo = new TabletInfoPB();
                tabletInfo.setTabletId(tablet.getId());
                tabletInfo.setMinVersion(lakeTablet.getMinVersion());
                nodeToTablets.computeIfAbsent(pickNode, k -> Lists.newArrayList()).add(tabletInfo);
            }
        }

        ClusterSnapshotMgr clusterSnapshotMgr = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
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
            if (vacuumImmediatelyPartition(partition)) {
                // If the partition is in the ignore list, we set graceTimestamp to startTime.
                // This means that the vacuum operation will not be delayed by graceTimestamp.
                // So version will be vacuumed immediately.
                vacuumRequest.graceTimestamp = startTime / MILLISECONDS_PER_SECOND;
            }
            vacuumRequest.graceTimestamp = Math.min(vacuumRequest.graceTimestamp,
                    Math.max(clusterSnapshotMgr.getSafeDeletionTimeMs() / MILLISECONDS_PER_SECOND, 1));
            vacuumRequest.retainVersions = clusterSnapshotMgr.getVacuumRetainVersions(
                                           db.getId(), table.getId(), partition.getParentId(), partition.getId());
            vacuumRequest.minActiveTxnId = txnLogSweepWatermark;
            vacuumRequest.partitionId = partition.getId();
            vacuumRequest.deleteTxnLog = needDeleteTxnLog;
            vacuumRequest.enableFileBundling = fileBundling;
            vacuumRequest.enableSharedFileCleanup = enableSharedFileCleanup;
            // The longest this FE waits for the response (the brpc timeout of the vacuum RPC).
            // The BE checks it periodically during execution and aborts the task once it has
            // elapsed, instead of running on as a zombie that no caller is waiting for.
            vacuumRequest.timeoutMs = LakeService.TIMEOUT_VACUUM;
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

        long extraFileSize = 0;
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
                    extraFileSize += response.extraFileSize;

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
            locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
            try {
                // hasError is false means that the vacuum operation on all tablets was successful.
                // the vacuumedVersion isthe minimum success vacuum version among all tablets within the partition which
                // means that all the garbage files before the vacuumVersion have been deleted.
                partition.setLastSuccVacuumVersion(vacuumedVersion);
                if (partition.getMetadataSwitchVersion() != 0 && vacuumedVersion >= partition.getMetadataSwitchVersion()) {
                    partition.setMetadataSwitchVersion(0);
                }
                long incrementExtraFileSize = partition.getExtraFileSize() - preExtraFileSize;
                partition.setExtraFileSize(extraFileSize + incrementExtraFileSize);
            } finally {
                locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
            }
        }
        MetricRepo.COUNTER_VACUUM_FILES_NUMBER.increase(vacuumedFiles);
        MetricRepo.COUNTER_VACUUM_FILES_BYTES.increase(vacuumedFileSize);
        LOG.info("Vacuumed {}.{}.{} hasError={} vacuumedFiles={} vacuumedFileSize={} " +
                        "visibleVersion={} minRetainVersion={} minActiveTxnId={} txnLogSweepWatermark={} " +
                        "vacuumVersion={} extraFileSize={} cost={}ms",
                db.getFullName(), table.getName(), partition.getId(), hasError, vacuumedFiles, vacuumedFileSize,
                visibleVersion, minRetainVersion, minActiveTxnId, txnLogSweepWatermark,
                vacuumedVersion, extraFileSize, System.currentTimeMillis() - startTime);
    }

    private static long computeMinActiveTxnId(Database db, Table table) {
        return LakeTableHelper.computeMinActiveTxnId(db.getId(), table.getId());
    }

    private boolean vacuumImmediatelyPartition(PhysicalPartition partition) {
        if (Config.lake_vacuum_immediately_partition_ids.isEmpty()) {
            return false;
        }
        String[] ids = Config.lake_vacuum_immediately_partition_ids.split(";");
        for (String id : ids) {
            if (id.equals(String.valueOf(partition.getId()))) {
                return true;
            }
        }
        return false;
    }

    public void testVacuumPartitionImpl(Database db, OlapTable table, PhysicalPartition partition) {
        vacuumPartitionImpl(db, table, partition);
    }
}
