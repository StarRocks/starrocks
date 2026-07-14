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
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.LeaderDaemon;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

public class AutovacuumDaemon extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(AutovacuumDaemon.class);

    private static final long MILLISECONDS_PER_SECOND = 1000;
    private static final long SECONDS_PER_MINUTE = 60;
    private static final long MINUTES_PER_HOUR = 60;
    private static final long MILLISECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE * MILLISECONDS_PER_SECOND;

    // Queue capacity large enough to absorb the brief window after lake_autovacuum_parallel_partitions is
    // raised but before the ConfigRefreshDaemon listener has resized the pool. The outer gate in
    // scheduleVacuumRound() already caps the number of in-flight partitions, so the queue is normally empty.
    private static final int EXECUTOR_QUEUE_SIZE = 4096;

    // Hard cap on effective parallelism. lake_autovacuum_parallel_partitions is mutable, so clamp it to
    // this: it keeps in-flight submissions far below EXECUTOR_QUEUE_SIZE, so the pool's BlockedPolicy queue
    // never fills and never blocks the daemon thread, however aggressively the config is tuned.
    private static final int MAX_PARALLEL_PARTITIONS = 256;

    // When a collection finds nothing to vacuum, wait this long before scanning again so an idle cluster
    // is not walked every round. Rounds with a non-empty queue are unaffected.
    private static final long EMPTY_COLLECT_BACKOFF_MS = 30 * 1000L;

    private final Set<Long> vacuumingPartitions = Sets.newConcurrentHashSet();

    // LRU-ordered candidates from the last collection, drained across subsequent rounds. A round collects
    // again only once this queue is empty, so most rounds skip the expensive full scan (see
    // refillPendingCandidatesIfEmpty).
    private final Deque<VacuumCandidate> pendingCandidates = new ArrayDeque<>();
    // Set after a collection that found no candidates; suppresses re-scanning until this time.
    private long nextCollectTimeMs = 0;

    // Lazily created on the leader (see getExecutorService()). It is resized in place when
    // lake_autovacuum_parallel_partitions changes, never rebuilt.
    private ThreadPoolExecutor executorService = null;
    private boolean executorListenerRegistered = false;

    public AutovacuumDaemon() {
        super("auto-vacuum", 2000 /* 2s */);
    }

    // A fixed-size pool resized in place via the ConfigRefreshDaemon listener (mirrors
    // PublishVersionDaemon#getTaskExecutor). The pool's BlockedPolicy would block the caller for up to 60s
    // once its queue fills; that never happens here because the outer gate in scheduleVacuumRound() plus the
    // MAX_PARALLEL_PARTITIONS clamp keep in-flight work far below EXECUTOR_QUEUE_SIZE. Created lazily so that
    // unit tests exercising only vacuumPartitionImpl() never register a listener on the global ConfigRefreshDaemon.
    private ThreadPoolExecutor getExecutorService() {
        if (executorService == null) {
            int numThreads = Math.min(Math.max(1, Config.lake_autovacuum_parallel_partitions), MAX_PARALLEL_PARTITIONS);
            executorService = ThreadPoolManager.newDaemonFixedThreadPool(
                    numThreads, EXECUTOR_QUEUE_SIZE, "auto_vacuum", true);
            executorService.allowCoreThreadTimeOut(true);
            // Register the config-change listener only once per daemon instance.
            if (!executorListenerRegistered) {
                GlobalStateMgr.getCurrentState().getConfigRefreshDaemon()
                        .registerListener(this::adjustExecutorService);
                executorListenerRegistered = true;
            }
        }
        return executorService;
    }

    private void adjustExecutorService() {
        if (executorService == null) {
            return;
        }
        int newNumThreads = Math.min(Config.lake_autovacuum_parallel_partitions, MAX_PARALLEL_PARTITIONS);
        if (newNumThreads <= 0) {
            return;
        }
        ThreadPoolManager.setFixedThreadPoolSize(executorService, newNumThreads);
    }

    @Override
    protected void onStopped() {
        // Leader demotion: release leader-session-only state so a follower does not retain it.
        // executorListenerRegistered stays set so the ConfigRefreshDaemon listener is registered
        // exactly once per instance across demote/re-elect cycles (mirrors PublishVersionDaemon).
        ThreadPoolExecutor executor = executorService;
        if (executor != null) {
            executorService = null;
            // shutdownNow() interrupts running tasks but does not wait for them: a task stuck in a
            // non-interruptible section can outlive demotion and even re-election. Its entry in
            // vacuumingPartitions must survive so the next leader session keeps skipping the
            // partition until the task's finally releases it; clearing the set wholesale would
            // permit two concurrent vacuums of the same partition (and the straggler's late
            // finally would then drop the new session's live entry, permitting a third). Only
            // reservations of drained tasks that never ran are released here, so every entry is
            // released exactly once: either by its task's finally or by this loop.
            for (Runnable task : executor.shutdownNow()) {
                if (task instanceof VacuumTask) {
                    vacuumingPartitions.remove(((VacuumTask) task).partitionId);
                }
            }
        }
        pendingCandidates.clear();
        nextCollectTimeMs = 0;
    }

    @Override
    protected void runAfterLeaseValid() {
        if (FeConstants.runningUnitTest) {
            return;
        }
        scheduleVacuumRound();
    }

    private void scheduleVacuumRound() {
        // Concurrency is bounded here (the "outer gate"), not by the thread pool capacity. This makes
        // lake_autovacuum_parallel_partitions take effect at runtime and, crucially, keeps a slow vacuum
        // from ever blocking this daemon thread. The value is clamped to MAX_PARALLEL_PARTITIONS so
        // in-flight work stays well below the pool queue and its BlockedPolicy never blocks the daemon. A
        // non-positive value disables AutoVacuum entirely (see the config docs); adjustExecutorService
        // likewise leaves the pool untouched for such values.
        int parallelPartitions = Math.min(Config.lake_autovacuum_parallel_partitions, MAX_PARALLEL_PARTITIONS);
        if (parallelPartitions <= 0 || vacuumingPartitions.size() >= parallelPartitions) {
            return;
        }
        refillPendingCandidatesIfEmpty();
        submitPendingCandidates(parallelPartitions);
    }

    // A full candidate collection walks every db/table under a table lock, which is too expensive to run
    // every couple of seconds just to fill a few free slots. So collect only once the previous batch has
    // been fully drained, cache the result ordered oldest-first (LRU), and let subsequent rounds drain from
    // that cache. Re-collecting and re-sorting each time the queue empties keeps fairness: a partition that
    // keeps losing the race only gets older and rises toward the front on the next collection, so nothing
    // starves.
    private void refillPendingCandidatesIfEmpty() {
        if (!pendingCandidates.isEmpty()) {
            return;
        }
        // Back off scanning when the previous collection came up empty, so an idle cluster is not walked
        // on every round.
        if (System.currentTimeMillis() < nextCollectTimeMs) {
            return;
        }
        List<VacuumCandidate> candidates = collectVacuumCandidates();
        if (candidates.isEmpty()) {
            nextCollectTimeMs = System.currentTimeMillis() + EMPTY_COLLECT_BACKOFF_MS;
            return;
        }
        candidates.sort(Comparator.comparingLong(candidate -> candidate.lastVacuumTime));
        pendingCandidates.addAll(candidates);
    }

    private void submitPendingCandidates(int parallelPartitions) {
        while (vacuumingPartitions.size() < parallelPartitions) {
            VacuumCandidate candidate = pendingCandidates.poll();
            if (candidate == null) {
                break;
            }
            PhysicalPartition partition = candidate.partition;
            long partitionId = partition.getId();
            // The cached candidate may be stale by now (already picked up in an earlier round, or vacuumed
            // since the last full collection), so re-check before submitting.
            if (vacuumingPartitions.contains(partitionId) || !shouldVacuum(partition)) {
                continue;
            }
            if (vacuumingPartitions.add(partitionId)) {
                try {
                    getExecutorService().execute(new VacuumTask(partitionId,
                            () -> vacuumPartition(candidate.db, candidate.table, partition)));
                } catch (RuntimeException e) {
                    // Submission failed (e.g. RejectedExecutionException when the pool queue is saturated).
                    // The task never runs, so vacuumPartition's finally never removes the id; roll it back
                    // here, otherwise this partition would stay "in flight" forever until an FE restart.
                    // Stop the round too: a saturated pool would otherwise block on BlockedPolicy for every
                    // remaining candidate.
                    vacuumingPartitions.remove(partitionId);
                    LOG.warn("Failed to submit vacuum task for partition {}, stopping this round", partitionId, e);
                    break;
                }
            }
        }
    }

    private List<VacuumCandidate> collectVacuumCandidates() {
        List<VacuumCandidate> candidates = new ArrayList<>();
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
                collectTableCandidates(db, (OlapTable) table, candidates);
            }
        }
        return candidates;
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
            // the file before minRetainVersion vacuum success
            if (partition.getLastSuccVacuumVersion() >= minRetainVersion) {
                return false;
            }
        }
        // TODO(zhangqiang)
        // add partition data size and storage size on S3 to decide vacuum or not
        return true;
    }

    private void collectTableCandidates(Database db, OlapTable table, List<VacuumCandidate> candidates) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        try {
            for (PhysicalPartition partition : table.getPhysicalPartitions()) {
                // Skip partitions already being vacuumed so only fresh candidates take part in this round's
                // fairness ordering (mirrors CompactionScheduler excluding runningCompactions).
                if (!vacuumingPartitions.contains(partition.getId()) && shouldVacuum(partition)) {
                    candidates.add(new VacuumCandidate(db, table, partition));
                }
            }
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        }
    }

    // Carries the partition id so onStopped() can identify queued-but-never-run tasks returned by
    // shutdownNow() and release exactly their vacuumingPartitions reservations.
    private static class VacuumTask implements Runnable {
        private final long partitionId;
        private final Runnable work;

        VacuumTask(long partitionId, Runnable work) {
            this.partitionId = partitionId;
            this.work = work;
        }

        @Override
        public void run() {
            work.run();
        }
    }

    // A snapshot of a partition that needs vacuuming, captured under the table lock. lastVacuumTime is
    // sampled here so this round's ordering stays stable even if the partition is updated concurrently.
    private static class VacuumCandidate {
        private final Database db;
        private final OlapTable table;
        private final PhysicalPartition partition;
        private final long lastVacuumTime;

        VacuumCandidate(Database db, OlapTable table, PhysicalPartition partition) {
            this.db = db;
            this.table = table;
            this.partition = partition;
            this.lastVacuumTime = partition.getLastVacuumTime();
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
