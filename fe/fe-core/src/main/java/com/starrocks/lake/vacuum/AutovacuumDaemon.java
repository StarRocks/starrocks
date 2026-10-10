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

    // Package-private so AutovacuumDaemonTest can assert onStopped() clears it, without reflection.
    final Set<Long> vacuumingPartitions = Sets.newConcurrentHashSet();

    // LRU-ordered candidates from the last collection, drained across subsequent rounds. A round collects
    // again only once this queue is empty, so most rounds skip the expensive full scan (see
    // refillPendingCandidatesIfEmpty).
    private final Deque<VacuumCandidate> pendingCandidates = new ArrayDeque<>();
    // Set after a collection that found no candidates; suppresses re-scanning until this time.
    private long nextCollectTimeMs = 0;

    // Lazily created on the leader (see getExecutorService()) and resized in place when
    // lake_autovacuum_parallel_partitions changes; onStopped() drains it to termination and nulls it on
    // demotion, so getExecutorService() rebuilds a fresh pool on re-election. Package-private for tests.
    ThreadPoolExecutor executorService = null;
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
        // Leader demotion: drain the vacuum pool to termination before releasing leader-session state, so
        // this worker does not clear isRunning until the pool is quiescent - the re-activation cleanliness
        // gate reads isRunning as the single quiescence signal (consistent with the other LeaderDaemons).
        // executorListenerRegistered stays set so the ConfigRefreshDaemon listener is registered exactly
        // once per instance across demote/re-elect cycles (mirrors PublishVersionDaemon); the executor is
        // nulled so getExecutorService() lazily rebuilds a fresh pool on re-election.
        ThreadPoolExecutor executor = executorService;
        if (executor != null) {
            executorService = null;
            shutdownNowAndAwaitTermination("AutovacuumDaemon.executorService", executor);
        }
        // Every task has terminated (each removes its own vacuumingPartitions entry in a finally), so it is
        // now safe to clear any residue left by queued-but-never-run tasks, along with the transient
        // scheduling state, letting a re-elected leader re-derive everything from a clean slate.
        vacuumingPartitions.clear();
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
                    getExecutorService().execute(
                            () -> vacuumPartition(candidate.db, candidate.table, partition));
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

        long baseGenerationTakeover = 0;
        long preExtraFileSize = 0;
        // If shared file cleanup is enabled, vacuum runs on a single aggregator node.
        Map<ComputeNode, List<TabletInfoPB>> nodeToTablets = new HashMap<>();
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.READ);
        boolean fileBundling = table.isFileBundling();
        boolean rangeDistribution = table.isRangeDistribution();
        try {
            for (MaterializedIndex index : partition.getMaterializedIndicesForVacuum(IndexExtState.VISIBLE)) {
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
<<<<<<< HEAD
        if (!hasError && vacuumedVersion > partition.getLastSuccVacuumVersion()) {
            locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
            try {
                // hasError is false means that the vacuum operation on all tablets was successful.
                // the vacuumedVersion isthe minimum success vacuum version among all tablets within the partition which
                // means that all the garbage files before the vacuumVersion have been deleted.
                partition.setLastSuccVacuumVersion(vacuumedVersion);
                if (partition.getMetadataSwitchVersion() != 0 && vacuumedVersion >= partition.getMetadataSwitchVersion()) {
=======
        // The proposal to log below is the band actually PERSISTED for the next round, not the raw BE response:
        // on a completed pass updateVacuumState() resets the state and intentionally discards the BE's
        // final-round re-proposal, so logging that raw band -- which the next round neither commits nor resumes
        // from -- would read like a spurious re-propose. It stays zero on an error round (nothing persisted, the
        // state is left untouched and the next round re-tries) -- the raw response may be partially non-zero
        // across nodes and must not be mistaken for progress; hasError=true is what to read.
        long logToDeleteLow = 0;
        long logToDeleteHigh = 0;
        long logNextProposeStart = 0;
        // Persist the pass state only on a good round: no send error and at least one request actually
        // went out. Otherwise leave it untouched so the next round re-commits + re-proposes.
        // Skip the state update on a grace-blocked round: a tablet is still within grace, so the pass must
        // WAIT -- not commit a wider range (it would delete that tablet's not-yet-eligible garbage) and not
        // complete (completing would advance the floor past garbage that is merely waiting on grace). The
        // BE already re-committed the previously-sent band this round (idempotent), and the next round
        // re-commits + re-proposes once grace passes, so leaving the state untouched is a safe retry that
        // never discards an in-flight band. (Absent grace blocking, an all-drained round still completes.)
        if (!hasError && !responseFutures.isEmpty() && !anyGraceBlocked) {
            updateVacuumState(db, table, partition, locker, freshRound, committingFinalBand, tablets,
                    respToDeleteLow, respToDeleteHigh, respNextProposeStart, respPassStartVersion, currentIndexIds,
                    extraFileSize, preExtraFileSize);
            // Read the just-persisted proposal back for the log line below.
            VacuumState persisted = partition.getVacuumState();
            logToDeleteLow = persisted.getToDeleteLow();
            logToDeleteHigh = persisted.getToDeleteHigh();
            logNextProposeStart = persisted.getNextProposeStartVersion();
        }

        // One round is counted once: failed when any request could not be sent or came back with an error,
        // succeeded when at least one request went out and all of them returned OK. A round that sent nothing
        // (no node picked) is neither.
        if (hasError) {
            MetricRepo.COUNTER_VACUUM_FAILED.increase(1L);
        } else if (!responseFutures.isEmpty()) {
            MetricRepo.COUNTER_VACUUM_SUCCESS.increase(1L);
        }
        MetricRepo.COUNTER_VACUUM_FILES_NUMBER.increase(vacuumedFiles);
        MetricRepo.COUNTER_VACUUM_FILES_BYTES.increase(vacuumedFileSize);
        // One line per round (always logged, so error rounds are visible too). committed=[..) is the range
        // this round told the BE to delete (proposed by the previous round); proposed=[..) is the band
        // persisted for the next round to commit -- empty on a completed pass, whose BE re-proposal that round
        // is discarded and re-derived fresh next round. resume_from is the cursor this round sent;
        // next_propose_start is the persisted resume cursor (0 == chain bottom reached / pass complete).
        // vacuumVersion is the persisted success watermark (lastSuccVacuumVersion) AFTER this round, which
        // advances to the pass retain floor when a pass completes. On an error round the values come back 0,
        // so hasError=true is what to read.
        LOG.info("incremental vacuum {}.{}.{} hasError={} visibleVersion={} minRetainVersion={} minActiveTxnId={} " +
                        "txnLogSweepWatermark={} pass_start_version={} committed=[{},{}) resume_from={} " +
                        "proposed=[{},{}) next_propose_start={} vacuumVersion={} vacuumedFiles={} " +
                        "vacuumedFileSize={} cost={}ms",
                db.getFullName(), table.getName(), partition.getId(), hasError,
                visibleVersion, minRetainVersion, minActiveTxnId, txnLogSweepWatermark,
                passStartVersion, toDeleteLow, toDeleteHigh, nextProposeStart,
                logToDeleteLow, logToDeleteHigh, logNextProposeStart,
                partition.getLastSuccVacuumVersion(), vacuumedFiles, vacuumedFileSize,
                System.currentTimeMillis() - startTime);
    }

    // Persist the result of one incremental vacuum round into the partition's in-memory pass state.
    // On a confirmed round the BE has already committed the range we sent (toDeleteLow/High) and returned
    // the next proposed range plus resume cursor; here we either advance into that range or, when the pass
    // has reached the chain bottom, finish it: advance the success watermark to the pass retain floor and
    // clear the state so the next round starts fresh. The caller invokes this only on a good round (no
    // send error, at least one request sent); leaving the state untouched otherwise makes the next round
    // re-commit + re-propose (both idempotent on the BE).
    private void updateVacuumState(Database db, OlapTable table, PhysicalPartition partition,
            Locker locker, boolean freshRound, boolean committingFinalBand, List<Tablet> tablets,
            long respToDeleteLow, long respToDeleteHigh, long respNextProposeStart, long respPassStartVersion,
            Set<Long> currentIndexIds, long extraFileSize, long preExtraFileSize) {
        // The pass completes when either (a) this round just committed the pass's final band -- the band the
        // BE previously proposed with resume cursor 0, i.e. the chain bottom was reached -- or (b) the BE now
        // proposes nothing more to delete. Case (a) is the common one: when the whole remaining band fits in
        // one round the BE returns a NON-empty final band together with cursor 0 and never a subsequent
        // empty proposal, so detecting completion only from an empty proposal (b) would loop forever
        // (re-committing that band every round) and the success watermark would never advance.
        boolean passComplete = committingFinalBand || (respToDeleteLow >= respToDeleteHigh);
        VacuumState state = partition.getVacuumState();
        locker.lockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        try {
            // Per-round storage-accounting bookkeeping, independent of the incremental pass state: refresh the
            // partition's extra-file size from the BE's report plus whatever compaction added concurrently
            // during this round. Kept under the table WRITE lock so this read-modify-write cannot lose a
            // concurrent CompactionScheduler.commitCompaction() -> incExtraFileSize() (which takes the same
            // lock), and only reached on a good round (the caller gates on !hasError && !responseFutures
            // .isEmpty()), so a partial-failure round never overwrites the previous accurate total.
            long incrementExtraFileSize = partition.getExtraFileSize() - preExtraFileSize;
            partition.setExtraFileSize(extraFileSize + incrementExtraFileSize);
            if (passComplete) {
                // The pass deleted everything below its retain floor band by band; advance the success
                // watermark to that floor (mirrors the legacy path advancing to the retain boundary). The
                // floor is the value captured on this pass's fresh round and held constant since; a pass
                // that proposed nothing from the very first fresh round has floor 0 and so does not move
                // the watermark. On a final-band round the BE also returned a fresh re-proposal this round;
                // we intentionally discard it via reset() -- the pass has reached the bottom, so the next
                // pass re-derives from a fresh walk.
                // On a fresh round the pass floor is what the BE just reported (respPassStartVersion): a
                // non-fresh round completes via committingFinalBand and reads the floor captured on the
                // pass's fresh round (state), but a fresh round that completes immediately -- an empty
                // proposal because everything at/below the retain floor is already vacuumed (drained) -- only
                // has it in the current response, where the BE reports the retain floor. Reading the stale
                // state (0) there would strand a pinned metadataSwitchVersion and freeze the watermark.
                long passFloor = freshRound ? respPassStartVersion : state.getPassStartVersion();
                if (passFloor > partition.getLastSuccVacuumVersion()) {
                    partition.setLastSuccVacuumVersion(passFloor);
                }
                if (partition.getMetadataSwitchVersion() != 0
                        && passFloor >= partition.getMetadataSwitchVersion()) {
>>>>>>> 42f5d018ea3... [BugFix] Add success and failure counters for lake vacuum on FE and BE (#64168)
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
