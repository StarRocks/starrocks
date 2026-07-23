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
import com.starrocks.catalog.VacuumState;
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
import com.starrocks.persist.PartitionVacuumStateInfo;
import com.starrocks.proto.TabletInfoPB;
import com.starrocks.proto.VacuumRequest;
import com.starrocks.proto.VacuumResponse;
import com.starrocks.proto.VacuumStatePB;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
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

    // Floor for lake_autovacuum_max_versions_per_round. A smaller per-round budget would make the
    // incremental vacuum take too many rounds to drain a backlog -- each round carries fixed RPC and
    // edit-log overhead -- so a configured value below this is clamped up to it.
    private static final long MIN_VERSIONS_PER_ROUND = 100;

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

        // An in-flight incremental vacuum pass must keep being scheduled until it drains, independent of
        // the lastSuccVacuumVersion watermark below (which only advances when a pass completes).
        if (partition.getVacuumState().isInFlight()) {
            return true;
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
        // Visible MaterializedIndex ids (getId()) this round -- the tablet generation. Stored with each
        // proposal so the next round can detect a wholesale tablet-set replacement (split / sort-key change).
        Set<Long> currentIndexIds = new HashSet<>();
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
                currentIndexIds.add(index.getId());
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
        // The partition-level walk floor (everything strictly below is already vacuumed), carried durably in
        // the VacuumState. Every tablet in the round gets the same value: the incremental protocol commits a
        // partition-level intersection, so the walk floor is partition-level too. It survives FE failover
        // (VacuumState is journaled), so a new leader does not re-walk from the bottom into the metacache's
        // stale entries for already-deleted versions.
        long minRetainedVersion = partition.getVacuumState().getMinRetainedVersion();

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
                TabletInfoPB tabletInfo = new TabletInfoPB();
                tabletInfo.setTabletId(tablet.getId());
                tabletInfo.setMinVersion(minRetainedVersion);
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
                tabletInfo.setMinVersion(minRetainedVersion);
                nodeToTablets.computeIfAbsent(pickNode, k -> Lists.newArrayList()).add(tabletInfo);
            }
        }

        // The incremental (bounded, resumable) vacuum protocol is the ONLY vacuum path -- it is always on
        // and cannot be toggled off. Once a round deletes a bounded range it leaves holes in the
        // prev_garbage_version chain, and the legacy one-shot path (which walks that chain top-down) would
        // stop at the first hole and silently miss everything below it, so we must never fall back to
        // legacy. max_versions_per_round is only a per-round work cap; it is clamped positive so a mis-set
        // 0 cannot stall the walk. State is tracked per partition (lake versions are partition-wide); when
        // a partition's tablets span several nodes, each node returns the intersection over its own subset
        // and the FE recombines them (MIN floor / MAX low / MAX cursor) into the same partition-level
        // values a single node would have computed, so tablet-to-node routing is left exactly as is.
        long maxVersionsPerRound = Math.max(MIN_VERSIONS_PER_ROUND, Config.lake_autovacuum_max_versions_per_round);
        VacuumState state = partition.getVacuumState();
        long toDeleteLow = state.getToDeleteLow();
        long toDeleteHigh = state.getToDeleteHigh();
        long nextProposeStart = state.getNextProposeStartVersion();
        long passStartVersion = state.getPassStartVersion();
        // A pass starts fresh exactly when it is NOT in flight -- no band left to commit and no resume
        // cursor to continue from; only then does the BE return the pass retain floor, and only then does
        // the FE capture it. NOTE: a resume cursor of 0 by itself does NOT mean "fresh": the BE returns
        // cursor 0 to signal that the chain bottom was reached (the proposed band is the pass's LAST band),
        // so an in-flight state whose cursor is 0 is a final-band-pending state, distinct from a fresh start.
        boolean freshRound = !state.isInFlight();
        // The band this round is about to commit was proposed by the BE with resume cursor 0 (chain bottom
        // reached): it is the pass's final band, so once this round commits it the pass is complete -- see
        // updateVacuumState(), which must not wait for a subsequent empty proposal that never comes.
        boolean committingFinalBand = state.isInFlight() && (nextProposeStart == 0);

        // Cross-generation guard: the in-flight proposal was computed against a previous round's tablet set.
        // If a concurrent DDL replaced that set's indices wholesale -- a range split or a sort-key schema
        // change rebuilds every base tablet under a NEW indexId -- committing the stale proposal against the
        // new generation only walks tablets that do not exist in the proposed range, while the old
        // generation's data + metadata is reclaimed by its own delete-tablet teardown, so vacuum has nothing
        // to commit for it. Detect it via the visible index-id set: if NONE of the indices the proposal was
        // based on survive, the tablet set was fully replaced -- drop the proposal and start a fresh pass on
        // the current set. A rollup add/drop only adds/removes a SEPARATE index (metaId) and leaves the base
        // index-id intact, so the sets still intersect and the proposal is kept (any bounded cross-tablet
        // work on freshly-added rollup tablets is capped by the BE's per-round width bound).
        Set<Long> passStartIndexIds = state.getPassStartIndexIds();
        if (state.isInFlight() && !currentIndexIds.isEmpty()
                && passStartIndexIds != null && !passStartIndexIds.isEmpty()
                && Collections.disjoint(currentIndexIds, passStartIndexIds)) {
            LOG.info("incremental vacuum {}.{}.{} dropping cross-generation proposal: dropped "
                            + "committed=[{},{}) resume_from={} pass_start_version={} passStartIndexIds={} "
                            + "currentIndexIds={}; restarting fresh pass",
                    db.getFullName(), table.getName(), partition.getId(), toDeleteLow, toDeleteHigh,
                    nextProposeStart, passStartVersion, passStartIndexIds, currentIndexIds);
            toDeleteLow = 0;
            toDeleteHigh = 0;
            nextProposeStart = 0;
            passStartVersion = 0;
            freshRound = true;
            committingFinalBand = false;
        }

        ClusterSnapshotMgr clusterSnapshotMgr = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr();
        boolean hasError = false;
        long vacuumedFiles = 0;
        long vacuumedFileSize = 0;
        // Per-node incremental results recombined into one partition-level proposal. high/passStart take the
        // MIN across nodes (the global retain floor), low/cursor the MAX (the deepest version every node
        // agrees is deletable / still has to walk). A node that proposed nothing (grace blocked) makes the
        // whole round propose nothing, mirroring the single-node walk that short-circuits on the first
        // grace-blocked tablet.
        long aggToDeleteLow = 0;
        long aggToDeleteHigh = Long.MAX_VALUE;
        long aggNextProposeStart = 0;
        long aggPassStartVersion = Long.MAX_VALUE;
        boolean anyEmptyProposal = false;
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
            // The incremental protocol is always selected (max_versions_per_round is always set). Every
            // node in the partition gets the same partition-level state (the range to commit, the resume
            // cursor, and the pass retain floor); each node commits the range for its own tablets and
            // proposes over its own subset.
            vacuumRequest.maxVersionsPerRound = maxVersionsPerRound;
            // Bound the propose walk's pre-anchor hole search (searching down for the nearest existing version
            // below the resume cursor) by the batch-publish fold width: the only legitimate run of missing
            // versions is a batch-publish hole, at most lake_batch_publish_max_version_num versions wide. A
            // tablet that cannot anchor within that many step-downs is lagging / cross-generation and grinding
            // on to maxVersionsPerRound is wasted remote reads. The BE bounds only the pre-anchor step-down
            // with this; the chain walk and band width stay on maxVersionsPerRound.
            vacuumRequest.maxEmptyWalkVersions = (long) Config.lake_batch_publish_max_version_num;
            VacuumStatePB statePb = new VacuumStatePB();
            statePb.toDeleteLow = toDeleteLow;
            statePb.toDeleteHigh = toDeleteHigh;
            statePb.nextProposeStartVersion = nextProposeStart;
            statePb.passStartVersion = passStartVersion;
            vacuumRequest.vacuumState = statePb;
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
                    extraFileSize += response.extraFileSize;

                    // Recombine this node's proposal into the partition-level aggregate (see the field
                    // declarations above). The sub-message and its fields are optional: an old BE or a
                    // resume round may leave them unset, treat null as 0. A node that proposed an empty
                    // range (its tablets are grace blocked) zeroes the whole round, mirroring the
                    // single-node walk that short-circuits on the first grace-blocked tablet.
                    VacuumStatePB respState = response.vacuumState;
                    long nodeLow = (respState != null && respState.toDeleteLow != null) ? respState.toDeleteLow : 0;
                    long nodeHigh = (respState != null && respState.toDeleteHigh != null) ? respState.toDeleteHigh : 0;
                    long nodeCursor = (respState != null && respState.nextProposeStartVersion != null)
                            ? respState.nextProposeStartVersion : 0;
                    long nodePassStart = (respState != null && respState.passStartVersion != null)
                            ? respState.passStartVersion : 0;
                    if (nodeLow >= nodeHigh) {
                        anyEmptyProposal = true;
                    } else {
                        aggToDeleteHigh = Math.min(aggToDeleteHigh, nodeHigh);
                        aggToDeleteLow = Math.max(aggToDeleteLow, nodeLow);
                    }
                    // Fold in the pass retain floor even from an empty (drained) node: when nothing is left
                    // at/below the retain floor the BE reports its retain floor here, which is how a drained
                    // pass still advances the watermark and clears a pinned metadataSwitchVersion.
                    if (freshRound && nodePassStart > 0) {
                        aggPassStartVersion = Math.min(aggPassStartVersion, nodePassStart);
                    }
                    aggNextProposeStart = Math.max(aggNextProposeStart, nodeCursor);
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

        // Resolve the recombined proposal. If any node proposed nothing (grace blocked) or the nodes do
        // not agree on a non-empty range, the round proposes nothing and the pass is treated as drained.
        long respToDeleteLow = 0;
        long respToDeleteHigh = 0;
        if (!anyEmptyProposal && aggToDeleteHigh != Long.MAX_VALUE && aggToDeleteLow < aggToDeleteHigh) {
            respToDeleteLow = aggToDeleteLow;
            respToDeleteHigh = aggToDeleteHigh;
        }
        long respNextProposeStart = aggNextProposeStart;
        long respPassStartVersion = (aggPassStartVersion != Long.MAX_VALUE) ? aggPassStartVersion : 0;

        partition.setLastVacuumTime(startTime);
        // Per-round storage-accounting bookkeeping, independent of the incremental pass state: refresh the
        // partition's extra-file size from the BE's report plus whatever compaction added concurrently
        // during this round.
        long incrementExtraFileSize = partition.getExtraFileSize() - preExtraFileSize;
        partition.setExtraFileSize(extraFileSize + incrementExtraFileSize);
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
        if (!hasError && !responseFutures.isEmpty()) {
            updateVacuumState(db, table, partition, locker, freshRound, committingFinalBand, tablets,
                    respToDeleteLow, respToDeleteHigh, respNextProposeStart, respPassStartVersion, currentIndexIds);
            // Read the just-persisted proposal back for the log line below.
            VacuumState persisted = partition.getVacuumState();
            logToDeleteLow = persisted.getToDeleteLow();
            logToDeleteHigh = persisted.getToDeleteHigh();
            logNextProposeStart = persisted.getNextProposeStartVersion();
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
        LOG.info("incremental vacuum {}.{}.{} hasError={} visibleVersion={} min_retain={} minActiveTxnId={} " +
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
            Set<Long> currentIndexIds) {
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
                    partition.setMetadataSwitchVersion(0);
                }
                // Advance the walk floor to the completed pass's retain floor: the pass drained everything
                // below it, so the next round's prev_garbage_version walk stops there instead of
                // re-descending into the already-vacuumed band. It is kept in the VacuumState (persisted +
                // journaled, and preserved across reset()) so it survives an FE failover -- a new leader
                // keeps the floor instead of re-walking from the bottom and re-reading already-deleted
                // versions out of the metacache. passFloor is the partition-level floor (the MIN over the
                // tablets' per-tablet floors), applied uniformly to every tablet next round -- a higher
                // per-tablet floor would skip the [passFloor, tabletFloor) garbage the partition-level
                // intersection did not delete. Only advance at completion; a mid-pass advance would push the
                // floor above the resume cursor and strand the lower backlog.
                if (passFloor > state.getMinRetainedVersion()) {
                    state.setMinRetainedVersion(passFloor);
                }
                // Also mirror the floor onto each tablet's in-memory LakeTablet.minRetainedVersion. This copy does
                // NOT drive the walk (the request uses the durable VacuumState.minRetainedVersion above); it feeds
                // the tablet-repair / verify path (TabletRepairHelper) and SHOW TABLET's MinVersion column,
                // which read LakeTablet.getMinVersion() to bound the version range they inspect. Advance only
                // the tablets dispatched this round -- a completed pass had no send error, so every dispatched
                // tablet responded; a tablet that became visible after dispatch is absent here and keeps its
                // own floor rather than being wrongly marked vacuumed down to passFloor.
                for (Tablet tablet : tablets) {
                    LakeTablet lakeTablet = (LakeTablet) tablet;
                    if (passFloor > lakeTablet.getMinVersion()) {
                        lakeTablet.setMinVersion(passFloor);
                    }
                }
                state.reset();
            } else {
                // Persist the newly proposed range and resume cursor to commit next round; the floor is
                // captured only on a fresh round (advance() guards that internally).
                state.advance(respToDeleteLow, respToDeleteHigh, respNextProposeStart, freshRound,
                        respPassStartVersion, currentIndexIds);
            }
            // Persist the just-committed state to the edit log under the SAME lock as the in-memory
            // update, so the journal stays a faithful in-order record and a concurrent image checkpoint
            // stays consistent. submitLog() serializes synchronously here (still under the lock), so the
            // live state object is captured atomically.
            GlobalStateMgr.getCurrentState().getEditLog().logModifyPartitionVacuumState(
                    new PartitionVacuumStateInfo(db.getId(), table.getId(), partition.getId(), state));
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), Lists.newArrayList(table.getId()), LockType.WRITE);
        }
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
