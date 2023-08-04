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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/TabletScheduler.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.clone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.CatalogRecycleBin;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DiskInfo.DiskState;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletStatus;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Tablet;
import com.starrocks.clone.SchedException.Status;
import com.starrocks.clone.TabletSchedCtx.Priority;
import com.starrocks.clone.TabletSchedCtx.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.LogUtil;
import com.starrocks.persist.ReplicaPersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.CloneTask;
import com.starrocks.task.DropReplicaTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TGetTabletScheduleRequest;
import com.starrocks.thrift.TGetTabletScheduleResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TabletScheduler saved the tablets produced by TabletChecker and try to schedule them.
 * It also tries to balance the cluster load.
 * <p>
 * We are expecting an efficient way to recovery the entire cluster and make it balanced.
 * Case 1:
 * A Backend is down. All tablets which has replica on this BE should be repaired as soon as possible.
 * <p>
 * Case 1.1:
 * As Backend is down, some tables should be repaired in high priority. So the clone task should be able
 * to be preempted.
 * <p>
 * Case 2:
 * A new Backend is added to the cluster. Replicas should be transfer to that host to balance the cluster load.
 */
public class TabletScheduler extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TabletScheduler.class);

    // the minimum interval of updating cluster statistics and priority of tablet info
    private static final long STAT_UPDATE_INTERVAL_MS = 20L * 1000L; // 20s

    private static final long SCHEDULE_INTERVAL_MS = 1000; // 1s

    protected static final int MAX_SLOT_PER_PATH = 64;
    protected static final int MIN_SLOT_PER_PATH = 2;

    private static final long CLUSTER_LOAD_STATISTICS_LOGGING_INTERVAL_MS = 60000; // 1min

    /**
     * If the number of tablets which have finished scheduling is less than the
     * (total number of tablets per bucket in colocate group) * COLOCATE_BACKEND_RESET_RATIO,
     * the cost of backends reset is considered relatively cheap, we will do backend reset optimization.
     */
    private static final double COLOCATE_BACKEND_RESET_RATIO = 0.3;

    private static final int BLOCKING_ADD_SLEEP_DURATION_MS = 200;

    /*
     * Tablet is added to pendingTablets as well it's id in allTabletIds.
     * TabletScheduler will take tablet from pendingTablets but will not remove its id from allTabletIds when
     * handling a tablet.
     * Tablet' id can only be removed after the clone task is done(timeout, cancelled or finished).
     * So if a tablet's id is still in allTabletIds, TabletChecker can not add tablet to TabletScheduler.
     *
     * pendingTablets + runningTablets = allTabletIds
     *
     * pendingTablets, allTabletIds, runningTablets and schedHistory are protected by 'synchronized'
     */
    private PriorityQueue<TabletSchedCtx> pendingTablets = new PriorityQueue<>();
    private final Set<Long> allTabletIds = Sets.newHashSet();
    // contains all tabletCtxs which state are RUNNING
    private final Map<Long, TabletSchedCtx> runningTablets = Maps.newHashMap();
    // save the latest 1000 scheduled tablet info
    private final Queue<TabletSchedCtx> schedHistory = EvictingQueue.create(1000);

    // be id -> #working slots
    private final Map<Long, PathSlot> backendsWorkingSlots = Maps.newConcurrentMap();
    private ClusterLoadStatistic loadStatistic;
    private long lastStatUpdateTime = 0;
    private long lastClusterLoadLoggingTime = 0;

    private long lastSlotAdjustTime = 0;

    private int currentSlotPerPathConfig = 0;

    private final TabletSchedulerStat stat;
    private final Rebalancer rebalancer;

    private final AtomicBoolean forceCleanSchedQ = new AtomicBoolean(false);

    // result of adding a tablet to pendingTablets
    public enum AddResult {
        ADDED, // success to add
        ALREADY_IN, // already added, skip
        LIMIT_EXCEED // number of pending tablets exceed the limit
    }

    public TabletScheduler(TabletSchedulerStat stat) {
        super("tablet scheduler", SCHEDULE_INTERVAL_MS);
        this.stat = stat;
        this.rebalancer = new DiskAndTabletLoadReBalancer();
    }

    public TabletSchedulerStat getStat() {
        return stat;
    }

    public void setLoadStatistic(ClusterLoadStatistic loadStatistic) {
        this.loadStatistic = loadStatistic;
    }

    /*
     * update working slots at the beginning of each round
     */
    private boolean updateWorkingSlots() {
        // Compute delta that will be checked to update slot number per storage path and
        // record new value of `Config.schedule_slot_num_per_path`.
        int cappedVal = Config.tablet_sched_slot_num_per_path < MIN_SLOT_PER_PATH ? MIN_SLOT_PER_PATH :
                (Config.tablet_sched_slot_num_per_path > MAX_SLOT_PER_PATH ? MAX_SLOT_PER_PATH :
                        Config.tablet_sched_slot_num_per_path);
        int delta = 0;
        int oldSlotPerPathConfig = currentSlotPerPathConfig;
        if (currentSlotPerPathConfig != 0) {
            delta = cappedVal - currentSlotPerPathConfig;
        }
        currentSlotPerPathConfig = cappedVal;

        ImmutableMap<Long, Backend> backends = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend();
        if (backends == null) {
            return false;
        }

        for (Backend backend : backends.values()) {
            if (!backend.hasPathHash() && backend.isAlive()) {
                // when upgrading, backend may not get path info yet. so return false and wait for next round.
                // and we should check if backend is alive. If backend is dead when upgrading, this backend
                // will never report its path hash, and tablet scheduler is blocked.
                LOG.info("not all backends have path info");
                return false;
            }
        }

        // update exist backends
        Set<Long> deletedBeIds = Sets.newHashSet();
        for (Long beId : backendsWorkingSlots.keySet()) {
            if (backends.containsKey(beId)) {
                Backend backend = backends.get(beId);
                if (backend == null) {
                    continue;
                }
                ImmutableMap<String, DiskInfo> disks = backend.getDisks();
                if (disks == null) {
                    continue;
                }
                List<Long> pathHashes = disks.values().stream()
                        .filter(v -> v.getState() == DiskState.ONLINE)
                        .map(DiskInfo::getPathHash).collect(Collectors.toList());
                backendsWorkingSlots.get(beId).updatePaths(pathHashes, currentSlotPerPathConfig);
            } else {
                deletedBeIds.add(beId);
            }
        }

        // delete non-exist backends
        for (Long beId : deletedBeIds) {
            backendsWorkingSlots.remove(beId);
            LOG.info("delete non exist backend: {}", beId);
        }

        // add new backends
        for (Backend be : backends.values()) {
            if (!backendsWorkingSlots.containsKey(be.getId())) {
                List<Long> pathHashes =
                        be.getDisks().values().stream().map(DiskInfo::getPathHash).collect(Collectors.toList());
                PathSlot slot = new PathSlot(pathHashes, currentSlotPerPathConfig);
                backendsWorkingSlots.put(be.getId(), slot);
                LOG.info("add new backend {} with slots num: {}", be.getId(), be.getDisks().size());
            }
        }

        if (delta != 0) {
            LOG.info("Going to update slots per path. delta: {}, before: {}", delta, oldSlotPerPathConfig);
            int finalDelta = delta;
            backendsWorkingSlots.forEach((beId, pathSlot) -> pathSlot.updateSlot(finalDelta));
        }

        return true;
    }

    public Map<Long, PathSlot> getBackendsWorkingSlots() {
        return backendsWorkingSlots;
    }

    /**
     * add a ready-to-be-scheduled tablet to pendingTablets, if it has not being added before.
     */
    public synchronized AddResult addTablet(TabletSchedCtx tablet, boolean force) {
        // Under no circumstance should we repeatedly add a tablet to pending queue
        // to schedule, because this will break the scheduling logic. Besides, with current design,
        // we have to maintain the constraint that `allTabletIds = runningTablets + pendingTablets`.
        // `allTabletIds` and `runningTablets` are defined as unique container, if we schedule a
        // tablet with different `TabletSchedCtx` at the same time, the reference in `runningTablets`
        // can be messed up.
        // `force` here should only mean that we can exceed the size limit of
        // `pendingTablets` and `runningTablets` if there is too many tablets to schedule.
        if (containsTablet(tablet.getTabletId())) {
            return AddResult.ALREADY_IN;
        }

        // if this is not a BALANCE task, and not a force add,
        // and number of scheduling tablets exceed the limit,
        // refuse to add.
        if (tablet.getType() != TabletSchedCtx.Type.BALANCE && !force
                && (pendingTablets.size() > Config.tablet_sched_max_scheduling_tablets
                || runningTablets.size() > Config.tablet_sched_max_scheduling_tablets)) {
            return AddResult.LIMIT_EXCEED;
        }

        if (force) {
            LOG.debug("forcefully add tablet {} to table scheduler pending queue", tablet.getTabletId());
        }

        allTabletIds.add(tablet.getTabletId());
        pendingTablets.offer(tablet);
        return AddResult.ADDED;
    }

    public Pair<Boolean, Long> blockingAddTabletCtxToScheduler(Database db, TabletSchedCtx tabletSchedCtx,
                                                               boolean forceAdd) {
        // first: added or not, second: total sleep time in ms
        Pair<Boolean, Long> result = new Pair<>(false, 0L);
        try {
            do {
                AddResult res = addTablet(tabletSchedCtx, forceAdd /* force or not */);
                if (res == AddResult.LIMIT_EXCEED) {
                    db.readUnlock();
                    // It's ok to sleep a relative long time here so that the scheduler will spare more
                    // slots after the sleep and the following adding won't block.
                    Thread.sleep(BLOCKING_ADD_SLEEP_DURATION_MS);
                    result.second += BLOCKING_ADD_SLEEP_DURATION_MS;
                    db.readLock();
                } else {
                    result.first = (res == AddResult.ADDED);
                    break;
                }
            } while (true);
        } catch (InterruptedException e) {
            LOG.warn(e);
            db.readLock();
        }

        return result;
    }

    public void forceCleanSchedQ() {
        forceCleanSchedQ.set(true);
    }

    public synchronized boolean containsTablet(long tabletId) {
        return allTabletIds.contains(tabletId);
    }

    public synchronized Map<GroupId, Long> getTabletsNumInScheduleForEachCG() {
        Map<GroupId, Long> result = Maps.newHashMap();
        List<Stream<TabletSchedCtx>> streams = Lists.newArrayList(pendingTablets.stream(),
                runningTablets.values().stream());
        // Exclude the VERSION_INCOMPLETE tablet, because they are not added because of relocation.
        streams.forEach(s -> s.filter(t ->
                        (t.getColocateGroupId() != null && t.getTabletStatus() != TabletStatus.VERSION_INCOMPLETE)
                ).forEach(t -> result.merge(t.getColocateGroupId(), 1L, Long::sum))
        );
        return result;
    }

    private synchronized TabletSchedCtx getTabletSchedCtx(long tabletId) {
        TabletSchedCtx tabletSchedCtx = runningTablets.get(tabletId);
        if (tabletSchedCtx == null) {
            Optional<TabletSchedCtx> optional =
                    pendingTablets.stream().filter(ctx -> ctx.getTabletId() == tabletId).findFirst();
            if (optional.isPresent()) {
                tabletSchedCtx = optional.get();
            }
        }
        return tabletSchedCtx;
    }

    /**
     * For tablet with single replica set, after re-balancing, the src replica will be set to decommissioned state
     * to clean redundant replica, but because there may exist loading txns which have run concurrently with
     * re-balancing process, and the newly cloned replica would have failed versions if the loading txn doesn't
     * see these new replicas.
     * <p>
     * So the tablet may in a state where all of its replicas are in an abnormal state, the original replica
     * has complete versions but is in decommissioned state and the newly cloned replica has failed version.
     * In this situation, the tablet is not queryable, we should reset the original replica's state to normal ASAP.
     * <p>
     * Besides, here we reset the state only when the newly cloned replica has failed version to avoid
     * extra scheduling cost.
     *
     * @param replicas list of replicas belong to a tablet
     */
    public static void resetDecommStatForSingleReplicaTabletUnlocked(long tabletId, List<Replica> replicas) {
        TabletScheduler tabletScheduler = GlobalStateMgr.getCurrentState().getTabletScheduler();
        TabletSchedCtx tabletSchedCtx = tabletScheduler.getTabletSchedCtx(tabletId);
        if (tabletSchedCtx != null) {
            Replica decommissionedReplica = tabletSchedCtx.getDecommissionedReplica();
            for (Replica replica : replicas) {
                if (replica.getState() == Replica.ReplicaState.DECOMMISSION && replica.getLastFailedVersion() < 0
                        && decommissionedReplica != null && replica.getId() == decommissionedReplica.getId()) {
                    tabletScheduler.finalizeTabletCtx(tabletSchedCtx, TabletSchedCtx.State.CANCELLED,
                            "src replica of rebalance need to reset state, replicas: " +
                                    tabletSchedCtx.getTablet().getReplicaInfos());
                    break;
                }
            }
        }
    }

    /**
     * Iterate current tablets, change their priority to VERY_HIGH if necessary.
     */
    public synchronized void changeTabletsPriorityToVeryHigh(long dbId, long tblId, List<Long> partitionIds) {
        PriorityQueue<TabletSchedCtx> newPendingTablets = new PriorityQueue<>();
        for (TabletSchedCtx tabletCtx : pendingTablets) {
            if (tabletCtx.getDbId() == dbId && tabletCtx.getTblId() == tblId
                    && partitionIds.contains(tabletCtx.getPartitionId())) {
                tabletCtx.setOrigPriority(Priority.VERY_HIGH);
            }
            newPendingTablets.add(tabletCtx);
        }
        pendingTablets = newPendingTablets;
    }

    /**
     * TabletScheduler will run as a daemon thread at a very short interval(default 1 sec)
     * Firstly, it will try to update cluster load statistic and check if priority need to be adjusted.
     * Then, it will schedule the tablets in pendingTablets.
     * Thirdly, it will check the current running tasks.
     * Finally, it tries to balance the cluster if possible.
     * <p>
     * Schedule rules:
     * 1. tablet with higher priority will be scheduled first.
     * 2. high priority should be downgraded if it fails to be schedule too many times.
     * 3. priority may be upgraded if it is not being schedule for a long time.
     * 4. every pending task should have a max scheduled time, if schedule fails too many times, it should be removed.
     * 5. every running task should have a timeout, to avoid running forever.
     * 6. every running task should also have a max failure time,if clone task fails too many times,
     * it should be removed.
     */
    @Override
    protected void runAfterCatalogReady() {
        if (!updateWorkingSlots()) {
            return;
        }

        boolean loadStatUpdated = false;
        if (System.currentTimeMillis() - lastStatUpdateTime > STAT_UPDATE_INTERVAL_MS) {
            updateClusterLoadStatisticsAndPriority();
            loadStatUpdated = true;
        }

        schedulePendingTablets();

        handleRunningTablets();

        // selectTabletsForBalance should depend on latest load statistics
        // do not select others balance task when there is running or pending balance tasks
        // to avoid generating repeated task
        if (loadStatUpdated && getBalanceTabletsNumber() <= 0) {
            long startTS = System.currentTimeMillis();
            selectTabletsForBalance();
            long usedTS = System.currentTimeMillis() - startTS;
            if (usedTS > 1000L) {
                LOG.warn("select balance tablets cost too much time: {} seconds", usedTS / 1000L);
            }
        }

        handleForceCleanSchedQ();

        stat.counterTabletScheduleRound.incrementAndGet();
    }

    private void updateClusterLoadStatisticsAndPriority() {
        updateClusterLoadStatistic();
        rebalancer.updateLoadStatistic(loadStatistic);

        adjustPriorities();

        lastStatUpdateTime = System.currentTimeMillis();
    }

    /**
     * Here is the only place we update the cluster load statistic info.
     * We will not update this info dynamically along with the clone job's running.
     * Although it will cause a little inaccuracy, but is within a controllable range,
     * because we already limit the total number of running clone jobs in cluster by 'backend slots'
     */
    private void updateClusterLoadStatistic() {
        ClusterLoadStatistic clusterLoadStatistic =
                new ClusterLoadStatistic(GlobalStateMgr.getCurrentSystemInfo(), GlobalStateMgr.getCurrentInvertedIndex());
        clusterLoadStatistic.init();
        if (System.currentTimeMillis() - lastClusterLoadLoggingTime > CLUSTER_LOAD_STATISTICS_LOGGING_INTERVAL_MS) {
            LOG.info("update cluster load statistic:\n{}", clusterLoadStatistic.getBrief());
            lastClusterLoadLoggingTime = System.currentTimeMillis();
        }
        this.loadStatistic = clusterLoadStatistic;
    }

    public ClusterLoadStatistic getLoadStatistic() {
        return loadStatistic;
    }

    /**
     * adjust priorities of all tablet infos
     */
    private synchronized void adjustPriorities() {
        int size = pendingTablets.size();
        int changedNum = 0;
        TabletSchedCtx tabletCtx;
        for (int i = 0; i < size; i++) {
            tabletCtx = pendingTablets.poll();
            if (tabletCtx == null) {
                break;
            }

            if (tabletCtx.adjustPriority(stat)) {
                changedNum++;
            }
            pendingTablets.add(tabletCtx);
        }

        if (changedNum != 0) {
            LOG.info("adjust priority for all tablets. changed: {}, total: {}", changedNum, size);
        }
    }

    private void debugLogPendingTabletsStats() {
        StringBuilder sb = new StringBuilder();
        for (Priority prio : Priority.values()) {
            sb.append(String.format("%s priority tablets count: %d\n",
                    prio.name(),
                    pendingTablets.stream().filter(t -> t.getDynamicPriority() == prio).count()));
        }
        LOG.debug("pending tablets current count: {}\n{}", pendingTablets.size(), sb);
    }

    private boolean checkIfTabletExpired(TabletSchedCtx ctx) {
        return checkIfTabletExpired(ctx, GlobalStateMgr.getCurrentRecycleBin(), System.currentTimeMillis());
    }

    /**
     * make sure tablet won't be expired and erased soon
     */
    protected boolean checkIfTabletExpired(TabletSchedCtx ctx, CatalogRecycleBin recycleBin, long currentTimeMs) {
        // check if about to erase
        long dbId = ctx.getDbId();
        if (recycleBin.getDatabase(dbId) != null && !recycleBin.ensureEraseLater(dbId, currentTimeMs)) {
            LOG.warn("discard ctx because db {} will erase soon: {}", dbId, ctx);
            return true;
        }
        long tableId = ctx.getTblId();
        if (recycleBin.getTable(dbId, tableId) != null && !recycleBin.ensureEraseLater(tableId, currentTimeMs)) {
            LOG.warn("discard ctx because table {} will erase soon: {}", tableId, ctx);
            return true;
        }
        long partitionId = ctx.getPartitionId();
        if (recycleBin.getPartition(partitionId) != null
                && !recycleBin.ensureEraseLater(partitionId, currentTimeMs)) {
            LOG.warn("discard ctx because partition {} will erase soon: {}", partitionId, ctx);
            return true;
        }
        return false;
    }

    /**
     * get at most BATCH_NUM tablets from queue, and try to schedule them.
     * After handle, the tablet info should be
     * 1. in runningTablets with state RUNNING, if being scheduled success.
     * 2. or in schedHistory with state CANCELLING, if some unrecoverable error happens.
     * 3. or in pendingTablets with state PENDING, if failed to be scheduled.
     * <p>
     * if in schedHistory, it should be removed from allTabletIds.
     */
    private void schedulePendingTablets() {
        long start = System.currentTimeMillis();
        List<TabletSchedCtx> currentBatch = getNextTabletCtxBatch();
        if (LOG.isDebugEnabled()) {
            debugLogPendingTabletsStats();
            LOG.debug("get {} tablets to schedule", currentBatch.size());
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        for (TabletSchedCtx tabletCtx : currentBatch) {
            try {
                // reset errMsg for new scheduler round
                tabletCtx.setErrMsg(null);
                scheduleTablet(tabletCtx, batchTask);
            } catch (SchedException e) {
                tabletCtx.increaseFailedSchedCounter();
                tabletCtx.setErrMsg(e.getMessage());

                if (e.getStatus() == Status.SCHEDULE_RETRY) {
                    LOG.debug("scheduling for tablet[{}] failed, type: {}, reason: {}",
                            tabletCtx.getTabletId(), tabletCtx.getType().name(), e.getMessage());
                    if (tabletCtx.getType() == Type.BALANCE) {
                        // if balance is disabled, remove this tablet
                        if (Config.tablet_sched_disable_balance) {
                            finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED,
                                    "disable balance and " + e.getMessage());
                        } else {
                            // remove the balance task if it fails to be scheduled many times
                            if (tabletCtx.getFailedSchedCounter() > 10) {
                                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED,
                                        "schedule failed too many times and " + e.getMessage());
                            } else {
                                // we must release resource it currently holds, and be scheduled again
                                tabletCtx.releaseResource(this);
                                // adjust priority to avoid some higher priority always be the first in pendingTablets
                                stat.counterTabletScheduledFailed.incrementAndGet();
                                addBackToPendingTablets(tabletCtx);
                            }
                        }
                    } else {
                        // we must release resource it currently holds, and be scheduled again
                        tabletCtx.releaseResource(this);
                        // adjust priority to avoid some higher priority always be the first in pendingTablets
                        stat.counterTabletScheduledFailed.incrementAndGet();
                        addBackToPendingTablets(tabletCtx);
                    }
                } else if (e.getStatus() == Status.FINISHED) {
                    // schedule redundant tablet will throw this exception
                    stat.counterTabletScheduledSucceeded.incrementAndGet();
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.FINISHED, e.getMessage());
                } else {
                    Preconditions.checkState(e.getStatus() == Status.UNRECOVERABLE, e.getStatus());
                    // discard
                    stat.counterTabletScheduledDiscard.incrementAndGet();
                    finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getMessage());
                }
                continue;
            } catch (Exception e) {
                LOG.warn("got unexpected exception, discard this schedule. tablet: {}",
                        tabletCtx.getTabletId(), e);
                stat.counterTabletScheduledFailed.incrementAndGet();
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.UNEXPECTED, e.getMessage());
                continue;
            }

            Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.RUNNING);
            stat.counterTabletScheduledSucceeded.incrementAndGet();
            addToRunningTablets(tabletCtx);
        }

        // must send task after adding tablet info to runningTablets.
        for (AgentTask task : batchTask.getAllTasks()) {
            if (AgentTaskQueue.addTask(task)) {
                stat.counterCloneTask.incrementAndGet();
            }
            LOG.info("add clone task to agent task queue: {}", task);
        }

        // send task immediately
        AgentTaskExecutor.submit(batchTask);

        long cost = System.currentTimeMillis() - start;
        stat.counterTabletScheduleCostMs.addAndGet(cost);
    }

    private synchronized void addToRunningTablets(TabletSchedCtx tabletCtx) {
        runningTablets.put(tabletCtx.getTabletId(), tabletCtx);
    }

    /**
     * Only for test.
     *
     * @param tabletCtx tablet schedule context
     */
    private synchronized void addToPendingTablets(TabletSchedCtx tabletCtx) {
        pendingTablets.add(tabletCtx);
    }

    /**
     * we take the tablet out of the runningTablets and then handle it,
     * avoid other threads see it.
     * Whoever takes this tablet, make sure to put it to the schedHistory or back to runningTablets.
     */
    private synchronized TabletSchedCtx takeRunningTablets(long tabletId) {
        return runningTablets.remove(tabletId);
    }

    /**
     * Try to schedule a single tablet.
     */
    private void scheduleTablet(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        LOG.debug("schedule tablet: {}, type: {}, status: {}", tabletCtx.getTabletId(), tabletCtx.getType(),
                tabletCtx.getTabletStatus());
        long currentTime = System.currentTimeMillis();
        tabletCtx.setLastSchedTime(currentTime);
        tabletCtx.setLastVisitedTime(currentTime);
        stat.counterTabletScheduled.incrementAndGet();

        // check this tablet again
        Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(tabletCtx.getDbId());
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db does not exist");
        }

        Pair<TabletStatus, TabletSchedCtx.Priority> statusPair;
        db.readLock();
        try {
            OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState()
                    .getTableIncludeRecycleBin(db, tabletCtx.getTblId());
            if (tbl == null) {
                throw new SchedException(Status.UNRECOVERABLE, "tbl does not exist");
            }
            if (tbl.isCloudNativeTableOrMaterializedView()) {
                throw new SchedException(Status.UNRECOVERABLE, "tablet is managed externally");
            }

            boolean isColocateTable = GlobalStateMgr.getCurrentColocateIndex().isColocateTable(tbl.getId());

            OlapTableState tableState = tbl.getState();

            Partition partition = GlobalStateMgr.getCurrentState()
                    .getPartitionIncludeRecycleBin(tbl, tabletCtx.getPartitionId());
            if (partition == null) {
                throw new SchedException(Status.UNRECOVERABLE, "partition does not exist");
            }

            short replicaNum = GlobalStateMgr.getCurrentState()
                    .getReplicationNumIncludeRecycleBin(tbl.getPartitionInfo(), partition.getId());
            if (replicaNum == (short) -1) {
                throw new SchedException(Status.UNRECOVERABLE, "invalid replication number");
            }

            DataProperty dataProperty = GlobalStateMgr.getCurrentState()
                    .getDataPropertyIncludeRecycleBin(tbl.getPartitionInfo(), partition.getId());
            if (dataProperty == null) {
                throw new SchedException(Status.UNRECOVERABLE, "partition data property not exist");
            }

            MaterializedIndex idx = partition.getIndex(tabletCtx.getIndexId());
            if (idx == null) {
                throw new SchedException(Status.UNRECOVERABLE, "index does not exist");
            }

            LocalTablet tablet = (LocalTablet) idx.getTablet(tabletCtx.getTabletId());
            Preconditions.checkNotNull(tablet);

            if (isColocateTable) {
                GroupId groupId = GlobalStateMgr.getCurrentColocateIndex().getGroup(tbl.getId());
                if (groupId == null) {
                    throw new SchedException(Status.UNRECOVERABLE, "colocate group does not exist");
                }

                int tabletOrderIdx = tabletCtx.getTabletOrderIdx();
                if (tabletOrderIdx == -1) {
                    tabletOrderIdx = idx.getTabletOrderIdx(tablet.getId());
                }
                Preconditions.checkState(tabletOrderIdx != -1);

                Set<Long> backendsSet = GlobalStateMgr.getCurrentColocateIndex()
                        .getTabletBackendsByGroup(groupId, tabletOrderIdx);
                trySkipRelocateSchedAndResetBackendSeq(tabletCtx, partition.getVisibleVersion(),
                        replicaNum, backendsSet, tablet);
                TabletStatus st = tablet.getColocateHealthStatus(
                        partition.getVisibleVersion(),
                        replicaNum,
                        backendsSet);
                statusPair = Pair.create(st, Priority.HIGH);
                tabletCtx.setColocateGroupBackendIds(backendsSet);
            } else {
                List<Long> aliveBeIdsInCluster = GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true);
                statusPair = tablet.getHealthStatusWithPriority(
                        GlobalStateMgr.getCurrentSystemInfo(),
                        partition.getVisibleVersion(),
                        replicaNum,
                        aliveBeIdsInCluster);
            }

            if (tabletCtx.getType() == TabletSchedCtx.Type.BALANCE && tableState != OlapTableState.NORMAL) {
                // If table is under ALTER process, do not allow to do balance.
                throw new SchedException(Status.UNRECOVERABLE, "table's state is not NORMAL");
            }

            if (statusPair.first != TabletStatus.VERSION_INCOMPLETE
                    && (partition.getState() != PartitionState.NORMAL || tableState != OlapTableState.NORMAL)
                    && tableState != OlapTableState.WAITING_STABLE) {
                // If table is under ALTER process(before FINISHING), do not allow to add or delete replica.
                // VERSION_INCOMPLETE will repair the replica in place, which is allowed.
                // The WAITING_STABLE state is an exception. This state indicates that the table is
                // executing an alter job, but the alter job is in a PENDING state and is waiting for
                // the table to become stable. In this case, we allow the tablet repair to proceed.
                throw new SchedException(Status.UNRECOVERABLE,
                        "table is in alter process, but tablet status is " + statusPair.first.name());
            }

            TabletStatus oldStatus = tabletCtx.getTabletStatus();
            tabletCtx.setTabletStatus(statusPair.first);
            if (statusPair.first == TabletStatus.HEALTHY && tabletCtx.getType() == TabletSchedCtx.Type.REPAIR) {
                throw new SchedException(Status.UNRECOVERABLE, "tablet is healthy");
            } else if (statusPair.first != TabletStatus.HEALTHY
                    && tabletCtx.getType() == TabletSchedCtx.Type.BALANCE) {
                // we select an unhealthy tablet to do balance, which is not right.
                // so here we change it to a REPAIR task, and also reset its priority
                tabletCtx.releaseResource(this);
                tabletCtx.setType(TabletSchedCtx.Type.REPAIR);
                tabletCtx.setOrigPriority(statusPair.second);
                tabletCtx.setLastSchedTime(currentTime);
                tabletCtx.setLastVisitedTime(currentTime);
            }

            // we do not concern priority here.
            // once we take the tablet out of priority queue, priority is meaningless.
            tabletCtx.setTablet(tablet);
            tabletCtx.setVersionInfo(partition.getVisibleVersion(),
                    partition.getCommittedVersion(), partition.getVisibleTxnId());
            tabletCtx.setSchemaHash(tbl.getSchemaHashByIndexId(idx.getId()));
            tabletCtx.setStorageMedium(dataProperty.getStorageMedium());
            if (!Objects.equals(oldStatus, statusPair.first)) {
                LOG.info("change TabletSchedCtx status from {} to {}, partition visible version: {}," +
                                " visibleTxnId: {}, tablet: {}, all version infos: {}",
                        oldStatus, statusPair.first, tabletCtx.getVisibleVersion(),
                        tabletCtx.getVisibleTxnId(),
                        tabletCtx.getTabletId(),
                        tabletCtx.getTablet().getReplicaInfos());
            }
        } finally {
            db.readUnlock();
        }

        handleTabletByTypeAndStatus(statusPair.first, tabletCtx, batchTask);
    }

    /**
     * If all the replicas for tablets that belongs to some bucket are healthy(e.g. previously
     * considered not alive backend comes alive or backend decommission is canceled), in this case,
     * we don't really need to continue to execute the previously made relocation decision, we just
     * cancel the scheduling by setting the status to HEALTHY.
     * <p>
     * If some tablets belong to the same bucket has already been scheduled based on the new backend sequence,
     * we won't try to skip the current scheduling and reset the backend sequence because this will cause the tablets
     * which have finished scheduling to be scheduled again.
     */
    private void trySkipRelocateSchedAndResetBackendSeq(TabletSchedCtx ctx, long visibleVersion, short replicaNum,
                                                        Set<Long> currentBackendsSet, LocalTablet tablet) {
        if (ctx.getRelocationForRepair()) {
            Set<Long> lastBackendsSet = ColocateTableBalancer.getInstance().getLastBackendSeqForBucket(ctx);
            // if we have already reset the backend set to last backend set, skip the reset action
            if (lastBackendsSet.isEmpty() || currentBackendsSet.equals(lastBackendsSet)) {
                return;
            }

            // check the backends of original sequence are all available
            for (Long backendId : lastBackendsSet) {
                Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
                if (!be.isAvailable()) {
                    return;
                }
            }

            int num = ColocateTableBalancer.getInstance().getScheduledTabletNumForBucket(ctx);
            int totalTabletsPerBucket = GlobalStateMgr.getCurrentColocateIndex()
                    .getNumOfTabletsPerBucket(ctx.getColocateGroupId());
            if (num <= totalTabletsPerBucket * COLOCATE_BACKEND_RESET_RATIO) {
                TabletStatus st = tablet.getColocateHealthStatus(visibleVersion, replicaNum, lastBackendsSet);
                if (st != TabletStatus.COLOCATE_MISMATCH) {
                    GlobalStateMgr.getCurrentColocateIndex().setBackendsSetByIdxForGroup(ctx.getColocateGroupId(),
                            ctx.getTabletOrderIdx(), lastBackendsSet);
                    LOG.info("all current backends are available for tablet {}, bucket index: {}, reset " +
                                    "backend set to: {} for colocate group {}, before backend set: {}",
                            ctx.getTabletId(), ctx.getTabletOrderIdx(), lastBackendsSet,
                            ctx.getColocateGroupId(), currentBackendsSet);
                }
            }
        }
    }

    @VisibleForTesting
    public void handleTabletByTypeAndStatus(TabletStatus status, TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        if (tabletCtx.getType() == Type.REPAIR) {
            switch (status) {
                case REPLICA_MISSING:
                    handleReplicaMissing(tabletCtx, batchTask);
                    break;
                case VERSION_INCOMPLETE:
                case NEED_FURTHER_REPAIR: // same as version incomplete, it prefers to the dest replica which need further repair
                    handleReplicaVersionIncomplete(tabletCtx, batchTask);
                    break;
                case REPLICA_RELOCATING:
                    handleReplicaRelocating(tabletCtx, batchTask);
                    break;
                case REDUNDANT:
                    handleRedundantReplica(tabletCtx, false);
                    break;
                case FORCE_REDUNDANT:
                    handleRedundantReplica(tabletCtx, true);
                    break;
                case COLOCATE_MISMATCH:
                    handleColocateMismatch(tabletCtx, batchTask);
                    break;
                case COLOCATE_REDUNDANT:
                    handleColocateRedundant(tabletCtx);
                    break;
                default:
                    break;
            }
        } else {
            // balance
            doBalance(tabletCtx, batchTask);
        }
    }

    /**
     * Replica is missing, which means there is no enough alive replicas.
     * So we need to find a destination backend to clone a new replica as possible as we can.
     * 1. find an available path in a backend as destination:
     * 1. backend need to be alive.
     * 2. backend of existing replicas should be excluded. (should not be on same host either)
     * 3. backend has available slot for clone.
     * 4. replica can fit in the path (consider the threshold of disk capacity and usage percent).
     * 5. try to find a path with the lowest load score.
     * 2. find an appropriate source replica:
     * 1. source replica should be healthy
     * 2. backend of source replica has available slot for clone.
     * <p>
     * 3. send clone task to destination backend
     */
    private void handleReplicaMissing(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        stat.counterReplicaMissingErr.incrementAndGet();
        // find an available dest backend and path
        RootPathLoadStatistic destPath = chooseAvailableDestPath(tabletCtx, false /* not for colocate */);
        Preconditions.checkNotNull(destPath);
        tabletCtx.setDest(destPath.getBeId(), destPath.getPathHash());

        if (Config.recover_with_empty_tablet && tabletCtx.getReplicas().size() == 1) {
            batchTask.addTask(tabletCtx.createEmptyReplicaAndTask());
            return;
        }

        // choose a source replica for cloning from
        tabletCtx.chooseSrcReplica(backendsWorkingSlots);

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
    }

    /**
     * Replica version is incomplete, which means this replica is missing some version,
     * and need to be cloned from a healthy replica, in-place.
     * <p>
     * 1. find the incomplete replica as destination replica
     * 2. find a healthy replica as source replica
     * 3. send clone task
     */
    private void handleReplicaVersionIncomplete(TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        stat.counterReplicaVersionMissingErr.incrementAndGet();
        ClusterLoadStatistic statistic = loadStatistic;
        if (statistic == null) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster does not exist");
        }

        boolean needFurtherRepair = tabletCtx.chooseDestReplicaForVersionIncomplete(backendsWorkingSlots);
        tabletCtx.chooseSrcReplicaForVersionIncomplete(backendsWorkingSlots);

        LOG.info("Adding VERSION_INCOMPLETE clone task, replica on backend {} chosen, needFurtherRepair: {}," +
                        " partition visible version: {}, visibleTxnId: {}, tablet: {}, all version infos: {}",
                tabletCtx.getDestBackendId(),
                needFurtherRepair,
                tabletCtx.getVisibleVersion(),
                tabletCtx.getVisibleTxnId(),
                tabletCtx.getTabletId(),
                tabletCtx.getTablet().getReplicaInfos());

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
    }

    /*
     * There are enough alive replicas with complete version in this tablet, but some backends may
     * under decommission.
     * First, we try to find a version incomplete replica on available BE.
     * If failed to find, then try to find a new BE to clone the replicas.
     *
     * Give examples of why:
     * Tablet X has 3 replicas on A, B, C 3 BEs.
     * C is decommissioned, so we choose the BE D to relocating the new replica,
     * After relocating, Tablet X has 4 replicas: A, B, C(decommission), D(may be version incomplete)
     * But D may be version incomplete because the clone task ran a long time, the new version
     * has been published.
     * At the next time of tablet checking, Tablet X's status is still REPLICA_RELOCATING,
     * If we don't choose D as dest BE to do the new relocating, it will choose new backend E to
     * store the new replicas. So back and forth, the number of replicas will increase forever.
     */
    private void handleReplicaRelocating(TabletSchedCtx tabletCtx, AgentBatchTask batchTask)
            throws SchedException {
        stat.counterReplicaUnavailableErr.incrementAndGet();
        try {
            handleReplicaVersionIncomplete(tabletCtx, batchTask);
            LOG.info(
                    "succeed to find version incomplete replica from tablet relocating. tablet: {} replicas: {} {}->{}",
                    tabletCtx.getTabletId(), tabletCtx.getTablet().getReplicaInfos(),
                    tabletCtx.getSrcReplica().getBackendId(), tabletCtx.getDestBackendId());
        } catch (SchedException e) {
            if (e.getStatus() == Status.SCHEDULE_RETRY || e.getStatus() == Status.UNRECOVERABLE) {
                LOG.debug("failed to find version incomplete replica from tablet relocating. " +
                                "reason: [{}], tablet: [{}], replicas: {} dest:{} try to find a new backend", e.getMessage(),
                        tabletCtx.getTabletId(), tabletCtx.getTablet().getReplicaInfos(),
                        tabletCtx.getDestBackendId());
                // the dest or src slot may be taken after calling handleReplicaVersionIncomplete(),
                // so we need to release these slots first.
                // and reserve the tablet in TabletSchedCtx so that it can continue to be scheduled.
                tabletCtx.releaseResource(this, true);
                handleReplicaMissing(tabletCtx, batchTask);
                LOG.info("succeed to find new backend for tablet relocating. tablet: {} replicas: {} {}->{}",
                        tabletCtx.getTabletId(), tabletCtx.getTablet().getReplicaInfos(),
                        tabletCtx.getSrcReplica().getBackendId(), tabletCtx.getDestBackendId());
            } else {
                throw e;
            }
        }
    }

    /**
     * replica is redundant, which means there are more replicas than we expected, which need to be dropped.
     * we just drop one redundant replica at a time, for safety reason.
     * choosing a replica to drop base on following priority:
     * 1. backend has been dropped
     * 2. replica is bad
     * 3. backend is not available
     * 4. replica's state is CLONE or DECOMMISSION
     * 5. replica's last failed version > 0
     * 6. replica with lower version
     * 7. replica on same host
     * 8. replica not in right cluster
     * 9. replica is the src replica of a rebalance task, we can try to get it from rebalancer
     * 10. replica on higher load backend
     */
    private void handleRedundantReplica(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        stat.counterReplicaRedundantErr.incrementAndGet();

        Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(tabletCtx.getDbId());
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db " + tabletCtx.getDbId() + " not exist");
        }
        try {
            db.writeLock();
            checkMetaExist(tabletCtx);
            if (deleteBackendDropped(tabletCtx, force)
                    || deleteBadReplica(tabletCtx, force)
                    || deleteBackendUnavailable(tabletCtx, force)
                    || deleteCloneOrDecommissionReplica(tabletCtx, force)
                    || deleteReplicaWithFailedVersion(tabletCtx, force)
                    || deleteReplicaWithLowerVersion(tabletCtx, force)
                    || deleteReplicaOnSameHost(tabletCtx, force)
                    || deleteReplicaChosenByRebalancer(tabletCtx, force)
                    || deleteReplicaOnHighLoadBackend(tabletCtx, force)) {
                // if we delete at least one redundant replica, we still throw a SchedException with status FINISHED
                // to remove this tablet from the pendingTablets(consider it as finished)
                throw new SchedException(Status.FINISHED, "redundant replica is deleted");
            }
        } finally {
            db.writeUnlock();
        }
        throw new SchedException(Status.UNRECOVERABLE, "unable to delete any redundant replicas");
    }

    private boolean deleteBackendDropped(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            long beId = replica.getBackendId();
            if (GlobalStateMgr.getCurrentSystemInfo().getBackend(beId) == null) {
                deleteReplicaInternal(tabletCtx, replica, "backend dropped", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteBadReplica(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.isBad()) {
                deleteReplicaInternal(tabletCtx, replica, "replica is bad", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteBackendUnavailable(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                continue;
            }
            if (!be.isAvailable()) {
                deleteReplicaInternal(tabletCtx, replica, "backend unavailable", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteCloneOrDecommissionReplica(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.getState() == ReplicaState.CLONE || replica.getState() == ReplicaState.DECOMMISSION) {
                deleteReplicaInternal(tabletCtx, replica, replica.getState() + " state", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithFailedVersion(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (replica.getLastFailedVersion() > 0) {
                deleteReplicaInternal(tabletCtx, replica, "version incomplete", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaWithLowerVersion(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        for (Replica replica : tabletCtx.getReplicas()) {
            if (!replica
                    .checkVersionCatchUp(tabletCtx.getVisibleVersion(), false)) {
                deleteReplicaInternal(tabletCtx, replica, "lower version", force);
                return true;
            }
        }
        return false;
    }

    private boolean deleteReplicaOnSameHost(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        ClusterLoadStatistic statistic = loadStatistic;
        if (statistic == null) {
            return false;
        }

        // collect replicas of this tablet.
        // host -> (replicas on same host)
        Map<String, List<Replica>> hostToReplicas = Maps.newHashMap();
        for (Replica replica : tabletCtx.getReplicas()) {
            Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(replica.getBackendId());
            if (be == null) {
                // this case should be handled in deleteBackendDropped()
                return false;
            }
            List<Replica> replicas = hostToReplicas.computeIfAbsent(be.getHost(), k -> Lists.newArrayList());
            replicas.add(replica);
        }

        // find if there are replicas on same host, if yes, delete one.
        for (List<Replica> replicas : hostToReplicas.values()) {
            if (replicas.size() > 1) {
                // delete one replica from replicas on same host.
                // better to choose high load backend
                return deleteFromHighLoadBackend(tabletCtx, replicas, force, statistic);
            }
        }

        return false;
    }

    private boolean deleteReplicaChosenByRebalancer(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        Long id = rebalancer.getToDeleteReplicaId(tabletCtx.getTabletId());
        if (id == -1L) {
            return false;
        }
        Replica chosenReplica = tabletCtx.getTablet().getReplicaById(id);
        if (chosenReplica != null) {
            deleteReplicaInternal(tabletCtx, chosenReplica, "src replica of rebalance", force);
            return true;
        }
        return false;
    }

    private boolean deleteReplicaOnHighLoadBackend(TabletSchedCtx tabletCtx, boolean force) throws SchedException {
        ClusterLoadStatistic statistic = loadStatistic;
        if (statistic == null) {
            return false;
        }

        return deleteFromHighLoadBackend(tabletCtx, tabletCtx.getReplicas(), force, statistic);
    }

    private boolean deleteFromHighLoadBackend(TabletSchedCtx tabletCtx, List<Replica> replicas,
                                              boolean force, ClusterLoadStatistic statistic) throws SchedException {
        Replica chosenReplica = null;
        double maxScore = 0;
        for (Replica replica : replicas) {
            BackendLoadStatistic beStatistic = statistic.getBackendLoadStatistic(replica.getBackendId());
            if (beStatistic == null) {
                continue;
            }

            /*
             * If the backend does not have the specified storage medium, we use mix load score to make
             * sure that at least one replica can be chosen.
             * This can happen if the StarRocks cluster is deployed with all, for example, SSD medium,
             * but create all tables with HDD storage medium property. Then getLoadScore(SSD) will
             * always return 0.0, so that no replica will be chosen.
             */
            double loadScore;
            if (beStatistic.hasMedium(tabletCtx.getStorageMedium())) {
                loadScore = beStatistic.getLoadScore(tabletCtx.getStorageMedium());
            } else {
                loadScore = beStatistic.getMixLoadScore();
            }

            if (loadScore > maxScore) {
                maxScore = loadScore;
                chosenReplica = replica;
            }
        }

        if (chosenReplica != null) {
            deleteReplicaInternal(tabletCtx, chosenReplica, "high load", force);
            return true;
        }
        return false;
    }

    /**
     * Just delete replica which does not locate in colocate backends set.
     * return true if delete one replica, otherwise, return false.
     */
    private boolean handleColocateRedundant(TabletSchedCtx tabletCtx) throws SchedException {
        Set<Long> backendSet = tabletCtx.getColocateBackendsSet();
        Preconditions.checkNotNull(backendSet);
        stat.counterReplicaColocateRedundant.incrementAndGet();
        Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(tabletCtx.getDbId());
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db " + tabletCtx.getDbId() + " not exist");
        }
        try {
            db.writeLock();
            checkMetaExist(tabletCtx);
            List<Replica> replicas = tabletCtx.getReplicas();
            for (Replica replica : replicas) {
                boolean forceDropBad = false;
                if (backendSet.contains(replica.getBackendId())) {
                    if (replica.isBad() && replicas.size() > 1) {
                        forceDropBad = true;
                        LOG.info("colocate tablet {}, replica {} is bad," +
                                        "will forcefully drop it, current backend set: {}",
                                tabletCtx.getTabletId(), replica.getBackendId(), backendSet);
                    } else {
                        continue;
                    }
                }

                deleteReplicaInternal(tabletCtx, replica, "colocate redundant", forceDropBad);
                throw new SchedException(Status.FINISHED, "colocate redundant replica is deleted");
            }
            throw new SchedException(Status.UNRECOVERABLE, "unable to delete any colocate redundant replicas");
        } finally {
            db.writeUnlock();
        }
    }

    private void deleteReplicaInternal(TabletSchedCtx tabletCtx, Replica replica, String reason, boolean force)
            throws SchedException {
        if (Config.tablet_sched_always_force_decommission_replica) {
            force = true;
        }
        /*
         * Before deleting a replica, we should make sure that there is no running txn on it
         *  and no more txns will be on it.
         * So we do followings:
         * 1. If replica is loadable, set a watermark txn id on it and set it state as DECOMMISSION, but not
         *      deleting it this time. The DECOMMISSION state will ensure that no more txns will be on this replicas.
         * 2. Wait for any txns before the watermark txn id to be finished. If all are finished, which means
         *      this replica is safe to be deleted.
         */
        if (!force && replica.getState().canLoad() && replica.getWatermarkTxnId() == -1) {
            long nextTxnId =
                    GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
            replica.setWatermarkTxnId(nextTxnId);
            tabletCtx.resetDecommissionedReplicaState();
            tabletCtx.setDecommissionedReplica(replica);
            replica.setState(ReplicaState.DECOMMISSION);
            // set priority to normal because it may wait for a long time. Remain it as VERY_HIGH may block other task.
            tabletCtx.setOrigPriority(Priority.NORMAL);
            LOG.info("decommission tablet:" + tabletCtx.getTabletId() + " type:" + tabletCtx.getType() + " replica:" +
                    replica.getBackendId() + " reason:" + reason + " watermark:" + nextTxnId + " replicas:" +
                    tabletCtx.getTablet().getReplicaInfos());
            throw new SchedException(Status.SCHEDULE_RETRY, "set watermark txn " + nextTxnId);
        } else if (replica.getState() == ReplicaState.DECOMMISSION && replica.getWatermarkTxnId() != -1) {
            long watermarkTxnId = replica.getWatermarkTxnId();
            try {
                if (!GlobalStateMgr.getCurrentGlobalTransactionMgr().isPreviousTransactionsFinished(watermarkTxnId,
                        tabletCtx.getDbId(), Lists.newArrayList(tabletCtx.getTblId()))) {
                    throw new SchedException(Status.SCHEDULE_RETRY,
                            "wait txn before " + watermarkTxnId + " to be finished");
                }
            } catch (AnalysisException e) {
                throw new SchedException(Status.UNRECOVERABLE, e.getMessage());
            }
        }

        String replicaInfos = tabletCtx.getTablet().getReplicaInfos();
        // delete this replica from globalStateMgr.
        // it will also delete replica from tablet inverted index.
        if (!tabletCtx.deleteReplica(replica)) {
            LOG.warn("delete replica for tablet: {} failed backend {} not found replicas:{}", tabletCtx.getTabletId(),
                    replica.getBackendId(), replicaInfos);
        }

        if (force) {
            // send the replica deletion task.
            // also this may not be necessary, but delete it will make things simpler.
            // NOTICE: only delete the replica from meta may not work. sometimes we can depend on tablet report
            // to delete these replicas, but in FORCE_REDUNDANT case, replica may be added to meta again in report
            // process.
            sendDeleteReplicaTask(replica.getBackendId(), tabletCtx.getTabletId(), tabletCtx.getSchemaHash());
        }
        // NOTE: TabletScheduler is specific for LocalTablet, LakeTablet will never go here.
        GlobalStateMgr.getCurrentInvertedIndex().markTabletForceDelete(tabletCtx.getTabletId(), replica.getBackendId());

        // write edit log
        ReplicaPersistInfo info = ReplicaPersistInfo.createForDelete(tabletCtx.getDbId(),
                tabletCtx.getTblId(),
                tabletCtx.getPartitionId(),
                tabletCtx.getIndexId(),
                tabletCtx.getTabletId(),
                replica.getBackendId());

        GlobalStateMgr.getCurrentState().getEditLog().logDeleteReplica(info);

        LOG.info("delete replica. tablet id: {}, backend id: {}. reason: {}, force: {} replicas: {}",
                tabletCtx.getTabletId(), replica.getBackendId(), reason, force, replicaInfos);
    }

    private void sendDeleteReplicaTask(long backendId, long tabletId, int schemaHash) {
        DropReplicaTask task = new DropReplicaTask(backendId, tabletId, schemaHash, false);
        AgentBatchTask batchTask = new AgentBatchTask();
        batchTask.addTask(task);
        AgentTaskExecutor.submit(batchTask);
        LOG.info("send delete replica task for tablet {} in backend {}", tabletId, backendId);
    }

    /**
     * Replicas of colocate table's tablet does not locate on right backends set.
     * backends set:       1,2,3
     * tablet replicas:    1,2,5
     * <p>
     * backends set:       1,2,3
     * tablet replicas:    1,2
     * <p>
     * backends set:       1,2,3
     * tablet replicas:    1,2,4,5
     */
    private void handleColocateMismatch(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        Preconditions.checkNotNull(tabletCtx.getColocateBackendsSet());

        stat.counterReplicaColocateMismatch.incrementAndGet();
        // find an available dest backend and path
        RootPathLoadStatistic destPath = chooseAvailableDestPath(tabletCtx, true /* for colocate */);
        Preconditions.checkNotNull(destPath);
        tabletCtx.setDest(destPath.getBeId(), destPath.getPathHash());

        // choose a source replica for cloning from
        tabletCtx.chooseSrcReplica(backendsWorkingSlots);

        // create clone task
        batchTask.addTask(tabletCtx.createCloneReplicaAndTask());
    }

    /**
     * Try to select some alternative tablets for balance. Add them to pendingTablets with priority LOW,
     * and waiting to be scheduled.
     */
    private void selectTabletsForBalance() {
        if (Config.tablet_sched_disable_balance) {
            LOG.info("balance is disabled. skip selecting tablets for balance");
            return;
        }

        List<TabletSchedCtx> alternativeTablets = rebalancer.selectAlternativeTablets();
        for (TabletSchedCtx tabletCtx : alternativeTablets) {
            addTablet(tabletCtx, false);
        }
    }

    /**
     * Try to create a balance task for a tablet.
     */
    private void doBalance(TabletSchedCtx tabletCtx, AgentBatchTask batchTask) throws SchedException {
        stat.counterBalanceSchedule.incrementAndGet();
        rebalancer.createBalanceTask(tabletCtx, backendsWorkingSlots, batchTask);
    }

    private RootPathLoadStatistic findPath(TabletSchedCtx tabletCtx,
                                           List<RootPathLoadStatistic> allFitPaths,
                                           boolean matchMedium) throws SchedException {
        for (RootPathLoadStatistic rootPathLoadStatistic : allFitPaths) {
            if (matchMedium && rootPathLoadStatistic.getStorageMedium() != tabletCtx.getStorageMedium()) {
                continue;
            }

            PathSlot slot = backendsWorkingSlots.get(rootPathLoadStatistic.getBeId());
            if (slot == null) {
                LOG.debug("backend {} does not found when getting slots", rootPathLoadStatistic.getBeId());
                continue;
            }

            if (slot.takeSlot(rootPathLoadStatistic.getPathHash()) != -1) {
                return rootPathLoadStatistic;
            }
        }

        return null;
    }

    private RootPathLoadStatistic findAvailablePathWithMediumMatch(TabletSchedCtx tabletCtx,
                                                                   List<RootPathLoadStatistic> allFitPaths)
            throws SchedException {
        return findPath(tabletCtx, allFitPaths, true /* match medium */);
    }

    private RootPathLoadStatistic findAvailablePathArbitrary(TabletSchedCtx tabletCtx,
                                                             List<RootPathLoadStatistic> allFitPaths)
            throws SchedException {
        return findPath(tabletCtx, allFitPaths, false /* match medium */);
    }

    private RootPathLoadStatistic findAvailablePath(TabletSchedCtx tabletCtx,
                                                    List<RootPathLoadStatistic> allFitPaths) throws SchedException {
        RootPathLoadStatistic path = findAvailablePathWithMediumMatch(tabletCtx, allFitPaths);
        if (path == null) {
            path = findAvailablePathArbitrary(tabletCtx, allFitPaths);
        }

        return path;
    }

    // choose a path on a backend which is fit for the tablet
    private RootPathLoadStatistic chooseAvailableDestPath(TabletSchedCtx tabletCtx, boolean forColocate)
            throws SchedException {
        ClusterLoadStatistic statistic = loadStatistic;
        if (statistic == null) {
            throw new SchedException(Status.UNRECOVERABLE, "cluster does not exist");
        }
        List<BackendLoadStatistic> beStatistics = statistic.getSortedBeLoadStats(null /* sorted ignore medium */);

        // get all available paths which this tablet can fit in.
        // beStatistics is sorted by mix load score in ascend order, so select from first to last.
        List<RootPathLoadStatistic> allFitPaths = Lists.newArrayList();
        for (BackendLoadStatistic bes : beStatistics) {
            if (!bes.isAvailable()) {
                continue;
            }
            // exclude host which already has replica of this tablet
            if (tabletCtx.containsBE(bes.getBeId(), forColocate)) {
                continue;
            }

            if (forColocate && !tabletCtx.getColocateBackendsSet().contains(bes.getBeId())) {
                continue;
            }

            List<RootPathLoadStatistic> resultPaths = Lists.newArrayList();
            // in the following two cases, it's not a replica supplement task
            //   1. tabletCtx.getTabletStatus() == TabletStatus.REPLICA_RELOCATING, i.e. the source backend is
            //      decommissioned manually.
            //   2. it's a COLOCATE_MISMATCH task, but the task is created not because of unavailable backends,
            //      but because of balancing needs.
            boolean isSupplement = !(tabletCtx.getTabletStatus() == TabletStatus.REPLICA_RELOCATING ||
                    (tabletCtx.getTabletStatus() == TabletStatus.COLOCATE_MISMATCH &&
                            !tabletCtx.getRelocationForRepair()));
            BalanceStatus st = bes.isFit(tabletCtx.getTabletSize(), tabletCtx.getStorageMedium(),
                    resultPaths, isSupplement);
            if (!st.ok()) {
                LOG.debug("unable to find path for supplementing tablet: {}. {}", tabletCtx, st);
                continue;
            }

            Preconditions.checkState(resultPaths.size() == 1);
            allFitPaths.add(resultPaths.get(0));
        }

        if (allFitPaths.isEmpty()) {
            throw new SchedException(Status.UNRECOVERABLE, "unable to find dest path for new replica");
        }

        // all fit paths has already been sorted by load score in 'allFitPaths' in ascend order.
        // just get first available path.
        // we try to find a path with specified medium type, if not find, arbitrarily select one.
        RootPathLoadStatistic path = findAvailablePath(tabletCtx, allFitPaths);
        if (path != null) {
            return path;
        } else {
            throw new SchedException(Status.SCHEDULE_RETRY, "path busy, wait for next round");
        }
    }

    /**
     * For some reason, a tablet info failed to be scheduled this time,
     * Add back to queue, waiting for next round.
     */
    private synchronized void addBackToPendingTablets(TabletSchedCtx tabletCtx) {
        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.PENDING);
        // Since we know it's add back, corresponding tablet id is still recorded in `allTabletIds`,
        // so we explicitly remove the id from `allTabletIds`, otherwise `addTablet()` may fail.
        // And when adding back, we don't want it to be failed because of exceeding limit of
        // `Config.max_scheduling_tablets` since it's already got scheduled before, we just adjusted
        // its priority and want it to be scheduled again, so we set force to be true here.
        allTabletIds.remove(tabletCtx.getTabletId());
        addTablet(tabletCtx, true /* force */);
    }

    private void finalizeTabletCtx(TabletSchedCtx tabletCtx, TabletSchedCtx.State state, String reason) {
        // use 2 steps to avoid nested database lock and synchronized.(releaseTabletCtx() may hold db lock)
        // remove the tablet ctx, so that no other process can see it
        removeTabletCtx(tabletCtx, reason);
        // release resources taken by tablet ctx
        releaseTabletCtx(tabletCtx, state);
    }

    private void releaseTabletCtx(TabletSchedCtx tabletCtx, TabletSchedCtx.State state) {
        tabletCtx.setState(state);
        tabletCtx.resetDecommissionedReplicaState();
        tabletCtx.releaseResource(this);
        tabletCtx.setFinishedTime(System.currentTimeMillis());
    }

    private synchronized void removeTabletCtx(TabletSchedCtx tabletCtx, String reason) {
        runningTablets.remove(tabletCtx.getTabletId());
        allTabletIds.remove(tabletCtx.getTabletId());
        schedHistory.add(tabletCtx);
        LOG.info("remove the tablet {}. because: {}", tabletCtx.getTabletId(), reason);
    }

    @VisibleForTesting
    public void removeOneFromPendingQ() {
        pendingTablets.poll();
    }

    // get next batch of tablets from queue.
    private synchronized List<TabletSchedCtx> getNextTabletCtxBatch() {
        List<TabletSchedCtx> list = Lists.newArrayList();
        int count = pendingTablets.size();
        while (count > 0) {
            TabletSchedCtx tablet = pendingTablets.poll();
            if (tablet == null) {
                // no more tablets
                break;
            }
            // ignore tablets that will expire and erase soon
            if (checkIfTabletExpired(tablet)) {
                continue;
            }
            list.add(tablet);
            count--;
        }
        return list;
    }

    /**
     * return true if we want to remove the clone task from AgentTaskQueue
     */
    public boolean finishCloneTask(CloneTask cloneTask, TFinishTaskRequest request) {
        long tabletId = cloneTask.getTabletId();
        TabletSchedCtx tabletCtx = takeRunningTablets(tabletId);
        if (tabletCtx == null) {
            LOG.warn("tablet info does not exist, tablet:{} backend:{}", tabletId, cloneTask.getBackendId());
            // tablet does not exist, no need to keep task.
            return true;
        }

        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.RUNNING, tabletCtx.getState());
        try {
            tabletCtx.finishCloneTask(cloneTask, request);
        } catch (SchedException e) {
            tabletCtx.increaseFailedRunningCounter();
            tabletCtx.setErrMsg(e.getMessage());
            if (e.getStatus() == Status.UNRECOVERABLE) {
                // unrecoverable
                stat.counterTabletScheduledDiscard.incrementAndGet();
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getMessage());
                return true;
            } else if (e.getStatus() == Status.FINISHED) {
                // tablet is already healthy, just remove
                finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.CANCELLED, e.getMessage());
                return true;
            }
        } catch (Exception e) {
            LOG.warn("got unexpected exception when finish clone task. tablet: {}",
                    tabletCtx.getTabletId(), e);
            stat.counterTabletScheduledDiscard.incrementAndGet();
            finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.UNEXPECTED, e.getMessage());
            return true;
        }

        Preconditions.checkState(tabletCtx.getState() == TabletSchedCtx.State.FINISHED);
        stat.counterCloneTaskSucceeded.incrementAndGet();
        gatherStatistics(tabletCtx);
        ColocateTableBalancer.getInstance().increaseScheduledTabletNumForBucket(tabletCtx);
        finalizeTabletCtx(tabletCtx, TabletSchedCtx.State.FINISHED, "finished");
        return true;
    }

    /**
     * Gather the running statistic of the task.
     * It will be evaluated for future strategy.
     * This should only be called when the tablet is down with state FINISHED.
     */
    private void gatherStatistics(TabletSchedCtx tabletCtx) {
        if (tabletCtx.getCopySize() > 0 && tabletCtx.getCopyTimeMs() > 0) {
            if (tabletCtx.getSrcBackendId() != -1 && tabletCtx.getSrcPathHash() != -1) {
                PathSlot pathSlot = backendsWorkingSlots.get(tabletCtx.getSrcBackendId());
                if (pathSlot != null) {
                    pathSlot.updateStatistic(tabletCtx.getSrcPathHash(), tabletCtx.getCopySize(),
                            tabletCtx.getCopyTimeMs());
                }
            }

            if (tabletCtx.getDestBackendId() != -1 && tabletCtx.getDestPathHash() != -1) {
                PathSlot pathSlot = backendsWorkingSlots.get(tabletCtx.getDestBackendId());
                if (pathSlot != null) {
                    pathSlot.updateStatistic(tabletCtx.getDestPathHash(), tabletCtx.getCopySize(),
                            tabletCtx.getCopyTimeMs());
                }
            }
        }

        if (System.currentTimeMillis() - lastSlotAdjustTime < STAT_UPDATE_INTERVAL_MS) {
            return;
        }

        // TODO(cmy): update the slot num base on statistic.
        // need to find a better way to determine the slot number.

        lastSlotAdjustTime = System.currentTimeMillis();
    }

    /**
     * handle tablets which are running.
     * We should finish the task if
     * 1. Tablet is already healthy
     * 2. Task is timeout.
     * <p>
     * But here we just handle the timeout case here. Let the 'finishCloneTask()' check if tablet is healthy.
     * We guarantee that if tablet is in runningTablets, the 'finishCloneTask()' will finally be called,
     * so no need to worry that running tablets will never end.
     * This is also avoid nesting 'synchronized' and database lock.
     * <p>
     * If task is timeout, remove the tablet.
     */
    public void handleRunningTablets() {
        // 1. remove the tablet ctx if timeout
        List<TabletSchedCtx> timeoutTablets = Lists.newArrayList();
        synchronized (this) {
            runningTablets.values().stream().filter(TabletSchedCtx::isTimeout).forEach(timeoutTablets::add);

            for (TabletSchedCtx tabletSchedCtx : timeoutTablets) {
                removeTabletCtx(tabletSchedCtx, "timeout");
            }
        }

        // 2. release ctx
        timeoutTablets.forEach(t -> {
            releaseTabletCtx(t, TabletSchedCtx.State.CANCELLED);
            stat.counterCloneTaskTimeout.incrementAndGet();
        });
    }

    public void handleForceCleanSchedQ() {
        if (forceCleanSchedQ.get()) {
            // trigger only once
            forceCleanSchedQ.set(false);
            List<TabletSchedCtx> cleanedTablets = Lists.newArrayList();
            synchronized (this) {
                LOG.info("forcefully clean all the tablets from pending and running queue for tablet scheduler," +
                        " pending queue size {}, running queue size {}", pendingTablets.size(), runningTablets.size());
                cleanedTablets.addAll(pendingTablets);
                cleanedTablets.addAll(runningTablets.values());
                pendingTablets.clear();
                runningTablets.clear();
                allTabletIds.clear();
            }

            cleanedTablets.forEach(t -> releaseTabletCtx(t, TabletSchedCtx.State.CANCELLED));
        }
    }

    public List<List<String>> getPendingTabletsInfo(int limit) {
        List<TabletSchedCtx> tabletCtxs = getCopiedTablets(pendingTablets, limit);
        return collectTabletCtx(tabletCtxs);
    }

    public List<List<String>> getRunningTabletsInfo(int limit) {
        List<TabletSchedCtx> tabletCtxs;
        synchronized (this) {
            tabletCtxs = getCopiedTablets(runningTablets.values(), limit);
        }
        return collectTabletCtx(tabletCtxs);
    }

    public List<List<String>> getHistoryTabletsInfo(int limit) {
        List<TabletSchedCtx> tabletCtxs = getCopiedTablets(schedHistory, limit);
        return collectTabletCtx(tabletCtxs);
    }

    private List<List<String>> collectTabletCtx(List<TabletSchedCtx> tabletCtxs) {
        List<List<String>> result = Lists.newArrayList();
        tabletCtxs.forEach(t -> result.add(t.getBrief()));
        return result;
    }

    private synchronized List<TabletSchedCtx> getCopiedTablets(Collection<TabletSchedCtx> source, int limit) {
        List<TabletSchedCtx> tabletCtxs = Lists.newArrayList();
        source.stream().limit(limit).forEach(tabletCtxs::add);
        return tabletCtxs;
    }

    public synchronized int getPendingNum() {
        return pendingTablets.size();
    }

    public synchronized int getRunningNum() {
        return runningTablets.size();
    }

    public synchronized int getHistoryNum() {
        return schedHistory.size();
    }

    public synchronized int getTotalNum() {
        return allTabletIds.size();
    }

    public synchronized long getBalanceTabletsNumber() {
        return pendingTablets.stream().filter(t -> t.getType() == Type.BALANCE).count()
                + runningTablets.values().stream().filter(t -> t.getType() == Type.BALANCE).count();
    }

    public TGetTabletScheduleResponse getTabletSchedule(TGetTabletScheduleRequest request) {
        long tableId = request.isSetTable_id() ? request.table_id : -1;
        long partitionId = request.isSetPartition_id() ? request.partition_id : -1;
        long tabletId = request.isSetTablet_id() ? request.tablet_id : -1;
        String type = request.isSetType() ? request.type : null;
        String state = request.isSetState() ? request.state : null;
        long limit = request.isSetLimit() ? request.limit : -1;
        List<TabletSchedCtx> tabletCtxs;
        synchronized (this) {
            Stream<TabletSchedCtx> all;
            if (TabletSchedCtx.State.PENDING.name().equals(state)) {
                all = pendingTablets.stream();
            } else if (TabletSchedCtx.State.RUNNING.name().equals(state)) {
                all = runningTablets.values().stream();
            } else if (TabletSchedCtx.State.FINISHED.name().equals(state)) {
                all = schedHistory.stream();
            } else {
                // running first, then history, then pending
                all = Stream.concat(Stream.concat(runningTablets.values().stream(), schedHistory.stream()),
                        pendingTablets.stream());
                if (state != null) {
                    all = all.filter(t -> t.getState().name().equals(state));
                }
            }
            if (type != null) {
                all = all.filter(t -> t.getType().name().equals(type));
            }
            if (tabletId != -1) {
                all = all.filter(t -> t.getTabletId() == tabletId);
            } else if (partitionId != -1) {
                all = all.filter(t -> t.getPartitionId() == partitionId);
            } else if (tableId != -1) {
                all = all.filter(t -> t.getTblId() == tableId);
            }
            if (limit > 0) {
                all = all.limit((int) limit);
            }
            tabletCtxs = all.collect(Collectors.toList());
        }
        TGetTabletScheduleResponse response = new TGetTabletScheduleResponse();
        response.setTablet_schedules(
                tabletCtxs.stream().map(TabletSchedCtx::toTabletScheduleThrift).collect(Collectors.toList()));
        return response;
    }

    // caller should hold db lock
    private void checkMetaExist(TabletSchedCtx ctx) throws SchedException {
        Database db = GlobalStateMgr.getCurrentState().getDbIncludeRecycleBin(ctx.getDbId());
        if (db == null) {
            throw new SchedException(Status.UNRECOVERABLE, "db " + ctx.getDbId() + " dose not exist");
        }

        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getTableIncludeRecycleBin(db, ctx.getTblId());
        if (tbl == null) {
            throw new SchedException(Status.UNRECOVERABLE, "table " + ctx.getTblId() + " dose not exist");
        }

        Partition partition = GlobalStateMgr.getCurrentState().getPartitionIncludeRecycleBin(tbl, ctx.getPartitionId());
        if (partition == null) {
            throw new SchedException(Status.UNRECOVERABLE, "partition " + ctx.getPartitionId() + " dose not exist");
        }

        MaterializedIndex idx = partition.getIndex(ctx.getIndexId());
        if (idx == null) {
            throw new SchedException(Status.UNRECOVERABLE, "materialized index " + ctx.getIndexId() + " dose not exist");
        }

        Tablet tablet = idx.getTablet(ctx.getTabletId());
        if (tablet == null) {
            throw new SchedException(Status.UNRECOVERABLE, "tablet " + ctx.getTabletId() + " dose not exist");
        }
    }

    /**
     * PathSlot keeps track of slot num per path of a Backend.
     * Each path on a Backend has several slot.
     * If a path's available slot num become 0, no task should be assigned to this path.
     */
    public static class PathSlot {
        // path hash -> slot num
        private final Map<Long, Slot> pathSlots = Maps.newConcurrentMap();

        public PathSlot(List<Long> paths, int initSlotNum) {
            for (Long pathHash : paths) {
                pathSlots.put(pathHash, new Slot(initSlotNum));
            }
        }

        // update the path
        public synchronized void updatePaths(List<Long> paths, int currentSlotPerPathConfig) {
            // delete non exist path
            pathSlots.entrySet().removeIf(entry -> !paths.contains(entry.getKey()));

            // add new path
            for (Long pathHash : paths) {
                if (!pathSlots.containsKey(pathHash)) {
                    pathSlots.put(pathHash, new Slot(currentSlotPerPathConfig));
                }
            }
        }

        // Update the total slots num of every storage path on a specified BE based on new configuration.
        public synchronized void updateSlot(int delta) {
            for (Long pathHash : pathSlots.keySet()) {
                Slot slot = pathSlots.get(pathHash);
                if (slot == null) {
                    continue;
                }

                slot.total += delta;
                slot.available += delta;
                slot.rectify();
                LOG.debug("Update path {} slots num to {}", pathHash, slot.total);
            }
        }

        /**
         * Update the statistic of specified path
         */
        public synchronized void updateStatistic(long pathHash, long copySize, long copyTimeMs) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            slot.totalCopySize += copySize;
            slot.totalCopyTimeMs += copyTimeMs;
        }

        /**
         * If the specified 'pathHash' has available slot, decrease the slot number and return this path hash
         */
        public synchronized long takeSlot(long pathHash) throws SchedException {
            if (pathHash == -1) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("path hash is not set. current stack trace: {}", LogUtil.getCurrentStackTrace());
                }
                throw new SchedException(Status.UNRECOVERABLE, "path hash is not set");
            }

            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return -1;
            }
            slot.rectify();
            if (slot.available <= 0) {
                return -1;
            }
            slot.available--;
            return pathHash;
        }

        public synchronized void freeSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return;
            }
            slot.available++;
            slot.rectify();
        }

        public synchronized int peekSlot(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return -1;
            }
            slot.rectify();
            return slot.available;
        }

        public synchronized int getSlotTotal(long pathHash) {
            Slot slot = pathSlots.get(pathHash);
            if (slot == null) {
                return -1;
            }
            slot.rectify();
            return slot.total;
        }

        public synchronized List<List<String>> getSlotInfo(long beId) {
            List<List<String>> results = Lists.newArrayList();
            pathSlots.forEach((key, value) -> {
                value.rectify();
                List<String> result = Lists.newArrayList();
                result.add(String.valueOf(beId));
                result.add(String.valueOf(key));
                result.add(String.valueOf(value.available));
                result.add(String.valueOf(value.total));
                result.add(String.valueOf(value.getAvgRate()));
                results.add(result);
            });
            return results;
        }
    }

    public List<List<String>> getSlotsInfo() {
        List<List<String>> result = Lists.newArrayList();
        for (long beId : backendsWorkingSlots.keySet()) {
            PathSlot slot = backendsWorkingSlots.get(beId);
            result.addAll(slot.getSlotInfo(beId));
        }
        return result;
    }

    public static class Slot {
        public int total;
        public int available;

        public long totalCopySize = 0;
        public long totalCopyTimeMs = 0;

        public Slot(int total) {
            this.total = total;
            this.available = total;
        }

        public void rectify() {
            if (total <= 0) {
                total = 1;
            }
            if (available > total) {
                available = total;
            }
        }

        // return avg rate, Bytes/S
        public double getAvgRate() {
            if (totalCopyTimeMs / 1000 == 0) {
                return 0.0;
            }
            return totalCopySize / ((double) totalCopyTimeMs / 1000);
        }
    }
}
