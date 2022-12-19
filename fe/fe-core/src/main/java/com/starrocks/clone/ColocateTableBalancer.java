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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/ColocateTableBalancer.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.ColocateTableIndex.GroupId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.LocalTablet.TabletStatus;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.clone.TabletSchedCtx.Priority;
import com.starrocks.clone.TabletScheduler.AddResult;
import com.starrocks.common.Config;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * ColocateTableBalancer is responsible for tablets' repair and balance of colocated tables.
 */
public class ColocateTableBalancer extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(ColocateTableBalancer.class);

    private static final long CHECK_INTERVAL_MS = 20 * 1000L; // 20 second

    private ColocateTableBalancer(long intervalMs) {
        super("colocate group clone checker", intervalMs);
    }

    private static ColocateTableBalancer INSTANCE = null;

    /**
     * Only for unit test purpose.
     */
    public static boolean disableRepairPrecedence = false;
    public static boolean ignoreSingleReplicaCheck = false;

    public static ColocateTableBalancer getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ColocateTableBalancer(CHECK_INTERVAL_MS);
        }
        return INSTANCE;
    }

    private static class ColocateRelocationInfo {
        /**
         * indicate that the change of bucket sequence for a group is caused by
         * repair(true), i.e. there is unavailable backend, or balance(false).
         */
        private boolean relocationForRepair;

        /**
         * record the value of bucket sequence before changed, used to reset current bucket sequence
         * when condition is matched, e.g. the backend that caused colocate relocation before has come alive.
         */
        private List<List<Long>> lastBackendsPerBucketSeq;

        /**
         * Count the number of tablets which have been successfully scheduled by TabletScheduler after a relocation
         * decision has been made.
         * <p>
         * bucket index -> number of finished scheduling tablets
         */
        private Map<Integer, Integer> scheduledTabletNumPerBucket;

        public ColocateRelocationInfo(boolean relocationForRepair,
                                      List<List<Long>> lastBackendsPerBucketSeq,
                                      Map<Integer, Integer> scheduledTabletNumPerBucket) {
            this.relocationForRepair = relocationForRepair;
            this.lastBackendsPerBucketSeq = lastBackendsPerBucketSeq;
            this.scheduledTabletNumPerBucket = scheduledTabletNumPerBucket;
        }

        public boolean getRelocationForRepair() {
            return relocationForRepair;
        }

        public List<List<Long>> getLastBackendsPerBucketSeq() {
            return lastBackendsPerBucketSeq;
        }

        public synchronized Map<Integer, Integer> getScheduledTabletNumPerBucket() {
            return scheduledTabletNumPerBucket;
        }

        public Integer getScheduledTabletNumForBucket(int tabletOrderIdx) {
            synchronized (scheduledTabletNumPerBucket) {
                return scheduledTabletNumPerBucket.get(tabletOrderIdx);
            }
        }

        public void increaseScheduledTabletNumForBucket(int tabletOrderIdx) {
            synchronized (scheduledTabletNumPerBucket) {
                scheduledTabletNumPerBucket.merge(tabletOrderIdx, 1, Integer::sum);
            }
        }
    }

    /**
     * ColocateRelocationInfo per group.
     */
    private Map<GroupId, ColocateRelocationInfo> group2ColocateRelocationInfo = Maps.newConcurrentMap();

    @Override
    /*
     * Each round, we do 2 steps:
     * 1. Relocate and balance group:
     *      Backend is not available, find a new backend to replace it.
     *      and after all unavailable has been replaced, balance the group
     *
     * 2. Match group:
     *      If replica mismatch backends in a group, that group will be marked as unstable, and pass that
     *      tablet to TabletScheduler.
     *      Otherwise, mark the group as stable
     */
    protected void runAfterCatalogReady() {
        relocateAndBalanceGroup();
        matchGroup();
    }

    /*
     * relocate and balance group
     *  here we just let replicas in colocate table evenly distributed in cluster, not consider the
     *  cluster load statistic.
     *  for example:
     *  currently there are 4 backends A B C D with following load:
     *
     *                +-+
     *                | |
     * +-+  +-+  +-+  | |
     * | |  | |  | |  | |
     * +-+  +-+  +-+  +-+
     *  A    B    C    D
     *
     *  And colocate group balancer will still evenly distribute the replicas to all 4 backends, not
     *  just 3 low load backends.
     *
     *                 X
     *                 X
     *  X    X    X   +-+
     *  X    X    X   | |
     * +-+  +-+  +-+  | |
     * | |  | |  | |  | |
     * +-+  +-+  +-+  +-+
     * A    B    C    D
     *
     *  So After colocate balance, the cluster may still 'unbalanced' from a global perspective.
     *  And the LoadBalancer will balance the non-colocate table's replicas to make the
     *  cluster balance, eventually.
     *
     *  X    X    X    X
     *  X    X    X    X
     * +-+  +-+  +-+  +-+
     * | |  | |  | |  | |
     * | |  | |  | |  | |
     * +-+  +-+  +-+  +-+
     *  A    B    C    D
     */
    private void relocateAndBalanceGroup() {
        if (Config.tablet_sched_disable_colocate_balance) {
            return;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ColocateTableIndex colocateIndex = globalStateMgr.getColocateTableIndex();
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();

        // get all groups
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        TabletScheduler tabletScheduler = globalStateMgr.getTabletScheduler();
        TabletSchedulerStat stat = tabletScheduler.getStat();
        Map<GroupId, Long> group2InScheduleTabletNum = tabletScheduler.getTabletsNumInScheduleForEachCG();
        for (GroupId groupId : groupIds) {
            Database db = globalStateMgr.getDbIncludeRecycleBin(groupId.dbId);
            if (db == null) {
                continue;
            }
            ClusterLoadStatistic statistic = globalStateMgr.getTabletScheduler().getLoadStatistic();
            if (statistic == null) {
                continue;
            }
            List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
            if (backendsPerBucketSeq.isEmpty()) {
                continue;
            }

            Set<Long> unavailableBeIdsInGroup = getUnavailableBeIdsInGroup(infoService, colocateIndex, groupId);
            // Won't make new relocation decision if there is still colocate relocation tasks in schedule.
            // We want the previous relocation decision is handled completely before new decision is made, so
            // this won't trigger concurrent relocation decision and may mess up the scheduling.
            Long inScheduleTabletNum = group2InScheduleTabletNum.get(groupId);
            if (inScheduleTabletNum != null && inScheduleTabletNum >= 1L && unavailableBeIdsInGroup.isEmpty()) {
                LOG.info("colocate group {} still has {} tablets in schedule, won't make new relocation decision",
                        groupId, inScheduleTabletNum);
                ColocateRelocationInfo info = group2ColocateRelocationInfo.get(groupId);
                if (info != null) {
                    LOG.info("number of finished scheduling tablets per bucket: {} for colocate group {}",
                            info.getScheduledTabletNumPerBucket(), groupId);
                }
                continue;
            }

            stat.counterColocateBalanceRound.incrementAndGet();
            List<Long> availableBeIds = getAvailableBeIds(infoService);
            List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
            if (relocateAndBalance(groupId, unavailableBeIdsInGroup, availableBeIds, colocateIndex, infoService,
                    statistic, balancedBackendsPerBucketSeq)) {
                group2ColocateRelocationInfo.put(groupId,
                        new ColocateRelocationInfo(!unavailableBeIdsInGroup.isEmpty(),
                                colocateIndex.getBackendsPerBucketSeq(groupId), Maps.newHashMap()));
                colocateIndex.addBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeq);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeq);
                globalStateMgr.getEditLog().logColocateBackendsPerBucketSeq(info);
                LOG.info("balance colocate group {}. now backends per bucket sequence is: {}, " +
                                "bucket sequence before balance: {}", groupId, balancedBackendsPerBucketSeq,
                        group2ColocateRelocationInfo.get(groupId).getLastBackendsPerBucketSeq());
            } else {
                // clean historical relocation info if nothing changed after trying to do `relocateAndBalance()`
                group2ColocateRelocationInfo.remove(groupId);
            }
        }
        cleanRelocationInfoMap(groupIds);
    }

    /*
     * Check every tablet of a group, if replica's location does not match backends in group, relocating those
     * replicas, and mark that group as unstable.
     * If every replicas match the backends in group, mark that group as stable.
     */
    private void matchGroup() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ColocateTableIndex colocateIndex = globalStateMgr.getColocateTableIndex();
        TabletScheduler tabletScheduler = globalStateMgr.getTabletScheduler();
        long checkStartTime = System.currentTimeMillis();

        long start = System.nanoTime();
        long lockTotalTime = 0;
        long lockStart;
        // check each group
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        GROUP:
        for (GroupId groupId : groupIds) {
            List<Long> tableIds = colocateIndex.getAllTableIds(groupId);
            Database db = globalStateMgr.getDbIncludeRecycleBin(groupId.dbId);
            if (db == null) {
                continue;
            }

            List<Set<Long>> backendBucketsSeq = colocateIndex.getBackendsPerBucketSeqSet(groupId);
            if (backendBucketsSeq.isEmpty()) {
                continue;
            }

            boolean isGroupStable = true;
            // set the config to a local variable to avoid config params changed.
            int partitionBatchNum = Config.tablet_checker_partition_batch_num;
            int partitionChecked = 0;
            db.readLock();
            lockStart = System.nanoTime();
            try {
                TABLE:
                for (Long tableId : tableIds) {
                    OlapTable olapTable = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tableId);
                    if (olapTable == null || !colocateIndex.isColocateTable(olapTable.getId())) {
                        continue;
                    }

                    for (Partition partition : globalStateMgr.getPartitionsIncludeRecycleBin(olapTable)) {
                        partitionChecked++;
                        if (partitionChecked % partitionBatchNum == 0) {
                            lockTotalTime += System.nanoTime() - lockStart;
                            // release lock, so that lock can be acquired by other threads.
                            LOG.debug("partition checked reached batch value, release lock");
                            db.readUnlock();
                            db.readLock();
                            LOG.debug("balancer get lock again");
                            lockStart = System.nanoTime();
                            if (globalStateMgr.getDbIncludeRecycleBin(groupId.dbId) == null) {
                                continue GROUP;
                            }
                            if (globalStateMgr.getTableIncludeRecycleBin(db, olapTable.getId()) == null) {
                                continue TABLE;
                            }
                            if (globalStateMgr.getPartitionIncludeRecycleBin(olapTable, partition.getId()) == null) {
                                continue;
                            }
                        }
                        short replicationNum =
                                globalStateMgr.getReplicationNumIncludeRecycleBin(olapTable.getPartitionInfo(),
                                        partition.getId());
                        if (replicationNum == (short) -1) {
                            continue;
                        }
                        long visibleVersion = partition.getVisibleVersion();
                        // Here we only get VISIBLE indexes. All other indexes are not queryable.
                        // So it does not matter if tablets of other indexes are not matched.
                        for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                            Preconditions.checkState(backendBucketsSeq.size() == index.getTablets().size(),
                                    backendBucketsSeq.size() + " vs. " + index.getTablets().size());
                            int idx = 0;
                            for (Long tabletId : index.getTabletIdsInOrder()) {
                                LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
                                Set<Long> bucketsSeq = backendBucketsSeq.get(idx);
                                // Tablet has already been scheduled, no need to schedule again
                                if (!tabletScheduler.containsTablet(tablet.getId())) {
                                    Preconditions.checkState(bucketsSeq.size() == replicationNum,
                                            bucketsSeq.size() + " vs. " + replicationNum);
                                    TabletStatus st = tablet.getColocateHealthStatus(visibleVersion,
                                            replicationNum, bucketsSeq);
                                    if (st != TabletStatus.HEALTHY) {
                                        isGroupStable = false;
                                        Priority colocateUnhealthyPrio = Priority.HIGH;
                                        // We should also check if the tablet is ready to be repaired like
                                        // `TabletChecker` did. Slightly delay the repair action can avoid unnecessary
                                        // clone in situation like temporarily restart BE Nodes.
                                        if (tablet.readyToBeRepaired(st, colocateUnhealthyPrio)) {
                                            LOG.debug("get unhealthy tablet {} in colocate table. status: {}",
                                                    tablet.getId(),
                                                    st);

                                            // If tablet has bad replica, `getColocateHealthStatus()` will also return
                                            // COLOCATE_MISMATCH, we need to replace the backend which owns the bad
                                            // replica.
                                            adjustBackendSetIfReplicaBad(idx, tablet, st, groupId,
                                                    ignoreSingleReplicaCheck);

                                            TabletSchedCtx tabletCtx = new TabletSchedCtx(
                                                    TabletSchedCtx.Type.REPAIR,
                                                    db.getId(), tableId, partition.getId(), index.getId(),
                                                    tablet.getId(),
                                                    System.currentTimeMillis());
                                            // the tablet status will be set again when being scheduled
                                            tabletCtx.setTabletStatus(st);
                                            // using HIGH priority, because we want to stabilize the colocate group
                                            // as soon as possible
                                            tabletCtx.setOrigPriority(colocateUnhealthyPrio);
                                            tabletCtx.setTabletOrderIdx(idx);
                                            tabletCtx.setColocateGroupId(groupId);
                                            tabletCtx.setTablet(tablet);
                                            ColocateRelocationInfo info = group2ColocateRelocationInfo.get(groupId);
                                            if (info != null && info.getRelocationForRepair() &&
                                                    st == TabletStatus.COLOCATE_MISMATCH) {
                                                tabletCtx.setRelocationForRepair(true);
                                            } else {
                                                tabletCtx.setRelocationForRepair(false);
                                            }

                                            AddResult res = tabletScheduler.addTablet(tabletCtx, false /* not force */);
                                            if (res == AddResult.LIMIT_EXCEED) {
                                                // tablet in scheduler exceed limit, skip this group and check next one.
                                                LOG.info("number of scheduling tablets in tablet scheduler"
                                                        + " exceed to limit. stop colocate table check");
                                                break TABLE;
                                            }
                                            if (res == AddResult.ADDED && tabletCtx.getRelocationForRepair()) {
                                                LOG.info("add tablet relocation task to scheduler, tablet id: {}, " +
                                                                "bucket sequence before: {}, bucket sequence now: {}",
                                                        tableId,
                                                        info != null ? info.getLastBackendsPerBucketSeq().get(idx) :
                                                                Lists.newArrayList(),
                                                        bucketsSeq);
                                            }
                                        }
                                    } else {
                                        tablet.setLastStatusCheckTime(checkStartTime);
                                    }
                                } else {
                                    // tablet maybe added to scheduler because of balance between local disks,
                                    // in this case we shouldn't mark the group unstable
                                    if (tablet.getColocateHealthStatus(visibleVersion, replicationNum, bucketsSeq)
                                            != TabletStatus.HEALTHY) {
                                        isGroupStable = false;
                                    }
                                }
                                idx++;
                            }
                        }
                    }
                } // end for tables

                // mark group as stable or unstable
                if (isGroupStable) {
                    colocateIndex.markGroupStable(groupId, true);
                } else {
                    colocateIndex.markGroupUnstable(groupId, true);
                }
            } finally {
                lockTotalTime += System.nanoTime() - lockStart;
                db.readUnlock();
            }
        } // end for groups

        long cost = (System.nanoTime() - start) / 1000000;
        lockTotalTime = lockTotalTime / 1000000;
        LOG.info("finished to match colocate group. cost: {} ms, in lock time: {} ms", cost, lockTotalTime);
    }

    /*
     * The balance logic is as follows:
     *
     * All backends: A,B,C,D,E,F,G,H,I,J
     *
     * One group's buckets sequence:
     *
     * Buckets sequence:    0  1  2  3
     * Backend set:         A  A  A  A
     *                      B  D  F  H
     *                      C  E  G  I
     *
     * Then each backend has different replica num:
     *
     * Backends:    A B C D E F G H I J
     * Replica num: 4 1 1 1 1 1 1 1 1 0
     *
     * The goal of balance is to evenly distribute replicas on all backends. For this example, we want the
     * following result (one possible result):
     *
     * Backends:    A B C D E F G H I J
     * Replica num: 2 2 1 1 1 1 1 1 1 1
     *
     * Algorithm:
     * 0. Generate the flat list of backends per bucket sequence:
     *      A B C A D E A F G A H I
     * 1. Sort backends order by replication num and load score for same replication num backends, descending:
     *      A B C D E F G H I J
     * 2. Check the diff of the first backend(A)'s replica num and last backend(J)'s replica num.
     *      If diff is less or equal than 1, we consider this group as balance. Jump to step 5.
     * 3. Else, Replace the first occurrence of Backend A in flat list with Backend J.
     *      J B C A D E A F G A H I
     * 4. Recalculate the replica num of each backend and go to step 1.
     * 5. We should get the following flat list(one possible result):
     *      J B C J D E A F G A H I
     *    Partition this flat list by replication num:
     *      [J B C] [J D E] [A F G] [A H I]
     *    And this is our new balanced backends per bucket sequence.
     *
     *  relocate is similar to balance, but choosing unavailable be as src, and move all bucketIds on unavailable be to
     *  low be
     *
     *  Return true if backends per bucket sequence change and new sequence is saved in balancedBackendsPerBucketSeq.
     *  Return false if nothing changed.
     */
    private boolean relocateAndBalance(GroupId groupId, Set<Long> unavailableBeIds, List<Long> availableBeIds,
                                       ColocateTableIndex colocateIndex, SystemInfoService infoService,
                                       ClusterLoadStatistic statistic, List<List<Long>> balancedBackendsPerBucketSeq) {
        ColocateGroupSchema groupSchema = colocateIndex.getGroupSchema(groupId);
        int replicationNum = groupSchema.getReplicationNum();
        List<List<Long>> backendsPerBucketSeq = Lists.newArrayList(colocateIndex.getBackendsPerBucketSeq(groupId));
        // [[A,B,C],[B,C,D]] -> [A,B,C,B,C,D]
        List<Long> flatBackendsPerBucketSeq =
                backendsPerBucketSeq.stream().flatMap(List::stream).collect(Collectors.toList());

        boolean isChanged = false;
        OUT:
        while (true) {
            // update backends and hosts at each round
            backendsPerBucketSeq = Lists.partition(flatBackendsPerBucketSeq, replicationNum);
            List<List<String>> hostsPerBucketSeq = getHostsPerBucketSeq(backendsPerBucketSeq, infoService);
            Preconditions.checkState(hostsPerBucketSeq != null &&
                    backendsPerBucketSeq.size() == hostsPerBucketSeq.size());

            long srcBeId = -1;
            List<Integer> srcBeSeqIndexes = null;
            boolean hasUnavailableBe = false;
            // first choose the unavailable be as src be
            for (Long beId : unavailableBeIds) {
                srcBeSeqIndexes = getBeSeqIndexes(flatBackendsPerBucketSeq, beId);
                if (srcBeSeqIndexes.size() > 0) {
                    srcBeId = beId;
                    hasUnavailableBe = true;
                    break;
                }
            }

            if (!disableRepairPrecedence && !hasUnavailableBe && !unavailableBeIds.isEmpty() && isChanged) {
                // repair task should take precedence over balance task.
                // If there are unavailable backends, and we have made relocation decision to drain all tablets from
                // those backends, we will stop here and won't do any further balance work.
                break;
            }

            // Sort backends with replica num in desc order, the list contains only all the *available* backends.
            List<Map.Entry<Long, Long>> backendWithReplicaNum =
                    getSortedBackendReplicaNumPairs(availableBeIds, unavailableBeIds, statistic,
                            flatBackendsPerBucketSeq);
            Set<Long> decommissionedBackends = getDecommissionedBackendsInGroup(infoService, colocateIndex, groupId);
            if (backendWithReplicaNum.isEmpty() ||
                    (backendWithReplicaNum.size() == 1 && decommissionedBackends.isEmpty())) {
                // There is not enough replicas for us to do relocation or balance, because in this case we
                // can not choose a valid backend to migrate replica to, end the outer loop.
                break;
            }

            int leftBound;
            if (!hasUnavailableBe || (replicationNum == 1 && decommissionedBackends.isEmpty())) {
                // There are two cases:
                // 1. there is no unavailable bucketId to relocate
                // 2. there is unavailable(only dead) bucketId to relocate, but the number of replica is one, we can do
                //    nothing but wait for the dead BEs to come alive ( for decommissioned backends, they are alive,
                //    and we intend to migrate replicas from them, so in this case we
                //    should do relocation ), in this case we shouldn't change the bucket
                //    sequence which will cause a lot of wasted colocate balancing work and further more make the
                //    colocate group unstable for longer time, but we can still do the balance work.
                //
                // In both cases, we change the src be to which that have the most replicas.
                srcBeId = backendWithReplicaNum.get(0).getKey();
                srcBeSeqIndexes = getBeSeqIndexes(flatBackendsPerBucketSeq, srcBeId);
                leftBound = 0;
            } else {
                leftBound = -1;
            }

            int j = backendWithReplicaNum.size() - 1;
            boolean isThisRoundChanged = false;
            INNER:
            while (j > leftBound) {
                // we try to use a low backend to replace the src backend.
                // if replace failed(eg: both backends are on same host), select next low backend and try(j--)
                Map.Entry<Long, Long> lowBackend = backendWithReplicaNum.get(j);
                // leftBound == 0 indicates that we don't need to consider the unavailable backends and only do
                // balance work.
                if (leftBound == 0 && (srcBeSeqIndexes.size() - lowBackend.getValue()) <= 1) {
                    // balanced
                    break OUT;
                }

                long destBeId = lowBackend.getKey();
                Backend destBe = infoService.getBackend(destBeId);
                if (destBe == null) {
                    LOG.info("backend {} does not exist", destBeId);
                    return false;
                }

                for (int seqIndex : srcBeSeqIndexes) {
                    // the bucket index.
                    // eg: 0 / 3 = 0, so that the bucket index of the 4th backend id in flatBackendsPerBucketSeq is 0.
                    int bucketIndex = seqIndex / replicationNum;
                    List<Long> backendsSet = backendsPerBucketSeq.get(bucketIndex);
                    List<String> hostsSet = hostsPerBucketSeq.get(bucketIndex);
                    // the replicas of a tablet can not locate in same Backend or same host
                    if (!backendsSet.contains(destBeId) && !hostsSet.contains(destBe.getHost())) {
                        Preconditions.checkState(backendsSet.contains(srcBeId), srcBeId);
                        flatBackendsPerBucketSeq.set(seqIndex, destBeId);
                        LOG.info("replace backend {} with backend {} in colocate group {}, src be seq index: {}" +
                                        ", bucket index: {}, original backend list: {}",
                                srcBeId, destBeId, groupId, seqIndex, bucketIndex, backendsSet);
                        // just replace one backend at a time, src and dest BE id should be recalculated because
                        // flatBackendsPerBucketSeq is changed.
                        isChanged = true;
                        isThisRoundChanged = true;
                        break INNER;
                    }
                }

                LOG.info("unable to replace backend {} with backend {} in colocate group {}",
                        srcBeId, destBeId, groupId);
                j--;
            } // end inner loop

            if (!isThisRoundChanged) {
                // if all backends are checked but this round is not changed,
                // we should end the outer loop to avoid endless loops
                LOG.info("all backends are checked but this round is not changed, " +
                        "end outer loop in colocate group {}", groupId);
                break;
            }
        }

        if (isChanged) {
            balancedBackendsPerBucketSeq.addAll(Lists.partition(flatBackendsPerBucketSeq, replicationNum));
        }
        return isChanged;
    }

    // change the backend id to backend host
    // skip the backend that does not exist
    // NOTE: the result list should not be null and list size should be same with backendsPerBucketSeq.
    private List<List<String>> getHostsPerBucketSeq(List<List<Long>> backendsPerBucketSeq,
                                                    SystemInfoService infoService) {
        List<List<String>> hostsPerBucketSeq = Lists.newArrayList();
        for (List<Long> backendIds : backendsPerBucketSeq) {
            List<String> hosts = Lists.newArrayList();
            for (Long beId : backendIds) {
                Backend be = infoService.getBackend(beId);
                if (be == null) {
                    // just skip
                    LOG.info("backend {} does not exist", beId);
                    continue;
                }
                hosts.add(be.getHost());
            }
            hostsPerBucketSeq.add(hosts);
        }
        return hostsPerBucketSeq;
    }

    private List<Map.Entry<Long, Long>> getSortedBackendReplicaNumPairs(List<Long> allAvailBackendIds,
                                                                        Set<Long> unavailBackendIds,
                                                                        ClusterLoadStatistic statistic,
                                                                        List<Long> flatBackendsPerBucketSeq) {
        // backend id -> replica num, and sorted by replica num, descending.
        Map<Long, Long> backendToReplicaNum = flatBackendsPerBucketSeq.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        // remove unavailable backend
        for (Long backendId : unavailBackendIds) {
            backendToReplicaNum.remove(backendId);
        }
        // add backends which are not in flatBackendsPerBucketSeq, with replication number 0
        for (Long backendId : allAvailBackendIds) {
            if (!backendToReplicaNum.containsKey(backendId)) {
                backendToReplicaNum.put(backendId, 0L);
            }
        }

        return backendToReplicaNum
                .entrySet()
                .stream()
                .sorted((entry1, entry2) -> {
                    if (!entry1.getValue().equals(entry2.getValue())) {
                        return (int) (entry2.getValue() - entry1.getValue());
                    }
                    BackendLoadStatistic beStat1 = statistic.getBackendLoadStatistic(entry1.getKey());
                    BackendLoadStatistic beStat2 = statistic.getBackendLoadStatistic(entry2.getKey());
                    if (beStat1 == null || beStat2 == null) {
                        return 0;
                    }
                    double loadScore1 = beStat1.getMixLoadScore();
                    double loadScore2 = beStat2.getMixLoadScore();
                    if (Math.abs(loadScore1 - loadScore2) < 1e-6) {
                        return 0;
                    } else if (loadScore2 > loadScore1) {
                        return 1;
                    } else {
                        return -1;
                    }
                })
                .collect(Collectors.toList());
    }

    /*
     * get the array indexes of elements in flatBackendsPerBucketSeq which equals to beId
     * eg:
     * flatBackendsPerBucketSeq:
     *      A B C A D E A F G A H I
     * and srcBeId is A.
     * so seqIndexes is:
     *      0 3 6 9
     */
    private List<Integer> getBeSeqIndexes(List<Long> flatBackendsPerBucketSeq, long beId) {
        return IntStream.range(0, flatBackendsPerBucketSeq.size()).boxed().filter(
                idx -> flatBackendsPerBucketSeq.get(idx).equals(beId)).collect(Collectors.toList());
    }

    private Set<Long> getUnavailableBeIdsInGroup(SystemInfoService infoService, ColocateTableIndex colocateIndex,
                                                 GroupId groupId) {
        Set<Long> backends = colocateIndex.getBackendsByGroup(groupId);
        Set<Long> unavailableBeIds = Sets.newHashSet();
        for (Long backendId : backends) {
            if (!checkBackendAvailable(backendId, infoService)) {
                unavailableBeIds.add(backendId);
            }
        }
        return unavailableBeIds;
    }

    private Set<Long> getDecommissionedBackendsInGroup(SystemInfoService infoService, ColocateTableIndex colocateIndex,
                                                       GroupId groupId) {
        Set<Long> backends = colocateIndex.getBackendsByGroup(groupId);
        Set<Long> decommissionedBackends = Sets.newHashSet();
        for (Long backendId : backends) {
            Backend be = infoService.getBackend(backendId);
            if (be != null && be.isDecommissioned() && be.isAlive()) {
                decommissionedBackends.add(backendId);
            }
        }
        return decommissionedBackends;
    }

    private List<Long> getAvailableBeIds(SystemInfoService infoService) {
        // get all backends to allBackendIds, and check be availability using checkBackendAvailable
        // backend stopped for a short period of time is still considered available
        List<Long> allBackendIds = infoService.getBackendIds(false);
        List<Long> availableBeIds = Lists.newArrayList();
        for (Long backendId : allBackendIds) {
            if (checkBackendAvailable(backendId, infoService)) {
                availableBeIds.add(backendId);
            }
        }
        return availableBeIds;
    }

    /**
     * check backend available
     * backend stopped for a short period of time is still considered available
     */
    private boolean checkBackendAvailable(Long backendId, SystemInfoService infoService) {
        long currTime = System.currentTimeMillis();
        Backend be = infoService.getBackend(backendId);
        if (be == null) {
            return false;
        } else if (!be.isAvailable()) {
            // 1. BE is dead for a long time
            // 2. BE is under decommission
            if ((!be.isAlive() &&
                    (currTime - be.getLastUpdateMs()) > Config.tablet_sched_colocate_be_down_tolerate_time_s * 1000)
                    || be.isDecommissioned()) {
                return false;
            }
        }
        return true;
    }

    private void cleanRelocationInfoMap(Set<GroupId> allGroupIds) {
        group2ColocateRelocationInfo.entrySet().removeIf(e -> !allGroupIds.contains(e.getKey()));
    }

    public void increaseScheduledTabletNumForBucket(TabletSchedCtx ctx) {
        if (ctx.getRelocationForRepair()) {
            ColocateRelocationInfo info = group2ColocateRelocationInfo.get(ctx.getColocateGroupId());
            if (info != null) {
                info.increaseScheduledTabletNumForBucket(ctx.getTabletOrderIdx());
            }
        }
    }

    public int getScheduledTabletNumForBucket(TabletSchedCtx ctx) {
        ColocateRelocationInfo info = group2ColocateRelocationInfo.get(ctx.getColocateGroupId());
        if (info != null) {
            Integer num = info.getScheduledTabletNumForBucket(ctx.getTabletOrderIdx());
            if (num == null) {
                return 0;
            } else {
                return num;
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public Set<Long> getLastBackendSeqForBucket(TabletSchedCtx ctx) {
        ColocateRelocationInfo info = group2ColocateRelocationInfo.get(ctx.getColocateGroupId());
        if (info != null) {
            return Sets.newHashSet(info.getLastBackendsPerBucketSeq().get(ctx.getTabletOrderIdx()));
        } else {
            return Sets.newHashSet();
        }
    }

    void adjustBackendSetIfReplicaBad(int tabletOrderIdx, LocalTablet tablet, TabletStatus st, GroupId groupId,
                                      boolean ignoreSingleReplicaCheck) {
        if (st != TabletStatus.COLOCATE_MISMATCH) {
            return;
        }

        List<Replica> replicas = tablet.getImmutableReplicas();
        if (!ignoreSingleReplicaCheck && replicas.size() <= 1) {
            return;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
        ClusterLoadStatistic statistic = globalStateMgr.getTabletScheduler().getLoadStatistic();
        ColocateTableIndex colocateIndex = globalStateMgr.getColocateTableIndex();

        Set<Long> currentBackendsSet = colocateIndex.getTabletBackendsByGroup(groupId, tabletOrderIdx);
        Set<Long> savedBackendSet = ImmutableSet.copyOf(currentBackendsSet);
        List<Long> badReplicaIdList = Lists.newArrayList();
        Set<Long> unavailableBeIdsInGroup = getUnavailableBeIdsInGroup(infoService, colocateIndex, groupId);
        List<Long> availableBeIds = getAvailableBeIds(infoService);
        Set<Long> removedBackendIdSet = Sets.newHashSet();
        boolean isBackendSetChanged = false;

        // Traverse all replicas of the tablet to check if we need to replace backend of bad replica.
        for (Replica replica : replicas) {
            long backendId = replica.getBackendId();
            if (replica.isBad() && currentBackendsSet.contains(backendId)) {
                badReplicaIdList.add(replica.getId());
                long replaceBackendId = -1;
                List<List<Long>> backendsPerBucketSeq =
                        Lists.newArrayList(colocateIndex.getBackendsPerBucketSeq(groupId));
                List<Long> flatBackendsPerBucketSeq =
                        backendsPerBucketSeq.stream().flatMap(List::stream).collect(Collectors.toList());
                // Get the aggregated number of replicas per backend and sort it in desc order, if we have to replace
                // some backend in the current backend set, we will choose the backend with the least number
                // of replicas first. The returned list only contains available backend.
                List<Map.Entry<Long, Long>> backendWithReplicaNum =
                        getSortedBackendReplicaNumPairs(availableBeIds, unavailableBeIdsInGroup, statistic,
                                flatBackendsPerBucketSeq);

                int idx = backendWithReplicaNum.size() - 1;
                while (idx >= 0) {
                    long chosenBackendId = backendWithReplicaNum.get(idx).getKey();
                    if (chosenBackendId != backendId && !currentBackendsSet.contains(chosenBackendId) &&
                            !removedBackendIdSet.contains(chosenBackendId)) {
                        replaceBackendId = chosenBackendId;
                        removedBackendIdSet.add(backendId);
                        break;
                    }
                    idx--;
                }

                if (replaceBackendId != -1) {
                    currentBackendsSet.add(replaceBackendId);
                    currentBackendsSet.remove(backendId);
                    isBackendSetChanged = true;
                }
            }
        }

        if (isBackendSetChanged) {
            // Backend set changed, update metadata in ColocateIndex, ColocateBalancer will arrange tablet migration
            // based on the new backend set.
            colocateIndex.setBackendsSetByIdxForGroup(groupId, tabletOrderIdx, currentBackendsSet);
            LOG.info("backend set changed because of bad replica, tablet id: {}, bad replica: {}," +
                    " old backend set: {}, new backend set: {}",
                    tablet.getId(), badReplicaIdList, savedBackendSet, currentBackendsSet);
        }
    }
}
