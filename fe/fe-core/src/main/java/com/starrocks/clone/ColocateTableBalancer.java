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
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.persist.ColocatePersistInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
        private final boolean relocationForRepair;

        /**
         * record the value of bucket sequence before changed, used to reset current bucket sequence
         * when condition is matched, e.g. the backend that caused colocate relocation before has come alive.
         */
        private final List<List<Long>> lastBackendsPerBucketSeq;

        /**
         * Count the number of tablets which have been successfully scheduled by TabletScheduler after a relocation
         * decision has been made.
         * <p>
         * bucket index -> number of finished scheduling tablets
         */
        private final Map<Integer, Integer> scheduledTabletNumPerBucket;

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
     * The replica distribution info of all colocate groups for a single backend.
     */
    private static class AllGroupsReplicaDistInfoPerBe {
        long backendId;
        /**
         * Using an example to explain the difference between these two counters,
         * Assume that we have a colocate group which has two 3-replica tables with 4 buckets, each table
         * has 10 partitions, and backend `be1` has been assigned 3 of the 4 buckets,
         * so for `be1`, `numOfBucketsAssigned` calculated is 3,
         * and `numOfReplicasAssigned` is 3 * 2(two tables) * 10(ten partitions) = 60.
         * If multi groups has been assigned to this backend, the corresponding counter is added up per group.
         */
        long totalNumOfBucketsAssigned;
        long totalNumOfReplicasAssigned;
        /**
         * Classify the groups assigned to this backend based on the number of replicas
         * assigned to this backend per each group.
         */
        Map<Integer, Set<GroupId>> replicaNumToGroupsMap;
        /**
         * groupId -> [0, 2, 3, ..] (bucket index)
         */
        Map<GroupId, List<Integer>> groupToAssignedBucketIdx;

        public AllGroupsReplicaDistInfoPerBe(long backendId) {
            this.backendId = backendId;
            totalNumOfBucketsAssigned = totalNumOfReplicasAssigned = 0;
            replicaNumToGroupsMap = new HashMap<>();
            groupToAssignedBucketIdx = new HashMap<>();
        }

        public static AllGroupsReplicaDistInfoPerBe createNewDistInfoObj(long backendId) {
            return new AllGroupsReplicaDistInfoPerBe(backendId);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{beId: ")
                    .append(backendId)
                    .append(", bn: ")
                    .append(totalNumOfBucketsAssigned)
                    .append(", rn: ")
                    .append(totalNumOfReplicasAssigned)
                    .append(", grpToBkIdx: ")
                    .append(groupToAssignedBucketIdx)
                    .append("}");
            return sb.toString();
        }
    }

    /**
     * ColocateRelocationInfo per group.
     */
    private final Map<GroupId, ColocateRelocationInfo> group2ColocateRelocationInfo = Maps.newConcurrentMap();

    /*
     * Each round, we do 2 steps:
     * 1. Relocate and balance
     *   1.1 Firstly, we do it from a single group view, i.e. we will balance the replica distribution per group,
     *     - Backend is not available, find a new backend to replace it.
     *     - and after all unavailable has been replaced, balance the group
     *   1.2 After we have done 1.1, the replica distribution may not be balanced from overall view, for example,
     *     we have 4 backends and two colocate groups, each has 6 buckets with single replica, the replica distribution
     *     for these two groups are as follows:
     *       group1    2 2 1 1
     *       backend   A B C D
     *       -----------------
     *       group2    2 2 1 1
     *       backend   A B C D
     *     from per group view, it's balanced, but from overall view, it's not, because the replica
     *     distribution from overall looks like this,
     *       overall   4 4 2 2
     *       backend   A B C D
     *     we should try to balance it to the following state,
     *       group1    2 2 1 1
     *       backend   A B C D
     *       -----------------
     *       group2    1 1 2 2
     *       backend   A B C D
     *
     * 2. Match group:
     *   If replica mismatch backends in a group, that group will be marked as unstable, and pass that
     *   tablet to TabletScheduler.
     *   Otherwise, mark the group as stable
     */
    @Override
    protected void runAfterCatalogReady() {
        if (!Config.tablet_sched_disable_colocate_balance) {
            relocateAndBalancePerGroup();
            relocateAndBalanceAllGroups();
        }
        matchGroups();
    }

    /*
     * Relocate and balance per group view
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
    private boolean relocateAndBalancePerGroup() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ColocateTableIndex colocateIndex = globalStateMgr.getColocateTableIndex();
        SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();

        // get all groups
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        TabletScheduler tabletScheduler = globalStateMgr.getTabletScheduler();
        TabletSchedulerStat stat = tabletScheduler.getStat();
        Set<GroupId> toIgnoreGroupIds = new HashSet<>();
        boolean isAnyGroupChanged = false;
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

            if (toIgnoreGroupIds.contains(groupId)) {
                continue;
            }

            Set<Long> unavailableBeIdsInGroup = getUnavailableBeIdsInGroup(infoService, colocateIndex, groupId);
            stat.counterColocateBalanceRound.incrementAndGet();
            List<Long> availableBeIds = getAvailableBeIds(infoService);
            List<List<Long>> balancedBackendsPerBucketSeq = Lists.newArrayList();
            if (doRelocateAndBalance(groupId, unavailableBeIdsInGroup, availableBeIds, colocateIndex, infoService,
                    statistic, balancedBackendsPerBucketSeq)) {
                List<GroupId> colocateWithGroupsInOtherDb =
                        colocateIndex.getColocateWithGroupsInOtherDb(groupId);
                // For groups which have the same GroupId.grpId with current group, the bucket seq should be the same,
                // so here we will update the bucket seq for them directly and ignore the following traverse.
                toIgnoreGroupIds.addAll(colocateWithGroupsInOtherDb);
                // Add myself into the list so that we can persist the bucket seq info with other groups altogether.
                colocateWithGroupsInOtherDb.add(groupId);
                for (GroupId gid : colocateWithGroupsInOtherDb) {
                    group2ColocateRelocationInfo.put(gid,
                            new ColocateRelocationInfo(!unavailableBeIdsInGroup.isEmpty(),
                                    colocateIndex.getBackendsPerBucketSeq(gid), Maps.newHashMap()));
                    colocateIndex.addBackendsPerBucketSeq(gid, balancedBackendsPerBucketSeq);
                    ColocatePersistInfo info =
                            ColocatePersistInfo.createForBackendsPerBucketSeq(gid, balancedBackendsPerBucketSeq);
                    globalStateMgr.getEditLog().logColocateBackendsPerBucketSeq(info);
                    LOG.info("balance colocate per group , group id {}, " +
                                    "now backends per bucket sequence is: {}, " +
                                    "bucket sequence before balance: {}", gid, balancedBackendsPerBucketSeq,
                            group2ColocateRelocationInfo.get(gid).getLastBackendsPerBucketSeq());
                    isAnyGroupChanged = true;
                }
            } else {
                // clean historical relocation info if nothing changed after trying to do `relocateAndBalance()`
                group2ColocateRelocationInfo.remove(groupId);
            }
        }
        cleanRelocationInfoMap(groupIds);

        return isAnyGroupChanged;
    }

    private Map<GroupId, List<GroupId>> initGroupToCoGroupsInOtherDbMap(ColocateTableIndex colocateIndex,
                                                                        Set<GroupId> allGroups) {
        Map<GroupId, List<GroupId>> groupToCoGroupsInOtherDb = new HashMap<>();
        Set<GroupId> toIgnoreGroups = new HashSet<>();
        for (GroupId groupId : allGroups) {
            if (!toIgnoreGroups.contains(groupId)) {
                List<GroupId> colocateWithGroupsInOtherDb =
                        colocateIndex.getColocateWithGroupsInOtherDb(groupId);
                toIgnoreGroups.addAll(colocateWithGroupsInOtherDb);
                Preconditions.checkState(!groupToCoGroupsInOtherDb.containsKey(groupId));
                groupToCoGroupsInOtherDb.put(groupId, colocateWithGroupsInOtherDb);
            }
        }

        return groupToCoGroupsInOtherDb;
    }

    private Map<Long, AllGroupsReplicaDistInfoPerBe> initDistInfoPerBackendMap(ColocateTableIndex colocateIndex,
                                                                               SystemInfoService systemInfoService,
                                                                               Map<GroupId, List<GroupId>>
                                                                                       groupToCoGroupsInOtherDb) {
        Map<Long, AllGroupsReplicaDistInfoPerBe> distInfoPerBackendMap = new HashMap<>();
        for (Map.Entry<GroupId, List<GroupId>> entry : groupToCoGroupsInOtherDb.entrySet()) {
            GroupId groupId = entry.getKey();
            List<GroupId> coGroupsInOtherDb = entry.getValue();
            List<List<Long>> backendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(groupId);
            if (backendsPerBucketSeq.isEmpty()) {
                continue;
            }

            for (int i = 0; i < backendsPerBucketSeq.size(); i++) {
                for (long backendId : backendsPerBucketSeq.get(i)) {
                    distInfoPerBackendMap.putIfAbsent(backendId,
                            AllGroupsReplicaDistInfoPerBe.createNewDistInfoObj(backendId));
                    AllGroupsReplicaDistInfoPerBe distInfoPerBe = distInfoPerBackendMap.get(backendId);
                    distInfoPerBe.totalNumOfBucketsAssigned++;
                    int numOfTabletsPerBucket = colocateIndex.getNumOfTabletsPerBucket(groupId);
                    // if this group has groups colocated with in other databases, we should add them up
                    for (GroupId otherGroupId : coGroupsInOtherDb) {
                        numOfTabletsPerBucket += colocateIndex.getNumOfTabletsPerBucket(otherGroupId);
                    }
                    distInfoPerBe.totalNumOfReplicasAssigned += numOfTabletsPerBucket;
                    distInfoPerBe.replicaNumToGroupsMap
                            .computeIfAbsent(numOfTabletsPerBucket, k -> Sets.newHashSet())
                            .add(groupId);
                    distInfoPerBe.groupToAssignedBucketIdx
                            .computeIfAbsent(groupId, k -> Lists.newArrayList())
                            .add(i);
                }
            }
        }

        // for backends that haven't assigned any bucket of any colocate group, we should
        // also initialize its `AllGroupsReplicaDistInfoPerBe`, in order to migrate some buckets to them
        // in the following balancing process
        List<Long> availableBackends = systemInfoService.getAvailableBackendIds();
        availableBackends.forEach(backendId ->
                distInfoPerBackendMap.putIfAbsent(backendId,
                        AllGroupsReplicaDistInfoPerBe.createNewDistInfoObj(backendId)));

        return distInfoPerBackendMap;
    }

    // Sort `distInfoPerBeList` in desc order based on the following rules,
    // First we compare them using `totalNumOfBucketsAssigned`,
    // if two `AllGroupsReplicaDistInfoPerBe` object has the same number of assigned buckets,
    // then comparing them using `totalNumOfReplicasAssigned`
    private List<AllGroupsReplicaDistInfoPerBe> sortDistInfoPerBackendList(
            List<AllGroupsReplicaDistInfoPerBe> distInfoPerBackendList) {
        distInfoPerBackendList.sort((info1, info2) -> {
            if (info2.totalNumOfBucketsAssigned == info1.totalNumOfBucketsAssigned) {
                return (int) (info2.totalNumOfReplicasAssigned - info1.totalNumOfReplicasAssigned);
            } else {
                return (int) (info2.totalNumOfBucketsAssigned - info1.totalNumOfBucketsAssigned);
            }
        });

        return distInfoPerBackendList;
    }

    private Set<String> getHostSetOfBucket(Set<Long> backendIds, SystemInfoService systemInfoService) {
        Set<String> hostSet = new HashSet<>();
        for (long backendId : backendIds) {
            Backend backend = systemInfoService.getBackend(backendId);
            if (backend != null) {
                hostSet.add(backend.getHost());
            }
        }

        return hostSet;
    }

    private List<List<Long>> convertToListOfLists(List<Set<Long>> listOfSets) {
        List<List<Long>> listOfLists = new ArrayList<>();
        for (Set<Long> set : listOfSets) {
            List<Long> list = new ArrayList<>(set);
            listOfLists.add(list);
        }
        return listOfLists;
    }

    private void updateDistInfoAfterBalanceMove(AllGroupsReplicaDistInfoPerBe srcInfo,
                                                AllGroupsReplicaDistInfoPerBe destInfo,
                                                int numOfReplicas,
                                                GroupId groupId,
                                                int bucketIdx) {
        srcInfo.totalNumOfBucketsAssigned--;
        destInfo.totalNumOfBucketsAssigned++;

        srcInfo.totalNumOfReplicasAssigned -= numOfReplicas;
        destInfo.totalNumOfReplicasAssigned += numOfReplicas;

        List<Integer> srcAssignedBucketIdxList = srcInfo.groupToAssignedBucketIdx.get(groupId);
        Preconditions.checkState(srcAssignedBucketIdxList.remove(Integer.valueOf(bucketIdx)));
        if (srcAssignedBucketIdxList.isEmpty()) {
            srcInfo.groupToAssignedBucketIdx.remove(groupId);
            Set<GroupId> srcGroupIdSet = srcInfo.replicaNumToGroupsMap.get(numOfReplicas);
            srcGroupIdSet.remove(groupId);
            if (srcGroupIdSet.isEmpty()) {
                srcInfo.replicaNumToGroupsMap.remove(numOfReplicas);
            }
        }

        destInfo.groupToAssignedBucketIdx.computeIfAbsent(groupId, k -> new ArrayList<>()).add(bucketIdx);
        destInfo.replicaNumToGroupsMap.computeIfAbsent(numOfReplicas, k -> new HashSet<>()).add(groupId);
    }

    /**
     * Balance replica distribution from overall view.
     * First we collect the replica distribution info of all colocate groups for each backend, and put them into
     * a list `distInfoPerBackendList`. To be noticed, for groups that have colocated groups in other databases,
     * we should treat them as a whole, and only record one `AllGroupsReplicaDistInfoPerBe` object for them,
     * because the backend list per bucket sequence of them should be kept the same.
     * Then we sort `distInfoPerBackendList` based on the number of buckets and replicas assigned to each backend,
     * see `sortDistInfoPerBackendList()` for more details. After the sort, we try to move a bucket from backend which
     * has the most number of buckets to backend that has the lowest in each balance round.
     * If the src is not fitted to move bucket to dest, we try the next one. For all the groups which have
     * been assigned to the src backend, we choose the group which will make the replica distribution better after move
     * as the source.
     */
    private void relocateAndBalanceAllGroups() {
        if (Config.tablet_sched_disable_colocate_overall_balance) {
            return;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        ColocateTableIndex colocateIndex = globalStateMgr.getColocateTableIndex();
        Set<GroupId> allGroups = colocateIndex.getAllGroupIds();
        if (allGroups.isEmpty()) {
            return;
        }

        // group -> list of groups colocated with in other database
        // For those groups, we should treat them as a single group, we use one of the group to represent them all,
        // and change backend sequence of buckets for all of them when that representing one's has changed.
        // Besides, the `totalNumOfReplicasAssigned` in `AllGroupsReplicaDistInfoPerBe` for that "single group"
        // should be the totaling of all the colocated with groups.
        Map<GroupId, List<GroupId>> groupToCoGroupsInOtherDb =
                initGroupToCoGroupsInOtherDbMap(colocateIndex, allGroups);
        Map<GroupId, List<GroupId>> filteredMap = groupToCoGroupsInOtherDb.entrySet().stream()
                .filter(e -> !e.getValue().isEmpty())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!filteredMap.isEmpty()) {
            LOG.info("groups that have colocate groups in other databases: {}", filteredMap);
        }

        // backendId -> replica dist info for all groups,
        // see `AllGroupsReplicaDistInfoPerBe` for more details
        Map<Long, AllGroupsReplicaDistInfoPerBe> distInfoPerBackendMap =
                initDistInfoPerBackendMap(colocateIndex, systemInfoService, groupToCoGroupsInOtherDb);

        // Compute the average number of replicas every backend should have
        long totalReplicas = 0;
        for (AllGroupsReplicaDistInfoPerBe info : distInfoPerBackendMap.values()) {
            totalReplicas += info.totalNumOfReplicasAssigned;
        }
        long avgNumOfReplicasPerBackend = totalReplicas / distInfoPerBackendMap.size();

        // keep the balanced result of backend list per bucket sequence for each changed group
        //               bucket 0   bucket 1  ...
        // group id -> [[A, B, C], [C, D, E], ...]
        Map<GroupId, List<Set<Long>>> result = new HashMap<>();
        List<AllGroupsReplicaDistInfoPerBe> distInfoPerBackendList = new ArrayList<>(distInfoPerBackendMap.values());
        LOG.info("no backend list per bucket changed in per-group balance round, " +
                        "start overall colocate balance, avg replica num: {}, current state: {}",
                avgNumOfReplicasPerBackend, sortDistInfoPerBackendList(distInfoPerBackendList));
        OUTER:
        while (true) {
            boolean hasChangeInBalanceRound = false;
            sortDistInfoPerBackendList(distInfoPerBackendList);

            // try to move replica into dest
            AllGroupsReplicaDistInfoPerBe destInfo = distInfoPerBackendList.get(distInfoPerBackendList.size() - 1);
            if (distInfoPerBackendList.get(0).totalNumOfBucketsAssigned
                    - destInfo.totalNumOfBucketsAssigned <= 1) {
                // now in balanced state.
                break;
            }

            // try to move replica out from src
            for (int srcIdx = 0; srcIdx < distInfoPerBackendList.size() - 1; srcIdx++) {
                AllGroupsReplicaDistInfoPerBe srcInfo = distInfoPerBackendList.get(srcIdx);
                String srcBackendHost = systemInfoService.getBackendHostById(srcInfo.backendId);
                String destBackendHost = systemInfoService.getBackendHostById(destInfo.backendId);
                if (srcBackendHost == null || destBackendHost == null) {
                    continue;
                }

                List<Integer> srcNumOfReplicasList = new ArrayList<>(srcInfo.replicaNumToGroupsMap.keySet());
                long destCurrentTotalReplicaNum = destInfo.totalNumOfReplicasAssigned;
                // sort the classified number of replicas list on src in the order that will make the
                // `totalNumOfReplicasAssigned` of dest backend closer to `avgNumOfReplicasPerBackend`
                srcNumOfReplicasList.sort((replicaNum1, replicaNum2) -> Math.toIntExact(
                        Math.abs(replicaNum1 + destCurrentTotalReplicaNum - avgNumOfReplicasPerBackend) -
                                Math.abs(replicaNum2 + destCurrentTotalReplicaNum - avgNumOfReplicasPerBackend)));
                for (int numOfReplicas : srcNumOfReplicasList) {
                    List<GroupId> candidateGroupList =
                            new ArrayList<>(srcInfo.replicaNumToGroupsMap.get(numOfReplicas));
                    Collections.shuffle(candidateGroupList);
                    for (GroupId groupId : candidateGroupList) {
                        // we use `result` map to temporarily keep the balance result, so here we need
                        // to get the updated backend list if it has already changed because of the ongoing
                        // balance process, if not, get it from the metadata manager, i.e. ColocateIndex
                        List<Set<Long>> backendsPerBucketSeq =
                                result.getOrDefault(groupId, colocateIndex.getBackendsPerBucketSeqSet(groupId));
                        List<Integer> srcAssignedBucketIdxList = srcInfo.groupToAssignedBucketIdx.get(groupId);
                        for (int bucketIdx : srcAssignedBucketIdxList) {
                            List<Integer> destAssignedBucketIdxList =
                                    destInfo.groupToAssignedBucketIdx.getOrDefault(groupId, new ArrayList<>());
                            Set<Long> backendSetForBucket = backendsPerBucketSeq.get(bucketIdx);
                            Set<String> hostSetOfBucket =
                                    getHostSetOfBucket(backendSetForBucket, systemInfoService);
                            // we cannot move replica of a bucket to a backend which already has a replica assigned
                            // for that bucket or is on the same host with other replica
                            if (!destAssignedBucketIdxList.contains(bucketIdx) &&
                                    (srcBackendHost.equals(destBackendHost) ||
                                            !hostSetOfBucket.contains(destBackendHost))) {
                                Set<Long> oldBackendSetForBucket = new HashSet<>(backendSetForBucket);
                                // change the assigning for bucket with index `bucketIdx`
                                // from backend `srcInfo.backendId` to backend `destInfo.backendId`
                                backendSetForBucket.remove(srcInfo.backendId);
                                backendSetForBucket.add(destInfo.backendId);
                                // Here we check whether this move will break the per group balanced state,
                                // if it will, we won't make this move
                                if (!checkKeepingPerGroupBalanced(backendsPerBucketSeq)) {
                                    // restore backend set
                                    backendsPerBucketSeq.set(bucketIdx, oldBackendSetForBucket);
                                    continue;
                                }
                                result.putIfAbsent(groupId, backendsPerBucketSeq);
                                // also need to update the distribution info statistics for following balance round
                                updateDistInfoAfterBalanceMove(srcInfo, destInfo, numOfReplicas, groupId, bucketIdx);
                                LOG.info("overall colocate balance for group {}, replace backend {} for " +
                                                "bucket index {} with backend {}, original backend list: {}",
                                        groupId, srcInfo.backendId, bucketIdx, destInfo.backendId,
                                        oldBackendSetForBucket);
                                // balance one bucket each round, need to sort `distInfoPerBackendList` and make new
                                // balance decision based on updated replica distribution info
                                continue OUTER;
                            }
                        } // bucket idx traverse
                    } // group list traverse
                } // numOfReplicas list traverse
            } // for src traverse

            // to avoid deadliness loop
            if (!hasChangeInBalanceRound) {
                LOG.info("we hava unbalanced replica distribution from overall view, " +
                        "but we cannot find find any available src from which to move " +
                        "replica to dest backend {}, going to abort overall balance", destInfo.backendId);
                break;
            }
        } // while(true)

        // update backends per bucket sequence for each group according to the result
        for (Map.Entry<GroupId, List<Set<Long>>> entry : result.entrySet()) {
            List<List<Long>> balancedBackendsPerBucketSeq = convertToListOfLists(entry.getValue());
            // if the group colocate with groups in other db,
            // those groups should also be changed based on new backend list
            List<GroupId> toChangeGroups = groupToCoGroupsInOtherDb.get(entry.getKey());
            // add self
            toChangeGroups.add(entry.getKey());
            List<List<Long>> oldBackendsPerBucketSeq = colocateIndex.getBackendsPerBucketSeq(entry.getKey());
            for (GroupId groupId : toChangeGroups) {
                colocateIndex.addBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeq);
                ColocatePersistInfo info =
                        ColocatePersistInfo.createForBackendsPerBucketSeq(groupId, balancedBackendsPerBucketSeq);
                globalStateMgr.getEditLog().logColocateBackendsPerBucketSeq(info);
                LOG.info("overall colocate balance for group {}, now backends per bucket sequence is: {}, " +
                                "bucket sequence before balance: {}",
                        groupId, balancedBackendsPerBucketSeq, oldBackendsPerBucketSeq);

            }
        }

        if (result.size() > 0) {
            LOG.info("{} group(s) changed in overall colocate balance round, updated state: {}",
                    result.size(), sortDistInfoPerBackendList(distInfoPerBackendList));
        }
    }

    private boolean checkKeepingPerGroupBalanced(List<Set<Long>> backendsPerBucketSeq) {
        // backend id -> replica num, and sorted by replica num, descending.
        Map<Long, Long> backendToReplicaNum = backendsPerBucketSeq.stream().flatMap(Collection::stream)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        List<Map.Entry<Long, Long>> entries =
                backendToReplicaNum.entrySet().stream().sorted(Comparator.comparingLong(Map.Entry::getValue))
                        .collect(Collectors.toList());
        int lastIdx = entries.size() - 1;
        return entries.get(lastIdx).getValue() - entries.get(0).getValue() <= 1;
    }

    public static boolean needToForceRepair(TabletStatus st, LocalTablet tablet, Set<Long> backendsSet) {
        boolean hasBad = false;
        for (Replica replica : tablet.getImmutableReplicas()) {
            if (!backendsSet.contains(replica.getBackendId())) {
                continue;
            }
            if (replica.isBad()) {
                hasBad = true;
                break;
            }
        }

        return hasBad && st == TabletStatus.COLOCATE_REDUNDANT;
    }

    private long doMatchOneGroup(GroupId groupId,
                                 boolean isUrgent,
                                 GlobalStateMgr globalStateMgr,
                                 ColocateTableIndex colocateIndex,
                                 TabletScheduler tabletScheduler) {
        long checkStartTime = System.currentTimeMillis();
        long lockTotalTime = 0;
        long waitTotalTimeMs = 0;
        List<Long> tableIds = colocateIndex.getAllTableIds(groupId);
        Database db = globalStateMgr.getDbIncludeRecycleBin(groupId.dbId);
        if (db == null) {
            return lockTotalTime;
        }

        List<Set<Long>> backendBucketsSeq = colocateIndex.getBackendsPerBucketSeqSet(groupId);
        if (backendBucketsSeq.isEmpty()) {
            return lockTotalTime;
        }

        boolean isGroupStable = true;
        // set the config to a local variable to avoid config params changed.
        int partitionBatchNum = Config.tablet_checker_partition_batch_num;
        int partitionChecked = 0;
        db.readLock();
        long lockStart = System.nanoTime();
        try {
            TABLE:
            for (Long tableId : tableIds) {
                OlapTable olapTable = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tableId);
                if (olapTable == null || !colocateIndex.isColocateTable(olapTable.getId())) {
                    continue;
                }

                if ((isUrgent && !globalStateMgr.getTabletChecker().isUrgentTable(db.getId(), tableId))) {
                    continue;
                }

                for (Partition partition : globalStateMgr.getPartitionsIncludeRecycleBin(olapTable)) {
                    partitionChecked++;

                    boolean isPartitionUrgent =
                            globalStateMgr.getTabletChecker().isPartitionUrgent(db.getId(), tableId, partition.getId());
                    boolean isUrgentPartitionHealthy = true;
                    if ((isUrgent && !isPartitionUrgent) || (!isUrgent && isPartitionUrgent)) {
                        continue;
                    }

                    if (partitionChecked % partitionBatchNum == 0) {
                        lockTotalTime += System.nanoTime() - lockStart;
                        // release lock, so that lock can be acquired by other threads.
                        db.readUnlock();
                        db.readLock();
                        lockStart = System.nanoTime();
                        if (globalStateMgr.getDbIncludeRecycleBin(groupId.dbId) == null) {
                            return lockTotalTime;
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
                                backendBucketsSeq.size() + " v.s. " + index.getTablets().size());
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
                                    if (isPartitionUrgent) {
                                        colocateUnhealthyPrio = Priority.VERY_HIGH;
                                        isUrgentPartitionHealthy = false;
                                    }

                                    // We should also check if the tablet is ready to be repaired like
                                    // `TabletChecker` did. Slightly delay the repair action can avoid unnecessary
                                    // clone in situation like temporarily restart BE Nodes.
                                    if (tablet.readyToBeRepaired(st, colocateUnhealthyPrio)) {
                                        LOG.debug("get unhealthy tablet {} in colocate table. status: {}",
                                                tablet.getId(), st);
                                        TabletSchedCtx tabletCtx = new TabletSchedCtx(
                                                TabletSchedCtx.Type.REPAIR,
                                                db.getId(), tableId, partition.getId(), index.getId(),
                                                tablet.getId(),
                                                System.currentTimeMillis());
                                        // the tablet status will be checked and set again when being scheduled
                                        tabletCtx.setTabletStatus(st);
                                        // using HIGH priority, because we want to stabilize the colocate group
                                        // as soon as possible
                                        tabletCtx.setOrigPriority(colocateUnhealthyPrio);
                                        tabletCtx.setTabletOrderIdx(idx);
                                        tabletCtx.setColocateGroupId(groupId);
                                        tabletCtx.setTablet(tablet);
                                        ColocateRelocationInfo info = group2ColocateRelocationInfo.get(groupId);
                                        tabletCtx.setRelocationForRepair(info != null
                                                && info.getRelocationForRepair()
                                                && st == TabletStatus.COLOCATE_MISMATCH);

                                        // For bad replica, we ignore the size limit of scheduler queue
                                        Pair<Boolean, Long> result =
                                                tabletScheduler.blockingAddTabletCtxToScheduler(db, tabletCtx,
                                                        needToForceRepair(st, tablet,
                                                        bucketsSeq) || isPartitionUrgent /* forcefully add or not */);
                                        waitTotalTimeMs += result.second;
                                        if (result.first && tabletCtx.getRelocationForRepair()) {
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
                        } // end for tablets
                    } // end for materialize indexes

                    if (isUrgentPartitionHealthy && isPartitionUrgent) {
                        globalStateMgr.getTabletChecker().removeFromUrgentTable(
                                new TabletChecker.RepairTabletInfo(db.getId(), tableId,
                                        Lists.newArrayList(partition.getId())));
                    }
                } // end for partitions
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

        return lockTotalTime - waitTotalTimeMs * 1000000;
    }

    private long matchOneGroupUrgent(GroupId groupId,
                                     GlobalStateMgr globalStateMgr,
                                     ColocateTableIndex colocateIndex,
                                     TabletScheduler tabletScheduler) {
        return doMatchOneGroup(groupId, true, globalStateMgr, colocateIndex, tabletScheduler);
    }

    private long matchOneGroupNonUrgent(GroupId groupId,
                                        GlobalStateMgr globalStateMgr,
                                        ColocateTableIndex colocateIndex,
                                        TabletScheduler tabletScheduler) {
        return doMatchOneGroup(groupId, false, globalStateMgr, colocateIndex, tabletScheduler);
    }

    /*
     * Check every tablet of a group, if replica's location does not match backends in group, relocating those
     * replicas, and mark that group as unstable.
     * If every replicas match the backends in group, mark that group as stable.
     */
    private void matchGroups() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        ColocateTableIndex colocateIndex = globalStateMgr.getColocateTableIndex();
        TabletScheduler tabletScheduler = globalStateMgr.getTabletScheduler();

        long start = System.nanoTime();
        long lockTotalTime = 0;
        // check each group
        Set<GroupId> groupIds = colocateIndex.getAllGroupIds();
        for (GroupId groupId : groupIds) {
            lockTotalTime += matchOneGroupUrgent(groupId, globalStateMgr, colocateIndex, tabletScheduler);
            lockTotalTime += matchOneGroupNonUrgent(groupId, globalStateMgr, colocateIndex, tabletScheduler);
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
    private boolean doRelocateAndBalance(GroupId groupId, Set<Long> unavailableBeIds, List<Long> availableBeIds,
                                         ColocateTableIndex colocateIndex, SystemInfoService infoService,
                                         ClusterLoadStatistic statistic,
                                         List<List<Long>> balancedBackendsPerBucketSeq) {
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
            Preconditions.checkState(backendsPerBucketSeq.size() == hostsPerBucketSeq.size());

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
                    // eg: 3 / 3 = 1, so that the bucket index of the 4th backend id in flatBackendsPerBucketSeq is 1.
                    int bucketIndex = seqIndex / replicationNum;
                    List<Long> backendsSet = backendsPerBucketSeq.get(bucketIndex);
                    List<String> hostsSet = hostsPerBucketSeq.get(bucketIndex);
                    // the replicas of a tablet can not locate in same Backend or same host
                    if (!backendsSet.contains(destBeId) && !hostsSet.contains(destBe.getHost())) {
                        Preconditions.checkState(backendsSet.contains(srcBeId), srcBeId);
                        flatBackendsPerBucketSeq.set(seqIndex, destBeId);
                        LOG.info("per group colocate balance, replace backend {} with backend {} in colocate" +
                                        " group {}, src be seq index: {}" +
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

        List<Map.Entry<Long, Long>> entries = new ArrayList<>(backendToReplicaNum.entrySet());
        if (!FeConstants.runningUnitTest) {
            // to randomize the relative order of entries with the same number of replicas
            Collections.shuffle(entries);
        }
        entries.sort((entry1, entry2) -> {
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
        });

        return entries;
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
            return (be.isAlive() ||
                    (currTime - be.getLastUpdateMs()) <= Config.tablet_sched_colocate_be_down_tolerate_time_s * 1000)
                    && !be.isDecommissioned();
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
}
