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

package com.starrocks.clone;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.clone.BackendLoadStatistic.Classification;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.NodeSelector;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * DiskAndTabletLoadReBalancer is responsible for the balancing of disk usage and tablet distribution.
 * There are two balance types:
 * 1. balance between different backends in the cluster, through remote clone.
 * 2. balance between different disks in single backend, through local migration.
 * <p>
 * ReBalancer balance cluster first, then balance disks within backend.
 * <p>
 * When the disk space usage rate is relatively low (maxUsedPercent < 50%), or
 * when the usage rate is almost the same (maxUsedPercent - minUsedPercent < 10%),
 * Balancer will balance by tablet distribution. Otherwise, it will balance by disk usage.
 * <p>
 * Different storage medium will be scheduled separately.
 * <p>
 * Backend balance can also balance colocate table.
 */
public class DiskAndTabletLoadReBalancer extends Rebalancer {
    private static final Logger LOG = LogManager.getLogger(DiskAndTabletLoadReBalancer.class);
    // tabletId -> replicaId
    // used to delete src replica after copy task success
    private final Map<Long, Long> cachedReplicaId = new ConcurrentHashMap<>();

    @Override
    protected List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            ClusterLoadStatistic clusterStat, TStorageMedium medium) {
        if (RunMode.isSharedDataMode()) {
            return Collections.emptyList();
        }
        List<TabletSchedCtx> alternativeTablets;
        String balanceType;
        do {
            // balance cluster
            if (!isClusterDiskBalanced(clusterStat, medium)) {
                alternativeTablets = balanceClusterDisk(clusterStat, medium);
                balanceType = "cluster disk";
            } else {
                alternativeTablets = balanceClusterTablet(clusterStat, medium);
                balanceType = "cluster tablet distribution";
            }
            if (!alternativeTablets.isEmpty()) {
                break;
            }

            // balance backend
            if (!isBackendDiskBalanced(clusterStat, medium)) {
                alternativeTablets = balanceBackendDisk(clusterStat, medium);
                balanceType = "backend disk";
            } else {
                alternativeTablets = balanceBackendTablet(clusterStat, medium);
                balanceType = "backend tablet distribution";
            }
        } while (false);

        if (!alternativeTablets.isEmpty()) {
            LOG.info("select tablets to balance {}: total {}, medium {}, tablets[show up to 100]: {}",
                    balanceType, alternativeTablets.size(), medium,
                    alternativeTablets.stream().mapToLong(TabletSchedCtx::getTabletId).limit(100).toArray());
        }
        return alternativeTablets;
    }

    @Override
    public void completeSchedCtx(TabletSchedCtx tabletCtx, Map<Long, TabletScheduler.PathSlot> backendsWorkingSlots)
            throws SchedException {
        TStorageMedium medium = tabletCtx.getStorageMedium();
        ClusterLoadStatistic clusterStat = loadStatistic;
        if (clusterStat == null) {
            throw new SchedException(SchedException.Status.UNRECOVERABLE, "cluster does not exist");
        }

        long replicaSize = tabletCtx.getSrcReplica().getDataSize();
        boolean isLocalBalance = (tabletCtx.getDestBackendId() == tabletCtx.getSrcBackendId());
        // tabletCtx may wait a long time from the pending state to the running state, so we must double-check the task
        if (tabletCtx.getBalanceType() == BalanceType.DISK) {
            BackendLoadStatistic srcBeStat = clusterStat.getBackendLoadStatistic(tabletCtx.getSrcBackendId());
            BackendLoadStatistic destBeStat = clusterStat.getBackendLoadStatistic(tabletCtx.getDestBackendId());
            if (srcBeStat == null || destBeStat == null) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "src be or dest be statistic not exist");
            }

            long srcTotalCapacity;
            long destTotalCapacity;
            long srcTotalUsedCapacity;
            long destTotalUsedCapacity;
            if (isLocalBalance) {
                // get src disk and dest disk info that are on the same be.
                RootPathLoadStatistic srcPathStat = destBeStat.getPathStatistic(tabletCtx.getSrcPathHash());
                RootPathLoadStatistic destPathStat = destBeStat.getPathStatistic(tabletCtx.getDestPathHash());
                if (srcPathStat == null || destPathStat == null) {
                    throw new SchedException(SchedException.Status.UNRECOVERABLE,
                            "src disk or dest disk statistic not exist");
                }

                if (srcPathStat.getUsedPercent() < destPathStat.getUsedPercent()) {
                    throw new SchedException(SchedException.Status.UNRECOVERABLE,
                            "src be disk used percent is smaller than dest be");
                }

                srcTotalCapacity = srcPathStat.getCapacityB();
                destTotalCapacity = destPathStat.getCapacityB();
                srcTotalUsedCapacity = srcPathStat.getUsedCapacityB();
                destTotalUsedCapacity = destPathStat.getUsedCapacityB();
            } else {
                // get src be disks and dest be disks info, src be and dest be are different.
                if (srcBeStat.getUsedPercent(medium) < destBeStat.getUsedPercent(medium)) {
                    throw new SchedException(SchedException.Status.UNRECOVERABLE,
                            "src be disk used percent is smaller than dest be");
                }

                srcTotalCapacity = srcBeStat.getTotalCapacityB(medium);
                destTotalCapacity = destBeStat.getTotalCapacityB(medium);
                srcTotalUsedCapacity = srcBeStat.getTotalUsedCapacityB(medium);
                destTotalUsedCapacity = destBeStat.getTotalUsedCapacityB(medium);
            }

            // check total and used percent before clone
            if (srcTotalCapacity <= 0 || destTotalCapacity <= 0) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "src or dest total capacity error");
            }
            if (srcBeStat.getUsedPercent(medium) < destBeStat.getUsedPercent(medium)) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE,
                        "src be disk used percent is smaller than dest be");
            }

            // check used percent after clone
            double srcUsedPercent = (double) (srcTotalUsedCapacity - replicaSize) / srcTotalCapacity;
            double destUsedPercent = (double) (destTotalUsedCapacity + replicaSize) / destTotalCapacity;
            if (DiskInfo.exceedLimit(destTotalCapacity - destTotalUsedCapacity - replicaSize,
                    destTotalCapacity, false)) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "dest be disk used exceed limit");
            }
            if (srcUsedPercent < destUsedPercent) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE,
                        "src be disk used percent is smaller than dest be after clone");
            }
        } else {
            // for tablet balance task, first check whether it can preserve disk balance,
            // then check whether it can make tablet distribution balance better
            double maxUsedPercent = 0.0;
            double minUsedPercent = Double.MAX_VALUE;

            if (isLocalBalance) {
                BackendLoadStatistic beStat = clusterStat.getBackendLoadStatistic(tabletCtx.getDestBackendId());
                if (beStat == null) {
                    throw new SchedException(SchedException.Status.UNRECOVERABLE, "dest be statistic not exist");
                }

                List<RootPathLoadStatistic> pathStats = beStat.getPathStatistics(medium);
                for (RootPathLoadStatistic pathStat : pathStats) {
                    if (pathStat.getDiskState() != DiskInfo.DiskState.ONLINE) {
                        continue;
                    }
                    if (pathStat.getCapacityB() <= 0) {
                        continue;
                    }

                    long totalCapacity = pathStat.getCapacityB();
                    long totalUsedCapacity = pathStat.getUsedCapacityB();
                    if (pathStat.getPathHash() == tabletCtx.getSrcPathHash()) {
                        totalUsedCapacity -= replicaSize;
                    } else if (pathStat.getPathHash() == tabletCtx.getDestPathHash()) {
                        totalUsedCapacity += replicaSize;
                    } else {
                        continue;
                    }

                    double usedPercent = (double) totalUsedCapacity / totalCapacity;
                    if (DiskInfo.exceedLimit(totalCapacity - totalUsedCapacity,
                            totalCapacity, false)) {
                        throw new SchedException(SchedException.Status.UNRECOVERABLE,
                                "be disk used exceed limit, isLocalBalance: true");
                    }

                    if (usedPercent > maxUsedPercent) {
                        maxUsedPercent = usedPercent;
                    }
                    if (usedPercent < minUsedPercent) {
                        minUsedPercent = usedPercent;
                    }
                }
            } else {
                for (BackendLoadStatistic beStat : clusterStat.getAllBackendLoadStatistic()) {
                    if (beStat.getTotalCapacityB(medium) <= 0) {
                        continue;
                    }

                    long totalCapacity = beStat.getTotalCapacityB(medium);
                    long totalUsedCapacity = beStat.getTotalUsedCapacityB(medium);
                    if (beStat.getBeId() == tabletCtx.getSrcBackendId()) {
                        totalUsedCapacity -= replicaSize;
                    } else if (beStat.getBeId() == tabletCtx.getDestBackendId()) {
                        totalUsedCapacity += replicaSize;
                    } else {
                        continue;
                    }

                    double usedPercent = (double) totalUsedCapacity / totalCapacity;
                    if (DiskInfo.exceedLimit(totalCapacity - totalUsedCapacity,
                            totalCapacity, false)) {
                        throw new SchedException(SchedException.Status.UNRECOVERABLE, "be disk used exceed limit");
                    }

                    if (usedPercent > maxUsedPercent) {
                        maxUsedPercent = usedPercent;
                    }
                    if (usedPercent < minUsedPercent) {
                        minUsedPercent = usedPercent;
                    }
                }
            }
            if ((maxUsedPercent > Config.tablet_sched_balance_load_disk_safe_threshold) &&
                    ((maxUsedPercent - minUsedPercent) > Config.tablet_sched_balance_load_score_threshold)) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "disk balance will be broken");
            }

            long dbId = tabletCtx.getDbId();
            long tableId = tabletCtx.getTblId();
            long physicalPartitionId = tabletCtx.getPhysicalPartitionId();
            long indexId = tabletCtx.getIndexId();
            long srcPathHash = -1;
            long destPathHash = -1;
            if (isLocalBalance) {
                srcPathHash = tabletCtx.getSrcPathHash();
                destPathHash = tabletCtx.getDestPathHash();
            }
            int tabletNumOnSrc =
                    getPartitionTabletNumOnBePath(dbId, tableId, physicalPartitionId, indexId, tabletCtx.getSrcBackendId(),
                            srcPathHash);
            int tabletNumOnDest =
                    getPartitionTabletNumOnBePath(dbId, tableId, physicalPartitionId, indexId, tabletCtx.getDestBackendId(),
                            destPathHash);
            if (tabletNumOnSrc - tabletNumOnDest <= 1) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE,
                        "can not make tablet distribution balance better");
            }
        }

        checkAndUseWorkingSlots(tabletCtx.getSrcBackendId(), tabletCtx.getSrcPathHash(), backendsWorkingSlots);
        tabletCtx.setSrcPathResourceHold();
        checkAndUseWorkingSlots(tabletCtx.getDestBackendId(), tabletCtx.getDestPathHash(), backendsWorkingSlots);
        tabletCtx.setDestPathResourceHold();

        // NOTICE:
        // local balance in the same backend should not need set this.
        // otherwise the tablet will be deleted.
        if (!isLocalBalance) {
            setCachedReplicaId(tabletCtx.getTabletId(), tabletCtx.getSrcReplica().getId());
        }
    }

    @Override
    public Long getToDeleteReplicaId(Long tabletId) {
        Long replicaId = cachedReplicaId.remove(tabletId);
        return replicaId == null ? -1L : replicaId;
    }

    private void setCachedReplicaId(Long tabletId, Long replicaId) {
        cachedReplicaId.put(tabletId, replicaId);
    }

    private void checkAndUseWorkingSlots(long beId, long pathHash,
                                         Map<Long, TabletScheduler.PathSlot> backendsWorkingSlots)
            throws SchedException {
        TabletScheduler.PathSlot srcBePathSlot = backendsWorkingSlots.get(beId);
        if (srcBePathSlot == null) {
            throw new SchedException(SchedException.Status.UNRECOVERABLE, "working slots not exist for be: " + beId);
        }
        if (srcBePathSlot.takeSlot(pathHash) == -1) {
            throw new SchedException(SchedException.Status.SCHEDULE_RETRY, "path busy, wait for next round");
        }
    }

    /**
     * Disk is balanced if:
     * 1. max used percent smaller than Config.balance_load_disk_safe_threshold
     * or
     * 2. difference between max used percent and min used percent smaller than Config.balance_load_score_threshold
     */
    private boolean isDiskBalanced(double maxUsedPercent, double minUsedPercent) {
        return maxUsedPercent < Config.tablet_sched_balance_load_disk_safe_threshold ||
                (maxUsedPercent - minUsedPercent) < Config.tablet_sched_balance_load_score_threshold;
    }

    /**
     * Cluster disk is balanced if disk usage on all backends in the cluster is balanced.
     * Disk used percent is based on all disks on each backend.
     */
    private boolean isClusterDiskBalanced(ClusterLoadStatistic clusterStat, TStorageMedium medium) {
        List<BackendLoadStatistic> beStats = getValidBeStats(clusterStat, medium);
        double maxUsedPercent = Double.MIN_VALUE;
        double minUsedPercent = Double.MAX_VALUE;
        for (BackendLoadStatistic beStat : beStats) {
            double usedPercent = beStat.getUsedPercent(medium);
            if (usedPercent > maxUsedPercent) {
                maxUsedPercent = usedPercent;
            }
            if (usedPercent < minUsedPercent) {
                minUsedPercent = usedPercent;
            }
        }

        return isDiskBalanced(maxUsedPercent, minUsedPercent);
    }

    /**
     * Backend disk is balanced if all disk usage on each backend is balanced.
     * Disk used percent is based on each disk in single backend.
     */
    private boolean isBackendDiskBalanced(ClusterLoadStatistic clusterStat, TStorageMedium medium) {
        List<BackendLoadStatistic> beStats = getValidBeStats(clusterStat, medium);
        for (BackendLoadStatistic beStat : beStats) {
            Pair<Double, Double> maxMinUsedPercent = beStat.getMaxMinPathUsedPercent(medium);
            if (maxMinUsedPercent == null) {
                continue;
            }

            if (!isDiskBalanced(maxMinUsedPercent.first, maxMinUsedPercent.second)) {
                return false;
            }
        }

        return true;
    }

    /**
     * 1. calculate average used percent for all BE as avgUsedPercent
     * 2. divide BE into two group: one is higher than the avgUsedPercent, another is lower than the avgUsedPercent.
     * 3. Sort BE in high group by usedPercent in desc order, and BE in low group by usedPercent in asc order.
     * 4. for every BE in high group the max selected tablet is: highGroupThreshold =
     * (Config.tablet_sched_max_balancing_tablets + highGroup.size() - 1) / highGroup.size()
     * the max select tablet for backend in low group is: lowGroupThreshold =
     * (Config.tablet_sched_max_balancing_tablets + lowGroup.size() - 1) / lowGroup.size();
     * 5. Choose tablet to migrate from high group to low group.
     * 1) init the high group index(h) and low group index(l) to 0;
     * 2) Iterate the tablet in high_group(h) at the granularity of partitions. There are two iterations of partitions,
     * the first will make the tablet distribution better, the second will destroy the tablet distribution balance.
     * there are some limitations to choose tablet:
     * 1) it won't make the tablet distribution worse(for the first iteration of partitions).
     * 2) only choose the tablet on the high load path of high_group(h).
     * 3) there is no tablet located on the same host of low_group(l).
     * 4) after migration, the usedPercent of high_group(h) cannot be lower than avgUsedPercent,
     * and the usedPercent of low_group(l) cannot be higher than avgUsedPercent.
     * 5) the tablet must be healthy.
     * 3) if one tablet is chosen:
     * if the number of tablets selected in high_group(h) is bigger than highGroupThreshold, remove h from high group. go
     * to the next BE in the high group.
     * if the number of tablets selected in low_group(h) is bigger than lowGroupThreshold, remove l from high group. go
     * to the next BE in the low group.
     * if the number of tablets selected will break the tablet distribution balance for the first iteration or make the
     * skew worse for the second iteration, got to the next partition.
     * 4) After traverse all tablets, if neither the number of tablets selected in high load BE
     * nor low load BE exceed the limit, change the group index in succession.
     * 5) repeat 2), 3), 4) until there isn't any BE in high group.
     */
    private List<TabletSchedCtx> balanceClusterDisk(ClusterLoadStatistic clusterStat,
                                                    TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();

        List<BackendLoadStatistic> beStats = getValidBeStats(clusterStat, medium);
        if (beStats.size() <= 1) {
            return alternativeTablets;
        }

        double avgUsedPercent = beStats.stream().mapToDouble(be -> (be.getUsedPercent(medium))).sum() / beStats.size();

        LOG.debug("get backend stats for cluster disk balance. medium: {}, avgUsedPercent: {}, be stats: {}", medium,
                avgUsedPercent, beStats);

        // cache selected tablets to avoid select same tablet
        Set<Long> selectedTablets = Sets.newHashSet();
        // aliveBeIds to check tablet health
        List<Long> aliveBeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
        Map<String, List<Long>> hostGroups = getHostGroups(aliveBeIds);
        Map<Long, Integer> partitionReplicaCnt = getPartitionReplicaCnt();

        // divide BE into highGroup and lowGroup
        ArrayList<BackendLoadStatistic> highGroup = new ArrayList<>();
        ArrayList<BackendLoadStatistic> lowGroup = new ArrayList<>();
        for (BackendLoadStatistic beStat : beStats) {
            if (beStat.getUsedPercent(medium) > avgUsedPercent) {
                highGroup.add(beStat);
            } else {
                lowGroup.add(beStat);
            }
        }
        if (highGroup.isEmpty() || lowGroup.isEmpty()) {
            return alternativeTablets;
        }

        // sort highGroup in asc order, lowGroup in desc order;
        highGroup.sort(new BackendLoadStatistic.BeStatComparatorForUsedPercent(medium, false));
        lowGroup.sort(new BackendLoadStatistic.BeStatComparatorForUsedPercent(medium));

        Map<Long, BackendBalanceState> backendBalanceStates = new HashMap<>();
        int maxSearchTimes = highGroup.size() + lowGroup.size();
        int searchTimes = 0;
        int h = 0;
        int l = 0;
        int highGroupThreshold = (Config.tablet_sched_max_balancing_tablets + highGroup.size() - 1) / highGroup.size();
        int lowGroupThreshold = (Config.tablet_sched_max_balancing_tablets + lowGroup.size() - 1) / lowGroup.size();
        OUT:
        while (!highGroup.isEmpty() && !lowGroup.isEmpty()
                && ++searchTimes <= maxSearchTimes) {
            h %= highGroup.size();
            l %= lowGroup.size();
            BackendLoadStatistic hLoadStatistic = highGroup.get(h);
            BackendLoadStatistic lLoadStatistic = lowGroup.get(l);
            // source backend and target backend cannot be on the same host
            Backend hBackend =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(hLoadStatistic.getBeId());
            if (hBackend == null) {
                LOG.warn("backend: {} dose not exist", hLoadStatistic.getBeId());
                highGroup.remove(h);
                continue;
            }
            Backend lBackend =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(lLoadStatistic.getBeId());
            if (lBackend == null) {
                LOG.warn("backend: {} dose not exist", lLoadStatistic.getBeId());
                lowGroup.remove(l);
                continue;
            }
            if (hBackend.getHost().equals(lBackend.getHost())) {
                h++;
                continue;
            }

            BackendBalanceState hState = backendBalanceStates
                    .computeIfAbsent(hBackend.getId(),
                            beId -> getBackendBalanceState(beId,
                                    hLoadStatistic,
                                    medium,
                                    partitionReplicaCnt,
                                    beStats.size(),
                                    true));
            BackendBalanceState lState = backendBalanceStates
                    .computeIfAbsent(lBackend.getId(),
                            beId -> getBackendBalanceState(beId,
                                    lLoadStatistic,
                                    medium,
                                    partitionReplicaCnt,
                                    beStats.size(),
                                    false));

            List<Long> lBeHostGroup = hostGroups.get(lBackend.getHost());

            // tow round:
            // in the first round, we will migrate the tablet that will make tablet distribution better,
            // in the second round, we will migrate the tablet that will break the tablet distribution balance.
            for (int round = 1; round <= 2; round++) {
                PARTITION:
                for (Pair<Long, Long> physicalPartitionAndMaterializedIndexId : hState.sortedPartitions) {
                    List<Long> hPartitionTablets = hState.partitionTablets.get(physicalPartitionAndMaterializedIndexId);
                    List<Long> lPartitionTablets =
                            lState.partitionTablets.computeIfAbsent(physicalPartitionAndMaterializedIndexId,
                                    pmId -> new LinkedList<>());
                    int replicaTotalCnt = partitionReplicaCnt.getOrDefault(physicalPartitionAndMaterializedIndexId.first, 0);
                    int slotOfHighBE = hPartitionTablets.size() - (replicaTotalCnt / beStats.size());
                    int slotOfLowBE = ((replicaTotalCnt + beStats.size() - 1) / beStats.size())
                            - lPartitionTablets.size();
                    int slotCnt = Math.min(slotOfHighBE, slotOfLowBE);
                    // slotCnt <= 0 means hat we will make the tablet balance worse,
                    // ignore this partition for the first round
                    if (round == 1 && slotCnt <= 0) {
                        continue;
                    }

                    int selectedCnt = 0;
                    List<Long> highLoadPathTablets = hState.getTabletsInHighLoadPath(hPartitionTablets);
                    for (long tabletId : highLoadPathTablets) {
                        if (selectedTablets.contains(tabletId)) {
                            continue;
                        }
                        TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
                        if (tabletMeta == null) {
                            continue;
                        }
                        Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                                .getReplica(tabletId, hLoadStatistic.getBeId());
                        if (replica == null || replica.getPathHash() == -1L || replica.getDataSize() <= 0) {
                            continue;
                        }
                        OlapTable olapTable = getOlapTableById(tabletMeta.getDbId(), tabletMeta.getTableId());
                        if (olapTable == null) {
                            continue;
                        }
                        PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(tabletMeta.getPhysicalPartitionId());
                        if (physicalPartition == null) {
                            continue;
                        }

                        if (!olapTable.needSchedule(false)) {
                            continue;
                        }

                        if (isDestBackendLocationMismatch(olapTable, hBackend.getId(), lBackend.getId(),
                                physicalPartition.getParentId(), tabletId)) {
                            continue;
                        }

                        if (isTabletExistsInBackends(tabletId, lBeHostGroup)) {
                            continue;
                        }

                        // check used percent after move
                        double hBEUsedPercent = (double) (hState.usedCapacity - replica.getDataSize())
                                / hLoadStatistic.getTotalCapacityB(medium);
                        double lBEUsedPercent = (double) (lState.usedCapacity + replica.getDataSize())
                                / lLoadStatistic.getTotalCapacityB(medium);
                        if (lBEUsedPercent > avgUsedPercent && lBEUsedPercent - avgUsedPercent > 1e-6) {
                            lowGroup.remove(l);
                            continue OUT;
                        }
                        if (hBEUsedPercent < avgUsedPercent && avgUsedPercent - hBEUsedPercent > 1e-6) {
                            highGroup.remove(h);
                            continue OUT;
                        }

                        // check tablet health state, if unhealthy, won't choose this one
                        if (isTabletUnhealthy(tabletMeta.getDbId(), olapTable, tabletId, tabletMeta, aliveBeIds)) {
                            continue;
                        }

                        // NOTICE: state has been changed, the tablet must be selected
                        hPartitionTablets.remove(tabletId);
                        lPartitionTablets.add(tabletId);
                        hState.tabletSelected++;
                        lState.tabletSelected++;

                        Long destPathHash = lState.getLowestLoadPath();
                        lState.addUsedCapacity(destPathHash, replica.getDataSize());
                        hState.minusUsedCapacity(replica.getPathHash(), replica.getDataSize());

                        TabletSchedCtx schedCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                                tabletMeta.getDbId(), tabletMeta.getTableId(),
                                tabletMeta.getPhysicalPartitionId(), tabletMeta.getIndexId(),
                                tabletId, System.currentTimeMillis());
                        schedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
                        schedCtx.setSrc(replica);
                        schedCtx.setDest(lBackend.getId(), destPathHash);
                        schedCtx.setBalanceType(BalanceType.DISK);
                        selectedTablets.add(tabletId);
                        alternativeTablets.add(schedCtx);

                        selectedCnt++;

                        // number of tablets cloned from high load BE exceeds limit
                        if (hState.tabletSelected >= highGroupThreshold) {
                            highGroup.remove(h);
                            continue OUT;
                        }
                        // number of tablets cloned to low load BE exceeds limit
                        if (lState.tabletSelected >= lowGroupThreshold) {
                            lowGroup.remove(l);
                            continue OUT;
                        }
                        // for round1: the number of selected tablets is bigger than slotCnt,
                        //             selecting tablet from this partition will break the balance of tablet distribution,
                        //             so change to next partition
                        // for round2: In order not to skew the partition too much,
                        //             we only balance one tablet for a partition,
                        //             so change to next partition.
                        if ((round == 1 && selectedCnt >= slotCnt) || round == 2) {
                            continue PARTITION;
                        }
                    }
                }
            }
            // neither the number of tablets select in high load BE nor low load BE exceeds limit,
            // change group index in succession.
            if ((h + l) % 2 == 0) {
                h++;
            } else {
                l++;
            }
        }

        return alternativeTablets;
    }

    /**
     * Backend disk balance is same with cluster disk balance.
     * 1. select unbalanced be and sort be by path min|max used percent skew in desc order.
     * 2. balance each backend in order.
     * 2.1. sort disk according to used percent in asc order: path1, path2, ... path_n
     * 2.2. calculate average used percent for all disks as avgUsedPercent
     * 2.3. init srcPathIndex as n, destPathIndex as 1
     * 2.4. copy tablets from srcPath to destPath until
     * 1) usedPercent of srcPath less than avgUsedPercent, srcPathIndex--
     * 2) usedPercent of destPath more than avgUsedPercent, destPathIndex++
     * 2.5. repeat 2.4, until srcPathIndex <= destPathIndex
     * <p>
     * we prefer to choose tablets in partition that numOfTablet(srcPath) is more than numOfTablet(destPath)
     */
    private List<TabletSchedCtx> balanceBackendDisk(ClusterLoadStatistic clusterStat,
                                                    TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();

        // select unbalanced be
        List<BackendLoadStatistic> beStats = getValidBeStats(clusterStat, medium);
        if (beStats.isEmpty()) {
            return alternativeTablets;
        }

        List<BackendLoadStatistic> unbalancedBeStats = Lists.newArrayList();
        for (BackendLoadStatistic beStat : beStats) {
            Pair<Double, Double> maxMinUsedPercent = beStat.getMaxMinPathUsedPercent(medium);
            if (maxMinUsedPercent == null) {
                continue;
            }

            if (!isDiskBalanced(maxMinUsedPercent.first, maxMinUsedPercent.second)) {
                unbalancedBeStats.add(beStat);
            }
        }

        // sort be by path min|max used percent skew in desc order
        unbalancedBeStats.sort(new BackendLoadStatistic.BeStatComparatorForPathUsedPercentSkew(medium));
        LOG.debug("select unbalanced backends for backend disk balance. medium: {}, be stats: {}", medium,
                unbalancedBeStats);

        for (BackendLoadStatistic beStat : unbalancedBeStats) {
            long beId = beStat.getBeId();
            if (!GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkBackendAvailable(beId)) {
                continue;
            }

            List<RootPathLoadStatistic> pathStats = getValidBePathStats(beStat, medium);
            if (pathStats.size() <= 1) {
                continue;
            }

            double avgUsedPercent =
                    pathStats.stream().mapToDouble(RootPathLoadStatistic::getUsedPercent).sum() / pathStats.size();

            // sort disk by used percent in asc order
            Collections.sort(pathStats);
            LOG.debug(
                    "get backend path stats for backend disk balance. medium: {}, be id: {}, avgUsedPercent: {}, path stats: {}",
                    medium, beId, avgUsedPercent, pathStats);

            balanceBackendDisk(medium, avgUsedPercent, pathStats, beId, beStats.size(),
                    alternativeTablets);
            if (alternativeTablets.size() >= Config.tablet_sched_max_balancing_tablets) {
                break;
            }
        }
        return alternativeTablets;
    }

    private OlapTable getOlapTableById(long dbId, long tblId) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return null;
        }

        Locker locker = new Locker();
        try {
            locker.lockDatabase(db.getId(), LockType.READ);
            return (OlapTable) globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tblId);
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    private void balanceBackendDisk(TStorageMedium medium, double avgUsedPercent,
                                    List<RootPathLoadStatistic> pathStats, long beId, int beNum,
                                    List<TabletSchedCtx> alternativeTablets) {
        Preconditions.checkArgument(pathStats != null && pathStats.size() > 1 && beId > -1 && beNum > 0);

        // aliveBeIds to check tablet health
        List<Long> aliveBeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);

        // src|dest path stat
        int srcPathIndex = pathStats.size() - 1;
        int destPathIndex = 0;
        long srcPathUsedCap = pathStats.get(srcPathIndex).getUsedCapacityB();
        long destPathUsedCap = pathStats.get(destPathIndex).getUsedCapacityB();
        long srcPathHash = pathStats.get(srcPathIndex).getPathHash();
        long destPathHash = pathStats.get(destPathIndex).getPathHash();

        // (partition, index) => tabletIds
        Map<Pair<Long, Long>, Set<Long>> srcPathPartitionTablets = getPartitionTablets(beId, medium, srcPathHash, true);
        Map<Pair<Long, Long>, Set<Long>> destPathPartitionTablets = getPartitionTablets(beId, medium, destPathHash, true);
        Map<Pair<Long, Long>, PartitionStat> partitionStats = getPartitionStats(medium, true, null, null);

        boolean srcChanged = false;
        boolean destChanged = false;
        OUT:
        while (srcPathIndex > destPathIndex) {
            RootPathLoadStatistic srcPathStat = pathStats.get(srcPathIndex);
            RootPathLoadStatistic destPathStat = pathStats.get(destPathIndex);
            if (srcChanged) {
                srcPathUsedCap = srcPathStat.getUsedCapacityB();
                srcPathHash = srcPathStat.getPathHash();
                srcPathPartitionTablets = getPartitionTablets(beId, medium, srcPathHash, true);
                srcChanged = false;
            }
            if (destChanged) {
                destPathUsedCap = destPathStat.getUsedCapacityB();
                destPathHash = destPathStat.getPathHash();
                destPathPartitionTablets = getPartitionTablets(beId, medium, destPathHash, true);
                destChanged = false;
            }

            int totalPaths = beNum * pathStats.size();
            List<Long> tablets =
                    getSourceTablets(partitionStats, srcPathPartitionTablets, destPathPartitionTablets, totalPaths);

            long srcPathTotalCap = srcPathStat.getCapacityB();
            long destPathTotalCap = destPathStat.getCapacityB();
            for (Long tabletId : tablets) {
                TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    continue;
                }
                Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, beId);
                if (replica == null || replica.getPathHash() == -1L) {
                    continue;
                }

                // check used percent after move
                double destUsedPercent = (double) (destPathUsedCap + replica.getDataSize()) / destPathTotalCap;
                double srcUsedPercent = (double) (srcPathUsedCap - replica.getDataSize()) / srcPathTotalCap;
                if (Math.abs(destUsedPercent - avgUsedPercent) > 1e-6 && (destUsedPercent > avgUsedPercent)) {
                    destPathIndex++;
                    destChanged = true;
                    continue OUT;
                }
                if (Math.abs(srcUsedPercent - avgUsedPercent) > 1e-6 && (srcUsedPercent < avgUsedPercent)) {
                    srcPathIndex--;
                    srcChanged = true;
                    continue OUT;
                }

                OlapTable olapTable = getOlapTableById(tabletMeta.getDbId(), tabletMeta.getTableId());
                if (olapTable == null) {
                    continue;
                }
                // check tablet healthy
                if (isTabletUnhealthy(tabletMeta.getDbId(), olapTable, tabletId, tabletMeta, aliveBeIds)) {
                    continue;
                }

                // NOTICE: state has been changed, the tablet must be selected
                destPathUsedCap += replica.getDataSize();
                srcPathUsedCap -= replica.getDataSize();
                Pair<Long, Long> p = Pair.create(tabletMeta.getPhysicalPartitionId(), tabletMeta.getIndexId());
                // p: partition <physicalPartitionId, indexId>
                // k: partition same to p
                srcPathPartitionTablets.compute(p, (k, pTablets) -> {
                    if (pTablets != null) {
                        pTablets.remove(tabletId);
                    }
                    return pTablets;
                });
                destPathPartitionTablets.compute(p, (k, pTablets) -> {
                    if (pTablets != null) {
                        pTablets.add(tabletId);
                        return pTablets;
                    }
                    return Sets.newHashSet(tabletId);
                });

                TabletSchedCtx schedCtx =
                        new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, tabletMeta.getDbId(),
                                tabletMeta.getTableId(),
                                tabletMeta.getPhysicalPartitionId(),
                                tabletMeta.getIndexId(), tabletId, System.currentTimeMillis());
                schedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
                schedCtx.setSrc(replica);
                schedCtx.setDest(beId, destPathHash);
                schedCtx.setBalanceType(BalanceType.DISK);
                alternativeTablets.add(schedCtx);

                if (alternativeTablets.size() >= Config.tablet_sched_max_balancing_tablets) {
                    return;
                }
            }

            // code reach here means that all tablets have moved to destPath,
            // but srcPath and destPath both have not reached the average.
            // it is not easy to judge whether src or dest should be retained for next round, just random
            if ((int) (Math.random() * 100) % 2 == 0) {
                srcPathIndex--;
                srcChanged = true;
            } else {
                destPathIndex++;
                destChanged = true;
            }
        }
    }

    /**
     * Get source tablets from src be|path for balance to dest be|path.
     * Unbalanced tablets first.
     * <p>
     * totalDests: be num for cluster disk balance or (be num * path num) for backend disk balance
     */
    private List<Long> getSourceTablets(Map<Pair<Long, Long>, PartitionStat> partitionStats,
                                        Map<Pair<Long, Long>, Set<Long>> srcPartitionTablets,
                                        Map<Pair<Long, Long>, Set<Long>> destPartitionTablets,
                                        int totalDests) {
        // we store tablets that can make tablet distribution balance better to balancedTablets,
        // and those make tablet distribution balance worse to unbalancedTablets.
        List<Long> balancedTablets = Lists.newArrayList();
        List<Long> unbalancedTablets = Lists.newArrayList();
        for (Map.Entry<Pair<Long, Long>, Set<Long>> partitionTablets : srcPartitionTablets.entrySet()) {
            PartitionStat pStat = partitionStats.get(partitionTablets.getKey());
            if (pStat == null) {
                continue;
            }

            Set<Long> destTablets = destPartitionTablets.getOrDefault(partitionTablets.getKey(), Sets.newHashSet());
            Set<Long> srcTablets = partitionTablets.getValue();
            int avgNum = pStat.replicaNum / totalDests;
            // num of tablets that make tablet distribution balance better
            // avgNum - destTablets.size() is max tablets num moved to that will preserve dest be|path tablet distribution balance
            // srcTablets.size() - avgNum is max tablets num moved from that will preserve src be|path tablet distribution balance,
            // so we take the smallest value between the two as balanceNum
            int balanceNum = Math.min(avgNum - destTablets.size(), srcTablets.size() - avgNum);
            for (long tabletId : srcTablets) {
                if (balanceNum > 0) {
                    balancedTablets.add(tabletId);
                    balanceNum--;
                } else {
                    unbalancedTablets.add(tabletId);
                }
            }
        }

        // shuffle to avoid partition heavily skewed
        Collections.shuffle(balancedTablets);
        Collections.shuffle(unbalancedTablets);
        List<Long> tablets = Lists.newArrayList(balancedTablets);
        tablets.addAll(unbalancedTablets);
        return tablets;
    }

    /**
     * get backend which is alive and has medium of disk
     */
    private List<BackendLoadStatistic> getValidBeStats(ClusterLoadStatistic clusterStat, TStorageMedium medium) {
        List<BackendLoadStatistic> validBeStats = Lists.newArrayList();
        for (BackendLoadStatistic beStat : clusterStat.getAllBackendLoadStatistic()) {
            if (GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                    .checkBackendAvailable(beStat.getBeId()) && beStat.getTotalCapacityB(medium) > 0) {
                validBeStats.add(beStat);
            }
        }
        return validBeStats;
    }

    public List<RootPathLoadStatistic> getValidBePathStats(BackendLoadStatistic beStat, TStorageMedium medium) {
        List<RootPathLoadStatistic> validPathStats = Lists.newArrayList();
        for (RootPathLoadStatistic pathStat : beStat.getPathStatistics(medium)) {
            if (pathStat.getDiskState() == DiskInfo.DiskState.ONLINE) {
                validPathStats.add(pathStat);
            }
        }
        return validPathStats;
    }

    // group backends by hostname
    private Map<String, List<Long>> getHostGroups(List<Long> backendIds) {
        Map<String, List<Long>> hostGroups = Maps.newHashMap();
        for (Long backendId : backendIds) {
            Backend backend = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(backendId);
            if (backend == null) {
                continue;
            }

            hostGroups.compute(backend.getHost(), (host, backends) -> {
                if (backends == null) {
                    return Lists.newArrayList(backendId);
                } else {
                    backends.add(backendId);
                    return backends;
                }
            });
        }

        return hostGroups;
    }

    private boolean isTabletExistsInBackends(Long tabletId, List<Long> backends) {
        if (backends == null || backends.isEmpty()) {
            return false;
        }

        for (Long backendId : backends) {
            Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, backendId);
            if (replica != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return map : (physical partition id, index) => tablets
     */
    private Map<Pair<Long, Long>, Set<Long>> getPartitionTablets(long beId, TStorageMedium medium, long pathHash,
                                                                 boolean isLocalBalance) {
        Map<Pair<Long, Long>, Set<Long>> partitionTablets = Maps.newHashMap();
        List<Long> tabletIds =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendIdAndStorageMedium(beId, medium);
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
            }

            if (tabletMeta.isLakeTablet()) {
                // replicas are managed by StarOS and cloud storage.
                continue;
            }

            if (pathHash != -1) {
                Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, beId);
                if (replica == null || replica.getPathHash() != pathHash) {
                    continue;
                }
            }

            OlapTable olapTable = getOlapTableById(tabletMeta.getDbId(), tabletMeta.getTableId());
            if (olapTable != null && !olapTable.needSchedule(isLocalBalance)) {
                continue;
            }

            Pair<Long, Long> key = new Pair<>(tabletMeta.getPhysicalPartitionId(), tabletMeta.getIndexId());
            partitionTablets.computeIfAbsent(key, k -> Sets.newHashSet()).add(tabletId);
        }
        return partitionTablets;
    }

    private Map<Pair<Long, Long>, Double> getPartitionAvgReplicaSize(long beId,
                                                                     Map<Pair<Long, Long>, Set<Long>> partitionTablets) {
        Map<Pair<Long, Long>, Double> result = new HashMap<>();
        for (Map.Entry<Pair<Long, Long>, Set<Long>> entry : partitionTablets.entrySet()) {
            long totalSize = 0;
            for (Long tabletId : entry.getValue()) {
                Replica replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, beId);
                if (replica != null) {
                    totalSize += replica.getDataSize();
                }
            }
            result.put(entry.getKey(), (double) totalSize / (!entry.getValue().isEmpty() ? entry.getValue().size() : 1));
        }
        return result;
    }

    private int getPartitionTabletNumOnBePath(long dbId, long tableId, long physicalPartitionId, long indexId, long beId,
                                              long pathHash) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return 0;
        }

        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tableId);
            if (table == null) {
                return 0;
            }

            PhysicalPartition physicalPartition = globalStateMgr.getLocalMetastore()
                    .getPhysicalPartitionIncludeRecycleBin(table, physicalPartitionId);
            if (physicalPartition == null) {
                return 0;
            }

            int cnt = 0;
            MaterializedIndex index = physicalPartition.getIndex(indexId);
            if (index == null) {
                return 0;
            }

            for (Tablet tablet : index.getTablets()) {
                List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                if (replicas == null) {
                    continue;
                }

                for (Replica replica : replicas) {
                    if (replica.getState() == ReplicaState.NORMAL && replica.getBackendId() == beId) {
                        if (pathHash == -1 || replica.getPathHash() == pathHash) {
                            cnt++;
                        }
                    }
                }
            }
            return cnt;
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    /**
     * balance cluster tablet should preserve disk balance
     * 1. for every partition, calculate the distribution skew, skew is (max tablet number on be) - (min tablet number on be)
     * 2. sort partition by skew in desc order
     * 3. for every partition, sort be by tablet number
     * 4. try to copy one tablet from maxTabletsNum be to other be, minTabletsNum be first
     * 5. repeat 3 and 4, until no tablet can copy
     */
    private List<TabletSchedCtx> balanceClusterTablet(ClusterLoadStatistic clusterStat,
                                                      TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        List<BackendLoadStatistic> beStats = getValidBeStats(clusterStat, medium);
        if (beStats.size() <= 1) {
            return alternativeTablets;
        }

        // beId => (paths, index) , low or mid be disks to hold moved tablets
        Map<Long, Pair<List<Long>, Integer>> beDisks = Maps.newHashMap();
        for (BackendLoadStatistic beStat : beStats) {
            List<Long> pathHashList = Lists.newArrayList();
            for (RootPathLoadStatistic pathStat : beStat.getPathStatistics()) {
                if (pathStat.getStorageMedium() == medium
                        && pathStat.getDiskState() == DiskInfo.DiskState.ONLINE
                        && (pathStat.getClazz() == Classification.LOW || pathStat.getClazz() == Classification.MID)) {
                    pathHashList.add(pathStat.getPathHash());
                }
            }
            beDisks.put(beStat.getBeId(), new Pair<>(pathHashList, 0));
        }
        LOG.debug("get backend stats for cluster tablet distribution balance. medium: {}, be stats: {}, be disks: {}",
                medium, beStats, beDisks);

        balanceTablet(medium, alternativeTablets, false, beStats, beDisks, null, -1);
        return alternativeTablets;
    }

    /**
     * balance backend tablet is same with cluster tablet balance
     * for every backend:
     * 1. for every partition, calculate the distribution skew, skew is (max tablet number on path) - (min tablet number on path)
     * 2. sort partition by skew in desc order
     * 3. for every partition, sort disk path by tablet number
     * 4. try to copy one tablet from maxTabletsNum path to other path, minTabletsNum path first
     * 5. repeat 3 and 4, until no tablet can copy
     */
    private List<TabletSchedCtx> balanceBackendTablet(ClusterLoadStatistic clusterStat,
                                                      TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        for (BackendLoadStatistic beStat : getValidBeStats(clusterStat, medium)) {
            long beId = beStat.getBeId();
            if (!GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().checkBackendAvailable(beId)) {
                continue;
            }

            List<RootPathLoadStatistic> pathStats = getValidBePathStats(beStat, medium);
            LOG.debug(
                    "get backend path stats for backend tablet distribution balance. medium: {}, be id: {}, path stats: {}",
                    medium, beId, pathStats);
            if (pathStats.size() <= 1) {
                continue;
            }

            balanceTablet(medium, alternativeTablets, true, null, null, pathStats, beId);
            if (alternativeTablets.size() >= Config.tablet_sched_max_balancing_tablets) {
                break;
            }
        }
        return alternativeTablets;
    }

    /**
     * Base balance tablet for cluster tablet balance and backend tablet balance.
     * cluster balance args: beStats, beDisks, isLocalBalance is false.
     * backend balance args: pathStats, beId, isLocalBalance is true.
     */
    private void balanceTablet(TStorageMedium medium,
                               List<TabletSchedCtx> alternativeTablets,
                               boolean isLocalBalance,
                               List<BackendLoadStatistic> beStats,
                               Map<Long, Pair<List<Long>, Integer>> beDisks,
                               List<RootPathLoadStatistic> pathStats,
                               long beId) {
        if (!isLocalBalance) {
            Preconditions.checkArgument(beStats != null && beStats.size() > 1);
        } else {
            Preconditions.checkArgument(pathStats != null && pathStats.size() > 1 && beId > -1);
        }

        // beId|pathHash => (totalCapacity, totalUsedCapacity)
        Map<Long, Pair<Long, Long>> diskCapMap = Maps.newHashMap();
        List<Long> beIds = null;
        List<Long> paths = null;
        Map<Pair<Long, Long>, PartitionStat> partitionStats;
        if (!isLocalBalance) {
            for (BackendLoadStatistic beStat : beStats) {
                diskCapMap.put(beStat.getBeId(),
                        new Pair<>(beStat.getTotalCapacityB(medium), beStat.getTotalUsedCapacityB(medium)));
            }
            beIds = Lists.newArrayList(diskCapMap.keySet());
            partitionStats = getPartitionStats(medium, false, beIds, null);
        } else {
            for (RootPathLoadStatistic pathStat : pathStats) {
                if (pathStat.getDiskState() == DiskInfo.DiskState.ONLINE) {
                    diskCapMap.put(pathStat.getPathHash(),
                            Pair.create(pathStat.getCapacityB(), pathStat.getUsedCapacityB()));
                }
            }
            paths = Lists.newArrayList(diskCapMap.keySet());
            partitionStats = getPartitionStats(medium, true, null, Pair.create(beId, paths));
        }

        List<Pair<Long, Long>> partitions = sortPartitionBySkew(partitionStats);

        DiskBalanceChecker diskBalanceChecker = new DiskBalanceChecker(diskCapMap);
        diskBalanceChecker.init();
        Set<Long> selectedTablets = Sets.newHashSet();
        List<Long> aliveBeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
        Map<String, List<Long>> hostGroups = getHostGroups(aliveBeIds);
        for (Pair<Long, Long> partition : partitions) {
            PartitionStat pStat = partitionStats.get(partition);
            // skew <= 1 means partition is balanced
            // break all partitions because they are sorted by skew in desc order.
            if (pStat.skew <= 1) {
                break;
            }

            // List<Pair<beId|pathHash, Set<tabletId>>>
            List<Pair<Long, Set<Long>>> tablets;
            if (!isLocalBalance) {
                tablets = getPartitionTablets(pStat.dbId, pStat.tableId,
                        partition.first, partition.second, beIds, null);
            } else {
                tablets = getPartitionTablets(pStat.dbId, pStat.tableId, partition.first, partition.second, null,
                        Pair.create(beId, paths));
            }

            // partition may be dropped or materializedIndex may be replaced.
            if (tablets.size() <= 1) {
                continue;
            }
            boolean tabletFound;
            do {
                tabletFound = false;
                // sort be by tablets num in desc order, and used percent
                // in desc order for bes|paths with same tablets num
                tablets.sort((t1, t2) -> {
                    if (t1.second.size() != t2.second.size()) {
                        return t2.second.size() - t1.second.size();
                    } else {
                        double diff = diskBalanceChecker.getDiskUsedPercent(t2.first) -
                                diskBalanceChecker.getDiskUsedPercent(t1.first);
                        if (Math.abs(diff) < 1e-6) {
                            return 0;
                        } else if (diff > 0) {
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                });

                // try to copy one tablet from maxTabletsNum be to other be|path, minTabletsNum be|path first.
                Pair<Long, Set<Long>> srcTablets = tablets.get(0);
                for (int i = tablets.size() - 1; i > 0; i--) {
                    Pair<Long, Set<Long>> destTablets = tablets.get(i);

                    // partition is balanced
                    if (srcTablets.second.size() - destTablets.second.size() <= 1) {
                        break;
                    }

                    TabletSchedCtx schedCtx;
                    if (!isLocalBalance) {
                        Backend destBackend =
                                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackend(destTablets.first);
                        if (destBackend == null) {
                            continue;
                        }
                        schedCtx = tryToBalanceTablet(srcTablets, destTablets, diskBalanceChecker,
                                selectedTablets, aliveBeIds, false,
                                hostGroups.get(destBackend.getHost()), -1, pStat.replicationFactor);
                    } else {
                        schedCtx = tryToBalanceTablet(srcTablets, destTablets, diskBalanceChecker,
                                selectedTablets, aliveBeIds, true,
                                null, beId, pStat.replicationFactor);
                    }

                    if (schedCtx != null) {
                        // NOTICE: state has been changed, the tablet must be selected
                        // set dest beId and pathHash
                        if (!isLocalBalance) {
                            // round-robin to select dest be path
                            Pair<List<Long>, Integer> destPaths = beDisks.get(destTablets.first);
                            Long pathHash = destPaths.first.get(destPaths.second);
                            destPaths.second = (destPaths.second + 1) % destPaths.first.size();

                            schedCtx.setDest(destTablets.first, pathHash);
                        } else {
                            schedCtx.setDest(beId, destTablets.first);
                        }
                        alternativeTablets.add(schedCtx);
                        if (alternativeTablets.size() >= Config.tablet_sched_max_balancing_tablets) {
                            return;
                        }
                        tabletFound = true;
                        break;
                    }
                }
            } while (tabletFound);
        }
    }

    @NotNull
    private static List<Pair<Long, Long>> sortPartitionBySkew(Map<Pair<Long, Long>, PartitionStat> partitionStats) {
        List<Pair<Long, Long>> partitions = new ArrayList<>(partitionStats.keySet());
        // sort all partition by distribution skew in desc order, skew is (max tablet number on be|path) - (min tablet number on be|path)
        partitions.sort((o1, o2) -> {
            PartitionStat pStat1 = partitionStats.get(o1);
            PartitionStat pStat2 = partitionStats.get(o2);
            return pStat2.skew - pStat1.skew;
        });
        return partitions;
    }

    private boolean isDestBackendLocationMismatch(Multimap<String, String> requiredLocation,
                                                  long srcBackendId,
                                                  long destBackendId,
                                                  int replicationFactor,
                                                  long tabletId) {
        if (requiredLocation == null) {
            return false;
        }

        List<List<Long>> locBackendIdList = new ArrayList<>();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> availableBackends = Lists.newArrayList();
        availableBackends.addAll(systemInfoService.getAvailableBackends());
        int locBackendWithDiffHostLocNum = NodeSelector.getLocationMatchedBackendIdList(
                locBackendIdList, availableBackends, requiredLocation, systemInfoService);

        Pair<String, String> srcBackendLocKV;
        Backend srcBackend = systemInfoService.getBackend(srcBackendId);
        if (srcBackend == null) {
            return true;
        } else {
            srcBackendLocKV = srcBackend.getSingleLevelLocationKV();
        }
        Pair<String, String> destBackendLocKV;
        Backend destBackend = systemInfoService.getBackend(destBackendId);
        if (destBackend == null) {
            return true;
        } else {
            destBackendLocKV = destBackend.getSingleLevelLocationKV();
        }

        List<Long> flattenMatchedBackendIds = locBackendIdList.stream().flatMap(List::stream)
                .collect(Collectors.toList());
        boolean destBackendLocMatched = flattenMatchedBackendIds.contains(destBackendId);

        // Get all the location of replicas of this tablet.
        Set<Pair<String, String>> replicasLocKVs = new HashSet<>();
        for (Replica replica : GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                .getReplicasByTabletId(tabletId)) {
            Backend backend = systemInfoService.getBackend(replica.getBackendId());
            if (backend == null) {
                continue;
            }
            replicasLocKVs.add(backend.getSingleLevelLocationKV());
        }

        // If we have enough number of backends to match the location requirement of tablet,
        // but the current destination candidate backend cannot match the location requirement,
        // we will not move this tablet to dest.
        return locBackendWithDiffHostLocNum >= replicationFactor &&
                // In the following situations, dest backend should be considered a matched candidate to move replica to,
                //   1. src backend matched, and dest backend has the same location with src backend
                //   2. src backend matched, and dest backend has different loc with src and other replicas' location
                //   3. src backend unmatched, dest backend matched and has different loc with other replicas' location
                // The latter 2 cases can be merged into one check condition: `!replicasLocKVs.contains(destBackendLocKV)`.
                !(destBackendLocMatched &&
                        (Objects.equals(srcBackendLocKV, destBackendLocKV) ||
                                !replicasLocKVs.contains(destBackendLocKV)));
    }

    private boolean isDestBackendLocationMismatch(OlapTable olapTable,
                                                  long srcBackendId,
                                                  long destBackendId,
                                                  long partitionId,
                                                  long tabletId) {
        short replicationFactor =
                GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getReplicationNumIncludeRecycleBin(olapTable.getPartitionInfo(), partitionId);
        if (replicationFactor == (short) -1) {
            return true;
        }

        return isDestBackendLocationMismatch(olapTable.getLocation(), srcBackendId,
                destBackendId, replicationFactor, tabletId);
    }

    /**
     * cluster tablet balance args:
     * srcTablets: beId => Set<tabletId>
     * destTablets: beId => Set<tabletId>
     * destBackendHostGroup
     * isLocalBalance: false
     * <p>
     * backend tablet balance args:
     * srcTablets: pathHash => Set<tabletId>
     * destTablets: pathHash => Set<tabletId>
     * beId
     * isLocalBalance: true
     */
    private TabletSchedCtx tryToBalanceTablet(Pair<Long, Set<Long>> srcTablets,
                                              Pair<Long, Set<Long>> destTablets,
                                              DiskBalanceChecker diskBalanceChecker,
                                              Set<Long> selectedTablets,
                                              List<Long> aliveBeIds,
                                              boolean isLocalBalance,
                                              List<Long> destBackendHostGroup,
                                              long beId,
                                              int replicationFactor) {
        Preconditions.checkArgument(!isLocalBalance || beId != -1);

        for (Long tabletId : srcTablets.second) {
            if (!isLocalBalance) {
                if (isTabletExistsInBackends(tabletId, destBackendHostGroup)) {
                    continue;
                }
            }

            if (selectedTablets.contains(tabletId)) {
                continue;
            }

            // get and check meta
            TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
            }
            // Won't hold the db lock, dropped db or table will cause this clone task failed, this is acceptable.
            OlapTable olapTable = getOlapTableById(tabletMeta.getDbId(), tabletMeta.getTableId());
            if (olapTable == null) {
                continue;
            }

            if (!isLocalBalance && isDestBackendLocationMismatch(
                    olapTable.getLocation(), srcTablets.first, destTablets.first, replicationFactor, tabletId)) {
                continue;
            }

            Replica replica;
            if (!isLocalBalance) {
                replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, srcTablets.first);
            } else {
                replica = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(tabletId, beId);
            }
            if (replica == null || replica.getPathHash() == -1L) {
                continue;
            }

            if (!diskBalanceChecker.check(srcTablets.first, destTablets.first, replica.getDataSize())) {
                continue;
            }

            if (isTabletUnhealthy(tabletMeta.getDbId(), olapTable, tabletId, tabletMeta, aliveBeIds)) {
                continue;
            }

            TabletSchedCtx schedCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                    tabletMeta.getDbId(), tabletMeta.getTableId(),
                    tabletMeta.getPhysicalPartitionId(),
                    tabletMeta.getIndexId(), tabletId, System.currentTimeMillis());
            schedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
            schedCtx.setBalanceType(BalanceType.TABLET);
            schedCtx.setSrc(replica);

            // update state
            selectedTablets.add(tabletId);
            diskBalanceChecker.moveReplica(srcTablets.first, destTablets.first, replica.getDataSize());
            srcTablets.second.remove(tabletId);
            destTablets.second.add(tabletId);
            return schedCtx;
        }

        return null;
    }

    /**
     * Get beId or pathHash to tablets by physicalPartitionId and indexId.
     * If beIds is not null, return beId => Set<tabletId>.
     * If bePaths is not null, return pathHash => Set<tabletId>.
     */
    private List<Pair<Long, Set<Long>>> getPartitionTablets(Long dbId, Long tableId, Long physicalPartitionId, Long indexId,
                                                            List<Long> beIds, Pair<Long, List<Long>> bePaths) {
        Preconditions.checkArgument(beIds != null || bePaths != null);

        List<Pair<Long, Set<Long>>> result = Lists.newArrayList();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return result;
        }
        Locker locker = new Locker();
        try {
            locker.lockDatabase(db.getId(), LockType.READ);
            OlapTable table = (OlapTable) globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, tableId);
            if (table == null) {
                return result;
            }
            if (!table.needSchedule(beIds == null)) {
                return result;
            }

            if (table.isCloudNativeTableOrMaterializedView()) {
                // replicas are managed by StarOS and cloud storage.
                return result;
            }

            PhysicalPartition physicalPartition = globalStateMgr.getLocalMetastore()
                    .getPhysicalPartitionIncludeRecycleBin(table, physicalPartitionId);
            if (physicalPartition == null) {
                return result;
            }

            MaterializedIndex index = physicalPartition.getIndex(indexId);
            if (index == null) {
                return result;
            }

            // tablets on be|path
            Map<Long, Set<Long>> tablets = Maps.newHashMap();
            if (beIds != null) {
                for (Long beId : beIds) {
                    tablets.put(beId, Sets.newHashSet());
                }
            } else {
                for (Long pathHash : bePaths.second) {
                    tablets.put(pathHash, Sets.newHashSet());
                }
            }
            for (Tablet tablet : index.getTablets()) {
                List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                if (replicas == null) {
                    continue;
                }

                for (Replica replica : replicas) {
                    if (replica.getState() != ReplicaState.NORMAL) {
                        continue;
                    }

                    RootPathLoadStatistic pathLoadStatistic = loadStatistic
                            .getRootPathLoadStatistic(replica.getBackendId(), replica.getPathHash());
                    if (pathLoadStatistic == null || pathLoadStatistic.getDiskState() != DiskInfo.DiskState.ONLINE) {
                        continue;
                    }

                    if (beIds != null) {
                        tablets.computeIfPresent(replica.getBackendId(), (k, v) -> {
                            v.add(tablet.getId());
                            return v;
                        });
                    } else {
                        if (replica.getBackendId() != bePaths.first ||
                                !bePaths.second.contains(replica.getPathHash())) {
                            continue;
                        }
                        tablets.computeIfPresent(replica.getPathHash(), (k, v) -> {
                            v.add(tablet.getId());
                            return v;
                        });
                    }
                }
            }

            for (Map.Entry<Long, Set<Long>> entry : tablets.entrySet()) {
                result.add(new Pair<>(entry.getKey(), entry.getValue()));
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }

        return result;
    }

    private boolean isTabletUnhealthy(long dbId, OlapTable olapTable, Long tabletId,
                                      TabletMeta tabletMeta, List<Long> aliveBeIds) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return false;
        }

        Locker locker = new Locker();
        try {
            locker.lockDatabase(db.getId(), LockType.READ);
            PhysicalPartition physicalPartition = globalStateMgr.getLocalMetastore()
                    .getPhysicalPartitionIncludeRecycleBin(olapTable, tabletMeta.getPhysicalPartitionId());
            if (physicalPartition == null) {
                return true;
            }

            MaterializedIndex index = physicalPartition.getIndex(tabletMeta.getIndexId());
            if (index == null) {
                return true;
            }

            LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
            if (tablet == null) {
                return true;
            }

            short replicaNum = globalStateMgr.getLocalMetastore()
                    .getReplicationNumIncludeRecycleBin(olapTable.getPartitionInfo(), physicalPartition.getParentId());
            if (replicaNum == (short) -1) {
                return true;
            }

            Pair<LocalTablet.TabletHealthStatus, TabletSchedCtx.Priority> statusPair =
                    TabletChecker.getTabletHealthStatusWithPriority(
                            tablet,
                            globalStateMgr.getNodeMgr().getClusterInfo(),
                            physicalPartition.getVisibleVersion(),
                            replicaNum,
                            aliveBeIds,
                            olapTable.getLocation());

            return statusPair.first != LocalTablet.TabletHealthStatus.LOCATION_MISMATCH &&
                    statusPair.first != LocalTablet.TabletHealthStatus.HEALTHY;
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
    }

    /**
     * Get Map<(partition, index) => PartitionStat>
     * <p>
     * both beIds and bePaths can be null, skew will not be set in this case.
     * if beIds is not null, skew is between backends.
     * if bePaths is not null, skew is between paths in single backend.
     * <p>
     * if isLocalBalance, stable colocate table can be scheduled.
     */
    private Map<Pair<Long, Long>, PartitionStat> getPartitionStats(TStorageMedium medium, boolean isLocalBalance,
                                                                   List<Long> beIds, Pair<Long, List<Long>> bePaths) {
        if (beIds != null) {
            Preconditions.checkArgument(beIds.size() > 1);
        }
        if (bePaths != null) {
            Preconditions.checkArgument(bePaths.first != -1 && bePaths.second.size() > 1);
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Map<Pair<Long, Long>, PartitionStat> partitionStats = Maps.newHashMap();
        long start = System.nanoTime();
        long lockTotalTime = 0;
        long lockStart;
        List<Long> dbIds = globalStateMgr.getLocalMetastore().getDbIdsIncludeRecycleBin();
        DATABASE:
        for (Long dbId : dbIds) {
            Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }

            if (db.isSystemDatabase()) {
                continue;
            }

            // set the config to a local variable to avoid config params changed.
            int partitionBatchNum = Config.tablet_checker_partition_batch_num;
            int partitionChecked = 0;
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            lockStart = System.nanoTime();
            try {
                TABLE:
                for (Table table : globalStateMgr.getLocalMetastore().getTablesIncludeRecycleBin(db)) {
                    // check table is olap table or colocate table
                    if (!table.needSchedule(isLocalBalance)) {
                        continue;
                    }
                    if (table.isCloudNativeTableOrMaterializedView()) {
                        // replicas are managed by StarOS and cloud storage.
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    // Table not in NORMAL state is not allowed to do balance,
                    // because the change of tablet location can cause Schema change or rollup failed
                    if (olapTbl.getState() != OlapTable.OlapTableState.NORMAL) {
                        continue;
                    }

                    for (Partition partition : globalStateMgr.getLocalMetastore().getAllPartitionsIncludeRecycleBin(olapTbl)) {
                        partitionChecked++;
                        if (partitionChecked % partitionBatchNum == 0) {
                            lockTotalTime += System.nanoTime() - lockStart;
                            // release lock, so that lock can be acquired by other threads.
                            LOG.debug("partition checked reached batch value, release lock");
                            locker.unLockDatabase(db.getId(), LockType.READ);
                            locker.lockDatabase(db.getId(), LockType.READ);
                            LOG.debug("balancer get lock again");
                            lockStart = System.nanoTime();
                            if (globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId) == null) {
                                continue DATABASE;
                            }
                            if (globalStateMgr.getLocalMetastore().getTableIncludeRecycleBin(db, olapTbl.getId()) == null) {
                                continue TABLE;
                            }
                            if (globalStateMgr.getLocalMetastore().getPartitionIncludeRecycleBin(olapTbl, partition.getId()) ==
                                    null) {
                                continue;
                            }
                        }
                        if (partition.getState() != PartitionState.NORMAL) {
                            // when alter job is in FINISHING state, partition state will be set to NORMAL,
                            // and we can schedule the tablets in it.
                            continue;
                        }

                        DataProperty dataProperty = globalStateMgr.getLocalMetastore()
                                .getDataPropertyIncludeRecycleBin(olapTbl.getPartitionInfo(), partition.getId());
                        if (dataProperty == null) {
                            continue;
                        }
                        TStorageMedium pMedium = dataProperty.getStorageMedium();
                        if (pMedium != medium) {
                            continue;
                        }

                        int replicationFactor = globalStateMgr.getLocalMetastore()
                                .getReplicationNumIncludeRecycleBin(olapTbl.getPartitionInfo(), partition.getId());
                        int replicaNum = partition.getDistributionInfo().getBucketNum() * replicationFactor;
                        // replicaNum may be negative, cause getReplicationNumIncludeRecycleBin can return -1
                        if (replicaNum < 0) {
                            continue;
                        }
                        /*
                         * Tablet in SHADOW index can not be repaired of balanced
                         */
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            for (MaterializedIndex idx : physicalPartition
                                    .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                                PartitionStat pStat = new PartitionStat(dbId, table.getId(), 0, replicaNum,
                                        replicationFactor);
                                partitionStats.put(new Pair<>(physicalPartition.getId(), idx.getId()), pStat);

                                if (beIds == null && bePaths == null) {
                                    continue;
                                }

                                // calculate skew
                                // replicaNum on be|path
                                Map<Long, Integer> replicaNums = getBackendOrPathToReplicaNum(beIds, bePaths);
                                for (Tablet tablet : idx.getTablets()) {
                                    List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                                    if (replicas != null) {
                                        for (Replica replica : replicas) {
                                            if (replica.getState() != ReplicaState.NORMAL) {
                                                continue;
                                            }

                                            if (beIds != null) {
                                                replicaNums.computeIfPresent(replica.getBackendId(), (k, v) -> (v + 1));
                                            } else {
                                                if (replica.getBackendId() != bePaths.first) {
                                                    continue;
                                                }

                                                replicaNums.computeIfPresent(replica.getPathHash(), (k, v) -> (v + 1));
                                            }
                                        }
                                    }
                                }
                                int maxNum = Integer.MIN_VALUE;
                                int minNum = Integer.MAX_VALUE;
                                for (int num : replicaNums.values()) {
                                    if (maxNum < num) {
                                        maxNum = num;
                                    }
                                    if (minNum > num) {
                                        minNum = num;
                                    }
                                }

                                pStat.skew = maxNum - minNum;
                            }
                        }
                    }
                }
            } finally {
                lockTotalTime += System.nanoTime() - lockStart;
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }

        long cost = (System.nanoTime() - start) / 1000000;
        lockTotalTime = lockTotalTime / 1000000;
        if (lockTotalTime > Config.slow_lock_threshold_ms || cost > 30000) {
            LOG.info("finished to calculate partition stats. cost: {} ms, in lock time: {} ms",
                    cost, lockTotalTime);
        }

        return partitionStats;
    }

    @NotNull
    private static Map<Long, Integer> getBackendOrPathToReplicaNum(List<Long> beIds, Pair<Long, List<Long>> bePaths) {
        Map<Long, Integer> replicaNums = Maps.newHashMap();
        if (beIds != null) {
            for (Long beId : beIds) {
                replicaNums.put(beId, 0);
            }
        } else {
            for (Long pathHash : bePaths.second) {
                replicaNums.put(pathHash, 0);
            }
        }
        return replicaNums;
    }

    private Map<Long, Integer> getPartitionReplicaCnt() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Map<Long, Integer> partitionReplicaCnt = new HashMap<>();
        List<Long> dbIds = globalStateMgr.getLocalMetastore().getDbIdsIncludeRecycleBin();
        for (Long dbId : dbIds) {
            Database db = globalStateMgr.getLocalMetastore().getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }

            if (db.isSystemDatabase()) {
                continue;
            }

            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                for (Table table : globalStateMgr.getLocalMetastore().getTablesIncludeRecycleBin(db)) {
                    // check table is olap table or colocate table
                    if (!table.needSchedule(false)) {
                        continue;
                    }
                    if (table.isCloudNativeTable()) {
                        // replicas are managed by StarOS and cloud storage.
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    for (Partition partition : globalStateMgr.getLocalMetastore().getAllPartitionsIncludeRecycleBin(olapTbl)) {
                        int replicaTotalCnt = partition.getDistributionInfo().getBucketNum()
                                *
                                globalStateMgr.getLocalMetastore().getReplicationNumIncludeRecycleBin(olapTbl.getPartitionInfo(),
                                        partition.getId());
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            partitionReplicaCnt.put(physicalPartition.getId(), replicaTotalCnt);
                        }
                    }
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }

        return partitionReplicaCnt;
    }

    private BackendBalanceState getBackendBalanceState(long backendId,
                                                       BackendLoadStatistic backendLoadStatistic,
                                                       TStorageMedium medium,
                                                       Map<Long, Integer> partitionReplicaCnt,
                                                       int backendCnt,
                                                       boolean sortPartition) {
        Map<Pair<Long, Long>, Set<Long>> physicalPartitionTablets = getPartitionTablets(backendId, medium, -1L, false);
        Map<Pair<Long, Long>, List<Long>> partitionTabletList = new HashMap<>();
        for (Map.Entry<Pair<Long, Long>, Set<Long>> entry : physicalPartitionTablets.entrySet()) {
            partitionTabletList.put(entry.getKey(), new LinkedList<>(entry.getValue()));
        }
        Map<Pair<Long, Long>, Double> partitionAvgReplicaSize = getPartitionAvgReplicaSize(backendId, physicalPartitionTablets);
        List<Pair<Long, Long>> physicalPartitionAndMaterializedIndexId = new ArrayList<>(physicalPartitionTablets.keySet());
        if (sortPartition) {
            physicalPartitionAndMaterializedIndexId.sort((p1, p2) -> {
                // skew is (tablet cnt on current BE - average tablet cnt on every BE)
                // sort partitions by skew in desc order, if skew is same, sort by avgReplicaSize in desc order.
                int skew1 = physicalPartitionTablets.get(p1).size()
                        - partitionReplicaCnt.getOrDefault(p1.first, 0) / backendCnt;
                int skew2 = physicalPartitionTablets.get(p2).size()
                        - partitionReplicaCnt.getOrDefault(p2.first, 0) / backendCnt;
                if (skew2 != skew1) {
                    return skew2 - skew1;
                } else {
                    return Double.compare(partitionAvgReplicaSize.get(p2), partitionAvgReplicaSize.get(p1));
                }
            });

            for (List<Long> tabletList : partitionTabletList.values()) {
                if (tabletList.size() <= 1) {
                    continue;
                }
                tabletList.sort((t1, t2) -> {
                    Replica replica1 = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(t1, backendId);
                    Replica replica2 = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getReplica(t2, backendId);
                    return Long.compare(replica2 == null ? 0L : replica2.getDataSize(),
                            replica1 == null ? 0L : replica1.getDataSize());
                });
            }
        }

        BackendBalanceState backendBalanceState = new BackendBalanceState(backendId,
                backendLoadStatistic,
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex(),
                medium,
                partitionTabletList,
                physicalPartitionAndMaterializedIndexId);
        backendBalanceState.init();
        return backendBalanceState;
    }

    private static class PartitionStat {
        Long dbId;
        Long tableId;
        // skew is (max replica number on be) - (min replica number on be)
        int skew;
        int replicaNum;
        int replicationFactor;

        public PartitionStat(Long dbId, Long tableId, int skew, int replicaNum, int replicationFactor) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.skew = skew;
            this.replicaNum = replicaNum;
            this.replicationFactor = replicationFactor;
        }

        @Override
        public String toString() {
            return "dbId: " + dbId + ", tableId: " + tableId + ", skew: " + skew + ", replicaNum: " + replicaNum;
        }
    }

    // used to check disk balance when doing tablet distribution balance
    // we check disk balance using 0.9 * Config.balance_load_score_threshold to avoid trigger disk unbalance
    // todo optimization using segment tree
    public static class DiskBalanceChecker {
        // beId => (totalCapacity, totalUsedCapacity) for cluster balance
        // pathHash => (totalCapacity, totalUsedCapacity) for backend balance
        Map<Long, Pair<Long, Long>> diskCap;
        double maxUsedPercent;
        double minUsedPercent;

        public DiskBalanceChecker(Map<Long, Pair<Long, Long>> diskCap) {
            this.diskCap = diskCap;
        }

        public double getDiskUsedPercent(Long key) {
            Pair<Long, Long> cap = diskCap.get(key);
            if (cap == null) {
                return 0;
            }
            return (double) cap.second / cap.first;
        }

        public void init() {
            maxUsedPercent = Double.MIN_VALUE;
            minUsedPercent = Double.MAX_VALUE;
            for (Map.Entry<Long, Pair<Long, Long>> entry : diskCap.entrySet()) {
                double usedPercent = ((double) entry.getValue().second) / entry.getValue().first;
                if (usedPercent > maxUsedPercent) {
                    maxUsedPercent = usedPercent;
                }
                if (usedPercent < minUsedPercent) {
                    minUsedPercent = usedPercent;
                }
            }
        }

        public boolean check(Long src, Long dest, Long size) {
            Pair<Long, Long> srcCap = diskCap.get(src);
            Pair<Long, Long> destCap = diskCap.get(dest);
            double srcUsedPercent = (double) (srcCap.second - size) / srcCap.first;
            double destUsedPercent = (double) (destCap.second + size) / destCap.first;

            // first check dest be|path capacity limit
            if (DiskInfo.exceedLimit(destCap.first - destCap.second - size,
                    destCap.first, false)) {
                return false;
            }

            double maxUsedPercentAfterBalance = Double.MIN_VALUE;
            double minUsedPercentAfterBalance = Double.MAX_VALUE;
            for (Map.Entry<Long, Pair<Long, Long>> entry : diskCap.entrySet()) {
                double usedPercent;
                if (entry.getKey().equals(src)) {
                    usedPercent = srcUsedPercent;
                } else if (entry.getKey().equals(dest)) {
                    usedPercent = destUsedPercent;
                } else {
                    usedPercent = ((double) entry.getValue().second) / entry.getValue().first;
                }
                if (usedPercent > maxUsedPercentAfterBalance) {
                    maxUsedPercentAfterBalance = usedPercent;
                }
                if (usedPercent < minUsedPercentAfterBalance) {
                    minUsedPercentAfterBalance = usedPercent;
                }
            }

            // all bellow balance_load_disk_safe_threshold
            if (maxUsedPercentAfterBalance < Config.tablet_sched_balance_load_disk_safe_threshold) {
                return true;
            }

            // this will make disk balance better
            if (maxUsedPercentAfterBalance - minUsedPercentAfterBalance < maxUsedPercent - minUsedPercent) {
                return true;
            }

            // this will make disk balance worse, but can not exceed
            // Config.tablet_sched_num_based_balance_threshold_ratio * Config.balance_load_score_threshold;
            return maxUsedPercentAfterBalance - minUsedPercentAfterBalance <
                    Config.tablet_sched_num_based_balance_threshold_ratio *
                            Config.tablet_sched_balance_load_score_threshold;
        }

        public void moveReplica(Long src, Long dest, Long size) {
            Pair<Long, Long> srcCap = diskCap.get(src);
            Pair<Long, Long> destCap = diskCap.get(dest);
            srcCap.second -= size;
            destCap.second += size;

            init();
        }
    }

    public static class BackendBalanceState {
        long backendId;
        BackendLoadStatistic statistic;
        TStorageMedium medium;
        List<Pair<Long, Long>> sortedPartitions;
        TabletInvertedIndex tabletInvertedIndex;
        // <physicalPartitionId, mvId> => tablets in that partition
        // tablets is sorted by data size in desc order for the BE in high load group
        Map<Pair<Long, Long>, List<Long>> partitionTablets;
        // total data used capacity
        long usedCapacity;
        // pathHash => usedCapacity
        Map<Long, Long> pathUsedCapacity;
        int tabletSelected = 0;
        // Min heap of <pathHash, usedPercent>, only used for low load group
        PriorityQueue<Pair<Long, Double>> pathLoadHeap;
        // sorted path in desc order, only used for high load group
        List<Long> sortedPath;
        // pathHash => index of sortedPath, only used for high load group
        Map<Long, Integer> pathSortIndex;

        BackendBalanceState(long backendId,
                            BackendLoadStatistic statistic,
                            TabletInvertedIndex tabletInvertedIndex,
                            TStorageMedium medium,
                            Map<Pair<Long, Long>, List<Long>> partitionTablets,
                            List<Pair<Long, Long>> physicalPartitionAndMaterializedIndexId) {
            this.backendId = backendId;
            this.statistic = statistic;
            this.tabletInvertedIndex = tabletInvertedIndex;
            this.medium = medium;
            this.partitionTablets = partitionTablets;
            this.sortedPartitions = physicalPartitionAndMaterializedIndexId;
        }

        void init() {
            this.usedCapacity = statistic.getTotalUsedCapacityB(medium);
            this.pathLoadHeap = new PriorityQueue<>(Pair.comparingBySecond());
            this.pathUsedCapacity = new HashMap<>();
            this.sortedPath = new ArrayList<>();
            this.pathSortIndex = new HashMap<>();
            for (RootPathLoadStatistic pathStatistic : statistic.getPathStatistics()) {
                if (pathStatistic.getStorageMedium() != this.medium
                        || pathStatistic.getDiskState() != DiskInfo.DiskState.ONLINE
                        || pathStatistic.getCapacityB() <= 0) {
                    continue;
                }

                this.pathLoadHeap.add(new Pair<>(pathStatistic.getPathHash(), pathStatistic.getUsedPercent()));
                this.pathUsedCapacity.put(pathStatistic.getPathHash(), pathStatistic.getUsedCapacityB());
                this.sortedPath.add(pathStatistic.getPathHash());
            }
            sortedPath.sort((p1, p2) -> {
                double skew = statistic.getPathStatistic(p1).getUsedPercent()
                        - statistic.getPathStatistic(p2).getUsedPercent();
                if (Math.abs(skew) < 1e-6) {
                    return 0;
                }
                return skew > 0 ? -1 : 1;
            });
            for (int i = 0; i < sortedPath.size(); i++) {
                pathSortIndex.put(sortedPath.get(i), i);
            }
        }

        // used for low load group
        public Long getLowestLoadPath() {
            return Objects.requireNonNull(pathLoadHeap.poll()).first;
        }

        // used for low load group
        public void addUsedCapacity(long pathHash, long deltaCap) {
            this.usedCapacity += deltaCap;
            long newPathUsedCap = this.pathUsedCapacity.compute(pathHash, (path, cap) -> cap + deltaCap);
            this.pathLoadHeap.add(new Pair<>(pathHash,
                    (double) newPathUsedCap / statistic.getPathStatistic(pathHash).getCapacityB()));
        }

        // used for high load group
        public List<Long> getTabletsInHighLoadPath(List<Long> tablets) {
            double avgUsedPercent = pathUsedCapacity.values().stream().mapToLong(Long::longValue).sum()
                    / (double) statistic.getTotalCapacityB(medium);
            // find the last high load index, we only choose tablet in the high load paths
            int lastHighLoadIndex = -1;
            for (long pathHash : sortedPath) {
                double usedPercent = pathUsedCapacity.get(pathHash)
                        / (double) statistic.getPathStatistic(pathHash).getCapacityB();
                if (usedPercent - avgUsedPercent > -Config.tablet_sched_balance_load_score_threshold) {
                    lastHighLoadIndex++;
                } else {
                    break;
                }
            }
            Preconditions.checkState(lastHighLoadIndex >= 0, "there is no high load path");

            // group the tablet by path, put tablets in sortedPath[i] to tabletGroups[i]
            ArrayList<ArrayList<Long>> tabletGroups = new ArrayList<>();
            for (int i = 0; i < lastHighLoadIndex + 1; i++) {
                tabletGroups.add(new ArrayList<>());
            }
            for (long tabletId : tablets) {
                Replica replica = tabletInvertedIndex.getReplica(tabletId, this.backendId);
                if (replica == null) {
                    continue;
                }
                Integer sortIndex = pathSortIndex.get(replica.getPathHash());
                if (sortIndex == null) {
                    LOG.warn("Can not find path for tablet: {} on backend: {} by path hash: {}",
                            tabletId, this.backendId, replica.getPathHash());
                    continue;
                }
                if (sortIndex > lastHighLoadIndex) {
                    continue;
                }

                tabletGroups.get(sortIndex).add(tabletId);
            }

            List<Long> highLoadPathTablets = new ArrayList<>();
            for (ArrayList<Long> group : tabletGroups) {
                highLoadPathTablets.addAll(group);
            }
            return highLoadPathTablets;
        }

        // used for high load group
        public void minusUsedCapacity(long pathHash, long deltaCap) {
            this.usedCapacity -= deltaCap;
            long pathUsedCap = this.pathUsedCapacity.compute(pathHash, (path, cap) -> cap - deltaCap);
            double usedPercent = (double) pathUsedCap / this.statistic.getPathStatistic(pathHash).getCapacityB();

            // adjust the sort order
            for (int i = this.pathSortIndex.get(pathHash); i < this.sortedPath.size() - 1; i++) {
                long nextPathHash = this.sortedPath.get(i + 1);
                double nextUsedPercent = (double) this.pathUsedCapacity.get(nextPathHash)
                        / this.statistic.getPathStatistic(nextPathHash).getCapacityB();
                if (usedPercent < nextUsedPercent) {
                    this.sortedPath.set(i, nextPathHash);
                    this.sortedPath.set(i + 1, pathHash);
                    this.pathSortIndex.put(nextPathHash, i);
                    this.pathSortIndex.put(pathHash, i + 1);
                } else {
                    break;
                }
            }
        }
    }

    public enum BalanceType {
        DISK,
        TABLET
    }
}
