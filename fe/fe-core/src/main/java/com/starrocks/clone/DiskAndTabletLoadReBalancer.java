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
import com.google.common.collect.Sets;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Partition.PartitionState;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Replica.ReplicaState;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.clone.BackendLoadStatistic.Classification;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    public DiskAndTabletLoadReBalancer(SystemInfoService infoService, TabletInvertedIndex invertedIndex) {
        super(infoService, invertedIndex);
    }

    @Override
    protected List<TabletSchedCtx> selectAlternativeTabletsForCluster(
            ClusterLoadStatistic clusterStat, TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets;
        String balanceType = "";
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

        if (alternativeTablets.size() > 0) {
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

            long srcTotalCapacity = 0L;
            long destTotalCapacity = 0L;
            long srcTotalUsedCapacity = 0L;
            long destTotalUsedCapacity = 0L;
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
            if ((destUsedPercent > (Config.storage_flood_stage_usage_percent / 100.0)) ||
                    ((destTotalCapacity - destTotalUsedCapacity - replicaSize) <
                            Config.storage_flood_stage_left_capacity_bytes)) {
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
                    if ((usedPercent > (Config.storage_flood_stage_usage_percent / 100.0)) ||
                            ((totalCapacity - totalUsedCapacity) < Config.storage_flood_stage_left_capacity_bytes)) {
                        throw new SchedException(SchedException.Status.UNRECOVERABLE, "be disk used exceed limit");
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
                    if ((usedPercent > (Config.storage_flood_stage_usage_percent / 100.0)) ||
                            ((totalCapacity - totalUsedCapacity) < Config.storage_flood_stage_left_capacity_bytes)) {
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
            long partitionId = tabletCtx.getPartitionId();
            long indexId = tabletCtx.getIndexId();
            long srcPathHash = -1;
            long destPathHash = -1;
            if (isLocalBalance) {
                srcPathHash = tabletCtx.getSrcPathHash();
                destPathHash = tabletCtx.getDestPathHash();
            }
            int tabletNumOnSrc =
                    getPartitionTabletNumOnBePath(dbId, tableId, partitionId, indexId, tabletCtx.getSrcBackendId(),
                            srcPathHash);
            int tabletNumOnDest =
                    getPartitionTabletNumOnBePath(dbId, tableId, partitionId, indexId, tabletCtx.getDestBackendId(),
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
        Long beId = cachedReplicaId.remove(tabletId);
        return beId == null ? -1L : beId;
    }

    private void setCachedReplicaId(Long tabletId, Long replicaId) {
        cachedReplicaId.put(tabletId, replicaId);
    }

    private void checkAndUseWorkingSlots(long beId, long pathHash,
                                         Map<Long, TabletScheduler.PathSlot> backendsWorkingSlots)
            throws SchedException {
        TabletScheduler.PathSlot srcBePathSlot = backendsWorkingSlots.get(beId);
        if (srcBePathSlot == null) {
            throw new SchedException(SchedException.Status.UNRECOVERABLE, "working slots not exist for src be");
        }
        if (srcBePathSlot.takeSlot(pathHash) == -1) {
            throw new SchedException(SchedException.Status.SCHEDULE_FAILED, "path busy, wait for next round");
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
     * Cluster disk balance is the base for tablet balance, so we balance disk as much as possible
     * 1. sort be according to used percent in asc order: b1, b2, ... bn
     * 2. calculate average used percent for all be as avgUsedPercent
     * 3. init srcBEIndex as n, destBEIndex as 1
     * 4. copy tablets from srcBE to destBE until
     * 1) usedPercent of srcBE less than avgUsedPercent, srcBEIndex--
     * 2) usedPercent of destBE more than avgUsedPercent, destBEIndex++
     * 5. repeat 4, until srcBEIndex <= destBEIndex
     * <p>
     * we prefer to choose tablets in partition that numOfTablet(srcBE) is more than numOfTablet(destBE)
     */
    private List<TabletSchedCtx> balanceClusterDisk(ClusterLoadStatistic clusterStat,
                                                    TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();

        List<BackendLoadStatistic> beStats = getValidBeStats(clusterStat, medium);
        if (beStats.size() <= 1) {
            return alternativeTablets;
        }

        double avgUsedPercent = beStats.stream().mapToDouble(be -> (be.getUsedPercent(medium))).sum() / beStats.size();

        // sort be by disk used percent in asc order
        beStats.sort(new BackendLoadStatistic.BeStatComparatorForUsedPercent(medium));
        LOG.debug("get backend stats for cluster disk balance. medium: {}, avgUsedPercent: {}, be stats: {}", medium,
                avgUsedPercent, beStats);

        // cache selected tablets to avoid select same tablet
        Set<Long> selectedTablets = Sets.newHashSet();
        // aliveBeIds to check tablet health
        List<Long> aliveBeIds = infoService.getBackendIds(true);
        Map<String, List<Long>> hostGroups = getHostGroups(aliveBeIds);
        int srcBEIndex = beStats.size() - 1;
        int destBEIndex = 0;
        long srcBEUsedCap = beStats.get(srcBEIndex).getTotalUsedCapacityB(medium);
        long destBEUsedCap = beStats.get(destBEIndex).getTotalUsedCapacityB(medium);
        // (partition, index) => tabletIds
        Map<Pair<Long, Long>, Set<Long>> srcBEPartitionTablets =
                getPartitionTablets(beStats.get(srcBEIndex).getBeId(), medium, -1);
        Map<Pair<Long, Long>, Set<Long>> destBEPartitionTablets =
                getPartitionTablets(beStats.get(destBEIndex).getBeId(), medium, -1);
        boolean srcBEChanged = false;
        boolean destBEChanged = false;
        Map<Pair<Long, Long>, PartitionStat> partitionStats = getPartitionStats(medium, false, null, null);
        OUT:
        while (srcBEIndex > destBEIndex) {
            BackendLoadStatistic srcBEStat = beStats.get(srcBEIndex);
            BackendLoadStatistic destBEStat = beStats.get(destBEIndex);
            if (srcBEChanged) {
                srcBEUsedCap = srcBEStat.getTotalUsedCapacityB(medium);
                srcBEPartitionTablets = getPartitionTablets(srcBEStat.getBeId(), medium, -1);
            }
            if (destBEChanged) {
                destBEUsedCap = destBEStat.getTotalUsedCapacityB(medium);
                destBEPartitionTablets = getPartitionTablets(destBEStat.getBeId(), medium, -1);
            }

            Backend destBackend = infoService.getBackend(destBEStat.getBeId());
            if (destBackend == null) {
                destBEIndex++;
                destBEChanged = true;
                continue;
            }

            List<Long> destBeHostGroup = hostGroups.get(destBackend.getHost());
            int totalBes = beStats.size();
            List<Long> tablets =
                    getSourceTablets(partitionStats, srcBEPartitionTablets, destBEPartitionTablets, totalBes);
            // do not choose selected tablet
            // do not choose tablet that exists in backends whose host is same with dest be
            tablets = tablets.stream()
                    .filter(v -> !selectedTablets.contains(v) && !isTabletExistsInBackends(v, destBeHostGroup))
                    .collect(Collectors.toList());

            // copy tablets from srcBE high load paths to destBE low load paths
            Set<Long> srcBEPaths =
                    srcBEStat.getPathStatisticForMIDAndClazz(BackendLoadStatistic.Classification.HIGH, medium);
            List<Long> destBEPaths = new ArrayList<>(
                    destBEStat.getPathStatisticForMIDAndClazz(BackendLoadStatistic.Classification.LOW, medium));
            if (destBEPaths.size() <= 0) {
                destBEIndex++;
                destBEChanged = true;
                continue;
            }
            int destBEPathsIndex = 0;  // round robin to select dest be path
            long srcBETotalCap = srcBEStat.getTotalCapacityB(medium);
            long destBETotalCap = destBEStat.getTotalCapacityB(medium);
            for (Long tabletId : tablets) {
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    continue;
                }
                Replica replica = invertedIndex.getReplica(tabletId, srcBEStat.getBeId());
                if (replica == null || replica.getPathHash() == -1L) {
                    continue;
                }

                // only move tablet on high load disk
                if (!srcBEPaths.contains(replica.getPathHash())) {
                    continue;
                }

                // check used percent after move
                double destBEUsedPercent = (double) (destBEUsedCap + replica.getDataSize()) / destBETotalCap;
                double srcBEUsedPercent = (double) (srcBEUsedCap - replica.getDataSize()) / srcBETotalCap;
                if (Math.abs(destBEUsedPercent - avgUsedPercent) > 1e-6 && (destBEUsedPercent > avgUsedPercent)) {
                    destBEIndex++;
                    destBEChanged = true;
                    continue OUT;
                }
                if (Math.abs(srcBEUsedPercent - avgUsedPercent) > 1e-6 && (srcBEUsedPercent < avgUsedPercent)) {
                    srcBEIndex--;
                    srcBEChanged = true;
                    continue OUT;
                }

                // check tablet healthy
                if (!isTabletHealthy(tabletId, tabletMeta, aliveBeIds)) {
                    continue;
                }

                // NOTICE: state has been changed, the tablet must be selected
                destBEUsedCap += replica.getDataSize();
                srcBEUsedCap -= replica.getDataSize();
                Pair<Long, Long> p = Pair.create(tabletMeta.getPartitionId(), tabletMeta.getIndexId());
                //p: partition <partitionId, indexId>
                //k: partition same to p
                srcBEPartitionTablets.compute(p, (k, pTablets) -> {
                    if (pTablets != null) {
                        pTablets.remove(tabletId);
                    }
                    return pTablets;
                });
                destBEPartitionTablets.compute(p, (k, pTablets) -> {
                    if (pTablets != null) {
                        pTablets.add(tabletId);
                        return pTablets;
                    }
                    return Sets.newHashSet(tabletId);
                });
                // round robin to select dest be path
                Long destPathHash = destBEPaths.get(destBEPathsIndex);
                destBEPathsIndex = (destBEPathsIndex + 1) % destBEPaths.size();

                TabletSchedCtx schedCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                        tabletMeta.getDbId(), tabletMeta.getTableId(), tabletMeta.getPartitionId(),
                        tabletMeta.getIndexId(), tabletId, System.currentTimeMillis());
                schedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
                schedCtx.setSrc(replica);
                schedCtx.setDest(destBEStat.getBeId(), destPathHash);
                schedCtx.setBalanceType(BalanceType.DISK);
                selectedTablets.add(tabletId);
                alternativeTablets.add(schedCtx);
                if (alternativeTablets.size() >= Config.tablet_sched_max_balancing_tablets) {
                    break OUT;
                }
            }

            // code reach here means that all tablets have moved to destBE, but srcBE and destBE both have not reached the average.
            // it is not easy to judge whether src or dest should be retained for next round, just random
            if ((int) (Math.random() * 100) % 2 == 0) {
                srcBEIndex--;
                srcBEChanged = true;
            } else {
                destBEIndex++;
                destBEChanged = true;
            }
        }

        return alternativeTablets;
    }

    /**
     * Backend disk balance is same with cluster disk balance.
     * 1. select unbalanced be and sort be by path min|max used percent skew in desc order.
     * 2. balance each backend in order.
     * 2.1. sort disk according to used percent in asc order: path1, path2, ... pathn
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
            if (!infoService.checkBackendAvailable(beId)) {
                continue;
            }

            List<RootPathLoadStatistic> pathStats = getValidBePathStats(beStat, medium);
            if (pathStats.size() <= 1) {
                continue;
            }

            double avgUsedPercent =
                    pathStats.stream().mapToDouble(pathStat -> pathStat.getUsedPercent()).sum() / pathStats.size();

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

    private void balanceBackendDisk(TStorageMedium medium, double avgUsedPercent,
                                    List<RootPathLoadStatistic> pathStats, long beId, int beNum,
                                    List<TabletSchedCtx> alternativeTablets) {
        Preconditions.checkArgument(pathStats != null && pathStats.size() > 1 && beId > -1 && beNum > 0);

        // aliveBeIds to check tablet health
        List<Long> aliveBeIds = infoService.getBackendIds(true);

        // src|dest path stat
        int srcPathIndex = pathStats.size() - 1;
        int destPathIndex = 0;
        long srcPathUsedCap = pathStats.get(srcPathIndex).getUsedCapacityB();
        long destPathUsedCap = pathStats.get(destPathIndex).getUsedCapacityB();
        long srcBeId = beId;
        long destBeId = beId;
        long srcPathHash = pathStats.get(srcPathIndex).getPathHash();
        long destPathHash = pathStats.get(destPathIndex).getPathHash();

        // (partition, index) => tabletIds
        Map<Pair<Long, Long>, Set<Long>> srcPathPartitionTablets = getPartitionTablets(srcBeId, medium, srcPathHash);
        Map<Pair<Long, Long>, Set<Long>> destPathPartitionTablets = getPartitionTablets(destBeId, medium, destPathHash);
        Map<Pair<Long, Long>, PartitionStat> partitionStats = getPartitionStats(medium, true, null, null);

        boolean srcChanged = false;
        boolean destChanged = false;
        OUT:
        while (srcPathIndex > destPathIndex) {
            RootPathLoadStatistic srcPathStat = pathStats.get(srcPathIndex);
            RootPathLoadStatistic destPathStat = pathStats.get(destPathIndex);
            if (srcChanged) {
                srcPathUsedCap = srcPathStat.getUsedCapacityB();
                srcBeId = beId;
                srcPathHash = srcPathStat.getPathHash();
                srcPathPartitionTablets = getPartitionTablets(srcBeId, medium, srcPathHash);
                srcChanged = false;
            }
            if (destChanged) {
                destPathUsedCap = destPathStat.getUsedCapacityB();
                destBeId = beId;
                destPathHash = destPathStat.getPathHash();
                destPathPartitionTablets = getPartitionTablets(destBeId, medium, destPathHash);
                destChanged = false;
            }

            int totalPaths = beNum * pathStats.size();
            List<Long> tablets =
                    getSourceTablets(partitionStats, srcPathPartitionTablets, destPathPartitionTablets, totalPaths);

            long srcPathTotalCap = srcPathStat.getCapacityB();
            long destPathTotalCap = destPathStat.getCapacityB();
            for (Long tabletId : tablets) {
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
                if (tabletMeta == null) {
                    continue;
                }
                Replica replica = invertedIndex.getReplica(tabletId, srcBeId);
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

                // check tablet healthy
                if (!isTabletHealthy(tabletId, tabletMeta, aliveBeIds)) {
                    continue;
                }

                // NOTICE: state has been changed, the tablet must be selected
                destPathUsedCap += replica.getDataSize();
                srcPathUsedCap -= replica.getDataSize();
                Pair<Long, Long> p = Pair.create(tabletMeta.getPartitionId(), tabletMeta.getIndexId());
                // p: partition <partitionId, indexId>
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
                                tabletMeta.getTableId(), tabletMeta.getPartitionId(),
                                tabletMeta.getIndexId(), tabletId, System.currentTimeMillis());
                schedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
                schedCtx.setSrc(replica);
                schedCtx.setDest(destBeId, destPathHash);
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
            // srcTablets.size() - avgNum is max tablets num moved from that will preserve src be|path tablet distribution balance
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
            if (infoService.checkBackendAvailable(beStat.getBeId()) && beStat.getTotalCapacityB(medium) > 0) {
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
            Backend backend = infoService.getBackend(backendId);
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
        if (backends == null || backends.size() <= 0) {
            return false;
        }

        for (Long backendId : backends) {
            Replica replica = invertedIndex.getReplica(tabletId, backendId);
            if (replica != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return map : (partition, index) => tablets
     */
    private Map<Pair<Long, Long>, Set<Long>> getPartitionTablets(long beId, TStorageMedium medium, long pathHash) {
        Map<Pair<Long, Long>, Set<Long>> partitionTablets = Maps.newHashMap();
        List<Long> tabletIds = invertedIndex.getTabletIdsByBackendIdAndStorageMedium(beId, medium);
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
            }

            if (tabletMeta.isLakeTablet()) {
                // replicas are managed by StarOS and cloud storage.
                continue;
            }

            if (pathHash != -1) {
                Replica replica = invertedIndex.getReplica(tabletId, beId);
                if (replica.getPathHash() != pathHash) {
                    continue;
                }
            }

            Pair<Long, Long> key = new Pair<>(tabletMeta.getPartitionId(), tabletMeta.getIndexId());
            if (partitionTablets.containsKey(key)) {
                partitionTablets.get(key).add(tabletId);
            } else {
                partitionTablets.put(key, Sets.newHashSet(tabletId));
            }
        }
        return partitionTablets;
    }

    private int getPartitionTabletNumOnBePath(long dbId, long tableId, long partitionId, long indexId, long beId,
                                              long pathHash) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return 0;
        }

        db.readLock();
        try {
            OlapTable table = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tableId);
            if (table == null) {
                return 0;
            }

            Partition partition = globalStateMgr.getPartitionIncludeRecycleBin(table, partitionId);
            if (partition == null) {
                return 0;
            }

            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                return 0;
            }

            int cnt = 0;
            for (Tablet tablet : index.getTablets()) {
                List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
                if (replicas == null) {
                    continue;
                }

                for (Replica replica : replicas) {
                    if (replica.getState() == ReplicaState.NORMAL && replica.getBackendId() == beId) {
                        if (pathHash == -1 || (pathHash != -1 && replica.getPathHash() == pathHash)) {
                            cnt++;
                        }
                    }
                }
            }
            return cnt;
        } finally {
            db.readUnlock();
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
            if (!infoService.checkBackendAvailable(beId)) {
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
                               List<TabletSchedCtx> alternativeTablets, boolean isLocalBalance,
                               List<BackendLoadStatistic> beStats, Map<Long, Pair<List<Long>, Integer>> beDisks,
                               List<RootPathLoadStatistic> pathStats, long beId) {
        if (!isLocalBalance) {
            Preconditions.checkArgument(beStats != null && beStats.size() > 1);
        } else {
            Preconditions.checkArgument(pathStats != null && pathStats.size() > 1 && beId > -1);
        }

        // beId|pathHash => (totalCapacity, totalUsedCapacity)
        Map<Long, Pair<Long, Long>> diskCapMap = Maps.newHashMap();
        List<Long> beIds = null;
        List<Long> paths = null;
        Map<Pair<Long, Long>, PartitionStat> partitionStats = null;
        if (!isLocalBalance) {
            for (BackendLoadStatistic beStat : beStats) {
                diskCapMap.put(beStat.getBeId(),
                        new Pair<>(beStat.getTotalCapacityB(medium), beStat.getTotalUsedCapacityB(medium)));
            }
            beIds = Lists.newArrayList(diskCapMap.keySet());
            partitionStats = getPartitionStats(medium, false, beIds, null);
        } else {
            for (RootPathLoadStatistic pathStat : pathStats) {
                diskCapMap
                        .put(pathStat.getPathHash(), Pair.create(pathStat.getCapacityB(), pathStat.getUsedCapacityB()));
            }
            paths = Lists.newArrayList(diskCapMap.keySet());
            partitionStats = getPartitionStats(medium, true, null, Pair.create(beId, paths));
        }

        List<Pair<Long, Long>> partitions = new ArrayList<>(partitionStats.keySet());
        // sort all partition by distribution skew in desc order, skew is (max tablet number on be|path) - (min tablet number on be|path)
        Map<Pair<Long, Long>, PartitionStat> finalPartitionStats = partitionStats;
        partitions.sort((o1, o2) -> {
            PartitionStat pStat1 = finalPartitionStats.get(o1);
            PartitionStat pStat2 = finalPartitionStats.get(o2);
            return pStat2.skew - pStat1.skew;
        });

        DiskBalanceChecker diskBalanceChecker = new DiskBalanceChecker(diskCapMap);
        diskBalanceChecker.init();
        Set<Long> selectedTablets = Sets.newHashSet();
        List<Long> aliveBeIds = infoService.getBackendIds(true);
        Map<String, List<Long>> hostGroups = getHostGroups(aliveBeIds);
        for (Pair<Long, Long> partition : partitions) {
            PartitionStat pStat = partitionStats.get(partition);
            // skew <= 1 means partition is balanced
            if (pStat.skew <= 1) {
                break;
            }

            // List<Pair<beId|pathHash, Set<tabletId>>>
            List<Pair<Long, Set<Long>>> tablets = null;
            if (!isLocalBalance) {
                tablets =
                        getPartitionTablets(pStat.dbId, pStat.tableId, partition.first, partition.second, beIds, null);
            } else {
                tablets = getPartitionTablets(pStat.dbId, pStat.tableId, partition.first, partition.second, null,
                        Pair.create(beId, paths));
            }
            boolean tabletFound = false;
            do {
                tabletFound = false;
                // sort be by tablets num in desc order, and used percent in desc order for bes|paths with same tablets num
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

                    TabletSchedCtx schedCtx = null;
                    if (!isLocalBalance) {
                        Backend destBackend = infoService.getBackend(destTablets.first);
                        if (destBackend == null) {
                            continue;
                        }
                        schedCtx = tryToBalanceTablet(srcTablets, destTablets, diskBalanceChecker,
                                selectedTablets, aliveBeIds, isLocalBalance,
                                hostGroups.get(destBackend.getHost()), -1);
                    } else {
                        schedCtx = tryToBalanceTablet(srcTablets, destTablets, diskBalanceChecker,
                                selectedTablets, aliveBeIds, isLocalBalance,
                                null, beId);
                    }

                    if (schedCtx != null) {
                        // NOTICE: state has been changed, the tablet must be selected
                        // set dest beId and pathHash
                        if (!isLocalBalance) {
                            //round robin to select dest be path
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
                                              DiskBalanceChecker diskBalanceChecker, Set<Long> selectedTablets,
                                              List<Long> aliveBeIds,
                                              boolean isLocalBalance, List<Long> destBackendHostGroup, long beId) {
        Preconditions.checkArgument(!isLocalBalance || beId != -1);

        for (Long tabletId : srcTablets.second) {
            if (!isLocalBalance) {
                if (destBackendHostGroup != null && isTabletExistsInBackends(tabletId, destBackendHostGroup)) {
                    continue;
                }
            }

            if (selectedTablets.contains(tabletId)) {
                continue;
            }

            Replica replica = null;
            if (!isLocalBalance) {
                replica = invertedIndex.getReplica(tabletId, srcTablets.first);
            } else {
                replica = invertedIndex.getReplica(tabletId, beId);
            }
            if (replica == null || replica.getPathHash() == -1L) {
                continue;
            }

            if (!diskBalanceChecker.check(srcTablets.first, destTablets.first, replica.getDataSize())) {
                continue;
            }

            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
            }

            if (!isTabletHealthy(tabletId, tabletMeta, aliveBeIds)) {
                continue;
            }

            TabletSchedCtx schedCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE,
                    tabletMeta.getDbId(), tabletMeta.getTableId(), tabletMeta.getPartitionId(),
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
     * Get beId or pathHash to tablets by partitionId and indexId.
     * If beIds is not null, return beId => Set<tabletId>.
     * If bePaths is not null, return pathHash => Set<tabletId>.
     */
    private List<Pair<Long, Set<Long>>> getPartitionTablets(Long dbId, Long tableId, Long partitionId, Long indexId,
                                                            List<Long> beIds, Pair<Long, List<Long>> bePaths) {
        Preconditions.checkArgument(beIds != null || bePaths != null);

        List<Pair<Long, Set<Long>>> result = Lists.newArrayList();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();

        Database db = globalStateMgr.getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return result;
        }

        try {
            db.readLock();
            OlapTable table = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tableId);
            if (table == null) {
                return result;
            }
            if (table.isCloudNativeTable()) {
                // replicas are managed by StarOS and cloud storage.
                return result;
            }

            Partition partition = globalStateMgr.getPartitionIncludeRecycleBin(table, partitionId);
            if (partition == null) {
                return result;
            }

            MaterializedIndex index = partition.getIndex(indexId);
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
            db.readUnlock();
        }

        return result;
    }

    // NOTICE: call this function as little as possible, cause this will get db lock
    private boolean isTabletHealthy(Long tabletId, TabletMeta tabletMeta, List<Long> aliveBeIds) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getDbIncludeRecycleBin(tabletMeta.getDbId());
        if (db == null) {
            return false;
        }

        try {
            db.readLock();
            OlapTable table = (OlapTable) globalStateMgr.getTableIncludeRecycleBin(db, tabletMeta.getTableId());
            if (table == null) {
                return false;
            }

            Partition partition = globalStateMgr.getPartitionIncludeRecycleBin(table, tabletMeta.getPartitionId());
            if (partition == null) {
                return false;
            }

            MaterializedIndex index = partition.getIndex(tabletMeta.getIndexId());
            if (index == null) {
                return false;
            }

            LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);
            if (tablet == null) {
                return false;
            }

            short replicaNum =
                    globalStateMgr.getReplicationNumIncludeRecycleBin(table.getPartitionInfo(), partition.getId());
            if (replicaNum == (short) -1) {
                return false;
            }

            Pair<LocalTablet.TabletStatus, TabletSchedCtx.Priority> statusPair =
                    tablet.getHealthStatusWithPriority(infoService,
                            partition.getVisibleVersion(),
                            replicaNum,
                            aliveBeIds);

            return statusPair.first == LocalTablet.TabletStatus.HEALTHY;
        } finally {
            db.readUnlock();
        }
    }

    private static class PartitionStat {
        Long dbId;
        Long tableId;
        // skew is (max replica number on be) - (min replica number on be)
        int skew;
        int replicaNum;

        public PartitionStat(Long dbId, Long tableId, int skew, int replicaNum) {
            this.dbId = dbId;
            this.tableId = tableId;
            this.skew = skew;
            this.replicaNum = replicaNum;
        }

        @Override
        public String toString() {
            return "dbId: " + dbId + ", tableId: " + tableId + ", skew: " + skew + ", replicaNum: " + replicaNum;
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
        List<Long> dbIds = globalStateMgr.getDbIdsIncludeRecycleBin();
        DATABASE:
        for (Long dbId : dbIds) {
            Database db = globalStateMgr.getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }

            if (db.isInfoSchemaDb()) {
                continue;
            }

            // set the config to a local variable to avoid config params changed.
            int partitionBatchNum = Config.tablet_checker_partition_batch_num;
            int partitionChecked = 0;
            db.readLock();
            lockStart = System.nanoTime();
            try {
                TABLE:
                for (Table table : globalStateMgr.getTablesIncludeRecycleBin(db)) {
                    // check table is olap table or colocate table
                    if (!table.needSchedule(isLocalBalance)) {
                        continue;
                    }
                    if (table.isCloudNativeTable()) {
                        // replicas are managed by StarOS and cloud storage.
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    // Table not in NORMAL state is not allowed to do balance,
                    // because the change of tablet location can cause Schema change or rollup failed
                    if (olapTbl.getState() != OlapTable.OlapTableState.NORMAL) {
                        continue;
                    }

                    for (Partition partition : globalStateMgr.getAllPartitionsIncludeRecycleBin(olapTbl)) {
                        partitionChecked++;
                        if (partitionChecked % partitionBatchNum == 0) {
                            lockTotalTime += System.nanoTime() - lockStart;
                            // release lock, so that lock can be acquired by other threads.
                            LOG.debug("partition checked reached batch value, release lock");
                            db.readUnlock();
                            db.readLock();
                            LOG.debug("balancer get lock again");
                            lockStart = System.nanoTime();
                            if (globalStateMgr.getDbIncludeRecycleBin(dbId) == null) {
                                continue DATABASE;
                            }
                            if (globalStateMgr.getTableIncludeRecycleBin(db, olapTbl.getId()) == null) {
                                continue TABLE;
                            }
                            if (globalStateMgr.getPartitionIncludeRecycleBin(olapTbl, partition.getId()) == null) {
                                continue;
                            }
                        }
                        if (partition.getState() != PartitionState.NORMAL) {
                            // when alter job is in FINISHING state, partition state will be set to NORMAL,
                            // and we can schedule the tablets in it.
                            continue;
                        }

                        DataProperty dataProperty =
                                globalStateMgr.getDataPropertyIncludeRecycleBin(olapTbl.getPartitionInfo(),
                                        partition.getId());
                        if (dataProperty == null) {
                            continue;
                        }
                        TStorageMedium pMedium = dataProperty.getStorageMedium();
                        if (pMedium != medium) {
                            continue;
                        }

                        int replicaNum = partition.getDistributionInfo().getBucketNum()
                                * globalStateMgr.getReplicationNumIncludeRecycleBin(olapTbl.getPartitionInfo(),
                                partition.getId());
                        // replicaNum may be negative, cause getReplicationNumIncludeRecycleBin can return -1
                        if (replicaNum < 0) {
                            continue;
                        }
                        /*
                         * Tablet in SHADOW index can not be repaired of balanced
                         */
                        for (MaterializedIndex idx : partition
                                .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                            PartitionStat pStat = new PartitionStat(dbId, table.getId(), 0, replicaNum);
                            partitionStats.put(new Pair<>(partition.getId(), idx.getId()), pStat);

                            if (beIds == null && bePaths == null) {
                                continue;
                            }

                            // calculate skew
                            // replicaNum on be|path
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
            } finally {
                lockTotalTime += System.nanoTime() - lockStart;
                db.readUnlock();
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
            if ((destUsedPercent > (Config.storage_flood_stage_usage_percent / 100.0)) ||
                    ((destCap.first - destCap.second - size) < Config.storage_flood_stage_left_capacity_bytes)) {
                return false;
            }

            double maxUsedPercentAfterBalance = Double.MIN_VALUE;
            double minUsedPercentAfterBalance = Double.MAX_VALUE;
            for (Map.Entry<Long, Pair<Long, Long>> entry : diskCap.entrySet()) {
                double usedPercent = 0.0;
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

            // this will make disk balance worse, but can not exceed 0.9 * Config.balance_load_score_threshold;
            return maxUsedPercentAfterBalance - minUsedPercentAfterBalance <
                    0.9 * Config.tablet_sched_balance_load_score_threshold;
        }

        public void moveReplica(Long src, Long dest, Long size) {
            Pair<Long, Long> srcCap = diskCap.get(src);
            Pair<Long, Long> destCap = diskCap.get(dest);
            srcCap.second -= size;
            destCap.second += size;

            init();
        }
    }

    public enum BalanceType {
        DISK,
        TABLET
    }
}
