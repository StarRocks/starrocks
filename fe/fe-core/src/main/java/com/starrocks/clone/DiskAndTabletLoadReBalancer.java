// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.clone;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
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
            String clusterName, ClusterLoadStatistic clusterStat, TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets;
        if (!isDiskBalanced(clusterStat, medium)) {
            alternativeTablets = balanceDisk(clusterName, clusterStat, medium);
            if (alternativeTablets.size() > 0) {
                LOG.info("select tablets to balance disk: total {}, tablets[show up to 100]: {}",
                        alternativeTablets.size(),
                        alternativeTablets.stream().mapToLong(TabletSchedCtx::getTabletId).limit(100).toArray());
            }
        } else {
            alternativeTablets = balanceTablet(clusterName, clusterStat, medium);
            if (alternativeTablets.size() > 0) {
                LOG.info("select tablets to balance tablet distribution: total {}, tablets[show up to 100]: {}",
                        alternativeTablets.size(),
                        alternativeTablets.stream().mapToLong(TabletSchedCtx::getTabletId).limit(100).toArray());
            }
        }

        return alternativeTablets;
    }

    @Override
    public void completeSchedCtx(TabletSchedCtx tabletCtx, Map<Long, TabletScheduler.PathSlot> backendsWorkingSlots)
            throws SchedException {
        TStorageMedium medium = tabletCtx.getStorageMedium();
        ClusterLoadStatistic clusterStat = statisticMap.get(tabletCtx.getCluster());
        if (clusterStat == null) {
            throw new SchedException(SchedException.Status.UNRECOVERABLE, "cluster does not exist");
        }

        // tabletCtx may wait a long time from the pending state to the running state, so we must double-check the task
        if (tabletCtx.getBalanceType() == BalanceType.DISK) {
            BackendLoadStatistic srcBeStat = clusterStat.getBackendLoadStatistic(tabletCtx.getSrcBackendId());
            BackendLoadStatistic destBeStat = clusterStat.getBackendLoadStatistic(tabletCtx.getDestBackendId());
            if (srcBeStat == null || destBeStat == null) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "src be or dest be statistic not exist");
            }
            long srcBeTotalCapacity = srcBeStat.getTotalCapacityB(medium);
            long destBeTotalCapacity = destBeStat.getTotalCapacityB(medium);
            if (srcBeTotalCapacity <= 0 || destBeTotalCapacity <= 0) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "medium not exists on src or dest be");
            }
            long srcBeUsedTotalCapacity = srcBeStat.getTotalUsedCapacityB(medium);
            long destBeUsedTotalCapacity = destBeStat.getTotalUsedCapacityB(medium);
            long replicaSize = tabletCtx.getSrcReplica().getDataSize();
            double srcBeUsedPercent = (double) (srcBeUsedTotalCapacity - replicaSize) / srcBeTotalCapacity;
            double destBeUsedPercent = (double) (destBeUsedTotalCapacity + replicaSize) / destBeTotalCapacity;
            if ((destBeUsedPercent > (Config.storage_flood_stage_usage_percent / 100.0)) ||
                    ((destBeTotalCapacity - destBeUsedTotalCapacity - replicaSize) <
                            Config.storage_flood_stage_left_capacity_bytes)) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "be disk used exceed limit");
            }
            if (srcBeStat.getUsedPercent(medium) < destBeStat.getUsedPercent(medium)) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE,
                        "src be disk used percent is smaller than dest be");
            }
            if (srcBeUsedPercent < destBeUsedPercent) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE,
                        "src be disk used percent is smaller than dest be after copy");
            }
        } else {
            // for tablet balance task, first check whether it can preserve disk balance,
            // then check whether it can make tablet distribution balance better
            double maxUsedPercent = 0.0;
            double minUsedPercent = Double.MAX_VALUE;
            for (BackendLoadStatistic beStat : clusterStat.getAllBackendLoadStatistic()) {
                if (beStat.getTotalCapacityB(medium) <= 0) {
                    continue;
                }

                long totalCapacity = beStat.getTotalCapacityB(medium);
                long totalUsedCapacity = beStat.getTotalUsedCapacityB(medium);
                long replicaSize = tabletCtx.getSrcReplica().getDataSize();
                if (beStat.getBeId() == tabletCtx.getSrcBackendId()) {
                    totalUsedCapacity -= tabletCtx.getSrcReplica().getDataSize();
                }
                if (beStat.getBeId() == tabletCtx.getDestBackendId()) {
                    totalUsedCapacity += tabletCtx.getSrcReplica().getDataSize();
                }
                double usedPercent = (double) totalUsedCapacity / totalCapacity;

                if ((usedPercent > (Config.storage_flood_stage_usage_percent / 100.0)) ||
                        ((totalCapacity - totalUsedCapacity - replicaSize) <
                                Config.storage_flood_stage_left_capacity_bytes)) {
                    throw new SchedException(SchedException.Status.UNRECOVERABLE, "be disk used exceed limit");
                }

                if (usedPercent > maxUsedPercent) {
                    maxUsedPercent = usedPercent;
                }
                if (usedPercent < minUsedPercent) {
                    minUsedPercent = usedPercent;
                }
            }
            if ((maxUsedPercent > Config.balance_load_disk_safe_threshold) &&
                    ((maxUsedPercent - minUsedPercent) > Config.balance_load_score_threshold)) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE, "disk balance will be broken");
            }

            int tabletNumOnSrcBe =
                    getPartitionTabletNumOnBe(tabletCtx.getDbId(), tabletCtx.getTblId(), tabletCtx.getPartitionId(),
                            tabletCtx.getIndexId(), tabletCtx.getSrcBackendId());
            int tabletNumOnDestBe =
                    getPartitionTabletNumOnBe(tabletCtx.getDbId(), tabletCtx.getTblId(), tabletCtx.getPartitionId(),
                            tabletCtx.getIndexId(), tabletCtx.getDestBackendId());
            if (tabletNumOnSrcBe - tabletNumOnDestBe <= 1) {
                throw new SchedException(SchedException.Status.UNRECOVERABLE,
                        "can not make tablet distribution balance better");
            }
        }

        checkAndUseWorkingSlots(tabletCtx.getSrcBackendId(), tabletCtx.getSrcPathHash(), backendsWorkingSlots);
        tabletCtx.setSrcPathResourceHold();
        checkAndUseWorkingSlots(tabletCtx.getDestBackendId(), tabletCtx.getDestPathHash(), backendsWorkingSlots);
        tabletCtx.setDestPathResourceHold();

        setCachedReplicaId(tabletCtx.getTabletId(), tabletCtx.getSrcReplica().getId());
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
        if (srcBePathSlot.takeBalanceSlot(pathHash) == -1) {
            throw new SchedException(SchedException.Status.SCHEDULE_FAILED, "path busy, wait for next round");
        }
    }

    /**
     * cluster disk is balanced if:
     * 1. all be disk used percent smaller than Config.balance_load_disk_safe_threshold
     * or
     * 2. difference between max used percent and min used percent smaller than Config.balance_load_score_threshold
     */
    private boolean isDiskBalanced(ClusterLoadStatistic clusterStat, TStorageMedium medium) {
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

        return maxUsedPercent < Config.balance_load_disk_safe_threshold ||
                ((maxUsedPercent - minUsedPercent) < Config.balance_load_score_threshold);
    }

    /**
     * disk balance is the base for tablet balance, so we balance disk as much as possible
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
    private List<TabletSchedCtx> balanceDisk(String clusterName, ClusterLoadStatistic clusterStat,
                                             TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();

        List<BackendLoadStatistic> beStats = getValidBeStats(clusterStat, medium);
        if (beStats.size() <= 1) {
            return alternativeTablets;
        }

        double avgUsedPercent = beStats.stream().mapToDouble(be -> (be.getUsedPercent(medium))).sum() / beStats.size();

        // sort be by disk used percent in asc order
        beStats.sort(new BackendLoadStatistic.BeStatComparatorForUsedPercent(medium));

        // cache selected tablets to avoid select same tablet
        Set<Long> selectedTablets = Sets.newHashSet();
        // aliveBeIds to check tablet health
        List<Long> aliveBeIds = infoService.getClusterBackendIds(clusterName, true);
        Map<String, List<Long>> hostGroups = getHostGroups(aliveBeIds);
        int srcBEIndex = beStats.size() - 1;
        int destBEIndex = 0;
        long srcBEUsedCap = beStats.get(srcBEIndex).getTotalUsedCapacityB(medium);
        long destBEUsedCap = beStats.get(destBEIndex).getTotalUsedCapacityB(medium);
        // (partition, index) => tabletIds
        Map<Pair<Long, Long>, Set<Long>> srcBEPartitionTablets =
                getPartitionTablets(beStats.get(srcBEIndex).getBeId(), medium);
        Map<Pair<Long, Long>, Set<Long>> destBEPartitionTablets =
                getPartitionTablets(beStats.get(destBEIndex).getBeId(), medium);
        boolean srcBEChanged = false;
        boolean destBEChanged = false;
        Map<Pair<Long, Long>, PartitionStat> partitionStats = getPartitionStats(null, medium);
        OUT:
        while (srcBEIndex > destBEIndex) {
            BackendLoadStatistic srcBEStat = beStats.get(srcBEIndex);
            BackendLoadStatistic destBEStat = beStats.get(destBEIndex);
            if (srcBEChanged) {
                srcBEUsedCap = srcBEStat.getTotalUsedCapacityB(medium);
                srcBEPartitionTablets = getPartitionTablets(srcBEStat.getBeId(), medium);
            }
            if (destBEChanged) {
                destBEUsedCap = destBEStat.getTotalUsedCapacityB(medium);
                destBEPartitionTablets = getPartitionTablets(destBEStat.getBeId(), medium);
            }

            Backend destBackend = infoService.getBackend(destBEStat.getBeId());
            if (destBackend == null) {
                destBEIndex++;
                destBEChanged = true;
                continue;
            }

            List<Long> destBeHostGroup = hostGroups.get(destBackend.getHost());

            // we store tablets that can make tablet distribution balance better to balancedTablets,
            // and those make tablet distribution balance worse to unbalancedTablets.
            List<Long> balancedTablets = Lists.newArrayList();
            List<Long> unbalancedTablets = Lists.newArrayList();
            for (Map.Entry<Pair<Long, Long>, Set<Long>> partitionTablets : srcBEPartitionTablets.entrySet()) {
                PartitionStat pStat = partitionStats.get(partitionTablets.getKey());
                if (pStat == null) {
                    continue;
                }

                Set<Long> destTablets =
                        destBEPartitionTablets.getOrDefault(partitionTablets.getKey(), Sets.newHashSet());
                Set<Long> srcTablets = partitionTablets.getValue();
                int avgNum = pStat.replicaNum / beStats.size();
                // num of tablets that make tablet distribution balance better
                // avgNum - destTablets.size() is max tablets num moved to that will preserve dest be tablet distribution balance
                // srcTablets.size() - avgNum is max tablets num moved from that will preserve src be tablet distribution balance
                // so we take the smallest value between the two as balanceNum
                int balanceNum = Math.min(avgNum - destTablets.size(), srcTablets.size() - avgNum);
                for (Long tabletId : srcTablets) {
                    // do not choose selected tablet
                    if (selectedTablets.contains(tabletId)) {
                        continue;
                    }
                    // do not choose tablet that exists in backends whose host is same with dest be
                    if (isTabletExistsInBackends(tabletId, destBeHostGroup)) {
                        continue;
                    }

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
                Pair<Long, Long> p = new Pair<>(tabletMeta.getPartitionId(), tabletMeta.getIndexId());
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

                TabletSchedCtx schedCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, clusterName,
                        tabletMeta.getDbId(), tabletMeta.getTableId(), tabletMeta.getPartitionId(),
                        tabletMeta.getIndexId(), tabletId, System.currentTimeMillis());
                schedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
                schedCtx.setSrc(replica);
                schedCtx.setDest(destBEStat.getBeId(), destPathHash);
                schedCtx.setBalanceType(BalanceType.DISK);
                selectedTablets.add(tabletId);
                alternativeTablets.add(schedCtx);
                if (alternativeTablets.size() >= Config.max_balancing_tablets) {
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
    private Map<Pair<Long, Long>, Set<Long>> getPartitionTablets(long beId, TStorageMedium medium) {
        Map<Pair<Long, Long>, Set<Long>> partitionTablets = Maps.newHashMap();
        List<Long> tabletIds = invertedIndex.getTabletIdsByBackendIdAndStorageMedium(beId, medium);
        for (Long tabletId : tabletIds) {
            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
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

    private int getPartitionTabletNumOnBe(long dbId, long tableId, long partitionId, long indexId, long beId) {
        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return 0;
        }

        try {
            db.readLock();
            OlapTable table = (OlapTable) catalog.getTableIncludeRecycleBin(db, tableId);
            if (table == null) {
                return 0;
            }

            Partition partition = catalog.getPartitionIncludeRecycleBin(table, partitionId);
            if (partition == null) {
                return 0;
            }

            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                return 0;
            }

            int cnt = 0;
            for (Tablet tablet : index.getTablets()) {
                List<Replica> replicas = tablet.getReplicas();
                if (replicas == null) {
                    continue;
                }

                for (Replica replica : replicas) {
                    if (replica.getState() == ReplicaState.NORMAL
                            && replica.getBackendId() == beId) {
                        cnt++;
                    }
                }
            }
            return cnt;
        } finally {
            db.readUnlock();
        }
    }

    /**
     * balance Tablet should preserve disk balance
     * 1. for every partition, calculate the distribution skew, skew is (max tablet number on be) - (min tablet number on be)
     * 2. sort partition by skew in desc order
     * 3. for every partition, sort be by tablet number
     * 4. try to copy one tablet from maxTabletsNum be to other be, minTabletsNum be first
     * 5. repeat 3 and 4, until no tablet can copy
     */
    private List<TabletSchedCtx> balanceTablet(String clusterName, ClusterLoadStatistic clusterStat,
                                               TStorageMedium medium) {
        List<TabletSchedCtx> alternativeTablets = Lists.newArrayList();
        List<BackendLoadStatistic> beStatList = getValidBeStats(clusterStat, medium);
        if (beStatList.size() <= 1) {
            return alternativeTablets;
        }

        // beId => (totalCapacity, totalUsedCapacity)
        Map<Long, Pair<Long, Long>> beDiskCap = Maps.newHashMap();
        // beId => (paths, index) , low or mid be disks to hold moved tablets
        Map<Long, Pair<List<Long>, Integer>> beDisks = Maps.newHashMap();
        List<Long> beIds = Lists.newArrayList();
        for (BackendLoadStatistic beStat : beStatList) {
            beDiskCap.put(beStat.getBeId(),
                    new Pair<>(beStat.getTotalCapacityB(medium), beStat.getTotalUsedCapacityB(medium)));
            beIds.add(beStat.getBeId());
            List<Long> pathHashList = Lists.newArrayList();
            for (RootPathLoadStatistic pathStat : beStat.getPathStatistics()) {
                if (pathStat.getStorageMedium() == medium
                        && (pathStat.getClazz() == Classification.LOW || pathStat.getClazz() == Classification.MID)) {
                    pathHashList.add(pathStat.getPathHash());
                }
            }
            beDisks.put(beStat.getBeId(), new Pair<>(pathHashList, 0));
        }

        Map<Pair<Long, Long>, PartitionStat> partitionStats = getPartitionStats(beIds, medium);

        List<Pair<Long, Long>> partitions = new ArrayList<>(partitionStats.keySet());
        // sort all partition by distribution skew in desc order, skew is (max tablet number on be) - (min tablet number on be)
        partitions.sort((o1, o2) -> {
            PartitionStat pStat1 = partitionStats.get(o1);
            PartitionStat pStat2 = partitionStats.get(o2);
            return pStat2.skew - pStat1.skew;
        });

        DiskBalanceChecker diskBalanceChecker = new DiskBalanceChecker(beDiskCap);
        diskBalanceChecker.init();
        Set<Long> selectedTablets = Sets.newHashSet();
        List<Long> aliveBeIds = infoService.getBackendIds(true);
        Map<String, List<Long>> hostGroups = getHostGroups(aliveBeIds);
        OUT:
        for (Pair<Long, Long> partition : partitions) {
            PartitionStat pStat = partitionStats.get(partition);
            // skew <= 1 means partition is balanced
            if (pStat.skew <= 1) {
                break;
            }

            List<Pair<Long, Set<Long>>> beTablets =
                    getPartitionTabletsOnAllBE(pStat.dbId, pStat.tableId, partition.first, partition.second, beIds);
            boolean tabletFound;
            do {
                tabletFound = false;
                // sort be by tablets num in desc order, and used percent in desc order for bes with same tablets num
                beTablets.sort((t1, t2) -> {
                    if (t1.second.size() != t2.second.size()) {
                        return t2.second.size() - t1.second.size();
                    } else {
                        double diff = diskBalanceChecker.getBeDiskUsedPercent(t2.first) -
                                diskBalanceChecker.getBeDiskUsedPercent(t1.first);
                        if (Math.abs(diff) < 1e-6) {
                            return 0;
                        } else if (diff > 0) {
                            return 1;
                        } else {
                            return -1;
                        }
                    }
                });

                // try to copy one tablet from maxTabletsNum be to other be, minTabletsNum be first.
                Pair<Long, Set<Long>> srcBETablets = beTablets.get(0);
                for (int i = beTablets.size() - 1; i > 0; i--) {
                    Pair<Long, Set<Long>> destBETablets = beTablets.get(i);

                    // partition is balanced
                    if (srcBETablets.second.size() - destBETablets.second.size() <= 1) {
                        break;
                    }

                    Backend destBackend = infoService.getBackend(destBETablets.first);
                    if (destBackend == null) {
                        continue;
                    }

                    TabletSchedCtx schedCtx =
                            tryToBalanceTablet(clusterName, srcBETablets, destBETablets, diskBalanceChecker,
                                    selectedTablets, aliveBeIds, hostGroups.get(destBackend.getHost()));
                    if (schedCtx != null) {
                        // NOTICE: state has been changed, the tablet must be selected

                        //round robin to select dest be path
                        Pair<List<Long>, Integer> destPaths = beDisks.get(destBETablets.first);
                        Long pathHash = destPaths.first.get(destPaths.second);
                        destPaths.second = (destPaths.second + 1) % destPaths.first.size();

                        schedCtx.setDest(destBETablets.first, pathHash);
                        alternativeTablets.add(schedCtx);
                        if (alternativeTablets.size() >= Config.max_balancing_tablets) {
                            break OUT;
                        }
                        tabletFound = true;
                        break;
                    }
                }
            } while (tabletFound);
        }

        return alternativeTablets;
    }

    private TabletSchedCtx tryToBalanceTablet(String clusterName, Pair<Long, Set<Long>> srcBE,
                                              Pair<Long, Set<Long>> destBE,
                                              DiskBalanceChecker diskBalanceChecker, Set<Long> selectedTablets,
                                              List<Long> aliveBeIds,
                                              List<Long> destBackendHostGroup) {
        for (Long tabletId : srcBE.second) {
            if (isTabletExistsInBackends(tabletId, destBackendHostGroup)) {
                continue;
            }

            if (selectedTablets.contains(tabletId)) {
                continue;
            }

            Replica replica = invertedIndex.getReplica(tabletId, srcBE.first);
            if (replica == null || replica.getPathHash() == -1L) {
                continue;
            }

            if (!diskBalanceChecker.check(srcBE.first, destBE.first, replica.getDataSize())) {
                continue;
            }

            TabletMeta tabletMeta = invertedIndex.getTabletMeta(tabletId);
            if (tabletMeta == null) {
                continue;
            }

            if (!isTabletHealthy(tabletId, tabletMeta, aliveBeIds)) {
                continue;
            }

            TabletSchedCtx schedCtx = new TabletSchedCtx(TabletSchedCtx.Type.BALANCE, clusterName,
                    tabletMeta.getDbId(), tabletMeta.getTableId(), tabletMeta.getPartitionId(),
                    tabletMeta.getIndexId(), tabletId, System.currentTimeMillis());
            schedCtx.setOrigPriority(TabletSchedCtx.Priority.LOW);
            schedCtx.setBalanceType(BalanceType.TABLET);
            schedCtx.setSrc(replica);

            // update state
            selectedTablets.add(tabletId);
            diskBalanceChecker.moveReplica(srcBE.first, destBE.first, replica.getDataSize());
            srcBE.second.remove(tabletId);
            destBE.second.add(tabletId);
            return schedCtx;
        }

        return null;
    }

    private List<Pair<Long, Set<Long>>> getPartitionTabletsOnAllBE(Long dbId, Long tableId, Long partitionId,
                                                                   Long indexId, List<Long> beIds) {
        List<Pair<Long, Set<Long>>> result = Lists.newArrayList();
        Catalog catalog = Catalog.getCurrentCatalog();

        Database db = catalog.getDbIncludeRecycleBin(dbId);
        if (db == null) {
            return result;
        }

        try {
            db.readLock();
            OlapTable table = (OlapTable) catalog.getTableIncludeRecycleBin(db, tableId);
            if (table == null) {
                return result;
            }

            Partition partition = catalog.getPartitionIncludeRecycleBin(table, partitionId);
            if (partition == null) {
                return result;
            }

            MaterializedIndex index = partition.getIndex(indexId);
            if (index == null) {
                return result;
            }

            Map<Long, Set<Long>> tabletsOnBE = Maps.newHashMap();
            for (Long beId : beIds) {
                tabletsOnBE.put(beId, Sets.newHashSet());
            }
            for (Tablet tablet : index.getTablets()) {
                List<Replica> replicas = tablet.getReplicas();
                if (replicas == null) {
                    continue;
                }

                for (Replica replica : replicas) {
                    if (replica.getState() != ReplicaState.NORMAL) {
                        continue;
                    }

                    tabletsOnBE.computeIfPresent(replica.getBackendId(), (k, v) -> {
                        v.add(tablet.getId());
                        return v;
                    });
                }
            }

            for (Map.Entry<Long, Set<Long>> entry : tabletsOnBE.entrySet()) {
                result.add(new Pair<>(entry.getKey(), entry.getValue()));
            }
        } finally {
            db.readUnlock();
        }

        return result;
    }

    // NOTICE: call this function as little as possible, cause this will get db lock
    private boolean isTabletHealthy(Long tabletId, TabletMeta tabletMeta, List<Long> aliveBeIds) {
        Catalog catalog = Catalog.getCurrentCatalog();
        Database db = catalog.getDbIncludeRecycleBin(tabletMeta.getDbId());
        if (db == null) {
            return false;
        }

        try {
            db.readLock();
            OlapTable table = (OlapTable) catalog.getTableIncludeRecycleBin(db, tabletMeta.getTableId());
            if (table == null) {
                return false;
            }

            Partition partition = catalog.getPartitionIncludeRecycleBin(table, tabletMeta.getPartitionId());
            if (partition == null) {
                return false;
            }

            MaterializedIndex index = partition.getIndex(tabletMeta.getIndexId());
            if (index == null) {
                return false;
            }

            Tablet tablet = index.getTablet(tabletId);
            if (tablet == null) {
                return false;
            }

            short replicaNum = catalog.getReplicationNumIncludeRecycleBin(table.getPartitionInfo(), partition.getId());
            if (replicaNum == (short) -1) {
                return false;
            }

            Pair<Tablet.TabletStatus, TabletSchedCtx.Priority> statusPair =
                    tablet.getHealthStatusWithPriority(infoService,
                            db.getClusterName(),
                            partition.getVisibleVersion(),
                            partition.getVisibleVersionHash(),
                            replicaNum,
                            aliveBeIds);

            return statusPair.first == Tablet.TabletStatus.HEALTHY;
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

    // beIds can be null, skew will not be set in this case
    private Map<Pair<Long, Long>, PartitionStat> getPartitionStats(List<Long> beIds, TStorageMedium medium) {
        Catalog catalog = Catalog.getCurrentCatalog();
        Map<Pair<Long, Long>, PartitionStat> partitionStats = Maps.newHashMap();
        List<Long> dbIds = catalog.getDbIdsIncludeRecycleBin();
        for (Long dbId : dbIds) {
            Database db = catalog.getDbIncludeRecycleBin(dbId);
            if (db == null) {
                continue;
            }

            if (db.isInfoSchemaDb()) {
                continue;
            }

            db.readLock();
            try {
                for (Table table : catalog.getTablesIncludeRecycleBin(db)) {
                    if (!table.needSchedule()) {
                        continue;
                    }

                    OlapTable olapTbl = (OlapTable) table;
                    for (Partition partition : catalog.getAllPartitionsIncludeRecycleBin(olapTbl)) {
                        if (partition.getState() != PartitionState.NORMAL) {
                            // when alter job is in FINISHING state, partition state will be set to NORMAL,
                            // and we can schedule the tablets in it.
                            continue;
                        }

                        DataProperty dataProperty = catalog.getDataPropertyIncludeRecycleBin(olapTbl.getPartitionInfo(),
                                partition.getId());
                        if (dataProperty == null) {
                            continue;
                        }
                        TStorageMedium pMedium = dataProperty.getStorageMedium();
                        if (pMedium != medium) {
                            continue;
                        }

                        int replicaNum = partition.getDistributionInfo().getBucketNum()
                                * catalog.getReplicationNumIncludeRecycleBin(olapTbl.getPartitionInfo(),
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

                            if (beIds == null || beIds.size() <= 0) {
                                continue;
                            }

                            // calculate skew
                            Map<Long, Integer> replicaNumOnBE = Maps.newHashMap();
                            for (Long beId : beIds) {
                                replicaNumOnBE.put(beId, 0);
                            }
                            for (Tablet tablet : idx.getTablets()) {
                                List<Replica> replicas = tablet.getReplicas();
                                if (replicas != null) {
                                    for (Replica replica : replicas) {
                                        if (replica.getState() != ReplicaState.NORMAL) {
                                            continue;
                                        }

                                        replicaNumOnBE.computeIfPresent(replica.getBackendId(), (k, v) -> (v + 1));
                                    }
                                }
                            }
                            int maxNum = Integer.MIN_VALUE;
                            int minNum = Integer.MAX_VALUE;
                            for (int num : replicaNumOnBE.values()) {
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
                db.readUnlock();
            }
        }

        return partitionStats;
    }

    // used to check disk balance when doing tablet distribution balance
    // we check disk balance using 0.9 * Config.balance_load_score_threshold to avoid trigger disk unbalance
    // todo optimization using segment tree
    public static class DiskBalanceChecker {
        // beId => (totalCapacity, totalUsedCapacity)
        Map<Long, Pair<Long, Long>> beDiskCap;
        double maxUsedPercent;
        double minUsedPercent;

        public DiskBalanceChecker(Map<Long, Pair<Long, Long>> beDiskCap) {
            this.beDiskCap = beDiskCap;
        }

        public double getBeDiskUsedPercent(Long beId) {
            Pair<Long, Long> cap = beDiskCap.get(beId);
            if (cap == null) {
                return 0;
            }
            return (double) cap.second / cap.first;
        }

        public void init() {
            maxUsedPercent = Double.MIN_VALUE;
            minUsedPercent = Double.MAX_VALUE;
            for (Map.Entry<Long, Pair<Long, Long>> entry : beDiskCap.entrySet()) {
                double usedPercent = ((double) entry.getValue().second) / entry.getValue().first;
                if (usedPercent > maxUsedPercent) {
                    maxUsedPercent = usedPercent;
                }
                if (usedPercent < minUsedPercent) {
                    minUsedPercent = usedPercent;
                }
            }
        }

        public boolean check(Long srcBeId, Long destBeId, Long size) {
            Pair<Long, Long> srcBeCap = beDiskCap.get(srcBeId);
            Pair<Long, Long> destBeCap = beDiskCap.get(destBeId);
            double srcBeUsedPercent = (double) (srcBeCap.second - size) / srcBeCap.first;
            double destBeUsedPercent = (double) (destBeCap.second + size) / destBeCap.first;

            // first check dest be capacity limit
            if ((destBeUsedPercent > (Config.storage_flood_stage_usage_percent / 100.0)) ||
                    ((destBeCap.first - destBeCap.second - size) < Config.storage_flood_stage_left_capacity_bytes)) {
                return false;
            }

            double maxUsedPercentAfterBalance = Double.MIN_VALUE;
            double minUsedPercentAfterBalance = Double.MAX_VALUE;
            for (Map.Entry<Long, Pair<Long, Long>> entry : beDiskCap.entrySet()) {
                double usedPercent = 0.0;
                if (entry.getKey().equals(srcBeId)) {
                    usedPercent = srcBeUsedPercent;
                } else if (entry.getKey().equals(destBeId)) {
                    usedPercent = destBeUsedPercent;
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
            if (maxUsedPercentAfterBalance < Config.balance_load_disk_safe_threshold) {
                return true;
            }

            // this will make disk balance better
            if (maxUsedPercentAfterBalance - minUsedPercentAfterBalance < maxUsedPercent - minUsedPercent) {
                return true;
            }

            // this will make disk balance worse, but can not exceed 0.9 * Config.balance_load_score_threshold;
            return maxUsedPercentAfterBalance - minUsedPercentAfterBalance < 0.9 * Config.balance_load_score_threshold;
        }

        public void moveReplica(Long srcBeId, Long destBeId, Long size) {
            Pair<Long, Long> srcBeCap = beDiskCap.get(srcBeId);
            Pair<Long, Long> destBeCap = beDiskCap.get(destBeId);
            srcBeCap.second -= size;
            destBeCap.second += size;

            init();
        }
    }

    public enum BalanceType {
        DISK,
        TABLET
    }
}
