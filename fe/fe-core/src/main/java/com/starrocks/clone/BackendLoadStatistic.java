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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/BackendLoadStatistic.java

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.DiskInfo.DiskState;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.clone.BalanceStatus.ErrCode;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class BackendLoadStatistic {
    private static final Logger LOG = LogManager.getLogger(BackendLoadStatistic.class);

    // comparator based on load score and storage medium, smaller load score first
    public static class BeStatComparator implements Comparator<BackendLoadStatistic> {
        private TStorageMedium medium;

        public BeStatComparator(TStorageMedium medium) {
            this.medium = medium;
        }

        @Override
        public int compare(BackendLoadStatistic o1, BackendLoadStatistic o2) {
            double score1 = o1.getLoadScore(medium);
            double score2 = o2.getLoadScore(medium);
            return Double.compare(score1, score2);
        }
    }

    public static class BeStatMixComparator implements Comparator<BackendLoadStatistic> {
        @Override
        public int compare(BackendLoadStatistic o1, BackendLoadStatistic o2) {
            Double score1 = o1.getMixLoadScore();
            Double score2 = o2.getMixLoadScore();
            return score1.compareTo(score2);
        }
    }

    public static class BeStatComparatorForUsedPercent implements Comparator<BackendLoadStatistic> {
        private TStorageMedium medium;

        public BeStatComparatorForUsedPercent(TStorageMedium medium) {
            this.medium = medium;
        }

        @Override
        public int compare(BackendLoadStatistic o1, BackendLoadStatistic o2) {
            double percent1 = o1.getUsedPercent(medium);
            double percent2 = o2.getUsedPercent(medium);
            return Double.compare(percent1, percent2);
        }
    }

    public static class BeStatComparatorForPathUsedPercentSkew implements Comparator<BackendLoadStatistic> {
        private TStorageMedium medium;

        public BeStatComparatorForPathUsedPercentSkew(TStorageMedium medium) {
            this.medium = medium;
        }

        @Override
        public int compare(BackendLoadStatistic o1, BackendLoadStatistic o2) {
            Pair<Double, Double> maxMinUsedPercent1 = o1.getMaxMinPathUsedPercent(medium);
            Pair<Double, Double> maxMinUsedPercent2 = o2.getMaxMinPathUsedPercent(medium);
            double skew1 = 0;
            double skew2 = 0;
            if (maxMinUsedPercent1 != null) {
                skew1 = maxMinUsedPercent1.first - maxMinUsedPercent1.second;
            }
            if (maxMinUsedPercent2 != null) {
                skew2 = maxMinUsedPercent2.first - maxMinUsedPercent2.second;
            }
            return Double.compare(skew2, skew1);
        }
    }

    public static final BeStatComparator HDD_COMPARATOR = new BeStatComparator(TStorageMedium.HDD);
    public static final BeStatComparator SSD_COMPARATOR = new BeStatComparator(TStorageMedium.SSD);
    public static final BeStatMixComparator MIX_COMPARATOR = new BeStatMixComparator();

    public enum Classification {
        INIT,
        LOW, // load score is Config.balance_load_score_threshold lower than average load score of cluster
        MID, // between LOW and HIGH
        HIGH // load score is Config.balance_load_score_threshold higher than average load score of cluster
    }

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    private long beId;
    private String clusterName;

    private boolean isAvailable;

    public static class LoadScore {
        public double replicaNumCoefficient = 0.5;
        public double capacityCoefficient = 0.5;
        public double score = 0.0;

        public static final LoadScore DUMMY = new LoadScore();
    }

    private Map<TStorageMedium, Long> totalCapacityMap = Maps.newHashMap();
    private Map<TStorageMedium, Long> totalUsedCapacityMap = Maps.newHashMap();
    private Map<TStorageMedium, Long> totalReplicaNumMap = Maps.newHashMap();
    private Map<TStorageMedium, LoadScore> loadScoreMap = Maps.newHashMap();
    private Map<TStorageMedium, Classification> clazzMap = Maps.newHashMap();
    private List<RootPathLoadStatistic> pathStatistics = Lists.newArrayList();

    public BackendLoadStatistic(long beId, String clusterName, SystemInfoService infoService,
                                TabletInvertedIndex invertedIndex) {
        this.beId = beId;
        this.clusterName = clusterName;
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public long getBeId() {
        return beId;
    }

    public boolean isAvailable() {
        return isAvailable;
    }

    public long getTotalCapacityB(TStorageMedium medium) {
        return totalCapacityMap.getOrDefault(medium, 0L);
    }

    public long getTotalUsedCapacityB(TStorageMedium medium) {
        return totalUsedCapacityMap.getOrDefault(medium, 0L);
    }

    public double getUsedPercent(TStorageMedium medium) {
        double totalCapacity = getTotalCapacityB(medium);
        if (totalCapacity > 0) {
            return ((double) getTotalUsedCapacityB(medium)) / totalCapacity;
        }
        return 0.0;
    }

    // Get max|min path used percent.
    // Return Pair<max, min>, return null if be has no medium path.
    public Pair<Double, Double> getMaxMinPathUsedPercent(TStorageMedium medium) {
        List<RootPathLoadStatistic> pathStats = getPathStatistics(medium);
        if (pathStats.isEmpty()) {
            return null;
        }

        double maxUsedPercent = Double.MIN_VALUE;
        double minUsedPercent = Double.MAX_VALUE;
        for (RootPathLoadStatistic pathStat : pathStats) {
            if (pathStat.getDiskState() == DiskState.OFFLINE) {
                continue;
            }

            double usedPercent = pathStat.getUsedPercent();
            if (usedPercent > maxUsedPercent) {
                maxUsedPercent = usedPercent;
            }
            if (usedPercent < minUsedPercent) {
                minUsedPercent = usedPercent;
            }
        }
        return Pair.create(maxUsedPercent, minUsedPercent);
    }

    public long getReplicaNum(TStorageMedium medium) {
        return totalReplicaNumMap.getOrDefault(medium, 0L);
    }

    public double getLoadScore(TStorageMedium medium) {
        if (loadScoreMap.containsKey(medium)) {
            return loadScoreMap.get(medium).score;
        }
        return 0.0;
    }

    public double getMixLoadScore() {
        int mediumCount = 0;
        double totalLoadScore = 0.0;
        for (TStorageMedium medium : TStorageMedium.values()) {
            if (hasMedium(medium)) {
                mediumCount++;
                totalLoadScore += getLoadScore(medium);
            }
        }
        return totalLoadScore / (mediumCount == 0 ? 1 : mediumCount);
    }

    public void setClazz(TStorageMedium medium, Classification clazz) {
        this.clazzMap.put(medium, clazz);
    }

    public Classification getClazz(TStorageMedium medium) {
        return clazzMap.getOrDefault(medium, Classification.INIT);
    }

    public void init() throws LoadBalanceException {
        Backend be = infoService.getBackend(beId);
        if (be == null) {
            throw new LoadBalanceException("backend " + beId + " does not exist");
        }

        isAvailable = be.isAvailable();

        ImmutableMap<String, DiskInfo> disks = be.getDisks();
        for (DiskInfo diskInfo : disks.values()) {
            TStorageMedium medium = diskInfo.getStorageMedium();
            if (diskInfo.getState() == DiskState.ONLINE) {
                // we only collect online disk's capacity
                totalCapacityMap
                        .put(medium, totalCapacityMap.getOrDefault(medium, 0L) + diskInfo.getDataTotalCapacityB());
                totalUsedCapacityMap
                        .put(medium, totalUsedCapacityMap.getOrDefault(medium, 0L) + diskInfo.getDataUsedCapacityB());
            }

            RootPathLoadStatistic pathStatistic = new RootPathLoadStatistic(beId, diskInfo.getRootPath(),
                    diskInfo.getPathHash(), diskInfo.getStorageMedium(),
                    diskInfo.getDataTotalCapacityB(), diskInfo.getDataUsedCapacityB(), diskInfo.getState());
            pathStatistics.add(pathStatistic);
        }

        totalReplicaNumMap = invertedIndex.getReplicaNumByBeIdAndStorageMedium(beId);
        // This is very tricky. because the number of replica on specified medium we get
        // from getReplicaNumByBeIdAndStorageMedium() is defined by table properties,
        // but in fact there may not has SSD disk on this backend. So if we found that no SSD disk on this
        // backend, set the replica number to 0, otherwise, the average replica number on specified medium
        // will be incorrect.
        for (TStorageMedium medium : TStorageMedium.values()) {
            if (!hasMedium(medium)) {
                totalReplicaNumMap.put(medium, 0L);
            }
        }

        for (TStorageMedium storageMedium : TStorageMedium.values()) {
            classifyPathByLoad(storageMedium);
        }

        // sort the list
        Collections.sort(pathStatistics);
    }

    private void classifyPathByLoad(TStorageMedium medium) {
        long totalCapacity = 0;
        long totalUsedCapacity = 0;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getStorageMedium() == medium) {
                totalCapacity += pathStat.getCapacityB();
                totalUsedCapacity += pathStat.getUsedCapacityB();
            }
        }
        double avgUsedPercent = totalCapacity == 0 ? 0.0 : totalUsedCapacity / (double) totalCapacity;

        int lowCounter = 0;
        int midCounter = 0;
        int highCounter = 0;
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getStorageMedium() != medium) {
                continue;
            }

            if (Math.abs(pathStat.getUsedPercent() - avgUsedPercent)
                    / (avgUsedPercent == 0.0 ? 1 : avgUsedPercent) > Config.tablet_sched_balance_load_score_threshold) {
                if (pathStat.getUsedPercent() > avgUsedPercent) {
                    pathStat.setClazz(Classification.HIGH);
                    highCounter++;
                } else if (pathStat.getUsedPercent() < avgUsedPercent) {
                    pathStat.setClazz(Classification.LOW);
                    lowCounter++;
                }
            } else {
                pathStat.setClazz(Classification.MID);
                midCounter++;
            }
        }

        LOG.debug("classify path by load. backend: {}, medium: {}, avg used percent: {}. low/mid/high: {}/{}/{}",
                beId, medium, avgUsedPercent, lowCounter, midCounter, highCounter);
    }

    public void calcScore(Map<TStorageMedium, Double> avgClusterUsedCapacityPercentMap,
                          Map<TStorageMedium, Double> avgClusterReplicaNumPerBackendMap) {

        for (TStorageMedium medium : TStorageMedium.values()) {
            LoadScore loadScore = calcSore(totalUsedCapacityMap.getOrDefault(medium, 0L),
                    totalCapacityMap.getOrDefault(medium, 1L),
                    totalReplicaNumMap.getOrDefault(medium, 0L),
                    avgClusterUsedCapacityPercentMap.getOrDefault(medium, 0.0),
                    avgClusterReplicaNumPerBackendMap.getOrDefault(medium, 0.0));

            loadScoreMap.put(medium, loadScore);

            LOG.debug("backend {}, medium: {}, capacity coefficient: {}, replica coefficient: {}, load score: {}",
                    beId, medium, loadScore.capacityCoefficient, loadScore.replicaNumCoefficient, loadScore.score);
        }
    }

    public static LoadScore calcSore(long beUsedCapacityB, long beTotalCapacityB, long beTotalReplicaNum,
                                     double avgClusterUsedCapacityPercent, double avgClusterReplicaNumPerBackend) {

        double usedCapacityPercent = (beUsedCapacityB / (double) beTotalCapacityB);
        double capacityProportion = avgClusterUsedCapacityPercent <= 0 ? 0.0
                : usedCapacityPercent / avgClusterUsedCapacityPercent;
        double replicaNumProportion = avgClusterReplicaNumPerBackend <= 0 ? 0.0
                : beTotalReplicaNum / avgClusterReplicaNumPerBackend;

        LoadScore loadScore = new LoadScore();

        // If this backend's capacity used percent < 50%, set capacityCoefficient to 0.5.
        // Else if capacity used percent > 75%, set capacityCoefficient to 1.
        // Else, capacityCoefficient changed smoothly from 0.5 to 1 with used capacity increasing
        // Function: (2 * usedCapacityPercent - 0.5)
        loadScore.capacityCoefficient = usedCapacityPercent < 0.5 ? 0.5
                : (usedCapacityPercent > Config.capacity_used_percent_high_water ? 1.0
                : (2 * usedCapacityPercent - 0.5));
        loadScore.replicaNumCoefficient = 1 - loadScore.capacityCoefficient;
        loadScore.score = capacityProportion * loadScore.capacityCoefficient
                + replicaNumProportion * loadScore.replicaNumCoefficient;

        return loadScore;
    }

    public BalanceStatus isFit(long tabletSize, TStorageMedium medium,
                               List<RootPathLoadStatistic> result, boolean isSupplement) {
        BalanceStatus status = new BalanceStatus(ErrCode.COMMON_ERROR);
        // try choosing path from first to end (low usage to high usage)
        List<RootPathLoadStatistic> mediumNotMatchedPath = Lists.newArrayList();
        for (RootPathLoadStatistic pathStatistic : pathStatistics) {
            if (pathStatistic.getStorageMedium() != medium) {
                mediumNotMatchedPath.add(pathStatistic);
                continue;
            }

            BalanceStatus bStatus = pathStatistic.isFit(tabletSize, isSupplement);
            if (!bStatus.ok()) {
                status.addErrMsgs(bStatus.getErrMsgs());
                continue;
            }

            result.add(pathStatistic);
            return BalanceStatus.OK;
        }

        // if this is a supplement task, ignore the storage medium
        if (isSupplement || !Config.enable_strict_storage_medium_check) {
            for (RootPathLoadStatistic filteredPathStatistic : mediumNotMatchedPath) {
                BalanceStatus bStatus = filteredPathStatistic.isFit(tabletSize, isSupplement);
                if (!bStatus.ok()) {
                    status.addErrMsgs(bStatus.getErrMsgs());
                    continue;
                }

                result.add(filteredPathStatistic);
                return BalanceStatus.OK;
            }
        }
        return status;
    }

    public boolean hasAvailDisk() {
        for (RootPathLoadStatistic rootPathLoadStatistic : pathStatistics) {
            if (rootPathLoadStatistic.getDiskState() == DiskState.ONLINE) {
                return true;
            }
        }
        return false;
    }

    /**
     * Classify the paths into 'low', 'mid' and 'high',
     * and skip offline path, and path with different storage medium
     */
    public void getPathStatisticByClass(
            Set<Long> low, Set<Long> mid, Set<Long> high, TStorageMedium storageMedium) {

        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getDiskState() == DiskState.OFFLINE
                    || (storageMedium != null && pathStat.getStorageMedium() != storageMedium)) {
                continue;
            }

            if (pathStat.getClazz() == Classification.LOW) {
                low.add(pathStat.getPathHash());
            } else if (pathStat.getClazz() == Classification.HIGH) {
                high.add(pathStat.getPathHash());
            } else {
                mid.add(pathStat.getPathHash());
            }
        }

        LOG.debug("after adjust, backend {}, medium: {}, path classification low/mid/high: {}/{}/{}",
                beId, storageMedium, low.size(), mid.size(), high.size());
    }

    public Set<Long> getPathStatisticForMIDAndClazz(Classification clazz, TStorageMedium storageMedium) {
        Set<Long> paths = Sets.newHashSet();
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            if (pathStat.getDiskState() == DiskState.OFFLINE
                    || (storageMedium != null && pathStat.getStorageMedium() != storageMedium)) {
                continue;
            }

            if (pathStat.getClazz() == clazz || pathStat.getClazz() == Classification.MID) {
                paths.add(pathStat.getPathHash());
            }
        }
        return paths;
    }

    public List<RootPathLoadStatistic> getPathStatistics() {
        return pathStatistics;
    }

    public List<RootPathLoadStatistic> getPathStatistics(TStorageMedium storageMedium) {
        return pathStatistics.stream().filter(p -> p.getStorageMedium() == storageMedium).collect(Collectors.toList());
    }

    public RootPathLoadStatistic getPathStatistic(long pathHash) {
        Optional<RootPathLoadStatistic> pathStat = pathStatistics.stream().filter(
                p -> p.getPathHash() == pathHash).findFirst();
        return pathStat.isPresent() ? pathStat.get() : null;
    }

    public long getAvailPathNum(TStorageMedium medium) {
        return pathStatistics.stream().filter(
                p -> p.getDiskState() == DiskState.ONLINE && p.getStorageMedium() == medium).count();
    }

    public boolean hasMedium(TStorageMedium medium) {
        for (RootPathLoadStatistic rootPathLoadStatistic : pathStatistics) {
            if (rootPathLoadStatistic.getStorageMedium() == medium) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("be id: ").append(beId).append(", is available: ").append(isAvailable).append(", mediums: [");
        for (TStorageMedium medium : TStorageMedium.values()) {
            sb.append("{medium: ").append(medium).append(", replica: ").append(totalReplicaNumMap.get(medium));
            sb.append(", used: ").append(totalUsedCapacityMap.getOrDefault(medium, 0L));
            final Long totalCapacity = totalCapacityMap.getOrDefault(medium, 0L);
            sb.append(", total: ").append(new ByteSizeValue(totalCapacity));
            sb.append(", score: ").append(loadScoreMap.getOrDefault(medium, LoadScore.DUMMY).score).append("},");
        }
        sb.append("], paths: [");
        for (RootPathLoadStatistic pathStat : pathStatistics) {
            sb.append("{").append(pathStat).append("},");
        }
        return sb.append("]").toString();
    }

    public List<String> getInfo(TStorageMedium medium) {
        List<String> info = Lists.newArrayList();
        info.add(String.valueOf(beId));
        info.add(clusterName);
        info.add(String.valueOf(isAvailable));
        long used = totalUsedCapacityMap.getOrDefault(medium, 0L);
        long total = totalCapacityMap.getOrDefault(medium, 0L);
        info.add(String.valueOf(used));
        info.add(String.valueOf(total));
        info.add(String.valueOf(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(used * 100
                / (double) total)));
        info.add(String.valueOf(totalReplicaNumMap.getOrDefault(medium, 0L)));
        LoadScore loadScore = loadScoreMap.getOrDefault(medium, new LoadScore());
        info.add(String.valueOf(loadScore.capacityCoefficient));
        info.add(String.valueOf(loadScore.replicaNumCoefficient));
        info.add(String.valueOf(loadScore.score));
        info.add(clazzMap.getOrDefault(medium, Classification.INIT).name());
        return info;
    }
}
