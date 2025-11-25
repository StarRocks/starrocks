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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/clone/ClusterLoadStatistic.java

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
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.clone.BackendLoadStatistic.Classification;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/*
 * Load statistics of a cluster
 */
public class ClusterLoadStatistic {
    private static final Logger LOG = LogManager.getLogger(ClusterLoadStatistic.class);

    private SystemInfoService infoService;
    private TabletInvertedIndex invertedIndex;

    private Map<TStorageMedium, Long> totalCapacityMap = Maps.newHashMap();
    private Map<TStorageMedium, Long> totalUsedCapacityMap = Maps.newHashMap();
    private Map<TStorageMedium, Long> totalReplicaNumMap = Maps.newHashMap();
    private Map<TStorageMedium, Double> avgUsedCapacityPercentMap = Maps.newHashMap();
    private Map<TStorageMedium, Double> avgReplicaNumPercentMap = Maps.newHashMap();
    private Map<TStorageMedium, Double> avgLoadScoreMap = Maps.newHashMap();
    // storage medium -> number of backend which has this kind of medium
    private Map<TStorageMedium, Integer> backendNumMap = Maps.newHashMap();
    private List<BackendLoadStatistic> beLoadStatistics = Lists.newArrayList();
    private Map<Long, String> pathHashToRootPath = Maps.newHashMap();

    // Disk balance stats
    // TStorageMedium -> BalanceStat
    private final Map<TStorageMedium, BalanceStat> clusterDiskBalanceStats = new ConcurrentHashMap<>();
    // Pair<TStorageMedium, BeId> -> BalanceStat
    private final Map<Pair<TStorageMedium, Long>, BalanceStat> backendDiskBalanceStats = new ConcurrentHashMap<>();

    public ClusterLoadStatistic(SystemInfoService infoService, TabletInvertedIndex invertedIndex) {
        this.infoService = infoService;
        this.invertedIndex = invertedIndex;
    }

    public void init() {
        ImmutableMap<Long, Backend> backends = infoService.getIdToBackend();
        for (Backend backend : backends.values()) {
            BackendLoadStatistic beStatistic = new BackendLoadStatistic(backend.getId(),
                    SystemInfoService.DEFAULT_CLUSTER, infoService, invertedIndex);
            try {
                beStatistic.init();
            } catch (LoadBalanceException e) {
                LOG.info(e.getMessage());
                continue;
            }

            for (TStorageMedium medium : TStorageMedium.values()) {
                totalCapacityMap
                        .put(medium, totalCapacityMap.getOrDefault(medium, 0L) + beStatistic.getTotalCapacityB(medium));
                totalUsedCapacityMap.put(medium,
                        totalUsedCapacityMap.getOrDefault(medium, 0L) + beStatistic.getTotalUsedCapacityB(medium));
                totalReplicaNumMap
                        .put(medium, totalReplicaNumMap.getOrDefault(medium, 0L) + beStatistic.getReplicaNum(medium));
                if (beStatistic.hasMedium(medium)) {
                    backendNumMap.put(medium, backendNumMap.getOrDefault(medium, 0) + 1);
                }
            }

            beLoadStatistics.add(beStatistic);
        }

        for (TStorageMedium medium : TStorageMedium.values()) {
            avgUsedCapacityPercentMap.put(medium,
                    totalUsedCapacityMap.getOrDefault(medium, 0L) / (double) totalCapacityMap.getOrDefault(medium, 1L));
            avgReplicaNumPercentMap.put(medium,
                    totalReplicaNumMap.getOrDefault(medium, 0L) / (double) backendNumMap.getOrDefault(medium, 1));
        }

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            beStatistic.calcScore(avgUsedCapacityPercentMap, avgReplicaNumPercentMap);
        }

        // classify all backends
        for (TStorageMedium medium : TStorageMedium.values()) {
            classifyBackendByLoad(medium);
        }

        // sort be stats by mix load score
        Collections.sort(beLoadStatistics, BackendLoadStatistic.MIX_COMPARATOR);

        // create path hash to root path map
        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            for (RootPathLoadStatistic pathLoadStat : beStatistic.getPathStatistics()) {
                pathHashToRootPath.put(pathLoadStat.getPathHash(), pathLoadStat.getPath());
            }
        }
    }

    /*
     * classify backends into 'low', 'mid' and 'high', by load
     */
    private void classifyBackendByLoad(TStorageMedium medium) {
        if (backendNumMap.getOrDefault(medium, 0) == 0) {
            return;
        }
        double totalLoadScore = 0.0;
        for (BackendLoadStatistic beStat : beLoadStatistics) {
            totalLoadScore += beStat.getLoadScore(medium);
        }
        int numBe = backendNumMap.get(medium);
        double avgLoadScore = totalLoadScore / (numBe == 0 ? 1 : numBe);
        avgLoadScoreMap.put(medium, avgLoadScore);

        int lowCounter = 0;
        int midCounter = 0;
        int highCounter = 0;
        for (BackendLoadStatistic beStat : beLoadStatistics) {
            if (!beStat.hasMedium(medium)) {
                continue;
            }

            if (Math.abs(beStat.getLoadScore(medium) - avgLoadScore) / (avgLoadScore == 0.0 ? 1.0 : avgLoadScore) >
                    Config.tablet_sched_balance_load_score_threshold) {
                if (beStat.getLoadScore(medium) > avgLoadScore) {
                    beStat.setClazz(medium, Classification.HIGH);
                    highCounter++;
                } else if (beStat.getLoadScore(medium) < avgLoadScore) {
                    beStat.setClazz(medium, Classification.LOW);
                    lowCounter++;
                }
            } else {
                beStat.setClazz(medium, Classification.MID);
                midCounter++;
            }
        }

        LOG.info("classify backend by load. medium: {}, avg load score: {}, low/mid/high: {}/{}/{}",
                medium, avgLoadScore, lowCounter, midCounter, highCounter);
    }

    private static void sortBeStats(List<BackendLoadStatistic> beStats, TStorageMedium medium) {
        if (medium == null) {
            Collections.sort(beStats, BackendLoadStatistic.MIX_COMPARATOR);
        } else if (medium == TStorageMedium.HDD) {
            Collections.sort(beStats, BackendLoadStatistic.HDD_COMPARATOR);
        } else {
            Collections.sort(beStats, BackendLoadStatistic.SSD_COMPARATOR);
        }
    }

    public Set<TStorageMedium> getStorageMediums() {
        return beLoadStatistics.stream().flatMap(s -> s.getPathStatistics().stream()).map(RootPathLoadStatistic::getStorageMedium)
                .filter(Objects::nonNull).collect(Collectors.toSet());
    }

    public List<List<String>> getClusterLoadStats() {
        List<List<String>> stats = Lists.newArrayList();
        for (TStorageMedium medium : getStorageMediums()) {
            stats.add(Lists.newArrayList(medium.name(), getClusterDiskBalanceStat(medium).toString()));
        }
        return stats;
    }

    public List<List<String>> getBackendLoadStats(TStorageMedium medium) {
        List<List<String>> stats = Lists.newArrayList();

        for (BackendLoadStatistic beStatistic : beLoadStatistics) {
            if (!beStatistic.hasMedium(medium)) {
                continue;
            }
            List<String> beStat = beStatistic.getInfo(medium, getBackendDiskBalanceStat(medium, beStatistic.getBeId()));
            stats.add(beStat);
        }

        return stats;
    }

    public BackendLoadStatistic getBackendLoadStatistic(long beId) {
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            if (backendLoadStatistic.getBeId() == beId) {
                return backendLoadStatistic;
            }
        }
        return null;
    }

    public RootPathLoadStatistic getRootPathLoadStatistic(long beId, long pathHash) {
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            if (backendLoadStatistic.getBeId() == beId) {
                return backendLoadStatistic.getPathStatistic(pathHash);
            }
        }
        return null;
    }

    public List<BackendLoadStatistic> getAllBackendLoadStatistic() {
        return beLoadStatistics;
    }

    public List<BackendLoadStatistic> getSortedBeLoadStats(TStorageMedium medium) {
        if (medium != null) {
            List<BackendLoadStatistic> beStatsWithMedium = beLoadStatistics.stream().filter(
                    b -> b.hasMedium(medium)).collect(Collectors.toList());
            sortBeStats(beStatsWithMedium, medium);
            return beStatsWithMedium;
        } else {
            // be stats are already sorted by mix load score in init()
            return beLoadStatistics;
        }
    }

    public String getBrief() {
        StringBuilder sb = new StringBuilder();
        for (BackendLoadStatistic backendLoadStatistic : beLoadStatistics) {
            sb.append("    ").append(backendLoadStatistic).append("\n");
        }
        return sb.toString();
    }

    public String getPath(long pathHash) {
        return pathHashToRootPath.get(pathHash);
    }

    public BalanceStat getClusterDiskBalanceStat(TStorageMedium medium) {
        return clusterDiskBalanceStats.getOrDefault(medium, BalanceStat.BALANCED_STAT);
    }

    public Map<TStorageMedium, BalanceStat> getClusterDiskBalanceStats() {
        return clusterDiskBalanceStats;
    }

    public void updateClusterDiskBalanceStat(TStorageMedium medium, BalanceStat balanceStat) {
        clusterDiskBalanceStats.put(medium, balanceStat);
    }

    public void updateClusterDiskBalanceStats(Map<TStorageMedium, BalanceStat> clusterDiskBalanceStats) {
        this.clusterDiskBalanceStats.putAll(clusterDiskBalanceStats);
    }

    public BalanceStat getBackendDiskBalanceStat(TStorageMedium medium, long beId) {
        return backendDiskBalanceStats.getOrDefault(Pair.create(medium, beId), BalanceStat.BALANCED_STAT);
    }

    public Map<Pair<TStorageMedium, Long>, BalanceStat> getBackendDiskBalanceStats() {
        return backendDiskBalanceStats;
    }

    public void updateBackendDiskBalanceStat(Pair<TStorageMedium, Long> mediumBeId, BalanceStat balanceStat) {
        backendDiskBalanceStats.put(mediumBeId, balanceStat);
    }

    public void updateBackendDiskBalanceStats(Map<Pair<TStorageMedium, Long>, BalanceStat> backendDiskBalanceStats) {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (BackendLoadStatistic beStat : beLoadStatistics) {
            long beId = beStat.getBeId();
            if (!systemInfoService.checkBackendAvailable(beId)) {
                continue;
            }

            for (TStorageMedium medium : TStorageMedium.values()) {
                if (beStat.getTotalCapacityB(medium) > 0) {
                    Pair<TStorageMedium, Long> mediumBeId = Pair.create(medium, beId);
                    this.backendDiskBalanceStats.put(
                            mediumBeId, backendDiskBalanceStats.getOrDefault(mediumBeId, BalanceStat.BALANCED_STAT));
                }
            }
        }
    }
}
