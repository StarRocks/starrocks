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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/BackendsProcDir.java

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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.alter.DecommissionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.datacache.DataCacheMetrics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class BackendsProcDir implements ProcDirInterface {
    private static final Logger LOG = LogManager.getLogger(BackendsProcDir.class);

    public static final ImmutableList<String> TITLE_NAMES;
    static {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                .add("BackendId").add("IP").add("HeartbeatPort")
                .add("BePort").add("HttpPort").add("BrpcPort").add("LastStartTime").add("LastHeartbeat")
                .add("Alive").add("SystemDecommissioned").add("ClusterDecommissioned").add("TabletNum")
                .add("DataUsedCapacity").add("AvailCapacity").add("TotalCapacity").add("UsedPct")
                .add("MaxDiskUsedPct").add("ErrMsg").add("Version").add("Status").add("DataTotalCapacity")
                .add("DataUsedPct").add("CpuCores").add("NumRunningQueries").add("MemUsedPct").add("CpuUsedPct")
                .add("DataCacheMetrics")
                .add("location");
        if (RunMode.isSharedDataMode()) {
            builder.add("StarletPort").add("WorkerId");
        }
        TITLE_NAMES = builder.build();
    }

    private SystemInfoService clusterInfoService;

    public BackendsProcDir(SystemInfoService clusterInfoService) {
        this.clusterInfoService = clusterInfoService;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(clusterInfoService);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        final List<List<String>> backendInfos = getClusterBackendInfos();
        for (List<String> backendInfo : backendInfos) {
            List<String> oneInfo = new ArrayList<>(backendInfo.size());
            oneInfo.addAll(backendInfo);
            result.addRow(oneInfo);
        }
        return result;
    }

    // get backends of cluster
    public static List<List<String>> getClusterBackendInfos() {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentSystemInfo();
        List<List<String>> backendInfos = new LinkedList<>();
        List<Long> backendIds = clusterInfoService.getBackendIds(false);
        if (backendIds == null) {
            return backendInfos;
        }

        long start = System.currentTimeMillis();
        Stopwatch watch = Stopwatch.createUnstarted();
        List<List<Comparable>> comparableBackendInfos = new LinkedList<>();
        for (long backendId : backendIds) {
            Backend backend = clusterInfoService.getBackend(backendId);
            if (backend == null) {
                continue;
            }

            watch.start();
            long tabletNum = GlobalStateMgr.getCurrentInvertedIndex().getTabletNumByBackendId(backendId);
            watch.stop();
            List<Comparable> backendInfo = Lists.newArrayList();
            backendInfo.add(String.valueOf(backendId));
            backendInfo.add(backend.getHost());
            backendInfo.add(String.valueOf(backend.getHeartbeatPort()));
            backendInfo.add(String.valueOf(backend.getBePort()));
            backendInfo.add(String.valueOf(backend.getHttpPort()));
            backendInfo.add(String.valueOf(backend.getBrpcPort()));
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastStartTime()));
            backendInfo.add(TimeUtils.longToTimeString(backend.getLastUpdateMs()));
            backendInfo.add(String.valueOf(backend.isAlive()));
            if (backend.isDecommissioned() && backend.getDecommissionType() == DecommissionType.ClusterDecommission) {
                backendInfo.add("false");
                backendInfo.add("true");
            } else if (backend.isDecommissioned()
                    && backend.getDecommissionType() == DecommissionType.SystemDecommission) {
                backendInfo.add("true");
                backendInfo.add("false");
            } else {
                backendInfo.add("false");
                backendInfo.add("false");
            }
            backendInfo.add(tabletNum);

            // capacity
            // data used
            long dataUsedB = backend.getDataUsedCapacityB();
            Pair<Double, String> usedCapacity = DebugUtil.getByteUint(dataUsedB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(usedCapacity.first) + " " + usedCapacity.second);
            // available
            long availB = backend.getAvailableCapacityB();
            Pair<Double, String> availCapacity = DebugUtil.getByteUint(availB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(availCapacity.first) + " " + availCapacity.second);
            // total
            long totalB = backend.getTotalCapacityB();
            Pair<Double, String> totalCapacity = DebugUtil.getByteUint(totalB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(totalCapacity.first) + " " + totalCapacity.second);

            // used percent
            double used = 0.0;
            if (totalB <= 0) {
                used = 0.0;
            } else {
                used = (double) (totalB - availB) * 100 / totalB;
            }
            backendInfo.add(String.format("%.2f", used) + " %");
            backendInfo.add(String.format("%.2f", backend.getMaxDiskUsedPct() * 100) + " %");

            backendInfo.add(backend.getHeartbeatErrMsg());
            backendInfo.add(backend.getVersion());
            backendInfo.add(new Gson().toJson(backend.getBackendStatus()));

            // data total
            long dataTotalB = backend.getDataTotalCapacityB();
            Pair<Double, String> dataTotalCapacity = DebugUtil.getByteUint(dataTotalB);
            backendInfo.add(DebugUtil.DECIMAL_FORMAT_SCALE_3.format(dataTotalCapacity.first) + " " +
                    dataTotalCapacity.second);

            // data used percent
            double dataUsed = 0.0;
            if (dataTotalB <= 0) {
                dataUsed = 0.0;
            } else {
                dataUsed = (double) dataUsedB * 100 / dataTotalB;
            }
            backendInfo.add(String.format("%.2f", dataUsed) + " %");

            // Num CPU cores
            backendInfo.add(BackendCoreStat.getCoresOfBe(backendId));

            backendInfo.add(backend.getNumRunningQueries());
            double memUsedPct = backend.getMemUsedPct();
            backendInfo.add(String.format("%.2f", memUsedPct * 100) + " %");
            backendInfo.add(String.format("%.1f", backend.getCpuUsedPermille() / 10.0) + " %");

            Optional<DataCacheMetrics> dataCacheMetrics = backend.getDataCacheMetrics();
            if (dataCacheMetrics.isPresent()) {
                DataCacheMetrics.Status status = dataCacheMetrics.get().getStatus();
                if (status != DataCacheMetrics.Status.DISABLED) {
                    backendInfo.add(String.format("Status: %s, DiskUsage: %s, MemUsage: %s",
                            dataCacheMetrics.get().getStatus(),
                            dataCacheMetrics.get().getDiskUsage(),
                            dataCacheMetrics.get().getMemUsage()));
                } else {
                    // DataCache is disabled
                    backendInfo.add(String.format("Status: %s", DataCacheMetrics.Status.DISABLED));
                }
            } else {
                // Didn't receive any datacache report from be
                backendInfo.add("N/A");
            }

            backendInfo.add(PropertyAnalyzer.convertLocationMapToString(backend.getLocation()));

            if (RunMode.isSharedDataMode()) {
                backendInfo.add(String.valueOf(backend.getStarletPort()));
                long workerId = GlobalStateMgr.getCurrentStarOSAgent().getWorkerIdByBackendId(backendId);
                backendInfo.add(String.valueOf(workerId));
            }

            comparableBackendInfos.add(backendInfo);
        }

        // backends proc node get result too slow, add log to observer.
        LOG.info("backends proc get tablet num cost: {}, total cost: {}",
                watch.elapsed(TimeUnit.MILLISECONDS), (System.currentTimeMillis() - start));

        // sort by cluster name, host name
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(1, 3);
        comparableBackendInfos.sort(comparator);

        for (List<Comparable> backendInfo : comparableBackendInfos) {
            List<String> oneInfo = new ArrayList<String>(backendInfo.size());
            for (Comparable element : backendInfo) {
                oneInfo.add(element.toString());
            }
            backendInfos.add(oneInfo);
        }

        return backendInfos;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return true;
    }

    @Override
    public ProcNodeInterface lookup(String beIdStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(beIdStr)) {
            throw new AnalysisException("Backend id is null");
        }

        long backendId = -1L;
        try {
            backendId = Long.parseLong(beIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid backend id format: " + beIdStr);
        }

        Backend backend = clusterInfoService.getBackend(backendId);
        if (backend == null) {
            throw new AnalysisException("Backend[" + backendId + "] does not exist.");
        }

        return new BackendProcNode(backend);
    }

}


