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

package com.starrocks.common.proc;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.alter.DecommissionType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.datacache.DataCacheMetrics;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ComputeNodeProcDir implements ProcDirInterface {

    private static final Logger LOG = LogManager.getLogger(ComputeNodeProcDir.class);

    public static final ImmutableList<String> TITLE_NAMES;
    public static final ImmutableList<String> TITLE_NAMES_SHARED_DATA;

    static {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                .add("ComputeNodeId").add("IP").add("HeartbeatPort")
                .add("BePort").add("HttpPort").add("BrpcPort").add("LastStartTime").add("LastHeartbeat").add("Alive")
                .add("SystemDecommissioned").add("ClusterDecommissioned").add("ErrMsg")
                .add("Version")
                .add("CpuCores").add("MemLimit").add("NumRunningQueries").add("MemUsedPct").add("CpuUsedPct")
                .add("DataCacheMetrics").add("HasStoragePath").add("StatusCode");
        TITLE_NAMES = builder.build();
        builder = new ImmutableList.Builder<String>()
                .addAll(TITLE_NAMES)
                .add("StarletPort")
                .add("WorkerId")
                .add("WarehouseName")
                .add("TabletNum");
        TITLE_NAMES_SHARED_DATA = builder.build();
    }

    private SystemInfoService clusterInfoService;

    public ComputeNodeProcDir(SystemInfoService clusterInfoService) {
        this.clusterInfoService = clusterInfoService;
    }

    public static List<String> getMetadata() {
        if (RunMode.isSharedDataMode()) {
            return TITLE_NAMES_SHARED_DATA;
        } else {
            return TITLE_NAMES;
        }
    }

    @Override
    public ProcResult fetchResult()
            throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(getMetadata());

        final List<List<String>> computeNodesInfos = getClusterComputeNodesInfos();
        for (List<String> computeNodesInfo : computeNodesInfos) {
            List<String> oneInfo = new ArrayList<>(computeNodesInfo.size());
            oneInfo.addAll(computeNodesInfo);
            result.addRow(oneInfo);
        }
        return result;
    }

    /**
     * get compute nodes of cluster
     * copy from getClusterBackendInfos, It is necessary to refactor the two methods later
     */
    public static List<List<String>> getClusterComputeNodesInfos() {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<List<String>> computeNodesInfos = new LinkedList<>();
        List<Long> computeNodeIds;
        computeNodeIds = clusterInfoService.getComputeNodeIds(false);
        if (computeNodeIds == null) {
            return computeNodesInfos;
        }

        long start = System.currentTimeMillis();
        Stopwatch watch = Stopwatch.createUnstarted();
        List<List<Comparable>> comparableComputeNodeInfos = new LinkedList<>();
        for (Long computeNodeId : computeNodeIds) {
            ComputeNode computeNode = clusterInfoService.getComputeNode(computeNodeId);
            if (computeNode == null) {
                continue;
            }

            List<Comparable> computeNodeInfo = Lists.newArrayList();
            computeNodeInfo.add(String.valueOf(computeNodeId));
            computeNodeInfo.add(computeNode.getHost());

            computeNodeInfo.add(String.valueOf(computeNode.getHeartbeatPort()));
            computeNodeInfo.add(String.valueOf(computeNode.getBePort()));
            computeNodeInfo.add(String.valueOf(computeNode.getHttpPort()));
            computeNodeInfo.add(String.valueOf(computeNode.getBrpcPort()));

            computeNodeInfo.add(TimeUtils.longToTimeString(computeNode.getLastStartTime()));
            computeNodeInfo.add(TimeUtils.longToTimeString(computeNode.getLastUpdateMs()));
            computeNodeInfo.add(String.valueOf(computeNode.isAlive()));
            if (computeNode.isDecommissioned()
                    && computeNode.getDecommissionType() == DecommissionType.ClusterDecommission) {
                computeNodeInfo.add("false");
                computeNodeInfo.add("true");
            } else if (computeNode.isDecommissioned()
                    && computeNode.getDecommissionType() == DecommissionType.SystemDecommission) {
                computeNodeInfo.add("true");
                computeNodeInfo.add("false");
            } else {
                computeNodeInfo.add("false");
                computeNodeInfo.add("false");
            }

            computeNodeInfo.add(computeNode.getHeartbeatErrMsg());
            computeNodeInfo.add(computeNode.getVersion());

            computeNodeInfo.add(computeNode.getCpuCores());
            computeNodeInfo.add(DebugUtil.getPrettyStringBytes(computeNode.getMemLimitBytes()));

            computeNodeInfo.add(computeNode.getNumRunningQueries());
            double memUsedPct = computeNode.getMemUsedPct();
            computeNodeInfo.add(String.format("%.2f", memUsedPct * 100) + " %");
            computeNodeInfo.add(String.format("%.1f", computeNode.getCpuUsedPermille() / 10.0) + " %");

            Optional<DataCacheMetrics> dataCacheMetrics = computeNode.getDataCacheMetrics();
            if (dataCacheMetrics.isPresent()) {
                DataCacheMetrics.Status status = dataCacheMetrics.get().getStatus();
                if (status != DataCacheMetrics.Status.DISABLED) {
                    computeNodeInfo.add(String.format("Status: %s, DiskUsage: %s, MemUsage: %s",
                            dataCacheMetrics.get().getStatus(),
                            dataCacheMetrics.get().getDiskUsageStr(),
                            dataCacheMetrics.get().getMemUsageStr()));
                } else {
                    // DataCache is disabled
                    computeNodeInfo.add(String.format("Status: %s", DataCacheMetrics.Status.DISABLED));
                }
            } else {
                // Didn't receive any datacache report from be
                computeNodeInfo.add("N/A");
            }

            computeNodeInfo.add(String.valueOf(computeNode.isSetStoragePath()));
            computeNodeInfo.add(computeNode.getStatus().name());

            if (RunMode.isSharedDataMode()) {
                computeNodeInfo.add(String.valueOf(computeNode.getStarletPort()));
                long workerId = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerIdByNodeId(computeNodeId);
                computeNodeInfo.add(String.valueOf(workerId));
                Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(computeNode.getWarehouseId());
                computeNodeInfo.add(wh.getName());

                String workerAddr = computeNode.getHost() + ":" + computeNode.getStarletPort();
                long tabletNum = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerTabletNum(workerAddr);
                computeNodeInfo.add(tabletNum);
            }

            comparableComputeNodeInfos.add(computeNodeInfo);
        }

        // compute node proc node get result too slow, add log to observer.
        LOG.info("compute node proc get tablet num cost: {}, total cost: {}",
                watch.elapsed(TimeUnit.MILLISECONDS), (System.currentTimeMillis() - start));

        // sort by cluster name, host name
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(1, 3);
        comparableComputeNodeInfos.sort(comparator);

        for (List<Comparable> computeNodeInfo : comparableComputeNodeInfos) {
            List<String> oneInfo = new ArrayList<String>(computeNodeInfo.size());
            for (Comparable element : computeNodeInfo) {
                oneInfo.add(element.toString());
            }
            computeNodesInfos.add(oneInfo);
        }

        return computeNodesInfos;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return true;
    }

    @Override
    public ProcNodeInterface lookup(String name)
            throws AnalysisException {
        return null;
    }
}
