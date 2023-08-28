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

package com.starrocks.warehouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.epack.lake.StarOSAgentEpack;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.system.ComputeNode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// on-premise
public class LocalWarehouse extends Warehouse {
    private static final Logger LOG = LogManager.getLogger(LocalWarehouse.class);

    @SerializedName(value = "cluster")
    Cluster cluster;

    public static final ImmutableList<String> CLUSTER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ClusterId")
            .add("WorkerGroupId")
            .add("ComputeNodeIds")
            .add("Pending")
            .add("Running")
            .build();

    public LocalWarehouse(long id, String name, long clusterId, String comment) {
        super(id, name, comment);
        cluster = new Cluster(clusterId);
    }

    @Override
    public void initCluster() throws DdlException {
        cluster.init();
    }

    @Override
    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(
                String.valueOf(this.getId()),
                this.getName(),
                this.getState().toString(),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(-1L),   //TODO: need to be filled after
                String.valueOf(-1L),   //TODO: need to be filled after
                TimeUtils.longToTimeString(this.getCreatedTime()),
                TimeUtils.longToTimeString(this.getResumedTime()),
                TimeUtils.longToTimeString(this.getUpdatedTime()),
                this.getComment()));
    }

    @Override
    public Map<Long, Cluster> getClusters() throws DdlException {
        return ImmutableMap.of(cluster.getId(), cluster);
    }

    @Override
    public void setClusters(Map<Long, Cluster> clusters) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public Cluster getAnyAvailableCluster() {
        return cluster;
    }

    @Override
    public ProcResult getClusterProcData() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(CLUSTER_PROC_NODE_TITLE_NAMES);
        cluster.getProcNodeData(result);
        return result;
    }

    @Override
    public void dropSelf() throws DdlException {
        deleteWorkerFromStarMgr();
        dropNodeFromSystem();
    }

    @Override
    public void suspendSelf() {
        this.state = WarehouseState.SUSPENDED;
        long currentTime = System.currentTimeMillis();
        setResumedTime(currentTime);
        setUpdatedTime(currentTime);
    }

    @Override
    public void resumeSelf() {
        this.state = WarehouseState.AVAILABLE;
        long currentTime = System.currentTimeMillis();
        setUpdatedTime(currentTime);
    }

    private void deleteWorkerFromStarMgr() throws DdlException {
        long workerGroupId = cluster.getWorkerGroupId();
        StarOSAgentEpack starOSAgent = (StarOSAgentEpack) GlobalStateMgr.getCurrentStarOSAgent();
        starOSAgent.deleteWorkerGroup(workerGroupId);
    }

    private void dropNodeFromSystem() throws DdlException {
        List<ComputeNode> nodes = GlobalStateMgr.getCurrentSystemInfo().backendAndComputeNodeStream().
                filter(cn -> cn.getWarehouseId() == this.getId()).collect(Collectors.toList());

        for (ComputeNode node : nodes) {
            try {
                GlobalStateMgr.getCurrentSystemInfo().dropComputeNode(node.getHost(), node.getHeartbeatPort());
                GlobalStateMgr.getCurrentSystemInfo().dropBackend(node.getHost(), node.getHeartbeatPort(), false);
            } catch (DdlException e) {
                if (e.getMessage().contains("compute node does not exists")
                        || e.getMessage().contains("backend does not exists")) {
                    continue;
                } else {
                    throw e;
                }
            }
        }
    }

}
