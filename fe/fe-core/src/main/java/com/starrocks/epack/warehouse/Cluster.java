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

package com.starrocks.epack.warehouse;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.WorkerGroupDetailInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Cluster/CNGroup is a group of compute nodes, managed by StarMgr, associated with a unique workerGroupId.
 * It is a little bit twisted here.
 * The Java Class is named as `Cluster` for historic reason, however the concept to the end user is `CNGroup`,
 * so it will be mixed together with Cluster and CNGroup sometime.
 */
public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    @SerializedName(value = "id")
    private long id;
    @SerializedName(value = "wgid")
    private long workerGroupId;
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "enabled")
    private boolean enabled;

    private final AtomicInteger nextComputeNodeIndex = new AtomicInteger(0);

    private Cluster() {
        // Do nothing, for GSON deserialization
    }

    public Cluster(long id, String name) {
        this(id, name, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
    }

    public Cluster(long id, String name, long workerGroupId) {
        this.id = id;
        this.name = name;
        this.workerGroupId = workerGroupId;
        this.enabled = true;
    }

    public long getId() {
        return id;
    }

    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public AtomicInteger getNextComputeNodeHostId() {
        return nextComputeNodeIndex;
    }

    public String getName() {
        return name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Map<String, String> getProperties() throws DdlException {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        WorkerGroupDetailInfo info = starOSAgent.getWorkerGroupInfo(workerGroupId);
        return info.getPropertiesMap();
    }

    public String getPropertiesJsonString() throws DdlException {
        Map<String, String> properties = getProperties();
        return new Gson().toJson(properties);
    }

    public void updateProperties(Map<String, String> properties) throws DdlException {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        starOSAgent.updateWorkerGroup(workerGroupId, properties);
    }

    public List<Long> getComputeNodeIds() {
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkersByWorkerGroup(workerGroupId);
        } catch (StarRocksException e) {
            LOG.warn("Fail to get compute node ids from starMgr : {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    private void deleteWorkerGroupFromStarMgr() throws DdlException {
        GlobalStateMgr.getCurrentState().getStarOSAgent().deleteWorkerGroup(workerGroupId);
    }

    private void dropNodeFromSystem(String warehouseName) throws DdlException {
        // TODO: refactor this code
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes =
                systemInfoService.backendAndComputeNodeStream().filter(cn -> cn.getWorkerGroupId() == workerGroupId)
                        .collect(Collectors.toList());

        for (ComputeNode node : nodes) {
            if (node instanceof Backend) {
                if (systemInfoService.getBackendWithHeartbeatPort(node.getHost(), node.getHeartbeatPort()) == null) {
                    continue;
                }
                systemInfoService.dropBackend(node.getHost(), node.getHeartbeatPort(), warehouseName, "", false);
            } else {
                if (systemInfoService.getComputeNodeWithHeartbeatPort(node.getHost(), node.getHeartbeatPort()) ==
                        null) {
                    continue;
                }
                systemInfoService.dropComputeNode(node.getHost(), node.getHeartbeatPort(), warehouseName, "");
            }
        }
    }

    public void delete(String warehouseName) throws DdlException {
        dropNodeFromSystem(warehouseName);
        deleteWorkerGroupFromStarMgr();
    }

    public void setEnabled() {
        this.enabled = true;
    }

    public void setDisabled() {
        this.enabled = false;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public void postUpgradeUpdateNameIfNeeded() {
        if (Strings.isNullOrEmpty(name)) {
            name = LocalWarehouse.DEFAULT_CLUSTER_NAME;
            enabled = true;
        }
    }
}
