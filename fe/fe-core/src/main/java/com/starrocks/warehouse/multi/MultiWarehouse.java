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

package com.starrocks.warehouse.multi;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.warehouse.Warehouse;
import com.starrocks.warehouse.WarehouseProcDir;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A user-created warehouse backed by its own StarMgr worker group.
 *
 * <p> Each {@code MultiWarehouse} owns exactly one worker group. Every compute node assigned to the warehouse
 * (via {@code ALTER SYSTEM ADD COMPUTE NODE ... INTO WAREHOUSE <name>}) is registered into that group, and
 * {@link com.starrocks.warehouse.cngroup.WarehouseComputeResourceProvider} resolves the warehouse's nodes by
 * asking StarMgr for the members of the group.
 */
public class MultiWarehouse extends Warehouse {
    @SerializedName(value = "workerGroupId")
    private long workerGroupId;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    @SerializedName(value = "createdTime")
    private long createdTime;

    @SerializedName(value = "updatedTime")
    private long updatedTime;

    public MultiWarehouse(long id, String name, String comment, long workerGroupId,
                          Map<String, String> properties, long createdTime) {
        super(id, name, Strings.nullToEmpty(comment));
        this.workerGroupId = workerGroupId;
        this.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
        this.createdTime = createdTime;
        this.updatedTime = createdTime;
    }

    public Map<String, String> getProperties() {
        return properties == null ? Maps.newHashMap() : properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public long getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(long updatedTime) {
        this.updatedTime = updatedTime;
    }

    @Override
    public long getResumeTime() {
        // SUSPEND/RESUME is not supported, so the warehouse is never resumed.
        return -1L;
    }

    @Override
    public Long getAnyWorkerGroupId() {
        return workerGroupId;
    }

    @Override
    public List<Long> getWorkerGroupIds() {
        return ImmutableList.of(workerGroupId);
    }

    @Override
    public void addNodeToCNGroup(ComputeNode node, String cnGroupName) throws DdlException {
        if (!Strings.isNullOrEmpty(cnGroupName)) {
            // NOTE: CNGROUP is not implemented, the warehouse is the only grouping level.
            ErrorReport.reportDdlException(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED);
        }
        node.setWorkerGroupId(workerGroupId);
        node.setWarehouseId(getId());
    }

    @Override
    public void validateRemoveNodeFromCNGroup(ComputeNode node, String cnGroupName) throws DdlException {
        if (!Strings.isNullOrEmpty(cnGroupName)) {
            // NOTE: CNGROUP is not implemented, the warehouse is the only grouping level.
            ErrorReport.reportDdlException(ErrorCode.ERR_CNGROUP_NOT_IMPLEMENTED);
        }
    }

    @Override
    public boolean isAvailable() {
        // The warehouse cannot be suspended, so it is always a legal scheduling target. Whether it currently
        // has alive nodes is decided by ComputeResourceProvider#isResourceAvailable
        return true;
    }

    /**
     * Nodes (compute nodes and backends) currently assigned to this warehouse.
     */
    public List<ComputeNode> getNodes() {
        List<ComputeNode> nodes = new ArrayList<>();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        if (systemInfoService == null) {
            return nodes;
        }
        for (ComputeNode node : systemInfoService.getComputeNodes()) {
            if (node.getWarehouseId() == getId()) {
                nodes.add(node);
            }
        }
        for (ComputeNode node : systemInfoService.getBackends()) {
            if (node.getWarehouseId() == getId()) {
                nodes.add(node);
            }
        }
        return nodes;
    }

    @Override
    public List<String> getWarehouseInfo() {
        List<ComputeNode> nodes = getNodes();
        boolean anyAlive = nodes.stream().anyMatch(ComputeNode::isAlive);
        return Lists.newArrayList(
                String.valueOf(getId()),
                getName(),
                anyAlive ? "AVAILABLE" : "UNAVAILABLE",
                String.valueOf(nodes.size()),
                String.valueOf(1L),  // CurrentClusterCount: one worker group per warehouse
                String.valueOf(1L),  // MaxClusterCount
                String.valueOf(1L),  // StartedClusters
                String.valueOf(0L),  // RunningSql: not tracked per warehouse here
                String.valueOf(0L),  // QueuedSql: not tracked per warehouse here
                TimeUtils.longToTimeString(createdTime),
                "",                  // ResumedOn: SUSPEND/RESUME not supported
                TimeUtils.longToTimeString(updatedTime),
                propertiesToString(),
                comment);
    }

    @Override
    public List<List<String>> getWarehouseNodesInfo() {
        List<List<String>> rows = new ArrayList<>();
        for (ComputeNode node : getNodes()) {
            rows.add(Lists.newArrayList(
                    getName(),
                    "",  // CNGroupId: CNGROUP is not implemented
                    String.valueOf(node.getWorkerGroupId()),
                    String.valueOf(node.getId()),
                    String.valueOf(getWorkerId(node)),
                    node.getHost(),
                    String.valueOf(node.getHeartbeatPort()),
                    String.valueOf(node.getBePort()),
                    String.valueOf(node.getHttpPort()),
                    String.valueOf(node.getBrpcPort()),
                    String.valueOf(node.getStarletPort()),
                    TimeUtils.longToTimeString(node.getLastStartTime()),
                    TimeUtils.longToTimeString(node.getLastUpdateMs()),
                    String.valueOf(node.isAlive()),
                    Strings.nullToEmpty(node.getHeartbeatErrMsg()),
                    Strings.nullToEmpty(node.getVersion()),
                    String.valueOf(node.getNumRunningQueries()),
                    String.valueOf(node.getCpuCores()),
                    String.format("%.2f", node.getMemUsedPct() * 100),
                    String.format("%.2f", node.getCpuUsedPermille() / 10.0),
                    ""));  // CNGroupName: CNGROUP is not implemented
        }
        return rows;
    }

    /**
     * StarMgr's id for this node, or -1 when the node has not reported a starlet port yet (StarMgr only learns
     * about it on the first heartbeat that carries one, see HeartbeatMgr).
     */
    private long getWorkerId(ComputeNode node) {
        try {
            return GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerIdByNodeId(node.getId());
        } catch (Exception e) {
            return -1L;
        }
    }

    @Override
    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(WarehouseProcDir.WAREHOUSE_PROC_NODE_TITLE_NAMES);
        result.addRow(getWarehouseInfo());
        return result;
    }

    @Override
    public void createCNGroup(CreateCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void dropCNGroup(DropCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void enableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void disableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void alterCNGroup(AlterCnGroupStmt stmt) throws DdlException {
        throw new DdlException("CnGroup is not implemented");
    }

    @Override
    public void replayInternalOpLog(String payload) {
        // No internal op is journalled for this warehouse: CREATE/DROP/ALTER go through the dedicated
        // OP_CREATE/DROP/ALTER_WAREHOUSE entries, and CNGROUP is not implemented.
    }

    private String propertiesToString() {
        Map<String, String> props = getProperties();
        if (props.isEmpty()) {
            return "";
        }
        List<String> parts = new ArrayList<>(props.size());
        props.forEach((k, v) -> parts.add(k + "=" + v));
        return Joiner.on(",").join(parts);
    }
}
