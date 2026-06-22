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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.staros.proto.ReplicationType;
import com.staros.proto.WarmupLevel;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.persist.WarehouseInternalOpLog;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseEventListener;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.warehouse.cngroup.AlterCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.CreateCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.DropCnGroupStmt;
import com.starrocks.sql.ast.warehouse.cngroup.EnableDisableCnGroupStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.warehouse.WarehouseProcDir.WAREHOUSE_PROC_NODE_TITLE_NAMES;

// Hand-managed Warehouse (adding/removing nodes through SQL interface)
public class LocalWarehouse extends Warehouse implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(LocalWarehouse.class);

    public static final long DEFAULT_CLUSTER_ID = 0L;
    public static final String DEFAULT_CLUSTER_NAME = "_builtin_cngroup_0_";

    // Keep it for backwards compatibility so the newer version can be still rolled back to the old version.
    @SerializedName(value = "cluster")
    Cluster cluster;

    public enum WarehouseState {
        AVAILABLE,
        SUSPENDED,
    }

    @SerializedName(value = "state")
    protected WarehouseState state = WarehouseState.AVAILABLE;

    @SerializedName(value = "ctime")
    private volatile long createdTime;

    @SerializedName(value = "rtime")
    private volatile long resumedTime;

    @SerializedName(value = "mtime")
    private volatile long updatedTime;

    @SerializedName(value = "property")
    private WarehouseProperty property;

    @SerializedName(value = "clusters")
    private List<Cluster> clusters;

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // default_warehouse creation
    public static LocalWarehouse createDefaultLocalWarehouse(String comment) {
        LocalWarehouse warehouse =
                new LocalWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME,
                        new WarehouseProperty(), comment);
        warehouse.cluster = new Cluster(DEFAULT_CLUSTER_ID, DEFAULT_CLUSTER_NAME);
        warehouse.clusters.add(warehouse.cluster);
        return warehouse;
    }

    // Do nothing, for GSON deserialization only
    private LocalWarehouse() {
        this(0, "", new WarehouseProperty(), "");
    }

    // non-default warehouse creation
    public LocalWarehouse(long id, String name, WarehouseProperty property, String comment) {
        super(id, name, comment);
        this.property = property;
        if (this.property == null) {
            this.property = new WarehouseProperty();
        }
        this.clusters = new ArrayList<>();
        this.cluster = null;
        this.createdTime = System.currentTimeMillis();
    }

    public List<String> getWarehouseInfo() {
        int numOfNodes = clusters.stream().map(x -> x.getComputeNodeIds().size()).reduce(0, Integer::sum);
        return Lists.newArrayList(
                String.valueOf(getId()),
                getName(),
                state.toString(),
                String.valueOf(numOfNodes),
                String.valueOf(clusters.size()), // CurrentClusterCount
                String.valueOf(-1L), // MaxClusterCount
                String.valueOf(clusters.size()), // StartedClusters
                String.valueOf(0L),   // TODO: need to be filled after, RunningSql
                String.valueOf(0L),   // TODO: need to be filled after, QueuedSql
                TimeUtils.longToTimeString(createdTime),
                TimeUtils.longToTimeString(resumedTime),
                TimeUtils.longToTimeString(updatedTime),
                (property != null) ? property.toString() : "",
                comment);
    }

    public WarehouseState getState() {
        return state;
    }

    public void setProperty(WarehouseProperty property) {
        this.property = property;
    }

    public WarehouseProperty getProperty() {
        return property;
    }

    public Map<String, String> getWarehouseSessionVariable() {
        return property.getSessionVariables();
    }

    public Map<Long, Cluster> getClusters() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            ImmutableMap.Builder<Long, Cluster> builder = new ImmutableMap.Builder<>();
            for (Cluster c : clusters) {
                builder.put(c.getId(), c);
            }
            return builder.build();
        }
    }

    public Cluster getAnyAvailableCluster() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return clusters.stream().filter(Cluster::isEnabled).findAny().orElse(null);
        }
    }

    @Override
    public long getResumeTime() {
        return resumedTime;
    }

    public void delete() throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            for (Cluster c : clusters) {
                c.delete(getName());
            }
            clusters.clear();
            cluster = null;
        }
    }

    public void replayDelete() {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            clusters.clear();
            cluster = null;
        }
    }

    public void suspendSelf() {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            this.state = WarehouseState.SUSPENDED;
            long currentTime = System.currentTimeMillis();
            resumedTime = currentTime;
            updatedTime = currentTime;
        }
    }

    public void resumeSelf() {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            this.state = WarehouseState.AVAILABLE;
            resumedTime = System.currentTimeMillis();
        }
    }

    @Override
    public Long getAnyWorkerGroupId() {
        if (cluster == null) {
            return null;
        } else {
            return cluster.getWorkerGroupId();
        }
    }

    boolean isEmptyCNGroupNameAllowed() {
        // An empty CNGroupName is permitted only under the following condition:
        // 1. The warehouse contains exactly one CNGroup
        //
        // Note: This validation ensures backward compatibility for systems upgraded from pre-CNGroup-managed states.
        return clusters.size() == 1 && cluster != null;
    }

    @Override
    public void addNodeToCNGroup(ComputeNode node, String cnGroupName) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Cluster c = getClusterByNameCompatibility(cnGroupName);
            if (c == null) {
                if (Strings.isNullOrEmpty(cnGroupName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_CNGROUP_NAME);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_CNGROUP, cnGroupName);
                }
            }
            node.setWorkerGroupId(c.getWorkerGroupId());
            node.setWarehouseId(getId());
        }
    }

    // Get the Cluster/CNGroup via its name, accepting empty cnGroupName under certain conditions for backwards compatibility.
    private Cluster getClusterByNameCompatibility(String cnGroupName) {
        if (Strings.isNullOrEmpty(cnGroupName) && isEmptyCNGroupNameAllowed()) {
            return cluster;
        } else {
            return clusters.stream().filter(x -> x.getName().equals(cnGroupName)).findFirst().orElse(null);
        }
    }

    @Override
    public void validateRemoveNodeFromCNGroup(ComputeNode node, String cnGroupName) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            Cluster c = getClusterByNameCompatibility(cnGroupName);
            if (c == null) {
                if (Strings.isNullOrEmpty(cnGroupName)) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_CNGROUP_NAME);
                } else {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_CNGROUP, cnGroupName);
                }
            }
            if (node.getWorkerGroupId() != c.getWorkerGroupId()) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NODE_CNGROUP_MISMATCH);
            }
        }
    }

    @Override
    public List<Long> getWorkerGroupIds() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return clusters.stream().map(Cluster::getWorkerGroupId).collect(Collectors.toList());
        }
    }

    @Override
    public List<List<String>> getWarehouseNodesInfo() {
        List<List<String>> rows = new ArrayList<>();
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            for (Cluster cluster : getClusters().values()) {
                List<Long> computeNodes = cluster.getComputeNodeIds();
                for (Long computeNodeId : computeNodes) {
                    ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                            .getBackendOrComputeNode(computeNodeId);

                    List<String> computeNodeInfo = Lists.newArrayList();
                    long warehouseId = node.getWarehouseId();
                    Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                    computeNodeInfo.add(warehouse.getName());

                    computeNodeInfo.add(String.valueOf(cluster.getId()));
                    computeNodeInfo.add(String.valueOf(cluster.getWorkerGroupId()));
                    long nodeId = node.getId();
                    long workerId = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerIdByNodeId(nodeId);
                    computeNodeInfo.add(String.valueOf(nodeId));
                    computeNodeInfo.add(String.valueOf(workerId));

                    computeNodeInfo.add(node.getHost());

                    computeNodeInfo.add(String.valueOf(node.getHeartbeatPort()));
                    computeNodeInfo.add(String.valueOf(node.getBePort()));
                    computeNodeInfo.add(String.valueOf(node.getHttpPort()));
                    computeNodeInfo.add(String.valueOf(node.getBrpcPort()));
                    computeNodeInfo.add(String.valueOf(node.getStarletPort()));

                    computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastStartTime()));
                    computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastUpdateMs()));
                    computeNodeInfo.add(String.valueOf(node.isAlive()));

                    computeNodeInfo.add(node.getHeartbeatErrMsg());
                    computeNodeInfo.add(String.valueOf(node.getVersion()));

                    computeNodeInfo.add(String.valueOf(node.getNumRunningQueries()));
                    computeNodeInfo.add(String.valueOf(node.getCpuCores()));
                    double memUsedPct = node.getMemUsedPct();
                    computeNodeInfo.add(String.format("%.2f", memUsedPct * 100) + " %");
                    computeNodeInfo.add(String.format("%.1f", node.getCpuUsedPermille() / 10.0) + " %");

                    rows.add(computeNodeInfo);
                }
            }
        }

        return rows;
    }

    public List<List<String>> getClustersInfo() {
        List<List<String>> rows = new ArrayList<>();
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            for (Cluster cluster : getClusters().values()) {
                List<String> row = Lists.newArrayList();
                row.add(String.valueOf(cluster.getId())); // CNGroupID
                row.add(String.valueOf(cluster.getName())); // CNGroupName
                row.add(String.valueOf(cluster.getWorkerGroupId())); // WorkerGroupId
                String nodeIds =
                        cluster.getComputeNodeIds().stream().map(String::valueOf).collect(Collectors.joining(","));
                row.add(nodeIds); // ComputeNodeIds
                row.add(String.valueOf(-1)); // Pending
                row.add(String.valueOf(-1)); // Running
                row.add(String.valueOf(cluster.isEnabled())); // Enabled
                String properties = "{}";
                try {
                    properties = cluster.getPropertiesJsonString();
                } catch (Exception ignoredException) {
                    // ignore the exception
                }
                row.add(properties); // Properties
                rows.add(row);
            }
        }
        return rows;
    }

    public ProcResult fetchResult() {
        BaseProcResult result = new BaseProcResult();
        result.setNames(WAREHOUSE_PROC_NODE_TITLE_NAMES);
        List<Warehouse> warehouseIds = GlobalStateMgr.getCurrentState().getWarehouseMgr().getAllWarehouses();
        warehouseIds.forEach(x -> {
            if (x != null) {
                result.addRow(x.getWarehouseInfo());
            }
        });
        return result;
    }

    /**
     * Get the Cluster by its WorkGroupId.
     */
    @VisibleForTesting
    public Cluster getClusterByWorkGroupId(long workGroupId) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return clusters.stream().filter(x -> x.getWorkerGroupId() == workGroupId).findAny().orElse(null);
        }
    }

    private Cluster getClusterByNameNoExceptionNoLock(String name) {
        return clusters.stream().filter(x -> x.getName().equals(name)).findAny().orElse(null);
    }

    @VisibleForTesting
    Cluster getCluster(String cnGroupName) {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return getClusterByNameNoExceptionNoLock(cnGroupName);
        }
    }

    private void ensureWarehouseStateNotSuspended() throws DdlException {
        if (getState() == WarehouseState.SUSPENDED) {
            throw ErrorReportException.report(ErrorCode.ERR_WAREHOUSE_SUSPENDED,
                    String.format("name: %s", getName()));
        }
    }

    private Cluster ensureCnGroupExists(String cnGroupName) throws DdlException {
        ensureWarehouseStateNotSuspended();
        Cluster cluster = getClusterByNameNoExceptionNoLock(cnGroupName);
        if (cluster == null) {
            throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_CNGROUP, cnGroupName);
        }
        return cluster;
    }

    private void ensureCnGroupNotExists(String cnGroupName) throws DdlException {
        ensureWarehouseStateNotSuspended();
        Cluster cluster = getClusterByNameNoExceptionNoLock(cnGroupName);
        if (cluster != null) {
            throw ErrorReportException.report(ErrorCode.ERR_CNGROUP_EXISTS, cnGroupName);
        }
    }

    /**
     * Create the builtin cngroup along with the warehouse without EditLog.
     * It will be persistent along with the warehouse creation EditLog.
     *
     * @throws DdlException
     * @apiNote This interface should be only called when the warehouse is created. The CNGroup created here
     * is not persistent and will be lost otherwise.
     *
     * Make this interface package accessible by intention
     */
    void initializeBuiltinCNGroup() throws DdlException {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        ReplicationType replicationType = WarehouseProperty.toStarOSReplicationType(property.getReplicationType());
        WarmupLevel warmupLevel = WarehouseProperty.toStarOSWarmupLevel(property.getWarmupLevel());
        long clusterId = GlobalStateMgr.getCurrentState().getNextId();
        long workerGroupId = starOSAgent.createWorkerGroup("x0", property.getComputeReplica(), replicationType,
                warmupLevel, property.getWarmupTimeoutSecs(), ImmutableMap.of());
        cluster = new Cluster(clusterId, DEFAULT_CLUSTER_NAME, workerGroupId);
        clusters.add(cluster);
        final List<WarehouseEventListener> warehouseListeners = GlobalStateMgr.getCurrentState()
                .getWarehouseMgr().getWarehouseListeners();
        warehouseListeners.stream().forEach(
                listener -> listener.onCreateCNGroup(this, cluster.getWorkerGroupId()));
    }

    @Override
    public void createCNGroup(CreateCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            String cnGroupName = stmt.getCnGroupName();
            ensureWarehouseStateNotSuspended();
            Cluster c = getClusterByNameNoExceptionNoLock(cnGroupName);
            if (c != null) {
                if (stmt.isSetIfNotExists()) {
                    return;
                } else {
                    throw ErrorReportException.report(ErrorCode.ERR_CNGROUP_EXISTS, cnGroupName);
                }
            }

            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            ReplicationType replicationType = WarehouseProperty.toStarOSReplicationType(property.getReplicationType());
            WarmupLevel warmupLevel = WarehouseProperty.toStarOSWarmupLevel(property.getWarmupLevel());
            Map<String, String> groupProperties = stmt.getProperties();
            if (groupProperties == null) {
                groupProperties = ImmutableMap.of();
            }
            long clusterId = GlobalStateMgr.getCurrentState().getNextId();
            long workerGroupId = starOSAgent.createWorkerGroup(
                    "x0", property.getComputeReplica(), replicationType, warmupLevel,
                    property.getWarmupTimeoutSecs(), groupProperties);
            Cluster newCluster = new Cluster(clusterId, cnGroupName, workerGroupId);
            clusters.add(newCluster);
            if (cluster == null) {
                cluster = clusters.get(0);
            }
            final List<WarehouseEventListener> warehouseListeners = GlobalStateMgr.getCurrentState()
                    .getWarehouseMgr().getWarehouseListeners();
            warehouseListeners.stream().forEach(
                    listener -> listener.onCreateCNGroup(this, newCluster.getWorkerGroupId()));
            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.createCNGroupOpLog(newCluster);
            WarehouseInternalOpLog log = new WarehouseInternalOpLog(getName(), opLog.toJson());
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logJsonObject(OperationType.OP_WAREHOUSE_INTERNAL_OP, log);
        }
    }

    @Override
    public void dropCNGroup(DropCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            ensureWarehouseStateNotSuspended();
            Cluster clusterToDel = getClusterByNameNoExceptionNoLock(stmt.getCnGroupName());
            if (clusterToDel == null) {
                if (!stmt.isSetIfExists()) {
                    throw ErrorReportException.report(ErrorCode.ERR_UNKNOWN_CNGROUP, stmt.getCnGroupName());
                }
                return;
            }
            List<Long> computeNodeIds = clusterToDel.getComputeNodeIds();
            if (!computeNodeIds.isEmpty()) {
                if (!stmt.isSetForce()) {
                    throw ErrorReportException.report(ErrorCode.ERR_CNGROUP_NOT_EMPTY, stmt.getCnGroupName());
                }
            }
            clusterToDel.delete(getName());
            clusters.remove(clusterToDel);
            if (clusterToDel.getId() == cluster.getId()) {
                cluster = clusters.isEmpty() ? null : clusters.get(0);
            }
            final List<WarehouseEventListener> warehouseListeners = GlobalStateMgr.getCurrentState()
                    .getWarehouseMgr().getWarehouseListeners();
            warehouseListeners.stream().forEach(
                    listener -> listener.onDropCNGroup(this, clusterToDel.getWorkerGroupId()));
            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.dropCNGroupOpLog(clusterToDel.getName());
            WarehouseInternalOpLog log = new WarehouseInternalOpLog(getName(), opLog.toJson());
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logJsonObject(OperationType.OP_WAREHOUSE_INTERNAL_OP, log);
        }
    }

    @Override
    public void enableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(stmt.isSetEnable());
            Cluster c = ensureCnGroupExists(stmt.getCnGroupName());
            c.setEnabled();

            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.enableCNGroupOpLog(c.getName());
            WarehouseInternalOpLog log = new WarehouseInternalOpLog(getName(), opLog.toJson());
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logJsonObject(OperationType.OP_WAREHOUSE_INTERNAL_OP, log);
        }
    }

    @Override
    public void disableCNGroup(EnableDisableCnGroupStmt stmt) throws DdlException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(!stmt.isSetEnable());
            Cluster c = ensureCnGroupExists(stmt.getCnGroupName());
            c.setDisabled();

            LocalWarehouseOpLog opLog = LocalWarehouseOpLog.disableCNGroupOpLog(c.getName());
            WarehouseInternalOpLog log = new WarehouseInternalOpLog(getName(), opLog.toJson());
            EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
            editLog.logJsonObject(OperationType.OP_WAREHOUSE_INTERNAL_OP, log);
        }
    }

    public void replayAlterWarehouse(LocalWarehouse otherWarehouse) {
        // recover the warehouse state from the otherWarehouse
        this.state = otherWarehouse.state;
        this.createdTime = otherWarehouse.createdTime;
        this.resumedTime = otherWarehouse.resumedTime;
        this.updatedTime = otherWarehouse.updatedTime;
        setProperty(otherWarehouse.getProperty());
    }

    @Override
    public void alterCNGroup(AlterCnGroupStmt stmt) throws DdlException {
        // delegate the properties management to StarOS
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Cluster c = ensureCnGroupExists(stmt.getCnGroupName());
            try {
                c.updateProperties(stmt.getProperties());
            } catch (DdlException e) {
                LOG.warn(e);
                throw new DdlException(String.format("modify cngroup '%s' in warehouse '%s' failed, reason: %s",
                        getName(), stmt.getCnGroupName(), e.getMessage()));
            }
        }
    }

    private void replayCreateCNGroup(Cluster newCluster) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            try {
                ensureCnGroupNotExists(newCluster.getName());
            } catch (Exception exception) {
                LOG.fatal("cngroup {} already exists while replaying createCNGroup log.", newCluster.getName());
            }
            clusters.add(newCluster);
            if (cluster == null) {
                cluster = clusters.get(0);
            }
            final List<WarehouseEventListener> warehouseListeners = GlobalStateMgr.getCurrentState()
                    .getWarehouseMgr().getWarehouseListeners();
            warehouseListeners.stream().forEach(
                    listener -> listener.onCreateCNGroup(this, newCluster.getWorkerGroupId()));
        }
    }

    private void replayDropCNGroup(String cnGroupName) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Cluster clusterToDel = getClusterByNameNoExceptionNoLock(cnGroupName);
            if (clusterToDel == null) {
                return;
            }
            clusters.remove(clusterToDel);
            if (clusterToDel.getId() == cluster.getId()) {
                cluster = clusters.isEmpty() ? null : clusters.get(0);
            }
            final List<WarehouseEventListener> warehouseListeners = GlobalStateMgr.getCurrentState()
                    .getWarehouseMgr().getWarehouseListeners();
            warehouseListeners.stream().forEach(
                    listener -> listener.onDropCNGroup(this, clusterToDel.getWorkerGroupId()));
        }
    }

    private void replayEnableCNGroup(String cnGroupName) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Cluster c = getClusterByNameNoExceptionNoLock(cnGroupName);
            if (c == null) {
                return;
            }
            c.setEnabled();
        }
    }

    private void replayDisableCNGroup(String cnGroupName) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            Cluster c = getClusterByNameNoExceptionNoLock(cnGroupName);
            if (c == null) {
                return;
            }
            c.setDisabled();
        }
    }

    @Override
    public void replayInternalOpLog(String payload) {
        LocalWarehouseOpLog log = LocalWarehouseOpLog.fromJson(payload);
        short op = log.getOp();
        switch (op) {
            case LocalWarehouseOpLog.CREATE_CNGROUP: {
                Cluster c = log.getCluster();
                replayCreateCNGroup(c);
                break;
            }
            case LocalWarehouseOpLog.DROP_CNGROUP: {
                replayDropCNGroup(log.getCNGroupName());
                break;
            }
            case LocalWarehouseOpLog.ENABLE_CNGROUP: {
                replayEnableCNGroup(log.getCNGroupName());
                break;
            }
            case LocalWarehouseOpLog.DISABLE_CNGROUP: {
                replayDisableCNGroup(log.getCNGroupName());
                break;
            }
            default:
                LOG.warn("Unknown warehouse internal op type: {}, ignored!", op);
                break;
        }
    }

    @Override
    public void gsonPostProcess() {
        if (clusters.isEmpty()) {
            if (cluster != null) {
                // this is only true when the warehouse is upgraded from an older version without multi-cngroup implementation
                cluster.postUpgradeUpdateNameIfNeeded();
                // make sure `clusters` is usable after upgrading from an older version
                clusters.add(cluster);
            }
        }
    }

    @Override
    public boolean isAvailable() {
        return state == WarehouseState.AVAILABLE;
    }
}
