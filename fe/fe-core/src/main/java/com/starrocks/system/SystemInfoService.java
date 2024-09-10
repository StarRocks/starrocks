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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/SystemInfoService.java

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

package com.starrocks.system;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.datacache.DataCacheMetrics;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.CancelDecommissionDiskInfo;
import com.starrocks.persist.CancelDisableDiskInfo;
import com.starrocks.persist.DecommissionDiskInfo;
import com.starrocks.persist.DisableDiskInfo;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.ModifyBackendClause;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResourceGroupUsage;
import com.starrocks.thrift.TStatusCode;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SystemInfoService implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);
    public static final String DEFAULT_CLUSTER = "default_cluster";

    @SerializedName(value = "be")
    protected volatile ConcurrentHashMap<Long, Backend> idToBackendRef;

    @SerializedName(value = "ce")
    protected volatile ConcurrentHashMap<Long, ComputeNode> idToComputeNodeRef;

    protected volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef;
    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef;

    private final NodeSelector nodeSelector;

    public SystemInfoService() {
        idToBackendRef = new ConcurrentHashMap<>();
        idToComputeNodeRef = new ConcurrentHashMap<>();

        idToReportVersionRef = ImmutableMap.of();
        pathHashToDishInfoRef = ImmutableMap.of();

        nodeSelector = new NodeSelector(this);
    }

    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs)
            throws DdlException {

        for (Pair<String, Integer> pair : hostPortPairs) {
            checkSameNodeExist(pair.first, pair.second);
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addComputeNode(pair.first, pair.second);
        }
    }

    private ComputeNode getComputeNodeWithHeartbeatPort(String host, Integer heartPort) {
        for (ComputeNode computeNode : idToComputeNodeRef.values()) {
            if (computeNode.getHost().equals(host) && computeNode.getHeartbeatPort() == heartPort) {
                return computeNode;
            }
        }
        return null;
    }

    public NodeSelector getNodeSelector() {
        return nodeSelector;
    }

    /**
     * For test.
     */
    public void addComputeNode(ComputeNode computeNode) {
        idToComputeNodeRef.put(computeNode.getId(), computeNode);
    }

    /**
     * For TEST only!
     */
    public void dropComputeNode(ComputeNode computeNode) {
        idToComputeNodeRef.remove(computeNode.getId());
    }

    // Final entry of adding compute node
    private void addComputeNode(String host, int heartbeatPort) {
        ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        idToComputeNodeRef.put(newComputeNode.getId(), newComputeNode);
        setComputeNodeOwner(newComputeNode);

        newComputeNode.setWorkerGroupId(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        newComputeNode.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddComputeNode(newComputeNode);
        LOG.info("finished to add {} ", newComputeNode);
    }

    public void setComputeNodeOwner(ComputeNode computeNode) {
        computeNode.setBackendState(BackendState.using);
    }

    public boolean isSingleBackendAndComputeNode() {
        return idToBackendRef.size() + idToComputeNodeRef.size() == 1;
    }

    /**
     * @param hostPortPairs : backend's host and port
     */
    public void addBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            checkSameNodeExist(pair.first, pair.second);
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addBackend(pair.first, pair.second);
        }
    }

    public void checkSameNodeExist(String host, int heartPort) throws DdlException {
        // check is already exist
        if (getBackendWithHeartbeatPort(host, heartPort) != null) {
            throw new DdlException("Backend already exists with same host " + host + " and port " + heartPort);
        }

        if (getComputeNodeWithHeartbeatPort(host, heartPort) != null) {
            throw new DdlException("Compute node already exists with same host " + host + " and port " + heartPort);
        }
    }

    // for test
    public void dropBackend(Backend backend) {
        idToBackendRef.remove(backend.getId());

        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);
    }

    // for test
    public void addBackend(Backend backend) {
        idToBackendRef.put(backend.getId(), backend);

        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(backend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);
    }

    public void setBackendOwner(Backend backend) {
        backend.setBackendState(BackendState.using);
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort) {
        Backend newBackend = new Backend(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // update idToBackend
        idToBackendRef.put(newBackend.getId(), newBackend);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        // add backend to DEFAULT_CLUSTER
        setBackendOwner(newBackend);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends are changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public ShowResultSet modifyBackendHost(ModifyBackendClause modifyBackendClause) throws DdlException {
        String willBeModifiedHost = modifyBackendClause.getSrcHost();
        String fqdn = modifyBackendClause.getDestHost();
        List<Backend> candidateBackends = getBackendOnlyWithHost(willBeModifiedHost);
        if (null == candidateBackends || candidateBackends.isEmpty()) {
            throw new DdlException(String.format("backend [%s] not found", willBeModifiedHost));
        }

        Backend preUpdateBackend = candidateBackends.get(0);
        Backend updateBackend = idToBackendRef.get(preUpdateBackend.getId());
        updateBackend.setHost(fqdn);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logBackendStateChange(updateBackend);

        // Message
        StringBuilder formatSb = new StringBuilder();
        String opMessage;
        formatSb.append("%s:%d's host has been modified to %s");
        if (candidateBackends.size() >= 2) {
            formatSb.append("\nplease execute %d times, to modify the remaining backends\n");
            for (int i = 1; i < candidateBackends.size(); i++) {
                Backend be = candidateBackends.get(i);
                formatSb.append(NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())).
                        append("\n");
            }
            opMessage = String.format(
                    formatSb.toString(), willBeModifiedHost,
                    updateBackend.getHeartbeatPort(), fqdn, candidateBackends.size() - 1);
        } else {
            opMessage = String.format(formatSb.toString(), willBeModifiedHost, updateBackend.getHeartbeatPort(), fqdn);
        }
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Message", ScalarType.createVarchar(1024)));
        List<List<String>> messageResult = new ArrayList<>();
        messageResult.add(Collections.singletonList(opMessage));
        return new ShowResultSet(builder.build(), messageResult);
    }

    public ShowResultSet modifyBackendProperty(ModifyBackendClause modifyBackendClause) throws DdlException {
        String backendHostPort = modifyBackendClause.getBackendHostPort();
        Map<String, String> properties = modifyBackendClause.getProperties();

        // check backend existence
        Backend backend = getBackendWithHeartbeatPort(backendHostPort.split(":")[0],
                Integer.parseInt(backendHostPort.split(":")[1]));
        if (null == backend) {
            throw new DdlException(String.format("backend [%s] not found", backendHostPort));
        }

        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Message", ScalarType.createVarchar(1024)));
        List<List<String>> messageResult = new ArrayList<>();

        // update backend based on properties
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(AlterSystemStmtAnalyzer.PROP_KEY_LOCATION)) {
                Map<String, String> location = new HashMap<>();
                // "" means clean backend location label
                if (entry.getValue().isEmpty()) {
                    backend.setLocation(location);
                    continue;
                }
                String[] locKV = entry.getValue().split(":");
                location.put(locKV[0].trim(), locKV[1].trim());
                backend.setLocation(location);
                String opMessage = String.format("%s:%d's location has been modified to %s",
                        backend.getHost(), backend.getHeartbeatPort(), properties);
                messageResult.add(Collections.singletonList(opMessage));
            } else {
                throw new UnsupportedOperationException("unsupported property: " + entry.getKey());
            }
        }

        // persistence
        GlobalStateMgr.getCurrentState().getEditLog().logBackendStateChange(backend);

        // Return message
        return new ShowResultSet(builder.build(), messageResult);
    }

    public ShowResultSet modifyBackend(ModifyBackendClause modifyBackendClause) throws DdlException {
        String backendHostPort = modifyBackendClause.getBackendHostPort();
        if (backendHostPort == null) {
            // modify backend host
            return modifyBackendHost(modifyBackendClause);
        } else {
            // modify backend property
            return modifyBackendProperty(modifyBackendClause);
        }
    }

    public void dropComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getComputeNodeWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("compute node does not exists[" +
                        NetUtils.getHostPortInAccessibleFormat(pair.first, pair.second) + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropComputeNode(pair.first, pair.second);
        }
    }

    public void dropComputeNode(String host, int heartbeatPort)
            throws DdlException {
        ComputeNode dropComputeNode = getComputeNodeWithHeartbeatPort(host, heartbeatPort);
        if (dropComputeNode == null) {
            throw new DdlException("compute node does not exists[" +
                    NetUtils.getHostPortInAccessibleFormat(host, heartbeatPort) + "]");
        }

        // update idToComputeNode
        idToComputeNodeRef.remove(dropComputeNode.getId());

        // remove from BackendCoreStat
        BackendResourceStat.getInstance().removeBe(dropComputeNode.getId());

        // remove worker
        if (RunMode.isSharedDataMode()) {
            int starletPort = dropComputeNode.getStarletPort();
            // only need to remove worker after be reported its staretPort
            if (starletPort != 0) {
                String workerAddr = NetUtils.getHostPortInAccessibleFormat(dropComputeNode.getHost(), starletPort);
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(workerAddr, dropComputeNode.getWorkerGroupId());
            }
        }

        // log
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDropComputeNode(new DropComputeNodeLog(dropComputeNode.getId()));
        LOG.info("finished to drop {}", dropComputeNode);
    }

    public void dropBackends(DropBackendClause dropBackendClause) throws DdlException {
        List<Pair<String, Integer>> hostPortPairs = dropBackendClause.getHostPortPairs();
        boolean needCheckWithoutForce = !dropBackendClause.isForce();

        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("backend does not exists[" +
                        NetUtils.getHostPortInAccessibleFormat(pair.first, pair.second) + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropBackend(pair.first, pair.second, needCheckWithoutForce);
        }
    }

    // for decommission
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }

        dropBackend(backend.getHost(), backend.getHeartbeatPort(), false);
    }

    protected void checkWhenNotForceDrop(Backend droppedBackend) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Long> tabletIds =
                GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletIdsByBackendId(droppedBackend.getId());
        List<Long> dbs = globalStateMgr.getLocalMetastore().getDbIds();

        dbs.stream().map(dbId -> globalStateMgr.getLocalMetastore().getDb(dbId)).forEach(db -> {
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId()).stream()
                        .filter(Table::isOlapTableOrMaterializedView)
                        .map(table -> (OlapTable) table)
                        .filter(table -> table.getTableProperty().getReplicationNum() == 1)
                        .forEach(table -> table.getAllPhysicalPartitions().forEach(partition -> {
                            String errMsg = String.format("Tables such as [%s.%s] on the backend[%s:%d]" +
                                            " have only one replica. To avoid data loss," +
                                            " please change the replication_num of [%s.%s] to three." +
                                            " ALTER SYSTEM DROP BACKEND <backends> FORCE" +
                                            " can be used to forcibly drop the backend. ",
                                    db.getOriginName(), table.getName(), droppedBackend.getHost(),
                                    droppedBackend.getHeartbeatPort(), db.getOriginName(), table.getName());

                            partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)
                                    .forEach(rollupIdx -> {
                                        boolean existIntersection = rollupIdx.getTablets().stream()
                                                .map(Tablet::getId).anyMatch(tabletIds::contains);

                                        if (existIntersection) {
                                            throw new RuntimeException(errMsg);
                                        }
                                    });
                        }));
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        });
    }

    // final entry of dropping backend
    public void dropBackend(String host, int heartbeatPort, boolean needCheckWithoutForce) throws DdlException {
        if (getBackendWithHeartbeatPort(host, heartbeatPort) == null) {
            throw new DdlException("backend does not exists[" +
                    NetUtils.getHostPortInAccessibleFormat(host, heartbeatPort) + "]");
        }

        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (needCheckWithoutForce) {
            try {
                checkWhenNotForceDrop(droppedBackend);
            } catch (RuntimeException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // update idToBackend
        idToBackendRef.remove(droppedBackend.getId());

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(droppedBackend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        // remove from BackendCoreStat
        BackendResourceStat.getInstance().removeBe(droppedBackend.getId());

        // remove worker
        if (RunMode.isSharedDataMode()) {
            int starletPort = droppedBackend.getStarletPort();
            // only need to remove worker after be reported its staretPort
            if (starletPort != 0) {
                String workerAddr = NetUtils.getHostPortInAccessibleFormat(droppedBackend.getHost(), starletPort);
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(workerAddr, droppedBackend.getWorkerGroupId());
            }
        }

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends are changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    private Backend getBackendByHostPort(String hostPort) throws DdlException {
        String[] items = hostPort.split(":");
        if (items.length != 2) {
            throw new DdlException("invalid BE format: " + hostPort + ", host and port should be separated by ':'");
        }

        int port;
        try {
            port = Integer.parseInt(items[1]);
        } catch (NumberFormatException e) {
            throw new DdlException("invalid port format: " + items[1]);
        }

        Backend backend = getBackendWithHeartbeatPort(items[0], port);
        if (backend == null) {
            throw new DdlException("Backend: " + hostPort + " does not exist");
        }
        return backend;
    }

    public void decommissionDisks(String beHostPort, List<String> diskList) throws DdlException {
        Backend backend = getBackendByHostPort(beHostPort);
        for (String disk : diskList) {
            backend.decommissionDisk(disk);
        }

        GlobalStateMgr.getCurrentState().getEditLog()
                .logDecommissionDisk(new DecommissionDiskInfo(backend.getId(), diskList));
    }

    public void cancelDecommissionDisks(String beHostPort, List<String> diskList) throws DdlException {
        Backend backend = getBackendByHostPort(beHostPort);
        for (String disk : diskList) {
            backend.cancelDecommissionDisk(disk);
        }

        GlobalStateMgr.getCurrentState().getEditLog()
                .logCancelDecommissionDisk(new CancelDecommissionDiskInfo(backend.getId(), diskList));
    }

    public void disableDisks(String beHostPort, List<String> diskList) throws DdlException {
        Backend backend = getBackendByHostPort(beHostPort);
        for (String disk : diskList) {
            backend.disableDisk(disk);
        }

        GlobalStateMgr.getCurrentState().getEditLog().logDisableDisk(new DisableDiskInfo(backend.getId(), diskList));
    }

    public void cancelDisableDisks(String beHostPort, List<String> diskList) throws DdlException {
        Backend backend = getBackendByHostPort(beHostPort);
        for (String disk : diskList) {
            backend.cancelDisableDisk(disk);
        }

        GlobalStateMgr.getCurrentState().getEditLog()
                .logCancelDisableDisk(new CancelDisableDiskInfo(backend.getId(), diskList));
    }

    public void replayDecommissionDisks(DecommissionDiskInfo info) {
        Backend backend = getBackend(info.getBeId());
        if (backend == null) {
            LOG.warn("replay decommission disk failed, backend:{} does not exist", info.getBeId());
            return;
        }
        for (String disk : info.getDiskList()) {
            try {
                backend.decommissionDisk(disk);
            } catch (DdlException e) {
                LOG.warn("replay decommission disk failed", e);
            }
        }
    }

    public void replayCancelDecommissionDisks(CancelDecommissionDiskInfo info) {
        Backend backend = getBackend(info.getBeId());
        if (backend == null) {
            LOG.warn("replay cancel decommission disk failed, backend:{} does not exist", info.getBeId());
            return;
        }
        for (String disk : info.getDiskList()) {
            try {
                backend.cancelDecommissionDisk(disk);
            } catch (DdlException e) {
                LOG.warn("replay cancel decommission disk failed", e);
            }
        }
    }

    public void replayDisableDisks(DisableDiskInfo info) {
        Backend backend = getBackend(info.getBeId());
        if (backend == null) {
            LOG.warn("replay disable disk failed, backend:{} does not exist", info.getBeId());
            return;
        }

        for (String disk : info.getDiskList()) {
            try {
                backend.disableDisk(disk);
            } catch (DdlException e) {
                LOG.warn("replay disable disk failed", e);
            }
        }
    }

    public void replayCancelDisableDisks(CancelDisableDiskInfo info) {
        Backend backend = getBackend(info.getBeId());
        if (backend == null) {
            LOG.warn("replay cancel disable disk failed, backend:{} does not exist", info.getBeId());
            return;
        }

        for (String disk : info.getDiskList()) {
            try {
                backend.cancelDisableDisk(disk);
            } catch (DdlException e) {
                LOG.warn("replay cancel disable disk failed", e);
            }
        }
    }

    // only for test
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef.clear();
        // update idToReportVersion
        idToReportVersionRef = ImmutableMap.of();
    }

    // only for test
    public void dropAllComputeNode() {
        // update idToComputeNodeRef
        idToComputeNodeRef.clear();
    }

    public Backend getBackend(long backendId) {
        return idToBackendRef.get(backendId);
    }

    public ComputeNode getComputeNode(long computeNodeId) {
        return idToComputeNodeRef.get(computeNodeId);
    }

    public ComputeNode getBackendOrComputeNode(long nodeId) {
        ComputeNode backend = idToBackendRef.get(nodeId);
        if (backend == null) {
            backend = idToComputeNodeRef.get(nodeId);
        }
        return backend;
    }

    public void updateDataCacheMetrics(long backendId, DataCacheMetrics dataCacheMetrics) {
        ComputeNode node = getBackendOrComputeNode(backendId);
        if (node == null) {
            LOG.warn("updateDataCacheMetrics receives a non-exist backend/compute [id={}]", backendId);
            return;
        }
        node.updateDataCacheMetrics(dataCacheMetrics);
    }

    public void updateResourceUsage(long backendId, int numRunningQueries, long memUsedBytes,
                                    int cpuUsedPermille, List<TResourceGroupUsage> groupUsages) {
        ComputeNode node = getBackendOrComputeNode(backendId);
        if (node == null) {
            LOG.warn("updateResourceUsage receives a non-exist backend/compute [id={}]", backendId);
            return;
        }

        node.updateResourceUsage(numRunningQueries, memUsedBytes, cpuUsedPermille);

        if (groupUsages != null) {
            List<Pair<ResourceGroup, TResourceGroupUsage>> groupAndUsages = new ArrayList<>(groupUsages.size());
            for (TResourceGroupUsage usage : groupUsages) {
                ResourceGroup group = GlobalStateMgr.getCurrentState().getResourceGroupMgr()
                        .getResourceGroup(usage.getGroup_id());
                if (group == null) {
                    continue;
                }
                groupAndUsages.add(Pair.create(group, usage));
            }
            node.updateResourceGroupUsage(groupAndUsages);
        }

        GlobalStateMgr.getCurrentState().getResourceUsageMonitor().notifyResourceUsageUpdate();
    }

    public boolean checkBackendAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        return backend != null && backend.isAvailable();
    }

    public boolean checkNodeAvailable(ComputeNode node) {
        if (node != null) {
            if (node instanceof Backend) {
                return node.isAvailable();
            } else {
                return node.isAlive();
            }
        }
        return false;
    }

    public boolean checkBackendAlive(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        return backend != null && backend.isAlive();
    }

    public ComputeNode getComputeNodeWithHeartbeatPort(String host, int heartPort) {
        for (ComputeNode computeNode : idToComputeNodeRef.values()) {
            if (NetUtils.isSameIP(computeNode.getHost(), host) && computeNode.getHeartbeatPort() == heartPort) {
                return computeNode;
            }
        }
        return null;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        for (Backend backend : idToBackendRef.values()) {
            if (NetUtils.isSameIP(backend.getHost(), host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public long getBackendIdWithStarletPort(String host, int starletPort) {
        for (Backend backend : idToBackendRef.values()) {
            if (NetUtils.isSameIP(backend.getHost(), host) && backend.getStarletPort() == starletPort) {
                return backend.getId();
            }
        }
        return -1L;
    }

    public long getComputeNodeIdWithStarletPort(String host, int starletPort) {
        for (ComputeNode cn : idToComputeNodeRef.values()) {
            if (NetUtils.isSameIP(cn.getHost(), host) && cn.getStarletPort() == starletPort) {
                return cn.getId();
            }
        }
        return -1L;
    }

    public static TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
        ComputeNode computeNode = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (computeNode == null) {
            computeNode =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                            .getComputeNodeWithBePort(host.getHostname(), host.getPort());
            if (computeNode == null) {
                throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR);
            }
        }
        if (computeNode.getBrpcPort() < 0) {
            return null;
        }
        return new TNetworkAddress(computeNode.getHost(), computeNode.getBrpcPort());
    }

    public Backend getBackendWithBePort(String host, int bePort) {
        return getComputeNodeWithBePortCommon(host, bePort, idToBackendRef);
    }

    public ComputeNode getBackendOrComputeNodeWithBePort(String host, int bePort) {
        ComputeNode node = getBackendWithBePort(host, bePort);
        if (node == null) {
            node = getComputeNodeWithBePort(host, bePort);
        }
        return node;
    }

    public List<Backend> getBackendOnlyWithHost(String host) {
        List<Backend> resultBackends = new ArrayList<>();
        for (Backend backend : idToBackendRef.values()) {
            if (backend.getHost().equals(host)) {
                resultBackends.add(backend);
            }
        }
        return resultBackends;
    }

    public List<Long> getBackendIds() {
        return getBackendIds(false);
    }

    public int getAliveBackendNumber() {
        return getBackendIds(true).size();
    }

    public int getTotalBackendNumber() {
        return idToBackendRef.size();
    }

    public int getTotalComputeNodeNumber() {
        return idToComputeNodeRef.size();
    }

    public int getAliveComputeNodeNumber() {
        return getComputeNodeIds(true).size();
    }

    public ComputeNode getComputeNodeWithBePort(String host, int bePort) {
        return getComputeNodeWithBePortCommon(host, bePort, idToComputeNodeRef);
    }

    private <T extends ComputeNode> T getComputeNodeWithBePortCommon(String host, int bePort,
                                                                     Map<Long, T> nodeRef) {
        Pair<String, String> targetPair;
        try {
            targetPair = NetUtils.getIpAndFqdnByHost(host);
        } catch (UnknownHostException e) {
            LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
            return null;
        }

        for (T computeNode : nodeRef.values()) {
            Pair<String, String> curPair;
            try {
                curPair = NetUtils.getIpAndFqdnByHost(computeNode.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
                continue;
            }
            boolean hostMatch = false;
            // target, cur has same ip
            if (NetUtils.isSameIP(targetPair.first, curPair.first)) {
                hostMatch = true;
            }
            // target, cur has same fqdn and both of them are not equal ""
            if (!hostMatch && targetPair.second.equals(curPair.second) && !curPair.second.equals("")) {
                hostMatch = true;
            }
            if (hostMatch && (computeNode.getBePort() == bePort)) {
                return computeNode;
            }
        }
        return null;
    }

    public List<Long> getComputeNodeIds(boolean needAlive) {
        List<Long> computeNodeIds = Lists.newArrayList(idToComputeNodeRef.keySet());
        if (needAlive) {
            Iterator<Long> iter = computeNodeIds.iterator();
            while (iter.hasNext()) {
                ComputeNode computeNode = this.getComputeNode(iter.next());
                if (computeNode == null || !computeNode.isAlive()) {
                    iter.remove();
                }
            }
        }
        return computeNodeIds;
    }

    public List<Long> getBackendIds(boolean needAlive) {
        List<Long> backendIds = Lists.newArrayList(idToBackendRef.keySet());
        if (needAlive) {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
        }
        return backendIds;
    }

    public List<Long> getDecommissionedBackendIds() {
        List<Long> backendIds = Lists.newArrayList(idToBackendRef.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isDecommissioned()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    public List<Long> getAvailableBackendIds() {
        List<Long> backendIds = Lists.newArrayList(idToBackendRef.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isAvailable()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    public List<Long> getAvailableComputeNodeIds() {
        List<Long> computeNodeIds = Lists.newArrayList(idToComputeNodeRef.keySet());

        Iterator<Long> iter = computeNodeIds.iterator();
        while (iter.hasNext()) {
            ComputeNode cn = this.getComputeNode(iter.next());
            if (cn == null || !cn.isAvailable()) {
                iter.remove();
            }
        }
        return computeNodeIds;
    }

    public List<Backend> getBackends() {
        return Lists.newArrayList(idToBackendRef.values());
    }

    public List<Backend> getAvailableBackends() {
        return getBackends().stream()
                .filter(ComputeNode::isAvailable)
                .collect(Collectors.toList());
    }

    public List<ComputeNode> getComputeNodes() {
        return Lists.newArrayList(idToComputeNodeRef.values());
    }

    public List<ComputeNode> getAvailableComputeNodes() {
        return getComputeNodes().stream()
                .filter(ComputeNode::isAvailable)
                .collect(Collectors.toList());
    }

    public Stream<ComputeNode> backendAndComputeNodeStream() {
        return Stream.concat(idToBackendRef.values().stream(), idToComputeNodeRef.values().stream());
    }

    public ImmutableMap<Long, Backend> getIdToBackend() {
        return ImmutableMap.copyOf(idToBackendRef);
    }

    public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
        return ImmutableMap.copyOf(idToComputeNodeRef);
    }

    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong;
        if ((atomicLong = idToReportVersionRef.get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId) {
        ComputeNode node = getBackendOrComputeNode(backendId);
        // only backend need to report version
        if (node instanceof Backend) {
            AtomicLong atomicLong;
            if ((atomicLong = idToReportVersionRef.get(backendId)) != null) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
                if (db != null) {
                    updateReportVersionIncrementally(atomicLong, newReportVersion);
                    LOG.debug("update backend {} report version: {}, db: {}", backendId, newReportVersion, dbId);
                } else {
                    LOG.warn("failed to update backend report version, db {} does not exist", dbId);
                }
            } else {
                LOG.warn("failed to update backend report version, backend {} does not exist", backendId);
            }
        }
    }

    protected synchronized void updateReportVersionIncrementally(AtomicLong currentVersion, long newVersion) {
        if (currentVersion.get() < newVersion) {
            currentVersion.set(newVersion);
        }
    }

    public void clear() {
        this.idToBackendRef = new ConcurrentHashMap<>();
        this.idToReportVersionRef = ImmutableMap.of();
    }

    public static Pair<String, Integer> validateHostAndPort(String hostPort, boolean resolveHost) {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new SemanticException("Invalid host port: " + hostPort);
        }

        String[] hostInfo;
        try {
            hostInfo = NetUtils.resolveHostInfoFromHostPort(hostPort);
        } catch (AnalysisException e) {
            throw new SemanticException("Invalid host port: " + hostPort, e);
        }
        String host = hostInfo[0];
        if (Strings.isNullOrEmpty(host)) {
            throw new SemanticException("Host is null");
        }

        int heartbeatPort;
        try {
            // validate host
            if (resolveHost && !InetAddressValidator.getInstance().isValid(host)) {
                // maybe this is a hostname
                // if no IP address for the host could be found, 'getByName'
                // will throw
                // UnknownHostException
                InetAddress inetAddress = InetAddress.getByName(host);
                host = inetAddress.getHostAddress();
            }

            // validate port
            heartbeatPort = Integer.parseInt(hostInfo[1]);

            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new SemanticException("Port is out of range: " + heartbeatPort);
            }

            return new Pair<>(host, heartbeatPort);
        } catch (UnknownHostException e) {
            throw new SemanticException("Unknown host: " + e.getMessage());
        } catch (Exception e) {
            throw new SemanticException("Encounter unknown exception: " + e.getMessage());
        }
    }

    public void replayAddComputeNode(ComputeNode newComputeNode) {
        // update idToComputeNode
        newComputeNode.setBackendState(BackendState.using);
        idToComputeNodeRef.put(newComputeNode.getId(), newComputeNode);
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        idToBackendRef.put(newBackend.getId(), newBackend);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);
    }

    public void replayDropComputeNode(long computeNodeId) {
        LOG.debug("replayDropComputeNode: {}", computeNodeId);
        // update idToComputeNode
        ComputeNode cn = idToComputeNodeRef.remove(computeNodeId);

        // BackendCoreStat is a global state, checkpoint should not modify it.
        if (!GlobalStateMgr.isCheckpointThread()) {
            // remove from BackendCoreStat
            BackendResourceStat.getInstance().removeBe(computeNodeId);
        }

        // clear map in starosAgent
        if (RunMode.isSharedDataMode()) {
            int starletPort = cn.getStarletPort();
            if (starletPort == 0) {
                return;
            }
            String workerAddr = NetUtils.getHostPortInAccessibleFormat(cn.getHost(), starletPort);
            GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorkerFromMap(workerAddr);
        }
    }

    public void replayDropBackend(Backend backend) {
        LOG.debug("replayDropBackend: {}", backend);
        // update idToBackend
        idToBackendRef.remove(backend.getId());

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        // BackendCoreStat is a global state, checkpoint should not modify it.
        if (!GlobalStateMgr.isCheckpointThread()) {
            // remove from BackendCoreStat
            BackendResourceStat.getInstance().removeBe(backend.getId());
        }

        // clear map in starosAgent
        if (RunMode.isSharedDataMode()) {
            int starletPort = backend.getStarletPort();
            if (starletPort == 0) {
                return;
            }
            String workerAddr = NetUtils.getHostPortInAccessibleFormat(backend.getHost(), starletPort);
            GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorkerFromMap(workerAddr);
        }
    }

    public void updateInMemoryStateBackend(Backend persistentState) {
        long id = persistentState.getId();
        Backend inMemoryState = getBackend(id);
        if (inMemoryState == null) {
            // backend may already be dropped. this may happen when
            // 1. SystemHandler drop the decommission backend
            // 2. at same time, user try to cancel the decommission of that backend.
            // These two operations do not guarantee the order.
            return;
        }
        inMemoryState.setBePort(persistentState.getBePort());
        inMemoryState.setHost(persistentState.getHost());
        inMemoryState.setAlive(persistentState.isAlive());
        inMemoryState.setDecommissioned(persistentState.isDecommissioned());
        inMemoryState.setHttpPort(persistentState.getHttpPort());
        inMemoryState.setBeRpcPort(persistentState.getBeRpcPort());
        inMemoryState.setBrpcPort(persistentState.getBrpcPort());
        inMemoryState.setLastUpdateMs(persistentState.getLastUpdateMs());
        inMemoryState.setLastStartTime(persistentState.getLastStartTime());
        inMemoryState.setDisks(persistentState.getDisks());
        inMemoryState.setBackendState(persistentState.getBackendState());
        inMemoryState.setDecommissionType(persistentState.getDecommissionType());
        inMemoryState.setLocation(persistentState.getLocation());
    }

    public long getClusterAvailableCapacityB() {
        List<Backend> clusterBackends = getBackends();
        long capacity = 0L;
        for (Backend backend : clusterBackends) {
            // Here we do not check if backend is alive,
            // We suppose the dead backends will back to alive later.
            if (backend.isDecommissioned()) {
                // Data on decommissioned backend will move to other backends,
                // So we need to minus size of those data.
                capacity -= backend.getDataUsedCapacityB();
            } else {
                capacity += backend.getAvailableCapacityB();
            }
        }
        return capacity;
    }

    public void checkClusterCapacity() throws DdlException {
        if (RunMode.isSharedDataMode()) {
            return;
        }

        if (getClusterAvailableCapacityB() <= 0L) {
            throw new SemanticException("Cluster has no available capacity");
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    public long getBackendIdByHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = ImmutableMap.copyOf(idToBackendRef);
        List<Backend> selectedBackends = Lists.newArrayList();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host)) {
                selectedBackends.add(backend);
            }
        }

        if (selectedBackends.isEmpty()) {
            return -1L;
        }

        Collections.shuffle(selectedBackends);
        return selectedBackends.get(0).getId();
    }

    public String getBackendHostById(long backendId) {
        Backend backend = getBackend(backendId);
        return backend == null ? null : backend.getHost();
    }

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If usingHardLimit is true, it will check with the usingHardLimit threshold.
     *
     * return Status.OK if not reach the limit
     */
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean usingHardLimit) {
        LOG.debug("pathBeMap: {}", bePathsMap);
        if (RunMode.getCurrentRunMode() != RunMode.SHARED_DATA) {
            ImmutableMap<Long, DiskInfo> pathHashToDiskInfo = pathHashToDishInfoRef;
            for (Long beId : bePathsMap.keySet()) {
                for (Long pathHash : bePathsMap.get(beId)) {
                    DiskInfo diskInfo = pathHashToDiskInfo.get(pathHash);
                    if (diskInfo != null && diskInfo.exceedLimit(usingHardLimit)) {
                        return new Status(TStatusCode.CANCELLED,
                                "disk " + pathHash + " on backend " + beId + " exceed limit usage");
                    }
                }
            }
        }
        return Status.OK;
    }

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
        Map<Long, DiskInfo> copiedPathInfos = Maps.newHashMap(pathHashToDishInfoRef);
        for (DiskInfo diskInfo : addedDisks) {
            copiedPathInfos.put(diskInfo.getPathHash(), diskInfo);
        }
        for (DiskInfo diskInfo : removedDisks) {
            copiedPathInfos.remove(diskInfo.getPathHash());
        }
        ImmutableMap<Long, DiskInfo> newPathInfos = ImmutableMap.copyOf(copiedPathInfos);
        pathHashToDishInfoRef = newPathInfos;
        LOG.debug("update path infos: {}", newPathInfos);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        Map<Long, AtomicLong> idToReportVersion = new HashMap<>();
        for (long beId : idToBackendRef.keySet()) {
            idToReportVersion.put(beId, new AtomicLong(0));
        }
        idToReportVersionRef = ImmutableMap.copyOf(idToReportVersion);

        // BackendCoreStat is a global state, checkpoint should not modify it.
        if (!GlobalStateMgr.isCheckpointThread()) {
            // update BackendCoreStat
            for (ComputeNode node : idToBackendRef.values()) {
                BackendResourceStat.getInstance().setNumHardwareCoresOfBe(node.getId(), node.getCpuCores());
                BackendResourceStat.getInstance().setMemLimitBytesOfBe(node.getId(), node.getMemLimitBytes());
            }
            for (ComputeNode node : idToComputeNodeRef.values()) {
                BackendResourceStat.getInstance().setNumHardwareCoresOfBe(node.getId(), node.getCpuCores());
                BackendResourceStat.getInstance().setMemLimitBytesOfBe(node.getId(), node.getMemLimitBytes());
            }
        }
    }
}

