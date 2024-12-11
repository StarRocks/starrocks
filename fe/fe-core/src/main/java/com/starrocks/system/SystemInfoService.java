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

<<<<<<< HEAD
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
=======
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
import com.starrocks.catalog.Tablet;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.NetUtils;
=======
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.util.NetUtils;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.datacache.DataCacheMetrics;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.CancelDecommissionDiskInfo;
import com.starrocks.persist.CancelDisableDiskInfo;
import com.starrocks.persist.DecommissionDiskInfo;
import com.starrocks.persist.DisableDiskInfo;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.persist.gson.GsonPostProcessable;
<<<<<<< HEAD
import com.starrocks.persist.gson.GsonUtils;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
<<<<<<< HEAD
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
=======
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AddComputeNodeClause;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.DropComputeNodeClause;
import com.starrocks.sql.ast.ModifyBackendClause;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.system.Backend.BackendState;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TResourceGroupUsage;
import com.starrocks.thrift.TStatusCode;
<<<<<<< HEAD
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.collections.CollectionUtils;
=======
import com.starrocks.warehouse.Warehouse;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

<<<<<<< HEAD
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
<<<<<<< HEAD
import java.util.Arrays;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
<<<<<<< HEAD
import java.util.function.Predicate;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SystemInfoService implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);
    public static final String DEFAULT_CLUSTER = "default_cluster";

    @SerializedName(value = "be")
<<<<<<< HEAD
    private volatile ConcurrentHashMap<Long, Backend> idToBackendRef;

    @SerializedName(value = "ce")
    private volatile ConcurrentHashMap<Long, ComputeNode> idToComputeNodeRef;

    private long lastNodeIdForCreation = -1;
    private long lastNodeIdForOther = -1;

    private volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef;
    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef;

=======
    protected volatile ConcurrentHashMap<Long, Backend> idToBackendRef;

    @SerializedName(value = "ce")
    protected volatile ConcurrentHashMap<Long, ComputeNode> idToComputeNodeRef;

    protected volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef;
    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef;

    private final NodeSelector nodeSelector;

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public SystemInfoService() {
        idToBackendRef = new ConcurrentHashMap<>();
        idToComputeNodeRef = new ConcurrentHashMap<>();

        idToReportVersionRef = ImmutableMap.of();
        pathHashToDishInfoRef = ImmutableMap.of();
<<<<<<< HEAD
    }

    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs)
            throws DdlException {

        for (Pair<String, Integer> pair : hostPortPairs) {
            checkSameNodeExist(pair.first, pair.second);
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addComputeNode(pair.first, pair.second);
=======

        nodeSelector = new NodeSelector(this);
    }

    public void addComputeNodes(AddComputeNodeClause addComputeNodeClause)
            throws DdlException {

        for (Pair<String, Integer> pair : addComputeNodeClause.getHostPortPairs()) {
            checkSameNodeExist(pair.first, pair.second);
        }

        for (Pair<String, Integer> pair : addComputeNodeClause.getHostPortPairs()) {
            addComputeNode(pair.first, pair.second, addComputeNodeClause.getWarehouse());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
=======
    public NodeSelector getNodeSelector() {
        return nodeSelector;
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
    private void addComputeNode(String host, int heartbeatPort) {
        ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        idToComputeNodeRef.put(newComputeNode.getId(), newComputeNode);
        setComputeNodeOwner(newComputeNode);
=======
    private void addComputeNode(String host, int heartbeatPort, String warehouse) throws DdlException {
        ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        idToComputeNodeRef.put(newComputeNode.getId(), newComputeNode);
        setComputeNodeOwner(newComputeNode);
        addComputeNodeToWarehouse(newComputeNode, warehouse);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddComputeNode(newComputeNode);
        LOG.info("finished to add {} ", newComputeNode);
    }

<<<<<<< HEAD
    private void setComputeNodeOwner(ComputeNode computeNode) {
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        Preconditions.checkState(cluster != null);
        cluster.addComputeNode(computeNode.getId());
=======
    public void setComputeNodeOwner(ComputeNode computeNode) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        computeNode.setBackendState(BackendState.using);
    }

    public boolean isSingleBackendAndComputeNode() {
        return idToBackendRef.size() + idToComputeNodeRef.size() == 1;
    }

<<<<<<< HEAD
    /**
     * @param hostPortPairs : backend's host and port
     * @throws DdlException
     */
    public void addBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            checkSameNodeExist(pair.first, pair.second);
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addBackend(pair.first, pair.second);
        }
    }

    private void checkSameNodeExist(String host, int heartPort) throws DdlException {
=======
    public void addBackends(AddBackendClause addBackendClause) throws DdlException {
        for (Pair<String, Integer> pair : addBackendClause.getHostPortPairs()) {
            checkSameNodeExist(pair.first, pair.second);
        }

        for (Pair<String, Integer> pair : addBackendClause.getHostPortPairs()) {
            addBackend(pair.first, pair.second, addBackendClause.getWarehouse());
        }
    }

    public void checkSameNodeExist(String host, int heartPort) throws DdlException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);
=======
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    // for test
    public void addBackend(Backend backend) {
        idToBackendRef.put(backend.getId(), backend);

<<<<<<< HEAD
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.put(backend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);
    }

    private void setBackendOwner(Backend backend) {
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        Preconditions.checkState(cluster != null);
        cluster.addBackend(backend.getId());
        backend.setBackendState(BackendState.using);
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort) {
=======
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(backend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);
    }

    public void setBackendOwner(Backend backend) {
        backend.setBackendState(BackendState.using);
    }

    public void addComputeNodeToWarehouse(ComputeNode computeNode, String warehouseName)
            throws DdlException {
        Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseName);
        // check if the warehouse exist
        if (warehouse == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", warehouseName));
        }

        computeNode.setWorkerGroupId(warehouse.getAnyWorkerGroupId());
        computeNode.setWarehouseId(warehouse.getId());
    }

    // Final entry of adding backend
    private void addBackend(String host, int heartbeatPort, String warehouse) throws DdlException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        Backend newBackend = new Backend(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // update idToBackend
        idToBackendRef.put(newBackend.getId(), newBackend);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        // add backend to DEFAULT_CLUSTER
        setBackendOwner(newBackend);
<<<<<<< HEAD
=======
        addComputeNodeToWarehouse(newBackend, warehouse);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

<<<<<<< HEAD
        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public ShowResultSet modifyBackendHost(ModifyBackendAddressClause modifyBackendAddressClause) throws DdlException {
        String willBeModifiedHost = modifyBackendAddressClause.getSrcHost();
        String fqdn = modifyBackendAddressClause.getDestHost();
        List<Backend> candidateBackends = getBackendOnlyWithHost(willBeModifiedHost);
        if (null == candidateBackends || candidateBackends.size() == 0) {
=======
        // backends are changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    public ShowResultSet modifyBackendHost(ModifyBackendClause modifyBackendClause) throws DdlException {
        if (RunMode.isSharedDataMode()) {
            throw new DdlException("modify backend host is not supported in shared-data cluster.");
        }
        String willBeModifiedHost = modifyBackendClause.getSrcHost();
        String fqdn = modifyBackendClause.getDestHost();
        List<Backend> candidateBackends = getBackendOnlyWithHost(willBeModifiedHost);
        if (null == candidateBackends || candidateBackends.isEmpty()) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                formatSb.append(be.getHost() + ":" + be.getHeartbeatPort() + "\n");
=======
                formatSb.append(NetUtils.getHostPortInAccessibleFormat(be.getHost(), be.getHeartbeatPort())).
                        append("\n");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        messageResult.add(Arrays.asList(opMessage));
        return new ShowResultSet(builder.build(), messageResult);
    }

    public void dropComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getComputeNodeWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("compute node does not exists[" + pair.first + ":" + pair.second + "]");
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
            throw new DdlException("compute node does not exists[" + host + ":" + heartbeatPort + "]");
=======
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

    public void dropComputeNodes(DropComputeNodeClause dropComputeNodeClause) throws DdlException {
        String warehouse = dropComputeNodeClause.getWarehouse();
        // check if the warehouse exist
        if (GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouse) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", warehouse));
        }

        for (Pair<String, Integer> pair : dropComputeNodeClause.getHostPortPairs()) {
            dropComputeNode(pair.first, pair.second, warehouse);
        }
    }

    public void dropComputeNode(String host, int heartbeatPort, String warehouse)
            throws DdlException {
        ComputeNode dropComputeNode = getComputeNodeWithHeartbeatPort(host, heartbeatPort);
        if (dropComputeNode == null) {
            throw new DdlException("compute node does not exists[" +
                    NetUtils.getHostPortInAccessibleFormat(host, heartbeatPort) + "]");
        }

        // check if warehouseName is right
        Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(dropComputeNode.getWarehouseId());
        if (wh != null && !warehouse.equalsIgnoreCase(wh.getName())) {
            throw new DdlException("compute node [" + host + ":" + heartbeatPort +
                    "] does not exist in warehouse " + warehouse);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        // update idToComputeNode
        idToComputeNodeRef.remove(dropComputeNode.getId());

        // remove from BackendCoreStat
<<<<<<< HEAD
        BackendCoreStat.removeNumOfHardwareCoresOfBe(dropComputeNode.getId());

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            // remove worker
            if (RunMode.isSharedDataMode()) {
                long starletPort = dropComputeNode.getStarletPort();
                // only need to remove worker after be reported its staretPort
                if (starletPort != 0) {
                    String workerAddr = dropComputeNode.getHost() + ":" + starletPort;
                    GlobalStateMgr.getCurrentStarOSAgent().removeWorker(workerAddr);
                }
            }

            cluster.removeComputeNode(dropComputeNode.getId());
        } else {
            LOG.error("Cluster {} no exist.", SystemInfoService.DEFAULT_CLUSTER);
=======
        BackendResourceStat.getInstance().removeBe(dropComputeNode.getId());

        // remove worker
        if (RunMode.isSharedDataMode()) {
            int starletPort = dropComputeNode.getStarletPort();
            // only need to remove worker after be reported its staretPort
            if (starletPort != 0) {
                String workerAddr = NetUtils.getHostPortInAccessibleFormat(dropComputeNode.getHost(), starletPort);
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(workerAddr, dropComputeNode.getWorkerGroupId());
            }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        // log
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDropComputeNode(new DropComputeNodeLog(dropComputeNode.getId()));
        LOG.info("finished to drop {}", dropComputeNode);
    }

    public void dropBackends(DropBackendClause dropBackendClause) throws DdlException {
        List<Pair<String, Integer>> hostPortPairs = dropBackendClause.getHostPortPairs();
<<<<<<< HEAD
        boolean needCheckUnforce = !dropBackendClause.isForce();

        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("backend does not exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropBackend(pair.first, pair.second, needCheckUnforce);
=======
        boolean needCheckWithoutForce = !dropBackendClause.isForce();

        String warehouse = dropBackendClause.getWarehouse();
        // check if the warehouse exist
        if (GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouse) == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_WAREHOUSE, String.format("name: %s", warehouse));
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropBackend(pair.first, pair.second, warehouse, needCheckWithoutForce);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    // for decommission
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }

<<<<<<< HEAD
        dropBackend(backend.getHost(), backend.getHeartbeatPort(), false);
    }

    private void checkUnforce(Backend droppedBackend) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Long> tabletIds = GlobalStateMgr.getCurrentInvertedIndex().getTabletIdsByBackendId(droppedBackend.getId());
        List<Long> dbs = globalStateMgr.getDbIds();

        dbs.stream().map(globalStateMgr::getDb).forEach(db -> {
            db.readLock();
            try {
                db.getTables().stream()
                        .filter(table -> table.isOlapTableOrMaterializedView())
                        .map(table -> (OlapTable) table)
                        .filter(table -> table.getTableProperty().getReplicationNum() == 1)
                        .forEach(table -> {
                            table.getAllPartitions().forEach(partition -> {
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
                            });
                        });
            } finally {
                db.readUnlock();
=======
        dropBackend(backend.getHost(), backend.getHeartbeatPort(), WarehouseManager.DEFAULT_WAREHOUSE_NAME, false);
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        });
    }

    // final entry of dropping backend
<<<<<<< HEAD
    public void dropBackend(String host, int heartbeatPort, boolean needCheckUnforce) throws DdlException {
        if (getBackendWithHeartbeatPort(host, heartbeatPort) == null) {
            throw new DdlException("backend does not exists[" + host + ":" + heartbeatPort + "]");
        }

        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (needCheckUnforce) {
            try {
                checkUnforce(droppedBackend);
=======
    public void dropBackend(String host, int heartbeatPort, String warehouse, boolean needCheckWithoutForce) throws DdlException {
        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);

        if (droppedBackend == null) {
            throw new DdlException("backend does not exists[" +
                    NetUtils.getHostPortInAccessibleFormat(host, heartbeatPort) + "]");
        }

        // check if warehouseName is right
        Warehouse wh = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(droppedBackend.getWarehouseId());
        if (wh != null && !warehouse.equalsIgnoreCase(wh.getName())) {
            LOG.warn("warehouseName in dropBackends is not equal, " +
                            "warehouseName from dropBackendClause is {}, while actual one is {}",
                    warehouse, wh.getName());
            throw new DdlException("backend [" + host + ":" + heartbeatPort +
                    "] does not exist in warehouse " + warehouse);
        }

        if (needCheckWithoutForce) {
            try {
                checkWhenNotForceDrop(droppedBackend);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            } catch (RuntimeException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // update idToBackend
        idToBackendRef.remove(droppedBackend.getId());

        // update idToReportVersion
<<<<<<< HEAD
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(droppedBackend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // remove from BackendCoreStat
        BackendCoreStat.removeNumOfHardwareCoresOfBe(droppedBackend.getId());

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            // remove worker
            if (RunMode.isSharedDataMode()) {
                long starletPort = droppedBackend.getStarletPort();
                // only need to remove worker after be reported its staretPort
                if (starletPort != 0) {
                    String workerAddr = droppedBackend.getHost() + ":" + starletPort;
                    GlobalStateMgr.getCurrentStarOSAgent().removeWorker(workerAddr);
                }
            }

            cluster.removeBackend(droppedBackend.getId());
        } else {
            LOG.error("Cluster {} no exist.", SystemInfoService.DEFAULT_CLUSTER);
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

<<<<<<< HEAD
        // backends is changed, regenerated tablet number metrics
=======
        // backends are changed, regenerated tablet number metrics
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();
=======
        idToReportVersionRef = ImmutableMap.of();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    public void updateResourceUsage(long backendId, int numRunningQueries, long memLimitBytes, long memUsedBytes,
=======
    public void updateDataCacheMetrics(long backendId, DataCacheMetrics dataCacheMetrics) {
        ComputeNode node = getBackendOrComputeNode(backendId);
        if (node == null) {
            LOG.warn("updateDataCacheMetrics receives a non-exist backend/compute [id={}]", backendId);
            return;
        }
        node.updateDataCacheMetrics(dataCacheMetrics);
    }

    public void updateResourceUsage(long backendId, int numRunningQueries, long memUsedBytes,
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                                    int cpuUsedPermille, List<TResourceGroupUsage> groupUsages) {
        ComputeNode node = getBackendOrComputeNode(backendId);
        if (node == null) {
            LOG.warn("updateResourceUsage receives a non-exist backend/compute [id={}]", backendId);
            return;
        }

<<<<<<< HEAD
        node.updateResourceUsage(numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);
=======
        node.updateResourceUsage(numRunningQueries, memUsedBytes, cpuUsedPermille);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        if (groupUsages != null) {
            List<Pair<ResourceGroup, TResourceGroupUsage>> groupAndUsages = new ArrayList<>(groupUsages.size());
            for (TResourceGroupUsage usage : groupUsages) {
                ResourceGroup group = GlobalStateMgr.getCurrentState().getResourceGroupMgr()
<<<<<<< HEAD
                        .getResourceGroupIncludingDefault(usage.getGroup_id());
=======
                        .getResourceGroup(usage.getGroup_id());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        if (node instanceof Backend) {
            return node != null && node.isAvailable();
        }
        return node != null && node.isAlive();
=======
        if (node != null) {
            if (node instanceof Backend) {
                return node.isAvailable();
            } else {
                return node.isAlive();
            }
        }
        return false;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    public boolean checkBackendAlive(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        return backend != null && backend.isAlive();
    }

    public ComputeNode getComputeNodeWithHeartbeatPort(String host, int heartPort) {
        for (ComputeNode computeNode : idToComputeNodeRef.values()) {
<<<<<<< HEAD
            if (computeNode.getHost().equals(host) && computeNode.getHeartbeatPort() == heartPort) {
=======
            if (NetUtils.isSameIP(computeNode.getHost(), host) && computeNode.getHeartbeatPort() == heartPort) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return computeNode;
            }
        }
        return null;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        for (Backend backend : idToBackendRef.values()) {
<<<<<<< HEAD
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
=======
            if (NetUtils.isSameIP(backend.getHost(), host) && backend.getHeartbeatPort() == heartPort) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return backend;
            }
        }
        return null;
    }

    public long getBackendIdWithStarletPort(String host, int starletPort) {
        for (Backend backend : idToBackendRef.values()) {
<<<<<<< HEAD
            if (backend.getHost().equals(host) && backend.getStarletPort() == starletPort) {
=======
            if (NetUtils.isSameIP(backend.getHost(), host) && backend.getStarletPort() == starletPort) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return backend.getId();
            }
        }
        return -1L;
    }

    public long getComputeNodeIdWithStarletPort(String host, int starletPort) {
        for (ComputeNode cn : idToComputeNodeRef.values()) {
<<<<<<< HEAD
            if (cn.getHost().equals(host) && cn.getStarletPort() == starletPort) {
=======
            if (NetUtils.isSameIP(cn.getHost(), host) && cn.getStarletPort() == starletPort) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                return cn.getId();
            }
        }
        return -1L;
    }

    public static TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
<<<<<<< HEAD
        ComputeNode computeNode = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (computeNode == null) {
            computeNode =
                    GlobalStateMgr.getCurrentSystemInfo().getComputeNodeWithBePort(host.getHostname(), host.getPort());
            if (computeNode == null) {
                throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR);
=======
        ComputeNode computeNode = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (computeNode == null) {
            computeNode =
                    GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                            .getComputeNodeWithBePort(host.getHostname(), host.getPort());
            if (computeNode == null) {
                throw new StarRocksException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
        if (computeNode.getBrpcPort() < 0) {
            return null;
        }
        return new TNetworkAddress(computeNode.getHost(), computeNode.getBrpcPort());
    }

<<<<<<< HEAD
    public static TNetworkAddress toBrpcIp(TNetworkAddress host) throws Exception {
        ComputeNode computeNode = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (computeNode == null) {
            computeNode =
                    GlobalStateMgr.getCurrentSystemInfo().getComputeNodeWithBePort(host.getHostname(), host.getPort());
            if (computeNode == null) {
                throw new UserException(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR);
            }
        }
        if (computeNode.getBrpcPort() < 0) {
            return null;
        }
        return computeNode.getBrpcIpAddress();
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
=======
    public int getRetainedBackendNumber() {
        return getRetainedBackends().size();
    }

    public int getSystemTableExpectedReplicationNum() {
        if (RunMode.isSharedDataMode()) {
            return 1;
        }
        return Integer.max(1, Integer.min(Config.default_replication_num, getRetainedBackendNumber()));
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public int getTotalBackendNumber() {
        return idToBackendRef.size();
    }

    public int getTotalComputeNodeNumber() {
        return idToComputeNodeRef.size();
    }

<<<<<<< HEAD


=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
            if (targetPair.first.equals(curPair.first)) {
=======
            if (NetUtils.isSameIP(targetPair.first, curPair.first)) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        List<Long> computeNodeIds = Lists.newArrayList(idToComputeNodeRef.keySet());
        if (!needAlive) {
            return computeNodeIds;
        } else {
            Iterator<Long> iter = computeNodeIds.iterator();
            while (iter.hasNext()) {
                ComputeNode computeNode = this.getComputeNode(iter.next());
                if (computeNode == null || !computeNode.isAlive()) {
                    iter.remove();
                }
            }
            return computeNodeIds;
=======
        if (needAlive) {
            return idToComputeNodeRef.entrySet().stream()
                    .filter(entry -> entry.getValue().isAlive())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        } else {
            return Lists.newArrayList(idToComputeNodeRef.keySet());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    public List<Long> getBackendIds(boolean needAlive) {
<<<<<<< HEAD
        List<Long> backendIds = Lists.newArrayList(idToBackendRef.keySet());
        if (!needAlive) {
            return backendIds;
        } else {
            Iterator<Long> iter = backendIds.iterator();
            while (iter.hasNext()) {
                Backend backend = this.getBackend(iter.next());
                if (backend == null || !backend.isAlive()) {
                    iter.remove();
                }
            }
            return backendIds;
=======
        if (needAlive) {
            return idToBackendRef.entrySet().stream()
                    .filter(entry -> entry.getValue().isAlive())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        } else {
            return Lists.newArrayList(idToBackendRef.keySet());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    public List<Long> getDecommissionedBackendIds() {
<<<<<<< HEAD
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
=======
        return idToBackendRef.entrySet().stream()
                .filter(entry -> entry.getValue().isDecommissioned())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    public List<Long> getAvailableBackendIds() {
        return idToBackendRef.entrySet().stream().filter(entry -> entry.getValue().isAvailable()).map(
                Map.Entry::getKey).collect(Collectors.toList());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
=======
    /**
     * Available: not decommissioned and alive
     */
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public List<Backend> getAvailableBackends() {
        return getBackends().stream()
                .filter(ComputeNode::isAvailable)
                .collect(Collectors.toList());
    }

<<<<<<< HEAD
=======
    /**
     * Retained: not decommissioned, whatever alive or not
     */
    public List<Backend> getRetainedBackends() {
        return getBackends().stream().filter(x -> !x.isDecommissioned()).collect(Collectors.toList());
    }

    public List<ComputeNode> getComputeNodes() {
        return Lists.newArrayList(idToComputeNodeRef.values());
    }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public List<ComputeNode> getAvailableComputeNodes() {
        return getComputeNodes().stream()
                .filter(ComputeNode::isAvailable)
                .collect(Collectors.toList());
    }

<<<<<<< HEAD
    public List<ComputeNode> getComputeNodes() {
        return Lists.newArrayList(idToComputeNodeRef.values());
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public Stream<ComputeNode> backendAndComputeNodeStream() {
        return Stream.concat(idToBackendRef.values().stream(), idToComputeNodeRef.values().stream());
    }

<<<<<<< HEAD
    public List<Long> seqChooseBackendIdsByStorageMedium(int backendNum, boolean needAvailable, boolean isCreate,
                                                         TStorageMedium storageMedium) {

        return seqChooseBackendIds(backendNum, needAvailable, isCreate,
                v -> !v.checkDiskExceedLimitForCreate(storageMedium));
    }

    public Long seqChooseBackendOrComputeId() throws UserException {
        List<Long> backendIds = seqChooseBackendIds(1, true, false);
        if (CollectionUtils.isNotEmpty(backendIds)) {
            return backendIds.get(0);
        }
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_NOTHING) {
            throw new UserException("No backend alive.");
        }
        List<Long> computeNodes = seqChooseComputeNodes(1, true, false);
        if (CollectionUtils.isNotEmpty(computeNodes)) {
            return computeNodes.get(0);
        }
        throw new UserException("No backend or compute node alive.");
    }

    public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate) {

        if (isCreate) {
            return seqChooseBackendIds(backendNum, needAvailable, true, v -> !v.checkDiskExceedLimitForCreate());
        } else {
            return seqChooseBackendIds(backendNum, needAvailable, false, v -> !v.checkDiskExceedLimit());
        }
    }

    public List<Long> seqChooseComputeNodes(int computeNodeNum, boolean needAvailable, boolean isCreate) {

        final List<ComputeNode> candidateComputeNodes = needAvailable ? getAvailableComputeNodes() : getComputeNodes();
        if (CollectionUtils.isEmpty(candidateComputeNodes)) {
            LOG.warn("failed to find any compute nodes, needAvailable={}", needAvailable);
            return Collections.emptyList();
        }

        return seqChooseNodeIds(computeNodeNum, isCreate, candidateComputeNodes);
    }

    private List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate,
                                           Predicate<? super Backend> predicate) {
        final List<Backend> candidateBackends = needAvailable ? getAvailableBackends() : getBackends();
        if (CollectionUtils.isEmpty(candidateBackends)) {
            LOG.warn("failed to find any backend, needAvailable={}", needAvailable);
            return Collections.emptyList();
        }

        final List<ComputeNode> filteredBackends = candidateBackends.stream()
                .filter(predicate)
                .collect(Collectors.toList());

        if (CollectionUtils.isEmpty(filteredBackends)) {
            String backendInfo = candidateBackends.stream()
                    .map(backend -> "[host=" + backend.getHost() + ", bePort=" + backend.getBePort() + "]")
                    .collect(Collectors.joining("; "));

            LOG.warn(
                    "failed to find any backend with qualified disk usage from {} candidate backends, needAvailable={}, [{}]",
                    candidateBackends.size(), needAvailable, backendInfo);
            return Collections.emptyList();
        }
        return seqChooseNodeIds(backendNum, isCreate, filteredBackends);
    }

    /**
     * choose nodes by round-robin
     *
     * @param nodeNum  number of node wanted
     * @param isCreate    last node id for creation
     * @param srcNodes list of the candidate nodes
     * @return empty list if not enough node, otherwise return a list of node's id
     */
    private synchronized List<Long> seqChooseNodeIds(int nodeNum, boolean isCreate, final List<ComputeNode> srcNodes) {
        long lastNodeId;

        if (isCreate) {
            lastNodeId = lastNodeIdForCreation;
        } else {
            lastNodeId = lastNodeIdForOther;
        }

        // host -> BE list
        Map<String, List<ComputeNode>> nodeMaps = Maps.newHashMap();
        for (ComputeNode node : srcNodes) {
            String host = node.getHost();

            if (!nodeMaps.containsKey(host)) {
                nodeMaps.put(host, Lists.newArrayList());
            }

            nodeMaps.get(host).add(node);
        }

        // if more than one backend exists in same host, select a backend at random
        List<ComputeNode> nodes = Lists.newArrayList();
        for (List<ComputeNode> list : nodeMaps.values()) {
            Collections.shuffle(list);
            nodes.add(list.get(0));
        }

        List<Long> nodeIds = Lists.newArrayList();
        // get last node index
        int lastNodeIndex = -1;
        int index = -1;
        for (ComputeNode node : nodes) {
            index++;
            if (node.getId() == lastNodeId) {
                lastNodeIndex = index;
                break;
            }
        }
        Iterator<ComputeNode> iterator = Iterators.cycle(nodes);
        index = -1;
        boolean failed = false;
        // 2 cycle at most
        int maxIndex = 2 * nodes.size();
        while (iterator.hasNext() && nodeIds.size() < nodeNum) {
            ComputeNode node = iterator.next();
            index++;
            if (index <= lastNodeIndex) {
                continue;
            }

            if (index > maxIndex) {
                failed = true;
                break;
            }

            long nodeId = node.getId();
            if (!nodeIds.contains(nodeId)) {
                nodeIds.add(nodeId);
                lastNodeId = nodeId;
            } else {
                failed = true;
                break;
            }
        }

        if (nodeIds.size() != nodeNum) {
            failed = true;
        }

        if (failed) {
            // debug: print backend info when the selection failed
            for (ComputeNode node : nodes) {
                LOG.debug("random select: {}", node);
            }
            return Collections.emptyList();
        }

        if (isCreate) {
            lastNodeIdForCreation = lastNodeId;
        } else {
            lastNodeIdForOther = lastNodeId;
        }
        return nodeIds;
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public ImmutableMap<Long, Backend> getIdToBackend() {
        return ImmutableMap.copyOf(idToBackendRef);
    }

    public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
        return ImmutableMap.copyOf(idToComputeNodeRef);
    }

<<<<<<< HEAD
    public ImmutableCollection<ComputeNode> getComputeNodes(boolean needAlive) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = ImmutableMap.copyOf(idToComputeNodeRef);
        List<Long> computeNodeIds = getComputeNodeIds(needAlive);
        List<ComputeNode> computeNodes = new ArrayList<>();
        for (Long computeNodeId : computeNodeIds) {
            computeNodes.add(idToComputeNode.get(computeNodeId));
        }
        return ImmutableList.copyOf(computeNodes);
    }

    public ImmutableCollection<ComputeNode> getBackends(boolean needAlive) {
        ImmutableMap<Long, Backend> idToComputeNode = ImmutableMap.copyOf(idToBackendRef);
        List<Long> backendIds = getBackendIds(needAlive);
        List<ComputeNode> backends = new ArrayList<>();
        for (Long backendId : backendIds) {
            backends.add(idToComputeNode.get(backendId));
        }
        return ImmutableList.copyOf(backends);
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        if (node != null && (node instanceof Backend)) {
            AtomicLong atomicLong = null;
            if ((atomicLong = idToReportVersionRef.get(backendId)) != null) {
                Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
=======
        if (node instanceof Backend) {
            AtomicLong atomicLong;
            if ((atomicLong = idToReportVersionRef.get(backendId)) != null) {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    public long saveBackends(DataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = ImmutableMap.copyOf(idToBackendRef);
        int backendCount = idToBackend.size();
        checksum ^= backendCount;
        dos.writeInt(backendCount);
        for (Map.Entry<Long, Backend> entry : idToBackend.entrySet()) {
            long key = entry.getKey();
            checksum ^= key;
            dos.writeLong(key);
            entry.getValue().write(dos);
        }
        return checksum;
    }

    public long saveComputeNodes(DataOutputStream dos, long checksum) throws IOException {
        SystemInfoService.SerializeData data = new SystemInfoService.SerializeData();
        data.computeNodes = Lists.newArrayList(idToComputeNodeRef.values());
        checksum ^= data.computeNodes.size();
        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(dos, s);
        return checksum;
    }

    private static class SerializeData {
        @SerializedName("computeNodes")
        public List<ComputeNode> computeNodes;

    }

    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (int i = 0; i < count; i++) {
            long key = dis.readLong();
            checksum ^= key;
            Backend backend = Backend.read(dis);
            replayAddBackend(backend);
        }
        return checksum;
    }

    public long loadComputeNodes(DataInputStream dis, long checksum) throws IOException {
        int computeNodeSize = 0;
        try {
            String s = Text.readString(dis);
            SystemInfoService.SerializeData data = GsonUtils.GSON.fromJson(s, SystemInfoService.SerializeData.class);
            if (data != null && data.computeNodes != null) {
                for (ComputeNode computeNode : data.computeNodes) {
                    replayAddComputeNode(computeNode);
                }
                computeNodeSize = data.computeNodes.size();
            }
            checksum ^= computeNodeSize;
            LOG.info("finished replaying compute node from image");
        } catch (EOFException e) {
            LOG.info("no compute node to replay.");
        }
        return checksum;
    }

=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    public void clear() {
        this.idToBackendRef = new ConcurrentHashMap<>();
        this.idToReportVersionRef = ImmutableMap.of();
    }

<<<<<<< HEAD
    public static Pair<String, Integer> validateHostAndPort(String hostPort, boolean resolveHost) throws AnalysisException {
        hostPort = hostPort.replaceAll("\\s+", "");
        if (hostPort.isEmpty()) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String[] pair = hostPort.split(":");
        if (pair.length != 2) {
            throw new AnalysisException("Invalid host port: " + hostPort);
        }

        String host = pair[0];
        if (Strings.isNullOrEmpty(host)) {
            throw new AnalysisException("Host is null");
        }

        int heartbeatPort = -1;
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
            heartbeatPort = Integer.parseInt(pair[1]);

            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new Pair<String, Integer>(host, heartbeatPort);
        } catch (UnknownHostException e) {
            throw new AnalysisException("Unknown host: " + e.getMessage());
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
=======
            heartbeatPort = Integer.parseInt(hostInfo[1]);

            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new SemanticException("Port is out of range: " + heartbeatPort);
            }

            return new Pair<>(host, heartbeatPort);
        } catch (UnknownHostException e) {
            throw new SemanticException("Unknown host: " + e.getMessage());
        } catch (Exception e) {
            throw new SemanticException("Encounter unknown exception: " + e.getMessage());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    public void replayAddComputeNode(ComputeNode newComputeNode) {
        // update idToComputeNode
        newComputeNode.setBackendState(BackendState.using);
        idToComputeNodeRef.put(newComputeNode.getId(), newComputeNode);
<<<<<<< HEAD

        // to add compute to DEFAULT_CLUSTER
        if (newComputeNode.getBackendState() == BackendState.using) {
            final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
            if (null != cluster) {
                // replay log
                cluster.addComputeNode(newComputeNode.getId());
            } else {
                // This happens in loading image when fe is restarted, because loadCluster is after loadComputeNode,
                // cluster is not created. CN in cluster will be updated in loadCluster.
            }
        }
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        idToBackendRef.put(newBackend.getId(), newBackend);

        // set new backend's report version as 0L
<<<<<<< HEAD
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // to add be to DEFAULT_CLUSTER
        if (newBackend.getBackendState() == BackendState.using) {
            final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
            if (null != cluster) {
                // replay log
                cluster.addBackend(newBackend.getId());
            } else {
                // This happens in loading image when fe is restarted, because loadCluster is after loadBackend,
                // cluster is not created. Be in cluster will be updated in loadCluster.
            }
        }
=======
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }

    public void replayDropComputeNode(long computeNodeId) {
        LOG.debug("replayDropComputeNode: {}", computeNodeId);
        // update idToComputeNode
        ComputeNode cn = idToComputeNodeRef.remove(computeNodeId);

        // BackendCoreStat is a global state, checkpoint should not modify it.
        if (!GlobalStateMgr.isCheckpointThread()) {
            // remove from BackendCoreStat
<<<<<<< HEAD
            BackendCoreStat.removeNumOfHardwareCoresOfBe(computeNodeId);
        }

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            cluster.removeComputeNode(computeNodeId);
            // clear map in starosAgent
            if (RunMode.isSharedDataMode()) {
                long starletPort = cn.getStarletPort();
                if (starletPort == 0) {
                    return;
                }
                String workerAddr = cn.getHost() + ":" + starletPort;
                GlobalStateMgr.getCurrentStarOSAgent().removeWorkerFromMap(workerAddr);
            }
        } else {
            LOG.error("Cluster DEFAULT_CLUSTER " + DEFAULT_CLUSTER + " no exist.");
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
    }

    public void replayDropBackend(Backend backend) {
        LOG.debug("replayDropBackend: {}", backend);
        // update idToBackend
        idToBackendRef.remove(backend.getId());

        // update idToReportVersion
<<<<<<< HEAD
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);
=======
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

        // BackendCoreStat is a global state, checkpoint should not modify it.
        if (!GlobalStateMgr.isCheckpointThread()) {
            // remove from BackendCoreStat
<<<<<<< HEAD
            BackendCoreStat.removeNumOfHardwareCoresOfBe(backend.getId());
        }

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            cluster.removeBackend(backend.getId());

            // clear map in starosAgent
            if (RunMode.isSharedDataMode()) {
                long starletPort = backend.getStarletPort();
                if (starletPort == 0) {
                    return;
                }
                String workerAddr = backend.getHost() + ":" + starletPort;
                GlobalStateMgr.getCurrentStarOSAgent().removeWorkerFromMap(workerAddr);
            }
        } else {
            LOG.error("Cluster {} no exist.", SystemInfoService.DEFAULT_CLUSTER);
        }
    }

    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        if (memoryBe == null) {
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            // backend may already be dropped. this may happen when
            // 1. SystemHandler drop the decommission backend
            // 2. at same time, user try to cancel the decommission of that backend.
            // These two operations do not guarantee the order.
            return;
        }
<<<<<<< HEAD
        memoryBe.setBePort(be.getBePort());
        memoryBe.setHost(be.getHost());
        memoryBe.setAlive(be.isAlive());
        memoryBe.setDecommissioned(be.isDecommissioned());
        memoryBe.setHttpPort(be.getHttpPort());
        memoryBe.setBeRpcPort(be.getBeRpcPort());
        memoryBe.setBrpcPort(be.getBrpcPort());
        memoryBe.setLastUpdateMs(be.getLastUpdateMs());
        memoryBe.setLastStartTime(be.getLastStartTime());
        memoryBe.setDisks(be.getDisks());
        memoryBe.setBackendState(be.getBackendState());
        memoryBe.setDecommissionType(be.getDecommissionType());
=======
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
=======
        if (RunMode.isSharedDataMode()) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return;
        }

        if (getClusterAvailableCapacityB() <= 0L) {
<<<<<<< HEAD
            throw new DdlException("Cluster has no available capacity");
=======
            throw new SemanticException("Cluster has no available capacity");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                BackendCoreStat.setNumOfHardwareCoresOfBe(node.getId(), node.getCpuCores());
            }
            for (ComputeNode node : idToComputeNodeRef.values()) {
                BackendCoreStat.setNumOfHardwareCoresOfBe(node.getId(), node.getCpuCores());
=======
                BackendResourceStat.getInstance().setNumHardwareCoresOfBe(node.getId(), node.getCpuCores());
                BackendResourceStat.getInstance().setMemLimitBytesOfBe(node.getId(), node.getMemLimitBytes());
            }
            for (ComputeNode node : idToComputeNodeRef.values()) {
                BackendResourceStat.getInstance().setNumHardwareCoresOfBe(node.getId(), node.getCpuCores());
                BackendResourceStat.getInstance().setMemLimitBytesOfBe(node.getId(), node.getMemLimitBytes());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
    }
}

