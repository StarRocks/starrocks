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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Tablet;
import com.starrocks.cluster.Cluster;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.NetUtils;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.DropComputeNodeLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * SystemInfoService for local olap table.
 */
public class LocalSystemInfoService extends SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);

    private volatile ImmutableMap<Long, Backend> idToBackendRef;
    private volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef;

    private volatile ImmutableMap<Long, ComputeNode> idToComputeNodeRef;

    private long lastBackendIdForCreation = -1;
    private long lastBackendIdForOther = -1;

    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef;

    public LocalSystemInfoService() {
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();

        idToComputeNodeRef = ImmutableMap.<Long, ComputeNode>of();

        pathHashToDishInfoRef = ImmutableMap.<Long, DiskInfo>of();
    }

    @Override
    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs)
            throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
            if (getComputeNodeWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same compute node already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addComputeNode(pair.first, pair.second);
        }
    }

    private ComputeNode getComputeNodeWithHeartbeatPort(String host, Integer heartPort) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        for (ComputeNode computeNode : idToComputeNode.values()) {
            if (computeNode.getHost().equals(host) && computeNode.getHeartbeatPort() == heartPort) {
                return computeNode;
            }
        }
        return null;
    }

    // Final entry of adding compute node
    private void addComputeNode(String host, int heartbeatPort) throws DdlException {
        ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // update idToComputor
        Map<Long, ComputeNode> copiedComputeNodes = Maps.newHashMap(idToComputeNodeRef);
        copiedComputeNodes.put(newComputeNode.getId(), newComputeNode);
        idToComputeNodeRef = ImmutableMap.copyOf(copiedComputeNodes);

        setComputeNodeOwner(newComputeNode);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddComputeNode(newComputeNode);
        LOG.info("finished to add {} ", newComputeNode);
    }

    private void setComputeNodeOwner(ComputeNode computeNode) {
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        Preconditions.checkState(cluster != null);
        cluster.addComputeNode(computeNode.getId());
        computeNode.setBackendState(BackendState.using);
    }

    @Override
    public boolean isSingleBackendAndComputeNode(long clusterId) {
        return idToBackendRef.size() + idToComputeNodeRef.size() == 1;
    }

    /**
     * @param hostPortPairs : backend's host and port
     * @throws DdlException
     */
    @Override
    public void addBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addBackend(pair.first, pair.second);
        }
    }

    // for test
    @Override
    public void dropBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(backend.getId());
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);
    }

    // for test
    @Override
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(backend.getId(), backend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);
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
        Backend newBackend = new Backend(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // set new backend's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(newBackend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        // add backend to DEFAULT_CLUSTER
        setBackendOwner(newBackend);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddBackend(newBackend);
        LOG.info("finished to add {} ", newBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    @Override
    public ShowResultSet modifyBackendHost(ModifyBackendAddressClause modifyBackendAddressClause) throws DdlException {
        String willBeModifiedHost = modifyBackendAddressClause.getSrcHost();
        String fqdn = modifyBackendAddressClause.getDestHost();
        List<Backend> candidateBackends = getBackendOnlyWithHost(willBeModifiedHost);
        if (null == candidateBackends || candidateBackends.size() == 0) {
            throw new DdlException(String.format("backend [%s] not found", willBeModifiedHost));
        }

        // update idToBackend
        Backend preUpdateBackend = candidateBackends.get(0);
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        Backend updateBackend = copiedBackends.get(preUpdateBackend.getId());
        updateBackend.setHost(fqdn);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logBackendStateChange(updateBackend);

        // Message
        StringBuilder formatSb = new StringBuilder();
        String opMessage;
        formatSb.append("%s:%d's host has been modified to %s");
        if (candidateBackends.size() >= 2) {
            formatSb.append("\nplease exectue %d times, to modify the remaining backends\n");
            for (int i = 1; i < candidateBackends.size(); i++) {
                Backend be = candidateBackends.get(i);
                formatSb.append(be.getHost() + ":" + be.getHeartbeatPort() + "\n");
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
        messageResult.add(Arrays.asList(opMessage));
        return new ShowResultSet(builder.build(), messageResult);
    }

    @Override
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

    @Override
    public void dropComputeNode(String host, int heartbeatPort)
            throws DdlException {
        ComputeNode dropComputeNode = getComputeNodeWithHeartbeatPort(host, heartbeatPort);
        if (dropComputeNode == null) {
            throw new DdlException("compute node does not exists[" + host + ":" + heartbeatPort + "]");
        }

        // update idToComputeNode
        Map<Long, ComputeNode> copiedComputeNodes = Maps.newHashMap(idToComputeNodeRef);
        copiedComputeNodes.remove(dropComputeNode.getId());
        idToComputeNodeRef = ImmutableMap.copyOf(copiedComputeNodes);

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            cluster.removeComputeNode(dropComputeNode.getId());
        } else {
            LOG.error("Cluster {} no exist.", SystemInfoService.DEFAULT_CLUSTER);
        }
        // log
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDropComputeNode(new DropComputeNodeLog(dropComputeNode.getId()));
        LOG.info("finished to drop {}", dropComputeNode);
    }

    @Override
    public void dropBackends(DropBackendClause dropBackendClause) throws DdlException {
        List<Pair<String, Integer>> hostPortPairs = dropBackendClause.getHostPortPairs();
        boolean needCheckUnforce = !dropBackendClause.isForce();

        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getBackendWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("backend does not exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropBackend(pair.first, pair.second, needCheckUnforce);
        }
    }

    // for decommission
    @Override
    public void dropBackend(long backendId) throws DdlException {
        Backend backend = getBackend(backendId);
        if (backend == null) {
            throw new DdlException("Backend[" + backendId + "] does not exist");
        }

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
                        .filter(table -> table.isLocalTable())
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
            }
        });
    }

    // final entry of dropping backend
    @Override
    public void dropBackend(String host, int heartbeatPort, boolean needCheckUnforce) throws DdlException {
        if (getBackendWithHeartbeatPort(host, heartbeatPort) == null) {
            throw new DdlException("backend does not exists[" + host + ":" + heartbeatPort + "]");
        }

        Backend droppedBackend = getBackendWithHeartbeatPort(host, heartbeatPort);
        if (needCheckUnforce) {
            try {
                checkUnforce(droppedBackend);
            } catch (RuntimeException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(droppedBackend.getId());
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(droppedBackend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            cluster.removeBackend(droppedBackend.getId());
        } else {
            LOG.error("Cluster {} no exist.", SystemInfoService.DEFAULT_CLUSTER);
        }
        // log
        GlobalStateMgr.getCurrentState().getEditLog().logDropBackend(droppedBackend);
        LOG.info("finished to drop {}", droppedBackend);

        // backends is changed, regenerated tablet number metrics
        MetricRepo.generateBackendsTabletMetrics();
    }

    // only for test
    @Override
    public void dropAllBackend() {
        // update idToBackend
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        // update idToReportVersion
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();
    }

    @Override
    public Boolean checkWorkerHealthy(long workerId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public Backend getBackend(long backendId) {
        return idToBackendRef.get(backendId);
    }

    @Override
    public ComputeNode getComputeNode(long computeNodeId) {
        return idToComputeNodeRef.get(computeNodeId);
    }

    @Override
    public boolean checkBackendAvailable(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        return backend != null && backend.isAvailable();
    }

    @Override
    public boolean checkBackendAlive(long backendId) {
        Backend backend = idToBackendRef.get(backendId);
        return backend != null && backend.isAlive();
    }

    @Override
    public ComputeNode getComputeNodeWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        for (ComputeNode computeNode : idToComputeNode.values()) {
            if (computeNode.getHost().equals(host) && computeNode.getHeartbeatPort() == heartPort) {
                return computeNode;
            }
        }
        return null;
    }

    @Override
    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    @Override
    public long getBackendIdWithStarletPort(String host, int starletPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getStarletPort() == starletPort) {
                return backend.getId();
            }
        }
        return -1L;
    }

    @Override
    public TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
        ComputeNode computeNode = getBackendWithBePort(host.getHostname(), host.getPort());
        if (computeNode == null) {
            computeNode = getComputeNodeWithBePort(host.getHostname(), host.getPort());
            if (computeNode == null) {
                throw new UserException("Backend not found. Check if any backend is down or not");
            }
        }
        if (computeNode.getBrpcPort() < 0) {
            return null;
        }
        return new TNetworkAddress(computeNode.getHost(), computeNode.getBrpcPort());
    }

    @Override
    public Backend getBackendWithBePort(String host, int bePort) {

        Pair<String, String> targetPair;
        try {
            targetPair = NetUtils.getIpAndFqdnByHost(host);
        } catch (UnknownHostException e) {
            LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
            return null;
        }

        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            Pair<String, String> curPair;
            try {
                curPair = NetUtils.getIpAndFqdnByHost(backend.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("failed to get right ip by fqdn {}", e.getMessage());
                continue;
            }
            boolean hostMatch = false;
            // target, cur has same ip
            if (targetPair.first.equals(curPair.first)) {
                hostMatch = true;
            }
            // target, cur has same fqdn and both of them are not equal ""
            if (!hostMatch && targetPair.second.equals(curPair.second) && !curPair.second.equals("")) {
                hostMatch = true;
            }
            if (hostMatch && (backend.getBePort() == bePort)) {
                return backend;
            }
        }
        return null;
    }

    @Override
    public List<Backend> getBackendOnlyWithHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Backend> resultBackends = new ArrayList<>();
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host)) {
                resultBackends.add(backend);
            }
        }
        return resultBackends;
    }

    @Override
    public int getTotalBackendNumber() {
        return idToBackendRef.size();
    }

    @Override
    public int getTotalBackendNumber(long clusterId) {
        return getTotalBackendNumber();
    }

    @Override
    public ComputeNode getComputeNodeWithBePort(String host, int bePort) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        for (ComputeNode computeNode : idToComputeNode.values()) {
            if (computeNode.getHost().equals(host) && computeNode.getBePort() == bePort) {
                return computeNode;
            }
        }
        return null;
    }

    @Override
    public List<Long> getComputeNodeIds(boolean needAlive) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        List<Long> computeNodeIds = Lists.newArrayList(idToComputeNode.keySet());
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
        }
    }

    @Override
    public List<Long> getBackendIds(boolean needAlive) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());
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
        }
    }

    @Override
    public List<Long> getBackendIds(boolean needAlive, long clusterId) {
        return getBackendIds(needAlive);
    }

    @Override
    public List<Long> getDecommissionedBackendIds() {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isDecommissioned()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    @Override
    public List<Long> getAvailableBackendIds() {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        List<Long> backendIds = Lists.newArrayList(idToBackend.keySet());

        Iterator<Long> iter = backendIds.iterator();
        while (iter.hasNext()) {
            Backend backend = this.getBackend(iter.next());
            if (backend == null || !backend.isAvailable()) {
                iter.remove();
            }
        }
        return backendIds;
    }

    @Override
    public List<Backend> getBackends() {
        return idToBackendRef.values().asList();
    }

    @Override
    public List<Backend> getBackends(long clusterId) {
        return getBackends();
    }

    @Override
    public List<Long> seqChooseBackendIdsByStorageMedium(int backendNum, boolean needAvailable, boolean isCreate,
                                                         TStorageMedium storageMedium) {
        final List<Backend> backends =
                getBackends().stream().filter(v -> !v.diskExceedLimitByStorageMedium(storageMedium))
                        .collect(Collectors.toList());
        return seqChooseBackendIds(backendNum, needAvailable, isCreate, backends);
    }

    @Override
    public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate) {
        final List<Backend> backends =
                getBackends().stream().filter(v -> !v.diskExceedLimit()).collect(Collectors.toList());
        return seqChooseBackendIds(backendNum, needAvailable, isCreate, backends);
    }

    @Override
    public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate, long clusterId) {
        return seqChooseBackendIds(backendNum, needAvailable, isCreate);
    }

    // choose backends by round-robin
    // return null if not enough backend
    // use synchronized to run serially
    private synchronized List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate,
                                                        final List<Backend> srcBackends) {
        long lastBackendId;

        if (isCreate) {
            lastBackendId = lastBackendIdForCreation;
        } else {
            lastBackendId = lastBackendIdForOther;
        }

        // host -> BE list
        Map<String, List<Backend>> backendMaps = Maps.newHashMap();
        for (Backend backend : srcBackends) {
            // If needAvailable is true, unavailable backend won't go into the pick list
            if (needAvailable && !backend.isAvailable()) {
                continue;
            }

            if (backendMaps.containsKey(backend.getHost())) {
                backendMaps.get(backend.getHost()).add(backend);
            } else {
                List<Backend> list = Lists.newArrayList();
                list.add(backend);
                backendMaps.put(backend.getHost(), list);
            }
        }

        // if more than one backend exists in same host, select a backend at random
        List<Backend> backends = Lists.newArrayList();
        for (List<Backend> list : backendMaps.values()) {
            Collections.shuffle(list);
            backends.add(list.get(0));
        }

        List<Long> backendIds = Lists.newArrayList();
        // get last backend index
        int lastBackendIndex = -1;
        int index = -1;
        for (Backend backend : backends) {
            index++;
            if (backend.getId() == lastBackendId) {
                lastBackendIndex = index;
                break;
            }
        }
        Iterator<Backend> iterator = Iterators.cycle(backends);
        index = -1;
        boolean failed = false;
        // 2 cycle at most
        int maxIndex = 2 * backends.size();
        while (iterator.hasNext() && backendIds.size() < backendNum) {
            Backend backend = iterator.next();
            index++;
            if (index <= lastBackendIndex) {
                continue;
            }

            if (index > maxIndex) {
                failed = true;
                break;
            }

            long backendId = backend.getId();
            if (!backendIds.contains(backendId)) {
                backendIds.add(backendId);
                lastBackendId = backendId;
            } else {
                failed = true;
                break;
            }
        }

        if (backendIds.size() != backendNum) {
            failed = true;
        }

        if (!failed) {
            if (isCreate) {
                lastBackendIdForCreation = lastBackendId;
            } else {
                lastBackendIdForOther = lastBackendId;
            }
            return backendIds;
        }

        // debug
        for (Backend backend : backends) {
            LOG.debug("random select: {}", backend);
        }

        return Collections.emptyList();
    }

    @Override
    public ImmutableMap<Long, Backend> getIdToBackend() {
        return idToBackendRef;
    }

    @Override
    public ImmutableMap<Long, Backend> getIdToBackend(long clusterId) {
        return getIdToBackend();
    }

    @Override
    public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
        return idToComputeNodeRef;
    }

    @Override
    public ImmutableCollection<ComputeNode> getComputeNodes() {
        List<Long> computeNodeIds = getComputeNodeIds(true);
        if (computeNodeIds != null && computeNodeIds.size() > 0) {
            return getComputeNodes(true);
        } else {
            return getBackends(true);
        }
    }

    @Override
    public ImmutableCollection<ComputeNode> getComputeNodes(boolean needAlive) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        List<Long> computeNodeIds = getComputeNodeIds(needAlive);
        List<ComputeNode> computeNodes = new ArrayList<>();
        for (Long computeNodeId : computeNodeIds) {
            computeNodes.add(idToComputeNode.get(computeNodeId));
        }
        return ImmutableList.copyOf(computeNodes);
    }

    private ImmutableCollection<ComputeNode> getBackends(boolean needAlive) {
        ImmutableMap<Long, Backend> idToComputeNode = idToBackendRef;
        List<Long> backendIds = getBackendIds(needAlive);
        List<ComputeNode> backends = new ArrayList<>();
        for (Long backendId : backendIds) {
            backends.add(idToComputeNode.get(backendId));
        }
        return ImmutableList.copyOf(backends);
    }

    @Override
    public long getBackendReportVersion(long backendId) {
        AtomicLong atomicLong;
        if ((atomicLong = idToReportVersionRef.get(backendId)) == null) {
            return -1L;
        } else {
            return atomicLong.get();
        }
    }

    @Override
    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId) {
        AtomicLong atomicLong = null;
        if ((atomicLong = idToReportVersionRef.get(backendId)) != null) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            if (db != null) {
                db.readLock();
                try {
                    atomicLong.set(newReportVersion);
                    LOG.debug("update backend {} report version: {}, db: {}", backendId, newReportVersion, dbId);
                } finally {
                    db.readUnlock();
                }
            } else {
                LOG.warn("failed to update backend report version, db {} does not exist", dbId);
            }
        } else {
            LOG.warn("failed to update backend report version, backend {} does not exist", backendId);
        }
    }

    @Override
    public long saveBackends(DataOutputStream dos, long checksum) throws IOException {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
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

    @Override
    public long saveComputeNodes(DataOutputStream dos, long checksum) throws IOException {
        SerializeData data = new SerializeData();
        data.computeNodes = idToComputeNodeRef.values().asList();
        checksum ^= data.computeNodes.size();
        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(dos, s);
        return checksum;
    }

    private static class SerializeData {
        @SerializedName("computeNodes")
        public List<ComputeNode> computeNodes;

    }

    @Override
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

    @Override
    public long loadComputeNodes(DataInputStream dis, long checksum) throws IOException {
        int computeNodeSize = 0;
        try {
            String s = Text.readString(dis);
            SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);
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

    @Override
    public void clear() {
        this.idToBackendRef = null;
        this.idToReportVersionRef = null;
    }

    @Override
    public void replayAddComputeNode(ComputeNode newComputeNode) {
        // update idToComputeNode
        newComputeNode.setBackendState(BackendState.using);
        Map<Long, ComputeNode> copiedComputeNodes = Maps.newHashMap(idToComputeNodeRef);
        copiedComputeNodes.put(newComputeNode.getId(), newComputeNode);
        idToComputeNodeRef = ImmutableMap.copyOf(copiedComputeNodes);

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
    }

    @Override
    public void replayAddBackend(Backend newBackend) {
        // update idToBackend
        if (GlobalStateMgr.getCurrentStateJournalVersion() < FeMetaVersion.VERSION_30) {
            newBackend.setBackendState(BackendState.using);
        }
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(newBackend.getId(), newBackend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // set new backend's report version as 0L
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
    }

    @Override
    public void replayDropComputeNode(long computeNodeId) {
        LOG.debug("replayDropComputeNode: {}", computeNodeId);
        // update idToComputeNode
        Map<Long, ComputeNode> copiedComputeNodes = Maps.newHashMap(idToComputeNodeRef);
        copiedComputeNodes.remove(computeNodeId);
        idToComputeNodeRef = ImmutableMap.copyOf(copiedComputeNodes);

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            cluster.removeComputeNode(computeNodeId);
        } else {
            LOG.error("Cluster DEFAULT_CLUSTER " + DEFAULT_CLUSTER + " no exist.");
        }
    }

    @Override
    public void replayDropBackend(Backend backend) {
        LOG.debug("replayDropBackend: {}", backend);
        // update idToBackend
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(backend.getId());
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // update cluster
        final Cluster cluster = GlobalStateMgr.getCurrentState().getCluster();
        if (null != cluster) {
            cluster.removeBackend(backend.getId());
        } else {
            LOG.error("Cluster {} no exist.", SystemInfoService.DEFAULT_CLUSTER);
        }
    }

    @Override
    public void updateBackendState(Backend be) {
        long id = be.getId();
        Backend memoryBe = getBackend(id);
        if (memoryBe == null) {
            // backend may already be dropped. this may happen when
            // 1. SystemHandler drop the decommission backend
            // 2. at same time, user try to cancel the decommission of that backend.
            // These two operations do not guarantee the order.
            return;
        }
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
    }

    private long getClusterAvailableCapacityB() {
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

    @Override
    public void checkClusterCapacity() throws DdlException {
        if (getClusterAvailableCapacityB() <= 0L) {
            throw new DdlException("Cluster has no available capacity");
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    @Override
    public long getBackendIdByHost(String host) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
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

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If floodStage is true, it will check with the floodStage threshold.
     *
     * return Status.OK if not reach the limit
     */
    @Override
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        LOG.debug("pathBeMap: {}", bePathsMap);
        ImmutableMap<Long, DiskInfo> pathHashToDiskInfo = pathHashToDishInfoRef;
        for (Long beId : bePathsMap.keySet()) {
            for (Long pathHash : bePathsMap.get(beId)) {
                DiskInfo diskInfo = pathHashToDiskInfo.get(pathHash);
                if (diskInfo != null && diskInfo.exceedLimit(floodStage)) {
                    return new Status(TStatusCode.CANCELLED,
                            "disk " + pathHash + " on backend " + beId + " exceed limit usage");
                }
            }
        }
        return Status.OK;
    }

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    @Override
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
}