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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
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
import com.starrocks.server.RunMode;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.system.Backend.BackendState;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SystemInfoService {
    private static final Logger LOG = LogManager.getLogger(SystemInfoService.class);

    public static final String DEFAULT_CLUSTER = "default_cluster";

    private volatile ImmutableMap<Long, Backend> idToBackendRef;
    private volatile ImmutableMap<Long, AtomicLong> idToReportVersionRef;

    private volatile ImmutableMap<Long, ComputeNode> idToComputeNodeRef;

    // for data node management
    private volatile ImmutableMap<Long, DataNode> idToDataNodeRef;

    private long lastBackendIdForCreation = -1;
    private long lastBackendIdForOther = -1;

    private volatile ImmutableMap<Long, DiskInfo> pathHashToDishInfoRef;

    public SystemInfoService() {
        idToBackendRef = ImmutableMap.<Long, Backend>of();
        idToReportVersionRef = ImmutableMap.<Long, AtomicLong>of();

        idToComputeNodeRef = ImmutableMap.<Long, ComputeNode>of();

        pathHashToDishInfoRef = ImmutableMap.<Long, DiskInfo>of();
    }

    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs)
            throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
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

    /**
     * For test.
     */
    public void addComputeNode(ComputeNode computeNode) {
        Map<Long, ComputeNode> copiedComputeNodes = Maps.newHashMap(idToComputeNodeRef);
        copiedComputeNodes.put(computeNode.getId(), computeNode);
        idToComputeNodeRef = ImmutableMap.copyOf(copiedComputeNodes);
    }

    // Final entry of adding compute node
    private void addComputeNode(String host, int heartbeatPort) throws DdlException {
        ComputeNode newComputeNode = new ComputeNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // update idToComputor
        Map<Long, ComputeNode> copiedComputeNodes = Maps.newHashMap(idToComputeNodeRef);
        copiedComputeNodes.put(newComputeNode.getId(), newComputeNode);
        idToComputeNodeRef = ImmutableMap.copyOf(copiedComputeNodes);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddComputeNode(newComputeNode);
        LOG.info("finished to add {} ", newComputeNode);
    }

    public boolean isSingleComputeNode() {
        return idToComputeNodeRef.size() == 1;
    }

    // for test
    public void dropBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.remove(backend.getId());
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);

        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(backend.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);
    }

    // for test
    public void addBackend(Backend backend) {
        Map<Long, Backend> copiedBackends = Maps.newHashMap(idToBackendRef);
        copiedBackends.put(backend.getId(), backend);
        idToBackendRef = ImmutableMap.copyOf(copiedBackends);
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.put(backend.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);
    }

    public void modifyComputeNodeHost(String willBeModifiedHost, String fqdn) throws DdlException {
        // update idToComputeNode
        List<ComputeNode> candidateCns = getComputeNodesOnlyWithHost(willBeModifiedHost);
        if (null == candidateCns || candidateCns.size() == 0) {
            throw new DdlException(String.format("compute node [%s] not found", willBeModifiedHost));
        }

        ComputeNode preUpdateCn = candidateCns.get(0);
        Map<Long, ComputeNode> copiedCns = Maps.newHashMap(idToComputeNodeRef);
        ComputeNode updateCn = copiedCns.get(preUpdateCn.getId());
        updateCn.setHost(fqdn);
        idToComputeNodeRef = ImmutableMap.copyOf(copiedCns);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logComputeNodeStateChange(updateCn);
    }

    public ShowResultSet modifyDataNodeHost(String willBeModifiedHost, String fqdn) throws DdlException {
        // update idToDataNode
        List<DataNode> candidateDns = getDataNodesOnlyWithHost(willBeModifiedHost);
        if (null == candidateDns || candidateDns.size() == 0) {
            throw new DdlException(String.format("data node [%s] not found", willBeModifiedHost));
        }

        DataNode preUpdateDn = candidateDns.get(0);
        Map<Long, DataNode> copiedDns = Maps.newHashMap(idToDataNodeRef);
        DataNode updateDn = copiedDns.get(preUpdateDn.getId());
        updateDn.setHost(fqdn);
        idToDataNodeRef = ImmutableMap.copyOf(copiedDns);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logDataNodeStateChange(updateDn);

        // Message
        StringBuilder formatSb = new StringBuilder();
        String opMessage;
        formatSb.append("%s:%d's host has been modified to %s");
        if (candidateDns.size() >= 2) {
            formatSb.append("\nplease exectue %d times, to modify the remaining backends\n");
            for (int i = 1; i < candidateDns.size(); i++) {
                DataNode dn = candidateDns.get(i);
                formatSb.append(dn.getHost() + ":" + dn.getHeartbeatPort() + "\n");
            }
            opMessage = String.format(
                    formatSb.toString(), willBeModifiedHost,
                    updateDn.getHeartbeatPort(), fqdn, candidateDns.size() - 1);
        } else {
            opMessage = String.format(formatSb.toString(), willBeModifiedHost, updateDn.getHeartbeatPort(), fqdn);
        }

        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Message", ScalarType.createVarchar(1024)));
        List<List<String>> messageResult = new ArrayList<>();
        messageResult.add(Arrays.asList(opMessage));
        return new ShowResultSet(builder.build(), messageResult);
    }

    public ShowResultSet modifyBackendHost(ModifyBackendAddressClause modifyBackendAddressClause) throws DdlException {
        String willBeModifiedHost = modifyBackendAddressClause.getSrcHost();
        String fqdn = modifyBackendAddressClause.getDestHost();
        ShowResultSet resultSet = null;
        try {
            modifyComputeNodeHost(willBeModifiedHost, fqdn);
            resultSet = modifyDataNodeHost(willBeModifiedHost, fqdn);
        } catch (DdlException e) {
            throw new DdlException(String.format("backend [%s] not found", willBeModifiedHost));
        }

        return resultSet;
    }

    public void dropComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getComputeNodeWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("Does not exists[" + pair.first + ":" + pair.second + "]");
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
        }

        // update idToComputeNode
        Map<Long, ComputeNode> copiedComputeNodes = Maps.newHashMap(idToComputeNodeRef);
        copiedComputeNodes.remove(dropComputeNode.getId());
        idToComputeNodeRef = ImmutableMap.copyOf(copiedComputeNodes);

        // remove worker
        if (RunMode.allowCreateLakeTable()) {
            long starletPort = dropComputeNode.getStarletPort();
            // only need to remove worker after be reported its starletPort
            if (starletPort != 0) {
                String workerAddr = dropComputeNode.getHost() + ":" + starletPort;
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(workerAddr);
            }
        }

        // log
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDropComputeNode(new DropComputeNodeLog(dropComputeNode.getId()));
        LOG.info("finished to drop {}", dropComputeNode);
    }

    public Backend getBackend(long backendId) {
        return idToBackendRef.get(backendId);
    }

    public ComputeNode getComputeNode(long computeNodeId) {
        return idToComputeNodeRef.get(computeNodeId);
    }

    public boolean checkDataNodeAvailable(long dataNodeId) {
        DataNode dataNode = idToDataNodeRef.get(dataNodeId);
        return dataNode != null && dataNode.isAvailable();
    }

    public boolean checkDataNodeAlive(long dataNodeId) {
        DataNode dataNode = idToDataNodeRef.get(dataNodeId);
        return dataNode != null && dataNode.isAlive();
    }

    public ComputeNode getComputeNodeWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        for (ComputeNode computeNode : idToComputeNode.values()) {
            if (computeNode.getHost().equals(host) && computeNode.getHeartbeatPort() == heartPort) {
                return computeNode;
            }
        }
        return null;
    }

    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getHeartbeatPort() == heartPort) {
                return backend;
            }
        }
        return null;
    }

    public long getBackendIdWithStarletPort(String host, int starletPort) {
        ImmutableMap<Long, Backend> idToBackend = idToBackendRef;
        for (Backend backend : idToBackend.values()) {
            if (backend.getHost().equals(host) && backend.getStarletPort() == starletPort) {
                return backend.getId();
            }
        }
        return -1L;
    }

    public static TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
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
        return new TNetworkAddress(computeNode.getHost(), computeNode.getBrpcPort());
    }

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

    public List<ComputeNode> getComputeNodesOnlyWithHost(String host) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        List<ComputeNode> result = new ArrayList<>();
        for (ComputeNode cn : idToComputeNode.values()) {
            if (cn.getHost().equals(host)) {
                result.add(cn);
            }
        }
        return result;
    }

    public List<DataNode> getDataNodesOnlyWithHost(String host) {
        ImmutableMap<Long, DataNode> idToDataNode = idToDataNodeRef;
        List<DataNode> result = new ArrayList<>();
        for (DataNode dn : idToDataNode.values()) {
            if (dn.getHost().equals(host)) {
                result.add(dn);
            }
        }
        return result;
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

    public int getTotalDataNodeNumber() {
        return idToDataNodeRef.size();
    }

    public ComputeNode getComputeNodeWithBePort(String host, int bePort) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        for (ComputeNode computeNode : idToComputeNode.values()) {
            if (computeNode.getHost().equals(host) && computeNode.getBePort() == bePort) {
                return computeNode;
            }
        }
        return null;
    }

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

    public List<Backend> getBackends() {
        return idToBackendRef.values().asList();
    }

    public Stream<ComputeNode> computeNodeStream() {
        return idToComputeNodeRef.values().stream();
    }

    public List<Long> seqChooseBackendIdsByStorageMedium(int backendNum, boolean needAvailable, boolean isCreate,
                                                         TStorageMedium storageMedium) {
        final List<Backend> backends =
                getBackends().stream().filter(v -> !v.diskExceedLimitByStorageMedium(storageMedium))
                        .collect(Collectors.toList());
        return seqChooseBackendIds(backendNum, needAvailable, isCreate, backends);
    }

    public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate) {
        final List<Backend> backends =
                getBackends().stream().filter(v -> !v.diskExceedLimit()).collect(Collectors.toList());
        return seqChooseBackendIds(backendNum, needAvailable, isCreate, backends);
    }

    // choose backends by round-robin
    // return null if not enough backend
    // use synchronized to run serially
    public synchronized List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate,
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

    public ImmutableMap<Long, Backend> getIdToBackend() {
        return idToBackendRef;
    }

    public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
        return idToComputeNodeRef;
    }

    public ImmutableCollection<ComputeNode> getComputeNodes() {
        return getComputeNodes(true);
    }

    public ImmutableCollection<ComputeNode> getComputeNodes(boolean needAlive) {
        ImmutableMap<Long, ComputeNode> idToComputeNode = idToComputeNodeRef;
        List<Long> computeNodeIds = getComputeNodeIds(needAlive);
        List<ComputeNode> computeNodes = new ArrayList<>();
        for (Long computeNodeId : computeNodeIds) {
            computeNodes.add(idToComputeNode.get(computeNodeId));
        }
        return ImmutableList.copyOf(computeNodes);
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

    public long saveComputeNodes(DataOutputStream dos, long checksum) throws IOException {
        SystemInfoService.SerializeData data = new SystemInfoService.SerializeData();
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

    public static Pair<String, Integer> validateHostAndPort(String hostPort) throws AnalysisException {
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
        try {
            // validate host
            if (!InetAddressValidator.getInstance().isValid(host) && !FrontendOptions.isUseFqdn()) {
                // maybe this is a hostname
                // if no IP address for the host could be found, 'getByName'
                // will throw
                // UnknownHostException
                InetAddress inetAddress = InetAddress.getByName(host);
                host = inetAddress.getHostAddress();
            }

            // validate port
            heartbeatPort = Integer.parseInt(pair[1]);

            if (heartbeatPort <= 0 || heartbeatPort >= 65536) {
                throw new AnalysisException("Port is out of range: " + heartbeatPort);
            }

            return new Pair<String, Integer>(host, heartbeatPort);
        } catch (UnknownHostException e) {
            throw new AnalysisException("Unknown host: " + e.getMessage());
        } catch (Exception e) {
            throw new AnalysisException("Encounter unknown exception: " + e.getMessage());
        }
    }

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
            // clear map in starosAgent
            if (RunMode.allowCreateLakeTable()) {
                long starletPort = backend.getStarletPort();
                if (starletPort == 0) {
                    return;
                }
                String workerAddr = backend.getHost() + ":" + starletPort;
                long workerId = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerId(workerAddr);
                GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorkerFromMap(workerId, workerAddr);
            }
        } else {
            LOG.error("Cluster {} no exist.", SystemInfoService.DEFAULT_CLUSTER);
        }
    }

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

    public void checkClusterCapacity() throws DdlException {
        if (getClusterAvailableCapacityB() <= 0L) {
            throw new DdlException("Cluster has no available capacity");
        }
    }

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
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

    // ---------------------------------------- datanode management -----------------------------------------------------
    /**
     * @param hostPortPairs : backend's host and port
     * @throws DdlException
     */
    public void addDataNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getDataNodeWithHeartbeatPort(pair.first, pair.second) != null) {
                throw new DdlException("Same backend already exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            addDataNode(pair.first, pair.second);
        }
    }

    public DataNode getDataNodeWithHeartbeatPort(String host, int heartbeatPort) {
        ImmutableMap<Long, DataNode> idToDataNode = idToDataNodeRef;
        for (DataNode dataNode : idToDataNode.values()) {
            if (dataNode.getHost().equals(host) && dataNode.getHeartbeatPort() == heartbeatPort) {
                return dataNode;
            }
        }
        return null;
    }

    public void addDataNode(String host, int heartbeatPort) {
        DataNode dataNode = new DataNode(GlobalStateMgr.getCurrentState().getNextId(), host, heartbeatPort);
        // update idToBackend
        Map<Long, DataNode> copiedDataNodes = Maps.newHashMap(idToDataNodeRef);
        copiedDataNodes.put(dataNode.getId(), dataNode);
        idToDataNodeRef = ImmutableMap.copyOf(copiedDataNodes);

        // set new dataNode's report version as 0L
        Map<Long, AtomicLong> copiedReportVersions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVersions.put(dataNode.getId(), new AtomicLong(0L));
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVersions);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logAddDataNode(dataNode);
        LOG.info("finished to add data node {} ", dataNode);

        // backends is changed, regenerated tablet number metrics
        // TODO: change be -> cn
        MetricRepo.generateBackendsTabletMetrics();
    }

    public void dropDataNodes(List<Pair<String, Integer>> hostPortPairs, boolean needCheckUnforce)
            throws DdlException {
        for (Pair<String, Integer> pair : hostPortPairs) {
            // check is already exist
            if (getDataNodeWithHeartbeatPort(pair.first, pair.second) == null) {
                throw new DdlException("data node does not exists[" + pair.first + ":" + pair.second + "]");
            }
        }

        for (Pair<String, Integer> pair : hostPortPairs) {
            dropDataNode(pair.first, pair.second, needCheckUnforce);
        }
    }

    // final entry of dropping dataNode
    public void dropDataNode(String host, int heartbeatPort, boolean needCheckUnforce)
            throws DdlException {
        if (getDataNodeWithHeartbeatPort(host, heartbeatPort) == null) {
            throw new DdlException("data node does not exists[" + host + ":" + heartbeatPort + "]");
        }

        DataNode droppedDataNode = getDataNodeWithHeartbeatPort(host, heartbeatPort);
        if (needCheckUnforce) {
            try {
                checkUnforce(droppedDataNode);
            } catch (RuntimeException e) {
                throw new DdlException(e.getMessage());
            }
        }

        // update idToDataNode
        Map<Long, DataNode> copiedDataNodes = Maps.newHashMap(idToDataNodeRef);
        copiedDataNodes.remove(droppedDataNode.getId());
        idToDataNodeRef = ImmutableMap.copyOf(copiedDataNodes);

        // update idToReportVersion
        Map<Long, AtomicLong> copiedReportVerions = Maps.newHashMap(idToReportVersionRef);
        copiedReportVerions.remove(droppedDataNode.getId());
        idToReportVersionRef = ImmutableMap.copyOf(copiedReportVerions);

        // log
        GlobalStateMgr.getCurrentState().getEditLog().logDropDataNode(droppedDataNode);
        LOG.info("finished to drop {}", droppedDataNode);

        // backends is changed, regenerated tablet number metrics
        // TODO: change be -> cn
        MetricRepo.generateBackendsTabletMetrics();
    }

    private void checkUnforce(DataNode droppedDataNode) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Long> tabletIds = GlobalStateMgr.getCurrentInvertedIndex().getTabletIdsByDataNodeId(droppedDataNode.getId());
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
                                        db.getOriginName(), table.getName(), droppedDataNode.getHost(),
                                        droppedDataNode.getHeartbeatPort(), db.getOriginName(), table.getName());

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

    // for decommission
    public void dropDataNode(long dataNodeId) throws DdlException {
        DataNode dataNode = getDataNode(dataNodeId);
        if (dataNode == null) {
            throw new DdlException("DataNode[" + dataNodeId + "] does not exist");
        }

        dropDataNode(dataNode.getHost(), dataNode.getHeartbeatPort(), false);
    }

    public List<Long> getDataNodeIds() {
        ImmutableMap<Long, DataNode> idToDataNode = idToDataNodeRef;
        List<Long> dataNodeIds = Lists.newArrayList(idToDataNode.keySet());
        Iterator<Long> iter = dataNodeIds.iterator();
        while (iter.hasNext()) {
            DataNode dataNode = this.getDataNode(iter.next());
            if (dataNode == null || !dataNode.isAlive()) {
                iter.remove();
            }
        }
        return dataNodeIds;
    }

    public DataNode getDataNode(long dataNodeId) {
        return idToDataNodeRef.get(dataNodeId);
    }

    public ImmutableMap<Long, DataNode> getIdDataNode() {
        return idToDataNodeRef;
    }
}

