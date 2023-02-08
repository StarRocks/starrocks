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

package com.starrocks.lake;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStorageMedium;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SystemInfoService for lake table and warehouse.
 * Backend is generated from StarOS worker.
 */
public class LakeSystemInfoService extends SystemInfoService {
    private final Map<Long, Long> clusterIdToLastBackendId;
    private StarOSAgent agent;

    public LakeSystemInfoService(StarOSAgent starOSAgent) {
        this.agent = starOSAgent;
        this.clusterIdToLastBackendId = Maps.newHashMap();
    }

    @Override
    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public boolean isSingleBackendAndComputeNode(long clusterId) {
        try {
            return agent.getWorkersByWorkerGroup(Arrays.asList(clusterId)).size() == 1;
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public void addBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public void dropBackend(Backend backend) {
        throw new SemanticException("not implemented");
    }

    @Override
    public void addBackend(Backend backend) {
        throw new SemanticException("not implemented");
    }

    @Override
    public ShowResultSet modifyBackendHost(ModifyBackendAddressClause modifyBackendAddressClause) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public void dropComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public void dropComputeNode(String host, int heartbeatPort) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public void dropBackends(DropBackendClause dropBackendClause) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public void dropBackend(long backendId) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public void dropBackend(String host, int heartbeatPort, boolean needCheckUnforce) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public void dropAllBackend() {
        throw new SemanticException("not implemented");
    }

    @Override
    public Backend getBackend(long backendId) {
        try {
            return agent.getWorkerById(backendId);
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public Boolean checkWorkerHealthy(long workerId) {
        try {
            return agent.checkWorkerHealthy(workerId);
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public ComputeNode getComputeNode(long computeNodeId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public boolean checkBackendAvailable(long backendId) {
        try {
            Backend backend = agent.getWorkerById(backendId);
            return backend != null;
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public boolean checkBackendAlive(long backendId) {
        try {
            Backend backend = agent.getWorkerById(backendId);
            return backend != null && backend.isAlive();
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public ComputeNode getComputeNodeWithHeartbeatPort(String host, int heartPort) {
        throw new SemanticException("not implemented");
    }

    @Override
    public Backend getBackendWithHeartbeatPort(String host, int heartPort) {
        throw new SemanticException("not implemented");
    }

    @Override
    public long getBackendIdWithStarletPort(String host, int starletPort) {
        throw new SemanticException("not implemented");
    }

    @Override
    public TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception {
        Backend backend = getBackendWithBePort(host.getHostname(), host.getPort());
        if (backend == null) {
            throw new UserException("Worker not found");
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }

    @Override
    public Backend getBackendWithBePort(String host, int bePort) {
        try {
            List<Backend> backends = agent.getWorkers();
            for (Backend backend : backends) {
                if (backend.getHost().equals(host) && backend.getBePort() == bePort) {
                    return backend;
                }
            }
            return null;
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public List<Backend> getBackendOnlyWithHost(String host) {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> getBackendIds() {
        return Lists.newArrayList();
    }

    @Override
    public int getTotalBackendNumber() {
        try {
            return agent.getWorkers().size();
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public int getTotalBackendNumber(long clusterId) {
        try {
            return agent.getWorkersByWorkerGroup(Arrays.asList(clusterId)).size();
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public ComputeNode getComputeNodeWithBePort(String host, int bePort) {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> getComputeNodeIds(boolean needAlive) {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> getBackendIds(boolean needAlive) {
        try {
            List<Backend> backends = agent.getWorkers();
            return backends.stream().filter(b -> needAlive ? b.isAlive() : true).map(b -> b.getId())
                    .collect(Collectors.toList());
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public List<Long> getBackendIds(boolean needAlive, long clusterId) {
        try {
            List<Backend> backends = agent.getWorkersByWorkerGroup(Arrays.asList(clusterId));
            return backends.stream().filter(b -> needAlive ? b.isAlive() : true).map(b -> b.getId())
                    .collect(Collectors.toList());
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public List<Long> getDecommissionedBackendIds() {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> getAvailableBackendIds() {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Backend> getBackends() {
        try {
            return agent.getWorkers();
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public List<Backend> getBackends(long clusterId) {
        try {
            return agent.getWorkersByWorkerGroup(Arrays.asList(clusterId));
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public List<Long> seqChooseBackendIdsByStorageMedium(int backendNum, boolean needAvailable, boolean isCreate,
                                                         TStorageMedium storageMedium) {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate) {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate, long clusterId) {
        return seqChooseBackendIds(backendNum, clusterId);
    }

    private synchronized List<Long> seqChooseBackendIds(int backendNum, long clusterId) {
        List<Long> backendIds = Lists.newArrayList();

        List<Backend> backends = null;
        try {
            backends = agent.getWorkersByWorkerGroup(Arrays.asList(clusterId));
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }

        // get last backend index
        long lastBackendId = clusterIdToLastBackendId.get(clusterId);
        int lastBackendIndex = -1;
        int index = -1;
        for (Backend backend : backends) {
            ++index;
            if (backend.getId() == lastBackendId) {
                lastBackendIndex = index;
                break;
            }
        }

        // choose backends, 2 cycle at most
        Iterator<Backend> iterator = Iterators.cycle(backends);
        index = -1;
        int maxIndex = 2 * backends.size();
        while (iterator.hasNext() && backendIds.size() < backendNum) {
            Backend backend = iterator.next();

            ++index;
            if (index <= lastBackendIndex) {
                continue;
            }

            if (index > maxIndex) {
                break;
            }

            long backendId = backend.getId();
            if (!backendIds.contains(backendId)) {
                backendIds.add(backendId);
                lastBackendId = backendId;
            } else {
                break;
            }
        }

        if (backendIds.size() == backendNum) {
            clusterIdToLastBackendId.put(clusterId, lastBackendId);
            return backendIds;
        }
        return Lists.newArrayList();
    }

    @Override
    public ImmutableMap<Long, Backend> getIdToBackend() {
        try {
            List<Backend> backends = agent.getWorkers();
            Map<Long, Backend> idToBackend = backends.stream().collect(Collectors.toMap(Backend::getId, b -> b));
            return ImmutableMap.copyOf(idToBackend);
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public ImmutableMap<Long, Backend> getIdToBackend(long clusterId) {
        try {
            List<Backend> backends = agent.getWorkersByWorkerGroup(Arrays.asList(clusterId));
            Map<Long, Backend> idToBackend = backends.stream().collect(Collectors.toMap(Backend::getId, b -> b));
            return ImmutableMap.copyOf(idToBackend);
        } catch (UserException e) {
            throw new SemanticException(e.getMessage());
        }
    }

    @Override
    public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
        return ImmutableMap.<Long, ComputeNode>of();
    }

    @Override
    public ImmutableCollection<ComputeNode> getComputeNodes() {
        throw new SemanticException("not implemented");
    }

    @Override
    public ImmutableCollection<ComputeNode> getComputeNodes(boolean needAlive) {
        throw new SemanticException("not implemented");
    }

    @Override
    public long getBackendReportVersion(long backendId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public void updateBackendReportVersion(long backendId, long newReportVersion, long dbId) {
    }

    @Override
    public long saveBackends(DataOutputStream dos, long checksum) throws IOException {
        return checksum;
    }

    @Override
    public long saveComputeNodes(DataOutputStream dos, long checksum) throws IOException {
        return checksum;
    }

    @Override
    public long loadBackends(DataInputStream dis, long checksum) throws IOException {
        return checksum;
    }

    @Override
    public long loadComputeNodes(DataInputStream dis, long checksum) throws IOException {
        return checksum;
    }

    @Override
    public void clear() {
        throw new SemanticException("not implemented");
    }

    @Override
    public void replayAddComputeNode(ComputeNode newComputeNode) {
        throw new SemanticException("not implemented");
    }

    @Override
    public void replayAddBackend(Backend newBackend) {
        throw new SemanticException("not implemented");
    }

    @Override
    public void replayDropComputeNode(long computeNodeId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public void replayDropBackend(Backend backend) {
        throw new SemanticException("not implemented");
    }

    @Override
    public void updateBackendState(Backend be) {
        throw new SemanticException("not implemented");
    }

    @Override
    public void checkClusterCapacity() throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public long getBackendIdByHost(String host) {
        return -1L;
    }

    @Override
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        return Status.OK;
    }

    @Override
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
        throw new SemanticException("not implemented");
    }
}
