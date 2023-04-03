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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * ElasticSystemInfoService for cloudNative table.
 * Backend is generated from StarOS worker.
 */
public class ElasticSystemInfoService extends SystemInfoService {
    private final Map<Long, Long> clusterIdToLastBackendId = Maps.newHashMap();
    private final StarOSAgent agent;

    public ElasticSystemInfoService(StarOSAgent agent) {
        this.agent = agent;
    }

    @Override
    public void addComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException {
        throw new SemanticException("not implemented");
    }

    @Override
    public boolean isSingleBackendAndComputeNode(long clusterId) {
        throw new SemanticException("not implemented");
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
        throw new SemanticException("not implemented");
    }

    @Override
    public Boolean checkWorkerHealthy(long workerId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public ComputeNode getComputeNode(long computeNodeId) {
        return null;
    }

    @Override
    public boolean checkBackendAvailable(long backendId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public boolean checkBackendAlive(long backendId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public ComputeNode getComputeNodeWithHeartbeatPort(String host, int heartPort) {
        return null;
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
        throw new SemanticException("not implemented");
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
        throw new SemanticException("not implemented");
    }

    @Override
    public int getTotalBackendNumber(long clusterId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public ComputeNode getComputeNodeWithBePort(String host, int bePort) {
        return null;
    }

    @Override
    public List<Long> getComputeNodeIds(boolean needAlive) {
        return Lists.newArrayList();
    }

    @Override
    public List<Long> getBackendIds(boolean needAlive) {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> getBackendIds(boolean needAlive, long clusterId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Long> getDecommissionedBackendIds() {
        return Lists.newArrayList();
    }

    @Override
    public List<Long> getAvailableBackendIds() {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Backend> getBackends() {
        throw new SemanticException("not implemented");
    }

    @Override
    public List<Backend> getBackends(long clusterId) {
        throw new SemanticException("not implemented");
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
        throw new SemanticException("not implemented");
    }

    @Override
    public ImmutableMap<Long, Backend> getIdToBackend() {
        throw new SemanticException("not implemented");
    }

    @Override
    public ImmutableMap<Long, Backend> getIdToBackend(long clusterId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
        return ImmutableMap.of();
    }

    @Override
    public ImmutableCollection<ComputeNode> getComputeNodes() {
        return ImmutableList.of();
    }

    @Override
    public ImmutableCollection<ComputeNode> getComputeNodes(boolean needAlive) {
        return ImmutableList.of();
    }

    @Override
    public long getBackendReportVersion(long backendId) {
        return -1;
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
    }

    @Override
    public void checkClusterCapacity() throws DdlException {
    }

    @Override
    public long getBackendIdByHost(String host) {
        return -1;
    }

    @Override
    public Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage) {
        return Status.OK;
    }

    @Override
    public void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks) {
    }

    @Override
    public ComputeNode getBackendOrComputeNode(long nodeId) {
        throw new SemanticException("not implemented");
    }

    @Override
    public Stream<ComputeNode> backendAndComputeNodeStream() {
        throw new SemanticException("not implemented");
    }

    @Override
    public void addComputeNode(ComputeNode computeNode) {
        throw new SemanticException("not implemented");
    }
}
