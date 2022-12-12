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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.starrocks.catalog.DiskInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.ast.DropBackendClause;
import com.starrocks.sql.ast.ModifyBackendAddressClause;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStorageMedium;
import org.apache.commons.validator.routines.InetAddressValidator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * abstract SystemInfoService.
 * cluster id in function args is for lake table and warehouse.
 */
public abstract class SystemInfoService {
    public static final String DEFAULT_CLUSTER = "default_cluster";

    public abstract void addComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException;

    public abstract boolean isSingleBackendAndComputeNode(long clusterId);

    /**
     * @param hostPortPairs : backend's host and port
     * @throws DdlException
     */
    public abstract void addBackends(List<Pair<String, Integer>> hostPortPairs) throws DdlException;

    // for test
    public abstract void dropBackend(Backend backend);

    // for test
    public abstract void addBackend(Backend backend);

    public abstract ShowResultSet modifyBackendHost(ModifyBackendAddressClause modifyBackendAddressClause)
            throws DdlException;

    public abstract void dropComputeNodes(List<Pair<String, Integer>> hostPortPairs) throws DdlException;

    public abstract void dropComputeNode(String host, int heartbeatPort) throws DdlException;

    public abstract void dropBackends(DropBackendClause dropBackendClause) throws DdlException;

    // for decommission
    public abstract void dropBackend(long backendId) throws DdlException;

    // final entry of dropping backend
    public abstract void dropBackend(String host, int heartbeatPort, boolean needCheckUnforce) throws DdlException;

    // only for test
    public abstract void dropAllBackend();

    public abstract Backend getBackend(long backendId);

    public abstract Boolean checkWorkerHealthy(long workerId);

    public abstract ComputeNode getComputeNode(long computeNodeId);

    public abstract boolean checkBackendAvailable(long backendId);

    public abstract boolean checkBackendAlive(long backendId);

    public abstract ComputeNode getComputeNodeWithHeartbeatPort(String host, int heartPort);

    public abstract Backend getBackendWithHeartbeatPort(String host, int heartPort);

    public abstract long getBackendIdWithStarletPort(String host, int starletPort);

    public abstract TNetworkAddress toBrpcHost(TNetworkAddress host) throws Exception;

    public abstract Backend getBackendWithBePort(String host, int bePort);

    public abstract List<Backend> getBackendOnlyWithHost(String host);

    public List<Long> getBackendIds() {
        return getBackendIds(false);
    }

    public List<Long> getBackendIds(long clusterId) {
        return getBackendIds(false, clusterId);
    }

    public int getAliveBackendNumber() {
        return getBackendIds(true).size();
    }

    public int getAliveBackendNumber(long clusterId) {
        return getBackendIds(true, clusterId).size();
    }

    public abstract int getTotalBackendNumber();

    public abstract int getTotalBackendNumber(long clusterId);

    public abstract ComputeNode getComputeNodeWithBePort(String host, int bePort);

    public abstract List<Long> getComputeNodeIds(boolean needAlive);

    public abstract List<Long> getBackendIds(boolean needAlive);

    public abstract List<Long> getBackendIds(boolean needAlive, long clusterId);

    public abstract List<Long> getDecommissionedBackendIds();

    public abstract List<Long> getAvailableBackendIds();

    public abstract List<Backend> getBackends();

    public abstract List<Backend> getBackends(long clusterId);

    public abstract List<Long> seqChooseBackendIdsByStorageMedium(int backendNum, boolean needAvailable,
                                                                  boolean isCreate, TStorageMedium storageMedium);

    public abstract List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate);

    public abstract List<Long> seqChooseBackendIds(int backendNum, boolean needAvailable, boolean isCreate,
                                                   long clusterId);

    public abstract ImmutableMap<Long, Backend> getIdToBackend();

    public abstract ImmutableMap<Long, Backend> getIdToBackend(long clusterId);

    public abstract ImmutableMap<Long, ComputeNode> getIdComputeNode();

    public abstract ImmutableCollection<ComputeNode> getComputeNodes();

    public abstract ImmutableCollection<ComputeNode> getComputeNodes(boolean needAlive);

    public abstract long getBackendReportVersion(long backendId);

    public abstract void updateBackendReportVersion(long backendId, long newReportVersion, long dbId);

    public abstract long saveBackends(DataOutputStream dos, long checksum) throws IOException;

    public abstract long saveComputeNodes(DataOutputStream dos, long checksum) throws IOException;

    public abstract long loadBackends(DataInputStream dis, long checksum) throws IOException;

    public abstract long loadComputeNodes(DataInputStream dis, long checksum) throws IOException;

    public abstract void clear();

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

    public abstract void replayAddComputeNode(ComputeNode newComputeNode);

    public abstract void replayAddBackend(Backend newBackend);

    public abstract void replayDropComputeNode(long computeNodeId);

    public abstract void replayDropBackend(Backend backend);

    public abstract void updateBackendState(Backend be);

    public abstract void checkClusterCapacity() throws DdlException;

    /*
     * Try to randomly get a backend id by given host.
     * If not found, return -1
     */
    public abstract long getBackendIdByHost(String host);

    /*
     * Check if the specified disks' capacity has reached the limit.
     * bePathsMap is (BE id -> list of path hash)
     * If floodStage is true, it will check with the floodStage threshold.
     *
     * return Status.OK if not reach the limit
     */
    public abstract Status checkExceedDiskCapacityLimit(Multimap<Long, Long> bePathsMap, boolean floodStage);

    // update the path info when disk report
    // there is only one thread can update path info, so no need to worry about concurrency control
    public abstract void updatePathInfo(List<DiskInfo> addedDisks, List<DiskInfo> removedDisks);
}