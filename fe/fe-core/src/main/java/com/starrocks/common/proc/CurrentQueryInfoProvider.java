// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/CurrentQueryInfoProvider.java

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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.proto.PCollectFragmentStatistcs;
import com.starrocks.proto.PCollectFragmentStatisticsResult;
import com.starrocks.proto.PCollectQueryStatistics;
import com.starrocks.proto.PCollectQueryStatisticsResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PCollectFragmentStatisticsRequest;
import com.starrocks.rpc.PCollectQueryStatisticsRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Provide running query's statistics.
 */
public class CurrentQueryInfoProvider {
    private static final Logger LOG = LogManager.getLogger(CurrentQueryInfoProvider.class);

    public CurrentQueryInfoProvider() {
    }

    public Map<String, QueryStatistics> getQueryStatistics(Collection<QueryStatisticsItem> items)
        throws AnalysisException {
        return collectQueryStatistics(items);
    }

    public Collection<InstanceStatistics> getInstanceStatistics(QueryStatisticsItem item) throws AnalysisException {
        return collectFragmentStatistics(item);
    }

    public Map<String, QueryStatistics> getQueryStatisticsByHost(QueryStatisticsItem item) throws AnalysisException {
        final Map<TNetworkAddress, Request> requests = Maps.newHashMap();
        final Map<TNetworkAddress, TNetworkAddress> brpcAddresses = Maps.newHashMap();
        for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
            TNetworkAddress brpcNetAddress = brpcAddresses.get(instanceInfo.getAddress());
            if (brpcNetAddress == null) {
                try {
                    brpcNetAddress = toBrpcHost(instanceInfo.getAddress());
                    brpcAddresses.put(instanceInfo.getAddress(), brpcNetAddress);
                } catch (Exception e) {
                    LOG.warn(e.getMessage());
                    throw new AnalysisException(e.getMessage());
                }
            }
            Request request = requests.get(brpcNetAddress);
            if (request == null) {
                request = new Request(brpcNetAddress);
                requests.put(brpcNetAddress, request);
            }
            request.addQueryId(item.getExecutionId());
        }
        return handleCollectQueryResponseByHost(sendCollectQueryRequest(requests));
    }

    private Map<String, QueryStatistics> handleCollectQueryResponseByHost(
            List<Pair<Request, Future<PCollectQueryStatisticsResult>>> futures) throws AnalysisException {
        Map<String, QueryStatistics> statisticsMap = Maps.newHashMap();
        for (Pair<Request, Future<PCollectQueryStatisticsResult>> pair : futures) {
            try {
                final PCollectQueryStatisticsResult result = pair.second.get(10, TimeUnit.SECONDS);
                if (result.queryStatistics != null) {
                    Preconditions.checkState(result.queryStatistics.size() == 1);
                    PCollectQueryStatistics queryStatistics = result.queryStatistics.get(0);
                    QueryStatistics statistics = new QueryStatistics();
                    statistics.updateCpuCostNs(queryStatistics.cpuCostNs);
                    statistics.updateScanBytes(queryStatistics.scanBytes);
                    statistics.updateScanRows(queryStatistics.scanRows);
                    final Request request = pair.first;
                    String host = String.format("%s:%d",
                            request.getAddress().getHostname(), request.getAddress().getPort());
                    statisticsMap.put(host, statistics);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.warn("fail to receive result " + e.getCause());
                throw new AnalysisException(e.getMessage());
            }
        }
        return statisticsMap;
    }

    private Collection<InstanceStatistics> collectFragmentStatistics(
            QueryStatisticsItem item) throws AnalysisException {
        final Map<TNetworkAddress, Request> requests = Maps.newHashMap();
        final Map<TNetworkAddress, TNetworkAddress> brpcAddresses = Maps.newHashMap();

        for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
            TNetworkAddress brpcNetAddress = brpcAddresses.get(instanceInfo.getAddress());
            if (brpcNetAddress == null) {
                try {
                    brpcNetAddress = toBrpcHost(instanceInfo.getAddress());
                    brpcAddresses.put(instanceInfo.getAddress(), brpcNetAddress);
                } catch (Exception e) {
                    LOG.warn(e.getMessage());
                    throw new AnalysisException(e.getMessage());
                }
            }
            Request request = requests.get(brpcNetAddress);
            if (request == null) {
                request = new Request(brpcNetAddress);
                request.addQueryId(item.getExecutionId());
                requests.put(brpcNetAddress, request);
            }
            PUniqueId instanceId = new PUniqueId();
            instanceId.hi = instanceInfo.getInstanceId().hi;
            instanceId.lo = instanceInfo.getInstanceId().lo;
            request.addInstanceId(instanceId);
        }
        Map<String, QueryStatistics> statisticsV2Map
                = handleCollectFragmentResponse(sendCollectFragmentRequest(requests));
        List<InstanceStatistics> instanceStatisticsList = Lists.newArrayList();
        for (QueryStatisticsItem.FragmentInstanceInfo fragmentInstanceInfo : item.getFragmentInstanceInfos()) {
            TUniqueId instanceId = fragmentInstanceInfo.getInstanceId();
            QueryStatistics queryStatisticsV2 = statisticsV2Map.get(DebugUtil.printId(instanceId));
            if (queryStatisticsV2 == null) {
                continue;
            }
            String fragmentId = fragmentInstanceInfo.getFragmentId();
            TNetworkAddress address = fragmentInstanceInfo.getAddress();
            InstanceStatistics instanceStatisticsV2 =
                    new InstanceStatistics(fragmentId, instanceId, address, queryStatisticsV2);
            instanceStatisticsList.add(instanceStatisticsV2);
        }
        return instanceStatisticsList;
    }

    private List<Pair<Request, Future<PCollectFragmentStatisticsResult>>> sendCollectFragmentRequest(
            Map<TNetworkAddress, Request> requests) throws AnalysisException {
        final List<Pair<Request, Future<PCollectFragmentStatisticsResult>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : requests.keySet()) {
            final Request request = requests.get(address);
            List<TUniqueId> queryIds = request.getQueryIds();
            PUniqueId queryId = new PUniqueId();
            queryId.hi = queryIds.get(0).hi;
            queryId.lo = queryIds.get(0).lo;
            final PCollectFragmentStatisticsRequest pbRequest =
                    new PCollectFragmentStatisticsRequest(queryId, request.getInstanceIds());
            try {
                futures.add(Pair.create(
                        request, BackendServiceClient.getInstance().collectFragmentStatisticsAsync(address, pbRequest)));
            } catch (RpcException e) {
                throw new AnalysisException("Sending collect query statistics request fails.");
            }
        }
        return futures;
    }

    private Map<String, QueryStatistics> handleCollectFragmentResponse(
            List<Pair<Request, Future<PCollectFragmentStatisticsResult>>> futures) throws AnalysisException {
        Map<String, QueryStatistics> statisticsMap = Maps.newHashMap();
        for (Pair<Request, Future<PCollectFragmentStatisticsResult>> pair : futures) {
            try {
                final PCollectFragmentStatisticsResult result = pair.second.get(10, TimeUnit.SECONDS);
                for (PCollectFragmentStatistcs fragmentStatistics : result.fragmentStatistics) {
                    String instanceIdStr = DebugUtil.printId(fragmentStatistics.fragmentInstanceId);
                    QueryStatistics statistics = new QueryStatistics();
                    statistics.updateCpuCostNs(fragmentStatistics.cpuCostNs);
                    statistics.updateScanBytes(fragmentStatistics.scanBytes);
                    statistics.updateScanRows(fragmentStatistics.scanRows);
                    statisticsMap.put(instanceIdStr, statistics);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.warn("fail to receive result" + e.getCause());
                throw new AnalysisException(e.getMessage());
            }
        }
        return statisticsMap;
    }

    private Map<String, QueryStatistics> collectQueryStatistics(Collection<QueryStatisticsItem> items)
            throws AnalysisException {
        final Map<TNetworkAddress, Request> requests = Maps.newHashMap();
        final Map<TNetworkAddress, TNetworkAddress> brpcAddresses = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
                TNetworkAddress brpcNetAddress = brpcAddresses.get(instanceInfo.getAddress());
                if (brpcNetAddress == null) {
                    try {
                        brpcNetAddress = toBrpcHost(instanceInfo.getAddress());
                        brpcAddresses.put(instanceInfo.getAddress(), brpcNetAddress);
                    } catch (Exception e) {
                        LOG.warn(e.getMessage());
                        throw new AnalysisException(e.getMessage());
                    }
                }
                Request request = requests.get(brpcNetAddress);
                if (request == null) {
                    request = new Request(brpcNetAddress);
                    requests.put(brpcNetAddress, request);
                }
                request.addQueryId(item.getExecutionId());
            }
        }
        return handleCollectQueryResponse(sendCollectQueryRequest(requests));
    }

    private List<Pair<Request, Future<PCollectQueryStatisticsResult>>> sendCollectQueryRequest(
            Map<TNetworkAddress, Request> requests) throws AnalysisException {
        final List<Pair<Request, Future<PCollectQueryStatisticsResult>>> futures = Lists.newArrayList();
        for (TNetworkAddress address : requests.keySet()) {
            final Request request = requests.get(address);
            List<PUniqueId> queryIds = Lists.newArrayList();
            for (TUniqueId tQueryId : request.getQueryIds()) {
                PUniqueId queryId = new PUniqueId();
                queryId.hi = tQueryId.hi;
                queryId.lo = tQueryId.lo;
                queryIds.add(queryId);
            }
            final PCollectQueryStatisticsRequest pbRequest = new PCollectQueryStatisticsRequest(queryIds);
            try {
                futures.add(Pair.create(
                        request, BackendServiceClient.getInstance().collectQueryStatisticsAsync(address, pbRequest)));
            } catch (RpcException e) {
                throw new AnalysisException("Sending collect query statistics request fails.");
            }
        }
        return futures;
    }

    private Map<String, QueryStatistics> handleCollectQueryResponse(
            List<Pair<Request, Future<PCollectQueryStatisticsResult>>> futures) throws AnalysisException {
        Map<String, QueryStatistics> statisticsMap = Maps.newHashMap();
        for (Pair<Request, Future<PCollectQueryStatisticsResult>> pair : futures) {
            try {
                final PCollectQueryStatisticsResult result = pair.second.get(10, TimeUnit.SECONDS);
                if (result.queryStatistics != null) {
                    for (PCollectQueryStatistics queryStatistics : result.queryStatistics) {
                        final String queryIdStr = DebugUtil.printId(queryStatistics.queryId);
                        QueryStatistics statistics = statisticsMap.get(queryIdStr);
                        if (statistics == null) {
                            statistics = new QueryStatistics();
                            statisticsMap.put(queryIdStr, statistics);
                        }
                        statistics.updateCpuCostNs(queryStatistics.cpuCostNs);
                        statistics.updateScanBytes(queryStatistics.scanBytes);
                        statistics.updateScanRows(queryStatistics.scanRows);
                    }
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.warn("fail to receive result" + e.getCause());
                throw new AnalysisException(e.getMessage());
            }
        }
        return statisticsMap;
    }

    private TNetworkAddress toBrpcHost(TNetworkAddress host) throws AnalysisException {
        final Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackendWithBePort(
                host.getHostname(), host.getPort());
        if (backend == null) {
            throw new AnalysisException(new StringBuilder("Backend ")
                    .append(host.getHostname())
                    .append(":")
                    .append(host.getPort())
                    .append(" does not exist")
                    .toString());
        }
        if (backend.getBrpcPort() < 0) {
            throw new AnalysisException("BRPC port is't exist.");
        }
        return new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
    }

    public static class QueryStatistics {
        long cpuCostNs = 0;
        long scanBytes = 0;
        long scanRows = 0;

        public QueryStatistics() {

        }

        public long getCpuCostNs() {
            return cpuCostNs;
        }

        public void updateCpuCostNs(long value) {
            cpuCostNs += value;
        }

        public long getScanBytes() {
            return scanBytes;
        }

        public void updateScanBytes(long value) {
            scanBytes += value;
        }

        public long getScanRows() {
            return scanRows;
        }

        public void updateScanRows(long value) {
            scanRows += value;
        }
    }

    public static class InstanceStatistics {
        private final String fragmentId;
        private final TUniqueId instanceId;
        private final TNetworkAddress address;
        private final QueryStatistics statistics;

        public InstanceStatistics(String fragmentId,
                                    TUniqueId instanceId, TNetworkAddress address, QueryStatistics statistics) {
            this.fragmentId = fragmentId;
            this.instanceId = instanceId;
            this.address = address;
            this.statistics = statistics;
        }

        public String getFragmentId() {
            return fragmentId;
        }

        public TUniqueId getInstanceId() {
            return instanceId;
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public long getScanBytes() {
            return statistics.getScanBytes();
        }

        public long getScanRows() {
            return statistics.getScanRows();
        }

        public long getCPUCostNs() {
            return statistics.getCpuCostNs();
        }
    }

    private static class Request {
        private final TNetworkAddress address;
        private final List<PUniqueId> instanceIds;
        private final Set<TUniqueId> queryIds;

        public Request(TNetworkAddress address) {
            this.address = address;
            this.instanceIds = Lists.newArrayList();
            this.queryIds = new HashSet<>();
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public List<PUniqueId> getInstanceIds() {
            return instanceIds;
        }

        public void addInstanceId(PUniqueId instanceId) {
            this.instanceIds.add(instanceId);
        }

        public List<TUniqueId> getQueryIds() {
            return Lists.newArrayList(queryIds);
        }

        public void addQueryId(TUniqueId queryId) {
            this.queryIds.add(queryId);
        }
    }
}
