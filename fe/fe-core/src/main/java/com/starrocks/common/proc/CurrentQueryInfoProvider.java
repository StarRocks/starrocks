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
import com.starrocks.proto.PCollectQueryStatistics;
import com.starrocks.proto.PCollectQueryStatisticsResult;
import com.starrocks.proto.PUniqueId;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.rpc.PCollectQueryStatisticsRequest;
import com.starrocks.rpc.RpcException;
import com.starrocks.system.SystemInfoService;
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
import java.util.stream.Collectors;

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

    public Map<String, QueryStatistics> getQueryStatisticsByHost(QueryStatisticsItem item) throws AnalysisException {
        final Map<TNetworkAddress, Request> requests = Maps.newHashMap();
        final Map<TNetworkAddress, TNetworkAddress> brpcAddresses = Maps.newHashMap();
        for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
            TNetworkAddress brpcNetAddress = brpcAddresses.get(instanceInfo.getAddress());
            if (brpcNetAddress == null) {
                try {
                    brpcNetAddress = SystemInfoService.toBrpcHost(instanceInfo.getAddress());
                    brpcAddresses.put(instanceInfo.getAddress(), brpcNetAddress);
                } catch (Exception e) {
                    LOG.warn("collectQueryStatistics failed to find BE [{}:{}] [QueryID={}]",
                            instanceInfo.getAddress().getHostname(), instanceInfo.getAddress().getPort(),
                            DebugUtil.printId(item.getExecutionId()), e);
                    continue;
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
                if (result != null && result.queryStatistics != null) {
                    Preconditions.checkState(result.queryStatistics.size() == 1);
                    PCollectQueryStatistics queryStatistics = result.queryStatistics.get(0);
                    QueryStatistics statistics = new QueryStatistics();
                    statistics.updateCpuCostNs(queryStatistics.cpuCostNs);
                    statistics.updateScanBytes(queryStatistics.scanBytes);
                    statistics.updateScanRows(queryStatistics.scanRows);
                    statistics.updateMemUsageBytes(queryStatistics.memUsageBytes);
                    if (queryStatistics.spillBytes != null) {
                        statistics.updateSpillBytes(queryStatistics.spillBytes);
                    }
                    final Request request = pair.first;
                    String host = String.format("%s:%d",
                            request.getAddress().getHostname(), request.getAddress().getPort());
                    statisticsMap.put(host, statistics);
                }
            } catch (InterruptedException e) {
                LOG.warn("Thread interrupted! ", e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                String strQueryIds = pair.first.getQueryIds().stream()
                        .map(DebugUtil::printId).
                        collect(Collectors.joining(","));
                LOG.warn("collectQueryStatistics failed to receive result from BE [{}:{}] [QueryID={}]",
                        pair.first.address.getHostname(), pair.first.address.getPort(), strQueryIds, e);
            }
        }
        return statisticsMap;
    }

    private Map<String, QueryStatistics> collectQueryStatistics(Collection<QueryStatisticsItem> items) {
        final Map<TNetworkAddress, Request> requests = Maps.newHashMap();
        final Map<TNetworkAddress, TNetworkAddress> brpcAddresses = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            for (QueryStatisticsItem.FragmentInstanceInfo instanceInfo : item.getFragmentInstanceInfos()) {
                TNetworkAddress brpcNetAddress = brpcAddresses.get(instanceInfo.getAddress());
                if (brpcNetAddress == null) {
                    try {
                        brpcNetAddress = SystemInfoService.toBrpcHost(instanceInfo.getAddress());
                        brpcAddresses.put(instanceInfo.getAddress(), brpcNetAddress);
                    } catch (Exception e) {
                        LOG.warn("collectQueryStatistics failed to find BE [{}:{}] [QueryID={}]",
                                instanceInfo.getAddress().getHostname(), instanceInfo.getAddress().getPort(),
                                DebugUtil.printId(item.getExecutionId()), e);
                        continue;
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
            Map<TNetworkAddress, Request> requests) {
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
                String strQueryIds = request.getQueryIds().stream()
                        .map(DebugUtil::printId).
                        collect(Collectors.joining(","));
                LOG.warn("collectQueryStatistics failed to send request to BE [{}:{}] [QueryIDs={}]",
                        address.getHostname(), address.getPort(), strQueryIds, e);
            }
        }
        return futures;
    }

    private Map<String, QueryStatistics> handleCollectQueryResponse(
            List<Pair<Request, Future<PCollectQueryStatisticsResult>>> futures) {
        Map<String, QueryStatistics> statisticsMap = Maps.newHashMap();
        for (Pair<Request, Future<PCollectQueryStatisticsResult>> pair : futures) {
            try {
                final PCollectQueryStatisticsResult result = pair.second.get(10, TimeUnit.SECONDS);
                if (result != null && result.queryStatistics != null) {
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
                        statistics.updateMemUsageBytes(queryStatistics.memUsageBytes);
                        if (queryStatistics.spillBytes != null) {
                            statistics.updateSpillBytes(queryStatistics.spillBytes);
                        }
                    }
                }
            } catch (InterruptedException e) {
                LOG.warn("Thread interrupt! ", e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException | TimeoutException e) {
                String strQueryIds = pair.first.getQueryIds().stream()
                        .map(DebugUtil::printId).
                        collect(Collectors.joining(","));
                LOG.warn("collectQueryStatistics failed to receive result from BE [{}:{}] [QueryID={}]",
                        pair.first.address.getHostname(), pair.first.address.getPort(), strQueryIds, e);
            }
        }
        return statisticsMap;
    }

    public static class QueryStatistics {
        long cpuCostNs = 0;
        long scanBytes = 0;
        long scanRows = 0;
        long memUsageBytes = 0;
        long spillBytes = 0;

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

        public long getMemUsageBytes() {
            return memUsageBytes;
        }

        public void updateMemUsageBytes(long value) {
            memUsageBytes += value;
        }

        public void updateSpillBytes(long value) {
            spillBytes += value;
        }

        public long getSpillBytes() {
            return spillBytes;
        }
    }

    private static class Request {
        private final TNetworkAddress address;
        private final Set<TUniqueId> queryIds;

        public Request(TNetworkAddress address) {
            this.address = address;
            this.queryIds = new HashSet<>();
        }

        public TNetworkAddress getAddress() {
            return address;
        }

        public List<TUniqueId> getQueryIds() {
            return Lists.newArrayList(queryIds);
        }

        public void addQueryId(TUniqueId queryId) {
            this.queryIds.add(queryId);
        }
    }
}
