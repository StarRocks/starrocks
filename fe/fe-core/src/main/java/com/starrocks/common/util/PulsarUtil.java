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

package com.starrocks.common.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.UserException;
import com.starrocks.proto.PPulsarBacklogBatchProxyRequest;
import com.starrocks.proto.PPulsarBacklogProxyRequest;
import com.starrocks.proto.PPulsarBacklogProxyResult;
import com.starrocks.proto.PPulsarLoadInfo;
import com.starrocks.proto.PPulsarMetaProxyRequest;
import com.starrocks.proto.PPulsarProxyRequest;
import com.starrocks.proto.PPulsarProxyResult;
import com.starrocks.proto.PStringPair;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.warehouse.Warehouse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class PulsarUtil {
    private static final Logger LOG = LogManager.getLogger(PulsarUtil.class);

    private static final PulsarUtil.ProxyAPI PROXY_API = new PulsarUtil.ProxyAPI();

    public static List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                      ImmutableMap<String, String> properties) throws UserException {
        return PROXY_API.getAllPulsarPartitions(serviceUrl, topic, subscription, properties);
    }

    public static Map<String, Long> getBacklogNums(String serviceUrl, String topic, String subscription,
                                                   ImmutableMap<String, String> properties,
                                                   List<String> partitions) throws UserException {
        return PROXY_API.getBacklogNums(serviceUrl, topic, subscription, properties, partitions);
    }

    public static List<PPulsarBacklogProxyResult> getBatchBacklogNums(List<PPulsarBacklogProxyRequest> requests)
            throws UserException {
        return PROXY_API.getBatchBacklogNums(requests);
    }

    public static PPulsarLoadInfo genPPulsarLoadInfo(String serviceUrl, String topic, String subscription,
                                                     ImmutableMap<String, String> properties) {
        PPulsarLoadInfo pulsarLoadInfo = new PPulsarLoadInfo();
        pulsarLoadInfo.serviceUrl = serviceUrl;
        pulsarLoadInfo.topic = topic;
        pulsarLoadInfo.subscription = subscription;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            PStringPair pair = new PStringPair();
            pair.key = entry.getKey();
            pair.val = entry.getValue();
            if (pulsarLoadInfo.properties == null) {
                pulsarLoadInfo.properties = Lists.newArrayList();
            }
            pulsarLoadInfo.properties.add(pair);
        }
        return pulsarLoadInfo;
    }

    static class ProxyAPI {
        public List<String> getAllPulsarPartitions(String serviceUrl, String topic, String subscription,
                                                   ImmutableMap<String, String> convertedCustomProperties)
                throws UserException {
            // create request
            PPulsarMetaProxyRequest metaRequest = new PPulsarMetaProxyRequest();
            metaRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, convertedCustomProperties);
            PPulsarProxyRequest request = new PPulsarProxyRequest();
            request.pulsarMetaRequest = metaRequest;

            PPulsarProxyResult result = sendProxyRequest(request);
            return result.pulsarMetaResult.partitions;
        }

        public Map<String, Long> getBacklogNums(String serviceUrl, String topic, String subscription,
                                                ImmutableMap<String, String> properties, List<String> partitions)
                throws UserException {
            // create request
            PPulsarBacklogProxyRequest backlogRequest = new PPulsarBacklogProxyRequest();
            backlogRequest.pulsarInfo = genPPulsarLoadInfo(serviceUrl, topic, subscription, properties);
            backlogRequest.partitions = partitions;
            PPulsarProxyRequest request = new PPulsarProxyRequest();
            request.pulsarBacklogRequest = backlogRequest;

            // send request
            PPulsarProxyResult result = sendProxyRequest(request);

            // assembly result
            Map<String, Long> partitionBacklogs = Maps.newHashMapWithExpectedSize(partitions.size());
            List<Long> backlogs = result.pulsarBacklogResult.backlogNums;
            for (int i = 0; i < result.pulsarBacklogResult.partitions.size(); i++) {
                partitionBacklogs.put(result.pulsarBacklogResult.partitions.get(i), backlogs.get(i));
            }
            return partitionBacklogs;
        }

        public List<PPulsarBacklogProxyResult> getBatchBacklogNums(List<PPulsarBacklogProxyRequest> requests)
                throws UserException {
            // create request
            PPulsarProxyRequest pProxyRequest = new PPulsarProxyRequest();
            PPulsarBacklogBatchProxyRequest pPulsarBacklogBatchProxyRequest = new PPulsarBacklogBatchProxyRequest();
            pPulsarBacklogBatchProxyRequest.requests = requests;
            pProxyRequest.pulsarBacklogBatchRequest = pPulsarBacklogBatchProxyRequest;

            // send request
            PPulsarProxyResult result = sendProxyRequest(pProxyRequest);

            return result.pulsarBacklogBatchResult.results;
        }

        private PPulsarProxyResult sendProxyRequest(PPulsarProxyRequest request) throws UserException {
            TNetworkAddress address = new TNetworkAddress();
            try {
                // TODO: need to refactor after be split into cn + dn
                List<Long> nodeIds = new ArrayList<>();
                if ((RunMode.isSharedDataMode())) {
                    Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getDefaultWarehouse();
                    for (long nodeId : warehouse.getAnyAvailableCluster().getComputeNodeIds()) {
                        ComputeNode node =
                                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeId);
                        if (node != null && node.isAlive()) {
                            nodeIds.add(nodeId);
                        }
                    }
                    if (nodeIds.isEmpty()) {
                        throw new LoadException("Failed to send proxy request. No alive backends or computeNodes");
                    }
                } else {
                    nodeIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
                    if (nodeIds.isEmpty()) {
                        throw new LoadException("Failed to send proxy request. No alive backends");
                    }
                }

                Collections.shuffle(nodeIds);

                ComputeNode be =
                        GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(nodeIds.get(0));
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());

                // get info
                request.timeout = Config.routine_load_pulsar_timeout_second;
                Future<PPulsarProxyResult> future = BackendServiceClient.getInstance().getPulsarInfo(address, request);
                PPulsarProxyResult result = future.get(Config.routine_load_pulsar_timeout_second, TimeUnit.SECONDS);
                TStatusCode code = TStatusCode.findByValue(result.status.statusCode);
                if (code != TStatusCode.OK) {
                    LOG.warn("failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                    throw new UserException(
                            "failed to send proxy request to " + address + " err " + result.status.errorMsgs);
                } else {
                    return result;
                }
            } catch (InterruptedException ie) {
                LOG.warn("got interrupted exception when sending proxy request to " + address);
                Thread.currentThread().interrupt();
                throw new LoadException("got interrupted exception when sending proxy request to " + address);
            } catch (Exception e) {
                LOG.warn("failed to send proxy request to " + address + " err " + e.getMessage());
                throw new LoadException("failed to send proxy request to " + address + " err " + e.getMessage());
            }
        }
    }
}

